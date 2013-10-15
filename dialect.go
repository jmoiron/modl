package modl

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"reflect"
	"strings"
)

// The Dialect interface encapsulates behaviors that differ across
// SQL databases.  At present the Dialect is only used by CreateTables()
// but this could change in the future
type Dialect interface {

	// ToSqlType returns the SQL column type to use when creating a
	// table of the given Go Type.  maxsize can be used to switch based on
	// size.  For example, in MySQL []byte could map to BLOB, MEDIUMBLOB,
	// or LONGBLOB depending on the maxsize
	ToSqlType(col *ColumnMap) string

	// string to append to primary key column definitions
	AutoIncrStr() string

	AutoIncrBindValue() string

	AutoIncrInsertSuffix(col *ColumnMap) string

	// string to append to "create table" statement for vendor specific
	// table attributes
	CreateTableSuffix() string

	InsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error)

	// bind variable string to use when forming SQL statements
	// in many dbs it is "?", but Postgres appears to use $1
	//
	// i is a zero based index of the bind variable in this statement
	//
	BindVar(i int) string

	// Handles quoting of a field name to ensure that it doesn't raise any
	// SQL parsing exceptions by using a reserved word as a field name.
	QuoteField(field string) string

	// string used to truncate tables
	TruncateClause() string

	// Get the driver name from a dialect
	DriverName() string
}

func standardInsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error) {
	res, err := exec.Exec(insertSql, params...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

///////////////////////////////////////////////////////
// sqlite3 //
/////////////

type SqliteDialect struct {
	suffix string
}

func (d SqliteDialect) DriverName() string {
	return "sqlite"
}

func (d SqliteDialect) ToSqlType(col *ColumnMap) string {
	switch col.gotype.Kind() {
	case reflect.Bool:
		return "integer"
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float64, reflect.Float32:
		return "real"
	case reflect.Slice:
		if col.gotype.Elem().Kind() == reflect.Uint8 {
			return "blob"
		}
	}

	switch col.gotype.Name() {
	case "NullableInt64":
		return "integer"
	case "NullableFloat64":
		return "real"
	case "NullableBool":
		return "integer"
	case "NullableBytes":
		return "blob"
	case "Time", "NullTime":
		return "datetime"
	}

	// sqlite ignores maxsize, so we will do that here too
	return fmt.Sprintf("text")
}

// Returns autoincrement
func (d SqliteDialect) AutoIncrStr() string {
	return "autoincrement"
}

func (d SqliteDialect) AutoIncrBindValue() string {
	return "null"
}

func (d SqliteDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return ""
}

// Returns suffix
func (d SqliteDialect) CreateTableSuffix() string {
	return d.suffix
}

// Returns "?"
func (d SqliteDialect) BindVar(i int) string {
	return "?"
}

func (d SqliteDialect) InsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error) {
	return standardInsertAutoIncr(exec, insertSql, params...)
}

func (d SqliteDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

// With sqlite, there technically isn't a TRUNCATE statement,
// but a DELETE FROM uses a truncate optimization:
// http://www.sqlite.org/lang_delete.html
func (d SqliteDialect) TruncateClause() string {
	return "delete from"
}

///////////////////////////////////////////////////////
// PostgreSQL //
////////////////

type PostgresDialect struct {
	suffix string
}

func (d PostgresDialect) DriverName() string {
	return "postgres"
}

func (d PostgresDialect) ToSqlType(col *ColumnMap) string {

	switch col.gotype.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Uint16, reflect.Uint32:
		if col.isAutoIncr {
			return "serial"
		}
		return "integer"
	case reflect.Int64, reflect.Uint64:
		if col.isAutoIncr {
			return "bigserial"
		}
		return "bigint"
	case reflect.Float64, reflect.Float32:
		return "real"
	case reflect.Slice:
		if col.gotype.Elem().Kind() == reflect.Uint8 {
			return "bytea"
		}
	}

	switch col.gotype.Name() {
	case "NullableInt64":
		return "bigint"
	case "NullableFloat64":
		return "double"
	case "NullableBool":
		return "smallint"
	case "NullableBytes":
		return "bytea"
	case "Time", "Nulltime":
		return "timestamp with time zone"
	}

	maxsize := col.MaxSize
	if col.MaxSize < 1 {
		maxsize = 255
	}
	return fmt.Sprintf("varchar(%d)", maxsize)
}

// Returns empty string
func (d PostgresDialect) AutoIncrStr() string {
	return ""
}

func (d PostgresDialect) AutoIncrBindValue() string {
	return "default"
}

func (d PostgresDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return " returning " + col.ColumnName
}

// Returns suffix
func (d PostgresDialect) CreateTableSuffix() string {
	return d.suffix
}

// Returns "$(i+1)"
func (d PostgresDialect) BindVar(i int) string {
	return fmt.Sprintf("$%d", i+1)
}

func (d PostgresDialect) InsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error) {
	rows, err := exec.query(insertSql, params...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if rows.Next() {
		var id int64
		err := rows.Scan(&id)
		return id, err
	}

	return 0, errors.New("No serial value returned for insert: " + insertSql + ", error: " + rows.Err().Error())
}

func (d PostgresDialect) QuoteField(f string) string {
	return `"` + sqlx.NameMapper(f) + `"`
}

func (d PostgresDialect) TruncateClause() string {
	return "truncate"
}

///////////////////////////////////////////////////////
// MySQL //
///////////

// Implementation of Dialect for MySQL databases.
type MySQLDialect struct {

	// Engine is the storage engine to use "InnoDB" vs "MyISAM" for example
	Engine string

	// Encoding is the character encoding to use for created tables
	Encoding string
}

func (d MySQLDialect) DriverName() string {
	return "mysql"
}

func (m MySQLDialect) ToSqlType(col *ColumnMap) string {
	switch col.gotype.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Uint16, reflect.Uint32:
		return "int"
	case reflect.Int64, reflect.Uint64:
		return "bigint"
	case reflect.Float64, reflect.Float32:
		return "double"
	case reflect.Slice:
		if col.gotype.Elem().Kind() == reflect.Uint8 {
			return "mediumblob"
		}
	}

	switch col.gotype.Name() {
	case "NullableInt64":
		return "bigint"
	case "NullableFloat64":
		return "double"
	case "NullableBool":
		return "tinyint"
	case "NullableBytes":
		return "mediumblob"
	case "Time", "NullTime":
		return "datetime"
	}

	maxsize := col.MaxSize
	if col.MaxSize < 1 {
		maxsize = 255
	}
	return fmt.Sprintf("varchar(%d)", maxsize)
}

// Returns auto_increment
func (m MySQLDialect) AutoIncrStr() string {
	return "auto_increment"
}

func (m MySQLDialect) AutoIncrBindValue() string {
	return "null"
}

func (m MySQLDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return ""
}

// Returns engine=%s charset=%s  based on values stored on struct
func (m MySQLDialect) CreateTableSuffix() string {
	return fmt.Sprintf(" engine=%s charset=%s", m.Engine, m.Encoding)
}

// Returns "?"
func (m MySQLDialect) BindVar(i int) string {
	return "?"
}

func (m MySQLDialect) InsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error) {
	return standardInsertAutoIncr(exec, insertSql, params...)
}

func (d MySQLDialect) QuoteField(f string) string {
	return "`" + f + "`"
}

// Formats the bindvars in the query string (these are '?') for the dialect.
func ReBind(query string, dialect Dialect) string {

	binder := dialect.BindVar(0)
	if binder == "?" {
		return query
	}

	for i, j := 0, strings.Index(query, "?"); j >= 0; i++ {
		query = strings.Replace(query, "?", dialect.BindVar(i), 1)
		j = strings.Index(query, "?")
	}
	return query
}

func (m MySQLDialect) TruncateClause() string {
	return "truncate"
}
