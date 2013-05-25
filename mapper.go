// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
//
package gorp

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"reflect"
	"strings"
)

// DbMap is the root gorp mapping object. Create one of these for each
// database schema you wish to map.  Each DbMap contains a list of
// mapped tables.
//
// Example:
//
//     dialect := gorp.MySQLDialect{"InnoDB", "UTF8"}
//     dbmap := &gorp.DbMap{Db: db, Dialect: dialect}
//
type DbMap struct {
	// Db handle to use with this map
	Db  *sql.DB
	Dbx *sqlx.DB

	// Dialect implementation to use with this map
	Dialect Dialect

	tables    []*TableMap
	logger    *log.Logger
	logPrefix string
}

// Return a new DbMap using the db connection and dialect
func NewDbMap(db *sql.DB, dialect Dialect) *DbMap {
	return &DbMap{Db: db, Dialect: dialect, Dbx: &sqlx.DB{*db}}
}

// TraceOn turns on SQL statement logging for this DbMap.  After this is
// called, all SQL statements will be sent to the logger.  If prefix is
// a non-empty string, it will be written to the front of all logged
// strings, which can aid in filtering log lines.
//
// Use TraceOn if you want to spy on the SQL statements that gorp
// generates.
func (m *DbMap) TraceOn(prefix string, logger *log.Logger) {
	m.logger = logger
	if len(prefix) == 0 {
		m.logPrefix = prefix
	} else {
		m.logPrefix = prefix + " "
	}
}

// TraceOff turns off tracing. It is idempotent.
func (m *DbMap) TraceOff() {
	m.logger = nil
	m.logPrefix = ""
}

// AddTable registers the given interface type with gorp. The table name
// will be given the name of the TypeOf(i), lowercased.
//
// This operation is idempotent. If i's type is already mapped, the
// existing *TableMap is returned
func (m *DbMap) AddTable(i interface{}, name ...string) *TableMap {
	Name := ""
	if len(name) > 0 {
		Name = name[0]
	}

	t := reflect.TypeOf(i)
	if len(Name) == 0 {
		Name = strings.ToLower(t.Name())
	}

	// check if we have a table for this type already
	// if so, update the name and return the existing pointer
	for i := range m.tables {
		table := m.tables[i]
		if table.gotype == t {
			table.TableName = Name
			return table
		}
	}

	tmap := &TableMap{gotype: t, TableName: Name, dbmap: m}

	n := t.NumField()
	tmap.columns = make([]*ColumnMap, 0, n)
	for i := 0; i < n; i++ {
		f := t.Field(i)
		columnName := f.Tag.Get("db")
		if columnName == "" {
			columnName = f.Name
		}

		cm := &ColumnMap{
			ColumnName: columnName,
			Transient:  columnName == "-",
			fieldName:  f.Name,
			gotype:     f.Type,
		}
		tmap.columns = append(tmap.columns, cm)
		if cm.fieldName == "Version" {
			tmap.version = tmap.columns[len(tmap.columns)-1]
		}
	}
	m.tables = append(m.tables, tmap)

	return tmap

}

func (m *DbMap) AddTableWithName(i interface{}, name string) *TableMap {
	return m.AddTable(i, name)
}

// CreateTables iterates through TableMaps registered to this DbMap and
// executes "create table" statements against the database for each.
//
// This is particularly useful in unit tests where you want to create
// and destroy the schema automatically.
func (m *DbMap) CreateTables() error {
	return m.createTables(false)
}

// CreateTablesIfNotExists is similar to CreateTables, but starts
// each statement with "create table if not exists" so that existing
// tables do not raise errors
func (m *DbMap) CreateTablesIfNotExists() error {
	return m.createTables(true)
}

func (m *DbMap) createTables(ifNotExists bool) error {
	var err error
	for i := range m.tables {
		table := m.tables[i]

		create := "create table"
		if ifNotExists {
			create += " if not exists"
		}
		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("%s %s (", create, m.Dialect.QuoteField(table.TableName)))
		x := 0
		for _, col := range table.columns {
			if !col.Transient {
				if x > 0 {
					s.WriteString(", ")
				}
				stype := m.Dialect.ToSqlType(col.gotype, col.MaxSize, col.isAutoIncr)
				s.WriteString(fmt.Sprintf("%s %s", m.Dialect.QuoteField(col.ColumnName), stype))

				if col.isPK {
					s.WriteString(" not null")
					if len(table.keys) == 1 {
						s.WriteString(" primary key")
					}
				}
				if col.Unique {
					s.WriteString(" unique")
				}
				if col.isAutoIncr {
					s.WriteString(fmt.Sprintf(" %s", m.Dialect.AutoIncrStr()))
				}

				x++
			}
		}
		if len(table.keys) > 1 {
			s.WriteString(", primary key (")
			for x := range table.keys {
				if x > 0 {
					s.WriteString(", ")
				}
				s.WriteString(m.Dialect.QuoteField(table.keys[x].ColumnName))
			}
			s.WriteString(")")
		}
		s.WriteString(") ")
		s.WriteString(m.Dialect.CreateTableSuffix())
		s.WriteString(";")
		_, err = m.Exec(s.String())
		if err != nil {
			break
		}
	}
	return err
}

// DropTables iterates through TableMaps registered to this DbMap and
// executes "drop table" statements against the database for each.
func (m *DbMap) DropTables() error {
	var err error
	for i := range m.tables {
		table := m.tables[i]
		_, e := m.Exec(fmt.Sprintf("drop table %s;", m.Dialect.QuoteField(table.TableName)))
		if e != nil {
			err = e
		}
	}
	return err
}
