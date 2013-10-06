// Changes Copyright 2013 Jason Moiron.  Original Gorp code
// Copyright 2012 James Cooper. All rights reserved.
//
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
//
// Source code and project home:
// https://github.com/jmoiron/modl
//
package modl

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"reflect"
	"strings"
)

type NoKeysErr struct {
	Table *TableMap
}

func (n NoKeysErr) Error() string {
	return fmt.Sprintf("Could not find keys for table %v", n.Table)
}

const versFieldConst = "[modl_ver_field]"

// OptimisticLockError is returned by Update() or Delete() if the
// struct being modified has a Version field and the value is not equal to
// the current value in the database
type OptimisticLockError struct {
	// Table name where the lock error occurred
	TableName string

	// Primary key values of the row being updated/deleted
	Keys []interface{}

	// true if a row was found with those keys, indicating the
	// LocalVersion is stale.  false if no value was found with those
	// keys, suggesting the row has been deleted since loaded, or
	// was never inserted to begin with
	RowExists bool

	// Version value on the struct passed to Update/Delete. This value is
	// out of sync with the database.
	LocalVersion int64
}

// Error returns a description of the cause of the lock error
func (e OptimisticLockError) Error() string {
	if e.RowExists {
		return fmt.Sprintf("OptimisticLockError table=%s keys=%v out of date version=%d", e.TableName, e.Keys, e.LocalVersion)
	}

	return fmt.Sprintf("OptimisticLockError no row found for table=%s keys=%v", e.TableName, e.Keys)
}

// TableMap represents a mapping between a Go struct and a database table
// Use dbmap.AddTable() or dbmap.AddTableWithName() to create these
type TableMap struct {
	// Name of database table.
	TableName  string
	gotype     reflect.Type
	columns    []*ColumnMap
	keys       []*ColumnMap
	version    *ColumnMap
	insertPlan bindPlan
	updatePlan bindPlan
	deletePlan bindPlan
	getPlan    bindPlan
	dbmap      *DbMap
	// Cached capabilities for the struct mapped to this table
	CanPreInsert  bool
	CanPostInsert bool
	CanPostGet    bool
	CanPreUpdate  bool
	CanPostUpdate bool
	CanPreDelete  bool
	CanPostDelete bool
}

// ResetSql removes cached insert/update/select/delete SQL strings
// associated with this TableMap.  Call this if you've modified
// any column names or the table name itself.
func (t *TableMap) ResetSql() {
	t.insertPlan = bindPlan{}
	t.updatePlan = bindPlan{}
	t.deletePlan = bindPlan{}
	t.getPlan = bindPlan{}
}

// SetKeys lets you specify the fields on a struct that map to primary
// key columns on the table.  If isAutoIncr is set, result.LastInsertId()
// will be used after INSERT to bind the generated id to the Go struct.
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
func (t *TableMap) SetKeys(isAutoIncr bool, fieldNames ...string) *TableMap {
	t.keys = make([]*ColumnMap, 0)
	for _, name := range fieldNames {
		colmap := t.ColMap(strings.ToLower(name))
		colmap.isPK = true
		colmap.isAutoIncr = isAutoIncr
		t.keys = append(t.keys, colmap)
	}
	t.ResetSql()

	return t
}

// ColMap returns the ColumnMap pointer matching the given struct field
// name.  It panics if the struct does not contain a field matching this
// name.
func (t *TableMap) ColMap(field string) *ColumnMap {
	col := colMapOrNil(t, field)
	if col == nil {
		e := fmt.Sprintf("No ColumnMap in table %s type %s with field %s",
			t.TableName, t.gotype.Name(), field)

		panic(e)
	}
	return col
}

func colMapOrNil(t *TableMap, field string) *ColumnMap {
	for _, col := range t.columns {
		if col.fieldName == field || col.ColumnName == field {
			return col
		}
	}
	return nil
}

// SetVersionCol sets the column to use as the Version field.  By default
// the "Version" field is used.  Returns the column found, or panics
// if the struct does not contain a field matching this name.
//
// Automatically calls ResetSql() to ensure SQL statements are regenerated.
func (t *TableMap) SetVersionCol(field string) *ColumnMap {
	c := t.ColMap(field)
	t.version = c
	t.ResetSql()
	return c
}

// A bindPlan saves a query type (insert, get, updated, delete) so it doesn't
// have to be re-created every time it's executed.
type bindPlan struct {
	query       string
	argFields   []string
	keyFields   []string
	versField   string
	autoIncrIdx int
}

func (plan bindPlan) createBindInstance(elem reflect.Value) bindInstance {
	bi := bindInstance{query: plan.query, autoIncrIdx: plan.autoIncrIdx, versField: plan.versField}
	if plan.versField != "" {
		bi.existingVersion = elem.FieldByName(plan.versField).Int()
	}

	for i := 0; i < len(plan.argFields); i++ {
		k := plan.argFields[i]
		if k == versFieldConst {
			newVer := bi.existingVersion + 1
			bi.args = append(bi.args, newVer)
			if bi.existingVersion == 0 {
				elem.FieldByName(plan.versField).SetInt(int64(newVer))
			}
		} else {
			val := elem.FieldByName(k).Interface()
			bi.args = append(bi.args, val)
		}
	}

	for i := 0; i < len(plan.keyFields); i++ {
		k := plan.keyFields[i]
		val := elem.FieldByName(k).Interface()
		bi.keys = append(bi.keys, val)
	}

	return bi
}

type bindInstance struct {
	query           string
	args            []interface{}
	keys            []interface{}
	existingVersion int64
	versField       string
	autoIncrIdx     int
}

func (t *TableMap) bindInsert(elem reflect.Value) bindInstance {
	plan := t.insertPlan
	if plan.query == "" {
		plan.autoIncrIdx = -1

		s := bytes.Buffer{}
		s2 := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("insert into %s (", t.dbmap.Dialect.QuoteField(t.TableName)))

		x := 0
		first := true
		for y := range t.columns {
			col := t.columns[y]

			if !col.Transient {
				if !first {
					s.WriteString(",")
					s2.WriteString(",")
				}
				s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))

				if col.isAutoIncr {
					s2.WriteString(t.dbmap.Dialect.AutoIncrBindValue())
					plan.autoIncrIdx = y
				} else {
					s2.WriteString(t.dbmap.Dialect.BindVar(x))
					if col == t.version {
						plan.versField = col.fieldName
						plan.argFields = append(plan.argFields, versFieldConst)
					} else {
						plan.argFields = append(plan.argFields, col.fieldName)
					}

					x++
				}

				first = false
			}
		}
		s.WriteString(") values (")
		s.WriteString(s2.String())
		s.WriteString(")")
		if plan.autoIncrIdx > -1 {
			s.WriteString(t.dbmap.Dialect.AutoIncrInsertSuffix(t.columns[plan.autoIncrIdx]))
		}
		s.WriteString(";")

		plan.query = s.String()
		t.insertPlan = plan
	}

	return plan.createBindInstance(elem)
}

func (t *TableMap) bindUpdate(elem reflect.Value) bindInstance {
	plan := t.updatePlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("update %s set ", t.dbmap.Dialect.QuoteField(t.TableName)))
		x := 0

		for y := range t.columns {
			col := t.columns[y]
			if !col.isPK && !col.Transient {
				if x > 0 {
					s.WriteString(", ")
				}
				s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
				s.WriteString("=")
				s.WriteString(t.dbmap.Dialect.BindVar(x))

				if col == t.version {
					plan.versField = col.fieldName
					plan.argFields = append(plan.argFields, versFieldConst)
				} else {
					plan.argFields = append(plan.argFields, col.fieldName)
				}
				x++
			}
		}

		s.WriteString(" where ")
		for y := range t.keys {
			col := t.keys[y]
			if y > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.argFields = append(plan.argFields, col.fieldName)
			plan.keyFields = append(plan.keyFields, col.fieldName)
			x++
		}
		if plan.versField != "" {
			s.WriteString(" and ")
			s.WriteString(t.dbmap.Dialect.QuoteField(t.version.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))
			plan.argFields = append(plan.argFields, plan.versField)
		}
		s.WriteString(";")

		plan.query = s.String()
		t.updatePlan = plan
	}

	return plan.createBindInstance(elem)
}

func (t *TableMap) bindDelete(elem reflect.Value) bindInstance {
	plan := t.deletePlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("delete from %s", t.dbmap.Dialect.QuoteField(t.TableName)))

		for y := range t.columns {
			col := t.columns[y]
			if !col.Transient {
				if col == t.version {
					plan.versField = col.fieldName
				}
			}
		}

		s.WriteString(" where ")
		for x := range t.keys {
			k := t.keys[x]
			if x > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(k.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.keyFields = append(plan.keyFields, k.fieldName)
			plan.argFields = append(plan.argFields, k.fieldName)
		}
		if plan.versField != "" {
			s.WriteString(" and ")
			s.WriteString(t.dbmap.Dialect.QuoteField(t.version.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(len(plan.argFields)))

			plan.argFields = append(plan.argFields, plan.versField)
		}
		s.WriteString(";")

		plan.query = s.String()
		t.deletePlan = plan
	}

	return plan.createBindInstance(elem)
}

func (t *TableMap) bindGet() bindPlan {
	plan := t.getPlan
	if plan.query == "" {

		s := bytes.Buffer{}
		s.WriteString("select ")

		x := 0
		for _, col := range t.columns {
			if !col.Transient {
				if x > 0 {
					s.WriteString(",")
				}
				s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
				plan.argFields = append(plan.argFields, col.fieldName)
				x++
			}
		}
		s.WriteString(" from ")
		s.WriteString(t.dbmap.Dialect.QuoteField(t.TableName))
		s.WriteString(" where ")
		for x := range t.keys {
			col := t.keys[x]
			if x > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.keyFields = append(plan.keyFields, col.fieldName)
		}
		s.WriteString(";")

		plan.query = s.String()
		t.getPlan = plan
	}

	return plan
}

// ColumnMap represents a mapping between a Go struct field and a single
// column in a table.
// Unique and MaxSize only inform the
// CreateTables() function and are not used by Insert/Update/Delete/Get.
type ColumnMap struct {
	// Column name in db table
	ColumnName string

	// If true, this column is skipped in generated SQL statements
	Transient bool

	// If true, " unique" is added to create table statements.
	// Not used elsewhere
	Unique bool

	// Passed to Dialect.ToSqlType() to assist in informing the
	// correct column type to map to in CreateTables()
	// Not used elsewhere
	MaxSize int

	fieldName  string
	gotype     reflect.Type
	sqltype    string
	isPK       bool
	isAutoIncr bool
}

// SetTransient allows you to mark the column as transient. If true
// this column will be skipped when SQL statements are generated
func (c *ColumnMap) SetTransient(b bool) *ColumnMap {
	c.Transient = b
	return c
}

// If true " unique" will be added to create table statements for this
// column
func (c *ColumnMap) SetUnique(b bool) *ColumnMap {
	c.Unique = b
	return c
}

// Set the column's sql type.  This is a string, such as 'varchar(32)' or
// 'text', which will be used by CreateTable and nothing else.  It is the
// caller's responsibility to ensure this will map cleanly to the struct
func (c *ColumnMap) SetSqlType(t string) *ColumnMap {
	c.sqltype = t
	return c
}

// SetMaxSize specifies the max length of values of this column. This is
// passed to the dialect.ToSqlType() function, which can use the value
// to alter the generated type for "create table" statements
func (c *ColumnMap) SetMaxSize(size int) *ColumnMap {
	c.MaxSize = size
	return c
}

// SqlExecutor exposes modl operations that can be run from Pre/Post
// hooks.  This hides whether the current operation that triggered the
// hook is in a transaction.
//
// See the DbMap function docs for each of the functions below for more
// information.
type SqlExecutor interface {
	Get(dest interface{}, keys ...interface{}) error
	Insert(list ...interface{}) error
	Update(list ...interface{}) (int64, error)
	Delete(list ...interface{}) (int64, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Select(dest interface{}, query string, args ...interface{}) error
	query(query string, args ...interface{}) (*sql.Rows, error)
	queryRow(query string, args ...interface{}) *sql.Row
	queryRowx(query string, args ...interface{}) *sqlx.Row
}

// Compile-time check that DbMap and Transaction implement the SqlExecutor
// interface.
var _, _ SqlExecutor = &DbMap{}, &Transaction{}

///////////////

// Transaction represents a database transaction.
// Insert/Update/Delete/Get/Exec operations will be run in the context
// of that transaction.  Transactions should be terminated with
// a call to Commit() or Rollback()
type Transaction struct {
	dbmap *DbMap
	tx    *sqlx.Tx
}

// Same behavior as DbMap.Insert(), but runs in a transaction
func (t *Transaction) Insert(list ...interface{}) error {
	return insert(t.dbmap, t, list...)
}

// Same behavior as DbMap.Update(), but runs in a transaction
func (t *Transaction) Update(list ...interface{}) (int64, error) {
	return update(t.dbmap, t, list...)
}

// Same behavior as DbMap.Delete(), but runs in a transaction
func (t *Transaction) Delete(list ...interface{}) (int64, error) {
	return delete(t.dbmap, t, list...)
}

// Same behavior as DbMap.Get(), but runs in a transaction
func (t *Transaction) Get(dest interface{}, keys ...interface{}) error {
	return get(t.dbmap, t, dest, keys...)
}

// Same behavior as DbMap.Select(), but runs in a transaction
func (t *Transaction) Select(dest interface{}, query string, args ...interface{}) error {
	return hookedselect(t.dbmap, t, dest, query, args...)
}

// Same behavior as DbMap.Exec(), but runs in a transaction
func (t *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	t.dbmap.trace(query, args)
	return t.tx.Exec(query, args...)
}

// Commits the underlying database transaction
func (t *Transaction) Commit() error {
	t.dbmap.trace("commit;")
	return t.tx.Commit()
}

// Rolls back the underlying database transaction
func (t *Transaction) Rollback() error {
	t.dbmap.trace("rollback;")
	return t.tx.Rollback()
}

func (t *Transaction) queryRow(query string, args ...interface{}) *sql.Row {
	t.dbmap.trace(query, args)
	return t.tx.QueryRow(query, args...)
}

func (t *Transaction) queryRowx(query string, args ...interface{}) *sqlx.Row {
	t.dbmap.trace(query, args)
	return t.tx.QueryRowx(query, args...)
}

func (t *Transaction) query(query string, args ...interface{}) (*sql.Rows, error) {
	t.dbmap.trace(query, args)
	return t.tx.Query(query, args...)
}

///////////////

func hookedselect(m *DbMap, exec SqlExecutor, dest interface{}, query string, args ...interface{}) error {

	// select can use arbitrary structs for join queries, so we needn't find a table
	table := m.TableFor(dest)

	err := rawselect(m, exec, dest, query, args...)
	if err != nil {
		return err
	}

	// FIXME: In order to run hooks here we have to use reflect to loop over dest
	// If we used sqlx.Rows.StructScan to pull one row at a time, we could do it
	// all at once, but then we would lose some of the efficiencies of StructScan(sql.Rows)

	// FIXME: should PostGet hooks be run on regular selects?  a PostGet
	// hook has access to the object and the database, and I'd hate for
	// a query to execute SQL on every row of a queryset.

	if table != nil && table.CanPostGet {
		var x interface{}
		v := reflect.ValueOf(dest)
		if v.Kind() == reflect.Ptr {
			v = reflect.Indirect(v)
		}
		l := v.Len()
		for i := 0; i < l; i++ {
			x = v.Index(i).Interface()
			err = x.(PostGetter).PostGet(exec)
			if err != nil {
				return err
			}

		}
	}
	return nil
}

func rawselect(m *DbMap, exec SqlExecutor, dest interface{}, query string, args ...interface{}) error {
	// FIXME: we need to verify dest is a pointer-to-slice

	// Run the query
	sqlrows, err := exec.query(query, args...)
	if err != nil {
		return err
	}

	defer sqlrows.Close()
	err = sqlx.StructScan(sqlrows, dest)
	return err

}

func get(m *DbMap, exec SqlExecutor, dest interface{}, keys ...interface{}) error {

	table := m.TableFor(dest)

	if table == nil {
		return fmt.Errorf("Could not find table for %v", dest)
	}
	if len(table.keys) < 1 {
		return &NoKeysErr{table}
	}

	plan := table.bindGet()
	row := exec.queryRowx(plan.query, keys...)
	err := row.StructScan(dest)

	if err != nil {
		return err
	}

	if table.CanPostGet {
		err = dest.(PostGetter).PostGet(exec)
		if err != nil {
			return err
		}
	}

	return nil
}

// Return a table for a pointer;  error if i is not a pointer or if the
// table is not found
func tableForPointer(m *DbMap, i interface{}, checkPk bool) (*TableMap, reflect.Value, error) {
	v := reflect.ValueOf(i)
	if v.Kind() != reflect.Ptr {
		return nil, v, fmt.Errorf("Value %v not a pointer", v)
	}
	v = v.Elem()
	t := m.TableForType(v.Type())
	if t == nil {
		return nil, v, fmt.Errorf("Could not find table for %v", t)
	}
	if checkPk && len(t.keys) < 1 {
		return t, v, &NoKeysErr{t}
	}
	return t, v, nil
}

func delete(m *DbMap, exec SqlExecutor, list ...interface{}) (int64, error) {
	var err error
	var table *TableMap
	var elem reflect.Value
	var count int64

	for _, ptr := range list {
		table, elem, err = tableForPointer(m, ptr, true)
		if err != nil {
			return -1, err
		}

		if table.CanPreDelete {
			err = ptr.(PreDeleter).PreDelete(exec)
			if err != nil {
				return -1, err
			}
		}

		bi := table.bindDelete(elem)

		res, err := exec.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		if rows == 0 && bi.existingVersion > 0 {
			return lockError(m, exec, table.TableName, bi.existingVersion, elem, bi.keys...)
		}

		count += rows

		if table.CanPostDelete {
			err = ptr.(PostDeleter).PostDelete(exec)
			if err != nil {
				return -1, err
			}
		}
	}

	return count, nil
}

func update(m *DbMap, exec SqlExecutor, list ...interface{}) (int64, error) {
	var err error
	var table *TableMap
	var elem reflect.Value
	var count int64

	for _, ptr := range list {
		table, elem, err = tableForPointer(m, ptr, true)
		if err != nil {
			return -1, err
		}

		if table.CanPreUpdate {
			err = ptr.(PreUpdater).PreUpdate(exec)
			if err != nil {
				return -1, err
			}
		}

		bi := table.bindUpdate(elem)
		if err != nil {
			return -1, err
		}

		res, err := exec.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		if rows == 0 && bi.existingVersion > 0 {
			return lockError(m, exec, table.TableName,
				bi.existingVersion, elem, bi.keys...)
		}

		if bi.versField != "" {
			elem.FieldByName(bi.versField).SetInt(bi.existingVersion + 1)
		}

		count += rows

		if table.CanPostUpdate {
			err = ptr.(PostUpdater).PostUpdate(exec)

			if err != nil {
				return -1, err
			}
		}
	}
	return count, nil
}

func insert(m *DbMap, exec SqlExecutor, list ...interface{}) error {
	var err error
	var table *TableMap
	var elem reflect.Value

	for _, ptr := range list {
		table, elem, err = tableForPointer(m, ptr, false)
		if err != nil {
			return err
		}

		if table.CanPreInsert {
			err = ptr.(PreInserter).PreInsert(exec)
			if err != nil {
				return err
			}
		}

		bi := table.bindInsert(elem)

		if bi.autoIncrIdx > -1 {
			id, err := m.Dialect.InsertAutoIncr(exec, bi.query, bi.args...)
			if err != nil {
				return err
			}
			f := elem.Field(bi.autoIncrIdx)
			k := f.Kind()
			if (k == reflect.Int) || (k == reflect.Int16) || (k == reflect.Int32) || (k == reflect.Int64) {
				f.SetInt(id)
			} else {
				return fmt.Errorf("modl: Cannot set autoincrement value on non-Int field. SQL=%s  autoIncrIdx=%d", bi.query, bi.autoIncrIdx)
			}
		} else {
			_, err := exec.Exec(bi.query, bi.args...)
			if err != nil {
				return err
			}
		}

		if table.CanPostInsert {
			err = ptr.(PostInserter).PostInsert(exec)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func lockError(m *DbMap, exec SqlExecutor, tableName string, existingVer int64, elem reflect.Value, keys ...interface{}) (int64, error) {

	dest := reflect.New(elem.Type()).Interface()
	err := get(m, exec, dest, keys...)
	if err != nil {
		return -1, err
	}

	ole := OptimisticLockError{tableName, keys, true, existingVer}
	if dest == nil {
		ole.RowExists = false
	}
	return -1, ole
}
