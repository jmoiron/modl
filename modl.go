package modl

// Changes Copyright 2013 Jason Moiron.  Original Gorp code
// Copyright 2012 James Cooper. All rights reserved.
//
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
//
// Source code and project home:
// https://github.com/jmoiron/modl

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
)

// NoKeysErr is a special error type returned when modl's CRUD helpers are
// used on tables which have not been set up with a primary key.
type NoKeysErr struct {
	Table *TableMap
}

// Error returns the string representation of a NoKeysError.
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

// Insert has the same behavior as DbMap.Insert(), but runs in a transaction.
func (t *Transaction) Insert(list ...interface{}) error {
	return insert(t.dbmap, t, list...)
}

// Update has the same behavior as DbMap.Update(), but runs in a transaction.
func (t *Transaction) Update(list ...interface{}) (int64, error) {
	return update(t.dbmap, t, list...)
}

// Delete has the same behavior as DbMap.Delete(), but runs in a transaction.
func (t *Transaction) Delete(list ...interface{}) (int64, error) {
	return delete(t.dbmap, t, list...)
}

// Get has the Same behavior as DbMap.Get(), but runs in a transaction.
func (t *Transaction) Get(dest interface{}, keys ...interface{}) error {
	return get(t.dbmap, t, dest, keys...)
}

// Select has the Same behavior as DbMap.Select(), but runs in a transaction.
func (t *Transaction) Select(dest interface{}, query string, args ...interface{}) error {
	return hookedselect(t.dbmap, t, dest, query, args...)
}

// Exec has the same behavior as DbMap.Exec(), but runs in a transaction.
func (t *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	t.dbmap.trace(query, args)
	return t.tx.Exec(query, args...)
}

// Commit commits the underlying database transaction.
func (t *Transaction) Commit() error {
	t.dbmap.trace("commit;")
	return t.tx.Commit()
}

// Rollback rolls back the underlying database transaction.
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
		return fmt.Errorf("could not find table for %v", dest)
	}
	if len(table.Keys) < 1 {
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
