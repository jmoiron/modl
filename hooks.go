package modl

import (
	"reflect"
)

type PreInserter interface {
	PreInsert(SqlExecutor) error
}

type PostInserter interface {
	PostInsert(SqlExecutor) error
}

type PostGetter interface {
	PostGet(SqlExecutor) error
}

type PreUpdater interface {
	PreUpdate(SqlExecutor) error
}

type PostUpdater interface {
	PostUpdate(SqlExecutor) error
}

type PreDeleter interface {
	PreDelete(SqlExecutor) error
}

type PostDeleter interface {
	PostDelete(SqlExecutor) error
}

// Determine which hooks are supported by the mapper struct i
func (t *TableMap) setupHooks(i interface{}) {
	// These hooks must be implemented on a pointer, so if a value is passed in
	// we have to get a pointer for a new value of that type in order for the
	// type assertions to pass.
	ptr := i
	if reflect.ValueOf(i).Kind() == reflect.Struct {
		ptr = reflect.New(reflect.ValueOf(i).Type()).Interface()
	}

	_, t.CanPreInsert = ptr.(PreInserter)
	_, t.CanPostInsert = ptr.(PostInserter)
	_, t.CanPostGet = ptr.(PostGetter)
	_, t.CanPreUpdate = ptr.(PreUpdater)
	_, t.CanPostUpdate = ptr.(PostUpdater)
	_, t.CanPreDelete = ptr.(PreDeleter)
	_, t.CanPostDelete = ptr.(PostDeleter)
}
