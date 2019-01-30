package bsql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

/*
Package for bulk insert/delete on sql storage
 */

/**
Executor for sql strings it could be transaction or db connection
 */
type BulkSQLExecutor interface {
	exec(query string, args []interface{}) (sql.Result, error)
}

type bulkExecutorDB struct {
	db *sql.DB
}

func (e *bulkExecutorDB) exec(query string, args []interface{}) (sql.Result, error) {
	return e.db.Exec(query, args...)
}

func NewExecutorDB(db *sql.DB) *bulkExecutorDB {
	return &bulkExecutorDB{
		db: db,
	}
}

type bulkExecutorTx struct {
	tx *sql.Tx
}

func (e *bulkExecutorTx) exec(query string, args []interface{}) (sql.Result, error) {
	return e.tx.Exec(query, args...)
}

func NewExecutorTx(tx *sql.Tx) *bulkExecutorTx {
	return &bulkExecutorTx{
		tx: tx,
	}
}

var unsupportedTypes = map[reflect.Kind]bool{
	reflect.Invalid:       true,
	reflect.Array:         true,
	reflect.Chan:          true,
	reflect.Func:          true,
	reflect.Interface:     true,
	reflect.Map:           true,
	reflect.Ptr:           true,
	reflect.UnsafePointer: true,
}

type bulkSQLConfig struct {
	collectID       bool
	bulkMaxSize     int
	columnsLen      int
	startSQL        string
	repeatSQLParams string
	trimSuffix      string
}

type BulkSQL struct {
	executor BulkSQLExecutor
	mu       *sync.Mutex
	args     []interface{}
	curSize  int
	config   bulkSQLConfig
	ids      []int64
}

func newBulkSQL(config bulkSQLConfig, executor BulkSQLExecutor) *BulkSQL {
	return &BulkSQL{
		executor: executor,
		mu:       &sync.Mutex{},
		args:     make([]interface{}, 0, config.columnsLen*config.bulkMaxSize),
		curSize:  0,
		config:   config,
		ids:      make([]int64, 0),
	}
}

func NewBulkInsert(table string, columns []string, bulkSize int, executor BulkSQLExecutor) *BulkSQL {
	return newBulkSQL(bulkSQLConfig{
		collectID:       true,
		bulkMaxSize:     bulkSize,
		columnsLen:      len(columns),
		startSQL:        fmt.Sprintf("insert into %s(%s) values ", table, strings.Join(columns, ", ")),
		repeatSQLParams: fmt.Sprintf("(%s),", strings.TrimSuffix(strings.Repeat("?,", len(columns)), ",")),
		trimSuffix:      ",",
	}, executor)
}

func NewBulkDelete(table string, columns []string, bulkSize int, executor BulkSQLExecutor) *BulkSQL {
	return newBulkSQL(bulkSQLConfig{
		collectID:       false,
		bulkMaxSize:     bulkSize,
		columnsLen:      len(columns),
		startSQL:        fmt.Sprintf("delete from %s where ", table),
		repeatSQLParams: fmt.Sprintf("(%s) or ", strings.Join(columns, " = ? and ")+" = ?"),
		trimSuffix:      "or ",
	}, executor)
}

func (b *BulkSQL) exec() error {

	if b.curSize == 0 {
		return nil
	}

	query := b.config.startSQL + strings.TrimSuffix(strings.Repeat(b.config.repeatSQLParams, b.curSize), b.config.trimSuffix)
	res, err := b.executor.exec(query, b.args)
	if err != nil {
		return err
	}

	if b.config.collectID {
		li, err := res.LastInsertId()
		if err != nil {
			return err
		}
		ra, err := res.RowsAffected()
		if err != nil {
			return err
		}
		var i int64 = 0
		for ; i < ra; i ++ {
			b.ids = append(b.ids, li+i)
		}
	}

	b.curSize = 0
	b.args = make([]interface{}, 0, b.config.bulkMaxSize*b.config.columnsLen)

	return nil
}

func (b *BulkSQL) Add(args ...interface{}) error {
	tempArgs := make([]interface{}, 0, b.config.columnsLen)
	for _, arg := range args {
		reflectArg := reflect.ValueOf(arg)
		if _, present := unsupportedTypes[reflectArg.Kind()]; present {
			// todo: do we need errors struct ?
			return fmt.Errorf("unsupported argument type %s", reflectArg.Kind().String())
		}

		switch reflectArg.Kind() {
		case reflect.Slice:
			for i := 0; i < reflectArg.Len(); i++ {
				tempArgs = append(tempArgs, reflectArg.Index(i).Interface())
			}
		case reflect.Struct:
			for i := 0; i < reflectArg.NumField(); i++ {
				tempArgs = append(tempArgs, reflectArg.Field(i).Interface())
			}
		default:
			tempArgs = append(tempArgs, reflectArg.Interface())
		}
	}

	if len(tempArgs) != b.config.columnsLen {
		// todo: do we need errors struct ?
		return fmt.Errorf("expected %d parameters, received %d", b.config.columnsLen, len(tempArgs))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for i := 0; i < b.config.columnsLen; i++ {
		b.args = append(b.args, tempArgs[i])
	}
	b.curSize++

	if b.curSize == b.config.bulkMaxSize {
		return b.exec()
	} else {
		return nil
	}
}

func (b *BulkSQL) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.exec()
}

func (b *BulkSQL) InsertedIDS() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ids
}
