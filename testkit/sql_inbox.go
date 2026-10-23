package testkit

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
)

var tableName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// SQLInbox is a processor.Inbox in a PostgreSQL table, which records a key in the transaction of
// its operation: a reference for the Inbox of an application.
type SQLInbox struct {
	db    *sql.DB
	table string
}

// NewSQLInbox returns an SQLInbox in table, a lowercase SQL identifier.
func NewSQLInbox(db *sql.DB, table string) (*SQLInbox, error) {
	if !tableName.MatchString(table) {
		return nil, errors.New("the Inbox table name must be a lowercase SQL identifier")
	}
	return &SQLInbox{db: db, table: table}, nil
}

// CreateTable creates the table unless it exists.
func (i *SQLInbox) CreateTable(ctx context.Context) error {
	_, err := i.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+i.table+
		` (observation_id TEXT PRIMARY KEY, processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)`)
	return err
}

// ProcessOnce runs operation in a transaction that records key, unless key was recorded before. The
// context of operation carries the transaction, which Transaction returns.
func (i *SQLInbox) ProcessOnce(ctx context.Context, key string, operation func(context.Context) error) error {
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	inserted, err := tx.ExecContext(ctx, `INSERT INTO `+i.table+` (observation_id) VALUES ($1) ON CONFLICT (observation_id) DO NOTHING`, key)
	if err != nil {
		return err
	}
	if rows, err := inserted.RowsAffected(); err != nil || rows == 0 {
		return err
	}
	if err := operation(context.WithValue(ctx, transactionKey{}, tx)); err != nil {
		return err
	}
	return tx.Commit()
}

type transactionKey struct{}

// Transaction returns the transaction of an SQLInbox operation.
func Transaction(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(transactionKey{}).(*sql.Tx)
	return tx, ok
}
