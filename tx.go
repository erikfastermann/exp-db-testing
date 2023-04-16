package main

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Tx is an on demand transaction. It is started when the first query is run.
// The commit happens outside the handler, but only if read only is true.
// All queries must use an index. Longer running operations are handled
// by something else, making sure the analysis database is used.
// Not thread safe.
//
// TODO:
//   - Test that all queries use an index.
//   - Split read and read-write?
//   - Analysis Queries
type Tx struct {
	db   *sql.DB
	tx   *sql.Tx // might be nil
	opts *sql.TxOptions
}

// TxFinalizer is usually run after a handler is finished.
type TxFinalizer func(success bool) error

func PrepareTx(db *sql.DB, opts *sql.TxOptions) (*Tx, TxFinalizer) {
	tx := &Tx{
		db:   db,
		opts: opts,
	}
	return tx, func(success bool) error {
		// TODO: what happens if we don't commit or rollback anything?
		if opts.ReadOnly || tx.tx == nil {
			return nil
		}
		if success {
			return tx.tx.Commit()
		}
		return tx.tx.Rollback()
	}
}

// TODO:
// kinda confusing using the random context passed in here,
// because it is used for the whole transaction.
func (tx *Tx) begin(ctx context.Context) error {
	if tx.tx != nil {
		return nil
	}
	var err error
	tx.tx, err = tx.db.BeginTx(ctx, tx.opts)
	return err
}

// Query runs a query in the transaction, mapping the result rows to out.
// Only scan to slice is provided, because
// Should be called by a type-safe wrapper.
//
// TODO:
//   - use a statement abstraction
//   - support other mappers (danger is this becomes to adhoc)
func (tx *Tx) Query(ctx context.Context, dest any, query string, args ...any) error {
	if err := tx.begin(ctx); err != nil {
		return err
	}
	rows, err := tx.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return sqlx.StructScan(rows, dest)
}

func (tx *Tx) Exec(ctx context.Context, query string, args ...any) error {
	if err := tx.begin(ctx); err != nil {
		return err
	}
	_, err := tx.tx.ExecContext(ctx, query, args...)
	return err
}
