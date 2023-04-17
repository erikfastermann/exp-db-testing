package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// TODO: not a global, should be initialized with a config.
var config = &AppConfig{
	MyOption: "foobar",
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	// TODO:
	//   - configurable
	//   - separate users for DDL and DQL/DML
	db, err := sqlx.Open("postgres", "user=erik dbname=db1 sslmode=disable")
	if err != nil {
		return err
	}
	if err := initDB(db); err != nil {
		return err
	}
	if err := txTest(db); err != nil {
		return err
	}
	panic("TODO")
	// http.ListenAndServe(":8080", http.HandlerFunc(handlerFunc))
}

func initDB(db *sqlx.DB) error {
	// TODO:
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
	if err := installTriggers(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			panic(fmt.Errorf("%w (%v)", rollbackErr, err))
		}
		return err
	}
	return tx.Commit()
}

func txTest(db *sqlx.DB) error {
	ctx := context.Background() // TODO
	tx, _ := PrepareTx(db, false)
	if err := tx.Exec(ctx, "insert into data.foo(id, bar) values(1, 'blub')"); err != nil {
		return err
	}
	var events []Event
	if err := tx.Query(ctx, &events, "select * from events.events"); err != nil {
		return err
	}
	fmt.Println(events)
	return nil
}

func handlerFunc(w http.ResponseWriter, r *http.Request) {
	// Use a router, middleware, etc. here.
	var input myInputData
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ctx := &AppContext{
		Context:   r.Context(),
		AppConfig: config,
		// TODO Tx: PrepareTx(),
	}
	output, err := myReadHandler(ctx, &input)
	if err != nil {
		// Wrap the error with a status code to modify this.
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK) // Also communicate this somehow.
	if err := json.NewEncoder(w).Encode(output); err != nil {
		// Log here.
		return
	}
}

type AppConfig struct {
	MyOption string
}

// AppContext can be global to the project or more fine granular for each file.
// TODO: as interface?
type AppContext struct {
	context.Context
	*AppConfig // read-only
	*Tx
}

type myInputData struct {
	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

type myOutputData struct {
	Baz bool `json:"baz"`
}

func myReadHandler(ctx *AppContext, data *myInputData) ([]*myOutputData, error) {
	var out []*myOutputData
	err := ctx.Tx.Query(
		ctx,
		&out,
		"select baz from tab where foo = ? and bar = ?",
		data.Foo,
		data.Bar,
	)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func myWriteHandler(ctx *AppContext, data *myInputData) ([]*myOutputData, error) {
	panic("TODO")
}
