package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	schemaData   = "data"
	schemaEvents = "events"
)

// TODO: Maybe a more Datomic like structure.

// Only works with table names which don't require quotes.
// User truncate permission is assumed to NOT exist.
// Triggers are only created for the data schema.
// The user should only have permissions for that schema.
// The events.events table is assumed to exist.

const createTriggerSQLFormat = `create trigger events_%s
after %s on %s.%s
for each row
execute function %s.events_%s_function()`

func insertTriggerSQL(tableName string) string {
	name := fmt.Sprintf("insert_%s", tableName)
	return fmt.Sprintf(
		createTriggerSQLFormat,
		name,
		"insert",
		schemaData,
		tableName,
		schemaData,
		name,
	)
}

func updateTriggerSQL(tableName, columnName string) string {
	name := fmt.Sprintf("update_%s_%s", tableName, columnName)
	update := fmt.Sprintf("update of %s", columnName)
	return fmt.Sprintf(
		createTriggerSQLFormat,
		name,
		update,
		schemaData,
		tableName,
		schemaData,
		name,
	)
}

func deleteTriggerSQL(tableName string) string {
	name := fmt.Sprintf("delete_%s", tableName)
	return fmt.Sprintf(
		createTriggerSQLFormat,
		name,
		"delete",
		schemaData,
		tableName,
		schemaData,
		name,
	)
}

const txSettingName = "tx_vars.tx_id"

// TODO:
//   - better way than current_setting for tx id?
//   - enforce txSettingName is set
//   - to_json might not always work
//   - exclude row_id, requires row_id never changes
const eventsInsertSQLFormat = `insert into %s.events(tx_id, table_id, column_id, row_id, action, value)
values(current_setting('` + txSettingName + `')::bigint, %d, %d, new.id, '%s', to_json(new.%s));
`

const createTriggerFunctionSQLFormat = `create function %s.events_%s_function()
returns trigger
as $$
begin
%s
return new;
end;
$$
language plpgsql`

type Table struct {
	ID      int
	Name    string
	Columns []Column
}

type Column struct {
	ID   int
	Name string
}

func insertTriggerFunctionSQL(table Table) string {
	name := fmt.Sprintf("insert_%s", table.Name)
	var body strings.Builder
	for _, column := range table.Columns {
		fmt.Fprintf(
			&body,
			eventsInsertSQLFormat,
			schemaEvents,
			table.ID,
			column.ID,
			"insert",
			column.Name,
		)
	}
	return fmt.Sprintf(
		createTriggerFunctionSQLFormat,
		schemaData,
		name,
		body.String(),
	)
}

func updateTriggerFunctionSQL(table Table, column Column) string {
	name := fmt.Sprintf("update_%s_%s", table.Name, column.Name)
	body := fmt.Sprintf(
		eventsInsertSQLFormat,
		schemaEvents,
		table.ID,
		column.ID,
		"update",
		column.Name,
	)
	return fmt.Sprintf(createTriggerFunctionSQLFormat, schemaData, name, body)
}

// TODO: All values could be null here.
func deleteTriggerFunctionSQL(table Table) string {
	name := fmt.Sprintf("delete_%s", table.Name)
	var body strings.Builder
	for _, column := range table.Columns {
		fmt.Fprintf(
			&body,
			eventsInsertSQLFormat,
			schemaEvents,
			table.ID,
			column.ID,
			"delete",
			column.Name,
		)
	}
	return fmt.Sprintf(
		createTriggerFunctionSQLFormat,
		schemaData,
		name,
		body.String(),
	)
}

func generateTableTriggerStatements(table Table) []string {
	statements := make([]string, 0)
	statements = append(statements, insertTriggerFunctionSQL(table))
	statements = append(statements, insertTriggerSQL(table.Name))
	for _, column := range table.Columns {
		statements = append(statements, updateTriggerFunctionSQL(table, column))
		statements = append(statements, updateTriggerSQL(table.Name, column.Name))
	}
	statements = append(statements, deleteTriggerFunctionSQL(table))
	statements = append(statements, deleteTriggerSQL(table.Name))
	return statements
}

func generateTriggerStatements(tables []Table) []string {
	statements := make([]string, 0)
	for _, table := range tables {
		statements = append(statements, generateTableTriggerStatements(table)...)
	}
	return statements
}

// TODO: possible problems include
//   - incompatible between postgres versions
//   - probably does not consider some edge cases
//   - should not used postgres id's
const queryTablesSql = `select
	a.attrelid as table_id,
	c.relname as table_name,
	a.attnum as column_id,
	a.attname as column_name
from pg_attribute as a
join pg_class as c on a.attrelid = c.oid
where c.relnamespace = $1::regnamespace
and c.relkind = 'r' -- ordinary table
and a.attnum > 0
and not a.attisdropped
order by table_id, column_id`

func queryTables(ctx context.Context, tx *sql.Tx) ([]Table, error) {
	rows, err := tx.QueryContext(ctx, queryTablesSql, schemaData)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]Table, 0)
	lastTableID := -1 // TODO: check if this value is a possible table id
	for rows.Next() {
		var (
			tableID    int
			tableName  string
			columnID   int
			columnName string
		)
		err := rows.Scan(&tableID, &tableName, &columnID, &columnName)
		if err != nil {
			return nil, err
		}
		if tableID != lastTableID {
			table := Table{
				ID:   tableID,
				Name: tableName,
			}
			tables = append(tables, table)
		}
		column := Column{
			ID:   columnID,
			Name: columnName,
		}
		columns := &tables[len(tables)-1].Columns
		*columns = append(*columns, column)
		lastTableID = tableID
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func installTriggers(ctx context.Context, tx *sql.Tx) error {
	tables, err := queryTables(ctx, tx)
	if err != nil {
		return err
	}
	statements := generateTriggerStatements(tables)
	// TODO: remove the triggers again?
	for _, statement := range statements {
		fmt.Println(statement) // TODO
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
