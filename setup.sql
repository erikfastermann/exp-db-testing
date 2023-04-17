-- USAGE:
-- psql --user erik db1 -f setup.sql -v ON_ERROR_STOP=1

create schema if not exists data;
create schema if not exists events;

-- TODO: not repeatable
-- create type action as enum('insert', 'update', 'delete');

create sequence if not exists events.tx_id as bigint;

-- TODO:
--   - store extra information for each transaction
--   - detach from postgres types
--   - change column order, probably table_id, row_id, tx_id, column_id
create table if not exists events.events(
    tx_id bigint not null,
    table_id oid not null,
    column_id int2 not null,
    row_id bigint not null,
    action action not null,
    value jsonb,
    primary key (tx_id, table_id, column_id, row_id)
);

-- TODO
create table data.foo(id bigint not null primary key, bar text not null);
