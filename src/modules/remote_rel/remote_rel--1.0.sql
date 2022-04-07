-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION apply_log_wrapper" to load this file. \quit

CREATE FUNCTION apply_log_wrapper(pg_catalog.int8,
    pg_catalog.text,
    pg_catalog.text,
    pg_catalog.text,
	pg_catalog.text,
    pg_catalog.text)
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;
