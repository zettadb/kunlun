/* src/modules/cluster_log_applier/cluster_log_applier--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION cluster_log_applier" to load this file. \quit

CREATE FUNCTION cluster_log_applier_launch(pg_catalog.int4)
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;
