/* src/modules/global_deadlock_detector/global_deadlock_detector--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION global_deadlock_detector" to load this file. \quit

CREATE FUNCTION global_deadlock_detector_launch(pg_catalog.int4)
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;
