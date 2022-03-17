
-- Simple create
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test(i INT) WITH (FiLLFaCToR=30,
	autovacuum_enabled = false, autovacuum_analyze_scale_factor = 0.2);
--DDL_STATEMENT_END--
SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test'::regclass;

-- Fail min/max values check
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (fillfactor=2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (fillfactor=110);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (autovacuum_analyze_scale_factor = -10.0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (autovacuum_analyze_scale_factor = 110.0);
--DDL_STATEMENT_END--

-- Fail when option and namespace do not exist
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (not_existing_option=2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (not_existing_namespace.fillfactor=2);
--DDL_STATEMENT_END--

-- Fail while setting improper values
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (fillfactor=30.5);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (fillfactor='string');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (fillfactor=true);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (autovacuum_enabled=12);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (autovacuum_enabled=30.5);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (autovacuum_enabled='string');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (autovacuum_analyze_scale_factor='string');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2(i INT) WITH (autovacuum_analyze_scale_factor=true);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- Fail if option is specified twice
CREATE TABLE reloptions_test2(i INT) WITH (fillfactor=30, fillfactor=40);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- Specifying name only for a non-Boolean option should fail
CREATE TABLE reloptions_test2(i INT) WITH (fillfactor);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- Simple ALTER TABLE
ALTER TABLE reloptions_test SET (fillfactor=31,
	autovacuum_analyze_scale_factor = 0.3);
--DDL_STATEMENT_END--
SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test'::regclass;

-- Set boolean option to true without specifying value
--DDL_STATEMENT_BEGIN--
ALTER TABLE reloptions_test SET (autovacuum_enabled, fillfactor=32);
--DDL_STATEMENT_END--
SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test'::regclass;

-- Check that RESET works well
--DDL_STATEMENT_BEGIN--
ALTER TABLE reloptions_test RESET (fillfactor);
--DDL_STATEMENT_END--
SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test'::regclass;

-- Resetting all values causes the column to become null
--DDL_STATEMENT_BEGIN--
ALTER TABLE reloptions_test RESET (autovacuum_enabled,
	autovacuum_analyze_scale_factor);
--DDL_STATEMENT_END--
SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test'::regclass AND
       reloptions IS NULL;

-- RESET fails if a value is specified
--DDL_STATEMENT_BEGIN--
ALTER TABLE reloptions_test RESET (fillfactor=12);
--DDL_STATEMENT_END--
-- The OIDS option is not stored as reloption
--DDL_STATEMENT_BEGIN--
DROP TABLE reloptions_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test(i INT) WITH (fillfactor=20, oids=true);
--DDL_STATEMENT_END--
SELECT reloptions, relhasoids FROM pg_class WHERE oid = 'reloptions_test'::regclass;

-- Test toast.* options
--DDL_STATEMENT_BEGIN--
DROP TABLE reloptions_test;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test (s VARCHAR)
	WITH (toast.autovacuum_vacuum_cost_delay = 23);
--DDL_STATEMENT_END--
SELECT reltoastrelid as toast_oid
	FROM pg_class WHERE oid = 'reloptions_test'::regclass \gset
SELECT reloptions FROM pg_class WHERE oid = :toast_oid;

--DDL_STATEMENT_BEGIN--
ALTER TABLE reloptions_test SET (toast.autovacuum_vacuum_cost_delay = 24);
SELECT reloptions FROM pg_class WHERE oid = :toast_oid;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE reloptions_test RESET (toast.autovacuum_vacuum_cost_delay);
SELECT reloptions FROM pg_class WHERE oid = :toast_oid;
--DDL_STATEMENT_END--

-- Fail on non-existent options in toast namespace
--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test2 (i int) WITH (toast.not_existing_option = 42);
--DDL_STATEMENT_END--

-- Mix TOAST & heap
--DDL_STATEMENT_BEGIN--
DROP TABLE reloptions_test;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE reloptions_test (s VARCHAR) WITH
	(toast.autovacuum_vacuum_cost_delay = 23,
	autovacuum_vacuum_cost_delay = 24, fillfactor = 40);
--DDL_STATEMENT_END--

SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test'::regclass;
SELECT reloptions FROM pg_class WHERE oid = (
	SELECT reltoastrelid FROM pg_class WHERE oid = 'reloptions_test'::regclass);

--
-- CREATE INDEX, ALTER INDEX for btrees
--

--DDL_STATEMENT_BEGIN--
CREATE INDEX reloptions_test_idx ON reloptions_test (s) WITH (fillfactor=30);
--DDL_STATEMENT_END--
SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test_idx'::regclass;

-- Fail when option and namespace do not exist
--DDL_STATEMENT_BEGIN--
CREATE INDEX reloptions_test_idx ON reloptions_test (s)
	WITH (not_existing_option=2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX reloptions_test_idx ON reloptions_test (s)
	WITH (not_existing_ns.fillfactor=2);
--DDL_STATEMENT_END--

-- Check allowed ranges
--DDL_STATEMENT_BEGIN--
CREATE INDEX reloptions_test_idx2 ON reloptions_test (s) WITH (fillfactor=1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX reloptions_test_idx2 ON reloptions_test (s) WITH (fillfactor=130);
--DDL_STATEMENT_END--

-- Check ALTER
--DDL_STATEMENT_BEGIN--
ALTER INDEX reloptions_test_idx SET (fillfactor=40);
--DDL_STATEMENT_END--
SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test_idx'::regclass;

-- Check ALTER on empty reloption list
--DDL_STATEMENT_BEGIN--
CREATE INDEX reloptions_test_idx3 ON reloptions_test (s);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER INDEX reloptions_test_idx3 SET (fillfactor=40);
--DDL_STATEMENT_END--
SELECT reloptions FROM pg_class WHERE oid = 'reloptions_test_idx3'::regclass;
