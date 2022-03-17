--
-- Regression tests for schemas (namespaces)
--

--DDL_STATEMENT_BEGIN--
CREATE SCHEMA test_ns_schema_1;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE test_ns_schema_1.abc (
              a serial,
              b int UNIQUE
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE UNIQUE INDEX abc_a_idx ON test_ns_schema_1.abc (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW test_ns_schema_1.abc_view AS SELECT a+1 AS a, b+1 AS b FROM test_ns_schema_1.abc;
--DDL_STATEMENT_END--

-- verify that the objects were created
SELECT COUNT(*) FROM pg_class WHERE relnamespace =
    (SELECT oid FROM pg_namespace WHERE nspname = 'test_ns_schema_1');

INSERT INTO test_ns_schema_1.abc DEFAULT VALUES;
INSERT INTO test_ns_schema_1.abc DEFAULT VALUES;
INSERT INTO test_ns_schema_1.abc DEFAULT VALUES;

SELECT * FROM test_ns_schema_1.abc;
SELECT * FROM test_ns_schema_1.abc_view;

--DDL_STATEMENT_BEGIN--
DROP TABLE test_ns_schema_1.abc cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA test_ns_schema_1 CASCADE;
--DDL_STATEMENT_END--