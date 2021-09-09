--
-- Regression tests for schemas (namespaces)
--

CREATE SCHEMA test_ns_schema_1;

CREATE TABLE test_ns_schema_1.abc (
              a serial,
              b int UNIQUE
);
CREATE UNIQUE INDEX abc_a_idx ON test_ns_schema_1.abc (a);

CREATE VIEW test_ns_schema_1.abc_view AS SELECT a+1 AS a, b+1 AS b FROM test_ns_schema_1.abc;

-- verify that the objects were created
SELECT COUNT(*) FROM pg_class WHERE relnamespace =
    (SELECT oid FROM pg_namespace WHERE nspname = 'test_ns_schema_1');

INSERT INTO test_ns_schema_1.abc DEFAULT VALUES;
INSERT INTO test_ns_schema_1.abc DEFAULT VALUES;
INSERT INTO test_ns_schema_1.abc DEFAULT VALUES;

SELECT * FROM test_ns_schema_1.abc;
SELECT * FROM test_ns_schema_1.abc_view;

DROP TABLE test_ns_schema_1.abc cascade;
DROP SCHEMA test_ns_schema_1 CASCADE;
