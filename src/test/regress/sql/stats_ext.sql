-- Generic extended statistics support

-- We will be checking execution plans without/with statistics, so
-- let's make sure we get simple non-parallel plans. Also set the
-- work_mem low so that we can use small amounts of data.
SET max_parallel_workers = 0;
SET max_parallel_workers_per_gather = 0;
SET work_mem = '128kB';

-- Verify failures
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst ON a, b;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst FROM sometab;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst ON a, b FROM nonexistant;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst ON a, b FROM pg_class;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst ON relname, relname, relnatts FROM pg_class;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst ON relnatts + relpages FROM pg_class;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst ON (relpages, reltuples) FROM pg_class;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tst (unrecognized) ON relname, relnatts FROM pg_class;
--DDL_STATEMENT_END--

-- Ensure stats are dropped sanely, and test IF NOT EXISTS while at it
--DDL_STATEMENT_BEGIN--
CREATE TABLE ab1 (a INTEGER, b INTEGER, c INTEGER);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS IF NOT EXISTS ab1_a_b_stats ON a, b FROM ab1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS IF NOT EXISTS ab1_a_b_stats ON a, b FROM ab1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP STATISTICS ab1_a_b_stats;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE SCHEMA regress_schema_2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS regress_schema_2.ab1_a_b_stats ON a, b FROM ab1;
--DDL_STATEMENT_END--

-- Let's also verify the pg_get_statisticsobjdef output looks sane.
SELECT pg_get_statisticsobjdef(oid) FROM pg_statistic_ext WHERE stxname = 'ab1_a_b_stats';

--DDL_STATEMENT_BEGIN--
DROP STATISTICS regress_schema_2.ab1_a_b_stats;
--DDL_STATEMENT_END--

-- Ensure statistics are dropped when columns are
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS ab1_b_c_stats ON b, c FROM ab1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS ab1_a_b_c_stats ON a, b, c FROM ab1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS ab1_b_a_stats ON b, a FROM ab1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE ab1 DROP COLUMN a;
--DDL_STATEMENT_END--
\d ab1
-- Ensure statistics are dropped when table is
SELECT stxname FROM pg_statistic_ext WHERE stxname LIKE 'ab1%';
--DDL_STATEMENT_BEGIN--
DROP TABLE ab1;
--DDL_STATEMENT_END--
SELECT stxname FROM pg_statistic_ext WHERE stxname LIKE 'ab1%';

-- Ensure things work sanely with SET STATISTICS 0
--DDL_STATEMENT_BEGIN--
CREATE TABLE ab1 (a INTEGER, b INTEGER);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE ab1 ALTER a SET STATISTICS 0;
--DDL_STATEMENT_END--
INSERT INTO ab1 SELECT a, a%23 FROM generate_series(1, 1000) a;
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS ab1_a_b_stats ON a, b FROM ab1;
--DDL_STATEMENT_END--
ANALYZE ab1;
--DDL_STATEMENT_BEGIN--
ALTER TABLE ab1 ALTER a SET STATISTICS -1;
--DDL_STATEMENT_END--
-- partial analyze doesn't build stats either
ANALYZE ab1 (a);
ANALYZE ab1;
--DDL_STATEMENT_BEGIN--
DROP TABLE ab1;
--DDL_STATEMENT_END--

-- Ensure we can build statistics for tables with inheritance.
--DDL_STATEMENT_BEGIN--
CREATE TABLE ab1 (a INTEGER, b INTEGER);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ab1c () INHERITS (ab1);
--DDL_STATEMENT_END--
INSERT INTO ab1 VALUES (1,1);
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS ab1_a_b_stats ON a, b FROM ab1;
--DDL_STATEMENT_END--
ANALYZE ab1;
--DDL_STATEMENT_BEGIN--
DROP TABLE ab1 CASCADE;
--DDL_STATEMENT_END--

-- Verify supported object types for extended statistics
--DDL_STATEMENT_BEGIN--
CREATE schema tststats;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE tststats.t (a int, b int, c text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX ti ON tststats.t (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE tststats.s;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW tststats.v AS SELECT * FROM tststats.t;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW tststats.mv AS SELECT * FROM tststats.t;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE tststats.ty AS (a int, b int, c text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FOREIGN DATA WRAPPER extstats_dummy_fdw;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SERVER extstats_dummy_srv FOREIGN DATA WRAPPER extstats_dummy_fdw;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FOREIGN TABLE tststats.f (a int, b int, c text) SERVER extstats_dummy_srv;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tststats.pt (a int, b int, c text) PARTITION BY RANGE (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tststats.pt1 PARTITION OF tststats.pt FOR VALUES FROM (-10, -10) TO (10, 10);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s1 ON a, b FROM tststats.t;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s2 ON a, b FROM tststats.ti;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s3 ON a, b FROM tststats.s;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s4 ON a, b FROM tststats.v;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s5 ON a, b FROM tststats.mv;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s6 ON a, b FROM tststats.ty;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s7 ON a, b FROM tststats.f;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s8 ON a, b FROM tststats.pt;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS tststats.s9 ON a, b FROM tststats.pt1;
--DDL_STATEMENT_END--
DO $$
DECLARE
	relname text = reltoastrelid::regclass FROM pg_class WHERE oid = 'tststats.t'::regclass;
BEGIN
	EXECUTE 'CREATE STATISTICS tststats.s10 ON a, b FROM ' || relname;
EXCEPTION WHEN wrong_object_type THEN
	RAISE NOTICE 'stats on toast table not created';
END;
$$;
--DDL_STATEMENT_BEGIN--

\set VERBOSITY terse \\ -- suppress cascade details
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA tststats CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FOREIGN DATA WRAPPER extstats_dummy_fdw CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
\set VERBOSITY default
--DDL_STATEMENT_END--
-- n-distinct tests
--DDL_STATEMENT_BEGIN--
CREATE TABLE ndistinct (
    filler1 TEXT,
    filler2 NUMERIC,
    a INT,
    b INT,
    filler3 DATE,
    c INT,
    d INT
);
--DDL_STATEMENT_END--

-- over-estimates when using only per-column statistics
INSERT INTO ndistinct (a, b, c, filler1)
     SELECT i/100, i/100, i/100, cash_words((i/100)::money)
       FROM generate_series(1,30000) s(i);

ANALYZE ndistinct;

-- Group Aggregate, due to over-estimate of the number of groups
EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY b, c;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c, d;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY b, c, d;

-- correct command
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS s10 ON a, b, c FROM ndistinct;
--DDL_STATEMENT_END--

ANALYZE ndistinct;

SELECT stxkind, stxndistinct
  FROM pg_statistic_ext WHERE stxrelid = 'ndistinct'::regclass;

-- Hash Aggregate, thanks to estimates improved by the statistic
EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY b, c;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c;

-- last two plans keep using Group Aggregate, because 'd' is not covered
-- by the statistic and while it's NULL-only we assume 200 values for it
EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c, d;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY b, c, d;

TRUNCATE TABLE ndistinct;

-- under-estimates when using only per-column statistics
INSERT INTO ndistinct (a, b, c, filler1)
     SELECT mod(i,50), mod(i,51), mod(i,32),
            cash_words(mod(i,33)::int::money)
       FROM generate_series(1,10000) s(i);

ANALYZE ndistinct;

SELECT stxkind, stxndistinct
  FROM pg_statistic_ext WHERE stxrelid = 'ndistinct'::regclass;

-- plans using Group Aggregate, thanks to using correct esimates
EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c, d;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY b, c, d;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, d;
 
--DDL_STATEMENT_BEGIN--
DROP STATISTICS s10;
--DDL_STATEMENT_END--

SELECT stxkind, stxndistinct
  FROM pg_statistic_ext WHERE stxrelid = 'ndistinct'::regclass;

-- dropping the statistics switches the plans to Hash Aggregate,
-- due to under-estimates
EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c, d;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY b, c, d;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, d;

-- functional dependencies tests
--DDL_STATEMENT_BEGIN--
CREATE TABLE functional_dependencies (
    filler1 TEXT,
    filler2 NUMERIC,
    a INT,
    b TEXT,
    filler3 DATE,
    c INT,
    d TEXT
);
--DDL_STATEMENT_END--

SET random_page_cost = 1.2;

--DDL_STATEMENT_BEGIN--
CREATE INDEX fdeps_ab_idx ON functional_dependencies (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX fdeps_abc_idx ON functional_dependencies (a, b, c);
--DDL_STATEMENT_END--

-- random data (no functional dependencies)
INSERT INTO functional_dependencies (a, b, c, filler1)
     SELECT mod(i, 23), mod(i, 29), mod(i, 31), i FROM generate_series(1,5000) s(i);

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1';

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

-- create statistics
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS func_deps_stat (dependencies) ON a, b, c FROM functional_dependencies;
--DDL_STATEMENT_END--

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1';

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

-- a => b, a => c, b => c
TRUNCATE functional_dependencies;
--DDL_STATEMENT_BEGIN--
DROP STATISTICS func_deps_stat;
--DDL_STATEMENT_END--

INSERT INTO functional_dependencies (a, b, c, filler1)
     SELECT mod(i,100), mod(i,50), mod(i,25), i FROM generate_series(1,5000) s(i);

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1';

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

-- create statistics
--DDL_STATEMENT_BEGIN--
CREATE STATISTICS func_deps_stat (dependencies) ON a, b, c FROM functional_dependencies;
--DDL_STATEMENT_END--

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1';

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

-- check change of column type doesn't break it
--DDL_STATEMENT_BEGIN--
ALTER TABLE functional_dependencies ALTER COLUMN c TYPE numeric;
--DDL_STATEMENT_END--

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

RESET random_page_cost;
