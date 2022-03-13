-- create a table to use as a basis for views and materialized views in various combinations
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists mvtest_t cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE mvtest_t (id int NOT NULL PRIMARY KEY, type varchar(50) NOT NULL, amt numeric NOT NULL);
--DDL_STATEMENT_END--
INSERT INTO mvtest_t VALUES
  (1, 'x', 2),
  (2, 'x', 3),
  (3, 'y', 5),
  (4, 'y', 7),
  (5, 'z', 11);

-- we want a view based on the table, too, since views present additional challenges
--DDL_STATEMENT_BEGIN--
CREATE VIEW mvtest_tv AS SELECT type, sum(amt) AS totamt FROM mvtest_t GROUP BY type;
--DDL_STATEMENT_END--
SELECT * FROM mvtest_tv ORDER BY type;

-- create a materialized view with no data, and confirm correct behavior
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_tm AS SELECT type, sum(amt) AS totamt FROM mvtest_t GROUP BY type WITH NO DATA;
--DDL_STATEMENT_END--
SELECT relispopulated FROM pg_class WHERE oid = 'mvtest_tm'::regclass;
SELECT * FROM mvtest_tm ORDER BY type;
REFRESH MATERIALIZED VIEW mvtest_tm;
SELECT relispopulated FROM pg_class WHERE oid = 'mvtest_tm'::regclass;
--CREATE UNIQUE INDEX mvtest_tm_type ON mvtest_tm (type);
SELECT * FROM mvtest_tm ORDER BY type;

-- create various views
--DDL_STATEMENT_BEGIN--
EXPLAIN (costs off)
  CREATE MATERIALIZED VIEW mvtest_tvm AS SELECT * FROM mvtest_tv ORDER BY type;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_tvm AS SELECT * FROM mvtest_tv ORDER BY type;
--DDL_STATEMENT_END--
SELECT * FROM mvtest_tvm;
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_tmm AS SELECT sum(totamt) AS grandtot FROM mvtest_tm;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_tvmm AS SELECT sum(totamt) AS grandtot FROM mvtest_tvm;
--DDL_STATEMENT_END--
--CREATE UNIQUE INDEX mvtest_tvmm_expr ON mvtest_tvmm ((grandtot > 0));
--CREATE UNIQUE INDEX mvtest_tvmm_pred ON mvtest_tvmm (grandtot) WHERE grandtot < 0;
--DDL_STATEMENT_BEGIN--
CREATE VIEW mvtest_tvv AS SELECT sum(totamt) AS grandtot FROM mvtest_tv;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
EXPLAIN (costs off)
  CREATE MATERIALIZED VIEW mvtest_tvvm AS SELECT * FROM mvtest_tvv;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_tvvm AS SELECT * FROM mvtest_tvv;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW mvtest_tvvmv AS SELECT * FROM mvtest_tvvm;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_bb AS SELECT * FROM mvtest_tvvmv;
--DDL_STATEMENT_END--
--CREATE INDEX mvtest_aa ON mvtest_bb (grandtot);

-- check that plans seem reasonable
\d+ mvtest_tvm
\d+ mvtest_tvm
\d+ mvtest_tvvm
\d+ mvtest_bb

-- test schema behavior
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA mvtest_mvschema;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER MATERIALIZED VIEW mvtest_tvm SET SCHEMA mvtest_mvschema;
--DDL_STATEMENT_END--
\d+ mvtest_tvm
\d+ mvtest_tvmm
SET search_path = mvtest_mvschema, public;
\d+ mvtest_tvm

-- modify the underlying table data
INSERT INTO mvtest_t VALUES (6, 'z', 13);

-- confirm pre- and post-refresh contents of fairly simple materialized views
SELECT * FROM mvtest_tm ORDER BY type;
SELECT * FROM mvtest_tvm ORDER BY type;
REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_tm;
REFRESH MATERIALIZED VIEW mvtest_tvm;
SELECT * FROM mvtest_tm ORDER BY type;
SELECT * FROM mvtest_tvm ORDER BY type;
RESET search_path;

-- confirm pre- and post-refresh contents of nested materialized views
EXPLAIN (costs off)
  SELECT * FROM mvtest_tmm;
EXPLAIN (costs off)
  SELECT * FROM mvtest_tvmm;
EXPLAIN (costs off)
  SELECT * FROM mvtest_tvvm;
SELECT * FROM mvtest_tmm;
SELECT * FROM mvtest_tvmm;
SELECT * FROM mvtest_tvvm;
REFRESH MATERIALIZED VIEW mvtest_tmm;
REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_tvmm;
REFRESH MATERIALIZED VIEW mvtest_tvmm;
REFRESH MATERIALIZED VIEW mvtest_tvvm;
EXPLAIN (costs off)
  SELECT * FROM mvtest_tmm;
EXPLAIN (costs off)
  SELECT * FROM mvtest_tvmm;
EXPLAIN (costs off)
  SELECT * FROM mvtest_tvvm;
SELECT * FROM mvtest_tmm;
SELECT * FROM mvtest_tvmm;
SELECT * FROM mvtest_tvvm;

-- test diemv when the mv does not exist
--DDL_STATEMENT_BEGIN--
DROP MATERIALIZED VIEW IF EXISTS no_such_mv;
--DDL_STATEMENT_END--
-- make sure invalid combination of options is prohibited

REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_tvmm WITH NO DATA;

-- no tuple locks on materialized views
SELECT * FROM mvtest_tvvm FOR SHARE;

-- test join of mv and view
SELECT type, m.totamt AS mtot, v.totamt AS vtot FROM mvtest_tm m LEFT JOIN mvtest_tv v USING (type) ORDER BY type;

-- make sure that dependencies are reported properly when they block the drop
--DDL_STATEMENT_BEGIN--
DROP TABLE mvtest_t;
--DDL_STATEMENT_END--
-- some additional tests not using base tables
--DDL_STATEMENT_BEGIN--
CREATE VIEW mvtest_vt1 AS SELECT 1 moo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW mvtest_vt2 AS SELECT moo, 2*moo FROM mvtest_vt1 UNION ALL SELECT moo, 3*moo FROM mvtest_vt1;
--DDL_STATEMENT_END--
\d+ mvtest_vt2
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mv_test2 AS SELECT moo, 2*moo FROM mvtest_vt2 UNION ALL SELECT moo, 3*moo FROM mvtest_vt2;
--DDL_STATEMENT_END--
\d+ mv_test2
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mv_test3 AS SELECT * FROM mv_test2 WHERE moo = 12345;
--DDL_STATEMENT_END--
SELECT relispopulated FROM pg_class WHERE oid = 'mv_test3'::regclass;
--DDL_STATEMENT_BEGIN--
DROP VIEW mvtest_vt1 CASCADE;
--DDL_STATEMENT_END--
-- test that duplicate values on unique index prevent refresh
--DDL_STATEMENT_BEGIN--
CREATE TABLE mvtest_foo(a int, b int);
--DDL_STATEMENT_END--
INSERT INTO mvtest_foo VALUES(1, 10);
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv AS SELECT * FROM mvtest_foo;
--DDL_STATEMENT_END--
--CREATE UNIQUE INDEX ON mvtest_mv(a);
INSERT INTO mvtest_foo SELECT * FROM mvtest_foo;
REFRESH MATERIALIZED VIEW mvtest_mv;
REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_mv;
--DDL_STATEMENT_BEGIN--
DROP TABLE mvtest_foo CASCADE;
--DDL_STATEMENT_END--

-- make sure that all columns covered by unique indexes works、
--DDL_STATEMENT_BEGIN--
CREATE TABLE mvtest_foo(a int, b int, c int); 
--DDL_STATEMENT_END--
insert into mvtest_foo VALUES(1, 2, 3);
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv AS SELECT * FROM mvtest_foo;
--DDL_STATEMENT_END--
--CREATE UNIQUE INDEX ON mvtest_mv (a);
--CREATE UNIQUE INDEX ON mvtest_mv (b);
--CREATE UNIQUE INDEX on mvtest_mv (c);
INSERT INTO mvtest_foo VALUES(2, 3, 4);
INSERT INTO mvtest_foo VALUES(3, 4, 5);
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW mvtest_mv;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_mv;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE mvtest_foo CASCADE;
--DDL_STATEMENT_END--
-- allow subquery to reference unpopulated matview if WITH NO DATA is specified
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv1 AS SELECT 1 AS col1 WITH NO DATA;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv2 AS SELECT * FROM mvtest_mv1
  WHERE col1 = (SELECT LEAST(col1) FROM mvtest_mv1) WITH NO DATA;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP MATERIALIZED VIEW mvtest_mv1 CASCADE;
--DDL_STATEMENT_END--
-- make sure that types with unusual equality tests work

--DDL_STATEMENT_BEGIN--
CREATE temp TABLE mvtest_boxes (id serial primary key, b box);
--DDL_STATEMENT_END--
INSERT INTO mvtest_boxes (b) VALUES
  ('(32,32),(31,31)'),
  ('(2.0000004,2.0000004),(1,1)'),
  ('(1.9999996,1.9999996),(1,1)');
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_boxmv AS SELECT * FROM mvtest_boxes;
--DDL_STATEMENT_END--
--CREATE UNIQUE INDEX mvtest_boxmv_id ON mvtest_boxmv (id);
UPDATE mvtest_boxes SET b = '(2,2),(1,1)' WHERE id = 2;
REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_boxmv;
SELECT * FROM mvtest_boxmv ORDER BY id;

--DDL_STATEMENT_BEGIN--
DROP TABLE mvtest_boxes CASCADE;
--DDL_STATEMENT_END--
-- make sure that column names are handled correctly
--DDL_STATEMENT_BEGIN--
CREATE TABLE mvtest_v (i int, j int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_v (ii, jj, kk) AS SELECT i, j FROM mvtest_v; -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_v (ii, jj) AS SELECT i, j FROM mvtest_v; -- ok
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_v_2 (ii) AS SELECT i, j FROM mvtest_v; -- ok
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_v_3 (ii, jj, kk) AS SELECT i, j FROM mvtest_v WITH NO DATA; -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_v_3 (ii, jj) AS SELECT i, j FROM mvtest_v WITH NO DATA; -- ok
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_v_4 (ii) AS SELECT i, j FROM mvtest_v WITH NO DATA; -- ok
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE mvtest_v RENAME COLUMN i TO x;
--DDL_STATEMENT_END--
INSERT INTO mvtest_v values (1, 2);
--CREATE UNIQUE INDEX mvtest_mv_v_ii ON mvtest_mv_v (ii);
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW mvtest_mv_v;
--DDL_STATEMENT_END--
UPDATE mvtest_v SET j = 3 WHERE x = 1;
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_mv_v;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW mvtest_mv_v_2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW mvtest_mv_v_3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW mvtest_mv_v_4;
--DDL_STATEMENT_END--
SELECT * FROM mvtest_v;
SELECT * FROM mvtest_mv_v;
SELECT * FROM mvtest_mv_v_2;
SELECT * FROM mvtest_mv_v_3;
SELECT * FROM mvtest_mv_v_4;
--DDL_STATEMENT_BEGIN--
DROP TABLE mvtest_v CASCADE;
--DDL_STATEMENT_END--
-- Check that unknown literals are converted to "text" in CREATE MATVIEW,
-- so that we don't end up with unknown-type columns.
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mv_unspecified_types AS
  SELECT 42 as i, 42.5 as num, 'foo' as u, 'foo'::unknown as u2, null as n;
--DDL_STATEMENT_END--
\d+ mv_unspecified_types
SELECT * FROM mv_unspecified_types;
--DDL_STATEMENT_BEGIN--
DROP MATERIALIZED VIEW mv_unspecified_types;
--DDL_STATEMENT_END--
-- make sure that create WITH NO DATA does not plan the query (bug #13907)
--DDL_STATEMENT_BEGIN--
create materialized view mvtest_error as select 1/0 as x;  -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create materialized view mvtest_error as select 1/0 as x with no data;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
refresh materialized view mvtest_error;  -- fail here
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop materialized view mvtest_error;
--DDL_STATEMENT_END--
-- make sure that matview rows can be referenced as source rows (bug #9398)
--DDL_STATEMENT_BEGIN--
CREATE TABLE mvtest_v(a int);
--DDL_STATEMENT_END--
insert into mvtest_v SELECT generate_series(1,10);
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_v AS SELECT a FROM mvtest_v WHERE a <= 5;
--DDL_STATEMENT_END--
--kunlun has bug here currently.
--DELETE FROM mvtest_v WHERE EXISTS ( SELECT * FROM mvtest_mv_v WHERE mvtest_mv_v.a = mvtest_v.a );
SELECT * FROM mvtest_v;
SELECT * FROM mvtest_mv_v;
--DDL_STATEMENT_BEGIN--
DROP TABLE mvtest_v CASCADE;
--DDL_STATEMENT_END--
-- make sure running as superuser works when MV owned by another role (bug #11208)

--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_user_mvtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET ROLE regress_user_mvtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE mvtest_foo_data (i int, md5v text);
--DDL_STATEMENT_END--
insert into mvtest_foo_data SELECT i, md5(random()::text) FROM generate_series(1, 10) i;
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_foo AS SELECT * FROM mvtest_foo_data;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW mvtest_mv_foo AS SELECT * FROM mvtest_foo_data;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE MATERIALIZED VIEW IF NOT EXISTS mvtest_mv_foo AS SELECT * FROM mvtest_foo_data;
--DDL_STATEMENT_END--
--CREATE UNIQUE INDEX ON mvtest_mv_foo (i);
RESET ROLE;
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW mvtest_mv_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_mv_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP MATERIALIZED VIEW mvtest_mv_foo cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE mvtest_foo_data;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_user_mvtest;
--DDL_STATEMENT_END--

-- make sure that create WITH NO DATA works via SPI
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION mvtest_func()
  RETURNS void AS $$
BEGIN
  CREATE MATERIALIZED VIEW mvtest1 AS SELECT 1 AS x;
  CREATE MATERIALIZED VIEW mvtest2 AS SELECT 1 AS x WITH NO DATA;
END;
$$ LANGUAGE plpgsql;
--DDL_STATEMENT_END--
SELECT mvtest_func();
SELECT * FROM mvtest1;
SELECT * FROM mvtest2;
--DDL_STATEMENT_BEGIN--
DROP MATERIALIZED VIEW mvtest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP MATERIALIZED VIEW mvtest2;
--DDL_STATEMENT_END--
