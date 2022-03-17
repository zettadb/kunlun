--
-- CREATE_VIEW
-- Virtual class definitions
--	(this also tests the query rewrite system)
--

--DDL_STATEMENT_BEGIN--
CREATE VIEW toyemp AS
   SELECT name, age, 12*salary AS annualsal
   FROM emp;
--DDL_STATEMENT_END--

-- Test comments
COMMENT ON VIEW noview IS 'no view';
COMMENT ON VIEW toyemp IS 'is a view';
COMMENT ON VIEW toyemp IS NULL;

-- These views are left around mainly to exercise special cases in pg_dump.

--DDL_STATEMENT_BEGIN--
drop table if exists view_base_table cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE view_base_table (key1 int PRIMARY KEY, data varchar(20));
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW key_dependent_view AS
   SELECT * FROM view_base_table GROUP BY key1;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE view_base_table DROP CONSTRAINT view_base_table_pkey;  -- fails
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW key_dependent_view_no_cols AS
   SELECT FROM view_base_table GROUP BY key1 HAVING length(data) > 0;
--DDL_STATEMENT_END--

--
-- CREATE OR REPLACE VIEW
--

--DDL_STATEMENT_BEGIN--
CREATE TABLE viewtest_tbl (a int, b int);
--DDL_STATEMENT_END--
COPY viewtest_tbl FROM stdin;
5	10
10	15
15	20
20	25
\.

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW viewtest AS
	SELECT * FROM viewtest_tbl;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW viewtest AS
	SELECT * FROM viewtest_tbl WHERE a > 10;
--DDL_STATEMENT_END--

SELECT * FROM viewtest;

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b FROM viewtest_tbl WHERE a > 5 ORDER BY b DESC;
--DDL_STATEMENT_END--

SELECT * FROM viewtest;

-- should fail
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW viewtest AS
	SELECT a FROM viewtest_tbl WHERE a <> 20;
--DDL_STATEMENT_END--

-- should fail
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW viewtest AS
	SELECT 1, * FROM viewtest_tbl;
--DDL_STATEMENT_END--

-- should fail
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b::numeric FROM viewtest_tbl;
--DDL_STATEMENT_END--

-- should work
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b, 0 AS c FROM viewtest_tbl;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP VIEW viewtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE viewtest_tbl;
--DDL_STATEMENT_END--

-- tests for temporary views

--DDL_STATEMENT_BEGIN--
CREATE SCHEMA temp_view_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE temp_view_test.base_table (a int, id int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE temp_view_test.base_table2 (a int, id int);
--DDL_STATEMENT_END--

SET search_path TO temp_view_test, public;

--DDL_STATEMENT_BEGIN--
CREATE TEMPORARY TABLE temp_table (a int, id int);
--DDL_STATEMENT_END--

-- should be created in temp_view_test schema
--DDL_STATEMENT_BEGIN--
CREATE VIEW v1 AS SELECT * FROM base_table;
--DDL_STATEMENT_END--
-- should be created in temp object schema
--DDL_STATEMENT_BEGIN--
CREATE VIEW v1_temp AS SELECT * FROM temp_table;
--DDL_STATEMENT_END--
-- should be created in temp object schema
--DDL_STATEMENT_BEGIN--
CREATE TEMP VIEW v2_temp AS SELECT * FROM base_table;
--DDL_STATEMENT_END--
-- should be created in temp_views schema
--DDL_STATEMENT_BEGIN--
CREATE VIEW temp_view_test.v2 AS SELECT * FROM base_table;
--DDL_STATEMENT_END--
-- should fail
--DDL_STATEMENT_BEGIN--
CREATE VIEW temp_view_test.v3_temp AS SELECT * FROM temp_table;
--DDL_STATEMENT_END--
-- should fail
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA test_view_schema;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP VIEW test_view_schema.testview AS SELECT 1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW test_view_schema.testview;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA test_view_schema;
--DDL_STATEMENT_END--
-- joins: if any of the join relations are temporary, the view
-- should also be temporary

-- should be non-temp
--DDL_STATEMENT_BEGIN--
CREATE VIEW v3 AS
    SELECT t1.a AS t1_a, t2.a AS t2_a
    FROM base_table t1, base_table2 t2
    WHERE t1.id = t2.id;
--DDL_STATEMENT_END--
-- should be temp (one join rel is temp)
--DDL_STATEMENT_BEGIN--
CREATE VIEW v4_temp AS
    SELECT t1.a AS t1_a, t2.a AS t2_a
    FROM base_table t1, temp_table t2
    WHERE t1.id = t2.id;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- should be temp
CREATE VIEW v5_temp AS
    SELECT t1.a AS t1_a, t2.a AS t2_a, t3.a AS t3_a
    FROM base_table t1, base_table2 t2, temp_table t3
    WHERE t1.id = t2.id and t2.id = t3.id;
--DDL_STATEMENT_END--

-- subqueries
--DDL_STATEMENT_BEGIN--
CREATE VIEW v4 AS SELECT * FROM base_table WHERE id IN (SELECT id FROM base_table2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v5 AS SELECT t1.id, t2.a FROM base_table t1, (SELECT * FROM base_table2) t2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v6 AS SELECT * FROM base_table WHERE EXISTS (SELECT 1 FROM base_table2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v7 AS SELECT * FROM base_table WHERE NOT EXISTS (SELECT 1 FROM base_table2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v8 AS SELECT * FROM base_table WHERE EXISTS (SELECT 1);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW v6_temp AS SELECT * FROM base_table WHERE id IN (SELECT id FROM temp_table);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v7_temp AS SELECT t1.id, t2.a FROM base_table t1, (SELECT * FROM temp_table) t2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v8_temp AS SELECT * FROM base_table WHERE EXISTS (SELECT 1 FROM temp_table);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v9_temp AS SELECT * FROM base_table WHERE NOT EXISTS (SELECT 1 FROM temp_table);
--DDL_STATEMENT_END--

-- a view should also be temporary if it references a temporary view
--DDL_STATEMENT_BEGIN--
CREATE VIEW v10_temp AS SELECT * FROM v7_temp;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v11_temp AS SELECT t1.id, t2.a FROM base_table t1, v10_temp t2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v12_temp AS SELECT true FROM v11_temp;
--DDL_STATEMENT_END--

-- a view should also be temporary if it references a temporary sequence
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMPORARY SEQUENCE seq1_temp;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v9 AS SELECT seq1.is_called FROM seq1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v13_temp AS SELECT seq1_temp.is_called FROM seq1_temp;
--DDL_STATEMENT_END--

SELECT relname FROM pg_class
    WHERE relname LIKE 'v_'
    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'temp_view_test')
    ORDER BY relname;
SELECT relname FROM pg_class
    WHERE relname LIKE 'v%'
    AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_temp%')
    ORDER BY relname;
	
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA testviewschm2;
--DDL_STATEMENT_END--
SET search_path TO testviewschm2, public;

--DDL_STATEMENT_BEGIN--
CREATE TABLE t1 (num int, name text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE t2 (num2 int, value text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE tt (num2 int, value text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW nontemp1 AS SELECT * FROM t1 CROSS JOIN t2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW temporal1 AS SELECT * FROM t1 CROSS JOIN tt;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW nontemp2 AS SELECT * FROM t1 INNER JOIN t2 ON t1.num = t2.num2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW temporal2 AS SELECT * FROM t1 INNER JOIN tt ON t1.num = tt.num2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW nontemp3 AS SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW temporal3 AS SELECT * FROM t1 LEFT JOIN tt ON t1.num = tt.num2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW nontemp4 AS SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num2 AND t2.value = 'xxx';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW temporal4 AS SELECT * FROM t1 LEFT JOIN tt ON t1.num = tt.num2 AND tt.value = 'xxx';
--DDL_STATEMENT_END--

SELECT relname FROM pg_class
    WHERE relname LIKE 'nontemp%'
    AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'testviewschm2')
    ORDER BY relname;
SELECT relname FROM pg_class
    WHERE relname LIKE 'temporal%'
    AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_temp%')
    ORDER BY relname;
	
--DDL_STATEMENT_BEGIN--
CREATE TABLE tbl1 ( a int, b int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tbl2 (c int, d int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tbl3 (e int, f int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tbl4 (g int, h int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE tmptbl (i int, j int);
--DDL_STATEMENT_END--

--Should be in testviewschm2
--DDL_STATEMENT_BEGIN--
CREATE   VIEW  pubview AS SELECT * FROM tbl1 WHERE tbl1.a
BETWEEN (SELECT d FROM tbl2 WHERE c = 1) AND (SELECT e FROM tbl3 WHERE f = 2)
AND EXISTS (SELECT g FROM tbl4 LEFT JOIN tbl3 ON tbl4.h = tbl3.f);
--DDL_STATEMENT_END--

SELECT count(*) FROM pg_class where relname = 'pubview'
AND relnamespace IN (SELECT OID FROM pg_namespace WHERE nspname = 'testviewschm2');

--Should be in temp object schema
--DDL_STATEMENT_BEGIN--
CREATE   VIEW  mytempview AS SELECT * FROM tbl1 WHERE tbl1.a
BETWEEN (SELECT d FROM tbl2 WHERE c = 1) AND (SELECT e FROM tbl3 WHERE f = 2)
AND EXISTS (SELECT g FROM tbl4 LEFT JOIN tbl3 ON tbl4.h = tbl3.f)
AND NOT EXISTS (SELECT g FROM tbl4 LEFT JOIN tmptbl ON tbl4.h = tmptbl.j);
--DDL_STATEMENT_END--

SELECT count(*) FROM pg_class where relname LIKE 'mytempview'
And relnamespace IN (SELECT OID FROM pg_namespace WHERE nspname LIKE 'pg_temp%');

--
-- CREATE VIEW and WITH(...) clause
--
--DDL_STATEMENT_BEGIN--
CREATE VIEW mysecview1
       AS SELECT * FROM tbl1 WHERE a = 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW mysecview2 WITH (security_barrier=true)
       AS SELECT * FROM tbl1 WHERE a > 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW mysecview3 WITH (security_barrier=false)
       AS SELECT * FROM tbl1 WHERE a < 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW mysecview4 WITH (security_barrier)
       AS SELECT * FROM tbl1 WHERE a <> 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW mysecview5 WITH (security_barrier=100)	-- Error
       AS SELECT * FROM tbl1 WHERE a > 100;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW mysecview6 WITH (invalid_option)		-- Error
       AS SELECT * FROM tbl1 WHERE a < 100;
--DDL_STATEMENT_END--
SELECT relname, relkind, reloptions FROM pg_class
       WHERE oid in ('mysecview1'::regclass, 'mysecview2'::regclass,
                     'mysecview3'::regclass, 'mysecview4'::regclass)
       ORDER BY relname;
	   
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW mysecview1
       AS SELECT * FROM tbl1 WHERE a = 256;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW mysecview2
       AS SELECT * FROM tbl1 WHERE a > 256;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW mysecview3 WITH (security_barrier=true)
       AS SELECT * FROM tbl1 WHERE a < 256;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW mysecview4 WITH (security_barrier=false)
       AS SELECT * FROM tbl1 WHERE a <> 256;
--DDL_STATEMENT_END--
SELECT relname, relkind, reloptions FROM pg_class
       WHERE oid in ('mysecview1'::regclass, 'mysecview2'::regclass,
                     'mysecview3'::regclass, 'mysecview4'::regclass)
       ORDER BY relname;

-- Check that unknown literals are converted to "text" in CREATE VIEW,
-- so that we don't end up with unknown-type columns.

--DDL_STATEMENT_BEGIN--
CREATE VIEW unspecified_types AS
  SELECT 42 as i, 42.5 as num, 'foo' as u, 'foo'::unknown as u2, null as n;
--DDL_STATEMENT_END--
\d+ unspecified_types
SELECT * FROM unspecified_types;

-- This test checks that proper typmods are assigned in a multi-row VALUES

--DDL_STATEMENT_BEGIN--
CREATE VIEW tt1 AS
  SELECT * FROM (
    VALUES
       ('abc'::varchar(3), '0123456789', 42, 'abcd'::varchar(4)),
       ('0123456789', 'abc'::varchar(3), 42.12, 'abc'::varchar(4))
  ) vv(a,b,c,d);
--DDL_STATEMENT_END--
\d+ tt1
SELECT * FROM tt1;
SELECT a::varchar(3) FROM tt1;
--DDL_STATEMENT_BEGIN--
DROP VIEW tt1;
--DDL_STATEMENT_END--

-- Test view decompilation in the face of relation renaming conflicts

--DDL_STATEMENT_BEGIN--
CREATE TABLE tt1 (f1 int, f2 int, f3 text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tx1 (x1 int, x2 int, x3 text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE temp_view_test.tt1 (y1 int, f2 int, f3 text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW aliased_view_1 AS
  select * from tt1
    where exists (select 1 from tx1 where tt1.f1 = tx1.x1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW aliased_view_2 AS
  select * from tt1 a1
    where exists (select 1 from tx1 where a1.f1 = tx1.x1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW aliased_view_3 AS
  select * from tt1
    where exists (select 1 from tx1 a2 where tt1.f1 = a2.x1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW aliased_view_4 AS
  select * from temp_view_test.tt1
    where exists (select 1 from tt1 where temp_view_test.tt1.y1 = tt1.f1);
--DDL_STATEMENT_END--

\d+ aliased_view_1
\d+ aliased_view_2
\d+ aliased_view_3
\d+ aliased_view_4

--DDL_STATEMENT_BEGIN--
ALTER TABLE tx1 RENAME TO a1;
--DDL_STATEMENT_END--

\d+ aliased_view_1
\d+ aliased_view_2
\d+ aliased_view_3
\d+ aliased_view_4

--DDL_STATEMENT_BEGIN--
ALTER TABLE tt1 RENAME TO a2;
--DDL_STATEMENT_END--

\d+ aliased_view_1
\d+ aliased_view_2
\d+ aliased_view_3
\d+ aliased_view_4

--DDL_STATEMENT_BEGIN--
ALTER TABLE a1 RENAME TO tt1;
--DDL_STATEMENT_END--

\d+ aliased_view_1
\d+ aliased_view_2
\d+ aliased_view_3
\d+ aliased_view_4

--DDL_STATEMENT_BEGIN--
ALTER TABLE a2 RENAME TO tx1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tx1 SET SCHEMA temp_view_test;
--DDL_STATEMENT_END--

\d+ aliased_view_1
\d+ aliased_view_2
\d+ aliased_view_3
\d+ aliased_view_4

--DDL_STATEMENT_BEGIN--
ALTER TABLE temp_view_test.tt1 RENAME TO tmp1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE temp_view_test.tmp1 SET SCHEMA testviewschm2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tmp1 RENAME TO tx1;
--DDL_STATEMENT_END--

\d+ aliased_view_1
\d+ aliased_view_2
\d+ aliased_view_3
\d+ aliased_view_4

-- Test aliasing of joins

--DDL_STATEMENT_BEGIN--
create view view_of_joins as
select * from
  (select * from (tbl1 cross join tbl2) same) ss,
  (tbl3 cross join tbl4) same;
--DDL_STATEMENT_END--
\d+ view_of_joins

-- Test view decompilation in the face of column addition/deletion/renaming

--DDL_STATEMENT_BEGIN--
create table tt2 (a int, b int, c int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table tt3 (ax int8, b int2, c numeric);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table tt4 (ay int, b int, q int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create view v1 as select * from tt2 natural join tt3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view v1a as select * from (tt2 natural join tt3) j;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view v2 as select * from tt2 join tt3 using (b,c) join tt4 using (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view v2a as select * from (tt2 join tt3 using (b,c) join tt4 using (b)) j;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view v3 as select * from tt2 join tt3 using (b,c) full join tt4 using (b);
--DDL_STATEMENT_END--

select pg_get_viewdef('v1', true);
select pg_get_viewdef('v1a', true);
select pg_get_viewdef('v2', true);
select pg_get_viewdef('v2a', true);
select pg_get_viewdef('v3', true);

--DDL_STATEMENT_BEGIN--
alter table tt2 add column d int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table tt2 add column e int;
--DDL_STATEMENT_END--

select pg_get_viewdef('v1', true);
select pg_get_viewdef('v1a', true);
select pg_get_viewdef('v2', true);
select pg_get_viewdef('v2a', true);
select pg_get_viewdef('v3', true);

--DDL_STATEMENT_BEGIN--
alter table tt3 rename c to d;
--DDL_STATEMENT_END--

select pg_get_viewdef('v1', true);
select pg_get_viewdef('v1a', true);
select pg_get_viewdef('v2', true);
select pg_get_viewdef('v2a', true);
select pg_get_viewdef('v3', true);

--DDL_STATEMENT_BEGIN--
alter table tt3 add column c int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table tt3 add column e int;
--DDL_STATEMENT_END--

select pg_get_viewdef('v1', true);
select pg_get_viewdef('v1a', true);
select pg_get_viewdef('v2', true);
select pg_get_viewdef('v2a', true);
select pg_get_viewdef('v3', true);

--DDL_STATEMENT_BEGIN--
alter table tt2 drop column d;
--DDL_STATEMENT_END--

select pg_get_viewdef('v1', true);
select pg_get_viewdef('v1a', true);
select pg_get_viewdef('v2', true);
select pg_get_viewdef('v2a', true);
select pg_get_viewdef('v3', true);

--DDL_STATEMENT_BEGIN--
create table tt5 (a int, b int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table tt6 (c int, d int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view vv1 as select * from (tt5 cross join tt6) j(aa,bb,cc,dd);
--DDL_STATEMENT_END--
select pg_get_viewdef('vv1', true);
--DDL_STATEMENT_BEGIN--
alter table tt5 add column c int;
--DDL_STATEMENT_END--
select pg_get_viewdef('vv1', true);
--DDL_STATEMENT_BEGIN--
alter table tt5 add column cc int;
--DDL_STATEMENT_END--
select pg_get_viewdef('vv1', true);
--DDL_STATEMENT_BEGIN--
alter table tt5 drop column c;
--DDL_STATEMENT_END--
select pg_get_viewdef('vv1', true);

-- Unnamed FULL JOIN USING is lots of fun too

--DDL_STATEMENT_BEGIN--
create table tt7 (x int, xx int, y int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table tt7 drop column xx;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table tt8 (x int, z int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create view vv2 as
select * from (values(1,2,3,4,5)) v(a,b,c,d,e)
union all
select * from tt7 full join tt8 using (x), tt8 tt8x;
--DDL_STATEMENT_END--

select pg_get_viewdef('vv2', true);

--DDL_STATEMENT_BEGIN--
create view vv3 as
select * from (values(1,2,3,4,5,6)) v(a,b,c,x,e,f)
union all
select * from
  tt7 full join tt8 using (x),
  tt7 tt7x full join tt8 tt8x using (x);
--DDL_STATEMENT_END--

select pg_get_viewdef('vv3', true);

--DDL_STATEMENT_BEGIN--
create view vv4 as
select * from (values(1,2,3,4,5,6,7)) v(a,b,c,x,e,f,g)
union all
select * from
  tt7 full join tt8 using (x),
  tt7 tt7x full join tt8 tt8x using (x) full join tt8 tt8y using (x);
--DDL_STATEMENT_END--

select pg_get_viewdef('vv4', true);

--DDL_STATEMENT_BEGIN--
alter table tt7 add column zz int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table tt7 add column z int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table tt7 drop column zz;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table tt8 add column z2 int;
--DDL_STATEMENT_END--

select pg_get_viewdef('vv2', true);
select pg_get_viewdef('vv3', true);
select pg_get_viewdef('vv4', true);

-- Implicit coercions in a JOIN USING create issues similar to FULL JOIN

--DDL_STATEMENT_BEGIN--
create table tt7a (x date, xx int, y int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table tt7a drop column xx;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table tt8a (x timestamptz, z int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create view vv2a as
select * from (values(now(),2,3,now(),5)) v(a,b,c,d,e)
union all
select * from tt7a left join tt8a using (x), tt8a tt8ax;
--DDL_STATEMENT_END--

select pg_get_viewdef('vv2a', true);

--
-- Also check dropping a column that existed when the view was made
--

--DDL_STATEMENT_BEGIN--
create table tt9 (x int, xx int, y int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table tt10 (x int, z int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create view vv5 as select x,y,z from tt9 join tt10 using(x);
--DDL_STATEMENT_END--

select pg_get_viewdef('vv5', true);

--DDL_STATEMENT_BEGIN--
alter table tt9 drop column xx;
--DDL_STATEMENT_END--

select pg_get_viewdef('vv5', true);

--
-- Another corner case is that we might add a column to a table below a
-- JOIN USING, and thereby make the USING column name ambiguous
--

--DDL_STATEMENT_BEGIN--
create table tt11 (x int, y int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table tt12 (x int, z int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table tt13 (z int, q int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create view vv6 as select x,y,z,q from
  (tt11 join tt12 using(x)) join tt13 using(z);
--DDL_STATEMENT_END--

select pg_get_viewdef('vv6', true);

--DDL_STATEMENT_BEGIN--
alter table tt11 add column z int;
--DDL_STATEMENT_END--

select pg_get_viewdef('vv6', true);

--
-- Check cases involving dropped/altered columns in a function's rowtype result
--

--DDL_STATEMENT_BEGIN--
create table tt14t (f1 text, f2 text, f3 text, f4 text);
--DDL_STATEMENT_END--
insert into tt14t values('foo', 'bar', 'baz', '42');
--DDL_STATEMENT_BEGIN--
alter table tt14t drop column f2;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function tt14f() returns setof tt14t as
$$
declare
    rec1 record;
begin
    for rec1 in select * from tt14t
    loop
        return next rec1;
    end loop;
end;
$$
language plpgsql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create view tt14v as select t.* from tt14f() t;
--DDL_STATEMENT_END--

select pg_get_viewdef('tt14v', true);
select * from tt14v;

-- this perhaps should be rejected, but it isn't:
--DDL_STATEMENT_BEGIN--
alter table tt14t drop column f3;
--DDL_STATEMENT_END--

-- f3 is still in the view ...
select pg_get_viewdef('tt14v', true);
-- but will fail at execution
select f1, f4 from tt14v;
select * from tt14v;

-- this perhaps should be rejected, but it isn't:
--DDL_STATEMENT_BEGIN--
alter table tt14t alter column f4 type integer using f4::integer;
--DDL_STATEMENT_END--

-- f4 is still in the view ...
select pg_get_viewdef('tt14v', true);
-- but will fail at execution
select f1, f3 from tt14v;
select * from tt14v;

-- check display of whole-row variables in some corner cases

--DDL_STATEMENT_BEGIN--
create type nestedcomposite as (x int8_tbl);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view tt15v as select row(i)::nestedcomposite from int8_tbl i;
--DDL_STATEMENT_END--
select * from tt15v;
select pg_get_viewdef('tt15v', true);
select row(i.*::int8_tbl)::nestedcomposite from int8_tbl i;

--DDL_STATEMENT_BEGIN--
create view tt16v as select * from int8_tbl i, lateral(values(i)) ss;
--DDL_STATEMENT_END--
select * from tt16v;
select pg_get_viewdef('tt16v', true);
select * from int8_tbl i, lateral(values(i.*::int8_tbl)) ss;

--DDL_STATEMENT_BEGIN--
create view tt17v as select * from int8_tbl i where i in (values(i));
--DDL_STATEMENT_END--
select * from tt17v;
select pg_get_viewdef('tt17v', true);
select * from int8_tbl i where i.* in (values(i.*::int8_tbl));

-- check unique-ification of overlength names

--DDL_STATEMENT_BEGIN--
create view tt18v as
  select * from int8_tbl xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxy
  union all
  select * from int8_tbl xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxz;
--DDL_STATEMENT_END--
select pg_get_viewdef('tt18v', true);
explain (costs off) select * from tt18v;

-- check display of ScalarArrayOp with a sub-select

select 'foo'::text = any(array['abc','def','foo']::text[]);
select 'foo'::text = any((select array['abc','def','foo']::text[]));  -- fail
select 'foo'::text = any((select array['abc','def','foo']::text[])::text[]);

--DDL_STATEMENT_BEGIN--
create view tt19v as
select 'foo'::text = any(array['abc','def','foo']::text[]) c1,
       'foo'::text = any((select array['abc','def','foo']::text[])::text[]) c2;
--DDL_STATEMENT_END--
select pg_get_viewdef('tt19v', true);

-- check display of assorted RTE_FUNCTION expressions

--DDL_STATEMENT_BEGIN--
create view tt20v as
select * from
  coalesce(1,2) as c,
  collation for ('x'::text) col,
  current_date as d,
  localtimestamp(3) as t,
  cast(1+2 as int4) as i4,
  cast(1+2 as int8) as i8;
--DDL_STATEMENT_END--
select pg_get_viewdef('tt20v', true);

-- corner cases with empty join conditions

--DDL_STATEMENT_BEGIN--
create view tt21v as
select * from tt5 natural inner join tt6;
--DDL_STATEMENT_END--
select pg_get_viewdef('tt21v', true);

--DDL_STATEMENT_BEGIN--
create view tt22v as
select * from tt5 natural left join tt6;
--DDL_STATEMENT_END--
select pg_get_viewdef('tt22v', true);

-- check handling of views with immediately-renamed columns

--DDL_STATEMENT_BEGIN--
create view tt23v (col_a, col_b) as
select q1 as other_name1, q2 as other_name2 from int8_tbl
union
select 42, 43;
--DDL_STATEMENT_END--

select pg_get_viewdef('tt23v', true);
select pg_get_ruledef(oid, true) from pg_rewrite
  where ev_class = 'tt23v'::regclass and ev_type = '1';

-- clean up all the random objects we made above
\set VERBOSITY terse \\ -- suppress cascade details
--DDL_STATEMENT_BEGIN--
DROP TABLE temp_view_test.base_table cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE temp_view_test.base_table2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE temp_view_test.tt1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE temp_view_test.tx1 cascade;
--DDL_STATEMENT_END--
DROP VIEW temp_view_test.v9 cascade;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE temp_view_test.seq1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA temp_view_test;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.t1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.t2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tbl1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tbl2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tbl3 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tbl4 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt3 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt4 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt5 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt6 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt7 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt7a cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt8 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt8a cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tx1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt9 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt10 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt11 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt12 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt13 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testviewschm2.tt14t cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA testviewschm2 cascade;
--DDL_STATEMENT_END--