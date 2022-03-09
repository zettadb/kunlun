--
-- Test access privileges
--

-- Clean up in case a prior regression run failed

-- Suppress NOTICE messages when users/groups don't exist
--DDL_STATEMENT_BEGIN--
SET client_min_messages TO 'warning';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_priv_group1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_priv_group2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_priv_user5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_priv_user6;
--DDL_STATEMENT_END--
RESET client_min_messages;

-- test proper begins here
--DDL_STATEMENT_BEGIN--
CREATE USER regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_priv_user5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_priv_user5;	-- duplicate
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE GROUP regress_priv_group1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE GROUP regress_priv_group2 WITH USER regress_priv_user1, regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER GROUP regress_priv_group1 ADD USER regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER GROUP regress_priv_group2 ADD USER regress_priv_user2;	-- duplicate
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER GROUP regress_priv_group2 DROP USER regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT regress_priv_group2 TO regress_priv_user4 WITH ADMIN OPTION;
--DDL_STATEMENT_END--
-- test owner privileges
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SELECT session_user, current_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists atest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE atest1 ( a int, b text );
--DDL_STATEMENT_END--
SELECT * FROM atest1;
INSERT INTO atest1 VALUES (1, 'one');
DELETE FROM atest1;
UPDATE atest1 SET a = 1 WHERE b = 'blech';
delete from atest1;
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK atest1 IN ACCESS EXCLUSIVE MODE;
COMMIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON atest1 FROM PUBLIC;
--DDL_STATEMENT_END--
SELECT * FROM atest1;
--DDL_STATEMENT_BEGIN--
GRANT ALL ON atest1 TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atest1 TO regress_priv_user3, regress_priv_user4;
--DDL_STATEMENT_END--
SELECT * FROM atest1;
--DDL_STATEMENT_BEGIN--
drop table if exists atest2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE atest2 (col1 varchar(10), col2 boolean);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atest2 TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON atest2 TO regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT INSERT ON atest2 TO regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user2;
--DDL_STATEMENT_END--
SELECT session_user, current_user;

-- try various combinations of queries on atest1 and atest2

SELECT * FROM atest1; -- ok
SELECT * FROM atest2; -- ok
INSERT INTO atest1 VALUES (2, 'two'); -- ok
INSERT INTO atest2 VALUES ('foo', true); -- fail
INSERT INTO atest1 SELECT 1, b FROM atest1; -- ok
UPDATE atest1 SET a = 1 WHERE a = 2; -- ok
UPDATE atest2 SET col2 = NOT col2; -- fail
DELETE FROM atest2; -- fail
delete from atest2; -- fail
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK atest2 IN ACCESS EXCLUSIVE MODE; -- fail
COMMIT;
--DDL_STATEMENT_END--
COPY atest2 FROM stdin; -- fail

--DDL_STATEMENT_BEGIN--
GRANT ALL ON atest1 TO PUBLIC; -- fail
--DDL_STATEMENT_END--
-- checks in subquery, both ok
SELECT * FROM atest1 WHERE ( b IN ( SELECT col1 FROM atest2 ) );
SELECT * FROM atest2 WHERE ( col1 IN ( SELECT b FROM atest1 ) );

--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user3;
--DDL_STATEMENT_END--
SELECT session_user, current_user;

SELECT * FROM atest1; -- ok
SELECT * FROM atest2; -- fail
INSERT INTO atest1 VALUES (2, 'two'); -- fail
INSERT INTO atest2 VALUES ('foo', true); -- fail
INSERT INTO atest1 SELECT 1, b FROM atest1; -- fail
UPDATE atest1 SET a = 1 WHERE a = 2; -- fail
UPDATE atest2 SET col2 = NULL; -- ok
UPDATE atest2 SET col2 = NOT col2; -- fails; requires SELECT on atest2
delete from atest2; -- fail
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK atest2 IN ACCESS EXCLUSIVE MODE; -- ok
COMMIT;
--DDL_STATEMENT_END--
COPY atest2 FROM stdin; -- fail

-- checks in subquery, both fail
SELECT * FROM atest1 WHERE ( b IN ( SELECT col1 FROM atest2 ) );
SELECT * FROM atest2 WHERE ( col1 IN ( SELECT b FROM atest1 ) );
SET SESSION AUTHORIZATION regress_priv_user4;
COPY atest2 FROM stdin; -- ok
bar	true
\.
SELECT * FROM atest1; -- ok


-- test leaky-function protections in selfuncs

-- regress_priv_user1 will own a table and provide views for it.
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists atest12;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table atest12(a int, b int);
--DDL_STATEMENT_END--
insert into atest12 SELECT x AS a, 10001 - x AS b FROM generate_series(1,10000) x;
--DDL_STATEMENT_BEGIN--
CREATE INDEX ON atest12 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION leak(integer,integer) RETURNS boolean
  AS $$begin return $1 < $2; end$$
  LANGUAGE plpgsql immutable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR <<< (procedure = leak, leftarg = integer, rightarg = integer,
                     restrict = scalarltsel);
--DDL_STATEMENT_END--
-- views with leaky operator
--DDL_STATEMENT_BEGIN--
CREATE VIEW atest12v AS
  SELECT * FROM atest12 WHERE b <<< 5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW atest12sbv AS
  SELECT * FROM atest12 WHERE b <<< 5;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atest12v TO PUBLIC;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atest12sbv TO PUBLIC;
--DDL_STATEMENT_END--
-- This plan should use nestloop, knowing that few rows will be selected.
EXPLAIN (COSTS OFF) SELECT * FROM atest12v x, atest12v y WHERE x.a = y.b;

-- And this one.
EXPLAIN (COSTS OFF) SELECT * FROM atest12 x, atest12 y
  WHERE x.a = y.b and abs(y.a) <<< 5;

-- This should also be a nestloop, but the security barrier forces the inner
-- scan to be materialized
EXPLAIN (COSTS OFF) SELECT * FROM atest12sbv x, atest12sbv y WHERE x.a = y.b;

-- Check if regress_priv_user2 can break security.
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION leak2(integer,integer) RETURNS boolean
  AS $$begin raise notice 'leak % %', $1, $2; return $1 > $2; end$$
  LANGUAGE plpgsql immutable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR >>> (procedure = leak2, leftarg = integer, rightarg = integer,
                     restrict = scalargtsel);
--DDL_STATEMENT_END--
-- This should not show any "leak" notices before failing.
EXPLAIN (COSTS OFF) SELECT * FROM atest12 WHERE a >>> 0;

-- These plans should continue to use a nestloop, since they execute with the
-- privileges of the view owner.
EXPLAIN (COSTS OFF) SELECT * FROM atest12v x, atest12v y WHERE x.a = y.b;
EXPLAIN (COSTS OFF) SELECT * FROM atest12sbv x, atest12sbv y WHERE x.a = y.b;

-- A non-security barrier view does not guard against information leakage.
EXPLAIN (COSTS OFF) SELECT * FROM atest12v x, atest12v y
  WHERE x.a = y.b and abs(y.a) <<< 5;

-- But a security barrier view isolates the leaky operator.
EXPLAIN (COSTS OFF) SELECT * FROM atest12sbv x, atest12sbv y
  WHERE x.a = y.b and abs(y.a) <<< 5;

-- Now regress_priv_user1 grants sufficient access to regress_priv_user2.
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (a, b) ON atest12 TO PUBLIC;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user2;

-- regress_priv_user2 should continue to get a good row estimate.
EXPLAIN (COSTS OFF) SELECT * FROM atest12v x, atest12v y WHERE x.a = y.b;

-- But not for this, due to lack of table-wide permissions needed
-- to make use of the expression index's statistics.
EXPLAIN (COSTS OFF) SELECT * FROM atest12 x, atest12 y
  WHERE x.a = y.b and abs(y.a) <<< 5;

-- clean up (regress_priv_user1's objects are all dropped later)
--DDL_STATEMENT_BEGIN--
DROP FUNCTION leak2(integer, integer) CASCADE;
--DDL_STATEMENT_END--
-- groups
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists atest3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE atest3 (one int, two int, three int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT DELETE ON atest3 TO GROUP regress_priv_group2;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user1;

SELECT * FROM atest3; -- fail
DELETE FROM atest3; -- ok


-- views

SET SESSION AUTHORIZATION regress_priv_user3;
--DDL_STATEMENT_BEGIN--
CREATE VIEW atestv1 AS SELECT * FROM atest1; -- ok
--DDL_STATEMENT_END--
/* The next *should* fail, but it's not implemented that way yet. */
--DDL_STATEMENT_BEGIN--
CREATE VIEW atestv2 AS SELECT * FROM atest2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW atestv3 AS SELECT * FROM atest3; -- ok
--DDL_STATEMENT_END--
/* Empty view is a corner case that failed in 9.2. */
--DDL_STATEMENT_BEGIN--
CREATE VIEW atestv0 AS SELECT 0 as x WHERE false; -- ok
--DDL_STATEMENT_END--
SELECT * FROM atestv1; -- ok
SELECT * FROM atestv2; -- fail
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atestv1, atestv3 TO regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atestv2 TO regress_priv_user2;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;

SELECT * FROM atestv1; -- ok
SELECT * FROM atestv2; -- fail
SELECT * FROM atestv3; -- ok
SELECT * FROM atestv0; -- fail

-- Appendrels excluded by constraints failed to check permissions in 8.4-9.2.
select * from
  ((select a.q1 as x from int8_tbl a offset 0)
   union all
   (select b.q2 as x from int8_tbl b offset 0)) ss
where false;

set constraint_exclusion = on;
select * from
  ((select a.q1 as x, random() from int8_tbl a where q1 > 0)
   union all
   (select b.q2 as x, random() from int8_tbl b where q2 > 0)) ss
where x < 0;
reset constraint_exclusion;
--DDL_STATEMENT_BEGIN--
CREATE VIEW atestv4 AS SELECT * FROM atestv3; -- nested view
--DDL_STATEMENT_END--
SELECT * FROM atestv4; -- ok
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atestv4 TO regress_priv_user2;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user2;

-- Two complex cases:

SELECT * FROM atestv3; -- fail
SELECT * FROM atestv4; -- ok (even though regress_priv_user2 cannot access underlying atestv3)

SELECT * FROM atest2; -- ok
SELECT * FROM atestv2; -- fail (even though regress_priv_user2 can access underlying atest2)

-- Test column level permissions
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists atest5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE atest5 (one int, two int unique, three int, four int unique);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists atest6;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE atest6 (one int, two int, blue int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (one), INSERT (two), UPDATE (three) ON atest5 TO regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL (one) ON atest5 TO regress_priv_user3;
--DDL_STATEMENT_END--
INSERT INTO atest5 VALUES (1,2,3);

SET SESSION AUTHORIZATION regress_priv_user4;
SELECT * FROM atest5; -- fail
SELECT one FROM atest5; -- ok
COPY atest5 (one) TO stdout; -- ok
SELECT two FROM atest5; -- fail
COPY atest5 (two) TO stdout; -- fail
SELECT atest5 FROM atest5; -- fail
COPY atest5 (one,two) TO stdout; -- fail
SELECT 1 FROM atest5; -- ok
SELECT 1 FROM atest5 a JOIN atest5 b USING (one); -- ok
SELECT 1 FROM atest5 a JOIN atest5 b USING (two); -- fail
SELECT 1 FROM atest5 a NATURAL JOIN atest5 b; -- fail
SELECT (j.*) IS NULL FROM (atest5 a JOIN atest5 b USING (one)) j; -- fail
SELECT 1 FROM atest5 WHERE two = 2; -- fail
SELECT * FROM atest1, atest5; -- fail
SELECT atest1.* FROM atest1, atest5; -- ok
SELECT atest1.*,atest5.one FROM atest1, atest5; -- ok
SELECT atest1.*,atest5.one FROM atest1 JOIN atest5 ON (atest1.a = atest5.two); -- fail
SELECT atest1.*,atest5.one FROM atest1 JOIN atest5 ON (atest1.a = atest5.one); -- ok
SELECT one, two FROM atest5; -- fail
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (one,two) ON atest6 TO regress_priv_user4;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;
SELECT one, two FROM atest5 NATURAL JOIN atest6; -- fail still

SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_BEGIN--
GRANT SELECT (two) ON atest5 TO regress_priv_user4;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;
SELECT one, two FROM atest5 NATURAL JOIN atest6; -- ok now

-- test column-level privileges for INSERT and UPDATE
INSERT INTO atest5 (two) VALUES (3); -- ok
COPY atest5 FROM stdin; -- fail
COPY atest5 (two) FROM stdin; -- ok
1
\.
INSERT INTO atest5 (three) VALUES (4); -- fail
INSERT INTO atest5 VALUES (5,5,5); -- fail
UPDATE atest5 SET three = 10; -- ok
UPDATE atest5 SET one = 8; -- fail
UPDATE atest5 SET three = 5, one = 2; -- fail
-- Check that column level privs are enforced in RETURNING

-- Check that the columns in the inference require select privileges
INSERT INTO atest5(four) VALUES (4); -- fail
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT INSERT (four) ON atest5 TO regress_priv_user4;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;

INSERT INTO atest5(four) VALUES (4); -- ok
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (four) ON atest5 TO regress_priv_user4;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;

SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_BEGIN--
REVOKE ALL (one) ON atest5 FROM regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (one,two,blue) ON atest6 TO regress_priv_user4;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;
SELECT one FROM atest5; -- fail
UPDATE atest5 SET one = 1; -- fail
SELECT atest6 FROM atest6; -- ok
COPY atest6 TO stdout; -- ok

-- check error reporting with column privs
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE t1 (c1 int, c2 int, c3 int, primary key (c1, c2));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (c1) ON t1 TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT INSERT (c1, c2, c3) ON t1 TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE (c1, c2, c3) ON t1 TO regress_priv_user2;
--DDL_STATEMENT_END--

-- seed data
INSERT INTO t1 VALUES (1, 1, 1);
INSERT INTO t1 VALUES (1, 2, 1);
INSERT INTO t1 VALUES (2, 1, 2);
INSERT INTO t1 VALUES (2, 2, 2);
INSERT INTO t1 VALUES (3, 1, 3);

SET SESSION AUTHORIZATION regress_priv_user2;
INSERT INTO t1 (c1, c2) VALUES (1, 1); -- fail, but row not shown
UPDATE t1 SET c2 = 1; -- fail, but row not shown
INSERT INTO t1 (c1, c2) VALUES (null, null); -- fail, but see columns being inserted
INSERT INTO t1 (c3) VALUES (null); -- fail, but see columns being inserted or have SELECT
INSERT INTO t1 (c1) VALUES (5); -- fail, but see columns being inserted or have SELECT
UPDATE t1 SET c3 = 10; -- fail, but see columns with SELECT rights, or being modified

SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_BEGIN--
DROP TABLE t1;
--DDL_STATEMENT_END--
-- test column-level privileges when involved with DELETE
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_BEGIN--
ALTER TABLE atest6 ADD COLUMN three integer;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT DELETE ON atest5 TO regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (two) ON atest5 TO regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL (one) ON atest5 FROM regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (one) ON atest5 TO regress_priv_user4;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;
SELECT atest6 FROM atest6; -- fail
SELECT one FROM atest5 NATURAL JOIN atest6; -- fail

SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_BEGIN--
ALTER TABLE atest6 DROP COLUMN three;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;
SELECT one FROM atest5 NATURAL JOIN atest6; -- ok

SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_BEGIN--
ALTER TABLE atest6 DROP COLUMN two;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE SELECT (one,blue) ON atest6 FROM regress_priv_user4;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user4;
SELECT * FROM atest6; -- fail
SELECT 1 FROM atest6; -- fail

SET SESSION AUTHORIZATION regress_priv_user3;
DELETE FROM atest5 WHERE one = 1; -- fail
DELETE FROM atest5 WHERE two = 2; -- ok

-- privileges on functions, languages

-- switch to superuser
\c -
--DDL_STATEMENT_BEGIN--
REVOKE ALL PRIVILEGES ON LANGUAGE sql FROM PUBLIC;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON LANGUAGE sql TO regress_priv_user1; -- 
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON LANGUAGE c TO PUBLIC; -- fail
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON LANGUAGE sql TO regress_priv_user2; -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION priv_testfunc1(int) RETURNS int AS 'select 2 * $1;' LANGUAGE sql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION priv_testfunc2(int) RETURNS int AS 'select 3 * $1;' LANGUAGE sql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE priv_testagg1(int) (sfunc = int4pl, stype = int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE priv_testproc1(int) AS 'select $1;' LANGUAGE sql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON FUNCTION priv_testfunc1(int), priv_testfunc2(int), priv_testagg1(int) FROM PUBLIC;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT EXECUTE ON FUNCTION priv_testfunc1(int), priv_testfunc2(int), priv_testagg1(int) TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON FUNCTION priv_testproc1(int) FROM PUBLIC; -- fail, not a function
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON PROCEDURE priv_testproc1(int) FROM PUBLIC;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT EXECUTE ON PROCEDURE priv_testproc1(int) TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON FUNCTION priv_testfunc1(int) TO regress_priv_user3; -- semantic error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON FUNCTION priv_testagg1(int) TO regress_priv_user3; -- semantic error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON PROCEDURE priv_testproc1(int) TO regress_priv_user3; -- semantic error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL PRIVILEGES ON FUNCTION priv_testfunc1(int) TO regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL PRIVILEGES ON FUNCTION priv_testfunc_nosuch(int) TO regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL PRIVILEGES ON FUNCTION priv_testagg1(int) TO regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL PRIVILEGES ON PROCEDURE priv_testproc1(int) TO regress_priv_user4;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE FUNCTION priv_testfunc4(boolean) RETURNS text
  AS 'select col1 from atest2 where col2 = $1;'
  LANGUAGE sql SECURITY DEFINER;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT EXECUTE ON FUNCTION priv_testfunc4(boolean) TO regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user2;
--DDL_STATEMENT_END--
SELECT priv_testfunc1(5), priv_testfunc2(5); -- ok
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION priv_testfunc3(int) RETURNS int AS 'select 2 * $1;' LANGUAGE sql; -- fail
--DDL_STATEMENT_END--
SELECT priv_testagg1(x) FROM (VALUES (1), (2), (3)) _(x); -- ok
CALL priv_testproc1(6); -- ok

SET SESSION AUTHORIZATION regress_priv_user3;
SELECT priv_testfunc1(5); -- fail
SELECT priv_testagg1(x) FROM (VALUES (1), (2), (3)) _(x); -- fail
CALL priv_testproc1(6); -- fail
SELECT col1 FROM atest2 WHERE col2 = true; -- fail
SELECT priv_testfunc4(true); -- ok
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user4;
--DDL_STATEMENT_END--
SELECT priv_testfunc1(5); -- ok
SELECT priv_testagg1(x) FROM (VALUES (1), (2), (3)) _(x); -- ok
CALL priv_testproc1(6); -- ok
--DDL_STATEMENT_BEGIN--
DROP FUNCTION priv_testfunc1(int); -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP AGGREGATE priv_testagg1(int); -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PROCEDURE priv_testproc1(int); -- fail
--DDL_STATEMENT_END--
\c -
--DDL_STATEMENT_BEGIN--
DROP FUNCTION priv_testfunc1(int); -- ok
--DDL_STATEMENT_END--
-- restore to sanity
--DDL_STATEMENT_BEGIN--
GRANT ALL PRIVILEGES ON LANGUAGE sql TO PUBLIC;
--DDL_STATEMENT_END--
-- verify privilege checks on array-element coercions
BEGIN;
SELECT '{1}'::int4[]::int8[];
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON FUNCTION int8(integer) FROM PUBLIC;
--DDL_STATEMENT_END--
SELECT '{1}'::int4[]::int8[]; --superuser, suceed
SET SESSION AUTHORIZATION regress_priv_user4;
SELECT '{1}'::int4[]::int8[]; --other user, fail
ROLLBACK;

-- privileges on types

-- switch to superuser
\c -

SET SESSION AUTHORIZATION regress_priv_user5;
delete from atest2; -- ok
delete from atest3; -- fail

-- has_table_privilege function

-- bad-input checks
select has_table_privilege(NULL,'pg_authid','select');
select has_table_privilege('pg_shad','select');
select has_table_privilege('nosuchuser','pg_authid','select');
select has_table_privilege('pg_authid','sel');
select has_table_privilege(-999999,'pg_authid','update');
select has_table_privilege(1,'select');

-- superuser
\c -

select has_table_privilege(current_user,'pg_authid','select');
select has_table_privilege(current_user,'pg_authid','insert');

select has_table_privilege(t2.oid,'pg_authid','update')
from (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,'pg_authid','delete')
from (select oid from pg_roles where rolname = current_user) as t2;

-- 'rule' privilege no longer exists, but for backwards compatibility
-- has_table_privilege still recognizes the keyword and says FALSE
select has_table_privilege(current_user,t1.oid,'rule')
from (select oid from pg_class where relname = 'pg_authid') as t1;
select has_table_privilege(current_user,t1.oid,'references')
from (select oid from pg_class where relname = 'pg_authid') as t1;

select has_table_privilege(t2.oid,t1.oid,'select')
from (select oid from pg_class where relname = 'pg_authid') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,t1.oid,'insert')
from (select oid from pg_class where relname = 'pg_authid') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege('pg_authid','update');
select has_table_privilege('pg_authid','delete');

select has_table_privilege(t1.oid,'select')
from (select oid from pg_class where relname = 'pg_authid') as t1;
select has_table_privilege(t1.oid,'trigger')
from (select oid from pg_class where relname = 'pg_authid') as t1;

-- non-superuser
SET SESSION AUTHORIZATION regress_priv_user3;

select has_table_privilege(current_user,'pg_class','select');
select has_table_privilege(current_user,'pg_class','insert');

select has_table_privilege(t2.oid,'pg_class','update')
from (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,'pg_class','delete')
from (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege(current_user,t1.oid,'references')
from (select oid from pg_class where relname = 'pg_class') as t1;

select has_table_privilege(t2.oid,t1.oid,'select')
from (select oid from pg_class where relname = 'pg_class') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,t1.oid,'insert')
from (select oid from pg_class where relname = 'pg_class') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege('pg_class','update');
select has_table_privilege('pg_class','delete');

select has_table_privilege(t1.oid,'select')
from (select oid from pg_class where relname = 'pg_class') as t1;
select has_table_privilege(t1.oid,'trigger')
from (select oid from pg_class where relname = 'pg_class') as t1;

select has_table_privilege(current_user,'atest1','select');
select has_table_privilege(current_user,'atest1','insert');

select has_table_privilege(t2.oid,'atest1','update')
from (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,'atest1','delete')
from (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege(current_user,t1.oid,'references')
from (select oid from pg_class where relname = 'atest1') as t1;

select has_table_privilege(t2.oid,t1.oid,'select')
from (select oid from pg_class where relname = 'atest1') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;
select has_table_privilege(t2.oid,t1.oid,'insert')
from (select oid from pg_class where relname = 'atest1') as t1,
  (select oid from pg_roles where rolname = current_user) as t2;

select has_table_privilege('atest1','update');
select has_table_privilege('atest1','delete');

select has_table_privilege(t1.oid,'select')
from (select oid from pg_class where relname = 'atest1') as t1;
select has_table_privilege(t1.oid,'trigger')
from (select oid from pg_class where relname = 'atest1') as t1;

-- has_column_privilege function

-- bad-input checks (as non-super-user)
select has_column_privilege('pg_authid',NULL,'select');
select has_column_privilege('pg_authid','nosuchcol','select');
select has_column_privilege(9999,'nosuchcol','select');
select has_column_privilege(9999,99::int2,'select');
select has_column_privilege('pg_authid',99::int2,'select');
select has_column_privilege(9999,99::int2,'select');

--DDL_STATEMENT_BEGIN--
create temp table mytable(f1 int, f2 int, f3 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table mytable drop column f2;
--DDL_STATEMENT_END--
select has_column_privilege('mytable','f2','select');
select has_column_privilege('mytable','........pg.dropped.2........','select');
select has_column_privilege('mytable',2::int2,'select');
--DDL_STATEMENT_BEGIN--
revoke select on table mytable from regress_priv_user3;
--DDL_STATEMENT_END--
select has_column_privilege('mytable',2::int2,'select');
--DDL_STATEMENT_BEGIN--
drop table mytable;
--DDL_STATEMENT_END--

-- Grant options
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists atest4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE atest4 (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atest4 TO regress_priv_user2 WITH GRANT OPTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON atest4 TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atest4 TO GROUP regress_priv_group1 WITH GRANT OPTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON atest4 TO regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON atest4 TO regress_priv_user3; -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE SELECT ON atest4 FROM regress_priv_user3; -- does nothing
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SELECT has_table_privilege('regress_priv_user3', 'atest4', 'SELECT'); -- true
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE SELECT ON atest4 FROM regress_priv_user2; -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE GRANT OPTION FOR SELECT ON atest4 FROM regress_priv_user2 CASCADE; -- ok
--DDL_STATEMENT_END--
SELECT has_table_privilege('regress_priv_user2', 'atest4', 'SELECT'); -- true
SELECT has_table_privilege('regress_priv_user3', 'atest4', 'SELECT'); -- false

SELECT has_table_privilege('regress_priv_user1', 'atest4', 'SELECT WITH GRANT OPTION'); -- true


-- Admin options
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION dogrant_ok() RETURNS void LANGUAGE sql SECURITY DEFINER AS
	'GRANT regress_priv_group2 TO regress_priv_user5';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT regress_priv_group2 TO regress_priv_user5; -- ok: had ADMIN OPTION
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET ROLE regress_priv_group2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT regress_priv_group2 TO regress_priv_user5; -- fails: SET ROLE suspended privilege
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_BEGIN--
GRANT regress_priv_group2 TO regress_priv_user5; -- fails: no ADMIN OPTION
--DDL_STATEMENT_END--
SELECT dogrant_ok();			-- ok: SECURITY DEFINER conveys ADMIN
SET ROLE regress_priv_group2;
--DDL_STATEMENT_BEGIN--
GRANT regress_priv_group2 TO regress_priv_user5; -- fails: SET ROLE did not help
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_priv_group2;
--DDL_STATEMENT_BEGIN--
--DDL_STATEMENT_BEGIN--
GRANT regress_priv_group2 TO regress_priv_user5; -- ok: a role can self-admin
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION dogrant_fails() RETURNS void LANGUAGE sql SECURITY DEFINER AS
	'GRANT regress_priv_group2 TO regress_priv_user5';
--DDL_STATEMENT_END--
--DDL_STATEMENT_END--
SELECT dogrant_fails();			-- fails: no self-admin in SECURITY DEFINER
--DDL_STATEMENT_BEGIN--
DROP FUNCTION dogrant_fails();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION dogrant_ok();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE regress_priv_group2 FROM regress_priv_user5;
--DDL_STATEMENT_END--

-- has_sequence_privilege tests
\c -
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE x_seq;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE on x_seq to regress_priv_user2;
--DDL_STATEMENT_END--
SELECT has_sequence_privilege('regress_priv_user1', 'atest1', 'SELECT');
SELECT has_sequence_privilege('regress_priv_user1', 'x_seq', 'INSERT');
SELECT has_sequence_privilege('regress_priv_user1', 'x_seq', 'SELECT');

SET SESSION AUTHORIZATION regress_priv_user2;

SELECT has_sequence_privilege('x_seq', 'USAGE');

-- largeobject privilege tests
\c -
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_priv_user1;
--DDL_STATEMENT_END--
SELECT lo_create(1001);
SELECT lo_create(1002);
SELECT lo_create(1003);
SELECT lo_create(1004);
SELECT lo_create(1005);
--DDL_STATEMENT_BEGIN--
GRANT ALL ON LARGE OBJECT 1001 TO PUBLIC;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON LARGE OBJECT 1003 TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT,UPDATE ON LARGE OBJECT 1004 TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON LARGE OBJECT 1005 TO regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON LARGE OBJECT 1005 TO regress_priv_user2 WITH GRANT OPTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT, INSERT ON LARGE OBJECT 1001 TO PUBLIC;	-- to be failed
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT, UPDATE ON LARGE OBJECT 1001 TO nosuchuser;	-- to be failed
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT, UPDATE ON LARGE OBJECT  999 TO PUBLIC;	-- to be failed
--DDL_STATEMENT_END--

\c -
SET SESSION AUTHORIZATION regress_priv_user2;

SELECT lo_create(2001);
SELECT lo_create(2002);

SELECT loread(lo_open(1001, x'20000'::int), 32);	-- allowed, for now
SELECT lowrite(lo_open(1001, x'40000'::int), 'abcd');	-- fail, wrong mode

SELECT loread(lo_open(1001, x'40000'::int), 32);
SELECT loread(lo_open(1002, x'40000'::int), 32);	-- to be denied
SELECT loread(lo_open(1003, x'40000'::int), 32);
SELECT loread(lo_open(1004, x'40000'::int), 32);

SELECT lowrite(lo_open(1001, x'20000'::int), 'abcd');
SELECT lowrite(lo_open(1002, x'20000'::int), 'abcd');	-- to be denied
SELECT lowrite(lo_open(1003, x'20000'::int), 'abcd');	-- to be denied
SELECT lowrite(lo_open(1004, x'20000'::int), 'abcd');
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON LARGE OBJECT 1005 TO regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON LARGE OBJECT 1006 TO regress_priv_user3;	-- to be denied
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON LARGE OBJECT 2001, 2002 FROM PUBLIC;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON LARGE OBJECT 2001 TO regress_priv_user3;
--DDL_STATEMENT_END--

SELECT lo_unlink(1001);		-- to be denied
SELECT lo_unlink(2002);

\c -
-- confirm ACL setting
SELECT oid, pg_get_userbyid(lomowner) ownername, lomacl FROM pg_largeobject_metadata WHERE oid >= 1000 AND oid < 3000 ORDER BY oid;

SET SESSION AUTHORIZATION regress_priv_user3;

SELECT loread(lo_open(1001, x'40000'::int), 32);
SELECT loread(lo_open(1003, x'40000'::int), 32);	-- to be denied
SELECT loread(lo_open(1005, x'40000'::int), 32);

SELECT lo_truncate(lo_open(1005, x'20000'::int), 10);	-- to be denied
SELECT lo_truncate(lo_open(2001, x'20000'::int), 10);

-- compatibility mode in largeobject permission
\c -
SET lo_compat_privileges = false;	-- default setting
SET SESSION AUTHORIZATION regress_priv_user4;

SELECT loread(lo_open(1002, x'40000'::int), 32);	-- to be denied
SELECT lowrite(lo_open(1002, x'20000'::int), 'abcd');	-- to be denied
SELECT lo_truncate(lo_open(1002, x'20000'::int), 10);	-- to be denied
SELECT lo_put(1002, 1, 'abcd');				-- to be denied
SELECT lo_unlink(1002);					-- to be denied
SELECT lo_export(1001, '/dev/null');			-- to be denied
SELECT lo_import('/dev/null');				-- to be denied
SELECT lo_import('/dev/null', 2003);			-- to be denied

\c -
SET lo_compat_privileges = true;	-- compatibility mode
SET SESSION AUTHORIZATION regress_priv_user4;

SELECT loread(lo_open(1002, x'40000'::int), 32);
SELECT lowrite(lo_open(1002, x'20000'::int), 'abcd');
SELECT lo_truncate(lo_open(1002, x'20000'::int), 10);
SELECT lo_unlink(1002);
SELECT lo_export(1001, '/dev/null');			-- to be denied

-- don't allow unpriv users to access pg_largeobject contents
\c -
SELECT * FROM pg_largeobject LIMIT 0;

SET SESSION AUTHORIZATION regress_priv_user1;
SELECT * FROM pg_largeobject LIMIT 0;			-- to be denied

-- test default ACLs
\c -
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA testns;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON SCHEMA testns TO regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testns.acltest1 (x int);
--DDL_STATEMENT_END--
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'SELECT'); -- no
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'INSERT'); -- no
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES IN SCHEMA testns GRANT SELECT ON TABLES TO public;
--DDL_STATEMENT_END--
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'SELECT'); -- no
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'INSERT'); -- no
--DDL_STATEMENT_BEGIN--
DROP TABLE testns.acltest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testns.acltest1 (x int);
--DDL_STATEMENT_END--
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'SELECT'); -- yes
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'INSERT'); -- no
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES IN SCHEMA testns GRANT INSERT ON TABLES TO regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testns.acltest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testns.acltest1 (x int);
--DDL_STATEMENT_END--
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'SELECT'); -- yes
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'INSERT'); -- yes
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES IN SCHEMA testns REVOKE INSERT ON TABLES FROM regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testns.acltest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testns.acltest1 (x int);
--DDL_STATEMENT_END--
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'SELECT'); -- yes
SELECT has_table_privilege('regress_priv_user1', 'testns.acltest1', 'INSERT'); -- no
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES FOR ROLE regress_priv_user1 REVOKE EXECUTE ON FUNCTIONS FROM public;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES IN SCHEMA testns GRANT USAGE ON SCHEMAS TO regress_priv_user2; -- error
--DDL_STATEMENT_END--
SET ROLE regress_priv_user1;
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION testns.foo() RETURNS int AS 'select 1' LANGUAGE sql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE testns.agg1(int) (sfunc = int4pl, stype = int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE testns.bar() AS 'select 1' LANGUAGE sql;
--DDL_STATEMENT_END--
SELECT has_function_privilege('regress_priv_user2', 'testns.foo()', 'EXECUTE'); -- no
SELECT has_function_privilege('regress_priv_user2', 'testns.agg1(int)', 'EXECUTE'); -- no
SELECT has_function_privilege('regress_priv_user2', 'testns.bar()', 'EXECUTE'); -- no
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES IN SCHEMA testns GRANT EXECUTE ON ROUTINES to public;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION testns.foo();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION testns.foo() RETURNS int AS 'select 1' LANGUAGE sql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP AGGREGATE testns.agg1(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE testns.agg1(int) (sfunc = int4pl, stype = int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PROCEDURE testns.bar();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE testns.bar() AS 'select 1' LANGUAGE sql;
--DDL_STATEMENT_END--
SELECT has_function_privilege('regress_priv_user2', 'testns.foo()', 'EXECUTE'); -- yes
SELECT has_function_privilege('regress_priv_user2', 'testns.agg1(int)', 'EXECUTE'); -- yes
SELECT has_function_privilege('regress_priv_user2', 'testns.bar()', 'EXECUTE'); -- yes (counts as function here)
--DDL_STATEMENT_BEGIN--
DROP FUNCTION testns.foo();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP AGGREGATE testns.agg1(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PROCEDURE testns.bar();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES FOR ROLE regress_priv_user1 REVOKE USAGE ON TYPES FROM public;
--DDL_STATEMENT_END--
RESET ROLE;

SELECT count(*)
  FROM pg_default_acl d LEFT JOIN pg_namespace n ON defaclnamespace = n.oid
  WHERE nspname = 'testns';
--DDL_STATEMENT_BEGIN--
DROP TABLE testns.acltest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA testns CASCADE;
--DDL_STATEMENT_END--
SELECT d.*     -- check that entries went away
  FROM pg_default_acl d LEFT JOIN pg_namespace n ON defaclnamespace = n.oid
  WHERE nspname IS NULL AND defaclnamespace != 0;


-- Grant on all objects of given type in a schema
\c -
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA testns;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testns.t1 (f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testns.t2 (f1 int);
--DDL_STATEMENT_END--

SELECT has_table_privilege('regress_priv_user1', 'testns.t1', 'SELECT'); -- false
--DDL_STATEMENT_BEGIN--
GRANT ALL ON ALL TABLES IN SCHEMA testns TO regress_priv_user1;
--DDL_STATEMENT_END--
SELECT has_table_privilege('regress_priv_user1', 'testns.t1', 'SELECT'); -- true
SELECT has_table_privilege('regress_priv_user1', 'testns.t2', 'SELECT'); -- true
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON ALL TABLES IN SCHEMA testns FROM regress_priv_user1;
--DDL_STATEMENT_END--
SELECT has_table_privilege('regress_priv_user1', 'testns.t1', 'SELECT'); -- false
SELECT has_table_privilege('regress_priv_user1', 'testns.t2', 'SELECT'); -- false
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION testns.priv_testfunc(int) RETURNS int AS 'select 3 * $1;' LANGUAGE sql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE testns.priv_testagg(int) (sfunc = int4pl, stype = int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE testns.priv_testproc(int) AS 'select 3' LANGUAGE sql;
--DDL_STATEMENT_END--

SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testfunc(int)', 'EXECUTE'); -- true by default
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testagg(int)', 'EXECUTE'); -- true by default
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testproc(int)', 'EXECUTE'); -- true by default
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA testns FROM PUBLIC;
--DDL_STATEMENT_END--
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testfunc(int)', 'EXECUTE'); -- false
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testagg(int)', 'EXECUTE'); -- false
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testproc(int)', 'EXECUTE'); -- still true, not a function
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON ALL PROCEDURES IN SCHEMA testns FROM PUBLIC;
--DDL_STATEMENT_END--
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testproc(int)', 'EXECUTE'); -- now false
--DDL_STATEMENT_BEGIN--
GRANT ALL ON ALL ROUTINES IN SCHEMA testns TO PUBLIC;
--DDL_STATEMENT_END--
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testfunc(int)', 'EXECUTE'); -- true
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testagg(int)', 'EXECUTE'); -- true
SELECT has_function_privilege('regress_priv_user1', 'testns.priv_testproc(int)', 'EXECUTE'); -- true

\set VERBOSITY terse \\ -- suppress cascade details
--DDL_STATEMENT_BEGIN--
drop table testns.t1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table testns.t2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA testns CASCADE;
--DDL_STATEMENT_END--
\set VERBOSITY default


-- Change owner of the schema & and rename of new schema owner
\c -
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_schemauser1 superuser login;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_schemauser2 superuser login;
--DDL_STATEMENT_END--

SET SESSION ROLE regress_schemauser1;
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA testns;
--DDL_STATEMENT_END--
SELECT nspname, rolname FROM pg_namespace, pg_roles WHERE pg_namespace.nspname = 'testns' AND pg_namespace.nspowner = pg_roles.oid;
--DDL_STATEMENT_BEGIN--
ALTER SCHEMA testns OWNER TO regress_schemauser2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_schemauser2 RENAME TO regress_schemauser_renamed;
--DDL_STATEMENT_END--
SELECT nspname, rolname FROM pg_namespace, pg_roles WHERE pg_namespace.nspname = 'testns' AND pg_namespace.nspowner = pg_roles.oid;

set session role regress_schemauser_renamed;
\set VERBOSITY terse \\ -- suppress cascade details
--DDL_STATEMENT_BEGIN--
DROP SCHEMA testns CASCADE;
--DDL_STATEMENT_END--
\set VERBOSITY default

-- clean up
\c -
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_schemauser1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table testns.t1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop schema testns;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_schemauser_renamed;
--DDL_STATEMENT_END--

-- test that dependent privileges are revoked (or not) properly
\c -
--DDL_STATEMENT_BEGIN--
set session role regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table dep_priv_test (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant select on dep_priv_test to regress_priv_user2 with grant option;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant select on dep_priv_test to regress_priv_user3 with grant option;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
set session role regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant select on dep_priv_test to regress_priv_user4 with grant option;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
set session role regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant select on dep_priv_test to regress_priv_user4 with grant option;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
set session role regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant select on dep_priv_test to regress_priv_user5;
--DDL_STATEMENT_END--
\dp dep_priv_test
--DDL_STATEMENT_BEGIN--
set session role regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
revoke select on dep_priv_test from regress_priv_user4 cascade;
--DDL_STATEMENT_END--
\dp dep_priv_test
--DDL_STATEMENT_BEGIN--
set session role regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
revoke select on dep_priv_test from regress_priv_user4 cascade;
--DDL_STATEMENT_END--
\dp dep_priv_test
--DDL_STATEMENT_BEGIN--
set session role regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table dep_priv_test;
--DDL_STATEMENT_END--

-- clean up

\c
--DDL_STATEMENT_BEGIN--
drop sequence x_seq;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP AGGREGATE priv_testagg1(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION priv_testfunc2(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION priv_testfunc4(boolean);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PROCEDURE priv_testproc1(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW atestv0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW atestv1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW atestv2;
--DDL_STATEMENT_END--
-- this should cascade to drop atestv4
--DDL_STATEMENT_BEGIN--
DROP VIEW atestv3 CASCADE;
--DDL_STATEMENT_END--
-- this should complain "does not exist"
--DDL_STATEMENT_BEGIN--
DROP VIEW atestv4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE atest1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE atest2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE atest3 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE atest4 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE atest5 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE atest6 cascade;
--DDL_STATEMENT_END--


SELECT lo_unlink(oid) FROM pg_largeobject_metadata WHERE oid >= 1000 AND oid < 3000 ORDER BY oid;
--DDL_STATEMENT_BEGIN--
DROP GROUP regress_priv_group1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP GROUP regress_priv_group2;
--DDL_STATEMENT_END--

-- these are needed to clean up permissions
--DDL_STATEMENT_BEGIN--
REVOKE USAGE ON LANGUAGE sql FROM regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table atest12 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop function leak(integer,integer) cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES FOR ROLE regress_priv_user1 grant EXECUTE ON FUNCTIONS to public;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES FOR ROLE regress_priv_user1 grant USAGE ON TYPES to public;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_priv_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_priv_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_priv_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_priv_user4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_priv_user5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_priv_user6;
--DDL_STATEMENT_END--


-- permissions with LOCK TABLE
--DDL_STATEMENT_BEGIN--
CREATE USER regress_locktable_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE lock_table (a int);
--DDL_STATEMENT_END--

-- LOCK TABLE and SELECT permission
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON lock_table TO regress_locktable_user;
SET SESSION AUTHORIZATION regress_locktable_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ROW EXCLUSIVE MODE; -- should fail
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS SHARE MODE; -- should pass
COMMIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS EXCLUSIVE MODE; -- should fail
ROLLBACK
--DDL_STATEMENT_END--
\c
--DDL_STATEMENT_BEGIN--
REVOKE SELECT ON lock_table FROM regress_locktable_user;
--DDL_STATEMENT_END--
-- LOCK TABLE and INSERT permission
--DDL_STATEMENT_BEGIN--
GRANT INSERT ON lock_table TO regress_locktable_user;
SET SESSION AUTHORIZATION regress_locktable_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ROW EXCLUSIVE MODE; -- should pass
COMMIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS SHARE MODE; -- should fail
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS EXCLUSIVE MODE; -- should fail
ROLLBACK;
--DDL_STATEMENT_END--
\c
--DDL_STATEMENT_BEGIN--
REVOKE INSERT ON lock_table FROM regress_locktable_user;
--DDL_STATEMENT_END--
-- LOCK TABLE and UPDATE permission
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON lock_table TO regress_locktable_user;
SET SESSION AUTHORIZATION regress_locktable_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ROW EXCLUSIVE MODE; -- should pass
COMMIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS SHARE MODE; -- should fail
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS EXCLUSIVE MODE; -- should pass
COMMIT;
--DDL_STATEMENT_END--
\c
--DDL_STATEMENT_BEGIN--
REVOKE UPDATE ON lock_table FROM regress_locktable_user;
--DDL_STATEMENT_END--
-- LOCK TABLE and DELETE permission
--DDL_STATEMENT_BEGIN--
GRANT DELETE ON lock_table TO regress_locktable_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_locktable_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ROW EXCLUSIVE MODE; -- should pass
COMMIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS SHARE MODE; -- should fail
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS EXCLUSIVE MODE; -- should pass
COMMIT;
--DDL_STATEMENT_END--
\c
--DDL_STATEMENT_BEGIN--
REVOKE DELETE ON lock_table FROM regress_locktable_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_locktable_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ROW EXCLUSIVE MODE; -- should pass
COMMIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS SHARE MODE; -- should fail
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_table IN ACCESS EXCLUSIVE MODE; -- should pass
COMMIT;
--DDL_STATEMENT_END--
\c

-- clean up
--DDL_STATEMENT_BEGIN--
DROP TABLE lock_table;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_locktable_user;
--DDL_STATEMENT_END--