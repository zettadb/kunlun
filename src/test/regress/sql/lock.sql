--
-- Test the LOCK statement
--

-- Setup
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA lock_schema1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET search_path = lock_schema1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE lock_tbl1 (a BIGINT);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE lock_tbl1a (a BIGINT);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW lock_view1 AS SELECT * FROM lock_tbl1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW lock_view2(a,b) AS SELECT * FROM lock_tbl1, lock_tbl1a;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW lock_view3 AS SELECT * from lock_view2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW lock_view4 AS SELECT (select a from lock_tbl1a limit 1) from lock_tbl1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW lock_view5 AS SELECT * from lock_tbl1 where a in (select * from lock_tbl1a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW lock_view6 AS SELECT * from (select * from lock_tbl1) sub;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_rol_lock1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_rol_lock1 SET search_path = lock_schema1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON SCHEMA lock_schema1 TO regress_rol_lock1;
--DDL_STATEMENT_END--

-- Try all valid lock options; also try omitting the optional TABLE keyword.
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN ACCESS SHARE MODE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK lock_tbl1 IN ROW SHARE MODE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN ROW EXCLUSIVE MODE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN SHARE UPDATE EXCLUSIVE MODE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN SHARE MODE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK lock_tbl1 IN SHARE ROW EXCLUSIVE MODE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN EXCLUSIVE MODE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN ACCESS EXCLUSIVE MODE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--

-- Try using NOWAIT along with valid options.
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN ACCESS SHARE MODE NOWAIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN ROW SHARE MODE NOWAIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN ROW EXCLUSIVE MODE NOWAIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN SHARE MODE NOWAIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN SHARE ROW EXCLUSIVE MODE NOWAIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN EXCLUSIVE MODE NOWAIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
LOCK TABLE lock_tbl1 IN ACCESS EXCLUSIVE MODE NOWAIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--

-- Verify that we can lock views.
BEGIN TRANSACTION;
LOCK TABLE lock_view1 IN EXCLUSIVE MODE;
-- lock_view1 and lock_tbl1 are locked.
select relname from pg_locks l, pg_class c
 where l.relation = c.oid and relname like '%lock_%' and mode = 'ExclusiveLock'
 order by relname;
ROLLBACK;
BEGIN TRANSACTION;
LOCK TABLE lock_view2 IN EXCLUSIVE MODE;
-- lock_view1, lock_tbl1, and lock_tbl1a are locked.
select relname from pg_locks l, pg_class c
 where l.relation = c.oid and relname like '%lock_%' and mode = 'ExclusiveLock'
 order by relname;
ROLLBACK;
BEGIN TRANSACTION;
LOCK TABLE lock_view3 IN EXCLUSIVE MODE;
-- lock_view3, lock_view2, lock_tbl1, and lock_tbl1a are locked recursively.
select relname from pg_locks l, pg_class c
 where l.relation = c.oid and relname like '%lock_%' and mode = 'ExclusiveLock'
 order by relname;
ROLLBACK;
BEGIN TRANSACTION;
LOCK TABLE lock_view4 IN EXCLUSIVE MODE;
-- lock_view4, lock_tbl1, and lock_tbl1a are locked.
select relname from pg_locks l, pg_class c
 where l.relation = c.oid and relname like '%lock_%' and mode = 'ExclusiveLock'
 order by relname;
ROLLBACK;
BEGIN TRANSACTION;
LOCK TABLE lock_view5 IN EXCLUSIVE MODE;
-- lock_view5, lock_tbl1, and lock_tbl1a are locked.
select relname from pg_locks l, pg_class c
 where l.relation = c.oid and relname like '%lock_%' and mode = 'ExclusiveLock'
 order by relname;
ROLLBACK;
BEGIN TRANSACTION;
LOCK TABLE lock_view6 IN EXCLUSIVE MODE;
-- lock_view6 an lock_tbl1 are locked.
select relname from pg_locks l, pg_class c
 where l.relation = c.oid and relname like '%lock_%' and mode = 'ExclusiveLock'
 order by relname;
ROLLBACK;
-- detecting infinite recursions in view definitions
--CREATE OR REPLACE VIEW lock_view2 AS SELECT * from lock_view3;
--BEGIN TRANSACTION;
--LOCK TABLE lock_view2 IN EXCLUSIVE MODE;
--ROLLBACK;
--CREATE VIEW lock_view7 AS SELECT * from lock_view2;
--BEGIN TRANSACTION;
--LOCK TABLE lock_view7 IN EXCLUSIVE MODE;
--ROLLBACK;

-- Verify that we can lock a table with inheritance children.
--DDL_STATEMENT_BEGIN--
CREATE TABLE lock_tbl2 (b BIGINT) INHERITS (lock_tbl1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE lock_tbl3 () INHERITS (lock_tbl2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
LOCK TABLE lock_tbl1 * IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

--DDL_STATEMENT_END--
-- Verify that we can't lock a child table just because we have permission
-- on the parent, but that we can lock the parent only.
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON TABLE lock_tbl1 TO regress_rol_lock1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET ROLE regress_rol_lock1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE lock_tbl1 * IN ACCESS EXCLUSIVE MODE;
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
LOCK TABLE ONLY lock_tbl1;
ROLLBACK;
--DDL_STATEMENT_END--
RESET ROLE;

--
-- Clean up
--
--DDL_STATEMENT_BEGIN--
DROP VIEW lock_view7;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW lock_view6;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW lock_view5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW lock_view4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW lock_view3 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW lock_view1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE lock_tbl3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE lock_tbl2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE lock_tbl1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE lock_tbl1a cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA lock_schema1 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_rol_lock1;
--DDL_STATEMENT_END--


-- atomic ops tests
RESET search_path;
SELECT test_atomic_ops();
