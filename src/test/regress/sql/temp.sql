--
-- TEMP
-- Test temp relations and indexes
--

-- test temp table/index masking

--DDL_STATEMENT_BEGIN--
CREATE TABLE temptest(col int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE INDEX i_temptest ON temptest(col);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE temptest(tcol int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE INDEX i_temptest ON temptest(tcol);
--DDL_STATEMENT_END--

SELECT * FROM temptest;

--DDL_STATEMENT_BEGIN--
DROP INDEX i_temptest;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP TABLE temptest;
--DDL_STATEMENT_END--

SELECT * FROM temptest;

--DDL_STATEMENT_BEGIN--
DROP INDEX i_temptest;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP TABLE temptest;
--DDL_STATEMENT_END--

-- test temp table selects

--DDL_STATEMENT_BEGIN--
CREATE TABLE temptest(col int);
--DDL_STATEMENT_END--

INSERT INTO temptest VALUES (1);

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE temptest(tcol float);
--DDL_STATEMENT_END--

INSERT INTO temptest VALUES (2.1);

SELECT * FROM temptest;

--DDL_STATEMENT_BEGIN--
DROP TABLE temptest;
--DDL_STATEMENT_END--

SELECT * FROM temptest;

--DDL_STATEMENT_BEGIN--
DROP TABLE temptest;
--DDL_STATEMENT_END--

-- test temp table deletion

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE temptest(col int);
--DDL_STATEMENT_END--
\c

SELECT * FROM temptest;

-- Test ON COMMIT DELETE ROWS

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE temptest(col int) ON COMMIT DELETE ROWS;
--DDL_STATEMENT_END--

BEGIN;
INSERT INTO temptest VALUES (1);
INSERT INTO temptest VALUES (2);

SELECT * FROM temptest;
COMMIT;

SELECT * FROM temptest;

--DDL_STATEMENT_BEGIN--
DROP TABLE temptest;
--DDL_STATEMENT_END--

-- Test ON COMMIT DROP

BEGIN;

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE temptest(col int) ON COMMIT DROP;
--DDL_STATEMENT_END--

INSERT INTO temptest VALUES (1);
INSERT INTO temptest VALUES (2);

SELECT * FROM temptest;
COMMIT;

SELECT * FROM temptest;

-- Test manipulation of temp schema's placement in search path

--DDL_STATEMENT_BEGIN--
create table public.whereami (f1 text);
--DDL_STATEMENT_END--
insert into public.whereami values ('public');

--DDL_STATEMENT_BEGIN--
create temp table whereami (f1 text);
--DDL_STATEMENT_END--
insert into whereami values ('temp');

--DDL_STATEMENT_BEGIN--
create function public.whoami() returns text
  as $$select 'public'::text$$ language sql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function pg_temp.whoami() returns text
  as $$select 'temp'::text$$ language sql;
--DDL_STATEMENT_END--

-- default should have pg_temp implicitly first, but only for tables
select * from whereami;
select whoami();

-- can list temp first explicitly, but it still doesn't affect functions
set search_path = pg_temp, public;
select * from whereami;
select whoami();

-- or put it last for security
set search_path = public, pg_temp;
select * from whereami;
select whoami();

-- you can invoke a temp function explicitly, though
select pg_temp.whoami();

--DDL_STATEMENT_BEGIN--
drop table public.whereami;
--DDL_STATEMENT_END--

reset search_path;

-- For partitioned temp tables, ON COMMIT actions ignore storage-less
-- partitioned tables.
begin;
--DDL_STATEMENT_BEGIN--
create temp table temp_parted_oncommit (a int)
  partition by list (a) on commit delete rows;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table temp_parted_oncommit_1
  partition of temp_parted_oncommit
  for values in (1) on commit delete rows;
--DDL_STATEMENT_END--
insert into temp_parted_oncommit values (1);
commit;
-- partitions are emptied by the previous commit
select * from temp_parted_oncommit;
--DDL_STATEMENT_BEGIN--
drop table temp_parted_oncommit;
--DDL_STATEMENT_END--

-- Check dependencies between ON COMMIT actions with a partitioned
-- table and its partitions.  Using ON COMMIT DROP on a parent removes
-- the whole set.
begin;
--DDL_STATEMENT_BEGIN--
create temp table temp_parted_oncommit_test (a int)
  partition by list (a) on commit drop;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table temp_parted_oncommit_test1
  partition of temp_parted_oncommit_test
  for values in (1) on commit delete rows;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table temp_parted_oncommit_test2
  partition of temp_parted_oncommit_test
  for values in (2) on commit drop;
--DDL_STATEMENT_END--
insert into temp_parted_oncommit_test values (1), (2);
commit;
-- no relations remain in this case.
select relname from pg_class where relname like 'temp_parted_oncommit_test%';
-- Using ON COMMIT DELETE on a partitioned table does not remove
-- all rows if partitions preserve their data.
begin;
--DDL_STATEMENT_BEGIN--
create temp table temp_parted_oncommit_test (a int)
  partition by list (a) on commit delete rows;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table temp_parted_oncommit_test1
  partition of temp_parted_oncommit_test
  for values in (1) on commit preserve rows;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table temp_parted_oncommit_test2
  partition of temp_parted_oncommit_test
  for values in (2) on commit drop;
--DDL_STATEMENT_END--
insert into temp_parted_oncommit_test values (1), (2);
commit;
-- Data from the remaining partition is still here as its rows are
-- preserved.
select * from temp_parted_oncommit_test;
-- two relations remain in this case.
select relname from pg_class where relname like 'temp_parted_oncommit_test%';
--DDL_STATEMENT_BEGIN--
drop table temp_parted_oncommit_test;
--DDL_STATEMENT_END--

-- Check dependencies between ON COMMIT actions with inheritance trees.
-- Using ON COMMIT DROP on a parent removes the whole set.
begin;
--DDL_STATEMENT_BEGIN--
create temp table temp_inh_oncommit_test (a int) on commit drop;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table temp_inh_oncommit_test1 ()
  inherits(temp_inh_oncommit_test) on commit delete rows;
--DDL_STATEMENT_END--
insert into temp_inh_oncommit_test1 values (1);
commit;
-- no relations remain in this case
select relname from pg_class where relname like 'temp_inh_oncommit_test%';
-- Data on the parent is removed, and the child goes away.
begin;
--DDL_STATEMENT_BEGIN--
create temp table temp_inh_oncommit_test (a int) on commit delete rows;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table temp_inh_oncommit_test1 ()
  inherits(temp_inh_oncommit_test) on commit drop;
--DDL_STATEMENT_END--
insert into temp_inh_oncommit_test1 values (1);
insert into temp_inh_oncommit_test values (1);
commit;
select * from temp_inh_oncommit_test;
-- one relation remains
select relname from pg_class where relname like 'temp_inh_oncommit_test%';
--DDL_STATEMENT_BEGIN--
drop table temp_inh_oncommit_test;
--DDL_STATEMENT_END--

-- Tests with two-phase commit
-- Transactions creating objects in a temporary namespace cannot be used
-- with two-phase commit.

-- These cases generate errors about temporary namespace.
-- Function creation
begin;
--DDL_STATEMENT_BEGIN--
create function pg_temp.twophase_func() returns void as
  $$ select '2pc_func'::text $$ language sql;
--DDL_STATEMENT_END--
prepare transaction 'twophase_func';
-- Function drop
--DDL_STATEMENT_BEGIN--
create function pg_temp.twophase_func() returns void as
  $$ select '2pc_func'::text $$ language sql;
--DDL_STATEMENT_END--
begin;
--DDL_STATEMENT_BEGIN--
drop function pg_temp.twophase_func();
--DDL_STATEMENT_END--
prepare transaction 'twophase_func';
-- Operator creation
begin;
--create operator pg_temp.@@ (leftarg = int4, rightarg = int4, procedure = int4mi);
prepare transaction 'twophase_operator';

-- These generate errors about temporary tables.
-- ERROR:  Kunlun-db: Statement 'CREATE TYPE' not support temporary object mixied normal object
begin;
--DDL_STATEMENT_BEGIN--
--create type pg_temp.twophase_type as (a int);
--DDL_STATEMENT_END--
--prepare transaction 'twophase_type';
--DDL_STATEMENT_BEGIN--
create view pg_temp.twophase_view as select 1;
--DDL_STATEMENT_END--
prepare transaction 'twophase_view';
begin;
--DDL_STATEMENT_BEGIN--
create sequence pg_temp.twophase_seq;
--DDL_STATEMENT_END--
prepare transaction 'twophase_sequence';

-- Temporary tables cannot be used with two-phase commit.
--DDL_STATEMENT_BEGIN--
create temp table twophase_tab (a int);
--DDL_STATEMENT_END--
begin;
select a from twophase_tab;
prepare transaction 'twophase_tab';
begin;
insert into twophase_tab values (1);
prepare transaction 'twophase_tab';
begin;
lock twophase_tab in access exclusive mode;
prepare transaction 'twophase_tab';
begin;
--DDL_STATEMENT_BEGIN--
drop table twophase_tab;
--DDL_STATEMENT_END--
prepare transaction 'twophase_tab';

-- Corner case: current_schema may create a temporary schema if namespace
-- creation is pending, so check after that.  First reset the connection
-- to remove the temporary namespace, and make sure that non-parallel plans
-- are used.
\c -
SET max_parallel_workers = 0;
SET max_parallel_workers_per_gather = 0;
SET search_path TO 'pg_temp';
BEGIN;
SELECT current_schema() ~ 'pg_temp' AS is_temp_schema;
PREPARE TRANSACTION 'twophase_search';
