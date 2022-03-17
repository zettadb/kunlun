--
-- Tests to exercise the plan caching/invalidation mechanism
--

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE pcachetest(like int8_tbl);
--DDL_STATEMENT_END--
insert into pcachetest SELECT * FROM int8_tbl;

-- create and use a cached plan
PREPARE prepstmt AS SELECT * FROM pcachetest;

EXECUTE prepstmt;

-- and one with parameters
PREPARE prepstmt2(bigint) AS SELECT * FROM pcachetest WHERE q1 = $1;

EXECUTE prepstmt2(123);

-- invalidate the plans and see what happens
--DDL_STATEMENT_BEGIN--
DROP TABLE pcachetest;
--DDL_STATEMENT_END--

EXECUTE prepstmt;
EXECUTE prepstmt2(123);

-- recreate the temp table (this demonstrates that the raw plan is
-- purely textual and doesn't depend on OIDs, for instance)
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE pcachetest(like int8_tbl);
--DDL_STATEMENT_END--
insert into pcachetest SELECT * FROM int8_tbl ORDER BY 2;

EXECUTE prepstmt;
EXECUTE prepstmt2(123);

-- prepared statements should prevent change in output tupdesc,
-- since clients probably aren't expecting that to change on the fly
--DDL_STATEMENT_BEGIN--
ALTER TABLE pcachetest ADD COLUMN q3 bigint;
--DDL_STATEMENT_END--
EXECUTE prepstmt;
EXECUTE prepstmt2(123);

-- but we're nice guys and will let you undo your mistake
--DDL_STATEMENT_BEGIN--
ALTER TABLE pcachetest DROP COLUMN q3;
--DDL_STATEMENT_END--

EXECUTE prepstmt;
EXECUTE prepstmt2(123);

-- Try it with a view, which isn't directly used in the resulting plan
-- but should trigger invalidation anyway
--DDL_STATEMENT_BEGIN--
CREATE TEMP VIEW pcacheview AS
  SELECT * FROM pcachetest;
--DDL_STATEMENT_END--

PREPARE vprep AS SELECT * FROM pcacheview;

EXECUTE vprep;

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE TEMP VIEW pcacheview AS
  SELECT q1, q2/2 AS q2 FROM pcachetest;
--DDL_STATEMENT_END--

EXECUTE vprep;

-- Check basic SPI plan invalidation

--DDL_STATEMENT_BEGIN--
drop function if exists cache_test(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function cache_test(int) returns int as $$
declare total int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
begin
	create temp table t1(f1 int);
	insert into t1 values($1);
	insert into t1 values(11);
	insert into t1 values(12);
	insert into t1 values(13);
	select sum(f1) into total from t1;
	drop table t1;
	return total;
end
$$ language plpgsql;
--DDL_STATEMENT_END--

select cache_test(1);
select cache_test(2);
select cache_test(3);

-- Check invalidation of plpgsql "simple expression"

--DDL_STATEMENT_BEGIN--
create temp view v1 as
  select 2+2 as f1;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop function if exists cache_test_2();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function cache_test_2() returns int as $$
begin
	return f1 from v1;
end$$ language plpgsql;
--DDL_STATEMENT_END--

select cache_test_2();

--DDL_STATEMENT_BEGIN--
create or replace temp view v1 as
  select 2+2+4 as f1;
--DDL_STATEMENT_END--
select cache_test_2();

--DDL_STATEMENT_BEGIN--
create or replace temp view v1 as
  select 2+2+4+(select max(unique1) from tenk1) as f1;
--DDL_STATEMENT_END--
select cache_test_2();

--- Check that change of search_path is honored when re-using cached plan

--DDL_STATEMENT_BEGIN--
create schema s1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table s1.abc (f1 int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create schema s2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table s2.abc (f1 int);
--DDL_STATEMENT_END--

insert into s1.abc values(123);
insert into s2.abc values(456);

set search_path = s1;

prepare p1 as select f1 from abc;

execute p1;

set search_path = s2;

select f1 from abc;

execute p1;

--DDL_STATEMENT_BEGIN--
alter table s1.abc add column f2 float8;   -- force replan
--DDL_STATEMENT_END--

execute p1;

reset search_path;

--DDL_STATEMENT_BEGIN--
drop table s1.abc cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop schema s1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table s2.abc cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop schema s2 cascade;
--DDL_STATEMENT_END--

-- Check that invalidation deals with regclass constants

--DDL_STATEMENT_BEGIN--
create temp sequence seq;
--DDL_STATEMENT_END--

prepare p2 as select nextval('seq');

execute p2;

--DDL_STATEMENT_BEGIN--
drop sequence seq;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create temp sequence seq;
--DDL_STATEMENT_END--

execute p2;

-- Check DDL via SPI, immediately followed by SPI plan re-use
-- (bug in original coding)

--DDL_STATEMENT_BEGIN--
drop function if exists cachebug();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function cachebug() returns void as $$
declare r int;
--DDL_STATEMENT_END--
begin
--DDL_STATEMENT_BEGIN--
  drop table if exists temptable cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
  create temp table temptable(f1 int);
--DDL_STATEMENT_END--
  insert into temptable select * from generate_series(1,3) as f1;
--DDL_STATEMENT_BEGIN--
  create temp view vv as select * from temptable;
--DDL_STATEMENT_END--
  for r in select * from vv loop
    raise notice '%', r;
  end loop;
end$$ language plpgsql;

select cachebug();
select cachebug();

-- Check that addition or removal of any partition is correctly dealt with by
-- default partition table when it is being used in prepared statement.
--DDL_STATEMENT_BEGIN--
create table pc_list_parted (a int) partition by list(a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table pc_list_part_null partition of pc_list_parted for values in (null);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table pc_list_part_1 partition of pc_list_parted for values in (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table pc_list_part_2 partition of pc_list_parted for values in (2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table pc_list_parted detach partition pc_list_part_null;
--DDL_STATEMENT_END--
-- should be ok
--DDL_STATEMENT_BEGIN--
drop table pc_list_part_1;
--DDL_STATEMENT_END--
-- should be ok
--DDL_STATEMENT_BEGIN--
drop table pc_list_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table pc_list_part_null;
--DDL_STATEMENT_END--