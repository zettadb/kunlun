--
-- TRANSACTIONS
--

--DDL_STATEMENT_BEGIN--
drop table if exists xacttest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table xacttest (like aggtest);
--DDL_STATEMENT_END--
BEGIN;
INSERT INTO xacttest (a, b) VALUES (777, 777.777);
END;

-- should retrieve one value--
SELECT a FROM xacttest WHERE a > 100;

-- should have members again
SELECT * FROM aggtest;


-- Read-only tests
--DDL_STATEMENT_BEGIN--
drop table if exists writetest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE writetest (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMPORARY TABLE temptest (a int);
--DDL_STATEMENT_END--

BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE; -- ok
SELECT * FROM writetest; -- ok
SET TRANSACTION READ WRITE; --fail
COMMIT;

BEGIN;
SET TRANSACTION READ ONLY; -- ok
SET TRANSACTION READ WRITE; -- ok
SET TRANSACTION READ ONLY; -- ok
SELECT * FROM writetest; -- ok
SAVEPOINT x;
SET TRANSACTION READ ONLY; -- ok
SELECT * FROM writetest; -- ok
SET TRANSACTION READ ONLY; -- ok
SET TRANSACTION READ WRITE; --fail
COMMIT;

BEGIN;
SET TRANSACTION READ WRITE; -- ok
SAVEPOINT x;
SET TRANSACTION READ WRITE; -- ok
SET TRANSACTION READ ONLY; -- ok
SELECT * FROM writetest; -- ok
SET TRANSACTION READ ONLY; -- ok
SET TRANSACTION READ WRITE; --fail
COMMIT;

BEGIN;
SET TRANSACTION READ WRITE; -- ok
SAVEPOINT x;
SET TRANSACTION READ ONLY; -- ok
SELECT * FROM writetest; -- ok
ROLLBACK TO SAVEPOINT x;
SHOW transaction_read_only;  -- off
SAVEPOINT y;
SET TRANSACTION READ ONLY; -- ok
SELECT * FROM writetest; -- ok
-- RELEASE SAVEPOINT y;
SHOW transaction_read_only;  -- off
COMMIT;

SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;

--DDL_STATEMENT_BEGIN--
DROP TABLE writetest; -- fail
--DDL_STATEMENT_END--
INSERT INTO writetest VALUES (1); -- fail
SELECT * FROM writetest; -- ok
DELETE FROM temptest; -- ok
UPDATE temptest SET a = 0 FROM writetest WHERE temptest.a = 1 AND writetest.a = temptest.a; -- ok
PREPARE test AS UPDATE writetest SET a = 0; -- ok
EXECUTE test; -- fail
SELECT * FROM writetest, temptest; -- ok

SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;
--DDL_STATEMENT_BEGIN--
DROP TABLE writetest; -- ok
--DDL_STATEMENT_END--

-- Subtransactions, basic tests
-- create & drop tables
SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;
--DDL_STATEMENT_BEGIN--
drop table if exists trans_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists trans_baz;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists trans_barbaz;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE trans_foo (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE trans_baz (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE trans_barbaz (a int);
--DDL_STATEMENT_END--

-- should exist: trans_barbaz, trans_baz, trans_foo
SELECT * FROM trans_foo;		-- should be empty
SELECT * FROM trans_bar;		-- shouldn't exist
SELECT * FROM trans_barbaz;	-- should be empty
SELECT * FROM trans_baz;		-- should be empty

-- inserts
BEGIN;
	INSERT INTO trans_foo VALUES (1);
	SAVEPOINT one;
		INSERT into trans_bar VALUES (1);
	ROLLBACK TO one;
	-- RELEASE SAVEPOINT one;
	SAVEPOINT two;
		INSERT into trans_barbaz VALUES (1);
	-- RELEASE two;
	SAVEPOINT three;
		SAVEPOINT four;
			INSERT INTO trans_foo VALUES (2);
		-- RELEASE SAVEPOINT four;
	ROLLBACK TO SAVEPOINT three;
	-- RELEASE SAVEPOINT three;
	INSERT INTO trans_foo VALUES (3);
COMMIT;
SELECT * FROM trans_foo;		-- should have 1 and 3
SELECT * FROM trans_barbaz;	-- should have 1

--DDL_STATEMENT_BEGIN--
drop table if exists savepoints;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE savepoints (a int);
--DDL_STATEMENT_END--
-- test whole-tree commit
BEGIN;
	SAVEPOINT one;
		SELECT trans_foo;
	ROLLBACK TO SAVEPOINT one;
	-- RELEASE SAVEPOINT one;
		SAVEPOINT three;
			INSERT INTO savepoints VALUES (1);
			SAVEPOINT four;
				INSERT INTO savepoints VALUES (2);
				SAVEPOINT five;
					INSERT INTO savepoints VALUES (3);
				ROLLBACK TO SAVEPOINT five;
COMMIT;
COMMIT;		-- should not be in a transaction block
SELECT * FROM savepoints;

-- test whole-tree rollback
BEGIN;
	SAVEPOINT one;
		DELETE FROM savepoints WHERE a=1;
	-- RELEASE SAVEPOINT one;
	SAVEPOINT two;
		DELETE FROM savepoints WHERE a=1;
		SAVEPOINT three;
			DELETE FROM savepoints WHERE a=2;
ROLLBACK;
COMMIT;		-- should not be in a transaction block

SELECT * FROM savepoints;

-- test whole-tree commit on an aborted subtransaction
BEGIN;
	INSERT INTO savepoints VALUES (4);
	SAVEPOINT one;
		INSERT INTO savepoints VALUES (5);
		SELECT trans_foo;
COMMIT;
SELECT * FROM savepoints;

BEGIN;
	INSERT INTO savepoints VALUES (6);
	SAVEPOINT one;
		INSERT INTO savepoints VALUES (7);
	-- RELEASE SAVEPOINT one;
	INSERT INTO savepoints VALUES (8);
COMMIT;
-- rows 6 and 8 should have been created by the same xact
-- SELECT a.xmin = b.xmin FROM savepoints a, savepoints b WHERE a.a=6 AND b.a=8;
-- rows 6 and 7 should have been created by different xacts
-- SELECT a.xmin = b.xmin FROM savepoints a, savepoints b WHERE a.a=6 AND b.a=7;

BEGIN;
	INSERT INTO savepoints VALUES (9);
	SAVEPOINT one;
		INSERT INTO savepoints VALUES (10);
	ROLLBACK TO SAVEPOINT one;
		INSERT INTO savepoints VALUES (11);
COMMIT;
SELECT a FROM savepoints WHERE a in (9, 10, 11);
-- rows 9 and 11 should have been created by different xacts
-- SELECT a.xmin = b.xmin FROM savepoints a, savepoints b WHERE a.a=9 AND b.a=11;

BEGIN;
	INSERT INTO savepoints VALUES (12);
	SAVEPOINT one;
		INSERT INTO savepoints VALUES (13);
		SAVEPOINT two;
			INSERT INTO savepoints VALUES (14);
	ROLLBACK TO SAVEPOINT one;
		INSERT INTO savepoints VALUES (15);
		SAVEPOINT two;
			INSERT INTO savepoints VALUES (16);
			SAVEPOINT three;
				INSERT INTO savepoints VALUES (17);
COMMIT;
SELECT a FROM savepoints WHERE a BETWEEN 12 AND 17;

BEGIN;
	INSERT INTO savepoints VALUES (18);
	SAVEPOINT one;
		INSERT INTO savepoints VALUES (19);
		SAVEPOINT two;
			INSERT INTO savepoints VALUES (20);
	ROLLBACK TO SAVEPOINT one;
		INSERT INTO savepoints VALUES (21);
	ROLLBACK TO SAVEPOINT one;
		INSERT INTO savepoints VALUES (22);
COMMIT;
SELECT a FROM savepoints WHERE a BETWEEN 18 AND 22;

--DDL_STATEMENT_BEGIN--
DROP TABLE savepoints;
--DDL_STATEMENT_END--

-- only in a transaction block:
SAVEPOINT one;
ROLLBACK TO SAVEPOINT one;
-- RELEASE SAVEPOINT one;

-- Only "rollback to" allowed in aborted state
BEGIN;
  SAVEPOINT one;
  SELECT 0/0;
  SAVEPOINT two;    -- ignored till the end of ...
  -- RELEASE SAVEPOINT one;      -- ignored till the end of ...
  ROLLBACK TO SAVEPOINT one;
  SELECT 1;
COMMIT;
SELECT 1;			-- this should work

--
-- Check that "stable" functions are really stable.  They should not be
-- able to see the partial results of the calling query.  (Ideally we would
-- also check that they don't see commits of concurrent transactions, but
-- that's a mite hard to do within the limitations of pg_regress.)
--
select * from xacttest;

--DDL_STATEMENT_BEGIN--
create or replace function max_xacttest() returns smallint language sql as
'select max(a) from xacttest' stable;
--DDL_STATEMENT_END--

begin;
update xacttest set a = max_xacttest() + 10 where a > 0;
select * from xacttest;
rollback;

--DDL_STATEMENT_BEGIN--
-- But a volatile function can see the partial results of the calling query
create or replace function max_xacttest() returns smallint language sql as
'select max(a) from xacttest' volatile;
--DDL_STATEMENT_END--

-- not supported: begin;
--update xacttest set a = max_xacttest() + 10 where a > 0;
--select * from xacttest;
--rollback;

--DDL_STATEMENT_BEGIN--
-- Now the same test with plpgsql (since it depends on SPI which is different)
create or replace function max_xacttest() returns smallint language plpgsql as
'begin return max(a) from xacttest; end' stable;
--DDL_STATEMENT_END--

--not support: begin;
--update xacttest set a = max_xacttest() + 10 where a > 0;
--select * from xacttest;
--rollback;

--DDL_STATEMENT_BEGIN--
create or replace function max_xacttest() returns smallint language plpgsql as
'begin return max(a) from xacttest; end' volatile;
--DDL_STATEMENT_END--

-- not support: begin;
--update xacttest set a = max_xacttest() + 10 where a > 0;
--select * from xacttest;
--rollback;

--DDL_STATEMENT_BEGIN--
DROP TABLE trans_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE trans_baz;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE trans_barbaz;
--DDL_STATEMENT_END--


-- test case for problems with revalidating an open relation during abort
--DDL_STATEMENT_BEGIN--
create function inverse(int) returns float8 as
$$
begin
  analyze revalidate_bug;
  return 1::float8/$1;
exception
  when division_by_zero then return 0;
end$$ language plpgsql volatile;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists revalidate_bug;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table revalidate_bug (c float8 unique);
--DDL_STATEMENT_END--
insert into revalidate_bug values (1);
-- insert into revalidate_bug values (inverse(0)); -- crash happens --

--DDL_STATEMENT_BEGIN--
drop table revalidate_bug;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop function inverse(int);
--DDL_STATEMENT_END--

-- Test for proper cleanup after a failure in a cursor portal
-- that was created in an outer subtransaction
CREATE FUNCTION invert(x float8) RETURNS float8 LANGUAGE plpgsql AS
$$ begin return 1/x; end $$;

CREATE FUNCTION create_temp_tab() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE new_table (f1 float8);
  -- case of interest is that we fail while holding an open
  -- relcache reference to new_table
  INSERT INTO new_table SELECT invert(0.0);
  RETURN 'foo';
END $$;

BEGIN;
DECLARE ok CURSOR FOR SELECT * FROM int8_tbl;
DECLARE ctt CURSOR FOR SELECT create_temp_tab();
FETCH ok;
SAVEPOINT s1;
FETCH ok;  -- should work
FETCH ctt; -- error occurs here
ROLLBACK TO s1;
FETCH ok;  -- should work
FETCH ctt; -- must be rejected
COMMIT;

DROP FUNCTION create_temp_tab();
DROP FUNCTION invert(x float8);

-- Test assorted behaviors around the implicit transaction block created
-- when multiple SQL commands are sent in a single Query message.  These
-- tests rely on the fact that psql will not break SQL commands apart at a
-- backslash-quoted semicolon, but will send them as one Query.

-- psql will show only the last result in a multi-statement Query
SELECT 1\; SELECT 2\; SELECT 3;

-- this implicitly commits:
insert into i_table values(1)\; select * from i_table;
-- 1/0 error will cause rolling back the whole implicit transaction
insert into i_table values(2)\; select * from i_table\; select 1/0;
select * from i_table;

rollback;  -- we are not in a transaction at this point

-- can use regular begin/commit/rollback within a single Query
begin\; insert into i_table values(3)\; commit;
rollback;  -- we are not in a transaction at this point
begin\; insert into i_table values(4)\; rollback;
rollback;  -- we are not in a transaction at this point

-- begin converts implicit transaction into a regular one that
-- can extend past the end of the Query
select 1\; begin\; insert into i_table values(5);
commit;
select 1\; begin\; insert into i_table values(6);
rollback;

-- commit in implicit-transaction state commits but issues a warning.
insert into i_table values(7)\; commit\; insert into i_table values(8)\; select 1/0;
-- similarly, rollback aborts but issues a warning.
insert into i_table values(9)\; rollback\; select 2;

select * from i_table;

rollback;  -- we are not in a transaction at this point

-- implicit transaction block is still a transaction block, for e.g. VACUUM
SELECT 1\;
SELECT 1\; COMMIT\;

-- we disallow savepoint-related commands in implicit-transaction state
SELECT 1\; SAVEPOINT sp;
SELECT 1\; COMMIT\; SAVEPOINT sp;
ROLLBACK TO SAVEPOINT sp\; SELECT 2;
SELECT 2\; SELECT 3;

-- but this is OK, because the BEGIN converts it to a regular xact
SELECT 1\; BEGIN\; SAVEPOINT sp\; ROLLBACK TO SAVEPOINT sp\; COMMIT;


-- Test for successful cleanup of an aborted transaction at session exit.
-- THIS MUST BE THE LAST TEST IN THIS FILE.

begin;
select 1/0;
rollback to X;

-- DO NOT ADD ANYTHING HERE.
