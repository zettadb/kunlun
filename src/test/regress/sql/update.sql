--
-- UPDATE syntax tests
--
--DDL_STATEMENT_BEGIN--
drop table if exists update_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE update_test (
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists upsert_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE upsert_test (
    a   INT PRIMARY KEY,
    b   TEXT
);
--DDL_STATEMENT_END--

INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test(b, a) VALUES (15, 10);

SELECT * FROM update_test;

UPDATE update_test SET a = DEFAULT, b = DEFAULT;

SELECT * FROM update_test;

-- aliases for the UPDATE target table
UPDATE update_test AS t SET b = 10 WHERE t.a = 10;

SELECT * FROM update_test;

UPDATE update_test t SET b = t.b + 10 WHERE t.a = 10;

SELECT * FROM update_test;

--
-- Test multiple-set-clause syntax
--

INSERT INTO update_test SELECT a,b+1,c FROM update_test;
SELECT * FROM update_test;

UPDATE update_test SET (c,b,a) = ('bugle', b+11, DEFAULT) WHERE c = 'foo';
SELECT * FROM update_test order by 1,2,3;
UPDATE update_test SET (c,b) = ('car', a+b), a = a + 1 WHERE a = 10;
SELECT * FROM update_test order by 1,2,3;
-- fail, multi assignment to same column:
UPDATE update_test SET (c,b) = ('car', a+b), b = a + 1 WHERE a = 10;

-- uncorrelated sub-select:
-- not supported: UPDATE update_test
--  SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
--  WHERE a = 100 AND b = 20;
--SELECT * FROM update_test order by 1,2,3;
-- correlated sub-select:
--UPDATE update_test o
--  SET (b,a) = (select a+1,b from update_test i
--               where i.a=o.a and i.b=o.b and i.c is not distinct from o.c);
--SELECT * FROM update_test order by 1,2,3;
-- fail, multiple rows supplied:
--UPDATE update_test SET (b,a) = (select a+1,b from update_test);
-- set to null if no rows supplied:
--UPDATE update_test SET (b,a) = (select a+1,b from update_test where a = 1000)
--  WHERE a = 11;
SELECT * FROM update_test order by 1,2,3;

-- if an alias for the target table is specified, don't allow references
-- to the original table name
UPDATE update_test AS t SET b = update_test.b + 10 WHERE t.a = 10;

-- Make sure that we can update to a TOASTed value.
UPDATE update_test SET c = repeat('x', 10000) WHERE c = 'car';
SELECT a, b, char_length(c) FROM update_test;

-- Check multi-assignment with a Result node to handle a one-time filter.
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE update_test t
  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
  WHERE CURRENT_USER = SESSION_USER;
--not support: UPDATE update_test t
--  SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a)
--  WHERE CURRENT_USER = SESSION_USER;
SELECT a, b, char_length(c) FROM update_test order by 1,2,3;

--DDL_STATEMENT_BEGIN--
DROP TABLE update_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE upsert_test;
--DDL_STATEMENT_END--


---------------------------
-- UPDATE with row movement
---------------------------

-- When a partitioned table receives an UPDATE to the partitioned key and the
-- new values no longer meet the partition's bound, the row must be moved to
-- the correct partition for the new partition key (if one exists). We must
-- also ensure that updatable views on partitioned tables properly enforce any
-- WITH CHECK OPTION that is defined. 

--DDL_STATEMENT_BEGIN--
drop table if exists range_parted cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE range_parted (
	a text,
	b bigint,
	c numeric,
	d int,
	e varchar
) PARTITION BY RANGE (a, b);
--DDL_STATEMENT_END--

-- Create partitions intentionally in descending bound order, so as to test
-- that update-row-movement works with the leaf partitions not in bound order.
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_b_20_b_30 PARTITION OF range_parted FOR VALUES FROM ('b', 20) TO ('b', 30);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_b_10_b_20 PARTITION OF range_parted FOR VALUES FROM ('b', 10) TO ('b', 20) PARTITION BY RANGE (c);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_b_1_b_10 PARTITION OF range_parted FOR VALUES FROM ('b', 1) TO ('b', 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_a_10_a_20 PARTITION OF range_parted FOR VALUES FROM ('a', 10) TO ('a', 20);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_a_1_a_10 PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('a', 10);
--DDL_STATEMENT_END--

-- Check that partition-key UPDATE works sanely on a partitioned table that
-- does not have any child partitions.
UPDATE part_b_10_b_20 set b = b - 6;

-- Create some more partitions following the above pattern of descending bound
-- order, but let's make the situation a bit more complex by having the
-- attribute numbers of the columns vary from their parent partition.
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_c_100_200 PARTITION OF part_b_10_b_20 FOR VALUES FROM (100) TO (200) PARTITION BY range (abs(d));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_d_1_15 PARTITION OF part_c_100_200 FOR VALUES FROM (1) TO (15);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_d_15_20 PARTITION OF part_c_100_200 FOR VALUES FROM (15) TO (20);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE part_c_1_100 PARTITION OF part_b_10_b_20 FOR VALUES FROM (1) TO (100);
--DDL_STATEMENT_END--

\set init_range_parted 'delete from range_parted; insert into range_parted VALUES (''a'', 1, 1, 1), (''a'', 10, 200, 1), (''b'', 12, 96, 1), (''b'', 13, 97, 2), (''b'', 15, 105, 16), (''b'', 17, 105, 19)'
\set show_data 'select * from range_parted ORDER BY 1, 2, 3, 4, 5, 6'
:init_range_parted;
:show_data;

-- The order of subplans should be in bound order
-- ERROR:  Can not update partition key of a remote relation.
--EXPLAIN (costs off) UPDATE range_parted set c = c - 50 WHERE c > 97;

-- fail, row movement happens only within the partition subtree.
--UPDATE part_c_100_200 set c = c - 20, d = c WHERE c = 105;
-- fail, no partition key update, so no attempt to move tuple,
-- but "a = 'a'" violates partition constraint enforced by root partition)
UPDATE part_b_10_b_20 set a = 'a';
-- ok, partition key update, no constraint violation
-- Can not update partition key of a remote relation.
--UPDATE range_parted set d = d - 10 WHERE d > 10;
-- ok, no partition key update, no constraint violation
UPDATE range_parted set e = d;
-- No row found
UPDATE part_c_1_100 set c = c + 20 WHERE c = 98;
-- ok, row movement
-- Can not update partition key of a remote relation.
--UPDATE part_b_10_b_20 set c = c + 20 returning c, b, a;
:show_data;

-- fail, row movement happens only within the partition subtree.
--Crash due to unsupported functionality
--UPDATE part_b_10_b_20 set b = b - 6 WHERE c > 116 returning *;
-- ok, row movement, with subset of rows moved into different partition.
-- Can not update partition key of a remote relation.
--UPDATE range_parted set b = b - 6 WHERE c > 116 returning a, b + c;
--:show_data;

-- Common table needed for multiple test scenarios.
--DDL_STATEMENT_BEGIN--
drop table if exists mintab;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE mintab(c1 int);
--DDL_STATEMENT_END--
INSERT into mintab VALUES (120);

-- update partition key using updatable view.
-- with check option is not support, so 'with check option' is removed from the create view statement.
--DDL_STATEMENT_BEGIN--
CREATE VIEW upview AS SELECT * FROM range_parted WHERE (select c > c1 FROM mintab);
--DDL_STATEMENT_END--
-- ok
--UPDATE upview set c = 199 WHERE b = 4;
-- fail, check option violation
--UPDATE upview set c = 120 WHERE b = 4;
-- fail, row movement with check option violation
--UPDATE upview set a = 'b', b = 15, c = 120 WHERE b = 4;
-- ok, row movement, check option passes
--UPDATE upview set a = 'b', b = 15 WHERE b = 4;

:show_data;

-- cleanup
--DDL_STATEMENT_BEGIN--
DROP VIEW upview;
--DDL_STATEMENT_END--

-- RETURNING having whole-row vars.
:init_range_parted;
--UPDATE range_parted set c = 95 WHERE a = 'b' and b > 10 and c > 100 returning (range_parted), *;
--:show_data;


-- Transition tables with update row movement
:init_range_parted;

--UPDATE range_parted set c = (case when c = 96 then 110 else c + 1 end ) WHERE a = 'b' and b > 10 and c >= 96;
--:show_data;
:init_range_parted;

--UPDATE range_parted set c = c + 50 WHERE a = 'b' and b > 10 and c >= 96;
--:show_data;
-- Don't drop trans_updatetrig yet. It is required below.

:init_range_parted;
--UPDATE range_parted set c = (case when c = 96 then 110 else c + 1 end) WHERE a = 'b' and b > 10 and c >= 96;
--:show_data;
:init_range_parted;
--UPDATE range_parted set c = c + 50 WHERE a = 'b' and b > 10 and c >= 96;
--:show_data;

-- Case where per-partition tuple conversion map array is allocated, but the
-- map is not required for the particular tuple that is routed, thanks to
-- matching table attributes of the partition and the target table.
:init_range_parted;
--UPDATE range_parted set b = 15 WHERE b = 1;
--:show_data;

-- RLS policies with update-row-movement
-----------------------------------------

--not support: ALTER TABLE range_parted ENABLE ROW LEVEL SECURITY;
--DDL_STATEMENT_BEGIN--
CREATE USER regress_range_parted_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON range_parted, mintab TO regress_range_parted_user;
--DDL_STATEMENT_END--
-- not support: CREATE POLICY seeall ON range_parted AS PERMISSIVE FOR SELECT USING (true);
-- not support: CREATE POLICY policy_range_parted ON range_parted for UPDATE USING (true) WITH CHECK (c % 2 = 0);

:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
-- This should fail with RLS violation error while moving row from
-- part_a_10_a_20 to part_d_1_15, because we are setting 'c' to an odd number.
--UPDATE range_parted set a = 'b', c = 151 WHERE a = 'a' and c = 200;

RESET SESSION AUTHORIZATION;

:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
--UPDATE range_parted set a = 'b', c = 151 WHERE a = 'a' and c = 200;
RESET SESSION AUTHORIZATION;

:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
--UPDATE range_parted set a = 'b', c = 150 WHERE a = 'a' and c = 200;

-- Cleanup
RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
DROP FUNCTION func_d_1_15();
--DDL_STATEMENT_END--

-- Policy expression contains SubPlan
RESET SESSION AUTHORIZATION;
:init_range_parted;
--CREATE POLICY policy_range_parted_subplan on range_parted
--    AS RESTRICTIVE for UPDATE USING (true)
--    WITH CHECK ((SELECT range_parted.c <= c1 FROM mintab));
SET SESSION AUTHORIZATION regress_range_parted_user;
-- fail, mintab has row with c1 = 120
--UPDATE range_parted set a = 'b', c = 122 WHERE a = 'a' and c = 200;
-- ok
--UPDATE range_parted set a = 'b', c = 120 WHERE a = 'a' and c = 200;

-- RLS policy expression contains whole row.

RESET SESSION AUTHORIZATION;
:init_range_parted;
--CREATE POLICY policy_range_parted_wholerow on range_parted AS RESTRICTIVE for UPDATE USING (true)
--   WITH CHECK (range_parted = row('b', 10, 112, 1, NULL)::range_parted);
SET SESSION AUTHORIZATION regress_range_parted_user;
-- ok, should pass the RLS check
--UPDATE range_parted set a = 'b', c = 112 WHERE a = 'a' and c = 200;
RESET SESSION AUTHORIZATION;
:init_range_parted;
SET SESSION AUTHORIZATION regress_range_parted_user;
-- fail, the whole row RLS check should fail
--UPDATE range_parted set a = 'b', c = 116 WHERE a = 'a' and c = 200;

-- Cleanup
RESET SESSION AUTHORIZATION;
--DROP POLICY policy_range_parted ON range_parted;
--DROP POLICY policy_range_parted_subplan ON range_parted;
--DROP POLICY policy_range_parted_wholerow ON range_parted;
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON range_parted, mintab FROM regress_range_parted_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_range_parted_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE mintab;
--DDL_STATEMENT_END--


:init_range_parted;

--UPDATE range_parted set c = c - 50 WHERE c > 97;
--:show_data;

-- Creating default partition for range
:init_range_parted;
\d+ part_def
insert into range_parted values ('c', 9);
-- ok
update part_def set a = 'd' where a = 'c';
-- fail
update part_def set a = 'a' where a = 'd';

:show_data;

-- Update row movement from non-default to default partition.
-- fail, default partition is not under part_a_10_a_20;
UPDATE part_a_10_a_20 set a = 'ad' WHERE a = 'a';
-- ok
--UPDATE range_parted set a = 'ad' WHERE a = 'a';
--UPDATE range_parted set a = 'bd' WHERE a = 'b';
:show_data;
-- Update row movement from default to non-default partitions.
-- ok
--UPDATE range_parted set a = 'a' WHERE a = 'ad';
--UPDATE range_parted set a = 'b' WHERE a = 'bd';
:show_data;

-- Cleanup: range_parted no longer needed.
--DDL_STATEMENT_BEGIN--
DROP TABLE range_parted;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists list_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE list_parted (
	a text,
	b int
) PARTITION BY list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE list_part1  PARTITION OF list_parted for VALUES in ('a', 'b');
--DDL_STATEMENT_END--
INSERT into list_part1 VALUES ('a', 1);
--DDL_STATEMENT_BEGIN--
DROP TABLE list_parted;
--DDL_STATEMENT_END--

--------------
-- Some more update-partition-key test scenarios below. This time use list
-- partitions.
--------------

-- Setup for list partitions
--DDL_STATEMENT_BEGIN--
CREATE TABLE list_parted (a numeric, b int, c int8) PARTITION BY list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE sub_parted PARTITION OF list_parted for VALUES in (1) PARTITION BY list (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE sub_part1 PARTITION OF sub_parted for VALUES in (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE sub_part2 PARTITION OF sub_parted for VALUES in (2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE list_part1 PARTITION OF list_parted for VALUES in (2,3);;
--DDL_STATEMENT_END--

INSERT into list_parted VALUES (2,5,50);
INSERT into list_parted VALUES (3,6,60);
INSERT into sub_parted VALUES (1,1,60);
INSERT into sub_parted VALUES (1,2,10);

-- Test partition constraint violation when intermediate ancestor is used and
-- constraint is inherited from upper root.
UPDATE sub_parted set a = 2 WHERE c = 10;

-- Test update-partition-key, where the unpruned partitions do not have their
-- partition keys updated.
SELECT * FROM list_parted WHERE a = 2 ORDER BY 1;
UPDATE list_parted set b = c + a WHERE a = 2;
SELECT * FROM list_parted WHERE a = 2 ORDER BY 1;

SELECT * FROM list_parted ORDER BY 1, 2, 3;

UPDATE list_parted set c = 70 WHERE b  = 1;
SELECT * FROM list_parted ORDER BY 1, 2, 3;

--UPDATE list_parted set b = 1 WHERE c = 70;
SELECT * FROM list_parted ORDER BY 1, 2, 3;
--UPDATE list_parted set b = 1 WHERE c = 70;
SELECT * FROM list_parted ORDER BY 1, 2, 3;

-- UPDATE partition-key with FROM clause. If join produces multiple output
-- rows for the same row to be modified, we should tuple-route the row only
-- once. There should not be any rows inserted.
--DDL_STATEMENT_BEGIN--
drop table if exists non_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE non_parted (id int);
--DDL_STATEMENT_END--
INSERT into non_parted VALUES (1), (1), (1), (2), (2), (2), (3), (3), (3);
-- not supported: UPDATE list_parted t1 set a = 2 FROM non_parted t2 WHERE t1.a = t2.id and a = 1;
SELECT * FROM list_parted ORDER BY 1, 2, 3;
--DDL_STATEMENT_BEGIN--
DROP TABLE non_parted;
--DDL_STATEMENT_END--

-- Cleanup: list_parted no longer needed.
--DDL_STATEMENT_BEGIN--
DROP TABLE list_parted;
--DDL_STATEMENT_END--
-- create custom operator class and hash function, for the same reason
-- explained in alter_table.sql
--DDL_STATEMENT_BEGIN--
create or replace function dummy_hashint4(a int4, seed int8) returns int8 as
$$ begin return (a + seed); end; $$ language 'plpgsql' immutable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator class custom_opclass for type int4 using hash as
operator 1 = , function 2 dummy_hashint4(int4, int8);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists hash_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table hash_parted (
	a int,
	b int
) partition by hash (a custom_opclass, b custom_opclass);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table hpart1 partition of hash_parted for values with (modulus 2, remainder 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table hpart2 partition of hash_parted for values with (modulus 4, remainder 2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table hpart3 partition of hash_parted for values with (modulus 8, remainder 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table hpart4 partition of hash_parted for values with (modulus 8, remainder 4);
--DDL_STATEMENT_END--
insert into hpart1 values (1, 1);
insert into hpart2 values (2, 5);
insert into hpart4 values (3, 4);

-- fail
update hpart1 set a = 3, b=4 where a = 1;
-- ok, row movement
update hash_parted set b = b - 1 where b = 1;
-- ok
update hash_parted set b = b + 8 where b = 1;

-- cleanup
--DDL_STATEMENT_BEGIN--
drop table hash_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop operator class custom_opclass using hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop function dummy_hashint4(a int4, seed int8);
--DDL_STATEMENT_END--