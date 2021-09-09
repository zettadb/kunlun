--
-- insert...on conflict do unique index inference
--
drop table if exists insertconflicttest;
create table insertconflicttest(key1 int4, fruit text);

--
-- Test unique index inference with operator class specifications and
-- named collations
--
-- fails
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (key1) do nothing;
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (fruit) do nothing;

-- succeeds
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (key1, fruit) do nothing;
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (fruit, key1, fruit, key1) do nothing;
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (lower(fruit), key1, lower(fruit), key1) do nothing;
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (key1, fruit) do update set fruit = excluded.fruit
  where exists (select 1 from insertconflicttest ii where ii.key1 = excluded.key1);
-- Neither collation nor operator class specifications are required --
-- supplying them merely *limits* matches to indexes with matching opclasses
-- used for relevant indexes
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (key1, fruit ) do nothing;
-- does not appear.
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (key1, fruit) do nothing;
-- Okay, but only accepts the single index where both opclass and collation are
-- specified
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (fruit, key1) do nothing;
-- Okay, but only accepts the single index where both opclass and collation are
-- specified (plus expression variant)
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (lower(fruit), key1, key1) do nothing;
-- Attribute appears twice, while not all attributes/expressions on attributes
-- appearing within index definition match in terms of both opclass and
-- collation.
--
-- Works because every attribute in inference specification needs to be
-- satisfied once or more by cataloged index attribute, and as always when an
-- attribute in the cataloged definition has a non-default opclass/collation,
-- it still satisfied some inference attribute lacking any particular
-- opclass/collation specification.
--
-- The implementation is liberal in accepting inference specifications on the
-- assumption that multiple inferred unique indexes will prevent problematic
-- cases.  It rolls with unique indexes where attributes redundantly appear
-- multiple times, too (which is not tested here).
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (fruit, key1, fruit , key1) do nothing;
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (lower(fruit), key1, key1) do nothing;


-- fails:
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (lower(fruit) , upper(fruit)) do nothing;
-- works:
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (lower(fruit), upper(fruit) ) do nothing;

--
-- Single key tests
--
create unique index key_index on insertconflicttest(key1);

--
-- Explain tests
--
explain (costs off) insert into insertconflicttest values (0, 'Bilberry') on conflict (key1) do update set fruit = excluded.fruit;
-- Should display qual actually attributable to internal sequential scan:
explain (costs off) insert into insertconflicttest values (0, 'Bilberry') on conflict (key1) do update set fruit = excluded.fruit where insertconflicttest.fruit != 'Cawesh';
-- With EXCLUDED.* expression in scan node:
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (key1) do update set fruit = excluded.fruit where excluded.fruit != 'Elderberry';
-- Does the same, but JSON format shows "Conflict Arbiter Index" as JSON array:
explain (costs off, format json) insert into insertconflicttest values (0, 'Bilberry') on conflict (key1) do update set fruit = excluded.fruit where insertconflicttest.fruit != 'Lime' returning *;

-- Fails (no unique index inference specification, required for do update variant):
insert into insertconflicttest values (1, 'Apple') on conflict do update set fruit = excluded.fruit;

-- inference succeeds:
insert into insertconflicttest values (1, 'Apple') on conflict (key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (2, 'Orange') on conflict (key1, key1, key1) do update set fruit = excluded.fruit;

-- Succeed, since multi-assignment does not involve subquery:
insert into insertconflicttest
values (1, 'Apple'), (2, 'Orange')
on conflict (key1) do update set (fruit, key1) = (excluded.fruit, excluded.key1);

-- Give good diagnostic message when EXCLUDED.* spuriously referenced from
-- RETURNING:
insert into insertconflicttest values (1, 'Apple') on conflict (key1) do update set fruit = excluded.fruit RETURNING excluded.fruit;

-- Only suggest <table>.* column when inference element misspelled:
insert into insertconflicttest values (1, 'Apple') on conflict (keyy1) do update set fruit = excluded.fruit;

-- Have useful HINT for EXCLUDED.* RTE within UPDATE:
insert into insertconflicttest values (1, 'Apple') on conflict (key1) do update set fruit = excluded.fruitt;

-- inference fails:
insert into insertconflicttest values (3, 'Kiwi') on conflict (key1, fruit) do update set fruit = excluded.fruit;
insert into insertconflicttest values (4, 'Mango') on conflict (fruit, key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (5, 'Lemon') on conflict (fruit) do update set fruit = excluded.fruit;
insert into insertconflicttest values (6, 'Passionfruit') on conflict (lower(fruit)) do update set fruit = excluded.fruit;

-- Check the target relation can be aliased
insert into insertconflicttest AS ict values (6, 'Passionfruit') on conflict (key1) do update set fruit = excluded.fruit; -- ok, no reference to target table
insert into insertconflicttest AS ict values (6, 'Passionfruit') on conflict (key1) do update set fruit = ict.fruit; -- ok, alias
insert into insertconflicttest AS ict values (6, 'Passionfruit') on conflict (key1) do update set fruit = insertconflicttest.fruit; -- error, references aliased away name

drop index key_index;

--
-- Composite key tests
--
-- inference succeeds:
insert into insertconflicttest values (7, 'Raspberry') on conflict (key1, fruit) do update set fruit = excluded.fruit;
insert into insertconflicttest values (8, 'Lime') on conflict (fruit, key1) do update set fruit = excluded.fruit;

-- inference fails:
insert into insertconflicttest values (9, 'Banana') on conflict (key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (10, 'Blueberry') on conflict (key1, key1, key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (11, 'Cherry') on conflict (key1, lower(fruit)) do update set fruit = excluded.fruit;
insert into insertconflicttest values (12, 'Date') on conflict (lower(fruit), key1) do update set fruit = excluded.fruit;

-- inference fails:
insert into insertconflicttest values (13, 'Grape') on conflict (key1, fruit) do update set fruit = excluded.fruit;
insert into insertconflicttest values (14, 'Raisin') on conflict (fruit, key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (15, 'Cranberry') on conflict (key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (16, 'Melon') on conflict (key1, key1, key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (17, 'Mulberry') on conflict (key1, lower(fruit)) do update set fruit = excluded.fruit;
insert into insertconflicttest values (18, 'Pineapple') on conflict (lower(fruit), key1) do update set fruit = excluded.fruit;

-- inference succeeds:
insert into insertconflicttest values (20, 'Quince') on conflict (lower(fruit)) do update set fruit = excluded.fruit;
insert into insertconflicttest values (21, 'Pomegranate') on conflict (lower(fruit), lower(fruit)) do update set fruit = excluded.fruit;

-- inference fails:
insert into insertconflicttest values (22, 'Apricot') on conflict (upper(fruit)) do update set fruit = excluded.fruit;
insert into insertconflicttest values (23, 'Blackberry') on conflict (fruit) do update set fruit = excluded.fruit;

-- inference succeeds:
insert into insertconflicttest values (24, 'Plum') on conflict (key1, lower(fruit)) do update set fruit = excluded.fruit;
insert into insertconflicttest values (25, 'Peach') on conflict (lower(fruit), key1) do update set fruit = excluded.fruit;
-- Should not infer "tricky_expr_comp_key_index" index:
explain (costs off) insert into insertconflicttest values (26, 'Fig') on conflict (lower(fruit), key1, lower(fruit), key1) do update set fruit = excluded.fruit;

-- inference fails:
insert into insertconflicttest values (27, 'Prune') on conflict (key1, upper(fruit)) do update set fruit = excluded.fruit;
insert into insertconflicttest values (28, 'Redcurrant') on conflict (fruit, key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (29, 'Nectarine') on conflict (key1) do update set fruit = excluded.fruit;

--
-- Non-spurious duplicate violation tests
--
create unique index key_index on insertconflicttest(key1);

-- succeeds, since UPDATE happens to update "fruit" to existing value:
insert into insertconflicttest values (26, 'Fig') on conflict (key1) do update set fruit = excluded.fruit;
-- fails, since UPDATE is to row with key value 26, and we're updating "fruit"
-- to a value that happens to exist in another row ('peach'):
insert into insertconflicttest values (26, 'Peach') on conflict (key1) do update set fruit = excluded.fruit;
-- succeeds, since "key" isn't repeated/referenced in UPDATE, and "fruit"
-- arbitrates that statement updates existing "Fig" row:
insert into insertconflicttest values (25, 'Fig') on conflict (fruit) do update set fruit = excluded.fruit;

drop index key_index;

-- Succeeds
insert into insertconflicttest values (23, 'Blackberry') on conflict (key1) where fruit like '%berry' do update set fruit = excluded.fruit;
insert into insertconflicttest values (23, 'Blackberry') on conflict (key1) where fruit like '%berry' and fruit = 'inconsequential' do nothing;

-- fails
insert into insertconflicttest values (23, 'Blackberry') on conflict (key1) do update set fruit = excluded.fruit;
insert into insertconflicttest values (23, 'Blackberry') on conflict (key1) where fruit like '%berry' or fruit = 'consequential' do nothing;
insert into insertconflicttest values (23, 'Blackberry') on conflict (fruit) where fruit like '%berry' do update set fruit = excluded.fruit;

--
-- Test that wholerow references to ON CONFLICT's EXCLUDED work
--
create unique index plain on insertconflicttest(key1);

-- Succeeds, updates existing row:
insert into insertconflicttest as i values (23, 'Jackfruit') on conflict (key1) do update set fruit = excluded.fruit
  where i.* != excluded.* returning *;
-- No update this time, though:
insert into insertconflicttest as i values (23, 'Jackfruit') on conflict (key1) do update set fruit = excluded.fruit
  where i.* != excluded.* returning *;
-- Predicate changed to require match rather than non-match, so updates once more:
insert into insertconflicttest as i values (23, 'Jackfruit') on conflict (key1) do update set fruit = excluded.fruit
  where i.* = excluded.* returning *;
-- Assign:
insert into insertconflicttest as i values (23, 'Avocado') on conflict (key1) do update set fruit = excluded.*::text
  returning *;
-- deparse whole row var in WHERE and SET clauses:
explain (costs off) insert into insertconflicttest as i values (23, 'Avocado') on conflict (key1) do update set fruit = excluded.fruit where excluded.* is null;
explain (costs off) insert into insertconflicttest as i values (23, 'Avocado') on conflict (key1) do update set fruit = excluded.*::text;

drop index plain;

-- Cleanup
drop table insertconflicttest;


--
-- Previous tests all managed to not test any expressions requiring
-- planner preprocessing ...
--
create table insertconflict (a bigint, b bigint);

-- computing column index is not suported
-- create unique index insertconflicti1 on insertconflict(coalesce(a, 0));

insert into insertconflict values (1, 2)
on conflict (coalesce(a, 0)) do nothing;

insert into insertconflict values (1, 2)
on conflict (b) where coalesce(a, 1) > 0 do nothing;

insert into insertconflict values (1, 2)
on conflict (b) where coalesce(a, 1) > 1 do nothing;

drop table insertconflict;

--
-- test insertion through view
-- not support on conflict clause ,so comment it currently.
--create table insertconflict (f1 int primary key, f2 text);
--create view insertconflictv as
--  select * from insertconflict with cascaded check option;
--insert into insertconflictv values (1,'foo')
--  on conflict (f1) do update set f2 = excluded.f2;
--select * from insertconflict;
--insert into insertconflictv values (1,'bar')
--  on conflict (f1) do update set f2 = excluded.f2;
--select * from insertconflict;
--drop view insertconflictv;
-- drop table insertconflict;


-- ******************************************************************
-- *                                                                *
-- * Test inheritance (example taken from tutorial)                 *
-- *                                                                *
-- ******************************************************************
create table cities (
	name		text,
	population	float8,
	altitude	int		-- (in ft)
);

create table capitals (
	state		char(2)
) inherits (cities);

-- prepopulate the tables.
insert into cities values ('San Francisco', 7.24E+5, 63);
insert into cities values ('Las Vegas', 2.583E+5, 2174);
insert into cities values ('Mariposa', 1200, 1953);

insert into capitals values ('Sacramento', 3.694E+5, 30, 'CA');
insert into capitals values ('Madison', 1.913E+5, 845, 'WI');

-- Tests proper for inheritance:
select * from capitals;

-- Succeeds:
insert into cities values ('Las Vegas', 2.583E+5, 2174) on conflict do nothing;
insert into capitals values ('Sacramento', 4664.E+5, 30, 'CA') on conflict (name) do update set population = excluded.population;
-- Wrong "Sacramento", so do nothing:
insert into capitals values ('Sacramento', 50, 2267, 'NE') on conflict (name) do nothing;
select * from capitals;
insert into cities values ('Las Vegas', 5.83E+5, 2001) on conflict (name) do update set population = excluded.population, altitude = excluded.altitude;
insert into capitals values ('Las Vegas', 5.83E+5, 2222, 'NV') on conflict (name) do update set population = excluded.population;
-- Capitals will contain new capital, Las Vegas:
select * from capitals;
-- Cities contains two instances of "Las Vegas", since unique constraints don't
-- work across inheritance:
-- This only affects "cities" version of "Las Vegas":
insert into cities values ('Las Vegas', 5.86E+5, 2223) on conflict (name) do update set population = excluded.population, altitude = excluded.altitude;

-- clean up
drop table capitals;
drop table cities;


-- Make sure a table named excluded is handled properly
create table excluded(key1 int primary key, data text);
insert into excluded values(1, '1');
-- error, ambiguous
insert into excluded values(1, '2') on conflict (key1) do update set data = excluded.data RETURNING *;
-- ok, aliased
insert into excluded AS target values(1, '2') on conflict (key1) do update set data = excluded.data RETURNING *;
-- ok, aliased
insert into excluded AS target values(1, '2') on conflict (key1) do update set data = target.data RETURNING *;
-- make sure excluded isn't a problem in returning clause
insert into excluded values(1, '2') on conflict (key1) do update set data = 3 RETURNING excluded.*;

-- clean up
drop table excluded;


-- Check tables w/o oids are handled correctly
create table testoids(key1 int primary key, data text) without oids;
-- first without oids
insert into testoids values(1, '1') on conflict (key1) do update set data = excluded.data RETURNING *;
insert into testoids values(1, '2') on conflict (key1) do update set data = excluded.data RETURNING *;
-- update existing row, that didn't have an oid
insert into testoids values(1, '3') on conflict (key1) do update set data = excluded.data RETURNING *;
-- insert a new row
insert into testoids values(2, '1') on conflict (key1) do update set data = excluded.data RETURNING *;
-- and update it
insert into testoids values(2, '2') on conflict (key1) do update set data = excluded.data RETURNING *;
-- remove oids again, test
alter table testoids set without oids;
insert into testoids values(1, '4') on conflict (key1) do update set data = excluded.data RETURNING *;
insert into testoids values(3, '1') on conflict (key1) do update set data = excluded.data RETURNING *;
insert into testoids values(3, '2') on conflict (key1) do update set data = excluded.data RETURNING *;

drop table testoids;


-- check that references to columns after dropped columns are handled correctly
create table dropcol(key1 int primary key, drop1 int, keep1 text, drop2 numeric, keep2 float);
insert into dropcol(key1, drop1, keep1, drop2, keep2) values(1, 1, '1', '1', 1);
-- set using excluded
insert into dropcol(key1, drop1, keep1, drop2, keep2) values(1, 2, '2', '2', 2) on conflict(key1)
    do update set drop1 = excluded.drop1, keep1 = excluded.keep1, drop2 = excluded.drop2, keep2 = excluded.keep2
    where excluded.drop1 is not null and excluded.keep1 is not null and excluded.drop2 is not null and excluded.keep2 is not null
          and dropcol.drop1 is not null and dropcol.keep1 is not null and dropcol.drop2 is not null and dropcol.keep2 is not null
    returning *;
;
-- set using existing table
insert into dropcol(key1, drop1, keep1, drop2, keep2) values(1, 3, '3', '3', 3) on conflict(key1)
    do update set drop1 = dropcol.drop1, keep1 = dropcol.keep1, drop2 = dropcol.drop2, keep2 = dropcol.keep2
    returning *;
;
alter table dropcol drop column drop1, drop column drop2;
-- set using excluded
insert into dropcol(key1, keep1, keep2) values(1, '4', 4) on conflict(key1)
    do update set keep1 = excluded.keep1, keep2 = excluded.keep2
    where excluded.keep1 is not null and excluded.keep2 is not null
          and dropcol.keep1 is not null and dropcol.keep2 is not null
    returning *;
;
-- set using existing table
insert into dropcol(key1, keep1, keep2) values(1, '5', 5) on conflict(key1)
    do update set keep1 = dropcol.keep1, keep2 = dropcol.keep2
    returning *;
;

drop table dropcol;

-- check handling of regular btree constraint along with gist constraint

create temp table twoconstraints (f1 int unique, f2 box);
insert into twoconstraints values(1, '((0,0),(1,1))');
insert into twoconstraints values(1, '((2,2),(3,3))');  -- fail on f1
insert into twoconstraints values(2, '((0,0),(1,2))');  -- fail on f2
insert into twoconstraints values(2, '((0,0),(1,2))')
  on conflict on constraint twoconstraints_f1_key do nothing;  -- fail on f2
insert into twoconstraints values(2, '((0,0),(1,2))')
  on conflict on constraint twoconstraints_f2_excl do nothing;  -- do nothing
select * from twoconstraints;
drop table twoconstraints;

-- check handling of self-conflicts at various isolation levels

create table selfconflict (f1 int primary key, f2 int);

begin transaction isolation level read committed;
insert into selfconflict values (1,1), (1,2) on conflict do nothing;
commit;

begin transaction isolation level repeatable read;
insert into selfconflict values (2,1), (2,2) on conflict do nothing;
commit;

begin transaction isolation level serializable;
insert into selfconflict values (3,1), (3,2) on conflict do nothing;
commit;

begin transaction isolation level read committed;
insert into selfconflict values (4,1), (4,2) on conflict(f1) do update set f2 = 0;
commit;

begin transaction isolation level repeatable read;
insert into selfconflict values (5,1), (5,2) on conflict(f1) do update set f2 = 0;
commit;

begin transaction isolation level serializable;
insert into selfconflict values (6,1), (6,2) on conflict(f1) do update set f2 = 0;
commit;

select * from selfconflict;

drop table selfconflict;

-- check ON CONFLICT handling with partitioned tables
create table parted_conflict_test (a int unique, b char) partition by list (a);
create table parted_conflict_test_1 partition of parted_conflict_test (b unique) for values in (1, 2);

-- no indexes required here
insert into parted_conflict_test values (1, 'a') on conflict do nothing;

-- index on a required, which does exist in parent
insert into parted_conflict_test values (1, 'a') on conflict (a) do nothing;
insert into parted_conflict_test values (1, 'a') on conflict (a) do update set b = excluded.b;

-- targeting partition directly will work
insert into parted_conflict_test_1 values (1, 'a') on conflict (a) do nothing;
insert into parted_conflict_test_1 values (1, 'b') on conflict (a) do update set b = excluded.b;

-- index on b required, which doesn't exist in parent
insert into parted_conflict_test values (2, 'b') on conflict (b) do update set a = excluded.a;

-- targeting partition directly will work
insert into parted_conflict_test_1 values (2, 'b') on conflict (b) do update set a = excluded.a;

-- should see (2, 'b')
select * from parted_conflict_test order by a;

-- should see (3, 'b')
select * from parted_conflict_test order by a;

-- case where parent will have a dropped column, but the partition won't
alter table parted_conflict_test drop b, add b char;
create table parted_conflict_test_3 partition of parted_conflict_test for values in (4);
delete from parted_conflict_test;
insert into parted_conflict_test (a, b) values (4, 'a') on conflict (a) do update set b = excluded.b;
insert into parted_conflict_test (a, b) values (4, 'b') on conflict (a) do update set b = excluded.b where parted_conflict_test.b = 'a';

-- should see (4, 'b')
select * from parted_conflict_test order by a;

-- case with multi-level partitioning
create table parted_conflict_test_4 partition of parted_conflict_test for values in (5) partition by list (a);
create table parted_conflict_test_4_1 partition of parted_conflict_test_4 for values in (5);
delete from parted_conflict_test;
insert into parted_conflict_test (a, b) values (5, 'a') on conflict (a) do update set b = excluded.b;
insert into parted_conflict_test (a, b) values (5, 'b') on conflict (a) do update set b = excluded.b where parted_conflict_test.b = 'a';

-- should see (5, 'b')
select * from parted_conflict_test order by a;

-- test with multiple rows
delete from parted_conflict_test;
insert into parted_conflict_test (a, b) values (1, 'a'), (2, 'a'), (4, 'a') on conflict (a) do update set b = excluded.b where excluded.b = 'b';
insert into parted_conflict_test (a, b) values (1, 'b'), (2, 'c'), (4, 'b') on conflict (a) do update set b = excluded.b where excluded.b = 'b';

-- should see (1, 'b'), (2, 'a'), (4, 'b')
select * from parted_conflict_test order by a;

drop table parted_conflict_test;

-- test behavior of inserting a conflicting tuple into an intermediate
-- partitioning level
create table parted_conflict (a int primary key, b text) partition by range (a);
create table parted_conflict_1 partition of parted_conflict for values from (0) to (1000) partition by range (a);
create table parted_conflict_1_1 partition of parted_conflict_1 for values from (0) to (500);
insert into parted_conflict values (40, 'forty');
insert into parted_conflict_1 values (40, 'cuarenta')
  on conflict (a) do update set b = excluded.b;
drop table parted_conflict;

-- same thing, but this time try to use an index that's created not in the
-- partition
create table parted_conflict (a int, b text) partition by range (a);
create table parted_conflict_1 partition of parted_conflict for values from (0) to (1000) partition by range (a);
create table parted_conflict_1_1 partition of parted_conflict_1 for values from (0) to (500);
create unique index on only parted_conflict_1 (a);
create unique index on only parted_conflict (a);
--alter index parted_conflict_a_idx attach partition parted_conflict_1_a_idx;
insert into parted_conflict values (40, 'forty');
insert into parted_conflict_1 values (40, 'cuarenta')
  on conflict (a) do update set b = excluded.b;
drop table parted_conflict;
