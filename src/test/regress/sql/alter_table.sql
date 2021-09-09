--
-- ALTER_TABLE
--

-- Clean up in case a prior regression run failed
SET client_min_messages TO 'warning';
DROP ROLE IF EXISTS regress_alter_table_user1;
RESET client_min_messages;

CREATE USER regress_alter_table_user1;

--
-- add attribute
--

CREATE TABLE attmp (initial int4);

COMMENT ON TABLE attmp_wrong IS 'table comment';
COMMENT ON TABLE attmp IS 'table comment';
COMMENT ON TABLE attmp IS NULL;

ALTER TABLE attmp ADD COLUMN xmin integer; -- fails

ALTER TABLE attmp ADD COLUMN a int4 default 3;

ALTER TABLE attmp ADD COLUMN b name;

ALTER TABLE attmp ADD COLUMN c text;

ALTER TABLE attmp ADD COLUMN d float8;

ALTER TABLE attmp ADD COLUMN e float4;

ALTER TABLE attmp ADD COLUMN f int2;

ALTER TABLE attmp ADD COLUMN i char;

ALTER TABLE attmp ADD COLUMN k int4;

ALTER TABLE attmp ADD COLUMN l tid;

ALTER TABLE attmp ADD COLUMN m xid;

ALTER TABLE attmp ADD COLUMN v timestamp;

--tid value is not supported fully: INSERT INTO attmp (a, b, c, d, e, f, i, k, l, m, v)
--   VALUES (4, 'name', 'text', 4.1, 4.1, 2, 'c', 314159, '(1,1)', '512', 'epoch');

INSERT INTO attmp (a, b, c, d, e, f, i, k, m, v)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, 'c', 314159, '512', 'epoch');

SELECT * FROM attmp;

DROP TABLE attmp;

-- the wolf bug - schema mods caused inconsistent row descriptors
CREATE TABLE attmp (
	initial 	int4
);

ALTER TABLE attmp ADD COLUMN a int4;

ALTER TABLE attmp ADD COLUMN b name;

ALTER TABLE attmp ADD COLUMN c text;

ALTER TABLE attmp ADD COLUMN d float8;

ALTER TABLE attmp ADD COLUMN e float4;

ALTER TABLE attmp ADD COLUMN f int2;

ALTER TABLE attmp ADD COLUMN i char;

ALTER TABLE attmp ADD COLUMN k int4;

ALTER TABLE attmp ADD COLUMN l tid;

ALTER TABLE attmp ADD COLUMN m xid;

ALTER TABLE attmp ADD COLUMN v timestamp;

--INSERT INTO attmp (a, b, c, d, e, f, i, k, l, m, v)
--   VALUES (4, 'name', 'text', 4.1, 4.1, 2, 'c', 314159, '(1,1)', '512', 'epoch');

INSERT INTO attmp (a, b, c, d, e, f, i, k,  m, v)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, 'c', 314159, '512', 'epoch');

SELECT * FROM attmp;

DROP TABLE attmp;

--
-- rename - check on both non-temp and temp tables
--
CREATE TABLE attmp (regtable int);
CREATE TEMP TABLE attmp (attmptable int);

ALTER TABLE attmp RENAME TO attmp_new;

SELECT * FROM attmp;
SELECT * FROM attmp_new;

ALTER TABLE attmp RENAME TO attmp_new2;

SELECT * FROM attmp;		-- should fail
SELECT * FROM attmp_new;
SELECT * FROM attmp_new2;

DROP TABLE attmp_new;
DROP TABLE attmp_new2;

-- check rename of partitioned tables and indexes also
CREATE TABLE part_attmp (a int primary key) partition by range (a);
CREATE TABLE part_attmp1 PARTITION OF part_attmp FOR VALUES FROM (0) TO (100);
ALTER INDEX part_attmp_pkey RENAME TO part_attmp_index;
ALTER INDEX part_attmp1_pkey RENAME TO part_attmp1_index;
ALTER TABLE part_attmp RENAME TO part_at2tmp;
ALTER TABLE part_attmp1 RENAME TO part_at2tmp1;
SET ROLE regress_alter_table_user1;
ALTER INDEX part_attmp_index RENAME TO fail;
ALTER INDEX part_attmp1_index RENAME TO fail;
ALTER TABLE part_at2tmp RENAME TO fail;
ALTER TABLE part_at2tmp1 RENAME TO fail;
RESET ROLE;
DROP TABLE part_at2tmp;

-- The original test is to use name of _attmp_array, but kunlun does not
-- support name starting with _, so we change it.
CREATE TABLE attmp_array (id int);
ALTER TABLE attmp_array RENAME TO attmp_array_new;
DROP TABLE attmp_array_new;

-- ALTER TABLE ... RENAME on non-table relations
-- renaming indexes (FIXME: this should probably test the index's functionality)
ALTER INDEX IF EXISTS __onek_unique1 RENAME TO attmp_onek_unique1;
ALTER INDEX IF EXISTS __attmp_onek_unique1 RENAME TO onek_unique1;

ALTER INDEX onek_unique1 RENAME TO attmp_onek_unique1;
ALTER INDEX attmp_onek_unique1 RENAME TO onek_unique1;

SET ROLE regress_alter_table_user1;
ALTER INDEX onek_unique1 RENAME TO fail;  -- permission denied
RESET ROLE;

-- renaming views
CREATE VIEW attmp_view (unique1) AS SELECT unique1 FROM tenk1;
ALTER TABLE attmp_view RENAME TO attmp_view_new;

SET ROLE regress_alter_table_user1;
ALTER VIEW attmp_view_new RENAME TO fail;  -- permission denied
RESET ROLE;

-- hack to ensure we get an indexscan here
set enable_seqscan to off;
set enable_bitmapscan to off;
-- 5 values, sorted
SELECT unique1 FROM tenk1 WHERE unique1 < 5;
reset enable_seqscan;
reset enable_bitmapscan;

DROP VIEW attmp_view_new;
-- toast-like relation name
alter table stud_emp rename to pg_toast_stud_emp;
alter table pg_toast_stud_emp rename to stud_emp;

-- renaming index should rename constraint as well
ALTER TABLE onek ADD CONSTRAINT onek_unique1_constraint UNIQUE (unique1);
ALTER INDEX onek_unique1_constraint RENAME TO onek_unique1_constraint_foo;
ALTER TABLE onek DROP CONSTRAINT onek_unique1_constraint_foo;

-- renaming constraint should rename index as well
ALTER TABLE onek ADD CONSTRAINT onek_unique1_constraint UNIQUE (unique1);
DROP INDEX onek_unique1_constraint;  -- to see whether it's there
ALTER TABLE onek RENAME CONSTRAINT onek_unique1_constraint TO onek_unique1_constraint_foo;
DROP INDEX onek_unique1_constraint_foo;  -- to see whether it's there
ALTER TABLE onek DROP CONSTRAINT onek_unique1_constraint_foo;

-- renaming constraints with cache reset of target relation
CREATE TABLE constraint_rename_cache (a int, PRIMARY KEY (a));
ALTER TABLE constraint_rename_cache
  RENAME CONSTRAINT constraint_rename_cache_pkey TO constraint_rename_pkey_new;
CREATE TABLE like_constraint_rename_cache
  (LIKE constraint_rename_cache INCLUDING ALL);
\d like_constraint_rename_cache
DROP TABLE constraint_rename_cache;
DROP TABLE like_constraint_rename_cache;

-- test unique constraint adding
create table atacc1 ( test int );
-- add a unique constraint
alter table atacc1 add constraint atacc_test1 unique (test);
-- insert first value
insert into atacc1 (test) values (2);
-- should fail
insert into atacc1 (test) values (2);
-- should succeed
insert into atacc1 (test) values (4);
-- try adding a unique oid constraint
-- try to create duplicates via alter table using - should fail
alter table atacc1 alter column test type integer using 0;
drop table atacc1;

-- let's do one where the unique constraint fails when added
create table atacc1 ( test int );
-- insert soon to be failing rows
insert into atacc1 (test) values (2);
insert into atacc1 (test) values (2);
-- add a unique constraint (fails)
alter table atacc1 add constraint atacc_test1 unique (test);
insert into atacc1 (test) values (3);
drop table atacc1;

-- let's do one where the unique constraint fails
-- because the column doesn't exist
create table atacc1 ( test int );
-- add a unique constraint (fails)
alter table atacc1 add constraint atacc_test1 unique (test1);
drop table atacc1;

-- something a little more complicated
create table atacc1 ( test int, test2 int);
-- add a unique constraint
alter table atacc1 add constraint atacc_test1 unique (test, test2);
-- insert initial value
insert into atacc1 (test,test2) values (4,4);
-- should fail
insert into atacc1 (test,test2) values (4,4);
-- should all succeed
insert into atacc1 (test,test2) values (4,5);
insert into atacc1 (test,test2) values (5,4);
insert into atacc1 (test,test2) values (5,5);
drop table atacc1;

-- lets do some naming tests
create table atacc1 (test int, test2 int, unique(test));
alter table atacc1 add unique (test2);
-- should fail for @@ second one @@
insert into atacc1 (test2, test) values (3, 3);
insert into atacc1 (test2, test) values (2, 3);
drop table atacc1;

-- test primary key constraint adding

create table atacc1 ( test int );
-- add a primary key constraint
alter table atacc1 add constraint atacc_test1 primary key (test);
-- insert first value
insert into atacc1 (test) values (2);
-- should fail
insert into atacc1 (test) values (2);
-- should succeed
insert into atacc1 (test) values (4);
-- inserting NULL should fail
insert into atacc1 (test) values(NULL);
-- try adding a second primary key (should fail)
alter table atacc1 drop constraint atacc_test1 restrict;
drop table atacc1;

-- let's do one where the primary key constraint fails when added
create table atacc1 ( test int );
-- insert soon to be failing rows
insert into atacc1 (test) values (2);
insert into atacc1 (test) values (2);
-- add a primary key (fails)
alter table atacc1 add constraint atacc_test1 primary key (test);
insert into atacc1 (test) values (3);
drop table atacc1;

-- let's do another one where the primary key constraint fails when added
create table atacc1 ( test int );
-- insert soon to be failing row
insert into atacc1 (test) values (NULL);
-- add a primary key (fails)
alter table atacc1 add constraint atacc_test1 primary key (test);
insert into atacc1 (test) values (3);
drop table atacc1;

-- let's do one where the primary key constraint fails
-- because the column doesn't exist
create table atacc1 ( test int );
-- add a primary key constraint (fails)
alter table atacc1 add constraint atacc_test1 primary key (test1);
drop table atacc1;

-- adding a new column as primary key to a non-empty table.
-- should fail unless the column has a non-null default value.
create table atacc1 ( test int );
insert into atacc1 (test) values (0);
-- add a primary key column without a default (fails).
alter table atacc1 add column test2 int primary key;
-- now add a primary key column with a default (succeeds).
alter table atacc1 add column test2 int default 0 primary key;
drop table atacc1;

-- something a little more complicated
create table atacc1 ( test int, test2 int);
-- add a primary key constraint
alter table atacc1 add constraint atacc_test1 primary key (test, test2);
-- try adding a second primary key - should fail
alter table atacc1 add constraint atacc_test2 primary key (test);
-- insert initial value
insert into atacc1 (test,test2) values (4,4);
-- should fail
insert into atacc1 (test,test2) values (4,4);
insert into atacc1 (test,test2) values (NULL,3);
insert into atacc1 (test,test2) values (3, NULL);
insert into atacc1 (test,test2) values (NULL,NULL);
-- should all succeed
insert into atacc1 (test,test2) values (4,5);
insert into atacc1 (test,test2) values (5,4);
insert into atacc1 (test,test2) values (5,5);
drop table atacc1;

-- lets do some naming tests
create table atacc1 (test int, test2 int, primary key(test));
-- only first should succeed
insert into atacc1 (test2, test) values (3, 3);
insert into atacc1 (test2, test) values (2, 3);
insert into atacc1 (test2, test) values (1, NULL);
drop table atacc1;

-- alter table / alter column [set/drop] not null tests
-- try altering system catalogs, should fail
alter table pg_class alter column relname drop not null;
alter table pg_class alter relname set not null;

-- try altering non-existent table, should fail
alter table non_existent alter column bar set not null;
alter table non_existent alter column bar drop not null;

-- test setting columns to null and not null and vice versa
-- test checking for null values and primary key
create table atacc1 (test int not null);
alter table atacc1 add constraint "atacc1_pkey" primary key (test);
alter table atacc1 alter column test drop not null;
alter table atacc1 drop constraint "atacc1_pkey";
alter table atacc1 alter column test drop not null;
insert into atacc1 values (null);
alter table atacc1 alter test set not null;
delete from atacc1;
alter table atacc1 alter test set not null;

-- try altering a non-existent column, should fail
alter table atacc1 alter bar set not null;
alter table atacc1 alter bar drop not null;

-- try creating a view and altering that, should fail
create view myview as select * from atacc1;
alter table myview alter column test drop not null;
alter table myview alter column test set not null;
drop view myview;

drop table atacc1;

-- test setting and removing default values
create table def_test (
	c1	int4 default 5,
	c2	text default 'initial_default'
);
insert into def_test default values;
alter table def_test alter column c1 drop default;
insert into def_test default values;
alter table def_test alter column c2 drop default;
insert into def_test default values;
alter table def_test alter column c1 set default 10;
alter table def_test alter column c2 set default 'new_default';
insert into def_test default values;
select * from def_test;

-- set defaults to an incorrect type: this should fail
alter table def_test alter column c1 set default 'wrong_datatype';
alter table def_test alter column c2 set default 20;

-- set defaults on a non-existent column: this should fail
alter table def_test alter column c3 set default 30;

-- set defaults on views: we need to create a view, add a rule
-- to allow insertions into it, and then alter the view to add
-- a default
create view def_view_test as select * from def_test;
insert into def_view_test default values;
alter table def_view_test alter column c1 set default 45;
insert into def_view_test default values;
alter table def_view_test alter column c2 set default 'view_default';
insert into def_view_test default values;
select * from def_view_test;

drop view def_view_test;
drop table def_test;

-- alter table / drop column tests
-- try altering system catalogs, should fail
alter table pg_class drop column relname;

-- try altering non-existent table, should fail
alter table nosuchtable drop column bar;

-- test dropping columns
create table atacc1 (a int4 not null, b int4, c int4 not null, d int4);
insert into atacc1 values (1, 2, 3, 4);
alter table atacc1 drop a;
alter table atacc1 drop a;

-- SELECTs
select * from atacc1;
select * from atacc1 order by a;
select * from atacc1 order by "........pg.dropped.1........";
select * from atacc1 group by a;
select * from atacc1 group by "........pg.dropped.1........";
select atacc1.* from atacc1;
select a from atacc1;
select atacc1.a from atacc1;
select b,c,d from atacc1;
select a,b,c,d from atacc1;
select * from atacc1 where a = 1;
select "........pg.dropped.1........" from atacc1;
select atacc1."........pg.dropped.1........" from atacc1;
select "........pg.dropped.1........",b,c,d from atacc1;
select * from atacc1 where "........pg.dropped.1........" = 1;

-- UPDATEs
update atacc1 set a = 3;
update atacc1 set b = 2 where a = 3;
update atacc1 set "........pg.dropped.1........" = 3;
update atacc1 set b = 2 where "........pg.dropped.1........" = 3;

-- INSERTs
insert into atacc1 values (10, 11, 12, 13);
insert into atacc1 values (default, 11, 12, 13);
insert into atacc1 values (11, 12, 13);
insert into atacc1 (a) values (10);
insert into atacc1 (a) values (default);
insert into atacc1 (a,b,c,d) values (10,11,12,13);
insert into atacc1 (a,b,c,d) values (default,11,12,13);
insert into atacc1 (b,c,d) values (11,12,13);
insert into atacc1 ("........pg.dropped.1........") values (10);
insert into atacc1 ("........pg.dropped.1........") values (default);
insert into atacc1 ("........pg.dropped.1........",b,c,d) values (10,11,12,13);
insert into atacc1 ("........pg.dropped.1........",b,c,d) values (default,11,12,13);

-- DELETEs
delete from atacc1 where a = 3;
delete from atacc1 where "........pg.dropped.1........" = 3;
delete from atacc1;

-- try dropping a non-existent column, should fail
alter table atacc1 drop bar;

-- try dropping the xmin column, should fail
alter table atacc1 drop xmin;

-- try creating a view and altering that, should fail
create view myview as select * from atacc1;
select * from myview;
alter table myview drop d;
drop view myview;

-- test some commands to make sure they fail on the dropped column
comment on column atacc1.a is 'testing';
comment on column atacc1."........pg.dropped.1........" is 'testing';
--alter table atacc1 alter a set storage plain;
--alter table atacc1 alter "........pg.dropped.1........" set storage plain;
--alter table atacc1 alter a set statistics 0;
--alter table atacc1 alter "........pg.dropped.1........" set statistics 0;
alter table atacc1 alter a set default 3;
alter table atacc1 alter "........pg.dropped.1........" set default 3;
alter table atacc1 alter a drop default;
alter table atacc1 alter "........pg.dropped.1........" drop default;
alter table atacc1 alter a set not null;
alter table atacc1 alter "........pg.dropped.1........" set not null;
alter table atacc1 alter a drop not null;
alter table atacc1 alter "........pg.dropped.1........" drop not null;
alter table atacc1 rename a to x;
alter table atacc1 rename "........pg.dropped.1........" to x;
alter table atacc1 add primary key(a);
alter table atacc1 add primary key("........pg.dropped.1........");
alter table atacc1 add unique(a);
alter table atacc1 add unique("........pg.dropped.1........");
create table atacc2 (id int4 unique);
drop table atacc2;
create index "testing_idx" on atacc1(a);
create index "testing_idx" on atacc1("........pg.dropped.1........");

-- test create as and select into
insert into atacc1 values (21, 22, 23);
-- try dropping all columns
alter table atacc1 drop c;
alter table atacc1 drop d;
--not support to drop all table(by MySQL): alter table atacc1 drop b;
select * from atacc1;

drop table atacc1;

-- test constraint error reporting in presence of dropped columns
create table atacc1 (id serial primary key, value int);
insert into atacc1(value) values (100);
alter table atacc1 drop column value;
insert into atacc1(value) values (100);
insert into atacc1(id, value) values (null, 0);
drop table atacc1;

create table p1(id int, name text);
create table p2(id2 int, name text, height int);

-- should work
alter table only p1 drop column name;
-- should work. Now c1.name is local and inhcount is 0.
alter table p2 drop column name;
-- should work and drop the attribute in all tables
alter table p2 drop column height;

-- IF EXISTS test
--not support empty table: create table dropColumnExists ();
--alter table dropColumnExists drop column non_existing; --fail
--alter table dropColumnExists drop column if exists non_existing; --succeed

drop table p1 cascade;
drop table p2 cascade;


-- test that operations with a dropped column do not try to reference
-- its datatype

create temp table foo (f1 text, f2 text, f3 text);
insert into foo values('bb','cc','dd');
select * from foo;

select * from foo;
insert into foo values('qq','rr');
select * from foo;
update foo set f3 = 'zz';
select * from foo;
select f3,max(f1) from foo group by f3;

-- Simple tests for alter table column type
alter table foo alter f1 TYPE integer; -- fails
alter table foo alter f1 TYPE varchar(10);

create table anothertab (atcol1 serial8, atcol2 boolean);

insert into anothertab (atcol1, atcol2) values (default, true);
insert into anothertab (atcol1, atcol2) values (default, false);
select * from anothertab;

alter table anothertab alter column atcol1 type boolean; -- fails
alter table anothertab alter column atcol1 type boolean using atcol1::int; -- fails
alter table anothertab alter column atcol1 type integer;

select * from anothertab;

insert into anothertab (atcol1, atcol2) values (45, null); -- fails
insert into anothertab (atcol1, atcol2) values (default, null);

select * from anothertab;

alter table anothertab alter column atcol2 type text
      using case when atcol2 is true then 'IT WAS TRUE'
                 when atcol2 is false then 'IT WAS FALSE'
                 else 'IT WAS NULL!' end;

select * from anothertab;
alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end; -- fails
alter table anothertab alter column atcol1 drop default;
alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end; -- fails
alter table anothertab drop constraint anothertab_chk;
alter table anothertab drop constraint anothertab_chk; -- fails
alter table anothertab drop constraint IF EXISTS anothertab_chk; -- succeeds

alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end;

select * from anothertab;

drop table anothertab;

-- Test index handling in alter table column type (cf. bugs #15835, #15865)
create table anothertab(f1 int primary key, f2 int unique,
                        f3 int, f4 int, f5 int);
alter table anothertab add unique(f1,f4);
create index on anothertab(f2,f3);
create unique index on anothertab(f4);

\d anothertab
alter table anothertab alter column f1 type bigint;
alter table anothertab
  alter column f2 type bigint,
  alter column f3 type bigint,
  alter column f4 type bigint;
alter table anothertab alter column f5 type bigint;
\d anothertab

drop table anothertab;

create table another (f1 int, f2 text);

insert into another values(1, 'one');
insert into another values(2, 'two');
insert into another values(3, 'three');

select * from another;

alter table another
  alter f1 type text using f2 || ' more',
  alter f2 type bigint using f1 * 10;

select * from another;

drop table another;

-- table's row type
create table tab1 (a int, b text);
alter table tab1 alter column b type varchar; -- fails
drop table tab1;

-- Alter column type that's part of a partitioned index
create table at_partitioned (a int, b varchar(50)) partition by range (a);
create table at_part_1 partition of at_partitioned for values from (0) to (1000);
insert into at_partitioned values (512, '0.123');
create index on at_partitioned (b);
create index on at_partitioned (a);
\d at_part_1
alter table at_partitioned alter column b type numeric using b::numeric;
\d at_part_1
drop table at_partitioned;

-- Alter column type when no table rewrite is required
-- Also check that comments are preserved
create table at_partitioned(id int, name varchar(64), unique (id, name))
  partition by hash(id);
comment on constraint at_partitioned_id_name_key on at_partitioned is 'parent constraint';
comment on index at_partitioned_id_name_key is 'parent index';
create table at_partitioned_0 partition of at_partitioned
  for values with (modulus 2, remainder 0);
comment on constraint at_partitioned_0_id_name_key on at_partitioned_0 is 'child 0 constraint';
comment on index at_partitioned_0_id_name_key is 'child 0 index';
create table at_partitioned_1 partition of at_partitioned
  for values with (modulus 2, remainder 1);
comment on constraint at_partitioned_1_id_name_key on at_partitioned_1 is 'child 1 constraint';
comment on index at_partitioned_1_id_name_key is 'child 1 index';
insert into at_partitioned values(1, 'foo');
insert into at_partitioned values(3, 'bar');

select conname, obj_description(oid, 'pg_constraint') as desc
  from pg_constraint where conname like 'at_partitioned%'
  order by conname;

alter table at_partitioned alter column name type varchar(127);

select conname, obj_description(oid, 'pg_constraint') as desc
  from pg_constraint where conname like 'at_partitioned%'
  order by conname;

-- Don't remove this DROP, it exposes bug #15672
drop table at_partitioned;

-- ALTER COLUMN TYPE with a check constraint and a child table (bug #13779)
CREATE TABLE test_inh_check (a float , b float);
\d test_inh_check
ALTER TABLE test_inh_check ALTER COLUMN a TYPE numeric;
\d test_inh_check
ALTER TABLE test_inh_check ALTER COLUMN b TYPE numeric;
\d test_inh_check
drop table test_inh_check;

-- ALTER COLUMN TYPE with different schema in children
-- Bug at https://postgr.es/m/20170102225618.GA10071@telsasoft.com
CREATE TABLE test_type_diff (f1 int);
ALTER TABLE test_type_diff ADD COLUMN f2 int;
ALTER TABLE test_type_diff ALTER COLUMN f2 TYPE bigint USING f2::bigint;
CREATE TABLE test_type_diff2 (int_two int2, int_four int4, int_eight int8);
INSERT INTO test_type_diff2 VALUES (1, 2, 3);
INSERT INTO test_type_diff2 VALUES (4, 5, 6);
INSERT INTO test_type_diff2 VALUES (7, 8, 9);
ALTER TABLE test_type_diff2 ALTER COLUMN int_four TYPE int8 USING int_four::int8;
-- whole-row references are disallowed
ALTER TABLE test_type_diff2 ALTER COLUMN int_four TYPE int4 USING (pg_column_size(test_type_diff2));
drop table test_type_diff;
drop table test_type_diff2;

-- check column addition within a view (bug #14876)
create table at_base_table(id int, stuff text);
insert into at_base_table values (23, 'skidoo');
create view at_view_1 as select * from at_base_table bt;
create view at_view_2 as select *, to_json(v1) as j from at_view_1 v1;
\d+ at_view_1
\d+ at_view_2
explain (verbose, costs off) select * from at_view_2;
select * from at_view_2;

create or replace view at_view_1 as select *, 2+2 as more from at_base_table bt;
\d+ at_view_1
\d+ at_view_2
explain (verbose, costs off) select * from at_view_2;
select * from at_view_2;

drop view at_view_2;
drop view at_view_1;
drop table at_base_table;

--
-- alter function
--
create function test_strict(text) returns text as
    'select coalesce($1, ''got passed a null'');'
    language sql returns null on null input;
select test_strict(NULL);
alter function test_strict(text) called on null input;
select test_strict(NULL);
drop function test_strict(text);

create function non_strict(text) returns text as
    'select coalesce($1, ''got passed a null'');'
    language sql called on null input;
select non_strict(NULL);
alter function non_strict(text) returns null on null input;
select non_strict(NULL);
drop function non_strict(text);
--
-- alter object set schema
--

create schema alter1;
create schema alter2;

create table alter1.t1(f1 serial primary key, f2 int);

create view alter1.v1 as select * from alter1.t1;

create function alter1.plus1(int) returns int as 'select $1+1' language sql;

create type alter1.ctype as (f1 int, f2 text);

create function alter1.same(alter1.ctype, alter1.ctype) returns boolean language sql
as 'select $1.f1 is not distinct from $2.f1 and $1.f2 is not distinct from $2.f2';

create operator alter1.=(procedure = alter1.same, leftarg  = alter1.ctype, rightarg = alter1.ctype);

create operator class alter1.ctype_hash_ops default for type alter1.ctype using hash as
  operator 1 alter1.=(alter1.ctype, alter1.ctype);

create conversion alter1.ascii_to_utf8 for 'sql_ascii' to 'utf8' from ascii_to_utf8;

create text search parser alter1.prs(start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);
create text search configuration alter1.cfg(parser = alter1.prs);
create text search template alter1.tmpl(init = dsimple_init, lexize = dsimple_lexize);
create text search dictionary alter1.dict(template = alter1.tmpl);

insert into alter1.t1(f2) values(11);
insert into alter1.t1(f2) values(12);

alter table alter1.t1 set schema alter1; -- no-op, same schema
alter table alter1.t1 set schema alter2;
alter table alter1.v1 set schema alter2;
alter function alter1.plus1(int) set schema alter2;
alter operator class alter1.ctype_hash_ops using hash set schema alter2;
alter operator family alter1.ctype_hash_ops using hash set schema alter2;
alter operator alter1.=(alter1.ctype, alter1.ctype) set schema alter2;
alter function alter1.same(alter1.ctype, alter1.ctype) set schema alter2;
alter type alter1.ctype set schema alter1; -- no-op, same schema
alter type alter1.ctype set schema alter2;
alter conversion alter1.ascii_to_utf8 set schema alter2;
alter text search parser alter1.prs set schema alter2;
alter text search configuration alter1.cfg set schema alter2;
alter text search template alter1.tmpl set schema alter2;
alter text search dictionary alter1.dict set schema alter2;

-- this should succeed because nothing is left in alter1
drop schema alter1;

insert into alter2.t1(f2) values(13);
insert into alter2.t1(f2) values(14);

select * from alter2.t1;

select * from alter2.v1;

select alter2.plus1(41);

-- clean up
drop table alter2.t1 cascade;
drop schema alter2 cascade;

--
-- composite types
--

CREATE TYPE test_type AS (a int);
\d test_type

ALTER TYPE nosuchtype ADD ATTRIBUTE b text; -- fails

ALTER TYPE test_type ADD ATTRIBUTE b text;
\d test_type

ALTER TYPE test_type ADD ATTRIBUTE b text; -- fails

ALTER TYPE test_type ALTER ATTRIBUTE b SET DATA TYPE varchar;
\d test_type

ALTER TYPE test_type ALTER ATTRIBUTE b SET DATA TYPE integer;
\d test_type

ALTER TYPE test_type DROP ATTRIBUTE b;
\d test_type

ALTER TYPE test_type DROP ATTRIBUTE c; -- fails

ALTER TYPE test_type DROP ATTRIBUTE IF EXISTS c;

ALTER TYPE test_type DROP ATTRIBUTE a, ADD ATTRIBUTE d boolean;
\d test_type

ALTER TYPE test_type RENAME ATTRIBUTE a TO aa;
ALTER TYPE test_type RENAME ATTRIBUTE d TO dd;
\d test_type

DROP TYPE test_type;

CREATE TYPE test_type1 AS (a int, b text);
ALTER TYPE test_type1 ALTER ATTRIBUTE b TYPE varchar; -- fails
DROP TYPE test_type1;

CREATE TYPE test_type2 AS (a int, b text);
\d test_type2

ALTER TYPE test_type2 ADD ATTRIBUTE c text; -- fails
ALTER TYPE test_type2 ADD ATTRIBUTE c text CASCADE;
\d test_type2

ALTER TYPE test_type2 ALTER ATTRIBUTE b TYPE varchar; -- fails
ALTER TYPE test_type2 ALTER ATTRIBUTE b TYPE varchar CASCADE;
\d test_type2

ALTER TYPE test_type2 DROP ATTRIBUTE b; -- fails
ALTER TYPE test_type2 DROP ATTRIBUTE b CASCADE;
\d test_type2

ALTER TYPE test_type2 RENAME ATTRIBUTE a TO aa; -- fails
ALTER TYPE test_type2 RENAME ATTRIBUTE a TO aa CASCADE;
\d test_type2
drop type test_type2;

CREATE TYPE test_typex AS (a int, b text);
ALTER TYPE test_typex DROP ATTRIBUTE a; -- fails
ALTER TYPE test_typex DROP ATTRIBUTE a CASCADE;
\d test_tblx
DROP TYPE test_typex;

--
-- IF EXISTS test
--
ALTER TABLE IF EXISTS tt8 ADD COLUMN f int;
ALTER TABLE IF EXISTS tt8 ADD CONSTRAINT xxx PRIMARY KEY(f);
ALTER TABLE IF EXISTS tt8 ADD CHECK (f BETWEEN 0 AND 10);
ALTER TABLE IF EXISTS tt8 ALTER COLUMN f SET DEFAULT 0;
ALTER TABLE IF EXISTS tt8 RENAME COLUMN f TO f1;
ALTER TABLE IF EXISTS tt8 SET SCHEMA alter2;

CREATE TABLE tt8(a int);
CREATE SCHEMA alter2;

ALTER TABLE IF EXISTS tt8 ADD COLUMN f int;
ALTER TABLE IF EXISTS tt8 ADD CONSTRAINT xxx PRIMARY KEY(f);
ALTER TABLE IF EXISTS tt8 ALTER COLUMN f SET DEFAULT 0;
ALTER TABLE IF EXISTS tt8 RENAME COLUMN f TO f1;
ALTER TABLE IF EXISTS tt8 SET SCHEMA alter2;

\d alter2.tt8

DROP TABLE alter2.tt8;
DROP SCHEMA alter2;

--
-- Check conflicts between index and CHECK constraint names
--
CREATE TABLE tt9(c integer);
ALTER TABLE tt9 ADD UNIQUE(c);
ALTER TABLE tt9 ADD UNIQUE(c);  -- picks nonconflicting name
ALTER TABLE tt9 ADD CONSTRAINT tt9_c_key UNIQUE(c);  -- fail, dup name
ALTER TABLE tt9 ADD CONSTRAINT foo UNIQUE(c);  -- fail, dup name
ALTER TABLE tt9 ADD UNIQUE(c);  -- picks nonconflicting name
\d tt9
DROP TABLE tt9;


-- Check that comments on constraints and indexes are not lost at ALTER TABLE.
CREATE TABLE comment_test (
  id int,
  positive_col int,
  indexed_col int,
  CONSTRAINT comment_test_pk PRIMARY KEY (id));
CREATE INDEX comment_test_index ON comment_test(indexed_col);

COMMENT ON COLUMN comment_test.id IS 'Column ''id'' on comment_test';
COMMENT ON INDEX comment_test_index IS 'Simple index on comment_test';
COMMENT ON CONSTRAINT comment_test_pk ON comment_test IS 'PRIMARY KEY constraint of comment_test';
COMMENT ON INDEX comment_test_pk IS 'Index backing the PRIMARY KEY of comment_test';

SELECT col_description('comment_test'::regclass, 1) as comment;
SELECT indexrelid::regclass::text as index, obj_description(indexrelid, 'pg_class') as comment FROM pg_index where indrelid = 'comment_test'::regclass ORDER BY 1, 2;
SELECT conname as constraint, obj_description(oid, 'pg_constraint') as comment FROM pg_constraint where conrelid = 'comment_test'::regclass ORDER BY 1, 2;

-- Change the datatype of all the columns. ALTER TABLE is optimized to not
-- rebuild an index if the new data type is binary compatible with the old
-- one. Check do a dummy ALTER TABLE that doesn't change the datatype
-- first, to test that no-op codepath, and another one that does.
ALTER TABLE comment_test ALTER COLUMN indexed_col SET DATA TYPE int;
ALTER TABLE comment_test ALTER COLUMN indexed_col SET DATA TYPE varchar(50);
ALTER TABLE comment_test ALTER COLUMN id SET DATA TYPE int;
ALTER TABLE comment_test ALTER COLUMN id SET DATA TYPE varchar(50);
ALTER TABLE comment_test ALTER COLUMN positive_col SET DATA TYPE int;
ALTER TABLE comment_test ALTER COLUMN positive_col SET DATA TYPE bigint;

-- Check that the comments are intact.
SELECT col_description('comment_test'::regclass, 1) as comment;
SELECT indexrelid::regclass::text as index, obj_description(indexrelid, 'pg_class') as comment FROM pg_index where indrelid = 'comment_test'::regclass ORDER BY 1, 2;
SELECT conname as constraint, obj_description(oid, 'pg_constraint') as comment FROM pg_constraint where conrelid = 'comment_test'::regclass ORDER BY 1, 2;

-- Change column type of parent
ALTER TABLE comment_test ALTER COLUMN id SET DATA TYPE varchar(50);
ALTER TABLE comment_test ALTER COLUMN id SET DATA TYPE int USING id::integer;

-- Comments should be intact
SELECT col_description('comment_test_child'::regclass, 1) as comment;
SELECT indexrelid::regclass::text as index, obj_description(indexrelid, 'pg_class') as comment FROM pg_index where indrelid = 'comment_test_child'::regclass ORDER BY 1, 2;
SELECT conname as constraint, obj_description(oid, 'pg_constraint') as comment FROM pg_constraint where conrelid = 'comment_test_child'::regclass ORDER BY 1, 2;

-- Checks on creating and manipulation of user defined relations in
-- pg_catalog.
--
-- XXX: It would be useful to add checks around trying to manipulate
-- catalog tables, but that might have ugly consequences when run
-- against an existing server with allow_system_table_mods = on.

SHOW allow_system_table_mods;
-- disallowed because of search_path issues with pg_dump
CREATE TABLE pg_catalog.new_system_table();
-- instead create in public first, move to catalog
CREATE TABLE new_system_table(id serial primary key, othercol text);
ALTER TABLE new_system_table SET SCHEMA pg_catalog;

-- XXX: it's currently impossible to move relations out of pg_catalog
ALTER TABLE new_system_table SET SCHEMA public;
-- move back, will be ignored -- already there
ALTER TABLE new_system_table SET SCHEMA pg_catalog;
ALTER TABLE new_system_table RENAME TO old_system_table;
INSERT INTO old_system_table(othercol) VALUES ('somedata'), ('otherdata');
UPDATE old_system_table SET id = -id;
DELETE FROM old_system_table WHERE othercol = 'somedata';
delete from old_system_table;
ALTER TABLE old_system_table DROP CONSTRAINT new_system_table_pkey;
ALTER TABLE old_system_table DROP COLUMN othercol;
DROP TABLE old_system_table;

-- test ADD COLUMN IF NOT EXISTS
CREATE TABLE test_add_column(c1 integer);
\d test_add_column
ALTER TABLE test_add_column
	ADD COLUMN c2 integer;
\d test_add_column
ALTER TABLE test_add_column
	ADD COLUMN c2 integer; -- fail because c2 already exists
ALTER TABLE ONLY test_add_column
	ADD COLUMN c2 integer; -- fail because c2 already exists
\d test_add_column
ALTER TABLE test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer; -- skipping because c2 already exists
ALTER TABLE ONLY test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer; -- skipping because c2 already exists
\d test_add_column
ALTER TABLE test_add_column
	ADD COLUMN c2 integer, -- fail because c2 already exists
	ADD COLUMN c3 integer;
\d test_add_column
ALTER TABLE test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer, -- skipping because c2 already exists
	ADD COLUMN c3 integer; -- fail because c3 already exists
\d test_add_column
ALTER TABLE test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer, -- skipping because c2 already exists
	ADD COLUMN IF NOT EXISTS c3 integer; -- skipping because c3 already exists
\d test_add_column
ALTER TABLE test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer, -- skipping because c2 already exists
	ADD COLUMN IF NOT EXISTS c3 integer, -- skipping because c3 already exists
	ADD COLUMN c4 integer;
\d test_add_column
DROP TABLE test_add_column;

-- unsupported constraint types for partitioned tables
CREATE TABLE partitioned (
	a int,
	b int
) PARTITION BY RANGE (a, (a+b+1));

-- cannot drop column that is part of the partition key
ALTER TABLE partitioned DROP COLUMN a;
ALTER TABLE partitioned ALTER COLUMN a TYPE char(5);
ALTER TABLE partitioned DROP COLUMN b;
ALTER TABLE partitioned ALTER COLUMN b TYPE char(5);

DROP TABLE partitioned;

--
-- DETACH PARTITION
--

CREATE TABLE list_parted2 (
        a int,
        b char
) PARTITION BY LIST (a);
-- cannot add/drop column to/from *only* the parent
ALTER TABLE ONLY list_parted2 ADD COLUMN c int;
ALTER TABLE ONLY list_parted2 DROP COLUMN b;

CREATE TABLE part_2 (LIKE list_parted2);
INSERT INTO part_2 VALUES (3, 'a');
-- cannot add a column to partition or drop an inherited one
ALTER TABLE part_2 ADD COLUMN c text;
ALTER TABLE part_2 DROP COLUMN b;

-- Nor rename, alter type
ALTER TABLE part_2 RENAME COLUMN b to c;
ALTER TABLE part_2 ALTER COLUMN b TYPE text;

-- cannot add/drop NOT NULL or check constraints to *only* the parent, when
-- partitions exist
ALTER TABLE ONLY list_parted2 ALTER b SET NOT NULL;

ALTER TABLE list_parted2 ALTER b SET NOT NULL;
ALTER TABLE ONLY list_parted2 ALTER b DROP NOT NULL;

-- It's alright though, if no partitions are yet created
CREATE TABLE parted_no_parts (a int) PARTITION BY LIST (a);
ALTER TABLE ONLY parted_no_parts ALTER a SET NOT NULL;
ALTER TABLE ONLY parted_no_parts ALTER a DROP NOT NULL;
DROP TABLE parted_no_parts;

-- cannot drop inherited NOT NULL or check constraints from partition
ALTER TABLE list_parted2 ALTER b SET NOT NULL;
ALTER TABLE part_2 ALTER b DROP NOT NULL;

-- cannot drop or alter type of partition key columns of lower level
-- partitioned tables; for example, part_5, which is list_parted2's
-- partition, is partitioned on b;
ALTER TABLE list_parted2 DROP COLUMN b;
ALTER TABLE list_parted2 ALTER COLUMN b TYPE text;

-- dropping non-partition key columns should be allowed on the parent table.
ALTER TABLE list_parted DROP COLUMN b;
SELECT * FROM list_parted;

-- cleanup
DROP TABLE list_parted;
drop table list_parted2;
drop table range_parted;
DROP TABLE fail_def_part;
DROP TABLE hash_parted;

-- validate constraint on partitioned tables should only scan leaf partitions
create table parted_validate_test (a int) partition by list (a);
create table parted_validate_test_1 partition of parted_validate_test for values in (0, 1);
drop table parted_validate_test;
-- test alter column options
CREATE TABLE attmp(i integer);
INSERT INTO attmp VALUES (1);
ALTER TABLE attmp ALTER COLUMN i SET (n_distinct = 1, n_distinct_inherited = 2);
ALTER TABLE attmp ALTER COLUMN i RESET (n_distinct_inherited);
DROP TABLE attmp;

DROP USER regress_alter_table_user1;

-- test case where the partitioning operator is a SQL function whose
-- evaluation results in the table's relcache being rebuilt partway through
-- the execution of an ATTACH PARTITION command
create function at_test_sql_partop (int4, int4) returns int language sql
as $$ select case when $1 = $2 then 0 when $1 > $2 then 1 else -1 end; $$;
create operator class at_test_sql_partop for type int4 using btree as
    operator 1 < (int4, int4), operator 2 <= (int4, int4),
    operator 3 = (int4, int4), operator 4 >= (int4, int4),
    operator 5 > (int4, int4), function 1 at_test_sql_partop(int4, int4);
create table at_test_sql_partop (a int) partition by range (a at_test_sql_partop);
drop table at_test_sql_partop;
drop operator class at_test_sql_partop using btree;
drop function at_test_sql_partop;
