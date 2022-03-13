--
-- ALTER_TABLE
--

-- Clean up in case a prior regression run failed
SET client_min_messages TO 'warning';
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_alter_table_user1;
--DDL_STATEMENT_END--
RESET client_min_messages;
--DDL_STATEMENT_BEGIN--
CREATE USER regress_alter_table_user1;
--DDL_STATEMENT_END--
--
-- add attribute
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE attmp (initial int4);
--DDL_STATEMENT_END--
COMMENT ON TABLE attmp_wrong IS 'table comment';
COMMENT ON TABLE attmp IS 'table comment';
COMMENT ON TABLE attmp IS NULL;
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN xmin integer; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN a int4 default 3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN b name;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN c text;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN d float8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN e float4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN f int2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN i char;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN k int4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN l tid;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN m xid;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN v timestamp;
--DDL_STATEMENT_END--
--tid value is not supported fully: INSERT INTO attmp (a, b, c, d, e, f, i, k, l, m, v)
--   VALUES (4, 'name', 'text', 4.1, 4.1, 2, 'c', 314159, '(1,1)', '512', 'epoch');

INSERT INTO attmp (a, b, c, d, e, f, i, k, m, v)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, 'c', 314159, '512', 'epoch');

SELECT * FROM attmp;
--DDL_STATEMENT_BEGIN--
DROP TABLE attmp;
--DDL_STATEMENT_END--
-- the wolf bug - schema mods caused inconsistent row descriptors
--DDL_STATEMENT_BEGIN--
CREATE TABLE attmp (
	initial 	int4
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN a int4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN b name;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN c text;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN d float8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN e float4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN f int2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN i char;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN k int4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN l tid;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN m xid;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ADD COLUMN v timestamp;
--DDL_STATEMENT_END--

--INSERT INTO attmp (a, b, c, d, e, f, i, k, l, m, v)
--   VALUES (4, 'name', 'text', 4.1, 4.1, 2, 'c', 314159, '(1,1)', '512', 'epoch');

INSERT INTO attmp (a, b, c, d, e, f, i, k,  m, v)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, 'c', 314159, '512', 'epoch');

SELECT * FROM attmp;
--DDL_STATEMENT_BEGIN--
DROP TABLE attmp;
--DDL_STATEMENT_END--
--
-- rename - check on both non-temp and temp tables
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE attmp (regtable int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE attmp (attmptable int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp RENAME TO attmp_new;
--DDL_STATEMENT_END--
SELECT * FROM attmp;
SELECT * FROM attmp_new;
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp RENAME TO attmp_new2;
--DDL_STATEMENT_END--
SELECT * FROM attmp;		-- should fail
SELECT * FROM attmp_new;
SELECT * FROM attmp_new2;
--DDL_STATEMENT_BEGIN--
DROP TABLE attmp_new;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE attmp_new2;
--DDL_STATEMENT_END--

-- check rename of partitioned tables and indexes also
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_attmp (a int primary key) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_attmp1 PARTITION OF part_attmp FOR VALUES FROM (0) TO (100);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER INDEX part_attmp_pkey RENAME TO part_attmp_index;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER INDEX part_attmp1_pkey RENAME TO part_attmp1_index;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_attmp RENAME TO part_at2tmp;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_attmp1 RENAME TO part_at2tmp1;
--DDL_STATEMENT_END--
SET ROLE regress_alter_table_user1;
--DDL_STATEMENT_BEGIN--
ALTER INDEX part_attmp_index RENAME TO fail;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER INDEX part_attmp1_index RENAME TO fail;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_at2tmp RENAME TO fail;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_at2tmp1 RENAME TO fail;
--DDL_STATEMENT_END--
RESET ROLE;
--DDL_STATEMENT_BEGIN--
DROP TABLE part_at2tmp;
--DDL_STATEMENT_END--
-- The original test is to use name of _attmp_array, but kunlun does not
-- support name starting with _, so we change it.
--DDL_STATEMENT_BEGIN--
CREATE TABLE attmp_array (id int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp_array RENAME TO attmp_array_new;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE attmp_array_new;
--DDL_STATEMENT_END--
-- ALTER TABLE ... RENAME on non-table relations
-- renaming indexes (FIXME: this should probably test the index's functionality)
--DDL_STATEMENT_BEGIN--
ALTER INDEX IF EXISTS __onek_unique1 RENAME TO attmp_onek_unique1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER INDEX IF EXISTS __attmp_onek_unique1 RENAME TO onek_unique1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER INDEX onek_unique1 RENAME TO attmp_onek_unique1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER INDEX attmp_onek_unique1 RENAME TO onek_unique1;
--DDL_STATEMENT_END--
SET ROLE regress_alter_table_user1;
--DDL_STATEMENT_BEGIN--
ALTER INDEX onek_unique1 RENAME TO fail;  -- permission denied
--DDL_STATEMENT_END--
RESET ROLE;

-- renaming views
--DDL_STATEMENT_BEGIN--
CREATE VIEW attmp_view (unique1) AS SELECT unique1 FROM tenk1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp_view RENAME TO attmp_view_new;
--DDL_STATEMENT_END--
SET ROLE regress_alter_table_user1;
--DDL_STATEMENT_BEGIN--
ALTER VIEW attmp_view_new RENAME TO fail;  -- permission denied
--DDL_STATEMENT_END--
RESET ROLE;

-- hack to ensure we get an indexscan here
set enable_seqscan to off;
set enable_bitmapscan to off;
-- 5 values, sorted
SELECT unique1 FROM tenk1 WHERE unique1 < 5;
reset enable_seqscan;
reset enable_bitmapscan;
--DDL_STATEMENT_BEGIN--
DROP VIEW attmp_view_new;
--DDL_STATEMENT_END--
-- toast-like relation name
--DDL_STATEMENT_BEGIN--
alter table stud_emp rename to pg_toast_stud_emp;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table pg_toast_stud_emp rename to stud_emp;
--DDL_STATEMENT_END--
-- renaming index should rename constraint as well
--DDL_STATEMENT_BEGIN--
ALTER TABLE onek ADD CONSTRAINT onek_unique1_constraint UNIQUE (unique1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER INDEX onek_unique1_constraint RENAME TO onek_unique1_constraint_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE onek DROP CONSTRAINT onek_unique1_constraint_foo;
--DDL_STATEMENT_END--
-- renaming constraint should rename index as well
--DDL_STATEMENT_BEGIN--
ALTER TABLE onek ADD CONSTRAINT onek_unique1_constraint UNIQUE (unique1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP INDEX onek_unique1_constraint;  -- to see whether it's there
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE onek RENAME CONSTRAINT onek_unique1_constraint TO onek_unique1_constraint_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP INDEX onek_unique1_constraint_foo;  -- to see whether it's there
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE onek DROP CONSTRAINT onek_unique1_constraint_foo;
--DDL_STATEMENT_END--
-- renaming constraints with cache reset of target relation
--DDL_STATEMENT_BEGIN--
CREATE TABLE constraint_rename_cache (a int, PRIMARY KEY (a));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE constraint_rename_cache
  RENAME CONSTRAINT constraint_rename_cache_pkey TO constraint_rename_pkey_new;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE like_constraint_rename_cache
  (LIKE constraint_rename_cache INCLUDING ALL);
--DDL_STATEMENT_END--
\d like_constraint_rename_cache
--DDL_STATEMENT_BEGIN--
DROP TABLE constraint_rename_cache;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE like_constraint_rename_cache;
--DDL_STATEMENT_END--
-- test unique constraint adding
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int );
--DDL_STATEMENT_END--
-- add a unique constraint
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 unique (test);
--DDL_STATEMENT_END--
-- insert first value
--DDL_STATEMENT_BEGIN--
insert into atacc1 (test) values (2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- should fail
insert into atacc1 (test) values (2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- should succeed
insert into atacc1 (test) values (4);
--DDL_STATEMENT_END--
-- try adding a unique oid constraint
-- try to create duplicates via alter table using - should fail
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter column test type integer using 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- let's do one where the unique constraint fails when added
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int );
--DDL_STATEMENT_END--
-- insert soon to be failing rows
insert into atacc1 (test) values (2);
insert into atacc1 (test) values (2);
-- add a unique constraint (fails)
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 unique (test);
--DDL_STATEMENT_END--
insert into atacc1 (test) values (3);
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- let's do one where the unique constraint fails
-- because the column doesn't exist
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int );
--DDL_STATEMENT_END--
-- add a unique constraint (fails)
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 unique (test1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- something a little more complicated
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int, test2 int);
--DDL_STATEMENT_END--
-- add a unique constraint
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 unique (test, test2);
--DDL_STATEMENT_END--
-- insert initial value
insert into atacc1 (test,test2) values (4,4);
-- should fail
insert into atacc1 (test,test2) values (4,4);
-- should all succeed
insert into atacc1 (test,test2) values (4,5);
insert into atacc1 (test,test2) values (5,4);
insert into atacc1 (test,test2) values (5,5);
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- lets do some naming tests
--DDL_STATEMENT_BEGIN--
create table atacc1 (test int, test2 int, unique(test));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 add unique (test2);
--DDL_STATEMENT_END--
-- should fail for @ second one @
insert into atacc1 (test2, test) values (3, 3);
insert into atacc1 (test2, test) values (2, 3);
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--

-- test primary key constraint adding
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int );
--DDL_STATEMENT_END--
-- add a primary key constraint
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 primary key (test);
--DDL_STATEMENT_END--
-- insert first value
insert into atacc1 (test) values (2);
-- should fail
insert into atacc1 (test) values (2);
-- should succeed
insert into atacc1 (test) values (4);
-- inserting NULL should fail
insert into atacc1 (test) values(NULL);
-- try adding a second primary key (should fail)
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop constraint atacc_test1 restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- let's do one where the primary key constraint fails when added
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int );
--DDL_STATEMENT_END--
-- insert soon to be failing rows
insert into atacc1 (test) values (2);
insert into atacc1 (test) values (2);
-- add a primary key (fails)
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 primary key (test);
--DDL_STATEMENT_END--
insert into atacc1 (test) values (3);
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- let's do another one where the primary key constraint fails when added
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int );
--DDL_STATEMENT_END--
-- insert soon to be failing row
insert into atacc1 (test) values (NULL);
-- add a primary key (fails)
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 primary key (test);
--DDL_STATEMENT_END--
insert into atacc1 (test) values (3);
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- let's do one where the primary key constraint fails
-- because the column doesn't exist
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int );
--DDL_STATEMENT_END--
-- add a primary key constraint (fails)
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 primary key (test1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- adding a new column as primary key to a non-empty table.
-- should fail unless the column has a non-null default value.
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int );
--DDL_STATEMENT_END--
insert into atacc1 (test) values (0);
-- add a primary key column without a default (fails).
--DDL_STATEMENT_BEGIN--
alter table atacc1 add column test2 int primary key;
--DDL_STATEMENT_END--
-- now add a primary key column with a default (succeeds).
--DDL_STATEMENT_BEGIN--
alter table atacc1 add column test2 int default 0 primary key;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- something a little more complicated
--DDL_STATEMENT_BEGIN--
create table atacc1 ( test int, test2 int);
--DDL_STATEMENT_END--
-- add a primary key constraint
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint atacc_test1 primary key (test, test2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- try adding a second primary key - should fail
alter table atacc1 add constraint atacc_test2 primary key (test);
--DDL_STATEMENT_END--
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
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- lets do some naming tests
--DDL_STATEMENT_BEGIN--
create table atacc1 (test int, test2 int, primary key(test));
--DDL_STATEMENT_END--
-- only first should succeed
insert into atacc1 (test2, test) values (3, 3);
insert into atacc1 (test2, test) values (2, 3);
insert into atacc1 (test2, test) values (1, NULL);
drop table atacc1;

-- alter table / alter column [set/drop] not null tests
-- try altering system catalogs, should fail
--DDL_STATEMENT_BEGIN--
alter table pg_class alter column relname drop not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table pg_class alter relname set not null;
--DDL_STATEMENT_END--
-- try altering non-existent table, should fail
--DDL_STATEMENT_BEGIN--
alter table non_existent alter column bar set not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table non_existent alter column bar drop not null;
--DDL_STATEMENT_END--
-- test setting columns to null and not null and vice versa
-- test checking for null values and primary key
--DDL_STATEMENT_BEGIN--
create table atacc1 (test int not null);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 add constraint "atacc1_pkey" primary key (test);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter column test drop not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop constraint "atacc1_pkey";
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter column test drop not null;
--DDL_STATEMENT_END--
insert into atacc1 values (null);
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter test set not null;
--DDL_STATEMENT_END--
delete from atacc1;
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter test set not null;
--DDL_STATEMENT_END--
-- try altering a non-existent column, should fail
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter bar set not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter bar drop not null;
--DDL_STATEMENT_END--

-- try creating a view and altering that, should fail
--DDL_STATEMENT_BEGIN--
create view myview as select * from atacc1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table myview alter column test drop not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table myview alter column test set not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop view myview;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
-- test setting and removing default values
--DDL_STATEMENT_BEGIN--
create table def_test (
	c1	int4 default 5,
	c2	text default 'initial_default'
);
--DDL_STATEMENT_END--
insert into def_test default values;
--DDL_STATEMENT_BEGIN--
alter table def_test alter column c1 drop default;
--DDL_STATEMENT_END--
insert into def_test default values;
--DDL_STATEMENT_BEGIN--
alter table def_test alter column c2 drop default;
--DDL_STATEMENT_END--
insert into def_test default values;
--DDL_STATEMENT_BEGIN--
alter table def_test alter column c1 set default 10;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table def_test alter column c2 set default 'new_default';
--DDL_STATEMENT_END--
insert into def_test default values;
select * from def_test;

-- set defaults to an incorrect type: this should fail
--DDL_STATEMENT_BEGIN--
alter table def_test alter column c1 set default 'wrong_datatype';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table def_test alter column c2 set default 20;
--DDL_STATEMENT_END--
-- set defaults on a non-existent column: this should fail
--DDL_STATEMENT_BEGIN--
alter table def_test alter column c3 set default 30;
--DDL_STATEMENT_END--
-- set defaults on views: we need to create a view, add a rule
-- to allow insertions into it, and then alter the view to add
-- a default
--DDL_STATEMENT_BEGIN--
create view def_view_test as select * from def_test;
--DDL_STATEMENT_END--
insert into def_view_test default values;
--DDL_STATEMENT_BEGIN--
alter table def_view_test alter column c1 set default 45;
--DDL_STATEMENT_END--
insert into def_view_test default values;
--DDL_STATEMENT_BEGIN--
alter table def_view_test alter column c2 set default 'view_default';
--DDL_STATEMENT_END--
insert into def_view_test default values;
select * from def_view_test;
--DDL_STATEMENT_BEGIN--
drop view def_view_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table def_test;
--DDL_STATEMENT_END--
-- alter table / drop column tests
-- try altering system catalogs, should fail
--DDL_STATEMENT_BEGIN--
alter table pg_class drop column relname;
--DDL_STATEMENT_END--
-- try altering non-existent table, should fail
--DDL_STATEMENT_BEGIN--
alter table nosuchtable drop column bar;
--DDL_STATEMENT_END--
-- test dropping columns
--DDL_STATEMENT_BEGIN--
create table atacc1 (a int4 not null, b int4, c int4 not null, d int4);
--DDL_STATEMENT_END--
insert into atacc1 values (1, 2, 3, 4);
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop a;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop a;
--DDL_STATEMENT_END--
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
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop bar;
--DDL_STATEMENT_END--
-- try dropping the xmin column, should fail
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop xmin;
--DDL_STATEMENT_END--
-- try creating a view and altering that, should fail
--DDL_STATEMENT_BEGIN--
create view myview as select * from atacc1;
--DDL_STATEMENT_END--
select * from myview;
--DDL_STATEMENT_BEGIN--
alter table myview drop d;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop view myview;
--DDL_STATEMENT_END--
-- test some commands to make sure they fail on the dropped column
comment on column atacc1.a is 'testing';
comment on column atacc1."........pg.dropped.1........" is 'testing';
--alter table atacc1 alter a set storage plain;
--alter table atacc1 alter "........pg.dropped.1........" set storage plain;
--alter table atacc1 alter a set statistics 0;
--alter table atacc1 alter "........pg.dropped.1........" set statistics 0;
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter a set default 3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter "........pg.dropped.1........" set default 3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter a drop default;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter "........pg.dropped.1........" drop default;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter a set not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter "........pg.dropped.1........" set not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter a drop not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 alter "........pg.dropped.1........" drop not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 rename a to x;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 rename "........pg.dropped.1........" to x;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 add primary key(a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 add primary key("........pg.dropped.1........");
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 add unique(a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 add unique("........pg.dropped.1........");
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table atacc2 (id int4 unique);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table atacc2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index "testing_idx" on atacc1(a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index "testing_idx" on atacc1("........pg.dropped.1........");
--DDL_STATEMENT_END--

-- test create as and select into
insert into atacc1 values (21, 22, 23);
-- try dropping all columns
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop c;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop d;
--DDL_STATEMENT_END--
--not support to drop all table(by MySQL): alter table atacc1 drop b;
select * from atacc1;
--DDL_STATEMENT_BEGIN--

drop table atacc1;
--DDL_STATEMENT_END--

-- test constraint error reporting in presence of dropped columns
--DDL_STATEMENT_BEGIN--
create table atacc1 (id serial primary key, value int);
--DDL_STATEMENT_END--
insert into atacc1(value) values (100);
--DDL_STATEMENT_BEGIN--
alter table atacc1 drop column value;
--DDL_STATEMENT_END--
insert into atacc1(value) values (100);
insert into atacc1(id, value) values (null, 0);
--DDL_STATEMENT_BEGIN--
drop table atacc1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table p1(id int, name text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table p2(id2 int, name text, height int);
--DDL_STATEMENT_END--
-- test copy in/out
--DDL_STATEMENT_BEGIN--
create table attest (a int4, b int4, c int4);
--DDL_STATEMENT_END--
insert into attest values (1,2,3);
--DDL_STATEMENT_BEGIN--
alter table attest drop a;
--DDL_STATEMENT_END--
copy attest to stdout;
copy attest(a) to stdout;
copy attest("........pg.dropped.1........") to stdout;
copy attest from stdin;
10	11	12
\.
select * from attest;
copy attest from stdin;
21	22
\.
select * from attest;
copy attest(a) from stdin;
copy attest("........pg.dropped.1........") from stdin;
copy attest(b,c) from stdin;
31	32
\.
select * from attest;
--DDL_STATEMENT_BEGIN--
drop table attest;
--DDL_STATEMENT_END--
-- should work
--DDL_STATEMENT_BEGIN--
alter table only p1 drop column name;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- should work. Now c1.name is local and inhcount is 0.
alter table p2 drop column name;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- should work and drop the attribute in all tables
alter table p2 drop column height;
--DDL_STATEMENT_END--
-- IF EXISTS test
--not support empty table: create table dropColumnExists ();
--alter table dropColumnExists drop column non_existing; --fail
--alter table dropColumnExists drop column if exists non_existing; --succeed
--DDL_STATEMENT_BEGIN--
drop table p1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table p2 cascade;
--DDL_STATEMENT_END--

-- test that operations with a dropped column do not try to reference
-- its datatype
--DDL_STATEMENT_BEGIN--
create temp table foo (f1 text, f2 text, f3 text);
--DDL_STATEMENT_END--
insert into foo values('bb','cc','dd');
select * from foo;

select * from foo;
insert into foo values('qq','rr');
select * from foo;
update foo set f3 = 'zz';
select * from foo;
select f3,max(f1) from foo group by f3;

-- Simple tests for alter table column type
--DDL_STATEMENT_BEGIN--
alter table foo alter f1 TYPE integer; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table foo alter f1 TYPE varchar(10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table anothertab (atcol1 serial8, atcol2 boolean);
--DDL_STATEMENT_END--
insert into anothertab (atcol1, atcol2) values (default, true);
insert into anothertab (atcol1, atcol2) values (default, false);
select * from anothertab;
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column atcol1 type boolean; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column atcol1 type boolean using atcol1::int; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column atcol1 type integer;
--DDL_STATEMENT_END--
select * from anothertab;

insert into anothertab (atcol1, atcol2) values (45, null); -- fails
insert into anothertab (atcol1, atcol2) values (default, null);

select * from anothertab;
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column atcol2 type text
      using case when atcol2 is true then 'IT WAS TRUE'
                 when atcol2 is false then 'IT WAS FALSE'
                 else 'IT WAS NULL!' end;
--DDL_STATEMENT_END--
select * from anothertab;
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column atcol1 drop default;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab drop constraint anothertab_chk;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab drop constraint anothertab_chk; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab drop constraint IF EXISTS anothertab_chk; -- succeeds
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column atcol1 type boolean
        using case when atcol1 % 2 = 0 then true else false end;
--DDL_STATEMENT_END--
select * from anothertab;
--DDL_STATEMENT_BEGIN--
drop table anothertab;
--DDL_STATEMENT_END--
-- Test index handling in alter table column type (cf. bugs #15835, #15865)
--DDL_STATEMENT_BEGIN--
create table anothertab(f1 int primary key, f2 int unique,
                        f3 int, f4 int, f5 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab add unique(f1,f4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on anothertab(f2,f3);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create unique index on anothertab(f4);
--DDL_STATEMENT_END--
\d anothertab
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column f1 type bigint;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab
  alter column f2 type bigint,
  alter column f3 type bigint,
  alter column f4 type bigint;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table anothertab alter column f5 type bigint;
--DDL_STATEMENT_END--
\d anothertab

--DDL_STATEMENT_BEGIN--
drop table anothertab;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table another (f1 int, f2 text);
--DDL_STATEMENT_END--
insert into another values(1, 'one');
insert into another values(2, 'two');
insert into another values(3, 'three');

select * from another;
--DDL_STATEMENT_BEGIN--
alter table another
  alter f1 type text using f2 || ' more',
  alter f2 type bigint using f1 * 10;
--DDL_STATEMENT_END--
select * from another;
--DDL_STATEMENT_BEGIN--
drop table another;
--DDL_STATEMENT_END--
-- table's row type
--DDL_STATEMENT_BEGIN--
create table tab1 (a int, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table tab1 alter column b type varchar; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table tab1;
--DDL_STATEMENT_END--
-- Alter column type that's part of a partitioned index
--DDL_STATEMENT_BEGIN--
create table at_partitioned (a int, b varchar(50)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table at_part_1 partition of at_partitioned for values from (0) to (1000);
--DDL_STATEMENT_END--

insert into at_partitioned values (512, '0.123');

--DDL_STATEMENT_BEGIN--
create index on at_partitioned (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on at_partitioned (a);
--DDL_STATEMENT_END--
\d at_part_1
--DDL_STATEMENT_BEGIN--
alter table at_partitioned alter column b type numeric using b::numeric;
--DDL_STATEMENT_END--
\d at_part_1
--DDL_STATEMENT_BEGIN--
drop table at_partitioned;
--DDL_STATEMENT_END--
-- Alter column type when no table rewrite is required
-- Also check that comments are preserved
--DDL_STATEMENT_BEGIN--
create table at_partitioned(id int, name varchar(64), unique (id, name))
  partition by hash(id);
--DDL_STATEMENT_END--
comment on constraint at_partitioned_id_name_key on at_partitioned is 'parent constraint';
comment on index at_partitioned_id_name_key is 'parent index';
--DDL_STATEMENT_BEGIN--
create table at_partitioned_0 partition of at_partitioned
  for values with (modulus 2, remainder 0);
--DDL_STATEMENT_END--
comment on constraint at_partitioned_0_id_name_key on at_partitioned_0 is 'child 0 constraint';
comment on index at_partitioned_0_id_name_key is 'child 0 index';
--DDL_STATEMENT_BEGIN--
create table at_partitioned_1 partition of at_partitioned
  for values with (modulus 2, remainder 1);
--DDL_STATEMENT_END--
comment on constraint at_partitioned_1_id_name_key on at_partitioned_1 is 'child 1 constraint';
comment on index at_partitioned_1_id_name_key is 'child 1 index';
insert into at_partitioned values(1, 'foo');
insert into at_partitioned values(3, 'bar');

select conname, obj_description(oid, 'pg_constraint') as desc
  from pg_constraint where conname like 'at_partitioned%'
  order by conname;
--DDL_STATEMENT_BEGIN--
alter table at_partitioned alter column name type varchar(127);
--DDL_STATEMENT_END--
select conname, obj_description(oid, 'pg_constraint') as desc
  from pg_constraint where conname like 'at_partitioned%'
  order by conname;

-- Don't remove this DROP, it exposes bug #15672
--DDL_STATEMENT_BEGIN--
drop table at_partitioned;
--DDL_STATEMENT_END--
-- ALTER COLUMN TYPE with a check constraint and a child table (bug #13779)
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_inh_check (a float , b float);
--DDL_STATEMENT_END--
\d test_inh_check
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_inh_check ALTER COLUMN a TYPE numeric;
--DDL_STATEMENT_END--
\d test_inh_check
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_inh_check ALTER COLUMN b TYPE numeric;
--DDL_STATEMENT_END--
\d test_inh_check
--DDL_STATEMENT_BEGIN--
drop table test_inh_check;
--DDL_STATEMENT_END--
-- ALTER COLUMN TYPE with different schema in children
-- Bug at https://postgr.es/m/20170102225618.GA10071@telsasoft.com
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_type_diff (f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_type_diff ADD COLUMN f2 int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_type_diff ALTER COLUMN f2 TYPE bigint USING f2::bigint;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_type_diff2 (int_two int2, int_four int4, int_eight int8);
--DDL_STATEMENT_END--
INSERT INTO test_type_diff2 VALUES (1, 2, 3);
INSERT INTO test_type_diff2 VALUES (4, 5, 6);
INSERT INTO test_type_diff2 VALUES (7, 8, 9);
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_type_diff2 ALTER COLUMN int_four TYPE int8 USING int_four::int8;
--DDL_STATEMENT_END--
-- whole-row references are disallowed
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_type_diff2 ALTER COLUMN int_four TYPE int4 USING (pg_column_size(test_type_diff2));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table test_type_diff;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table test_type_diff2;
--DDL_STATEMENT_END--
-- check column addition within a view (bug #14876)
--DDL_STATEMENT_BEGIN--
create table at_base_table(id int, stuff text);
--DDL_STATEMENT_END--
insert into at_base_table values (23, 'skidoo');
--DDL_STATEMENT_BEGIN--
create view at_view_1 as select * from at_base_table bt;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view at_view_2 as select *, to_json(v1) as j from at_view_1 v1;
--DDL_STATEMENT_END--
\d+ at_view_1
\d+ at_view_2
explain (verbose, costs off) select * from at_view_2;
select * from at_view_2;
--DDL_STATEMENT_BEGIN--
create or replace view at_view_1 as select *, 2+2 as more from at_base_table bt;
--DDL_STATEMENT_END--
\d+ at_view_1
\d+ at_view_2
explain (verbose, costs off) select * from at_view_2;
select * from at_view_2;
--DDL_STATEMENT_BEGIN--
drop view at_view_2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop view at_view_1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table at_base_table;
--DDL_STATEMENT_END--
--
-- alter function
--
--DDL_STATEMENT_BEGIN--
create function test_strict(text) returns text as
    'select coalesce($1, ''got passed a null'');'
    language sql returns null on null input;
--DDL_STATEMENT_END--
select test_strict(NULL);
--DDL_STATEMENT_BEGIN--
alter function test_strict(text) called on null input;
--DDL_STATEMENT_END--
select test_strict(NULL);
--DDL_STATEMENT_BEGIN--
drop function test_strict(text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function non_strict(text) returns text as
    'select coalesce($1, ''got passed a null'');'
    language sql called on null input;
--DDL_STATEMENT_END--

select non_strict(NULL);
--DDL_STATEMENT_BEGIN--
alter function non_strict(text) returns null on null input;
--DDL_STATEMENT_END--
select non_strict(NULL);
--DDL_STATEMENT_BEGIN--
drop function non_strict(text);
--DDL_STATEMENT_END--
--
-- alter object set schema
--
--DDL_STATEMENT_BEGIN--
create schema alter1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table alter1.t1(f1 serial primary key, f2 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view alter1.v1 as select * from alter1.t1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function alter1.plus1(int) returns int as 'select $1+1' language sql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type alter1.ctype as (f1 int, f2 text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function alter1.same(alter1.ctype, alter1.ctype) returns boolean language sql
as 'select $1.f1 is not distinct from $2.f1 and $1.f2 is not distinct from $2.f2';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator alter1.=(procedure = alter1.same, leftarg  = alter1.ctype, rightarg = alter1.ctype);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator class alter1.ctype_hash_ops default for type alter1.ctype using hash as
  operator 1 alter1.=(alter1.ctype, alter1.ctype);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create conversion alter1.ascii_to_utf8 for 'sql_ascii' to 'utf8' from ascii_to_utf8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create text search parser alter1.prs(start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create text search configuration alter1.cfg(parser = alter1.prs);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create text search template alter1.tmpl(init = dsimple_init, lexize = dsimple_lexize);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create text search dictionary alter1.dict(template = alter1.tmpl);
--DDL_STATEMENT_END--
insert into alter1.t1(f2) values(11);
insert into alter1.t1(f2) values(12);
--DDL_STATEMENT_BEGIN--
alter table alter1.t1 set schema alter1; -- no-op, same schema
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table alter1.t1 set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table alter1.v1 set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter function alter1.plus1(int) set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator class alter1.ctype_hash_ops using hash set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator family alter1.ctype_hash_ops using hash set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator alter1.=(alter1.ctype, alter1.ctype) set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter function alter1.same(alter1.ctype, alter1.ctype) set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter type alter1.ctype set schema alter1; -- no-op, same schema
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter type alter1.ctype set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter conversion alter1.ascii_to_utf8 set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter text search parser alter1.prs set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter text search configuration alter1.cfg set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter text search template alter1.tmpl set schema alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter text search dictionary alter1.dict set schema alter2;
--DDL_STATEMENT_END--
-- this should succeed because nothing is left in alter1
--DDL_STATEMENT_BEGIN--
drop schema alter1;
--DDL_STATEMENT_END--
insert into alter2.t1(f2) values(13);
insert into alter2.t1(f2) values(14);

select * from alter2.t1;

select * from alter2.v1;

select alter2.plus1(41);

-- clean up
--DDL_STATEMENT_BEGIN--
drop table alter2.t1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop schema alter2 cascade;
--DDL_STATEMENT_END--
--
-- composite types
--
--DDL_STATEMENT_BEGIN--
CREATE TYPE test_type AS (a int);
--DDL_STATEMENT_END--
\d test_type
--DDL_STATEMENT_BEGIN--
ALTER TYPE nosuchtype ADD ATTRIBUTE b text; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type ADD ATTRIBUTE b text;
--DDL_STATEMENT_END--
\d test_type
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type ADD ATTRIBUTE b text; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type ALTER ATTRIBUTE b SET DATA TYPE varchar;
--DDL_STATEMENT_END--
\d test_type

--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type ALTER ATTRIBUTE b SET DATA TYPE integer;
--DDL_STATEMENT_END--
\d test_type
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type DROP ATTRIBUTE b;
--DDL_STATEMENT_END--
\d test_type
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type DROP ATTRIBUTE c; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type DROP ATTRIBUTE IF EXISTS c;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type DROP ATTRIBUTE a, ADD ATTRIBUTE d boolean;
--DDL_STATEMENT_END--
\d test_type

--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type RENAME ATTRIBUTE a TO aa;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type RENAME ATTRIBUTE d TO dd;
--DDL_STATEMENT_END--
\d test_type

--DDL_STATEMENT_BEGIN--
DROP TYPE test_type;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE test_type1 AS (a int, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type1 ALTER ATTRIBUTE b TYPE varchar; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE test_type1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE test_type2 AS (a int, b text);
--DDL_STATEMENT_END--
\d test_type2
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type2 ADD ATTRIBUTE c text; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type2 ADD ATTRIBUTE c text CASCADE;
--DDL_STATEMENT_END--
\d test_type2
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type2 ALTER ATTRIBUTE b TYPE varchar; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type2 ALTER ATTRIBUTE b TYPE varchar CASCADE;
--DDL_STATEMENT_END--
\d test_type2
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type2 DROP ATTRIBUTE b; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type2 DROP ATTRIBUTE b CASCADE;
--DDL_STATEMENT_END--
\d test_type2
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type2 RENAME ATTRIBUTE a TO aa; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_type2 RENAME ATTRIBUTE a TO aa CASCADE;
--DDL_STATEMENT_END--
\d test_type2
--DDL_STATEMENT_BEGIN--
drop type test_type2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE test_typex AS (a int, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_typex DROP ATTRIBUTE a; -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE test_typex DROP ATTRIBUTE a CASCADE;
--DDL_STATEMENT_END--
\d test_tblx
--DDL_STATEMENT_BEGIN--
DROP TYPE test_typex;
--DDL_STATEMENT_END--
--
-- IF EXISTS test
--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 ADD COLUMN f int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 ADD CONSTRAINT xxx PRIMARY KEY(f);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 ADD CHECK (f BETWEEN 0 AND 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 ALTER COLUMN f SET DEFAULT 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 RENAME COLUMN f TO f1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 SET SCHEMA alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tt8(a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA alter2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 ADD COLUMN f int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 ADD CONSTRAINT xxx PRIMARY KEY(f);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 ALTER COLUMN f SET DEFAULT 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 RENAME COLUMN f TO f1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE IF EXISTS tt8 SET SCHEMA alter2;
--DDL_STATEMENT_END--
\d alter2.tt8
--DDL_STATEMENT_BEGIN--
DROP TABLE alter2.tt8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA alter2;
--DDL_STATEMENT_END--
--
-- Check conflicts between index and CHECK constraint names
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tt9(c integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tt9 ADD UNIQUE(c);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tt9 ADD UNIQUE(c);  -- picks nonconflicting name
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tt9 ADD CONSTRAINT tt9_c_key UNIQUE(c);  -- fail, dup name
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tt9 ADD CONSTRAINT foo UNIQUE(c);  -- fail, dup name
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tt9 ADD UNIQUE(c);  -- picks nonconflicting name
--DDL_STATEMENT_END--
\d tt9
--DDL_STATEMENT_BEGIN--
DROP TABLE tt9;
--DDL_STATEMENT_END--

-- Check that comments on constraints and indexes are not lost at ALTER TABLE.
--DDL_STATEMENT_BEGIN--
CREATE TABLE comment_test (
  id int,
  positive_col int,
  indexed_col int,
  CONSTRAINT comment_test_pk PRIMARY KEY (id));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX comment_test_index ON comment_test(indexed_col);
--DDL_STATEMENT_END--
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
--DDL_STATEMENT_BEGIN--
ALTER TABLE comment_test ALTER COLUMN indexed_col SET DATA TYPE int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE comment_test ALTER COLUMN indexed_col SET DATA TYPE varchar(50);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE comment_test ALTER COLUMN id SET DATA TYPE int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE comment_test ALTER COLUMN id SET DATA TYPE varchar(50);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE comment_test ALTER COLUMN positive_col SET DATA TYPE int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE comment_test ALTER COLUMN positive_col SET DATA TYPE bigint;
--DDL_STATEMENT_END--
-- Check that the comments are intact.
SELECT col_description('comment_test'::regclass, 1) as comment;
SELECT indexrelid::regclass::text as index, obj_description(indexrelid, 'pg_class') as comment FROM pg_index where indrelid = 'comment_test'::regclass ORDER BY 1, 2;
SELECT conname as constraint, obj_description(oid, 'pg_constraint') as comment FROM pg_constraint where conrelid = 'comment_test'::regclass ORDER BY 1, 2;

-- Change column type of parent
--DDL_STATEMENT_BEGIN--
ALTER TABLE comment_test ALTER COLUMN id SET DATA TYPE varchar(50);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE comment_test ALTER COLUMN id SET DATA TYPE int USING id::integer;
--DDL_STATEMENT_END--
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
--DDL_STATEMENT_BEGIN--
CREATE TABLE pg_catalog.new_system_table();
--DDL_STATEMENT_END--
-- instead create in public first, move to catalog
--DDL_STATEMENT_BEGIN--
CREATE TABLE new_system_table(id serial primary key, othercol text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE new_system_table SET SCHEMA pg_catalog;
--DDL_STATEMENT_END--
-- XXX: it's currently impossible to move relations out of pg_catalog
--DDL_STATEMENT_BEGIN--
ALTER TABLE new_system_table SET SCHEMA public;
--DDL_STATEMENT_END--
-- move back, will be ignored -- already there
--DDL_STATEMENT_BEGIN--
ALTER TABLE new_system_table SET SCHEMA pg_catalog;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE new_system_table RENAME TO old_system_table;
--DDL_STATEMENT_END--
INSERT INTO old_system_table(othercol) VALUES ('somedata'), ('otherdata');
UPDATE old_system_table SET id = -id;
DELETE FROM old_system_table WHERE othercol = 'somedata';
delete from old_system_table;
--DDL_STATEMENT_BEGIN--
ALTER TABLE old_system_table DROP CONSTRAINT new_system_table_pkey;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE old_system_table DROP COLUMN othercol;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE old_system_table;
--DDL_STATEMENT_END--

-- test ADD COLUMN IF NOT EXISTS
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_add_column(c1 integer);
--DDL_STATEMENT_END--
\d test_add_column
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_add_column
	ADD COLUMN c2 integer;
--DDL_STATEMENT_END--
\d test_add_column
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_add_column
	ADD COLUMN c2 integer; -- fail because c2 already exists
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY test_add_column
	ADD COLUMN c2 integer; -- fail because c2 already exists
--DDL_STATEMENT_END--
\d test_add_column
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer; -- skipping because c2 already exists
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer; -- skipping because c2 already exists
--DDL_STATEMENT_END--
\d test_add_column
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_add_column
	ADD COLUMN c2 integer, -- fail because c2 already exists
	ADD COLUMN c3 integer;
--DDL_STATEMENT_END--
\d test_add_column
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer, -- skipping because c2 already exists
	ADD COLUMN c3 integer; -- fail because c3 already exists
--DDL_STATEMENT_END--
\d test_add_column
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer, -- skipping because c2 already exists
	ADD COLUMN IF NOT EXISTS c3 integer; -- skipping because c3 already exists
--DDL_STATEMENT_END--
\d test_add_column
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_add_column
	ADD COLUMN IF NOT EXISTS c2 integer, -- skipping because c2 already exists
	ADD COLUMN IF NOT EXISTS c3 integer, -- skipping because c3 already exists
	ADD COLUMN c4 integer;
--DDL_STATEMENT_END--
\d test_add_column
--DDL_STATEMENT_BEGIN--
DROP TABLE test_add_column;
--DDL_STATEMENT_END--
-- unsupported constraint types for partitioned tables
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int,
	b int
) PARTITION BY RANGE (a, (a+b+1));
--DDL_STATEMENT_END--
-- cannot drop column that is part of the partition key
--DDL_STATEMENT_BEGIN--
ALTER TABLE partitioned DROP COLUMN a;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE partitioned ALTER COLUMN a TYPE char(5);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE partitioned DROP COLUMN b;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE partitioned ALTER COLUMN b TYPE char(5);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE partitioned;
--DDL_STATEMENT_END--
--
-- DETACH PARTITION
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE list_parted2 (
        a int,
        b char
) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
-- cannot add/drop column to/from *only* the parent
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY list_parted2 ADD COLUMN c int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY list_parted2 DROP COLUMN b;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_2 (LIKE list_parted2);
--DDL_STATEMENT_END--
INSERT INTO part_2 VALUES (3, 'a');
-- cannot add a column to partition or drop an inherited one
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_2 ADD COLUMN c text;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_2 DROP COLUMN b;
--DDL_STATEMENT_END--
-- Nor rename, alter type
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_2 RENAME COLUMN b to c;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_2 ALTER COLUMN b TYPE text;
--DDL_STATEMENT_END--
-- cannot add/drop NOT NULL or check constraints to *only* the parent, when
-- partitions exist
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY list_parted2 ALTER b SET NOT NULL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE list_parted2 ALTER b SET NOT NULL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY list_parted2 ALTER b DROP NOT NULL;
--DDL_STATEMENT_END--
-- It's alright though, if no partitions are yet created
--DDL_STATEMENT_BEGIN--
CREATE TABLE parted_no_parts (a int) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY parted_no_parts ALTER a SET NOT NULL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY parted_no_parts ALTER a DROP NOT NULL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE parted_no_parts;
--DDL_STATEMENT_END--
-- cannot drop inherited NOT NULL or check constraints from partition
--DDL_STATEMENT_BEGIN--
ALTER TABLE list_parted2 ALTER b SET NOT NULL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE part_2 ALTER b DROP NOT NULL;
--DDL_STATEMENT_END--
-- cannot drop or alter type of partition key columns of lower level
-- partitioned tables; for example, part_5, which is list_parted2's
-- partition, is partitioned on b;
--DDL_STATEMENT_BEGIN--
ALTER TABLE list_parted2 DROP COLUMN b;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE list_parted2 ALTER COLUMN b TYPE text;
--DDL_STATEMENT_END--
-- dropping non-partition key columns should be allowed on the parent table.
--DDL_STATEMENT_BEGIN--
ALTER TABLE list_parted DROP COLUMN b;
--DDL_STATEMENT_END--
SELECT * FROM list_parted;

-- cleanup
--DDL_STATEMENT_BEGIN--
DROP TABLE list_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table list_parted2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table range_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE fail_def_part;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE hash_parted;
--DDL_STATEMENT_END--
-- validate constraint on partitioned tables should only scan leaf partitions
--DDL_STATEMENT_BEGIN--
create table parted_validate_test (a int) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_validate_test_1 partition of parted_validate_test for values in (0, 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table parted_validate_test;
--DDL_STATEMENT_END--
-- test alter column options
--DDL_STATEMENT_BEGIN--
CREATE TABLE attmp(i integer);
--DDL_STATEMENT_END--
INSERT INTO attmp VALUES (1);
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ALTER COLUMN i SET (n_distinct = 1, n_distinct_inherited = 2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE attmp ALTER COLUMN i RESET (n_distinct_inherited);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE attmp;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_alter_table_user1;
--DDL_STATEMENT_END--
-- test case where the partitioning operator is a SQL function whose
-- evaluation results in the table's relcache being rebuilt partway through
-- the execution of an ATTACH PARTITION command
--DDL_STATEMENT_BEGIN--
create function at_test_sql_partop (int4, int4) returns int language sql
as $$ select case when $1 = $2 then 0 when $1 > $2 then 1 else -1 end; $$;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator class at_test_sql_partop for type int4 using btree as
    operator 1 < (int4, int4), operator 2 <= (int4, int4),
    operator 3 = (int4, int4), operator 4 >= (int4, int4),
    operator 5 > (int4, int4), function 1 at_test_sql_partop(int4, int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table at_test_sql_partop (a int) partition by range (a at_test_sql_partop);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table at_test_sql_partop;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop operator class at_test_sql_partop using btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop function at_test_sql_partop;
--DDL_STATEMENT_END--