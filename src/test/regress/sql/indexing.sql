-- Creating an index on a partitioned table makes the partitions
-- automatically get the index
--DDL_STATEMENT_BEGIN--
drop table if exists idxpart;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, c text) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (10) to (100)
	partition by range (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart21 partition of idxpart2 for values from (0) to (100);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart (a);
--DDL_STATEMENT_END--
select relname, relkind, inhparent::regclass
    from pg_class left join pg_index ix on (indexrelid = oid)
	left join pg_inherits on (ix.indexrelid = inhrelid)
	where relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Some unsupported features
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, c text) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index concurrently on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Verify bugfix with query on indexed partitioned table with no partitions
-- https://postgr.es/m/20180124162006.pmapfiznhgngwtjf@alvherre.pgsql
--DDL_STATEMENT_BEGIN--
CREATE TABLE idxpart (col1 INT) PARTITION BY RANGE (col1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX ON idxpart (col1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE idxpart_two (col2 INT);
--DDL_STATEMENT_END--
SELECT col2 FROM idxpart_two fk LEFT OUTER JOIN idxpart pk ON (col1 = col2);
--DDL_STATEMENT_BEGIN--
DROP table idxpart;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart_two;
--DDL_STATEMENT_END--

-- Verify bugfix with index rewrite on ALTER TABLE / SET DATA TYPE
-- https://postgr.es/m/CAKcux6mxNCGsgATwf5CGMF8g4WSupCXicCVMeKUTuWbyxHOMsQ@mail.gmail.com
--DDL_STATEMENT_BEGIN--
CREATE TABLE idxpart (a INT, b varchar(50), c INT) PARTITION BY RANGE(a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE idxpart1 PARTITION OF idxpart FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX partidx_abc_idx ON idxpart (a, b, c);
--DDL_STATEMENT_END--
INSERT INTO idxpart (a, b, c) SELECT i, i, i FROM generate_series(1, 50) i;
--DDL_STATEMENT_BEGIN--
ALTER TABLE idxpart ALTER COLUMN c TYPE numeric;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE idxpart;
--DDL_STATEMENT_END--

-- If a table without index is attached as partition to a table with
-- an index, the index is automatically created
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, c varchar(50)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxparti on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxparti2 on idxpart (b, c);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (10);;
--DDL_STATEMENT_END--
\d idxpart1
\d idxpart1
\d+ idxpart1_a_idx
\d+ idxpart1_b_c_idx
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- If a partition already has an index, don't create a duplicative one
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int) partition by range (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0, 0) to (10, 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart1 (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart (a, b);
--DDL_STATEMENT_END--
\d idxpart1
select relname, relkind, inhparent::regclass
    from pg_class left join pg_index ix on (indexrelid = oid)
	left join pg_inherits on (ix.indexrelid = inhrelid)
	where relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- DROP behavior for partitioned indexes
--DDL_STATEMENT_BEGIN--
create table idxpart (a int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop index idxpart1_a_idx;	-- no way
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop index idxpart_a_idx;	-- both indexes go away
--DDL_STATEMENT_END--
select relname, relkind from pg_class
  where relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
create index on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart1;		-- the index on partition goes away too
--DDL_STATEMENT_END--
select relname, relkind from pg_class
  where relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- ALTER INDEX .. ATTACH, error cases
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int) partition by range (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0, 0) to (10, 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxpart_a_b_idx on only idxpart (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxpart1_a_b_idx on idxpart1 (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxpart1_tst1 on idxpart1 (b, a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxpart1_tst2 on idxpart1 using hash (a);
--DDL_STATEMENT_END--
-- create index idxpart1_tst3 on idxpart1 (a, b) where a > 10;

-- alter index idxpart attach partition idxpart1;
-- alter index idxpart_a_b_idx attach partition idxpart1;
-- alter index idxpart_a_b_idx attach partition idxpart_a_b_idx;
-- alter index idxpart_a_b_idx attach partition idxpart1_b_idx;
-- alter index idxpart_a_b_idx attach partition idxpart1_tst1;
-- alter index idxpart_a_b_idx attach partition idxpart1_tst2;
-- alter index idxpart_a_b_idx attach partition idxpart1_tst3;
-- -- OK
-- alter index idxpart_a_b_idx attach partition idxpart1_a_b_idx;
-- alter index idxpart_a_b_idx attach partition idxpart1_a_b_idx; -- quiet

-- reject dupe
create index idxpart1_2_a_b on idxpart1 (a, b);
alter index idxpart_a_b_idx attach partition idxpart1_2_a_b;

--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--
-- make sure everything's gone
select indexrelid::regclass, indrelid::regclass
  from pg_index where indexrelid::regclass::text like 'idxpart%';

-- Don't auto-attach incompatible indexes
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (1000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart1 using hash (a);
--DDL_STATEMENT_END--
create index on idxpart1 (a) where b > 1;
create index on idxpart1 ((a + 0));
--DDL_STATEMENT_BEGIN--
create index on idxpart (a);
--DDL_STATEMENT_END--
\d idxpart1
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- If CREATE INDEX ONLY, don't create indexes on partitions; and existing
-- indexes on partitions don't change parent.  ALTER INDEX ATTACH can change
-- the parent after the fact.
--DDL_STATEMENT_BEGIN--
create table idxpart (a int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (100);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (100) to (1000)
  partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart21 partition of idxpart2 for values from (100) to (200);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart22 partition of idxpart2 for values from (200) to (300);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart22 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on only idxpart2 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart (a);
--DDL_STATEMENT_END--
-- Here we expect that idxpart1 and idxpart2 have a new index, but idxpart21
-- does not; also, idxpart22 is not attached.
\d idxpart1
\d idxpart2
\d idxpart21
select indexrelid::regclass, indrelid::regclass, inhparent::regclass
  from pg_index idx left join pg_inherits inh on (idx.indexrelid = inh.inhrelid)
where indexrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
alter index idxpart2_a_idx attach partition idxpart22_a_idx;
select indexrelid::regclass, indrelid::regclass, inhparent::regclass
  from pg_index idx left join pg_inherits inh on (idx.indexrelid = inh.inhrelid)
where indexrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
-- attaching idxpart22 is not enough to set idxpart22_a_idx valid ...
alter index idxpart2_a_idx attach partition idxpart22_a_idx;
\d idxpart2
-- ... but this one is.
--DDL_STATEMENT_BEGIN--
create index on idxpart21 (a);
--DDL_STATEMENT_END--
alter index idxpart2_a_idx attach partition idxpart21_a_idx;
\d idxpart2
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- When a table is attached a partition and it already has an index, a
-- duplicate index should not get created, but rather the index becomes
-- attached to the parent's index.
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, c varchar(50)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxparti on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxparti2 on idxpart (b, c);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 (like idxpart including indexes);
\d idxpart1
select relname, relkind, inhparent::regclass
    from pg_class left join pg_index ix on (indexrelid = oid)
	left join pg_inherits on (ix.indexrelid = inhrelid)
	where relname like 'idxpart%' order by relname;
alter table idxpart attach partition idxpart1 for values from (0) to (10);
--DDL_STATEMENT_END--
\d idxpart1
select relname, relkind, inhparent::regclass
    from pg_class left join pg_index ix on (indexrelid = oid)
	left join pg_inherits on (ix.indexrelid = inhrelid)
	where relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Verify that attaching an invalid index does not mark the parent index valid.
-- On the other hand, attaching a valid index marks not only its direct
-- ancestor valid, but also any indirect ancestor that was only missing the one
-- that was just made valid
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (1) to (1000) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart11 partition of idxpart1 for values from (1) to (100);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on only idxpart1 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on only idxpart (a);
--DDL_STATEMENT_END--
-- this results in two invalid indexes:
select relname, indisvalid from pg_class join pg_index on indexrelid = oid
   where relname like 'idxpart%' order by relname;
-- idxpart1_a_idx is not valid, so idxpart_a_idx should not become valid:
alter index idxpart_a_idx attach partition idxpart1_a_idx;
select relname, indisvalid from pg_class join pg_index on indexrelid = oid
   where relname like 'idxpart%' order by relname;
-- after creating and attaching this, both idxpart1_a_idx and idxpart_a_idx
-- should become valid
--DDL_STATEMENT_BEGIN--
create index on idxpart11 (a);
--DDL_STATEMENT_END--
alter index idxpart1_a_idx attach partition idxpart11_a_idx;
select relname, indisvalid from pg_class join pg_index on indexrelid = oid
   where relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- verify dependency handling during ALTER TABLE DETACH PARTITION
--DDL_STATEMENT_BEGIN--
create table idxpart (a int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0000) to (1000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart1 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (1000) to (2000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart3 partition of idxpart for values from (2000) to (3000);
--DDL_STATEMENT_END--
select relname, relkind from pg_class where relname like 'idxpart%' order by relname;
-- a) after detaching partitions, the indexes can be dropped independently
alter table idxpart detach partition idxpart1;
alter table idxpart detach partition idxpart2;
alter table idxpart detach partition idxpart3;
drop index idxpart1_a_idx;

--DDL_STATEMENT_BEGIN--
drop index idxpart2_a_idx;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop index idxpart3_a_idx;
--DDL_STATEMENT_END--
select relname, relkind from pg_class where relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart3;
--DDL_STATEMENT_END--
select relname, relkind from pg_class where relname like 'idxpart%' order by relname;

--DDL_STATEMENT_BEGIN--
create table idxpart (a int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0000) to (1000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart1 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (1000) to (2000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart3 partition of idxpart for values from (2000) to (3000);
--DDL_STATEMENT_END--
-- b) after detaching, dropping the index on parent does not remove the others
select relname, relkind from pg_class where relname like 'idxpart%' order by relname;
alter table idxpart detach partition idxpart1;
alter table idxpart detach partition idxpart2;
alter table idxpart detach partition idxpart3;
drop index idxpart_a_idx;

select relname, relkind from pg_class where relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart3;
--DDL_STATEMENT_END--
select relname, relkind from pg_class where relname like 'idxpart%' order by relname;

-- Verify that expression indexes inherit correctly
create table idxpart (a int, b int) partition by range (a);
create table idxpart1 (like idxpart);
create index on idxpart1 ((a + b));
create index on idxpart ((a + b));
create table idxpart2 (like idxpart);
alter table idxpart attach partition idxpart1 for values from (0000) to (1000);
					 
					   
alter table idxpart attach partition idxpart2 for values from (1000) to (2000);
					 
		
--DDL_STATEMENT_BEGIN--
create table idxpart3 partition of idxpart for values from (2000) to (3000);
--DDL_STATEMENT_END--
select relname as child, inhparent::regclass as parent, pg_get_indexdef as childdef
  from pg_class join pg_inherits on inhrelid = oid,
  lateral pg_get_indexdef(pg_class.oid)
  where relkind in ('i', 'I') and relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Verify behavior for collation (mis)matches
					   
create table idxpart (a text) partition by range (a);
					 
					   
create table idxpart1 (like idxpart);
					 
					   
create table idxpart2 (like idxpart);
					 
create index on idxpart2 (a collate "POSIX");
					   
create index on idxpart2 (a);
					 
create index on idxpart2 (a collate "C");
alter table idxpart attach partition idxpart1 for values from ('aaa') to ('bbb');
alter table idxpart attach partition idxpart2 for values from ('bbb') to ('ccc');
create table idxpart3 partition of idxpart for values from ('ccc') to ('ddd');
					 
create index on idxpart (a collate "C");
					   
create table idxpart4 partition of idxpart for values from ('ddd') to ('eee');
					 
select relname as child, inhparent::regclass as parent, pg_get_indexdef as childdef
  from pg_class left join pg_inherits on inhrelid = oid,
  lateral pg_get_indexdef(pg_class.oid)
  where relkind in ('i', 'I') and relname like 'idxpart%' order by relname;
					   
drop table idxpart;
					 

-- Verify behavior for opclass (mis)matches
--DDL_STATEMENT_BEGIN--
create table idxpart (a varchar(50)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from ('aaa') to ('bbb');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from ('bbb') to ('ccc');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart2 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart3 partition of idxpart for values from ('ccc') to ('ddd');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart4 partition of idxpart for values from ('ddd') to ('eee');
--DDL_STATEMENT_END--
-- must *not* have attached the index we created on idxpart2
select relname as child, inhparent::regclass as parent, pg_get_indexdef as childdef
  from pg_class left join pg_inherits on inhrelid = oid,
  lateral pg_get_indexdef(pg_class.oid)
  where relkind in ('i', 'I') and relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop index idxpart_a_idx;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on only idxpart (a text_pattern_ops);
--DDL_STATEMENT_END--
-- must reject
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Verify that attaching indexes maps attribute numbers correctly
--DDL_STATEMENT_BEGIN--
create table idxpart (col1 int, a int, col2 int, b int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart drop column col1, drop column col2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxpart_1_idx on only idxpart (b, a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxpart1_1_idx on idxpart1 (b, a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxpart1_1b_idx on idxpart1 (b);
--DDL_STATEMENT_END--
-- test expressions and partial-index predicate, too
select relname as child, inhparent::regclass as parent, pg_get_indexdef as childdef
  from pg_class left join pg_inherits on inhrelid = oid,
  lateral pg_get_indexdef(pg_class.oid)
  where relkind in ('i', 'I') and relname like 'idxpart%' order by relname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Make sure the partition columns are mapped correctly
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, c varchar(50)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxparti on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index idxparti2 on idxpart (c, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (10) to (20);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart2 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart2 (c, b);
--DDL_STATEMENT_END--
select c.relname, pg_get_indexdef(indexrelid)
  from pg_class c join pg_index i on c.oid = i.indexrelid
  where indrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Verify that columns are mapped correctly in expression indexes
create table idxpart (col1 int, col2 int, a int, b int) partition by range (a);
					 
					   
create table idxpart1 (col2 int, b int, col1 int, a int);
					 
					   
create table idxpart2 (col1 int, col2 int, b int, a int);
					 
					   
alter table idxpart drop column col1, drop column col2;
alter table idxpart1 drop column col1, drop column col2;
alter table idxpart2 drop column col1, drop column col2;
create index on idxpart2 (abs(b));
alter table idxpart attach partition idxpart2 for values from (0) to (1);
create index on idxpart (abs(b));
create index on idxpart ((b + 1));
alter table idxpart attach partition idxpart1 for values from (1) to (2);
select c.relname, pg_get_indexdef(indexrelid)
  from pg_class c join pg_index i on c.oid = i.indexrelid
  where indrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--
-- Verify that columns are mapped correctly for WHERE in a partial index
--DDL_STATEMENT_BEGIN--
create table idxpart (col1 int, a int, col3 int, b int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart drop column col1, drop column col3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (1000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (1000) to (2000);
--DDL_STATEMENT_END--
select c.relname, pg_get_indexdef(indexrelid)
  from pg_class c join pg_index i on c.oid = i.indexrelid
  where indrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
drop table idxpart;
-- Column number mapping: dropped columns in the partition
create table idxpart1 (drop_1 int, drop_2 int, col_keep int, drop_3 int);
alter table idxpart1 drop column drop_1;
alter table idxpart1 drop column drop_2;
alter table idxpart1 drop column drop_3;
create index on idxpart1 (col_keep);
create table idxpart (col_keep int) partition by range (col_keep);
create index on idxpart (col_keep);
alter table idxpart attach partition idxpart1 for values from (0) to (1000);
\d idxpart
\d idxpart1
select attrelid::regclass, attname, attnum from pg_attribute
  where attrelid::regclass::text like 'idxpart%' and attnum > 0
  order by attrelid::regclass, attnum;
drop table idxpart;

-- Column number mapping: dropped columns in the parent table
create table idxpart(drop_1 int, drop_2 int, col_keep int, drop_3 int) partition by range (col_keep);
alter table idxpart drop column drop_1;
alter table idxpart drop column drop_2;
alter table idxpart drop column drop_3;
create table idxpart1 (col_keep int);
create index on idxpart1 (col_keep);
create index on idxpart (col_keep);
alter table idxpart attach partition idxpart1 for values from (0) to (1000);
\d idxpart
\d idxpart1
select attrelid::regclass, attname, attnum from pg_attribute
  where attrelid::regclass::text like 'idxpart%' and attnum > 0
  order by attrelid::regclass, attnum;
drop table idxpart;


--
-- Constraint-related indexes
--

-- Verify that it works to add primary key / unique to partitioned tables
--DDL_STATEMENT_BEGIN--
create table idxpart (a int primary key, b int) partition by range (a);
--DDL_STATEMENT_END--
\d idxpart
-- multiple primary key on child should fail
--DDL_STATEMENT_BEGIN--
create table failpart partition of idxpart (b primary key) for values from (0) to (100);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--
-- primary key on child is okay if there's no PK in the parent, though
--DDL_STATEMENT_BEGIN--
create table idxpart (a int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1pk partition of idxpart (a primary key) for values from (0) to (100);
--DDL_STATEMENT_END--
\d idxpart1pk
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Failing to use the full partition key is not allowed
--DDL_STATEMENT_BEGIN--
create table idxpart (a int unique, b int) partition by range (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int unique) partition by range (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart (a int primary key, b int) partition by range (b, a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int primary key) partition by range (b, a);
--DDL_STATEMENT_END--

-- OK if you use them in some other order
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, c varchar(50), primary key  (a, b, c)) partition by range (b, c, a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- not other types of index-based constraints
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, exclude (a with = )) partition by range (a);
--DDL_STATEMENT_END--

-- no expressions in partition key for PK/UNIQUE
--DDL_STATEMENT_BEGIN--
create table idxpart (a int primary key, b int) partition by range ((b + a));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart (a int unique, b int) partition by range ((b + a));
--DDL_STATEMENT_END--

-- use ALTER TABLE to add a primary key
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, c text) partition by range (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart add primary key (a);	-- not an incomplete one though
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart add primary key (a, b);	-- this works
--DDL_STATEMENT_END--
\d idxpart
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0, 0) to (1000, 1000);
--DDL_STATEMENT_END--
\d idxpart1
drop table idxpart;

-- use ALTER TABLE to add a unique constraint
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int) partition by range (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart add unique (a);			-- not an incomplete one though
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart add unique (b, a);		-- this works
--DDL_STATEMENT_END--
\d idxpart
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Exclusion constraints cannot be added
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart add exclude (a with =);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- When (sub)partitions are created, they also contain the constraint
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, primary key (a, b)) partition by range (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (1, 1) to (10, 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (10, 10) to (20, 20)
  partition by range (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart21 partition of idxpart2 for values from (10) to (15);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart22 partition of idxpart2 for values from (15) to (20);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart3 partition of idxpart for values from from (20, 20) to (30, 30);
--DDL_STATEMENT_END--
select conname, contype, conrelid::regclass, conindid::regclass, conkey
  from pg_constraint where conrelid::regclass::text like 'idxpart%'
  order by conname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Verify that multi-layer partitioning honors the requirement that all
-- columns in the partition key must appear in primary/unique key
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, primary key (a)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart
--DDL_STATEMENT_END--
for values from (0) to (1000) partition by range (b); -- fail
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- Multi-layer partitioning works correctly in this case:
--DDL_STATEMENT_BEGIN--
create table idxpart (a int, b int, primary key (a, b)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (0) to (1000) partition by range (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart21 partition of idxpart2 for values from (0) to (1000);
--DDL_STATEMENT_END--
select conname, contype, conrelid::regclass, conindid::regclass, conkey
  from pg_constraint where conrelid::regclass::text like 'idxpart%'
  order by conname;
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- If a partitioned table has a unique/PK constraint, then it's not possible
-- to drop the corresponding constraint in the children; nor it's possible
-- to drop the indexes individually.  Dropping the constraint in the parent
-- gets rid of the lot.
--DDL_STATEMENT_BEGIN--
create table idxpart (i int) partition by hash (i);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart0 partition of idxpart (i) for values with (modulus 2, remainder 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart (i) for values with (modulus 2, remainder 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart0 add primary key(i);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart add primary key(i);
--DDL_STATEMENT_END--
select indrelid::regclass, indexrelid::regclass, inhparent::regclass, indisvalid,
  conname, conislocal, coninhcount, connoinherit, convalidated
  from pg_index idx left join pg_inherits inh on (idx.indexrelid = inh.inhrelid)
  left join pg_constraint con on (idx.indexrelid = con.conindid)
  where indrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
--DDL_STATEMENT_BEGIN--
drop index idxpart0_pkey;								-- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop index idxpart1_pkey;								-- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart0 drop constraint idxpart0_pkey;		-- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart1 drop constraint idxpart1_pkey;		-- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table idxpart drop constraint idxpart_pkey;		-- ok
--DDL_STATEMENT_END--
select indrelid::regclass, indexrelid::regclass, inhparent::regclass, indisvalid,
  conname, conislocal, coninhcount, connoinherit, convalidated
  from pg_index idx left join pg_inherits inh on (idx.indexrelid = inh.inhrelid)
  left join pg_constraint con on (idx.indexrelid = con.conindid)
  where indrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
drop table idxpart;

-- If the partition to be attached already has a primary key, fail if
-- it doesn't match the parent's PK.
CREATE TABLE idxpart (c1 INT PRIMARY KEY, c2 INT, c3 VARCHAR(10)) PARTITION BY RANGE(c1);
CREATE TABLE idxpart1 (LIKE idxpart);
ALTER TABLE idxpart1 ADD PRIMARY KEY (c1, c2);
ALTER TABLE idxpart ATTACH PARTITION idxpart1 FOR VALUES FROM (100) TO (200);
DROP TABLE idxpart, idxpart1;

-- Ditto if there is some distance between the PKs (subpartitioning)
create table idxpart (a int, b int, primary key (a)) partition by range (a);
create table idxpart1 (a int not null, b int) partition by range (a);
create table idxpart11 (a int not null, b int primary key);
alter table idxpart1 attach partition idxpart11 for values from (0) to (1000);
alter table idxpart attach partition idxpart1 for values from (0) to (10000);
drop table idxpart, idxpart1, idxpart11;

-- If a partitioned table has a constraint whose index is not valid,
-- attaching a missing partition makes it valid.
create table idxpart (a int) partition by range (a);
create table idxpart0 (like idxpart);
alter table idxpart0 add primary key (a);
alter table idxpart attach partition idxpart0 for values from (0) to (1000);
alter table only idxpart add primary key (a);
select indrelid::regclass, indexrelid::regclass, inhparent::regclass, indisvalid,
  conname, conislocal, coninhcount, connoinherit, convalidated
  from pg_index idx left join pg_inherits inh on (idx.indexrelid = inh.inhrelid)
  left join pg_constraint con on (idx.indexrelid = con.conindid)
  where indrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
alter index idxpart_pkey attach partition idxpart0_pkey;
select indrelid::regclass, indexrelid::regclass, inhparent::regclass, indisvalid,
  conname, conislocal, coninhcount, connoinherit, convalidated
  from pg_index idx left join pg_inherits inh on (idx.indexrelid = inh.inhrelid)
  left join pg_constraint con on (idx.indexrelid = con.conindid)
  where indrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
drop table idxpart;

-- if a partition has a unique index without a constraint, does not attach
-- automatically; creates a new index instead.
create table idxpart (a int, b int) partition by range (a);
create table idxpart1 (a int not null, b int);
create unique index on idxpart1 (a);
alter table idxpart add primary key (a);
alter table idxpart attach partition idxpart1 for values from (1) to (1000);
select indrelid::regclass, indexrelid::regclass, inhparent::regclass, indisvalid,
  conname, conislocal, coninhcount, connoinherit, convalidated
  from pg_index idx left join pg_inherits inh on (idx.indexrelid = inh.inhrelid)
  left join pg_constraint con on (idx.indexrelid = con.conindid)
  where indrelid::regclass::text like 'idxpart%'
  order by indexrelid::regclass::text collate "C";
drop table idxpart;
-- Can't attach an index without a corresponding constraint
create table idxpart (a int, b int) partition by range (a);
create table idxpart1 (a int not null, b int);
create unique index on idxpart1 (a);
alter table idxpart attach partition idxpart1 for values from (1) to (1000);
alter table only idxpart add primary key (a);
alter index idxpart_pkey attach partition idxpart1_a_idx;	-- fail
drop table idxpart;


-- Test that unique constraints are working
					   
create table idxpart (a int, b text, primary key (a, b)) partition by range (a);
					 
					   
create table idxpart1 partition of idxpart for values from (0) to (100000);
create table idxpart2 (c int, like idxpart);
insert into idxpart2 (c, a, b) values (42, 572814, 'inserted first');
alter table idxpart2 drop column c;
create unique index on idxpart (a);
alter table idxpart attach partition idxpart2 for values from (100000) to (1000000);
insert into idxpart values (0, 'zero'), (42, 'life'), (2^16, 'sixteen');
insert into idxpart select 2^g, format('two to power of %s', g) from generate_series(15, 17) g;
insert into idxpart values (16, 'sixteen');
insert into idxpart (b, a) values ('one', 142857), ('two', 285714);
insert into idxpart select a * 2, b || b from idxpart where a between 2^16 and 2^19;
insert into idxpart values (572814, 'five');
insert into idxpart values (857142, 'six');
select tableoid::regclass, * from idxpart order by a;
drop table idxpart;


-- test fastpath mechanism for index insertion
--DDL_STATEMENT_BEGIN--
drop table if exists fastpath;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table fastpath (a int, b varchar(50), c numeric);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create unique index fpindex1 on fastpath(a);
--DDL_STATEMENT_END--

insert into fastpath values (1, 'b1', 100.00);
insert into fastpath values (1, 'b1', 100.00); -- unique key check

truncate fastpath;
insert into fastpath select generate_series(1,10000), 'b', 100;

set enable_seqscan to false;
set enable_bitmapscan to false;

select sum(a) from fastpath where a = 6456;
select sum(a) from fastpath where a >= 5000 and a < 5700;

-- drop the only index on the table and compute hashes for
-- a few queries which orders the results in various different ways.
--DDL_STATEMENT_BEGIN--
drop index fpindex1;
--DDL_STATEMENT_END--
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- now create a multi-column index with both column asc
--DDL_STATEMENT_BEGIN--
create index fpindex2 on fastpath(a, b);
--DDL_STATEMENT_END--
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- same queries with a different kind of index now. the final result must not
-- change irrespective of what kind of index we have.
--DDL_STATEMENT_BEGIN--
drop index fpindex2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index fpindex3 on fastpath(a desc, b asc);
--DDL_STATEMENT_END--
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- repeat again
--DDL_STATEMENT_BEGIN--
drop index fpindex3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index fpindex4 on fastpath(a asc, b desc);
--DDL_STATEMENT_END--
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- and again, this time indexing by (b, a). Note that column "b" has non-unique
-- values.
--DDL_STATEMENT_BEGIN--
drop index fpindex4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index fpindex5 on fastpath(b asc, a desc);
--DDL_STATEMENT_END--
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- one last time
--DDL_STATEMENT_BEGIN--
drop index fpindex5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index fpindex6 on fastpath(b desc, a desc);
--DDL_STATEMENT_END--
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
	
--DDL_STATEMENT_BEGIN--
drop table fastpath;
--DDL_STATEMENT_END--

-- intentionally leave some objects around
--DDL_STATEMENT_BEGIN--
drop table if exists idxpart;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart (a int) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart1 partition of idxpart for values from (0) to (100);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart2 partition of idxpart for values from (100) to (1000)
  partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart21 partition of idxpart2 for values from (100) to (200);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart22 partition of idxpart2 for values from (200) to (300);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart22 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on only idxpart2 (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on idxpart (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart_another (a int, b int, primary key (a, b)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart_another_1 partition of idxpart_another for values from (0) to (100);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart3 partition of idxpart for values from (1000) to (2000) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart31 partition of idxpart3 for values from (1000) to (1200);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table idxpart32 partition of idxpart3 for values from (1200) to (1400);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart_another;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table idxpart;
--DDL_STATEMENT_END--

-- check that detaching a partition also detaches the primary key constraint
--DDL_STATEMENT_BEGIN--
drop table if exists parted_pk_detach_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_pk_detach_test (a int primary key) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_pk_detach_test1 partition of parted_pk_detach_test for values in (1);
--DDL_STATEMENT_END--

alter table parted_pk_detach_test1 drop constraint parted_pk_detach_test1_pkey;	-- should fail
alter table parted_pk_detach_test detach partition parted_pk_detach_test1;
alter table parted_pk_detach_test1 drop constraint parted_pk_detach_test1_pkey;

--DDL_STATEMENT_BEGIN--
drop table parted_pk_detach_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table parted_pk_detach_test1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_uniq_detach_test (a int unique) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_uniq_detach_test1 partition of parted_uniq_detach_test for values in (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table parted_uniq_detach_test1 drop constraint parted_uniq_detach_test1_a_key;	-- should fail
--DDL_STATEMENT_END--
alter table parted_uniq_detach_test detach partition parted_uniq_detach_test1;
alter table parted_uniq_detach_test1 drop constraint parted_uniq_detach_test1_a_key;

drop table parted_uniq_detach_test;

--DDL_STATEMENT_BEGIN--
drop table parted_uniq_detach_test1;
--DDL_STATEMENT_END--