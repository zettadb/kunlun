--
-- Test inheritance features
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE a (aa TEXT);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE b (bb TEXT) INHERITS (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE c (cc TEXT) INHERITS (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE d (dd TEXT) INHERITS (b,c,a);
--DDL_STATEMENT_END--

INSERT INTO a(aa) VALUES('aaa');
INSERT INTO a(aa) VALUES('aaaa');
INSERT INTO a(aa) VALUES('aaaaa');
INSERT INTO a(aa) VALUES('aaaaaa');
INSERT INTO a(aa) VALUES('aaaaaaa');
INSERT INTO a(aa) VALUES('aaaaaaaa');

INSERT INTO b(aa) VALUES('bbb');
INSERT INTO b(aa) VALUES('bbbb');
INSERT INTO b(aa) VALUES('bbbbb');
INSERT INTO b(aa) VALUES('bbbbbb');
INSERT INTO b(aa) VALUES('bbbbbbb');
INSERT INTO b(aa) VALUES('bbbbbbbb');

INSERT INTO c(aa) VALUES('ccc');
INSERT INTO c(aa) VALUES('cccc');
INSERT INTO c(aa) VALUES('ccccc');
INSERT INTO c(aa) VALUES('cccccc');
INSERT INTO c(aa) VALUES('ccccccc');
INSERT INTO c(aa) VALUES('cccccccc');

INSERT INTO d(aa) VALUES('ddd');
INSERT INTO d(aa) VALUES('dddd');
INSERT INTO d(aa) VALUES('ddddd');
INSERT INTO d(aa) VALUES('dddddd');
INSERT INTO d(aa) VALUES('ddddddd');
INSERT INTO d(aa) VALUES('dddddddd');

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

UPDATE a SET aa='zzzz' WHERE aa='aaaa';
UPDATE ONLY a SET aa='zzzzz' WHERE aa='aaaaa';
UPDATE b SET aa='zzz' WHERE aa='aaa';
UPDATE ONLY b SET aa='zzz' WHERE aa='aaa';
UPDATE a SET aa='zzzzzz' WHERE aa LIKE 'aaa%';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

UPDATE b SET aa='new';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

UPDATE a SET aa='new';

DELETE FROM ONLY c WHERE aa='new';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

DELETE FROM a;

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

-- Confirm PRIMARY KEY adds NOT NULL constraint to child table
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE z (b TEXT, PRIMARY KEY(aa, b)) inherits (a);
--DDL_STATEMENT_END--
INSERT INTO z VALUES (NULL, 'text'); -- should fail

-- Check inherited UPDATE with all children excluded
--DDL_STATEMENT_BEGIN--
create table some_tab (a int, b int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table some_tab_child () inherits (some_tab);
--DDL_STATEMENT_END--
insert into some_tab_child values(1,2);

explain (verbose, costs off)
update some_tab set a = a + 1 where false;
update some_tab set a = a + 1 where false;
explain (verbose, costs off)
update some_tab set a = a + 1 where false returning b, a;
update some_tab set a = a + 1 where false returning b, a;
table some_tab;
--DDL_STATEMENT_BEGIN--
drop table some_tab cascade;
--DDL_STATEMENT_END--
-- Check UPDATE with inherited target and an inherited source table
--DDL_STATEMENT_BEGIN--
create temp table foo(f1 int, f2 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table foo2(f3 int) inherits (foo);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table bar(f1 int, f2 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table bar2(f3 int) inherits (bar);
--DDL_STATEMENT_END--

insert into foo values(1,1);
insert into foo values(3,3);
insert into foo2 values(2,2,2);
insert into foo2 values(3,3,3);
insert into bar values(1,1);
insert into bar values(2,2);
insert into bar values(3,3);
insert into bar values(4,4);
insert into bar2 values(1,1,1);
insert into bar2 values(2,2,2);
insert into bar2 values(3,3,3);
insert into bar2 values(4,4,4);

update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

select tableoid::regclass::text as relname, bar.* from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

select tableoid::regclass::text as relname, bar.* from bar order by 1,2;

-- Check UPDATE with *partitioned* inherited target and an appendrel subquery
--DDL_STATEMENT_BEGIN--
create table some_tab (a int);
--DDL_STATEMENT_END--
insert into some_tab values (0);
--DDL_STATEMENT_BEGIN--
create table some_tab_child () inherits (some_tab);\
--DDL_STATEMENT_END--
insert into some_tab_child values (1);
--DDL_STATEMENT_BEGIN--
create table parted_tab (a int, b char) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_tab_part1 partition of parted_tab for values in (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_tab_part2 partition of parted_tab for values in (2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_tab_part3 partition of parted_tab for values in (3);
--DDL_STATEMENT_END--

insert into parted_tab values (1, 'a'), (2, 'a'), (3, 'a');

update parted_tab set b = 'b'
from
  (select a from some_tab union all select a+1 from some_tab) ss (a)
where parted_tab.a = ss.a;
select tableoid::regclass::text as relname, parted_tab.* from parted_tab order by 1,2;

truncate parted_tab;
insert into parted_tab values (1, 'a'), (2, 'a'), (3, 'a');
update parted_tab set b = 'b'
from
  (select 0 from parted_tab union all select 1 from parted_tab) ss (a)
where parted_tab.a = ss.a;
select tableoid::regclass::text as relname, parted_tab.* from parted_tab order by 1,2;

-- modifies partition key, but no rows will actually be updated
explain update parted_tab set a = 2 where false;
--DDL_STATEMENT_BEGIN--
drop table parted_tab;
--DDL_STATEMENT_END--
-- Check UPDATE with multi-level partitioned inherited target
--DDL_STATEMENT_BEGIN--
create table mlparted_tab (a int, b char, c text) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mlparted_tab_part1 partition of mlparted_tab for values in (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mlparted_tab_part2 partition of mlparted_tab for values in (2) partition by list (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mlparted_tab_part3 partition of mlparted_tab for values in (3);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mlparted_tab_part2a partition of mlparted_tab_part2 for values in ('a');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mlparted_tab_part2b partition of mlparted_tab_part2 for values in ('b');
--DDL_STATEMENT_END--
insert into mlparted_tab values (1, 'a'), (2, 'a'), (2, 'b'), (3, 'a');

update mlparted_tab mlp set c = 'xxx'
from
  (select a from some_tab union all select a+1 from some_tab) ss (a)
where (mlp.a = ss.a and mlp.b = 'b') or mlp.a = 3;
select tableoid::regclass::text as relname, mlparted_tab.* from mlparted_tab order by 1,2;
--DDL_STATEMENT_BEGIN--
drop table mlparted_tab;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table some_tab cascade;
--DDL_STATEMENT_END--

/* Test multiple inheritance of column defaults */
--DDL_STATEMENT_BEGIN--
CREATE TABLE firstparent (tomorrow date default now()::date + 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE secondparent (tomorrow date default  now() :: date  +  1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE jointchild () INHERITS (firstparent, secondparent);  -- ok
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE thirdparent (tomorrow date default now()::date - 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE otherchild () INHERITS (firstparent, thirdparent);  -- not ok
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE otherchild (tomorrow date default now())
  INHERITS (firstparent, thirdparent);  -- ok, child resolves ambiguous default
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE firstparent, secondparent, jointchild, thirdparent, otherchild;
--DDL_STATEMENT_END--
-- Test changing the type of inherited columns
insert into d values('test','one','two','three');
--DDL_STATEMENT_BEGIN--
alter table a alter column aa type integer using bit_length(aa);
--DDL_STATEMENT_END--
select * from d;

-- check that oid column is handled properly during alter table inherit
--DDL_STATEMENT_BEGIN--
create table oid_parent (a int) with oids;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table oid_child () inherits (oid_parent);
--DDL_STATEMENT_END--
select attinhcount, attislocal from pg_attribute
  where attrelid = 'oid_child'::regclass and attname = 'oid';
--DDL_STATEMENT_BEGIN--
drop table oid_child;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table oid_child (a int) without oids;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table oid_child inherit oid_parent;  -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table oid_child set with oids;
--DDL_STATEMENT_END--
select attinhcount, attislocal from pg_attribute
  where attrelid = 'oid_child'::regclass and attname = 'oid';
--DDL_STATEMENT_BEGIN--
alter table oid_child inherit oid_parent;
--DDL_STATEMENT_END--
select attinhcount, attislocal from pg_attribute
  where attrelid = 'oid_child'::regclass and attname = 'oid';
--DDL_STATEMENT_BEGIN--
alter table oid_child set without oids;  -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table oid_parent set without oids;
--DDL_STATEMENT_END--
select attinhcount, attislocal from pg_attribute
  where attrelid = 'oid_child'::regclass and attname = 'oid';
--DDL_STATEMENT_BEGIN--
alter table oid_child set without oids;
--DDL_STATEMENT_END--
select attinhcount, attislocal from pg_attribute
  where attrelid = 'oid_child'::regclass and attname = 'oid';
--DDL_STATEMENT_BEGIN--
drop table oid_parent cascade;
--DDL_STATEMENT_END--
-- Test non-inheritable parent constraints
--DDL_STATEMENT_BEGIN--
create table p1(ff1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table p1 add constraint p1chk check (ff1 > 0) no inherit;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table p1 add constraint p2chk check (ff1 > 10);
--DDL_STATEMENT_END--
-- connoinherit should be true for NO INHERIT constraint
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.connoinherit from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname = 'p1' order by 1,2;

-- Test that child does not inherit NO INHERIT constraints
--DDL_STATEMENT_BEGIN--
create table c1 () inherits (p1);
--DDL_STATEMENT_END--
\d p1
\d c1

-- Test that child does not override inheritable constraints of the parent
--DDL_STATEMENT_BEGIN--
create table c2 (constraint p2chk check (ff1 > 10) no inherit) inherits (p1);	--fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table p1 cascade;
--DDL_STATEMENT_END--

-- Tests for casting between the rowtypes of parent and child
-- tables. See the pgsql-hackers thread beginning Dec. 4/04
--DDL_STATEMENT_BEGIN--
create table base (i integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table derived () inherits (base);
--DDL_STATEMENT_END--
insert into derived (i) values (0);
select derived::base from derived;
select NULL::derived::base;
--DDL_STATEMENT_BEGIN--
drop table derived;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table base;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table p1(ff1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table p2(f1 text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function p2text(p2) returns text as 'select $1.f1' language sql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table c1(f3 int) inherits(p1,p2);
--DDL_STATEMENT_END--
insert into c1 values(123456789, 'hi', 42);
select p2text(c1.*) from c1;
--DDL_STATEMENT_BEGIN--
drop function p2text(p2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table c1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table p2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table p1;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE ac (aa TEXT);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table ac add constraint ac_check check (aa is not null);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE bc (bb TEXT) INHERITS (ac);
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

insert into ac (aa) values (NULL);
insert into bc (aa) values (NULL);
--DDL_STATEMENT_BEGIN--
alter table bc drop constraint ac_check;  -- fail, disallowed
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table ac drop constraint ac_check;
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

-- try the unnamed-constraint case
--DDL_STATEMENT_BEGIN--
alter table ac add check (aa is not null);
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

insert into ac (aa) values (NULL);
insert into bc (aa) values (NULL);
--DDL_STATEMENT_BEGIN--
alter table bc drop constraint ac_aa_check;  -- fail, disallowed
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table ac drop constraint ac_aa_check;
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
--DDL_STATEMENT_BEGIN--
alter table ac add constraint ac_check check (aa is not null);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table bc no inherit ac;
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
--DDL_STATEMENT_BEGIN--
alter table bc drop constraint ac_check;
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
--DDL_STATEMENT_BEGIN--
alter table ac drop constraint ac_check;
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
--DDL_STATEMENT_BEGIN--
drop table bc;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ac;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table ac (a int constraint check_a check (a <> 0));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table bc (a int constraint check_a check (a <> 0), b int constraint check_b check (b <> 0)) inherits (ac);
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
--DDL_STATEMENT_BEGIN--
drop table bc;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ac;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table ac (a int constraint check_a check (a <> 0));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table bc (b int constraint check_b check (b <> 0));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table cc (c int constraint check_c check (c <> 0)) inherits (ac, bc);
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc', 'cc') order by 1,2;
--DDL_STATEMENT_BEGIN--
alter table cc no inherit bc;
--DDL_STATEMENT_END--
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc', 'cc') order by 1,2;
--DDL_STATEMENT_BEGIN--
drop table cc;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table bc;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ac;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table p1(f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table p2(f2 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table c1(f3 int) inherits(p1,p2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
insert into c1 values(1,-1,2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table p2 add constraint cc check (f2>0);  -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table p2 add check (f2>0);  -- check it without a name, too
--DDL_STATEMENT_END--
delete from c1;
insert into c1 values(1,1,2);
--DDL_STATEMENT_BEGIN--
alter table p2 add check (f2>0);
--DDL_STATEMENT_END--
insert into c1 values(1,-1,2);  -- fail
--DDL_STATEMENT_BEGIN--
create table c2(f3 int) inherits(p1,p2);
--DDL_STATEMENT_END--
\d c2
--DDL_STATEMENT_BEGIN--
create table c3 (f4 int) inherits(c1,c2);
--DDL_STATEMENT_END--
\d c3
--DDL_STATEMENT_BEGIN--
drop table p1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table p2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table pp1 (f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table cc1 (f2 text, f3 int) inherits (pp1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table pp1 add column a1 int check (a1 > 0);
--DDL_STATEMENT_END--
\d cc1
--DDL_STATEMENT_BEGIN--
create table cc2(f4 float) inherits(pp1,cc1);
--DDL_STATEMENT_END--
\d cc2
--DDL_STATEMENT_BEGIN--
alter table pp1 add column a2 int check (a2 > 0);
--DDL_STATEMENT_END--
\d cc2
--DDL_STATEMENT_BEGIN--
drop table pp1 cascade;
--DDL_STATEMENT_END--

-- Test for renaming in simple multiple inheritance
--DDL_STATEMENT_BEGIN--
CREATE TABLE inht1 (a int, b int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhs1 (b int, c int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhts (d int) INHERITS (inht1, inhs1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE inht1 RENAME a TO aa;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE inht1 RENAME b TO bb;                -- to be failed
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE inhts RENAME aa TO aaa;      -- to be failed
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE inhts RENAME d TO dd;
--DDL_STATEMENT_END--
\d+ inhts
--DDL_STATEMENT_BEGIN--
DROP TABLE inhts;
--DDL_STATEMENT_END--

-- Test for renaming in diamond inheritance
--DDL_STATEMENT_BEGIN--
CREATE TABLE inht2 (x int) INHERITS (inht1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE inht3 (y int) INHERITS (inht1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE inht4 (z int) INHERITS (inht2, inht3);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE inht1 RENAME aa TO aaa;
--DDL_STATEMENT_END--
\d+ inht4
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhts (d int) INHERITS (inht2, inhs1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE inht1 RENAME aaa TO aaaa;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE inht1 RENAME b TO bb;                -- to be failed
--DDL_STATEMENT_END--
\d+ inhts

WITH RECURSIVE r AS (
  SELECT 'inht1'::regclass AS inhrelid
UNION ALL
  SELECT c.inhrelid FROM pg_inherits c, r WHERE r.inhrelid = c.inhparent
)
SELECT a.attrelid::regclass, a.attname, a.attinhcount, e.expected
  FROM (SELECT inhrelid, count(*) AS expected FROM pg_inherits
        WHERE inhparent IN (SELECT inhrelid FROM r) GROUP BY inhrelid) e
  JOIN pg_attribute a ON e.inhrelid = a.attrelid WHERE NOT attislocal
  ORDER BY a.attrelid::regclass::name, a.attnum;
--DDL_STATEMENT_BEGIN--
DROP TABLE inht1, inhs1 CASCADE;
--DDL_STATEMENT_END--


-- Test non-inheritable indices [UNIQUE, EXCLUDE] constraints
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_constraints (id int, val1 varchar, val2 int, UNIQUE(val1, val2));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_constraints_inh () INHERITS (test_constraints);
--DDL_STATEMENT_END--
\d+ test_constraints
--DDL_STATEMENT_BEGIN--
ALTER TABLE ONLY test_constraints DROP CONSTRAINT test_constraints_val1_val2_key;
--DDL_STATEMENT_END--
\d+ test_constraints
\d+ test_constraints_inh
--DDL_STATEMENT_BEGIN--
DROP TABLE test_constraints_inh;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE test_constraints;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_ex_constraints (
    c circle,
    EXCLUDE USING gist (c WITH &&)
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_ex_constraints_inh () INHERITS (test_ex_constraints);
--DDL_STATEMENT_END--
\d+ test_ex_constraints
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_ex_constraints DROP CONSTRAINT test_ex_constraints_c_excl;
--DDL_STATEMENT_END--
\d+ test_ex_constraints
\d+ test_ex_constraints_inh
--DDL_STATEMENT_BEGIN--
DROP TABLE test_ex_constraints_inh;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE test_ex_constraints;
--DDL_STATEMENT_END--

-- Test non-inheritable foreign key constraints
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_primary_constraints(id int PRIMARY KEY);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_foreign_constraints(id1 int REFERENCES test_primary_constraints(id));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_foreign_constraints_inh () INHERITS (test_foreign_constraints);
--DDL_STATEMENT_END--
\d+ test_primary_constraints
\d+ test_foreign_constraints
--DDL_STATEMENT_BEGIN--
ALTER TABLE test_foreign_constraints DROP CONSTRAINT test_foreign_constraints_id1_fkey;
--DDL_STATEMENT_END--
\d+ test_foreign_constraints
\d+ test_foreign_constraints_inh
--DDL_STATEMENT_BEGIN--
DROP TABLE test_foreign_constraints_inh;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE test_foreign_constraints;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE test_primary_constraints;
--DDL_STATEMENT_END--

-- Test foreign key behavior
--DDL_STATEMENT_BEGIN--
create table inh_fk_1 (a int primary key);
--DDL_STATEMENT_END--
insert into inh_fk_1 values (1), (2), (3);
--DDL_STATEMENT_BEGIN--
create table inh_fk_2 (x int primary key, y int references inh_fk_1 on delete cascade);
--DDL_STATEMENT_END--
insert into inh_fk_2 values (11, 1), (22, 2), (33, 3);
--DDL_STATEMENT_BEGIN--
create table inh_fk_2_child () inherits (inh_fk_2);
--DDL_STATEMENT_END--
insert into inh_fk_2_child values (111, 1), (222, 2);
delete from inh_fk_1 where a = 1;
select * from inh_fk_1 order by 1;
select * from inh_fk_2 order by 1, 2;
--DDL_STATEMENT_BEGIN--
drop table inh_fk_1, inh_fk_2, inh_fk_2_child;
--DDL_STATEMENT_END--

-- Test that parent and child CHECK constraints can be created in either order
--DDL_STATEMENT_BEGIN--
create table p1(f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table p1_c1() inherits(p1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table p1 add constraint inh_check_constraint1 check (f1 > 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table p1_c1 add constraint inh_check_constraint1 check (f1 > 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table p1_c1 add constraint inh_check_constraint2 check (f1 < 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table p1 add constraint inh_check_constraint2 check (f1 < 10);\
--DDL_STATEMENT_END--

select conrelid::regclass::text as relname, conname, conislocal, coninhcount
from pg_constraint where conname like 'inh\_check\_constraint%'
order by 1, 2;
--DDL_STATEMENT_BEGIN--
drop table p1 cascade;
--DDL_STATEMENT_END--
-- Test that a valid child can have not-valid parent, but not vice versa
--DDL_STATEMENT_BEGIN--
create table invalid_check_con(f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table invalid_check_con_child() inherits(invalid_check_con);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table invalid_check_con_child add constraint inh_check_constraint check(f1 > 0) not valid;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table invalid_check_con add constraint inh_check_constraint check(f1 > 0); -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table invalid_check_con_child drop constraint inh_check_constraint;
--DDL_STATEMENT_END--

insert into invalid_check_con values(0);
--DDL_STATEMENT_BEGIN--
alter table invalid_check_con_child add constraint inh_check_constraint check(f1 > 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table invalid_check_con add constraint inh_check_constraint check(f1 > 0) not valid;
--DDL_STATEMENT_END--

insert into invalid_check_con values(0); -- fail
insert into invalid_check_con_child values(0); -- fail

select conrelid::regclass::text as relname, conname,
       convalidated, conislocal, coninhcount, connoinherit
from pg_constraint where conname like 'inh\_check\_constraint%'
order by 1, 2;

-- We don't drop the invalid_check_con* tables, to test dump/reload with

--
-- Test parameterized append plans for inheritance trees
--
--DDL_STATEMENT_BEGIN--
create temp table patest0 (id, x) as
  select x, x from generate_series(0,1000) x;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create temp table patest1() inherits (patest0);
--DDL_STATEMENT_END--
insert into patest1
  select x, x from generate_series(0,1000) x;
--DDL_STATEMENT_BEGIN--
create temp table patest2() inherits (patest0);
--DDL_STATEMENT_END--
insert into patest2
  select x, x from generate_series(0,1000) x;
--DDL_STATEMENT_BEGIN--
create index patest0i on patest0(id);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index patest1i on patest1(id);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index patest2i on patest2(id);
--DDL_STATEMENT_END--
analyze patest0;
analyze patest1;
analyze patest2;

explain (costs off)
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;
--DDL_STATEMENT_BEGIN--
drop index patest2i;
--DDL_STATEMENT_END--
explain (costs off)
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;
--DDL_STATEMENT_BEGIN--
drop table patest0 cascade;
--DDL_STATEMENT_END--
--
-- Test merge-append plans for inheritance trees
--
--DDL_STATEMENT_BEGIN--
create table matest0 (id serial primary key, name text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table matest1 (id integer primary key) inherits (matest0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table matest2 (id integer primary key) inherits (matest0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table matest3 (id integer primary key) inherits (matest0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index matest0i on matest0 ((1-id));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index matest1i on matest1 ((1-id));
--DDL_STATEMENT_END--
-- create index matest2i on matest2 ((1-id));  -- intentionally missing
--DDL_STATEMENT_BEGIN--
create index matest3i on matest3 ((1-id));
--DDL_STATEMENT_END--

insert into matest1 (name) values ('Test 1');
insert into matest1 (name) values ('Test 2');
insert into matest2 (name) values ('Test 3');
insert into matest2 (name) values ('Test 4');
insert into matest3 (name) values ('Test 5');
insert into matest3 (name) values ('Test 6');

set enable_indexscan = off;  -- force use of seqscan/sort, so no merge
explain (verbose, costs off) select * from matest0 order by 1-id;
select * from matest0 order by 1-id;
explain (verbose, costs off) select min(1-id) from matest0;
select min(1-id) from matest0;
reset enable_indexscan;

set enable_seqscan = off;  -- plan with fewest seqscans should be merge
set enable_parallel_append = off; -- Don't let parallel-append interfere
explain (verbose, costs off) select * from matest0 order by 1-id;
select * from matest0 order by 1-id;
explain (verbose, costs off) select min(1-id) from matest0;
select min(1-id) from matest0;
reset enable_seqscan;
reset enable_parallel_append;
--DDL_STATEMENT_BEGIN--
drop table matest0 cascade;
--DDL_STATEMENT_END--
--
-- Check that use of an index with an extraneous column doesn't produce
-- a plan with extraneous sorting
--
--DDL_STATEMENT_BEGIN--
create table matest0 (a int, b int, c int, d int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table matest1 () inherits(matest0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index matest0i on matest0 (b, c);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index matest1i on matest1 (b, c);
--DDL_STATEMENT_END--

set enable_nestloop = off;  -- we want a plan with two MergeAppends

explain (costs off)
select t1.* from matest0 t1, matest0 t2
where t1.b = t2.b and t2.c = t2.d
order by t1.b limit 10;

reset enable_nestloop;
--DDL_STATEMENT_BEGIN--
drop table matest0 cascade;
--DDL_STATEMENT_END--
--
-- Test merge-append for UNION ALL append relations
--

set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;

-- Check handling of duplicated, constant, or volatile targetlist items
explain (costs off)
SELECT thousand, tenthous FROM tenk1
UNION ALL
SELECT thousand, thousand FROM tenk1
ORDER BY thousand, tenthous;

explain (costs off)
SELECT thousand, tenthous, thousand+tenthous AS x FROM tenk1
UNION ALL
SELECT 42, 42, hundred FROM tenk1
ORDER BY thousand, tenthous;

explain (costs off)
SELECT thousand, tenthous FROM tenk1
UNION ALL
SELECT thousand, random()::integer FROM tenk1
ORDER BY thousand, tenthous;

-- Check min/max aggregate optimization
explain (costs off)
SELECT min(x) FROM
  (SELECT unique1 AS x FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x FROM tenk1 b) s;

explain (costs off)
SELECT min(y) FROM
  (SELECT unique1 AS x, unique1 AS y FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x, unique2 AS y FROM tenk1 b) s;

-- XXX planner doesn't recognize that index on unique2 is sufficiently sorted
explain (costs off)
SELECT x, y FROM
  (SELECT thousand AS x, tenthous AS y FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x, unique2 AS y FROM tenk1 b) s
ORDER BY x, y;

-- exercise rescan code path via a repeatedly-evaluated subquery
explain (costs off)
SELECT
    ARRAY(SELECT f.i FROM (
        (SELECT d + g.i FROM generate_series(4, 30, 3) d ORDER BY 1)
        UNION ALL
        (SELECT d + g.i FROM generate_series(0, 30, 5) d ORDER BY 1)
    ) f(i)
    ORDER BY f.i LIMIT 10)
FROM generate_series(1, 3) g(i);

SELECT
    ARRAY(SELECT f.i FROM (
        (SELECT d + g.i FROM generate_series(4, 30, 3) d ORDER BY 1)
        UNION ALL
        (SELECT d + g.i FROM generate_series(0, 30, 5) d ORDER BY 1)
    ) f(i)
    ORDER BY f.i LIMIT 10)
FROM generate_series(1, 3) g(i);

reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;

--
-- Check handling of a constant-null CHECK constraint
--
--DDL_STATEMENT_BEGIN--
create table cnullparent (f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table cnullchild (check (f1 = 1 or f1 = null)) inherits(cnullparent);
--DDL_STATEMENT_END--
insert into cnullchild values(1);
insert into cnullchild values(2);
insert into cnullchild values(null);
select * from cnullparent;
select * from cnullparent where f1 = 2;
--DDL_STATEMENT_BEGIN--
drop table cnullparent cascade;
--DDL_STATEMENT_END--

--
-- Check that constraint exclusion works correctly with partitions using
-- implicit constraints generated from the partition bound information.
--
--DDL_STATEMENT_BEGIN--
create table list_parted (
	a	varchar
) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_ab_cd partition of list_parted for values in ('ab', 'cd');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_ef_gh partition of list_parted for values in ('ef', 'gh');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_null_xy partition of list_parted for values in (null, 'xy');
--DDL_STATEMENT_END--

explain (costs off) select * from list_parted;
explain (costs off) select * from list_parted where a is null;
explain (costs off) select * from list_parted where a is not null;
explain (costs off) select * from list_parted where a in ('ab', 'cd', 'ef');
explain (costs off) select * from list_parted where a = 'ab' or a in (null, 'cd');
explain (costs off) select * from list_parted where a = 'ab';
--DDL_STATEMENT_BEGIN--
create table range_list_parted (
	a	int,
	b	char(2)
) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_1_10 partition of range_list_parted for values from (1) to (10) partition by list (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_1_10_ab partition of part_1_10 for values in ('ab');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_1_10_cd partition of part_1_10 for values in ('cd');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_10_20 partition of range_list_parted for values from (10) to (20) partition by list (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_10_20_ab partition of part_10_20 for values in ('ab');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_10_20_cd partition of part_10_20 for values in ('cd');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_21_30 partition of range_list_parted for values from (21) to (30) partition by list (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_21_30_ab partition of part_21_30 for values in ('ab');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_21_30_cd partition of part_21_30 for values in ('cd');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_40_inf partition of range_list_parted for values from (40) to (maxvalue) partition by list (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_40_inf_ab partition of part_40_inf for values in ('ab');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_40_inf_cd partition of part_40_inf for values in ('cd');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table part_40_inf_null partition of part_40_inf for values in (null);
--DDL_STATEMENT_END--

explain (costs off) select * from range_list_parted;
explain (costs off) select * from range_list_parted where a = 5;
explain (costs off) select * from range_list_parted where b = 'ab';
explain (costs off) select * from range_list_parted where a between 3 and 23 and b in ('ab');

/* Should select no rows because range partition key cannot be null */
explain (costs off) select * from range_list_parted where a is null;

/* Should only select rows from the null-accepting partition */
explain (costs off) select * from range_list_parted where b is null;
explain (costs off) select * from range_list_parted where a is not null and a < 67;
explain (costs off) select * from range_list_parted where a >= 30;
--DDL_STATEMENT_BEGIN--
drop table list_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table range_list_parted;
--DDL_STATEMENT_END--

-- check that constraint exclusion is able to cope with the partition
-- constraint emitted for multi-column range partitioned tables
--DDL_STATEMENT_BEGIN--
create table mcrparted (a int, b int, c int) partition by range (a, abs(b), c);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mcrparted_def partition of mcrparted default;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mcrparted0 partition of mcrparted for values from (minvalue, minvalue, minvalue) to (1, 1, 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mcrparted1 partition of mcrparted for values from (1, 1, 1) to (10, 5, 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mcrparted2 partition of mcrparted for values from (10, 5, 10) to (10, 10, 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mcrparted3 partition of mcrparted for values from (11, 1, 1) to (20, 10, 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mcrparted4 partition of mcrparted for values from (20, 10, 10) to (20, 20, 20);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table mcrparted5 partition of mcrparted for values from (20, 20, 20) to (maxvalue, maxvalue, maxvalue);
--DDL_STATEMENT_END--
explain (costs off) select * from mcrparted where a = 0;	-- scans mcrparted0, mcrparted_def
explain (costs off) select * from mcrparted where a = 10 and abs(b) < 5;	-- scans mcrparted1, mcrparted_def
explain (costs off) select * from mcrparted where a = 10 and abs(b) = 5;	-- scans mcrparted1, mcrparted2, mcrparted_def
explain (costs off) select * from mcrparted where abs(b) = 5;	-- scans all partitions
explain (costs off) select * from mcrparted where a > -1;	-- scans all partitions
explain (costs off) select * from mcrparted where a = 20 and abs(b) = 10 and c > 10;	-- scans mcrparted4
explain (costs off) select * from mcrparted where a = 20 and c > 20; -- scans mcrparted3, mcrparte4, mcrparte5, mcrparted_def
--DDL_STATEMENT_BEGIN--
drop table mcrparted;
--DDL_STATEMENT_END--

-- check that partitioned table Appends cope with being referenced in
-- subplans
--DDL_STATEMENT_BEGIN--
create table parted_minmax (a int, b varchar(16)) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_minmax1 partition of parted_minmax for values from (1) to (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index parted_minmax1i on parted_minmax1 (a, b);
--DDL_STATEMENT_END--
insert into parted_minmax values (1,'12345');
explain (costs off) select min(a), max(a) from parted_minmax where b = '12345';
select min(a), max(a) from parted_minmax where b = '12345';
--DDL_STATEMENT_BEGIN--
drop table parted_minmax;
--DDL_STATEMENT_END--
