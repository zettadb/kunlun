--
-- Test domains.
--

-- Test Comment / Drop
--DDL_STATEMENT_BEGIN--
create domain domaindroptest int4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
comment on domain domaindroptest is 'About to drop this..';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dependenttypetest domaindroptest;
--DDL_STATEMENT_END--
-- fail because of dependent type
--DDL_STATEMENT_BEGIN--
drop domain domaindroptest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain domaindroptest cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- this should fail because already gone
drop domain domaindroptest cascade;
--DDL_STATEMENT_END--

-- Test domain input.

-- Note: the point of checking both INSERT and COPY FROM is that INSERT
-- exercises CoerceToDomain while COPY exercises domain_in.
--DDL_STATEMENT_BEGIN--
create domain domainvarchar varchar(5);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain domainnumeric numeric(8,2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain domainint4 int4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain domaintext text;
--DDL_STATEMENT_END--
-- Test explicit coercions --- these should succeed (and truncate)
SELECT cast('123456' as domainvarchar);
SELECT cast('12345' as domainvarchar);

-- Test tables using domains
--DDL_STATEMENT_BEGIN--
create table basictest
           ( testint4 domainint4
           , testtext domaintext
           , testvarchar domainvarchar
           , testnumeric domainnumeric
           );
--DDL_STATEMENT_END--		   

INSERT INTO basictest values ('88', 'haha', 'short', '123.12');      -- Good
INSERT INTO basictest values ('88', 'haha', 'short text', '123.12'); -- Bad varchar
INSERT INTO basictest values ('88', 'haha', 'short', '123.1212');    -- Truncate numeric

-- Test copy
COPY basictest (testvarchar) FROM stdin; -- fail
notsoshorttext
\.

COPY basictest (testvarchar) FROM stdin;
short
\.

select * from basictest;

-- check that domains inherit operations from base types
select testtext || testvarchar as concat, testnumeric + 42 as sum
from basictest;

-- check that union/case/coalesce type resolution handles domains properly
select coalesce(4::domainint4, 7) is of (int4) as t;
select coalesce(4::domainint4, 7) is of (domainint4) as f;
select coalesce(4::domainint4, 7::domainint4) is of (domainint4) as t;
--DDL_STATEMENT_BEGIN--
drop table basictest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain domainvarchar restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain domainnumeric restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain domainint4 restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain domaintext;
--DDL_STATEMENT_END--

-- Test domains over array types
--DDL_STATEMENT_BEGIN--
create domain domainint4arr int4[1];
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain domainchar4arr varchar(4)[2][3];
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table domarrtest
           ( testint4arr domainint4arr
           , testchar4arr domainchar4arr
            );
--DDL_STATEMENT_END--			
INSERT INTO domarrtest values ('{2,2}', '{{"a","b"},{"c","d"}}');
INSERT INTO domarrtest values ('{{2,2},{2,2}}', '{{"a","b"}}');
INSERT INTO domarrtest values ('{2,2}', '{{"a","b"},{"c","d"},{"e","f"}}');
INSERT INTO domarrtest values ('{2,2}', '{{"a"},{"c"}}');
INSERT INTO domarrtest values (NULL, '{{"a","b","c"},{"d","e","f"}}');
INSERT INTO domarrtest values (NULL, '{{"toolong","b","c"},{"d","e","f"}}');
INSERT INTO domarrtest (testint4arr[1], testint4arr[3]) values (11,22);
select * from domarrtest;
select testint4arr[1], testchar4arr[2:2] from domarrtest;
select array_dims(testint4arr), array_dims(testchar4arr) from domarrtest;

COPY domarrtest FROM stdin;
{3,4}	{q,w,e}
\N	\N
\.

COPY domarrtest FROM stdin;	-- fail
{3,4}	{qwerty,w,e}
\.

select * from domarrtest;

update domarrtest set
  testint4arr[1] = testint4arr[1] + 1,
  testint4arr[3] = testint4arr[3] - 1
where testchar4arr is null;

select * from domarrtest where testchar4arr is null;
--DDL_STATEMENT_BEGIN--
drop table domarrtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain domainint4arr restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain domainchar4arr restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dia as int[];
--DDL_STATEMENT_END--
select '{1,2,3}'::dia;
select array_dims('{1,2,3}'::dia);
select pg_typeof('{1,2,3}'::dia);
select pg_typeof('{1,2,3}'::dia || 42); -- should be int[] not dia
--DDL_STATEMENT_BEGIN--
drop domain dia;
--DDL_STATEMENT_END--

-- Test domains over composites
--DDL_STATEMENT_BEGIN--
create type comptype as (r float8, i float8);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dcomptype as comptype;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table dcomptable (d1 dcomptype unique);
--DDL_STATEMENT_END--

insert into dcomptable values (row(1,2)::dcomptype);
insert into dcomptable values (row(3,4)::comptype);
insert into dcomptable values (row(1,2)::dcomptype);  -- fail on uniqueness
insert into dcomptable (d1.r) values(11);

select * from dcomptable;
select (d1).r, (d1).i, (d1).* from dcomptable;
update dcomptable set d1.r = (d1).r + 1 where (d1).i > 0;
select * from dcomptable;
--DDL_STATEMENT_BEGIN--
alter domain dcomptype add constraint c1 check ((value).r <= (value).i);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain dcomptype add constraint c2 check ((value).r > (value).i);  -- fail
--DDL_STATEMENT_END--
select row(2,1)::dcomptype;  -- fail
insert into dcomptable values (row(1,2)::comptype);
insert into dcomptable values (row(2,1)::comptype);  -- fail
insert into dcomptable (d1.r) values(99);
insert into dcomptable (d1.r, d1.i) values(99, 100);
insert into dcomptable (d1.r, d1.i) values(100, 99);  -- fail
update dcomptable set d1.r = (d1).r + 1 where (d1).i > 0;  -- fail
update dcomptable set d1.r = (d1).r - 1, d1.i = (d1).i + 1 where (d1).i > 0;
select * from dcomptable;

explain (verbose, costs off)
  update dcomptable set d1.r = (d1).r - 1, d1.i = (d1).i + 1 where (d1).i > 0;
--DDL_STATEMENT_BEGIN--
create rule silly as on delete to dcomptable do instead
  update dcomptable set d1.r = (d1).r - 1, d1.i = (d1).i + 1 where (d1).i > 0;
--DDL_STATEMENT_END--
\d+ dcomptable
--DDL_STATEMENT_BEGIN--
drop table dcomptable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop type comptype cascade;
--DDL_STATEMENT_END--

-- check altering and dropping columns used by domain constraints
--DDL_STATEMENT_BEGIN--
create type comptype as (r float8, i float8);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dcomptype as comptype;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain dcomptype add constraint c1 check ((value).r > 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
comment on constraint c1 on domain dcomptype is 'random commentary';
--DDL_STATEMENT_END--
select row(0,1)::dcomptype;  -- fail
--DDL_STATEMENT_BEGIN--
alter type comptype alter attribute r type varchar;  -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter type comptype alter attribute r type bigint;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter type comptype drop attribute r;  -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter type comptype drop attribute i;
--DDL_STATEMENT_END--

select conname, obj_description(oid, 'pg_constraint') from pg_constraint
  where contypid = 'dcomptype'::regtype;  -- check comment is still there
--DDL_STATEMENT_BEGIN--
drop type comptype cascade;
--DDL_STATEMENT_END--

-- Test domains over arrays of composite
--DDL_STATEMENT_BEGIN--
create type comptype as (r float8, i float8);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dcomptypea as comptype[];
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table dcomptable (d1 dcomptypea unique);
--DDL_STATEMENT_END--

insert into dcomptable values (array[row(1,2)]::dcomptypea);
insert into dcomptable values (array[row(3,4), row(5,6)]::comptype[]);
insert into dcomptable values (array[row(7,8)::comptype, row(9,10)::comptype]);
insert into dcomptable values (array[row(1,2)]::dcomptypea);  -- fail on uniqueness
insert into dcomptable (d1[1]) values(row(9,10));
insert into dcomptable (d1[1].r) values(11);

select * from dcomptable;
select d1[2], d1[1].r, d1[1].i from dcomptable;
update dcomptable set d1[2] = row(d1[2].i, d1[2].r);
select * from dcomptable;
update dcomptable set d1[1].r = d1[1].r + 1 where d1[1].i > 0;
select * from dcomptable;
--DDL_STATEMENT_BEGIN--
alter domain dcomptypea add constraint c1 check (value[1].r <= value[1].i);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain dcomptypea add constraint c2 check (value[1].r > value[1].i);  -- fail
--DDL_STATEMENT_END--

select array[row(2,1)]::dcomptypea;  -- fail
insert into dcomptable values (array[row(1,2)]::comptype[]);
insert into dcomptable values (array[row(2,1)]::comptype[]);  -- fail
insert into dcomptable (d1[1].r) values(99);
insert into dcomptable (d1[1].r, d1[1].i) values(99, 100);
insert into dcomptable (d1[1].r, d1[1].i) values(100, 99);  -- fail
update dcomptable set d1[1].r = d1[1].r + 1 where d1[1].i > 0;  -- fail
update dcomptable set d1[1].r = d1[1].r - 1, d1[1].i = d1[1].i + 1
  where d1[1].i > 0;
select * from dcomptable;

explain (verbose, costs off)
  update dcomptable set d1[1].r = d1[1].r - 1, d1[1].i = d1[1].i + 1
    where d1[1].i > 0;
--DDL_STATEMENT_BEGIN--
create rule silly as on delete to dcomptable do instead
  update dcomptable set d1[1].r = d1[1].r - 1, d1[1].i = d1[1].i + 1
    where d1[1].i > 0;
--DDL_STATEMENT_END--
\d+ dcomptable
--DDL_STATEMENT_BEGIN--
drop table dcomptable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop type comptype cascade;
--DDL_STATEMENT_END--

-- Test arrays over domains
--DDL_STATEMENT_BEGIN--
create domain posint as int check (value > 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table pitable (f1 posint[]);
--DDL_STATEMENT_END--
insert into pitable values(array[42]);
insert into pitable values(array[-1]);  -- fail
insert into pitable values('{0}');  -- fail
update pitable set f1[1] = f1[1] + 1;
update pitable set f1[1] = 0;  -- fail
select * from pitable;
--DDL_STATEMENT_BEGIN--
drop table pitable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain vc4 as varchar(4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table vc4table (f1 vc4[]);
--DDL_STATEMENT_END--
insert into vc4table values(array['too long']);  -- fail
insert into vc4table values(array['too long']::vc4[]);  -- cast truncates
select * from vc4table;
--DDL_STATEMENT_BEGIN--
drop table vc4table;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop type vc4;
--DDL_STATEMENT_END--

-- You can sort of fake arrays-of-arrays by putting a domain in between
--DDL_STATEMENT_BEGIN--
create domain dposinta as posint[];
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table dposintatable (f1 dposinta[]);
--DDL_STATEMENT_END--
insert into dposintatable values(array[array[42]]);  -- fail
insert into dposintatable values(array[array[42]::posint[]]); -- still fail
insert into dposintatable values(array[array[42]::dposinta]); -- but this works
select f1, f1[1], (f1[1])[1] from dposintatable;
select pg_typeof(f1) from dposintatable;
select pg_typeof(f1[1]) from dposintatable;
select pg_typeof(f1[1][1]) from dposintatable;
select pg_typeof((f1[1])[1]) from dposintatable;
update dposintatable set f1[2] = array[99];
select f1, f1[1], (f1[2])[1] from dposintatable;
-- it'd be nice if you could do something like this, but for now you can't:
update dposintatable set f1[2][1] = array[97];
-- maybe someday we can make this syntax work:
update dposintatable set (f1[2])[1] = array[98];
--DDL_STATEMENT_BEGIN--
drop table dposintatable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain posint cascade;
--DDL_STATEMENT_END--

-- Test not-null restrictions
--DDL_STATEMENT_BEGIN--
create domain dnotnull varchar(15) NOT NULL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dnull    varchar(15);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dcheck   varchar(15) NOT NULL CHECK (VALUE = 'a' OR VALUE = 'c' OR VALUE = 'd');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table nulltest
           ( col1 dnotnull
           , col2 dnotnull NULL  -- NOT NULL in the domain cannot be overridden
           , col3 dnull    NOT NULL
           , col4 dnull
           , col5 dcheck CHECK (col5 IN ('c', 'd'))
           );
--DDL_STATEMENT_END--
INSERT INTO nulltest DEFAULT VALUES;
INSERT INTO nulltest values ('a', 'b', 'c', 'd', 'c');  -- Good
insert into nulltest values ('a', 'b', 'c', 'd', NULL);
insert into nulltest values ('a', 'b', 'c', 'd', 'a');
INSERT INTO nulltest values (NULL, 'b', 'c', 'd', 'd');
INSERT INTO nulltest values ('a', NULL, 'c', 'd', 'c');
INSERT INTO nulltest values ('a', 'b', NULL, 'd', 'c');
INSERT INTO nulltest values ('a', 'b', 'c', NULL, 'd'); -- Good

-- Test copy
COPY nulltest FROM stdin; --fail
a	b	\N	d	d
\.

COPY nulltest FROM stdin; --fail
a	b	c	d	\N
\.

-- Last row is bad
COPY nulltest FROM stdin;
a	b	c	\N	c
a	b	c	\N	d
a	b	c	\N	a
\.

select * from nulltest;

-- Test out coerced (casted) constraints
SELECT cast('1' as dnotnull);
SELECT cast(NULL as dnotnull); -- fail
SELECT cast(cast(NULL as dnull) as dnotnull); -- fail
SELECT cast(col4 as dnotnull) from nulltest; -- fail

-- cleanup
--DDL_STATEMENT_BEGIN--
drop table nulltest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain dnotnull restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain dnull restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain dcheck restrict;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create domain ddef1 int4 DEFAULT 3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain ddef2 oid DEFAULT '12';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- Type mixing, function returns int8
create domain ddef3 text DEFAULT 5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create sequence ddef4_seq;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain ddef4 int4 DEFAULT nextval('ddef4_seq');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain ddef5 numeric(8,2) NOT NULL DEFAULT '12.12';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table defaulttest
            ( col1 ddef1
            , col2 ddef2
            , col3 ddef3
            , col4 ddef4 PRIMARY KEY
            , col5 ddef1 NOT NULL DEFAULT NULL
            , col6 ddef2 DEFAULT '88'
            , col7 ddef4 DEFAULT 8000
            , col8 ddef5
            );
--DDL_STATEMENT_END--			
insert into defaulttest(col4) values(0); -- fails, col5 defaults to null
alter table defaulttest alter column col5 drop default;
insert into defaulttest default values; -- succeeds, inserts domain default
-- We used to treat SET DEFAULT NULL as equivalent to DROP DEFAULT; wrong
alter table defaulttest alter column col5 set default null;
insert into defaulttest(col4) values(0); -- fails
alter table defaulttest alter column col5 drop default;
insert into defaulttest default values;
insert into defaulttest default values;

-- Test defaults with copy
COPY defaulttest(col5) FROM stdin;
42
\.

select * from defaulttest;
--DDL_STATEMENT_BEGIN--
drop table defaulttest cascade;
--DDL_STATEMENT_END--
-- Test ALTER DOMAIN .. NOT NULL
--DDL_STATEMENT_BEGIN--
create domain dnotnulltest integer;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table domnotnull
( col1 dnotnulltest
, col2 dnotnulltest
);
--DDL_STATEMENT_END--

insert into domnotnull default values;
alter domain dnotnulltest set not null; -- fails

update domnotnull set col1 = 5;
--DDL_STATEMENT_BEGIN--
alter domain dnotnulltest set not null; -- fails
--DDL_STATEMENT_END--
update domnotnull set col2 = 6;
--DDL_STATEMENT_BEGIN--
alter domain dnotnulltest set not null;
--DDL_STATEMENT_END--
update domnotnull set col1 = null; -- fails
--DDL_STATEMENT_BEGIN--
alter domain dnotnulltest drop not null;
--DDL_STATEMENT_END--
update domnotnull set col1 = null;
--DDL_STATEMENT_BEGIN--
drop domain dnotnulltest cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- Test ALTER DOMAIN .. DEFAULT ..
create table domdeftest (col1 ddef1);
--DDL_STATEMENT_END--
insert into domdeftest default values;
select * from domdeftest;
--DDL_STATEMENT_BEGIN--
alter domain ddef1 set default '42';
--DDL_STATEMENT_END--
insert into domdeftest default values;
select * from domdeftest;
--DDL_STATEMENT_BEGIN--
alter domain ddef1 drop default;
--DDL_STATEMENT_END--
insert into domdeftest default values;
select * from domdeftest;
--DDL_STATEMENT_BEGIN--
drop table domdeftest;
--DDL_STATEMENT_END--
-- Test ALTER DOMAIN .. CONSTRAINT ..
--DDL_STATEMENT_BEGIN--
create domain con as integer;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table domcontest (col1 con);
--DDL_STATEMENT_END--
insert into domcontest values (1);
insert into domcontest values (2);
--DDL_STATEMENT_BEGIN--
alter domain con add constraint t check (VALUE < 1); -- fails
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain con add constraint t check (VALUE < 34);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain con add check (VALUE > 0);
--DDL_STATEMENT_END--

insert into domcontest values (-5); -- fails
insert into domcontest values (42); -- fails
insert into domcontest values (5);
--DDL_STATEMENT_BEGIN--
alter domain con drop constraint t;
--DDL_STATEMENT_END--
insert into domcontest values (-5); --fails
insert into domcontest values (42);
--DDL_STATEMENT_BEGIN--
alter domain con drop constraint nonexistent;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain con drop constraint if exists nonexistent;
--DDL_STATEMENT_END--
-- Test ALTER DOMAIN .. CONSTRAINT .. NOT VALID
--DDL_STATEMENT_BEGIN--
create domain things AS INT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE thethings (stuff things);
--DDL_STATEMENT_END--
INSERT INTO thethings (stuff) VALUES (55);
--DDL_STATEMENT_BEGIN--
ALTER DOMAIN things ADD CONSTRAINT meow CHECK (VALUE < 11);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DOMAIN things ADD CONSTRAINT meow CHECK (VALUE < 11) NOT VALID;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DOMAIN things VALIDATE CONSTRAINT meow;
--DDL_STATEMENT_END--
UPDATE thethings SET stuff = 10;
--DDL_STATEMENT_BEGIN--
ALTER DOMAIN things VALIDATE CONSTRAINT meow;
--DDL_STATEMENT_END--
-- Confirm ALTER DOMAIN with RULES.
--DDL_STATEMENT_BEGIN--
create table domtab (col1 integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dom as integer;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view domview as select cast(col1 as dom) from domtab;
--DDL_STATEMENT_END--
insert into domtab (col1) values (null);
insert into domtab (col1) values (5);
select * from domview;
--DDL_STATEMENT_BEGIN--
alter domain dom set not null;
--DDL_STATEMENT_END--
select * from domview; -- fail
--DDL_STATEMENT_BEGIN--
alter domain dom drop not null;
--DDL_STATEMENT_END--
select * from domview;
--DDL_STATEMENT_BEGIN--
alter domain dom add constraint domchkgt6 check(value > 6);
--DDL_STATEMENT_END--
select * from domview; --fail
--DDL_STATEMENT_BEGIN--
alter domain dom drop constraint domchkgt6 restrict;
--DDL_STATEMENT_END--
select * from domview;

-- cleanup
--DDL_STATEMENT_BEGIN--
drop domain ddef1 restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain ddef2 restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain ddef3 restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain ddef4 restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain ddef5 restrict;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop sequence ddef4_seq;
--DDL_STATEMENT_END--
-- Test domains over domains
--DDL_STATEMENT_BEGIN--
create domain vchar4 varchar(4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dinter vchar4 check (substring(VALUE, 1, 1) = 'x');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain dtop dinter check (substring(VALUE, 2, 1) = '1');
--DDL_STATEMENT_END--
select 'x123'::dtop;
select 'x1234'::dtop; -- explicit coercion should truncate
select 'y1234'::dtop; -- fail
select 'y123'::dtop; -- fail
select 'yz23'::dtop; -- fail
select 'xz23'::dtop; -- fail
--DDL_STATEMENT_BEGIN--
create temp table dtest(f1 dtop);
--DDL_STATEMENT_END--
insert into dtest values('x123');
insert into dtest values('x1234'); -- fail, implicit coercion
insert into dtest values('y1234'); -- fail, implicit coercion
insert into dtest values('y123'); -- fail
insert into dtest values('yz23'); -- fail
insert into dtest values('xz23'); -- fail
--DDL_STATEMENT_BEGIN--
drop table dtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain vchar4 cascade;
--DDL_STATEMENT_END--
-- Make sure that constraints of newly-added domain columns are
-- enforced correctly, even if there's no default value for the new
-- column. Per bug #1433
--DDL_STATEMENT_BEGIN--
create domain str_domain as text not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table domain_test (a int, b int);
--DDL_STATEMENT_END--
insert into domain_test values (1, 2);
insert into domain_test values (1, 2);

-- should fail
--DDL_STATEMENT_BEGIN--
alter table domain_test add column c str_domain;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain str_domain2 as text check (value <> 'foo') default 'foo';
--DDL_STATEMENT_END--
-- should fail
--DDL_STATEMENT_BEGIN--
alter table domain_test add column d str_domain2;
--DDL_STATEMENT_END--
-- Check that domain constraints on prepared statement parameters of
-- unknown type are enforced correctly.
--DDL_STATEMENT_BEGIN--
create domain pos_int as int4 check (value > 0) not null;
--DDL_STATEMENT_END--
prepare s1 as select $1::pos_int = 10 as "is_ten";

execute s1(10);
execute s1(0); -- should fail
execute s1(NULL); -- should fail

-- Check that domain constraints on plpgsql function parameters, results,
-- and local variables are enforced correctly.
--DDL_STATEMENT_BEGIN--
create function doubledecrement(p1 pos_int) returns pos_int as $$
declare v pos_int;
begin
    return p1;
end$$ language plpgsql;
--DDL_STATEMENT_END--
select doubledecrement(3); -- fail because of implicit null assignment
--DDL_STATEMENT_BEGIN--
create or replace function doubledecrement(p1 pos_int) returns pos_int as $$
declare v pos_int = 0;
begin
    return p1;
end$$ language plpgsql;
--DDL_STATEMENT_END--
select doubledecrement(3); -- fail at initialization assignment
--DDL_STATEMENT_BEGIN--
create or replace function doubledecrement(p1 pos_int) returns pos_int as $$
declare v pos_int = 1;
begin
    v = p1 - 1;
    return v - 1;
end$$ language plpgsql;
--DDL_STATEMENT_END--
select doubledecrement(null); -- fail before call
select doubledecrement(0); -- fail before call
select doubledecrement(1); -- fail at assignment to v
select doubledecrement(2); -- fail at return
select doubledecrement(3); -- good

-- Check that ALTER DOMAIN tests columns of derived types
--DDL_STATEMENT_BEGIN--
create domain posint as int4;
--DDL_STATEMENT_END--
-- Currently, this doesn't work for composite types, but verify it complains
--DDL_STATEMENT_BEGIN--
create type ddtest1 as (f1 posint);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table ddtest2(f1 ddtest1);
--DDL_STATEMENT_END--
insert into ddtest2 values(row(-1));
--DDL_STATEMENT_BEGIN--
alter domain posint add constraint c1 check(value >= 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ddtest2;
--DDL_STATEMENT_END--
-- Likewise for domains within arrays of composite
--DDL_STATEMENT_BEGIN--
create table ddtest2(f1 ddtest1[]);
--DDL_STATEMENT_END--
insert into ddtest2 values('{(-1)}');
--DDL_STATEMENT_BEGIN--
alter domain posint add constraint c1 check(value >= 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ddtest2;
--DDL_STATEMENT_END--
-- Likewise for domains within domains over composite
--DDL_STATEMENT_BEGIN--
create domain ddtest1d as ddtest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table ddtest2(f1 ddtest1d);
--DDL_STATEMENT_END--
insert into ddtest2 values('(-1)');
--DDL_STATEMENT_BEGIN--
alter domain posint add constraint c1 check(value >= 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ddtest2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain ddtest1d;
--DDL_STATEMENT_END--
-- Likewise for domains within domains over array of composite
--DDL_STATEMENT_BEGIN--
create domain ddtest1d as ddtest1[];
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table ddtest2(f1 ddtest1d);
--DDL_STATEMENT_END--
insert into ddtest2 values('{(-1)}');
--DDL_STATEMENT_BEGIN--
alter domain posint add constraint c1 check(value >= 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ddtest2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain ddtest1d;
--DDL_STATEMENT_END--
-- Doesn't work for ranges, either
--DDL_STATEMENT_BEGIN--
create type rposint as range (subtype = posint);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table ddtest2(f1 rposint);
--DDL_STATEMENT_END--
insert into ddtest2 values('(-1,3]');
--DDL_STATEMENT_BEGIN--
alter domain posint add constraint c1 check(value >= 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ddtest2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop type rposint;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain posint add constraint c1 check(value >= 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain posint2 as posint check (value % 2 = 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table ddtest2(f1 posint2);
--DDL_STATEMENT_END--
insert into ddtest2 values(11); -- fail
insert into ddtest2 values(-2); -- fail
insert into ddtest2 values(2);
--DDL_STATEMENT_BEGIN--
alter domain posint add constraint c2 check(value >= 10); -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain posint add constraint c2 check(value > 0); -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table ddtest2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop type ddtest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain posint cascade;
--DDL_STATEMENT_END--
--
-- Check enforcement of domain-related typmod in plpgsql (bug #5717)
--
--DDL_STATEMENT_BEGIN--
create or replace function array_elem_check(numeric) returns numeric as $$
declare
  x numeric(4,2)[1];
begin
  x[1] = $1;
  return x[1];
end$$ language plpgsql;
--DDL_STATEMENT_END--
select array_elem_check(121.00);
select array_elem_check(1.23456);
--DDL_STATEMENT_BEGIN--
create domain mynums as numeric(4,2)[1];
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create or replace function array_elem_check(numeric) returns numeric as $$
declare
  x mynums;
begin
  x[1] = $1;
  return x[1];
end$$ language plpgsql;
--DDL_STATEMENT_END--
select array_elem_check(121.00);
select array_elem_check(1.23456);
--DDL_STATEMENT_BEGIN--
create domain mynums2 as mynums;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create or replace function array_elem_check(numeric) returns numeric as $$
declare
  x mynums2;
begin
  x[1] = $1;
  return x[1];
end$$ language plpgsql;
--DDL_STATEMENT_END--
select array_elem_check(121.00);
select array_elem_check(1.23456);
--DDL_STATEMENT_BEGIN--
drop function array_elem_check(numeric);
--DDL_STATEMENT_END--
--
-- Check enforcement of array-level domain constraints
--
--DDL_STATEMENT_BEGIN--
create domain orderedpair as int[2] check (value[1] < value[2]);
--DDL_STATEMENT_END--
select array[1,2]::orderedpair;
select array[2,1]::orderedpair;  -- fail

create temp table op (f1 orderedpair);
insert into op values (array[1,2]);
insert into op values (array[2,1]);  -- fail

update op set f1[2] = 3;
update op set f1[2] = 0;  -- fail
select * from op;
--DDL_STATEMENT_BEGIN--
create or replace function array_elem_check(int) returns int as $$
declare
  x orderedpair = '{1,2}';
begin
  x[2] = $1;
  return x[2];
end$$ language plpgsql;
--DDL_STATEMENT_END--
select array_elem_check(3);
select array_elem_check(-1);
--DDL_STATEMENT_BEGIN--
drop function array_elem_check(int);
--DDL_STATEMENT_END--
--
-- Check enforcement of changing constraints in plpgsql
--
--DDL_STATEMENT_BEGIN--
create domain di as int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function dom_check(int) returns di as $$
declare d di;
begin
  d = $1;
  return d;
end
$$ language plpgsql immutable;
--DDL_STATEMENT_END--
select dom_check(0);
--DDL_STATEMENT_BEGIN--
alter domain di add constraint pos check (value > 0);
--DDL_STATEMENT_END--
select dom_check(0); -- fail
--DDL_STATEMENT_BEGIN--
alter domain di drop constraint pos;
--DDL_STATEMENT_END--
select dom_check(0);
--DDL_STATEMENT_BEGIN--
drop function dom_check(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain di;
--DDL_STATEMENT_END--
--
-- Check use of a (non-inline-able) SQL function in a domain constraint;
-- this has caused issues in the past
--
--DDL_STATEMENT_BEGIN--
create function sql_is_distinct_from(anyelement, anyelement)
returns boolean language sql
as 'select $1 is distinct from $2 limit 1';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create domain inotnull int
  check (sql_is_distinct_from(value, null));
--DDL_STATEMENT_END--
select 1::inotnull;
select null::inotnull;
--DDL_STATEMENT_BEGIN--
create table dom_table (x inotnull);
--DDL_STATEMENT_END--
insert into dom_table values ('1');
insert into dom_table values (1);
insert into dom_table values (null);
--DDL_STATEMENT_BEGIN--
drop table dom_table;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain inotnull;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop function sql_is_distinct_from(anyelement, anyelement);
--DDL_STATEMENT_END--
--
-- Renaming
--
--DDL_STATEMENT_BEGIN--
create domain testdomain1 as int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain testdomain1 rename to testdomain2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter type testdomain2 rename to testdomain3;  -- alter type also works
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain testdomain3;
--DDL_STATEMENT_END--

--
-- Renaming domain constraints
--
--DDL_STATEMENT_BEGIN--
create domain testdomain1 as int constraint unsigned check (value > 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain testdomain1 rename constraint unsigned to unsigned_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter domain testdomain1 drop constraint unsigned_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop domain testdomain1;
--DDL_STATEMENT_END--