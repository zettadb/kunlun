--DDL_STATEMENT_BEGIN--
drop table if exists t1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t100;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t101;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t102;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t103;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t30;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t301;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t302;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t303;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t304;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create table t1(a serial primary key, b int) partition by hash(a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t100 partition of t1 for values with (modulus 4, remainder 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t101 partition of t1 for values with (modulus 4, remainder 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t102 partition of t1 for values with (modulus 4, remainder 2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t103 partition of t1 for values with (modulus 4, remainder 3);
--DDL_STATEMENT_END--

insert into t1(b) values(11),(22),(33);

--alter table t1 add column c int not null;
--\d+ t1;
--select*from t1;
--alter table t1 drop column c;
select*from t1;
insert into t1(b) values(11),(22),(33), (11),(22),(33), (11),(22),(33), (11),(22),(33);
--DDL_STATEMENT_BEGIN--
alter table t1 add column c int;
--DDL_STATEMENT_END--
--\d+ t1;
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 alter column c set default 123;
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1(b) values(44);
insert into t1(b,c) values(44, 45);
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 alter column c drop default;
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1(b) values(55);
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 drop column c;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
alter table t1 add column c int default 123456789;
--DDL_STATEMENT_END--
--\d+ t1;
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 alter column c smallint not null;
--DDL_STATEMENT_END--
--alter table t1 alter column c type smallint;
--\d+ t1;

--DDL_STATEMENT_BEGIN--
alter table t1 alter column c type int not null;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t1 alter column c set not null;
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1(b) values(66);
select*from t1;
--\d+ t1;
--DDL_STATEMENT_BEGIN--
alter table t1 alter column c drop not null;
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1(b) values(66);
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 alter column c set not null;
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1(b) values(66);
select*from t1;

--alter table t1 add column d varchar(32) not null;
--DDL_STATEMENT_BEGIN--
alter table t1 add column d varchar(32);
--DDL_STATEMENT_END--
--\d+ t1;
select*, d is null as disnull, length(d) from t1;

--DDL_STATEMENT_BEGIN--
alter table t1 alter column d set default 'ddd';
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1(b) values(77);
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 alter column d drop default;
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1(b) values(77);
select*from t1;

--DDL_STATEMENT_BEGIN--
alter table t1 alter column d ;
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1(b) values(77);
--select*from t1;
--alter table t1 alter column d drop not null;
--\d+ t1;
insert into t1(b) values(77);
select*from t1;

--DDL_STATEMENT_BEGIN--
alter table t1 drop column d;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t1 add column d varchar(32) default 'dddddddd';
--DDL_STATEMENT_END--
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 alter column d type varchar(2) default 'd';
--DDL_STATEMENT_END--
--alter table t1 alter column d type varchar(2);
--DDL_STATEMENT_BEGIN--
alter table t1 alter column d type char(2) default 'd';
--DDL_STATEMENT_END--
--alter table t1 alter column d type char(2);
--DDL_STATEMENT_BEGIN--
alter table t1 alter column d type char(32);
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1 (b) values(88);
select*from t1;

--DDL_STATEMENT_BEGIN--
alter table t1 add column e varchar(32) not null default 'abc';
--DDL_STATEMENT_END--
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 rename c to cc, drop column c;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t1 rename c to cc, rename b to bb;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t1 rename a to aa;
--DDL_STATEMENT_END--
--\d+ t1;
insert into t1 (b) values(99);
select*from t1;

--DDL_STATEMENT_BEGIN--
alter table t1 drop column c, add column f int not null default 123, add column g int not null default 123, alter column b type bigint;
--DDL_STATEMENT_END--
--\d+ t1;
select*from t1;
insert into t1(f) values(333);
insert into t1(b) values(222);
select*from t1;
--DDL_STATEMENT_BEGIN--
alter table t1 rename f to ff;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t1 rename d to dd;
--DDL_STATEMENT_END--
--\d+ t1;
select*from t1;

--DDL_STATEMENT_BEGIN--
alter table t1 rename to t11;
--DDL_STATEMENT_END--
--\d+ t1;
--\d+ t11;
--DDL_STATEMENT_BEGIN--
alter table t11 rename b to bb;
--DDL_STATEMENT_END--
--\d+ t11;
select*from t11;
--DDL_STATEMENT_BEGIN--
create index t11_b on t11(bb);
--DDL_STATEMENT_END--
--\d+ t11;
--DDL_STATEMENT_BEGIN--
alter index t11_b rename to t11_bb;
--DDL_STATEMENT_END--
--\d+ t11;
select*from t11;

--DDL_STATEMENT_BEGIN--
create schema scm3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t11 set schema scm3;
--DDL_STATEMENT_END--
insert into scm3.t11(bb) values(333),(334),(335),(336),(337),(338),(339),(340),(341),(342),(343),(344),(345);
select*from scm3.t11;
update scm3.t11 set ff=ff+1 where bb > 10;
select*from scm3.t11 order by aa;
delete from scm3.t11 where bb < 10;
select*from scm3.t11 order by aa;

--DDL_STATEMENT_BEGIN--
create table t11 (like scm3.t11 including all);
--DDL_STATEMENT_END--
select*from t11;
insert into t11(bb) values(333),(334),(335),(336),(337),(338),(339),(340),(341),(342),(343),(344),(345);
select*from t11;
update t11 set ff=ff+1 where bb > 10;
select*from t11;
delete from t11 where bb < 10;
select*from t11;

--DDL_STATEMENT_BEGIN--
create table t30(a serial , b int unique not null default 3, primary key(a,b)) partition by range(b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t301 partition of t30 for values from (0) to (10000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t302 partition of t30 for values from (10000) to (20000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t303 partition of t30 for values from (20000) to (30000);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t304 partition of t30 for values from (30000) to (40000);
--DDL_STATEMENT_END--

-- it's impossible to require c to be unique because that would require partition keys.
--DDL_STATEMENT_BEGIN--
alter table t30 add column c serial;
--DDL_STATEMENT_END--
insert into t30 (b) values(11),(12),(13),(14),(15);
insert into t30 (b) values(14000),(14001),(14002),(14003),(14004);
insert into t30 (b) values(25000),(25001),(25002),(25003),(25004);
insert into t30 (b) values(36000),(36001),(36002),(36003),(36004);
select*from t30;
-- when there are rows, can't demand uniqueness for the target column even the
-- column is nullable because mysql will add 0 as 'default default value' when
-- no default value specified.
--alter table t30 add column d serial;
select*from t30;

--DDL_STATEMENT_BEGIN--
alter table t30 rename a to aa;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t30 rename b to bb;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t30 rename c to cc;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
alter table t30 add column e int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t30 add column f int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table t30 alter column bb drop default, alter column bb add generated by default as identity(start 100);
--DDL_STATEMENT_END--
--\d+ t30;
insert into t30(e) values(1111),(1111),(1111),(1111);
update t30 set f=2222;
select*from t30;

--DDL_STATEMENT_BEGIN--
alter table t30 alter column e add generated by default as identity, alter column f set not null, alter column f add generated by default as identity, alter bb drop identity;
--DDL_STATEMENT_END--
--\d+ t30;
insert into t30(bb) values(200),(201),(202),(203);
select*from t30;
--DDL_STATEMENT_BEGIN--
alter table t30 alter column e set start 1000;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t31 (like t30 including all) partition by hash(bb);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t310 partition of t31 for values with (modulus 4, remainder 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t311 partition of t31 for values with (modulus 4, remainder 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t312 partition of t31 for values with (modulus 4, remainder 2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t313 partition of t31 for values with (modulus 4, remainder 3);
--DDL_STATEMENT_END--

--\d+ t31;
insert into t31 (bb) values(1),(2),(3),(4),(5),(10004),(10005),(10006),(20007),(20008),(30009);
select*from t31;
--update t31 set bb=bb-1 where bb < 30000;
--select*from t31;
delete from t31 where cc > 18000;
select*from t31;

--DDL_STATEMENT_BEGIN--
drop table t11;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table t31;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table t30;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table scm3.t11;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop schema scm3 cascade;
--DDL_STATEMENT_END--