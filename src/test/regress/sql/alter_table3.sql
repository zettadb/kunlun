drop table if exists t1;
drop table if exists t100;
drop table if exists t101;
drop table if exists t102;
drop table if exists t103;
drop table if exists t30;
drop table if exists t301;
drop table if exists t302;
drop table if exists t303;
drop table if exists t304;

create table t1(a serial primary key, b int) partition by hash(a);
create table t100 partition of t1 for values with (modulus 4, remainder 0);
create table t101 partition of t1 for values with (modulus 4, remainder 1);
create table t102 partition of t1 for values with (modulus 4, remainder 2);
create table t103 partition of t1 for values with (modulus 4, remainder 3);

insert into t1(b) values(11),(22),(33);

alter table t1 add column c int not null;
\d+ t1;
select*from t1;
alter table t1 drop column c;
select*from t1;
insert into t1(b) values(11),(22),(33), (11),(22),(33), (11),(22),(33), (11),(22),(33);
alter table t1 add column c int;
\d+ t1;
select*from t1;
alter table t1 alter column c set default 123;
\d+ t1;
insert into t1(b) values(44);
insert into t1(b,c) values(44, 45);
select*from t1;
alter table t1 alter column c drop default;
\d+ t1;
insert into t1(b) values(55);
select*from t1;
alter table t1 drop column c;

alter table t1 add column c int default 123456789;
\d+ t1;
select*from t1;
alter table t1 alter column c smallint not null;
alter table t1 alter column c type smallint;
\d+ t1;

alter table t1 alter column c type int not null;
alter table t1 alter column c set not null;
\d+ t1;
insert into t1(b) values(66);
select*from t1;
\d+ t1;
alter table t1 alter column c drop not null;
\d+ t1;
insert into t1(b) values(66);
select*from t1;
alter table t1 alter column c set not null;
\d+ t1;
insert into t1(b) values(66);
select*from t1;

alter table t1 add column d varchar(32) not null;
alter table t1 add column d varchar(32);
\d+ t1;
select*, d is null as disnull, length(d) from t1;

alter table t1 alter column d set default 'ddd';
\d+ t1;
insert into t1(b) values(77);
select*from t1;
alter table t1 alter column d drop default;
\d+ t1;
insert into t1(b) values(77);
select*from t1;

alter table t1 alter column d set not null;
\d+ t1;
insert into t1(b) values(77);
select*from t1;
alter table t1 alter column d drop not null;
\d+ t1;
insert into t1(b) values(77);
select*from t1;

alter table t1 drop column d;
alter table t1 add column d varchar(32) default 'dddddddd';
select*from t1;
alter table t1 alter column d type varchar(2) default 'd';
alter table t1 alter column d type varchar(2);
alter table t1 alter column d type char(2) default 'd';
alter table t1 alter column d type char(2);
alter table t1 alter column d type char(32);
\d+ t1;
insert into t1 (b) values(88);
select*from t1;

alter table t1 add column e varchar(32) not null default 'abc';
select*from t1;
alter table t1 rename c to cc, drop column c;
alter table t1 rename c to cc, rename b to bb;
alter table t1 rename a to aa;
\d+ t1;
insert into t1 (b) values(99);
select*from t1;

alter table t1 drop column c, add column f int not null default 123, add column g serial not null, alter column b type bigint;
\d+ t1;
select*from t1;
insert into t1(f) values(333);
insert into t1(b) values(222);
select*from t1;
alter table t1 rename f to ff;
alter table t1 rename d to dd;
\d+ t1;
select*from t1;

alter table t1 rename to t11;
\d+ t1;
\d+ t11;
alter table t11 rename b to bb;
\d+ t11;
select*from t11;
create index t11_b on t11(bb);
\d+ t11;
alter index t11_b rename to t11_bb;
\d+ t11;
select*from t11;

create schema scm3;
alter table t11 set schema scm3;
insert into scm3.t11(bb) values(333),(334),(335),(336),(337),(338),(339),(340),(341),(342),(343),(344),(345);
select*from scm3.t11;
update scm3.t11 set ff=ff+1 where bb > 10;
select*from scm3.t11;
delete from scm3.t11 where bb < 10;
select*from scm3.t11;

create table t11 (like scm3.t11 including all);
select*from t11;
insert into t11(bb) values(333),(334),(335),(336),(337),(338),(339),(340),(341),(342),(343),(344),(345);
select*from t11;
update t11 set ff=ff+1 where bb > 10;
select*from t11;
delete from t11 where bb < 10;
select*from t11;

create table t30(a serial , b int unique not null default 3, primary key(a,b)) partition by range(b);
create table t301 partition of t30 for values from (0) to (10000);
create table t302 partition of t30 for values from (10000) to (20000);
create table t303 partition of t30 for values from (20000) to (30000);
create table t304 partition of t30 for values from (30000) to (40000);

-- it's impossible to require c to be unique because that would require partition keys.
alter table t30 add column c serial;
insert into t30 (b) values(11),(12),(13),(14),(15);
insert into t30 (b) values(14000),(14001),(14002),(14003),(14004);
insert into t30 (b) values(25000),(25001),(25002),(25003),(25004);
insert into t30 (b) values(36000),(36001),(36002),(36003),(36004);
select*from t30;
-- when there are rows, can't demand uniqueness for the target column even the
-- column is nullable because mysql will add 0 as 'default default value' when
-- no default value specified.
alter table t30 add column d serial;
select*from t30;

alter table t30 rename a to aa;
alter table t30 rename b to bb;
alter table t30 rename c to cc;

alter table t30 add column e int not null;
alter table t30 add column f int;
alter table t30 alter column bb drop default, alter column bb add generated by default as identity(start 100);
\d+ t30;
insert into t30(e) values(1111),(1111),(1111),(1111);
update t30 set f=2222;
select*from t30;

alter table t30 alter column e add generated by default as identity, alter column f set not null, alter column f add generated by default as identity, alter bb drop identity;
\d+ t30;
insert into t30(bb) values(200),(201),(202),(203);
select*from t30;
--alter table t30 alter column e set start 1000;
create table t31 (like t30 including all) partition by hash(bb);
create table t310 partition of t31 for values with (modulus 4, remainder 0);
create table t311 partition of t31 for values with (modulus 4, remainder 1);
create table t312 partition of t31 for values with (modulus 4, remainder 2);
create table t313 partition of t31 for values with (modulus 4, remainder 3);

\d+ t31;
insert into t31 (bb) values(1),(2),(3),(4),(5),(10004),(10005),(10006),(20007),(20008),(30009);
select*from t31;
update t31 set bb=bb-1 where bb < 30000;
select*from t31;
delete from t31 where cc > 18000;
select*from t31;

drop table t11;
drop table t31;
drop table t30;
drop table scm3.t11;
drop schema scm3 cascade;
