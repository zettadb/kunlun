show autocommit;
drop schema if exists autocommit_schm cascade;
create schema autocommit_schm;
use autocommit_schm;
show search_path;
create table t1(a serial primary key,  b int);

insert into t1(b) values(17);
select*from t1;

set autocommit=off;
insert into t1(b) values(19);
commit;
-- better check before and after above commit in another session
select*from t1;

set autocommit=on;
insert into t1(b) values(31);
select*from t1;

begin;insert into t1(b) values(37);commit;
-- better check before and after above commit in another session
select*from t1;

set autocommit to off;
insert into t1(b) values(53);
commit;
-- better check before and after above commit in another session
select*from t1;

BEGIN;BEGIN;
insert into t1(b) values(59);
commit;
-- better check before and after above commit and BEGIN in another session
select*from t1;

BEGIN;BEGIN;BEGIN;
insert into t1(b) values(61);
BEGIN; -- implicitly commit
insert into t1(b) values(67);
begin; -- implicitly commit
commit;
-- better check before and after above commit and each BEGIN stmt in another session
select*from t1;

show autocommit;

insert into t1(b) values(71);

insert into t1(b) values(73);
set autocommit=true; -- implicitly commit

-- better check before and after above stmt in another session
select*from t1;

set autocommit=off;
insert into t1(b) values(79);
begin; -- implicitly commit
insert into t1(b) values(83);
set autocommit=1; -- implicitly commit
insert into t1(b) values(89);

-- better check before and after above begin and "set autocommit=1" stmt in another session
select*from t1;

drop schema if exists autocommit_schm cascade;
