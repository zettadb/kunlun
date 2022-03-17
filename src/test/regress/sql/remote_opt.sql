--DDL_STATEMENT_BEGIN--
drop table if exists t10 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t100 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t101 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t102 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t103 cascade;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create table t10(a serial primary key, b int, c int not null) partition by hash(a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t100 partition of t10 for values with (modulus 4, remainder 0); 
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t101 partition of t10 for values with (modulus 4, remainder 1); 
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t102 partition of t10 for values with (modulus 4, remainder 2); 
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t103 partition of t10 for values with (modulus 4, remainder 3); 
--DDL_STATEMENT_END--
insert into t10(a  , b  , c) values
(1 , 16 , 0),
(12 ,  3 , 4),
(26 , NULL, 1),
(28 , NULL, 2),
(30 , NULL, 3),
(32 , NULL, 1),
(34 , NULL, 3),
(44 , NULL, 3),
(45 , NULL, 3),
(50 , NULL, 3),
(3 , 37 , 0),
(11 ,  1 , 2),
(21 , NULL, 1),
(31 , NULL, 1),
(35 , NULL, 3),
(41 , NULL, 1),
(46 , NULL, 1),
(2 , 27 , 0),
(13 ,  5 , 6),
(23 , NULL, 2),
(25 , NULL, 3),
(27 , NULL, 1),
(42 , NULL, 1),
(43 , NULL, 2),
(47 , NULL, 1),
(48 , NULL, 2),
(49 , NULL, 3),
(4 , 48 , 4),
(22 , NULL, 1),
(24 , NULL, 3),
(29 , NULL, 3),
(33 , NULL, 2);

select*from t10 order by a;

--DDL_STATEMENT_BEGIN--
drop table if exists t11 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t11 (like t10 INCLUDING ALL);
--DDL_STATEMENT_END--
insert into t11 select*from t10;
select*from t11 order by a;

--DDL_STATEMENT_BEGIN--
drop table if exists t12 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t12 (a int primary key, b int, c int not null) partition by range (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t120 partition of t12 for values from (minvalue) to (13);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t121 partition of t12 for values from (13) to (22);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t122 partition of t12 for values from (22) to (44);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t123 partition of t12 for values from (44) to (maxvalue);
--DDL_STATEMENT_END--

insert into t12 select*from t10;
-- explain insert into t12 select*from t10;

select*from t12 order by a;
delete from t12;
insert into t12 select*from t11;
-- explain insert into t12 select*from t11;

--DDL_STATEMENT_BEGIN--
drop table if exists t13 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t13 (like t11 including all);
--DDL_STATEMENT_END--

insert into t13 select*from t11;
-- explain insert into t13 select*from t11;
select*from t13 order by a;


select*from t12 where c in (select c from t13 where a > 30) order by t12.a;
-- explain select*from t12 where c in (select c from t13 where a > 30) order by t12.a;


select*from t12 where c not in (select c from t13 where a > 30) order by t12.a;
explain select*from t12 where c not in (select c from t13 where a > 30) order by t12.a;

select*from t12 where c not in (select c from t13 where a < 10) order by t12.a;
explain select*from t12 where c not in (select c from t13 where a < 10) order by t12.a;

select*from t12 where exists (select c from t13 where a < 10) order by t13.a;
-- explain select*from t12 where exists (select c from t13 where a < 10) order by t13.a;
select*from t12 where exists (select c from t13 where a < t12.b) order by t12.a;
-- explain select*from t12 where exists (select c from t13 where a < t12.b) order by t12.a;
select*from t12 where exists (select c from t13 where a < t12.c) order by t12.a;
-- explain select*from t12 where exists (select c from t13 where a < t12.c) order by t12.a;
select*from t12 where exists (select c from t13 where a < t12.a) order by t13.a;
-- explain select*from t12 where exists (select c from t13 where a < t12.a) order by t13.a;

set enable_hashjoin=false;
set enable_mergejoin=false;
select*from t10, t12 where t10.a=t12.a order by t12.a;
-- explain select*from t10, t12 where t10.a=t12.a order by t12.a;
select*from t10, t13 where t10.a=t13.a order by t10.a;
-- explain select*from t10, t13 where t10.a=t13.a order by t10.a;
select*from t11, t13 where t11.a=t13.a order by t11.a;
-- explain select*from t11, t13 where t11.a=t13.a order by t11.a;

set enable_hashjoin=true;
set enable_nestloop=false;
set enable_mergejoin=false;
select*from t10, t12 where t10.a=t12.a order by t10.a;
-- explain select*from t10, t12 where t10.a=t12.a order by t10.a;
select*from t10, t13 where t10.a=t13.a order by t10.a;
-- explain select*from t10, t13 where t10.a=t13.a order by t10.a;
select*from t11, t13 where t11.a=t13.a order by t13.a;
-- explain select*from t11, t13 where t11.a=t13.a order by t13.a;

set enable_hashjoin=false;
set enable_nestloop=false;
set enable_mergejoin=true;
select*from t10, t12 where t10.a=t12.a order by t10.a;
-- explain select*from t10, t12 where t10.a=t12.a order by t10.a;
select*from t10, t13 where t10.a=t13.a order by t10.a;
-- explain select*from t10, t13 where t10.a=t13.a order by t10.a;
select*from t11, t13 where t11.a=t13.a order by t11.a;
-- explain select*from t11, t13 where t11.a=t13.a order by t11.a;

--DDL_STATEMENT_BEGIN--
drop table if exists t14 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t14 (a int primary key, b varchar(32), c char(16));
--DDL_STATEMENT_END--
insert into t14 values(1, 'abc', 'def'),(2, '',''),(3, '',NULL),(4, NULL,''),(5, '','xyz'),(6,NULL, 'cbn'), (7, 'mit',''),(8, 'yale',NULL);
select*from t14 order by a;

select a, (case b when null then 'NULL' else b end) as b from t14 order by a;
select a, (case b when NULL then 'NULL' else b end) as b, (case c when NULL then 'NULL' else c end) as c from t14 order by a;
select a, (case b  when '' then 'empty' else b end) as b, c is null from t14 order by a;
select a, (case b  when '' then 'empty' when NULL then 'NULL' else b end) as b, c is null from t14 order by a;
select a, b is null as bisnull, c is null as cisnull from t14 order by a;
select a, (case b  when '' then 'empty' else case b is null when  true then 'NULL' else b end end) as b, c is null from t14 order by a;
select a, (case (b is null)  when true then 'NULL' else case b when '' then 'empty' else b end end) as b, c is null from t14 order by a;
