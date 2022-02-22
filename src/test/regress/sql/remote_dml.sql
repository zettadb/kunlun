drop table if exists t1;
drop table if exists t100;
drop table if exists t101;
drop table if exists t102;
drop table if exists t103;
create table t1(a int primary key, b serial) partition by hash(a);
create table t100 partition of t1 for values with (modulus 4, remainder 0); 
create table t101 partition of t1 for values with (modulus 4, remainder 1); 
create table t102 partition of t1 for values with (modulus 4, remainder 2); 
create table t103 partition of t1 for values with (modulus 4, remainder 3); 
insert into t1 values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20);
select*from t1;
select a from t1 where a between 3 and 11;
select * from t1 where a in (3,7,11, 13);
--update t1 set a=a+1 where a > 1 and a < 5;
delete from t1 where a %5=0;
select * from t1 where a %5=0;
select*from t1;

--start transaction;
delete from t1 where a > 10;
select*from t1;
create table tfail(a int);
delete from t1;
select*from t1;
commit;

--start transaction;
delete from t1;
select*from t1;
rollback;
select*from t1;

drop table if exists t2;
create table t2(a int primary key, b timestamptz default now(), c varchar(32) default 'xyz');
--start transaction;
insert into t2 values(1) returning *;
insert into t2 values(8, NULL, 'xxx'),(9, NULL, NULL) returning *;
insert into t2 values(10, now(), 'abc'),(11, '2006-06-02 13:36:35+08', '你好');
commit;

insert into t2 values(1) returning *;

begin;
insert into t2 values(1) returning *;
commit;

select*from t2;


drop table if exists t3;
drop table if exists t301;
drop table if exists t302;
create table t3(a int, b varchar(16) NOT NULL, c int, primary key(b,a)) partition by list(a);
create table t301 partition of t3 for values in (1,3,5,7,9);
create table t302 partition of t3 for values in (2,4,6, 8, 10);
insert into t3 values (1, 'amd', 24254),(2, 'intel', 325332),(3, 'broadcom', 345220),(4, 'nvidia', 87902),(5, 'huawei',89790),(6, 'apple',45232);
create index on t3(c);
select*from t3 where c > 100000;

drop table if exists t5;
drop table if exists t501;
drop table if exists t502;
drop table if exists t503;
drop table if exists t504;
create table t5(a int primary key, b timestamptz default now(), c varchar(32) default 'abc') partition by range(a);
create table t501 partition of t5 for values from (MINVALUE) to (10);
create table t502 partition of t5 for values from (10) to (20);
create table t503 partition of t5 for values from (20) to (30);
create table t504 partition of t5 for values from (30) to (MAXVALUE);
insert into t5 values(-10),(0), (15),(40) returning *;
insert into t5 values(-20),(10), (25),(400);
select*from t5 where a between 30 and 100;

PREPARE q1(int, int) AS SELECT * FROM t5 WHERE a between $1 and $2;

EXECUTE q1(-100, 20);
EXECUTE q1(0, 40);


PREPARE q2(int, int) AS SELECT * FROM t1 WHERE a between $1 and $2;

EXECUTE q2(0, 10);
EXECUTE q1(10, 30);
EXECUTE q2(5, 40);
deallocate q1;
drop table if exists t4 cascade;
create table t4(a int primary key, b serial);
insert into t4 values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20);

PREPARE q3(int, int) AS SELECT * FROM t4 WHERE a between $1 and $2;


EXECUTE q3(6, 16);

EXECUTE q1(0, 30);
EXECUTE q2(5, 10);
deallocate q2;

EXECUTE q3(0, 40);

PREPARE q4(int, int) as update t1 set b=$1 where a=$2;
select*from t1;
EXECUTE q4(10, 9);
EXECUTE q3(0, 40);
EXECUTE q4(11, 10);

prepare q6(int) as insert into t5 values($1);
EXECUTE q6(1);
EXECUTE q6(2);

begin;
EXECUTE q6(12);
EXECUTE q4(14, 13);
EXECUTE q6(13);
commit;

EXECUTE q6(21);
EXECUTE q6(22);
EXECUTE q6(23);

begin;
EXECUTE q6(31);
EXECUTE q6(33);
select*from t5;
commit;

prepare q5(varchar(32), int, varchar(32)) as update  t5 set c=$1 where a%7=$2 and c=$3;

begin;
EXECUTE q5('def', 3, 'abc');

EXECUTE q6(3);
EXECUTE q6(4);
commit;

EXECUTE q6(11);
EXECUTE q6(32);
EXECUTE q3(0, 40);
deallocate q3;
EXECUTE q3(10, 40);

begin;
EXECUTE q4(12, 11);
select*from t1 where a between 9 and 12;
select*from t5 ;

EXECUTE q5('xyz', 3, 'def');
EXECUTE q5('XYZ', 4, 'abc');
commit;

EXECUTE q5('MNO', 5, 'abc');
select*from t5 where a%7=3 or a%7=4 or a%7=5;
EXECUTE q5('qps', 11, 'XYZ');
deallocate q4;
deallocate q5;

create table t31(a int primary key, b int, c int, d int);

insert into t31 values
(21 ,  1 , 21 ,  1),
( 22 ,  2 , 22 ,  2),
( 23 ,  3 , 23 ,  3),
( 24 , 14 , 24 ,  4),
( 25 , 15 , 25 ,  5);
select a,b,c, case a%2 when 0 then b when 1 then c end as x from t31 where (case when b > 10 then b%3=2 when b <10 then b%2=1 end);
select count(*) from t2 where b <= current_timestamp;
select * from t31 where a=any(select b+23 from t31);
select * from t31 where a=all(select b+23 from t31);
select * from t31 where a!=all(values(1),(21),(23));
select * from t31 where a=any(values(1),(21),(22));
select greatest(a,b,c) as g, least(a,b,c) as l from t31;
select greatest(a,b,c) as g, least(a,b,c) as l, coalesce(null,a,b,c) from t31;

drop table t31;
drop table t2;
drop table t1;
drop table t4;
drop table t5;
