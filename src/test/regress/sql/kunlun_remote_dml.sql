drop table if exists t1;

create table t1(a int primary key, b serial) partition by hash(a);
create table t100 partition of t1 for values with (modulus 4, remainder 0); 
create table t101 partition of t1 for values with (modulus 4, remainder 1); 
create table t102 partition of t1 for values with (modulus 4, remainder 2); 
create table t103 partition of t1 for values with (modulus 4, remainder 3); 
insert into t1 values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20);
select*from t1;
select a from t1 where a between 3 and 11;
select * from t1 where a in (3,7,11, 13);
update t1 set a=a+1 where a > 1 and a < 5;
delete from t1 where a %5=0;
select * from t1 where a %5=0;
select*from t1;

start transaction;
delete from t1 where a > 10;
select*from t1;
create table tfail(a int);
delete from t1;
select*from t1;
commit;

start transaction;
delete from t1;
select*from t1;
rollback;
select*from t1;


drop table if exists t5 cascade;
create table t5(a int primary key, b timestamptz default '2022-06-02 13:00:00+00', c varchar(32) default 'abc') partition by range(a);
create table t501 partition of t5 for values from (MINVALUE) to (10);
create table t502 partition of t5 for values from (10) to (20);
create table t503 partition of t5 for values from (20) to (30);
create table t504 partition of t5 for values from (30) to (MAXVALUE);
insert into t5 values(-10),(0), (15),(40) returning *;
insert into t5 values(-20),(10), (25),(400);

PREPARE q1(int, int) AS SELECT * FROM t5 WHERE a between $1 and $2 order by 1;

EXECUTE q1(-100, 20);
EXECUTE q1(0, 40);


PREPARE q2(int, int) AS SELECT * FROM t1 WHERE a between $1 and $2 order by 1;

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

PREPARE q4(int, int) as update t1 set b=$1 where a=$2;
select*from t1;
EXECUTE q4(10, 9);
EXECUTE q3(0, 40);
EXECUTE q4(11, 10);

begin;
EXECUTE q4(14, 13);
EXECUTE q4(12, 11);
select*from t1 where a between 9 and 12;
commit;

drop table t1;
drop table t4;
drop table t5;

-- bug 30
drop table if exists uv_iocu_tab cascade;
create table uv_iocu_tab (a serial primary key, b varchar(50));
insert into uv_iocu_tab (b) values('abc'),('bcd'),('xyz');
create view uv_iocu_view as select b as bb, a as aa, uv_iocu_tab::varchar(50) as cc from uv_iocu_tab;
select * from uv_iocu_view;
drop view uv_iocu_view;
create view uv_iocu_view as select a as aa, b as bb, uv_iocu_tab::varchar(50) as cc from uv_iocu_tab;
select * from uv_iocu_view;


-- bug 31
drop table if exists base_tbl cascade;
CREATE TABLE base_tbl(id serial primary key, a float);
INSERT INTO base_tbl (a) SELECT i/10.0 FROM generate_series(1,10) g(i);
CREATE VIEW rw_view1 AS SELECT sin(a) s, a, cos(a) c FROM base_tbl WHERE a != 0 ORDER BY abs(a);
select*from rw_view1;
drop view rw_view1;
CREATE VIEW rw_view1 AS SELECT sin(a) s, a, cos(a) c FROM base_tbl WHERE a != 0 ORDER BY abs(a);
select*from rw_view1;
INSERT INTO rw_view1 (a) VALUES (1.1) RETURNING a, s, c;
select*from rw_view1;

-- bug 39
drop table if exists SUBSELECT_TBL cascade;
CREATE TABLE SUBSELECT_TBL ( id serial primary key, f1 integer, f2 integer, f3 float );
INSERT INTO SUBSELECT_TBL (f1, f2, f3) VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5), (1, 1, 1), (2, 2, 2), (3, 3, 3), (6, 7, 8), (8, 9, NULL);
SELECT f1 AS "Correlated Field", f2 AS "Second Field" FROM SUBSELECT_TBL upper WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1);

create temp table rngfunc(f1 int8, f2 int8);
create function testrngfunc() returns record as $$

    insert into rngfunc values (1,2) returning *;

$$ language sql;
select testrngfunc();
select * from testrngfunc() as t(f1 int8,f2 int8);
select * from testrngfunc() as t(f1 int8,f2 int8);

-- bug 66 
DROP TABLE if exists prt1 cascade;
DROP TABLE if exists prt2 cascade;
CREATE TABLE prt1 (a int primary key, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 2 = 0;
CREATE TABLE prt2 (a int, b int primary key, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 3 = 0;

SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
EXPLAIN(verbose)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM prt1 t1, prt2 t2 WHERE t1::text = t2::text AND t1.a = t2.b ORDER BY t1.a;
EXPLAIN(verbose)
SELECT t1.a, t2.b FROM prt1 t1, prt2 t2 WHERE t1::text = t2::text AND t1.a = t2.b ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN  (SELECT t2.a AS t2a, t3.a AS t3a, least(t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
EXPLAIN(verbose)
SELECT * FROM prt1 t1 LEFT JOIN  (SELECT t2.a AS t2a, t3.a AS t3a, least(t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

EXPLAIN(verbose)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

SET enable_partitionwise_join to true;
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
EXPLAIN(verbose)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM prt1 t1, prt2 t2 WHERE t1::text = t2::text AND t1.a = t2.b ORDER BY t1.a;
EXPLAIN(verbose)
SELECT t1.a, t2.b FROM prt1 t1, prt2 t2 WHERE t1::text = t2::text AND t1.a = t2.b ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN  (SELECT t2.a AS t2a, t3.a AS t3a, least(t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
EXPLAIN(verbose)
SELECT * FROM prt1 t1 LEFT JOIN  (SELECT t2.a AS t2a, t3.a AS t3a, least(t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

EXPLAIN(verbose)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;


-- bug 71
DROP table if exists T;
CREATE TABLE T(pk INT NOT NULL PRIMARY KEY);
INSERT INTO T VALUES (1);
ALTER TABLE T ADD COLUMN c1 TIMESTAMP DEFAULT '2020-10-24 13:36:35+08';
select*from T;
insert into T values(2);
select*from T;

 CREATE OR REPLACE FUNCTION foo(a INT) RETURNS TEXT AS $$
DECLARE res TEXT = 'xyz';

    i INT;

BEGIN

    i = 0;
    WHILE (i < a) LOOP

        res = res || chr(ascii('a') + i);

        i = i + 1;

    END LOOP;
    RETURN res;

END; $$ LANGUAGE PLPGSQL STABLE;
DROP table if exists T;
CREATE TABLE T(pk INT NOT NULL PRIMARY KEY, c_int INT DEFAULT LENGTH(foo(6)));
INSERT INTO T VALUES (1), (2);
select*from t;
ALTER TABLE T ADD COLUMN c_bpchar BPCHAR(50) DEFAULT foo(4), ALTER COLUMN c_int SET DEFAULT LENGTH(foo(8));
select*from t;



-- bug 65
drop table if exists itest13 cascade;
CREATE TABLE itest13 (a int primary key);
ALTER TABLE itest13 ADD COLUMN b int GENERATED BY DEFAULT AS IDENTITY;
INSERT INTO itest13 VALUES (1), (2), (3);
SELECT * FROM itest13;
ALTER TABLE itest13 ADD COLUMN c int GENERATED BY DEFAULT AS IDENTITY;
SELECT * FROM itest13;

drop table if exists itest6 cascade;
CREATE TABLE itest6 (a int GENERATED ALWAYS AS IDENTITY primary key, b text);
INSERT INTO itest6 DEFAULT VALUES;
ALTER TABLE itest6 ALTER COLUMN a SET GENERATED BY DEFAULT SET INCREMENT BY 2 SET START WITH 100 RESTART;
SELECT * FROM itest6;
INSERT INTO itest6 DEFAULT VALUES;
INSERT INTO itest6 DEFAULT VALUES;
SELECT * FROM itest6;


-- bug 68
drop table  if exists t2 cascade;
create table t2(a int);
create index on t2(a,a);


-- bug 13
SELECT SESSION_USER, CURRENT_USER;
drop schema if exists testschema cascade;
CREATE SCHEMA if not exists testschema;
SELECT SESSION_USER, CURRENT_USER;
CREATE TABLE testschema.foo (i serial primary key, j serial);
insert into testschema.foo default values;
insert into testschema.foo default values;
select*from  testschema.foo;
create sequence testschema.seq1;
select testschema.seq1.nextval, nextval('testschema.seq1');
drop schema testschema cascade;

-- bug 78
-- create table b1(v box);

-- bug 50 THIS BUG Is postponed
create user user1;
drop table if exists r1 cascade;
SET SESSION AUTHORIZATION user1;

SET row_security = on;
CREATE TABLE r1 (a int primary key);
INSERT INTO r1 VALUES (10), (20);
-- CREATE POLICY p1 ON r1 USING (false);
-- ALTER TABLE r1 ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE r1 FORCE ROW LEVEL SECURITY;
TABLE r1;
INSERT INTO r1 VALUES (1);
UPDATE r1 SET a = 1;
DELETE FROM r1;
SET SESSION AUTHORIZATION abc;

-- bug 84
drop table if exists collate_test10 cascade;
 CREATE TABLE collate_test10 (

    a int primary key,
    x varchar(50) COLLATE "C",
    y varchar(50) COLLATE "POSIX"

);
INSERT INTO collate_test10 VALUES (1, 'hij', 'hij'), (2, 'HIJ', 'HIJ');
select x < y from collate_test10;

drop table if exists collate_test1 cascade;
drop table if exists collate_test2 cascade;
CREATE TABLE collate_test1 (id serial primary key, a int, b varchar(50) COLLATE "C" NOT NULL);
CREATE TABLE collate_test2 (id serial primary key,  a int, b varchar(50) COLLATE "POSIX" );
INSERT INTO collate_test1 (a,b) VALUES (1, 'abc'), (2, 'Abc'), (3, 'bbc'), (4, 'ABD');
INSERT INTO collate_test2 (a,b) SELECT a,b FROM collate_test1;
SELECT a, b FROM collate_test2 WHERE a < 4 INTERSECT SELECT a, b FROM collate_test2 WHERE a > 1 ORDER BY 2;


-- bug 114
drop table if exists update_test cascade;
CREATE TABLE update_test (a INT DEFAULT 10, b INT, c TEXT);
INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test(b, a) VALUES (15, 10);
UPDATE update_test t SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a) WHERE CURRENT_USER = SESSION_USER;

-- bug 121
drop table if exists indext1 cascade;
create table indext1(id integer);
ALTER TABLE indext1 ADD CONSTRAINT oindext1_id_constraint UNIQUE (id);
ALTER TABLE indext1 DROP CONSTRAINT oindext1_id_constraint;

CREATE TABLE part1 (a serial primary key, b int, c varchar(32), unique (b,a)) PARTITION BY LIST (a);
create index part1_b_c2 on part1(b,c);
create table part1_0 partition of part1 for values in (1,2,3,4);

insert into part1 (b,c) values(11, 'abc'),(12,'bcd'),(13,'cde');
select*from part1;
ALTER TABLE part1 ADD CONSTRAINT opart1_c_constraint UNIQUE (c,a);
create table part1_1 partition of part1 for values in (5,6,7,8);
insert into part1 (b,c) values(14, 'def'),(15,'efg'),(16,'fgh');
alter table part1 add column d int not null;
select*from part1;
create table part1_2 partition of part1 for values in (9,10,11,12);
insert into part1 (b,c,d) values(17, 'ghi', 21),(18,'hij',22),(19,'ijk',23);
select*from part1;
ALTER TABLE part1 ADD CONSTRAINT opart1_b_constraint UNIQUE (b,a);
create table part1_3 partition of part1 for values in (13,14,15,16);
insert into part1 (b,c,d) values(20, 'jkl', 24),(21,'klm',25),(22,'lmn',26);
ALTER TABLE part1 ADD CONSTRAINT opart1_d_constraint UNIQUE (d,a);
ALTER TABLE part1 DROP CONSTRAINT opart1_c_constraint;
ALTER TABLE part1 DROP CONSTRAINT opart1_d_constraint;
ALTER TABLE part1 DROP CONSTRAINT opart1_b_constraint;
drop index part1_b_c2;
drop table part1 cascade;


-- bug 94
 drop table if exists FLOAT4_TBL cascade;
CREATE TABLE FLOAT4_TBL (f1 float4);
INSERT INTO FLOAT4_TBL(f1) VALUES (' 0.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1004.30 ');
INSERT INTO FLOAT4_TBL(f1) VALUES (' -34.84 ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e+20');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e-20');
select f1 from float4_tbl;
SELECT f.* FROM FLOAT4_TBL f WHERE f.f1 = 1004.3;
SELECT f.* FROM FLOAT4_TBL f WHERE f.f1 <> '1004.3';


