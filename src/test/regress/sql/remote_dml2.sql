-- regression test cases from all bug reports in trac.

-- bug 17 The command does not return the number of affected records 
drop table if exists tx1 cascade;
create table tx1(id int primary key, c char(50));
insert into tx1 values(1,'1');
insert into tx1 values(2, '2');
insert into tx1 values(3, '3');
delete from tx1;

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
CREATE VIEW rw_view1 AS SELECT ctid, sin(a) s, a, cos(a) c FROM base_tbl WHERE a != 0 ORDER BY abs(a);
select*from rw_view1;
drop view rw_view1;
CREATE VIEW rw_view1 AS SELECT sin(a) s, a, cos(a) c FROM base_tbl WHERE a != 0 ORDER BY abs(a);
select*from rw_view1;
INSERT INTO rw_view1 (a) VALUES (1.1) RETURNING a, s, c;
select*from rw_view1;

-- bug 29
drop table if exists base_tbl cascade;
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
INSERT INTO base_tbl SELECT i, 'Row ' i FROM generate_series(-2, 2) g(i);
CREATE VIEW rw_view16 AS SELECT a, b, a AS aa FROM base_tbl;
UPDATE rw_view16 SET aa=-3 WHERE a=3;
UPDATE rw_view16 SET aa=-5 WHERE a=5;
select*from rw_view16;
delete from rw_view16 where aa=2;
insert into rw_view16 values(4,'new row');
UPDATE rw_view16 SET aa=-4 WHERE a=4;
select*from rw_view16;

drop table if exists base_tbl cascade;
CREATE TABLE base_tbl(a int primary key, b varchar(50), c float);
CREATE VIEW rw_view2 AS SELECT b AS bb, c AS cc, a AS aa FROM base_tbl;
UPDATE base_tbl SET a=a, c=c;
UPDATE rw_view2 SET bb=bb, cc=cc;
select*from base_tbl;
select*from rw_view2;

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

-- bug 48
create user userw;
SELECT SESSION_USER, CURRENT_USER;
SET SESSION AUTHORIZATION userw;
drop table if exists testu1 cascade;
create table testu1(id integer primary key, name varchar(50));
insert into testu1 values(1, 'userx');
insert into testu1 values(2, 'userw');
SELECT SESSION_USER, CURRENT_USER;
SET SESSION AUTHORIZATION dzw;

-- bug 51
drop table if exists r1 cascade;
drop table if exists r2 cascade;
CREATE TABLE r1 (id serial primary key, a int);
CREATE TABLE r2 (id serial primary key, a int);
INSERT INTO r1 (a) VALUES (10), (20);
INSERT INTO r2 (a) VALUES (10), (20);
INSERT INTO r1 (a) SELECT a + 1 FROM r2;
INSERT INTO r1 (a) SELECT a + 1 FROM r2 RETURNING *;
select*from r1;
select*from r2;

-- bug 60
drop table if exists at_base_table cascade;
create table at_base_table(id int primary key, stuff text);
insert into at_base_table values (23, 'skidoo');
create view at_view_1 as select * from at_base_table bt;
create view at_view_2 as select *, to_json(v1) as j from at_view_1 v1;
explain (verbose, costs off) select * from at_view_2;
explain (verbose, costs off) select * from at_view_1;
select * from at_view_2;
select * from at_view_1;

-- bug 69
drop table if exists pagg_tab_m cascade;
CREATE TABLE pagg_tab_m (id serial , a int, b int, c int, primary key(id, a,b)) PARTITION BY RANGE(a, b);
CREATE TABLE pagg_tab_m_p1 PARTITION OF pagg_tab_m FOR VALUES FROM (0, 0) TO (10, 10);
CREATE TABLE pagg_tab_m_p2 PARTITION OF pagg_tab_m FOR VALUES FROM (10, 10) TO (20, 20);
CREATE TABLE pagg_tab_m_p3 PARTITION OF pagg_tab_m FOR VALUES FROM (20, 20) TO (30, 30);
INSERT INTO pagg_tab_m(a,b,c) SELECT i % 30, i % 40, i % 50 FROM generate_series(0, 2999) i;
EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY a, (a+b)/2 HAVING sum(b) < 50 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY a, (a+b)/2 HAVING sum(b) < 50 ORDER BY 1, 2, 3;
EXPLAIN (COSTS OFF)
SELECT a, c, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY (a+b)/2, 2, 1 HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;
SELECT a, c, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY (a+b)/2, 2, 1 HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY a,b HAVING sum(b) < 2000 and avg(c) > 27;
explain (verbose)
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY a,b HAVING sum(b) < 2000 and avg(c) > 27;

-- bug 53
drop table if exists trunc_stats_test1 cascade;
CREATE TABLE trunc_stats_test1(id serial primary key, stuff text);
insert into trunc_stats_test1 (stuff) values('abc'), ('xyz');
select*from trunc_stats_test1;
UPDATE trunc_stats_test1 SET id = id + 10 WHERE id IN (1, 2);
select*from trunc_stats_test1;

drop table if exists itest1 cascade;
CREATE TABLE itest1 (a int generated by default as identity primary key, b text);
INSERT INTO itest1 DEFAULT VALUES;
insert into itest1 values(DEFAULT, 'bbb');
INSERT INTO itest1 VALUES (2, 'b');
INSERT INTO itest1 VALUES (10, 'xyz');
UPDATE itest1 SET a = DEFAULT WHERE a = 2;
select*from itest1;

alter table itest1 add column c int default 3;
UPDATE itest1 SET c = DEFAULT WHERE a = 2;
select*from itest1;
insert into itest1(b,c) values('aaa', 44);
select*from itest1;
UPDATE itest1 SET c = DEFAULT WHERE a = 2;
select*from itest1;
insert into itest1(b,c) values('aaa', 44);
select*from itest1;
UPDATE itest1 SET c = DEFAULT WHERE a = 3;
select*from itest1;
UPDATE itest1 SET a = DEFAULT WHERE a = 3;
select*from itest1;
insert into itest1 values(DEFAULT, 'xxx', 333), (DEFAULT, 'yyy', 444);
select*from itest1;

-- bug 28
DROP table if exists base_tbl cascade;
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
INSERT INTO base_tbl SELECT i, 'Row ' i FROM generate_series(-2, 2) g(i);
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a>0;
EXPLAIN (costs off) UPDATE rw_view1 SET a=6 WHERE a=5;
EXPLAIN (costs off) DELETE FROM rw_view1 WHERE a=5;

DROP table if exists T cascade;
CREATE TABLE T (pk INT NOT NULL PRIMARY KEY);
INSERT INTO T SELECT * FROM generate_series(1, 10) a;
EXPLAIN (VERBOSE TRUE, COSTS FALSE) DELETE FROM T WHERE pk BETWEEN 10 AND 20 RETURNING *;

drop table if exists mvtest_t cascade;
CREATE TABLE mvtest_t (id int NOT NULL PRIMARY KEY, type varchar(50) NOT NULL, amt numeric NOT NULL);
EXPLAIN (costs off)
	CREATE MATERIALIZED VIEW mvtest_tm AS SELECT type, sum(amt) AS totamt FROM mvtest_t GROUP BY type WITH NO DATA;


-- bug 36
DROP table if exists parted_conflict cascade;
create table parted_conflict (a int primary key, b text) partition by range (a);
create table parted_conflict_1 partition of parted_conflict for values from (0) to (1000) partition by range (a);
create table parted_conflict_1_1 partition of parted_conflict_1 for values from (0) to (500);
insert into parted_conflict values (40, 'forty'), (30, 'thirty');
alter table parted_conflict add column c int default 3;
select*from parted_conflict;
delete from parted_conflict where a = 30;
update parted_conflict set c=c+10 where a=40;
select*from parted_conflict;

-- bug 43 
drop table if exists int4_tbl cascade;
drop table if exists int8_tbl cascade;
drop table if exists text_tbl cascade;
CREATE TABLE INT4_TBL(id serial primary key, f1 int4);
CREATE TABLE INT8_TBL(id serial primary key, q1 int8, q2 int8);
CREATE TABLE TEXT_TBL (id serial primary key, f1 text);
insert into int4_tbl(f1) values(1),(2),(3),(4);
insert into int8_tbl(q1, q2) values(1,2),(2,3),(3,4),(4,5);
insert into text_tbl(f1) values('1'),('2'),('3'),('4'),('5');

explain (verbose, costs off)
select * from

    text_tbl t1
    left join int8_tbl i8
    on i8.q2 = 2,
    lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
    lateral (select ss1.* from text_tbl t3 limit 1) as ss2

where t1.f1 = ss2.f1;

-- AND no explain--

select * from

    text_tbl t1
    left join int8_tbl i8
    on i8.q2 = 2,
    lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
    lateral (select ss1.* from text_tbl t3 limit 1) as ss2

where t1.f1 = ss2.f1;

-- AND --

explain (verbose)
select ss2.* from

    int4_tbl i41
    left join int8_tbl i8

        join (select i42.f1 as c1, i43.f1 as c2, 42 as c3

            from int4_tbl i42, int4_tbl i43) ss1

        on i8.q1 = ss1.c2

    on i41.f1 = ss1.c1,
    lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2

where ss1.c2 = 2;

select ss2.* from

    int4_tbl i41
    left join int8_tbl i8

        join (select i42.f1 as c1, i43.f1 as c2, 42 as c3

            from int4_tbl i42, int4_tbl i43) ss1

        on i8.q1 = ss1.c2

    on i41.f1 = ss1.c1,
    lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2

where ss1.c2 = 2;
-----------------------------
drop table if exists rngfunc2 cascade;
CREATE TABLE rngfunc2(id serial primary key, rngfuncid int, f2 int);
INSERT INTO rngfunc2(rngfuncid, f2) VALUES(1, 11);
INSERT INTO rngfunc2(rngfuncid, f2) VALUES(2, 22);
INSERT INTO rngfunc2(rngfuncid, f2) VALUES(1, 111);
drop function if exists rngfunct;
CREATE FUNCTION rngfunct(int) returns setof rngfunc2 as 'SELECT * FROM rngfunc2 WHERE rngfuncid = $1 ORDER BY f2;' LANGUAGE SQL;
select * from rngfunc2, rngfunct(rngfunc2.rngfuncid) z where rngfunc2.f2 = z.f2;

select * from rngfunc2, rngfunct(rngfunc2.rngfuncid) with ordinality as z(rngfuncid,f2,ord) where rngfunc2.f2 = z.f2;

select * from rngfunc2 where f2 in (select f2 from rngfunct(rngfunc2.rngfuncid) z where z.rngfuncid = rngfunc2.rngfuncid) ORDER BY 1,2;

select * from rngfunc2 where f2 in (select f2 from rngfunct(1) z where z.rngfuncid = rngfunc2.rngfuncid) ORDER BY 1,2;

select * from rngfunc2 where f2 in (select f2 from rngfunct(rngfunc2.rngfuncid) z where z.rngfuncid = 1) ORDER BY 1,2;


DROP TABLE if exists prt1_l cascade;
CREATE TABLE prt1_l (a int , b int, c varchar, primary key(a,c,b)) PARTITION BY RANGE(a);
CREATE TABLE prt1_l_p1 PARTITION OF prt1_l FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_l_p2 PARTITION OF prt1_l FOR VALUES FROM (250) TO (500) PARTITION BY LIST (c);
CREATE TABLE prt1_l_p2_p1 PARTITION OF prt1_l_p2 FOR VALUES IN ('0000', '0001');
CREATE TABLE prt1_l_p2_p2 PARTITION OF prt1_l_p2 FOR VALUES IN ('0002', '0003');
CREATE TABLE prt1_l_p3 PARTITION OF prt1_l FOR VALUES FROM (500) TO (600) PARTITION BY RANGE (b);
CREATE TABLE prt1_l_p3_p1 PARTITION OF prt1_l_p3 FOR VALUES FROM (0) TO (13);
CREATE TABLE prt1_l_p3_p2 PARTITION OF prt1_l_p3 FOR VALUES FROM (13) TO (25);
INSERT INTO prt1_l SELECT i, i % 25, to_char(i % 4, 'FM0000') FROM generate_series(0, 599, 2) i;
DROP TABLE if exists prt2_l cascade;
CREATE TABLE prt2_l (a int, b int , c varchar, primary key(b,c,a)) PARTITION BY RANGE(b);
CREATE TABLE prt2_l_p1 PARTITION OF prt2_l FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_l_p2 PARTITION OF prt2_l FOR VALUES FROM (250) TO (500) PARTITION BY LIST (c);
CREATE TABLE prt2_l_p2_p1 PARTITION OF prt2_l_p2 FOR VALUES IN ('0000', '0001');
CREATE TABLE prt2_l_p2_p2 PARTITION OF prt2_l_p2 FOR VALUES IN ('0002', '0003');
CREATE TABLE prt2_l_p3 PARTITION OF prt2_l FOR VALUES FROM (500) TO (600) PARTITION BY RANGE (a);
CREATE TABLE prt2_l_p3_p1 PARTITION OF prt2_l_p3 FOR VALUES FROM (0) TO (13);
CREATE TABLE prt2_l_p3_p2 PARTITION OF prt2_l_p3 FOR VALUES FROM (13) TO (25);
INSERT INTO prt2_l SELECT i % 25, i, to_char(i % 4, 'FM0000') FROM generate_series(0, 599, 3) i;
SELECT * FROM prt1_l t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.b AS t3b, least(t1.a,t2.a,t3.b) FROM prt1_l t2 JOIN prt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss ON t1.a = ss.t2a AND t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.a;
EXPLAIN(verbose)
SELECT * FROM prt1_l t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.b AS t3b, least(t1.a,t2.a,t3.b) FROM prt1_l t2 JOIN prt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss ON t1.a = ss.t2a AND t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.a;


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
-- bug 67
SET enable_partitionwise_join to true;
DROP TABLE if exists prt1_l cascade;
CREATE TABLE prt1_l (a int, b int, c varchar, primary key(a,b,c)) PARTITION BY RANGE(a);
CREATE TABLE prt1_l_p1 PARTITION OF prt1_l FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_l_p2 PARTITION OF prt1_l FOR VALUES FROM (250) TO (500) PARTITION BY LIST (c);
CREATE TABLE prt1_l_p2_p1 PARTITION OF prt1_l_p2 FOR VALUES IN ('0000', '0001');
CREATE TABLE prt1_l_p2_p2 PARTITION OF prt1_l_p2 FOR VALUES IN ('0002', '0003');
CREATE TABLE prt1_l_p3 PARTITION OF prt1_l FOR VALUES FROM (500) TO (600) PARTITION BY RANGE (b);
CREATE TABLE prt1_l_p3_p1 PARTITION OF prt1_l_p3 FOR VALUES FROM (0) TO (13);
CREATE TABLE prt1_l_p3_p2 PARTITION OF prt1_l_p3 FOR VALUES FROM (13) TO (25);
INSERT INTO prt1_l SELECT i, i % 25, to_char(i % 4, 'FM0000') FROM generate_series(0, 599, 2) i;
DROP TABLE if exists prt2_l cascade;
CREATE TABLE prt2_l (a int, b int, c varchar, primary key(a,b,c)) PARTITION BY RANGE(b);
CREATE TABLE prt2_l_p1 PARTITION OF prt2_l FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_l_p2 PARTITION OF prt2_l FOR VALUES FROM (250) TO (500) PARTITION BY LIST (c);
CREATE TABLE prt2_l_p2_p1 PARTITION OF prt2_l_p2 FOR VALUES IN ('0000', '0001');
CREATE TABLE prt2_l_p2_p2 PARTITION OF prt2_l_p2 FOR VALUES IN ('0002', '0003');
CREATE TABLE prt2_l_p3 PARTITION OF prt2_l FOR VALUES FROM (500) TO (600) PARTITION BY RANGE (a);
CREATE TABLE prt2_l_p3_p1 PARTITION OF prt2_l_p3 FOR VALUES FROM (0) TO (13);
CREATE TABLE prt2_l_p3_p2 PARTITION OF prt2_l_p3 FOR VALUES FROM (13) TO (25);
INSERT INTO prt2_l SELECT i % 25, i, to_char(i % 4, 'FM0000') FROM generate_series(0, 599, 3) i;
EXPLAIN (COSTS OFF) SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

SET enable_partitionwise_join to false;
EXPLAIN (COSTS OFF) SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

-- bug 16
DROP TABLE if exists insertconflicttest1 cascade;
create table insertconflicttest1(key1 int4, fruit text);
create index idx1 on insertconflicttest1(fruit);


-- bug 40 
drop table if exists SUBSELECT_TBL cascade;
CREATE TABLE SUBSELECT_TBL ( id serial primary key, f1 integer, f2 integer, f3 float );
INSERT INTO SUBSELECT_TBL (f1, f2, f3) VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5), (1, 1, 1), (2, 2, 2), (3, 3, 3), (6, 7, 8), (8, 9, NULL);
SELECT f1 AS "Correlated Field", f3 AS "Second Field" FROM SUBSELECT_TBL upper WHERE f3 IN (SELECT upper.f1 + f2 FROM SUBSELECT_TBL WHERE f2 = CAST(f3 AS integer));

-- bug 33
begin;
savepoint sa;
release savepoint sa;
commit;

-- bug 42
drop table if exists tenk1 cascade;
drop table if exists INT4_TBL cascade;
drop table if exists FLOAT8_TBL cascade;
 CREATE TABLE tenk1 (

    unique1 int4,
    unique2 int4,
    two int4,
    four int4,
    ten int4,
    twenty int4,
    hundred int4,
    thousand int4,
    twothousand int4,
    fivethous int4,
    tenthous int4,
    odd int4,
    even int4,
    stringu1 name,
    stringu2 name,
    string4 name

);
COPY tenk1 FROM '/home/dzw/work/kunlun-enterprise/postgresql-11.5/src/test/regress/data/tenk.data';
CREATE TABLE INT4_TBL(f1 int4);
INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

INSERT INTO INT4_TBL(f1) VALUES ('34.5');
CREATE TABLE FLOAT8_TBL(f1 float8);
INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');

begin;

select count(*) from tenk1 x where x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and

    x.unique1 = 0 and x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

rollback;


-- bug 81
drop table if exists INT8_TBL cascade;
CREATE TABLE INT8_TBL(id serial primary key, q1 int8, q2 int8);
INSERT INTO INT8_TBL(q1, q2)  VALUES(' 123 ',' 456');
INSERT INTO INT8_TBL(q1, q2)  VALUES('123 ','4567890123456789');
INSERT INTO INT8_TBL(q1, q2)  VALUES('4567890123456789','123');
INSERT INTO INT8_TBL(q1, q2)  VALUES(+4567890123456789,'4567890123456789');
INSERT INTO INT8_TBL(q1, q2)  VALUES('+4567890123456789','-4567890123456789');
select t1.q2, count(t2.*) from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;
explain(verbose)
select t1.q2, count(t2.*) from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;

select t1.q2, count(t2.*) from int8_tbl t1 left join (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;
explain (verbose)
select t1.q2, count(t2.*) from int8_tbl t1 left join (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;
select t1.q2, count(t2.*) from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;
explain (verbose)
select t1.q2, count(t2.*) from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;

explain (verbose)
select t1.q2, count(*) from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;
select t1.q2, count(*) from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;

select t1.q2, count(*) from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;
explain(verbose)
select t1.q2, count(*) from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;

select t1.q2, count(*) from int8_tbl t1 left join (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;
explain (verbose)
select t1.q2, count(*) from int8_tbl t1 left join (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;

explain (verbose)
select t1.q2, count(*) from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;
select t1.q2, count(*) from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1) group by t1.q2 order by 1;

-- bug 71
DROP table if exists T;
CREATE TABLE T(pk INT NOT NULL PRIMARY KEY);
INSERT INTO T VALUES (1);
ALTER TABLE T ADD COLUMN c1 TIMESTAMP DEFAULT now();
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


-- bug 21
drop table if exists mlparted cascade;
drop table if exists mlparted1 cascade;
create table mlparted(id integer primary key);
create table mlparted1(id integer primary key);
select attrelid::regclass, attname, attnum from pg_attribute where attname = 'a' and (attrelid = 'mlparted'::regclass or attrelid = 'mlparted1'::regclass);
insert into mlparted values(1),(2),(3),(4);
insert into mlparted1 values(5),(2),(3),(4);
select attrelid::regclass, attname, attnum from pg_attribute where attname = 'a' and (attrelid = 'mlparted'::regclass or attrelid = 'mlparted1'::regclass);


-- bug 32
drop table if exists test_missing_target cascade;
CREATE TABLE test_missing_target (a int primary key, b int, c char(8), d char);
INSERT INTO test_missing_target VALUES (0, 1, 'XXXX', 'A'), (1, 2, 'ABAB', 'b'), (2, 2, 'ABAB', 'c'), (3, 3, 'BBBB', 'D'), (4, 3, 'BBBB', 'e'), (5, 3, 'bbbb', 'F'), (6, 4, 'cccc', 'g'), (7, 4, 'cccc', 'h'), (8, 4, 'CCCC', 'I'), (9, 4, 'CCCC', 'j');
SELECT x.b, count(*) FROM test_missing_target x, test_missing_target y WHERE x.a = y.a GROUP BY x.b ORDER BY x.b;
explain (verbose)
SELECT x.b, count(*) FROM test_missing_target x, test_missing_target y WHERE x.a = y.a GROUP BY x.b ORDER BY x.b;

SELECT count(*) FROM test_missing_target x, test_missing_target y WHERE x.a = y.a GROUP BY x.b ORDER BY x.b;
explain (verbose)
SELECT count(*) FROM test_missing_target x, test_missing_target y WHERE x.a = y.a GROUP BY x.b ORDER BY x.b;

SELECT x.b/2, count(x.b) FROM test_missing_target x, test_missing_target y WHERE x.a = y.a GROUP BY x.b/2 ORDER BY x.b/2;
explain (verbose)
SELECT x.b/2, count(x.b) FROM test_missing_target x, test_missing_target y WHERE x.a = y.a GROUP BY x.b/2 ORDER BY x.b/2;


-- bug 64
drop table if exists itest7 cascade;
CREATE TABLE itest7 (id int primary key, a int GENERATED ALWAYS AS IDENTITY);
insert into itest7 (id) values(1),(2),(3);
insert into itest7 values(4, 40),(5, 50);
select*from itest7;
ALTER TABLE itest7 ALTER COLUMN a SET GENERATED BY DEFAULT;
insert into itest7 values(6, 60),(7, 70);
select*from itest7;

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

-- bug 83
drop table if exists revalidate_bug cascade cascade;
drop function if exists inverse(int);
create function inverse(int) returns float8 as
$$
begin

    return 1::float8/$1;

exception

    when division_by_zero then return 0;

end$$ language plpgsql volatile;
create table revalidate_bug (id serial primary key, c float8 unique);
insert into revalidate_bug (c) values (1);
insert into revalidate_bug (c) values (inverse(0));
select*from revalidate_bug ;

-- bug 61
drop schema if exists s1 cascade;
create schema s1;
create table s1.t1(id int);
alter table s1.t1 set schema s1;
drop schema if exists s1 cascade;

--bug 80
drop table if exists SUBSELECT_TBL cascade;
CREATE TABLE SUBSELECT_TBL ( id serial primary key, f1 integer, f2 integer, f3 float );
INSERT INTO SUBSELECT_TBL (f1, f2, f3) VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5), (1, 1, 1), (2, 2, 2), (3, 3, 3), (6, 7, 8), (8, 9, NULL);
SELECT f1, f2 FROM SUBSELECT_TBL WHERE (f1, f2) NOT IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL WHERE f3 IS NOT NULL);


-- bug 33
begin;
savepoint sa;
release savepoint sa;
commit;
begin;
savepoint sa;
release savepoint sa;
rollback;

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

-- bug 44 todo
drop table if exists tenk1 cascade;
drop table if exists INT4_TBL cascade;
 CREATE TABLE tenk1 (
 	id serial primary key,
    unique1 int4,
    unique2 int4,
    two int4,
    four int4,
    ten int4,
    twenty int4,
    hundred int4,
    thousand int4,
    twothousand int4,
    fivethous int4,
    tenthous int4,
    odd int4,
    even int4,
    stringu1 name,
    stringu2 name,
    string4 name
);
-- unique1, unique2, two, four, ten, twenty, hundred, thousand, twothousand, fivethous, tenthous, odd, even, stringu1 , stringu2 ,string4 
CREATE TABLE INT4_TBL(id serial primary key, f1 int4);

COPY tenk1(unique1, unique2, two, four, ten, twenty, hundred, thousand, twothousand, fivethous, tenthous, odd, even, stringu1 , stringu2 ,string4) FROM '/home/dzw/work/postgresql-11.5/src/test/regress/data/tenk.data';
insert into int4_tbl (f1) select generate_series(1,13);

explain (verbose, costs off)
    select b.unique1 from
        tenk1 a join tenk1 b on a.unique1 = b.unique2
        left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
        join int4_tbl i1 on b.thousand = f1
        right join int4_tbl i2 on i2.f1 = b.tenthous
        order by 1;

explain (verbose, costs off)
    select b.unique1 from
        tenk1 a join tenk1 b on a.unique1 = b.unique2
        join int4_tbl i1 on b.thousand = f1
        right join int4_tbl i2 on i2.f1 = b.tenthous
        order by 1;

set enable_nestloop=false;
    select b.unique1 from
        tenk1 a join tenk1 b on a.unique1 = b.unique2
        left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
        join int4_tbl i1 on b.thousand = f1
        right join int4_tbl i2 on i2.f1 = b.tenthous
        order by 1;

set enable_nestloop=false;
    select b.unique1 from
        tenk1 a join tenk1 b on a.unique1 = b.unique2
        join int4_tbl i1 on b.thousand = f1
        right join int4_tbl i2 on i2.f1 = b.tenthous
        order by 1;

set enable_remote_join_pushdown = false;
explain (costs off)
    select b.unique1 from
        tenk1 a join tenk1 b on a.unique1 = b.unique2
        left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
        join int4_tbl i1 on b.thousand = f1
        right join int4_tbl i2 on i2.f1 = b.tenthous
        order by 1;

explain (costs off)
    select b.unique1 from
        tenk1 a join tenk1 b on a.unique1 = b.unique2
        join int4_tbl i1 on b.thousand = f1
        right join int4_tbl i2 on i2.f1 = b.tenthous
        order by 1;

set enable_nestloop=false;
    select b.unique1 from
        tenk1 a join tenk1 b on a.unique1 = b.unique2
        left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
        join int4_tbl i1 on b.thousand = f1
        right join int4_tbl i2 on i2.f1 = b.tenthous
        order by 1;

set enable_nestloop=false;
    select b.unique1 from
        tenk1 a join tenk1 b on a.unique1 = b.unique2
        join int4_tbl i1 on b.thousand = f1
        right join int4_tbl i2 on i2.f1 = b.tenthous
        order by 1;
-- below portion is fixed, above portion not yet. TODO
 drop table if exists mlparted cascade;
drop table if exists mlparted1 cascade;
drop table if exists mlparted11 cascade;
create table mlparted (a int, b int) partition by range (a, b);
create table mlparted1 (b int not null, a int not null) partition by range ((b+0));
create table mlparted11 (like mlparted1);
alter table mlparted1 attach partition mlparted11 for values from (2) to (5);
alter table mlparted attach partition mlparted1 for values from (1, 2) to (1, 10);
with ins (a, b, c) as (insert into mlparted (b, a) select s.a, 1 from generate_series(2, 39) s(a) returning tableoid::regclass, *) select a, b, min(c), max(c) from ins group by a, b order by 1;
with ins (a, b, c) as (insert into mlparted (b, a) select s.a, 1 from generate_series(2, 39) s(a) returning tableoid::regclass, *) select a, b, min(c), max(c) from ins group by a, b order by 1;


-- bug 41
 drop table if exists INT8_TBL cascade;
CREATE TABLE INT8_TBL(id serial primary key, q1 int8, q2 int8);

INSERT INTO INT8_TBL (q1, q2) VALUES(' 123 ',' 456');
INSERT INTO INT8_TBL (q1, q2) VALUES('123 ','4567890123456789');
INSERT INTO INT8_TBL (q1, q2) VALUES('4567890123456789','123');
INSERT INTO INT8_TBL (q1, q2) VALUES(+4567890123456789,'4567890123456789');
INSERT INTO INT8_TBL (q1, q2) VALUES('+4567890123456789','-4567890123456789');
select q1, float8(count(*)) / (select count(*) from int8_tbl) from int8_tbl group by q1 order by q1;
explain (verbose)
select q1, float8(count(*)) / (select count(*) from int8_tbl) from int8_tbl group by q1 order by q1;



-- bug 78
create table b1(v box);

-- bug 50 THIS BUG Is postponed
create user user1;
SET SESSION AUTHORIZATION user1;

SET row_security = on;
drop table if exists r1 cascade;
CREATE TABLE r1 (a int primary key);
INSERT INTO r1 VALUES (10), (20);
CREATE POLICY p1 ON r1 USING (false);
ALTER TABLE r1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE r1 FORCE ROW LEVEL SECURITY;
TABLE r1;
INSERT INTO r1 VALUES (1);
UPDATE r1 SET a = 1;
DELETE FROM r1;
SET SESSION AUTHORIZATION dzw;

-- bug 57
drop table if exists temptest1 cascade;
begin;
CREATE TEMP TABLE temptest1(col int PRIMARY KEY);
insert into temptest1 values (1),(2);
select*from temptest1;
commit;

drop table if exists temptest2 cascade;
begin;
CREATE TEMP TABLE temptest2(col int PRIMARY KEY) ON COMMIT DELETE ROWS;
insert into temptest2 values (1),(2);
select*from temptest2;
commit;

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


-- bug 37
CREATE TABLE moneyp (a money) PARTITION BY LIST (a);
CREATE TABLE moneyp_10 PARTITION OF moneyp FOR VALUES IN (10);
drop table if exists list_parted cascade;
CREATE TABLE list_parted (a int) PARTITION BY LIST (a);
CREATE TABLE part_1 PARTITION OF list_parted FOR VALUES IN ('1');
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN (int '1');
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN ('1'::int);

-- bug 72
drop table if exists onek cascade;
CREATE TABLE onek (
    unique1 int4,
    unique2 int4,
    two int4,
    four int4,
    ten int4,
    twenty int4,
    hundred int4,
    thousand int4,
    twothousand int4,
    fivethous int4,
    tenthous int4,
    odd int4,
    even int4,
    stringu1 name,
    stringu2 name,
    string4 name
);

CREATE UNIQUE INDEX onek_idx ON onek (unique2 nulls first,unique1);

-- bug 34
drop table if exists part_attmp cascade;
CREATE TABLE part_attmp (a int primary key) partition by range (a);
CREATE TABLE part_attmp1 PARTITION OF part_attmp FOR VALUES FROM (0) TO (100);
ALTER INDEX part_attmp_pkey RENAME TO part_attmp_index;
ALTER INDEX part_attmp1_pkey RENAME TO part_attmp1_index;
alter table part_attmp add column b int;
create index part_attmp_b_idx on part_attmp(b);
ALTER INDEX part_attmp_b_idx RENAME TO part_attmp_b_index;
ALTER INDEX part_attmp1_b_idx RENAME TO part_attmp1_b_index;


-- bug 14
drop table if exists test_default_tab cascade;
CREATE TABLE test_default_tab(pk serial primary key, id int);
CREATE INDEX test_index1 on test_default_tab (id);
ALTER TABLE test_default_tab ALTER id TYPE bigint;

drop table if exists anothertab cascade;
create table anothertab(f1 int primary key, f2 int unique,f3 int, f4 int, f5 int);
alter table anothertab add unique(f1,f4);
create index on anothertab(f2,f3);
create unique index on anothertab(f4);

alter table anothertab alter column f1 type bigint;
alter table anothertab
    alter column f2 type bigint,
    alter column f3 type bigint,
    alter column f4 type bigint;

create table at_partitioned(id int primary key, name varchar(64), unique (id, name)) partition by hash(id);
create table at_partitioned_1 partition of at_partitioned for values with (modulus 2, remainder 1);
alter table at_partitioned alter column name type varchar(127);

-- bug 24 this doesn't pass now. we have to disable DEFAULT partitions now.
-- drop table if exists range_parted2 cascade;
--  CREATE TABLE range_parted2 (a int primary key) PARTITION BY RANGE (a);
-- CREATE TABLE range2_default PARTITION OF range_parted2 DEFAULT;
-- INSERT INTO range_parted2 VALUES (85);
-- CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (80) TO (90);
-- CREATE TABLE part4 PARTITION OF range_parted2 FOR VALUES FROM (90) TO (100);
-- 
-- 
-- drop table if exists pc_list_parted cascade;
-- create table pc_list_parted (a int primary key) partition by list(a);
-- create table pc_list_part_1 partition of pc_list_parted for values in (1);
-- create table pc_list_part_2 partition of pc_list_parted for values in (2);
-- create table pc_list_part_def partition of pc_list_parted default;
-- create table pc_list_part_3 partition of pc_list_parted for values in (3);
-- 
-- drop table if exists quuux cascade;
-- CREATE TABLE quuux (a int primary key, b text) PARTITION BY LIST (a);
-- CREATE TABLE quuux_default PARTITION OF quuux DEFAULT PARTITION BY LIST (b);
-- CREATE TABLE quuux_default1 PARTITION OF quuux_default FOR VALUES IN ('b');
-- CREATE TABLE quuux1 (a int, b text);
-- ALTER TABLE quuux ATTACH PARTITION quuux1 FOR VALUES IN (1);
-- CREATE TABLE quuux2 (a int, b text);
-- ALTER TABLE quuux ATTACH PARTITION quuux2 FOR VALUES IN (2);
-- DROP TABLE quuux1;
-- DROP TABLE quuux2;
-- CREATE TABLE quuux1 PARTITION OF quuux FOR VALUES IN (1);
-- CREATE TABLE quuux2 PARTITION OF quuux FOR VALUES IN (2);

-- bug 62 this doesn't pass now. we have to disable 'ALTER TABLE ... ATTACH PARTITION' now.
-- CREATE TABLE list_parted2 (a int,b char) PARTITION BY LIST (a);
-- CREATE TABLE part_2 (LIKE list_parted2);
-- INSERT INTO part_2 VALUES (3, 'a');
-- ALTER TABLE list_parted2 ATTACH PARTITION part_2 FOR VALUES IN (2);
--

-- bug 56
drop table if exists pc_list_parted  cascade;
create table pc_list_parted (a int primary key) partition by list(a);
create table pc_list_part_null partition of pc_list_parted for values in (null);
create table pc_list_part_1 partition of pc_list_parted for values in (1);
insert into pc_list_part_1 values(2);

-- bug 25
drop table if exists concur_heap cascade;
CREATE TABLE concur_heap (id serial primary key, f1 int, f2 int);
insert into concur_heap (f1, f2) values(2,3),(3,5),(5,7),(7,11);
CREATE INDEX CONCURRENTLY concur_index1 ON concur_heap(f2,f1);
select*from concur_heap;

-- bug 110
drop table if exists INT8_TBL cascade;
CREATE TABLE INT8_TBL(q1 int8, q2 int8);
INSERT INTO INT8_TBL VALUES(' 123 ',' 456');
INSERT INTO INT8_TBL VALUES('123 ','4567890123456789');
INSERT INTO INT8_TBL VALUES('4567890123456789','123');
INSERT INTO INT8_TBL VALUES(+4567890123456789,'4567890123456789');
INSERT INTO INT8_TBL VALUES('+4567890123456789','-4567890123456789');
create view tt17v as select * from int8_tbl i where i in (values(i));
select * from tt17v;

-- bug 111
drop table if exists persons cascade;
drop type if exists person_type cascade;
CREATE TYPE person_type AS (id int, name varchar(50));
CREATE TABLE persons OF person_type;

-- bug 112
drop table if exists base_tbl cascade;
drop view if existrs rw_view1;
CREATE TABLE base_tbl (a int, b int DEFAULT 10);
INSERT INTO base_tbl VALUES (1,2), (2,3), (1,-1);
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a < b WITH LOCAL CHECK OPTION;
INSERT INTO rw_view1 values(3,2),(4,3), (3,4);

-- bug 114
drop table if exists update_test cascade;
CREATE TABLE update_test (a INT DEFAULT 10, b INT, c TEXT);
INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test(b, a) VALUES (15, 10);
UPDATE update_test t SET (a, b) = (SELECT b, a FROM update_test s WHERE s.a = t.a) WHERE CURRENT_USER = SESSION_USER;

-- bug 116
drop table if exists t1 cascade;
create table t1(pk int not null primary key);
ALTER TABLE t1 ADD COLUMN c_num NUMERIC DEFAULT 1.00000000001;
insert into t1 values(1),(2),(3),(4),(5);
select * from t1;

-- bug 126
drop table if exists t1 cascade;
create table t1(a int);
insert into t1 values(1),(2),(3);
update t1 set a=3 returning *;
delete from t1 returning *;

-- bug 127
-- TODO: verify the stmt is sent to ddl log after each ddl stmt. so far we don't have such test facility.
DROP SCHEMA if exists testschema cascade;
CREATE SCHEMA testschema;
CREATE TABLE testschema.part0 (a int) PARTITION BY LIST (a);
drop table testschema.part0 cascade;
CREATE TABLE testschema.part1 (a serial primary key, b int, c varchar(32), unique (b,a)) PARTITION BY LIST (a);
create index part1_b_c2 on testschema.part1(b,c);
create table testschema.part1_0 partition of testschema.part1 for values in (1,2,3,4);
create table testschema.part1_1 partition of testschema.part1 for values in (5,6,7,8);
insert into testschema.part1 (b,c) values(14, 'def'),(15,'efg'),(16,'fgh');
select*from testschema.part1 ;
create index part1_b_c on testschema.part1(b,c);
insert into testschema.part1 (b,c) values(17, 'ghi'),(18,'hij'),(19,'ijk');
select*from testschema.part1 ;
drop index testschema.part1_b_c;
insert into testschema.part1 (b,c) values(11, 'abc'),(12,'bcd'),(13,'cde');
select*from testschema.part1 ;
drop schema testschema cascade;

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

-- bug 135
drop table if exists t1 cascade;
create table t1(v1 decimal(10,2));
insert into t1 values(1.1), (2.2);
select*from t1;
update t1 set v1 = v1 + 1;
select*from t1;

PREPARE pq1(int, int) AS SELECT * FROM t1 WHERE v1 between $1 and $2;
EXECUTE pq1(-4, 4);
PREPARE pq2(int) AS update t1 set v1=v1+$1;
EXECUTE pq2(1);
select*from t1;
PREPARE pq3(float) AS update t1 set v1=v1+$1;
EXECUTE pq3(2.0);
PREPARE pq4(float, float) AS SELECT * FROM t1 WHERE v1 between $1 and $2;
EXECUTE pq4(-8.0, 8.0);

-- bug 190
drop table if exists t2 cascade;
create table t2(id int primary key, good boolean);
insert into t2 values(1, true);
insert into t2 values(2, false);
update t2 set good = false where id = 1;

-- bug 199
drop table if exists t4 cascade;
create table t4(a money);

insert into t4 values(999);
insert into t4 values(1000);
insert into t4 values (-92233720368547758.08);
insert into t4 values (+92233720368547758.07);
select*from t4;
insert into t4 values (-92233720368547758.09); -- bigint out of range
insert into t4 values (+92233720368547758.08); -- bigint out of range

insert into t4 values (-1111);
select*from t4;

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

-- bug 118 Bad results for partition join query when SET enable_partitionwise_join to true
SET enable_partitionwise_join TO true;
drop table if exists pagg_tab1 cascade;
CREATE TABLE pagg_tab1(x int, y int) PARTITION BY RANGE(x);
CREATE TABLE pagg_tab1_p1 PARTITION OF pagg_tab1 FOR VALUES FROM (0) TO (10);
CREATE TABLE pagg_tab1_p2 PARTITION OF pagg_tab1 FOR VALUES FROM (10) TO (20);
CREATE TABLE pagg_tab1_p3 PARTITION OF pagg_tab1 FOR VALUES FROM (20) TO (30);
drop table if exists pagg_tab2 cascade;
CREATE TABLE pagg_tab2(x int, y int) PARTITION BY RANGE(y);
CREATE TABLE pagg_tab2_p1 PARTITION OF pagg_tab2 FOR VALUES FROM (0) TO (10);
CREATE TABLE pagg_tab2_p2 PARTITION OF pagg_tab2 FOR VALUES FROM (10) TO (20);
CREATE TABLE pagg_tab2_p3 PARTITION OF pagg_tab2 FOR VALUES FROM (20) TO (30);
INSERT INTO pagg_tab1 SELECT i % 30, i % 20 FROM generate_series(0, 299, 2) i;
INSERT INTO pagg_tab2 SELECT i % 20, i % 30 FROM generate_series(0, 299, 3) i;

DROP TABLE if exists prt1 cascade;
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 2 = 0;
DROP TABLE if exists prt2 cascade;
CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 3 = 0;

DROP TABLE if exists prt1_e cascade;
CREATE TABLE prt1_e (a int, b int, c int) PARTITION BY RANGE(((a + b)/2));
CREATE TABLE prt1_e_p1 PARTITION OF prt1_e FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_e_p2 PARTITION OF prt1_e FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_e_p3 PARTITION OF prt1_e FOR VALUES FROM (500) TO (600);
INSERT INTO prt1_e SELECT i, i, i % 25 FROM generate_series(0, 599, 2) i;
DROP TABLE if exists prt2_e cascade;
CREATE TABLE prt2_e (a int, b int, c int) PARTITION BY RANGE(((b + a)/2));
CREATE TABLE prt2_e_p1 PARTITION OF prt2_e FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_e_p2 PARTITION OF prt2_e FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_e_p3 PARTITION OF prt2_e FOR VALUES FROM (500) TO (600);
INSERT INTO prt2_e SELECT i, i, i % 25 FROM generate_series(0, 599, 3) i;


set enable_hashjoin=false;
set enable_mergejoin=false;
set enable_nestloop=true;
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;

SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;

SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;
EXPLAIN (verbose, COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;

SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;
EXPLAIN (verbose, COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;

SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
EXPLAIN (verbose, COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.c = 0 ORDER BY t1.a, t2.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.c = 0 ORDER BY t1.a, t2.b;

SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.a = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.b = 0 ORDER BY t1.a;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.a = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.b = 0 ORDER BY t1.a;

set enable_hashjoin=true;
set enable_mergejoin=false;
set enable_nestloop=false;
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;

SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;

SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;
EXPLAIN (verbose, COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;

SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;
EXPLAIN (verbose, COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;

SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
EXPLAIN (verbose, COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.c = 0 ORDER BY t1.a, t2.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.c = 0 ORDER BY t1.a, t2.b;

SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.a = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.b = 0 ORDER BY t1.a;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.a = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.b = 0 ORDER BY t1.a;



set enable_hashjoin=false;
set enable_mergejoin=true;
set enable_nestloop=false;
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;

SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;

SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;
EXPLAIN (verbose, COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;

SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;
EXPLAIN (verbose, COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY b.y ORDER BY 1 NULLS LAST;

SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
EXPLAIN (verbose, COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.c = 0 ORDER BY t1.a, t2.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.c = 0 ORDER BY t1.a, t2.b;

SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.a = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.b = 0 ORDER BY t1.a;
EXPLAIN (verbose, COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.a = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.b = 0 ORDER BY t1.a;

-- bug 226 Wrong Assert causing failure when an expression target is pushed down
DROP TABLE if exists SUBSELECT_TBL cascade;
CREATE TABLE SUBSELECT_TBL (

    f1 integer,
    f2 integer,
    f3 float

);
INSERT INTO SUBSELECT_TBL VALUES (1, 2, 3);
INSERT INTO SUBSELECT_TBL VALUES (2, 3, 4);
INSERT INTO SUBSELECT_TBL VALUES (3, 4, 5);
INSERT INTO SUBSELECT_TBL VALUES (1, 1, 1);
INSERT INTO SUBSELECT_TBL VALUES (2, 2, 2);
INSERT INTO SUBSELECT_TBL VALUES (3, 3, 3);
INSERT INTO SUBSELECT_TBL VALUES (6, 7, 8);
INSERT INTO SUBSELECT_TBL VALUES (8, 9, NULL);

DROP table if exists INT4_TBL;
CREATE TABLE INT4_TBL(f1 int4);
INSERT INTO INT4_TBL(f1) VALUES (' 0 ');
INSERT INTO INT4_TBL(f1) VALUES ('123456 ');
INSERT INTO INT4_TBL(f1) VALUES (' -123456');
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

SELECT ss.f1 AS "Correlated Field", ss.f3 AS "Second Field"

    FROM SUBSELECT_TBL ss
    WHERE f1 NOT IN (SELECT f1+1 FROM INT4_TBL

        WHERE f1 != ss.f1 AND f1 < 2147483647);

-- bug #236 Error about 'distinct on'
DROP table if exists INT4_TBL;
CREATE TABLE INT4_TBL(f1 int4);
INSERT INTO INT4_TBL(f1) VALUES (' 0 ');
INSERT INTO INT4_TBL(f1) VALUES ('123456 ');
INSERT INTO INT4_TBL(f1) VALUES (' -123456');
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

select distinct on (1) floor(random()) as r, f1 from int4_tbl order by 1,2;
