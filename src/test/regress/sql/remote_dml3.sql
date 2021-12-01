-- bug 215 Crash when no column needed from a RemoteJoin node 
drop table if exists atest5 cascade;
CREATE TABLE atest5 (one int, two int unique, three int, four int unique);
INSERT INTO atest5 VALUES (1,2,3);
SELECT 1 FROM atest5 a JOIN atest5 b USING (one);
SELECT 1 FROM atest5;

-- bug #223 Crash when no column needed from a remote node 
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
insert into onek(unique1, unique2,two,four,ten) select i,i,i%2, i%4,i%10 from generate_series(1,1000) i;
select ten, sum(distinct four) from onek a
group by grouping sets((ten,four),(ten))
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- bug #227 Window function causes agg pushdown check failure 
-- first continue use onek table created by prev case.
SELECT SUM(COUNT(four)) OVER () FROM onek WHERE ten=5;
SELECT SUM(COUNT(ten)) OVER () FROM onek WHERE four=3;
-- create own table
DROP table if exists INT4_TBL;
CREATE TABLE INT4_TBL(f1 int4);
INSERT INTO INT4_TBL(f1) VALUES (' 0 ');
INSERT INTO INT4_TBL(f1) VALUES ('123456 ');
INSERT INTO INT4_TBL(f1) VALUES (' -123456');
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');
SELECT SUM(COUNT(f1)) OVER () FROM int4_tbl WHERE f1=42;
SELECT SUM(COUNT(f1)) OVER () FROM int4_tbl WHERE f1=0;


-- bug 228 Column name may overflow if qualified with its owner table name 
drop table if exists INT8_TBL;
CREATE TABLE INT8_TBL(q1 int8, q2 int8);

INSERT INTO INT8_TBL VALUES(' 123 ',' 456');
INSERT INTO INT8_TBL VALUES('123 ','4567890123456789');
INSERT INTO INT8_TBL VALUES('4567890123456789','123');
INSERT INTO INT8_TBL VALUES(+4567890123456789,'4567890123456789');
INSERT INTO INT8_TBL VALUES('+4567890123456789','-4567890123456789');
SELECT * FROM INT8_TBL;

create view tt18v as

    select * from int8_tbl xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxy
    union all
    select * from int8_tbl xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxz;

explain (costs off) select * from tt18v;
select * from tt18v;


-- bug #229 partition_join.sql,connection to server was lost 
DROP TABLE if exists prt2;
CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 3 = 0;

DROP TABLE if exists prt1;
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 2 = 0;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;

-- bug [#247] Wrong Assert for multi-layer Append nodes 
SELECT 1 AS three UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;
EXPLAIN SELECT 1 AS three UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;
SELECT 1 AS three UNION SELECT 3 UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;
EXPLAIN SELECT 1 AS three UNION SELECT 3 UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;

-- bug 245 Sort should not be pushed down if it uses a set returning funcs or exprs to sort
drop table if exists few;
CREATE TABLE few(id int, dataa text, datab text);
INSERT INTO few VALUES(1, 'a', 'foo'),(2, 'a', 'bar'),(3, 'b', 'bar');

SELECT few.id, generate_series(1,3) g FROM few ORDER BY id, g DESC;
EXPLAIN SELECT few.id, generate_series(1,3) g FROM few ORDER BY id, g DESC;
SELECT few.id  FROM few ORDER BY id, random() DESC;
EXPLAIN SELECT few.id  FROM few ORDER BY id, random() DESC;

-- bug #221 string_agg omitted from agg pushdown 
CREATE TABLE ctv_data (v varchar(30), h varchar(30), c varchar(30), i int, d date);
insert into ctv_data VALUES

    ('v1','h2','foo', 3, '2015-04-01'::date),
    ('v2','h1','bar', 3, '2015-01-02'),
    ('v1','h0','baz', NULL, '2015-07-12'),
    ('v0','h4','qux', 4, '2015-07-15'),
    ('v0','h4','dbl', -3, '2014-12-15'),
    ('v0',NULL,'qux', 5, '2014-07-15'),
    ('v1','h2','quux',7, '2015-04-04');


    SELECT v,h, string_agg(c, E'\n') AS c, row_number() OVER(ORDER BY h) AS r

FROM ctv_data GROUP BY v, h ORDER BY 1,3,2

    \crosstabview v h c r

-- bug  #244 Agg not pushed down for count(1) 
drop table if exists atest5;
CREATE TABLE atest5 (one int, two int unique, three int, four int unique);
INSERT INTO atest5 VALUES (1,2,3),(2,3,4),(3,4,5),(5,6,7);
SELECT count(1) FROM atest5 a JOIN atest5 b USING (one);
EXPLAIN SELECT count(1) FROM atest5 a JOIN atest5 b USING (one);
SELECT count(1) FROM atest5;
EXPLAIN SELECT count(1) FROM atest5;
SELECT sum(2+3) FROM atest5 a JOIN atest5 b USING (one);
EXPLAIN SELECT sum(2+3) FROM atest5 a JOIN atest5 b USING (one);
SELECT sum(2+3) FROM atest5;
EXPLAIN SELECT sum(2+3) FROM atest5;


-- bug  #234 View derived conflicting RemoteScans not materialized 
drop table if exists test1;
create table test1 (id serial, t text);
insert into test1 (t) values ('a');
insert into test1 (t) values ('b');
insert into test1 (t) values ('c');
insert into test1 (t) values ('d');
insert into test1 (t) values ('e');

create view v_test1
as select 'v_' || t from test1;

copy (select t from test1 where id = 1 UNION select * from v_test1 ORDER BY 1) to stdout;

copy (select * from (select t from test1 where id = 1 UNION select * from v_test1 ORDER BY 1) t1) to stdout;

-- bug  #257 subquery produces more content than expected 
DROP TABLE if exists SUBSELECT_TBL;
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

SELECT f1, f2

    FROM SUBSELECT_TBL
    WHERE (f1, f2) NOT IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL

        WHERE f3 IS NOT NULL);


-- bug  #230 value too long for type character 
DROP TABLE if exists TEXT_TBL;
CREATE TABLE TEXT_TBL (f1 text);
INSERT INTO TEXT_TBL VALUES ('doh!');
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');

SELECT CAST(f1 AS char(10)) AS "char(text)" FROM TEXT_TBL;
