-- bug 215 Crash when no column needed from a RemoteJoin node 
--DDL_STATEMENT_BEGIN--
drop table if exists atest5 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE atest5 (one int, two int unique, three int, four int unique);
--DDL_STATEMENT_END--
INSERT INTO atest5 VALUES (1,2,3);
SELECT 1 FROM atest5 a JOIN atest5 b USING (one);
SELECT 1 FROM atest5;

-- bug #223 Crash when no column needed from a remote node 
--DDL_STATEMENT_BEGIN--
drop table if exists onek cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
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
--DDL_STATEMENT_END--
insert into onek(unique1, unique2,two,four,ten) select i,i,i%2, i%4,i%10 from generate_series(1,1000) i;
select ten, sum(distinct four) from onek a
group by grouping sets((ten,four),(ten))
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- bug #227 Window function causes agg pushdown check failure 
-- first continue use onek table created by prev case.
SELECT SUM(COUNT(four)) OVER () FROM onek WHERE ten=5;
SELECT SUM(COUNT(ten)) OVER () FROM onek WHERE four=3;
-- create own table
--DDL_STATEMENT_BEGIN--
DROP table if exists INT4_TBL cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE INT4_TBL(f1 int4);
--DDL_STATEMENT_END--
INSERT INTO INT4_TBL(f1) VALUES (' 0 ');
INSERT INTO INT4_TBL(f1) VALUES ('123456 ');
INSERT INTO INT4_TBL(f1) VALUES (' -123456');
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');
SELECT SUM(COUNT(f1)) OVER () FROM int4_tbl WHERE f1=42;
SELECT SUM(COUNT(f1)) OVER () FROM int4_tbl WHERE f1=0;


-- bug 228 Column name may overflow if qualified with its owner table name 
--DDL_STATEMENT_BEGIN--
drop table if exists INT8_TBL cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE INT8_TBL(q1 int8, q2 int8);
--DDL_STATEMENT_END--

INSERT INTO INT8_TBL VALUES(' 123 ',' 456');
INSERT INTO INT8_TBL VALUES('123 ','4567890123456789');
INSERT INTO INT8_TBL VALUES('4567890123456789','123');
INSERT INTO INT8_TBL VALUES(+4567890123456789,'4567890123456789');
INSERT INTO INT8_TBL VALUES('+4567890123456789','-4567890123456789');
SELECT * FROM INT8_TBL;

--DDL_STATEMENT_BEGIN--
create view tt18v as

    select * from int8_tbl xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxy
    union all
    select * from int8_tbl xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxz;
--DDL_STATEMENT_END--

explain (costs off) select * from tt18v;
select * from tt18v;


-- bug #229 partition_join.sql,connection to server was lost
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists prt2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
--DDL_STATEMENT_END--
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 3 = 0;

--DDL_STATEMENT_BEGIN--
DROP TABLE if exists prt1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
--DDL_STATEMENT_END--
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
--DDL_STATEMENT_BEGIN--
drop table if exists few cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE few(id int, dataa text, datab text);
--DDL_STATEMENT_END--
INSERT INTO few VALUES(1, 'a', 'foo'),(2, 'a', 'bar'),(3, 'b', 'bar');

SELECT few.id, generate_series(1,3) g FROM few ORDER BY id, g DESC;
EXPLAIN SELECT few.id, generate_series(1,3) g FROM few ORDER BY id, g DESC;
SELECT few.id  FROM few ORDER BY id, random() DESC;
EXPLAIN SELECT few.id  FROM few ORDER BY id, random() DESC;

-- bug #221 string_agg omitted from agg pushdown 
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctv_data (v varchar(30), h varchar(30), c varchar(30), i int, d date);
--DDL_STATEMENT_END--
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
--DDL_STATEMENT_BEGIN--
drop table if exists atest5 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE atest5 (one int, two int unique, three int, four int unique);
--DDL_STATEMENT_END--
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
--DDL_STATEMENT_BEGIN--
drop table if exists test1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table test1 (id serial, t text);
--DDL_STATEMENT_END--
insert into test1 (t) values ('a');
insert into test1 (t) values ('b');
insert into test1 (t) values ('c');
insert into test1 (t) values ('d');
insert into test1 (t) values ('e');

--DDL_STATEMENT_BEGIN--
create view v_test1
as select 'v_' || t from test1;
--DDL_STATEMENT_END--

copy (select t from test1 where id = 1 UNION select * from v_test1 ORDER BY 1) to stdout;

copy (select * from (select t from test1 where id = 1 UNION select * from v_test1 ORDER BY 1) t1) to stdout;

-- bug  #257 subquery produces more content than expected 
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists SUBSELECT_TBL cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE SUBSELECT_TBL (

    f1 integer,
    f2 integer,
    f3 float

);
--DDL_STATEMENT_END--

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
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists TEXT_TBL cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE TEXT_TBL (f1 text);
--DDL_STATEMENT_END--
INSERT INTO TEXT_TBL VALUES ('doh!');
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');

SELECT CAST(f1 AS char(10)) AS "char(text)" FROM TEXT_TBL;

-- bug #
--DDL_STATEMENT_BEGIN--
DROP table if exists INT4_TBL cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE INT4_TBL(f1 int4);
--DDL_STATEMENT_END--
INSERT INTO INT4_TBL(f1) VALUES (' 0 ');
INSERT INTO INT4_TBL(f1) VALUES ('123456 ');
INSERT INTO INT4_TBL(f1) VALUES (' -123456');
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');
INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

--DDL_STATEMENT_BEGIN--
DROP table if exists FLOAT8_TBL cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE FLOAT8_TBL(f1 float8);
--DDL_STATEMENT_END--

INSERT INTO FLOAT8_TBL(f1) VALUES ('    0.0   ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1004.30  ');
INSERT INTO FLOAT8_TBL(f1) VALUES ('   -34.84');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e+200');
INSERT INTO FLOAT8_TBL(f1) VALUES ('1.2345678901234e-200');
SELECT f1 AS five FROM FLOAT8_TBL
  WHERE f1 BETWEEN -1e6 AND 1e6
UNION
SELECT f1 FROM INT4_TBL
  WHERE f1 BETWEEN 0 AND 1000000
ORDER BY 1;


-- bug#  #318 Connection invalid after it's killed by timeout mechanism 
--DDL_STATEMENT_BEGIN--
drop table if exists join_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table join_foo(id integer, t varchar(50));
--DDL_STATEMENT_END--
insert into join_foo select generate_series(1, 3) as id, 'xxxxx'::varchar(50) as t;
--alter table join_foo set (parallel_workers = 0);
--DDL_STATEMENT_BEGIN--
drop table if exists join_bar;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table join_bar(id integer, t varchar(50));
--DDL_STATEMENT_END--
insert into join_bar select generate_series(1, 10000) as id, 'xxxxx'::varchar(50) as t;
--alter table join_bar set (parallel_workers = 2);
-- single-batch with rescan, parallel-oblivious
begin;
-- multi-batch with rescan, parallel-oblivious
savepoint settings;
set enable_parallel_hash = off;
set parallel_leader_participation = off;
set min_parallel_table_scan_size = 0;
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set max_parallel_workers_per_gather = 2;
set enable_material = off;
set enable_mergejoin = off;
set work_mem = '64kB';
explain (costs off)
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

set statement_timeout=5000;
set mysql_read_timeout=5;
-- times out and connections to storage shards killed
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);
-- rollback cmds fail because conns invalid and we do not reconnect in this case
rollback to settings;
-- local txn aborted
commit;


set statement_timeout=5000;
set mysql_read_timeout=5;
-- times out and connections to storage shards killed
select final > 1 as multibatch
  from hash_join_batches(
$$
  select count(*) from join_foo
    left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
    on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;
$$);
-- automatically reconnects but query times out again
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

set statement_timeout=50000;
set mysql_read_timeout=50;
-- automatically reconnects again and query succeeds
select count(*) from join_foo
  left join (select b1.id, b1.t from join_bar b1 join join_bar b2 using (id)) ss
  on join_foo.id < ss.id + 1 and join_foo.id > ss.id - 1;

