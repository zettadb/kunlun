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
