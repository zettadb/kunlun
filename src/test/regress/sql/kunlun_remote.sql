--DDL_STATEMENT_BEGIN--
drop table if exists t4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists t6;
--DDL_STATEMENT_END--
--create table t4(a int primary key, b int unique) --with oids;
--DDL_STATEMENT_BEGIN--
create table t4(a int primary key, b int unique, c varchar(32) not null);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create index t4_cb on t4(c desc, b);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t4;
\d+ t4_cb;

-- foreign keys are forbidden
--DDL_STATEMENT_BEGIN--
create table t5(a int references t4(a));
--DDL_STATEMENT_END--
-- temp table exits on computing node.
--DDL_STATEMENT_BEGIN--
create temporary table t6(a int);
--DDL_STATEMENT_END--
insert into t6 values(1),(2),(3);
select*from t6;
--DDL_STATEMENT_BEGIN--
drop table if exists t7;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t7(a int, b varchar(16), c int not null, d varchar(16) not null, primary key(c,d),unique(a,c),unique(b,d));
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create index t7_abc on t7(b desc, a asc, c desc);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t7;
\d+ t7_abc;

-- forbidden create index clauses start
--DDL_STATEMENT_BEGIN--
create index t7_b on t7(b collate en_US);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index t7_b on t7(b collate zh_CN);
--DDL_STATEMENT_END--
--create index t7_b on t7(b ) include (c);
--DDL_STATEMENT_BEGIN--
create index t7_b on t7(b zh_pinyin_cmp);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index t7_b on t7(b zh_stroke_cmp);
--DDL_STATEMENT_END--
--create index t7_a on t7((a+c));
--create index t7_a on t7 (a) where a between 100 and 200;

--DDL_STATEMENT_BEGIN--
create index t7_b on t7(b nulls first);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop index t7_b;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index t7_b on t7(b nulls last);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t7_b;

-- forbidden create index clauses end

--DDL_STATEMENT_BEGIN--
drop table if exists t3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t3(a int) partition by list(a);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t3;
--DDL_STATEMENT_BEGIN--
create table t301 partition of t3 for values in (1,3,5);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t301;
--DDL_STATEMENT_BEGIN--
create table t302 partition of t3 for values in (2,4,6);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t302;
--DDL_STATEMENT_BEGIN--
drop table if exists t3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t3(a int, b date, c timestamp, d time, e varchar(64), primary key(b,d)) partition by range(b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create unique index on t3(c, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index on t3(c);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t3;
--DDL_STATEMENT_BEGIN--
create table t301 partition of t3 for values from ('2000-01-01') to ('2009-12-31');
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t301;
\d+ t3;
--DDL_STATEMENT_BEGIN--
create table t303 partition of t3 for values from ('2020-01-01') to ('2029-12-31');
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t303;
\d+ t3;

-- create a new index t3_b_a_idx , existing leaf partitions should be given t3_b_a_idx and t3_a_idx.
--DDL_STATEMENT_BEGIN--
create unique index t3_b_a_idx on t3(b, a);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create index t3_a_idx on t3(a);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t301;
\d+ t303;
\d+ t3;

-- new leaf partitions should have t3_b_a_idx in 'create table' stmt, and also an independent
-- 'create index t3_a_idx' stmt, because in pg non-unique index can't be created in 'create table' stmt.
--DDL_STATEMENT_BEGIN--
create table t302 partition of t3 for values from ('2010-01-01') to ('2019-12-31');
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t3;
\d+ t302;
\d+ t301;
\d+ t303;
--DDL_STATEMENT_BEGIN--
create table t304 partition of t3 for values from ('2030-01-01') to ('2049-12-31');
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t3;
\d+ t304;
\d+ t301;
\d+ t302;
\d+ t303;

-- create a new idx for t302 only.
--DDL_STATEMENT_BEGIN--
create unique index t302_b_e_idx on t302(b,e);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t302;
\d+ t3;

-- create a new idx for t304 only.
--DDL_STATEMENT_BEGIN--
create index t304_b_e_idx on t304(b,e);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t304;
\d+ t3;

-- create another index on t3, existing leaf partitions should all be given
-- this idx. only the last 'create index' can be printed here.
--DDL_STATEMENT_BEGIN--
create index t3_e_idx on t3(e);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t301;
\d+ t302;
\d+ t303;
\d+ t304;
\d+ t3;
insert into t3 values(2, '2025-01-16 11:25:09', '2015-01-16 11:25:09', '2015-01-16 11:25:09', 'abc'),(3, '2015-01-16 11:25:09', '2020-01-16 11:25:09', '2015-01-16 11:25:09', 'def'), (4, '2000-01-16 11:25:09', '2020-01-16 11:25:10', '2015-01-16 11:25:09', 'xyz'),(5, '2035-01-16 11:25:09', '2020-01-16 11:25:11', '2015-01-16 11:25:09', 'spider');

--DDL_STATEMENT_BEGIN--
drop table if exists t1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t1(a int, b int) partition by hash(a,b);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t1;
--DDL_STATEMENT_BEGIN--
create table t100 partition of t1 for values with (modulus 4, remainder 0);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create table t101 partition of t1 for values with (modulus 4, remainder 1);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create table t102 partition of t1 for values with (modulus 4, remainder 2);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create table t103 partition of t1 for values with (modulus 4, remainder 3);
--DDL_STATEMENT_END--
--show last_remote_sql;

--DDL_STATEMENT_BEGIN--
drop table if exists t2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t2(a int, b int, c varchar(32), d char(16), primary key(a,b), unique(b,c)) partition by hash(b);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t2;
--DDL_STATEMENT_BEGIN--
create table t200 partition of t2 for values with (modulus 4, remainder 0);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create table t201 partition of t2 for values with (modulus 4, remainder 1);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create table t202 partition of t2 for values with (modulus 4, remainder 2);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create table t203 partition of t2 for values with (modulus 4, remainder 3);
--DDL_STATEMENT_END--
--show last_remote_sql;

set log_min_messages=debug5;
--DDL_STATEMENT_BEGIN--
create index t2_c_d on t2(d desc, c);
--DDL_STATEMENT_END--
-- only the last index's remote sql is printed,others are overwritten, but
-- it's OK, this is only for debugging. all remote sql will be sent to remote
-- targets properly. debug5 is turned on so you can check the log file for all
-- four statements generated by above 'create index' stmt.

--show last_remote_sql;
\d+ t2_c_d;
\d+ t200_d_c_idx;
\d+ t201_d_c_idx;
\d+ t202_d_c_idx;
\d+ t203_d_c_idx;

--DDL_STATEMENT_BEGIN--
drop table if exists t1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t1(a varchar(30), b int, c date, d time, e timestamp, f char(20), primary key(a,b),unique(e,b)) partition by hash(b);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create index t1_f on t1(f);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t1;
--DDL_STATEMENT_BEGIN--
create table t100 partition of t1 for values with (modulus 4, remainder 0);
--DDL_STATEMENT_END--
--show last_remote_sql;

--DDL_STATEMENT_BEGIN--
create table t101 partition of t1 for values with (modulus 4, remainder 1);
--DDL_STATEMENT_END--
--show last_remote_sql;

--DDL_STATEMENT_BEGIN--
create unique index t1_d_b on t1(d,b desc);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create index t1_d on t1(d desc);
--DDL_STATEMENT_END--
--show last_remote_sql;

--DDL_STATEMENT_BEGIN--
create table t102 partition of t1 for values with (modulus 4, remainder 2);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create index t102_a_b on t102(a desc, b);
--DDL_STATEMENT_END--
--show last_remote_sql;

--DDL_STATEMENT_BEGIN--
create table t103 partition of t1 for values with (modulus 4, remainder 3);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create unique index t103_a_b on t102(c, b desc, e desc);
--DDL_STATEMENT_END--
--show last_remote_sql;


--DDL_STATEMENT_BEGIN--
drop table if exists t2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t2(a int, b timestamptz, c timestamp, d money, e numeric(7,4), f real, g double precision, primary key(a,b), unique(a,b,c)) partition by list(a);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t2;
--DDL_STATEMENT_BEGIN--
create table t200 partition of t2 for values in (1,2,3,4);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t200;
--DDL_STATEMENT_BEGIN--
create index t2_d_g on t2(d,g desc);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t2;
\d+ t200;
--DDL_STATEMENT_BEGIN--
create table t201 partition of t2 for values in (5,6,7);
--DDL_STATEMENT_END----show last_remote_sql;
\d+ t201;
--DDL_STATEMENT_BEGIN--
create unique index t2_c_d on t2(a,c,d desc);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t2;
\d+ t200;
\d+ t201;

--DDL_STATEMENT_BEGIN--
create unique index t201_c_a on t201(c,a desc);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t201;
--DDL_STATEMENT_BEGIN--
create table t202 partition of t2 for values in (8,9,10)
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t202;
--DDL_STATEMENT_BEGIN--
create index on t2(b,e);
--DDL_STATEMENT_END--
--show last_remote_sql;
--DDL_STATEMENT_BEGIN--
create index on t2(g desc,f);
--DDL_STATEMENT_END--
--show last_remote_sql;
\d+ t2;
\d+ t201;
\d+ t202;

--create table t10 (a decimal(66));
--DDL_STATEMENT_BEGIN--
create table t10 (a decimal(10,31));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table t10(a decimal, b decimal(12), c decimal(20, 10), d real, e float(10), f float(30), g bit(11), h money, i timestamptz, j timestamp(6), k time(6), l date,primary key(a), unique(b,d), unique(g,j), unique(c,k));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create unique index t10ei on t10(e,i);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index t10fh on t10(f,h,k);
--DDL_STATEMENT_END--

