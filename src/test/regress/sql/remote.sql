set enable_remote_relations=on;

drop table if exists t4;
drop table if exists t5;
drop table if exists t6;

create table t4(a int primary key, b int unique) with oids;
create table t4(a int primary key, b int unique, c varchar(32) not null);
show last_remote_sql;
create index t4_cb on t4(c desc, b);
show last_remote_sql;
\d+ t4;
\d+ t4_cb;

-- foreign keys are forbidden
create table t5(a int references t4(a));

-- temp table exits on computing node.
create temporary table t6(a int);
insert into t6 values(1),(2),(3);
select*from t6;

drop table if exists t7;
create table t7(a int, b varchar(16), c int not null, d varchar(16) not null, primary key(c,d),unique(a,c),unique(b,d));
show last_remote_sql;
create index t7_abc on t7(b desc, a asc, c desc);
show last_remote_sql;
\d+ t7;
\d+ t7_abc;

-- forbidden create index clauses start
create index t7_b on t7(b collate en_US);
create index t7_b on t7(b collate zh_CN);

create index t7_b on t7(b ) include (c);
create index t7_b on t7(b zh_pinyin_cmp);
create index t7_b on t7(b zh_stroke_cmp);
create index t7_a on t7((a+c));
create index t7_a on t7 (a) where a between 100 and 200;

create index t7_b on t7(b nulls first);
drop index t7_b;
create index t7_b on t7(b nulls last);
show last_remote_sql;
\d+ t7_b;

-- forbidden create index clauses end


drop table if exists t3;
create table t3(a int) partition by list(a);
show last_remote_sql;
\d+ t3;

create table t301 partition of t3 for values in (1,3,5);
show last_remote_sql;
\d+ t301;
create table t302 partition of t3 for values in (2,4,6);
show last_remote_sql;
\d+ t302;

drop table if exists t3;
create table t3(a int, b date, c timestamp, d time, e varchar(64), primary key(b,d)) partition by range(b);
create unique index on t3(c, b);
create index on t3(c);
show last_remote_sql;
\d+ t3;

create table t301 partition of t3 for values from ('2000-01-01') to ('2009-12-31');
show last_remote_sql;
\d+ t301;
\d+ t3;

create table t303 partition of t3 for values from ('2020-01-01') to ('2029-12-31');
show last_remote_sql;
\d+ t303;
\d+ t3;

-- create a new index t3_b_a_idx , existing leaf partitions should be given t3_b_a_idx and t3_a_idx.
create unique index t3_b_a_idx on t3(b, a);
show last_remote_sql;
create index t3_a_idx on t3(a);
show last_remote_sql;
\d+ t301;
\d+ t303;
\d+ t3;

-- new leaf partitions should have t3_b_a_idx in 'create table' stmt, and also an independent
-- 'create index t3_a_idx' stmt, because in pg non-unique index can't be created in 'create table' stmt.
create table t302 partition of t3 for values from ('2010-01-01') to ('2019-12-31');
show last_remote_sql;
\d+ t3;
\d+ t302;
\d+ t301;
\d+ t303;

create table t304 partition of t3 for values from ('2030-01-01') to ('2049-12-31');
show last_remote_sql;
\d+ t3;
\d+ t304;
\d+ t301;
\d+ t302;
\d+ t303;

-- create a new idx for t302 only.
create unique index t302_b_e_idx on t302(b,e);
show last_remote_sql;
\d+ t302;
\d+ t3;

-- create a new idx for t304 only.
create index t304_b_e_idx on t304(b,e);
show last_remote_sql;
\d+ t304;
\d+ t3;

-- create another index on t3, existing leaf partitions should all be given
-- this idx. only the last 'create index' can be printed here.
create index t3_e_idx on t3(e);
show last_remote_sql;
\d+ t301;
\d+ t302;
\d+ t303;
\d+ t304;
\d+ t3;
insert into t3 values(2, '2025-01-16 11:25:09', '2015-01-16 11:25:09', '2015-01-16 11:25:09', 'abc'),(3, '2015-01-16 11:25:09', '2020-01-16 11:25:09', '2015-01-16 11:25:09', 'def'), (4, '2000-01-16 11:25:09', '2020-01-16 11:25:10', '2015-01-16 11:25:09', 'xyz'),(5, '2035-01-16 11:25:09', '2020-01-16 11:25:11', '2015-01-16 11:25:09', 'spider');


drop table if exists t1;
create table t1(a int, b int) partition by hash(a,b);
show last_remote_sql;
\d+ t1;
create table t100 partition of t1 for values with (modulus 4, remainder 0);
show last_remote_sql;
create table t101 partition of t1 for values with (modulus 4, remainder 1);
show last_remote_sql;
create table t102 partition of t1 for values with (modulus 4, remainder 2);
show last_remote_sql;
create table t103 partition of t1 for values with (modulus 4, remainder 3);
show last_remote_sql;


drop table if exists t2;
create table t2(a int, b int, c varchar(32), d char(16), primary key(a,b), unique(b,c)) partition by hash(b);
show last_remote_sql;
\d+ t2;
create table t200 partition of t2 for values with (modulus 4, remainder 0);
show last_remote_sql;
create table t201 partition of t2 for values with (modulus 4, remainder 1);
show last_remote_sql;
create table t202 partition of t2 for values with (modulus 4, remainder 2);
show last_remote_sql;
create table t203 partition of t2 for values with (modulus 4, remainder 3);
show last_remote_sql;

set log_min_messages=debug5;
create index t2_c_d on t2(d desc, c);

-- only the last index's remote sql is printed,others are overwritten, but
-- it's OK, this is only for debugging. all remote sql will be sent to remote
-- targets properly. debug5 is turned on so you can check the log file for all
-- four statements generated by above 'create index' stmt.

show last_remote_sql;
\d+ t2_c_d;
\d+ t200_d_c_idx;
\d+ t201_d_c_idx;
\d+ t202_d_c_idx;
\d+ t203_d_c_idx;


drop table if exists t1;
create table t1(a varchar(30), b int, c date, d time, e timestamp, f char(20), primary key(a,b),unique(e,b)) partition by hash(b);
show last_remote_sql;
create index t1_f on t1(f);
show last_remote_sql;
\d+ t1;
create table t100 partition of t1 for values with (modulus 4, remainder 0);
show last_remote_sql;

create table t101 partition of t1 for values with (modulus 4, remainder 1);
show last_remote_sql;

create unique index t1_d_b on t1(d,b desc);
show last_remote_sql;
create index t1_d on t1(d desc);
show last_remote_sql;

create table t102 partition of t1 for values with (modulus 4, remainder 2);
show last_remote_sql;
create index t102_a_b on t102(a desc, b);
show last_remote_sql;

create table t103 partition of t1 for values with (modulus 4, remainder 3);
show last_remote_sql;
create unique index t103_a_b on t102(c, b desc, e desc);
show last_remote_sql;


drop table if exists t2;
create table t2(a int, b timestamptz, c timestamp, d money, e numeric(7,4), f real, g double precision, primary key(a,b), unique(a,b,c)) partition by list(a);
show last_remote_sql;
\d+ t2;
create table t200 partition of t2 for values in (1,2,3,4);
show last_remote_sql;
\d+ t200;
create index t2_d_g on t2(d,g desc);
show last_remote_sql;
\d+ t2;
\d+ t200;

create table t201 partition of t2 for values in (5,6,7);
show last_remote_sql;
\d+ t201;
create unique index t2_c_d on t2(a,c,d desc);
show last_remote_sql;
\d+ t2;
\d+ t200;
\d+ t201;

create unique index t201_c_a on t201(c,a desc);
show last_remote_sql;
\d+ t201;

create table t202 partition of t2 for values in (8,9,10)
show last_remote_sql;
\d+ t202;
create index on t2(b,e);
show last_remote_sql;
create index on t2(g desc,f);
show last_remote_sql;
\d+ t2;
\d+ t201;
\d+ t202;

create table t10 (a decimal(66));
create table t10 (a decimal(10,31));

create table t10(a decimal, b decimal(12), c decimal(20, 10), d real, e float(10), f float(30), g bit(11), h money, i timestamptz, j timestamp(6), k time(6), l date,primary key(a), unique(b,d), unique(g,j), unique(c,k));
create unique index t10ei on t10(e,i);
create index t10fh on t10(f,h,k);


