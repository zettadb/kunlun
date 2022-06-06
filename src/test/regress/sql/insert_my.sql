-- kunlundb新增加的一些语法,这些语法是mysql独有的
-- insert ignore  如果数据库没有数据，就插入新的数据，如果有数据的话就跳过这条数据
-- insert on duplicate key update 表中存在PRIMARY，或者UNIQUE索引,表中存在的数据,则会用新的进行替换,没有的数据,效果如同insert INTO
-- replace into 如果存在PRIMARY或UNIQUE相同的记录，则先删除掉。再插入新记录
--  update/delete limit

-- insert ignore
-- https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#ignore-effect-on-execution
drop table if exists t1;
create table t1 (a int PRIMARY KEY , b int not null,CONSTRAINT t1_b_key UNIQUE (b));

-- 违背唯一约束，不进行插入
insert ignore into t1(a,b) values (4,4);

insert ignore into t1(a,b) values (4,4);

insert into t1(a,b) values (2,3);
-- 等价的PG语法
insert into t1 values(3,3) on conflict do nothing;

-- 不忽略违背约束的错误（例如分区约束、非null约束）

insert ignore into t1(a,b) values (4,NULL); --error

-- insert on duplicate key update
-- 更新第一个冲突的元组
select * from t1;
insert into t1 values(3,4) on duplicate key update b=2;
select * from t1;

-- 等价的PG语法
insert into t1 values(3,3) on conflict do update set b=3;
select * from t1;


-- 明确指定约束名，且表中存在多个唯一约束时，则报错
insert into t1 values(3,3) on conflict on constraint t1_pkey do update set b=3; --error



-- replace into
-- 如果存在冲突，则先删除其他冲突的元组，然后再进行插入

select * from t1;
replace into t1 values(3,4);

select * from t1;

-- 非临时表存放于MysQL中。如果与"当前sql"中先插入的元组冲突，不报错

replace into t1 values(1,1),(1,2);
select * from t1;


--- 临时表存放于计算节点。由于PG不允许在一条SQL中对同一个元组更新多次，
-- 因此如果与"当前sql"中先插入的元组冲突，则报错。

create temp table t3(a int primary key, b int unique);

replace into t3 values(1,1),(1,2);



-- support mysql insert syntax: update/delete limit #581
drop table if exists t3p1;
drop table if exists t3p2;
drop table if exists t3 ;
CREATE TABLE t3 (A INT PRIMARY KEY, B INT) PARTITION BY RANGE(a);
CREATE TABLE t3p1 PARTITION OF t3 FOR VALUES FROM (0) TO (100);
CREATE TABLE t3p2 PARTITION OF t3 FOR VALUES FROM (100) TO (200);
REPLACE INTO t3 SELECT GENERATE_SERIES(0,400) % 200, GENERATE_SERIES(0,400);

-- 分区表暂不支持全局有序的删除
UPDATE t3 SET b=-b ORDER BY a LIMIT 1; -- should be fail, global order is not supported


-- 支持不跨越分区的有序删除
UPDATE t3 SET b=-b WHERE a<100 ORDER BY a LIMIT 1 RETURNING *;

WITH foo as (UPDATE t3 SET b=-b WHERE b > 0 LIMIT 10 RETURNING *) SELECT count(1) FROM foo;


SELECT count(1) FROM t3 where b<0;





