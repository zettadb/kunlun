-- sharding/sharding_stmt_utils.c
drop table if exists test;

create table test(
    f1 int2,
    f2 int4,
    f3 int8,
    f4 float4,
    f5 float8,
    f6 numeric,
    f7 bool,
    f8 text,
    f9 time,
    f10 timestamp,
    f11 bytea
);

INSERT INTO
    test
VALUES
    (
        0x7fff,
        0x7fffffff,
        0x7fffffffffffffff,
        3.40282346638528860e+38,
        1.79769313486231470e+308,
        1234567890123456789012345678901234567890.12345678901234567890,
        true,
        repeat('x', 32 * 1024),
        '10:01:01',
        '2023-1-14 10:01:01',
        repeat('x', 32 * 1024)::bytea
    );

INSERT INTO
    test
VALUES
    (
        - 0x8000,
        - 0x80000000,
        - 0x8000000000000000,
        1.40129846432481707e-45,
        4.94065645841246544e-324,
        1234567890123456789012345678901234567890.12345678901234567890,
        false,
        repeat('x', 32 * 1024),
        '10:01:01',
        '2023-1-14 10:01:01',
        repeat('x', 32 * 1024)::bytea
    );

BEGIN;
SET force_remote_sql_parameterize = true;
SET enable_shard_binary_protocol = true;
SELECT f1 FROM test WHERE f1 = 0x7fff OR f1 = -0x8000;
SELECT f2 FROM test WHERE f2 = 0x7fffffff OR f2 = -0x80000000;
SELECT f4 FROM test WHERE f4 = (3.40282346638528860e+38) OR f3 = (1.40129846432481707e-45); -- 浮点数不准 
SELECT f5 FROM test WHERE f5 = (1.79769313486231470e+308) OR f4 = (4.94065645841246544e-324); -- 浮点数不准
SELECT f6 FROM test WHERE f6 = 1234567890123456789012345678901234567890.12345678901234567890;
SELECT f7 FROM test WHERE f7 = true;
--SELECT f8 FROM test WHERE f8 = repeat('x', 32 * 1024);
SELECT f9 FROM test WHERE f9 = '10:01:01';
SELECT f10 FROM test WHERE f10 = '2023-1-14 10:01:01';
rollback;

-- 生成超过100条不同的sql（默认缓存100条PREPARED的不同的SQL，超过就会触发PREPARE的语句的回收）
DO $$
BEGIN
    for i in 1..120 loop
    execute concat('select f1', repeat('+(f1-f1)', i), ' from test where f1=0x7fff');
    end loop;
    
    for i in 1..120 loop
    execute concat('select f1', repeat('-(f1-f1)', i), ' from test where f2=0x7fff');
    end loop;
END;
$$;

-- 模拟连接断开, 使得prepare的语句被回收
DO $$
BEGIN
    execute concat('send ', (select name from pg_shard order by id limit 1), '''kill connection (select connection_id())''');
END; $$;

-- 
DO $$
BEGIN
    for i in 1..120 loop
    execute concat('select f1', repeat('+(f1-f1)', i), ' from test where f1=0x7fff');
    end loop;
    
    for i in 1..120 loop
    execute concat('select f1', repeat('-(f1-f1)', i), ' from test where f2=0x7fff');
    end loop;
END;
$$;


-- sharding/sharding_conn.c
drop table if exists test;
create table test(a int, b int);
INSERT INTO test VALUES(1,1);

create or replace function recursive_func(level int) returns int as $$
DECLARE result integer;
BEGIN
    if level = 0 then
        return level;
    end if;
    select sum(recursive_func(level - 1) + 1) from test INTO result;
    return result;
END
$$ language plpgsql;

-- 递归调用
SELECT recursive_func(500) from test;


drop table if exists test;
create table test(a int, b int);
insert into test values(1,1), (2,2), (3,3);

create or replace function return_unnamed_refcursor() returns refcursor as $$
declare
    rc refcursor;
begin
    -- open rc for select a from generate_series(1,3) s(a);
    open rc for select a from test;
    return rc;
end
$$ language plpgsql;

-- 测试创建多个未的用户cursor(pg本身也存在bug，内存会蹭蹭往上涨)
create or replace function massive_cursors() returns int as $$
DECLARE
    cursors refcursor[] default '{}';
    rc refcursor;
    foo integer;
    cnt integer default 0;
BEGIN
    for i in 1..100000 LOOP
    BEGIN
        rc = return_unnamed_refcursor();
        fetch next from rc into foo;
        cursors = array_append(cursors, rc);
    END;
    END LOOP;

    for i in 1..100000 loop
    begin
        rc = cursors[i];
        fetch next from rc into foo;
        close rc;
    end;
    end loop;
    return 1;
END
$$ language plpgsql;

create or replace function exec_storage_sql(sql text) returns void
as $$
DECLARE
    rc refcursor;
    shard name;
BEGIN
    -- 设置mysql连接在空闲1s之后自动断开
    open rc for execute 'select name from pg_shard';
    fetch next from rc into shard;
    while found loop
        execute concat('send ', shard, '''', sql, '''');
        fetch next from rc into shard;
    end loop;
END; 
$$ language plpgsql;

-- 执行过程中连接断开
BEGIN;
	 
set enable_parallel_remotescan = false;
   
set enable_parallel_append = false;
   
select exec_storage_sql('set wait_timeout=1');
												
				  
				  
 
	   

select * from test where pg_sleep(2) is null union all select * from test where pg_sleep(2) is null;
																																									  
															 
rollback;

-- 使用cursor与存储节点进行交互，测试cursor交替使用
drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int);
create table t2(a int, b int);
insert into t1 select i,i from generate_series(1,100) as s(i);
insert into t2 select i,i from generate_series(1,100) as s(i);
ANALYZE t1, t2;
BEGIN;
SET force_remote_sql_parameterize = true;
SET enable_shard_binary_protocol = true;
SET enable_parallel_remotescan = false;
SET enable_hashjoin = off;
set enable_nestloop= on;
set enable_mergejoin = off;
set enable_material = off;
set enable_remote_join_pushdown = off;
set enable_remote_cursor = on;
set remote_cursor_prefetch_rows = 10;
select count(1) from t1, t2 where t1.a=t2.a;
select * From t1, t2 where t1.a=t2.a limit 4;
-- 切换成非cursor模式(验证存储节点是否修复了bug？)
set enable_remote_cursor = off;
select * From t1, t2 where t1.a=t2.a limit 4;
-- 执行过程中连接断开
set enable_remote_cursor = on;
select * from t1, t2 where t1.a=t2.a and exec_storage_sql('kill connection (select connection_id())') is null;
rollback;

-- 向存储节点发起prepare时出错
drop table if exists t3;
create table t3(a int, b int);
select exec_storage_sql(concat('drop table if exists `', database(), '_$$_', current_schema(), '`.t3'));
begin;
set force_remote_sql_parameterize = true;
set enable_shard_binary_protocol = true;
select * From t3;
rollback;

-- db返回一部分数据之后，报错
drop table if exists t1;
create table t1(a int primary key, b int);
insert into t1 select i, i from generate_series(1,10000) s(i);
begin;
set force_remote_sql_parameterize = true;
set enable_shard_binary_protocol = true;
select a, power(1.09, b) from t1 order by a; -- 当a过大时，存储节点上power函数会报错
rollback;

-- 更新分区表+returning（测试对同一个连接的争强和调度）
set sharding_policy = 3; -- 总是选择第一个shard作为表的持久化节点
drop table if exists t1;
create table t1(a int, b int) partition by range(a);
create table t1p1 partition of t1 for values from (0) to (1000);
create table t1p2 partition of t1 for values from (1000) to (2000);
insert into t1 select i,i from generate_series(1, 1999) s(i);

drop table if exists t2;
create table t2(like t1);
insert into t2 select * from t1;
explain ANALYZE with s as (update t1 set b=b+1 returning *) select * From t2 where a not in (select a from s);

-- 触发大字段的物化
set sharding_policy = 3;
drop table if exists t1;
create table t1(a int);
insert into t1 select i from generate_series(1,100) s(i);
begin;
set enable_hashjoin = off;
set enable_mergejoin = off;
set enable_parallel_remotescan = off;

EXPLAIN ANALYZE SELECT *
FROM (
	SELECT a, repeat('x', t1.a * 100)
	FROM t1
	ORDER BY a DESC
	LIMIT 10
) tmp
WHERE a IN (
	SELECT a
	FROM t1
	ORDER BY a DESC
	LIMIT 10
);
rollback;

-- sharding/global_txid.c
drop table if exists t1, t2;
create table t1(a int, b int) with(shard=1);
create table t2(a int, b int) with(shard=1);
set session_debug = '+d,inject_reserve_global_txids_failure,force_reload_reserved_global_txid';
BEGIN;
insert into t1 values(1,1);
insert into t2 values(2,2);
rollback;
set session_debug = '-d,inject_reserve_global_txids_failure,force_reload_reserved_global_txid';

-- executor/nodeRemotePlan.c

-- 并行查询
drop table if exists t1;
create table t1(a int, b int);
create index on t1(a);
create index on t1(b);
insert into t1 select i,i%10 from generate_series(1,10000) s(i);
ANALYZE t1(a, b);
 -- analyze的结果会先写入ddl log，然后再被计算节点拉下来存放到本地系统表，因此需要等待几秒
select pg_sleep(2);
select * From pg_statistic where starelid = (select oid from pg_class where relname = 't1');

BEGIN;
set min_parallel_index_scan_size = '1B';
set min_parallel_table_scan_size = '1B';
set enable_parallel_remotescan = true;
set parallel_setup_cost=0;
set parallel_tuple_cost =0;
-- text protocal
set force_remote_sql_parameterize = 0;
-- 使用等宽直方图对扫描范围进行切分
explain (costs off) select count(a) from t1;
select count(a) from t1;
-- 使用mcv直方图对扫描范围进行切分
explain (costs off) select count(b) from t1;
select count(b) from t1;
-- 使用in表达式对扫描范围进行切分
explain (costs off) select count(b) from t1 where b in (1,4,5,7,9);
select count(b) from t1 where b in (1,4,5,7,9);

-- binary protocal
set force_remote_sql_parameterize = 1;
set enable_shard_binary_protocol = 1;
explain (costs off) select count(1) from t1;
select count(1) from t1;
-- 关闭聚合下推
set enable_remote_agg_pushdown = off;
explain (costs off) select count(1) from t1;
select count(1) from t1;
rollback;

-- MIRROR表的测试
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t1(a int, b int) with(shard=all);
create table t2(a int, b int) with(shard=1);
create table t3(a int, b int) with(shard=1);
INSERT into t1 select i,i from generate_series(1,10) s(i);
INSERT into t2 select i,i from generate_series(1,10) s(i);
INSERT into t3 select i,i from generate_series(1,10) s(i);
BEGIN;
set enable_remote_join_pushdown = true;
SELECT count(1) from t1; -- 随机发送给给某个shard
explain select count(1) from t1, t2 where t1.a=t2.a; -- 确保能够下推
select count(1) from t1, t2 where t1.a=t2.a;
explain select count(1) from t1, t3 where t1.a=t3.a; -- 确保能够下推
select count(1) from t1, t3 where t1.a=t3.a;

update t1 set a=a+1;
select count(1) from t1, t3 where t1.a=t3.a;
rollback;

-- cursor的优化
drop table if exists t1;
drop table if exists t2;
create table t1(a int, b int);
create table t2(a int, b int);
insert into t1 select i, i from generate_series(1,100) s(i);
insert into t2 select i, i from generate_series(1,100) s(i);
begin;
-- 使用cursor从存储节点逐行获取数据
set force_remote_sql_parameterize = 1;
set enable_remote_cursor = 1;
set remote_cursor_prefetch_rows = 1; 
-- 只访问存储节点一次的不使用cursor
explain analyze select * from t1;
-- 用户自定义cursor（暂时不识别）
do $$
declare
    rc refcursor;
    foo int;
begin
    open rc for select a from t1;
    fetch next from rc into foo;
end;
$$;
-- 非相关子查询，采用hash算法，不使用cursor
explain (costs off) select a in (select a from t2) from t1;
select a in (select a from t2) from t1;
-- 相关子查询，使用cursor
explain (costs off) select a in (select a from t2 where t1.b=t2.b) from t1;
select a in (select a from t2 where t1.b=t2.b) from t1;

savepoint s1;
set enable_remote_join_pushdown = off;
set enable_mergejoin = off;
set enable_nestloop = off;
set enable_hashjoin = on;
-- hash join的内表不需要cursor
explain select * From t1, t2 where t1.a=t2.a;
rollback to s1;

-- sort算子的子节点不使用cursor
set enable_remote_orderby_pushdown = off;
explain select * from t1 where a in (select a from t2 order by a limit 10);
rollback;

-- executor/remotePlanUtils.c

drop table if exists t1;
		  
create table t1(a int primary key, b int);
			
insert into t1 select i, i from generate_series(1,10) as s(i);

-- 测试语法
-- skip locked
explain (costs off) select * from t1 for update skip locked;;
-- NOWAIT
explain (costs off) select * from t1 for update nowait;

-- 使用dblink测试并发连接之间的互斥
create database jenkins;
create extension if not exists dblink ;

select dblink_connect('conn1', concat('hostaddr=127.0.0.1 port=', inet_server_port())); 
select dblink_connect('conn2', concat('hostaddr=127.0.0.1 port=', inet_server_port()));
-- conn1使用开启事务，并更新列1
select dblink('conn1', 'begin');
select dblink('conn1', 'create table t1(a int primary key, b int)');
select dblink('conn1', 'insert into t1 select i, i from generate_series(1,10) as s(i)');
select dblink('conn1', 'update t1 set b=b+1 where a=1');
-- conn2
-- 预期超时错误
begin;
	 
set innodb_lock_wait_timeout = 1; -- 1s超时
select * from dblink('conn2', 'select * from t1 for update') as t1(a int,b int);

rollback;

-- 直接报错
select * from dblink('conn2', 'select * from t1 for update nowait') as t1(a int,b int);
-- 跳过被锁住的列1
select * from dblink('conn2', 'select * from t1 for update skip locked') as t1(a int,b int);
select dblink('conn1', 'end');

select dblink_disconnect('conn1');
select dblink_disconnect('conn2');

-- remote/remote_dml.c

drop table if exists t1;
create table t1(a int primary key, b int) with (shard=all);
insert into t1 select i, i from generate_series(1,10) as s(i);
alter table t1 add check(b<100);

begin;
set enable_remote_orderby_pushdown = off;
set enable_remote_limit_pushdown = off;
-- 验证order/limit未下推时，能否正确判断update是能够下推的
explain update t1 set b=b+1 order by a limit 10;
update t1 set b=b+1 order by a limit 10;
-- 预期失败，不符合约束
savepoint sp1;
update t1 set b=b+100 order by a limit 10;
rollback to sp1;
update t1 set b=b+100 where a in (select a from t1 order by a limit 10);
rollback to sp1;
rollback;

-- remote_rel/ddl_logger.c

drop table if exists t1 cascade;
create table t1(a serial, b int) with(shard=all);
insert into t1 (b) select i  from generate_series(1,100) s(i);
alter table t1 add column c int not null;
-- 指定sequence名称和存储的shard位置
alter table t1 alter column c add generated by default as identity(sequence name s3 shard 1);

-- 添加非法约束（预期报错）
alter table t1 add constraint illegal_check check(b<0);

-- 预期拒绝
create table information_schema.t1(a int);

-- 物化视图（需要确认其他计算节点成功回放）
drop materialized view if exists v1;
create materialized view v1 as
 select b as b From t1;
select * From v1 limit 10;

drop materialized view if exists v1;
create materialized view v1 with (shard=all) as
    select b as b From t1;
select * From v1 limit 10;
update t1 set b=-b;
refresh materialized view v1;
select * from v1 limit 10;


-- remote_rel/hooks.c

drop table if exists t1 cascade;
set  remote_rel.enable_mirror_ddl = 1;
create table t1(a int) with(shard=all);

-- 禁止对mirror表的ddl
set statement_timeout='1s';
set remote_rel.enable_mirror_ddl = 0;
drop table t1; -- 预期报错
create materialized view v1 with(shard=all) as select * from t1; --预期报次
create materialized view v1 as select * from t1; -- ok
set remote_rel.enable_mirror_ddl = 1;

-- mirror表的路由fence
drop extension if exists dblink;
create extension if not exists dblink;
select dblink_connect('conn1', concat('hostaddr=127.0.0.1 port=', inet_server_port()));
select dblink('conn1', 'begin');
select dblink('conn1', 'update t1 set a=a+1');
-- 预期超时报错
-- 等到当前所有正在访问mirror表的事务提交成功才返回成功。
select mirror_route_fence();

-- 事务已提交，预期成功获取fence
select dblink('conn1', 'end');
select dblink_disconnect('conn1');
select mirror_route_fence();

-- remote_rel/remote_ddl.c
reset statement_timeout;
drop table if exists t1 cascade;
-- 指定innodb的属性
create table t1(a int, b text) WITH (
    row_format = compressed, compression = zlib, encryption = 'N', autoextend_size = '16M');

show create table t1;

insert into t1 select i, md5(i::text) from generate_series(1,1000) s(i);

-- 修改innodb属性
alter table t1 set (row_format=dynamic, compression = lz4);

-- 应用属性
optimize t1;

-- 修改列类型
alter table t1 alter a type text;
alter table t1 alter a type int using a::integer;



-- plan/subselect.c
-- 相关子查询展开优化
drop table if exists t1 cascade;
drop table if exists t2 cascade;
create table t1(a int, b int);
create table t2(a int, b int);
insert into t1 select i, i%10 from generate_series(1,100) s(i);
insert into t2 select i, i%10 from generate_series(1,100) s(i);
analyze t1(a,b), t2(a,b);
select pg_sleep(2);
begin;
-- 开启相关子查询展开优化
set enable_flatten_correlated_sublink = 1;
explain select * from t1 where a in (select max(a) from t2 where t2.b=t1.b);
select * from t1 where a in (select max(a) from t2 where t2.b=t1.b);

explain select * from t1 where a = (select max(a) from t2 where t2.b=t1.b);
select * from t1 where a = (select max(a) from t2 where t2.b=t1.b);

-- 关闭相关子查询展开优化
set enable_flatten_correlated_sublink = 0;
select * from t1 where a in (select max(a) from t2 where t2.b=t1.b);
rollback;
