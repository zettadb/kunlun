--create--
drop DATABASE if exists 星际穿越;
CREATE DATABASE 星际穿越;
\c 星际穿越
CREATE USER 月亮 with PASSWORD '月亮';
GRANT ALL on database  星际穿越 TO 月亮;
ALTER USER 月亮 RENAME TO 太阳;
select * from pg_user;
revoke  ALL on database  星际穿越 from 太阳;
drop user 太阳;
select * from pg_user;

CREATE SCHEMA 地球;
SELECT * FROM pg_namespace;
create table 地球.中国(编号 int primary key ,城市 text not NULL,省会 text not null);
select * from 地球.中国;
\d
drop schema 地球  CASCADE;
SELECT * FROM pg_namespace;

--CREATE TABLE--
create table 个人表(编号 integer primary key, 姓名 varchar(10),年龄 serial,身高 text);

--alter table--
alter table 个人表  rename to 信息表;
alter table 信息表  alter column 年龄 drop default;
\d 信息表
alter table 信息表 add constraint 年龄 unique(年龄);
\d 信息表
alter table 信息表 drop constraint "年龄";
\d 信息表

alter table 信息表  add 性别 int NULL;
select * from 信息表;
alter table 信息表  rename  性别 to 地址;
select * from 信息表;
alter table 信息表  rename  年龄 to 性别;
select * from 信息表;
alter table 信息表  rename  身高 to 爱好;
\d 信息表
alter table 信息表 alter column 地址 type text USING 地址::text;
alter table 信息表 alter column 性别 type char USING 性别::char;
\d 信息表

--alter table 信息表 add constraint 性别 check(2) (性别='男' or 性别='女') (性别);  #暂时不支持check

--INSERT into-- 
insert into 信息表(编号,姓名,性别,爱好,地址) values(1,'张三','男','乒乓球','上海');
insert into 信息表(编号,姓名,性别,爱好,地址) values(2,'李四','男','乒乓球','广东');
insert into 信息表(编号,姓名,性别,爱好,地址) values(3,'王五','男','篮球','四川');
insert into 信息表(编号,姓名,性别,爱好,地址) values(4,'陈六','男','爬山','四川');
insert into 信息表(编号,姓名,性别,爱好,地址) values(5,'陈琪','女','蹦极','重庆');
insert into 信息表(编号,姓名,性别,爱好,地址) values(6,'李舞','女',DEFAULT,'重庆'),(7,'张霸','男','篮球',DEFAULT);
select * from 信息表;

--noll--
SELECT * FROM 信息表 WHERE 地址 IS   NOT NULL;
SELECT * FROM 信息表 WHERE 地址 IS  NULL;

--UPDATE --
update 信息表 set 爱好 ='爬山'  where 编号 = 6;
update 信息表 set 爱好 ='蹦极',地址='广东'  where 编号 = 7;
select * from 信息表;

--select--
select 姓名,爱好 from 信息表;
select * from 信息表 where 性别='男';
select * from 信息表 where 编号>=4;
select * from 信息表 where 地址!='四川';
select * from 信息表 where 编号>2 and 编号<5;
select * from 信息表 where 编号>=2 and 编号<=5;
select * from 信息表 where 编号 in (2,6);
select * from 信息表 where 编号 BETWEEN 2 and 5;
select * from 信息表 where 性别='男' and 爱好='乒乓球';
select * from 信息表 where 性别='女' or 爱好='乒乓球';

select * from 信息表 where 姓名 like '陈%';
select * from 信息表 where 姓名 like '%五%';
select * from 信息表 limit 4;
select * from 信息表 limit 4 OFFSET 2;
select * from 信息表 order by 编号 desc;

		
create table 个人表(编号 integer primary key, 姓名 varchar(10),工资 integer,工作 text);

insert into 个人表(编号,姓名,工资,工作) values(1,'张三',15000,'内科'),(2,'李四',18000,'内科'),(3,'王五',25000,'外科'),(4,'陈六',25000,'外科'),
(5,'陈琪',10000,'前台'),(6,'李舞',15000,'外科'),(7,'张霸',20000 ,'内科');

SELECT 姓名,SUM(工资) FROM 个人表 GROUP BY 姓名 ORDER BY 姓名;
SELECT 工作,avg(工资) FROM 个人表 GROUP BY 工作 order by AVG(工资);
select 工作,max(工资) from 个人表 group by 工作 order by max(工资)desc; 
select 工作,min(工资) from 个人表 group by 工作order by min(工资)desc;
select 工作,count(工资) from 个人表 group by 工作order by 工作 desc;
select 工资,count(工作) from 个人表 group by 工资 order by 工资 desc;
select 工资 from 个人表 group by 工资 having count(工资) > 1 order by 工资;
select 工作,count(工作) from 个人表 group by 工作 having count(工作) > 2  order by 工作desc;
select 工作 from 个人表 where 工作 = '前台';
select 工作,count(工作) from 个人表 where 工作='前台' group by 工作;
select * from  个人表 where 编号 in (select 编号 from 个人表 where 工资 > 20000 );


insert into 个人表(编号,姓名,工资,工作) values(8,'王发',45000,DEFAULT);

select * from 个人表;

insert into 信息表(编号,姓名,性别,爱好,地址) values(9,'李珐','女','乒乓球','上海');



select  一.编号,一.姓名,二.性别,一.工资,一.工作,二.地址,二.爱好  
from 个人表 as 一 inner  join 信息表 as 二 
on 一.编号=二.编号;

select  一.编号,一.姓名,二.性别,一.工资,一.工作,二.地址,二.爱好  
from 个人表 as 一 right outer join 信息表 as 二 
on 一.编号=二.编号;

select  一.编号,一.姓名,二.性别,一.工资,一.工作,二.地址,二.爱好  
from 个人表 as 一 left outer join 信息表 as 二 
on 一.编号=二.编号;


--CREATE INDEX--
create index 姓名索引 on 个人表 (姓名);
\d 个人表
ALTER INDEX 姓名索引 RENAME TO 索引名字;
\d 个人表
create index 组合索引 on 个人表 (工资,工作);
\d 个人表
drop index 索引名字;
drop index 组合索引;
\d 个人表

--view--
CREATE VIEW 个人表_视图 AS select 编号,姓名,工作 from 个人表;
select * from 个人表_视图;
drop view 个人表_视图;
\d 
--create table as--暂时不支持
--create table 个人表二 as select * from 个人表
--\d 个人表二
--select * from 个人表二;

--删除--

select * from 个人表;
delete from 个人表 where 编号=2;
select * from 个人表;
delete from 个人表 where 工作='前台' or 工资 > 20000;
select * from 个人表;
delete from 个人表 where 编号 <=5;
select * from 个人表;
delete from 信息表 where  爱好 = '篮球' and 地址='四川';
select * from 信息表;
delete from 信息表 where  性别 = '男' or 爱好='爬山';
select * from 信息表;

--truncate table 信息表; 暂时不支持
--\d 信息表
delete from 个人表;
\d 个人表
delete from 信息表;
\d 信息表



drop table 信息表;
drop table 个人表;
\c postgres
drop database 星际穿越;