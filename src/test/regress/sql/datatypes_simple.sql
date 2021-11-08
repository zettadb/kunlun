--Unsupported types
--Domain Types 域类型
--Range Types  范围类型
--Composite Types 组合类型
--Arrays  数组
--Geometric Types 几何类型

--XML Type--XML类型 需要编译时,插入--with-libxml参数
--DETAIL:  This functionality requires the server to be built with libxml support.
--HINT:  You need to rebuild PostgreSQL using --with-libxml.
-- ./configure --with-libxml  gmake && gmake install 


--Numeric Types--
drop table if exists t1;
create table t1 (
id serial,
a integer, 
b smallint, 
c bigint);
--Testing of boundary values 
insert into t1 values(DEFAULT,88,99,100);
insert into t1 (a,b,c) values (2147483647,32767,9223372036854775807);
insert into t1 (a,b,c) values(-2147483648,-32768,-9223372036854775808);
select * from t1;
--integer
insert into t1 VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT);
UPDATE t1 set a='456' where id=4;
UPDATE t1 set b='123' where id=4;
UPDATE t1 set c='789' where id=4;
select * from t1;
delete  from  t1 where a=456;
--smallint
insert into t1 VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT);
UPDATE t1 set a='234' where id=5;
UPDATE t1 set b='678' where id=5;
UPDATE t1 set c='222' where id=5;
select * from t1;
delete  from  t1 where b=678;
--bigint
insert into t1 VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT);
UPDATE t1 set a='12' where id=6;
UPDATE t1 set b='34' where id=6;
UPDATE t1 set c='56' where id=6;
select * from t1;
delete  from  t1 where c=56;

--serial
UPDATE t1 set id='99' where c=100;
select * from t1 order by id ;	
delete  from  t1 where id=99;

drop table t1;

--Arbitrary Precision Numbers 任意精度数字
drop table if exists t2;
create table t2 (
id smallserial,
a decimal,
b decimal(4,2),
c numeric(2)
);
insert into t2 values (DEFAULT,DEFAULT,88.88,77);
insert into t2 values (DEFAULT,DEFAULT,88.888,77.7);
--smallserial
insert into t2 VALUES(32767,DEFAULT,22.22,22);
UPDATE t2 set id='6' where c=22;
select * from t2 order by id ;	
delete  from  t2 where id=6;
--decimal()
select * from t2;
UPDATE t2 set b='55.55' where id=1;
select * from t2 ORDER by id;
delete  from  t2 where id=1;
UPDATE t2 set c='33' where id=2;
select * from t2 order by id;
delete  from  t2 where c=33;
drop table t2;

--Floating-Point Types and Serial Types -- 浮点和序数

drop table if exists t3;
create table t3(
id bigserial,
a real,  
b double precision); 
insert into t3 values(default,333.333,44.12233);
insert into t3(a,b) values(44556,123456);
insert into t3(a,b) values(1.2345678901234e+20,1.2345678901234e+200);
insert into t3(a,b) values(1.2345678901234e-20,1.2345678901234e-200);
select * from t3;
--bigserial
insert into t3 VALUES(9223372036854775807,123,456);
select * from t3;
UPDATE t3 set id='4' where b=456;
select * from t3 order by id ;	
delete  from  t3 where id=4;


--real
UPDATE t3 set a='333' where id=1;
select * from t3 order by id ;
delete  from  t3 where a=333;
select * from t3 order by id ;
--double precision
UPDATE t3 set b='666' where id=2;
select * from t3 order by id ;
delete  from  t3 where b=666;
drop table t3;


--Monetary Types--
drop table if exists t4;
create table t4(
id int2,
a money);
insert into t4 values (1,1);
insert into t4 values (2,3.141);
insert into t4 values (3,3.145);
set lc_monetary='zh_CN.UTF-8';
select * from t4;
UPDATE t4 set a='666' where id=1;
select * from t4 order by id;
delete  from  t4 where id=1;
select sum(a) from t4;
drop table t4;

--Character Types 字符类型

drop table if exists char_test;
create table char_test(
id int2,
a char(4),
b varchar(4));

insert into char_test values(1,'abcd', 'qwer');
insert into char_test values(2,'a', 'b');
insert into char_test values(3,'1234', '5678');
insert into char_test values(4,1234,5678);
select * from char_test;
--char(4)
UPDATE char_test set a='666' where id=3;
select * from char_test ORDER by id;
delete  from  char_test  where id=3;

--varchar(4)
UPDATE char_test set b='3333' where id=4;
delete  from  char_test  where id=4;
select  * from char_test ORDER by id;

SELECT a, char_length(a),b,char_length(b) FROM char_test;

drop table char_test;

--Binary Data Types 
drop table if exists tab_bytea;
CREATE TABLE tab_bytea(a bytea,b bytea);

INSERT INTO tab_bytea VALUES('\047',E'\xF');
UPDATE tab_bytea set a='\134' where a='\047';
delete  from  tab_bytea  where a='\134';

--SELECT * FROM tab_bytea;

drop table tab_bytea;

--Date/Time Types 日期/时间类型

drop table if exists tab_date;
CREATE TABLE tab_date(id smallserial,a date);
show datestyle;
INSERT INTO tab_date VALUES(DEFAULT,'2020-10-26');
UPDATE tab_date set a='2021-10-26' where id=1;
SET datestyle = 'MDY';
INSERT INTO tab_date VALUES(DEFAULT,'10-26-2020');

--在MDY风格下，也支持YMD的输入方式，但是不支持DMY或者其它格式的输入，如下会报错

INSERT INTO tab_date VALUES(DEFAULT, '2021-10-28');
SET datestyle = 'DMY';
INSERT INTO tab_date VALUES(DEFAULT, '26-10-2021');
UPDATE tab_date set a='2021-10-27' where id=4;
SELECT * FROM tab_date;
delete from tab_date where a='2021-10-27';
delete from tab_date where id=3;
drop table tab_date;



--timestamp
drop table if exists tab_timestamp;
CREATE TABLE tab_timestamp(a timestamptz,b timestamp);
INSERT INTO tab_timestamp VALUES('2020-04-26 13:20:34 CST','2020-04-08 14:40:12+08');
INSERT INTO tab_timestamp VALUES('2020-04-25 14:56:34','2020-04-09 18:54:12 CST');
update tab_timestamp set a='2021-10-26 13:20:34 CST' where b = '2020-04-08 14:40:12+08';
update tab_timestamp set b='2021-10-08 14:40:12' where a='2021-10-27 03:20:34+08';
SELECT * FROM tab_timestamp ORDER by a;
delete from tab_timestamp where a='2021-10-27 03:20:34+08';
drop table tab_timestamp;


--Boolean Type-- 布尔类型
drop table if exists tab_boolean;
CREATE TABLE tab_boolean(a boolean,b boolean);
INSERT INTO tab_boolean VALUES('true','false');
INSERT INTO tab_boolean VALUES('1','0');
INSERT INTO tab_boolean VALUES('on','off');
INSERT INTO tab_boolean VALUES('ON','OFF');
INSERT INTO tab_boolean VALUES('y','n');
INSERT INTO tab_boolean VALUES('Y','N');
INSERT INTO tab_boolean VALUES('yes','no');
INSERT INTO tab_boolean VALUES('Yes','No');
INSERT INTO tab_boolean VALUES('YES','NO');
SELECT * FROM tab_boolean;
UPDATE tab_boolean set a='no' where b='false';
UPDATE tab_boolean set b='true' where a='no';
SELECT * FROM tab_boolean;
delete from tab_boolean where a='no';
drop table tab_boolean;

--Enumerated Types--  枚举类型

CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
drop table if exists person;
CREATE TABLE person (id int2,a text,b mood);
INSERT INTO person VALUES (1,'Moe', 'happy');
INSERT INTO person VALUES (2,'Larry', 'sad');
INSERT INTO person VALUES (3,DEFAULT, DEFAULT);
select * from person;
UPDATE person set a='Curly' where id=3;
UPDATE person set b='ok' where id=3;
select * from person;
SELECT * FROM person WHERE b > 'sad';
SELECT * FROM person WHERE b > 'sad' ORDER BY b;
delete from person where b='ok';

drop table person;
DROP TYPE mood; 



--Network Address Types-- 网络地址类型
drop table if exists tab_icm;
CREATE TABLE tab_icm(a cidr,b inet,c macaddr);
INSERT INTO tab_icm VALUES(DEFAULT,'10.10.20.10','00-50-56-C0-00-07');
INSERT INTO tab_icm VALUES('10.10/16',DEFAULT,'00-50-56-C0-00-08');
INSERT INTO tab_icm VALUES('10/8','fe80::81a7:c17c:788c:7723','00-50-56-C0-00-01');
SELECT * FROM tab_icm; 
UPDATE tab_icm set a='10.10.20.10/32' where b='10.10.20.10';
UPDATE tab_icm set b='::10.2.3.4' where a='10.10/16';
UPDATE tab_icm set c='00-50-56-C0-00-09' where a='10/8';
SELECT * FROM tab_icm;
delete from tab_icm where a='10/8';
drop table tab_icm;


-- Bit String Types--位串类型
drop table if exists tab_bit_string;
CREATE TABLE tab_bit_string (id int2,a BIT(3), b BIT VARYING(5));

INSERT INTO tab_bit_string VALUES (1,B'101', B'00');
INSERT INTO tab_bit_string VALUES (2,B'10'::bit(3), B'101');
select * from tab_bit_string;

UPDATE tab_bit_string set b=B'11011' where id=1; 
select * from tab_bit_string order by id;
delete from tab_bit_string where id =2;

SELECT * FROM tab_bit_string;
drop table tab_bit_string;




--JSON Types
drop table if exists tab_json;
CREATE TABLE tab_json(id int2,a json);
INSERT INTO tab_json VALUES(1,'{"广东省": "深圳市", "江苏省": "南京市", "甘肃省": "兰州市"}');
 
INSERT INTO tab_json VALUES(2,'{"四川省": "成都市", "湖北省": "武汉市", "陕西省": "西安市"}');

SELECT * FROM tab_json;
UPDATE tab_json set a='{"广东省": "广州市", "江苏省": "南京市", "甘肃省": "兰州市"}' where id=1;
SELECT * FROM tab_json order by id;
delete from tab_json where id =2;
drop table tab_json;


 
--pg_lsn Type pg_lsn 类型
drop table if exists tab_pg_lsn;
create table tab_pg_lsn (id int2, a pg_lsn);
insert into tab_pg_lsn values (1,'0/0');
insert into tab_pg_lsn values (2,'0/12345678');
UPDATE tab_pg_lsn set a='1/1' where id=1;
delete from tab_pg_lsn where id =2;
drop table tab_pg_lsn;

 
--Object Identifier Types 对象标识符类型
drop table if exists tab_oid;
CREATE TABLE tab_oid(id int, a oid);
INSERT INTO tab_oid VALUES (1,'1234');
INSERT INTO tab_oid VALUES (2,'987');
INSERT INTO tab_oid VALUES (3,'   10  ');
select * from tab_oid;
UPDATE tab_oid set a='9999999' where id=1;
UPDATE tab_oid set a='-1040' where id=2;
delete from tab_oid where id =3;
select * from tab_oid order by id;
drop table tab_oid;


--UUID Type-- UUID类型
drop table if exists tab_uuid;
create table tab_uuid(id int2, a uuid);
insert into tab_uuid values(1,'11111111-1111-1111-1111-111111111111');
insert into tab_uuid values(2,'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');
select * from tab_uuid;

UPDATE tab_uuid set a='22222222-2222-2222-2222-222222222222' where id=1;
delete from tab_uuid where id =2;
select * from tab_uuid;
drop table tab_uuid;


-- XML Type--XML类型
--drop table if exists tab_xml;
--create table tab_xml(id int2, a xml);
--INSERT INTO tab_xml VALUES (1, '<value>one</value>');
--INSERT INTO tab_xml VALUES (2, '<value>two</value>');
--select * from tab_xml;
--UPDATE tab_xml set a='<value>three</value>' where id=1;
--UPDATE tab_xml set a='<value>four</value>' where id=2;
--delete from tab_xml where id =2;
--drop table tab_xml;


--Range Types  范围类型
--drop table if exists tab_range;
--create table tab_range(id int2,a int4range,b int8range,c numrange);
--insert into tab_range values (1,'[2,8]','[-2,88)','[3.14,55.14]');
--insert into tab_range values (2,int4range(123,123,'[]'),int8range(456,789,'[)'),numrange(222.22,333.33,'()'));
--select * from tab_range
--UPDATE tab_range set a='[1,9]' where id=1;
--UPDATE tab_range set b='[-88,2)' where id=1;
--UPDATE tab_range set c='[-3.14,55.14]' where id=1;
--select * from tab_range order by id;
--delete from tab_range where a='[1,9]';
--drop table tab_range;

--drop table if exists tab_range2;
--create table tab_range2(id int2,a tsrange,b tstzrange,c daterange);
--insert into tab_range2 VALUES(1,
--'[2010-01-01 14:30, 2010-01-01 15:30)',
--'[2011-01-01 16:30, 2011-01-01 17:30]',
--'[2011-01-01,2012-01-01]');
--UPDATE tab_range2 set c='[2020-01-01,2021-01-01]' where id=1;
--delete from tab_range2 where id=1;
--drop table tab_range2;


--Domain Types 域类型
--CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
--drop table if exists mytable;
--CREATE TABLE mytable (id posint,a int2);
--INSERT INTO mytable VALUES(1,123);
--INSERT INTO mytable VALUES(10,456);
--select * from mytable;
--UPDATE mytable set id=2 where a=456;
--UPDATE mytable set id=-1 where a=123;
--delete from mytable where id =2;
--drop table mytable;
--drop domain posint;


--Arrays  数组
--drop table if exists tab_array;
--CREATE TABLE tab_array(id int2,a text[],b integer[][],c integer ARRAY[3]);
--INSERT INTO tab_array VALUES(1,'{"江苏省","甘肃省","北京市"}','{1,2,3,4,5}','{21,22,31}');
--INSERT INTO tab_array VALUES(2,'{"天津市","湖北省","陕西市"}','{5,4,3,2,1}','{21,22,31,44}');
--UPDATE tab_array set a='{"江苏省","甘肃省","广东省"}' where id=1;
--UPDATE tab_array set b[1]='广州市' where id=2;
--delete from tab_array where id =1;
--SELECT * FROM tab_array;
--SELECT a[1],b[3],c[4] FROM tab_array;
 
--drop table tab_array;
 
 
 
 
--Composite Types 组合类型
 
--CREATE TYPE tab_com AS (
--    name            text,
--    supplier_id     integer,
--    price           numeric
--);


--CREATE FUNCTION price_extension(tab_com, integer) RETURNS numeric
--AS 'SELECT $1.price * $2' LANGUAGE SQL;


--CREATE TABLE on_hand ( item      tab_com, count     integer);

--INSERT INTO on_hand VALUES (ROW('fuzzy dice', 42, 1.99), 1000);

--SELECT price_extension(item, 10) FROM on_hand;

--SELECT * FROM on_hand;
--drop table on_hand;


--Geometric Types 几何类型
--drop table if exists tab_geometric;
--CREATE TABLE tab_geometric(id int2,a point,b lseg,c box,d path,e path,f polygon,j circle);
--INSERT INTO tab_geometric VALUES(1,'(1,2)',
--'[(1,2),(2,3)]',
--'((1,2),(1,3))',
--'[(1,2),(2,3),(2,4),(1,3),(0,2)]',
--'[(1,2),(2,3),(3,4)]',
--'((1,2),(2,3),(2,4),(1,3),(0,2))',
--'<(2,3),3>');
--SELECT * FROM tab_geometric;

--drop table tab_geometric;



