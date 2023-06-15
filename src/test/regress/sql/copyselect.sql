--
-- Test cases for COPY (select) TO
--
--DDL_STATEMENT_BEGIN--
create table test1 (id serial, t text);
--DDL_STATEMENT_END--
insert into test1 (t) values ('a');
insert into test1 (t) values ('b');
insert into test1 (t) values ('c');
insert into test1 (t) values ('d');
insert into test1 (t) values ('e');

--DDL_STATEMENT_BEGIN--
create table test2 (id serial, t text);
--DDL_STATEMENT_END--
insert into test2 (t) values ('A');
insert into test2 (t) values ('B');
insert into test2 (t) values ('C');
insert into test2 (t) values ('D');
insert into test2 (t) values ('E');

--DDL_STATEMENT_BEGIN--
create view v_test1
as select 'v_'||t from test1;
--DDL_STATEMENT_END--

--
-- Test COPY table TO
--
copy test1 to stdout;
--
-- This should fail
--
copy v_test1 to stdout;
--
-- Test COPY (select) TO
--
copy (select t from test1 where id=1) to stdout;
--
-- Test COPY (select for update) TO
--
copy (select t from test1 where id=3 for update) to stdout;
--
-- This should fail
--
copy (select t into temp test3 from test1 where id=3) to stdout;
--
-- This should fail
--
copy (select * from test1) from stdin;
--
-- This should fail
--
copy (select * from test1) (t,id) to stdout;
--
-- Test JOIN
--
copy (select * from test1 join test2 using (id)) to stdout;
--
-- Test UNION SELECT
--
copy (select t from test1 where id = 1 UNION select * from v_test1 ORDER BY 1) to stdout;
--
-- Test subselect
--
copy (select * from (select t from test1 where id = 1 UNION select * from v_test1 ORDER BY 1) t1) to stdout;
--
-- Test headers, CSV and quotes
--
copy (select t from test1 where id = 1) to stdout csv header force quote t;
--
-- Test psql builtins, plain table
--
\copy test1 to stdout
--
-- This should fail
--
\copy v_test1 to stdout
--
-- Test \copy (select ...)
--
\copy (select "id",'id','id""'||t,(id + 1)*id,t,"test1"."t" from test1 where id=3) to stdout
--
-- Drop everything
--
--DDL_STATEMENT_BEGIN--
drop table test2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop view v_test1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table test1;
--DDL_STATEMENT_END--

-- psql handling of COPY in multi-command strings
copy (select 1) to stdout\; select 1/0;	-- row, then error
select 1/0\; copy (select 1) to stdout; -- error only
copy (select 1) to stdout\; copy (select 2) to stdout\; select 0\; select 3; -- 1 2 3

--DDL_STATEMENT_BEGIN--
create table test3 (c int);
--DDL_STATEMENT_END--
select 0\; copy test3 from stdin\; copy test3 from stdin\; select 1; -- 1
1
\.
2
\.
select * from test3;
--DDL_STATEMENT_BEGIN--
drop table test3;
--DDL_STATEMENT_END--
