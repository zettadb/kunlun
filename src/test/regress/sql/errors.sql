--
-- ERRORS
--

-- bad in postquel, but ok in PostgreSQL
select 1;


--
-- UNSUPPORTED STUFF

-- doesn't work
-- notify pg_class
--

--
-- SELECT

-- this used to be a syntax error, but now we allow an empty target list
select;

-- no such relation
select * from nonesuch;

-- bad name in target list
select nonesuch from pg_database;

-- empty distinct list isn't OK
select distinct from pg_database;

-- bad attribute name on lhs of operator
select * from pg_database where nonesuch = pg_database.datname;

-- bad attribute name on rhs of operator
select * from pg_database where pg_database.datname = nonesuch;

-- bad attribute name in select distinct on
select distinct on (foobar) * from pg_database;


--
-- DELETE

-- missing relation name (this had better not wildcard!)
delete from;

-- no such relation
delete from nonesuch;


--
-- DROP

-- missing relation name (this had better not wildcard!)
--DDL_STATEMENT_BEGIN--
drop table;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- no such relation
drop table nonesuch;
--DDL_STATEMENT_END--


--
-- ALTER TABLE

-- relation renaming

-- missing relation name
--DDL_STATEMENT_BEGIN--
alter table rename;
--DDL_STATEMENT_END--

-- no such relation
--DDL_STATEMENT_BEGIN--
alter table nonesuch rename to newnonesuch;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such relation
alter table nonesuch rename to stud_emp;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- conflict
alter table stud_emp rename to aggtest;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- self-conflict
alter table stud_emp rename to stud_emp;
--DDL_STATEMENT_END--


-- attribute renaming

--DDL_STATEMENT_BEGIN--
-- no such relation
alter table nonesuchrel rename column nonesuchatt to newnonesuchatt;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such attribute
alter table emp rename column nonesuchatt to newnonesuchatt;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- conflict
alter table emp rename column salary to manager;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- conflict
alter table emp rename column salary to oid;
--DDL_STATEMENT_END--


--
-- TRANSACTION STUFF

-- not in a xact
abort;

-- not in a xact
end;


--
-- CREATE AGGREGATE

-- sfunc/finalfunc type disagreement
--DDL_STATEMENT_BEGIN--
create aggregate newavg2 (sfunc = int4pl,
			  basetype = int4,
			  stype = int4,
			  finalfunc = int2um,
			  initcond = '0');
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- left out basetype
create aggregate newcnt1 (sfunc = int4inc,
			  stype = int4,
			  initcond = '0');
			  

--DDL_STATEMENT_END--
--
-- DROP INDEX

--DDL_STATEMENT_BEGIN--
-- missing index name
drop index;

--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- bad index name
drop index 314159;

--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- no such index
drop index nonesuch;
--DDL_STATEMENT_END--


--
-- DROP AGGREGATE

--DDL_STATEMENT_BEGIN--

-- missing aggregate name
drop aggregate;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- missing aggregate type
drop aggregate newcnt1;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- bad aggregate name
drop aggregate 314159 (int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- bad aggregate type
drop aggregate newcnt (nonesuch);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such aggregate
drop aggregate nonesuch (int4);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such aggregate for type
drop aggregate newcnt (float4);

--DDL_STATEMENT_END--

--
-- DROP FUNCTION

--DDL_STATEMENT_BEGIN--
-- missing function name
drop function ();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- bad function name
drop function 314159();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such function
drop function nonesuch();
--DDL_STATEMENT_END--


--
-- DROP TYPE

--DDL_STATEMENT_BEGIN--
-- missing type name
drop type;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- bad type name
drop type 314159;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such type
drop type nonesuch;


--DDL_STATEMENT_END--
--
-- DROP OPERATOR

--DDL_STATEMENT_BEGIN--
-- missing everything
drop operator;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- bad operator name
drop operator equals;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- missing type list
drop operator ===;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- missing parentheses
drop operator int4, int4;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- missing operator name
drop operator (int4, int4);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- missing type list contents
drop operator === ();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such operator
drop operator === (int4);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such operator by that name
drop operator === (int4, int4);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such type1
drop operator = (nonesuch);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such type1
drop operator = ( , int4);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such type1
drop operator = (nonesuch, int4);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such type2
drop operator = (int4, nonesuch);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such type2
drop operator = (int4, );
--DDL_STATEMENT_END--

--
-- DROP RULE

--DDL_STATEMENT_BEGIN--
-- missing rule name
drop rule;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- bad rule name
drop rule 314159;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- no such rule
drop rule nonesuch on noplace;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- these postquel variants are no longer supported
drop tuple rule nonesuch;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop instance rule nonesuch on noplace;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop rewrite rule nonesuch;
--DDL_STATEMENT_END--

--
-- Check that division-by-zero is properly caught.
--

select 1/0;

select 1::int8/0;

select 1/0::int8;

select 1::int2/0;

select 1/0::int2;

select 1::numeric/0;

select 1/0::numeric;

select 1::float8/0;

select 1/0::float8;

select 1::float4/0;

select 1/0::float4;


--
-- Test psql's reporting of syntax error location
--

xxx;

--DDL_STATEMENT_BEGIN--
CREATE foo;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE ;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE
--DDL_STATEMENT_END--
\g

INSERT INTO foo VALUES(123) foo;

INSERT INTO 123
VALUES(123);

INSERT INTO foo
VALUES(123) 123
;

-- with a tab
--DDL_STATEMENT_BEGIN--
CREATE TABLE foo
  (id INT4 UNIQUE NOT NULL, id2 varchar(50) NOT NULL PRIMARY KEY,
	id3 INTEGER NOT NUL,
   id4 INT4 UNIQUE NOT NULL, id5 varchar(50) UNIQUE NOT NULL);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- long line to be truncated on the left
CREATE TABLE foo(id INT4 UNIQUE NOT NULL, id2 varchar(50) NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL,
id4 INT4 UNIQUE NOT NULL, id5 varchar(50) UNIQUE NOT NULL);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- long line to be truncated on the right
CREATE TABLE foo(
id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 varchar(50) UNIQUE NOT NULL, id INT4 UNIQUE NOT NULL, id2 varchar(50) NOT NULL PRIMARY KEY);

--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- long line to be truncated both ways
CREATE TABLE foo(id INT4 UNIQUE NOT NULL, id2 varchar(50) NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 varchar(50) UNIQUE NOT NULL);
--DDL_STATEMENT_END--

-- long line to be truncated on the left, many lines
--DDL_STATEMENT_BEGIN--
CREATE
TEMPORARY
TABLE
foo(id INT4 UNIQUE NOT NULL, id2 varchar(50) NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL,
id4 INT4
UNIQUE
NOT
NULL,
id5 varchar(50)
UNIQUE
NOT
NULL)
;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- long line to be truncated on the right, many lines
CREATE
TEMPORARY
TABLE
foo(
id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 varchar(50) UNIQUE NOT NULL, id INT4 UNIQUE NOT NULL, id2 varchar(50) NOT NULL PRIMARY KEY)
;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- long line to be truncated both ways, many lines
CREATE
TEMPORARY
TABLE
foo
(id
INT4
UNIQUE NOT NULL, idx INT4 UNIQUE NOT NULL, idy INT4 UNIQUE NOT NULL, id2 varchar(50) NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 varchar(50) UNIQUE NOT NULL,
idz INT4 UNIQUE NOT NULL,
idv INT4 UNIQUE NOT NULL);
--DDL_STATEMENT_END--

-- more than 10 lines...
--DDL_STATEMENT_BEGIN--
CREATE
TEMPORARY
TABLE
foo
(id
INT4
UNIQUE
NOT
NULL
,
idm
INT4
UNIQUE
NOT
NULL,
idx INT4 UNIQUE NOT NULL, idy INT4 UNIQUE NOT NULL, id2 varchar(50) NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 varchar(50) UNIQUE NOT NULL,
idz INT4 UNIQUE NOT NULL,
idv
INT4
UNIQUE
NOT
NULL);
--DDL_STATEMENT_END--

-- Check that stack depth detection mechanism works and
-- max_stack_depth is not set too high
--DDL_STATEMENT_BEGIN--
create function infinite_recurse() returns int as
'select infinite_recurse()' language sql;
--DDL_STATEMENT_END--
\set VERBOSITY terse
select infinite_recurse();
