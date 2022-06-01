--
-- ALTER TABLE ADD COLUMN DEFAULT test
--

SET search_path = fast_default;
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA fast_default;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists has_volatile;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE has_volatile(id int);
--DDL_STATEMENT_END--
insert into has_volatile SELECT * FROM generate_series(1,10) id;

--DDL_STATEMENT_BEGIN--
ALTER TABLE has_volatile ADD col1 int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE has_volatile ADD col2 int DEFAULT 1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE has_volatile ADD col3 timestamptz DEFAULT current_timestamp;
--DDL_STATEMENT_END--
--mysql的自身的限制，可以忽略 [#728]
--ALTER TABLE has_volatile ADD col4 int DEFAULT (random() * 10000)::int;



-- Test a large sample of different datatypes
--DDL_STATEMENT_BEGIN--
drop table if not exists T;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE T(pk INT NOT NULL PRIMARY KEY, c_int INT DEFAULT 1);
--DDL_STATEMENT_END--

INSERT INTO T VALUES (1), (2);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_bpchar BPCHAR(5) DEFAULT 'hello',
              ALTER COLUMN c_int SET DEFAULT 2;	  
--DDL_STATEMENT_END--

INSERT INTO T VALUES (3), (4);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_text TEXT,
              ALTER COLUMN c_bpchar SET DEFAULT 'dog';	  
--DDL_STATEMENT_END--

INSERT INTO T VALUES (5), (6);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_date DATE DEFAULT '2016-06-02';
--             ALTER COLUMN c_text SET DEFAULT 'cat';
--DDL_STATEMENT_END--

INSERT INTO T VALUES (7), (8);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_timestamp TIMESTAMP DEFAULT '2016-09-01 12:00:00',
              ADD COLUMN c_timestamp_null TIMESTAMP,
              ALTER COLUMN c_date SET DEFAULT '2010-01-01';
--DDL_STATEMENT_END--

INSERT INTO T VALUES (9), (10);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ALTER COLUMN c_timestamp SET DEFAULT '1970-12-31 11:12:13',
              ALTER COLUMN c_timestamp_null SET DEFAULT '2016-09-29 12:00:00';
--DDL_STATEMENT_END--

INSERT INTO T VALUES (11), (12);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_small SMALLINT DEFAULT -5,
              ADD COLUMN c_small_null SMALLINT;
--DDL_STATEMENT_END--

INSERT INTO T VALUES (13), (14);
--DDL_STATEMENT_BEGIN--

ALTER TABLE T ADD COLUMN c_big BIGINT DEFAULT 180000000000018,
              ALTER COLUMN c_small SET DEFAULT 9,
              ALTER COLUMN c_small_null SET DEFAULT 13;
--DDL_STATEMENT_END--

INSERT INTO T VALUES (15), (16);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_num NUMERIC(10,10) DEFAULT 1.00000000001,
              ALTER COLUMN c_big SET DEFAULT -9999999999999999;
--DDL_STATEMENT_END--

INSERT INTO T VALUES (17), (18);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_time TIME DEFAULT '12:00:00',
              ALTER COLUMN c_num SET DEFAULT 2.000000000000002;
--DDL_STATEMENT_END--

INSERT INTO T VALUES (19), (20);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ALTER COLUMN c_time SET DEFAULT '23:59:59';
--DDL_STATEMENT_END--

INSERT INTO T VALUES (21), (22);

INSERT INTO T VALUES (23), (24);

INSERT INTO T VALUES (25), (26);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ALTER COLUMN c_bpchar    DROP DEFAULT,
              ALTER COLUMN c_date      DROP DEFAULT,
              ALTER COLUMN c_text      DROP DEFAULT,
              ALTER COLUMN c_timestamp DROP DEFAULT,
              ALTER COLUMN c_small     DROP DEFAULT,
              ALTER COLUMN c_big       DROP DEFAULT,
              ALTER COLUMN c_num       DROP DEFAULT,
              ALTER COLUMN c_time      DROP DEFAULT;
--DDL_STATEMENT_END--

INSERT INTO T VALUES (27), (28);

SELECT pk, c_int, c_bpchar, c_text, c_date, c_timestamp,
       c_timestamp_null, c_small, c_small_null,
       c_big, c_num, c_time
FROM T ORDER BY pk;

--DDL_STATEMENT_BEGIN--
DROP TABLE T;
--DDL_STATEMENT_END--

-- Test expressions in the defaults
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION foo(a INT) RETURNS TEXT AS $$
DECLARE res TEXT = '';
        i INT;
BEGIN
  i = 0;
  WHILE (i < a) LOOP
    res = res || chr(ascii('a') + i);
    i = i + 1;
  END LOOP;
  RETURN res;
END; $$ LANGUAGE PLPGSQL STABLE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE T(pk INT NOT NULL PRIMARY KEY, c_int INT DEFAULT LENGTH(foo(6)));
--DDL_STATEMENT_END--

INSERT INTO T VALUES (1), (2);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_bpchar BPCHAR(5) DEFAULT foo(4),
              ALTER COLUMN c_int SET DEFAULT LENGTH(foo(8));
--DDL_STATEMENT_END--

INSERT INTO T VALUES (3), (4);
-- MySQL storage node (1, 1) returned error: 1101, BLOB, TEXT, GEOMETRY or JSON column 'c_text' can't have a default value.
--ALTER TABLE T ADD COLUMN c_text TEXT  DEFAULT foo(6),
--             ALTER COLUMN c_bpchar SET DEFAULT foo(3);
--DDL_STATEMENT_BEGIN--
ALTER TABLE T ALTER COLUMN c_bpchar SET DEFAULT foo(3);
--DDL_STATEMENT_END--

INSERT INTO T VALUES (5), (6);

--DDL_STATEMENT_BEGIN--
--ALTER TABLE T ADD COLUMN c_date DATE
--                  DEFAULT '2016-06-02'::DATE  + LENGTH(foo(10)),
--             ALTER COLUMN c_text SET DEFAULT foo(12);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_date DATE
               DEFAULT '2016-06-02'::DATE  + LENGTH(foo(10));
--DDL_STATEMENT_END--

INSERT INTO T VALUES (7), (8);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_timestamp TIMESTAMP
                  DEFAULT '2016-09-01'::DATE + LENGTH(foo(10)),
              ALTER COLUMN c_date
                  SET DEFAULT '2010-01-01'::DATE - LENGTH(foo(4));
--DDL_STATEMENT_END--

INSERT INTO T VALUES (9), (10);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ALTER COLUMN c_timestamp
                  SET DEFAULT '1970-12-31'::DATE + LENGTH(foo(30));
--DDL_STATEMENT_END--

INSERT INTO T VALUES (11), (12);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ALTER COLUMN c_int DROP DEFAULT;
--DDL_STATEMENT_END--

INSERT INTO T VALUES (13), (14);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ALTER COLUMN c_bpchar    DROP DEFAULT,
              ALTER COLUMN c_date      DROP DEFAULT,
              ALTER COLUMN c_timestamp DROP DEFAULT;
--DDL_STATEMENT_END--

INSERT INTO T VALUES (15), (16);

SELECT * FROM T;

--DDL_STATEMENT_BEGIN--
DROP TABLE T;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP FUNCTION foo(INT);
--DDL_STATEMENT_END--

-- Fall back to full rewrite for volatile expressions
--DDL_STATEMENT_BEGIN--
CREATE TABLE T(pk INT NOT NULL PRIMARY KEY);
--DDL_STATEMENT_END--

INSERT INTO T VALUES (1);

-- now() is stable, because it returns the transaction timestamp
--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c1 TIMESTAMP DEFAULT now();
--DDL_STATEMENT_END--

-- clock_timestamp() is volatile
--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c2 TIMESTAMP DEFAULT clock_timestamp();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP TABLE T;
--DDL_STATEMENT_END--

-- Simple querie
--DDL_STATEMENT_BEGIN--
CREATE TABLE T (pk INT NOT NULL PRIMARY KEY);
--DDL_STATEMENT_END--

INSERT INTO T SELECT * FROM generate_series(1, 10) a;

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_bigint BIGINT NOT NULL DEFAULT -1;
--DDL_STATEMENT_END--

INSERT INTO T SELECT b, b - 10 FROM generate_series(11, 20) a(b);
-- MySQL storage node (1, 1) returned error: 1101, BLOB, TEXT, GEOMETRY or JSON column 'c_text' can't have a default value.
--DDL_STATEMENT_BEGIN--
--ALTER TABLE T ADD COLUMN c_text TEXT DEFAULT 'hello';
--DDL_STATEMENT_END--

--INSERT INTO T SELECT b, b - 10, (b + 10)::text FROM generate_series(21, 30) a(b);

-- WHERE clause
SELECT c_bigint FROM T WHERE c_bigint = -1 LIMIT 1;

EXPLAIN (VERBOSE TRUE, COSTS FALSE)
SELECT c_bigint FROM T WHERE c_bigint = -1 LIMIT 1;

--SELECT c_bigint, c_text FROM T WHERE c_text = 'hello' LIMIT 1;

--EXPLAIN (VERBOSE TRUE, COSTS FALSE) SELECT c_bigint, c_text FROM T WHERE c_text = 'hello' LIMIT 1;


-- COALESCE
--SELECT COALESCE(c_bigint, pk), COALESCE(c_text, pk::text)
--FROM T
--ORDER BY pk LIMIT 10;
SELECT COALESCE(c_bigint, pk)
FROM T
ORDER BY pk LIMIT 10;

-- Aggregate function
--SELECT SUM(c_bigint), MAX(c_text COLLATE "C" ), MIN(c_text COLLATE "C") FROM T;
SELECT SUM(c_bigint) FROM T;
-- ORDER BY
--SELECT * FROM T ORDER BY c_bigint, c_text, pk LIMIT 10;
SELECT * FROM T ORDER BY c_bigint, pk LIMIT 10;
--EXPLAIN (VERBOSE TRUE, COSTS FALSE)
--ELECT * FROM T ORDER BY c_bigint, c_text, pk LIMIT 10;
EXPLAIN (VERBOSE TRUE, COSTS FALSE)
ELECT * FROM T ORDER BY c_bigint, pk LIMIT 10;
-- LIMIT
--SELECT * FROM T WHERE c_bigint > -1 ORDER BY c_bigint, c_text, pk LIMIT 10;
SELECT * FROM T WHERE c_bigint > -1 ORDER BY c_bigint, pk LIMIT 10;
--EXPLAIN (VERBOSE TRUE, COSTS FALSE)
--SELECT * FROM T WHERE c_bigint > -1 ORDER BY c_bigint, c_text, pk LIMIT 10;
SELECT * FROM T WHERE c_bigint > -1 ORDER BY c_bigint, pk LIMIT 10;
--  DELETE with RETURNING
DELETE FROM T WHERE pk BETWEEN 10 AND 20 RETURNING *;
-- will crash: EXPLAIN (VERBOSE TRUE, COSTS FALSE)
-- DELETE FROM T WHERE pk BETWEEN 10 AND 20 RETURNING *;

-- UPDATE
--UPDATE T SET c_text = '"' || c_text || '"'  WHERE pk < 10;
--SELECT * FROM T WHERE c_text LIKE '"%"' ORDER BY PK;

--DDL_STATEMENT_BEGIN--
DROP TABLE T;
--DDL_STATEMENT_END--


-- Combine with other DDL
--DDL_STATEMENT_BEGIN--
CREATE TABLE T(pk INT NOT NULL PRIMARY KEY);
--DDL_STATEMENT_END--

INSERT INTO T VALUES (1), (2);

--DDL_STATEMENT_BEGIN--
ALTER TABLE T ADD COLUMN c_int INT NOT NULL DEFAULT -1;
--DDL_STATEMENT_END--

INSERT INTO T VALUES (3), (4);
--MySQL storage node (1, 1) returned error: 1101, BLOB, TEXT, GEOMETRY or JSON column 'c_text' can't have a default value.
--DDL_STATEMENT_BEGIN--
--ALTER TABLE T ADD COLUMN c_text TEXT DEFAULT 'Hello';
--DDL_STATEMENT_END--

INSERT INTO T VALUES (5), (6);

--DDL_STATEMENT_BEGIN--
--ALTER TABLE T ALTER COLUMN c_text SET DEFAULT 'world',
--              ALTER COLUMN c_int  SET DEFAULT 1;
--DDL_STATEMENT_END--
ALTER TABLE T 
              ALTER COLUMN c_int  SET DEFAULT 1;
INSERT INTO T VALUES (7), (8);

SELECT * FROM T ORDER BY pk;

-- Add an index
--DDL_STATEMENT_BEGIN--
CREATE INDEX i ON T(c_int);
--DDL_STATEMENT_END--

--SELECT c_text FROM T WHERE c_int = -1;

--DDL_STATEMENT_BEGIN--
DROP TABLE T;
--DDL_STATEMENT_END--

-- 2 new columns, both have defaults
--DDL_STATEMENT_BEGIN--
CREATE TABLE t (id serial PRIMARY KEY, a int, b int, c int);
--DDL_STATEMENT_END--
INSERT INTO t (a,b,c) VALUES (1,2,3);
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN x int NOT NULL DEFAULT 4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN y int NOT NULL DEFAULT 5;
--DDL_STATEMENT_END--
SELECT * FROM t;
UPDATE t SET y = 2;
SELECT * FROM t;
--DDL_STATEMENT_BEGIN--
DROP TABLE t;
--DDL_STATEMENT_END--

-- 2 new columns, first has default
--DDL_STATEMENT_BEGIN--
CREATE TABLE t (id serial PRIMARY KEY, a int, b int, c int);
--DDL_STATEMENT_END--
INSERT INTO t (a,b,c) VALUES (1,2,3);
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN x int NOT NULL DEFAULT 4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN y int;
--DDL_STATEMENT_END--
SELECT * FROM t;
UPDATE t SET y = 2;
SELECT * FROM t;
--DDL_STATEMENT_BEGIN--
DROP TABLE t;
--DDL_STATEMENT_END--

-- 2 new columns, second has default
--DDL_STATEMENT_BEGIN--
CREATE TABLE t (id serial PRIMARY KEY, a int, b int, c int);
--DDL_STATEMENT_END--
INSERT INTO t (a,b,c) VALUES (1,2,3);
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN x int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN y int NOT NULL DEFAULT 5;
--DDL_STATEMENT_END--
SELECT * FROM t;
UPDATE t SET y = 2;
SELECT * FROM t;
--DDL_STATEMENT_BEGIN--
DROP TABLE t;
--DDL_STATEMENT_END--

-- 2 new columns, neither has default
--DDL_STATEMENT_BEGIN--
CREATE TABLE t (id serial PRIMARY KEY, a int, b int, c int);
--DDL_STATEMENT_END--
INSERT INTO t (a,b,c) VALUES (1,2,3);
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN x int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN y int;
--DDL_STATEMENT_END--
SELECT * FROM t;
UPDATE t SET y = 2;
SELECT * FROM t;
--DDL_STATEMENT_BEGIN--
DROP TABLE t;
--DDL_STATEMENT_END--

-- same as last 4 tests but here the last original column has a NULL value
-- 2 new columns, both have defaults
--DDL_STATEMENT_BEGIN--
CREATE TABLE t (id serial PRIMARY KEY, a int, b int, c int);
--DDL_STATEMENT_END--
INSERT INTO t (a,b,c) VALUES (1,2,NULL);
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN x int NOT NULL DEFAULT 4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN y int NOT NULL DEFAULT 5;
--DDL_STATEMENT_END--
SELECT * FROM t;
UPDATE t SET y = 2;
SELECT * FROM t;
--DDL_STATEMENT_BEGIN--
DROP TABLE t;
--DDL_STATEMENT_END--

-- 2 new columns, first has default
--DDL_STATEMENT_BEGIN--
CREATE TABLE t (id serial PRIMARY KEY, a int, b int, c int);
--DDL_STATEMENT_END--
INSERT INTO t (a,b,c) VALUES (1,2,NULL);
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN x int NOT NULL DEFAULT 4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN y int;
--DDL_STATEMENT_END--
SELECT * FROM t;
UPDATE t SET y = 2;
SELECT * FROM t;
--DDL_STATEMENT_BEGIN--
DROP TABLE t;
--DDL_STATEMENT_END--

-- 2 new columns, second has default
--DDL_STATEMENT_BEGIN--
CREATE TABLE t (id serial PRIMARY KEY, a int, b int, c int);
--DDL_STATEMENT_END--
INSERT INTO t (a,b,c) VALUES (1,2,NULL);
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN x int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN y int NOT NULL DEFAULT 5;
--DDL_STATEMENT_END--
SELECT * FROM t;
UPDATE t SET y = 2;
SELECT * FROM t;
--DDL_STATEMENT_BEGIN--
DROP TABLE t;
--DDL_STATEMENT_END--

-- 2 new columns, neither has default
--DDL_STATEMENT_BEGIN--
CREATE TABLE t (id serial PRIMARY KEY, a int, b int, c int);
--DDL_STATEMENT_END--
INSERT INTO t (a,b,c) VALUES (1,2,NULL);
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN x int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE t ADD COLUMN y int;
--DDL_STATEMENT_END--
SELECT * FROM t;
UPDATE t SET y = 2;
SELECT * FROM t;
--DDL_STATEMENT_BEGIN--
DROP TABLE t;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE leader (a int PRIMARY KEY, b int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE follower (a int, b int);
--DDL_STATEMENT_END--
INSERT INTO leader VALUES (1, 1), (2, 2);
--DDL_STATEMENT_BEGIN--
ALTER TABLE leader ADD c int;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE leader DROP c;
--DDL_STATEMENT_END--
DELETE FROM leader;

-- check that ALTER TABLE ... ALTER TYPE does the right thing

--DDL_STATEMENT_BEGIN--
CREATE TABLE vtype( a integer);
--DDL_STATEMENT_END--
INSERT INTO vtype VALUES (1);
--DDL_STATEMENT_BEGIN--
ALTER TABLE vtype ADD COLUMN b DOUBLE PRECISION DEFAULT 0.2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE vtype ADD COLUMN c BOOLEAN DEFAULT true;
--DDL_STATEMENT_END--
SELECT * FROM vtype;
--DDL_STATEMENT_BEGIN--
ALTER TABLE vtype
      ALTER b TYPE text USING b::text,
      ALTER c TYPE text USING c::text;
--DDL_STATEMENT_END--
SELECT * FROM vtype;

-- also check the case that doesn't rewrite the table

--DDL_STATEMENT_BEGIN--
CREATE TABLE vtype2 (a int);
--DDL_STATEMENT_END--
INSERT INTO vtype2 VALUES (1);
--DDL_STATEMENT_BEGIN--
ALTER TABLE vtype2 ADD COLUMN b varchar(10) DEFAULT 'xxx';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE vtype2 ALTER COLUMN b SET DEFAULT 'yyy';
--DDL_STATEMENT_END--
INSERT INTO vtype2 VALUES (2);

--DDL_STATEMENT_BEGIN--
ALTER TABLE vtype2 ALTER COLUMN b TYPE varchar(20) USING b::varchar(20);
--DDL_STATEMENT_END--
SELECT * FROM vtype2;

-- cleanup
--DDL_STATEMENT_BEGIN--
DROP TABLE vtype;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE vtype2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE follower;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE leader;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE t1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE has_volatile;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION log_rewrite;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA fast_default;
--DDL_STATEMENT_END--

-- Leave a table with an active fast default in place, for pg_upgrade testing
set search_path = public;
--DDL_STATEMENT_BEGIN--
create table has_fast_default(f1 int);
--DDL_STATEMENT_END--
insert into has_fast_default values(1);
--DDL_STATEMENT_BEGIN--
alter table has_fast_default add column f2 int default 42;
--DDL_STATEMENT_END--
table has_fast_default;
--DDL_STATEMENT_BEGIN--
drop table has_fast_default;
--DDL_STATEMENT_END--