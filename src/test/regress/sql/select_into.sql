--
-- SELECT_INTO
--

SELECT *
   INTO TABLE sitmp1
   FROM onek
   WHERE onek.unique1 < 2;
--DDL_STATEMENT_BEGIN--
DROP TABLE sitmp1;
--DDL_STATEMENT_END--
SELECT *
   INTO TABLE sitmp1
   FROM onek2
   WHERE onek2.unique1 < 2;
--DDL_STATEMENT_BEGIN--
DROP TABLE sitmp1;
--DDL_STATEMENT_END--
--
-- SELECT INTO and INSERT permission, if owner is not allowed to insert.
--
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA selinto_schema;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_selinto_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES FOR ROLE regress_selinto_user
	  REVOKE INSERT ON TABLES FROM regress_selinto_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON SCHEMA selinto_schema TO public;
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_selinto_user;
SELECT * INTO TABLE selinto_schema.tmp1
	  FROM pg_class WHERE relname like '%a%';	-- Error
SELECT oid AS clsoid, relname, relnatts + 10 AS x
	  INTO selinto_schema.tmp2
	  FROM pg_class WHERE relname like '%b%';	-- Error
--DDL_STATEMENT_BEGIN--
CREATE TABLE selinto_schema.tmp3 (a,b,c)
	   AS SELECT oid,relname,relacl FROM pg_class
	   WHERE relname like '%c%';	-- Error
--DDL_STATEMENT_END--
RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES FOR ROLE regress_selinto_user
	  GRANT INSERT ON TABLES TO regress_selinto_user;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_selinto_user;
SELECT * INTO TABLE selinto_schema.tmp1
	  FROM pg_class WHERE relname like '%a%';	-- OK
SELECT oid AS clsoid, relname, relnatts + 10 AS x
	  INTO selinto_schema.tmp2
	  FROM pg_class WHERE relname like '%b%';	-- OK
--DDL_STATEMENT_BEGIN--	  
CREATE TABLE selinto_schema.tmp3 (a,b,c)
	   AS SELECT oid,relname,relacl FROM pg_class
	   WHERE relname like '%c%';	-- OK
--DDL_STATEMENT_END--
RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
DROP SCHEMA selinto_schema CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_selinto_user;
--DDL_STATEMENT_END--

-- Tests for WITH NO DATA and column name consistency
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctas_base (i int, j int);
--DDL_STATEMENT_END--
INSERT INTO ctas_base VALUES (1, 2);
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctas_nodata (ii, jj, kk) AS SELECT i, j FROM ctas_base; -- Error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctas_nodata (ii, jj, kk) AS SELECT i, j FROM ctas_base WITH NO DATA; -- Error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctas_nodata (ii, jj) AS SELECT i, j FROM ctas_base; -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctas_nodata_2 (ii, jj) AS SELECT i, j FROM ctas_base WITH NO DATA; -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctas_nodata_3 (ii) AS SELECT i, j FROM ctas_base; -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctas_nodata_4 (ii) AS SELECT i, j FROM ctas_base WITH NO DATA; -- OK
--DDL_STATEMENT_END--
SELECT * FROM ctas_nodata;
SELECT * FROM ctas_nodata_2;
SELECT * FROM ctas_nodata_3;
SELECT * FROM ctas_nodata_4;
--DDL_STATEMENT_BEGIN--
DROP TABLE ctas_base;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctas_nodata;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctas_nodata_2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctas_nodata_3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctas_nodata_4;
--DDL_STATEMENT_END--

--
-- CREATE TABLE AS/SELECT INTO as last command in a SQL function
-- have been known to cause problems
--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION make_table() RETURNS VOID
AS $$
  CREATE TABLE created_table AS SELECT * FROM int8_tbl;
$$ LANGUAGE SQL;
--DDL_STATEMENT_END--
SELECT make_table();

SELECT * FROM created_table;

-- Try EXPLAIN ANALYZE SELECT INTO, but hide the output since it won't
-- be stable.
DO $$
BEGIN
	EXECUTE 'EXPLAIN ANALYZE SELECT * INTO TABLE easi FROM int8_tbl';
END$$;
--DDL_STATEMENT_BEGIN--
DROP TABLE created_table;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE easi;
--DDL_STATEMENT_END--
--
-- Disallowed uses of SELECT ... INTO.  All should fail
--
DECLARE foo CURSOR FOR SELECT 1 INTO b;
COPY (SELECT 1 INTO frak UNION SELECT 2) TO 'blob';
SELECT * FROM (SELECT 1 INTO f) bar;
--DDL_STATEMENT_BEGIN--
CREATE VIEW foo AS SELECT 1 INTO b;
--DDL_STATEMENT_END--
INSERT INTO b SELECT 1 INTO f;
