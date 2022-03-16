/* Test inheritance of structure (LIKE) */
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists inhx;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhx (xx varchar(100) DEFAULT 'text');
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE foo (LIKE nonexistent);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE inhe (ee text, LIKE inhx);
--DDL_STATEMENT_END--
INSERT INTO inhe VALUES (DEFAULT, 'ee-col4');
SELECT * FROM inhe; /* Columns aa, bb, xx value NULL, ee */
SELECT * FROM inhx; /* Empty set since LIKE inherits structure only */

--DDL_STATEMENT_BEGIN--
CREATE TABLE inhf (LIKE inhx, LIKE inhx); /* Throw error */
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE inhf (LIKE inhx INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
--DDL_STATEMENT_END--
INSERT INTO inhf DEFAULT VALUES;
SELECT * FROM inhf; /* Single entry with value 'text' */

--DDL_STATEMENT_BEGIN--
ALTER TABLE inhx ADD PRIMARY KEY (xx);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhg (LIKE inhx); /* Doesn't copy constraint */
--DDL_STATEMENT_END--
INSERT INTO inhg VALUES ('foo');
--DDL_STATEMENT_BEGIN--
DROP TABLE inhg;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhg (x text, LIKE inhx INCLUDING CONSTRAINTS, y text); /* Copies constraints */
--DDL_STATEMENT_END--
INSERT INTO inhg VALUES ('x', 'text', 'y'); /* Succeeds */
INSERT INTO inhg VALUES ('x', 'text', 'y'); /* Succeeds -- Unique constraints not copied */
INSERT INTO inhg VALUES ('x', 'foo',  'y');  /* fails due to constraint */
SELECT * FROM inhg; /* Two records with three columns in order x=x, xx=text, y=y */
--DDL_STATEMENT_BEGIN--
DROP TABLE inhg;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE test_like_id_1 (a bigint GENERATED ALWAYS AS IDENTITY, b text);
--DDL_STATEMENT_END--
\d test_like_id_1
INSERT INTO test_like_id_1 (b) VALUES ('b1');
SELECT * FROM test_like_id_1;
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_like_id_2 (LIKE test_like_id_1);
--DDL_STATEMENT_END--
\d test_like_id_2
INSERT INTO test_like_id_2 (b) VALUES ('b2');
SELECT * FROM test_like_id_2;  -- identity was not copied
--DDL_STATEMENT_BEGIN--
CREATE TABLE test_like_id_3 (LIKE test_like_id_1 INCLUDING IDENTITY);
--DDL_STATEMENT_END--
\d test_like_id_3
INSERT INTO test_like_id_3 (b) VALUES ('b3');
SELECT * FROM test_like_id_3;  -- identity was copied and applied
--DDL_STATEMENT_BEGIN--
DROP TABLE test_like_id_1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE test_like_id_2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE test_like_id_3;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE inhg (x text, LIKE inhx INCLUDING INDEXES, y text); /* copies indexes */
--DDL_STATEMENT_END--
INSERT INTO inhg VALUES (5, 10);
INSERT INTO inhg VALUES (20, 10); -- should fail
--DDL_STATEMENT_BEGIN--
DROP TABLE inhg;
--DDL_STATEMENT_END--
/* Multiple primary keys creation should fail */
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhg (x varchar(100), LIKE inhx INCLUDING INDEXES, PRIMARY KEY(x)); /* fails */
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhz (xx varchar(100) DEFAULT 'text', yy int UNIQUE);
--DDL_STATEMENT_END--
-- CREATE UNIQUE INDEX inhz_xx_idx on inhz (xx) WHERE xx <> 'test'; partial not supported in Kunlun
--DDL_STATEMENT_BEGIN--
CREATE UNIQUE INDEX inhz_xx_idx on inhz (xx);
--DDL_STATEMENT_END--
/* Ok to create multiple unique indexes */
--DDL_STATEMENT_BEGIN--
CREATE TABLE inhg (x varchar(100) UNIQUE, LIKE inhz INCLUDING INDEXES);
--DDL_STATEMENT_END--
INSERT INTO inhg (xx, yy, x) VALUES ('test', 5, 10);
INSERT INTO inhg (xx, yy, x) VALUES ('test', 10, 15);
INSERT INTO inhg (xx, yy, x) VALUES ('foo', 10, 15); -- should fail
--DDL_STATEMENT_BEGIN--
DROP TABLE inhg;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE inhz;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE inhx;
--DDL_STATEMENT_END--

-- including storage and comments
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt1 (a varchar(100) PRIMARY KEY, b varchar(100));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX ctlt1_b_key ON ctlt1 (b);
--DDL_STATEMENT_END--
-- CREATE INDEX ctlt1_fnidx ON ctlt1 ((a || b));
COMMENT ON COLUMN ctlt1.a IS 'A';
COMMENT ON COLUMN ctlt1.b IS 'B';
COMMENT ON CONSTRAINT ctlt1_a_check ON ctlt1 IS 't1_a_check';
COMMENT ON INDEX ctlt1_pkey IS 'index pkey';
COMMENT ON INDEX ctlt1_b_key IS 'index b_key';

--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt2 (c text);
--DDL_STATEMENT_END--
COMMENT ON COLUMN ctlt2.c IS 'C';

--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt3 (a text, c text);
--DDL_STATEMENT_END--
COMMENT ON COLUMN ctlt3.a IS 'A3';
COMMENT ON COLUMN ctlt3.c IS 'C';
COMMENT ON CONSTRAINT ctlt3_a_check ON ctlt3 IS 't3_a_check';

--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt4 (a text, c text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt12_storage (LIKE ctlt1 INCLUDING STORAGE, LIKE ctlt2 INCLUDING STORAGE);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt12_comments (LIKE ctlt1 INCLUDING COMMENTS, LIKE ctlt2 INCLUDING COMMENTS);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt_all (LIKE ctlt1 INCLUDING ALL);
--DDL_STATEMENT_END--
\d+ ctlt_all
SELECT c.relname, objsubid, description FROM pg_description, pg_index i, pg_class c WHERE classoid = 'pg_class'::regclass AND objoid = i.indexrelid AND c.oid = i.indexrelid AND i.indrelid = 'ctlt_all'::regclass ORDER BY c.relname, objsubid;
SELECT s.stxname, objsubid, description FROM pg_description, pg_statistic_ext s WHERE classoid = 'pg_statistic_ext'::regclass AND objoid = s.oid AND s.stxrelid = 'ctlt_all'::regclass ORDER BY s.stxname, objsubid;

--DDL_STATEMENT_BEGIN--
DROP TABLE ctlt1 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctlt2 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctlt3 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctlt4 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctlt12_storage CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctlt12_comments CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE ctlt_all CASCADE;
--DDL_STATEMENT_END--

/* LIKE with other relation kinds */

--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt4 (a int, b text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE ctlseq1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt10 (LIKE ctlseq1);  -- fail
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW ctlv1 AS SELECT * FROM ctlt4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt11 (LIKE ctlv1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt11a (LIKE ctlv1 INCLUDING ALL);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TYPE ctlty1 AS (a int, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE ctlt12 (LIKE ctlty1);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP SEQUENCE ctlseq1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE ctlty1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW ctlv1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE IF EXISTS ctlt4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE IF EXISTS ctlt10;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE IF EXISTS ctlt11;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE IF EXISTS ctlt11a;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE IF EXISTS ctlt12;
--DDL_STATEMENT_END--

/* LIKE WITH OIDS */
--DDL_STATEMENT_BEGIN--
CREATE TABLE no_oid (y INTEGER);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE like_test2 (z INTEGER, LIKE no_oid);
--DDL_STATEMENT_END--
SELECT oid FROM like_test2; -- fail
--DDL_STATEMENT_BEGIN--
DROP TABLE no_oid;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE like_test2;
--DDL_STATEMENT_END--
