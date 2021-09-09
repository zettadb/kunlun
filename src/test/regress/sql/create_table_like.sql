/* Test inheritance of structure (LIKE) */
DROP TABLE if exists inhx;
CREATE TABLE inhx (xx varchar(100) DEFAULT 'text');

CREATE TABLE foo (LIKE nonexistent);

CREATE TABLE inhe (ee text, LIKE inhx);
INSERT INTO inhe VALUES (DEFAULT, 'ee-col4');
SELECT * FROM inhe; /* Columns aa, bb, xx value NULL, ee */
SELECT * FROM inhx; /* Empty set since LIKE inherits structure only */

CREATE TABLE inhf (LIKE inhx, LIKE inhx); /* Throw error */

CREATE TABLE inhf (LIKE inhx INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
INSERT INTO inhf DEFAULT VALUES;
SELECT * FROM inhf; /* Single entry with value 'text' */

ALTER TABLE inhx ADD PRIMARY KEY (xx);
CREATE TABLE inhg (LIKE inhx); /* Doesn't copy constraint */
INSERT INTO inhg VALUES ('foo');
DROP TABLE inhg;
CREATE TABLE inhg (x text, LIKE inhx INCLUDING CONSTRAINTS, y text); /* Copies constraints */
INSERT INTO inhg VALUES ('x', 'text', 'y'); /* Succeeds */
INSERT INTO inhg VALUES ('x', 'text', 'y'); /* Succeeds -- Unique constraints not copied */
INSERT INTO inhg VALUES ('x', 'foo',  'y');  /* fails due to constraint */
SELECT * FROM inhg; /* Two records with three columns in order x=x, xx=text, y=y */
DROP TABLE inhg;

CREATE TABLE test_like_id_1 (a bigint GENERATED ALWAYS AS IDENTITY, b text);
\d test_like_id_1
INSERT INTO test_like_id_1 (b) VALUES ('b1');
SELECT * FROM test_like_id_1;
CREATE TABLE test_like_id_2 (LIKE test_like_id_1);
\d test_like_id_2
INSERT INTO test_like_id_2 (b) VALUES ('b2');
SELECT * FROM test_like_id_2;  -- identity was not copied
CREATE TABLE test_like_id_3 (LIKE test_like_id_1 INCLUDING IDENTITY);
\d test_like_id_3
INSERT INTO test_like_id_3 (b) VALUES ('b3');
SELECT * FROM test_like_id_3;  -- identity was copied and applied
DROP TABLE test_like_id_1;
DROP TABLE test_like_id_2;
DROP TABLE test_like_id_3;

CREATE TABLE inhg (x text, LIKE inhx INCLUDING INDEXES, y text); /* copies indexes */
INSERT INTO inhg VALUES (5, 10);
INSERT INTO inhg VALUES (20, 10); -- should fail
DROP TABLE inhg;
/* Multiple primary keys creation should fail */
CREATE TABLE inhg (x varchar(100), LIKE inhx INCLUDING INDEXES, PRIMARY KEY(x)); /* fails */
CREATE TABLE inhz (xx varchar(100) DEFAULT 'text', yy int UNIQUE);
-- CREATE UNIQUE INDEX inhz_xx_idx on inhz (xx) WHERE xx <> 'test'; partial not supported in Kunlun
CREATE UNIQUE INDEX inhz_xx_idx on inhz (xx);
/* Ok to create multiple unique indexes */
CREATE TABLE inhg (x varchar(100) UNIQUE, LIKE inhz INCLUDING INDEXES);
INSERT INTO inhg (xx, yy, x) VALUES ('test', 5, 10);
INSERT INTO inhg (xx, yy, x) VALUES ('test', 10, 15);
INSERT INTO inhg (xx, yy, x) VALUES ('foo', 10, 15); -- should fail
DROP TABLE inhg;
DROP TABLE inhz;
DROP TABLE inhx;

-- including storage and comments
CREATE TABLE ctlt1 (a varchar(100) PRIMARY KEY, b varchar(100));
CREATE INDEX ctlt1_b_key ON ctlt1 (b);
-- CREATE INDEX ctlt1_fnidx ON ctlt1 ((a || b));
COMMENT ON COLUMN ctlt1.a IS 'A';
COMMENT ON COLUMN ctlt1.b IS 'B';
COMMENT ON CONSTRAINT ctlt1_a_check ON ctlt1 IS 't1_a_check';
COMMENT ON INDEX ctlt1_pkey IS 'index pkey';
COMMENT ON INDEX ctlt1_b_key IS 'index b_key';

CREATE TABLE ctlt2 (c text);
COMMENT ON COLUMN ctlt2.c IS 'C';

CREATE TABLE ctlt3 (a text, c text);
COMMENT ON COLUMN ctlt3.a IS 'A3';
COMMENT ON COLUMN ctlt3.c IS 'C';
COMMENT ON CONSTRAINT ctlt3_a_check ON ctlt3 IS 't3_a_check';

CREATE TABLE ctlt4 (a text, c text);

CREATE TABLE ctlt12_storage (LIKE ctlt1 INCLUDING STORAGE, LIKE ctlt2 INCLUDING STORAGE);
CREATE TABLE ctlt12_comments (LIKE ctlt1 INCLUDING COMMENTS, LIKE ctlt2 INCLUDING COMMENTS);

CREATE TABLE ctlt_all (LIKE ctlt1 INCLUDING ALL);
\d+ ctlt_all
SELECT c.relname, objsubid, description FROM pg_description, pg_index i, pg_class c WHERE classoid = 'pg_class'::regclass AND objoid = i.indexrelid AND c.oid = i.indexrelid AND i.indrelid = 'ctlt_all'::regclass ORDER BY c.relname, objsubid;
SELECT s.stxname, objsubid, description FROM pg_description, pg_statistic_ext s WHERE classoid = 'pg_statistic_ext'::regclass AND objoid = s.oid AND s.stxrelid = 'ctlt_all'::regclass ORDER BY s.stxname, objsubid;

DROP TABLE ctlt1 CASCADE;
DROP TABLE ctlt2 CASCADE;
DROP TABLE ctlt3 CASCADE;
DROP TABLE ctlt4 CASCADE;
DROP TABLE ctlt12_storage CASCADE;
DROP TABLE ctlt12_comments CASCADE;
DROP TABLE ctlt_all CASCADE;

/* LIKE with other relation kinds */

CREATE TABLE ctlt4 (a int, b text);

CREATE SEQUENCE ctlseq1;
CREATE TABLE ctlt10 (LIKE ctlseq1);  -- fail

CREATE VIEW ctlv1 AS SELECT * FROM ctlt4;
CREATE TABLE ctlt11 (LIKE ctlv1);
CREATE TABLE ctlt11a (LIKE ctlv1 INCLUDING ALL);

CREATE TYPE ctlty1 AS (a int, b text);
CREATE TABLE ctlt12 (LIKE ctlty1);

DROP SEQUENCE ctlseq1;
DROP TYPE ctlty1;
DROP VIEW ctlv1;
DROP TABLE IF EXISTS ctlt4;
DROP TABLE IF EXISTS ctlt10;
DROP TABLE IF EXISTS ctlt11;
DROP TABLE IF EXISTS ctlt11a;
DROP TABLE IF EXISTS ctlt12;

/* LIKE WITH OIDS */
CREATE TABLE no_oid (y INTEGER);
CREATE TABLE like_test2 (z INTEGER, LIKE no_oid);
SELECT oid FROM like_test2; -- fail
DROP TABLE no_oid;
DROP TABLE like_test2;
