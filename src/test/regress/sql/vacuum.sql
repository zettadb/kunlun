--
-- VACUUM
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE vactst (i INT);
--DDL_STATEMENT_END--
INSERT INTO vactst VALUES (1);
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
SELECT * FROM vactst;
VACUUM FULL vactst;
UPDATE vactst SET i = i + 1;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
VACUUM (FULL) vactst;
DELETE FROM vactst;
SELECT * FROM vactst;

VACUUM (FULL, FREEZE) vactst;
VACUUM (ANALYZE, FULL) vactst;
--DDL_STATEMENT_BEGIN--
CREATE TABLE vaccluster (i INT PRIMARY KEY);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE vaccluster CLUSTER ON vaccluster_pkey;
--DDL_STATEMENT_END--
CLUSTER vaccluster;
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION do_analyze() RETURNS VOID VOLATILE LANGUAGE SQL
	AS 'ANALYZE pg_am';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION wrap_do_analyze(c INT) RETURNS INT IMMUTABLE LANGUAGE SQL
	AS 'SELECT $1 FROM do_analyze()';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX ON vaccluster(wrap_do_analyze(i));
INSERT INTO vaccluster VALUES (1), (2);
--DDL_STATEMENT_END--
ANALYZE vaccluster;

VACUUM FULL pg_am;
VACUUM FULL pg_class;
VACUUM FULL pg_database;
VACUUM FULL vaccluster;
VACUUM FULL vactst;

VACUUM (DISABLE_PAGE_SKIPPING) vaccluster;

-- partitioned table
--DDL_STATEMENT_BEGIN--
CREATE TABLE vacparted (a int, b char) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE vacparted1 PARTITION OF vacparted FOR VALUES IN (1);
--DDL_STATEMENT_END--
INSERT INTO vacparted VALUES (1, 'a');
UPDATE vacparted SET b = 'b';
VACUUM (ANALYZE) vacparted;
VACUUM (FULL) vacparted;
VACUUM (FREEZE) vacparted;

-- check behavior with duplicate column mentions
VACUUM ANALYZE vacparted(a,b,a);
ANALYZE vacparted(a,b,b);

-- multiple tables specified
VACUUM vaccluster, vactst;
VACUUM vacparted, does_not_exist;
VACUUM (FREEZE) vacparted, vaccluster, vactst;
VACUUM (FREEZE) does_not_exist, vaccluster;
VACUUM ANALYZE vactst, vacparted (a);
VACUUM ANALYZE vactst (does_not_exist), vacparted (b);
VACUUM FULL vacparted, vactst;
VACUUM FULL vactst, vacparted (a, b), vaccluster (i);
ANALYZE vactst, vacparted;
ANALYZE vacparted (b), vactst;
ANALYZE vactst, does_not_exist, vacparted;
ANALYZE vactst (i), vacparted (does_not_exist);

-- parenthesized syntax for ANALYZE
ANALYZE (VERBOSE) does_not_exist;
ANALYZE (nonexistant-arg) does_not_exist;
--DDL_STATEMENT_BEGIN--
DROP TABLE vaccluster;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE vactst;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE vacparted;
--DDL_STATEMENT_END--
