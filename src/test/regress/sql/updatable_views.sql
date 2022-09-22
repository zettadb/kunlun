--
-- UPDATABLE VIEWS
--

-- check that non-updatable views and columns are rejected with useful error
-- messages

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
--DDL_STATEMENT_END--
INSERT INTO base_tbl SELECT i, 'Row ' || i FROM generate_series(-2, 2) g(i);

--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view1 AS SELECT DISTINCT a, b FROM base_tbl; -- DISTINCT not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view2 AS SELECT a, b FROM base_tbl GROUP BY a, b; -- GROUP BY not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view3 AS SELECT 1 FROM base_tbl HAVING max(a) > 0; -- HAVING not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view4 AS SELECT count(*) FROM base_tbl; -- Aggregate functions not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view5 AS SELECT a, rank() OVER() FROM base_tbl; -- Window functions not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view6 AS SELECT a, b FROM base_tbl UNION SELECT -a, b FROM base_tbl; -- Set ops not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view7 AS WITH t AS (SELECT a, b FROM base_tbl) SELECT * FROM t; -- WITH not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view8 AS SELECT a, b FROM base_tbl ORDER BY a OFFSET 1; -- OFFSET not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view9 AS SELECT a, b FROM base_tbl ORDER BY a LIMIT 1; -- LIMIT not supported
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view10 AS SELECT 1 AS a; -- No base relations
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view11 AS SELECT b1.a, b2.b FROM base_tbl b1, base_tbl b2; -- Multiple base relations
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view12 AS SELECT * FROM generate_series(1, 10) AS g(a); -- SRF in rangetable
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view13 AS SELECT a, b FROM (SELECT * FROM base_tbl) AS t; -- Subselect in rangetable
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view14 AS SELECT a, b FROM base_tbl; -- System columns may be part of an updatable view
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view15 AS SELECT a, upper(b) FROM base_tbl; -- Expression/function may be part of an updatable view
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view16 AS SELECT a, b, a AS aa FROM base_tbl; -- Repeated column may be part of an updatable view
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view17 AS SELECT * FROM ro_view1; -- Base relation not updatable
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view18 AS SELECT * FROM (VALUES(1)) AS tmp(a); -- VALUES in rangetable
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE uv_seq;
--DDL_STATEMENT_END--
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view19 AS SELECT * FROM uv_seq; -- View based on a sequence
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW ro_view20 AS SELECT a, b, generate_series(1, a) g FROM base_tbl; -- SRF in targetlist not supported
--DDL_STATEMENT_END--

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE E'r_\\_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE E'r_\\_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE E'r_\\_view%'
 ORDER BY table_name, ordinal_position;

-- Read-only views
DELETE FROM ro_view1;
DELETE FROM ro_view2;
DELETE FROM ro_view3;
DELETE FROM ro_view4;
DELETE FROM ro_view5;
DELETE FROM ro_view6;
UPDATE ro_view7 SET a=a+1;
UPDATE ro_view8 SET a=a+1;
UPDATE ro_view9 SET a=a+1;
UPDATE ro_view10 SET a=a+1;
UPDATE ro_view11 SET a=a+1;
UPDATE ro_view12 SET a=a+1;
INSERT INTO ro_view13 VALUES (3, 'Row 3');
-- Partially updatable view
INSERT INTO rw_view14 VALUES (null, 3, 'Row 3'); -- should fail
INSERT INTO rw_view14 (a, b) VALUES (3, 'Row 3'); -- should be OK
UPDATE rw_view14 SET ctid=null WHERE a=3; -- should fail
UPDATE rw_view14 SET b='ROW 3' WHERE a=3; -- should be OK
SELECT * FROM base_tbl;
DELETE FROM rw_view14 WHERE a=3; -- should be OK
-- Partially updatable view
INSERT INTO rw_view15 VALUES (3, 'ROW 3'); -- should fail
INSERT INTO rw_view15 (a) VALUES (3); -- should be OK
SELECT * FROM rw_view15;
SELECT * FROM rw_view15;
SELECT * FROM rw_view15;
SELECT * FROM rw_view15;
SELECT * FROM rw_view15;
ALTER VIEW rw_view15 ALTER COLUMN upper SET DEFAULT 'NOT SET';
INSERT INTO rw_view15 (a) VALUES (4); -- should fail
UPDATE rw_view15 SET upper='ROW 3' WHERE a=3; -- should fail
UPDATE rw_view15 SET upper=DEFAULT WHERE a=3; -- should fail
UPDATE rw_view15 SET a=4 WHERE a=3; -- should be OK
SELECT * FROM base_tbl;
DELETE FROM rw_view15 WHERE a=4; -- should be OK
-- Partially updatable view
INSERT INTO rw_view16 VALUES (3, 'Row 3', 3); -- should fail
INSERT INTO rw_view16 (a, b) VALUES (3, 'Row 3'); -- should be OK
UPDATE rw_view16 SET a=3, aa=-3 WHERE a=3; -- should fail
UPDATE rw_view16 SET aa=-3 WHERE a=3; -- should be OK
SELECT * FROM base_tbl order by 1, 2;
DELETE FROM rw_view16 WHERE a=-3; -- should be OK
-- Read-only views
INSERT INTO ro_view17 VALUES (3, 'ROW 3');
DELETE FROM ro_view18;
UPDATE ro_view19 SET last_value=1000;
UPDATE ro_view20 SET b=upper(b);

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW ro_view10, ro_view12, ro_view18;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE uv_seq CASCADE;
--DDL_STATEMENT_END--

-- simple updatable view

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
--DDL_STATEMENT_END--
INSERT INTO base_tbl SELECT i, 'Row ' || i FROM generate_series(-2, 2) g(i);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a>0;
--DDL_STATEMENT_END--

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name = 'rw_view1';

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name = 'rw_view1';

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name = 'rw_view1'
 ORDER BY ordinal_position;

INSERT INTO rw_view1 VALUES (3, 'Row 3');
INSERT INTO rw_view1 (a) VALUES (4);
UPDATE rw_view1 SET a=5 WHERE a=4;
DELETE FROM rw_view1 WHERE b='Row 2';
SELECT * FROM base_tbl;

-- these two crashes kunlun
-- EXPLAIN (costs off) UPDATE rw_view1 SET a=6 WHERE a=5;
-- EXPLAIN (costs off) DELETE FROM rw_view1 WHERE a=5;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- view on top of view

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
--DDL_STATEMENT_END--
INSERT INTO base_tbl SELECT i, 'Row ' || i FROM generate_series(-2, 2) g(i);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT b AS bb, a AS aa FROM base_tbl WHERE a>0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS SELECT aa AS aaa, bb AS bbb FROM rw_view1 WHERE aa<10;
--DDL_STATEMENT_END--

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name = 'rw_view2';

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name = 'rw_view2';

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name = 'rw_view2'
 ORDER BY ordinal_position;

INSERT INTO rw_view2 VALUES (3, 'Row 3');
INSERT INTO rw_view2 (aaa) VALUES (4);
SELECT * FROM rw_view2;
UPDATE rw_view2 SET bbb='Row 4' WHERE aaa=4;
DELETE FROM rw_view2 WHERE aaa=2;
SELECT * FROM rw_view2;

-- EXPLAIN (costs off) UPDATE rw_view2 SET aaa=5 WHERE aaa=4;
-- EXPLAIN (costs off) DELETE FROM rw_view2 WHERE aaa=4;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- view on top of view with rules

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
--DDL_STATEMENT_END--
INSERT INTO base_tbl SELECT i, 'Row ' || i FROM generate_series(-2, 2) g(i);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a>0 OFFSET 0; -- not updatable without rules/triggers
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS SELECT * FROM rw_view1 WHERE a<10;
--DDL_STATEMENT_END--

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name, ordinal_position;

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name, ordinal_position;

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name, ordinal_position;

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name, ordinal_position;

INSERT INTO rw_view2 VALUES (3, 'Row 3') RETURNING *;
UPDATE rw_view2 SET b='Row three' WHERE a=3 RETURNING *;
SELECT * FROM rw_view2;
DELETE FROM rw_view2 WHERE a=3 RETURNING *;
SELECT * FROM rw_view2;

-- EXPLAIN (costs off) UPDATE rw_view2 SET a=3 WHERE a=2;
-- EXPLAIN (costs off) DELETE FROM rw_view2 WHERE a=2;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- view on top of view with triggers

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
--DDL_STATEMENT_END--
INSERT INTO base_tbl SELECT i, 'Row ' || i FROM generate_series(-2, 2) g(i);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a>0 OFFSET 0; -- not updatable without rules/triggers
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS SELECT * FROM rw_view1 WHERE a<10;
--DDL_STATEMENT_END--

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into,
       is_trigger_updatable, is_trigger_deletable,
       is_trigger_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name, ordinal_position;

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into,
       is_trigger_updatable, is_trigger_deletable,
       is_trigger_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name, ordinal_position;

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into,
       is_trigger_updatable, is_trigger_deletable,
       is_trigger_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name, ordinal_position;

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into,
       is_trigger_updatable, is_trigger_deletable,
       is_trigger_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name, ordinal_position;

INSERT INTO rw_view2 VALUES (3, 'Row 3') RETURNING *;
UPDATE rw_view2 SET b='Row three' WHERE a=3 RETURNING *;
SELECT * FROM rw_view2;
DELETE FROM rw_view2 WHERE a=3 RETURNING *;
SELECT * FROM rw_view2;

-- EXPLAIN (costs off) UPDATE rw_view2 SET a=3 WHERE a=2;
-- EXPLAIN (costs off) DELETE FROM rw_view2 WHERE a=2;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- update using whole row from view

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
--DDL_STATEMENT_END--
INSERT INTO base_tbl SELECT i, 'Row ' || i FROM generate_series(-2, 2) g(i);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT b AS bb, a AS aa FROM base_tbl;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE FUNCTION rw_view1_aa(x rw_view1)
  RETURNS int AS $$ SELECT x.aa $$ LANGUAGE sql;
--DDL_STATEMENT_END-- 

UPDATE rw_view1 v SET bb='Updated row 2' WHERE rw_view1_aa(v)=2
  RETURNING rw_view1_aa(v), v.bb;
SELECT * FROM base_tbl;

-- EXPLAIN (costs off)
-- UPDATE rw_view1 v SET bb='Updated row 2' WHERE rw_view1_aa(v)=2
--  RETURNING rw_view1_aa(v), v.bb;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- permissions checks

--DDL_STATEMENT_BEGIN--
CREATE USER regress_view_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_view_user2;
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_view_user1;
--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl(a int, b varchar(50), c float);
--DDL_STATEMENT_END--
INSERT INTO base_tbl VALUES (1, 'Row 1', 1.0);
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT b AS bb, c AS cc, a AS aa FROM base_tbl;
--DDL_STATEMENT_END--
INSERT INTO rw_view1 VALUES ('Row 2', 2.0, 2);

--DDL_STATEMENT_BEGIN--
GRANT SELECT ON base_tbl TO regress_view_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON rw_view1 TO regress_view_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE (a,c) ON base_tbl TO regress_view_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE (bb,cc) ON rw_view1 TO regress_view_user2;
--DDL_STATEMENT_END--
RESET SESSION AUTHORIZATION;

SET SESSION AUTHORIZATION regress_view_user2;
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS SELECT b AS bb, c AS cc, a AS aa FROM base_tbl;
--DDL_STATEMENT_END--
SELECT * FROM base_tbl; -- ok
SELECT * FROM rw_view1; -- ok
SELECT * FROM rw_view2; -- ok

INSERT INTO base_tbl VALUES (3, 'Row 3', 3.0); -- not allowed
INSERT INTO rw_view1 VALUES ('Row 3', 3.0, 3); -- not allowed
INSERT INTO rw_view2 VALUES ('Row 3', 3.0, 3); -- not allowed

UPDATE base_tbl SET a=a, c=c; -- ok
UPDATE base_tbl SET b=b; -- not allowed
UPDATE rw_view1 SET bb=bb, cc=cc; -- ok
UPDATE rw_view1 SET aa=aa; -- not allowed
UPDATE rw_view2 SET aa=aa, cc=cc; -- ok
UPDATE rw_view2 SET bb=bb; -- not allowed

DELETE FROM base_tbl; -- not allowed
DELETE FROM rw_view1; -- not allowed
DELETE FROM rw_view2; -- not allowed
RESET SESSION AUTHORIZATION;

SET SESSION AUTHORIZATION regress_view_user1;
--DDL_STATEMENT_BEGIN--
GRANT INSERT, DELETE ON base_tbl TO regress_view_user2;
--DDL_STATEMENT_END--
RESET SESSION AUTHORIZATION;

SET SESSION AUTHORIZATION regress_view_user2;
INSERT INTO base_tbl VALUES (3, 'Row 3', 3.0); -- ok
INSERT INTO rw_view1 VALUES ('Row 4', 4.0, 4); -- not allowed
INSERT INTO rw_view2 VALUES ('Row 4', 4.0, 4); -- ok
DELETE FROM base_tbl WHERE a=1; -- ok
DELETE FROM rw_view1 WHERE aa=2; -- not allowed
DELETE FROM rw_view2 WHERE aa=2; -- ok
SELECT * FROM base_tbl;
RESET SESSION AUTHORIZATION;

SET SESSION AUTHORIZATION regress_view_user1;
--DDL_STATEMENT_BEGIN--
REVOKE INSERT, DELETE ON base_tbl FROM regress_view_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT INSERT, DELETE ON rw_view1 TO regress_view_user2;
--DDL_STATEMENT_END--
RESET SESSION AUTHORIZATION;

SET SESSION AUTHORIZATION regress_view_user2;
INSERT INTO base_tbl VALUES (5, 'Row 5', 5.0); -- not allowed
INSERT INTO rw_view1 VALUES ('Row 5', 5.0, 5); -- ok
INSERT INTO rw_view2 VALUES ('Row 6', 6.0, 6); -- not allowed
DELETE FROM base_tbl WHERE a=3; -- not allowed
DELETE FROM rw_view1 WHERE aa=3; -- ok
DELETE FROM rw_view2 WHERE aa=4; -- not allowed
SELECT * FROM base_tbl;
RESET SESSION AUTHORIZATION;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- nested-view permissions

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl(a int, b varchar(50), c float);
--DDL_STATEMENT_END--
INSERT INTO base_tbl VALUES (1, 'Row 1', 1.0);

SET SESSION AUTHORIZATION regress_view_user1;
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl;
--DDL_STATEMENT_END--
--SELECT * FROM rw_view1;  -- not allowed
--SELECT * FROM rw_view1 FOR UPDATE;  -- not allowed
UPDATE rw_view1 SET b = 'foo' WHERE a = 1;  -- not allowed

SET SESSION AUTHORIZATION regress_view_user2;
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS SELECT * FROM rw_view1;
--DDL_STATEMENT_END--
SELECT * FROM rw_view2;  -- not allowed
--SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- not allowed

RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON base_tbl TO regress_view_user1;
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_view_user1;
SELECT * FROM rw_view1;
--SELECT * FROM rw_view1 FOR UPDATE;  -- not allowed
UPDATE rw_view1 SET b = 'foo' WHERE a = 1;  -- not allowed

SET SESSION AUTHORIZATION regress_view_user2;
SELECT * FROM rw_view2;  -- not allowed
--SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- not allowed

SET SESSION AUTHORIZATION regress_view_user1;
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON rw_view1 TO regress_view_user2;
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_view_user2;
SELECT * FROM rw_view2;
--SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- not allowed

RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON base_tbl TO regress_view_user1;
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_view_user1;
SELECT * FROM rw_view1;
--SELECT * FROM rw_view1 FOR UPDATE;
UPDATE rw_view1 SET b = 'foo' WHERE a = 1;

SET SESSION AUTHORIZATION regress_view_user2;
SELECT * FROM rw_view2;
-- SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- not allowed

SET SESSION AUTHORIZATION regress_view_user1;
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON rw_view1 TO regress_view_user2;
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_view_user2;
SELECT * FROM rw_view2;
-- SELECT * FROM rw_view2 FOR UPDATE;
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;

RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
REVOKE UPDATE ON base_tbl FROM regress_view_user1;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_view_user1;
SELECT * FROM rw_view1;
--SELECT * FROM rw_view1 FOR UPDATE;  -- not allowed
UPDATE rw_view1 SET b = 'foo' WHERE a = 1;  -- not allowed

SET SESSION AUTHORIZATION regress_view_user2;
SELECT * FROM rw_view2;
--SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- not allowed

RESET SESSION AUTHORIZATION;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP USER regress_view_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_view_user2;
--DDL_STATEMENT_END--

-- column defaults

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified', c serial);
--DDL_STATEMENT_END--
INSERT INTO base_tbl VALUES (1, 'Row 1');
INSERT INTO base_tbl VALUES (2, 'Row 2');
INSERT INTO base_tbl VALUES (3);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT a AS aa, b AS bb FROM base_tbl;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER VIEW rw_view1 ALTER COLUMN bb SET DEFAULT 'View default';
--DDL_STATEMENT_END--

INSERT INTO rw_view1 VALUES (4, 'Row 4');
INSERT INTO rw_view1 (aa) VALUES (5);

SELECT * FROM base_tbl;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- Table having triggers
--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int PRIMARY KEY, b varchar(50) DEFAULT 'Unspecified');
--DDL_STATEMENT_END--
INSERT INTO base_tbl VALUES (1, 'Row 1');
INSERT INTO base_tbl VALUES (2, 'Row 2');

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT a AS aa, b AS bb FROM base_tbl;
--DDL_STATEMENT_END--

INSERT INTO rw_view1 VALUES (3, 'Row 3');
select * from base_tbl;

--DDL_STATEMENT_BEGIN--
DROP VIEW rw_view1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl;
--DDL_STATEMENT_END--

-- view with ORDER BY

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int, b int);
--DDL_STATEMENT_END--
INSERT INTO base_tbl VALUES (1,2), (4,5), (3,-3);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl ORDER BY a+b;
--DDL_STATEMENT_END--

SELECT * FROM rw_view1;

INSERT INTO rw_view1 VALUES (7,-8);
SELECT * FROM rw_view1;

-- EXPLAIN (verbose, costs off) UPDATE rw_view1 SET b = b + 1 RETURNING *;
UPDATE rw_view1 SET b = b + 1 RETURNING *;
SELECT * FROM rw_view1;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- views with updatable and non-updatable columns
--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl(a float);
--DDL_STATEMENT_END--
INSERT INTO base_tbl SELECT i/10.0 FROM generate_series(1,10) g(i);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS
  SELECT sin(a) s, a, cos(a) c
  FROM base_tbl
  WHERE a != 0
  ORDER BY abs(a);
--DDL_STATEMENT_END--

INSERT INTO rw_view1 VALUES (null, null, 1.1, null); -- should fail
INSERT INTO rw_view1 (s, c, a) VALUES (null, null, 1.1); -- should fail
INSERT INTO rw_view1 (a) VALUES (1.1) RETURNING a, s, c; -- OK
UPDATE rw_view1 SET s = s WHERE a = 1.1; -- should fail
UPDATE rw_view1 SET a = 1.05 WHERE a = 1.1 RETURNING s; -- OK
DELETE FROM rw_view1 WHERE a = 1.05; -- OK

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS
  SELECT s, c, s/c t, a base_a
  FROM rw_view1;
--DDL_STATEMENT_END--

INSERT INTO rw_view2 VALUES (null, null, null, 1.1, null); -- should fail
INSERT INTO rw_view2(s, c, base_a) VALUES (null, null, 1.1); -- should fail
INSERT INTO rw_view2(base_a) VALUES (1.1) RETURNING t; -- OK
UPDATE rw_view2 SET s = s WHERE base_a = 1.1; -- should fail
UPDATE rw_view2 SET t = t WHERE base_a = 1.1; -- should fail
UPDATE rw_view2 SET base_a = 1.05 WHERE base_a = 1.1; -- OK
DELETE FROM rw_view2 WHERE base_a = 1.05 RETURNING base_a, s, c, t; -- OK

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view3 AS
  SELECT s, c, s/c t
  FROM rw_view1;
--DDL_STATEMENT_END--

INSERT INTO rw_view3 VALUES (null, null, null, null); -- should fail
INSERT INTO rw_view3(s) VALUES (null); -- should fail
UPDATE rw_view3 SET s = s; -- should fail
DELETE FROM rw_view3 WHERE s = sin(0.1); -- should be OK
SELECT * FROM base_tbl ORDER BY a;

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name LIKE E'r_\\_view%'
 ORDER BY table_name;

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name LIKE E'r_\\_view%'
 ORDER BY table_name;

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name LIKE E'r_\\_view%'
 ORDER BY table_name, ordinal_position;

SELECT events & 4 != 0 AS upd,
       events & 8 != 0 AS ins,
       events & 16 != 0 AS del
  FROM pg_catalog.pg_relation_is_updatable('rw_view3'::regclass, false) t(events);
  
--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int, b int DEFAULT 10);
--DDL_STATEMENT_END--
INSERT INTO base_tbl VALUES (1,2), (2,3), (1,-1);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a < b;
--DDL_STATEMENT_END-- 

\d+ rw_view1
SELECT * FROM information_schema.views WHERE table_name = 'rw_view1';

INSERT INTO rw_view1 VALUES(3,4); -- ok
INSERT INTO rw_view1 VALUES(4,3); -- should fail
INSERT INTO rw_view1 VALUES(5,null); -- should fail
UPDATE rw_view1 SET b = 5 WHERE a = 3; -- ok
UPDATE rw_view1 SET b = -5 WHERE a = 3; -- should fail
INSERT INTO rw_view1(a) VALUES (9); -- ok
INSERT INTO rw_view1(a) VALUES (10); -- should fail
SELECT * FROM base_tbl order by 1,2;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a > 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS SELECT * FROM rw_view1 WHERE a < 10;
--DDL_STATEMENT_END--
\d+ rw_view2
SELECT * FROM information_schema.views WHERE table_name = 'rw_view2';

INSERT INTO rw_view2 VALUES (-5); -- should fail
INSERT INTO rw_view2 VALUES (5); -- ok
INSERT INTO rw_view2 VALUES (15); -- should fail
SELECT * FROM base_tbl;

UPDATE rw_view2 SET a = a - 10; -- should fail
UPDATE rw_view2 SET a = a + 10; -- should fail

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW rw_view2 AS SELECT * FROM rw_view1 WHERE a < 10;
--DDL_STATEMENT_END--
\d+ rw_view2
SELECT * FROM information_schema.views WHERE table_name = 'rw_view2';

INSERT INTO rw_view2 VALUES (-10); -- ok, but not in view
INSERT INTO rw_view2 VALUES (20); -- should fail
SELECT * FROM base_tbl order by 1;

-- ALTER VIEW rw_view1 SET (check_option=here); -- invalid
-- ALTER VIEW rw_view1 SET (check_option=local);

INSERT INTO rw_view2 VALUES (-20); -- should fail
INSERT INTO rw_view2 VALUES (30); -- should fail

-- ALTER VIEW rw_view2 RESET (check_option);
\d+ rw_view2
SELECT * FROM information_schema.views WHERE table_name = 'rw_view2';
INSERT INTO rw_view2 VALUES (30); -- ok, but not in view
SELECT * FROM base_tbl order by 1;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS SELECT * FROM rw_view1 WHERE a > 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view3 AS SELECT * FROM rw_view2;
--DDL_STATEMENT_END--
SELECT * FROM information_schema.views WHERE table_name LIKE E'rw\\_view_' ORDER BY table_name;

INSERT INTO rw_view1 VALUES (-1); -- ok
INSERT INTO rw_view1 VALUES (1); -- ok
INSERT INTO rw_view2 VALUES (-2); -- ok, but not in view
INSERT INTO rw_view2 VALUES (2); -- ok
INSERT INTO rw_view3 VALUES (-3); -- should fail
INSERT INTO rw_view3 VALUES (3); -- ok

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

--CREATE TABLE base_tbl (a int);
--CREATE TABLE ref_tbl (a int PRIMARY KEY);
--INSERT INTO ref_tbl SELECT * FROM generate_series(1,10);

--CREATE VIEW rw_view1 AS
--  SELECT * FROM base_tbl b
--  WHERE EXISTS(SELECT 1 FROM ref_tbl r WHERE r.a = b.a);

--INSERT INTO rw_view1 VALUES (5); -- ok
--INSERT INTO rw_view1 VALUES (15); -- should fail

--UPDATE rw_view1 SET a = a + 5; -- ok
--UPDATE rw_view1 SET a = a + 5; -- should fail

-- EXPLAIN (costs off) INSERT INTO rw_view1 VALUES (5);
-- EXPLAIN (costs off) UPDATE rw_view1 SET a = a + 5;

--DROP TABLE base_tbl CASCADE;
--DROP TABLE ref_tbl CASCADE;

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int, b int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a < b;
--DDL_STATEMENT_END--

INSERT INTO rw_view1 VALUES (5,0); -- ok
INSERT INTO rw_view1 VALUES (15, 20); -- should fail
UPDATE rw_view1 SET a = 20, b = 30; -- should fail

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int, b int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT a FROM base_tbl WHERE a < b;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS
  SELECT * FROM rw_view1 WHERE a > 0;
--DDL_STATEMENT_END--

INSERT INTO rw_view2 VALUES (-5); -- should fail
INSERT INTO rw_view2 VALUES (5); -- ok
INSERT INTO rw_view2 VALUES (50); -- ok, but not in view
UPDATE rw_view2 SET a = a - 10; -- should fail
SELECT * FROM base_tbl;

-- Check option won't cascade down to base view with INSTEAD OF triggers

-- ALTER VIEW rw_view2 SET (check_option=cascaded);
INSERT INTO rw_view2 VALUES (100); -- ok, but not in view (doesn't fail rw_view1's check)
UPDATE rw_view2 SET a = 200 WHERE a = 5; -- ok, but not in view (doesn't fail rw_view1's check)
SELECT * FROM base_tbl;

-- Neither local nor cascaded check options work with INSTEAD rules

INSERT INTO rw_view2 VALUES (-10); -- ok, but not in view (doesn't fail rw_view2's check)
INSERT INTO rw_view2 VALUES (5); -- ok
INSERT INTO rw_view2 VALUES (20); -- ok, but not in view (doesn't fail rw_view1's check)
UPDATE rw_view2 SET a = 30 WHERE a = 5; -- ok, but not in view (doesn't fail rw_view1's check)
INSERT INTO rw_view2 VALUES (5); -- ok
UPDATE rw_view2 SET a = -5 WHERE a = 5; -- ok, but not in view (doesn't fail rw_view2's check)
SELECT * FROM base_tbl;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS SELECT a,10 AS b FROM base_tbl;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 AS
  SELECT * FROM rw_view1 WHERE a > b;
INSERT INTO rw_view2 VALUES (2,3); -- ok, but not in view (doesn't fail rw_view2's check)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- security barrier view

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl (person varchar(50), visibility varchar(50));
--DDL_STATEMENT_END--
INSERT INTO base_tbl VALUES ('Tom', 'public'),
                            ('Dick', 'private'),
                            ('Harry', 'public');
							
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 AS
  SELECT person FROM base_tbl WHERE visibility = 'public';
  
CREATE FUNCTION snoop(anyelement)
RETURNS boolean AS
$$
BEGIN
  RAISE NOTICE 'snooped value: %', $1;
  RETURN true;
END;
$$
LANGUAGE plpgsql COST 0.000001;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION leakproof(anyelement)
RETURNS boolean AS
$$
BEGIN
  RETURN true;
END;
$$
LANGUAGE plpgsql STRICT IMMUTABLE LEAKPROOF;
--DDL_STATEMENT_END--

SELECT * FROM rw_view1 WHERE snoop(person);
-- delete/update不支持用户自定义函数，或者mysql不支持的函数 #777
--UPDATE rw_view1 SET person=person WHERE snoop(person);
--DELETE FROM rw_view1 WHERE NOT snoop(person);

-- ALTER VIEW rw_view1 SET (security_barrier = true);

SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name = 'rw_view1';

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name = 'rw_view1';

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name = 'rw_view1'
 ORDER BY ordinal_position;

SELECT * FROM rw_view1 WHERE snoop(person);
--UPDATE rw_view1 SET person=person WHERE snoop(person);
--DELETE FROM rw_view1 WHERE NOT snoop(person);

-- EXPLAIN (costs off) SELECT * FROM rw_view1 WHERE snoop(person);
-- EXPLAIN (costs off) UPDATE rw_view1 SET person=person WHERE snoop(person);
-- EXPLAIN (costs off) DELETE FROM rw_view1 WHERE NOT snoop(person);
-- security barrier view on top of security barrier view
--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view2 WITH (security_barrier = true) AS
  SELECT * FROM rw_view1 WHERE snoop(person);
  
--DDL_STATEMENT_END--
SELECT table_name, is_insertable_into
  FROM information_schema."tables"
 WHERE table_name = 'rw_view2';

SELECT table_name, is_updatable, is_insertable_into
  FROM information_schema.views
 WHERE table_name = 'rw_view2';

SELECT table_name, column_name, is_updatable
  FROM information_schema.columns
 WHERE table_name = 'rw_view2'
 ORDER BY ordinal_position;

SELECT * FROM rw_view2 WHERE snoop(person);
--UPDATE rw_view2 SET person=person WHERE snoop(person);
--DELETE FROM rw_view2 WHERE NOT snoop(person);

-- EXPLAIN (costs off) SELECT * FROM rw_view2 WHERE snoop(person);
-- EXPLAIN (costs off) UPDATE rw_view2 SET person=person WHERE snoop(person);
-- EXPLAIN (costs off) DELETE FROM rw_view2 WHERE NOT snoop(person);

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

-- security barrier view on top of table with rules

--DDL_STATEMENT_BEGIN--
CREATE TABLE base_tbl(id int PRIMARY KEY, data varchar(50), deleted boolean);
--DDL_STATEMENT_END--
INSERT INTO base_tbl VALUES (1, 'Row 1', false), (2, 'Row 2', true);

--DDL_STATEMENT_BEGIN--
CREATE VIEW rw_view1 WITH (security_barrier=true) AS
  SELECT id, data FROM base_tbl WHERE NOT deleted;
--DDL_STATEMENT_END--

SELECT * FROM rw_view1;

-- EXPLAIN (costs off) DELETE FROM rw_view1 WHERE id = 1 AND snoop(data);
--DELETE FROM rw_view1 WHERE id = 1 AND snoop(data);

-- EXPLAIN (costs off) INSERT INTO rw_view1 VALUES (2, 'New row 2');
INSERT INTO rw_view1 VALUES (2, 'New row 2');

SELECT * FROM base_tbl;

--DDL_STATEMENT_BEGIN--
DROP TABLE base_tbl CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE tx1 (a integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tx2 (b integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tx3 (c integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW vx1 AS SELECT a FROM tx1 WHERE EXISTS(SELECT 1 FROM tx2 JOIN tx3 ON b=c);
--DDL_STATEMENT_END--
INSERT INTO vx1 values (1);
SELECT * FROM tx1;
SELECT * FROM vx1;

--DDL_STATEMENT_BEGIN--
DROP VIEW vx1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx3;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE tx1 (a integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tx2 (b integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tx3 (c integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW vx1 AS SELECT a FROM tx1 WHERE EXISTS(SELECT 1 FROM tx2 JOIN tx3 ON b=c);
--DDL_STATEMENT_END--
INSERT INTO vx1 VALUES (1);
INSERT INTO vx1 VALUES (1);
SELECT * FROM tx1;
SELECT * FROM vx1;

--DDL_STATEMENT_BEGIN--
DROP VIEW vx1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx3;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE tx1 (a integer, b integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tx2 (b integer, c integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tx3 (c integer, d integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tx1 DROP COLUMN b;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tx2 DROP COLUMN c;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE tx3 DROP COLUMN d;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW vx1 AS SELECT a FROM tx1 WHERE EXISTS(SELECT 1 FROM tx2 JOIN tx3 ON b=c);
--DDL_STATEMENT_END--
INSERT INTO vx1 VALUES (1);
INSERT INTO vx1 VALUES (1);
SELECT * FROM tx1;
SELECT * FROM vx1;

--DDL_STATEMENT_BEGIN--
DROP VIEW vx1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE tx3;
--DDL_STATEMENT_END--

--
-- Test CREATE OR REPLACE VIEW turning a non-updatable view into an
-- auto-updatable view and adding check options in a single step
--
drop table if exists t1 cascade;
--DDL_STATEMENT_BEGIN--
CREATE TABLE t1 (a int, b varchar(50));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW v1 AS SELECT null::int AS a;
--DDL_STATEMENT_END--
-- CREATE OR REPLACE VIEW v1 AS SELECT * FROM t1 WHERE a > 0;
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE VIEW v1 AS SELECT * FROM t1 WHERE a > 0;
--DDL_STATEMENT_END--

INSERT INTO v1 VALUES (1, 'ok'); -- ok
INSERT INTO v1 VALUES (-1, 'invalid'); -- should fail

--DDL_STATEMENT_BEGIN--
DROP VIEW v1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE t1;
--DDL_STATEMENT_END--
UPDATE rw_view2 SET aa=aa, cc=cc; 
-- check that an auto-updatable view on a partitioned table works correctly
--DDL_STATEMENT_BEGIN--
drop table if exists uv_pt;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table uv_pt (a int, b int, v varchar) partition by range (a, b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table uv_pt1 partition of uv_pt for values from (1, 2) to (1, 10) partition by range (b);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table uv_pt11 partition of uv_pt1 for values from (2) to (5);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create view uv_ptv as select * from uv_pt;
--DDL_STATEMENT_END--
select events & 4 != 0 AS upd,
       events & 8 != 0 AS ins,
       events & 16 != 0 AS del
  from pg_catalog.pg_relation_is_updatable('uv_pt'::regclass, false) t(events);
select pg_catalog.pg_column_is_updatable('uv_pt'::regclass, 1::smallint, false);
select pg_catalog.pg_column_is_updatable('uv_pt'::regclass, 2::smallint, false);
select table_name, is_updatable, is_insertable_into
  from information_schema.views where table_name = 'uv_ptv';
select table_name, column_name, is_updatable
  from information_schema.columns where table_name = 'uv_ptv' order by column_name;
insert into uv_ptv values (1, 2);
--select tableoid::regclass, * from uv_pt;
--DDL_STATEMENT_BEGIN--
create view uv_ptv_wco as select * from uv_pt where a = 0;
--DDL_STATEMENT_END--
insert into uv_ptv_wco values (1, 2);
--DDL_STATEMENT_BEGIN--
drop view uv_ptv, uv_ptv_wco;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table uv_pt;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table uv_pt1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table uv_pt11;
--DDL_STATEMENT_END--

-- work fine with partitioned tables
--DDL_STATEMENT_BEGIN--
drop table if exists wcowrtest cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table wcowrtest (a int) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table wcowrtest1 partition of wcowrtest for values in (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view wcowrtest_v as select * from wcowrtest where wcowrtest = '(2)'::wcowrtest;
--DDL_STATEMENT_END--
insert into wcowrtest_v values (1);

--DDL_STATEMENT_BEGIN--
alter table wcowrtest add b varchar(50);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table wcowrtest2 partition of wcowrtest for values in (2);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists sometable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table sometable (a int, b varchar(50));
--DDL_STATEMENT_END--
insert into sometable values (1, 'a'), (2, 'b');
--DDL_STATEMENT_BEGIN--
create view wcowrtest_v2 as
    select *
      from wcowrtest r
      where r in (select s from sometable s where r.a = s.a);
--DDL_STATEMENT_END--

-- WITH CHECK qual will be processed with wcowrtest2's
-- rowtype after tuple-routing
insert into wcowrtest_v2 values (2, 'no such row in sometable');

--DDL_STATEMENT_BEGIN--
drop view wcowrtest_v, wcowrtest_v2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table wcowrtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table sometable;
--DDL_STATEMENT_END--

-- Check INSERT .. ON CONFLICT DO UPDATE works correctly when the view's
-- columns are named and ordered differently than the underlying table's.
--DDL_STATEMENT_BEGIN--
drop table if exists uv_iocu_tab;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table uv_iocu_tab (a varchar(50) unique, b float);
--DDL_STATEMENT_END--
insert into uv_iocu_tab values ('xyxyxy', 0);
--DDL_STATEMENT_BEGIN--
create view uv_iocu_view as
   select b, b+1 as c, a, '2.0'::varchar(50) as two from uv_iocu_tab;
--DDL_STATEMENT_END--

select * from uv_iocu_tab;
select * from uv_iocu_tab;

-- OK to access view columns that are not present in underlying base
-- relation in the ON CONFLICT portion of the query
select * from uv_iocu_tab;
select * from uv_iocu_tab;

--DDL_STATEMENT_BEGIN--
drop view uv_iocu_view;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table uv_iocu_tab;
--DDL_STATEMENT_END--

-- Test whole-row references to the view
--DDL_STATEMENT_BEGIN--
create table uv_iocu_tab (a int unique, b varchar(50));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view uv_iocu_view as
    select a as aa, b as bb, uv_iocu_tab::varchar(50) as cc from uv_iocu_tab;
--DDL_STATEMENT_END--

insert into uv_iocu_view (aa,bb) values (1,'x');
select * from uv_iocu_view;

-- Test omitting a column of the base relation
delete from uv_iocu_view;
insert into uv_iocu_view (aa,bb) values (1,'x');
select * from uv_iocu_view;

--DDL_STATEMENT_BEGIN--
alter table uv_iocu_tab alter column b set default 'table default';
--DDL_STATEMENT_END--
select * from uv_iocu_view;

--DDL_STATEMENT_BEGIN--
alter view uv_iocu_view alter column bb set default 'view default';
--DDL_STATEMENT_END--
select * from uv_iocu_view;

--DDL_STATEMENT_BEGIN--
drop view uv_iocu_view;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table uv_iocu_tab;
--DDL_STATEMENT_END--

-- ON CONFLICT DO UPDATE permissions checks
--DDL_STATEMENT_BEGIN--
create user regress_view_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create user regress_view_user2;
--DDL_STATEMENT_END--

set session authorization regress_view_user1;
--DDL_STATEMENT_BEGIN--
drop table if exists base_tbl;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table base_tbl(a int unique, b varchar(50), c float);
--DDL_STATEMENT_END--
insert into base_tbl values (1,'xxx',1.0);
--DDL_STATEMENT_BEGIN--
create view rw_view1 as select b as bb, c as cc, a as aa from base_tbl;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
grant select (aa,bb) on rw_view1 to regress_view_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant insert on rw_view1 to regress_view_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant update (bb) on rw_view1 to regress_view_user2;
--DDL_STATEMENT_END--

set session authorization regress_view_user2;
reset session authorization;
select * from base_tbl;

set session authorization regress_view_user1;
--DDL_STATEMENT_BEGIN--
grant select (a,b) on base_tbl to regress_view_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant insert (a,b) on base_tbl to regress_view_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant update (a,b) on base_tbl to regress_view_user2;
--DDL_STATEMENT_END--

set session authorization regress_view_user2;
--DDL_STATEMENT_BEGIN--
create view rw_view2 as select b as bb, c as cc, a as aa from base_tbl;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view rw_view3 as select b as bb, a as aa from base_tbl;
--DDL_STATEMENT_END--
reset session authorization;
select * from base_tbl;

set session authorization regress_view_user2;
--DDL_STATEMENT_BEGIN--
create view rw_view4 as select aa, bb, cc FROM rw_view1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view rw_view5 as select aa, bb FROM rw_view1;
--DDL_STATEMENT_END--
reset session authorization;
select * from base_tbl;

--DDL_STATEMENT_BEGIN--
drop view rw_view5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop view rw_view4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop view rw_view3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop view rw_view2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop view rw_view1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table base_tbl;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop user regress_view_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop user regress_view_user2;
--DDL_STATEMENT_END--

-- Test single- and multi-row inserts with table and view defaults.
-- Table defaults should be used, unless overridden by view defaults.
--DDL_STATEMENT_BEGIN--
drop table if exists base_tab_def;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table base_tab_def (a int, b varchar(50) default 'Table default',
                           c varchar(50) default 'Table default', d varchar(50), e varchar(50));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create view base_tab_def_view as select * from base_tab_def;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter view base_tab_def_view alter b set default 'View default';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter view base_tab_def_view alter d set default 'View default';
--DDL_STATEMENT_END--
insert into base_tab_def values (1);
insert into base_tab_def values (2), (3);
insert into base_tab_def values (4, default, default, default, default);
insert into base_tab_def values (5, default, default, default, default),
                                (6, default, default, default, default);
insert into base_tab_def_view values (11);
insert into base_tab_def_view values (12), (13);
insert into base_tab_def_view values (14, default, default, default, default);
insert into base_tab_def_view values (15, default, default, default, default),
                                     (16, default, default, default, default);
insert into base_tab_def_view values (17), (default);
select * from base_tab_def order by a;

delete from base_tab_def;
insert into base_tab_def values (1);
insert into base_tab_def values (2), (3);
insert into base_tab_def values (4, default, default, default, default);
insert into base_tab_def values (5, default, default, default, default),
                                (6, default, default, default, default);
insert into base_tab_def_view values (11);
insert into base_tab_def_view values (12), (13);
insert into base_tab_def_view values (14, default, default, default, default);
insert into base_tab_def_view values (15, default, default, default, default),
                                     (16, default, default, default, default);
insert into base_tab_def_view values (17), (default);
select * from base_tab_def order by a;

-- Using an unconditional DO INSTEAD rule should also cause NULLs to be
-- inserted where there are no view defaults.
--DDL_STATEMENT_BEGIN--
drop function base_tab_def_view_instrig_func;
--DDL_STATEMENT_END--
delete from base_tab_def;
insert into base_tab_def values (1);
insert into base_tab_def values (2), (3);
insert into base_tab_def values (4, default, default, default, default);
insert into base_tab_def values (5, default, default, default, default),
                                (6, default, default, default, default);
insert into base_tab_def_view values (11);
insert into base_tab_def_view values (12), (13);
insert into base_tab_def_view values (14, default, default, default, default);
insert into base_tab_def_view values (15, default, default, default, default),
                                     (16, default, default, default, default);
insert into base_tab_def_view values (17), (default);
select * from base_tab_def order by a;

-- A DO ALSO rule should cause each row to be inserted twice. The first
-- insert should behave the same as an auto-updatable view (using table
-- defaults, unless overridden by view defaults). The second insert should
-- behave the same as a rule-updatable view (inserting NULLs where there are
-- no view defaults).
delete from base_tab_def;
insert into base_tab_def values (1);
insert into base_tab_def values (2), (3);
insert into base_tab_def values (4, default, default, default, default);
insert into base_tab_def values (5, default, default, default, default),
                                (6, default, default, default, default);
insert into base_tab_def_view values (11);
insert into base_tab_def_view values (12), (13);
insert into base_tab_def_view values (14, default, default, default, default);
insert into base_tab_def_view values (15, default, default, default, default),
                                     (16, default, default, default, default);
insert into base_tab_def_view values (17), (default);
select * from base_tab_def order by a, c NULLS LAST;

--DDL_STATEMENT_BEGIN--
drop view base_tab_def_view;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table base_tab_def;
--DDL_STATEMENT_END--
