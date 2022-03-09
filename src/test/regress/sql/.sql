--
-- POINT
--
-- Test that GiST indexes provide same behavior as sequential scan
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE point_gist_tbl(f1 point);
--DDL_STATEMENT_END--
INSERT INTO point_gist_tbl SELECT '(0,0)' FROM generate_series(0,1000);
--DDL_STATEMENT_BEGIN--
CREATE INDEX point_gist_tbl_index ON point_gist_tbl USING gist (f1);
--DDL_STATEMENT_END--
INSERT INTO point_gist_tbl VALUES ('(0.0000009,0.0000009)');
SET enable_seqscan TO true;
SET enable_indexscan TO false;
SET enable_bitmapscan TO false;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000009,0.0000009)'::point;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 <@ '(0.0000009,0.0000009),(0.0000009,0.0000009)'::box;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000018,0.0000018)'::point;
SET enable_seqscan TO false;
SET enable_indexscan TO true;
SET enable_bitmapscan TO true;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000009,0.0000009)'::point;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 <@ '(0.0000009,0.0000009),(0.0000009,0.0000009)'::box;
SELECT COUNT(*) FROM point_gist_tbl WHERE f1 ~= '(0.0000018,0.0000018)'::point;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
--DDL_STATEMENT_BEGIN--
DROP TABLE point_gist_tbl;
--DDL_STATEMENT_END--