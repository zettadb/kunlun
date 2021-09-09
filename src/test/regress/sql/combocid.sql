--
-- Tests for some likely failure cases with combo cmin/cmax mechanism
--
CREATE TEMP TABLE combocidtest (foobar int);

BEGIN;

-- a few dummy ops to push up the CommandId counter
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;

INSERT INTO combocidtest VALUES (1);
INSERT INTO combocidtest VALUES (2);

SELECT * FROM combocidtest;

SAVEPOINT s1;

UPDATE combocidtest SET foobar = foobar + 10;

-- here we should see only updated tuples
SELECT * FROM combocidtest;

ROLLBACK TO s1;

-- now we should see old tuples, but with combo CIDs starting at 0
SELECT * FROM combocidtest;

COMMIT;

-- combo data is not there anymore, but should still see tuples
SELECT * FROM combocidtest;

-- Test combo cids with portals
BEGIN;

INSERT INTO combocidtest VALUES (333);

ROLLBACK;

SELECT * FROM combocidtest;

-- check behavior with locked tuples
BEGIN;

-- a few dummy ops to push up the CommandId counter
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;
INSERT INTO combocidtest SELECT 1 LIMIT 0;

INSERT INTO combocidtest VALUES (444);

SELECT * FROM combocidtest;

SAVEPOINT s1;

-- this doesn't affect cmin
SELECT * FROM combocidtest FOR UPDATE;
SELECT * FROM combocidtest;

-- but this does
UPDATE combocidtest SET foobar = foobar + 10;

SELECT * FROM combocidtest;

ROLLBACK TO s1;

SELECT * FROM combocidtest;

COMMIT;

SELECT * FROM combocidtest;

-- test for bug reported in
-- CABRT9RC81YUf1=jsmWopcKJEro=VoeG2ou6sPwyOUTx_qteRsg@mail.gmail.com
CREATE TABLE IF NOT EXISTS testcase(
	id int PRIMARY KEY,
	balance numeric
);
INSERT INTO testcase VALUES (1, 0);
BEGIN;
-- syntax error for kunlun: SELECT * FROM testcase WHERE testcase.id = 1 FOR UPDATE;
UPDATE testcase SET balance = balance + 400 WHERE id=1;
SAVEPOINT subxact;
UPDATE testcase SET balance = balance - 100 WHERE id=1;
ROLLBACK TO SAVEPOINT subxact;
-- should return one tuple
-- syntax error for kunlun: SELECT * FROM testcase WHERE id = 1 FOR UPDATE;
ROLLBACK;
DROP TABLE testcase;
