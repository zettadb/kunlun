--
-- CREATE SEQUENCE
--

-- various error cases
--DDL_STATEMENT_BEGIN--
CREATE UNLOGGED SEQUENCE sequence_testx;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx INCREMENT BY 0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx INCREMENT BY -1 MINVALUE 20;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx INCREMENT BY 1 MAXVALUE -20;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx INCREMENT BY -1 START 10;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx INCREMENT BY 1 START -10;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx CACHE 0;
--DDL_STATEMENT_END--
-- OWNED BY errors
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx OWNED BY nobody;  -- nonsense word
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx OWNED BY pg_class_oid_index.oid;  -- not a table
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx OWNED BY pg_class.relname;  -- not same schema
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE sequence_test_table (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx OWNED BY sequence_test_table.b;  -- wrong column
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE sequence_test_table;
--DDL_STATEMENT_END--
-- sequence data types
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test5 AS integer;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--@
CREATE SEQUENCE sequence_test6 AS smallint;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test7 AS bigint;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test8 AS integer MAXVALUE 100000;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test9 AS integer INCREMENT BY -1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test10 AS integer MINVALUE -100000 START 1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test11 AS smallint;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test12 AS smallint INCREMENT -1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test13 AS smallint MINVALUE -32768;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test14 AS smallint MAXVALUE 32767 INCREMENT -1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx AS text;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx AS nosuchtype;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx AS smallint MAXVALUE 100000;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_testx AS smallint MINVALUE -100000;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test5 AS smallint;  -- success, max will be adjusted
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test8 AS smallint;  -- fail, max has to be adjusted
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test8 AS smallint MAXVALUE 20000;  -- ok now
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test9 AS smallint;  -- success, min will be adjusted
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test10 AS smallint;  -- fail, min has to be adjusted
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test10 AS smallint MINVALUE -20000;  -- ok now
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test11 AS int;  -- max will be adjusted
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test12 AS int;  -- min will be adjusted
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test13 AS int;  -- min and max will be adjusted
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test14 AS int;  -- min and max will be adjusted
--DDL_STATEMENT_END--
---
--- test creation of SERIAL column
---
--DDL_STATEMENT_BEGIN--
CREATE TABLE serialTest1 (f1 text, f2 serial);
--DDL_STATEMENT_END--
INSERT INTO serialTest1 VALUES ('foo');
INSERT INTO serialTest1 VALUES ('bar');
INSERT INTO serialTest1 VALUES ('force', 100);
INSERT INTO serialTest1 VALUES ('wrong', NULL);

SELECT * FROM serialTest1;

SELECT pg_get_serial_sequence('serialTest1', 'f2');

-- test smallserial / bigserial
--DDL_STATEMENT_BEGIN--
CREATE TABLE serialTest2 (f1 text, f2 serial, f3 smallserial, f4 serial2,
  f5 bigserial, f6 serial8);
--DDL_STATEMENT_END--
INSERT INTO serialTest2 (f1)
  VALUES ('test_defaults');

INSERT INTO serialTest2 (f1, f2, f3, f4, f5, f6)
  VALUES ('test_max_vals', 2147483647, 32767, 32767, 9223372036854775807,
          9223372036854775807),
         ('test_min_vals', -2147483648, -32768, -32768, -9223372036854775808,
          -9223372036854775808);

-- All these INSERTs should fail:
INSERT INTO serialTest2 (f1, f3)
  VALUES ('bogus', -32769);

INSERT INTO serialTest2 (f1, f4)
  VALUES ('bogus', -32769);

INSERT INTO serialTest2 (f1, f3)
  VALUES ('bogus', 32768);

INSERT INTO serialTest2 (f1, f4)
  VALUES ('bogus', 32768);

INSERT INTO serialTest2 (f1, f5)
  VALUES ('bogus', -9223372036854775809);

INSERT INTO serialTest2 (f1, f6)
  VALUES ('bogus', -9223372036854775809);

INSERT INTO serialTest2 (f1, f5)
  VALUES ('bogus', 9223372036854775808);

INSERT INTO serialTest2 (f1, f6)
  VALUES ('bogus', 9223372036854775808);

SELECT * FROM serialTest2 ORDER BY f2 ASC;

SELECT nextval('serialTest2_f2_seq');
SELECT nextval('serialTest2_f3_seq');
SELECT nextval('serialTest2_f4_seq');
SELECT nextval('serialTest2_f5_seq');
SELECT nextval('serialTest2_f6_seq');

-- basic sequence operations using both text and oid references
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE IF NOT EXISTS sequence_test;
--DDL_STATEMENT_END--
SELECT nextval('sequence_test'::text);
SELECT nextval('sequence_test'::regclass);
SELECT currval('sequence_test'::text);
SELECT currval('sequence_test'::regclass);
SELECT setval('sequence_test'::text, 32);
SELECT nextval('sequence_test'::regclass);
SELECT setval('sequence_test'::text, 99, false);
SELECT nextval('sequence_test'::regclass);
SELECT setval('sequence_test'::regclass, 32);
SELECT nextval('sequence_test'::text);
SELECT setval('sequence_test'::regclass, 99, false);
SELECT nextval('sequence_test'::text);
DISCARD SEQUENCES;
SELECT currval('sequence_test'::regclass);
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE sequence_test;
--DDL_STATEMENT_END--
-- renaming sequences
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE foo_seq;
--DDL_STATEMENT_END--
--not support: ALTER TABLE foo_seq RENAME TO foo_seq_new;
--not support: SELECT * FROM foo_seq_new;
SELECT nextval('foo_seq_new');
SELECT nextval('foo_seq_new');
-- log_cnt can be higher if there is a checkpoint just at the right
-- time, so just test for the expected range
-- not supporte: SELECT last_value, log_cnt IN (31, 32) AS log_cnt_ok, is_called FROM foo_seq_new;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE foo_seq_new;
--DDL_STATEMENT_END--
-- renaming serial sequences
--DDL_STATEMENT_BEGIN--
ALTER TABLE serialtest1_f2_seq RENAME TO serialtest1_f2_foo;
--DDL_STATEMENT_END--
INSERT INTO serialTest1 VALUES ('more');
SELECT * FROM serialTest1;

--
-- Check dependencies of serial and ordinary sequences
--
--DDL_STATEMENT_BEGIN--
CREATE TEMP SEQUENCE myseq2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP SEQUENCE myseq3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE t1 (
  f1 serial,
  f2 int DEFAULT nextval('myseq2'),
  f3 int DEFAULT nextval('myseq3'::text)
);
--DDL_STATEMENT_END--
-- Both drops should fail, but with different error messages:
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE t1_f1_seq;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE myseq2;
--DDL_STATEMENT_END--
-- This however will work:
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE myseq3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE t1;
--DDL_STATEMENT_END--
-- Fails because no longer existent:
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE t1_f1_seq;
--DDL_STATEMENT_END--
-- Now OK:
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE myseq2;
--DDL_STATEMENT_END--
--
-- Alter sequence
--

--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE serialTest1 CYCLE;  -- error, not a sequence
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop sequence if exists sequence_test2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop sequence if exists sequence_test4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test2 START WITH 32;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test4 INCREMENT BY -1;
--DDL_STATEMENT_END--
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test4');
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE IF EXISTS sequence_test2 RESTART WITH 24
  INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;
--DDL_STATEMENT_END--
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test4');
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test2 RESTART;
--DDL_STATEMENT_END--
SELECT nextval('sequence_test2');
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test2 RESTART WITH 0;  -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test4 RESTART WITH 40;  -- error
--DDL_STATEMENT_END--
-- test CYCLE and NO CYCLE
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test2 RESTART WITH 24
  INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;
--DDL_STATEMENT_END--
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');  -- cycled
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test2 RESTART WITH 24
  NO CYCLE;
--DDL_STATEMENT_END--
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');  -- error
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test2 RESTART WITH -24 START WITH -24
  INCREMENT BY -4 MINVALUE -36 MAXVALUE -5 CYCLE;
--DDL_STATEMENT_END--
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');  -- cycled
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test2 RESTART WITH -24
  NO CYCLE;
--DDL_STATEMENT_END--
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');  -- error

-- reset
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE IF EXISTS sequence_test2 RESTART WITH 32 START WITH 32
  INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;
--DDL_STATEMENT_END--
SELECT setval('sequence_test2', -100);  -- error
SELECT setval('sequence_test2', 100);  -- error
SELECT setval('sequence_test2', 5);
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE sequence_test3;  -- not read from, to test is_called
--DDL_STATEMENT_END--

-- Information schema
SELECT * FROM information_schema.sequences
  WHERE sequence_name ~ ANY(ARRAY['sequence_test', 'serialtest'])
  ORDER BY sequence_name ASC;

SELECT schemaname, sequencename, start_value, min_value, max_value, increment_by, cycle, cache_size, last_value
FROM pg_sequences
WHERE sequencename ~ ANY(ARRAY['sequence_test', 'serialtest'])
  ORDER BY sequencename ASC;


SELECT * FROM pg_sequence_parameters('sequence_test4'::regclass);


\d sequence_test4
\d serialtest2_f2_seq


-- Test comments
COMMENT ON SEQUENCE asdf IS 'won''t work';
COMMENT ON SEQUENCE sequence_test2 IS 'will work';
COMMENT ON SEQUENCE sequence_test2 IS NULL;

-- Test lastval()
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq;
--DDL_STATEMENT_END--
SELECT nextval('seq');
SELECT lastval();
SELECT setval('seq', 99);
SELECT lastval();
DISCARD SEQUENCES;
SELECT lastval();
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq2;
--DDL_STATEMENT_END--
SELECT nextval('seq2');
SELECT lastval();
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq2;
--DDL_STATEMENT_END--
-- should fail
SELECT lastval();
--DDL_STATEMENT_BEGIN--
CREATE USER regress_seq_user;
--DDL_STATEMENT_END--
-- Test sequences in read-only transactions
--DDL_STATEMENT_BEGIN--
CREATE TEMPORARY SEQUENCE sequence_test_temp1;
--DDL_STATEMENT_END--
START TRANSACTION READ ONLY;
SELECT nextval('sequence_test_temp1');  -- ok
SELECT nextval('sequence_test2');  -- error
ROLLBACK;
START TRANSACTION READ ONLY;
SELECT setval('sequence_test_temp1', 1);  -- ok
SELECT setval('sequence_test2', 1);  -- error
ROLLBACK;

-- privileges tests

-- nextval
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SELECT nextval('seq3');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT nextval('seq3');
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT nextval('seq3');
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
-- currval
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT currval('seq3');
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT currval('seq3');
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT currval('seq3');
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
-- lastval
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT lastval();
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT lastval();
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT lastval();
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
-- setval
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE seq3;
--DDL_STATEMENT_END--
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON seq3 FROM regress_seq_user;
--DDL_STATEMENT_END--
SAVEPOINT save;
SELECT setval('seq3', 5);
ROLLBACK TO save;
--DDL_STATEMENT_BEGIN--
GRANT UPDATE ON seq3 TO regress_seq_user;
--DDL_STATEMENT_END--
SELECT setval('seq3', 5);
SELECT nextval('seq3');
ROLLBACK;
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq3;
--DDL_STATEMENT_END--
-- ALTER SEQUENCE
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE sequence_test2 START WITH 1;
--DDL_STATEMENT_END--
ROLLBACK;

-- Sequences should get wiped out as well:
--DDL_STATEMENT_BEGIN--
DROP TABLE serialTest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE serialTest2;
--DDL_STATEMENT_END--
-- Make sure sequences are gone:
SELECT * FROM information_schema.sequences WHERE sequence_name IN
  ('sequence_test2', 'serialtest2_f2_seq', 'serialtest2_f3_seq',
   'serialtest2_f4_seq', 'serialtest2_f5_seq', 'serialtest2_f6_seq')
  ORDER BY sequence_name ASC;
--DDL_STATEMENT_BEGIN--
DROP USER regress_seq_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE seq;
--DDL_STATEMENT_END--
-- cache tests
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE test_seq1 CACHE 10;
--DDL_STATEMENT_END--
SELECT nextval('test_seq1');
SELECT nextval('test_seq1');
SELECT nextval('test_seq1');
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE test_seq1;
--DDL_STATEMENT_END--