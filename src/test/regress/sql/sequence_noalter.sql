--
-- CREATE SEQUENCE
--

-- various error cases
CREATE UNLOGGED SEQUENCE sequence_testx;
CREATE SEQUENCE sequence_testx INCREMENT BY 0;
CREATE SEQUENCE sequence_testx INCREMENT BY -1 MINVALUE 20;
CREATE SEQUENCE sequence_testx INCREMENT BY 1 MAXVALUE -20;
CREATE SEQUENCE sequence_testx INCREMENT BY -1 START 10;
CREATE SEQUENCE sequence_testx INCREMENT BY 1 START -10;
CREATE SEQUENCE sequence_testx CACHE 0;

-- OWNED BY errors
CREATE SEQUENCE sequence_testx OWNED BY nobody;  -- nonsense word
CREATE SEQUENCE sequence_testx OWNED BY pg_class_oid_index.oid;  -- not a table
CREATE SEQUENCE sequence_testx OWNED BY pg_class.relname;  -- not same schema
CREATE TABLE sequence_test_table (a int primary key);
CREATE SEQUENCE sequence_testx OWNED BY sequence_test_table.b;  -- wrong column
DROP TABLE sequence_test_table;

-- sequence data types
CREATE SEQUENCE sequence_test5 AS integer;
CREATE SEQUENCE sequence_test6 AS smallint;
CREATE SEQUENCE sequence_test7 AS bigint;
CREATE SEQUENCE sequence_test8 AS integer MAXVALUE 100000;
CREATE SEQUENCE sequence_test9 AS integer INCREMENT BY -1;
CREATE SEQUENCE sequence_test10 AS integer MINVALUE -100000 START 1;
CREATE SEQUENCE sequence_test11 AS smallint;
CREATE SEQUENCE sequence_test12 AS smallint INCREMENT -1;
CREATE SEQUENCE sequence_test13 AS smallint MINVALUE -32768;
CREATE SEQUENCE sequence_test14 AS smallint MAXVALUE 32767 INCREMENT -1;
CREATE SEQUENCE sequence_testx AS text;
CREATE SEQUENCE sequence_testx AS nosuchtype;

CREATE SEQUENCE sequence_testx AS smallint MAXVALUE 100000;
CREATE SEQUENCE sequence_testx AS smallint MINVALUE -100000;


---
--- test creation of SERIAL column
---

CREATE TABLE serialTest1 (f1 text, f2 serial primary key);

INSERT INTO serialTest1 VALUES ('foo');
INSERT INTO serialTest1 VALUES ('bar');
INSERT INTO serialTest1 VALUES ('force', 100);
INSERT INTO serialTest1 VALUES ('wrong', NULL);

SELECT * FROM serialTest1;

SELECT pg_get_serial_sequence('serialTest1', 'f2');

-- test smallserial / bigserial
CREATE TABLE serialTest2 (f1 text, f2 serial, f3 smallserial, f4 serial2,
  f5 bigserial, f6 serial8 primary key);

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
CREATE SEQUENCE sequence_test;
CREATE SEQUENCE IF NOT EXISTS sequence_test;

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

DROP SEQUENCE sequence_test;

-- renaming sequences
CREATE SEQUENCE foo_seq_new;
SELECT nextval('foo_seq_new');
DROP SEQUENCE foo_seq_new;


--
-- Check dependencies of serial and ordinary sequences
--
CREATE TEMP SEQUENCE myseq2;
CREATE TEMP SEQUENCE myseq3;
CREATE TEMP TABLE t1 (
  f1 serial,
  f2 int DEFAULT nextval('myseq2'),
  f3 int DEFAULT nextval('myseq3'::text)
);
-- Both drops should fail, but with different error messages:
DROP SEQUENCE t1_f1_seq;
DROP SEQUENCE myseq2;
-- This however will work:
DROP SEQUENCE myseq3;
DROP TABLE t1;
-- Fails because no longer existent:
DROP SEQUENCE t1_f1_seq;
-- Now OK:
DROP SEQUENCE myseq2;


CREATE SEQUENCE sequence_test3;  -- not read from, to test is_called


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
CREATE SEQUENCE seq;
SELECT nextval('seq');
SELECT lastval();
SELECT setval('seq', 99);
SELECT lastval();
DISCARD SEQUENCES;
SELECT lastval();

CREATE SEQUENCE seq2;
SELECT nextval('seq2');
SELECT lastval();

DROP SEQUENCE seq2;
-- should fail
SELECT lastval();

CREATE USER regress_seq_user;

-- Test sequences in read-only transactions
CREATE TEMPORARY SEQUENCE sequence_test_temp1;
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
CREATE SEQUENCE seq3;
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT SELECT ON seq3 TO regress_seq_user;
SELECT nextval('seq3');
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT UPDATE ON seq3 TO regress_seq_user;
SELECT nextval('seq3');
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT USAGE ON seq3 TO regress_seq_user;
SELECT nextval('seq3');
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
-- currval
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT SELECT ON seq3 TO regress_seq_user;
SELECT currval('seq3');
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT UPDATE ON seq3 TO regress_seq_user;
SELECT currval('seq3');
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT USAGE ON seq3 TO regress_seq_user;
SELECT currval('seq3');
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
-- lastval
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT SELECT ON seq3 TO regress_seq_user;
SELECT lastval();
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT UPDATE ON seq3 TO regress_seq_user;
SELECT lastval();
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM regress_seq_user;
GRANT USAGE ON seq3 TO regress_seq_user;
SELECT lastval();
ROLLBACK;

DROP SEQUENCE seq3;
CREATE SEQUENCE seq3;
-- setval
BEGIN;
SET LOCAL SESSION AUTHORIZATION regress_seq_user;
REVOKE ALL ON seq3 FROM regress_seq_user;
SAVEPOINT save;
SELECT setval('seq3', 5);
ROLLBACK TO save;
GRANT UPDATE ON seq3 TO regress_seq_user;
SELECT setval('seq3', 5);
SELECT nextval('seq3');
ROLLBACK;

DROP SEQUENCE seq3;

-- Sequences should get wiped out as well:
DROP TABLE serialTest1;
DROP TABLE serialTest2;

-- Make sure sequences are gone:
SELECT * FROM information_schema.sequences WHERE sequence_name IN
  ('sequence_test2', 'serialtest2_f2_seq', 'serialtest2_f3_seq',
   'serialtest2_f4_seq', 'serialtest2_f5_seq', 'serialtest2_f6_seq')
  ORDER BY sequence_name ASC;

DROP USER regress_seq_user;
DROP SEQUENCE seq;

-- cache tests
CREATE SEQUENCE test_seq1 CACHE 10;
SELECT nextval('test_seq1');
SELECT nextval('test_seq1');
SELECT nextval('test_seq1');

DROP SEQUENCE test_seq1;

create table tseq(a serial primary key, b int);
insert into tseq(b) values(1),(2),(3);
select*from tseq;

create table tseq3(a serial primary key, b smallserial, c bigserial);
insert into tseq3(c) values(1),(2),(3);
select*from tseq3;

CREATE TABLE itest8 (a int GENERATED ALWAYS AS IDENTITY primary key, b text, c bigserial);
insert into itest8(b) values('1'),('abc2'),('def3');
select*from itest8;

create sequence seq2;
create table tseq2(a serial primary key, b smallserial, c bigserial, d int auto_increment, e int generated by default as identity, f int default nextval('seq2')) partition by hash(a);
create table tseq21 partition of tseq2 for values with (modulus 4, remainder 0);
create table tseq22 partition of tseq2 for values with (modulus 4, remainder 1);
create table tseq23 partition of tseq2 for values with (modulus 4, remainder 2);
create table tseq24 partition of tseq2 for values with (modulus 4, remainder 3);
insert into tseq2(e, f) values(default, default),(34, default),(default, default),(45, default);
select*from tseq2;
insert into tseq2(e) values(11),(default),(33),(default);
select*from tseq2;
insert into tseq2(f) values(111),(default),(333),(default);
select*from tseq2;

create table tseq1(a serial primary key, b smallserial, c bigserial, d int) partition by hash(a);
create table tseq11 partition of tseq1 for values with (modulus 4, remainder 0);
create table tseq12 partition of tseq1 for values with (modulus 4, remainder 1);
create table tseq13 partition of tseq1 for values with (modulus 4, remainder 2);
create table tseq14 partition of tseq1 for values with (modulus 4, remainder 3);
insert into tseq1(d) values(1),(2),(3),(4), (5),(6),(7),(8);
select*from tseq1;

drop table tseq1;
drop table tseq2;
drop table itest8;
drop table tseq3;
drop table tseq;
drop sequence seq2;

CREATE TABLE itest8 (a int GENERATED ALWAYS AS IDENTITY primary key, b text, c bigserial);
create table tseq(a serial primary key, b int);
create table tseq3(a serial primary key, b smallserial, c bigserial);
create table tseq2(a serial primary key, b smallserial, c bigserial) partition by hash(a);
create table tseq4(a serial primary key, b int generated always as identity, c bigserial);
create table tseq5(a serial primary key, b int generated always as identity);
create table tseq6(a serial primary key, b int);

create table tseq1(a serial primary key, b smallserial, c bigserial) partition by hash(a);
create table tseq11 partition of tseq1 for values with (modulus 4, remainder 0);
create table tseq12 partition of tseq1 for values with (modulus 4, remainder 1);
create table tseq13 partition of tseq1 for values with (modulus 4, remainder 2);
create table tseq14 partition of tseq1 for values with (modulus 4, remainder 3);
drop table tseq3;
drop table tseq;
drop table itest8;
drop table tseq2;
drop table tseq4;
drop table tseq5;
drop table tseq6;
drop table tseq1;

-- oracle sequence grammar
create sequence seq34 nomaxvalue nominvalue nocache nocycle noorder starts with 34;
create sequence seq35 nomaxvalue nominvalue cache 1 no cycle order starts with 35;
select seq34.nextval;
select seq35.currval;
select seq35.nextval;
select seq35.currval;
drop sequence seq35;
drop sequence seq34;

-- seq value fetch
create table t6(a int auto_increment primary key, b serial, c int generated by default as identity,d int);
insert into t6(d) values(11),(22),(33);
insert into t6(d) values(11),(22),(33);
insert into t6(d) values(11),(22),(33);
insert into t6(d) values(11),(22),(33);
select*from t6;
select currval('t6_a_seq');
select currval('t6_b_seq');
select nextval('t6_b_seq');
select currval('t6_c_seq');

create sequence seq7;
create sequence seq6;

create sequence seq56;
create table t55(a serial primary key, b int default nextval('seq56') , c int);
alter sequence seq56 owned by t55.b;
insert into t55(c) values(1),(2),(3);
select*from t55;

drop table if exists t7;
create table t7(a int auto_increment primary key, b serial, c int generated by default as identity,d int default nextval('seq7'), e int);
insert into t7 (e) values(11),(22),(33) returning *;
insert into t7 (e) values(11),(22),(33) returning *;
insert into t7 (e) values(11),(22),(33) returning *;
insert into t7 (e) values(11),(22),(33) returning *;
select*from t7;
select currval('t7_a_seq');
select currval('t7_b_seq');
select currval('t7_c_seq');
select currval('seq7');
select seq7.nextval, seq7.currval, seq7.nextval, seq7.currval;
select seq6.nextval, seq7.currval, seq7.nextval, seq6.currval;

insert into t6(c,d) values(seq6.nextval, seq7.nextval),(seq6.nextval, seq7.nextval),(seq6.currval, seq7.currval);
insert into t6(c,d) values(seq6.nextval, seq7.nextval),(seq6.nextval, seq7.nextval),(seq6.currval, seq7.currval);
insert into t6(c,d) values(seq6.nextval, seq7.nextval),(seq6.nextval, seq7.nextval),(seq6.currval, seq7.currval);
insert into t6(c,d) values(seq6.currval, seq7.currval),(seq6.currval, seq7.currval),(seq6.nextval, seq7.nextval);
select*from t6;

create schema scm1;
alter sequence seq7 set schema scm1;
-- alter sequence scm1.seq7 rename to scm1.seq77;
-- above doesn't work, new name can't have schema name prepended.
-- below stmt rename a seq but doesn't move it to public schema, seq77 is still in scm1.
alter sequence scm1.seq7 rename to seq77;

select scm1.seq77.nextval, scm1.seq77.currval;
select lastval();
drop schema scm1;
drop schema scm1 cascade;

drop table t6;
drop table t7;
drop table t55;
drop sequence seq7;
drop sequence seq6;
drop sequence seq56;

drop sequence sequence_test5;
drop sequence sequence_test6;
drop sequence sequence_test7;
drop sequence sequence_test8;
drop sequence sequence_test9;
drop sequence sequence_test10;
drop sequence sequence_test11;
drop sequence sequence_test12;
drop sequence sequence_test13;
drop sequence sequence_test14;
drop sequence sequence_test3;
