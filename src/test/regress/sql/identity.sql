-- sanity check of system catalog
SELECT attrelid, attname, attidentity FROM pg_attribute WHERE attidentity NOT IN ('', 'a', 'd');


--DDL_STATEMENT_BEGIN--
CREATE TABLE itest1 (a int generated by default as identity, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest2 (a bigint generated always as identity, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest3 (a smallint generated by default as identity (start with 7 increment by 5), b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest3 ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;  -- error
--DDL_STATEMENT_END--

SELECT table_name, column_name, column_default, is_nullable, is_identity, identity_generation, identity_start, identity_increment, identity_maximum, identity_minimum, identity_cycle FROM information_schema.columns WHERE table_name LIKE 'itest_' ORDER BY 1, 2;

-- internal sequences should not be shown here
SELECT sequence_name FROM information_schema.sequences WHERE sequence_name LIKE 'itest%';

SELECT pg_get_serial_sequence('itest1', 'a');

\d itest1_a_seq

--DDL_STATEMENT_BEGIN--
CREATE TABLE itest4 (a int, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;  -- error, requires NOT NULL
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a SET NOT NULL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;  -- ok
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a DROP NOT NULL;  -- error, disallowed
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;  -- error, already set
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN b ADD GENERATED ALWAYS AS IDENTITY;  -- error, wrong data type
--DDL_STATEMENT_END--

-- for later
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN b SET DEFAULT '';
--DDL_STATEMENT_END--

-- invalid column type
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest_err_1 (a text generated by default as identity);
--DDL_STATEMENT_END--

-- duplicate identity
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest_err_2 (a int generated always as identity generated by default as identity);
--DDL_STATEMENT_END--

-- cannot have default and identity
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest_err_3 (a int default 5 generated by default as identity);
--DDL_STATEMENT_END--

-- cannot combine serial and identity
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest_err_4 (a serial generated by default as identity);
--DDL_STATEMENT_END--

INSERT INTO itest1 DEFAULT VALUES;
INSERT INTO itest1 DEFAULT VALUES;
INSERT INTO itest2 DEFAULT VALUES;
INSERT INTO itest2 DEFAULT VALUES;
INSERT INTO itest3 DEFAULT VALUES;
INSERT INTO itest3 DEFAULT VALUES;
INSERT INTO itest4 DEFAULT VALUES;
INSERT INTO itest4 DEFAULT VALUES;

SELECT * FROM itest1;
SELECT * FROM itest2;
SELECT * FROM itest3;
SELECT * FROM itest4;


-- VALUES RTEs

INSERT INTO itest3 VALUES (DEFAULT, 'a');
INSERT INTO itest3 VALUES (DEFAULT, 'b'), (DEFAULT, 'c');

SELECT * FROM itest3;


-- OVERRIDING tests

INSERT INTO itest1 VALUES (10, 'xyz');
INSERT INTO itest1 OVERRIDING USER VALUE VALUES (10, 'xyz');

SELECT * FROM itest1;

INSERT INTO itest2 VALUES (10, 'xyz');
INSERT INTO itest2 OVERRIDING SYSTEM VALUE VALUES (10, 'xyz');

SELECT * FROM itest2;


-- UPDATE tests

UPDATE itest1 SET a = 101 WHERE a = 1;
UPDATE itest1 SET a = DEFAULT WHERE a = 2;
SELECT * FROM itest1 order by 1,2;

UPDATE itest2 SET a = 101 WHERE a = 1;
UPDATE itest2 SET a = DEFAULT WHERE a = 2;
SELECT * FROM itest2 order by 1,2;


-- COPY tests

--DDL_STATEMENT_BEGIN--
CREATE TABLE itest9 (a int GENERATED ALWAYS AS IDENTITY, b text, c bigint);
--DDL_STATEMENT_END--

COPY itest9 FROM stdin;
100	foo	200
101	bar	201
\.

COPY itest9 (b, c) FROM stdin;
foo2	202
bar2	203
\.

SELECT * FROM itest9 ORDER BY c;


-- DROP IDENTITY tests

--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a DROP IDENTITY;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a DROP IDENTITY;  -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a DROP IDENTITY IF EXISTS;  -- noop
--DDL_STATEMENT_END--

INSERT INTO itest4 DEFAULT VALUES;  -- fails because NOT NULL is not dropped
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest4 ALTER COLUMN a DROP NOT NULL;
--DDL_STATEMENT_END--
INSERT INTO itest4 DEFAULT VALUES;
SELECT * FROM itest4;

-- check that sequence is removed
SELECT sequence_name FROM itest4_a_seq;


-- test views

--DDL_STATEMENT_BEGIN--
CREATE TABLE itest10 (a int generated by default as identity, b text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE itest11 (a int generated always as identity, b text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE VIEW itestv10 AS SELECT * FROM itest10;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW itestv11 AS SELECT * FROM itest11;
--DDL_STATEMENT_END--

INSERT INTO itestv10 DEFAULT VALUES;
INSERT INTO itestv10 DEFAULT VALUES;

INSERT INTO itestv11 DEFAULT VALUES;
INSERT INTO itestv11 DEFAULT VALUES;

SELECT * FROM itestv10;
SELECT * FROM itestv11;

INSERT INTO itestv10 VALUES (10, 'xyz');
INSERT INTO itestv10 OVERRIDING USER VALUE VALUES (11, 'xyz');

SELECT * FROM itestv10;

INSERT INTO itestv11 VALUES (10, 'xyz');
INSERT INTO itestv11 OVERRIDING SYSTEM VALUE VALUES (11, 'xyz');

SELECT * FROM itestv11;


-- ADD COLUMN

--DDL_STATEMENT_BEGIN--
CREATE TABLE itest13 (a int);
--DDL_STATEMENT_END--
-- add column to empty table
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest13 ADD COLUMN b int GENERATED BY DEFAULT AS IDENTITY;
--DDL_STATEMENT_END--
INSERT INTO itest13 VALUES (1), (2), (3);
-- add column to populated table
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest13 ADD COLUMN c int GENERATED BY DEFAULT AS IDENTITY;
--DDL_STATEMENT_END--
SELECT * FROM itest13 order by 1,2,3;


-- various ALTER COLUMN tests

-- fail, not allowed for identity columns
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest1 ALTER COLUMN a SET DEFAULT 1;
--DDL_STATEMENT_END--

-- fail, not allowed, already has a default
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest5 (a serial, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest5 ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE itest3 ALTER COLUMN a TYPE int;
--DDL_STATEMENT_END--
SELECT seqtypid::regtype FROM pg_sequence WHERE seqrelid = 'itest3_a_seq'::regclass;
\d itest3

--DDL_STATEMENT_BEGIN--
ALTER TABLE itest3 ALTER COLUMN a TYPE text;  -- error
--DDL_STATEMENT_END--


-- ALTER COLUMN ... SET

--DDL_STATEMENT_BEGIN--
CREATE TABLE itest6 (a int GENERATED ALWAYS AS IDENTITY, b text);
--DDL_STATEMENT_END--
INSERT INTO itest6 DEFAULT VALUES;

--DDL_STATEMENT_BEGIN--
ALTER TABLE itest6 ALTER COLUMN a SET GENERATED BY DEFAULT SET INCREMENT BY 2 SET START WITH 100 RESTART;
--DDL_STATEMENT_END--
INSERT INTO itest6 DEFAULT VALUES;
INSERT INTO itest6 DEFAULT VALUES;
SELECT * FROM itest6 order by 1,2;

SELECT table_name, column_name, is_identity, identity_generation FROM information_schema.columns WHERE table_name = 'itest6';

--DDL_STATEMENT_BEGIN--
ALTER TABLE itest6 ALTER COLUMN b SET INCREMENT BY 2;  -- fail, not identity
--DDL_STATEMENT_END--


-- prohibited direct modification of sequence

--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE itest6_a_seq OWNED BY NONE;
--DDL_STATEMENT_END--


-- inheritance

--DDL_STATEMENT_BEGIN--
CREATE TABLE itest7 (a int GENERATED ALWAYS AS IDENTITY);
--DDL_STATEMENT_END--
INSERT INTO itest7 DEFAULT VALUES;
SELECT * FROM itest7;

-- identity property is not inherited
CREATE TABLE itest7a (b text) INHERITS (itest7);

-- make column identity in child table
CREATE TABLE itest7b (a int);
CREATE TABLE itest7c (a int GENERATED ALWAYS AS IDENTITY) INHERITS (itest7b);
INSERT INTO itest7c DEFAULT VALUES;
SELECT * FROM itest7c;

CREATE TABLE itest7d (a int not null);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest7d ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest7d ADD COLUMN b int GENERATED ALWAYS AS IDENTITY;  -- error
--DDL_STATEMENT_END--

SELECT table_name, column_name, is_nullable, is_identity, identity_generation FROM information_schema.columns WHERE table_name LIKE 'itest7%' ORDER BY 1, 2;

-- These ALTER TABLE variants will not recurse.
ALTER TABLE itest7 ALTER COLUMN a SET GENERATED BY DEFAULT;
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest7 ALTER COLUMN a RESTART;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE itest7 ALTER COLUMN a DROP IDENTITY;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP TABLE itest1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest3 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest4 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest5 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest6 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest7 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest7d cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest10 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest11 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest13 cascade;
--DDL_STATEMENT_END--

-- privileges
--DDL_STATEMENT_BEGIN--
CREATE USER regress_identity_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest8 (a int GENERATED ALWAYS AS IDENTITY, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT, INSERT ON itest8 TO regress_identity_user1;
--DDL_STATEMENT_END--
SET ROLE regress_identity_user1;
INSERT INTO itest8 DEFAULT VALUES;
SELECT * FROM itest8;
RESET ROLE;
--DDL_STATEMENT_BEGIN--
DROP TABLE itest8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_identity_user1;
--DDL_STATEMENT_END--

-- table partitions (currently not supported)
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest_parent (f1 date NOT NULL, f2 text, f3 bigint) PARTITION BY RANGE (f1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE itest_child PARTITION OF itest_parent (
    f3 WITH OPTIONS GENERATED ALWAYS AS IDENTITY
) FOR VALUES FROM ('2016-07-01') TO ('2016-08-01'); -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE itest_parent;
--DDL_STATEMENT_END--