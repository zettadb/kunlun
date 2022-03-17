--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE x (
	a serial,
	b int,
	c text not null default 'stuff',
	d text,
	e text
) WITH OIDS;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--

CREATE FUNCTION fn_x_before () RETURNS TRIGGER AS '
  BEGIN
		NEW.e = ''before trigger fired''::text;
		return NEW;
	END;
' LANGUAGE plpgsql;

--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_x_after () RETURNS TRIGGER AS '
  BEGIN
		UPDATE x set e=''after trigger fired'' where c=''stuff'';
		return NULL;
	END;
' LANGUAGE plpgsql;
--DDL_STATEMENT_END--

--CREATE TRIGGER trg_x_after AFTER INSERT ON x
--FOR EACH ROW EXECUTE PROCEDURE fn_x_after();

--CREATE TRIGGER trg_x_before BEFORE INSERT ON x
--FOR EACH ROW EXECUTE PROCEDURE fn_x_before();

COPY x (a, b, c, d, e) from stdin;
9999	\N	\\N	\NN	\N
10000	21	31	41	51
\.

COPY x (b, d) from stdin;
1	test_1
\.

COPY x (b, d) from stdin;
2	test_2
3	test_3
4	test_4
5	test_5
\.

COPY x (a, b, c, d, e) from stdin;
10001	22	32	42	52
10002	23	33	43	53
10003	24	34	44	54
10004	25	35	45	55
10005	26	36	46	56
\.

-- non-existent column in column list: should fail
COPY x (xyz) from stdin;

-- too many columns in column list: should fail
COPY x (a, b, c, d, e, d, c) from stdin;

-- missing data: should fail
COPY x from stdin;

\.
COPY x from stdin;
2000	230	23	23
\.
COPY x from stdin;
2001	231	\N	\N
\.

-- extra data: should fail
COPY x from stdin;
2002	232	40	50	60	70	80
\.

-- various COPY options: delimiters, oids, NULL string, encoding
COPY x (b, c, d, e) from stdin with oids delimiter ',' null 'x';
500000,x,45,80,90
500001,x,\x,\\x,\\\x
500002,x,\,,\\\,,\\
\.

COPY x from stdin WITH DELIMITER AS ';' NULL AS '';
3000;;c;;
\.

COPY x from stdin WITH DELIMITER AS ':' NULL AS E'\\X' ENCODING 'sql_ascii';
4000:\X:C:\X:\X
4001:1:empty::
4002:2:null:\X:\X
4003:3:Backslash:\\:\\
4004:4:BackslashX:\\X:\\X
4005:5:N:\N:\N
4006:6:BackslashN:\\N:\\N
4007:7:XX:\XX:\XX
4008:8:Delimiter:\::\:
\.

-- check results of copy in
SELECT * FROM x;

-- COPY w/ oids on a table w/o oids should fail
--DDL_STATEMENT_BEGIN--
CREATE TABLE no_oids (
	a	int,
	b	int
) WITHOUT OIDS;
--DDL_STATEMENT_END--

INSERT INTO no_oids (a, b) VALUES (5, 10);
INSERT INTO no_oids (a, b) VALUES (20, 30);

-- should fail
COPY no_oids FROM stdin WITH OIDS;
COPY no_oids TO stdout WITH OIDS;

-- check copy out
COPY x TO stdout;
COPY x (c, e) TO stdout;
COPY x (b, e) TO stdout WITH NULL 'I''m null';
--DDL_STATEMENT_BEGIN--

CREATE TEMP TABLE y (
	col1 text,
	col2 text
);
--DDL_STATEMENT_END--

INSERT INTO y VALUES ('Jackson, Sam', E'\\h');
INSERT INTO y VALUES ('It is "perfect".',E'\t');
INSERT INTO y VALUES ('', NULL);

COPY y TO stdout WITH CSV;
COPY y TO stdout WITH CSV QUOTE '''' DELIMITER '|';
COPY y TO stdout WITH CSV FORCE QUOTE col2 ESCAPE E'\\' ENCODING 'sql_ascii';
COPY y TO stdout WITH CSV FORCE QUOTE *;

-- Repeat above tests with new 9.0 option syntax

COPY y TO stdout (FORMAT CSV);
COPY y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE *);

\copy y TO stdout (FORMAT CSV)
\copy y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE *)

--test that we read consecutive LFs properly

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE testnl (a int, b text, c int);
--DDL_STATEMENT_END--

COPY testnl FROM stdin CSV;
1,"a field with two LFs

inside",2
\.

-- test end of copy marker
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE testeoc (a text);
--DDL_STATEMENT_END--

COPY testeoc FROM stdin CSV;
a\.
\.b
c\.d
"\."
\.

COPY testeoc TO stdout CSV;

-- test handling of nonstandard null marker that violates escaping rules

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE testnull(a int, b text);
--DDL_STATEMENT_END--
INSERT INTO testnull VALUES (1, E'\\0'), (NULL, NULL);

COPY testnull TO stdout WITH NULL AS E'\\0';

COPY testnull FROM stdin WITH NULL AS E'\\0';
42	\\0
\0	\0
\.

SELECT * FROM testnull;

--BEGIN;
--DDL_STATEMENT_BEGIN--
CREATE TABLE vistest (LIKE testeoc);
--DDL_STATEMENT_END--
COPY vistest FROM stdin CSV;
a0
b
\.
--COMMIT;
SELECT * FROM vistest;
--BEGIN;
--TRUNCATE vistest;
COPY vistest FROM stdin CSV;
a1
b
\.
SELECT * FROM vistest;
SAVEPOINT s1;
--TRUNCATE vistest;
COPY vistest FROM stdin CSV;
d1
e
\.
SELECT * FROM vistest;
--COMMIT;
SELECT * FROM vistest;

--BEGIN;
--TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
a2
b
\.
SELECT * FROM vistest;
SAVEPOINT s1;
--TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
d2
e
\.
SELECT * FROM vistest;
--COMMIT;
SELECT * FROM vistest;

--BEGIN;
--TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
x
y
\.
SELECT * FROM vistest;
--COMMIT;
--TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
p
g
\.
--BEGIN;
--TRUNCATE vistest;
SAVEPOINT s1;
COPY vistest FROM stdin CSV FREEZE;
m
k
\.
--COMMIT;
--BEGIN;
INSERT INTO vistest VALUES ('z');
SAVEPOINT s1;
--TRUNCATE vistest;
ROLLBACK TO SAVEPOINT s1;
COPY vistest FROM stdin CSV FREEZE;
d3
e
\.
--COMMIT;
CREATE FUNCTION truncate_in_subxact() RETURNS VOID AS
$$
--BEGIN
	TRUNCATE vistest;
EXCEPTION
  WHEN OTHERS THEN
	INSERT INTO vistest VALUES ('subxact failure');
END;
$$ language plpgsql;
--BEGIN;
INSERT INTO vistest VALUES ('z');
SELECT truncate_in_subxact();
COPY vistest FROM stdin CSV FREEZE;
d4
e
\.
SELECT * FROM vistest;
--COMMIT;
SELECT * FROM vistest;
-- Test FORCE_NOT_NULL and FORCE_NULL options
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE forcetest (
    a INT NOT NULL,
    b TEXT NOT NULL,
    c TEXT,
    d TEXT,
    e TEXT
);
--DDL_STATEMENT_END--
\pset null NULL
-- should succeed with no effect ("b" remains an empty string, "c" remains NULL)
BEGIN;
COPY forcetest (a, b, c) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(b), FORCE_NULL(c));
1,,""
\.
COMMIT;
SELECT b, c FROM forcetest WHERE a = 1;
-- should succeed, FORCE_NULL and FORCE_NOT_NULL can be both specified
BEGIN;
COPY forcetest (a, b, c, d) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(c,d), FORCE_NULL(c,d));
2,'a',,""
\.
COMMIT;
SELECT c, d FROM forcetest WHERE a = 2;
-- should fail with not-null constraint violation
BEGIN;
COPY forcetest (a, b, c) FROM STDIN WITH (FORMAT csv, FORCE_NULL(b), FORCE_NOT_NULL(c));
3,,""
\.
ROLLBACK;
-- should fail with "not referenced by COPY" error
BEGIN;
COPY forcetest (d, e) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(b));
ROLLBACK;
-- should fail with "not referenced by COPY" error
BEGIN;
COPY forcetest (d, e) FROM STDIN WITH (FORMAT csv, FORCE_NULL(b));
ROLLBACK;
\pset null ''

-- test case with whole-row Var in a check constraint
--DDL_STATEMENT_BEGIN--
create table check_con_tbl (f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function check_con_function(check_con_tbl) returns bool as $$
begin
  raise notice 'input = %', row_to_json($1);
  return $1.f1 > 0;
end $$ language plpgsql immutable;
--DDL_STATEMENT_END--
--alter table check_con_tbl add check (check_con_function(check_con_tbl.*));
\d+ check_con_tbl
copy check_con_tbl from stdin;
1
\N
\.
copy check_con_tbl from stdin;
0
\.
select * from check_con_tbl;

-- test with RLS enabled.
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_rls_copy_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_rls_copy_user_colperms;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE rls_t1 (a int, b int, c int);
--DDL_STATEMENT_END--

COPY rls_t1 (a, b, c) from stdin;
1	4	1
2	3	2
3	2	3
4	1	4
\.

--CREATE POLICY p1 ON rls_t1 FOR SELECT USING (a % 2 = 0);
--ALTER TABLE rls_t1 ENABLE ROW LEVEL SECURITY;
--ALTER TABLE rls_t1 FORCE ROW LEVEL SECURITY;

--DDL_STATEMENT_BEGIN--
GRANT SELECT ON TABLE rls_t1 TO regress_rls_copy_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (a, b) ON TABLE rls_t1 TO regress_rls_copy_user_colperms;
--DDL_STATEMENT_END--

-- all columns
COPY rls_t1 TO stdout;
COPY rls_t1 (a, b, c) TO stdout;

-- subset of columns
COPY rls_t1 (a) TO stdout;
COPY rls_t1 (a, b) TO stdout;

-- column reordering
COPY rls_t1 (b, a) TO stdout;

SET SESSION AUTHORIZATION regress_rls_copy_user;

-- all columns
COPY rls_t1 TO stdout;
COPY rls_t1 (a, b, c) TO stdout;

-- subset of columns
COPY rls_t1 (a) TO stdout;
COPY rls_t1 (a, b) TO stdout;

-- column reordering
COPY rls_t1 (b, a) TO stdout;

RESET SESSION AUTHORIZATION;

SET SESSION AUTHORIZATION regress_rls_copy_user_colperms;

-- attempt all columns (should fail)
COPY rls_t1 TO stdout;
COPY rls_t1 (a, b, c) TO stdout;

-- try to copy column with no privileges (should fail)
COPY rls_t1 (c) TO stdout;

-- subset of columns (should succeed)
COPY rls_t1 (a) TO stdout;
COPY rls_t1 (a, b) TO stdout;

RESET SESSION AUTHORIZATION;

-- test with INSTEAD OF INSERT trigger on a view
--DDL_STATEMENT_BEGIN--
CREATE TABLE instead_of_insert_tbl(id serial, name text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW instead_of_insert_tbl_view AS SELECT ''::text AS str;
--DDL_STATEMENT_END--

COPY instead_of_insert_tbl_view FROM stdin; -- fail
test1
\.

--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fun_instead_of_insert_tbl() RETURNS trigger AS $$
BEGIN
  INSERT INTO instead_of_insert_tbl (name) VALUES (NEW.str);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
--DDL_STATEMENT_END--
--CREATE TRIGGER trig_instead_of_insert_tbl_view
 --INSTEAD OF INSERT ON instead_of_insert_tbl_view
 -- FOR EACH ROW EXECUTE PROCEDURE fun_instead_of_insert_tbl();

COPY instead_of_insert_tbl_view FROM stdin;
test1
\.

SELECT * FROM instead_of_insert_tbl;

-- Test of COPY optimization with view using INSTEAD OF INSERT
-- trigger when relation is created in the same transaction as
-- when COPY is executed.
--BEGIN;
--DDL_STATEMENT_BEGIN--
CREATE VIEW instead_of_insert_tbl_view_2 as select ''::text as str;
--DDL_STATEMENT_END--
--CREATE TRIGGER trig_instead_of_insert_tbl_view_2
 -- INSTEAD OF INSERT ON instead_of_insert_tbl_view_2
  --FOR EACH ROW EXECUTE PROCEDURE fun_instead_of_insert_tbl();

COPY instead_of_insert_tbl_view_2 FROM stdin;
test1
\.

SELECT * FROM instead_of_insert_tbl;
--COMMIT;

-- clean up
--DDL_STATEMENT_BEGIN--
DROP TABLE forcetest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE vistest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION truncate_in_subxact();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE x, y;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE rls_t1 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_rls_copy_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_rls_copy_user_colperms;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION fn_x_before();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION fn_x_after();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE instead_of_insert_tbl;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW instead_of_insert_tbl_view;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW instead_of_insert_tbl_view_2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION fun_instead_of_insert_tbl();
--DDL_STATEMENT_END--