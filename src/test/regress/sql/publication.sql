--
-- PUBLICATION
--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_publication_user LOGIN SUPERUSER;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_publication_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_publication_user_dummy LOGIN NOSUPERUSER;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION 'regress_publication_user';

--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub_default;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
COMMENT ON PUBLICATION testpub_default IS 'test publication';
--DDL_STATEMENT_END--
SELECT obj_description(p.oid, 'pg_publication') FROM pg_publication p;

--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpib_ins_trunct WITH (publish = insert);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default SET (publish = update);
--DDL_STATEMENT_END--

-- error cases
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub_xxx WITH (foo);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub_xxx WITH (publish = 'cluster, vacuum');
--DDL_STATEMENT_END--

\dRp

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default SET (publish = 'insert, update, delete');
--DDL_STATEMENT_END--

\dRp

--- adding tables
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA pub_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testpub_tbl1 (id serial primary key, data text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE pub_test.testpub_nopk (foo int, bar int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW testpub_view AS SELECT 1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testpub_parted (a int) PARTITION BY LIST (a);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub_foralltables FOR ALL TABLES WITH (publish = 'insert');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_foralltables SET (publish = 'insert, update');
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE testpub_tbl2 (id serial primary key, data text);
--DDL_STATEMENT_END--
-- fail - can't add to for all tables publication
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_foralltables ADD TABLE testpub_tbl2;
--DDL_STATEMENT_END--
-- fail - can't drop from all tables publication
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_foralltables DROP TABLE testpub_tbl2;
--DDL_STATEMENT_END--
-- fail - can't add to for all tables publication
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_foralltables SET TABLE pub_test.testpub_nopk;
--DDL_STATEMENT_END--

SELECT pubname, puballtables FROM pg_publication WHERE pubname = 'testpub_foralltables';
\d+ testpub_tbl2
\dRp+ testpub_foralltables

--DDL_STATEMENT_BEGIN--
DROP TABLE testpub_tbl2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PUBLICATION testpub_foralltables;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE testpub_tbl3 (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testpub_tbl3a (b text) INHERITS (testpub_tbl3);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub3 FOR TABLE testpub_tbl3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub4 FOR TABLE ONLY testpub_tbl3;
--DDL_STATEMENT_END--
\dRp+ testpub3

\dRp+ testpub4
--DDL_STATEMENT_BEGIN--
DROP TABLE testpub_tbl3, testpub_tbl3a;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PUBLICATION testpub3, testpub4;
--DDL_STATEMENT_END--

-- fail - view
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub_fortbl FOR TABLE testpub_view;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub_fortbl FOR TABLE testpub_tbl1, pub_test.testpub_nopk;
--DDL_STATEMENT_END--
-- fail - already added
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_fortbl ADD TABLE testpub_tbl1;
--DDL_STATEMENT_END--
-- fail - already added
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub_fortbl FOR TABLE testpub_tbl1;
--DDL_STATEMENT_END--

\dRp+ testpub_fortbl

-- fail - view
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default ADD TABLE testpub_view;
--DDL_STATEMENT_END--
-- fail - partitioned table
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_fortbl ADD TABLE testpub_parted;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default ADD TABLE testpub_tbl1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default SET TABLE testpub_tbl1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default ADD TABLE pub_test.testpub_nopk;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpib_ins_trunct ADD TABLE pub_test.testpub_nopk, testpub_tbl1;
--DDL_STATEMENT_END--

\d+ pub_test.testpub_nopk
\d+ testpub_tbl1
\dRp+ testpub_default

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default DROP TABLE testpub_tbl1, pub_test.testpub_nopk;
--DDL_STATEMENT_END--
-- fail - nonexistent
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default DROP TABLE pub_test.testpub_nopk;
--DDL_STATEMENT_END--

\d+ testpub_tbl1

-- permissions
SET ROLE regress_publication_user2;
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub2;  -- fail
--DDL_STATEMENT_END--

SET ROLE regress_publication_user;
--DDL_STATEMENT_BEGIN--
GRANT CREATE ON DATABASE regression TO regress_publication_user2;
--DDL_STATEMENT_END--
SET ROLE regress_publication_user2;
--DDL_STATEMENT_BEGIN--
CREATE PUBLICATION testpub2;  -- ok
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub2 ADD TABLE testpub_tbl1;  -- fail
--DDL_STATEMENT_END--

SET ROLE regress_publication_user;
--DDL_STATEMENT_BEGIN--
GRANT regress_publication_user TO regress_publication_user2;
--DDL_STATEMENT_END--
SET ROLE regress_publication_user2;

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub2 ADD TABLE testpub_tbl1;  -- ok
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PUBLICATION testpub2;
--DDL_STATEMENT_END--

SET ROLE regress_publication_user;
--DDL_STATEMENT_BEGIN--
REVOKE CREATE ON DATABASE regression FROM regress_publication_user2;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP TABLE testpub_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW testpub_view;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE testpub_tbl1;
--DDL_STATEMENT_END--

\dRp+ testpub_default

-- fail - must be owner of publication
SET ROLE regress_publication_user_dummy;
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default RENAME TO testpub_dummy;
--DDL_STATEMENT_END--
RESET ROLE;

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default RENAME TO testpub_foo;
--DDL_STATEMENT_END--

\dRp testpub_foo

-- rename back to keep the rest simple
--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_foo RENAME TO testpub_default;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER PUBLICATION testpub_default OWNER TO regress_publication_user2;
--DDL_STATEMENT_END--

\dRp testpub_default

--DDL_STATEMENT_BEGIN--
DROP PUBLICATION testpub_default;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PUBLICATION testpib_ins_trunct;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PUBLICATION testpub_fortbl;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP SCHEMA pub_test CASCADE;
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_publication_user, regress_publication_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_publication_user_dummy;
--DDL_STATEMENT_END--