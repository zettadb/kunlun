--
-- DEPENDENCIES
--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_dep_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_dep_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_dep_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE GROUP regress_dep_group;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists deptest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE deptest (f1 serial primary key, f2 text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON TABLE deptest TO GROUP regress_dep_group;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON TABLE deptest TO regress_dep_user, regress_dep_user2;
--DDL_STATEMENT_END--
-- can't drop neither because they have privileges somewhere
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP GROUP regress_dep_group;
--DDL_STATEMENT_END--
-- if we revoke the privileges we can drop the group
--DDL_STATEMENT_BEGIN--
REVOKE SELECT ON deptest FROM GROUP regress_dep_group;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP GROUP regress_dep_group;
--DDL_STATEMENT_END--
-- can't drop the user if we revoke the privileges partially
--DDL_STATEMENT_BEGIN--
REVOKE SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON deptest FROM regress_dep_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user;
--DDL_STATEMENT_END--
-- now we are OK to drop him
--DDL_STATEMENT_BEGIN--
REVOKE TRIGGER ON deptest FROM regress_dep_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user;
--DDL_STATEMENT_END--
-- we are OK too if we drop the privileges all at once
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON deptest FROM regress_dep_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user2;
--DDL_STATEMENT_END--
-- can't drop the owner of an object
-- the error message detail here would include a pg_toast_nnn name that
-- is not constant, so suppress it
--DDL_STATEMENT_BEGIN--
\set VERBOSITY terse
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE deptest OWNER TO regress_dep_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user3;
--DDL_STATEMENT_END--
\set VERBOSITY default
-- if we drop the object, we can drop the user too
--DDL_STATEMENT_BEGIN--
DROP TABLE deptest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_dep_user0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_dep_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_dep_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists deptest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE deptest1 (f1 int unique);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON deptest1 TO regress_dep_user1 WITH GRANT OPTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_dep_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE deptest (a serial primary key, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON deptest1 TO regress_dep_user2;
--DDL_STATEMENT_END--
RESET SESSION AUTHORIZATION;
\z deptest1

-- all grants revoked
\z deptest1
-- table was dropped
\d deptest
--DDL_STATEMENT_BEGIN--
drop table deptest;
--DDL_STATEMENT_END--
-- Test REASSIGN OWNED
--DDL_STATEMENT_BEGIN--
GRANT ALL ON deptest1 TO regress_dep_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT CREATE ON DATABASE postgres TO regress_dep_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
SET SESSION AUTHORIZATION regress_dep_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA deptest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE deptest (a serial primary key, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES FOR ROLE regress_dep_user1 IN SCHEMA deptest
  GRANT ALL ON TABLES TO regress_dep_user2;\
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION deptest_func() RETURNS void LANGUAGE plpgsql
  AS $$ BEGIN END; $$;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE deptest_enum AS ENUM ('red');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE deptest_range AS RANGE (SUBTYPE = int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE deptest2 (f1 int);
--DDL_STATEMENT_END--
-- make a serial column the hard way
--DDL_STATEMENT_BEGIN--
CREATE SEQUENCE ss1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE deptest2 ALTER f1 SET DEFAULT nextval('ss1');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SEQUENCE ss1 OWNED BY deptest2.f1;
--DDL_STATEMENT_END--
-- When reassigning ownership of a composite type, its pg_class entry
-- should match
--DDL_STATEMENT_BEGIN--
CREATE TYPE deptest_t AS (a int);
--DDL_STATEMENT_END--
SELECT typowner = relowner
FROM pg_type JOIN pg_class c ON typrelid = c.oid WHERE typname = 'deptest_t';

RESET SESSION AUTHORIZATION;
REASSIGN OWNED BY regress_dep_user1 TO regress_dep_user2;
\dt deptest

SELECT typowner = relowner
FROM pg_type JOIN pg_class c ON typrelid = c.oid WHERE typname = 'deptest_t';

-- doesn't work: grant still exists
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER DEFAULT PRIVILEGES FOR ROLE regress_dep_user1 IN SCHEMA deptest
  REVOKE ALL ON TABLES FROM regress_dep_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON deptest1 FROM regress_dep_user1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE CREATE ON DATABASE postgres FROM regress_dep_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION deptest_func();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE deptest_enum;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE deptest_range;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE deptest2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SEQUENCE ss1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE deptest_t;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA deptest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
\set VERBOSITY terse
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop sequence sequence deptest_a_seq cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table deptest cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_dep_user0;
--DDL_STATEMENT_END--

