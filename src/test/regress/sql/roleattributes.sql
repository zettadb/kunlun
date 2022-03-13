-- default for superuser is false
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_def_superuser;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_def_superuser';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_superuser WITH SUPERUSER;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_superuser';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_superuser WITH NOSUPERUSER;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_superuser';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_superuser WITH SUPERUSER;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_superuser';

-- default for inherit is true
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_def_inherit;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_def_inherit';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_inherit WITH NOINHERIT;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_inherit';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_inherit WITH INHERIT;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_inherit';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_inherit WITH NOINHERIT;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_inherit';

-- default for create role is false
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_def_createrole;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_def_createrole';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_createrole WITH CREATEROLE;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_createrole';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_createrole WITH NOCREATEROLE;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_createrole';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_createrole WITH CREATEROLE;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_createrole';

-- default for create database is false
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_def_createdb;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_def_createdb';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_createdb WITH CREATEDB;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_createdb';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_createdb WITH NOCREATEDB;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_createdb';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_createdb WITH CREATEDB;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_createdb';

-- default for can login is false for role
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_def_role_canlogin;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_def_role_canlogin';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_role_canlogin WITH LOGIN;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_role_canlogin';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_role_canlogin WITH NOLOGIN;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_role_canlogin';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_role_canlogin WITH LOGIN;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_role_canlogin';

-- default for can login is true for user
--DDL_STATEMENT_BEGIN--
CREATE USER regress_test_def_user_canlogin;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_def_user_canlogin';
--DDL_STATEMENT_BEGIN--
CREATE USER regress_test_user_canlogin WITH NOLOGIN;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_user_canlogin';
--DDL_STATEMENT_BEGIN--
ALTER USER regress_test_user_canlogin WITH LOGIN;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_user_canlogin';
--DDL_STATEMENT_BEGIN--
ALTER USER regress_test_user_canlogin WITH NOLOGIN;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_user_canlogin';

-- default for replication is false
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_def_replication;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_def_replication';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_replication WITH REPLICATION;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_replication';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_replication WITH NOREPLICATION;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_replication';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_replication WITH REPLICATION;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_replication';

-- default for bypassrls is false
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_def_bypassrls;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_def_bypassrls';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_test_bypassrls WITH BYPASSRLS;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_bypassrls';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_bypassrls WITH NOBYPASSRLS;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_bypassrls';
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_test_bypassrls WITH BYPASSRLS;
--DDL_STATEMENT_END--
SELECT * FROM pg_authid WHERE rolname = 'regress_test_bypassrls';

-- clean up roles
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_def_superuser;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_superuser;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_def_inherit;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_inherit;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_def_createrole;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_createrole;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_def_createdb;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_createdb;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_def_role_canlogin;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_role_canlogin;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_test_def_user_canlogin;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_test_user_canlogin;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_def_replication;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_replication;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_def_bypassrls;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_test_bypassrls;
--DDL_STATEMENT_END--