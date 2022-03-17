-- Test initial privileges

-- There should always be some initial privileges, set up by initdb
SELECT count(*) > 0 FROM pg_init_privs;

-- Intentionally include some non-initial privs for pg_dump to dump out
--DDL_STATEMENT_BEGIN--
GRANT SELECT ON pg_proc TO CURRENT_USER;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT SELECT (prosrc) ON pg_proc TO CURRENT_USER;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
GRANT SELECT (rolname, rolsuper) ON pg_authid TO CURRENT_USER;
--DDL_STATEMENT_END--