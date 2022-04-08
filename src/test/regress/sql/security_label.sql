--
-- Test for facilities of security label
--

-- initial setups
SET client_min_messages TO 'warning';

--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_seclabel_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_seclabel_user2;
--DDL_STATEMENT_END--

RESET client_min_messages;

--DDL_STATEMENT_BEGIN--
CREATE USER regress_seclabel_user1 WITH CREATEROLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_seclabel_user2;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE seclabel_tbl1 (a int, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE seclabel_tbl2 (x int, y text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE VIEW seclabel_view1 AS SELECT * FROM seclabel_tbl2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION seclabel_four() RETURNS integer AS $$SELECT 4$$ language sql;
--DDL_STATEMENT_END--

-- ERROR:  Statement 'CREATE DOMAIN' is not supported in Kunlun.
--DDL_STATEMENT_BEGIN--
--CREATE DOMAIN seclabel_domain AS text;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE seclabel_tbl1 OWNER TO regress_seclabel_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE seclabel_tbl2 OWNER TO regress_seclabel_user2;
--DDL_STATEMENT_END--

--
-- Test of SECURITY LABEL statement without a plugin
--
SECURITY LABEL ON TABLE seclabel_tbl1 IS 'classified';			-- fail
SECURITY LABEL FOR 'dummy' ON TABLE seclabel_tbl1 IS 'classified';		-- fail
SECURITY LABEL ON TABLE seclabel_tbl1 IS '...invalid label...';		-- fail
SECURITY LABEL ON TABLE seclabel_tbl3 IS 'unclassified';			-- fail

SECURITY LABEL ON ROLE regress_seclabel_user1 IS 'classified';			-- fail
SECURITY LABEL FOR 'dummy' ON ROLE regress_seclabel_user1 IS 'classified';		-- fail
SECURITY LABEL ON ROLE regress_seclabel_user1 IS '...invalid label...';		-- fail
SECURITY LABEL ON ROLE regress_seclabel_user3 IS 'unclassified';			-- fail

-- clean up objects
--DDL_STATEMENT_BEGIN--
DROP FUNCTION seclabel_four();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP DOMAIN seclabel_domain;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP VIEW seclabel_view1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE seclabel_tbl1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE seclabel_tbl2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_seclabel_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_seclabel_user2;
--DDL_STATEMENT_END--