--
-- Test for ALTER some_object {RENAME TO, OWNER TO, SET SCHEMA}
--

-- Clean up in case a prior regression run failed
SET client_min_messages TO 'warning';

--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_alter_generic_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_alter_generic_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE IF EXISTS regress_alter_generic_user3;
--DDL_STATEMENT_END--

RESET client_min_messages;

--DDL_STATEMENT_BEGIN--
CREATE USER regress_alter_generic_user3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_alter_generic_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_alter_generic_user1 IN ROLE regress_alter_generic_user3;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE SCHEMA alt_nsp1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA alt_nsp2;

--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON SCHEMA alt_nsp1, alt_nsp2 TO public;
--DDL_STATEMENT_END--

SET search_path = alt_nsp1, public;

--
-- Function and Aggregate
--
SET SESSION AUTHORIZATION regress_alter_generic_user1;
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION alt_func1(int) RETURNS int LANGUAGE sql
  AS 'SELECT $1 + 1';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION alt_func2(int) RETURNS int LANGUAGE sql
  AS 'SELECT $1 - 1';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE alt_agg1 (
  sfunc1 = int4pl, basetype = int4, stype1 = int4, initcond = 0
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE alt_agg2 (
  sfunc1 = int4mi, basetype = int4, stype1 = int4, initcond = 0
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_func1(int) RENAME TO alt_func3;  -- failed (not aggregate)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_func1(int) OWNER TO regress_alter_generic_user3;  -- failed (not aggregate)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_func1(int) SET SCHEMA alt_nsp2;  -- failed (not aggregate)
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func1(int) RENAME TO alt_func2;  -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func1(int) RENAME TO alt_func3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func2(int) OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func2(int) OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func2(int) SET SCHEMA alt_nsp1;  -- OK, already there
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func2(int) SET SCHEMA alt_nsp2;  -- OK
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg1(int) RENAME TO alt_agg2;   -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg1(int) RENAME TO alt_agg3;   -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg2(int) OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg2(int) OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg2(int) SET SCHEMA alt_nsp2;  -- OK
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_alter_generic_user2;
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION alt_func1(int) RETURNS int LANGUAGE sql
  AS 'SELECT $1 + 2';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION alt_func2(int) RETURNS int LANGUAGE sql
  AS 'SELECT $1 - 2';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE alt_agg1 (
  sfunc1 = int4pl, basetype = int4, stype1 = int4, initcond = 100
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE alt_agg2 (
  sfunc1 = int4mi, basetype = int4, stype1 = int4, initcond = -100
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func3(int) RENAME TO alt_func4;	-- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func1(int) RENAME TO alt_func4;	-- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func3(int) OWNER TO regress_alter_generic_user2;	-- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func2(int) OWNER TO regress_alter_generic_user3;	-- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func3(int) SET SCHEMA alt_nsp2;      -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION alt_func2(int) SET SCHEMA alt_nsp2;	-- failed (name conflicts)
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg3(int) RENAME TO alt_agg4;   -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg1(int) RENAME TO alt_agg4;   -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg3(int) OWNER TO regress_alter_generic_user2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg2(int) OWNER TO regress_alter_generic_user3;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg3(int) SET SCHEMA alt_nsp2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER AGGREGATE alt_agg2(int) SET SCHEMA alt_nsp2;  -- failed (name conflict)
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;

SELECT n.nspname, proname, prorettype::regtype, prokind, a.rolname
  FROM pg_proc p, pg_namespace n, pg_authid a
  WHERE p.pronamespace = n.oid AND p.proowner = a.oid
    AND n.nspname IN ('alt_nsp1', 'alt_nsp2')
  ORDER BY nspname, proname;

--
-- We would test collations here, but it's not possible because the error
-- messages tend to be nonportable.
--

--
-- Conversion
--
SET SESSION AUTHORIZATION regress_alter_generic_user1;
--DDL_STATEMENT_BEGIN--
CREATE CONVERSION alt_conv1 FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE CONVERSION alt_conv2 FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv1 RENAME TO alt_conv2;  -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv1 RENAME TO alt_conv3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv2 OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv2 OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv2 SET SCHEMA alt_nsp2;  -- OK
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_alter_generic_user2;
--DDL_STATEMENT_BEGIN--
CREATE CONVERSION alt_conv1 FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE CONVERSION alt_conv2 FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv3 RENAME TO alt_conv4;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv1 RENAME TO alt_conv4;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv3 OWNER TO regress_alter_generic_user2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv2 OWNER TO regress_alter_generic_user3;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv3 SET SCHEMA alt_nsp2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER CONVERSION alt_conv2 SET SCHEMA alt_nsp2;  -- failed (name conflict)
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;

SELECT n.nspname, c.conname, a.rolname
  FROM pg_conversion c, pg_namespace n, pg_authid a
  WHERE c.connamespace = n.oid AND c.conowner = a.oid
    AND n.nspname IN ('alt_nsp1', 'alt_nsp2')
  ORDER BY nspname, conname;

--
-- Foreign Data Wrapper and Foreign Server
--
--DDL_STATEMENT_BEGIN--
CREATE FOREIGN DATA WRAPPER alt_fdw1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FOREIGN DATA WRAPPER alt_fdw2;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE SERVER alt_fserv1 FOREIGN DATA WRAPPER alt_fdw1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SERVER alt_fserv2 FOREIGN DATA WRAPPER alt_fdw2;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER FOREIGN DATA WRAPPER alt_fdw1 RENAME TO alt_fdw2;  -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FOREIGN DATA WRAPPER alt_fdw1 RENAME TO alt_fdw3;  -- OK
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER SERVER alt_fserv1 RENAME TO alt_fserv2;   -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SERVER alt_fserv1 RENAME TO alt_fserv3;   -- OK
--DDL_STATEMENT_END--

SELECT fdwname FROM pg_foreign_data_wrapper WHERE fdwname like 'alt_fdw%';
SELECT srvname FROM pg_foreign_server WHERE srvname like 'alt_fserv%';

--
-- Procedural Language
--
--DDL_STATEMENT_BEGIN--
CREATE LANGUAGE alt_lang1 HANDLER plpgsql_call_handler;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE LANGUAGE alt_lang2 HANDLER plpgsql_call_handler;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER LANGUAGE alt_lang1 OWNER TO regress_alter_generic_user1;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER LANGUAGE alt_lang2 OWNER TO regress_alter_generic_user2;  -- OK
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_alter_generic_user1;
--DDL_STATEMENT_BEGIN--
ALTER LANGUAGE alt_lang1 RENAME TO alt_lang2;   -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER LANGUAGE alt_lang2 RENAME TO alt_lang3;   -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER LANGUAGE alt_lang1 RENAME TO alt_lang3;   -- OK
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER LANGUAGE alt_lang2 OWNER TO regress_alter_generic_user3;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER LANGUAGE alt_lang3 OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER LANGUAGE alt_lang3 OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;
SELECT lanname, a.rolname
  FROM pg_language l, pg_authid a
  WHERE l.lanowner = a.oid AND l.lanname like 'alt_lang%'
  ORDER BY lanname;

--
-- Operator
--
SET SESSION AUTHORIZATION regress_alter_generic_user1;

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR @-@ ( leftarg = int4, rightarg = int4, procedure = int4mi );
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR @+@ ( leftarg = int4, rightarg = int4, procedure = int4pl );
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER OPERATOR @+@(int4, int4) OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR @+@(int4, int4) OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR @-@(int4, int4) SET SCHEMA alt_nsp2;           -- OK
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_alter_generic_user2;

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR @-@ ( leftarg = int4, rightarg = int4, procedure = int4mi );
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER OPERATOR @+@(int4, int4) OWNER TO regress_alter_generic_user2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR @-@(int4, int4) OWNER TO regress_alter_generic_user3;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR @+@(int4, int4) SET SCHEMA alt_nsp2;   -- failed (not owner)
--DDL_STATEMENT_END--
-- can't test this: the error message includes the raw oid of namespace
-- ALTER OPERATOR @-@(int4, int4) SET SCHEMA alt_nsp2;   -- failed (name conflict)

RESET SESSION AUTHORIZATION;

SELECT n.nspname, oprname, a.rolname,
    oprleft::regtype, oprright::regtype, oprcode::regproc
  FROM pg_operator o, pg_namespace n, pg_authid a
  WHERE o.oprnamespace = n.oid AND o.oprowner = a.oid
    AND n.nspname IN ('alt_nsp1', 'alt_nsp2')
  ORDER BY nspname, oprname;

--
-- OpFamily and OpClass
--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf1 USING hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf2 USING hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf1 USING hash OWNER TO regress_alter_generic_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf2 USING hash OWNER TO regress_alter_generic_user1;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR CLASS alt_opc1 FOR TYPE uuid USING hash AS STORAGE uuid;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR CLASS alt_opc2 FOR TYPE uuid USING hash AS STORAGE uuid;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc1 USING hash OWNER TO regress_alter_generic_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc2 USING hash OWNER TO regress_alter_generic_user1;
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_alter_generic_user1;

--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf1 USING hash RENAME TO alt_opf2;  -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf1 USING hash RENAME TO alt_opf3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf2 USING hash OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf2 USING hash OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf2 USING hash SET SCHEMA alt_nsp2;  -- OK
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc1 USING hash RENAME TO alt_opc2;  -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc1 USING hash RENAME TO alt_opc3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc2 USING hash OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc2 USING hash OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc2 USING hash SET SCHEMA alt_nsp2;  -- OK
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf1 USING hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf2 USING hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf1 USING hash OWNER TO regress_alter_generic_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf2 USING hash OWNER TO regress_alter_generic_user2;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR CLASS alt_opc1 FOR TYPE macaddr USING hash AS STORAGE macaddr;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR CLASS alt_opc2 FOR TYPE macaddr USING hash AS STORAGE macaddr;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc1 USING hash OWNER TO regress_alter_generic_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc2 USING hash OWNER TO regress_alter_generic_user2;
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_alter_generic_user2;

--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf3 USING hash RENAME TO alt_opf4;	-- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf1 USING hash RENAME TO alt_opf4;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf3 USING hash OWNER TO regress_alter_generic_user2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf2 USING hash OWNER TO regress_alter_generic_user3;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf3 USING hash SET SCHEMA alt_nsp2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf2 USING hash SET SCHEMA alt_nsp2;  -- failed (name conflict)
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc3 USING hash RENAME TO alt_opc4;	-- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc1 USING hash RENAME TO alt_opc4;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc3 USING hash OWNER TO regress_alter_generic_user2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc2 USING hash OWNER TO regress_alter_generic_user3;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc3 USING hash SET SCHEMA alt_nsp2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR CLASS alt_opc2 USING hash SET SCHEMA alt_nsp2;  -- failed (name conflict)
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;

SELECT nspname, opfname, amname, rolname
  FROM pg_opfamily o, pg_am m, pg_namespace n, pg_authid a
  WHERE o.opfmethod = m.oid AND o.opfnamespace = n.oid AND o.opfowner = a.oid
    AND n.nspname IN ('alt_nsp1', 'alt_nsp2')
	AND NOT opfname LIKE 'alt_opc%'
  ORDER BY nspname, opfname;

SELECT nspname, opcname, amname, rolname
  FROM pg_opclass o, pg_am m, pg_namespace n, pg_authid a
  WHERE o.opcmethod = m.oid AND o.opcnamespace = n.oid AND o.opcowner = a.oid
    AND n.nspname IN ('alt_nsp1', 'alt_nsp2')
  ORDER BY nspname, opcname;

-- ALTER OPERATOR FAMILY ... ADD/DROP

-- Should work. Textbook case of CREATE / ALTER ADD / ALTER DROP / DROP
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf4 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD
--DDL_STATEMENT_END--
  -- int4 vs int2
  OPERATOR 1 < (int4, int2) ,
  OPERATOR 2 <= (int4, int2) ,
  OPERATOR 3 = (int4, int2) ,
  OPERATOR 4 >= (int4, int2) ,
  OPERATOR 5 > (int4, int2) ,
  FUNCTION 1 btint42cmp(int4, int2);
  
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING btree DROP
--DDL_STATEMENT_END--
  -- int4 vs int2
  OPERATOR 1 (int4, int2) ,
  OPERATOR 2 (int4, int2) ,
  OPERATOR 3 (int4, int2) ,
  OPERATOR 4 (int4, int2) ,
  OPERATOR 5 (int4, int2) ,
  FUNCTION 1 (int4, int2) ;
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf4 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. Invalid values for ALTER OPERATOR FAMILY .. ADD / DROP
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf4 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING invalid_index_method ADD  OPERATOR 1 < (int4, int2); -- invalid indexing_method
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD OPERATOR 6 < (int4, int2); -- operator number should be between 1 and 5
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD OPERATOR 0 < (int4, int2); -- operator number should be between 1 and 5
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD OPERATOR 1 < ; -- operator without argument types
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD FUNCTION 0 btint42cmp(int4, int2); -- function number should be between 1 and 5
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD FUNCTION 6 btint42cmp(int4, int2); -- function number should be between 1 and 5
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf4 USING btree ADD STORAGE invalid_storage; -- Ensure STORAGE is not a part of ALTER OPERATOR FAMILY
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf4 USING btree;
--DDL_STATEMENT_END--

-- Should fail. Need to be SUPERUSER to do ALTER OPERATOR FAMILY .. ADD / DROP
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_alter_generic_user5 NOSUPERUSER;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf5 USING btree;
--DDL_STATEMENT_END--
SET ROLE regress_alter_generic_user5;
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf5 USING btree ADD OPERATOR 1 < (int4, int2), FUNCTION 1 btint42cmp(int4, int2);
--DDL_STATEMENT_END--
RESET ROLE;
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf5 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. Need rights to namespace for ALTER OPERATOR FAMILY .. ADD / DROP
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA alt_nsp6;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_alter_generic_user6;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE ALL ON SCHEMA alt_nsp6 FROM regress_alter_generic_user6;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_nsp6.alt_opf6 USING btree;
--DDL_STATEMENT_END--
SET ROLE regress_alter_generic_user6;
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_nsp6.alt_opf6 USING btree ADD OPERATOR 1 < (int4, int2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA alt_nsp6;
--DDL_STATEMENT_END--

-- Should fail. Only two arguments required for ALTER OPERATOR FAMILY ... DROP OPERATOR
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf7 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf7 USING btree ADD OPERATOR 1 < (int4, int2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf7 USING btree DROP OPERATOR 1 (int4, int2, int8);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf7 USING btree;
--DDL_STATEMENT_END--

-- Should work. During ALTER OPERATOR FAMILY ... DROP OPERATOR
-- when left type is the same as right type, a DROP with only one argument type should work
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf8 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf8 USING btree ADD OPERATOR 1 < (int4, int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf8 USING btree;
--DDL_STATEMENT_END--

-- Should work. Textbook case of ALTER OPERATOR FAMILY ... ADD OPERATOR with FOR ORDER BY
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf9 USING gist;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf9 USING gist ADD OPERATOR 1 < (int4, int4) FOR ORDER BY float_ops;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf9 USING gist;
--DDL_STATEMENT_END--

-- Should fail. Ensure correct ordering methods in ALTER OPERATOR FAMILY ... ADD OPERATOR .. FOR ORDER BY
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf10 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf10 USING btree ADD OPERATOR 1 < (int4, int4) FOR ORDER BY float_ops;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf10 USING btree;
--DDL_STATEMENT_END--

-- Should work. Textbook case of ALTER OPERATOR FAMILY ... ADD OPERATOR with FOR ORDER BY
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf11 USING gist;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf11 USING gist ADD OPERATOR 1 < (int4, int4) FOR ORDER BY float_ops;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf11 USING gist DROP OPERATOR 1 (int4, int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf11 USING gist;
--DDL_STATEMENT_END--

-- Should fail. btree comparison functions should return INTEGER in ALTER OPERATOR FAMILY ... ADD FUNCTION
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf12 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_opf12  (int4, int2) RETURNS BIGINT AS 'SELECT NULL::BIGINT;' LANGUAGE SQL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf12 USING btree ADD FUNCTION 1 fn_opf12(int4, int2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf12 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. hash comparison functions should return INTEGER in ALTER OPERATOR FAMILY ... ADD FUNCTION
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf13 USING hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_opf13  (int4) RETURNS BIGINT AS 'SELECT NULL::BIGINT;' LANGUAGE SQL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf13 USING hash ADD FUNCTION 1 fn_opf13(int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf13 USING hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. btree comparison functions should have two arguments in ALTER OPERATOR FAMILY ... ADD FUNCTION
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf14 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_opf14 (int4) RETURNS BIGINT AS 'SELECT NULL::BIGINT;' LANGUAGE SQL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf14 USING btree ADD FUNCTION 1 fn_opf14(int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf14 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. hash comparison functions should have one argument in ALTER OPERATOR FAMILY ... ADD FUNCTION
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf15 USING hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_opf15 (int4, int2) RETURNS BIGINT AS 'SELECT NULL::BIGINT;' LANGUAGE SQL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf15 USING hash ADD FUNCTION 1 fn_opf15(int4, int2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf15 USING hash;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. In gist throw an error when giving different data types for function argument
-- without defining left / right type in ALTER OPERATOR FAMILY ... ADD FUNCTION
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf16 USING gist;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf16 USING gist ADD FUNCTION 1 btint42cmp(int4, int2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf16 USING gist;
--DDL_STATEMENT_END--

-- Should fail. duplicate operator number / function number in ALTER OPERATOR FAMILY ... ADD FUNCTION
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf17 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf17 USING btree ADD OPERATOR 1 < (int4, int4), OPERATOR 1 < (int4, int4); -- operator # appears twice in same statement
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf17 USING btree ADD OPERATOR 1 < (int4, int4); -- operator 1 requested first-time
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf17 USING btree ADD OPERATOR 1 < (int4, int4); -- operator 1 requested again in separate statement
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf17 USING btree ADD
--DDL_STATEMENT_END--
  OPERATOR 1 < (int4, int2) ,
  OPERATOR 2 <= (int4, int2) ,
  OPERATOR 3 = (int4, int2) ,
  OPERATOR 4 >= (int4, int2) ,
  OPERATOR 5 > (int4, int2) ,
  FUNCTION 1 btint42cmp(int4, int2) ,
  FUNCTION 1 btint42cmp(int4, int2);    -- procedure 1 appears twice in same statement
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf17 USING btree ADD
--DDL_STATEMENT_END--
  OPERATOR 1 < (int4, int2) ,
  OPERATOR 2 <= (int4, int2) ,
  OPERATOR 3 = (int4, int2) ,
  OPERATOR 4 >= (int4, int2) ,
  OPERATOR 5 > (int4, int2) ,
  FUNCTION 1 btint42cmp(int4, int2);    -- procedure 1 appears first time
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf17 USING btree ADD
--DDL_STATEMENT_END--
  OPERATOR 1 < (int4, int2) ,
  OPERATOR 2 <= (int4, int2) ,
  OPERATOR 3 = (int4, int2) ,
  OPERATOR 4 >= (int4, int2) ,
  OPERATOR 5 > (int4, int2) ,
  FUNCTION 1 btint42cmp(int4, int2);    -- procedure 1 requested again in separate statement
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf17 USING btree;
--DDL_STATEMENT_END--


-- Should fail. Ensure that DROP requests for missing OPERATOR / FUNCTIONS
-- return appropriate message in ALTER OPERATOR FAMILY ... DROP OPERATOR / FUNCTION
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR FAMILY alt_opf18 USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf18 USING btree DROP OPERATOR 1 (int4, int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf18 USING btree ADD
  OPERATOR 1 < (int4, int2) ,
  OPERATOR 2 <= (int4, int2) ,
  OPERATOR 3 = (int4, int2) ,
  OPERATOR 4 >= (int4, int2) ,
  OPERATOR 5 > (int4, int2) ,
  FUNCTION 1 btint42cmp(int4, int2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR FAMILY alt_opf18 USING btree DROP FUNCTION 2 (int4, int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR FAMILY alt_opf18 USING btree;
--DDL_STATEMENT_END--

--
-- Text Search Dictionary
--

SET SESSION AUTHORIZATION regress_alter_generic_user1;
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH DICTIONARY alt_ts_dict1 (template=simple);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH DICTIONARY alt_ts_dict2 (template=simple);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict1 RENAME TO alt_ts_dict2;  -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict1 RENAME TO alt_ts_dict3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict2 OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict2 OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict2 SET SCHEMA alt_nsp2;  -- OK
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_alter_generic_user2;
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH DICTIONARY alt_ts_dict1 (template=simple);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH DICTIONARY alt_ts_dict2 (template=simple);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict3 RENAME TO alt_ts_dict4;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict1 RENAME TO alt_ts_dict4;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict3 OWNER TO regress_alter_generic_user2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict2 OWNER TO regress_alter_generic_user3;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict3 SET SCHEMA alt_nsp2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH DICTIONARY alt_ts_dict2 SET SCHEMA alt_nsp2;  -- failed (name conflict)
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;

SELECT nspname, dictname, rolname
  FROM pg_ts_dict t, pg_namespace n, pg_authid a
  WHERE t.dictnamespace = n.oid AND t.dictowner = a.oid
    AND n.nspname in ('alt_nsp1', 'alt_nsp2')
  ORDER BY nspname, dictname;

--
-- Text Search Configuration
--
SET SESSION AUTHORIZATION regress_alter_generic_user1;
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH CONFIGURATION alt_ts_conf1 (copy=english);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH CONFIGURATION alt_ts_conf2 (copy=english);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf1 RENAME TO alt_ts_conf2;  -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf1 RENAME TO alt_ts_conf3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf2 OWNER TO regress_alter_generic_user2;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf2 OWNER TO regress_alter_generic_user3;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf2 SET SCHEMA alt_nsp2;  -- OK
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_alter_generic_user2;
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH CONFIGURATION alt_ts_conf1 (copy=english);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH CONFIGURATION alt_ts_conf2 (copy=english);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf3 RENAME TO alt_ts_conf4;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf1 RENAME TO alt_ts_conf4;  -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf3 OWNER TO regress_alter_generic_user2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf2 OWNER TO regress_alter_generic_user3;  -- failed (no role membership)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf3 SET SCHEMA alt_nsp2;  -- failed (not owner)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH CONFIGURATION alt_ts_conf2 SET SCHEMA alt_nsp2;  -- failed (name conflict)
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;

SELECT nspname, cfgname, rolname
  FROM pg_ts_config t, pg_namespace n, pg_authid a
  WHERE t.cfgnamespace = n.oid AND t.cfgowner = a.oid
    AND n.nspname in ('alt_nsp1', 'alt_nsp2')
  ORDER BY nspname, cfgname;

--
-- Text Search Template
--
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH TEMPLATE alt_ts_temp1 (lexize=dsimple_lexize);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH TEMPLATE alt_ts_temp2 (lexize=dsimple_lexize);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH TEMPLATE alt_ts_temp1 RENAME TO alt_ts_temp2; -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH TEMPLATE alt_ts_temp1 RENAME TO alt_ts_temp3; -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH TEMPLATE alt_ts_temp2 SET SCHEMA alt_nsp2;    -- OK
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH TEMPLATE alt_ts_temp2 (lexize=dsimple_lexize);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH TEMPLATE alt_ts_temp2 SET SCHEMA alt_nsp2;    -- failed (name conflict)
--DDL_STATEMENT_END--

-- invalid: non-lowercase quoted identifiers
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH TEMPLATE tstemp_case ("Init" = init_function);
--DDL_STATEMENT_END--

SELECT nspname, tmplname
  FROM pg_ts_template t, pg_namespace n
  WHERE t.tmplnamespace = n.oid AND nspname like 'alt_nsp%'
  ORDER BY nspname, tmplname;

--
-- Text Search Parser
--

--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH PARSER alt_ts_prs1
    (start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH PARSER alt_ts_prs2
    (start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH PARSER alt_ts_prs1 RENAME TO alt_ts_prs2; -- failed (name conflict)
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH PARSER alt_ts_prs1 RENAME TO alt_ts_prs3; -- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH PARSER alt_ts_prs2 SET SCHEMA alt_nsp2;   -- OK
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH PARSER alt_ts_prs2
    (start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TEXT SEARCH PARSER alt_ts_prs2 SET SCHEMA alt_nsp2;   -- failed (name conflict)
--DDL_STATEMENT_END--

-- invalid: non-lowercase quoted identifiers
--DDL_STATEMENT_BEGIN--
CREATE TEXT SEARCH PARSER tspars_case ("Start" = start_function);
--DDL_STATEMENT_END--

SELECT nspname, prsname
  FROM pg_ts_parser t, pg_namespace n
  WHERE t.prsnamespace = n.oid AND nspname like 'alt_nsp%'
  ORDER BY nspname, prsname;

---
--- Cleanup resources
---
\set VERBOSITY terse \\ -- suppress cascade details

--DDL_STATEMENT_BEGIN--
DROP FOREIGN DATA WRAPPER alt_fdw2 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FOREIGN DATA WRAPPER alt_fdw3 CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP LANGUAGE alt_lang2 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP LANGUAGE alt_lang3 CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP SCHEMA alt_nsp1 CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA alt_nsp2 CASCADE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP USER regress_alter_generic_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_alter_generic_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP USER regress_alter_generic_user3;
--DDL_STATEMENT_END--