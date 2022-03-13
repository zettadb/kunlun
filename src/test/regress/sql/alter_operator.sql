--DDL_STATEMENT_BEGIN--
CREATE FUNCTION alter_op_test_fn(boolean, boolean)
RETURNS boolean AS $$ SELECT NULL::BOOLEAN; $$ LANGUAGE sql IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION customcontsel(internal, oid, internal, integer)
RETURNS float8 AS 'contsel' LANGUAGE internal STABLE STRICT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR === (
    LEFTARG = boolean,
    RIGHTARG = boolean,
    PROCEDURE = alter_op_test_fn,
    COMMUTATOR = ===,
    NEGATOR = !==,
    RESTRICT = customcontsel,
    JOIN = contjoinsel,
    HASHES, MERGES
);
--DDL_STATEMENT_END--
SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;

--
-- Reset and set params
--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = NONE);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (JOIN = NONE);
--DDL_STATEMENT_END--
SELECT oprrest, oprjoin FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = contsel);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (JOIN = contjoinsel);
--DDL_STATEMENT_END--
SELECT oprrest, oprjoin FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = NONE, JOIN = NONE);
--DDL_STATEMENT_END--
SELECT oprrest, oprjoin FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = customcontsel, JOIN = contjoinsel);
--DDL_STATEMENT_END--
SELECT oprrest, oprjoin FROM pg_operator WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype AND oprright = 'boolean'::regtype;

SELECT pg_describe_object(refclassid,refobjid,refobjsubid) as ref, deptype
FROM pg_depend
WHERE classid = 'pg_operator'::regclass AND
      objid = '===(bool,bool)'::regoperator
ORDER BY 1;

--
-- Test invalid options.
--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (COMMUTATOR = ====);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (NEGATOR = ====);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = non_existent_func);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (JOIN = non_existent_func);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (COMMUTATOR = !==);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (NEGATOR = !==);
--DDL_STATEMENT_END--

-- invalid: non-lowercase quoted identifiers
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR & (bit, bit) SET ("Restrict" = _int_contsel, "Join" = _int_contjoinsel);
--DDL_STATEMENT_END--
--
-- Test permission check. Must be owner to ALTER OPERATOR.
--
--DDL_STATEMENT_BEGIN--
CREATE USER regress_alter_op_user;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_alter_op_user;
--DDL_STATEMENT_BEGIN--
ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = NONE);
--DDL_STATEMENT_END--
-- Clean up
RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
DROP USER regress_alter_op_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR === (boolean, boolean);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION customcontsel(internal, oid, internal, integer);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION alter_op_test_fn(boolean, boolean);
--DDL_STATEMENT_END--