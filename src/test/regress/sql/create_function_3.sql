--
-- CREATE FUNCTION
--
-- Assorted tests using SQL-language functions
--

-- All objects made in this test are in temp_func_test schema
--DDL_STATEMENT_BEGIN--
CREATE USER regress_unpriv_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA temp_func_test;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT ALL ON SCHEMA temp_func_test TO public;
--DDL_STATEMENT_END--
SET search_path TO temp_func_test, public;

--
-- Make sanity checks on the pg_proc entries created by CREATE FUNCTION
--

--
-- ARGUMENT and RETURN TYPES
--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_A_1(text, date) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 = ''abcd'' AND $2 > ''2001-01-01''';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_A_2(text[]) RETURNS int LANGUAGE 'sql'
       AS 'SELECT $1[0]::int';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_A_3() RETURNS bool LANGUAGE 'sql'
       AS 'SELECT false';
--DDL_STATEMENT_END--
SELECT proname, prorettype::regtype, proargtypes::regtype[] FROM pg_proc
       WHERE oid in ('functest_A_1'::regproc,
                     'functest_A_2'::regproc,
                     'functest_A_3'::regproc) ORDER BY proname;

--
-- IMMUTABLE | STABLE | VOLATILE
--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_B_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 0';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_B_2(int) RETURNS bool LANGUAGE 'sql'
       IMMUTABLE AS 'SELECT $1 > 0';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_B_3(int) RETURNS bool LANGUAGE 'sql'
       STABLE AS 'SELECT $1 = 0';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_B_4(int) RETURNS bool LANGUAGE 'sql'
       VOLATILE AS 'SELECT $1 < 0';
--DDL_STATEMENT_END--  
SELECT proname, provolatile FROM pg_proc
       WHERE oid in ('functest_B_1'::regproc,
                     'functest_B_2'::regproc,
                     'functest_B_3'::regproc,
		     'functest_B_4'::regproc) ORDER BY proname;
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_B_2(int) VOLATILE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_B_3(int) COST 100;	-- unrelated change, no effect\
--DDL_STATEMENT_END--
SELECT proname, provolatile FROM pg_proc
       WHERE oid in ('functest_B_1'::regproc,
                     'functest_B_2'::regproc,
                     'functest_B_3'::regproc,
		     'functest_B_4'::regproc) ORDER BY proname;

--
-- SECURITY DEFINER | INVOKER
--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_C_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 0';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_C_2(int) RETURNS bool LANGUAGE 'sql'
       SECURITY DEFINER AS 'SELECT $1 = 0';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_C_3(int) RETURNS bool LANGUAGE 'sql'
       SECURITY INVOKER AS 'SELECT $1 < 0';
--DDL_STATEMENT_END--
SELECT proname, prosecdef FROM pg_proc
       WHERE oid in ('functest_C_1'::regproc,
                     'functest_C_2'::regproc,
                     'functest_C_3'::regproc) ORDER BY proname;
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_C_1(int) IMMUTABLE;	-- unrelated change, no effect
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_C_2(int) SECURITY INVOKER;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_C_3(int) SECURITY DEFINER;
--DDL_STATEMENT_END--
SELECT proname, prosecdef FROM pg_proc
       WHERE oid in ('functest_C_1'::regproc,
                     'functest_C_2'::regproc,
                     'functest_C_3'::regproc) ORDER BY proname;

--
-- LEAKPROOF
--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_E_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 100';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_E_2(int) RETURNS bool LANGUAGE 'sql'
       LEAKPROOF AS 'SELECT $1 > 100';
--DDL_STATEMENT_END--
SELECT proname, proleakproof FROM pg_proc
       WHERE oid in ('functest_E_1'::regproc,
                     'functest_E_2'::regproc) ORDER BY proname;
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_E_1(int) LEAKPROOF;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_E_2(int) STABLE;	-- unrelated change, no effect
--DDL_STATEMENT_END--
SELECT proname, proleakproof FROM pg_proc
       WHERE oid in ('functest_E_1'::regproc,
                     'functest_E_2'::regproc) ORDER BY proname;
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_E_2(int) NOT LEAKPROOF;	-- remove leakproof attribute
--DDL_STATEMENT_END--
SELECT proname, proleakproof FROM pg_proc
       WHERE oid in ('functest_E_1'::regproc,
                     'functest_E_2'::regproc) ORDER BY proname;

-- it takes superuser privilege to turn on leakproof, but not to turn off
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_E_1(int) OWNER TO regress_unpriv_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_E_2(int) OWNER TO regress_unpriv_user;\
--DDL_STATEMENT_END--

SET SESSION AUTHORIZATION regress_unpriv_user;
SET search_path TO temp_func_test, public;
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_E_1(int) NOT LEAKPROOF;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_E_2(int) LEAKPROOF;\
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_E_3(int) RETURNS bool LANGUAGE 'sql'
       LEAKPROOF AS 'SELECT $1 < 200';	-- fail
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;

--
-- CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_F_1(int) RETURNS bool LANGUAGE 'sql'
       AS 'SELECT $1 > 50';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_F_2(int) RETURNS bool LANGUAGE 'sql'
       CALLED ON NULL INPUT AS 'SELECT $1 = 50';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_F_3(int) RETURNS bool LANGUAGE 'sql'
       RETURNS NULL ON NULL INPUT AS 'SELECT $1 < 50';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_F_4(int) RETURNS bool LANGUAGE 'sql'
       STRICT AS 'SELECT $1 = 50';
--DDL_STATEMENT_END--
SELECT proname, proisstrict FROM pg_proc
       WHERE oid in ('functest_F_1'::regproc,
                     'functest_F_2'::regproc,
                     'functest_F_3'::regproc,
                     'functest_F_4'::regproc) ORDER BY proname;
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_F_1(int) IMMUTABLE;	-- unrelated change, no effect
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_F_2(int) STRICT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION functest_F_3(int) CALLED ON NULL INPUT;
--DDL_STATEMENT_END--
SELECT proname, proisstrict FROM pg_proc
       WHERE oid in ('functest_F_1'::regproc,
                     'functest_F_2'::regproc,
                     'functest_F_3'::regproc,
                     'functest_F_4'::regproc) ORDER BY proname;


-- pg_get_functiondef tests

SELECT pg_get_functiondef('functest_A_1'::regproc);
SELECT pg_get_functiondef('functest_B_3'::regproc);
SELECT pg_get_functiondef('functest_C_3'::regproc);
SELECT pg_get_functiondef('functest_F_2'::regproc);


-- information_schema tests
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_IS_1(a int, b int default 1, c text default 'foo')
    RETURNS int
    LANGUAGE SQL
    AS 'SELECT $1 + $2';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_IS_2(out a int, b int default 1)
    RETURNS int
    LANGUAGE SQL
    AS 'SELECT $1';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_IS_3(a int default 1, out b int)
    RETURNS int
    LANGUAGE SQL
    AS 'SELECT $1';
--DDL_STATEMENT_END--
SELECT routine_name, ordinal_position, parameter_name, parameter_default
    FROM information_schema.parameters JOIN information_schema.routines USING (specific_schema, specific_name)
    WHERE routine_schema = 'temp_func_test' AND routine_name ~ '^functest_is_'
    ORDER BY 1, 2;
--DDL_STATEMENT_BEGIN--
DROP FUNCTION functest_IS_1(int, int, text), functest_IS_2(int), functest_IS_3(int);
--DDL_STATEMENT_END--
-- overload
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest_B_2(bigint) RETURNS bool LANGUAGE 'sql'
       IMMUTABLE AS 'SELECT $1 > 0';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION functest_b_1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION functest_b_1;  -- error, not found
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION functest_b_2;  -- error, ambiguous
--DDL_STATEMENT_END--

-- CREATE OR REPLACE tests
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION functest1(a int) RETURNS int LANGUAGE SQL AS 'SELECT $1';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION functest1(a int) RETURNS int LANGUAGE SQL WINDOW AS 'SELECT $1';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE PROCEDURE functest1(a int) LANGUAGE SQL AS 'SELECT $1';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION functest1(a int);
--DDL_STATEMENT_END--


-- Check behavior of VOID-returning SQL functions
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION voidtest1(a int) RETURNS VOID LANGUAGE SQL AS
$$ SELECT a + 1 $$;
--DDL_STATEMENT_END--
SELECT voidtest1(42);
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION voidtest2(a int, b int) RETURNS VOID LANGUAGE SQL AS
$$ SELECT voidtest1(a + b) $$;
--DDL_STATEMENT_END--
SELECT voidtest2(11,22);

-- currently, we can inline voidtest2 but not voidtest1
EXPLAIN (verbose, costs off) SELECT voidtest2(11,22);
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE sometable(f1 int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION voidtest3(a int) RETURNS VOID LANGUAGE SQL AS
$$ INSERT INTO sometable VALUES(a + 1) $$;
--DDL_STATEMENT_END--
SELECT voidtest3(17);
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION voidtest4(a int) RETURNS VOID LANGUAGE SQL AS
$$ INSERT INTO sometable VALUES(a - 1) RETURNING f1 $$;
--DDL_STATEMENT_END--
SELECT voidtest4(39);

TABLE sometable;
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION voidtest5(a int) RETURNS SETOF VOID LANGUAGE SQL AS
$$ SELECT generate_series(1, a) $$ STABLE;
--DDL_STATEMENT_END--
SELECT * FROM voidtest5(3);

-- Cleanup
\set VERBOSITY terse \\ -- suppress cascade details
--DDL_STATEMENT_BEGIN--
DROP SCHEMA temp_func_test CASCADE;
--DDL_STATEMENT_END--
\set VERBOSITY default
--DDL_STATEMENT_BEGIN--
DROP USER regress_unpriv_user;
--DDL_STATEMENT_END--
RESET search_path;
