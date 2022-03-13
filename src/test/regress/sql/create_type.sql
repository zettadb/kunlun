--
-- CREATE_TYPE
--

--
-- Note: widget_in/out were created in create_function_1, without any
-- prior shell-type creation.  These commands therefore complete a test
-- of the "old style" approach of making the functions first.
--
--DDL_STATEMENT_BEGIN--
CREATE TYPE widget (
   internallength = 24,
   input = widget_in,
   output = widget_out,
   typmod_in = numerictypmodin,
   typmod_out = numerictypmodout,
   alignment = double
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TYPE city_budget (
   internallength = 16,
   input = int44in,
   output = int44out,
   element = int4,
   category = 'x',   -- just to verify the system will take it
   preferred = true  -- ditto
);
--DDL_STATEMENT_END--

-- Test creation and destruction of shell types
--DDL_STATEMENT_BEGIN--
CREATE TYPE shell;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE shell;   -- fail, type already present
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE shell;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE shell;     -- fail, type not exist
--DDL_STATEMENT_END--

-- also, let's leave one around for purposes of pg_dump testing
--DDL_STATEMENT_BEGIN--
CREATE TYPE myshell;
--DDL_STATEMENT_END--
--
-- Test type-related default values (broken in releases before PG 7.2)
--
-- This part of the test also exercises the "new style" approach of making
-- a shell type and then filling it in.
--
--DDL_STATEMENT_BEGIN--
CREATE TYPE int42;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE text_w_default;
--DDL_STATEMENT_END--

-- Make dummy I/O routines using the existing internal support for int4, text
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION int42_in(cstring)
   RETURNS int42
   AS 'int4in'
   LANGUAGE internal STRICT IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION int42_out(int42)
   RETURNS cstring
   AS 'int4out'
   LANGUAGE internal STRICT IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION text_w_default_in(cstring)
   RETURNS text_w_default
   AS 'textin'
   LANGUAGE internal STRICT IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION text_w_default_out(text_w_default)
   RETURNS cstring
   AS 'textout'
   LANGUAGE internal STRICT IMMUTABLE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TYPE int42 (
   internallength = 4,
   input = int42_in,
   output = int42_out,
   alignment = int4,
   default = 42,
   passedbyvalue
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE text_w_default (
   internallength = variable,
   input = text_w_default_in,
   output = text_w_default_out,
   alignment = int4,
   default = 'zippo'
);
--DDL_STATEMENT_END--
-- invalid: non-lowercase quoted identifiers
--DDL_STATEMENT_BEGIN--
CREATE TYPE case_int42 (
	"Internallength" = 4,
	"Input" = int42_in,
	"Output" = int42_out,
	"Alignment" = int4,
	"Default" = 42,
	"Passedbyvalue"
);
--DDL_STATEMENT_END--

-- Test stand-alone composite type
--DDL_STATEMENT_BEGIN--
CREATE TYPE default_test_row AS (f1 text_w_default, f2 int42);
--DDL_STATEMENT_END--
-- Test comments
COMMENT ON TYPE bad IS 'bad comment';
COMMENT ON TYPE default_test_row IS 'good comment';
COMMENT ON TYPE default_test_row IS NULL;
COMMENT ON COLUMN default_test_row.nope IS 'bad comment';
COMMENT ON COLUMN default_test_row.f1 IS 'good comment';
COMMENT ON COLUMN default_test_row.f1 IS NULL;

-- Check shell type create for existing types
--DDL_STATEMENT_BEGIN--
CREATE TYPE text_w_default;		-- should fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE default_test_row CASCADE;
--DDL_STATEMENT_END--
-- Check type create with input/output incompatibility
--DDL_STATEMENT_BEGIN--
CREATE TYPE not_existing_type (INPUT = array_in,
    OUTPUT = array_out,
    ELEMENT = int,
    INTERNALLENGTH = 32);
--DDL_STATEMENT_END--

-- Check dependency transfer of opaque functions when creating a new type
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION base_fn_in(cstring) RETURNS opaque AS 'boolin'
    LANGUAGE internal IMMUTABLE STRICT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION base_fn_out(opaque) RETURNS opaque AS 'boolout'
    LANGUAGE internal IMMUTABLE STRICT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE base_type(INPUT = base_fn_in, OUTPUT = base_fn_out);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION base_fn_in(cstring); -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION base_fn_out(opaque); -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE base_type; -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE base_type CASCADE;
--DDL_STATEMENT_END--

-- Check usage of typmod with a user-defined type
-- (we have borrowed numeric's typmod functions)
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE mytab (foo widget(42,13,7));     -- should fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE mytab (foo widget(42,13));
--DDL_STATEMENT_END--

SELECT format_type(atttypid,atttypmod) FROM pg_attribute
WHERE attrelid = 'mytab'::regclass AND attnum > 0;

-- might as well exercise the widget type while we're here
INSERT INTO mytab VALUES ('(1,2,3)'), ('(-44,5.5,12)');
TABLE mytab;

-- and test format_type() a bit more, too
select format_type('varchar'::regtype, 42);
select format_type('bpchar'::regtype, null);
-- this behavior difference is intentional
select format_type('bpchar'::regtype, -1);
