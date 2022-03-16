--
-- CREATE_CAST
--

-- Create some types to test with
--DDL_STATEMENT_BEGIN--
CREATE TYPE casttesttype;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE FUNCTION casttesttype_in(cstring)
   RETURNS casttesttype
   AS 'textin'
   LANGUAGE internal STRICT IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION casttesttype_out(casttesttype)
   RETURNS cstring
   AS 'textout'
   LANGUAGE internal STRICT IMMUTABLE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TYPE casttesttype (
   internallength = variable,
   input = casttesttype_in,
   output = casttesttype_out,
   alignment = int4
);
--DDL_STATEMENT_END--

-- a dummy function to test with
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION casttestfunc(casttesttype) RETURNS int4 LANGUAGE SQL AS
$$ SELECT 1; $$;
--DDL_STATEMENT_END--

SELECT casttestfunc('foo'::text); -- fails, as there's no cast

-- Try binary coercion cast
--DDL_STATEMENT_BEGIN--
CREATE CAST (text AS casttesttype) WITHOUT FUNCTION;
--DDL_STATEMENT_END--
SELECT casttestfunc('foo'::text); -- doesn't work, as the cast is explicit
SELECT casttestfunc('foo'::text::casttesttype); -- should work
--DDL_STATEMENT_BEGIN--
DROP CAST (text AS casttesttype); -- cleanup
--DDL_STATEMENT_END--

-- Try IMPLICIT binary coercion cast
--DDL_STATEMENT_BEGIN--
CREATE CAST (text AS casttesttype) WITHOUT FUNCTION AS IMPLICIT;
--DDL_STATEMENT_END--
SELECT casttestfunc('foo'::text); -- Should work now

-- Try I/O conversion cast.
SELECT 1234::int4::casttesttype; -- No cast yet, should fail
--DDL_STATEMENT_BEGIN--

CREATE CAST (int4 AS casttesttype) WITH INOUT;
--DDL_STATEMENT_END--
SELECT 1234::int4::casttesttype; -- Should work now
--DDL_STATEMENT_BEGIN--

DROP CAST (int4 AS casttesttype);
--DDL_STATEMENT_END--

-- Try cast with a function

--DDL_STATEMENT_BEGIN--
CREATE FUNCTION int4_casttesttype(int4) RETURNS casttesttype LANGUAGE SQL AS
$$ SELECT ('foo'::text || $1::text)::casttesttype; $$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE CAST (int4 AS casttesttype) WITH FUNCTION int4_casttesttype(int4) AS IMPLICIT;
--DDL_STATEMENT_END--
SELECT 1234::int4::casttesttype; -- Should work now
