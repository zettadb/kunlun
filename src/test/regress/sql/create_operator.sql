--
-- CREATE_OPERATOR
--
--DDL_STATEMENT_BEGIN--

CREATE OPERATOR ## (
   leftarg = path,
   rightarg = path,
   function = path_inter,
   commutator = ##
);

--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR <% (
   leftarg = point,
   rightarg = widget,
   procedure = pt_in_widget,
   commutator = >% ,
   negator = >=%
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR @#@ (
   rightarg = int8,		-- left unary
   procedure = numeric_fac
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #@# (
   leftarg = int8,		-- right unary
   procedure = numeric_fac
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #%# (
   leftarg = int8,		-- right unary
   procedure = numeric_fac
);
--DDL_STATEMENT_END--

-- Test operator created above
SELECT point '(1,2)' <% widget '(0,0,3)' AS t,
       point '(1,2)' <% widget '(0,0,1)' AS f;

-- Test comments
COMMENT ON OPERATOR ###### (int4, NONE) IS 'bad right unary';

-- => is disallowed now
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR => (
   leftarg = int8,		-- right unary
   procedure = numeric_fac
);
--DDL_STATEMENT_END--

-- lexing of <=, >=, <>, != has a number of edge cases
-- (=> is tested elsewhere)

-- this is legal because ! is not allowed in sql ops
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR !=- (
   leftarg = int8,		-- right unary
   procedure = numeric_fac
);
--DDL_STATEMENT_END--
SELECT 2 !=-;
-- make sure lexer returns != as <> even in edge cases
SELECT 2 !=/**/ 1, 2 !=/**/ 2;
SELECT 2 !=-- comment to be removed by psql
  1;
DO $$ -- use DO to protect -- from psql
  declare r boolean;
  begin
    execute $e$ select 2 !=-- comment
      1 $e$ into r;
    raise info 'r = %', r;
  end;
$$;

-- check that <= etc. followed by more operator characters are returned
-- as the correct token with correct precedence
SELECT true<>-1 BETWEEN 1 AND 1;  -- BETWEEN has prec. above <> but below Op
SELECT false<>/**/1 BETWEEN 1 AND 1;
SELECT false<=-1 BETWEEN 1 AND 1;
SELECT false>=-1 BETWEEN 1 AND 1;
SELECT 2<=/**/3, 3>=/**/2, 2<>/**/3;
SELECT 3<=/**/2, 2>=/**/3, 2<>/**/2;

-- Should fail. CREATE OPERATOR requires USAGE on SCHEMA
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA schema_op1;
--DDL_STATEMENT_END--
CREATE ROLE regress_rol_op1;
BEGIN TRANSACTION;
--DDL_STATEMENT_BEGIN--
GRANT USAGE ON SCHEMA schema_op1 TO PUBLIC;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE USAGE ON SCHEMA schema_op1 FROM regress_rol_op1;
--DDL_STATEMENT_END--
SET ROLE regress_rol_op1;
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR schema_op1.#*# (
   leftarg = int8,		-- right unary
   procedure = numeric_fac
);
ROLLBACK;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA schema_op1;
--DDL_STATEMENT_END--

-- Should fail. SETOF type functions not allowed as argument (testing leftarg)
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
CREATE OPERATOR #*# (
   leftarg = SETOF int8,
   procedure = numeric_fac
);
ROLLBACK;


--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- Should fail. SETOF type functions not allowed as argument (testing rightarg)
BEGIN TRANSACTION;
CREATE OPERATOR #*# (
   rightarg = SETOF int8,
   procedure = numeric_fac
);
ROLLBACK;
--DDL_STATEMENT_END--


-- Should work. Sample text-book case
--DDL_STATEMENT_BEGIN--
BEGIN TRANSACTION;
CREATE OR REPLACE FUNCTION fn_op2(boolean, boolean)
RETURNS boolean AS $$
    SELECT NULL::BOOLEAN;
$$ LANGUAGE sql IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR === (
    LEFTARG = boolean,
    RIGHTARG = boolean,
    PROCEDURE = fn_op2,
    COMMUTATOR = ===,
    NEGATOR = !==,
    RESTRICT = contsel,
    JOIN = contjoinsel,
    SORT1, SORT2, LTCMP, GTCMP, HASHES, MERGES
);
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. Invalid attribute
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #@%# (
   leftarg = int8,		-- right unary
   procedure = numeric_fac,
   invalid_att = int8
);
--DDL_STATEMENT_END--

-- Should fail. At least leftarg or rightarg should be mandatorily specified
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #@%# (
   procedure = numeric_fac
);
--DDL_STATEMENT_END--
-- Should fail. Procedure should be mandatorily specified

--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #@%# (
   leftarg = int8
);
--DDL_STATEMENT_END--

-- Should fail. CREATE OPERATOR requires USAGE on TYPE
CREATE ROLE regress_rol_op3;
BEGIN TRANSACTION;
--DDL_STATEMENT_BEGIN--
CREATE TYPE type_op3 AS ENUM ('new', 'open', 'closed');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_op3(type_op3, int8)
RETURNS int8 AS $$
    SELECT NULL::int8;
$$ LANGUAGE sql IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE USAGE ON TYPE type_op3 FROM regress_rol_op3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE USAGE ON TYPE type_op3 FROM PUBLIC;  -- Need to do this so that regress_rol_op3 is not allowed USAGE via PUBLIC
--DDL_STATEMENT_END--
SET ROLE regress_rol_op3;
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #*# (
   leftarg = type_op3,
   rightarg = int8,
   procedure = fn_op3
);
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. CREATE OPERATOR requires USAGE on TYPE (need to check separately for rightarg)
CREATE ROLE regress_rol_op4;
BEGIN TRANSACTION;
--DDL_STATEMENT_BEGIN--
CREATE TYPE type_op4 AS ENUM ('new', 'open', 'closed');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_op4(int8, type_op4)
RETURNS int8 AS $$
    SELECT NULL::int8;
$$ LANGUAGE sql IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE USAGE ON TYPE type_op4 FROM regress_rol_op4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE USAGE ON TYPE type_op4 FROM PUBLIC;  -- Need to do this so that regress_rol_op3 is not allowed USAGE via PUBLIC
--DDL_STATEMENT_END--
SET ROLE regress_rol_op4;
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #*# (
   leftarg = int8,
   rightarg = type_op4,
   procedure = fn_op4
);
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. CREATE OPERATOR requires EXECUTE on function
CREATE ROLE regress_rol_op5;
BEGIN TRANSACTION;
--DDL_STATEMENT_BEGIN--
CREATE TYPE type_op5 AS ENUM ('new', 'open', 'closed');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_op5(int8, int8)
RETURNS int8 AS $$
    SELECT NULL::int8;
$$ LANGUAGE sql IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE EXECUTE ON FUNCTION fn_op5(int8, int8) FROM regress_rol_op5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE EXECUTE ON FUNCTION fn_op5(int8, int8) FROM PUBLIC;-- Need to do this so that regress_rol_op3 is not allowed EXECUTE via PUBLIC
--DDL_STATEMENT_END--
SET ROLE regress_rol_op5;
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #*# (
   leftarg = int8,
   rightarg = int8,
   procedure = fn_op5
);
ROLLBACK;
--DDL_STATEMENT_END--

-- Should fail. CREATE OPERATOR requires USAGE on return TYPE
CREATE ROLE regress_rol_op6;
BEGIN TRANSACTION;
--DDL_STATEMENT_BEGIN--
CREATE TYPE type_op6 AS ENUM ('new', 'open', 'closed');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION fn_op6(int8, int8)
RETURNS type_op6 AS $$
    SELECT NULL::type_op6;
$$ LANGUAGE sql IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE USAGE ON TYPE type_op6 FROM regress_rol_op6;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE USAGE ON TYPE type_op6 FROM PUBLIC;  -- Need to do this so that regress_rol_op3 is not allowed USAGE via PUBLIC
--DDL_STATEMENT_END--
SET ROLE regress_rol_op6;
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR #*# (
   leftarg = int8,
   rightarg = int8,
   procedure = fn_op6
);
ROLLBACK;

--DDL_STATEMENT_END--
-- invalid: non-lowercase quoted identifiers
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR ===
(
	"Leftarg" = box,
	"Rightarg" = box,
	"Procedure" = area_equal_function,
	"Commutator" = ===,
	"Negator" = !==,
	"Restrict" = area_restriction_function,
	"Join" = area_join_function,
	"Hashes",
	"Merges"
);
--DDL_STATEMENT_END--
