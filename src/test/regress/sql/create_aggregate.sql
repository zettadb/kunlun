--
-- CREATE_AGGREGATE
--

-- all functions CREATEd
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);
--DDL_STATEMENT_END--

-- test comments
COMMENT ON AGGREGATE newavg_wrong (int4) IS 'an agg comment';
COMMENT ON AGGREGATE newavg (int4) IS 'an agg comment';
COMMENT ON AGGREGATE newavg (int4) IS NULL;

-- without finalfunc; test obsolete spellings 'sfunc1' etc
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE newsum (
   sfunc1 = int4pl, basetype = int4, stype1 = int4,
   initcond1 = '0'
);
--DDL_STATEMENT_END--

-- zero-argument aggregate
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE newcnt (*) (
   sfunc = int8inc, stype = int8,
   initcond = '0', parallel = safe
);
--DDL_STATEMENT_END--

-- old-style spelling of same (except without parallel-safe; that's too new)
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE oldcnt (
   sfunc = int8inc, basetype = 'ANY', stype = int8,
   initcond = '0'
);
--DDL_STATEMENT_END--
-- aggregate that only cares about null/nonnull input
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE newcnt ("any") (
   sfunc = int8inc_any, stype = int8,
   initcond = '0'
);
--DDL_STATEMENT_END--
COMMENT ON AGGREGATE nosuchagg (*) IS 'should fail';
COMMENT ON AGGREGATE newcnt (*) IS 'an agg(*) comment';
COMMENT ON AGGREGATE newcnt ("any") IS 'an agg(any) comment';

-- multi-argument aggregate
--DDL_STATEMENT_BEGIN--
create function sum3(int8,int8,int8) returns int8 as
'select $1 + $2 + $3' language sql strict immutable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create aggregate sum2(int8,int8) (
   sfunc = sum3, stype = int8,
   initcond = '0'
);
--DDL_STATEMENT_END--
-- multi-argument aggregates sensitive to distinct/order, strict/nonstrict
--DDL_STATEMENT_BEGIN--
create type aggtype as (a integer, b integer, c text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function aggf_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql strict immutable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function aggfns_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql immutable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create aggregate aggfstr(integer,integer,text) (
   sfunc = aggf_trans, stype = aggtype[],
   initcond = '{}'
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create aggregate aggfns(integer,integer,text) (
   sfunc = aggfns_trans, stype = aggtype[], sspace = 10000,
   initcond = '{}'
);
--DDL_STATEMENT_END--

-- variadic aggregate
--DDL_STATEMENT_BEGIN--
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);
--DDL_STATEMENT_END--
-- test ordered-set aggs using built-in support functions
--DDL_STATEMENT_BEGIN--
create aggregate my_percentile_disc(float8 ORDER BY anyelement) (
  stype = internal,
  sfunc = ordered_set_transition,
  finalfunc = percentile_disc_final,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any") (
  stype = internal,
  sfunc = ordered_set_transition_multi,
  finalfunc = rank_final,
  finalfunc_extra = true,
  hypothetical
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter aggregate my_percentile_disc(float8 ORDER BY anyelement)
  rename to test_percentile_disc;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any")
  rename to test_rank;
--DDL_STATEMENT_END--
\da test_*

-- moving-aggregate options
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE sumdouble (float8)
(
    stype = float8,
    sfunc = float8pl,
    mstype = float8,
    msfunc = float8pl,
    minvfunc = float8mi
);
--DDL_STATEMENT_END--

-- aggregate combine and serialization functions

-- can't specify just one of serialfunc and deserialfunc
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE myavg (numeric)
(
	stype = internal,
	sfunc = numeric_avg_accum,
	serialfunc = numeric_avg_serialize
);
--DDL_STATEMENT_END--

-- serialfunc must have correct parameters
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE myavg (numeric)
(
	stype = internal,
	sfunc = numeric_avg_accum,
	serialfunc = numeric_avg_deserialize,
	deserialfunc = numeric_avg_deserialize
);
--DDL_STATEMENT_END--
-- deserialfunc must have correct parameters
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE myavg (numeric)
(
	stype = internal,
	sfunc = numeric_avg_accum,
	serialfunc = numeric_avg_serialize,
	deserialfunc = numeric_avg_serialize
);
--DDL_STATEMENT_END--
-- ensure combine function parameters are checked
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE myavg (numeric)
(
	stype = internal,
	sfunc = numeric_avg_accum,
	serialfunc = numeric_avg_serialize,
	deserialfunc = numeric_avg_deserialize,
	combinefunc = int4larger
);
--DDL_STATEMENT_END--
-- ensure create aggregate works.
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE myavg (numeric)
(
	stype = internal,
	sfunc = numeric_avg_accum,
	finalfunc = numeric_avg,
	serialfunc = numeric_avg_serialize,
	deserialfunc = numeric_avg_deserialize,
	combinefunc = numeric_avg_combine,
	finalfunc_modify = shareable  -- just to test a non-default setting
);
--DDL_STATEMENT_END--
-- Ensure all these functions made it into the catalog
SELECT aggfnoid, aggtransfn, aggcombinefn, aggtranstype::regtype,
       aggserialfn, aggdeserialfn, aggfinalmodify
FROM pg_aggregate
WHERE aggfnoid = 'myavg'::REGPROC;
--DDL_STATEMENT_BEGIN--
DROP AGGREGATE myavg (numeric);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- invalid: bad parallel-safety marking
CREATE AGGREGATE mysum (int)
(
	stype = int,
	sfunc = int4pl,
	parallel = pear
);
--DDL_STATEMENT_END--
-- invalid: nonstrict inverse with strict forward function
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION float8mi_n(float8, float8) RETURNS float8 AS
$$ SELECT $1 - $2; $$
LANGUAGE SQL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE invalidsumdouble (float8)
(
    stype = float8,
    sfunc = float8pl,
    mstype = float8,
    msfunc = float8pl,
    minvfunc = float8mi_n
);
--DDL_STATEMENT_END--
-- invalid: non-matching result types
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION float8mi_int(float8, float8) RETURNS int AS
$$ SELECT CAST($1 - $2 AS INT); $$
LANGUAGE SQL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE wrongreturntype (float8)
(
    stype = float8,
    sfunc = float8pl,
    mstype = float8,
    msfunc = float8pl,
    minvfunc = float8mi_int
);
--DDL_STATEMENT_END--
-- invalid: non-lowercase quoted identifiers
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE case_agg ( -- old syntax
	"Sfunc1" = int4pl,
	"Basetype" = int4,
	"Stype1" = int4,
	"Initcond1" = '0',
	"Parallel" = safe
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE case_agg(float8)
(
	"Stype" = internal,
	"Sfunc" = ordered_set_transition,
	"Finalfunc" = percentile_disc_final,
	"Finalfunc_extra" = true,
	"Finalfunc_modify" = read_write,
	"Parallel" = safe
);
--DDL_STATEMENT_END--