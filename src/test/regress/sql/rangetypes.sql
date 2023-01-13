-- Tests for range data types.
set lc_monetary='en_US.UTF-8';
--DDL_STATEMENT_BEGIN--
drop type if exists textrange;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type textrange as range (subtype=text, collation="C");
--DDL_STATEMENT_END--

--
-- test input parser
--

-- negative tests; should fail
select ''::textrange;
select '-[a,z)'::textrange;
select '[a,z) - '::textrange;
select '(",a)'::textrange;
select '(,,a)'::textrange;
select '(),a)'::textrange;
select '(a,))'::textrange;
select '(],a)'::textrange;
select '(a,])'::textrange;
select '[z,a]'::textrange;

-- should succeed
select '  empty  '::textrange;
select ' ( empty, empty )  '::textrange;
select ' ( " a " " a ", " z " " z " )  '::textrange;
select '(,z)'::textrange;
select '(a,)'::textrange;
select '[,z]'::textrange;
select '[a,]'::textrange;
select '(,)'::textrange;
select '[ , ]'::textrange;
select '["",""]'::textrange;
select '[",",","]'::textrange;
select '["\\","\\"]'::textrange;
select '(\\,a)'::textrange;
select '((,z)'::textrange;
select '([,z)'::textrange;
select '(!,()'::textrange;
select '(!,[)'::textrange;
select '[a,a]'::textrange;
-- these are allowed but normalize to empty:
select '[a,a)'::textrange;
select '(a,a]'::textrange;
select '(a,a)'::textrange;

select numrange(2.0, 1.0);

select numrange(2.0, 3.0) -|- numrange(3.0, 4.0);
select range_adjacent(numrange(2.0, 3.0), numrange(3.1, 4.0));
select range_adjacent(numrange(2.0, 3.0), numrange(3.1, null));
select numrange(2.0, 3.0, '[]') -|- numrange(3.0, 4.0, '()');
select numrange(1.0, 2.0) -|- numrange(2.0, 3.0,'[]');
select range_adjacent(numrange(2.0, 3.0, '(]'), numrange(1.0, 2.0, '(]'));

select numrange(1.1, 3.3) <@ numrange(0.1,10.1);
select numrange(0.1, 10.1) <@ numrange(1.1,3.3);

select numrange(1.1, 2.2) - numrange(2.0, 3.0);
select numrange(1.1, 2.2) - numrange(2.2, 3.0);
select numrange(1.1, 2.2,'[]') - numrange(2.0, 3.0);
select range_minus(numrange(10.1,12.2,'[]'), numrange(110.0,120.2,'(]'));
select range_minus(numrange(10.1,12.2,'[]'), numrange(0.0,120.2,'(]'));

select numrange(4.5, 5.5, '[]') && numrange(5.5, 6.5);
select numrange(1.0, 2.0) << numrange(3.0, 4.0);
select numrange(1.0, 3.0,'[]') << numrange(3.0, 4.0,'[]');
select numrange(1.0, 3.0,'()') << numrange(3.0, 4.0,'()');
select numrange(1.0, 2.0) >> numrange(3.0, 4.0);
select numrange(3.0, 70.0) &< numrange(6.6, 100.0);

select numrange(1.1, 2.2) < numrange(1.0, 200.2);
select numrange(1.1, 2.2) < numrange(1.1, 1.2);

select numrange(1.0, 2.0) + numrange(2.0, 3.0);
select numrange(1.0, 2.0) + numrange(1.5, 3.0);
select numrange(1.0, 2.0) + numrange(2.5, 3.0); -- should fail

select range_merge(numrange(1.0, 2.0), numrange(2.0, 3.0));
select range_merge(numrange(1.0, 2.0), numrange(1.5, 3.0));
select range_merge(numrange(1.0, 2.0), numrange(2.5, 3.0)); -- shouldn't fail

select numrange(1.0, 2.0) * numrange(2.0, 3.0);
select numrange(1.0, 2.0) * numrange(1.5, 3.0);
select numrange(1.0, 2.0) * numrange(2.5, 3.0);

-- test canonical form for int4range
select int4range(1, 10, '[]');
select int4range(1, 10, '[)');
select int4range(1, 10, '(]');
select int4range(1, 10, '()');
select int4range(1, 2, '()');

-- test canonical form for daterange
select daterange('2000-01-10'::date, '2000-01-20'::date, '[]');
select daterange('2000-01-10'::date, '2000-01-20'::date, '[)');
select daterange('2000-01-10'::date, '2000-01-20'::date, '(]');
select daterange('2000-01-10'::date, '2000-01-20'::date, '()');
select daterange('2000-01-10'::date, '2000-01-11'::date, '()');
select daterange('2000-01-10'::date, '2000-01-11'::date, '(]');
select daterange('-infinity'::date, '2000-01-01'::date, '()');
select daterange('-infinity'::date, '2000-01-01'::date, '[)');
select daterange('2000-01-01'::date, 'infinity'::date, '[)');
select daterange('2000-01-01'::date, 'infinity'::date, '[]');

-- test elem <@ range operator
--DDL_STATEMENT_BEGIN--
create table test_range_elem(i int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index test_range_elem_idx on test_range_elem (i);
--DDL_STATEMENT_END--
insert into test_range_elem select i from generate_series(1,100) i;

select count(*) from test_range_elem where i <@ int4range(10,50);

--DDL_STATEMENT_BEGIN--
drop table test_range_elem;
--DDL_STATEMENT_END--

-- test bigint ranges
select int8range(10000000000::int8, 20000000000::int8,'(]');
-- test tstz ranges
set timezone to '-08';
select '[2010-01-01 01:00:00 -05, 2010-01-01 02:00:00 -08)'::tstzrange;
-- should fail
select '[2010-01-01 01:00:00 -08, 2010-01-01 02:00:00 -05)'::tstzrange;
set timezone to default;

--
-- Test multiple range types over the same subtype
--

--DDL_STATEMENT_BEGIN--
create type textrange1 as range(subtype=text, collation="C");
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type textrange2 as range(subtype=text, collation="C");
--DDL_STATEMENT_END--

select textrange1('a','Z') @> 'b'::text;
select textrange2('a','z') @> 'b'::text;

--DDL_STATEMENT_BEGIN--
drop type textrange1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop type textrange2;
--DDL_STATEMENT_END--

--
-- Test polymorphic type system
--

--DDL_STATEMENT_BEGIN--
create function anyarray_anyrange_func(a anyarray, r anyrange)
  returns anyelement as 'select $1[1] + lower($2);' language sql;
--DDL_STATEMENT_END--

select anyarray_anyrange_func(ARRAY[1,2], int4range(10,20));

-- should fail
select anyarray_anyrange_func(ARRAY[1,2], numrange(10,20));

--DDL_STATEMENT_BEGIN--
drop function anyarray_anyrange_func(anyarray, anyrange);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail
create function bogus_func(anyelement)
  returns anyrange as 'select int4range(1,10)' language sql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail
create function bogus_func(int)
  returns anyrange as 'select int4range(1,10)' language sql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function range_add_bounds(anyrange)
  returns anyelement as 'select lower($1) + upper($1)' language sql;
--DDL_STATEMENT_END--

select range_add_bounds(int4range(1, 17));
select range_add_bounds(numrange(1.0001, 123.123));

--DDL_STATEMENT_BEGIN--
create function rangetypes_sql(q anyrange, b anyarray, out c anyelement)
  as $$ select upper($1) + $2[1] $$
  language sql;
--DDL_STATEMENT_END--

select rangetypes_sql(int4range(1,10), ARRAY[2,20]);
select rangetypes_sql(numrange(1,10), ARRAY[2,20]);  -- match failure

--DDL_STATEMENT_BEGIN--
drop function range_add_bounds(anyrange);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop function rangetypes_sql(anyrange, anyarray, anyelement);
--DDL_STATEMENT_END--

--
-- Arrays of ranges
--

select ARRAY[numrange(1.1, 1.2), numrange(12.3, 155.5)];

--
-- Ranges of arrays
--

--DDL_STATEMENT_BEGIN--
create type arrayrange as range (subtype=int4[]);
--DDL_STATEMENT_END--

select arrayrange(ARRAY[1,2], ARRAY[2,1]);
select arrayrange(ARRAY[2,1], ARRAY[1,2]);  -- fail

select array[1,1] <@ arrayrange(array[1,2], array[2,1]);
select array[1,3] <@ arrayrange(array[1,2], array[2,1]);
--DDL_STATEMENT_BEGIN--
drop type arrayrange;
--DDL_STATEMENT_END--

--
-- Ranges of composites
--

--DDL_STATEMENT_BEGIN--
create type two_ints as (a int, b int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type two_ints_range as range (subtype = two_ints);
--DDL_STATEMENT_END--

-- with force_parallel_mode on, this exercises tqueue.c's range remapping
select *, row_to_json(upper(t)) as u from
  (values (two_ints_range(row(1,2), row(3,4))),
          (two_ints_range(row(5,6), row(7,8)))) v(t);
--DDL_STATEMENT_BEGIN--
drop type two_ints cascade;
--DDL_STATEMENT_END--

--
-- Check behavior when subtype lacks a hash function
--

--DDL_STATEMENT_BEGIN--
create type cashrange as range (subtype = money);
--DDL_STATEMENT_END--

set enable_sort = off;  -- try to make it pick a hash setop implementation

select '(2,5)'::cashrange except select '(5,6)'::cashrange;

reset enable_sort;
--DDL_STATEMENT_BEGIN--
drop type cashrange;
--DDL_STATEMENT_END--

--
-- OUT/INOUT/TABLE functions
--

--DDL_STATEMENT_BEGIN--
create function outparam_succeed(i anyrange, out r anyrange, out t text)
  as $$ select $1, 'foo'::text $$ language sql;
--DDL_STATEMENT_END--

select * from outparam_succeed(int4range(1,2));
--DDL_STATEMENT_BEGIN--
drop function outparam_succeed(anyrange, anyrange, text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function inoutparam_succeed(out i anyelement, inout r anyrange)
  as $$ select upper($1), $1 $$ language sql;
--DDL_STATEMENT_END--

select * from inoutparam_succeed(int4range(1,2));
--DDL_STATEMENT_BEGIN--
drop function inoutparam_succeed(anyelement, anyrange);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function table_succeed(i anyelement, r anyrange) returns table(i anyelement, r anyrange)
  as $$ select $1, $2 $$ language sql;
--DDL_STATEMENT_END--

select * from table_succeed(123, int4range(1,11));
--DDL_STATEMENT_BEGIN--
drop function table_succeed(anyelement, anyrange);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail
create function outparam_fail(i anyelement, out r anyrange, out t text)
  as $$ select '[1,10]', 'foo' $$ language sql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
--should fail
create function inoutparam_fail(inout i anyelement, out r anyrange)
  as $$ select $1, '[1,10]' $$ language sql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
--should fail
create function table_fail(i anyelement) returns table(i anyelement, r anyrange)
  as $$ select $1, '[1,10]' $$ language sql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop type textrange;
--DDL_STATEMENT_END--
