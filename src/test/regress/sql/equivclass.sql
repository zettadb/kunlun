--
-- Tests for the planner's "equivalence class" mechanism
--

-- One thing that's not tested well during normal querying is the logic
-- for handling "broken" ECs.  This is because an EC can only become broken
-- if its underlying btree operator family doesn't include a complete set
-- of cross-type equality operators.  There are not (and should not be)
-- any such families built into Postgres; so we have to hack things up
-- to create one.  We do this by making two alias types that are really
-- int8 (so we need no new C code) and adding only some operators for them
-- into the standard integer_ops opfamily.

--DDL_STATEMENT_BEGIN--
drop type if exists int8alias1 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type int8alias1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function int8alias1in(cstring) returns int8alias1
  strict immutable language internal as 'int8in';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function int8alias1out(int8alias1) returns cstring
  strict immutable language internal as 'int8out';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type int8alias1 (
    input = int8alias1in,
    output = int8alias1out,
    like = int8
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop type if exists int8alias2 cascade;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type int8alias2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function int8alias2in(cstring) returns int8alias2
  strict immutable language internal as 'int8in';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function int8alias2out(int8alias2) returns cstring
  strict immutable language internal as 'int8out';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type int8alias2 (
    input = int8alias2in,
    output = int8alias2out,
    like = int8
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create cast (int8 as int8alias1) without function;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create cast (int8 as int8alias2) without function;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create cast (int8alias1 as int8) without function;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create cast (int8alias2 as int8) without function;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function int8alias1eq(int8alias1, int8alias1) returns bool
  strict immutable language internal as 'int8eq';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator = (
    procedure = int8alias1eq,
    leftarg = int8alias1, rightarg = int8alias1,
    commutator = =,
    restrict = eqsel, join = eqjoinsel,
    merges
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator family integer_ops using btree add
  operator 3 = (int8alias1, int8alias1);

--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create function int8alias2eq(int8alias2, int8alias2) returns bool
  strict immutable language internal as 'int8eq';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator = (
    procedure = int8alias2eq,
    leftarg = int8alias2, rightarg = int8alias2,
    commutator = =,
    restrict = eqsel, join = eqjoinsel,
    merges
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator family integer_ops using btree add
  operator 3 = (int8alias2, int8alias2);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function int8alias1eq(int8, int8alias1) returns bool
  strict immutable language internal as 'int8eq';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator = (
    procedure = int8alias1eq,
    leftarg = int8, rightarg = int8alias1,
    restrict = eqsel, join = eqjoinsel,
    merges
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator family integer_ops using btree add
  operator 3 = (int8, int8alias1);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function int8alias1eq(int8alias1, int8alias2) returns bool
  strict immutable language internal as 'int8eq';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator = (
    procedure = int8alias1eq,
    leftarg = int8alias1, rightarg = int8alias2,
    restrict = eqsel, join = eqjoinsel,
    merges
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator family integer_ops using btree add
  operator 3 = (int8alias1, int8alias2);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function int8alias1lt(int8alias1, int8alias1) returns bool
  strict immutable language internal as 'int8lt';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create operator < (
    procedure = int8alias1lt,
    leftarg = int8alias1, rightarg = int8alias1
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator family integer_ops using btree add
  operator 1 < (int8alias1, int8alias1);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create function int8alias1cmp(int8, int8alias1) returns int
  strict immutable language internal as 'btint8cmp';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter operator family integer_ops using btree add
  function 1 int8alias1cmp (int8, int8alias1);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists ec0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table ec0 (ff int8 primary key, f1 int8, f2 int8);
--DDL_STATEMENT_END--

-- for the moment we only want to look at nestloop plans
set enable_hashjoin = off;
set enable_mergejoin = off;

--
-- Note that for cases where there's a missing operator, we don't care so
-- much whether the plan is ideal as that we don't fail or generate an
-- outright incorrect plan.
--

explain (costs off)
  select * from ec0 where ff = f1 and f1 = '42'::int8;
explain (costs off)
  select * from ec0 where ff = f1 and f1 = '42'::int8alias1;

set enable_nestloop = on;
set enable_mergejoin = off;

--DDL_STATEMENT_BEGIN--
create user regress_user_ectest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
grant select on ec0 to regress_user_ectest;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
revoke select on ec0 from regress_user_ectest;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop user regress_user_ectest;
--DDL_STATEMENT_END--

-- check that X=X is converted to X IS NOT NULL when appropriate
explain (costs off)
  select * from tenk1 where unique1 = unique1 and unique2 = unique2;

-- this could be converted, but isn't at present
explain (costs off)
  select * from tenk1 where unique1 = unique1 or unique2 = unique2;
