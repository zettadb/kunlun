--
-- insert with DEFAULT in the target_list
--
drop table if exists inserttest;
create table inserttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing');
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);
insert into inserttest (col2, col3) values (3, DEFAULT);
insert into inserttest (col1, col2, col3) values (DEFAULT, 5, DEFAULT);
insert into inserttest values (DEFAULT, 5, 'test');
insert into inserttest values (DEFAULT, 7);

select * from inserttest;

--
-- insert with similar expression / target_list values (all fail)
--
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT);
insert into inserttest (col1, col2, col3) values (1, 2);
insert into inserttest (col1) values (1, 2);
insert into inserttest (col1) values (DEFAULT, DEFAULT);
226
select * from inserttest;

--
-- VALUES test
--
insert into inserttest values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select 2), (select i from (values(3)) as foo (i)), 'values are fun!');

select * from inserttest;

--
-- TOASTed value test
--
insert into inserttest values(30, 50, repeat('x', 10000));

select col1, col2, char_length(col3) from inserttest;

drop table inserttest;

-- direct partition inserts should check partition bound constraint
drop table if exists range_parted;
drop table if exists part1;
drop table if exists part2;
drop table if exists part3;
drop table if exists part4;
create table range_parted (
	a text,
	b int
) partition by range (a, (b+0));

-- no partitions, so fail
insert into range_parted values ('a', 11);

create table part1 partition of range_parted for values from ('a', 1) to ('a', 10);
create table part2 partition of range_parted for values from ('a', 10) to ('a', 20);
create table part3 partition of range_parted for values from ('b', 1) to ('b', 10);
create table part4 partition of range_parted for values from ('b', 10) to ('b', 20);

-- fail
insert into part1 values ('a', 11);
insert into part1 values ('b', 1);
-- ok
insert into part1 values ('a', 1);
-- fail
insert into part4 values ('b', 21);
insert into part4 values ('a', 10);
-- ok
insert into part4 values ('b', 10);

-- fail (partition key a has a NOT NULL constraint)
insert into part1 values (null);
-- fail (expression key (b+0) cannot be null either)
insert into part1 values (1);

drop table if exists list_parted;
create table list_parted (
	a text,
	b int
) partition by list (lower(a));
create table part_aa_bb partition of list_parted FOR VALUES IN ('aa', 'bb');
create table part_cc_dd partition of list_parted FOR VALUES IN ('cc', 'dd');
create table part_null partition of list_parted FOR VALUES IN (null);

-- fail
insert into part_aa_bb values ('cc', 1);
insert into part_aa_bb values ('AAa', 1);
insert into part_aa_bb values (null);
-- ok
insert into part_cc_dd values ('cC', 1);
insert into part_null values (null, 0);

-- check in case of multi-level partitioned table
create table part_ee_ff partition of list_parted for values in ('ee', 'ff') partition by range (b);
create table part_ee_ff1 partition of part_ee_ff for values from (1) to (10);
create table part_ee_ff2 partition of part_ee_ff for values from (10) to (20);

create table part_xx_yy partition of list_parted for values in ('xx', 'yy') partition by list (a);
create table part_xx_yy_p1 partition of part_xx_yy for values in ('xx');

-- fail
insert into part_ee_ff1 values ('EE', 11);
-- fail (even the parent's, ie, part_ee_ff's partition constraint applies)
insert into part_ee_ff1 values ('cc', 1);
-- ok
insert into part_ee_ff1 values ('ff', 1);
insert into part_ee_ff2 values ('ff', 11);
insert into list_parted values ('ab', 21);
insert into list_parted values ('xx', 1);
insert into list_parted values ('yy', 2);

-- Check tuple routing for partitioned tables

-- fail
insert into range_parted values ('a', 0);
-- ok
insert into range_parted values ('a', 1);
insert into range_parted values ('a', 10);
-- fail
insert into range_parted values ('a', 20);
-- ok
insert into range_parted values ('b', 1);
insert into range_parted values ('b', 10);
-- fail (partition key (b+0) is null)
insert into range_parted values ('a');

insert into range_parted values (null, null);
insert into range_parted values ('a', null);
insert into range_parted values (null, 19);
insert into range_parted values ('b', 20);

-- ok
insert into list_parted values (null, 1);
insert into list_parted (a) values ('aA');
-- fail (partition of part_ee_ff not found in both cases)
insert into list_parted values ('EE', 0);
insert into part_ee_ff values ('EE', 0);
-- ok
insert into list_parted values ('EE', 1);
insert into part_ee_ff values ('EE', 10);

-- some more tests to exercise tuple-routing with multi-level partitioning
create table part_gg partition of list_parted for values in ('gg') partition by range (b);
create table part_gg1 partition of part_gg for values from (minvalue) to (1);
create table part_gg2 partition of part_gg for values from (1) to (10) partition by range (b);
create table part_gg2_1 partition of part_gg2 for values from (1) to (5);
create table part_gg2_2 partition of part_gg2 for values from (5) to (10);

create table part_ee_ff3 partition of part_ee_ff for values from (20) to (30) partition by range (b);
create table part_ee_ff3_1 partition of part_ee_ff3 for values from (20) to (25);
create table part_ee_ff3_2 partition of part_ee_ff3 for values from (25) to (30);

delete from list_parted;
insert into list_parted values ('aa'), ('cc');
insert into list_parted select 'Ff', s.a from generate_series(1, 29) s(a);
insert into list_parted select 'gg', s.a from generate_series(1, 9) s(a);
insert into list_parted (b) values (1);

-- direct partition inserts should check hash partition bound constraint

-- Use hand-rolled hash functions and operator classes to get predictable
-- result on different matchines.  The hash function for int4 simply returns
-- the sum of the values passed to it and the one for text returns the length
-- of the non-empty string value passed to it or 0.

create or replace function part_hashint4_noop(value int4, seed int8)
returns int8 as $$
select value + seed;
$$ language sql immutable;

create operator class part_test_int4_ops
for type int4
using hash as
operator 1 =,
function 2 part_hashint4_noop(int4, int8);

create or replace function part_hashtext_length(value text, seed int8)
RETURNS int8 AS $$
select length(coalesce(value, ''))::int8
$$ language sql immutable;

create operator class part_test_text_ops
for type text
using hash as
operator 1 =,
function 2 part_hashtext_length(text, int8);

drop table if exists hash_parted;
create table hash_parted (
	a int
) partition by hash (a part_test_int4_ops);
create table hpart0 partition of hash_parted for values with (modulus 4, remainder 0);
create table hpart1 partition of hash_parted for values with (modulus 4, remainder 1);
create table hpart2 partition of hash_parted for values with (modulus 4, remainder 2);
create table hpart3 partition of hash_parted for values with (modulus 4, remainder 3);

insert into hash_parted values(generate_series(1,10));

-- direct insert of values divisible by 4 - ok;
insert into hpart0 values(12),(16);
-- fail;
insert into hpart0 values(11);
-- 11 % 4 -> 3 remainder i.e. valid data for hpart3 partition
insert into hpart3 values(11);

-- test \d+ output on a table which has both partitioned and unpartitioned
-- partitions
\d+ list_parted

-- cleanup
drop table range_parted;
drop table list_parted;
drop table hash_parted;

-- check routing error through a list partitioned table when the key is null
create table lparted_nonullpart (a int, b char) partition by list (b);
create table lparted_nonullpart_a partition of lparted_nonullpart for values in ('a');
insert into lparted_nonullpart values (1);
drop table lparted_nonullpart;

-- check that message shown after failure to find a partition shows the
-- appropriate key description (or none) in various situations
create table key_desc (a int, b int) partition by list ((a+0));
create table key_desc_1 partition of key_desc for values in (1) partition by range (b);

create user regress_insert_other_user;
grant select (a) on key_desc_1 to regress_insert_other_user;
grant insert on key_desc to regress_insert_other_user;

set role regress_insert_other_user;
-- no key description is shown
insert into key_desc values (1, 1);

reset role;
grant select (b) on key_desc_1 to regress_insert_other_user;
set role regress_insert_other_user;
-- key description (b)=(1) is now shown
insert into key_desc values (1, 1);

-- key description is not shown if key contains expression
insert into key_desc values (2, 1);
reset role;
revoke all on key_desc from regress_insert_other_user;
revoke all on key_desc_1 from regress_insert_other_user;
drop role regress_insert_other_user;
drop table key_desc;
drop table key_desc_1;

-- test minvalue/maxvalue restrictions
create table mcrparted (a int, b int, c int) partition by range (a, abs(b), c);
create table mcrparted0 partition of mcrparted for values from (minvalue, 0, 0) to (1, maxvalue, maxvalue);
create table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, minvalue);
create table mcrparted4 partition of mcrparted for values from (21, minvalue, 0) to (30, 20, minvalue);

-- check multi-column range partitioning expression enforces the same
-- constraint as what tuple-routing would determine it to be
create table mcrparted0 partition of mcrparted for values from (minvalue, minvalue, minvalue) to (1, maxvalue, maxvalue);
create table mcrparted1 partition of mcrparted for values from (2, 1, minvalue) to (10, 5, 10);
create table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, maxvalue);
create table mcrparted3 partition of mcrparted for values from (11, 1, 1) to (20, 10, 10);
create table mcrparted4 partition of mcrparted for values from (21, minvalue, minvalue) to (30, 20, maxvalue);
create table mcrparted5 partition of mcrparted for values from (30, 21, 20) to (maxvalue, maxvalue, maxvalue);

-- null not allowed in range partition
insert into mcrparted values (null, null, null);

-- routed to mcrparted0
insert into mcrparted values (0, 1, 1);
insert into mcrparted0 values (0, 1, 1);

-- routed to mcparted1
insert into mcrparted values (9, 1000, 1);
insert into mcrparted1 values (9, 1000, 1);
insert into mcrparted values (10, 5, -1);
insert into mcrparted1 values (10, 5, -1);
insert into mcrparted values (2, 1, 0);
insert into mcrparted1 values (2, 1, 0);

-- routed to mcparted2
insert into mcrparted values (10, 6, 1000);
insert into mcrparted2 values (10, 6, 1000);
insert into mcrparted values (10, 1000, 1000);
insert into mcrparted2 values (10, 1000, 1000);

-- no partition exists, nor does mcrparted3 accept it
insert into mcrparted values (11, 1, -1);
insert into mcrparted3 values (11, 1, -1);

-- routed to mcrparted5
insert into mcrparted values (30, 21, 20);
insert into mcrparted5 values (30, 21, 20);
insert into mcrparted4 values (30, 21, 20);	-- error

-- cleanup
drop table mcrparted;

-- check that a BR constraint can't make partition contain violating rows
create table brtrigpartcon (a int, b text) partition by list (a);
create table brtrigpartcon1 partition of brtrigpartcon for values in (1);
insert into brtrigpartcon values (1, 'hi there');
insert into brtrigpartcon1 values (1, 'hi there');

-- check that the message shows the appropriate column description in a
-- situation where the partitioned table is not the primary ModifyTable node
create table inserttest3 (f1 text default 'foo', f2 text default 'bar', f3 int);
create role regress_coldesc_role;
grant insert on inserttest3 to regress_coldesc_role;
grant insert on brtrigpartcon to regress_coldesc_role;
revoke select on brtrigpartcon from regress_coldesc_role;
set role regress_coldesc_role;
with result as (insert into brtrigpartcon values (1, 'hi there') returning 1)
  insert into inserttest3 (f3) select * from result;
reset role;

-- cleanup
revoke all on inserttest3 from regress_coldesc_role;
revoke all on brtrigpartcon from regress_coldesc_role;
drop role regress_coldesc_role;
drop table inserttest3;
drop table brtrigpartcon;

-- check that "do nothing" BR triggers work with tuple-routing (this checks
-- that estate->es_result_relation_info is appropriately set/reset for each
-- routed tuple)
create table donothingbrtrig_test (a int, b text) partition by list (a);
create table donothingbrtrig_test1 (b text, a int);
create table donothingbrtrig_test2 (c text, b text, a int);
alter table donothingbrtrig_test2 drop column c;
create or replace function donothingbrtrig_func() returns trigger as $$begin raise notice 'b: %', new.b; return NULL; end$$ language plpgsql;
--create trigger donothingbrtrig1 before insert on donothingbrtrig_test1 for each row execute procedure donothingbrtrig_func();
--create trigger donothingbrtrig2 before insert on donothingbrtrig_test2 for each row execute procedure donothingbrtrig_func();
--alter table donothingbrtrig_test attach partition donothingbrtrig_test1 for values in (1);
--alter table donothingbrtrig_test attach partition donothingbrtrig_test2 for values in (2);
--insert into donothingbrtrig_test values (1, 'foo'), (2, 'bar');
copy donothingbrtrig_test from stdout;
1	baz
2	qux
\.
select tableoid::regclass, * from donothingbrtrig_test;

-- cleanup
drop table donothingbrtrig_test;
drop function donothingbrtrig_func();


-- check multi-column range partitioning with minvalue/maxvalue constraints
create table mcrparted (a text, b int) partition by range(a, b);
create table mcrparted1_lt_b partition of mcrparted for values from (minvalue, minvalue) to ('b', minvalue);
create table mcrparted2_b partition of mcrparted for values from ('b', minvalue) to ('c', minvalue);
create table mcrparted3_c_to_common partition of mcrparted for values from ('c', minvalue) to ('common', minvalue);
create table mcrparted4_common_lt_0 partition of mcrparted for values from ('common', minvalue) to ('common', 0);
create table mcrparted5_common_0_to_10 partition of mcrparted for values from ('common', 0) to ('common', 10);
create table mcrparted6_common_ge_10 partition of mcrparted for values from ('common', 10) to ('common', maxvalue);
create table mcrparted7_gt_common_lt_d partition of mcrparted for values from ('common', maxvalue) to ('d', minvalue);
create table mcrparted8_ge_d partition of mcrparted for values from ('d', minvalue) to (maxvalue, maxvalue);

\d+ mcrparted
\d+ mcrparted1_lt_b
\d+ mcrparted2_b
\d+ mcrparted3_c_to_common
\d+ mcrparted4_common_lt_0
\d+ mcrparted5_common_0_to_10
\d+ mcrparted6_common_ge_10
\d+ mcrparted7_gt_common_lt_d
\d+ mcrparted8_ge_d

insert into mcrparted values ('aaa', 0), ('b', 0), ('bz', 10), ('c', -10),
    ('comm', -10), ('common', -10), ('common', 0), ('common', 10),
    ('commons', 0), ('d', -10), ('e', 0);
drop table mcrparted;

-- check that wholerow vars in the RETURNING list work with partitioned tables
create table returningwrtest (a int) partition by list (a);
create table returningwrtest1 partition of returningwrtest for values in (1);
insert into returningwrtest values (1) returning returningwrtest;
