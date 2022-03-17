--
-- CREATE_TABLE
--

--
-- CLASS DEFINITIONS
--
--DDL_STATEMENT_BEGIN--
drop table if exists hobbies_r;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hobbies_r (
	name		text,
	person 		text
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists equipment_r;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE equipment_r (
	name 		text,
	hobby		text
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists onek;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists tenk1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists tenk2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tenk2 (
	unique1 	int4,
	unique2 	int4,
	two 	 	int4,
	four 		int4,
	ten			int4,
	twenty 		int4,
	hundred 	int4,
	thousand 	int4,
	twothousand int4,
	fivethous 	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);
--DDL_STATEMENT_END--


--DDL_STATEMENT_BEGIN--
drop table if exists person;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE person (
	name 		text,
	age			int4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists emp;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE emp (
	name 		text,
	age			int4,
	salary 		int4,
	manager 	name
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists student;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE student (
	name 		text,
	age			int4,
	gpa 		float8
) ;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists stud_emp;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE stud_emp (
	name 		text,
	age			int4,`
	salary 		int4,
	manager 	name,
	gpa 		float8,
	percent 	int4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists dept;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE dept (
	dname		name,
	mgrname 	text
);
--DDL_STATEMENT_END--

--
-- test the "star" operators a bit more thoroughly -- this time,
-- throw in lots of NULL fields...
--
-- a is the type root
-- b and c inherit from a (one-level single inheritance)
-- d inherits from b and c (two-level multiple inheritance)
-- e inherits from c (two-level single inheritance)
-- f inherits from e (three-level single inheritance)
--
--DDL_STATEMENT_BEGIN--
drop table if exists a_star;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE a_star (
	class		char,
	a 			int4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists aggtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE aggtest (
	a 			int2,
	b			float4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists hash_i4_heap;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hash_i4_heap (
	seqno 		int4,
	random 		int4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists hash_name_heap;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hash_name_heap (
	seqno 		int4,
	random 		name
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists hash_txt_heap;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hash_txt_heap (
	seqno 		int4,
	random 		text
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists hash_f8_heap;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hash_f8_heap (
	seqno		int4,
	random 		float8
);
--DDL_STATEMENT_END--

-- don't include the hash_ovfl_heap stuff in the distribution
-- the data set is too large for what it's worth
--
-- CREATE TABLE hash_ovfl_heap (
--	x			int4,
--	y			int4
-- );
--DDL_STATEMENT_BEGIN--
drop table if exists bt_i4_heap;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE bt_i4_heap (
	seqno 		int4,
	random 		int4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists bt_name_heap;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE bt_name_heap (
	seqno 		name,
	random 		int4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists bt_txt_heap;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE bt_txt_heap (
	seqno 		text,
	random 		int4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists bt_f8_heap;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE bt_f8_heap (
	seqno 		float8,
	random 		int4
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists testjsonb;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testjsonb (
       j jsonb
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE unknowntab (
	u unknown    -- fail
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TYPE unknown_comptype AS (
	u unknown    -- fail
);
--DDL_STATEMENT_END--


-- invalid: non-lowercase quoted reloptions identifiers
-- CREATE TABLE tas_case WITH ("Fillfactor" = 10) AS SELECT 1 a;
--DDL_STATEMENT_BEGIN--
drop table if exists tas_case;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE tas_case (a text);
--DDL_STATEMENT_END--

-- CREATE UNLOGGED TABLE unlogged1 (a int primary key);			-- OK
--DDL_STATEMENT_BEGIN--
CREATE TEMPORARY TABLE unlogged2 (a int primary key);			-- OK
--DDL_STATEMENT_END--
SELECT relname, relkind, relpersistence FROM pg_class WHERE relname ~ '^unlogged\d' ORDER BY relname;
-- REINDEX INDEX unlogged1_pkey;
-- REINDEX INDEX unlogged2_pkey;
SELECT relname, relkind, relpersistence FROM pg_class WHERE relname ~ '^unlogged\d' ORDER BY relname;
--DDL_STATEMENT_BEGIN--
DROP TABLE unlogged2;
--DDL_STATEMENT_END--
-- INSERT INTO unlogged1 VALUES (42);
-- REATE UNLOGGED TABLE public.unlogged2 (a int primary key);		-- also OK
--DDL_STATEMENT_BEGIN--
-- CREATE UNLOGGED TABLE pg_temp.unlogged3 (a int primary key);	-- not OK
CREATE TABLE pg_temp.implicitly_temp (a int primary key);		-- OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE explicitly_temp (a int primary key);			-- also OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE pg_temp.doubly_temp (a int primary key);		-- also OK
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE public.temp_to_perm (a int primary key);		-- not OK
--DDL_STATEMENT_END--
-- DROP TABLE unlogged1;
-- DROP TABLE public.unlogged2;

--
-- Partitioned tables
--

-- cannot use more than 1 column as partition key for list partitioned table
--DDL_STATEMENT_BEGIN--
drop table if exists partitioned;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a1 int,
	a2 int
) PARTITION BY LIST (a1, a2);	-- fail

--DDL_STATEMENT_END--
-- prevent using prohibited expressions in the key
--DDL_STATEMENT_BEGIN--
drop function if exists retset(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION retset (a int) RETURNS SETOF int AS $$ SELECT 1; $$ LANGUAGE SQL IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (retset(a));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION retset(int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE ((avg(a)));
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int,
	b int
) PARTITION BY RANGE ((avg(a) OVER (PARTITION BY b)));
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int
) PARTITION BY LIST ((a LIKE (SELECT 1)));
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (('a'));
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop function if exists const_func ();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION const_func () RETURNS int AS $$ SELECT 1; $$ LANGUAGE SQL IMMUTABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (const_func());
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION const_func();
--DDL_STATEMENT_END--

-- only accept valid partitioning strategy
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
    a int
) PARTITION BY MAGIC (a);
--DDL_STATEMENT_END--

-- specified column must be present in the table
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (b);
--DDL_STATEMENT_END--

-- cannot use system columns in partition key
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (xmin);
--DDL_STATEMENT_END--

-- functions in key must be immutable
--DDL_STATEMENT_BEGIN--
drop function if exists immut_func(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION immut_func (a int) RETURNS int AS $$ SELECT a + random()::int; $$ LANGUAGE SQL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (immut_func(a));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION immut_func(int);
--DDL_STATEMENT_END--

-- cannot contain whole-row references
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a	int
) PARTITION BY RANGE ((partitioned));

--DDL_STATEMENT_END--
-- some checks after successful creation of a partitioned table
--DDL_STATEMENT_BEGIN--
drop function if exists plusone(int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION plusone(a int) RETURNS INT AS $$ SELECT a+1; $$ LANGUAGE SQL;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned (
	a int,
	b int,
	c text,
	d text
) PARTITION BY RANGE (a oid_ops, plusone(b), c collate "default", d collate "C");
--DDL_STATEMENT_END--

-- check relkind
SELECT relkind FROM pg_class WHERE relname = 'partitioned';

-- prevent a function referenced in partition key from being dropped
--DDL_STATEMENT_BEGIN--
DROP FUNCTION plusone(int);
--DDL_STATEMENT_END--

-- partitioned table cannot participate in regular inheritance
--DDL_STATEMENT_BEGIN--
drop table if exists partitioned2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE partitioned2 (
	a int,
	b text
) PARTITION BY RANGE ((a+1), substr(b, 1, 5));
--DDL_STATEMENT_END--

-- Partition key in describe output
\d partitioned
\d+ partitioned2

INSERT INTO partitioned2 VALUES (1, 'hello');
--DDL_STATEMENT_BEGIN--
CREATE TABLE part2_1 PARTITION OF partitioned2 FOR VALUES FROM (-1, 'aaaaa') TO (100, 'ccccc');
--DDL_STATEMENT_END--
\d+ part2_1

--DDL_STATEMENT_BEGIN--
DROP TABLE partitioned;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE partitioned2;
--DDL_STATEMENT_END--

--
-- Partitions
--

-- check partition bound syntax

--DDL_STATEMENT_BEGIN--
drop table if exists list_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE list_parted (
	a int
) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
-- syntax allows only string literal, numeric literal and null to be
-- specified for a partition bound value
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_1 PARTITION OF list_parted FOR VALUES IN ('1');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_2 PARTITION OF list_parted FOR VALUES IN (2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_null PARTITION OF list_parted FOR VALUES IN (null);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN (int '1');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN ('1'::int);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- syntax does not allow empty list of values for list partitions
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN ();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- trying to specify range for list partitioned table
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES FROM (1) TO (2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- trying to specify modulus and remainder for list partitioned table
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES WITH (MODULUS 10, REMAINDER 1);
--DDL_STATEMENT_END--

-- check default partition cannot be created more than once
--CREATE TABLE part_default PARTITION OF list_parted DEFAULT;
--CREATE TABLE fail_default_part PARTITION OF list_parted DEFAULT;

-- specified literal can't be cast to the partition column data type
--DDL_STATEMENT_BEGIN--
drop table if exists bools;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE bools (
	a bool
) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE bools_true PARTITION OF bools FOR VALUES IN (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE bools;
--DDL_STATEMENT_END--

-- specified literal can be cast, but cast isn't immutable
--DDL_STATEMENT_BEGIN--
drop table if exists moneyp;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE moneyp (
	a money
) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE moneyp_10 PARTITION OF moneyp FOR VALUES IN (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE moneyp_10 PARTITION OF moneyp FOR VALUES IN ('10');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE moneyp;
--DDL_STATEMENT_END--

-- immutable cast should work, though
--DDL_STATEMENT_BEGIN--
CREATE TABLE bigintp (
	a bigint
) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE bigintp_10 PARTITION OF bigintp FOR VALUES IN (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- fails due to overlap:
CREATE TABLE bigintp_10_2 PARTITION OF bigintp FOR VALUES IN ('10');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE bigintp;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE range_parted (
	a date
) PARTITION BY RANGE (a);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- trying to specify list for range partitioned table
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES IN ('a');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- trying to specify modulus and remainder for range partitioned table
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES WITH (MODULUS 10, REMAINDER 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- each of start and end bounds must have same number of values as the
-- length of the partition key
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('z');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES FROM ('a') TO ('z', 1);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- cannot specify null values in range bounds
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES FROM (null) TO (maxvalue);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- trying to specify modulus and remainder for range partitioned table
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES WITH (MODULUS 10, REMAINDER 1);
--DDL_STATEMENT_END--

-- check partition bound syntax for the hash partition
--DDL_STATEMENT_BEGIN--
dropt table if exists hash_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hash_parted (
	a int
) PARTITION BY HASH (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hpart_1 PARTITION OF hash_parted FOR VALUES WITH (MODULUS 10, REMAINDER 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hpart_2 PARTITION OF hash_parted FOR VALUES WITH (MODULUS 50, REMAINDER 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE hpart_3 PARTITION OF hash_parted FOR VALUES WITH (MODULUS 200, REMAINDER 2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- modulus 25 is factor of modulus of 50 but 10 is not factor of 25.
CREATE TABLE fail_part PARTITION OF hash_parted FOR VALUES WITH (MODULUS 25, REMAINDER 3);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- previous modulus 50 is factor of 150 but this modulus is not factor of next modulus 200.
CREATE TABLE fail_part PARTITION OF hash_parted FOR VALUES WITH (MODULUS 150, REMAINDER 3);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- trying to specify range for the hash partitioned table
CREATE TABLE fail_part PARTITION OF hash_parted FOR VALUES FROM ('a', 1) TO ('z');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- trying to specify list value for the hash partitioned table
CREATE TABLE fail_part PARTITION OF hash_parted FOR VALUES IN (1000);
--DDL_STATEMENT_END--

-- trying to create default partition for the hash partitioned table
--CREATE TABLE fail_default_part PARTITION OF hash_parted DEFAULT;

-- check if compatible with the specified parent

--DDL_STATEMENT_BEGIN--
-- cannot create as partition of a non-partitioned table
CREATE TABLE unparted (
	a int
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF unparted FOR VALUES IN ('a');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF unparted FOR VALUES WITH (MODULUS 2, REMAINDER 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE unparted;
--DDL_STATEMENT_END--

-- cannot create a permanent rel as partition of a temp rel
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE temp_parted (
	a int
) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF temp_parted FOR VALUES IN ('a');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE temp_parted;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- cannot create a table with oids as partition of table without oids
CREATE TABLE no_oids_parted (
	a int
) PARTITION BY RANGE (a) WITHOUT OIDS;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF no_oids_parted FOR VALUES FROM (1) TO (10);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP TABLE no_oids_parted;
--DDL_STATEMENT_END--
-- If the partitioned table has oids, then the partition must have them.
-- If the WITHOUT OIDS option is specified for partition, it is overridden.
--DDL_STATEMENT_BEGIN--
CREATE TABLE oids_parted (
	a int
) PARTITION BY RANGE (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_forced_oids PARTITION OF oids_parted FOR VALUES FROM (1) TO (10) WITHOUT OIDS;
--DDL_STATEMENT_END--
\d+ part_forced_oids
--DDL_STATEMENT_BEGIN--
DROP TABLE oids_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE part_forced_oids;
--DDL_STATEMENT_END--

-- check for partition bound overlap and other invalid specifications

--DDL_STATEMENT_BEGIN--
CREATE TABLE list_parted2 (
	a varchar
) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_null_z PARTITION OF list_parted2 FOR VALUES IN (null, 'z');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_ab PARTITION OF list_parted2 FOR VALUES IN ('a', 'b');
--DDL_STATEMENT_END--
--CREATE TABLE list_parted2_def PARTITION OF list_parted2 DEFAULT;

--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF list_parted2 FOR VALUES IN (null);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF list_parted2 FOR VALUES IN ('b', 'c');
--DDL_STATEMENT_END--
-- check default partition overlap
INSERT INTO list_parted2 VALUES('X');
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF list_parted2 FOR VALUES IN ('W', 'X', 'Y');
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE range_parted2 (
	a int
) PARTITION BY RANGE (a);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- trying to create range partition with empty range
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (1) TO (0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- note that the range '[1, 1)' has no elements
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (1) TO (1);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE part0 PARTITION OF range_parted2 FOR VALUES FROM (minvalue) TO (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (minvalue) TO (2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part1 PARTITION OF range_parted2 FOR VALUES FROM (1) TO (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (9) TO (maxvalue);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part2 PARTITION OF range_parted2 FOR VALUES FROM (20) TO (30);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part3 PARTITION OF range_parted2 FOR VALUES FROM (30) TO (40);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (10) TO (30);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (10) TO (50);
--DDL_STATEMENT_END--

-- Create a default partition for range partitioned table
--CREATE TABLE range2_default PARTITION OF range_parted2 DEFAULT;

-- More than one default partition is not allowed, so this should give error
--CREATE TABLE fail_default_part PARTITION OF range_parted2 DEFAULT;

-- Check if the range for default partitions overlap
INSERT INTO range_parted2 VALUES (85);
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (80) TO (90);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part4 PARTITION OF range_parted2 FOR VALUES FROM (90) TO (100);
--DDL_STATEMENT_END--

-- now check for multi-column range partition key
--DDL_STATEMENT_BEGIN--
CREATE TABLE range_parted3 (
	a int,
	b int
) PARTITION BY RANGE (a, (b+1));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part00 PARTITION OF range_parted3 FOR VALUES FROM (0, minvalue) TO (0, maxvalue);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (0, minvalue) TO (0, 1);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE part10 PARTITION OF range_parted3 FOR VALUES FROM (1, minvalue) TO (1, 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part11 PARTITION OF range_parted3 FOR VALUES FROM (1, 1) TO (1, 10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part12 PARTITION OF range_parted3 FOR VALUES FROM (1, 10) TO (1, maxvalue);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (1, 10) TO (1, 20);
--DDL_STATEMENT_END--
--CREATE TABLE range3_default PARTITION OF range_parted3 DEFAULT;

-- cannot create a partition that says column b is allowed to range
-- from -infinity to +infinity, while there exist partitions that have
-- more specific ranges
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (1, minvalue) TO (1, maxvalue);
--DDL_STATEMENT_END--

-- check for partition bound overlap and other invalid specifications for the hash partition
--DDL_STATEMENT_BEGIN--
CREATE TABLE hash_parted2 (
	a varchar
) PARTITION BY HASH (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE h2part_1 PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 4, REMAINDER 2);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE h2part_2 PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 8, REMAINDER 0);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE h2part_3 PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 8, REMAINDER 4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE h2part_4 PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 8, REMAINDER 5);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- overlap with part_4
CREATE TABLE fail_part PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- modulus must be greater than zero
CREATE TABLE fail_part PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 0, REMAINDER 1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- remainder must be greater than or equal to zero and less than modulus
CREATE TABLE fail_part PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 8, REMAINDER 8);
--DDL_STATEMENT_END--

-- check schema propagation from parent

--DDL_STATEMENT_BEGIN--
CREATE TABLE parted (
	a text,
	b int NOT NULL DEFAULT 0
) PARTITION BY LIST (a);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE part_a PARTITION OF parted FOR VALUES IN ('a');

--DDL_STATEMENT_END--
-- only inherited attributes (never local ones)
SELECT attname, attislocal, attinhcount FROM pg_attribute
  WHERE attrelid = 'part_a'::regclass and attnum > 0
  ORDER BY attnum;

-- able to specify column default, column constraint, and table constraint

-- first check the "column specified more than once" error
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_b PARTITION OF parted (
	b NOT NULL,
	b DEFAULT 1
) FOR VALUES IN ('b');
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE part_b PARTITION OF parted (
	b NOT NULL DEFAULT 1
) FOR VALUES IN ('b');
--DDL_STATEMENT_END--
-- conislocal should be false for any merged constraints
SELECT conislocal, coninhcount FROM pg_constraint WHERE conrelid = 'part_b'::regclass AND conname = 'check_a';

-- specify PARTITION BY for a partition
--DDL_STATEMENT_BEGIN--
CREATE TABLE fail_part_col_not_found PARTITION OF parted FOR VALUES IN ('c') PARTITION BY RANGE (c);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_c PARTITION OF parted (b WITH OPTIONS NOT NULL DEFAULT 0) FOR VALUES IN ('c') PARTITION BY RANGE ((b));

--DDL_STATEMENT_END--
-- create a level-2 partition
--DDL_STATEMENT_BEGIN--
CREATE TABLE part_c_1_10 PARTITION OF part_c FOR VALUES FROM (1) TO (10);

--DDL_STATEMENT_END--
-- check that NOT NULL and default value are inherited correctly
--DDL_STATEMENT_BEGIN--
create table parted_notnull_inh_test (a int default 1, b int not null default 0) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parted_notnull_inh_test1 partition of parted_notnull_inh_test (a not null, b default 1) for values in (1);
--DDL_STATEMENT_END--
insert into parted_notnull_inh_test (b) values (null);
-- note that while b's default is overriden, a's default is preserved
\d parted_notnull_inh_test1
--DDL_STATEMENT_BEGIN--
drop table parted_notnull_inh_test;
--DDL_STATEMENT_END--

-- check for a conflicting COLLATE clause
--DDL_STATEMENT_BEGIN--
create table parted_collate_must_match (a text collate "C", b text collate "C")
  partition by range (a);
--DDL_STATEMENT_END--
-- on the partition key
--DDL_STATEMENT_BEGIN--
create table parted_collate_must_match1 partition of parted_collate_must_match
  (a collate "POSIX") for values from ('a') to ('m');
--DDL_STATEMENT_END--
-- on another column
--DDL_STATEMENT_BEGIN--
create table parted_collate_must_match2 partition of parted_collate_must_match
  (b collate "POSIX") for values from ('m') to ('z');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table parted_collate_must_match;
--DDL_STATEMENT_END--

-- Partition bound in describe output
\d+ part_b

-- Both partition bound and partition key in describe output
\d+ part_c

-- a level-2 partition's constraint will include the parent's expressions
\d+ part_c_1_10

-- Show partition count in the parent's describe output
-- Tempted to include \d+ output listing partitions with bound info but
-- output could vary depending on the order in which partition oids are
-- returned.
\d parted
\d hash_parted

-- check that we get the expected partition constraints
--DDL_STATEMENT_BEGIN--
CREATE TABLE range_parted4 (a int, b int, c int) PARTITION BY RANGE (abs(a), abs(b), c);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE unbounded_range_part PARTITION OF range_parted4 FOR VALUES FROM (MINVALUE, MINVALUE, MINVALUE) TO (MAXVALUE, MAXVALUE, MAXVALUE);
--DDL_STATEMENT_END--
\d+ unbounded_range_part
--DDL_STATEMENT_BEGIN--
DROP TABLE unbounded_range_part;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE range_parted4_1 PARTITION OF range_parted4 FOR VALUES FROM (MINVALUE, MINVALUE, MINVALUE) TO (1, MAXVALUE, MAXVALUE);
--DDL_STATEMENT_END--
\d+ range_parted4_1
--DDL_STATEMENT_BEGIN--
CREATE TABLE range_parted4_2 PARTITION OF range_parted4 FOR VALUES FROM (3, 4, 5) TO (6, 7, MAXVALUE);
--DDL_STATEMENT_END--
\d+ range_parted4_2
--DDL_STATEMENT_BEGIN--
CREATE TABLE range_parted4_3 PARTITION OF range_parted4 FOR VALUES FROM (6, 8, MINVALUE) TO (9, MAXVALUE, MAXVALUE);
--DDL_STATEMENT_END--
\d+ range_parted4_3
--DDL_STATEMENT_BEGIN--
DROP TABLE range_parted4;
--DDL_STATEMENT_END--

-- user-defined operator class in partition key
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION my_int4_sort(int4,int4) RETURNS int LANGUAGE sql
  AS $$ SELECT CASE WHEN $1 = $2 THEN 0 WHEN $1 > $2 THEN 1 ELSE -1 END; $$;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OPERATOR CLASS test_int4_ops FOR TYPE int4 USING btree AS
  OPERATOR 1 < (int4,int4), OPERATOR 2 <= (int4,int4),
  OPERATOR 3 = (int4,int4), OPERATOR 4 >= (int4,int4),
  OPERATOR 5 > (int4,int4), FUNCTION 1 my_int4_sort(int4,int4);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE partkey_t (a int4) PARTITION BY RANGE (a test_int4_ops);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE partkey_t_1 PARTITION OF partkey_t FOR VALUES FROM (0) TO (1000);
--DDL_STATEMENT_END--
INSERT INTO partkey_t VALUES (100);
INSERT INTO partkey_t VALUES (200);

-- cleanup
--DDL_STATEMENT_BEGIN--
DROP TABLE parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE list_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE range_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE list_parted2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE range_parted2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE range_parted3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE partkey_t;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE hash_parted;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE hash_parted2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP OPERATOR CLASS test_int4_ops USING btree;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION my_int4_sort(int4,int4);
--DDL_STATEMENT_END--

-- comments on partitioned tables columns
--DDL_STATEMENT_BEGIN--
CREATE TABLE parted_col_comment (a int, b text) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
COMMENT ON TABLE parted_col_comment IS 'Am partitioned table';
COMMENT ON COLUMN parted_col_comment.a IS 'Partition key';
SELECT obj_description('parted_col_comment'::regclass);
\d+ parted_col_comment
--DDL_STATEMENT_BEGIN--
DROP TABLE parted_col_comment;
--DDL_STATEMENT_END--

-- partition on boolean column
--DDL_STATEMENT_BEGIN--
create table boolspart (a bool) partition by list (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table boolspart_t partition of boolspart for values in (true);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table boolspart_f partition of boolspart for values in (false);
--DDL_STATEMENT_END--
\d+ boolspart
--DDL_STATEMENT_BEGIN--
drop table boolspart;
--DDL_STATEMENT_END--
