
--
-- COPY
--

-- CLASS POPULATION
--	(any resemblance to real life is purely coincidental)
--
COPY aggtest FROM '/home/kunlun/pgregressdata/data/agg.data';

COPY onek FROM '/home/kunlun/pgregressdata/data/onek.data';
COPY onek TO '/home/kunlun/pgregressdata/results/onek.data';
DELETE FROM onek;

COPY onek FROM '/home/kunlun/pgregressdata/results/onek.data';

COPY tenk1 FROM '/home/kunlun/pgregressdata/data/tenk.data';


CREATE temp TABLE  slow_emp4000 (home_base box);
COPY slow_emp4000 FROM '/home/kunlun/pgregressdata/data/rect.data';

COPY person FROM '/home/kunlun/pgregressdata/data/person.data';

COPY emp FROM '/home/kunlun/pgregressdata/data/emp.data';

COPY student FROM '/home/kunlun/pgregressdata/data/student.data';

COPY stud_emp FROM '/home/kunlun/pgregressdata/data/stud_emp.data';


CREATE temp TABLE road (name	text,thepath 	path);
COPY road FROM '/home/kunlun/pgregressdata/data/streets.data';


CREATE  temp TABLE real_city (
	pop			int4,
	cname		text,
	outline 	path
);
COPY real_city FROM '/home/kunlun/pgregressdata/data/real_city.data';


COPY hash_i4_heap FROM '/home/kunlun/pgregressdata/data/hash.data';

COPY hash_name_heap FROM '/home/kunlun/pgregressdata/data/hash.data';

COPY hash_txt_heap FROM '/home/kunlun/pgregressdata/data/hash.data';

COPY hash_f8_heap FROM '/home/kunlun/pgregressdata/data/hash.data';


CREATE temp TABLE IF NOT EXISTS test_tsvector(t text,a tsvector);
COPY test_tsvector FROM '/home/kunlun/pgregressdata/data/tsearch.data';


COPY testjsonb FROM '/home/kunlun/pgregressdata/data/jsonb.data';

-- the data in this file has a lot of duplicates in the index key
-- fields, leading to long bucket chains and lots of table expansion.
-- this is therefore a stress test of the bucket overflow code (unlike
-- the data in hash.data, which has unique index keys).
--
-- COPY hash_ovfl_heap FROM '/home/kunlun/pgregressdata/data/hashovfl.data';

COPY bt_i4_heap FROM '/home/kunlun/pgregressdata/data/desc.data';

COPY bt_name_heap FROM '/home/kunlun/pgregressdata/data/hash.data';

COPY bt_txt_heap FROM '/home/kunlun/pgregressdata/data/desc.data';

COPY bt_f8_heap FROM '/home/kunlun/pgregressdata/data/hash.data';


CREATE temp TABLE array_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
);
COPY array_op_test FROM '/home/kunlun/pgregressdata/data/array.data';



CREATE temp  TABLE array_index_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
);
COPY array_index_op_test FROM '/home/kunlun/pgregressdata/data/array.data';

--- test copying in CSV mode with various styles
--- of embedded line ending characters

create temp table copytest (
	style	text,
	test 	text,
	filler	int);

insert into copytest values('DOS',E'abc\r\ndef',1);
insert into copytest values('Unix',E'abc\ndef',2);
insert into copytest values('Mac',E'abc\rdef',3);
insert into copytest values(E'esc\\ape',E'a\\r\\\r\\\n\\nb',4);

copy copytest to '/home/kunlun/pgregressdata/results/copytest.csv' csv;

create temp table copytest2 (like copytest);

copy copytest2 from '/home/kunlun/pgregressdata/results/copytest.csv' csv;

select * from copytest except select * from copytest2;

--truncate copytest2;

--- same test but with an escape char different from quote char

copy copytest to '/home/kunlun/pgregressdata/results/copytest.csv' csv quote '''' escape E'\\';

copy copytest2 from '/home/kunlun/pgregressdata/results/copytest.csv' csv quote '''' escape E'\\';

select * from copytest except select * from copytest2;


-- test header line feature

create temp table copytest3 (
	c1 int,
	"col with , comma" text,
	"col with "" quote"  int);

copy copytest3 from stdin csv header;
this is just a line full of junk that would error out if parsed
1,a,1
2,b,2
\.

copy copytest3 to stdout csv header;

-- test copy from with a partitioned table
create table parted_copytest (
	a int,
	b int,
	c text
) partition by list (b);

create table parted_copytest_a1 partition of parted_copytest for values in (1);
create table parted_copytest_a2 partition of parted_copytest for values in (2);
--alter table parted_copytest attach partition parted_copytest_a1 for values in(1);
--alter table parted_copytest attach partition parted_copytest_a2 for values in(2);

-- We must insert enough rows to trigger multi-inserts.  These are only
-- enabled adaptively when there are few enough partition changes.
insert into parted_copytest select x,1,'One' from generate_series(1,1000) x;	
insert into parted_copytest select x,2,'Two' from generate_series(1001,1010) x;
insert into parted_copytest select x,1,'One' from generate_series(1011,1020) x;

copy (select * from parted_copytest order by a) to '/home/kunlun/pgregressdata/results/parted_copytest.csv';

-- Ensure COPY FREEZE errors for partitioned tables.
begin;
--truncate parted_copytest;
copy parted_copytest from '/home/kunlun/pgregressdata/results/parted_copytest.csv' (freeze);
rollback;

drop table parted_copytest;