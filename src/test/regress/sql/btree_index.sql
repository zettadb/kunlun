--
-- BTREE_INDEX
-- test retrieval of min/max keys for each index
--

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno < 1;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno >= 9999;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno = 4500;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno < '1'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno >= '9999'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno = '4500'::name;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno < '1'::varchar(50);

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno >= '9999'::varchar(50);

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno = '4500'::varchar(50);

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno < '1'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno >= '9999'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno = '4500'::float8;

--
-- Check correct optimization of LIKE (special index operator support)
-- for both indexscan and bitmapscan cases
--

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;

set enable_indexscan to false;
set enable_bitmapscan to true;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;

--
-- Test B-tree page deletion. In particular, deleting a non-leaf page.
--

-- First create a tree that's at least four levels deep. The text inserted
-- is long and poorly compressible. That way only a few index tuples fit on
-- each page, allowing us to get a tall tree with fewer pages.
drop table if exists btree_tall_tbl;
create table btree_tall_tbl(id int4, t varchar(50));
create index btree_tall_idx on btree_tall_tbl (id, t);
insert into btree_tall_tbl
  select g, g::varchar(50) || '_' ||
          (select string_agg(md5(i::varchar(50)), '_') from generate_series(1, 50) i)
from generate_series(1, 100) g;

-- Delete most entries, and vacuum. This causes page deletions.
delete from btree_tall_tbl where id < 950;

