--
-- Test GiST indexes.
--
-- There are other tests to test different GiST opclasses. This is for
-- testing GiST code itself. Vacuuming in particular.

--DDL_STATEMENT_BEGIN--
create table gist_point_tbl(id int4, p point);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index gist_pointidx on gist_point_tbl using gist(p);
--DDL_STATEMENT_END--

-- Verify the fillfactor and buffering options
--DDL_STATEMENT_BEGIN--
create index gist_pointidx2 on gist_point_tbl using gist(p) with (buffering = on, fillfactor=50);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index gist_pointidx3 on gist_point_tbl using gist(p) with (buffering = off);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index gist_pointidx4 on gist_point_tbl using gist(p) with (buffering = auto);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop index gist_pointidx2, gist_pointidx3, gist_pointidx4;
--DDL_STATEMENT_END--

-- Make sure bad values are refused
--DDL_STATEMENT_BEGIN--
create index gist_pointidx5 on gist_point_tbl using gist(p) with (buffering = invalid_value);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index gist_pointidx5 on gist_point_tbl using gist(p) with (fillfactor=9);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create index gist_pointidx5 on gist_point_tbl using gist(p) with (fillfactor=101);
--DDL_STATEMENT_END--

-- Insert enough data to create a tree that's a couple of levels deep.
insert into gist_point_tbl (id, p)
select g,        point(g*10, g*10) from generate_series(1, 10000) g;

insert into gist_point_tbl (id, p)
select g+100000, point(g*10+1, g*10+1) from generate_series(1, 10000) g;

-- To test vacuum, delete some entries from all over the index.
delete from gist_point_tbl where id % 2 = 1;

-- And also delete some concentration of values. (GiST doesn't currently
-- attempt to delete pages even when they become empty, but if it did, this
-- would exercise it)
delete from gist_point_tbl where id < 10000;

vacuum analyze gist_point_tbl;

-- rebuild the index with a different fillfactor
--DDL_STATEMENT_BEGIN--
alter index gist_pointidx SET (fillfactor = 40);
--DDL_STATEMENT_END--
reindex index gist_pointidx;

--
-- Test Index-only plans on GiST indexes
--

--DDL_STATEMENT_BEGIN--
create table gist_tbl (b box, p point, c circle);
--DDL_STATEMENT_END--

insert into gist_tbl
select box(point(0.05*i, 0.05*i), point(0.05*i, 0.05*i)),
       point(0.05*i, 0.05*i),
       circle(point(0.05*i, 0.05*i), 1.0)
from generate_series(0,10000) as i;

vacuum analyze gist_tbl;

set enable_seqscan=off;
set enable_bitmapscan=off;
set enable_indexonlyscan=on;

-- Test index-only scan with point opclass
--DDL_STATEMENT_BEGIN--
create index gist_tbl_point_index on gist_tbl using gist (p);
--DDL_STATEMENT_END--
-- check that the planner chooses an index-only scan
explain (costs off)
select p from gist_tbl where p <@ box(point(0,0), point(0.5, 0.5));

-- execute the same
select p from gist_tbl where p <@ box(point(0,0), point(0.5, 0.5));

-- Also test an index-only knn-search
explain (costs off)
select p from gist_tbl where p <@ box(point(0,0), point(0.5, 0.5))
order by p <-> point(0.201, 0.201);

select p from gist_tbl where p <@ box(point(0,0), point(0.5, 0.5))
order by p <-> point(0.201, 0.201);

-- Check commuted case as well
explain (costs off)
select p from gist_tbl where p <@ box(point(0,0), point(0.5, 0.5))
order by point(0.101, 0.101) <-> p;

select p from gist_tbl where p <@ box(point(0,0), point(0.5, 0.5))
order by point(0.101, 0.101) <-> p;

-- Check case with multiple rescans (bug #14641)
explain (costs off)
select p from
  (values (box(point(0,0), point(0.5,0.5))),
          (box(point(0.5,0.5), point(0.75,0.75))),
          (box(point(0.8,0.8), point(1.0,1.0)))) as v(bb)
cross join lateral
  (select p from gist_tbl where p <@ bb order by p <-> bb[0] limit 2) ss;

select p from
  (values (box(point(0,0), point(0.5,0.5))),
          (box(point(0.5,0.5), point(0.75,0.75))),
          (box(point(0.8,0.8), point(1.0,1.0)))) as v(bb)
cross join lateral
  (select p from gist_tbl where p <@ bb order by p <-> bb[0] limit 2) ss;
  
--DDL_STATEMENT_BEGIN--
drop index gist_tbl_point_index;
--DDL_STATEMENT_END--

-- Test index-only scan with box opclass
--DDL_STATEMENT_BEGIN--
create index gist_tbl_box_index on gist_tbl using gist (b);
--DDL_STATEMENT_END--

-- check that the planner chooses an index-only scan
explain (costs off)
select b from gist_tbl where b <@ box(point(5,5), point(6,6));

-- execute the same
select b from gist_tbl where b <@ box(point(5,5), point(6,6));

--DDL_STATEMENT_BEGIN--
drop index gist_tbl_box_index;
--DDL_STATEMENT_END--

-- Test that an index-only scan is not chosen, when the query involves the
-- circle column (the circle opclass does not support index-only scans).
--DDL_STATEMENT_BEGIN--
create index gist_tbl_multi_index on gist_tbl using gist (p, c);
--DDL_STATEMENT_END--

explain (costs off)
select p, c from gist_tbl
where p <@ box(point(5,5), point(6, 6));

-- execute the same
select b, p from gist_tbl
where b <@ box(point(4.5, 4.5), point(5.5, 5.5))
and p <@ box(point(5,5), point(6, 6));

--DDL_STATEMENT_BEGIN--
drop index gist_tbl_multi_index;
--DDL_STATEMENT_END--

-- Clean up
reset enable_seqscan;
reset enable_bitmapscan;
reset enable_indexonlyscan;

--DDL_STATEMENT_BEGIN--
drop table gist_tbl;
--DDL_STATEMENT_END--