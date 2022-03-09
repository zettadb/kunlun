--
-- PARALLEL
--

-- Serializable isolation would disable parallel query, so explicitly use an
-- arbitrary other level.
begin isolation level repeatable read;

-- encourage use of parallel plans
set parallel_setup_cost=0;
set parallel_tuple_cost=0;
set min_parallel_table_scan_size=0;
set max_parallel_workers_per_gather=4;

--
-- Test write operations that has an underlying query that is eligble
-- for parallel plans
--
explain (costs off) create table parallel_write as
    select length(stringu1) from tenk1 group by length(stringu1);
--DDL_STATEMENT_BEGIN--	
create table parallel_write as
    select length(stringu1) from tenk1 group by length(stringu1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table parallel_write;
--DDL_STATEMENT_END--
explain (costs off) select length(stringu1) into parallel_write
    from tenk1 group by length(stringu1);
select length(stringu1) into parallel_write
    from tenk1 group by length(stringu1);
--DDL_STATEMENT_BEGIN--
drop table parallel_write;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
explain (costs off) create materialized view parallel_mat_view as
    select length(stringu1) from tenk1 group by length(stringu1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create materialized view parallel_mat_view as
    select length(stringu1) from tenk1 group by length(stringu1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop materialized view parallel_mat_view;
--DDL_STATEMENT_END--

prepare prep_stmt as select length(stringu1) from tenk1 group by length(stringu1);
--DDL_STATEMENT_BEGIN--
explain (costs off) create table paralylel_write as execute prep_stmt;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table parallel_write as execute prep_stmt;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table parallel_write;
--DDL_STATEMENT_END--
rollback;
