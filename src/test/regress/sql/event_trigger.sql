-- should fail, return type mismatch
--DDL_STATEMENT_BEGIN--
create event trigger regress_event_trigger
   on ddl_command_start
   execute procedure pg_backend_pid();
--DDL_STATEMENT_END--

-- OK
--DDL_STATEMENT_BEGIN--
create function test_event_trigger() returns event_trigger as $$
BEGIN
    RAISE NOTICE 'test_event_trigger: % %', tg_event, tg_tag;
END
$$ language plpgsql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, event triggers cannot have declared arguments
create function test_event_trigger_arg(name text)
returns event_trigger as $$ BEGIN RETURN 1; END $$ language plpgsql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, SQL functions cannot be event triggers
create function test_event_trigger_sql() returns event_trigger as $$
SELECT 1 $$ language sql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, no elephant_bootstrap entry point
create event trigger regress_event_trigger on elephant_bootstrap
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- OK
create event trigger regress_event_trigger on ddl_command_start
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- OK
create event trigger regress_event_trigger_end on ddl_command_end
   execute function test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, food is not a valid filter variable
create event trigger regress_event_trigger2 on ddl_command_start
   when food in ('sandwich')
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, sandwich is not a valid command tag
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('sandwich')
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, create skunkcabbage is not a valid command tag
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('create table', 'create skunkcabbage')
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, can't have event triggers on event triggers
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('DROP EVENT TRIGGER')
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, can't have event triggers on global objects
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('CREATE ROLE')
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, can't have event triggers on global objects
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('CREATE DATABASE')
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
-- should fail, can't have event triggers on global objects
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('CREATE TABLESPACE')
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--

-- should fail, can't have same filter variable twice
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('create table') and tag in ('CREATE FUNCTION')
   execute procedure test_event_trigger();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--

-- should fail, can't have arguments
create event trigger regress_event_trigger2 on ddl_command_start
   execute procedure test_event_trigger('argument not allowed');
   
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
-- OK
create event trigger regress_event_trigger2 on ddl_command_start
   when tag in ('create table', 'CREATE FUNCTION')
   execute procedure test_event_trigger();
   
--DDL_STATEMENT_END--
-- OK
comment on event trigger regress_event_trigger is 'test comment';

-- drop as non-superuser should fail
--DDL_STATEMENT_BEGIN--
create role regress_evt_user;
--DDL_STATEMENT_END--
set role regress_evt_user;
--DDL_STATEMENT_BEGIN--
create event trigger regress_event_trigger_noperms on ddl_command_start
   execute procedure test_event_trigger();
reset role;
--DDL_STATEMENT_END--

-- test enabling and disabling
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger disable;
--DDL_STATEMENT_END--
-- fires _trigger2 and _trigger_end should fire, but not _trigger
--DDL_STATEMENT_BEGIN--
create table event_trigger_fire1 (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger enable;
--DDL_STATEMENT_END--
set session_replication_role = replica;
-- fires nothing
--DDL_STATEMENT_BEGIN--
create table event_trigger_fire2 (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger enable replica;
--DDL_STATEMENT_END--
-- fires only _trigger
--DDL_STATEMENT_BEGIN--
create table event_trigger_fire3 (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger enable always;
--DDL_STATEMENT_END--
-- fires only _trigger
--DDL_STATEMENT_BEGIN--
create table event_trigger_fire4 (a int);
--DDL_STATEMENT_END--
reset session_replication_role;
-- fires all three
--DDL_STATEMENT_BEGIN--
create table event_trigger_fire5 (a int);
--DDL_STATEMENT_END--
-- clean up
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger disable;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table event_trigger_fire2, event_trigger_fire3, event_trigger_fire4, event_trigger_fire5;
--DDL_STATEMENT_END--

-- regress_event_trigger_end should fire on these commands
--DDL_STATEMENT_BEGIN--
grant all on table event_trigger_fire1 to public;
--DDL_STATEMENT_END--
comment on table event_trigger_fire1 is 'here is a comment';
--DDL_STATEMENT_BEGIN--
revoke all on table event_trigger_fire1 from public;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table event_trigger_fire1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create foreign data wrapper useless;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create server useless_server foreign data wrapper useless;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create user mapping for regress_evt_user server useless_server;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter default privileges for role regress_evt_user
 revoke delete on tables from regress_evt_user;
--DDL_STATEMENT_END--

-- alter owner to non-superuser should fail
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger owner to regress_evt_user;
--DDL_STATEMENT_END--

-- alter owner to superuser should work
--DDL_STATEMENT_BEGIN--
alter role regress_evt_user superuser;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger owner to regress_evt_user;
--DDL_STATEMENT_END--

-- should fail, name collision
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger rename to regress_event_trigger2;
--DDL_STATEMENT_END--

-- OK
--DDL_STATEMENT_BEGIN--
alter event trigger regress_event_trigger rename to regress_event_trigger3;
--DDL_STATEMENT_END--

-- should fail, doesn't exist any more
--DDL_STATEMENT_BEGIN--
drop event trigger regress_event_trigger;
--DDL_STATEMENT_END--

-- should fail, regress_evt_user owns some objects
--DDL_STATEMENT_BEGIN--
drop role regress_evt_user;
--DDL_STATEMENT_END--

-- cleanup before next test
-- these are all OK; the second one should emit a NOTICE
--DDL_STATEMENT_BEGIN--
drop event trigger if exists regress_event_trigger2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop event trigger if exists regress_event_trigger2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop event trigger regress_event_trigger3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop event trigger regress_event_trigger_end;
--DDL_STATEMENT_END--

-- test support for dropped objects
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA schema_one authorization regress_evt_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA schema_two authorization regress_evt_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA audit_tbls authorization regress_evt_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE a_temp_tbl ();
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION regress_evt_user;

--DDL_STATEMENT_BEGIN--
CREATE TABLE schema_one.table_one(a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE schema_one."table two"(a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE schema_one.table_three(a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE audit_tbls.schema_one_table_two(the_value text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE schema_two.table_two(a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE schema_two.table_three(a int, b text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE audit_tbls.schema_two_table_three(the_value text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION schema_two.add(int, int) RETURNS int LANGUAGE plpgsql
  CALLED ON NULL INPUT
  AS $$ BEGIN RETURN coalesce($1,0) + coalesce($2,0); END; $$;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE AGGREGATE schema_two.newton
  (BASETYPE = int, SFUNC = schema_two.add, STYPE = int);
--DDL_STATEMENT_END--

RESET SESSION AUTHORIZATION;

--DDL_STATEMENT_BEGIN--
CREATE TABLE undroppable_objs (
	object_type text,
	object_identity text
);
--DDL_STATEMENT_END--
INSERT INTO undroppable_objs VALUES
('table', 'schema_one.table_three'),
('table', 'audit_tbls.schema_two_table_three');

--DDL_STATEMENT_BEGIN--
CREATE TABLE dropped_objects (
	type text,
	schema text,
	object text
);
--DDL_STATEMENT_END--

-- This tests errors raised within event triggers; the one in audit_tbls
-- uses 2nd-level recursive invocation via test_evtrig_dropped_objects().
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION undroppable() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
	obj record;
BEGIN
	PERFORM 1 FROM pg_tables WHERE tablename = 'undroppable_objs';
	IF NOT FOUND THEN
		RAISE NOTICE 'table undroppable_objs not found, skipping';
		RETURN;
	END IF;
	FOR obj IN
		SELECT * FROM pg_event_trigger_dropped_objects() JOIN
			undroppable_objs USING (object_type, object_identity)
	LOOP
		RAISE EXCEPTION 'object % of type % cannot be dropped',
			obj.object_identity, obj.object_type;
	END LOOP;
END;
$$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE EVENT TRIGGER undroppable ON sql_drop
	EXECUTE PROCEDURE undroppable();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION test_evtrig_dropped_objects() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF obj.object_type = 'table' THEN
                EXECUTE format('DROP TABLE IF EXISTS audit_tbls.%I',
					format('%s_%s', obj.schema_name, obj.object_name));
        END IF;

	INSERT INTO dropped_objects
		(type, schema, object) VALUES
		(obj.object_type, obj.schema_name, obj.object_identity);
    END LOOP;
END
$$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE EVENT TRIGGER regress_event_trigger_drop_objects ON sql_drop
	WHEN TAG IN ('drop table', 'drop function', 'drop view',
		'drop owned', 'drop schema', 'alter table')
	EXECUTE PROCEDURE test_evtrig_dropped_objects();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE schema_one.table_one DROP COLUMN a;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA schema_one, schema_two CASCADE;
--DDL_STATEMENT_END--
DELETE FROM undroppable_objs WHERE object_identity = 'audit_tbls.schema_two_table_three';
--DDL_STATEMENT_BEGIN--
DROP SCHEMA schema_one, schema_two CASCADE;
--DDL_STATEMENT_END--
DELETE FROM undroppable_objs WHERE object_identity = 'schema_one.table_three';
--DDL_STATEMENT_BEGIN--
DROP SCHEMA schema_one, schema_two CASCADE;
--DDL_STATEMENT_END--

SELECT * FROM dropped_objects WHERE schema IS NULL OR schema <> 'pg_toast';

--DDL_STATEMENT_BEGIN--
DROP OWNED BY regress_evt_user;
--DDL_STATEMENT_END--
SELECT * FROM dropped_objects WHERE type = 'schema';

--DDL_STATEMENT_BEGIN--
DROP ROLE regress_evt_user;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP EVENT TRIGGER regress_event_trigger_drop_objects;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP EVENT TRIGGER undroppable;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION event_trigger_report_dropped()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
    FOR r IN SELECT * from pg_event_trigger_dropped_objects()
    LOOP
    IF NOT r.normal AND NOT r.original THEN
        CONTINUE;
    END IF;
    RAISE NOTICE 'NORMAL: orig=% normal=% istemp=% type=% identity=% name=% args=%',
        r.original, r.normal, r.is_temporary, r.object_type,
        r.object_identity, r.address_names, r.address_args;
    END LOOP;
END; $$;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE EVENT TRIGGER regress_event_trigger_report_dropped ON sql_drop
    EXECUTE PROCEDURE event_trigger_report_dropped();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA evttrig
	CREATE TABLE one (col_a SERIAL PRIMARY KEY, col_b text DEFAULT 'forty two')
	CREATE INDEX one_idx ON one (col_b)
	CREATE TABLE two (col_c INTEGER CHECK (col_c > 0) REFERENCES one DEFAULT 42);
--DDL_STATEMENT_END--

-- Partitioned tables with a partitioned index
--DDL_STATEMENT_BEGIN--
CREATE TABLE evttrig.parted (
    id int PRIMARY KEY)
    PARTITION BY RANGE (id);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE evttrig.part_1_10 PARTITION OF evttrig.parted (id)
  FOR VALUES FROM (1) TO (10);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE evttrig.part_10_20 PARTITION OF evttrig.parted (id)
  FOR VALUES FROM (10) TO (20) PARTITION BY RANGE (id);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE evttrig.part_10_15 PARTITION OF evttrig.part_10_20 (id)
  FOR VALUES FROM (10) TO (15);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE evttrig.part_15_20 PARTITION OF evttrig.part_10_20 (id)
  FOR VALUES FROM (15) TO (20);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE evttrig.two DROP COLUMN col_c;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE evttrig.one ALTER COLUMN col_b DROP DEFAULT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE evttrig.one DROP CONSTRAINT one_pkey;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP INDEX evttrig.one_idx;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SCHEMA evttrig CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE a_temp_tbl;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP EVENT TRIGGER regress_event_trigger_report_dropped;
--DDL_STATEMENT_END--

-- only allowed from within an event trigger function, should fail
select pg_event_trigger_table_rewrite_oid();

-- test Table Rewrite Event Trigger
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION test_evtrig_no_rewrite() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'rewrites not allowed';
END;
$$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create event trigger no_rewrite_allowed on table_rewrite
  execute procedure test_evtrig_no_rewrite();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create table rewriteme (id serial primary key, foo float);
--DDL_STATEMENT_END--
insert into rewriteme
     select x * 1.001 from generate_series(1, 500) as t(x);
--DDL_STATEMENT_BEGIN--
alter table rewriteme alter column foo type numeric;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter table rewriteme add column baz int default 0;
--DDL_STATEMENT_END--

-- test with more than one reason to rewrite a single table
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION test_evtrig_no_rewrite() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'Table ''%'' is being rewritten (reason = %)',
               pg_event_trigger_table_rewrite_oid()::regclass,
               pg_event_trigger_table_rewrite_reason();
END;
$$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
alter table rewriteme
 add column onemore int default 0,
 add column another int default -1,
 alter column foo type numeric(10,4);
--DDL_STATEMENT_END--

-- shouldn't trigger a table_rewrite event
--DDL_STATEMENT_BEGIN--
alter table rewriteme alter column foo type numeric(12,4);
--DDL_STATEMENT_END--

-- typed tables are rewritten when their type changes.  Don't emit table
-- name, because firing order is not stable.
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION test_evtrig_no_rewrite() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'Table is being rewritten (reason = %)',
               pg_event_trigger_table_rewrite_reason();
END;
$$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
create type rewritetype as (a int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table rewritemetoo1 of rewritetype;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create table rewritemetoo2 of rewritetype;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter type rewritetype alter attribute a type text cascade;
--DDL_STATEMENT_END--

-- but this doesn't work
--DDL_STATEMENT_BEGIN--
create table rewritemetoo3 (a rewritetype);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
alter type rewritetype alter attribute a type varchar cascade;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table rewriteme;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop event trigger no_rewrite_allowed;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop function test_evtrig_no_rewrite();
--DDL_STATEMENT_END--

-- test Row Security Event Trigger
RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
CREATE TABLE event_trigger_test (a integer, b text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION start_command()
RETURNS event_trigger AS $$
BEGIN
RAISE NOTICE '% - ddl_command_start', tg_tag;
END;
$$ LANGUAGE plpgsql;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION end_command()
RETURNS event_trigger AS $$
BEGIN
RAISE NOTICE '% - ddl_command_end', tg_tag;
END;
$$ LANGUAGE plpgsql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE FUNCTION drop_sql_command()
RETURNS event_trigger AS $$
BEGIN
RAISE NOTICE '% - sql_drop', tg_tag;
END;
$$ LANGUAGE plpgsql;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE EVENT TRIGGER start_rls_command ON ddl_command_start
    WHEN TAG IN ('CREATE POLICY', 'ALTER POLICY', 'DROP POLICY') EXECUTE PROCEDURE start_command();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE EVENT TRIGGER end_rls_command ON ddl_command_end
    WHEN TAG IN ('CREATE POLICY', 'ALTER POLICY', 'DROP POLICY') EXECUTE PROCEDURE end_command();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE EVENT TRIGGER sql_drop_command ON sql_drop
    WHEN TAG IN ('DROP POLICY') EXECUTE PROCEDURE drop_sql_command();
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE POLICY p1 ON event_trigger_test USING (FALSE);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER POLICY p1 ON event_trigger_test USING (TRUE);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER POLICY p1 ON event_trigger_test RENAME TO p2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP POLICY p2 ON event_trigger_test;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP EVENT TRIGGER start_rls_command;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP EVENT TRIGGER end_rls_command;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP EVENT TRIGGER sql_drop_command;
--DDL_STATEMENT_END--