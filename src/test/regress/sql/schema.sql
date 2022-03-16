-- All alter table operation of changing a column type is commented currently.
-- create a schema we can use
--DDL_STATEMENT_BEGIN--
CREATE SCHEMA testschema;
--DDL_STATEMENT_END--

-- try a table
--DDL_STATEMENT_BEGIN--
CREATE TABLE testschema.foo (i int);
--DDL_STATEMENT_END--
INSERT INTO testschema.foo VALUES(1);
INSERT INTO testschema.foo VALUES(2);

-- index
--DDL_STATEMENT_BEGIN--
CREATE INDEX foo_idx on testschema.foo(i);
--DDL_STATEMENT_END--
-- partitioned index
--DDL_STATEMENT_BEGIN--
CREATE TABLE testschema.part (a int) PARTITION BY LIST (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testschema.part1 PARTITION OF testschema.part FOR VALUES IN (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX part_a_idx ON testschema.part (a);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testschema.part2 PARTITION OF testschema.part FOR VALUES IN (2);
--DDL_STATEMENT_END--

-- two indexes
--DDL_STATEMENT_BEGIN--
CREATE TABLE testschema.test_default_tab(id bigint);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
INSERT INTO testschema.test_default_tab VALUES (1);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX test_index1 on testschema.test_default_tab (id);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX test_index2 on testschema.test_default_tab (id);
--DDL_STATEMENT_END--
-- ALTER TABLE testschema.test_default_tab ALTER id TYPE bigint;

SELECT * FROM testschema.test_default_tab;
-- ALTER TABLE testschema.test_default_tab ALTER id TYPE int;
SELECT * FROM testschema.test_default_tab;
-- ALTER TABLE testschema.test_default_tab ALTER id TYPE bigint
--DDL_STATEMENT_BEGIN--
DROP TABLE testschema.test_default_tab;
--DDL_STATEMENT_END--

-- constraint
--DDL_STATEMENT_BEGIN--
CREATE TABLE testschema.test_tab(id int);
--DDL_STATEMENT_END--
INSERT INTO testschema.test_tab VALUES (1);
--DDL_STATEMENT_BEGIN--
ALTER TABLE testschema.test_tab ADD CONSTRAINT test_tab_unique UNIQUE (id);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE testschema.test_tab ADD CONSTRAINT test_tab_pkey PRIMARY KEY (id);
--DDL_STATEMENT_END--
SELECT * FROM testschema.test_tab;
--DDL_STATEMENT_BEGIN--
DROP TABLE testschema.test_tab;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE testschema.tablespace_acl (c int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX k ON testschema.tablespace_acl (c);
--DDL_STATEMENT_END--
-- ALTER TABLE testschema.tablespace_acl ALTER c TYPE bigint;
--DDL_STATEMENT_BEGIN--

drop table testschema.foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table testschema.part;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table testschema.tablespace_acl;
--DDL_STATEMENT_END--
-- drop schema cascade does not work, we need to drop table manually.
--DDL_STATEMENT_BEGIN--
DROP SCHEMA testschema CASCADE;
--DDL_STATEMENT_END--
