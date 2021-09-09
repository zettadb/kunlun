-- All alter table operation of changing a column type is commented currently.
-- create a schema we can use
CREATE SCHEMA testschema;

-- try a table
CREATE TABLE testschema.foo (i int);
INSERT INTO testschema.foo VALUES(1);
INSERT INTO testschema.foo VALUES(2);

-- index
CREATE INDEX foo_idx on testschema.foo(i);
-- partitioned index
CREATE TABLE testschema.part (a int) PARTITION BY LIST (a);
CREATE TABLE testschema.part1 PARTITION OF testschema.part FOR VALUES IN (1);
CREATE INDEX part_a_idx ON testschema.part (a);
CREATE TABLE testschema.part2 PARTITION OF testschema.part FOR VALUES IN (2);

-- two indexes
CREATE TABLE testschema.test_default_tab(id bigint);
INSERT INTO testschema.test_default_tab VALUES (1);
CREATE INDEX test_index1 on testschema.test_default_tab (id);
CREATE INDEX test_index2 on testschema.test_default_tab (id);
-- ALTER TABLE testschema.test_default_tab ALTER id TYPE bigint;

SELECT * FROM testschema.test_default_tab;
-- ALTER TABLE testschema.test_default_tab ALTER id TYPE int;
SELECT * FROM testschema.test_default_tab;
-- ALTER TABLE testschema.test_default_tab ALTER id TYPE bigint;
DROP TABLE testschema.test_default_tab;

-- constraint
CREATE TABLE testschema.test_tab(id int);
INSERT INTO testschema.test_tab VALUES (1);
ALTER TABLE testschema.test_tab ADD CONSTRAINT test_tab_unique UNIQUE (id);
ALTER TABLE testschema.test_tab ADD CONSTRAINT test_tab_pkey PRIMARY KEY (id);
SELECT * FROM testschema.test_tab;
DROP TABLE testschema.test_tab;

CREATE TABLE testschema.tablespace_acl (c int);
CREATE INDEX k ON testschema.tablespace_acl (c);
-- ALTER TABLE testschema.tablespace_acl ALTER c TYPE bigint;

drop table testschema.foo;
drop table testschema.part;
drop table testschema.tablespace_acl;
-- drop schema cascade does not work, we need to drop table manually.
DROP SCHEMA testschema CASCADE;
