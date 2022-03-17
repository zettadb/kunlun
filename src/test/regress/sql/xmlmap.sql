--DDL_STATEMENT_BEGIN--
CREATE SCHEMA if not exists testxmlschema;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop table if exists  testxmlschema.test1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists  testxmlschema.test2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE testxmlschema.test1 (a int, b text);
--DDL_STATEMENT_END--
INSERT INTO testxmlschema.test1 VALUES (1, 'one'), (2, 'two'), (-1, null);
--DDL_STATEMENT_BEGIN--
CREATE TABLE testxmlschema.test2 (z int, y varchar(500), x char(6), w numeric(9,2), v smallint, u bigint, t real, s time, r timestamp, q date, p xml, n bool, m bytea, aaa text);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE testxmlschema.test2 DROP COLUMN aaa;
--DDL_STATEMENT_END--
INSERT INTO testxmlschema.test2 VALUES (55, 'abc', 'def', 98.6, 2, 999, 0, '21:07', '2009-06-08 21:07:30', '2009-06-08', NULL, true, 'XYZ');

SELECT table_to_xml('testxmlschema.test1', false, false, '');
SELECT table_to_xml('testxmlschema.test1', true, false, 'foo');
SELECT table_to_xml('testxmlschema.test1', false, true, '');
SELECT table_to_xml('testxmlschema.test1', true, true, '');
SELECT table_to_xml('testxmlschema.test2', false, false, '');

SELECT table_to_xmlschema('testxmlschema.test1', false, false, '');
SELECT table_to_xmlschema('testxmlschema.test1', true, false, '');
SELECT table_to_xmlschema('testxmlschema.test1', false, true, 'foo');
SELECT table_to_xmlschema('testxmlschema.test1', true, true, '');
SELECT table_to_xmlschema('testxmlschema.test2', false, false, '');

SELECT table_to_xml_and_xmlschema('testxmlschema.test1', false, false, '');
SELECT table_to_xml_and_xmlschema('testxmlschema.test1', true, false, '');
SELECT table_to_xml_and_xmlschema('testxmlschema.test1', false, true, '');
SELECT table_to_xml_and_xmlschema('testxmlschema.test1', true, true, 'foo');

SELECT query_to_xml('SELECT * FROM testxmlschema.test1', false, false, '');
SELECT query_to_xmlschema('SELECT * FROM testxmlschema.test1', false, false, '');
SELECT query_to_xml_and_xmlschema('SELECT * FROM testxmlschema.test1', true, true, '');

SELECT schema_to_xml('testxmlschema', false, true, '');
SELECT schema_to_xml('testxmlschema', true, false, '');
SELECT schema_to_xmlschema('testxmlschema', false, true, '');
SELECT schema_to_xmlschema('testxmlschema', true, false, '');
SELECT schema_to_xml_and_xmlschema('testxmlschema', true, true, 'foo');
