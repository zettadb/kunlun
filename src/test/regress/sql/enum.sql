--
-- Enum tests
--

--DDL_STATEMENT_BEGIN--
DROP TABLE if exists enumtest_child;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists enumtest_parent;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists enumtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists enumtest_bogus_child;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
drop type if exists rainbow;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE rainbow AS ENUM ('red', 'orange', 'yellow', 'green', 'blue', 'purple');
--DDL_STATEMENT_END--

--
-- Did it create the right number of rows?
--
SELECT COUNT(*) FROM pg_enum WHERE enumtypid = 'rainbow'::regtype;

--
-- I/O functions
--
SELECT 'red'::rainbow;
SELECT 'mauve'::rainbow;

--
-- adding new values
--

--DDL_STATEMENT_BEGIN--
drop type if exists planets;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE planets AS ENUM ( 'venus', 'earth', 'mars' );
--DDL_STATEMENT_END--

SELECT enumlabel, enumsortorder
FROM pg_enum
WHERE enumtypid = 'planets'::regtype
ORDER BY 2;

--DDL_STATEMENT_BEGIN--
ALTER TYPE planets ADD VALUE 'uranus';
--DDL_STATEMENT_END--

SELECT enumlabel, enumsortorder
FROM pg_enum
WHERE enumtypid = 'planets'::regtype
ORDER BY 2;

--not supported: ALTER TYPE planets ADD VALUE 'mercury' BEFORE 'venus';
--not supported: ALTER TYPE planets ADD VALUE 'saturn' BEFORE 'uranus';
--not supported: ALTER TYPE planets ADD VALUE 'jupiter' AFTER 'mars';
--DDL_STATEMENT_BEGIN--
ALTER TYPE planets ADD VALUE 'neptune' AFTER 'uranus';
--DDL_STATEMENT_END--

SELECT enumlabel, enumsortorder
FROM pg_enum
WHERE enumtypid = 'planets'::regtype
ORDER BY 2;

SELECT enumlabel, enumsortorder
FROM pg_enum
WHERE enumtypid = 'planets'::regtype
ORDER BY enumlabel::planets;

-- errors for adding labels
--DDL_STATEMENT_BEGIN--
ALTER TYPE planets ADD VALUE
  'plutoplutoplutoplutoplutoplutoplutoplutoplutoplutoplutoplutoplutopluto';
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TYPE planets ADD VALUE 'pluto' AFTER 'zeus';
--DDL_STATEMENT_END--

-- if not exists tests

--  existing value gives error
--DDL_STATEMENT_BEGIN--
ALTER TYPE planets ADD VALUE 'mercury';
--DDL_STATEMENT_END--

-- unless IF NOT EXISTS is specified
--DDL_STATEMENT_BEGIN--
ALTER TYPE planets ADD VALUE IF NOT EXISTS 'mercury';
--DDL_STATEMENT_END--

-- should be neptune, not mercury
SELECT enum_last(NULL::planets);

--DDL_STATEMENT_BEGIN--
ALTER TYPE planets ADD VALUE IF NOT EXISTS 'pluto';
--DDL_STATEMENT_END--

-- should be pluto, i.e. the new value
SELECT enum_last(NULL::planets);

--
-- Test inserting so many values that we have to renumber
--

--DDL_STATEMENT_BEGIN--
drop type if exists insenum;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
create type insenum as enum ('L1', 'L2');
--DDL_STATEMENT_END--

--not supported:alter type insenum add value 'i1' before 'L2';
--alter type insenum add value 'i2' before 'L2';
--alter type insenum add value 'i3' before 'L2';
--alter type insenum add value 'i4' before 'L2';
--alter type insenum add value 'i5' before 'L2';
--alter type insenum add value 'i6' before 'L2';
--alter type insenum add value 'i7' before 'L2';
--alter type insenum add value 'i8' before 'L2';
--alter type insenum add value 'i9' before 'L2';
--alter type insenum add value 'i10' before 'L2';
--alter type insenum add value 'i11' before 'L2';
--alter type insenum add value 'i12' before 'L2';
--alter type insenum add value 'i13' before 'L2';
--alter type insenum add value 'i14' before 'L2';
--alter type insenum add value 'i15' before 'L2';
--alter type insenum add value 'i16' before 'L2';
--alter type insenum add value 'i17' before 'L2';
--alter type insenum add value 'i18' before 'L2';
--alter type insenum add value 'i19' before 'L2';
--alter type insenum add value 'i20' before 'L2';
--alter type insenum add value 'i21' before 'L2';
--alter type insenum add value 'i22' before 'L2';
--alter type insenum add value 'i23' before 'L2';
--alter type insenum add value 'i24' before 'L2';
--alter type insenum add value 'i25' before 'L2';
--alter type insenum add value 'i26' before 'L2';
--alter type insenum add value 'i27' before 'L2';
--alter type insenum add value 'i28' before 'L2';
--alter type insenum add value 'i29' before 'L2';
--alter type insenum add value 'i30' before 'L2';

-- The exact values of enumsortorder will now depend on the local properties
-- of float4, but in any reasonable implementation we should get at least
-- 20 splits before having to renumber; so only hide values > 20.

SELECT enumlabel,
       case when enumsortorder > 20 then null else enumsortorder end as so
FROM pg_enum
WHERE enumtypid = 'insenum'::regtype
ORDER BY enumsortorder;

--
-- Basic table creation, row selection
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE enumtest (col rainbow);
--DDL_STATEMENT_END--
INSERT INTO enumtest values ('red'), ('orange'), ('yellow'), ('green');
COPY enumtest FROM stdin;
blue
purple
\.
SELECT * FROM enumtest;

--
-- Operators, no index
--
SELECT * FROM enumtest WHERE col = 'orange';
SELECT * FROM enumtest WHERE col <> 'orange' ORDER BY col;
SELECT * FROM enumtest WHERE col > 'yellow' ORDER BY col;
SELECT * FROM enumtest WHERE col >= 'yellow' ORDER BY col;
SELECT * FROM enumtest WHERE col < 'green' ORDER BY col;
SELECT * FROM enumtest WHERE col <= 'green' ORDER BY col;

--
-- Cast to/from text
--
SELECT 'red'::rainbow::text || 'hithere';
SELECT 'red'::text::rainbow = 'red'::rainbow;

--
-- Aggregates
--
SELECT min(col) FROM enumtest;
SELECT max(col) FROM enumtest;
SELECT max(col) FROM enumtest WHERE col < 'green';

--
-- Index tests, force use of index
--
SET enable_seqscan = off;
SET enable_bitmapscan = off;

--
-- Btree index / opclass with the various operators
--
--DDL_STATEMENT_BEGIN--
CREATE UNIQUE INDEX enumtest_btree ON enumtest USING btree (col);
--DDL_STATEMENT_END--
SELECT * FROM enumtest WHERE col = 'orange';
SELECT * FROM enumtest WHERE col <> 'orange' ORDER BY col;
SELECT * FROM enumtest WHERE col > 'yellow' ORDER BY col;
SELECT * FROM enumtest WHERE col >= 'yellow' ORDER BY col;
SELECT * FROM enumtest WHERE col < 'green' ORDER BY col;
SELECT * FROM enumtest WHERE col <= 'green' ORDER BY col;
SELECT min(col) FROM enumtest;
SELECT max(col) FROM enumtest;
SELECT max(col) FROM enumtest WHERE col < 'green';
--DDL_STATEMENT_BEGIN--
DROP INDEX enumtest_btree;
--DDL_STATEMENT_END--

--
-- Hash index / opclass with the = operator
--
--DDL_STATEMENT_BEGIN--
CREATE INDEX enumtest_hash ON enumtest USING hash (col);
--DDL_STATEMENT_END--
SELECT * FROM enumtest WHERE col = 'orange';
--DDL_STATEMENT_BEGIN--
DROP INDEX enumtest_hash;
--DDL_STATEMENT_END--

--
-- End index tests
--
RESET enable_seqscan;
RESET enable_bitmapscan;

--
-- Domains over enums, not supported in Kunlun
--
-- CREATE DOMAIN rgb AS rainbow CHECK (VALUE IN ('red', 'green', 'blue'));
-- SELECT 'red'::rgb;
-- SELECT 'purple'::rgb;
-- SELECT 'purple'::rainbow::rgb;
-- DROP DOMAIN rgb;

--
-- Arrays
--
SELECT '{red,green,blue}'::rainbow[];
SELECT ('{red,green,blue}'::rainbow[])[2];
SELECT 'red' = ANY ('{red,green,blue}'::rainbow[]);
SELECT 'yellow' = ANY ('{red,green,blue}'::rainbow[]);
SELECT 'red' = ALL ('{red,green,blue}'::rainbow[]);
SELECT 'red' = ALL ('{red,red}'::rainbow[]);

--
-- Support functions
--
SELECT enum_first(NULL::rainbow);
SELECT enum_last('green'::rainbow);
SELECT enum_range(NULL::rainbow);
SELECT enum_range('orange'::rainbow, 'green'::rainbow);
SELECT enum_range(NULL, 'green'::rainbow);
SELECT enum_range('orange'::rainbow, NULL);
SELECT enum_range(NULL::rainbow, NULL);

--
-- User functions, can't test perl/python etc here since may not be compiled.
--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION echo_me(anyenum) RETURNS text AS $$
BEGIN
RETURN $1::text || 'omg';
END
$$ LANGUAGE plpgsql;
--DDL_STATEMENT_END--
SELECT echo_me('red'::rainbow);
--
-- Concrete function should override generic one
--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION echo_me(rainbow) RETURNS text AS $$
BEGIN
RETURN $1::text || 'wtf';
END
$$ LANGUAGE plpgsql;
--DDL_STATEMENT_END--
SELECT echo_me('red'::rainbow);
--
-- If we drop the original generic one, we don't have to qualify the type
-- anymore, since there's only one match
--
--DDL_STATEMENT_BEGIN--
DROP FUNCTION echo_me(anyenum);
--DDL_STATEMENT_END--
SELECT echo_me('red');
--DDL_STATEMENT_BEGIN--
DROP FUNCTION echo_me(rainbow);
--DDL_STATEMENT_END--

--
-- RI triggers on enum types
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE enumtest_parent (id rainbow PRIMARY KEY);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE enumtest_child (parent rainbow);
--DDL_STATEMENT_END--
INSERT INTO enumtest_parent VALUES ('red');
INSERT INTO enumtest_child VALUES ('red');
INSERT INTO enumtest_child VALUES ('blue');
DELETE FROM enumtest_parent;
--
-- cross-type RI should fail
--
--DDL_STATEMENT_BEGIN--
drop type if exists bogus;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE bogus AS ENUM('good', 'bad', 'ugly');
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE enumtest_bogus_child(parent bogus);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE bogus;
--DDL_STATEMENT_END--

-- check renaming a value
-- not supported: ALTER TYPE rainbow RENAME VALUE 'red' TO 'crimson';
SELECT enumlabel, enumsortorder
FROM pg_enum
WHERE enumtypid = 'rainbow'::regtype
ORDER BY 2;
-- check that renaming a non-existent value fails
-- not supported: ALTER TYPE rainbow RENAME VALUE 'red' TO 'crimson';
-- check that renaming to an existent value fails
-- not supported: ALTER TYPE rainbow RENAME VALUE 'blue' TO 'green';

--
-- check transactional behaviour of ALTER TYPE ... ADD VALUE
--
--DDL_STATEMENT_BEGIN--
CREATE TYPE bogus AS ENUM('good');
--DDL_STATEMENT_END--

-- check that we can't add new values to existing enums in a transaction
--DDL_STATEMENT_BEGIN--
BEGIN;
ALTER TYPE bogus ADD VALUE 'bad';
COMMIT;
--DDL_STATEMENT_END--

-- check that we recognize the case where the enum already existed but was
-- modified in the current txn
--DDL_STATEMENT_BEGIN--
BEGIN;
-- not supported: ALTER TYPE bogus RENAME TO bogon;
ALTER TYPE bogon ADD VALUE 'bad';
ROLLBACK;
--DDL_STATEMENT_END--

-- but ALTER TYPE RENAME VALUE is safe in a transaction
BEGIN;
-- not supported: ALTER TYPE bogus RENAME VALUE 'good' to 'bad';
SELECT 'bad'::bogus;
ROLLBACK;

--DDL_STATEMENT_BEGIN--
DROP TYPE bogus;
--DDL_STATEMENT_END--

-- check that we *can* add new values to existing enums in a transaction,
-- if the type is new as well
--DDL_STATEMENT_BEGIN--
BEGIN;
CREATE TYPE bogus AS ENUM();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE bogus ADD VALUE 'good';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TYPE bogus ADD VALUE 'ugly';
ROLLBACK;
--DDL_STATEMENT_END--

--
-- Cleanup
--
--DDL_STATEMENT_BEGIN--
DROP TABLE enumtest_child;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE enumtest_parent;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE enumtest;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE enumtest_bogus_child;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE rainbow;
--DDL_STATEMENT_END--

--
-- Verify properly cleaned up
--
SELECT COUNT(*) FROM pg_type WHERE typname = 'rainbow';
SELECT * FROM pg_enum WHERE NOT EXISTS
  (SELECT 1 FROM pg_type WHERE pg_type.oid = enumtypid);
