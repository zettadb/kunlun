--DDL_STATEMENT_BEGIN--
CREATE TABLE ttable1 OF nothing;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists persons;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop type if exists person_type;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TYPE person_type AS (id int, name varchar(50));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE persons OF person_type;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE IF NOT EXISTS persons OF person_type;
--DDL_STATEMENT_END--
SELECT * FROM persons;
\d persons
--DDL_STATEMENT_BEGIN--
drop function if exists get_all_persons();
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION get_all_persons() RETURNS SETOF person_type
LANGUAGE SQL
AS $$
    SELECT * FROM persons;
$$;
--DDL_STATEMENT_END--
SELECT * FROM get_all_persons();

-- certain ALTER TABLE operations on typed tables are not allowed
--DDL_STATEMENT_BEGIN--
ALTER TABLE persons ADD COLUMN comment text;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE persons DROP COLUMN name;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE persons RENAME COLUMN id TO num;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE persons ALTER COLUMN name TYPE varchar;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE stuff (id int);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE persons INHERIT stuff;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE personsx OF person_type (myname WITH OPTIONS NOT NULL); -- error
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
drop table if exists persons2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE persons2 OF person_type (
    id WITH OPTIONS PRIMARY KEY,
    UNIQUE (name)
);
--DDL_STATEMENT_END--
\d persons2
--DDL_STATEMENT_BEGIN--
drop table if exists persons3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE persons3 OF person_type (
    PRIMARY KEY (id),
    name WITH OPTIONS DEFAULT ''
);
--DDL_STATEMENT_END--

\d persons3
--DDL_STATEMENT_BEGIN--
CREATE TABLE persons4 OF person_type (
    name WITH OPTIONS NOT NULL,
    name WITH OPTIONS DEFAULT ''  -- error, specified more than once
);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE person_type RESTRICT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE persons2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE persons3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TYPE person_type CASCADE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE persons5 OF stuff; -- only CREATE TYPE AS types may be used
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE stuff;
--DDL_STATEMENT_END--

-- implicit casting
--DDL_STATEMENT_BEGIN--
CREATE TYPE person_type AS (id int, name varchar(50));
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE persons OF person_type;
--DDL_STATEMENT_END--
INSERT INTO persons VALUES (1, 'test');
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION namelen(person_type) RETURNS int LANGUAGE SQL AS $$ SELECT length($1.name) $$;
--DDL_STATEMENT_END--
SELECT id, namelen(persons) FROM persons;
--DDL_STATEMENT_BEGIN--
CREATE TABLE persons2 OF person_type (
    id WITH OPTIONS PRIMARY KEY,
    UNIQUE (name)
);
--DDL_STATEMENT_END--

\d persons2
--DDL_STATEMENT_BEGIN--
CREATE TABLE persons3 OF person_type (
    PRIMARY KEY (id),
    name NOT NULL DEFAULT ''
);
--DDL_STATEMENT_END--
\d persons3
