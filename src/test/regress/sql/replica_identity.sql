--DDL_STATEMENT_BEGIN--
CREATE TABLE test_replica_identity (
       id serial primary key,
       keya varchar(50) not null,
       keyb varchar(50) not null,
       nonkey  varchar(50),
       CONSTRAINT test_replica_identity_unique_defer UNIQUE (keya, keyb) DEFERRABLE,
       CONSTRAINT test_replica_identity_unique_nondefer UNIQUE (keya, keyb)
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE test_replica_identity_othertable (id serial primary key);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE INDEX test_replica_identity_keyab ON test_replica_identity (keya, keyb);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE UNIQUE INDEX test_replica_identity_keyab_key ON test_replica_identity (keya, keyb);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE UNIQUE INDEX test_replica_identity_nonkey ON test_replica_identity (keya, nonkey);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE INDEX test_replica_identity_hash ON test_replica_identity USING hash (nonkey);
--DDL_STATEMENT_END--
CREATE UNIQUE INDEX test_replica_identity_expr ON test_replica_identity (keya, keyb, (3));
CREATE UNIQUE INDEX test_replica_identity_partial ON test_replica_identity (keya, keyb) WHERE keyb != '3';

-- default is 'd'/DEFAULT for user created tables
SELECT relreplident FROM pg_class WHERE oid = 'test_replica_identity'::regclass;
-- but 'none' for system tables
SELECT relreplident FROM pg_class WHERE oid = 'pg_class'::regclass;
SELECT relreplident FROM pg_class WHERE oid = 'pg_constraint'::regclass;

----
-- Make sure we detect ineligible indexes
----

-- Make sure index cases succeed
----

--DDL_STATEMENT_BEGIN--
DROP TABLE test_replica_identity;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP TABLE test_replica_identity_othertable;
--DDL_STATEMENT_END--