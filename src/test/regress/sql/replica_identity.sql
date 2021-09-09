CREATE TABLE test_replica_identity (
       id serial primary key,
       keya varchar(50) not null,
       keyb varchar(50) not null,
       nonkey  varchar(50),
       CONSTRAINT test_replica_identity_unique_defer UNIQUE (keya, keyb) DEFERRABLE,
       CONSTRAINT test_replica_identity_unique_nondefer UNIQUE (keya, keyb)
);

CREATE TABLE test_replica_identity_othertable (id serial primary key);

CREATE INDEX test_replica_identity_keyab ON test_replica_identity (keya, keyb);
CREATE UNIQUE INDEX test_replica_identity_keyab_key ON test_replica_identity (keya, keyb);
CREATE UNIQUE INDEX test_replica_identity_nonkey ON test_replica_identity (keya, nonkey);
CREATE INDEX test_replica_identity_hash ON test_replica_identity USING hash (nonkey);

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

DROP TABLE test_replica_identity;
DROP TABLE test_replica_identity_othertable;
