--
-- Tests for password verifiers
--

-- Tests for GUC password_encryption
SET password_encryption = 'novalue'; -- error
SET password_encryption = true; -- ok
SET password_encryption = 'md5'; -- ok
SET password_encryption = 'scram-sha-256'; -- ok

-- consistency of password entries
SET password_encryption = 'md5';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd1 PASSWORD 'role_pwd1';
--DDL_STATEMENT_END--
SET password_encryption = 'on';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd2 PASSWORD 'role_pwd2';
--DDL_STATEMENT_END--
SET password_encryption = 'scram-sha-256';
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd3 PASSWORD 'role_pwd3';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd4 PASSWORD NULL;
--DDL_STATEMENT_END--

-- check list of created entries
--
-- The scram verifier will look something like:
-- SCRAM-SHA-256$4096:E4HxLGtnRzsYwg==$6YtlR4t69SguDiwFvbVgVZtuz6gpJQQqUMZ7IQJK5yI=:ps75jrHeYU4lXCcXI4O8oIdJ3eO8o2jirjruw9phBTo=
--
-- Since the salt is random, the exact value stored will be different on every test
-- run. Use a regular expression to mask the changing parts.
SELECT rolname, regexp_replace(rolpassword, '(SCRAM-SHA-256)\$(\d+):([a-zA-Z0-9+/=]+)\$([a-zA-Z0-9+=/]+):([a-zA-Z0-9+/=]+)', '\1$\2:<salt>$<storedkey>:<serverkey>') as rolpassword_masked
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd%'
    ORDER BY rolname, rolpassword;

-- Rename a role
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_passwd2 RENAME TO regress_passwd2_new;
--DDL_STATEMENT_END--
-- md5 entry should have been removed
SELECT rolname, rolpassword
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd2_new'
    ORDER BY rolname, rolpassword;
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_passwd2_new RENAME TO regress_passwd2;
--DDL_STATEMENT_END--
-- Change passwords with ALTER USER. With plaintext or already-encrypted
-- passwords.
SET password_encryption = 'md5';

-- encrypt with MD5
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_passwd2 PASSWORD 'foo';
--DDL_STATEMENT_END--
-- already encrypted, use as they are
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_passwd1 PASSWORD 'md5cd3578025fe2c3d7ed1b9a9b26238b70';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_passwd3 PASSWORD 'SCRAM-SHA-256$4096:VLK4RMaQLCvNtQ==$6YtlR4t69SguDiwFvbVgVZtuz6gpJQQqUMZ7IQJK5yI=:ps75jrHeYU4lXCcXI4O8oIdJ3eO8o2jirjruw9phBTo=';
--DDL_STATEMENT_END--
SET password_encryption = 'scram-sha-256';
-- create SCRAM verifier
--DDL_STATEMENT_BEGIN--
ALTER ROLE  regress_passwd4 PASSWORD 'foo';
--DDL_STATEMENT_END--
-- already encrypted with MD5, use as it is
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd5 PASSWORD 'md5e73a4b11df52a6068f8b39f90be36023';
--DDL_STATEMENT_END--
-- This looks like a valid SCRAM-SHA-256 verifier, but it is not
-- so it should be hashed with SCRAM-SHA-256.
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd6 PASSWORD 'SCRAM-SHA-256$1234';
--DDL_STATEMENT_END--
-- These may look like valid MD5 verifiers, but they are not, so they
-- should be hashed with SCRAM-SHA-256.
-- trailing garbage at the end
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd7 PASSWORD 'md5012345678901234567890123456789zz';
--DDL_STATEMENT_END--
-- invalid length
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd8 PASSWORD 'md501234567890123456789012345678901zz';
--DDL_STATEMENT_END--
SELECT rolname, regexp_replace(rolpassword, '(SCRAM-SHA-256)\$(\d+):([a-zA-Z0-9+/=]+)\$([a-zA-Z0-9+=/]+):([a-zA-Z0-9+/=]+)', '\1$\2:<salt>$<storedkey>:<serverkey>') as rolpassword_masked
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd%'
    ORDER BY rolname, rolpassword;

-- An empty password is not allowed, in any form
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd_empty PASSWORD '';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_passwd_empty PASSWORD 'md585939a5ce845f1a1b620742e3c659e0a';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_passwd_empty PASSWORD 'SCRAM-SHA-256$4096:hpFyHTUsSWcR7O9P$LgZFIt6Oqdo27ZFKbZ2nV+vtnYM995pDh9ca6WSi120=:qVV5NeluNfUPkwm7Vqat25RjSPLkGeoZBQs6wVv+um4=';
--DDL_STATEMENT_END--
SELECT rolpassword FROM pg_authid WHERE rolname='regress_passwd_empty';

-- Test with invalid stored and server keys.
--
-- The first is valid, to act as a control. The others have too long
-- stored/server keys. They will be re-hashed.
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd_sha_len0 PASSWORD 'SCRAM-SHA-256$4096:A6xHKoH/494E941doaPOYg==$Ky+A30sewHIH3VHQLRN9vYsuzlgNyGNKCh37dy96Rqw=:COPdlNiIkrsacU5QoxydEuOH6e/KfiipeETb/bPw8ZI=';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd_sha_len1 PASSWORD 'SCRAM-SHA-256$4096:A6xHKoH/494E941doaPOYg==$Ky+A30sewHIH3VHQLRN9vYsuzlgNyGNKCh37dy96RqwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:COPdlNiIkrsacU5QoxydEuOH6e/KfiipeETb/bPw8ZI=';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_passwd_sha_len2 PASSWORD 'SCRAM-SHA-256$4096:A6xHKoH/494E941doaPOYg==$Ky+A30sewHIH3VHQLRN9vYsuzlgNyGNKCh37dy96Rqw=:COPdlNiIkrsacU5QoxydEuOH6e/KfiipeETb/bPw8ZIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=';
--DDL_STATEMENT_END--
-- Check that the invalid verifiers were re-hashed. A re-hashed verifier
-- should not contain the original salt.
SELECT rolname, rolpassword not like '%A6xHKoH/494E941doaPOYg==%' as is_rolpassword_rehashed
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd_sha_len%'
    ORDER BY rolname;
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd3;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd4;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd5;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd6;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd7;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd8;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd_empty;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd_sha_len0;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd_sha_len1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_passwd_sha_len2;
--DDL_STATEMENT_END--

-- all entries should have been removed
SELECT rolname, rolpassword
    FROM pg_authid
    WHERE rolname LIKE 'regress_passwd%'
    ORDER BY rolname, rolpassword;
