--
-- SUBSCRIPTION
--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_subscription_user LOGIN SUPERUSER;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_subscription_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE ROLE regress_subscription_user_dummy LOGIN NOSUPERUSER;
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION 'regress_subscription_user';

-- fail - no publications
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub CONNECTION 'foo';
--DDL_STATEMENT_END--

-- fail - no connection
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub PUBLICATION foo;
--DDL_STATEMENT_END--
-- fail - cannot do CREATE SUBSCRIPTION CREATE SLOT inside transaction block
--DDL_STATEMENT_BEGIN--
BEGIN;
CREATE SUBSCRIPTION testsub CONNECTION 'testconn' PUBLICATION testpub WITH (create_slot);
COMMIT;
--DDL_STATEMENT_END--
-- fail - invalid connection string
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub CONNECTION 'testconn' PUBLICATION testpub;
--DDL_STATEMENT_END--
-- fail - duplicate publications
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub CONNECTION 'dbname=doesnotexist' PUBLICATION foo, testpub, foo WITH (connect = false);
--DDL_STATEMENT_END--
-- ok
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (connect = false);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
COMMENT ON SUBSCRIPTION testsub IS 'test subscription';
--DDL_STATEMENT_END--
SELECT obj_description(s.oid, 'pg_subscription') FROM pg_subscription s;

-- fail - name already exists
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (connect = false);
--DDL_STATEMENT_END--
-- fail - must be superuser
SET SESSION AUTHORIZATION 'regress_subscription_user2';
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION foo WITH (connect = false);
--DDL_STATEMENT_END--
SET SESSION AUTHORIZATION 'regress_subscription_user';

-- fail - invalid option combinations

--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (connect = false, copy_data = true);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (connect = false, enabled = true);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (connect = false, create_slot = true);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (slot_name = NONE, enabled = true);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (slot_name = NONE, create_slot = true);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
C1REATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (slot_name = NONE);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (slot_name = NONE, enabled = false);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub2 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (slot_name = NONE, create_slot = false);
--DDL_STATEMENT_END--
-- ok - with slot_name = NONE
--DDL_STATEMENT_BEGIN--
CREATE SUBSCRIPTION testsub3 CONNECTION 'dbname=doesnotexist' PUBLICATION testpub WITH (slot_name = NONE, connect = false);
--DDL_STATEMENT_END--
-- fail
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub3 ENABLE;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub3 REFRESH PUBLICATION;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SUBSCRIPTION testsub3;
--DDL_STATEMENT_END--
-- fail - invalid connection string
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub CONNECTION 'foobar';
--DDL_STATEMENT_END--
\dRs+
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub SET PUBLICATION testpub2, testpub3 WITH (refresh = false);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub CONNECTION 'dbname=doesnotexist2';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub SET (slot_name = 'newname');
--DDL_STATEMENT_END--

-- fail
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION doesnotexist CONNECTION 'dbname=doesnotexist2';
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub SET (create_slot = false);
--DDL_STATEMENT_END--

\dRs+

BEGIN;
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub ENABLE;
--DDL_STATEMENT_END--
\dRs
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub DISABLE;
--DDL_STATEMENT_END--
\dRs

COMMIT;

-- fail - must be owner of subscription
SET ROLE regress_subscription_user_dummy;
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub RENAME TO testsub_dummy;
--DDL_STATEMENT_END--
RESET ROLE;
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub RENAME TO testsub_foo;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub_foo SET (synchronous_commit = local);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub_foo SET (synchronous_commit = foobar);
--DDL_STATEMENT_END--

\dRs+

-- rename back to keep the rest simple
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub_foo RENAME TO testsub;
--DDL_STATEMENT_END--

-- fail - new owner must be superuser
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub OWNER TO regress_subscription_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER ROLE regress_subscription_user2 SUPERUSER;
--DDL_STATEMENT_END--
-- now it works
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub OWNER TO regress_subscription_user2;
--DDL_STATEMENT_END--

-- fail - cannot do DROP SUBSCRIPTION inside transaction block with slot name
--DDL_STATEMENT_BEGIN--
BEGIN;
DROP SUBSCRIPTION testsub;
COMMIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER SUBSCRIPTION testsub SET (slot_name = NONE);
--DDL_STATEMENT_END--
-- now it works
--DDL_STATEMENT_BEGIN--
BEGIN;
DROP SUBSCRIPTION testsub;
COMMIT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SUBSCRIPTION IF EXISTS testsub;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP SUBSCRIPTION testsub;  -- fail
--DDL_STATEMENT_END--
RESET SESSION AUTHORIZATION;
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_subscription_user;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_subscription_user2;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP ROLE regress_subscription_user_dummy;
--DDL_STATEMENT_END--
