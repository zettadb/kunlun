--
-- Test of Row-level security feature
--

-- Clean up in case a prior regression run failed

-- Suppress NOTICE messages when users/groups don't exist
SET client_min_messages TO 'warning';

DROP USER IF EXISTS regress_rls_alice;
DROP USER IF EXISTS regress_rls_bob;
DROP USER IF EXISTS regress_rls_carol;
DROP USER IF EXISTS regress_rls_dave;
DROP USER IF EXISTS regress_rls_exempt_user;
DROP ROLE IF EXISTS regress_rls_group1;
DROP ROLE IF EXISTS regress_rls_group2;

DROP SCHEMA IF EXISTS regress_rls_schema CASCADE;

RESET client_min_messages;

-- initial setup
CREATE USER regress_rls_alice NOLOGIN;
CREATE USER regress_rls_bob NOLOGIN;
CREATE USER regress_rls_carol NOLOGIN;
CREATE USER regress_rls_dave NOLOGIN;
CREATE USER regress_rls_exempt_user BYPASSRLS NOLOGIN;
CREATE ROLE regress_rls_group1 NOLOGIN;
CREATE ROLE regress_rls_group2 NOLOGIN;

GRANT regress_rls_group1 TO regress_rls_bob;
GRANT regress_rls_group2 TO regress_rls_carol;

CREATE SCHEMA regress_rls_schema;
GRANT ALL ON SCHEMA regress_rls_schema to public;
SET search_path = regress_rls_schema;
-- setup of malicious function
CREATE OR REPLACE FUNCTION f_leak(text) RETURNS bool
    COST 0.0000001 LANGUAGE plpgsql
    AS 'BEGIN RAISE NOTICE ''f_leak => %'', $1; RETURN true; END';
GRANT EXECUTE ON FUNCTION f_leak(text) TO public;

-- BASIC Row-Level Security Scenario

SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE uaccount (
    pguser      name primary key,
    seclv       int
);
GRANT SELECT ON uaccount TO public;
INSERT INTO uaccount VALUES
    ('regress_rls_alice', 99),
    ('regress_rls_bob', 1),
    ('regress_rls_carol', 2),
    ('regress_rls_dave', 3);

CREATE TABLE category (
    cid        int primary key,
    cname      text
);
GRANT ALL ON category TO public;
INSERT INTO category VALUES
    (11, 'novel'),
    (22, 'science fiction'),
    (33, 'technology'),
    (44, 'manga');

CREATE TABLE document (
    did         int primary key,
    cid         int,
    dlevel      int not null,
    dauthor     name,
    dtitle      text
);
GRANT ALL ON document TO public;
INSERT INTO document VALUES
    ( 1, 11, 1, 'regress_rls_bob', 'my first novel'),
    ( 2, 11, 2, 'regress_rls_bob', 'my second novel'),
    ( 3, 22, 2, 'regress_rls_bob', 'my science fiction'),
    ( 4, 44, 1, 'regress_rls_bob', 'my first manga'),
    ( 5, 44, 2, 'regress_rls_bob', 'my second manga'),
    ( 6, 22, 1, 'regress_rls_carol', 'great science fiction'),
    ( 7, 33, 2, 'regress_rls_carol', 'great technology book'),
    ( 8, 44, 1, 'regress_rls_carol', 'great manga'),
    ( 9, 22, 1, 'regress_rls_dave', 'awesome science fiction'),
    (10, 33, 2, 'regress_rls_dave', 'awesome technology book');

\dp
\d document
SELECT * FROM pg_policies WHERE schemaname = 'regress_rls_schema' AND tablename = 'document' ORDER BY policyname;

-- viewpoint from regress_rls_bob
SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO ON;
--SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
--SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER BY did;

-- try a sampled version
--SELECT * FROM document TABLESAMPLE BERNOULLI(50) REPEATABLE(0)
--  WHERE f_leak(dtitle) ORDER BY did;

-- viewpoint from regress_rls_carol
SET SESSION AUTHORIZATION regress_rls_carol;
SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER BY did;

-- try a sampled version, not support
-- SELECT * FROM document TABLESAMPLE BERNOULLI(50) REPEATABLE(0)
--  WHERE f_leak(dtitle) ORDER BY did;

EXPLAIN (COSTS OFF) SELECT * FROM document WHERE f_leak(dtitle);
EXPLAIN (COSTS OFF) SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

-- viewpoint from regress_rls_dave
SET SESSION AUTHORIZATION regress_rls_dave;
SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER BY did;

EXPLAIN (COSTS OFF) SELECT * FROM document WHERE f_leak(dtitle);
EXPLAIN (COSTS OFF) SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

-- 44 would technically fail for both p2r and p1r, but we should get an error
-- back from p1r for this because it sorts first
INSERT INTO document VALUES (100, 44, 1, 'regress_rls_dave', 'testing sorting of policies'); -- fail
-- Just to see a p2r error
INSERT INTO document VALUES (100, 55, 1, 'regress_rls_dave', 'testing sorting of policies'); -- fail

SET SESSION AUTHORIZATION regress_rls_alice;

-- viewpoint from regress_rls_bob again
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER by did;

-- viewpoint from rls_regres_carol again
SET SESSION AUTHORIZATION regress_rls_carol;
SELECT * FROM document WHERE f_leak(dtitle) ORDER BY did;
SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle) ORDER by did;

EXPLAIN (COSTS OFF) SELECT * FROM document WHERE f_leak(dtitle);
EXPLAIN (COSTS OFF) SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

-- interaction of FK/PK constraints
SET SESSION AUTHORIZATION regress_rls_alice;

-- cannot delete PK referenced by invisible FK
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM document d FULL OUTER JOIN category c on d.cid = c.cid ORDER BY d.did, c.cid;
-- will crash: DELETE FROM category WHERE cid = 33;    -- fails with FK violation

-- can insert FK referencing invisible PK
SET SESSION AUTHORIZATION regress_rls_carol;
SELECT * FROM document d FULL OUTER JOIN category c on d.cid = c.cid ORDER BY d.did, c.cid;
INSERT INTO document VALUES (11, 33, 1, current_user, 'hoge');

-- UNIQUE or PRIMARY KEY constraint violation DOES reveal presence of row
SET SESSION AUTHORIZATION regress_rls_bob;
INSERT INTO document VALUES (8, 44, 1, 'regress_rls_bob', 'my third manga'); -- Must fail with unique violation, revealing presence of did we can't see
SELECT * FROM document WHERE did = 8; -- and confirm we can't see it

-- RLS policies are checked before constraints
INSERT INTO document VALUES (8, 44, 1, 'regress_rls_carol', 'my third manga'); -- Should fail with RLS check violation, not duplicate key violation
UPDATE document SET did = 8, dauthor = 'regress_rls_carol' WHERE did = 5; -- Should fail with RLS check violation, not duplicate key violation

-- database superuser does bypass RLS policy when enabled
RESET SESSION AUTHORIZATION;
SET row_security TO ON;
SELECT * FROM document order by 1,2,3,4,5;
SELECT * FROM category;

-- database superuser does bypass RLS policy when disabled
RESET SESSION AUTHORIZATION;
SET row_security TO OFF;
SELECT * FROM document order by 1,2,3,4,5;
SELECT * FROM category;

-- database non-superuser with bypass privilege can bypass RLS policy when disabled
SET SESSION AUTHORIZATION regress_rls_exempt_user;
SET row_security TO OFF;
SELECT * FROM document order by 1,2,3,4,5;
SELECT * FROM category;

-- RLS policy does not apply to table owner when RLS enabled.
SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security TO ON;
SELECT * FROM document order by 1,2,3,4,5;
SELECT * FROM category;

-- RLS policy does not apply to table owner when RLS disabled.
SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security TO OFF;
SELECT * FROM document order by 1,2,3,4,5;
SELECT * FROM category;

SET SESSION AUTHORIZATION regress_rls_alice;

SET row_security TO ON;

CREATE TABLE t1 (a int, junk1 text, b text);
ALTER TABLE t1 DROP COLUMN junk1;    -- just a disturbing factor
GRANT ALL ON t1 TO public;

SET SESSION AUTHORIZATION regress_rls_bob;

SELECT * FROM t1;
EXPLAIN (COSTS OFF) SELECT * FROM t1;

SELECT * FROM t1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);

-- superuser is allowed to bypass RLS checks
RESET SESSION AUTHORIZATION;
SET row_security TO OFF;
SELECT * FROM t1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);

-- non-superuser with bypass privilege can bypass RLS policy when disabled
SET SESSION AUTHORIZATION regress_rls_exempt_user;
SET row_security TO OFF;
SELECT * FROM t1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);

--
-- Partitioned Tables
--

SET SESSION AUTHORIZATION regress_rls_alice;

CREATE TABLE part_document (
    did         int,
    cid         int,
    dlevel      int not null,
    dauthor     name,
    dtitle      text
) PARTITION BY RANGE (cid);
GRANT ALL ON part_document TO public;

-- Create partitions for document categories
CREATE TABLE part_document_fiction PARTITION OF part_document FOR VALUES FROM (11) to (12);
CREATE TABLE part_document_satire PARTITION OF part_document FOR VALUES FROM (55) to (56);
CREATE TABLE part_document_nonfiction PARTITION OF part_document FOR VALUES FROM (99) to (100);

GRANT ALL ON part_document_fiction TO public;
GRANT ALL ON part_document_satire TO public;
GRANT ALL ON part_document_nonfiction TO public;

INSERT INTO part_document VALUES
    ( 1, 11, 1, 'regress_rls_bob', 'my first novel'),
    ( 2, 11, 2, 'regress_rls_bob', 'my second novel'),
    ( 3, 99, 2, 'regress_rls_bob', 'my science textbook'),
    ( 4, 55, 1, 'regress_rls_bob', 'my first satire'),
    ( 5, 99, 2, 'regress_rls_bob', 'my history book'),
    ( 6, 11, 1, 'regress_rls_carol', 'great science fiction'),
    ( 7, 99, 2, 'regress_rls_carol', 'great technology book'),
    ( 8, 55, 2, 'regress_rls_carol', 'great satire'),
    ( 9, 11, 1, 'regress_rls_dave', 'awesome science fiction'),
    (10, 99, 2, 'regress_rls_dave', 'awesome technology book');

\d+ part_document
SELECT * FROM pg_policies WHERE schemaname = 'regress_rls_schema' AND tablename like '%part_document%' ORDER BY policyname;

-- viewpoint from regress_rls_bob
SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO ON;
SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);

-- viewpoint from regress_rls_carol
SET SESSION AUTHORIZATION regress_rls_carol;
SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);

-- viewpoint from regress_rls_dave
SET SESSION AUTHORIZATION regress_rls_dave;
SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);

-- pp1 ERROR
INSERT INTO part_document VALUES (100, 11, 5, 'regress_rls_dave', 'testing pp1'); -- fail
-- pp1r ERROR
INSERT INTO part_document VALUES (100, 99, 1, 'regress_rls_dave', 'testing pp1r'); -- fail

-- Show that RLS policy does not apply for direct inserts to children
-- This should fail with RLS POLICY pp1r violation.
INSERT INTO part_document VALUES (100, 55, 1, 'regress_rls_dave', 'testing RLS with partitions'); -- fail
-- But this should succeed.
INSERT INTO part_document_satire VALUES (100, 55, 1, 'regress_rls_dave', 'testing RLS with partitions'); -- success
-- We still cannot see the row using the parent
SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
-- But we can if we look directly
SELECT * FROM part_document_satire WHERE f_leak(dtitle) ORDER BY did;

-- Turn on RLS and create policy on child to show RLS is checked before constraints
SET SESSION AUTHORIZATION regress_rls_alice;
-- This should fail with RLS violation now.
SET SESSION AUTHORIZATION regress_rls_dave;
INSERT INTO part_document_satire VALUES (101, 55, 1, 'regress_rls_dave', 'testing RLS with partitions'); -- fail
-- And now we cannot see directly into the partition either, due to RLS
-- will crash: SELECT * FROM part_document_satire WHERE f_leak(dtitle) ORDER BY did;
-- The parent looks same as before
-- viewpoint from regress_rls_dave
SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);

-- viewpoint from regress_rls_carol
SET SESSION AUTHORIZATION regress_rls_carol;
SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;
EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);

SET SESSION AUTHORIZATION regress_rls_alice;

-- viewpoint from regress_rls_bob again
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;

-- viewpoint from rls_regres_carol again
SET SESSION AUTHORIZATION regress_rls_carol;
SELECT * FROM part_document WHERE f_leak(dtitle) ORDER BY did;

EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);

-- database superuser does bypass RLS policy when enabled
RESET SESSION AUTHORIZATION;
SET row_security TO ON;
SELECT * FROM part_document ORDER BY did;
SELECT * FROM part_document_satire ORDER by did;

-- database non-superuser with bypass privilege can bypass RLS policy when disabled
SET SESSION AUTHORIZATION regress_rls_exempt_user;
SET row_security TO OFF;
SELECT * FROM part_document ORDER BY did;
SELECT * FROM part_document_satire ORDER by did;

-- RLS policy does not apply to table owner when RLS enabled.
SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security TO ON;
SELECT * FROM part_document ORDER by did;
SELECT * FROM part_document_satire ORDER by did;

-- When RLS disabled, other users get ERROR.
SET SESSION AUTHORIZATION regress_rls_dave;
SET row_security TO OFF;
SELECT * FROM part_document ORDER by did;
SELECT * FROM part_document_satire ORDER by did;

-- Check behavior with a policy that uses a SubPlan not an InitPlan.
SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security TO ON;

SET SESSION AUTHORIZATION regress_rls_carol;
INSERT INTO part_document VALUES (100, 11, 5, 'regress_rls_carol', 'testing pp3'); -- fail

----- Dependencies -----
SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security TO ON;

CREATE TABLE dependee (x integer, y integer);

CREATE TABLE dependent (x integer, y integer);

DROP TABLE dependee; -- Should fail without CASCADE due to dependency on row security qual?

DROP TABLE dependee CASCADE;

EXPLAIN (COSTS OFF) SELECT * FROM dependent; -- After drop, should be unqualified

-----   RECURSION    ----

--
-- Simple recursion
--
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE rec1 (x integer, y integer);
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM rec1; -- fail, direct recursion

--
-- Mutual recursion
--
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE rec2 (a integer, b integer);

SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM rec1;    -- fail, mutual recursion

--
-- Mutual recursion via views
--
SET SESSION AUTHORIZATION regress_rls_bob;
CREATE VIEW rec1v AS SELECT * FROM rec1;
CREATE VIEW rec2v AS SELECT * FROM rec2;
SET SESSION AUTHORIZATION regress_rls_alice;

SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM rec1;    -- fail, mutual recursion via views

--
-- Mutual recursion via .s.b views
--
SET SESSION AUTHORIZATION regress_rls_bob;

\set VERBOSITY terse \\ -- suppress cascade details
DROP VIEW rec1v, rec2v CASCADE;
\set VERBOSITY default

CREATE VIEW rec1v WITH (security_barrier) AS SELECT * FROM rec1;
CREATE VIEW rec2v WITH (security_barrier) AS SELECT * FROM rec2;
SET SESSION AUTHORIZATION regress_rls_alice;

SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM rec1;    -- fail, mutual recursion via s.b. views

--
-- recursive RLS and VIEWs in policy
--
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE s1 (a int, b text);
INSERT INTO s1 (SELECT x, md5(x::text) FROM generate_series(-10,10) x);

CREATE TABLE s2 (x int, y text);
INSERT INTO s2 (SELECT x, md5(x::text) FROM generate_series(-6,6) x);

GRANT SELECT ON s1, s2 TO regress_rls_bob;

SET SESSION AUTHORIZATION regress_rls_bob;
CREATE VIEW v2 AS SELECT * FROM s2 WHERE y like '%af%';
SELECT * FROM s1 WHERE f_leak(b); -- fail (infinite recursion)

INSERT INTO s1 VALUES (1, 'foo'); -- fail (infinite recursion)

SET SESSION AUTHORIZATION regress_rls_alice;

SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM s1 WHERE f_leak(b);	-- OK
EXPLAIN (COSTS OFF) SELECT * FROM only s1 WHERE f_leak(b);

SET SESSION AUTHORIZATION regress_rls_alice;
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM s1 WHERE f_leak(b);	-- OK
EXPLAIN (COSTS OFF) SELECT * FROM s1 WHERE f_leak(b);

SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 WHERE y like '%28%';
EXPLAIN (COSTS OFF) SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 WHERE y like '%28%';

SET SESSION AUTHORIZATION regress_rls_alice;
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM s1 WHERE f_leak(b);	-- fail (infinite recursion via view)

-- prepared statement with regress_rls_alice privilege
PREPARE p1(int) AS SELECT * FROM t1 WHERE a <= $1;
EXECUTE p1(2);
EXPLAIN (COSTS OFF) EXECUTE p1(2);

-- superuser is allowed to bypass RLS checks
RESET SESSION AUTHORIZATION;
SET row_security TO OFF;
SELECT * FROM t1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);

-- plan cache should be invalidated
EXECUTE p1(2);
EXPLAIN (COSTS OFF) EXECUTE p1(2);

PREPARE p2(int) AS SELECT * FROM t1 WHERE a = $1;
EXECUTE p2(2);
EXPLAIN (COSTS OFF) EXECUTE p2(2);

-- also, case when privilege switch from superuser
SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO ON;
EXECUTE p2(2);
EXPLAIN (COSTS OFF) EXECUTE p2(2);

--
-- UPDATE / DELETE and Row-level security
-- where clause using function not supported in kunlun, comment below:
--SET SESSION AUTHORIZATION regress_rls_bob;
--EXPLAIN (COSTS OFF) UPDATE t1 SET b = b || b WHERE f_leak(b);
--UPDATE t1 SET b = b || b WHERE f_leak(b);

--EXPLAIN (COSTS OFF) UPDATE only t1 SET b = b || '_updt' WHERE f_leak(b);
--UPDATE only t1 SET b = b || '_updt' WHERE f_leak(b);

-- returning clause with system column
--UPDATE t1 SET b = b WHERE f_leak(b) RETURNING *;

--EXPLAIN (COSTS OFF) UPDATE t1 t1_1 SET b = t1_2.b FROM t1 t1_2
--WHERE t1_1.a = 4 AND t1_2.a = t1_1.a AND t1_2.b = t1_1.b
--AND f_leak(t1_1.b) AND f_leak(t1_2.b) RETURNING *, t1_1, t1_2;

--UPDATE t1 t1_1 SET b = t1_2.b FROM t1 t1_2
--WHERE t1_1.a = 4 AND t1_2.a = t1_1.a AND t1_2.b = t1_1.b
--AND f_leak(t1_1.b) AND f_leak(t1_2.b) RETURNING *, t1_1, t1_2;

RESET SESSION AUTHORIZATION;
SET row_security TO OFF;
SELECT * FROM t1 ORDER BY a,b;

SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO ON;
EXPLAIN (COSTS OFF) DELETE FROM only t1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) DELETE FROM t1 WHERE f_leak(b);

--
-- S.b. view on top of Row-level security
--
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE b1 (a int, b text);
INSERT INTO b1 (SELECT x, md5(x::text) FROM generate_series(-10,10) x);

GRANT ALL ON b1 TO regress_rls_bob;

SET SESSION AUTHORIZATION regress_rls_bob;
CREATE VIEW bv1 WITH (security_barrier) AS SELECT * FROM b1 WHERE a > 0 WITH CHECK OPTION;
GRANT ALL ON bv1 TO regress_rls_carol;

SET SESSION AUTHORIZATION regress_rls_carol;

EXPLAIN (COSTS OFF) SELECT * FROM bv1 WHERE f_leak(b);
SELECT * FROM bv1 WHERE f_leak(b);

INSERT INTO bv1 VALUES (-1, 'xxx'); -- should fail view WCO
INSERT INTO bv1 VALUES (11, 'xxx'); -- should fail RLS check
INSERT INTO bv1 VALUES (12, 'xxx'); -- ok

--EXPLAIN (COSTS OFF) UPDATE bv1 SET b = 'yyy' WHERE a = 4 AND f_leak(b);
--UPDATE bv1 SET b = 'yyy' WHERE a = 4 AND f_leak(b);

--EXPLAIN (COSTS OFF) DELETE FROM bv1 WHERE a = 6 AND f_leak(b);
--DELETE FROM bv1 WHERE a = 6 AND f_leak(b);

SET SESSION AUTHORIZATION regress_rls_alice;
SELECT * FROM b1;
--
-- INSERT ... ON CONFLICT DO UPDATE and Row-level security
--

SET SESSION AUTHORIZATION regress_rls_alice;

SET SESSION AUTHORIZATION regress_rls_bob;

-- Exists...
SELECT * FROM document WHERE did = 2;

-- ...so violates actual WITH CHECK OPTION within UPDATE (not INSERT, since
-- alternative UPDATE path happens to be taken):
INSERT INTO document VALUES (2, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_carol', 'my first novel')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle, dauthor = EXCLUDED.dauthor;

-- Violates USING qual for UPDATE policy p3.
--
-- UPDATE path is taken, but UPDATE fails purely because *existing* row to be
-- updated is not a "novel"/cid 11 (row is not leaked, even though we have
-- SELECT privileges sufficient to see the row in this instance):
INSERT INTO document VALUES (33, 22, 1, 'regress_rls_bob', 'okay science fiction'); -- preparation for next statement
INSERT INTO document VALUES (33, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_bob', 'Some novel, replaces sci-fi') -- takes UPDATE path
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle;
-- Fine (we UPDATE, since INSERT WCOs and UPDATE security barrier quals + WCOs
-- not violated):
INSERT INTO document VALUES (2, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_bob', 'my first novel')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle RETURNING *;
-- Fine (we INSERT, so "cid = 33" ("technology") isn't evaluated):
INSERT INTO document VALUES (78, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_bob', 'some technology novel')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle, cid = 33 RETURNING *;
-- Fine (same query, but we UPDATE, so "cid = 33", ("technology") is not the
-- case in respect of *existing* tuple):
INSERT INTO document VALUES (78, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_bob', 'some technology novel')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle, cid = 33 RETURNING *;
-- Same query a third time, but now fails due to existing tuple finally not
-- passing quals:
INSERT INTO document VALUES (78, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_bob', 'some technology novel')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle, cid = 33 RETURNING *;
-- Don't fail just because INSERT doesn't satisfy WITH CHECK option that
-- originated as a barrier/USING() qual from the UPDATE.  Note that the UPDATE
-- path *isn't* taken, and so UPDATE-related policy does not apply:
INSERT INTO document VALUES (79, (SELECT cid from category WHERE cname = 'technology'), 1, 'regress_rls_bob', 'technology book, can only insert')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle RETURNING *;
-- But this time, the same statement fails, because the UPDATE path is taken,
-- and updating the row just inserted falls afoul of security barrier qual
-- (enforced as WCO) -- what we might have updated target tuple to is
-- irrelevant, in fact.
INSERT INTO document VALUES (79, (SELECT cid from category WHERE cname = 'technology'), 1, 'regress_rls_bob', 'technology book, can only insert')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle RETURNING *;

-- Test default USING qual enforced as WCO
SET SESSION AUTHORIZATION regress_rls_alice;

SET SESSION AUTHORIZATION regress_rls_bob;
-- Just because WCO-style enforcement of USING quals occurs with
-- existing/target tuple does not mean that the implementation can be allowed
-- to fail to also enforce this qual against the final tuple appended to
-- relation (since in the absence of an explicit WCO, this is also interpreted
-- as an UPDATE/ALL WCO in general).
--
-- UPDATE path is taken here (fails due to existing tuple).  Note that this is
-- not reported as a "USING expression", because it's an RLS UPDATE check that originated as
-- a USING qual for the purposes of RLS in general, as opposed to an explicit
-- USING qual that is ordinarily a security barrier.  We leave it up to the
-- UPDATE to make this fail:
INSERT INTO document VALUES (79, (SELECT cid from category WHERE cname = 'technology'), 1, 'regress_rls_bob', 'technology book, can only insert')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle RETURNING *;

-- UPDATE path is taken here.  Existing tuple passes, since its cid
-- corresponds to "novel", but default USING qual is enforced against
-- post-UPDATE tuple too (as always when updating with a policy that lacks an
-- explicit WCO), and so this fails:
INSERT INTO document VALUES (2, (SELECT cid from category WHERE cname = 'technology'), 1, 'regress_rls_bob', 'my first novel')
    ON CONFLICT (did) DO UPDATE SET cid = EXCLUDED.cid, dtitle = EXCLUDED.dtitle RETURNING *;

SET SESSION AUTHORIZATION regress_rls_alice;

--
-- Test ALL policies with ON CONFLICT DO UPDATE (much the same as existing UPDATE
-- tests)
--
SET SESSION AUTHORIZATION regress_rls_bob;

-- Fails, since ALL WCO is enforced in insert path:
INSERT INTO document VALUES (80, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_carol', 'my first novel')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle, cid = 33;
-- Fails, since ALL policy USING qual is enforced (existing, target tuple is in
-- violation, since it has the "manga" cid):
INSERT INTO document VALUES (4, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_bob', 'my first novel')
    ON CONFLICT (did) DO UPDATE SET dtitle = EXCLUDED.dtitle;
-- Fails, since ALL WCO are enforced:
INSERT INTO document VALUES (1, (SELECT cid from category WHERE cname = 'novel'), 1, 'regress_rls_bob', 'my first novel')
    ON CONFLICT (did) DO UPDATE SET dauthor = 'regress_rls_carol';

--
-- ROLE/GROUP
--
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE z1 (a int, b text);
CREATE TABLE z2 (a int, b text);

GRANT SELECT ON z1,z2 TO regress_rls_group1, regress_rls_group2,
    regress_rls_bob, regress_rls_carol;

INSERT INTO z1 VALUES
    (1, 'aba'),
    (2, 'bbb'),
    (3, 'ccc'),
    (4, 'dad');

SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM z1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM z1 WHERE f_leak(b);

PREPARE plancache_test AS SELECT * FROM z1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) EXECUTE plancache_test;

SET ROLE regress_rls_group1;
SELECT * FROM z1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM z1 WHERE f_leak(b);

EXPLAIN (COSTS OFF) EXECUTE plancache_test;

SET SESSION AUTHORIZATION regress_rls_carol;
SELECT * FROM z1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM z1 WHERE f_leak(b);

EXPLAIN (COSTS OFF) EXECUTE plancache_test;

SET ROLE regress_rls_group2;
SELECT * FROM z1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM z1 WHERE f_leak(b);

EXPLAIN (COSTS OFF) EXECUTE plancache_test;

--
-- Views should follow policy for view owner.
--
-- View and Table owner are the same.
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE VIEW rls_view AS SELECT * FROM z1 WHERE f_leak(b);
GRANT SELECT ON rls_view TO regress_rls_bob;

-- Query as role that is not owner of view or table.  Should return all records.
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM rls_view;
EXPLAIN (COSTS OFF) SELECT * FROM rls_view;

-- Query as view/table owner.  Should return all records.
SET SESSION AUTHORIZATION regress_rls_alice;
SELECT * FROM rls_view;
EXPLAIN (COSTS OFF) SELECT * FROM rls_view;
DROP VIEW rls_view;

-- View and Table owners are different.
SET SESSION AUTHORIZATION regress_rls_bob;
CREATE VIEW rls_view AS SELECT * FROM z1 WHERE f_leak(b);
GRANT SELECT ON rls_view TO regress_rls_alice;

-- Query as role that is not owner of view but is owner of table.
-- Should return records based on view owner policies.
SET SESSION AUTHORIZATION regress_rls_alice;
SELECT * FROM rls_view;
EXPLAIN (COSTS OFF) SELECT * FROM rls_view;

-- Query as role that is not owner of table but is owner of view.
-- Should return records based on view owner policies.
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM rls_view;
EXPLAIN (COSTS OFF) SELECT * FROM rls_view;

-- Query as role that is not the owner of the table or view without permissions.
SET SESSION AUTHORIZATION regress_rls_carol;
SELECT * FROM rls_view; --fail - permission denied.
EXPLAIN (COSTS OFF) SELECT * FROM rls_view; --fail - permission denied.

-- Query as role that is not the owner of the table or view with permissions.
SET SESSION AUTHORIZATION regress_rls_bob;
GRANT SELECT ON rls_view TO regress_rls_carol;
SELECT * FROM rls_view;
EXPLAIN (COSTS OFF) SELECT * FROM rls_view;

SET SESSION AUTHORIZATION regress_rls_bob;
DROP VIEW rls_view;

--
-- Command specific
--
SET SESSION AUTHORIZATION regress_rls_alice;

CREATE TABLE x1 (a int, b text, c text);
GRANT ALL ON x1 TO PUBLIC;

INSERT INTO x1 VALUES
    (1, 'abc', 'regress_rls_bob'),
    (2, 'bcd', 'regress_rls_bob'),
    (3, 'cde', 'regress_rls_carol'),
    (4, 'def', 'regress_rls_carol'),
    (5, 'efg', 'regress_rls_bob'),
    (6, 'fgh', 'regress_rls_bob'),
    (7, 'fgh', 'regress_rls_carol'),
    (8, 'fgh', 'regress_rls_carol');

--SET SESSION AUTHORIZATION regress_rls_bob;
--SELECT * FROM x1 WHERE f_leak(b) ORDER BY a ASC;
--UPDATE x1 SET b = b || '_updt' WHERE f_leak(b) RETURNING *;

--SET SESSION AUTHORIZATION regress_rls_carol;
--SELECT * FROM x1 WHERE f_leak(b) ORDER BY a ASC;
--UPDATE x1 SET b = b || '_updt' WHERE f_leak(b) RETURNING *;
--DELETE FROM x1 WHERE f_leak(b) RETURNING *;

--
-- Duplicate Policy Names
--
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE y1 (a int, b text);
CREATE TABLE y2 (a int, b text);

GRANT ALL ON y1, y2 TO regress_rls_bob;

--
-- Expression structure with SBV
--
-- Create view as table owner.  RLS should NOT be applied.
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE VIEW rls_sbv WITH (security_barrier) AS
    SELECT * FROM y1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM rls_sbv WHERE (a = 1);
DROP VIEW rls_sbv;

-- Create view as role that does not own table.  RLS should be applied.
SET SESSION AUTHORIZATION regress_rls_bob;
CREATE VIEW rls_sbv WITH (security_barrier) AS
    SELECT * FROM y1 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM rls_sbv WHERE (a = 1);
DROP VIEW rls_sbv;

--
-- Expression structure
--
SET SESSION AUTHORIZATION regress_rls_alice;
INSERT INTO y2 (SELECT x, md5(x::text) FROM generate_series(0,20) x);

SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM y2 WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM y2 WHERE f_leak(b);

--
-- Qual push-down of leaky functions, when not referring to table
--
SELECT * FROM y2 WHERE f_leak('abc');
EXPLAIN (COSTS OFF) SELECT * FROM y2 WHERE f_leak('abc');

CREATE TABLE test_qual_pushdown (
    abc text
);

INSERT INTO test_qual_pushdown VALUES ('abc'),('def');

SELECT * FROM y2 JOIN test_qual_pushdown ON (b = abc) WHERE f_leak(abc);
EXPLAIN (COSTS OFF) SELECT * FROM y2 JOIN test_qual_pushdown ON (b = abc) WHERE f_leak(abc);

SELECT * FROM y2 JOIN test_qual_pushdown ON (b = abc) WHERE f_leak(b);
EXPLAIN (COSTS OFF) SELECT * FROM y2 JOIN test_qual_pushdown ON (b = abc) WHERE f_leak(b);

DROP TABLE test_qual_pushdown;

--
-- Plancache invalidate on user change.
--
RESET SESSION AUTHORIZATION;

\set VERBOSITY terse \\ -- suppress cascade details
DROP TABLE t1 CASCADE;
\set VERBOSITY default

CREATE TABLE t1 (a integer);

GRANT SELECT ON t1 TO regress_rls_bob, regress_rls_carol;

-- Prepare as regress_rls_bob
SET ROLE regress_rls_bob;
PREPARE role_inval AS SELECT * FROM t1;
-- Check plan
EXPLAIN (COSTS OFF) EXECUTE role_inval;

-- Change to regress_rls_carol
SET ROLE regress_rls_carol;
-- Check plan- should be different
EXPLAIN (COSTS OFF) EXECUTE role_inval;

-- Change back to regress_rls_bob
SET ROLE regress_rls_bob;
-- Check plan- should be back to original
EXPLAIN (COSTS OFF) EXECUTE role_inval;

--
-- CTE and RLS
--
RESET SESSION AUTHORIZATION;
DROP TABLE t1 CASCADE;
CREATE TABLE t1 (a integer, b text);
GRANT ALL ON t1 TO regress_rls_bob;

INSERT INTO t1 (SELECT x, md5(x::text) FROM generate_series(0,20) x);

SET SESSION AUTHORIZATION regress_rls_bob;

WITH cte1 AS (SELECT * FROM t1 WHERE f_leak(b)) SELECT * FROM cte1;
EXPLAIN (COSTS OFF) WITH cte1 AS (SELECT * FROM t1 WHERE f_leak(b)) SELECT * FROM cte1;

WITH cte1 AS (UPDATE t1 SET a = a + 1 RETURNING *) SELECT * FROM cte1; --fail
-- not supported by kunlun: WITH cte1 AS (UPDATE t1 SET a = a RETURNING *) SELECT * FROM cte1; --ok

WITH cte1 AS (INSERT INTO t1 VALUES (21, 'Fail') RETURNING *) SELECT * FROM cte1; --fail
WITH cte1 AS (INSERT INTO t1 VALUES (20, 'Success') RETURNING *) SELECT * FROM cte1; --ok

--
-- Rename Policy
--
RESET SESSION AUTHORIZATION;

SELECT polname, relname
    FROM pg_policy pol
    JOIN pg_class pc ON (pc.oid = pol.polrelid)
    WHERE relname = 't1';

SELECT polname, relname
    FROM pg_policy pol
    JOIN pg_class pc ON (pc.oid = pol.polrelid)
    WHERE relname = 't1';

--
-- Check INSERT SELECT
--
SET SESSION AUTHORIZATION regress_rls_bob;
CREATE TABLE t2 (a integer, b text);
INSERT INTO t2 (SELECT * FROM t1);
EXPLAIN (COSTS OFF) INSERT INTO t2 (SELECT * FROM t1);
SELECT * FROM t2;
EXPLAIN (COSTS OFF) SELECT * FROM t2;

--
-- RLS with JOIN
--
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE blog (id integer, author text, post text);
CREATE TABLE comment (blog_id integer, message text);

GRANT ALL ON blog, comment TO regress_rls_bob;

INSERT INTO blog VALUES
    (1, 'alice', 'blog #1'),
    (2, 'bob', 'blog #1'),
    (3, 'alice', 'blog #2'),
    (4, 'alice', 'blog #3'),
    (5, 'john', 'blog #1');

INSERT INTO comment VALUES
    (1, 'cool blog'),
    (1, 'fun blog'),
    (3, 'crazy blog'),
    (5, 'what?'),
    (4, 'insane!'),
    (2, 'who did it?');

SET SESSION AUTHORIZATION regress_rls_bob;
-- Check RLS JOIN with Non-RLS.
SELECT id, author, message FROM blog JOIN comment ON id = blog_id;
-- Check Non-RLS JOIN with RLS.
SELECT id, author, message FROM comment JOIN blog ON id = blog_id;

SET SESSION AUTHORIZATION regress_rls_alice;

SET SESSION AUTHORIZATION regress_rls_bob;
-- Check RLS JOIN RLS
SELECT id, author, message FROM blog JOIN comment ON id = blog_id;
SELECT id, author, message FROM comment JOIN blog ON id = blog_id;

SET SESSION AUTHORIZATION regress_rls_alice;
DROP TABLE blog;
DROP TABLE comment;

--
-- Default Deny Policy
--
RESET SESSION AUTHORIZATION;
ALTER TABLE t1 OWNER TO regress_rls_alice;

-- Check that default deny does not apply to superuser.
RESET SESSION AUTHORIZATION;
SELECT * FROM t1;
EXPLAIN (COSTS OFF) SELECT * FROM t1;

-- Check that default deny does not apply to table owner.
SET SESSION AUTHORIZATION regress_rls_alice;
SELECT * FROM t1;
EXPLAIN (COSTS OFF) SELECT * FROM t1;

-- Check that default deny applies to non-owner/non-superuser when RLS on.
SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO ON;
SELECT * FROM t1;
EXPLAIN (COSTS OFF) SELECT * FROM t1;
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM t1;
EXPLAIN (COSTS OFF) SELECT * FROM t1;

--
-- COPY TO/FROM
--

RESET SESSION AUTHORIZATION;
DROP TABLE copy_t CASCADE;
CREATE TABLE copy_t (a integer, b text);
--CREATE POLICY p1 ON copy_t USING (a % 2 = 0);

--ALTER TABLE copy_t ENABLE ROW LEVEL SECURITY;

GRANT ALL ON copy_t TO regress_rls_bob, regress_rls_exempt_user;

INSERT INTO copy_t (SELECT x, md5(x::text) FROM generate_series(0,10) x);

-- Check COPY TO as Superuser/owner.
RESET SESSION AUTHORIZATION;
SET row_security TO OFF;
COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER ',';
SET row_security TO ON;
COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER ',';

-- Check COPY TO as user with permissions.
SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO OFF;
COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER ','; --fail - would be affected by RLS
SET row_security TO ON;
COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER ','; --ok

-- Check COPY TO as user with permissions and BYPASSRLS
SET SESSION AUTHORIZATION regress_rls_exempt_user;
SET row_security TO OFF;
COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER ','; --ok
SET row_security TO ON;
COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER ','; --ok

-- Check COPY TO as user without permissions. SET row_security TO OFF;
SET SESSION AUTHORIZATION regress_rls_carol;
SET row_security TO OFF;
COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER ','; --fail - would be affected by RLS
SET row_security TO ON;
COPY (SELECT * FROM copy_t ORDER BY a ASC) TO STDOUT WITH DELIMITER ','; --fail - permission denied

-- Check COPY relation TO; keep it just one row to avoid reordering issues
RESET SESSION AUTHORIZATION;
SET row_security TO ON;
CREATE TABLE copy_rel_to (a integer, b text);
--CREATE POLICY p1 ON copy_rel_to USING (a % 2 = 0);

--ALTER TABLE copy_rel_to ENABLE ROW LEVEL SECURITY;

GRANT ALL ON copy_rel_to TO regress_rls_bob, regress_rls_exempt_user;

INSERT INTO copy_rel_to VALUES (1, md5('1'));

-- Check COPY TO as Superuser/owner.
RESET SESSION AUTHORIZATION;
SET row_security TO OFF;
COPY copy_rel_to TO STDOUT WITH DELIMITER ',';
SET row_security TO ON;
COPY copy_rel_to TO STDOUT WITH DELIMITER ',';

-- Check COPY TO as user with permissions.
SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO OFF;
COPY copy_rel_to TO STDOUT WITH DELIMITER ','; --fail - would be affected by RLS
SET row_security TO ON;
COPY copy_rel_to TO STDOUT WITH DELIMITER ','; --ok

-- Check COPY TO as user with permissions and BYPASSRLS
SET SESSION AUTHORIZATION regress_rls_exempt_user;
SET row_security TO OFF;
COPY copy_rel_to TO STDOUT WITH DELIMITER ','; --ok
SET row_security TO ON;
COPY copy_rel_to TO STDOUT WITH DELIMITER ','; --ok

-- Check COPY TO as user without permissions. SET row_security TO OFF;
SET SESSION AUTHORIZATION regress_rls_carol;
SET row_security TO OFF;
COPY copy_rel_to TO STDOUT WITH DELIMITER ','; --fail - permission denied
SET row_security TO ON;
COPY copy_rel_to TO STDOUT WITH DELIMITER ','; --fail - permission denied

-- Check COPY FROM as Superuser/owner.
RESET SESSION AUTHORIZATION;
SET row_security TO OFF;
COPY copy_t FROM STDIN; --ok
1	abc
2	bcd
3	cde
4	def
\.
SET row_security TO ON;
COPY copy_t FROM STDIN; --ok
1	abc
2	bcd
3	cde
4	def
\.

-- Check COPY FROM as user with permissions.
SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO OFF;
COPY copy_t FROM STDIN; --fail - would be affected by RLS.
SET row_security TO ON;
COPY copy_t FROM STDIN; --fail - COPY FROM not supported by RLS.

-- Check COPY FROM as user with permissions and BYPASSRLS
SET SESSION AUTHORIZATION regress_rls_exempt_user;
SET row_security TO ON;
COPY copy_t FROM STDIN; --ok
1	abc
2	bcd
3	cde
4	def
\.

-- Check COPY FROM as user without permissions.
SET SESSION AUTHORIZATION regress_rls_carol;
SET row_security TO OFF;
COPY copy_t FROM STDIN; --fail - permission denied.
SET row_security TO ON;
COPY copy_t FROM STDIN; --fail - permission denied.

RESET SESSION AUTHORIZATION;
DROP TABLE copy_t;
DROP TABLE copy_rel_to CASCADE;

-- Check WHERE CURRENT OF
SET SESSION AUTHORIZATION regress_rls_alice;

CREATE TABLE current_check (currentid int, payload text, rlsuser text);
GRANT ALL ON current_check TO PUBLIC;

INSERT INTO current_check VALUES
    (1, 'abc', 'regress_rls_bob'),
    (2, 'bcd', 'regress_rls_bob'),
    (3, 'cde', 'regress_rls_bob'),
    (4, 'def', 'regress_rls_bob');

SET SESSION AUTHORIZATION regress_rls_bob;

-- Can SELECT even rows
SELECT * FROM current_check;

-- Cannot UPDATE row 2
-- will crash here: UPDATE current_check SET payload = payload || '_new' WHERE currentid = 2 RETURNING *;
--
-- check pg_stats view filtering
--
SET row_security TO ON;
SET SESSION AUTHORIZATION regress_rls_alice;
-- Stats visible
SELECT row_security_active('current_check');
SELECT attname, most_common_vals FROM pg_stats
  WHERE tablename = 'current_check'
  ORDER BY 1;

SET SESSION AUTHORIZATION regress_rls_bob;
-- Stats not visible
SELECT row_security_active('current_check');
SELECT attname, most_common_vals FROM pg_stats
  WHERE tablename = 'current_check'
  ORDER BY 1;

--
-- Shared Object Dependencies
--
RESET SESSION AUTHORIZATION;

--
-- Non-target relations are only subject to SELECT policies
--
SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE r1 (a int);
CREATE TABLE r2 (a int);
INSERT INTO r1 VALUES (10), (20);
INSERT INTO r2 VALUES (10), (20);

GRANT ALL ON r1, r2 TO regress_rls_bob;

SET SESSION AUTHORIZATION regress_rls_bob;
SELECT * FROM r1;
SELECT * FROM r2;

-- r2 is read-only
INSERT INTO r2 VALUES (2); -- Not allowed
UPDATE r2 SET a = 2 RETURNING *; -- Updates nothing
DELETE FROM r2 RETURNING *; -- Deletes nothing

-- r2 can be used as a non-target relation in DML
-- errors: INSERT INTO r1 SELECT a + 1 FROM r2 RETURNING *; -- OK
-- not support: UPDATE r1 SET a = r2.a + 2 FROM r2 WHERE r1.a = r2.a RETURNING *; -- OK
-- not support: DELETE FROM r1 USING r2 WHERE r1.a = r2.a + 2 RETURNING *; -- OK
SELECT * FROM r1;
SELECT * FROM r2;

SET SESSION AUTHORIZATION regress_rls_alice;
DROP TABLE r1;
DROP TABLE r2;

SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security = on;
CREATE TABLE r1 (a int);
INSERT INTO r1 VALUES (10), (20);

-- No error, but no rows
TABLE r1;

-- RLS error
INSERT INTO r1 VALUES (1);

-- No error (unable to see any rows to update)
UPDATE r1 SET a = 1;
TABLE r1;

-- No error (unable to see any rows to delete)
DELETE FROM r1;
TABLE r1;

SET row_security = off;
-- these all fail, would be affected by RLS
TABLE r1;
UPDATE r1 SET a = 1;
DELETE FROM r1;

DROP TABLE r1;

SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security = on;
CREATE TABLE r1 (a int PRIMARY KEY);
CREATE TABLE r2 (a int);
INSERT INTO r1 VALUES (10), (20);
INSERT INTO r2 VALUES (10), (20);

DELETE FROM r1;

-- clean out r2 for INSERT test below
DELETE FROM r2;

-- No rows seen
TABLE r1;

-- No error, RI still sees that row exists in r1
INSERT INTO r2 VALUES (10);

DROP TABLE r2;
DROP TABLE r1;

-- Ensure cascaded DELETE works
CREATE TABLE r1 (a int PRIMARY KEY);
CREATE TABLE r2 (a int);
INSERT INTO r1 VALUES (10), (20);
INSERT INTO r2 VALUES (10), (20);

-- Deletes all records from both
DELETE FROM r1;

-- As owner, we now bypass RLS
-- verify no rows in r2 now
TABLE r2;

DROP TABLE r2;
DROP TABLE r1;

-- Ensure cascaded UPDATE works
CREATE TABLE r1 (a int PRIMARY KEY);
CREATE TABLE r2 (a int);
INSERT INTO r1 VALUES (10), (20);
INSERT INTO r2 VALUES (10), (20);

-- Updates records in both
UPDATE r1 SET a = a+5;

-- As owner, we now bypass RLS
-- verify records in r2 updated
TABLE r2;

DROP TABLE r2;
DROP TABLE r1;

--
-- Test INSERT+RETURNING applies SELECT policies as
-- WithCheckOptions (meaning an error is thrown)
--
SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security = on;
CREATE TABLE r1 (a int);

-- Works fine
INSERT INTO r1 VALUES (10), (20);

-- No error, but no rows
TABLE r1;

SET row_security = off;
-- fail, would be affected by RLS
TABLE r1;

SET row_security = on;

-- Error
INSERT INTO r1 VALUES (10), (20) RETURNING *;

DROP TABLE r1;

--
-- Test UPDATE+RETURNING applies SELECT policies as
-- WithCheckOptions (meaning an error is thrown)
--
SET SESSION AUTHORIZATION regress_rls_alice;
SET row_security = on;
CREATE TABLE r1 (a int PRIMARY KEY);

INSERT INTO r1 VALUES (10);
-- Works fine
UPDATE r1 SET a = 30;

-- Show updated rows
TABLE r1;
-- reset value in r1 for test with RETURNING
UPDATE r1 SET a = 10;

-- Verify row reset
TABLE r1;

-- Error
UPDATE r1 SET a = 30 RETURNING *;

-- UPDATE path of INSERT ... ON CONFLICT DO UPDATE should also error out
INSERT INTO r1 VALUES (10)
    ON CONFLICT (a) DO UPDATE SET a = 30 RETURNING *;

-- Should still error out without RETURNING (use of arbiter always requires
-- SELECT permissions)
INSERT INTO r1 VALUES (10)
    ON CONFLICT (a) DO UPDATE SET a = 30;
INSERT INTO r1 VALUES (10)
    ON CONFLICT ON CONSTRAINT r1_pkey DO UPDATE SET a = 30;

DROP TABLE r1;

-- Check dependency handling
RESET SESSION AUTHORIZATION;
CREATE TABLE dep1 (c1 int);
CREATE TABLE dep2 (c1 int);

-- Should return one
SELECT count(*) = 1 FROM pg_depend
				   WHERE objid = (SELECT oid FROM pg_policy WHERE polname = 'dep_p1')
					 AND refobjid = (SELECT oid FROM pg_class WHERE relname = 'dep2');

-- Should return one
SELECT count(*) = 1 FROM pg_shdepend
				   WHERE objid = (SELECT oid FROM pg_policy WHERE polname = 'dep_p1')
					 AND refobjid = (SELECT oid FROM pg_authid WHERE rolname = 'regress_rls_bob');

-- Should return one
SELECT count(*) = 1 FROM pg_shdepend
				   WHERE objid = (SELECT oid FROM pg_policy WHERE polname = 'dep_p1')
					 AND refobjid = (SELECT oid FROM pg_authid WHERE rolname = 'regress_rls_carol');

-- Should return zero
SELECT count(*) = 0 FROM pg_depend
				   WHERE objid = (SELECT oid FROM pg_policy WHERE polname = 'dep_p1')
					 AND refobjid = (SELECT oid FROM pg_class WHERE relname = 'dep2');

RESET SESSION AUTHORIZATION;

CREATE ROLE regress_rls_dob_role1;
CREATE ROLE regress_rls_dob_role2;

CREATE TABLE dob_t1 (c1 int);
CREATE TABLE dob_t2 (c1 int) PARTITION BY RANGE (c1);

DROP USER regress_rls_dob_role1;
DROP USER regress_rls_dob_role2;

-- Bug #15708: view + table with RLS should check policies as view owner
CREATE TABLE ref_tbl (a int);
INSERT INTO ref_tbl VALUES (1);

CREATE TABLE rls_tbl (a int);
INSERT INTO rls_tbl VALUES (10);

GRANT SELECT ON ref_tbl TO regress_rls_bob;
GRANT SELECT ON rls_tbl TO regress_rls_bob;

CREATE VIEW rls_view AS SELECT * FROM rls_tbl;
ALTER VIEW rls_view OWNER TO regress_rls_bob;
GRANT SELECT ON rls_view TO regress_rls_alice;

SET SESSION AUTHORIZATION regress_rls_alice;
SELECT * FROM ref_tbl; -- Permission denied
SELECT * FROM rls_tbl; -- Permission denied
SELECT * FROM rls_view; -- OK
RESET SESSION AUTHORIZATION;

DROP VIEW rls_view;
DROP TABLE rls_tbl;
DROP TABLE ref_tbl;

--
-- Clean up objects
--
RESET SESSION AUTHORIZATION;

drop table regress_rls_schema.t1 cascade;
drop table regress_rls_schema.blog;
drop table regress_rls_schema.x1;
drop table regress_rls_schema.y1;
drop table regress_rls_schema.z1;
drop table regress_rls_schema.y2;
drop table regress_rls_schema.z2;
drop table regress_rls_schema.b1 cascade;
drop table regress_rls_schema.s1 cascade;
drop table regress_rls_schema.s2 cascade;
drop table regress_rls_schema.rec1 cascade;
drop table regress_rls_schema.rec2 cascade;
drop table regress_rls_schema.part_document cascade;
drop table regress_rls_schema.uaccount cascade;
drop table regress_rls_schema.document cascade;
drop table regress_rls_schema.category cascade;
drop table regress_rls_schema.dependent cascade;
drop table regress_rls_schema.current_check;
drop table regress_rls_schema.rls_tbl cascade;
drop function regress_rls_schema.op_leak(integer,integer) cascade;
DROP USER regress_rls_alice;

drop table regress_rls_schema.dep1 cascade;
drop table regress_rls_schema.t2 cascade;
DROP USER regress_rls_bob;

DROP USER regress_rls_carol;
DROP USER regress_rls_dave;
DROP USER regress_rls_exempt_user;
DROP ROLE regress_rls_group1;
DROP ROLE regress_rls_group2;

drop table regress_rls_schema.dob_t1 cascade;
drop table regress_rls_schema.dob_t2 cascade;
drop table regress_rls_schema.dep2 cascade;
\set VERBOSITY terse \\ -- suppress cascade details
DROP SCHEMA regress_rls_schema CASCADE;
\set VERBOSITY default

-- Arrange to have a few policies left over, for testing
-- pg_dump/pg_restore
CREATE SCHEMA regress_rls_schema;
CREATE TABLE rls_tbl (c1 int);

CREATE TABLE rls_tbl_force (c1 int);

drop table rls_tbl;
drop table rls_tbl_force;
drop schema regress_rls_schema;
