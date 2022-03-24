CALL nonexistent();  -- error
CALL random();  -- error
--DDL_STATEMENT_BEGIN--
CREATE FUNCTION cp_testfunc1(a int) RETURNS int LANGUAGE SQL AS $$ SELECT a $$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TABLE cp_test (a int, b text);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptest1(x text)
LANGUAGE SQL
AS $$
INSERT INTO cp_test VALUES (1, x);
$$;
--DDL_STATEMENT_END--

\df ptest1
SELECT pg_get_functiondef('ptest1'::regproc);

-- show only normal functions
\dfn public.*test*1

-- show only procedures
\dfp public.*test*1

SELECT ptest1('x');  -- error
CALL ptest1('a');  -- ok
CALL ptest1('xy' || 'zzy');  -- ok, constant-folded arg
CALL ptest1(substring(random()::numeric(20,15)::text, 1, 1));  -- ok, volatile arg

SELECT * FROM cp_test ORDER BY b COLLATE "C";


--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptest2()
LANGUAGE SQL
AS $$
SELECT 5;
$$;
--DDL_STATEMENT_END--

CALL ptest2();


-- nested CALL
delete from cp_test;

--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptest3(y text)
LANGUAGE SQL
AS $$
CALL ptest1(y);
CALL ptest1($1);
$$;
--DDL_STATEMENT_END--

CALL ptest3('b');

SELECT * FROM cp_test;


-- output arguments

--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptest4a(INOUT a int, INOUT b int)
LANGUAGE SQL
AS $$
SELECT 1, 2;
$$;
--DDL_STATEMENT_END--

CALL ptest4a(NULL, NULL);

--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptest4b(INOUT b int, INOUT a int)
LANGUAGE SQL
AS $$
CALL ptest4a(a, b);  -- error, not supported
$$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP PROCEDURE ptest4a;
--DDL_STATEMENT_END--


-- named and default parameters

--DDL_STATEMENT_BEGIN--
CREATE OR REPLACE PROCEDURE ptest5(a int, b text, c int default 100)
LANGUAGE SQL
AS $$
INSERT INTO cp_test VALUES(a, b);
INSERT INTO cp_test VALUES(c, b);
$$;
--DDL_STATEMENT_END--

delete from cp_test;

CALL ptest5(10, 'Hello', 20);
CALL ptest5(10, 'Hello');
CALL ptest5(10, b => 'Hello');
CALL ptest5(b => 'Hello', a => 10);

SELECT * FROM cp_test;


-- polymorphic types

--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptest6(a int, b anyelement)
LANGUAGE SQL
AS $$
SELECT NULL::int;
$$;
--DDL_STATEMENT_END--

CALL ptest6(1, 2);


-- collation assignment

--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptest7(a text, b text)
LANGUAGE SQL
AS $$
SELECT a = b;
$$;
--DDL_STATEMENT_END--

CALL ptest7(least('a', 'b'), 'a');


-- various error cases

CALL version();  -- error: not a procedure
CALL sum(1);  -- error: not a procedure

--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptestx() LANGUAGE SQL WINDOW AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptestx() LANGUAGE SQL STRICT AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE PROCEDURE ptestx(OUT a int) LANGUAGE SQL AS $$ INSERT INTO cp_test VALUES (1, 'a') $$;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER PROCEDURE ptest1(text) STRICT;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER FUNCTION ptest1(text) VOLATILE;  -- error: not a function
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER PROCEDURE cp_testfunc1(int) VOLATILE;  -- error: not a procedure
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER PROCEDURE nonexistent() VOLATILE;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP FUNCTION ptest1(text);  -- error: not a function
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PROCEDURE cp_testfunc1(int);  -- error: not a procedure
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PROCEDURE nonexistent();
--DDL_STATEMENT_END--


-- privileges

--DDL_STATEMENT_BEGIN--
CREATE USER regress_cp_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
GRANT INSERT ON cp_test TO regress_cp_user1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
REVOKE EXECUTE ON PROCEDURE ptest1(text) FROM PUBLIC;
--DDL_STATEMENT_END--
SET ROLE regress_cp_user1;
CALL ptest1('a');  -- error
RESET ROLE;
--DDL_STATEMENT_BEGIN--
GRANT EXECUTE ON PROCEDURE ptest1(text) TO regress_cp_user1;
--DDL_STATEMENT_END--
SET ROLE regress_cp_user1;
CALL ptest1('a');  -- ok
RESET ROLE;


-- ROUTINE syntax

--DDL_STATEMENT_BEGIN--
ALTER ROUTINE cp_testfunc1(int) RENAME TO cp_testfunc1a;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER ROUTINE cp_testfunc1a RENAME TO cp_testfunc1;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER ROUTINE ptest1(text) RENAME TO ptest1a;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER ROUTINE ptest1a RENAME TO ptest1;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP ROUTINE cp_testfunc1(int);
--DDL_STATEMENT_END--

-- cleanup

--DDL_STATEMENT_BEGIN--
DROP PROCEDURE ptest1;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
DROP PROCEDURE ptest2;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP TABLE cp_test;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP USER regress_cp_user1;
--DDL_STATEMENT_END--