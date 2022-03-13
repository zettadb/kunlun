--
-- CHAR
--

-- fixed-length by value
-- internally passed by value if <= 4 bytes in storage

SELECT char 'c' = char 'c' AS true;

--
-- Build a table for testing
--
--DDL_STATEMENT_BEGIN--
DROP TABLE if exists CHAR_TBL;
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TABLE CHAR_TBL(f1 char);
--DDL_STATEMENT_END--

INSERT INTO CHAR_TBL (f1) VALUES ('a');

INSERT INTO CHAR_TBL (f1) VALUES ('A');

-- any of the following three input formats are acceptable
INSERT INTO CHAR_TBL (f1) VALUES ('1');

INSERT INTO CHAR_TBL (f1) VALUES (2);

INSERT INTO CHAR_TBL (f1) VALUES ('3');

-- zero-length char
INSERT INTO CHAR_TBL (f1) VALUES ('');

-- try char's of greater than 1 length
INSERT INTO CHAR_TBL (f1) VALUES ('cd');
INSERT INTO CHAR_TBL (f1) VALUES ('c     ');


SELECT '' AS seven, * FROM CHAR_TBL;

SELECT '' AS six, c.*
   FROM CHAR_TBL c
   WHERE c.f1 <> 'a';

SELECT '' AS one, c.*
   FROM CHAR_TBL c
   WHERE c.f1 = 'a';

SELECT '' AS five, c.*
   FROM CHAR_TBL c
   WHERE c.f1 < 'a';

SELECT '' AS six, c.*
   FROM CHAR_TBL c
   WHERE c.f1 <= 'a';

SELECT '' AS one, c.*
   FROM CHAR_TBL c
   WHERE c.f1 > 'a';

SELECT '' AS two, c.*
   FROM CHAR_TBL c
   WHERE c.f1 >= 'a';
--DDL_STATEMENT_BEGIN--
DROP TABLE CHAR_TBL;
--DDL_STATEMENT_END--
--
-- Now test longer arrays of char
--
--DDL_STATEMENT_BEGIN--
CREATE TABLE CHAR_TBL(f1 char(4));
--DDL_STATEMENT_END--
INSERT INTO CHAR_TBL (f1) VALUES ('a');
INSERT INTO CHAR_TBL (f1) VALUES ('ab');
INSERT INTO CHAR_TBL (f1) VALUES ('abcd');
INSERT INTO CHAR_TBL (f1) VALUES ('abcde');
INSERT INTO CHAR_TBL (f1) VALUES ('abcd    ');

SELECT '' AS four, * FROM CHAR_TBL;
