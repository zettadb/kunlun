-- test quote handling for symbols and string constants in mysql connections

-- Part I. quotes for string constants
-- Do not run this part in pg connections, because pg always treats ""
-- as quotes for symbols, but mysql treats it as quotes for string literals
-- by default(assumed by all tests in this part)
-- and for symbols if mysql_ansi_quotes is true.

-- select "ab'cd";
-- select 'ab"c';
-- 
-- select "";
-- select """;
-- select """";
-- select """"";
-- 
-- select '';
-- select ''';
-- select '''';
-- select ''''';
-- 
-- select """''";
-- select '''""';
-- 
-- select """'''"";
-- select """'''\"'";
-- select '''"""'';
-- select '''"""\'"';
-- 
-- select "''\'\"\%%\__""'";
-- 
-- 
-- select "a'b""c\'d\"e''f";
-- select 'a"b''c\"d\'e""f';
-- select 'ab\tc\x\a\\';
-- select 'a\tb\nc';
-- select "ab\tc\x\a\\";
-- select "a\tb\nc";
-- 
-- select 'ab\tc';
-- select 'ab\nc';
-- select 'ab\xc';
-- select 'ab\rc';
-- select 'ab\arc';
-- select 'ab\fc';
-- select 'ab\cc';
-- 
-- select "ab\xc";
-- select "ab\nc";
-- select 'ab\\c';
-- select 'ab\c';
-- select "ab\\c";
-- select "ab\c";
-- 
-- 
-- select "abc';
-- select 'abc";
-- 
-- select "abc'";
-- select 'abc"';
-- 
-- select "abc''";
-- select 'abc""';
-- 
-- select "abc''def";
-- select "abc'def";
-- select "abc""def";
-- select "abc\"def";
-- 
-- select 'abc''def';
-- select 'abc\'def';
-- select 'abc"def';
-- select 'abc""def';
-- 
-- 
-- select 'abc'  'def';
-- select 'abc'  "def";
-- select "abc"  "def";
-- select "abc"  'def';
-- select 'abc'  
--     'def';
-- select 'abc'  
--     "def";
-- select "abc"  
--     "def";
-- select "abc"  
--     'def';
-- 
-- 
-- select 'abc''def';
-- 
-- select 'abc"def';
-- 
-- select 'abc""def';
-- 
-- select 'ab\tc';
-- 
-- select 'ab\fc';
-- select 'ab\cc';
-- select 'abc'  'def';
-- 
-- set mysql_ansi_quotes=true;
-- select 'abc'  "def";
-- 
-- select 'abc'  
--          "def";
-- 
-- set mysql_ansi_quotes=false;
-- 
-- select "abc''def";
-- 
-- select "abc'def";
-- 
-- select 'abc''def';
-- 
-- select 'abc"def';
-- 
-- select 'abc""def';
-- 
-- select 'abc'  'def';
-- 
-- select 'abc'  "def";
-- 
-- select 'abc'  
--          "def";
-- 
-- select 'abc'  
--      'def';
-- 
-- 
-- select "abc"  
--       "def";
-- 
-- select 'abc' 'def';
-- 
-- select 'abc' 
--      "def";
-- select 'abc'
--     "def" 
--     'xyz';
-- 
-- select 'abc'  "def" 'xyz';
-- select 'abc'  "def" "xyz";
-- select 'abc'  'def' 'xyz';
-- select 'abc'  'def' "xyz";
-- select "abc"  "def" 'xyz';
-- select "abc"  "def" "xyz";
-- select "abc"  'def' 'xyz';
-- select "abc"  'def' "xyz";
-- 
-- select 'abc'"def"'xyz';
-- select 'abc'"def""xyz";
-- select 'abc''def''xyz';
-- select 'abc''def'"xyz";
-- select "abc""def"'xyz';
-- select "abc""def""xyz";
-- select "abc"'def''xyz';
-- select "abc"'def'"xyz";
-- 
-- select "abc" 'def';
-- 
-- select "abc" 'def';
-- select 'ab\xc';
-- 
-- select 'ab\tc';
-- 
-- 
-- select 'abc' "def";
-- 
-- select "abc" "def";
-- select "abc" ;
-- 
-- select 'abc' ;
-- 
-- select 'ab"c' ;
-- 
-- select 'ab""c' ;
-- 
-- select "ab'c";
-- 
-- select "ab''c";
-- 
-- select "ab''c";
-- 
-- select "abc";
-- 
-- select 'abc' 'def';
-- select "abc" "def";
-- select "abc" 'def';
-- select 'abc' "def";
-- 
-- select 'abc' "'def";
-- 
-- select "abc"
--     "def";
-- select "abc"'def';
-- select 'abc'"def";
-- select "abc""def";
-- select 'abc''def';


-- above can only be run in mysql connections
-- Part II. Symbols quoting tests
set mysql_ansi_quotes = true;
drop schema if exists lexer_quotes cascade;
create schema lexer_quotes;
set search_path= 'lexer_quotes';
create table if not exists t1(a int);
create view "t""1" as select*from t1;
set mysql_ansi_quotes=true;
create view `t""1` as select*from t1;
drop view "t""1";
drop view `t""1`;

create view "t""1" as select*from t1;
drop view `t"1`;
create view "t`1" as select*from t1;
drop view `t``1`;
create view `t"2` as select*from t1;
drop view "t""2";
create view "t`1" as select*from t1;
drop view "t`1";

-- unlike mtr test driver, pg's make install simply give the file to psql so
-- below 3 queries are parsed as one query, but they should have been sent as 3
-- independent queries. so result isn't same as that of mtr.
create view `abc`" as select*from t1;
create view "abc"` as select*from t1;

create view `abc"` as select*from t1;
drop view `abc"`;
create view "abc`" as select*from t1;
drop view "abc`";
create view `abc``` as select*from t1;
drop view `abc```;
create view `abc""` as select*from t1;
drop view `abc""`;
create view "abc""" as select*from t1;
drop view "abc""";
create view "abc``" as select*from t1;
drop view "abc``";

create view `abc`d" as select*from t1;

create view "abc\"d`" as select*from t1;

create view `abc"d` as select*from t1;
drop view `abc"d`;
create view "abc`d" as select*from t1;
drop view "abc`d";
create view `abc``d` as select*from t1;
drop view "abc`d";
create view `abc""d` as select*from t1;
drop view "abc""""d";

create view "abc""d" as select*from t1;
drop view `abc"d`;
create view "abc``d" as select*from t1;
drop view `abc````d`;

drop schema if exists lexer_quotes cascade;
