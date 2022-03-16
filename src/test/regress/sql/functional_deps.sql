-- from http://www.depesz.com/index.php/2010/04/19/getting-unique-elements/

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE articles (
    id int CONSTRAINT articles_pkey PRIMARY KEY,
    keywords text,
    title text UNIQUE NOT NULL,
    body text UNIQUE,
    created date
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE articles_in_category (
    article_id int,
    category_id int,
    changed date,
    PRIMARY KEY (article_id, category_id)
);
--DDL_STATEMENT_END--

-- test functional dependencies based on primary keys/unique constraints

-- base tables

-- group by primary key (OK)
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id;

-- group by unique not null (fail/todo)
SELECT id, keywords, title, body, created
FROM articles
GROUP BY title;

-- group by unique nullable (fail)
SELECT id, keywords, title, body, created
FROM articles
GROUP BY body;

-- group by something else (fail)
SELECT id, keywords, title, body, created
FROM articles
GROUP BY keywords;

-- multiple tables

-- group by primary key (OK)
SELECT a.id, a.keywords, a.title, a.body, a.created
FROM articles AS a, articles_in_category AS aic
WHERE a.id = aic.article_id AND aic.category_id in (14,62,70,53,138)
GROUP BY a.id;

-- group by something else (fail)
SELECT a.id, a.keywords, a.title, a.body, a.created
FROM articles AS a, articles_in_category AS aic
WHERE a.id = aic.article_id AND aic.category_id in (14,62,70,53,138)
GROUP BY aic.article_id, aic.category_id;

-- JOIN syntax

-- group by left table's primary key (OK)
SELECT a.id, a.keywords, a.title, a.body, a.created
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY a.id;

-- group by something else (fail)
SELECT a.id, a.keywords, a.title, a.body, a.created
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY aic.article_id, aic.category_id;

-- group by right table's (composite) primary key (OK)
SELECT aic.changed
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY aic.category_id, aic.article_id;

-- group by right table's partial primary key (fail)
SELECT aic.changed
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY aic.article_id;


-- example from documentation

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE products (product_id int, name text, price numeric);
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE sales (product_id int, units int);
--DDL_STATEMENT_END--

-- OK
SELECT product_id, p.name, (sum(s.units) * p.price) AS sales
    FROM products p LEFT JOIN sales s USING (product_id)
    GROUP BY product_id, p.name, p.price;

-- fail
SELECT product_id, p.name, (sum(s.units) * p.price) AS sales
    FROM products p LEFT JOIN sales s USING (product_id)
    GROUP BY product_id;
	
--DDL_STATEMENT_BEGIN--
ALTER TABLE products ADD PRIMARY KEY (product_id);
--DDL_STATEMENT_END--

-- OK now
SELECT product_id, p.name, (sum(s.units) * p.price) AS sales
    FROM products p LEFT JOIN sales s USING (product_id)
    GROUP BY product_id;


-- Drupal example, http://drupal.org/node/555530

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE node (
    nid SERIAL,
    vid integer NOT NULL default '0',
    type varchar(32) NOT NULL default '',
    title varchar(128) NOT NULL default '',
    uid integer NOT NULL default '0',
    status integer NOT NULL default '1',
    created integer NOT NULL default '0',
    -- snip
    PRIMARY KEY (nid, vid)
);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
CREATE TEMP TABLE users (
    uid integer NOT NULL default '0',
    name varchar(60) NOT NULL default '',
    pass varchar(32) NOT NULL default '',
    -- snip
    PRIMARY KEY (uid),
    UNIQUE (name)
);
--DDL_STATEMENT_END--
-- OK
SELECT u.uid, u.name FROM node n
INNER JOIN users u ON u.uid = n.uid
WHERE n.type = 'blog' AND n.status = 1
GROUP BY u.uid, u.name;

-- OK
SELECT u.uid, u.name FROM node n
INNER JOIN users u ON u.uid = n.uid
WHERE n.type = 'blog' AND n.status = 1
GROUP BY u.uid;


-- Check views and dependencies

-- fail
--DDL_STATEMENT_BEGIN--
CREATE TEMP VIEW fdv1 AS
SELECT id, keywords, title, body, created
FROM articles
GROUP BY body;
--DDL_STATEMENT_END--

-- OK
--DDL_STATEMENT_BEGIN--
CREATE TEMP VIEW fdv1 AS
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id;
--DDL_STATEMENT_END--

-- fail
--DDL_STATEMENT_BEGIN--
ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP VIEW fdv1;
--DDL_STATEMENT_END--


-- multiple dependencies
--DDL_STATEMENT_BEGIN--
CREATE TEMP VIEW fdv2 AS
SELECT a.id, a.keywords, a.title, aic.category_id, aic.changed
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY a.id, aic.category_id, aic.article_id;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT; -- fail
--DDL_STATEMENT_END--
--DDL_STATEMENT_BEGIN--
ALTER TABLE articles_in_category DROP CONSTRAINT articles_in_category_pkey RESTRICT; --fail
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP VIEW fdv2;
--DDL_STATEMENT_END--


-- nested queries

--DDL_STATEMENT_BEGIN--
CREATE TEMP VIEW fdv3 AS
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id
UNION
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id;
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT; -- fail
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP VIEW fdv3;
--DDL_STATEMENT_END--


--DDL_STATEMENT_BEGIN--
CREATE TEMP VIEW fdv4 AS
SELECT * FROM articles WHERE title IN (SELECT title FROM articles GROUP BY id);
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT; -- fail
--DDL_STATEMENT_END--

--DDL_STATEMENT_BEGIN--
DROP VIEW fdv4;
--DDL_STATEMENT_END--

-- prepared query plans: this results in failure on reuse

PREPARE foo AS
  SELECT id, keywords, title, body, created
  FROM articles
  GROUP BY id;

EXECUTE foo;

--DDL_STATEMENT_BEGIN--
ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT;
--DDL_STATEMENT_END--

EXECUTE foo;  -- fail
