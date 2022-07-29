show local variables like '%lock%';
-- strict: only show variables that can be modified&cached in computing node
show variables like '%lock%' strict;
show global variables like '%lock%';
show global variables like '%lock%' strict;
show lock_wait_timeout;

set global innodb_lock_wait_timeout = 4;
show global variables like 'innodb_lock_wait_timeout' strict;
show session variables like 'innodb_lock_wait_timeout' strict;

set session innodb_lock_wait_timeout = 3;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set innodb_lock_wait_timeout = 2;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set local innodb_lock_wait_timeout = 1;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set persist innodb_lock_wait_timeout = 9;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set persist_only innodb_lock_wait_timeout = 11;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set shard global innodb_lock_wait_timeout = 4;
show global variables like 'innodb_lock_wait_timeout' strict;
show session variables like 'innodb_lock_wait_timeout' strict;

set shard session innodb_lock_wait_timeout = 3;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set shard local innodb_lock_wait_timeout = 1;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set shard persist innodb_lock_wait_timeout = 9;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set shard persist_only innodb_lock_wait_timeout = 11;
show session variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout' strict;

set innodb_lock_wait_timeout = 1;
set @@global.innodb_lock_wait_timeout = 4;
show global variables like 'innodb_lock_wait_timeout';
show session variables like 'innodb_lock_wait_timeout' strict;

set @@session.innodb_lock_wait_timeout = 3;
show variables like 'innodb_lock_wait_timeout';
show global variables like 'innodb_lock_wait_timeout';

set @@innodb_lock_wait_timeout = 2;
show session variables like 'innodb_lock_wait_timeout';
show global variables like 'innodb_lock_wait_timeout';

set @@local.innodb_lock_wait_timeout = 1;
show local variables like 'innodb_lock_wait_timeout';
show global variables like 'innodb_lock_wait_timeout';

set @@local.innodb_lock_wait_timeout = 5;
set @@persist.innodb_lock_wait_timeout = 9;
show local variables like 'innodb_lock_wait_timeout' strict;
show global variables like 'innodb_lock_wait_timeout';

set @@persist_only.innodb_lock_wait_timeout = 11;
show innodb_lock_wait_timeout;
show global variables like 'innodb_lock_wait_timeout';

show persist variables like 'innodb_lock_wait_timeout' strict;
show persist_only variables like 'innodb_lock_wait_timeout' strict;

-- bug 139 set [shard][@@]global/session/local/persist/persist_only varname=value come to nothing 
set @@local.innodb_lock_wait_timeout = 5;
set @@persist.innodb_lock_wait_timeout = 9;
set @@session.innodb_lock_wait_timeout = 3;
set @@global.innodb_lock_wait_timeout = 4;
set persist innodb_lock_wait_timeout = 11;
set shard persist innodb_lock_wait_timeout = 11;


create table tt(a int primary key, b int) partition by list(a);
create table tt1 partition of tt for values in (1);
create table tt2 partition of tt for values in (2);
create table tt3 partition of tt for values in (3);
create table tt4 partition of tt for values in (4);
insert into tt values(1,1),(2,2),(3,3),(4,4);
set session debug='+d,crash_before_flush_binlog';
-- connection breaks
update tt set b=b+1;
-- cached value (innodb_lock_wait_timeout=5) should be set.
-- manual test also show that the var cache&restore feature works alright. -- dzw
select pg_sleep(3);
select*from tt;
select pg_sleep(5);
select*from tt;
select pg_sleep(7);
select*from tt;
show innodb_lock_wait_timeout;
show local variables like 'innodb_lock_wait_timeout';
-- show variables like 'debug';
-- show session variables like '%debug%';
show global variables like 'innodb_lock_wait_timeout' strict;

-- show variables;
-- show variables strict;
-- show global variables;
show global variables strict;
drop table if exists tt;
drop table if exists tt;
drop table if exists tt;
