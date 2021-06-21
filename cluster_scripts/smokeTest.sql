-- Copyright (c) 2019 ZettaDB inc. All rights reserved.
-- This source code is licensed under Apache 2.0 License,
-- combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

set client_min_messages to 'warning';
drop table if exists t1;
reset client_min_messages;
create table t1(id integer primary key, info text, wt integer);
insert into t1(id,info,wt) values(1, 'record1', 1);
insert into t1(id,info,wt) values(2, 'record2', 2);
update t1 set wt = 12 where id = 1;
select * from t1;
delete from t1 where id = 1;
select * from t1;
drop table t1;
