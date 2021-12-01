create type vihicles as enum ('car', 'suv', 'truck', 'mpv', 'wagon', 'hatchback');
drop table if exists tvehicles;
create table tvehicles (a int, b vihicles);
insert into tvehicles values(1, 'car'),(2, 'suv'),(3, 'truck'),(4, 'hatchback'),(5, 'mpv'), (6, 'wagon');
select*from tvehicles;
select*from tvehicles where b > 'suv';
select*from tvehicles where b <= 'mpv';
select*from tvehicles where a>2;
select*from tvehicles where b='suv';
update tvehicles set b='mpv' where b='truck';
select*from tvehicles order by a;
delete from tvehicles where b='mpv';
select*from tvehicles;
delete from tvehicles where b='suv';
select*from tvehicles;
update tvehicles set b='mpv';
select*from tvehicles;

