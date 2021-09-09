drop function if exists find_hash(json);
create or replace function find_hash(node json)
returns json language plpgsql
as
$$
declare
  x json;
  child json;
begin
  if node->>'Node Type' = 'Hash' then
    return node;
  else
    for child in select json_array_elements(node->'Plans')
    loop
      x = find_hash(child);
      if x is not null then
        return x;
      end if;
    end loop;
    return null;
  end if;
end;
$$;

drop function if exists hash_join_batches(varchar(50))
create or replace function hash_join_batches(query varchar(50))
returns table (original int, final int) language plpgsql
as
$$
declare
  whole_plan json;
  hash_node json;
begin
  for whole_plan in
    execute 'explain (analyze, format ''json'') ' || query
  loop
    hash_node = find_hash(json_extract_path(whole_plan, '0', 'Plan'));
    original = hash_node->>'Original Hash Batches';
    final = hash_node->>'Hash Batches';
    return next;
  end loop;
end;
$$;
drop table if exists simple;
create table simple(id integer, info varchar(100));
insert into simple select generate_series(1, 20000) AS id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

