-- See: https://gitter.im/pipelinedb/pipelinedb?at=5c1cda153c4093612c3de9e5

create table updates
(
  view_name varchar(255),
  updated_at   timestamptz
);

create or replace function logupdate()
  returns trigger as
$$
begin
  insert
  into updates (view_name, updated_at)
  values (tg_argv[0], current_timestamp);
  return new;
end;
$$
  language plpgsql;

create foreign table users_stream
  (user_id varchar(255))
  server pipelinedb;

create view recent_users with (sw = '1 minute')
as
select user_id::integer, arrival_timestamp
from users_stream;

create view total_users with (action = materialize)
as
select count(*)
from users_stream;

create view t_recent_users
  with (action = transform, outputfunc=logupdate('recent_users'))
as
select (new).user_id
from output_of('recent_users');

create view t_total_users
  with (action = transform, outputfunc=logupdate('total_users'))
as
select (new).count
from output_of('total_users');

insert into users_stream
values (1);
insert into users_stream
values (2);
insert into users_stream
values (3);

select *
from recent_users;
select *
from total_users;
select * from updates;

drop table updates;
drop view t_recent_users;
drop view t_total_users;
drop view recent_users;
drop view total_users;
drop foreign table users_stream;
drop function logUpdate;