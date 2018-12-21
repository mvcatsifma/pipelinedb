-- see: http://docs.pipelinedb.com/streams.html#delta-streams

create foreign table status_updates
  (
  name varchar(255),
  status varchar(255)
  )
  server pipelinedb;

create view current_status with (action = materialize)
as
select name,
       max(arrival_timestamp)               as ts,
       keyed_max(arrival_timestamp, status) as status
from status_updates
group by name;

create view current_status_deltas
as
select (delta).name,
       (old).ts     as old_ts,
       (old).status as old_status,
       (new).ts     as new_ts,
       (new).status as new_status
from output_of('current_status');

insert into status_updates
values ('a', 'running');
insert into status_updates
values ('a', 'stopped');
insert into status_updates
values ('a', 'running');
insert into status_updates
values ('a', 'servicing');

select *
from current_status;

select *
from current_status_deltas;

drop view current_status_deltas;
drop view current_status;
drop foreign table status_updates;