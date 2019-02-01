-- see: http://docs.pipelinedb.com/streams.html#delta-streams

create foreign table updates
  (
  id int,
  name varchar(255)
  )
  server pipelinedb;

create view all_updates with (action = materialize)
as
select id, name
from updates;

create view recent_updates with (sw = '1 hour', action = materialize)
as
select id, name
from updates;

create view count_recent with (action = materialize)
as
select (new).name, count(*)
from output_of('recent_updates')
group by name;

create view count_all with (action = materialize)
as
select (new).name, count(*)
from output_of('all_updates')
group by name;

insert into updates
values (1, 'a');
insert into updates
values (2, 'b');
insert into updates
values (3, 'a');
insert into updates
values (4, 'a');
insert into updates
values (5, 'c');

-- FIXME: does not behave as expected
select * from count_recent;

select * from count_all;

drop view count_all;
drop view count_recent;
drop view recent_updates;
drop view all_updates;
drop foreign table updates;