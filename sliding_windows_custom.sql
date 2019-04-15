create foreign table stream (
    id integer,
    event_timestamp timestamptz
    )
    server pipelinedb;

create view sw with (action = materialize)
as
select id,
       count(id),
       max(event_timestamp) as last_modified
from stream
where (event_timestamp > clock_timestamp() - interval '24 hours')
group by id;

insert into stream values(1, current_timestamp - interval '23 hours');
insert into stream values(1, current_timestamp - interval '23 hours 59 minute');

select * from sw;

-- expected
-- 1	2	2019-04-14 11:35:25.704635

-- actual
-- 1	1	2019-04-14 11:35:25.704635

drop foreign table stream cascade;