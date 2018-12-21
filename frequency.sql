-- shows 5 errors that occur most frequently.

create foreign table events_stream
  (
  event_id varchar(255),
  type varchar(255)
  )
  server pipelinedb;

-- simple count (returns all unordered)
create view event_frequency with (action = materialize)
as
select type, count(*)
from events_stream
group by type;
-- [0a000] error: continuous queries don't support order by
-- order by count;

-- using freq (need to define values upfront)
create view event_frequency_2 with (action = materialize)
as
select freq(freq_agg(type), 'a') as a,
       freq(freq_agg(type), 'b') as b,
       freq(freq_agg(type), 'c') as c
from events_stream
group by type;

-- using top_k (ok!)
create view event_frequency_3 with (action = materialize)
as
select topk_agg(type, 3)
from events_stream;

insert into events_stream
values (1, 'a');
insert into events_stream
values (2, 'b');
insert into events_stream
values (3, 'b');
insert into events_stream
values (4, 'c');
insert into events_stream
values (5, 'c');
insert into events_stream
values (6, 'c');

select *
from event_frequency;

-- Returns:
-- type	count
-- c	  18
-- b	  12
-- a	  6

select *
from event_frequency_2;

-- Returns:
-- a  b c
-- 0  0	9
-- 0  6	0
-- 3	0	0


select topk(topk_agg)
from event_frequency_3;

-- Returns:
-- topk
-- (c,3)
-- (b,2)
-- (a,1)

drop view event_frequency;
drop view event_frequency_2;
drop view event_frequency_3;
drop foreign table events_stream;