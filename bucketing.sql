-- Bucket products produced in the last 24 hours per 5 minutes, count ok and nok.
-- See: http://docs.pipelinedb.com/builtin.html?highlight=minute#miscellaneous-functions

create foreign table products_stream
  (
  product_id varchar(255),
  ok boolean
  )
  server pipelinedb;

create view recent_products with (sw = '10 second')
as
select product_id, ok, arrival_timestamp
from products_stream;

-- FIXME: does not work as expected
create view recent_products_5sec_1
as
select date_round((new).arrival_timestamp, '5 seconds')  as bucket_5sec,
       sum(case when (new).ok = true then 1 else 0 end)  as oks,
       sum(case when (new).ok = false then 1 else 0 end) as noks
from output_of('recent_products')
group by bucket_5sec;

create view recent_products_5sec_2
as
select date_round(arrival_timestamp, '5 seconds')  as bucket_5sec,
       sum(case when ok = true then 1 else 0 end)  as oks,
       sum(case when ok = false then 1 else 0 end) as noks
from recent_products
group by bucket_5sec;

insert into products_stream
values (1, false);
insert into products_stream
values (2, false);
insert into products_stream
values (3, true);
insert into products_stream
values (4, true);
insert into products_stream
values (5, false);
insert into products_stream
values (6, false);

select *
from recent_products;

select *
from recent_products_5sec_1;

select *
from recent_products_5sec_2;

drop view recent_products_5sec_1;
drop view recent_products_5sec_2;
drop view recent_products;
drop foreign table products_stream;

