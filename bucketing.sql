-- Bucket products produced in the last 24 hours per 5 minutes, count ok and nok.
-- See: http://docs.pipelinedb.com/builtin.html?highlight=minute#miscellaneous-functions

create foreign table products_stream
  (
  product_id varchar(255),
  ok boolean
  )
  server pipelinedb;

create view recent_products with (sw = '24 hours')
as
select product_id, ok, arrival_timestamp
from products_stream;

create view recent_products_5sec
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
from recent_products_5sec;

drop view recent_products_5sec;
drop view recent_products;
drop foreign table products_stream;

