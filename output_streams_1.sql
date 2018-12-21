-- see: http://docs.pipelinedb.com/streams.html#delta-streams

create foreign table products_stream
  (
  product_id varchar(255),
  ok boolean
  )
  server pipelinedb;

create view total_products with (action = materialize)
as
select ok, count(*)
from products_stream
group by ok;

create view total_products_deltas
as
select (delta).ok, (old).count as old_count, (new).count as new_count, (delta).count as diff
from output_of('total_products');

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
from total_products;

select *
from total_products_deltas;

drop view total_products_deltas;
drop view total_products;
drop foreign table products_stream;
