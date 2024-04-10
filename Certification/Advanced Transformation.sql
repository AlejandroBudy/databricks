-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

select * from customers

-- COMMAND ----------

describe customers

-- COMMAND ----------

select customer_id, profile:first_name, profile:address:country
from customers

-- COMMAND ----------

select from_json(profile ) as profile_struct
from customers

-- COMMAND ----------

select profile from customers limit 1

-- COMMAND ----------

create or replace temp view parsed_customers as 
select customer_id, from_json(profile,schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}') ) as profile_struct
from customers; 

select * from parsed_customers;

-- COMMAND ----------

describe parsed_customers

-- COMMAND ----------

select customer_id, profile_struct.first_name, profile_struct.address.country from parsed_customers

-- COMMAND ----------

create or replace temp view customers_final as 
  select customer_id, profile_struct.*
  from parsed_customers;
select * from customers_final;  

-- COMMAND ----------

select order_id, customer_id, books from orders

-- COMMAND ----------

select order_id, customer_id, explode(books) as book
from orders;

-- COMMAND ----------

select customer_id, collect_set(order_id) as orders_set, collect_set(books.book_id) as books_set
from orders
group by customer_id

-- COMMAND ----------

select customer_id,
       collect_set(books.book_id) as before_flatten,
       array_distinct(flatten(collect_set(books.book_id))) as after_flatten
from orders
group by customer_id       

-- COMMAND ----------

create or replace view orders_enriched as 
select * 
from (
  select *, explode(books) as book
  from orders
 ) o
inner join books b 
on o.book.book_id = b.book_id;

select * from orders_enriched;


-- COMMAND ----------

create or replace temp view orders_updates
as select * from parquet.`${dataset.bookstore}/orders-new`;

select * from orders
union
select * from orders_updates

-- COMMAND ----------

select * from orders
intersect
select * from orders_updates

-- COMMAND ----------

select * from orders
minus
select * from orders_updates

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
