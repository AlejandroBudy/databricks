-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select
  order_id,
  books,
  filter(books, i -> i.quantity >= 2) as multiple_copies
from
  orders

-- COMMAND ----------

select
  order_id,
  multiple_copies
from
  (
    select
      order_id,
      filter(books, i -> i.quantity >= 2) as multiple_copies
    from
      orders
  )
where
  size(multiple_copies) > 0;

-- COMMAND ----------

select
  order_id,
  books,
  transform(books, b -> cast(b.subtotal * 0.8 as int)) as subtotal_after_discount
from
  orders

-- COMMAND ----------

create or replace function get_url(email string)
returns string

return concat("https://", split(email,"@" )[1])

-- COMMAND ----------

select email, get_url(email) domain
from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

describe function extended get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

select email, site_type(email) domain_category
from customers

-- COMMAND ----------

drop function get_url;
drop function site_type;

-- COMMAND ----------

  
