-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

create table orders as 
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders

-- COMMAND ----------

create or replace table orders as 
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

-- attempt new schema 
insert overwrite orders 
select *, current_timestamp() from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

insert into orders
select * from parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

select * from customers

-- COMMAND ----------

create or replace temp view customers_updates as 
select * from json.`${dataset.bookstore}/customers-json-new`;

-- merge with conditions
merge into customers c
using customers_updates u
on c.customer_id = u.customer_id
-- condicion extra, si existe check el email 
when matched and c.email is null and u.email is not null then
  update set email = u.email, updated = u.updated
-- si no existe   
when not matched then insert *  

-- COMMAND ----------

create or replace temp view books_updates
(book_id string, title string, author string, category string, price double) 
using csv
options(
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

select * from books_updates

-- COMMAND ----------

merge into books b 
using books_updates u 
on b.book_id = u.book_id and b.title = u.title
when not matched and u.category = 'Computer Science' then
  insert *
