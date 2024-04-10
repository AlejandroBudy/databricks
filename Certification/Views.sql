-- Databricks notebook source
CREATE TABLE smartphones (
    id INT,
    name STRING,
    brand STRING,
    year INT
);

INSERT INTO smartphones (id, name, brand, year)
VALUES (1, 'iPhone X', 'Apple', 2017),
       (2, 'Galaxy S9', 'Samsung', 2018),
       (3, 'Pixel 2', 'Google', 2017),
       (4, 'Xperia XZ2', 'Sony', 2018),
       (5, 'Mi 9', 'Xiaomi', 2019),
       (6, 'OnePlus 6', 'OnePlus', 2018),
       (7, 'Mate 20 Pro', 'Huawei', 2018),
       (8, 'LG G7 ThinQ', 'LG', 2018),
       (9, 'Razer Phone 2', 'Razer', 2018),
       (10, 'Nokia 8.1', 'Nokia', 2018);

-- COMMAND ----------

show tables

-- COMMAND ----------

create view view_apple_phones 
as select * from smartphones where brand = 'Apple'

-- COMMAND ----------

select * from view_apple_phones;

-- COMMAND ----------

show tables

-- COMMAND ----------

create temporary view temp_view_phones_brands as select distinct brand from smartphones; 

select * from temp_view_phones_brands;

-- COMMAND ----------

show tables 

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_view_smartphones AS
SELECT *
FROM smartphones
WHERE year > 2020
ORDER BY year DESC;

-- COMMAND ----------

select * from global_temp.global_view_smartphones;

-- COMMAND ----------

INSERT INTO smartphones (id, name, brand, year)
VALUES (1, 'Samsung Galaxy S21', 'Samsung', 2021),
       (2, 'iPhone 12', 'Apple', 2020),
       (3, 'OnePlus 9 Pro', 'OnePlus', 2021),
       (4, 'Google Pixel 5', 'Google', 2020),
       (5, 'Xiaomi Mi 11', 'Xiaomi', 2021),
       (6, 'Sony Xperia 1 III', 'Sony', 2021),
       (7, 'Motorola Edge+', 'Motorola', 2020),
       (8, 'LG Velvet', 'LG', 2020),
       (9, 'Nokia 9.3 PureView', 'Nokia', 2021),
       (10, 'Huawei Mate 40 Pro', 'Huawei', 2020);

-- COMMAND ----------

select * from global_temp.global_view_smartphones

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

drop table default.smartphones

-- COMMAND ----------

drop view view_apple_phones;
drop view global_temp.global_view_smartphones

-- COMMAND ----------


