-- Databricks notebook source
create or replace temp view 2_Models_accounts_pop
as
select Accountnumber  
from dwhdb.Models_Execution
where 1=1
 and Model_ID=202
 and Data_Mining_Date is not null
 and Model_Execution_Date is null
 and Comment is null
;
  
cache table 2_Models_accounts_pop;  

-- COMMAND ----------

select * from  2_Models_accounts_pop

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC accounts = spark.table("2_Models_accounts_pop").count()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC if accounts >3:
-- MAGIC  print(accounts,"accounts","ETL will run " )
-- MAGIC  dbutils.notebook.run("../2_DAYS/10. Customers 2 Days - 12. Execute Model", 0,{})
-- MAGIC else: print(accounts , "accounts" , "less then 4 accounts ETL will not run")

-- COMMAND ----------

