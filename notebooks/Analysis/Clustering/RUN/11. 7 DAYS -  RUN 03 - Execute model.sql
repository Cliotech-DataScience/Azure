-- Databricks notebook source
create or replace temp view 7_Models_accounts_pop
as
select Accountnumber  
from dwhdb.Models_Execution
where 1=1
 and Model_ID=207
 and Data_Mining_Date is not null
 and Model_Execution_Date is null
 and Comment is null
;
  
cache table 7_Models_accounts_pop;  

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC accounts = spark.table("7_Models_accounts_pop").count()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC if accounts >3:
-- MAGIC   print(accounts,"accounts","ETL will run " )
-- MAGIC   print("run ")
-- MAGIC   dbutils.notebook.run("../7_DAYS/11. Customers 7 Days - 12. Execute Model", 0,{})
-- MAGIC else: print(accounts , "accounts" , "less then 4 accounts ETL will not run")

-- COMMAND ----------

