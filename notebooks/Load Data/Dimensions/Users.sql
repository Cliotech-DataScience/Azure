-- Databricks notebook source
--  create table dwhdb.Users

-- (
--  User_ID	Int,
--  User_Name string,
--  User_Status string,
--  Manager_Name string,
--  User_Type string,
--  User_Category string,
--  Email string,
--  Last_Login_Date timestamp,
--  Salesman_Serial int,
--  Extension string,
--  Logon_Name string,
--  Team string,
--  AD_Name string,
--  AD_Display_Name string,
--  AD_OU_Path string,
--  AD_Extension string,
--  Language string,
--  Main_Broker string,
--  Main_Parent_Group string,
--  received timestamp
--  )
--  using delta;


-- COMMAND ----------

show create table dwhdb.Customers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Usr_df = spark.read \
-- MAGIC     .option('header',True) \
-- MAGIC     .option('charset', 'UTF-8') \
-- MAGIC      .option('quote', "") \
-- MAGIC      .option('sep', "\01") \
-- MAGIC     .csv('/mnt/dataloadestore/rawdata/DWH_Users/*.csv')
-- MAGIC Usr_df.createOrReplaceTempView('ods_Users')

-- COMMAND ----------

select * from ods_Users

-- COMMAND ----------

create or replace temp view vUsers
as
select  User_ID, 
    get_json_object(details, "$.User_Name") as User_Name
   ,get_json_object(details, "$.User_Status") as User_Status
   ,get_json_object(details, "$.Manager_Name") as Manager_Name
   ,get_json_object(details, "$.User_Type") as User_Type 
   ,get_json_object(details, "$.User_Category") as User_Category 
   ,get_json_object(details, "$.Email") as Email
   ,cast(get_json_object(details, "$.Last_Login_Date") as timestamp) as Last_Login_Date 
   ,cast(get_json_object(details, "$.Salesman_Serial") as int) as Salesman_Serial
   ,get_json_object(details, "$.Extension") as Extension 
   ,get_json_object(details, "$.Logon_Name") as Logon_Name
   ,get_json_object(details, "$.Team") as Team
   ,get_json_object(details, "$.AD_Name") as AD_Name
   ,get_json_object(details, "$.AD_Display_Name") as AD_Display_Name
   ,get_json_object(details, "$.AD_OU_Path") as AD_OU_Path
   ,get_json_object(details, "$.AD_Extension") as AD_Extension
   ,get_json_object(details, "$.Language") as Language
   ,get_json_object(details, "$.Main_Broker") as Main_Broker
   ,get_json_object(details, "$.Main_Parent_Group") as Main_Parent_Group
   ,details
   ,Now() as received

from  ods_Users 

-- COMMAND ----------

select * from vUsers

-- COMMAND ----------

MERGE INTO dwhdb.Users eu
USING vUsers u
ON eu.User_ID = u.User_ID
WHEN MATCHED THEN UPDATE SET *    
WHEN NOT MATCHED THEN INSERT *  

-- COMMAND ----------

OPTIMIZE dwhdb.Users

-- COMMAND ----------

  set spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

VACUUM dwhdb.Users RETAIN 12 HOURS 

-- COMMAND ----------

 ANALYZE TABLE dwhdb.Users COMPUTE STATISTICS ;