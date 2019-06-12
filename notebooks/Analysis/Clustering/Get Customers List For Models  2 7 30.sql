-- Databricks notebook source
-- DBTITLE 1,get deposit events older form last 2 days
create or replace temp view new_accounts_2_days
as
  select Accountnumber,First_Deposit_Date,now() as Date_Added
  from  dwhdb.Customers_Accumulation ca
  where First_Deposit_Day <=date_add(now(), -2)
      and First_Deposit_Day>='2018-01-01'
      and NOT EXISTS ( select 1 as t
                    from  dwhdb.Models_Execution b 
                    where ca.Accountnumber=b.Accountnumber 
                    and b.Model_ID=202
                    )


-- COMMAND ----------

  select count(*) as count
  from new_accounts_2_days

-- COMMAND ----------

-- DBTITLE 1,create view for insert 2 day 
create or replace temp view new_accounts_all_insert_2_day
as
select Accountnumber,
      First_Deposit_Date AS First_Deposit_Date ,
      202  AS Model_ID	 ,
      Date_Added ,
      NULL AS Data_Mining_Date	 ,
      NULL AS Model_Execution_Date	  ,
      NULL AS Model_Result	 ,
      NULL AS Sync_Date	  ,
      NULL AS Comment  
from new_accounts_2_days


-- COMMAND ----------

insert INTO dwhdb.Models_Execution
select  * from new_accounts_all_insert_2_day

-- COMMAND ----------

select count(*) 
from  dwhdb.Models_Execution
where Model_ID=202

-- COMMAND ----------

refresh table  dwhdb.Models_Execution;


-- COMMAND ----------

OPTIMIZE dwhdb.Models_Execution;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM   dwhdb.Models_Execution RETAIN 25 HOURS;

-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 1,get deposit events older form last 7 days
 create or replace temp view new_accounts_7_days
as
  select Accountnumber,First_Deposit_Date,now() as Date_Added
  from  dwhdb.Customers_Accumulation ca
  where First_Deposit_Day <=date_add(now(), -7)
      and First_Deposit_Day>='2018-01-01'
      and NOT EXISTS ( select 1 as t
                    from  dwhdb.Models_Execution b 
                    where ca.Accountnumber=b.Accountnumber 
                    and b.Model_ID=207
                    );

-- COMMAND ----------

  select count(*) as count
  from new_accounts_7_days

-- COMMAND ----------

create or replace temp view new_accounts_all_insert_7_day
as
select Accountnumber,
      First_Deposit_Date AS First_Deposit_Date ,
      207  AS Model_ID	 ,
      Date_Added ,
      NULL AS Data_Mining_Date	 ,
      NULL AS Model_Execution_Date	  ,
      NULL AS Model_Result	 ,
      NULL AS Sync_Date	  ,
      NULL AS Comment  
from new_accounts_7_days  ;

-- COMMAND ----------

insert INTO dwhdb.Models_Execution
select  * from new_accounts_all_insert_7_day

-- COMMAND ----------

refresh table  dwhdb.Models_Execution;

-- COMMAND ----------

OPTIMIZE dwhdb.Models_Execution;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM   dwhdb.Models_Execution RETAIN 25 HOURS;

-- COMMAND ----------

-- DBTITLE 1,get deposit events older form last 30 days
 create or replace temp view new_accounts_30_days
as
  select Accountnumber,First_Deposit_Date,now() as Date_Added
  from  dwhdb.Customers_Accumulation ca
  where First_Deposit_Day <=date_add(now(), -30)
      and First_Deposit_Day>='2018-01-01'
      and NOT EXISTS ( select 1 as t
                    from  dwhdb.Models_Execution b 
                    where ca.Accountnumber=b.Accountnumber 
                    and b.Model_ID=230
                    );

-- COMMAND ----------

select count(*) as count
  from new_accounts_30_days

-- COMMAND ----------

create or replace temp view new_accounts_all_insert_30_day
as
select Accountnumber,
      First_Deposit_Date AS First_Deposit_Date ,
      230  AS Model_ID	 ,
      Date_Added ,
      NULL AS Data_Mining_Date	 ,
      NULL AS Model_Execution_Date	  ,
      NULL AS Model_Result	 ,
      NULL AS Sync_Date	  ,
      NULL AS Comment  
from new_accounts_30_days  ;

-- COMMAND ----------

insert INTO dwhdb.Models_Execution
select  * from new_accounts_all_insert_30_day

-- COMMAND ----------

refresh table  dwhdb.Models_Execution;

-- COMMAND ----------

OPTIMIZE dwhdb.Models_Execution;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM   dwhdb.Models_Execution RETAIN 25 HOURS;

-- COMMAND ----------

select Model_ID ,count(*)
from dwhdb.Models_Execution
group by Model_ID

-- COMMAND ----------

