-- Databricks notebook source
-- DBTITLE 1,get population
create or replace temp view  accounts_pop
as
select Accountnumber
from dwhdb.Models_Execution
where Model_ID=230
and Data_Mining_Date is null;

cache table accounts_pop;

-- COMMAND ----------

select count(*) from accounts_pop

-- COMMAND ----------

-- DBTITLE 1,contacts data
 create or replace temp view contacts_data
as
select Contact_Id
   ,s.Supplier_Group
from dwhdb.contacts c
left join dwhdb.serials s on s.Serial= get_json_object(c.Contact_Details, "$.Contact_Serial");

cache table contacts_data

-- COMMAND ----------

create or replace temp view population_data
 as
 select 
      c.AccountNumber
     ,CAST(get_json_object(details, "$.FirstDepositDay")AS DATE) AS FirstDepositDay
     ,CAST(get_json_object(details, "$.FirstDepositDate")AS  timestamp) AS FirstDepositDate -- cast to datetime
     ,get_json_object(details, "$.FirstDeposit_USD")  AS FirstDeposit_USD
     ,get_json_object(details, "$.ContactID")  AS ContactID
     ,c.folder,c.FolderType
     ,con.Supplier_Group
from dwhdb.customers c
   left join  contacts_data con on con.Contact_Id= get_json_object(c.details, "$.ContactID")    
   join    accounts_pop ac on ac.Accountnumber=c.AccountNumber ;
 
 cache table population_data

-- COMMAND ----------

-- DBTITLE 1,Deposit Events for all analysis period
create or replace temp view  deposit_events
as

with deposits
as
(
select Contact_Id
      ,cast(Request.Amount_USD as int) as Event_Sum 
      ,Event_Date_day
      ,Event_Date
      ,p.AccountNumber
      ,P.folder
      ,p.FirstDepositday
     ,p.FirstDepositDate
     ,cast (p.FirstDeposit_USD as int) as FirstDeposit_USD
     ,nvl(p.Supplier_Group,'unknow') as Supplier_Group
from dwhdb.events e
     join population_data p on  p.AccountNumber = e.AccountNumber
where source='fxnet'
and Event_Name='accounting'
and Event_Details='Deposit'
and Event_Date_day>='2019-01-01'
--and Event_Date_day>=date_add(now(), -35)
 )
 select AccountNumber
      ,folder
      ,Supplier_Group
      ,FirstDepositDate 
     ,min( FirstDeposit_USD)   as first_deposit_amount
     ,sum(case when Event_Date>FirstDepositDate and  Event_Date<date_add(FirstDepositday,2)then Event_Sum else 0 end) as deposit_amount_1_days_after_deposit
     ,sum(case when Event_Date>FirstDepositDate and Event_Date<date_add(FirstDepositday,2) then 1 else 0 end )       as deposit_events_1_Days_after_Deposit                
     ,min(case when Event_Date>FirstDepositDate and Event_Date<date_add(FirstDepositday,2) then Event_Date end) as first_deposit_Date_1_Days_after_first
    ----- 30 days
     ,min(case when Event_Date>FirstDepositDate and Event_Date<date_add(FirstDepositday,30) then Event_Date end) as first_deposit_Date_30_Days_after_first
     ,max(case when Event_Date>FirstDepositDate and Event_Date<date_add(FirstDepositday,30) then Event_Date end) as last_deposit_Date_30_Days_after_first
     ,sum(case when Event_Date>FirstDepositDate and  Event_Date<date_add(FirstDepositday,30)then Event_Sum else 0 end) as deposit_amount_30_days_after_deposit
     ,sum(case when Event_Date>FirstDepositDate and Event_Date<date_add(FirstDepositday,30) then 1 else 0 end )       as deposit_events_30_Days_after_Deposit  
from deposits
group by AccountNumber,folder,Supplier_Group,FirstDepositDate;

 cache table deposit_events;

-- COMMAND ----------

-- DBTITLE 1,Transaction data for period
create or replace temp view  30_days_trading_activity
as
select 
       e.AccountNumber
      ,e.Event_Date
      ,e.Event_Date_day
      ,Request["TransactionType"] as TransactionType
      ,Request["PositionNumber"] as PositionNumber
      ,Request["Direction"] as Direction
      ,Request["Instrument_Name"] as Instrument_Name
      ,Request["Instrument_ID"] as Instrument_ID
     
from dwhdb.events e
join population_data p on  (p.AccountNumber = e.AccountNumber) and  e.Event_Date_day<= date_add(p.FirstDepositday,29) 
where Source='fxnet'
and Event_Name='deal'
and Event_Date_day>='2019-01-01' ;
-- and Event_Date_day>=date_add(now(), -29)  ; 
  
 cache table 30_days_trading_activity

-- COMMAND ----------

select count(*) from 30_days_trading_activity

-- COMMAND ----------

-- DBTITLE 1,  Transaction Events for all analysis period
create or replace temp view v_calc_transaction
as
with calc_transaction
as
(
select a.AccountNumber,a.PositionNumber
   ,min(Event_Date_day) as executionday 
   ,max(Instrument_Name) as instrument
   ,min(case when TransactionType='Open' then Event_Date end ) as open_position_date
from 30_days_trading_activity a
group by a.AccountNumber,a.PositionNumber
)
select AccountNumber
 ,count(distinct cast(executionday as date)) as trading_days
 ,count(*) as number_of_deals            
 ,count(distinct instrument) as unique_instruments_traded
 ,min(executionday) as first_deal_date_30  
  ,max(executionday) as last_deal_date_30  
  ,max(open_position_date) as last_deal_date
  ,min(open_position_date) as first_deal_date
  
from calc_transaction
group by AccountNumber ;

 cache table  v_calc_transaction

-- COMMAND ----------

-- DBTITLE 1,get account questions data
-- MAGIC %sql
-- MAGIC create or replace temp view questions_for_clustering
-- MAGIC as
-- MAGIC 
-- MAGIC select qu.AccountNumber,nvl(get_json_object(details, "$.AnnualIncome"),'didnt_answear') as Q_AnnualIncome  
-- MAGIC       ,nvl(get_json_object(details, "$.ExpectedInvestmentAmount"),'didnt_answear') as Q_ExpectedInvestmentAmount
-- MAGIC        ,nvl(get_json_object(details, "$.NetWorth"),'didnt_answear') as Q_NetWorth
-- MAGIC       ,nvl(get_json_object(details, "$.Occupation"),'didnt_answear') as Q_Occupation
-- MAGIC       ,nvl(get_json_object(details, "$.OriginOfFunds"),'didnt_answear') as Q_OriginOfFunds
-- MAGIC       ,nvl(get_json_object(details, "$.Title"),'didnt_answear') as Q_title
-- MAGIC       ,nvl(get_json_object(details, "$.CFDExperience"),'didnt_answear') as Q_CFDExperience
-- MAGIC from dwhdb.Customer_Questionnaires qu
-- MAGIC join accounts_pop ac on ac.Accountnumber = qu.AccountNumber

-- COMMAND ----------

-- DBTITLE 1,create view for all the data to insert 
create or replace temp view Customers_data_for_all_insert
as

with all_data
as
(
select   d.AccountNumber,
         case when (DATEDIFF(last_deal_date,first_deal_date)=0)  then 30
             when trading_days=1 then 30
             ELSE cast(DATEDIFF(last_deal_date,first_deal_date)/(trading_days-1) as int)
             end as avg_days_between_trading_days ,
             abs(DATEDIFF(last_deposit_Date_30_Days_after_first,  date_add(FirstDepositDate,29))) as days_from_last_deposit,
             DATEDIFF(first_deal_date,FirstDepositDate)                                           as days_from_activation_to_First_deal,
             abs(DATEDIFF(last_deal_date_30,  date_add(FirstDepositDate,29)))                     as days_from_last_trading_Day,
             deposit_amount_30_days_after_deposit+first_deposit_amount                            as IPA_30_DAYS,        
      
       d.folder,
       nvl(d.Supplier_Group,'unknow') as suppliergroup ,
       d.first_deposit_amount,
       c.number_of_deals,
       c.unique_instruments_traded,
       c.trading_days,
       d.FirstDepositDate,
       d.deposit_amount_30_days_after_deposit,
       d.deposit_events_30_Days_after_Deposit,
       qu.Q_AnnualIncome,
       Q_ExpectedInvestmentAmount,
       Q_NetWorth,
       Q_Occupation,
       Q_OriginOfFunds,
       Q_title,
       Q_CFDExperience       
from deposit_events d
left join v_calc_transaction c on  c.AccountNumber = d.AccountNumber
left join questions_for_clustering qu on qu.AccountNumber=d.AccountNumber

)
select * from all_data a

-- COMMAND ----------

select  count(*) from Customers_data_for_all_insert

-- COMMAND ----------

 
 -- drop table dwhdb.Clustering_Model_30_Days_Customers_Data
--  create table dwhdb.Clustering_Model_30_Days_Customers_Data
--  USING DELTA
--  select  * from Customers_data_for_all_insert
 

-- COMMAND ----------

-- DBTITLE 1,Add new Customers Data to Clustering Model 30 Days Customers_Data table
MERGE INTO dwhdb.Clustering_Model_30_Days_Customers_Data e
USING Customers_data_for_all_insert i
ON e.AccountNumber = i.AccountNumber    
WHEN  MATCHED  THEN UPDATE SET *   
WHEN NOT MATCHED THEN INSERT *  

-- COMMAND ----------

OPTIMIZE   dwhdb.Clustering_Model_30_Days_Customers_Data;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM   dwhdb.Clustering_Model_30_Days_Customers_Data RETAIN 0 HOURS;

-- COMMAND ----------

-- DBTITLE 1,Update Accounts Data Mining Date
create or replace temp view Accounts_to_update
as
select AccountNumber,FirstDepositDate from population_data ;


 cache table  Accounts_to_update;


-- COMMAND ----------

select count(*) 
from Accounts_to_update

-- COMMAND ----------

-- DBTITLE 1,Exclude special cases  IBS other cases
create or replace temp view  Accounts_to_Exclude
as

select Accountnumber,
       case when suppliergroup='IBS' then 'Exclude ibs suppliergroup' else null end  as  Comment
from Customers_data_for_all_insert
where suppliergroup='IBS'

-- COMMAND ----------

select count(*) from Accounts_to_Exclude

-- COMMAND ----------

create or replace temp view  Accounts_to_update_Data_Mining_Date
as
select a.Accountnumber,
      a.FirstDepositDate AS First_Deposit_Date ,
      230  AS Model_ID	 ,
      now()  AS Data_Mining_Date,
      e.Comment
from Accounts_to_update a
left join Accounts_to_Exclude e on  a.Accountnumber=e.Accountnumber

-- COMMAND ----------

-- DBTITLE 1,Update Models Execution
MERGE INTO dwhdb.Models_Execution  as t
USING Accounts_to_update_Data_Mining_Date  as s
ON  t.Accountnumber=s.Accountnumber and t.Model_ID=s.Model_ID
WHEN MATCHED THEN
  UPDATE SET  t.First_Deposit_Date=s.First_Deposit_Date,
              t.Data_Mining_Date=s.Data_Mining_Date,
              t.comment = s.comment
 

-- COMMAND ----------

OPTIMIZE dwhdb.Models_Execution;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM   dwhdb.Models_Execution RETAIN 25 HOURS;

-- COMMAND ----------

