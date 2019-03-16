-- Databricks notebook source
-- DBTITLE 1,Refresh external table to get the new files

-- create external table 

drop table if exists rawdata.FXNET_Accounting ;

create table rawdata.FXNET_Accounting
   (
   fact_account_trans_id	bigint,
   ActionDay timestamp,
   ActionDate timestamp,
   Details string,
   Received timestamp
   )
using csv
partitioned by (Received)
location '/mnt/dataloadestore/rawdata/FXNET_Accounting/'
options ('sep' = '\t' , 'quote'= "");


-- COMMAND ----------

-- read the new partitions
msck repair table rawdata.FXNET_Accounting

-- COMMAND ----------

-- DBTITLE 1,Create table service | get last  date | create view with the last date
-- MAGIC %python
-- MAGIC from azure.cosmosdb.table.tableservice import TableService
-- MAGIC from azure.cosmosdb.table.models import Entity
-- MAGIC table_service = TableService(account_name='dataloadestore', account_key='7wtLQIcK9q4QnXMCL6AO9I233TSi3hITG6tC4jO5VDEv3+ovoQo6NYv5IcboZo6Ncf5GeULV7uPdvUW+k8gJGA==')
-- MAGIC manage_table = 'etlManage'
-- MAGIC manage_partition_key = 'Load Events'
-- MAGIC manage_row_key = 'FXNET_Accounting'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC manage = table_service.get_entity(manage_table, manage_partition_key, manage_row_key)
-- MAGIC Last_Date_str = manage.Last_Incremental_Date
-- MAGIC Last_Date_df = sql("select cast('"+Last_Date_str+"' as timestamp) as Last_Date")
-- MAGIC Last_Date_df.createOrReplaceTempView("vlast_date")

-- COMMAND ----------

select *
from vlast_date

-- COMMAND ----------

-- DBTITLE 1,get max new data from mrr table and set to variable
-- MAGIC %python
-- MAGIC sql_query = "select max(raw.Received) as Last_Received_Date from rawdata.FXNET_Accounting raw  where raw.Received > cast('" + manage.Last_Incremental_Date +"' as timestamp) " 
-- MAGIC max_date_sql = sql(sql_query)
-- MAGIC max_received_col = max_date_sql.select('Last_Received_Date')
-- MAGIC 
-- MAGIC #do the if because if by any chance it will not find any date then the collect will fail "hive metadata error"
-- MAGIC if max_received_col is not None:
-- MAGIC   receivedDate = max_received_col.collect()[0][0]
-- MAGIC else:
-- MAGIC   receivedDate = None
-- MAGIC 
-- MAGIC max_date_sql.createOrReplaceTempView("vmax_date")

-- COMMAND ----------

cache table  vmax_date

-- COMMAND ----------

select *
from vmax_date

-- COMMAND ----------

-- DBTITLE 1,update entity table on next date = new max date received
-- MAGIC %python
-- MAGIC if receivedDate is not None:
-- MAGIC   new_val = {'PartitionKey': manage_partition_key, 'RowKey': manage_row_key,'Next_Incremental_Date' : receivedDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}
-- MAGIC else:
-- MAGIC   new_val = {'PartitionKey': manage_partition_key, 'RowKey': manage_row_key,'Next_Incremental_Date' : Last_Date_str}
-- MAGIC table_service.insert_or_merge_entity(manage_table , new_val)

-- COMMAND ----------

-- DBTITLE 1,prepare the data to be loaded to events
-- creating the view for load
create or replace temp view v_FXNET_Accounting
as
select 
    get_json_object(details, "$.Event_Name") as Event_Name, 
    get_json_object(details, "$.Event_Details") as Event_Details, 
    ActionDate,
    get_json_object(details, "$.AccountNumber") as AccountNumber,
    get_json_object(details, "$.Contact_Id") as Contact_Id,
  map(
        "fact_account_trans_id", get_json_object(details, "$.fact_account_trans_id"),
        "ActionDay", get_json_object(details, "$.ActionDay"),
        "ActionDate", get_json_object(details, "$.ActionDate"),
        "AccountNumber", get_json_object(details, "$.AccountNumber"),
        "Event_Name", get_json_object(details, "$.Event_Name"),
        "Event_Details", get_json_object(details, "$.Event_Details"),
        "ValueDate", get_json_object(details, "$.ValueDate"),
        "IsPending", get_json_object(details, "$.IsPending"),
        "Event_Broker_Id", get_json_object(details, "$.Event_Broker_Id"),
        "Event_Folder_Id", get_json_object(details, "$.Event_Folder_Id"),
        "Event_Folder", get_json_object(details, "$.Event_Folder"),
        "Event_Folder_Type", get_json_object(details, "$.Event_Folder_Type"),
        "AccountTypeId", get_json_object(details, "$.AccountTypeId"),
        "AccountType", get_json_object(details, "$.AccountType"),
        "NetDeposit", get_json_object(details, "$.NetDeposit"),
        "SymbolId", get_json_object(details, "$.SymbolId"),
        "Symbol", get_json_object(details, "$.Symbol"),
        "Amount", get_json_object(details, "$.Amount"),
        "Amount_USD", get_json_object(details, "$.Amount_USD"),
        "Bonus_Type", get_json_object(details, "$.Bonus_Type"),
        "PositionNumber", get_json_object(details, "$.PositionNumber"),
        "BopOptionId", get_json_object(details, "$.BopOptionId"),
        "NostroAccount", get_json_object(details, "$.NostroAccount"),
        "MoneyTransID", get_json_object(details, "$.MoneyTransID"),
        "AccountTransactionId", get_json_object(details, "$.AccountTransactionId"),
        "AccountBasesymbolId", get_json_object(details, "$.AccountBasesymbolId"),
        "AccountBasesymbol", get_json_object(details, "$.AccountBasesymbol"),
        "AccountBaseAmount", get_json_object(details, "$.AccountBaseAmount"),
        "Action_symbol_Id", get_json_object(details, "$.Action_symbol_Id"),
        "Action_Symbol", get_json_object(details, "$.Action_Symbol"),
        "Action_Amount", get_json_object(details, "$.Action_Amount"),
        "Action_to_AccountBase_Rate", get_json_object(details, "$.Action_to_AccountBase_Rate"),
        "CounterParty", get_json_object(details, "$.CounterParty"),
        "Received", Received
      ) as Request ,
  cast(ActionDay as date) as ActionDay
from rawdata.FXNET_Accounting r 
where r.Received >  (select cast(Last_Date as timestamp) from vlast_date) --Last_Call_Date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
  and r.Received <= (select cast(Last_Received_Date as timestamp) from vmax_date)--receivedDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] 
  

-- COMMAND ----------

select ActionDay, count(*)
from v_FXNET_Accounting
group by  ActionDay
order by ActionDay

-- COMMAND ----------

-- cahce account contacts for the join
cache table ods.accountcontacts

-- COMMAND ----------

-- DBTITLE 1,Insert the events to events table
-- -- insert the events
-- --create or replace temp view fxnet_deals_for_insert
-- --as
insert into dwhdb.Events
select 
  ActionDate as Event_Date    ,
  'fxnet' as Source  ,
  lower(Event_Name) as Event_Name,
  Event_Details as Event_Details,
  nvl(d.Contact_Id, ac.ContactID) as Contact_Id,
  d.Accountnumber,
  null as Broker_ID,
  null as  Broker,
  null	as Brand_Guid ,
  null as BrandId,
  null as Serial,
  null as Tunnel,
  null as Accountnumber_Demo,
-- dynamic serials
  null as d_Folder_Type,
  null as d_Test_Name ,
  null as d_Test_Parameter ,
  null as d_Hub ,
  null as d_Hub_Category ,
  null as d_Domain ,
  null as d_New_Site_Referrer ,
  null as d_Google_kwd ,
  null as d_Site ,
  null as d_Social ,
  null as d_Affiliate ,
  null as d_Referring_website ,
  null as d_Referrer_Type ,
  null as d_Referring_Page ,
  null as d_Referring_Kwd ,
  null as d_Device_Category ,
  null as d_OS ,
  null as d_Browser ,
  null as d_IP_Country ,
  null as d_Actual_lp ,
  null as Gclid ,
  null as Appsflyer_Device_Id,

  -- platform usage fields
  null as pu_Browser ,
  null as pu_DealSize, 
  null as pu_DealSlip,
  null as pu_Device ,
  null as pu_IPCountry,
  null as pu_Instrument,
  null as pu_OS ,
  null as pu_TabName ,
  null as pu_TrackingSessionId ,
  -- emerp process fields
  null as em_RAWDB_Id,
  null as em_Event_Sum,
  null as em_isFirst,
  null as em_isUnique,

-- request and partition
  Request ,
  ActionDay as Event_Date_day
from v_FXNET_Accounting as d 
  left join ods.accountcontacts as ac on d.Accountnumber = ac.Accountnumber



-- COMMAND ----------

-- DBTITLE 1,Update manage table with last date = new max date 
-- MAGIC %python
-- MAGIC manage = table_service.get_entity(manage_table, manage_partition_key, manage_row_key)
-- MAGIC Next_Date_str = manage.Next_Incremental_Date
-- MAGIC 
-- MAGIC if receivedDate is not None:
-- MAGIC   new_val = {'PartitionKey': manage_partition_key, 'RowKey': manage_row_key,'Last_Incremental_Date': Next_Date_str }
-- MAGIC 
-- MAGIC table_service.insert_or_merge_entity(manage_table, new_val)

-- COMMAND ----------

-- delete from dwhdb.events where source like 'calls' and event_date_day >= '2019-02-25'