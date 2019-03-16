-- Databricks notebook source
-- DBTITLE 1,Create table service | get last call date | create view with the last date
-- MAGIC %python
-- MAGIC from azure.cosmosdb.table.tableservice import TableService
-- MAGIC from azure.cosmosdb.table.models import Entity
-- MAGIC table_service = TableService(account_name='dataloadestore', account_key='7wtLQIcK9q4QnXMCL6AO9I233TSi3hITG6tC4jO5VDEv3+ovoQo6NYv5IcboZo6Ncf5GeULV7uPdvUW+k8gJGA==')
-- MAGIC calls_manage = table_service.get_entity('etlManage', 'Load Events', 'DWH_Calls')
-- MAGIC Last_Call_Date_str = calls_manage.Last_Incremental_Date
-- MAGIC Last_Call_Date_df = sql("select '"+Last_Call_Date_str+"' as Last_Call_Date")
-- MAGIC Last_Call_Date_df.createOrReplaceTempView("vlast_Calls")

-- COMMAND ----------

select *
from vlast_Calls

-- COMMAND ----------
-- -- create external table 

--   drop table if exists rawdata.DWH_Calls ;

--   create table rawdata.DWH_Calls
--   (
--   Call_Id	Int,
--   Call_Details string,
--   ExecutionDay date,
--   Received timestamp

--   )
--   using csv
--   partitioned by (received)
--   location '/mnt/dataloadestore/rawdata/DWH_Calls/'
--   options ('sep' = '\t' , 'quote'= "");

-- COMMAND ----------


-- -- create external table 
--   drop table if exists rawdata.DWH_Calls ;
--cache table rawdata.dwh_calls

--   create table rawdata.DWH_Calls
--   (
--   Call_Id	Int,
--   Call_Details string,
--   ExecutionDay date,
--   Received timestamp

--   )
--   using csv
--   partitioned by (received)
--   location '/mnt/dataloadestore/rawdata/DWH_Calls/'
--   options ('sep' = '\t' , 'quote'= "");


-- COMMAND ----------

-- read the new partitions
msck repair table rawdata.DWH_Calls

-- COMMAND ----------

--cache table rawdata.dwh_calls

-- COMMAND ----------

-- DBTITLE 1,get max new data from mrr table and set to variable
-- MAGIC %python
-- MAGIC max_date_sql = sql("select max(fx.received) as Last_Received_Date from rawdata.DWH_Calls fx  where fx.received > cast('" + calls_manage.Last_Incremental_Date +"' as timestamp)" )
-- MAGIC max_received_col = max_date_sql.select('Last_Received_Date')
-- MAGIC 
-- MAGIC #do the if because if by any chance it will not find any date then the collect will fail "hive metadata error"
-- MAGIC if max_received_col is not None:
-- MAGIC   receivedDate = max_received_col.collect()[0][0]
-- MAGIC else:
-- MAGIC   receivedDate = None
-- MAGIC 
-- MAGIC max_date_sql.createOrReplaceTempView("vmax_Calls")

-- COMMAND ----------

select *
from vmax_Calls

-- COMMAND ----------

-- DBTITLE 1,update entity table on next date = new max date received
-- MAGIC %python
-- MAGIC if receivedDate is not None:
-- MAGIC   new_val = {'PartitionKey': 'Load Events', 'RowKey': 'DWH_Calls','Next_Incremental_Date' : receivedDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}
-- MAGIC else:
-- MAGIC   new_val = {'PartitionKey': 'Load Events', 'RowKey': 'DWH_Calls','Next_Incremental_Date' : Last_Call_Date_str}
-- MAGIC table_service.insert_or_merge_entity('etlManage', new_val)

-- COMMAND ----------

-- creating the view for calls
create or replace temp view v_dwh_calls
as
select 
    get_json_object(Call_details, "$.Event_Name") as Event_Name, 
    get_json_object(Call_details, "$.Event_Details") as Event_Details, 
    
    cast(get_json_object(Call_details, "$.CallDate") as timeStamp) as ExecutionDate,
    get_json_object(Call_details, "$.AccountNumber") as AccountNumber,
    get_json_object(Call_details, "$.Contact_ID") as Contact_ID,
    get_json_object(Call_details, "$.BrokerID") as Broker_ID,
    get_json_object(Call_details, "$.BrokerName") as BrokerName,
    get_json_object(Call_details, "$.BrandId") as BrandId,
  map(
		"Call_ID", get_json_object(Call_details, "$.Call_ID"),
        "PBX_ID", get_json_object(Call_details, "$.PBX_ID"),
        "User_ID", get_json_object(Call_details, "$.User_ID"),
        "Extension", get_json_object(Call_details, "$.Extension"),
		"PBX_Uniqueid", get_json_object(Call_details, "$.PBX_Uniqueid"),
        "CallDate", get_json_object(Call_details, "$.CallDate"),
        "CallDate_PBX", get_json_object(Call_details, "$.CallDate_PBX"),
        "Call_Type", get_json_object(Call_details, "$.Call_Type"),
		"Disposition", get_json_object(Call_details, "$.Disposition"),
		"Dcontext",get_json_object(Call_details, "$.Dcontext"),
		"Lastapp",get_json_object(Call_details, "$.Lastapp"),
		"CallMode_ID",get_json_object(Call_details, "$.CallMode_ID"),
		"CallModeName", get_json_object(Call_details, "$.CallModeName"),
		"AccountNumber", get_json_object(Call_details, "$.AccountNumber"),
		"BrokerID", get_json_object(Call_details, "$.BrokerID"),
		"BrokerName", get_json_object(Call_details, "$.BrokerName"),
		"BrandId", get_json_object(Call_details, "$.BrandId"),
		"Contact_ID", get_json_object(Call_details, "$.Contact_ID"),
		"Country",get_json_object(Call_details, "$.Country"),
		"System", get_json_object(Call_details, "$.System"),
		"Pre_Prefix", get_json_object(Call_details, "$.Pre_Prefix"),
		"Trunk", get_json_object(Call_details, "$.Trunk"),
		"Provider",get_json_object(Call_details, "$.Provider"),
		"Protocol", get_json_object(Call_details, "$.Protocol"),
		"Billsec", get_json_object(Call_details, "$.Billsec"),
		"DurationSec", get_json_object(Call_details, "$.DurationSec"),
		"Prefix", get_json_object(Call_details, "$.Prefix"),
		"SelectedCallResult",get_json_object(Call_details, "$.SelectedCallResult"),
		"BillMinute_Answered", get_json_object(Call_details, "$.BillMinute_Answered"),
		"Has_Phone_Id", get_json_object(Call_details, "$.Has_Phone_Id"),
		"Has_Contact_Id", get_json_object(Call_details, "$.Has_Contact_Id"),
		"n2p_Price", get_json_object(Call_details, "$.n2p_Price"),
		"vxb_Price", get_json_object(Call_details, "$.vxb_Price"),
		"c4x_Price", get_json_object(Call_details, "$.c4x_Price"),
		"omn_Price", get_json_object(Call_details, "$.omn_Price"),
		"pccw_Price",get_json_object(Call_details, "$.pccw_Price"),
		"cpk_Price ", get_json_object(Call_details, "$.cpk_Price "),
		"res_Price", get_json_object(Call_details, "$.res_Price"),
		"N2P_Call_Price",get_json_object(Call_details, "$.N2P_Call_Price"),
		"VXB_Call_Price", get_json_object(Call_details, "$.VXB_Call_Price"),
		"C4X_Call_Price",get_json_object(Call_details, "$.C4X_Call_Price"),
		"OMN_Call_Price", get_json_object(Call_details, "$.OMN_Call_Price"),
		"PCCW_Call_Price", get_json_object(Call_details, "$.PCCW_Call_Price"),
		"CPK_Call_Price", get_json_object(Call_details, "$.CPK_Call_Price"),
		"RSC_Call_Price", get_json_object(Call_details, "$.RSC_Call_Price"),
		"ActualPrice",get_json_object(Call_details, "$.ActualPrice"),
		"CallDay", get_json_object(Call_details, "$.CallDay"),
        "Recieved", received
      ) as Request ,
  ExecutionDay
from rawdata.DWH_Calls r 
 where r.received >  (select cast(Last_Call_Date as timestamp) from vlast_Calls) --Last_Call_Date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
   and r.received <= (select cast(Last_Received_Date as timestamp) from vmax_Calls)--receivedDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] 

-- COMMAND ----------

select ExecutionDay, count(*)
from v_dwh_calls
group by  ExecutionDay
order by  ExecutionDay

-- COMMAND ----------

-- cahce account contacts for the join
cache table ods.accountcontacts

-- COMMAND ----------

-- -- insert the events
-- --create or replace temp view fxnet_deals_for_insert
-- --as
 insert into dwhdb.Events
select 
  ExecutionDate    ,
  'calls' as Source  ,
  lower(Event_Name) as Event_Name,
  Event_Details as Event_Details,
  nvl(d.Contact_ID, ac.ContactID) as Contact_Id,
  d.Accountnumber,
  Broker_ID,
  BrokerName as Broker,
  null	as Brand_Guid ,
  BrandId,
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
  ExecutionDay as Event_Date_day
from v_dwh_calls as d 
left join ods.accountcontacts as ac on d.Accountnumber = ac.Accountnumber



-- COMMAND ----------

-- DBTITLE 1,Update entity table with last call date = new max date 
-- MAGIC %python
-- MAGIC calls_manage_Last = table_service.get_entity('etlManage', 'Load Events', 'DWH_Calls')
-- MAGIC Next_Call_Date_str = calls_manage_Last.Next_Incremental_Date
-- MAGIC 
-- MAGIC if receivedDate is not None:
-- MAGIC   new_val = {'PartitionKey': 'Load Events', 'RowKey': 'DWH_Calls','Last_Incremental_Date': Next_Call_Date_str }
-- MAGIC 
-- MAGIC table_service.insert_or_merge_entity('etlManage', new_val)