-- Databricks notebook source
-- DBTITLE 1,Create table service | get last deal date | create view with the last date
-- MAGIC %python
-- MAGIC from azure.cosmosdb.table.tableservice import TableService
-- MAGIC from azure.cosmosdb.table.models import Entity
-- MAGIC manage_table = 'etlManage'
-- MAGIC manage_partition_key = 'Load Events'
-- MAGIC manage_row_key = 'FXNET_Deals'
-- MAGIC table_service = TableService(account_name='dataloadestore', account_key='7wtLQIcK9q4QnXMCL6AO9I233TSi3hITG6tC4jO5VDEv3+ovoQo6NYv5IcboZo6Ncf5GeULV7uPdvUW+k8gJGA==')
-- MAGIC deals_manage = table_service.get_entity(manage_table, manage_partition_key, manage_row_key)
-- MAGIC Last_Deal_Date_str = deals_manage.Last_Incremental_Date
-- MAGIC Last_Deal_Date_df = sql("select '"+Last_Deal_Date_str+"' as Last_Deal_Date")
-- MAGIC Last_Deal_Date_df.createOrReplaceTempView("vlast_Deals")

-- COMMAND ----------


--    drop table if exists rawdata.FXNET_deals ;

--    create table rawdata.FXNET_deals
--    (
--    TransactionNumber	bigint,
--    Trans_Details string,
--    ExecutionDay date,
--    Received timestamp

--    )
--    using csv
--    partitioned by (received)
--    location '/mnt/dataloadestore/rawdata/FXNET_Deals/'
--    options ('sep' = '\t' , 'quote'= "");


-- COMMAND ----------

-- read the new partitions
msck repair table rawdata.FXNET_deals

-- COMMAND ----------

--cache table rawdata.FXNET_deals

-- COMMAND ----------

-- select count(*),executionday
-- from rawdata.fxnet_deals 
-- where executionDay>'2019-01-01'
-- group by executionDay

-- COMMAND ----------

-- DBTITLE 1,get max new data from mrr table and set to variable
-- MAGIC %python
-- MAGIC max_date_sql = sql("select max(fx.Received) as Next_Received_Date from rawdata.FXNET_deals fx  where fx.Received > cast('" + deals_manage.Last_Incremental_Date +"' as timestamp)" )
-- MAGIC max_received_col = max_date_sql.select('Next_Received_Date')
-- MAGIC 
-- MAGIC #do the if because if by any chance it will not find any date then the collect will fail "hive metadata error"
-- MAGIC if max_received_col is not None:
-- MAGIC   receivedDate = max_received_col.collect()[0][0]
-- MAGIC else:
-- MAGIC   receivedDate = None
-- MAGIC 
-- MAGIC max_date_sql.createOrReplaceTempView("vmax_Deals")

-- COMMAND ----------

-- DBTITLE 1,update entity table on next date = new max date received
-- MAGIC %python
-- MAGIC if receivedDate is not None:
-- MAGIC   new_val = {'PartitionKey': manage_partition_key, 'RowKey': manage_row_key,'Next_Incremental_Date' : receivedDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] }
-- MAGIC else:
-- MAGIC   new_val = {'PartitionKey': manage_partition_key, 'RowKey': manage_row_key,'Next_Incremental_Date' : Last_Deal_Date_str}
-- MAGIC table_service.insert_or_merge_entity(manage_table, new_val)

-- COMMAND ----------

-- creating the view for load deals
create or replace temp view v_fxnet_deals
as
select 
    get_json_object(Trans_Details, "$.Event_Name") as Event_Name, 
    get_json_object(Trans_Details, "$.Event_Detaails") as Event_Details, 
    cast(get_json_object(Trans_Details, "$.ExecutionDate") as timeStamp) as ExecutionDate,
    get_json_object(Trans_Details, "$.AccountNumber") as AccountNumber,
    get_json_object(Trans_Details, "$.Broker_ID") as Broker_ID,
    get_json_object(Trans_Details, "$.BrokerName") as BrokerName,
    get_json_object(Trans_Details, "$.BrandId") as BrandId,
  map(
		"TransactionNumber", get_json_object(Trans_Details, "$.TransactionNumber"),
        "PositionNumber", get_json_object(Trans_Details, "$.PositionNumber"),
		"BrandId", get_json_object(Trans_Details, "$.BrandId"),
        "BrandName", get_json_object(Trans_Details, "$.BrandName"),
        "Broker_ID", get_json_object(Trans_Details, "$.Broker_ID"),
        "BrokerName", get_json_object(Trans_Details, "$.BrokerName"),
		"Folder_ID", get_json_object(Trans_Details, "$.Folder_ID"),
		"Folder",get_json_object(Trans_Details, "$.Folder"),
		"FolderType",get_json_object(Trans_Details, "$.FolderType"),
		"AccountNumber", get_json_object(Trans_Details, "$.AccountNumber"),
		"DealType", get_json_object(Trans_Details, "$.DealType"),
		"Direction", get_json_object(Trans_Details, "$.Direction"),
		"Source", get_json_object(Trans_Details, "$.Source"),
		"TransactionType",get_json_object(Trans_Details, "$.TransactionType"),
		"Instrument_ID", get_json_object(Trans_Details, "$.Instrument_ID"),
		"Instrument", get_json_object(Trans_Details, "$.Instrument"),
		"Instrument_Name", get_json_object(Trans_Details, "$.Instrument_Name"),
		"InstrumentType", get_json_object(Trans_Details, "$.InstrumentType"),
		"InstrumentCategory",get_json_object(Trans_Details, "$.InstrumentCategory"),
		"ExecutionDate", get_json_object(Trans_Details, "$.ExecutionDate"),
		"ExecutionDay", get_json_object(Trans_Details, "$.ExecutionDay"),
		"ExecutionTime", get_json_object(Trans_Details, "$.ExecutionTime"),
		"TransactionCloseDate",get_json_object(Trans_Details, "$.TransactionCloseDate"),
		"BaseAmount", get_json_object(Trans_Details, "$.TransactionCloseDate"),
		"OtherAmount", get_json_object(Trans_Details, "$.OtherAmount"),
		"SpotRate", get_json_object(Trans_Details, "$.SpotRate"),
		"ForwardPips", get_json_object(Trans_Details, "$.ForwardPips"),
		"TotRate",get_json_object(Trans_Details, "$.TotRate"),
		"Volume_USD", get_json_object(Trans_Details, "$.Volume_USD"),
		"ServerRate", get_json_object(Trans_Details, "$.ServerRate"),
		"OtherServerRate", get_json_object(Trans_Details, "$.OtherServerRate"),
		"ExecutedLimit", get_json_object(Trans_Details, "$.ExecutedLimit"),
		"Deleted", get_json_object(Trans_Details, "$.Deleted"),
		"PositionClosed", get_json_object(Trans_Details, "$.PositionClosed"),
		"DealPL_USD", get_json_object(Trans_Details, "$.DealPL_USD"),
		"PL_SymbolId",get_json_object(Trans_Details, "$.PL_SymbolId"),
		"PL_Symbol ", get_json_object(Trans_Details, "$.PL_Symbol "),
		"PL", get_json_object(Trans_Details, "$.PL"),
		"SpreadCost",get_json_object(Trans_Details, "$.SpreadCost"),
		"AccountBasesymbolId", get_json_object(Trans_Details, "$.AccountBasesymbolId"),
		"AccountBasesymbol",get_json_object(Trans_Details, "$.AccountBasesymbol"),
		"PL_AccountBase", get_json_object(Trans_Details, "$.PL_AccountBase"),
		"Customer_Favor_Rate_diff", get_json_object(Trans_Details, "$.Customer_Favor_Rate_diff"),
		"ConversionRate", get_json_object(Trans_Details, "$.ConversionRate"),
		"CommissionInAccountBase", get_json_object(Trans_Details, "$.CommissionInAccountBase"),
		"SpreadCostInAccountBase",get_json_object(Trans_Details, "$.SpreadCostInAccountBase"),
		"RolloverSpreadCostInAccountBase", get_json_object(Trans_Details, "$.RolloverSpreadCostInAccountBase"),
		"CommissionInOther", get_json_object(Trans_Details, "$.CommissionInOther"),
		"SpreadCostInOther", get_json_object(Trans_Details, "$.SpreadCostInOther"),
		"RolloverSpreadCostInOther",get_json_object(Trans_Details, "$.RolloverSpreadCostInOther"),
		"MarkupCostInAccountCcy", get_json_object(Trans_Details, "$.MarkupCostInAccountCcy"),
		"MarkupCostInOther", get_json_object(Trans_Details, "$.MarkupCostInOther"), 
		"MarkupCostInUSD", get_json_object(Trans_Details, "$.MarkupCostInUSD"),
        "Recieved", received
      ) as Request ,
  ExecutionDay
from rawdata.FXNET_deals r
where r.received >  (select mng.Last_Deal_Date from vlast_Deals mng)
  and r.received <= (select mng.Next_Received_Date from vmax_Deals mng) 

-- COMMAND ----------

cache table v_fxnet_deals

-- COMMAND ----------

-- cahce account contacts for the join
cache table ods.accountcontacts

-- COMMAND ----------

-- insert the events
--create or replace temp view fxnet_deals_for_insert
--as
insert into dwhdb.Events
select 
  ExecutionDate    ,
  'fxnet' as Source  ,
  lower(Event_Name) as Event_Name,
  Event_Details as Event_Details,
  ac.ContactID as Contact_Id,
  d.Accountnumber,
  Broker_Id,
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
from v_fxnet_deals as d left join ods.accountcontacts as ac on d.Accountnumber = ac.Accountnumber



-- COMMAND ----------

-- DBTITLE 1,Update entity table with last deal date = new max date 
-- MAGIC %python
-- MAGIC deals_manage_Last = table_service.get_entity(manage_table, manage_partition_key, manage_row_key)
-- MAGIC Next_Deal_Date_str = deals_manage_Last.Next_Incremental_Date
-- MAGIC 
-- MAGIC if receivedDate is not None:
-- MAGIC   new_val = {'PartitionKey': manage_partition_key, 'RowKey': manage_row_key, 'Last_Incremental_Date': Next_Deal_Date_str }
-- MAGIC 
-- MAGIC table_service.insert_or_merge_entity(manage_table, new_val)