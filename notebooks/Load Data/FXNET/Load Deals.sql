-- Databricks notebook source
-- MAGIC %md
-- MAGIC -- create external table serials
-- MAGIC 
-- MAGIC drop table if exists rawdata.FXNET_deals ;
-- MAGIC 
-- MAGIC create table rawdata.FXNET_deals
-- MAGIC (
-- MAGIC TransactionNumber	bigint,
-- MAGIC Trans_Details string,
-- MAGIC ExecutionDay date,
-- MAGIC received timestamp
-- MAGIC 
-- MAGIC )
-- MAGIC using csv
-- MAGIC partitioned by (received)
-- MAGIC location '/mnt/dataloadestore/rawdata/FXNET_Deals/'
-- MAGIC options ('sep' = '\t' , 'quote'= "");

-- COMMAND ----------

-- read the new partitions
msck repair table rawdata.FXNET_deals

-- COMMAND ----------

-- update the next value for reading deals into events table
with fd as
(
  select max(received) as max_received
  from rawdata.FXNET_deals fx
  where fx.received > (select m.last_received from ods.etl_manage as m where m.table_name = 'FXNET_Deals' )
)
update ods.etl_manage
set next_received = ifnull((select fd.max_received from fd),next_received)
where table_name = 'FXNET_Deals'


-- COMMAND ----------

select * from ods.etl_manage

-- COMMAND ----------

-- optimize delta file
OPTIMIZE ods.etl_manage;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM ods.etl_manage RETAIN 0 HOURS ;


-- COMMAND ----------

cache table ods.etl_manage

-- COMMAND ----------

-- the deals event should be added to events
select distinct received
from rawdata.FXNET_deals r 
where r.received > (select mng.last_received from ods.etl_manage mng where mng.table_name = 'FXNET_Deals')
  and r.received <= (select mng.next_received from ods.etl_manage mng where mng.table_name = 'FXNET_Deals') 

-- COMMAND ----------

-- creating the view for load deals
create or replace temp view v_fxnet_deals
as
select 
    get_json_object(Trans_Details, "$.Event_Name") as Event_Name, 
    get_json_object(Trans_Details, "$.Event_Detaails") as Event_Details, 
    cast(get_json_object(Call_details, "$.ExecutionDate") as timeStamp) as ExecutionDate,
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
where r.received > (select mng.last_received from ods.etl_manage mng where mng.table_name = 'FXNET_Deals')
  and r.received <= (select mng.next_received from ods.etl_manage mng where mng.table_name = 'FXNET_Deals') 

-- COMMAND ----------

--cache table v_fxnet_deals

-- COMMAND ----------

--select Event_Name,Event_Details,BrokerName, ExecutionDay, count(*)
--from v_fxnet_deals
--group by Event_Name,Event_Details, BrokerName , ExecutionDay

-- COMMAND ----------

--select *
--from ods.accountcontacts

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

--insert into dwhdb.Events
--select *
--from  fxnet_deals_for_insert

-- COMMAND ----------

update ods.etl_manage
set last_received = ifnull(next_received,last_received)
where table_name = 'FXNET_Deals'

-- COMMAND ----------

-- optimize delta file
OPTIMIZE ods.etl_manage;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM ods.etl_manage RETAIN 0 HOURS ;


-- COMMAND ----------

-- optimize delta file
OPTIMIZE dwhdb.events;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM ods.etl_manage RETAIN 0 HOURS ;