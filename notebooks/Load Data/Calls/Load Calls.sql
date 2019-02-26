-- Databricks notebook source
--select 'some cell'

-- COMMAND ----------


-- create external table 

drop table if exists rawdata.DWH_Calls ;

create table rawdata.DWH_Calls
(
Call_Id	Int,
Call_Details string,
ExecutionDay date,
received timestamp

)
using csv
partitioned by (received)
location '/mnt/dataloadestore/rawdata/DWH_Calls/'
options ('sep' = '\t' , 'quote'= "");


-- COMMAND ----------

-- read the new partitions
msck repair table rawdata.DWH_Calls

-- COMMAND ----------

select max(received) as max_received
  from rawdata.DWH_Calls fx
  where fx.received > (select m.last_received from ods.etl_manage as m where m.table_name = 'DWH_Calls')

-- COMMAND ----------

-- update the next value for reading calls into events table
with fd as
(
  select max(received) as max_received
  from rawdata.DWH_Calls fx
  where fx.received > (select m.last_received from ods.etl_manage as m where m.table_name = 'DWH_Calls' )
)
update ods.etl_manage
set next_received = ifnull((select fd.max_received from fd),next_received)
where table_name = 'DWH_Calls'


-- COMMAND ----------

--update ods.etl_manage set next_received = '2019-02-12' where table_name ='DWH_Calls'
select * from ods.etl_manage


-- COMMAND ----------

--insert into ods.etl_manage
--select "DWH_Calls",'2011-01-01','2012-01-01'

-- COMMAND ----------

-- optimize delta file
OPTIMIZE ods.etl_manage;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM ods.etl_manage RETAIN 0 HOURS ;


-- COMMAND ----------

cache table ods.etl_manage

-- COMMAND ----------

-- the calls event that should be added to events
select distinct received
from rawdata.DWH_Calls r 
where r.received > (select mng.last_received from ods.etl_manage mng where mng.table_name = 'DWH_Calls')
  and r.received <= (select mng.next_received from ods.etl_manage mng where mng.table_name = 'DWH_Calls') 

-- COMMAND ----------

-- creating the view for calls
create or replace temp view v_dwh_calls
as
select 
    get_json_object(Call_details, "$.Event_Name") as Event_Name, 
    get_json_object(Call_details, "$.Event_Details") as Event_Details, 
    cast(get_json_object(Call_details, "$.CallDate") as timeStamp) as ExecutionDate,
    get_json_object(Call_details, "$.AccountNumber") as AccountNumber,
    get_json_object(Call_details, "$.BrokerID") as Broker_ID,
    get_json_object(Call_details, "$.BrokerName") as BrokerName,
    get_json_object(Call_details, "$.BrandId") as BrandId,
  map(
		"Call_ID", get_json_object(Call_details, "$.Call_ID"),
        "PBX_ID", get_json_object(Call_details, "$.PBX_ID"),
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
where r.received > (select mng.last_received from ods.etl_manage mng where mng.table_name = 'DWH_Calls')
  and r.received <= (select mng.next_received from ods.etl_manage mng where mng.table_name = 'DWH_Calls') 

-- COMMAND ----------

cache table v_dwh_calls

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
  'calls' as Source  ,
  lower(Event_Name) as Event_Name,
  Event_Details as Event_Details,
  ac.ContactID as Contact_Id,
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
from v_dwh_calls as d left join ods.accountcontacts as ac on d.Accountnumber = ac.Accountnumber



-- COMMAND ----------

update ods.etl_manage
set last_received = ifnull(next_received,last_received)
where table_name = 'DWH_Calls'

-- COMMAND ----------

-- optimize delta file
OPTIMIZE ods.etl_manage;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM ods.etl_manage RETAIN 0 HOURS ;
