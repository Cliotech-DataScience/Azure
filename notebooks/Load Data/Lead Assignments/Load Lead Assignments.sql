-- Databricks notebook source
-- DBTITLE 1,Create table service | get last assignment date | create view with the last date
-- MAGIC %python
-- MAGIC from azure.cosmosdb.table.tableservice import TableService
-- MAGIC from azure.cosmosdb.table.models import Entity
-- MAGIC table_service = TableService(account_name='dataloadestore', account_key='7wtLQIcK9q4QnXMCL6AO9I233TSi3hITG6tC4jO5VDEv3+ovoQo6NYv5IcboZo6Ncf5GeULV7uPdvUW+k8gJGA==')
-- MAGIC leads_manage = table_service.get_entity('etlManage', 'Load Events', 'DWH_LeadAssignments')
-- MAGIC Last_Assignment_Date_str = leads_manage.Last_Incremental_Date
-- MAGIC Last_Assignment_Date_df = sql("select '"+Last_Assignment_Date_str+"' as Last_Assignment_Date")
-- MAGIC Last_Assignment_Date_df.createOrReplaceTempView("vlast_Assignments")

-- COMMAND ----------

select * from vlast_Assignments

-- COMMAND ----------


-- create external table 

--   drop table if exists rawdata.DWH_LeadAssignments ;

--   create table rawdata.DWH_LeadAssignments
--   (
--  Lead_Assignments_ID	Int,
--   Assignment_Details string,
--   Assignment_Day date,
--   Received timestamp

--   )
--   using csv
--   partitioned by (Received)
--   location '/mnt/dataloadestore/rawdata/DWH_LeadAssignments/'
--   options ('sep' = '\t' , 'quote'= "");


-- COMMAND ----------

-- read the new partitions
msck repair table rawdata.DWH_LeadAssignments

-- COMMAND ----------

--cache table rawdata.DWH_LeadAssignments

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_date_sql = sql("select max(fx.received) as Last_Received_Date from rawdata.DWH_LeadAssignments fx  where fx.received > cast('" + leads_manage.Last_Incremental_Date +"' as timestamp)" )
-- MAGIC max_received_col = max_date_sql.select('Last_Received_Date')
-- MAGIC 
-- MAGIC #do the if because if by any chance it will not find any date then the collect will fail "hive metadata error"
-- MAGIC if max_received_col is not None:
-- MAGIC   receivedDate = max_received_col.collect()[0][0]
-- MAGIC else:
-- MAGIC   receivedDate = None
-- MAGIC 
-- MAGIC max_date_sql.createOrReplaceTempView("vmax_Assignments")

-- COMMAND ----------

-- DBTITLE 1,update entity table on next date = new max date received
-- MAGIC %python
-- MAGIC if receivedDate is not None:
-- MAGIC   new_val = {'PartitionKey': 'Load Events', 'RowKey': 'DWH_LeadAssignments','Next_Incremental_Date' : receivedDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}
-- MAGIC else:
-- MAGIC   new_val = {'PartitionKey': 'Load Events', 'RowKey': 'DWH_LeadAssignments','Next_Incremental_Date' : Last_Assignment_Date_str}
-- MAGIC table_service.insert_or_merge_entity('etlManage', new_val)

-- COMMAND ----------

--insert into ods.etl_manage
--select "DWH_Calls",'2011-01-01','2012-01-01'

-- COMMAND ----------

-- creating the view for DWH_LeadAssignments
create or replace temp view v_dwh_lead_assignments
as
select 
    get_json_object(Assignment_Details, "$.Event_Name") as Event_Name, 
    get_json_object(Assignment_Details, "$.Event_Details") as Event_Details, 
    get_json_object(Assignment_Details, "$.Assignment_Date") as Assignment_Date,
    get_json_object(Assignment_Details, "$.AccountNumber") as AccountNumber,
    get_json_object(Assignment_Details, "$.BrokerID") as Broker_ID,
    get_json_object(Assignment_Details, "$.BrokerName") as BrokerName,
    get_json_object(Assignment_Details, "$.BrandId") as BrandId,
  map(
		"Fact_Lead_Assignments_ID", get_json_object(Assignment_Details, "$.Fact_Lead_Assignments_ID"),
        "Event_Name", get_json_object(Assignment_Details, "$.Event_Name"),
		"Event_Details", get_json_object(Assignment_Details, "$.Event_Details"),
        "ContactID", get_json_object(Assignment_Details, "$.ContactID"),
        "Salesman_ID", get_json_object(Assignment_Details, "$.Salesman_ID"),
        "Assignment_Date", get_json_object(Assignment_Details, "$.Assignment_Date"),
		"Assignment_Time", get_json_object(Assignment_Details, "$.Assignment_Time"),
		"BrokerID",get_json_object(Assignment_Details, "$.BrokerID"),
		"GroupID",get_json_object(Assignment_Details, "$.GroupID"),
		"Next_Lead_Assignment_Attribute_ID",get_json_object(Assignment_Details, "$.Next_Lead_Assignment_Attribute_ID"),
		"Entity_Assignment_ID", get_json_object(Assignment_Details, "$.Entity_Assignment_ID"),
		"Assigned_By_ID", get_json_object(Assignment_Details, "$.Assigned_By_ID"),
		"Is_NLA", get_json_object(Assignment_Details, "$.Is_NLA"),
		"Is_LAS", get_json_object(Assignment_Details, "$.Is_LAS"),
		"AccountNumber", get_json_object(Assignment_Details, "$.AccountNumber"),
		"Serial", get_json_object(Assignment_Details, "$.Serial"),
		"BrokerName",get_json_object(Assignment_Details, "$.BrokerName"),
		"BrandId", get_json_object(Assignment_Details, "$.BrandId"),
		"BrandName", get_json_object(Assignment_Details, "$.BrandName"),
		"Country", get_json_object(Assignment_Details, "$.Country"),
		"Assignment_Day",get_json_object(Assignment_Details, "$.Assignment_Day"),
        "Received", received
      ) as Request ,
  Assignment_Day
from rawdata.DWH_LeadAssignments r 
where r.received > (select mng.Last_Assignment_Date from vlast_Assignments mng)
  and r.received <= (select mng.Last_Received_Date from vmax_Assignments mng) 

-- COMMAND ----------

select * from v_dwh_lead_assignments

-- COMMAND ----------

cache table v_dwh_lead_assignments

-- COMMAND ----------

-- cahce account contacts for the join
cache table ods.accountcontacts

-- COMMAND ----------

-- insert the events
--create or replace temp view fxnet_deals_for_insert
--as
insert into dwhdb.Events
select 
  Assignment_Date    ,
  'scmm_LM' as Source  ,
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
  Assignment_Day as Event_Date_day
from v_dwh_lead_assignments as d 
left join ods.accountcontacts as ac on d.Accountnumber = ac.Accountnumber



-- COMMAND ----------

-- DBTITLE 1,Update entity table with last assignment date = new max date 
-- MAGIC %python
-- MAGIC assignments_manage_Last = table_service.get_entity('etlManage', 'Load Events', 'DWH_LeadAssignments')
-- MAGIC Next_Assignment_Date_str = assignments_manage_Last.Next_Incremental_Date
-- MAGIC 
-- MAGIC if receivedDate is not None:
-- MAGIC   new_val = {'PartitionKey': 'Load Events', 'RowKey': 'DWH_LeadAssignments','Last_Incremental_Date': Next_Assignment_Date_str }
-- MAGIC 
-- MAGIC table_service.insert_or_replace_entity('etlManage', new_val)

-- COMMAND ----------

 --optimize delta file
 OPTIMIZE dwhdb.events;
 --set spark.databricks.delta.retentionDurationCheck.enabled = false;
 --VACUUM dwhdb.events RETAIN 24 HOURS ;
