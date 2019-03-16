-- Databricks notebook source
-- DBTITLE 1,Refresh external table to get the new files

-- create external table 

drop table if exists rawdata.DWH_Lead_Assignments ;

create table rawdata.DWH_Lead_Assignments
(
  Lead_Assignments_ID	Int,
  Assignment_Day date,
  Assignment_Date timestamp,
  Details string,
  Received timestamp
)
using csv
partitioned by (Received)
location '/mnt/dataloadestore/rawdata/DWH_Lead_Assignments/'
options ('sep' = '\t' , 'quote'= "");


-- COMMAND ----------

-- read the new partitions
msck repair table rawdata.DWH_Lead_Assignments

-- COMMAND ----------

-- DBTITLE 1,Create table service | get last assignment date | create view with the last date
-- MAGIC %python
-- MAGIC from azure.cosmosdb.table.tableservice import TableService
-- MAGIC from azure.cosmosdb.table.models import Entity
-- MAGIC table_service = TableService(account_name='dataloadestore', account_key='7wtLQIcK9q4QnXMCL6AO9I233TSi3hITG6tC4jO5VDEv3+ovoQo6NYv5IcboZo6Ncf5GeULV7uPdvUW+k8gJGA==')
-- MAGIC manage_table = 'etlManage'
-- MAGIC manage_partition_key = 'Load Events'
-- MAGIC manage_row_key = 'DWH_Lead_Assignments'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC manage = table_service.get_entity(manage_table, manage_partition_key, manage_row_key)
-- MAGIC Last_Date_str = manage.Last_Incremental_Date
-- MAGIC Last_Date_df = sql("select cast('"+Last_Date_str+"' as timestamp) as Last_Date")
-- MAGIC Last_Date_df.createOrReplaceTempView("vlast_date")

-- COMMAND ----------

select * from vlast_date

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_date_sql = sql("select max(fx.received) as Last_Received_Date from rawdata.DWH_Lead_Assignments fx  where fx.received > cast('" + leads_manage.Last_Incremental_Date +"' as timestamp)" )
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

select * from rawdata.DWH_Lead_Assignments
where Lead_Assignments_ID= 1852388
limit 1

-- COMMAND ----------

-- creating the view for DWH_LeadAssignments
create or replace temp view v_dwh_lead_assignments
as
select 
    concat('sales ',get_json_object(Details, "$.Event_Name")) as Event_Name, 
    get_json_object(Details, "$.Event_Details") as Event_Details, 
    Assignment_Date,
    get_json_object(Details, "$.ContactID") as ContactID, 
  map(
        "Fact_Lead_Assignments_ID" , get_json_object(Details, "$.Fact_Lead_Assignments_ID"),
        "Event_Name" , get_json_object(Details, "$.Event_Name"),
        "Event_Details" , get_json_object(Details, "$.Event_Details"),
        "ContactID" , get_json_object(Details, "$.ContactID"),
        "Salesman_ID" , get_json_object(Details, "$.Salesman_ID"),
        "Assignment_Date" , get_json_object(Details, "$.Assignment_Date"),
        "Assignment_Day" , get_json_object(Details, "$.Assignment_Day"),
        "Assignment_Time" , get_json_object(Details, "$.Assignment_Time"),
        "Next_Lead_Assignment_Attribute_ID" , get_json_object(Details, "$.Next_Lead_Assignment_Attribute_ID"),
        "Next_Lead_Reason" , get_json_object(Details, "$.Next_Lead_Reason"),
        "Contact_Action" , get_json_object(Details, "$.Contact_Action"),
        "Original_Contact_Action" , get_json_object(Details, "$.Original_Contact_Action"),
        "Contact_Action_Age" , get_json_object(Details, "$.Contact_Action_Age"),
        "Original_Contact_Action_Age" , get_json_object(Details, "$.Original_Contact_Action_Age"),
        "From_Stack" , get_json_object(Details, "$.From_Stack"),
        "Has_Account" , get_json_object(Details, "$.Has_Account"),
        "Is_Online" , get_json_object(Details, "$.Is_Online"),
        "Already_Assigned" , get_json_object(Details, "$.Already_Assigned"),
        "Entity_Assignment_ID" , get_json_object(Details, "$.Entity_Assignment_ID"),
        "Assigned_By_ID" , get_json_object(Details, "$.Assigned_By_ID"),
        "Is_NLA" , get_json_object(Details, "$.Is_NLA"),
        "Is_LAS" , get_json_object(Details, "$.Is_LAS"),
        "Is_LAS" , get_json_object(Details, "$.Is_LAS"),
        "Received", received
      ) as Request ,
  Assignment_Day
from rawdata.DWH_Lead_Assignments r 
where r.Received >  (select cast(Last_Date as timestamp) from vlast_date) --Last_Call_Date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
  and r.Received <= (select cast(Last_Received_Date as timestamp) from vmax_date)--receivedDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] 
 

-- COMMAND ----------

select Assignment_Day, count(*)
from v_dwh_lead_assignments
group by Assignment_Day
order by Assignment_Day

-- COMMAND ----------

-- insert the events
--create or replace temp view fxnet_deals_for_insert
--as
insert into dwhdb.Events
select 
  Assignment_Date    ,
  'scmm' as Source  ,
  lower(Event_Name) as Event_Name,
  Event_Details as Event_Details,
  ContactID as Contact_Id,
  null as Accountnumber,
  null as Broker_ID,
  null as Broker,
  null	as Brand_Guid ,
  null BrandId,
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