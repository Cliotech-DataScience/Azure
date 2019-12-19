# Databricks notebook source
# MAGIC %md
# MAGIC dbutils.widgets.dropdown("env_type","dev",["dev","qa","prod"])

# COMMAND ----------

dbutils.widgets.get("env_type")

# COMMAND ----------

# DBTITLE 1,Imports
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import time

ENV_DEV = "dev"
ENV_QA = "qa"
ENV_PROD = "prod"

env_type = dbutils.widgets.get("env_type")

# COMMAND ----------

# DBTITLE 1,config and environment
# Storage
if env_type == ENV_DEV :
  storage_account = "cliotestingstore"
  container = "testdata"
  raw_db = "dev_raw"
  dwh_db = "dev_dwh"

elif env_type == ENV_QA:
  storage_account = "cliotestingstore"
  container = "testdata"
  raw_db = "qa_raw"
  dwh_db = "qa_dwh"

elif env_type == ENV_PROD:
  storage_account = "dwhdbstore"
  container = "databases"
  raw_db = "prod_raw"
  dwh_db = "prod_dwh"
  
raw_db_path =  "/mnt/" + storage_account +"/" + container + "/" + raw_db + "/"
raw_db_checkpointLocation =  raw_db_path + ".checkpoints/"

dwh_db_path = "/mnt/" + storage_account +"/" + container + "/" + dwh_db + "/"
dwh_db_checkpointLocation =  dwh_db_path + ".checkpoints/"

log_table = env_type + "_log.debug_log"
log_path =  "/mnt/" + storage_account +"/" + container + "/" + env_type + "_log/debug_log"

print ("raw_db:",raw_db)
print ("raw_db_path:",raw_db_path)
print ("raw_db_checkpointLocation:",raw_db_checkpointLocation)

print ("dwh_db:",dwh_db)
print ("dwh_db_path:",dwh_db_path)
print ("dwh_db_checkpointLocation:",dwh_db_checkpointLocation)

print ("log_table:",log_table)
print ("log_path:",log_path)



# COMMAND ----------

sql("create table if not exists " + log_table + " (log_date timestamp, description string) using delta location '" + log_path + "'")


# COMMAND ----------

# DBTITLE 1,Events Hub Configuration 
# Connection String
if env_type == ENV_DEV :
  ev_namespace    ="qaeventshubclio"
  ev_name         ="openpl_dev"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "T+FDlX3FrYLmvQws3bkaVtAhthAFyksp/jrknjo+C1M="

elif env_type == ENV_QA:
  ev_namespace    ="qaeventshubclio"
  ev_name         ="openpl_qa"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "T+FDlX3FrYLmvQws3bkaVtAhthAFyksp/jrknjo+C1M="

elif env_type == ENV_PROD:
  ev_namespace    ="prodeventsbyl"
  ev_name         ="openpl"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "mrIEusfFod8Whi/HVr06fRL4lPX42trRv5tG4wKSnPM="
  
conn_string="Endpoint=sb://{0}.servicebus.windows.net/;EntityPath={1};SharedAccessKeyName={2};SharedAccessKey={3}".format(ev_namespace, ev_name, ev_sas_key_name, ev_sas_key_val)

ehConf = {}
ehConf['eventhubs.connectionString'] = conn_string
#ehConf['eventhubs.maxEventsPerTrigger'] = 5
#ehConf['eventhubs.consumerGroup'] = "$Default"


# COMMAND ----------

# DBTITLE 1,offset configuration - stream starting point
# Start from beginning of stream
startOffset = "-1"

# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

# Put the positions into the Event Hub config dictionary
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

# COMMAND ----------

# DBTITLE 1,connect to stream
# Creating an Event Hubs Source for Streaming Queries
df_stream = (
  spark
    .readStream
    .format("eventhubs")
    .options(**ehConf)
    .option('multiLine', True)
    .option("mode", "PERMISSIVE")
    .load()
)

# COMMAND ----------

# DBTITLE 1,config stream structure for raw data
events_schema =  ArrayType(StringType())
nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
jsonOptions = { "timestampFormat": nestTimestampFormat }

df_stream_raw = df_stream \
  .withColumn("Event_Offset", col("offset").cast("long")) \
  .withColumn("Received", col("enqueuedTime").cast("timestamp")) \
  .withColumn("Received_Day", col("enqueuedTime").cast("timestamp").cast("date")) \
  .withColumn("Raw_Event", explode(from_json(col("body").cast("string"), events_schema,  jsonOptions))) \
  .withColumn("Event_Source_Env", get_json_object(col("Raw_Event"),"$.Event_Source_Env").cast("string")) \
  .withColumn("Event_Source_System", get_json_object(col("Raw_Event"),"$.Event_Source_System").cast("string")) \
  .withColumn("Event_Source", get_json_object(col("Raw_Event"),"$.Event_Source").cast("string")) \
  .select("Event_Offset", "Received", "Event_Source_Env", "Event_Source_System", "Event_Source", "Raw_Event", "Received_Day")


# COMMAND ----------

# DBTITLE 1,save raw data to delta file
spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

query_raw = (
  df_stream_raw
    .writeStream
    .trigger(processingTime = "10 seconds")    #.trigger(processingTime = "10 seconds") for continuos    #.trigger(once = True) for ontime
    .format("delta")        
    .partitionBy("Received_Day")
    .option("path", raw_db_path + "open_pl")
    .option("checkpointLocation", raw_db_checkpointLocation + "open_pl")
    .start()
)

# COMMAND ----------

print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Waiting for stream to end")

# wait for streaimng to start 
time.sleep(5 * 60)

t1 = time.time()
timeout_duration = 10 * 60

rc = query_raw.lastProgress["numInputRows"] 
print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Row Count: ", rc)

while ((rc > 0) & ((time.time() - t1) < timeout_duration)) :
  time.sleep(30)
  rc =  query_raw.lastProgress["numInputRows"] 
  print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Row Count: ", rc)


print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Stream ended")
query_raw.stop()


# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC create table prod_raw.dwh_open_pl
# MAGIC (  Event_Offset bigint,
# MAGIC    Received timestamp,
# MAGIC    Event_Source_Env string, 
# MAGIC    Event_Source_System string,
# MAGIC    Event_Source string, 
# MAGIC    Raw_Event string,
# MAGIC    Received_Day date
# MAGIC  )
# MAGIC  using delta
# MAGIC  partitioned by (Received_Day)
# MAGIC  location '/mnt/dwhdbstore/databases/prod_raw/dwh_open_pl'
# MAGIC  

# COMMAND ----------

# DBTITLE 1,etl process events

df_stream_exrtacted = df_stream_raw \
  .filter("lower(Event_Source_Env) = lower('" + env_type + "') AND lower(Event_Source_System) = lower('DWH') AND lower(Event_Source) = lower('OpenPL')") \

  .withColumn("Date", get_json_object(col("Raw_Event"),"$.Date").cast("date")) \
  .withColumn("Transaction_Number", get_json_object(col("Raw_Event"),"$.TransactionNumber").cast("long")) \
  .withColumn("Position_Number", get_json_object(col("Raw_Event"),"$.PositionNumber").cast("long")) \
  .withColumn("Accountnumber", get_json_object(col("Raw_Event"),"$.Accountnumber").cast("long")) \
  .withColumn("Base_Symbol", get_json_object(col("Raw_Event"),"$.BaseSymbol").cast("STRING")) \
  .withColumn("Other_Symbol", get_json_object(col("Raw_Event"),"$.OtherSymbol").cast("STRING")) \
  .withColumn("Base_Amount", get_json_object(col("Raw_Event"),"$.BaseAmount").cast("FLOAT")) \
  .withColumn("Other_Amount", get_json_object(col("Raw_Event"),"$.OtherAmount").cast("FLOAT")) \
  .withColumn("Volume_Usd", get_json_object(col("Raw_Event"),"$.USDValue").cast("FLOAT")) \
  .withColumn("Open_PL_Usd", get_json_object(col("Raw_Event"),"$.OpenPL_USD").cast("FLOAT")) 
  .withColumn("Date_Year", col("Date")[0:7].cast("int")) \
  .select("Event_Source_System", "Event_Source", "Source_Event_Name"
          "Date", "Transaction_Number", "Position_Number", "Accountnumber", 
          "Base_Symbol", "Other_Symbol", "Base_Amount", "Other_Amount", "Volume_Usd", "Open_PL_Usd", 
          "Raw_Event", "Date_Year")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small 

# Start the query to continuously upsert into aggregates tables in update mode
query_dwh = (
  df_stream_exrtacted
    .writeStream
    .trigger(processingTime = "10 seconds")    #.trigger(processingTime = "10 seconds") for continuos .trigger(once = True)
    .format("delta")
    .partitionBy("TTime_Month")
    .option("path", dwh_db_path + "fact_open_pl")
    .option("checkpointLocation", dwh_db_checkpointLocation + "fact_open_pl" )
    .start()
)

# COMMAND ----------

print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Waiting for stream to end")

# wait for streaimng to start 
time.sleep(10 * 60)

t1 = time.time()
timeout_duration = 10 * 60

rc = query_dwh.lastProgress["numInputRows"] 
print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Row Count: ", rc)

while ((rc > 0) & ((time.time() - t1) < timeout_duration)) :
  time.sleep(30)
  rc =  query_dwh.lastProgress["numInputRows"] 
  print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Row Count: ", rc)

print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Stream ended, waiting for trigger to end")

while ( query_dwh.status["isTriggerActive"] ) :
  pass

print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), " Stream stopped")

query_dwh.stop()


# COMMAND ----------

# MAGIC %sql
# MAGIC select concat_ws(':', collect_list(Model_ID) over (partition by AccountNumber order by Model_ID)),*
# MAGIC from dwhdb.models_execution
# MAGIC where AccountNumber = 216847