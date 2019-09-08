# Databricks notebook source
# DBTITLE 1,Imports
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

ENV_DEV = "dev"
ENV_QA = "qa"
ENV_PROD = "prod"

env = ENV_PROD

# COMMAND ----------

# DBTITLE 1,config and environment
# Storage
if env == ENV_DEV :
  storage_account = "cliotestingstore"
  container = "testdata"
  raw_db = "dev_raw"
  dwh_db = "dev_dwh"

elif env == ENV_QA:
  storage_account = "cliotestingstore"
  container = "testdata"
  raw_db = "qa_raw"
  dwh_db = "qa_dwh"

elif env == ENV_PROD:
  storage_account = "dwhdbstore"
  container = "databases"
  raw_db = "prod_raw"
  dwh_db = "prod_dwh"
  
raw_db_path =  "/mnt/" + storage_account +"/" + container + "/" + raw_db + "/"
raw_db_checkpointLocation =  raw_db_path + ".checkpoints/"

dwh_db_path = "/mnt/" + storage_account +"/" + container + "/" + dwh_db + "/"
dwh_db_checkpointLocation =  dwh_db_path + ".checkpoints/"

log_table = env + "_log.debug_log"
log_path =  "/mnt/" + storage_account +"/" + container + "/" + env + "_log/debug_log"

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
if env == ENV_DEV :
  ev_namespace    ="qaeventshubclio"
  ev_name         ="communication_messages_dev"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "T+FDlX3FrYLmvQws3bkaVtAhthAFyksp/jrknjo+C1M="

elif env == ENV_QA:
  ev_namespace    ="qaeventshubclio"
  ev_name         ="communication_messages_qa"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "T+FDlX3FrYLmvQws3bkaVtAhthAFyksp/jrknjo+C1M="

elif env == ENV_PROD:
  ev_namespace    ="prodeventsbyl"
  ev_name         ="communication_messages"
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
  .filter("lower(Event_Source_Env) = lower('" + env + "') AND lower(Event_Source_System) = lower('COMMUNICATION') AND lower(Event_Source) = lower('Messages')") \
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
    .option("path", raw_db_path + "communication_messages")
    .option("checkpointLocation", raw_db_checkpointLocation + "communication_messages")
    .start()
)

# COMMAND ----------


sql("create table if not exists " +  raw_db + ".communication_messages" + " using delta location '" + raw_db_path + "communication_messages'")


# COMMAND ----------

# DBTITLE 1,etl process events

df_stream_exrtacted = df_stream_raw \
  .filter("lower(Event_Source_Env) = lower('" + env + "') AND lower(Event_Source_System) = lower('COMMUNICATION') AND lower(Event_Source) = lower('Messages')") \
  .withColumn("Event_Source_Ref_Id", get_json_object(col("Raw_Event"),"$.SendID").cast("long")) \
  .withColumn("Event_Date", get_json_object(col("Raw_Event"),"$.Event_Date").cast("timestamp")) \
  .withColumn("Event_Date_Day", col("Event_Date").cast("date")) \
  .withColumn("Accountnumber", get_json_object(col("Raw_Event"),"$.Accountnumber").cast("long")) \
  .withColumn("Source_Event_Name", get_json_object(col("Raw_Event"),"$.Event_Name").cast("string")) \
  .withColumn("Dc_Channel_Id", get_json_object(col("Raw_Event"),"$.ChannelID").cast("long")) \
  .withColumn("Event_Date_Month", col("Event_Date_Day")[0:7].cast("string"))

# COMMAND ----------

# Add event id 
ed_df = table(dwh_db + ".event_definition").persist()

df_stream_events = df_stream_exrtacted \
  .join(broadcast(ed_df),
        (lower(df_stream_exrtacted["Event_Source_System"]) == lower(ed_df["Event_Source_System"])) &
        (lower(df_stream_exrtacted["Event_Source"]) == lower(ed_df["Event_Source"])) &
        (lower(df_stream_exrtacted["Source_Event_Name"]) == lower(ed_df["Source_Event_Name"]))
        , "left_outer")  \
  .select(df_stream_exrtacted["*"], 
          coalesce(ed_df["Event_Name"], 
                   concat(df_stream_exrtacted["Event_Source_System"],lit(';'),
                          df_stream_exrtacted["Event_Source"],lit(';'),
                          df_stream_exrtacted["Source_Event_Name"])).alias("Event_Name")) \
  .select("Event_Source_System", "Event_Source", "Source_Event_Name", "Event_Source_Ref_Id", 
          "Event_Date", "Event_Date_Day", "Accountnumber", "Event_Name",
          "Dc_Channel_Id",
          "Raw_Event", "Event_Date_Month")

# COMMAND ----------

# DBTITLE 1,add data to the dwh
# Function to upsert `microBatchOutputDF` into Delta table using MERGE
def merge_to_dwh (stg_events_df, batch_id): 

  sql("insert into " + log_table + " values  (now(), 'starting microbatch for batchId : " + str(batch_id)  + "')")
  stg_events_df.persist()
  sql("insert into " + log_table + " values  (now(), 'row count : " + str(stg_events_df.count())  + "')")

  stg_events_df.write.format("delta").mode("append").partitionBy("Event_Date_Month").save(dwh_db_path + "fact_events" )
  
  sql("insert into " + log_table + " values  (now(), 'finished microbatch for batchId : " + str(batch_id)  + "')")
  stg_events_df.unpersist()


# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small 

# Start the query to continuously upsert into aggregates tables in update mode
query_dwh = (
  df_stream_events
    .writeStream
    .trigger(processingTime = "10 seconds")    #.trigger(processingTime = "10 seconds") for continuos .trigger(once = True)
    .foreachBatch(merge_to_dwh)
    .outputMode("update")
    .option("checkpointLocation", dwh_db_checkpointLocation + "communication_messages" )
    .start()
)