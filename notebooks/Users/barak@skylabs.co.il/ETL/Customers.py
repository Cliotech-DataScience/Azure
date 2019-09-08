# Databricks notebook source
# DBTITLE 1,Imports
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

ENV_DEV = "dev"
ENV_QA = "qa"
ENV_PROD = "prod"

env = "prod"

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

# DBTITLE 1,Events Hub Configuration 
# Connection String
if env == ENV_DEV :
  ev_namespace    ="qaeventshubclio"
  ev_name         ="customers_dev"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "T+FDlX3FrYLmvQws3bkaVtAhthAFyksp/jrknjo+C1M="

elif env == ENV_QA:
  ev_namespace    ="qaeventshubclio"
  ev_name         ="customers_qa"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "T+FDlX3FrYLmvQws3bkaVtAhthAFyksp/jrknjo+C1M="

elif env == ENV_PROD:
  ev_namespace    ="prodeventsbyl"
  ev_name         ="customers"
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
dfStream = (
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

dfStreamCustomersRaw = dfStream \
  .withColumn("Event_Offset", col("offset").cast("long")) \
  .withColumn("Received", col("enqueuedTime").cast("timestamp")) \
  .withColumn("Received_Day", col("enqueuedTime").cast("timestamp").cast("date")) \
  .withColumn("Raw_Event", explode(from_json(col("body").cast("string"), events_schema,  jsonOptions))) \
  .withColumn("Event_Source_Env", get_json_object(col("Raw_Event"),"$.Event_Source_Env").cast("string")) \
  .withColumn("Event_Source_System", get_json_object(col("Raw_Event"),"$.Event_Source_System").cast("string")) \
  .withColumn("Event_Source", get_json_object(col("Raw_Event"),"$.Event_Source").cast("string")) \
  .withColumn("Accountnumber", get_json_object(col("Raw_Event"),"$.AccountNumber").cast("long")) \
  .select("Event_Offset", "Received", "Event_Source_System", "Event_Source", "Accountnumber", "Raw_Event", "Received_Day")


# COMMAND ----------

# DBTITLE 1,save raw data to delta file
spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

query_raw = (
  dfStreamCustomersRaw
    .writeStream
    .trigger(processingTime = "1 minute")    #.trigger(processingTime = "10 seconds") for continuos    #.trigger(once = True) for ontime
    .format("delta")        
    .partitionBy("Received_Day")
    .option("path", raw_db_path + "customers")
    .option("checkpointLocation", raw_db_checkpointLocation + 'customers' )
    .start()
)

# COMMAND ----------


sql("create table if not exists " + raw_db + ".customers" + " using delta location '" + raw_db_path + "customers'")


# COMMAND ----------

# DBTITLE 1,etl process dim customers - extract relevant fields

dfStreamCustomersDim = dfStreamCustomersRaw \
  .filter("lower(Event_Source_Env) = lower('" + env + "') AND lower(Event_Source_System) = lower('FXNET') AND lower(Event_Source) = lower('Customers')") \
  .withColumn("Broker_Id", get_json_object(col("Raw_Event"),"$.BrokerID").cast("int")) \
  .withColumn("Broker", get_json_object(col("Raw_Event"),"$.BrokerName").cast("string")) \
  .withColumn("Folder_Id", get_json_object(col("Raw_Event"),"$.Folder_Id").cast("int")) \
  .withColumn("Folder", get_json_object(col("Raw_Event"),"$.Folder").cast("string")) \
  .withColumn("Registration_Date", get_json_object(col("Raw_Event"),"$.DateAdded").cast("timestamp")) \
  .withColumn("Country_Id", get_json_object(col("Raw_Event"),"$.CountryID").cast("int")) \
  .withColumn("Country", get_json_object(col("Raw_Event"),"$.Country").cast("string")) \
  .withColumn("Details", col("Raw_Event")) \
  .select("Accountnumber", "Received", "Broker_Id", "Broker", "Folder_Id", "Folder", "Registration_Date", "Country_Id", "Country", "Details")


# COMMAND ----------

sql(" \
  create table if not exists " + dwh_db + ".dim_customers" + " ( \
    Accountnumber int, \
    Broker_Id int, \
    Broker string, \
    Folder_Id int, \
    Folder string, \
    Registration_Date timestamp, \
    Country_Id int, \
    Country string, \
    Details string \
  ) \
  using delta \
  location '" + dwh_db_path + "dim_customers" + "'")


# COMMAND ----------

# DBTITLE 1,add data to the dimension

# Function to upsert `microBatchOutputDF` into Delta table using MERGE
def mergeCustomersToDim(microBatchOutputDF, batchId): 

  sql("insert into " + log_table + " values  (now(), 'starting microbatch for batchId : " + str(batchId)  + "')")

  # set a window and filter to take last row per account from the micro batch
  window = Window.partitionBy(microBatchOutputDF["Accountnumber"]).orderBy(microBatchOutputDF["Received"].desc())
  stg_customers_df = microBatchOutputDF \
    .withColumn("rn", row_number().over(window)).filter("rn=1") \
    .drop("rn","Received")
  
  stg_customers_df.persist()

  cnt=str(stg_customers_df.count())
  sql("insert into " + log_table + " values  (now(), 'rowcount for batchId : " + cnt  + "')")

  # Set the dataframe to view name
  stg_customers_df.createOrReplaceTempView("stg_customers")
  
  # Use the view name to apply MERGE
  # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  stg_customers_df._jdf.sparkSession().sql("""
    MERGE INTO """ + dwh_db + """.dim_customers t
    USING stg_customers s
    ON s.Accountnumber = t.Accountnumber
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

  stg_customers_df.unpersist()

  sql("insert into " + log_table + " values  (now(), 'finished microbatch for batchId : " + str(batchId)  + "')")


# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small 

# Start the query to continuously upsert into aggregates tables in update mode
query_dim = (
  dfStreamCustomersDim
    .writeStream
    .trigger(processingTime = "1 minute")    #.trigger(processingTime = "10 seconds") for continuos .trigger(once = True)
    .foreachBatch(mergeCustomersToDim)
    .outputMode("update")
    .option("checkpointLocation", dwh_db_checkpointLocation + 'dim_customers' )
    .start()
)