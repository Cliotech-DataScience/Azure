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
  ev_name         ="trading_dev"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "T+FDlX3FrYLmvQws3bkaVtAhthAFyksp/jrknjo+C1M="

elif env == ENV_QA:
  ev_namespace    ="qaeventshubclio"
  ev_name         ="trading_qa"
  ev_sas_key_name ="ListenEventsAccessKey"
  ev_sas_key_val  = "T+FDlX3FrYLmvQws3bkaVtAhthAFyksp/jrknjo+C1M="

elif env == ENV_PROD:
  ev_namespace    ="prodeventsbyl"
  ev_name         ="trading"
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
  .filter("lower(Event_Source_Env) = lower('" + env + "') AND lower(Event_Source_System) = lower('FXNET') AND lower(Event_Source) = lower('Accountcard')") \
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
    .option("path", raw_db_path + "trading")
    .option("checkpointLocation", raw_db_checkpointLocation + "trading")
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from prod_raw.trading
# MAGIC where Received_Day > '2019-09-02'
# MAGIC order by Received desc

# COMMAND ----------

sql("create table if not exists " + raw_db + ".trading" + " using delta location '" + raw_db_path + "trading" + "'")


# COMMAND ----------

# DBTITLE 1,etl process fact trading events

df_stream_exrtacted = df_stream_raw \
  .filter("lower(Event_Source_Env) = lower('" + env + "') AND lower(Event_Source_System) = lower('FXNET') AND lower(Event_Source) = lower('Accountcard')") \
  .withColumn("Event_Source_Ref_Id", get_json_object(col("Raw_Event"),"$.TransactionID").cast("long")) \
  .withColumn("Event_Date", get_json_object(col("Raw_Event"),"$.RunDate").cast("timestamp")) \
  .withColumn("Event_Date_Day", col("Event_Date").cast("date")) \
  .withColumn("Accountnumber", get_json_object(col("Raw_Event"),"$.AccountNumber").cast("long")) \
  .withColumn("Currency", get_json_object(col("Raw_Event"),"$.Currency").cast("string")) \
  .withColumn("Credit", get_json_object(col("Raw_Event"),"$.Credit").cast("double")) \
  .withColumn("Debit", get_json_object(col("Raw_Event"),"$.Debit").cast("double")) \
  .withColumn("Amount", expr("Credit - Debit").cast("double")) \
  .withColumn("Source_Event_Name", get_json_object(col("Raw_Event"),"$.TypeName").cast("string")) \
  .withColumn("Amount_Usd", get_json_object(col("Raw_Event"),"$.USDvalue").cast("double")) \
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
          "Currency", "Amount", "Amount_Usd",
          "Raw_Event", "Event_Date_Month")

# COMMAND ----------

# DBTITLE 1,add data to the fact

# Function to upsert `microBatchOutputDF` into Delta table using MERGE
def merge_to_dwh(stg_events_df, batch_id): 

  sql("insert into " + log_table + " values  (now(), 'starting microbatch for batchId : " + str(batch_id)  + "')")
  stg_events_df.persist()
  sql("insert into " + log_table + " values  (now(), 'after persist and count : " + str(stg_events_df.count())  + "')")


  # get max counter per event
  max_ec_df = sql("""select Accountnumber, Event_Name, max(Event_Count) as Max_Event_Counter
                from """ + dwh_db + """.fact_events
                group by Accountnumber, Event_Name
                having max(Event_Count) > 0 """)
  
  # add counter for the event + cummulative sum
  trade_events_df = broadcast(stg_events_df) \
    .join(max_ec_df, 
          (stg_events_df["Accountnumber"] == max_ec_df["Accountnumber"]) &
          (stg_events_df["Event_Name"] == max_ec_df["Event_Name"]) ,
          "left_outer") \
    .select(stg_events_df["*"], \
            max_ec_df["Max_Event_Counter"]) \
    .withColumn("Event_Counter", row_number().over(Window
                                                   .partitionBy(["Accountnumber","Event_Name"])
                                                   .orderBy(stg_events_df["Event_Date"].asc()))) \
    .withColumn("Event_Count", expr("Event_Counter + nvl(Max_Event_Counter,0)").cast("int")) \
    .select("Event_Source_System", "Event_Source",  "Source_Event_Name", "Event_Source_Ref_Id", 
            "Event_Date", "Event_Date_Day", "Accountnumber", "Event_Name",
            "Currency", "Amount", "Amount_Usd",
            "Event_Count", 
            "Raw_Event", "Event_Date_Month") 
  
  sql("insert into " + log_table + " values  (now(), 'after trade_events_df : " + str(trade_events_df.count())  + "')")

#   #df for deposits
#   deposits_df = trade_events_df.filter("Event_Name = 'Deposit'")

#   # set a window and filter to take first and second deposit
#   ftd_df = deposits_df \
#     .filter("Event_Count in (1,2)") \
#     .withColumn("Counter_Name", 
#                 when(col("Event_Count") == 1, "First")
#                 .when(col("Event_Count") == 2, "Second")) \
#     .groupBy("Accountnumber") \
#     .pivot("Counter_Name").agg(first("Event_Date").alias("Deposit_Date"), 
#                                first("Amount_Usd").alias("Deposit_Usd"))
  
#   sql("insert into " + log_table + " values  (now(), 'after ftd_df : " + str(ftd_df.count())  + "')")

#   # set a window and filter to take last row per account from the micro batch
#   last_deposit_df = deposits_df \
#     .withColumn("last_dep", row_number().over(Window
#                                               .partitionBy(["Accountnumber"])
#                                               .orderBy(deposits_df["Event_Count"].desc()))) \
#     .filter("last_dep = 1") \
#     .selectExpr("Accountnumber", "Event_Count as Deposit_Count", "Event_Usd_Total as Deposit_Usd_Total" )
  
#   sql("insert into " + log_table + " values  (now(), 'after last_deposit_df : " + str(last_deposit_df.count())  + "')")

#   #joining data
#   stg_customers_trading_details = ftd_df \
#     .join(broadcast(last_deposit_df), ftd_df["Accountnumber"] == last_deposit_df["Accountnumber"], "fullouter") \
#     .withColumn("acc",coalesce(ftd_df["Accountnumber"],last_deposit_df["Accountnumber"])) \
#     .drop("Accountnumber") \
#     .withColumnRenamed("acc","Accountnumber")
  
#   sql("insert into " + log_table + " values  (now(), 'after stg_customers_trading_details : " + str(stg_customers_trading_details.count())  + "')")

  trade_events_df.write.format("delta").mode("append").partitionBy("Event_Date_Month").save(dwh_db_path + 'fact_events')
  sql("refresh table " + dwh_db + ".fact_events") 
#   if (stg_customers_trading_details.count() >0):
#     # Set the dataframe to view name
#     stg_customers_trading_details.createOrReplaceTempView("stg_customers_trading_details")
#     # Use the view name to apply MERGE
#     # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
#     stg_customers_trading_details._jdf.sparkSession().sql("""
#       MERGE INTO """ + dwh_db + ".dim_customers_trading_details" + """ t
#       USING (SELECT *, Now() as Update_date FROM stg_customers_trading_details) AS s
#       ON s.Accountnumber = t.Accountnumber
#       WHEN MATCHED THEN UPDATE SET *
#       WHEN NOT MATCHED THEN INSERT *
#     """)

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
    .option("checkpointLocation", dwh_db_checkpointLocation + 'trading' )
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from prod_log.debug_log
# MAGIC where log_date >'2019-09-02'
# MAGIC order by  log_date desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from dev_dwh.dim_customers_trading_details--dev_dwh.dim_customers_trading_details
# MAGIC 
# MAGIC --order by Received 

# COMMAND ----------

# MAGIC %sql
# MAGIC select Event_Source_System, Event_Date_Day, count(*) as events, max(Event_Date) as last_event
# MAGIC from prod_dwh.fact_events
# MAGIC where Event_Date_Month ='2019-09'
# MAGIC group by Event_Source_System, Event_Date_Day
# MAGIC order by 1,2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from dev_raw.customers
# MAGIC where Accountnumber = 1017
# MAGIC order by Received

# COMMAND ----------

# MAGIC %sql
# MAGIC select Accountnumber
# MAGIC from dev_dwh.dim_customers
# MAGIC 
# MAGIC --order by Received 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE  dev_raw.trading ; 
# MAGIC OPTIMIZE  dev_dwh.dim_customers_trading_details ZORDER BY (Accountnumber); 
# MAGIC OPTIMIZE  dev_dwh.fact_trading  ZORDER BY (Accountnumber) ; 
# MAGIC OPTIMIZE  dev_log.debug_log 

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM dev_raw.trading RETAIN 0 HOURS ; 
# MAGIC VACUUM dev_dwh.dim_customers_trading_details RETAIN 0 HOURS ;
# MAGIC VACUUM dev_dwh.fact_trading RETAIN 0 HOURS ;
# MAGIC VACUUM dev_log.debug_log RETAIN 0 HOURS 

# COMMAND ----------

# MAGIC %sql
# MAGIC select  max(cast(get_json_object(Raw_Event,"$.LastUpdate") as timestamp))
# MAGIC from prod_dwh.fact_events_p
# MAGIC where Event_Source_System = 'FXNET'
# MAGIC and Event_Date_Month >= '2019-07'