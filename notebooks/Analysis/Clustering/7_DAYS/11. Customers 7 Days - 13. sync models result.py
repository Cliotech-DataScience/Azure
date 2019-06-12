# Databricks notebook source
# DBTITLE 1,get accounts to sync
# MAGIC %sql
# MAGIC 
# MAGIC create or replace temp view  accounts_to_sync
# MAGIC as
# MAGIC select Accountnumber ,Model_ID ,Model_Result
# MAGIC from dwhdb.Models_Execution
# MAGIC where Model_ID=207
# MAGIC and Model_Execution_Date > nvL(Sync_Date,'1900-01-01');
# MAGIC 
# MAGIC cache table accounts_to_sync;

# COMMAND ----------

# MAGIC %scala
# MAGIC val model_df = spark.sql("select * from accounts_to_sync")

# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNT(*) from accounts_to_sync

# COMMAND ----------

# DBTITLE 1, Truncate mrr_2day_customers_data table
# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC import sqlContext.implicits._
# MAGIC val query = """    TRUNCATE TABLE dbo.mrr_7day_customers_data""".stripMargin
# MAGIC  
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> "eventsdw.database.windows.net",
# MAGIC   "databaseName" -> "bi-db",
# MAGIC   "user"         -> "bi_user",
# MAGIC   "password"     -> "tLymmiFPnLYp5yOUOlPz",
# MAGIC   "queryCustom"  -> query
# MAGIC ))
# MAGIC 
# MAGIC //spark.azurePushdownQuery(config)
# MAGIC sqlContext.sqlDBQuery(config)

# COMMAND ----------

# DBTITLE 1,Upload accounts to sync
# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC // Aquire a DataFrame collection (val collection)
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"            -> "eventsdw.database.windows.net",
# MAGIC   "databaseName"   -> "bi-db",
# MAGIC   "dbTable"        -> "mrr_7day_customers_data",
# MAGIC   "user"           -> "bi_user",
# MAGIC   "password"       -> "tLymmiFPnLYp5yOUOlPz",
# MAGIC   "connectTimeout" -> "5", //seconds
# MAGIC   "queryTimeout"   -> "30"  //seconds
# MAGIC ))
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC 
# MAGIC model_df.write.mode(SaveMode.Append).sqlDB(config)

# COMMAND ----------

# DBTITLE 1,MERGE  mrr data
# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC import sqlContext.implicits._
# MAGIC val query = """ EXEC dbo.azure_merge_mrr_7day_customers_data_to_Customers_Models_Data""".stripMargin
# MAGIC  
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> "eventsdw.database.windows.net",
# MAGIC   "databaseName" -> "bi-db",
# MAGIC   "user"         -> "bi_user",
# MAGIC   "password"     -> "tLymmiFPnLYp5yOUOlPz",
# MAGIC   "queryCustom"  -> query
# MAGIC ))
# MAGIC 
# MAGIC //spark.azurePushdownQuery(config)
# MAGIC sqlContext.sqlDBQuery(config)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view  Accounts_clustering_to_load
# MAGIC as
# MAGIC select Accountnumber , now() as Sync_Date ,207 as Model_ID,Model_Result 
# MAGIC from accounts_to_sync

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from Accounts_clustering_to_load

# COMMAND ----------

# DBTITLE 1,Update Models Execution
# MAGIC %sql
# MAGIC MERGE INTO dwhdb.Models_Execution  as t
# MAGIC USING Accounts_clustering_to_load  as s
# MAGIC ON  t.Accountnumber=s.Accountnumber and t.Model_ID = s.Model_ID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET  t.Sync_Date=s.Sync_Date

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dwhdb.Models_Execution;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM   dwhdb.Models_Execution RETAIN 1 HOURS;

# COMMAND ----------

