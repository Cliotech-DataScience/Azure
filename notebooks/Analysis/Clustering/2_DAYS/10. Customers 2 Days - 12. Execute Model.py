# Databricks notebook source
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import preprocessing
from datetime import datetime
import scipy.cluster.hierarchy as sch
from sklearn.cluster import AgglomerativeClustering
from scipy.cluster.hierarchy import dendrogram, linkage 
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
from sklearn import preprocessing
import pickle

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")


# COMMAND ----------

import pandas
print('pandas: {}'.format(pandas.__version__))

# COMMAND ----------

# DBTITLE 1,get population
# MAGIC %sql
# MAGIC create or replace temp view accounts_pop
# MAGIC as
# MAGIC select Accountnumber  
# MAGIC from dwhdb.Models_Execution
# MAGIC where 1=1
# MAGIC  and Model_ID=202
# MAGIC  and Data_Mining_Date is not null
# MAGIC  and Model_Execution_Date is null
# MAGIC  and Comment is null
# MAGIC ;
# MAGIC   
# MAGIC cache table accounts_pop;  

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from accounts_pop

# COMMAND ----------

# DBTITLE 1,get customers data
# MAGIC %sql
# MAGIC create or replace temp view new_customers_data
# MAGIC as
# MAGIC select da.AccountNumber as acc
# MAGIC       ,da.AccountNumber
# MAGIC       ,folder
# MAGIC       ,suppliergroup	
# MAGIC       ,first_deposit_amount	
# MAGIC       ,number_of_deals	
# MAGIC       ,unique_instruments_traded	
# MAGIC       ,trading_days
# MAGIC       ,FirstDepositDate	
# MAGIC       ,deposit_amount_1_days_after_deposit	
# MAGIC       ,deposit_events_1_Days_after_Deposit
# MAGIC       ,IPA_1_DAYS	
# MAGIC       ,Q_AnnualIncome	
# MAGIC       ,Q_ExpectedInvestmentAmount	
# MAGIC       ,Q_NetWorth	
# MAGIC       ,Q_Occupation	
# MAGIC       ,Q_OriginOfFunds	
# MAGIC       ,Q_title	
# MAGIC       ,Q_CFDExperience
# MAGIC from dwhdb.Clustering_Model_2_Days_Customers_Data da
# MAGIC join accounts_pop ac on ac.Accountnumber=da.Accountnumber

# COMMAND ----------

# DBTITLE 1,Get  data for quantile to pandas data frame
df_quantile=sqlContext.sql('SELECT * FROM dwhdb.2_day_cluster_models_quantile').toPandas()
df_folder=sqlContext.sql('SELECT * FROM dwhdb.2_day_cluster_models_folder_mean').toPandas()
df_Q_Occupation=sqlContext.sql('SELECT * FROM dwhdb.2_day_cluster_models_Q_Occupation_mean').toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dwhdb.2_day_cluster_models_folder_mean

# COMMAND ----------

# DBTITLE 1,#Read the data to pandas data frame
pddf=sqlContext.sql('SELECT * FROM new_customers_data').toPandas()

# COMMAND ----------

display(df_quantile)

# COMMAND ----------

df_quantile=df_quantile.set_index('measure')
 

# COMMAND ----------

display(df_quantile)

# COMMAND ----------

# DBTITLE 1,set quantile to pram

ipa_95_quantile=df_quantile.loc["ipa","95_quantile_training"]
deposit_95_quantile=df_quantile.loc["deposit","95_quantile_training"]
instrument_95_quantile=df_quantile.loc["instrument","95_quantile_training"]
deals_95_quantile=df_quantile.loc["deals","95_quantile_training"]

# COMMAND ----------

# DBTITLE 1,quantile
pddf['IPA_1_DAYS_quantile']=np.where(pddf.IPA_1_DAYS<ipa_95_quantile,pddf.IPA_1_DAYS,ipa_95_quantile)
pddf['deposit_events']=pddf.deposit_events_1_Days_after_Deposit+1
pddf['number_of_deposit_quantile']=np.where(pddf.deposit_events<deposit_95_quantile,pddf.deposit_events,deposit_95_quantile)
pddf['number_of_deals_quantile']=np.where(pddf.number_of_deals<deals_95_quantile,pddf.number_of_deals,deals_95_quantile)
pddf['unique_instruments_traded_quantile']=np.where(pddf.unique_instruments_traded<instrument_95_quantile,pddf.unique_instruments_traded,instrument_95_quantile)
 
print ("  ipa_95_quantile(0.95)           is ..",ipa_95_quantile) 
print ("  deposit_95_quantile             is ..",deposit_95_quantile) 
print ("  instrument_95_quantile          is ..",instrument_95_quantile) 
print ("   deals_95_quantile              is ..",deals_95_quantile) 

# COMMAND ----------

pddf=pddf.merge(df_folder, how='left', on='folder')
pddf['folder_mean'] = pddf.folder_mean.replace(np.NaN, 318.647377)

# COMMAND ----------

##pddf=pddf[pddf.suppliergroup!='IBS']

# COMMAND ----------

pddf['Q_Occupation']=np.where(pddf['Q_Occupation'].isnull()==1,"didnt_answear",pddf['Q_Occupation'])

# COMMAND ----------

pddf=pddf.merge(df_Q_Occupation, how='left', on='Q_Occupation')
display(df_Q_Occupation)

# COMMAND ----------

df_Activity=pddf[pddf.number_of_deals.isnull()==0]

# COMMAND ----------

len(pddf)

# COMMAND ----------

len(df_Activity)

# COMMAND ----------

display(df_Activity)

# COMMAND ----------

df_no_bronze=df_Activity[ (df_Activity.unique_instruments_traded>1) |
                         
                       (df_Activity.number_of_deals_quantile>2)  |
                    (df_Activity.IPA_1_DAYS_quantile>100) | (df_Activity.trading_days>1)]

# COMMAND ----------

clusters_2_Days=df_no_bronze[['acc','number_of_deals_quantile','IPA_1_DAYS_quantile','folder_mean',
                    'Q_Occupation_mean','unique_instruments_traded_quantile','number_of_deposit_quantile']]

# COMMAND ----------

clusters_2_Days=clusters_2_Days.dropna()
clusters_2_Days=clusters_2_Days.set_index('acc') 

# COMMAND ----------

# DBTITLE 1,normalize data
normalized_data = preprocessing.normalize(clusters_2_Days)

# COMMAND ----------

# DBTITLE 1,Customers 2 Days model
filename = '/dbfs/mnt/dwhdbstore/models/Clustering//Customers_2_Days_Model.pickle'
Cluster_model = pickle.load(open(filename, 'rb'))

# COMMAND ----------

predictions = Cluster_model.fit_predict(normalized_data)

# COMMAND ----------

clusters_2_Days['Cluster_kmeans'] = predictions

# COMMAND ----------

clusters_2_Days['Cluster_kmeans'].value_counts()

# COMMAND ----------

display (clusters_2_Days.groupby('Cluster_kmeans').mean())

# COMMAND ----------

# DBTITLE 1,fixed order of clusters
res_0=clusters_2_Days[clusters_2_Days.Cluster_kmeans==0]
res_1=clusters_2_Days[clusters_2_Days.Cluster_kmeans==1]
avg_0=res_0.IPA_1_DAYS_quantile.mean()
avg_1=res_1.IPA_1_DAYS_quantile.mean()
if (avg_0>avg_1):
  clusters_2_Days['Cluster_kmeans'] = clusters_2_Days['Cluster_kmeans'].replace(0,7)
else:
  clusters_2_Days['Cluster_kmeans'] = clusters_2_Days['Cluster_kmeans'].replace(1,7)

# COMMAND ----------

clusters_2_Days['Cluster_kmeans'].value_counts()

# COMMAND ----------

cluster_analysis = pd.merge(df_Activity, clusters_2_Days[['Cluster_kmeans']], how='left', on=['acc'])


# COMMAND ----------

cluster_analysis['Cluster_kmeans'] = cluster_analysis.Cluster_kmeans.replace(np.NaN, 2)

# COMMAND ----------

cluster_analysis.Cluster_kmeans.value_counts()

# COMMAND ----------

display(cluster_analysis.groupby('Cluster_kmeans').mean())

# COMMAND ----------

pddf['FirstDepositDate'] = pddf['FirstDepositDate'].astype('datetime64[ns]')
pddf['week_day']=pddf['FirstDepositDate'].dt.weekday


# COMMAND ----------

pddf['cluster_ipa']=np.where((pddf.IPA_1_DAYS<=101) & (pddf.folder!="JP-LIVE") & (pddf.folder!="INDIA-LIVE") 
                            & (pddf.folder!="other") & (pddf.folder!="SK-LIVE") & (pddf.Q_Occupation!='A_Occupation_Sales')
                            & (pddf.suppliergroup!='Others') & (pddf.Q_ExpectedInvestmentAmount!='A_ExpectedInvestmentAmount_51000USD–200000USD')
                           & (pddf.Q_AnnualIncome!='A_AnnualIncome_250000USD–999999USD')  & (pddf.Q_OriginOfFunds!='A_OriginOfFunds_Pension')
                           & (pddf.Q_NetWorth!='A_NetWorth_100000USD–249999USD') & (pddf.week_day!=4)
                           ,"Bronze",
                          np.where(pddf.IPA_1_DAYS<=100,"Silver",
                          np.where(pddf.IPA_1_DAYS<=500,
                                   "Silver","Gold"
                          )))

# COMMAND ----------

cluster_analysis=cluster_analysis.set_index('acc')
cluster_analysis = pd.merge(pddf, cluster_analysis[['Cluster_kmeans']], how='left', on=['acc'])

# COMMAND ----------

cluster_analysis['Cluster_final']=np.where(cluster_analysis.Cluster_kmeans==7,"Gold",
                                   np.where((cluster_analysis.Cluster_kmeans!=7) & (cluster_analysis.Cluster_kmeans!=2) & (cluster_analysis.Cluster_kmeans.isnull()==0),"Silver",
                                   np.where(cluster_analysis.Cluster_kmeans==2,"Bronze",
                                   np.where(cluster_analysis.Cluster_kmeans.isnull()==1,cluster_analysis.cluster_ipa,'none' )))) 

# COMMAND ----------

cluster_analysis['Cluster_final'].value_counts()

# COMMAND ----------

cut_cluster_analysis=cluster_analysis[['AccountNumber','Cluster_final']]

# COMMAND ----------

display(cut_cluster_analysis)

# COMMAND ----------

# DBTITLE 1,Create a Spark DataFrame from a Pandas DataFrame
df = spark.createDataFrame(cut_cluster_analysis)

# COMMAND ----------

df.createOrReplaceTempView("new_customers_clustering")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view  Accounts_clustering_to_load
# MAGIC as
# MAGIC select Accountnumber,202 as Model_ID , now() as Model_Execution_Date,  to_json(named_struct('Cluster_2_day', Cluster_final)) as Model_Result
# MAGIC from new_customers_clustering

# COMMAND ----------

# DBTITLE 1,Update Models Execution
# MAGIC %sql
# MAGIC MERGE INTO dwhdb.Models_Execution  as t
# MAGIC USING Accounts_clustering_to_load  as s
# MAGIC ON  t.Accountnumber=s.Accountnumber AND  t.Model_ID = s.Model_ID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET  t.Model_Execution_Date=s.Model_Execution_Date,t.Model_Result=s.Model_Result

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE dwhdb.Models_Execution;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM   dwhdb.Models_Execution RETAIN 1 HOURS;

# COMMAND ----------

