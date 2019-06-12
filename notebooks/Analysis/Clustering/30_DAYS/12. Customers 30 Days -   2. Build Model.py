# Databricks notebook source
# import packages

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
import pickle
from sklearn.metrics import silhouette_score
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view questions_for_clustering
# MAGIC as
# MAGIC 
# MAGIC select AccountNumber,nvl(get_json_object(details, "$.AnnualIncome"),'didnt_answear') as Q_AnnualIncome  
# MAGIC       ,nvl(get_json_object(details, "$.ExpectedInvestmentAmount"),'didnt_answear') as Q_ExpectedInvestmentAmount
# MAGIC        ,nvl(get_json_object(details, "$.NetWorth"),'didnt_answear') as Q_NetWorth
# MAGIC       ,nvl(get_json_object(details, "$.Occupation"),'didnt_answear') as Q_Occupation
# MAGIC       ,nvl(get_json_object(details, "$.OriginOfFunds"),'didnt_answear') as Q_OriginOfFunds
# MAGIC       ,nvl(get_json_object(details, "$.Title"),'didnt_answear') as Q_title
# MAGIC       ,nvl(get_json_object(details, "$.CFDExperience"),'didnt_answear') as Q_CFDExperience
# MAGIC from dwhdb.Customer_Questionnaires

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view Model_data
# MAGIC as
# MAGIC select d.AccountNumber as acc,
# MAGIC        d.AccountNumber,
# MAGIC        d.folder,
# MAGIC        nvl(d.Supplier_Group,'unknow') as suppliergroup ,
# MAGIC        d.first_deposit_amount,
# MAGIC        d.number_of_deals,
# MAGIC        d.unique_instruments_traded,
# MAGIC        d.trading_days,
# MAGIC        d.FirstDepositDate,
# MAGIC        d.deposit_amount_30_days_after_deposit,
# MAGIC        d.deposit_events_30_Days_after_Deposit,
# MAGIC        d.days_from_last_deposit,
# MAGIC        d.days_from_last_trading_Day,
# MAGIC        d.IPA_30_DAYS,
# MAGIC        ---questions_
# MAGIC       q.Q_AnnualIncome ,
# MAGIC       q.Q_ExpectedInvestmentAmount,
# MAGIC       q.Q_NetWorth,
# MAGIC       q.Q_Occupation,
# MAGIC       q.Q_OriginOfFunds,
# MAGIC       q.q_title ,
# MAGIC       q.Q_CFDExperience
# MAGIC from dwhdb.Models_Customers_Data_30_Days  d
# MAGIC left join questions_for_clustering q on  q.AccountNumber=d.AccountNumber

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from Model_data

# COMMAND ----------

#Read the data to pandas dataframe
pddf=sqlContext.sql('SELECT * FROM Model_data').toPandas()


# COMMAND ----------

display(pddf.head(5))

# COMMAND ----------

# DBTITLE 1,no deal 
# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from Model_data
# MAGIC where number_of_deals is null

# COMMAND ----------

pddf['deposit_events']=pddf.deposit_events_30_Days_after_Deposit+1

# COMMAND ----------

# DBTITLE 1,quantile 
IPA_30_DAYS_qunt_95=pddf.IPA_30_DAYS.quantile(0.95)
number_of_deals_qunt_95=pddf.number_of_deals.quantile(0.95)
unique_instruments_traded_quantile_95=pddf.unique_instruments_traded.quantile(0.95)
deposit_events_quantile_95=pddf.deposit_events.quantile(0.95) 

pddf['IPA_30_DAYS_quantile']=np.where(pddf.IPA_30_DAYS<pddf.IPA_30_DAYS.quantile(0.95),pddf.IPA_30_DAYS,pddf.IPA_30_DAYS.quantile(0.95))
pddf['number_of_deals_quantile']=np.where(pddf.number_of_deals<pddf.number_of_deals.quantile(0.95),pddf.number_of_deals,pddf.number_of_deals.quantile(0.95))
pddf['unique_instruments_traded_quantile']=np.where(pddf.unique_instruments_traded<pddf.unique_instruments_traded.quantile(0.95),pddf.unique_instruments_traded,pddf.unique_instruments_traded.quantile(0.95))
pddf['number_of_deposit_quantile']=np.where(pddf.deposit_events<pddf.deposit_events.quantile(0.95),pddf.deposit_events,pddf.deposit_events.quantile(0.95))
 
print ("IPA 30 DAYS quantile(0.95)                is ..",IPA_30_DAYS_qunt_95) 
print ("number of deals quantile(0.95)           is ..",number_of_deals_qunt_95) 
print ("unique instruments traded quantile(0.95) is ..",unique_instruments_traded_quantile_95) 
print ("deposit events quantile(0.95)            is ..",deposit_events_quantile_95) 


# COMMAND ----------

pddf['days_from_last_deposit']=np.where(pddf['days_from_last_deposit'].isnull()==1,30,pddf['days_from_last_deposit'])

# COMMAND ----------

# DBTITLE 1,save quantile 
IPA_30_DAYS_quantile_95=pddf.IPA_30_DAYS.quantile(0.95)
number_of_deals_quantile_95=pddf.number_of_deals.quantile(0.95)
unique_instruments_traded_quantile_95=pddf.unique_instruments_traded.quantile(0.95)
number_of_deposit_quantile_95=pddf.deposit_events.quantile(0.95)

quantiles = [['ipa', IPA_30_DAYS_quantile_95], ['deposit', number_of_deposit_quantile_95], ['instrument', unique_instruments_traded_quantile_95],['deals',number_of_deals_quantile_95] ]

# Create the pandas DataFrame 
quantile_df = pd.DataFrame(quantiles, columns = ['measure', '95_quantile_training']) 

display(quantile_df)

# COMMAND ----------

# DBTITLE 1, quantile to Spark Data Frame
# Create a Spark DataFrame from a Pandas DataFrame using Arrow
quantile_df_df = spark.createDataFrame(quantile_df)
quantile_df_df.createOrReplaceTempView("quantile")

##quantile_df.to_csv("W:/Marketing_Documents/Analysis team/Nati/Models/traders_segmentaion/quantile_df.csv")

# COMMAND ----------

# MAGIC  %sql
# MAGIC   CREATE TABLE dwhdb.30_day_cluster_models_quantile
# MAGIC   USING delta
# MAGIC   AS SELECT * FROM quantile
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC   OPTIMIZE dwhdb.30_day_cluster_models_quantile;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM dwhdb.30_day_cluster_models_quantile RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dwhdb.30_day_cluster_models_quantile

# COMMAND ----------

pddf=pddf[pddf.suppliergroup!='IBS']

# COMMAND ----------

threshold = 100
vc = pddf['folder'].value_counts() 
to_remove = vc[vc <= threshold].index

pddf.loc[pddf['folder'].isin(to_remove), "folder"] = "other"

# COMMAND ----------

folder_mean = pddf.groupby(['folder'], as_index=False).agg(
                      {'IPA_30_DAYS_quantile':['mean']})

folder_mean.columns=['folder','folder_mean']
pddf=pddf.merge(folder_mean, how='left', on='folder')

display(folder_mean)

# COMMAND ----------

Q_Occupation_mean = pddf.groupby(['Q_Occupation'], as_index=False).agg(
                      {'IPA_30_DAYS_quantile':['mean']})
Q_Occupation_mean.columns=['Q_Occupation','Q_Occupation_mean']
pddf=pddf.merge(Q_Occupation_mean, how='left', on='Q_Occupation')

display(Q_Occupation_mean)

# COMMAND ----------

# DBTITLE 1,save Q_Occupation and folder mean
# Create a Spark DataFrame from a Pandas DataFrame using Arrow
Q_Occupation_mean_df = spark.createDataFrame(Q_Occupation_mean)
Q_Occupation_mean_df.createOrReplaceTempView("Q_Occupation_mean")

folder_mean_df = spark.createDataFrame(folder_mean)
folder_mean_df.createOrReplaceTempView("folder_mean")

# COMMAND ----------

# MAGIC  %sql
# MAGIC   CREATE TABLE dwhdb.30_day_cluster_models_Q_Occupation_mean
# MAGIC   USING delta
# MAGIC   AS SELECT * FROM Q_Occupation_mean ; 
# MAGIC   
# MAGIC OPTIMIZE  dwhdb.30_day_cluster_models_Q_Occupation_mean ;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM   dwhdb.30_day_cluster_models_Q_Occupation_mean RETAIN 0 HOURS;
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM folder_mean

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from   dwhdb.30_day_cluster_models_folder_mean

# COMMAND ----------

# MAGIC  %sql
# MAGIC CREATE TABLE dwhdb.30_day_cluster_models_folder_mean
# MAGIC   USING delta
# MAGIC   AS SELECT * FROM folder_mean

# COMMAND ----------

# MAGIC  %sql
# MAGIC   OPTIMIZE dwhdb.30_day_cluster_models_folder_mean ;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM  dwhdb.30_day_cluster_models_folder_mean RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dwhdb.30_day_cluster_models_folder_mean

# COMMAND ----------

df_Activity=pddf[pddf.number_of_deals.isnull()==0]

# COMMAND ----------

len(df_Activity )

# COMMAND ----------

bronze=df_Activity[(df_Activity.IPA_30_DAYS<=101) ]
len(bronze)

# COMMAND ----------

bronze.unique_instruments_traded_quantile.describe()

# COMMAND ----------

df_no_bronze=df_Activity[ (df_Activity.IPA_30_DAYS>101)]
len(df_no_bronze)

# COMMAND ----------

display(df_no_bronze)

# COMMAND ----------

## Clustering

# COMMAND ----------

clusters_30_Days=df_no_bronze[['acc','days_from_last_deposit','folder_mean','Q_Occupation_mean',
                      'trading_days','number_of_deals_quantile','days_from_last_trading_Day',
                       'IPA_30_DAYS_quantile','number_of_deposit_quantile']]

# COMMAND ----------

clusters_30_Days.isnull().sum()

# COMMAND ----------

clusters_30_Days.days_from_last_deposit.describe()

# COMMAND ----------

clusters_30_Days=clusters_30_Days.dropna()
clusters_30_Days=clusters_30_Days.set_index('acc') 

# COMMAND ----------

# DBTITLE 1,normalize data 
# normalize data 
from sklearn import preprocessing
normalized_data = preprocessing.normalize(clusters_30_Days)

# COMMAND ----------

# k means clustering 

kmeans_30_days = KMeans(n_clusters=2 ,random_state=125)
predictions = kmeans_30_days.fit_predict(normalized_data) 

# COMMAND ----------

clusters_30_Days['Cluster_kmeans'] = predictions

# COMMAND ----------

clusters_30_Days['Cluster_kmeans'].value_counts()

# COMMAND ----------

# DBTITLE 1,fixed order of clusters
res_0=clusters_30_Days[clusters_30_Days.Cluster_kmeans==0]
res_1=clusters_30_Days[clusters_30_Days.Cluster_kmeans==1]
avg_0=res_0.IPA_30_DAYS_quantile.mean()
avg_1=res_1.IPA_30_DAYS_quantile.mean()
if (avg_0>avg_1):
  clusters_30_Days['Cluster_kmeans'] = clusters_30_Days['Cluster_kmeans'].replace(0,7)
else:
  clusters_30_Days['Cluster_kmeans'] = clusters_30_Days['Cluster_kmeans'].replace(1,7)

# COMMAND ----------

clusters_30_Days['Cluster_kmeans'].value_counts()

# COMMAND ----------

cluster_analysis = pd.merge(df_Activity,
                   clusters_30_Days[['Cluster_kmeans']]
                  ,how='left'
                  ,on='acc')

# COMMAND ----------

display(cluster_analysis)

# COMMAND ----------

pd.__version__

# COMMAND ----------

cluster_analysis['Cluster_kmeans'] = cluster_analysis.Cluster_kmeans.replace(np.NaN, 2)
pddf['FirstDepositDate'] = pddf['FirstDepositDate'].astype('datetime64[ns]')
pddf['week_day']=pddf['FirstDepositDate'].dt.weekday


# COMMAND ----------

cluster_analysis.Cluster_kmeans.value_counts()

# COMMAND ----------

pddf['cluster_ipa']=np.where((pddf.IPA_30_DAYS<=101) & (pddf.folder!="JP-LIVE") & (pddf.folder!="INDIA-LIVE") 
                            & (pddf.folder!="other") & (pddf.folder!="SK-LIVE") & (pddf.Q_Occupation!='A_Occupation_Sales')
                            & (pddf.suppliergroup!='Others') & (pddf.Q_ExpectedInvestmentAmount!='A_ExpectedInvestmentAmount_51000USD–200000USD')
                           & (pddf.Q_AnnualIncome!='A_AnnualIncome_250000USD–999999USD')  & (pddf.Q_OriginOfFunds!='A_OriginOfFunds_Pension')
                           & (pddf.Q_NetWorth!='A_NetWorth_100000USD–249999USD')  
                           ,"Bronze",
                          np.where(pddf.IPA_30_DAYS<=101,"Silver",
                          np.where(pddf.IPA_30_DAYS<=500,
                                   "Silver","Gold"
                          )))

# COMMAND ----------

cluster_analysis.Cluster_kmeans.value_counts()

# COMMAND ----------

cluster_analysis=cluster_analysis.set_index('acc')

# COMMAND ----------

cluster_analysis = pd.merge(pddf, 
                            cluster_analysis[['Cluster_kmeans']],
                            how='left',
                            on=['acc'])

# COMMAND ----------

display(cluster_analysis.groupby('Cluster_kmeans').mean())

# COMMAND ----------

cluster_analysis['Cluster_final']=np.where(cluster_analysis.Cluster_kmeans==7,"Gold",
                                  np.where((cluster_analysis.Cluster_kmeans!=7) & (cluster_analysis.Cluster_kmeans!=2)&                              (cluster_analysis.Cluster_kmeans.isnull()==0),"Silver",np.where(cluster_analysis.Cluster_kmeans==2,"Bronze",
np.where(cluster_analysis.Cluster_kmeans.isnull()==1,cluster_analysis.cluster_ipa,'none' ))))

# COMMAND ----------

cluster_analysis['Cluster_final'].value_counts()

# COMMAND ----------

display(cluster_analysis)

# COMMAND ----------

display( cluster_analysis.groupby('Cluster_final').mean())

# COMMAND ----------

# DBTITLE 1,Save Customers Clustering 
# Create a Spark DataFrame from a Pandas DataFrame using Arrow
cluster_analysis_df = spark.createDataFrame(cluster_analysis)
cluster_analysis_df.createOrReplaceTempView("Customers_cluster")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view Customers_cluster_to_update
# MAGIC as
# MAGIC    select AccountNumber,230 as ModelID, 'Customers_Clustering_30_Days'as Model_Name,now()  as update_date 
# MAGIC        ,Cluster_final 
# MAGIC    from Customers_cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Customers_cluster_to_update

# COMMAND ----------

# MAGIC  %sql
# MAGIC   --drop table avi.Customers_cluster_model_30

# COMMAND ----------

# MAGIC  %sql
# MAGIC   CREATE TABLE avi.Customers_cluster_model_30
# MAGIC   USING delta
# MAGIC  
# MAGIC   AS SELECT * FROM Customers_cluster_to_update
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE avi.Customers_cluster_model_30;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM  avi.Customers_cluster_model_30 RETAIN 0 HOURS;

# COMMAND ----------

# DBTITLE 1,Save Clustering Model
filename = '/dbfs/mnt/dwhdbstore/models/Clustering//Customers_30_Days_Model.pickle'
pickle.dump(kmeans_30_days, open(filename, 'wb'))


# COMMAND ----------

