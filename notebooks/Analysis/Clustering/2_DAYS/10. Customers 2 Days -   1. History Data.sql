-- Databricks notebook source
-- MAGIC %md
-- MAGIC This notebook collects histoiry data of customers for creating the model
-- MAGIC 
-- MAGIC The data contains:
-- MAGIC 1. features of customer on the first 6 days (tradong data - deals, deposits )
-- MAGIC 2. development of the customer after   monthes (Total deposits)
-- MAGIC 
-- MAGIC 
-- MAGIC Populations :
-- MAGIC  customer fd from  2018-01 to  2018-10   

-- COMMAND ----------

 create or replace temp view contacts_data
as
select Contact_Id
,get_json_object(c.Contact_Details, "$.Contact_Serial") as Serial
,get_json_object(c.Contact_Details, "$.Country") as Country
,s.Targetmarket,s.Supplier_Group,s.Landing_Page,s.Landing_Page
from dwhdb.contacts c
left join dwhdb.serials s on s.Serial= get_json_object(c.Contact_Details, "$.Contact_Serial");

cache table contacts_data

-- COMMAND ----------

-- DBTITLE 1,get population for clustering
  create or replace temp view population
 as
 select AccountNumber
     ,CAST(get_json_object(details, "$.FirstDepositDay")AS DATE) AS FirstDepositDay
     ,CAST(get_json_object(details, "$.FirstDepositDate")AS  timestamp) AS FirstDepositDate -- cast to datetime
     ,get_json_object(details, "$.FirstDeposit_USD")  AS FirstDeposit_USD
     ,get_json_object(details, "$.ContactID")  AS ContactID
     ,c.folder,c.FolderType
     ,con.Country
     ,con.Targetmarket
     ,con.Supplier_Group
     ,con.Landing_Page
     ,con.Landing_Page
     ,con.serial
from dwhdb.customers c
      left join  contacts_data con on con.Contact_Id= get_json_object(c.details, "$.ContactID")    
WHERE FolderType like 'Live'
    and   CAST(get_json_object(details, "$.FirstDepositDay")AS DATE) >='2018-02-01' 
    and   CAST(get_json_object(details, "$.FirstDepositDay")AS DATE) <='2019-02-20'
    and   CAST(get_json_object(details, "$.FirstDealDate")AS DATE) >='2018-02-01';

 cache table population

-- 21 368 nati
-- 21,534  live

-- COMMAND ----------

select count (*) from population

-- COMMAND ----------

-- DBTITLE 1,Deposit Events for all analysis period
create or replace temp view  deposit_events
as

with deposits
as
(
select Contact_Id
      ,cast(Request.Amount_USD as int) as Event_Sum 
      ,Event_Date_day
      ,Event_Date
      ,p.AccountNumber
      ,P.folder
      ,P.serial
      ,p.FirstDepositday
     ,p.FirstDepositDate
     ,cast (p.FirstDeposit_USD as int) as FirstDeposit_USD
     ,p.Country
     ,p.Targetmarket
     ,nvl(p.Supplier_Group,'unknow') as Supplier_Group
     ,p.Landing_Page

from dwhdb.events e
     join population p on  p.AccountNumber = e.AccountNumber
where source='fxnet'
and Event_Name='accounting'
and Event_Details='Deposit'
   and Event_Date_day>='2018-01-01'

 )
 select AccountNumber
      ,folder
      ,Targetmarket,country
      ,Supplier_Group
      ,serial
      ,Landing_Page
      ,FirstDepositDate 
     ,min( FirstDeposit_USD)   as first_deposit_amount
     ,sum(case when Event_Date>FirstDepositDate and  Event_Date<date_add(FirstDepositday,2)then Event_Sum else 0 end) as deposit_amount_1_days_after_deposit
     ,sum(case when Event_Date>FirstDepositDate and Event_Date<date_add(FirstDepositday,2) then 1 else 0 end )       as deposit_events_1_Days_after_Deposit                
     ,sum(case when date_add(FirstDepositDate,6)<Event_Date and Event_Date<=date_add(FirstDepositday,120)  then Event_Sum else 0 end) as IPA_120_DAYS
     ,sum(case when date_add(FirstDepositDate,6)<Event_Date and Event_Date<=date_add(FirstDepositday,60)  then Event_Sum else 0 end) as IPA_30_DAYS
     ,min(case when Event_Date>FirstDepositDate and Event_Date<date_add(FirstDepositday,2) then Event_Date end) as first_deposit_Date_1_Days_after_first
from deposits
group by AccountNumber,folder,Targetmarket,country,Supplier_Group,serial,Landing_Page,FirstDepositDate;

cache table deposit_events;

-- COMMAND ----------

--to dell

select * from deposit_events
where  AccountNumber=7937894

-- COMMAND ----------

-- DBTITLE 1,get data for calculate 2 days activity after deposit 
create or replace temp view  1_days_trading_activity
as
select 
     e.AccountNumber
      ,e.Event_Details
      ,e.Event_Date
      ,e.Event_Date_day
      ,Request["Source"] as Source 
      ,Request["DealType"] as DealType
      ,Request["InstrumentType"] as InstrumentType
      ,Request["DealPL_USD"] as DealPL_USD
      ,Request["TransactionType_ID"] as TransactionType_ID
      ,Request["TransactionType"] as TransactionType
      ,Request["PositionNumber"] as PositionNumber
      ,Request["Direction"] as Direction
      ,Request["Instrument_Name"] as Instrument_Name
      ,Request["Instrument_ID"] as Instrument_ID
     
   -- ,e.* 
from dwhdb.events e
join population p on  (p.AccountNumber = e.AccountNumber) and  e.Event_Date_day<= date_add(p.FirstDepositday,1) 
where Source='fxnet'
and Event_Name='deal'
and Event_Date_day>'2018-01-01'  ; 
  
cache table 1_days_trading_activity

-- COMMAND ----------

create or replace temp view v_calc_transaction
as
with calc_transaction
as
(
select a.AccountNumber,a.PositionNumber
  ,min(Event_Date_day) as executionday 
  ,sum(case when InstrumentType='Commodity' and TransactionType='Open' then 1 else 0 end) as  Commodity_deals
  ,sum(case when InstrumentType='Crypto' and TransactionType='Open' then 1 else 0 end) as  Crypto_deals
  ,sum(case when InstrumentType='ETF' and TransactionType='Open' then 1 else 0 end) as  ETF_deals
  ,sum(case when InstrumentType='Forex' and TransactionType='Open' then 1 else 0 end) as  Forex_deals
  ,sum(case when InstrumentType='Index' and TransactionType='Open' then 1 else 0 end) as  Index_deals
  ,sum(case when InstrumentType='Shares' and TransactionType='Open' then 1 else 0 end) as  Shares_deals
  ,min(case when TransactionType='Open' then Event_Date end ) as open_position_date
  ,max(case when TransactionType='Close' then Event_Date end ) as close_position_date
  ,sum(case when TransactionType='Close' then DealPL_USD else 0 end) as profit_lose_trading
  ,sum(case when TransactionType='Close' and DealPL_USD>0 then 1 else 0 end) as  win_trade
  ,sum(case when TransactionType='Close' and DealPL_USD<=0 then 1 else 0 end) as lose_trade
  ,max(DealPL_USD) as deal_pl_usd
  ,max(Instrument_Name) as instrument
  ,sum(case when a.Direction ='Long' and TransactionType='Open' then 1 else 0 end) as long_deals
  ,sum(case when a.Direction ='Short' and TransactionType='Open'  then 1 else 0 end) as Short_deals
  ,sum(case when a.source='FXnetWeb' and TransactionType='Open' then 1 else 0 end ) as open_Web_deals
  ,sum(case when a.source='FXnetMobile' and TransactionType='Open' then 1 else 0 end ) as open_mobile_deals
  ,sum(case when a.source='FXnetWeb' and TransactionType='Close' then 1 else 0 end ) as close_Web_deals
  ,sum(case when a.source='FXnetMobile' and TransactionType='Close' then 1 else 0 end ) as close_mobile_deals
  ,sum(case when a.source='DealMonitor' and TransactionType='Open' then 1 else 0 end ) as open_deal_monitor_deals
  ,sum(case when a.source='DealMonitor' and TransactionType='Close' then 1 else 0 end ) as close_deal_monitor_deals
from 1_days_trading_activity a
group by a.AccountNumber,a.PositionNumber
)
select AccountNumber,

min(executionday) as first_deal_date_1  
  ,max(executionday) as last_deal_date_1  
  ,sum(commodity_deals) as commodity_deals
  ,sum(crypto_deals) as crypto_deals
  ,sum(etf_deals) as etf_deals
  ,sum(Forex_deals) as Forex_deals
  ,sum(Index_deals) as Index_deals
  ,sum(Shares_deals) as Shares_deals
  ,count(distinct cast(executionday as date)) as trading_days
  ,max(open_position_date) as last_deal_date
  ,sum(case when close_position_date is not null then 1 else 0 end) as number_of_closed_deals
  ,count(*) as number_of_deals
  ,sum(open_web_deals) as open_web_deals
  ,sum(open_mobile_deals) as open_mobile_deals
  ,sum(close_Web_deals) as close_Web_deals
  ,sum(close_mobile_deals) as close__mobile_deals
  ,sum(open_deal_monitor_deals) as open_deal_monitor_deals
  ,sum(close_deal_monitor_deals) as close_deal_monitor_deals
  ,sum(long_deals) as long_deals
  ,sum(Short_deals) as Short_deals
  ,count(distinct instrument) as unique_instruments_traded
  ,max(coalesce(deal_pl_usd,0.0)) as max_win
  ,min(coalesce(deal_pl_usd,0.0)) as max_lose
  ,min(open_position_date) as first_deal_date
  ,sum(profit_lose_trading) as profit_trading
  ,sum(case when close_position_date is not null then win_trade else 0 end)  as win_trades
  ,sum(case when close_position_date is not null then lose_trade else 0 end) as lose_trades
  ,avg(( unix_timestamp(close_position_date) -unix_timestamp(open_position_date))  /60 ) as  avg_minutes_between_deals
  ,avg(datediff(close_position_date,open_position_date)) as avg_Days_between_deals
  ,sum(case when close_position_date is null then 1 else 0 end) as open_trades
 from calc_transaction
--where open_position_date is not null
group by AccountNumber ;

 cache table  v_calc_transaction



-- COMMAND ----------


create or replace temp view Customers_data_for_all_insert
as

with all_data
as
(
select d.*,
        case when (DATEDIFF(last_deal_date,first_deal_date)=0)  then 99
             when trading_days=1 then 99
             ELSE DATEDIFF(last_deal_date,first_deal_date)/(trading_days-1) end as avg_days_between_trading_days
      , deposit_amount_1_days_after_deposit+first_deposit_amount     as IPA_1_DAYS
             
 ----calc_transaction  
   ,c.first_deal_date_1
   ,c.last_deal_date_1
   ,c.commodity_deals
   ,c.crypto_deals
   ,c.etf_deals
   ,c.Forex_deals
   ,c.Index_deals
   ,c.Shares_deals
   ,c.trading_days
   ,c.last_deal_date
   ,c.number_of_closed_deals
   ,c.number_of_deals
   ,c.open_web_deals
   ,c.open_mobile_deals
   ,c.close_Web_deals
   ,c.close__mobile_deals
   ,c.open_deal_monitor_deals
   ,c.close_deal_monitor_deals
   ,c.long_deals,Short_deals
   ,c.unique_instruments_traded
   ,c.max_win
   ,c.max_lose
   ,c.first_deal_date
   ,c.profit_trading
   ,c.win_trades
   ,c.lose_trades
   ,c.avg_minutes_between_deals
   ,c.avg_Days_between_deals
   ,c.open_trades
   from deposit_events d
left join v_calc_transaction c on  c.AccountNumber = d.AccountNumber

)
select * from all_data a

-- COMMAND ----------

select * from Customers_data_for_all_insert

-- COMMAND ----------

--drop table dwhdb.Models_Customers_Data_2_Days

-- COMMAND ----------

create table dwhdb.Models_Customers_Data_2_Days
USING DELTA
select  * from Customers_data_for_all_insert

-- COMMAND ----------

OPTIMIZE dwhdb.Models_Customers_Data_2_Days;
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM   dwhdb.Models_Customers_Data_2_Days RETAIN 0 HOURS;

-- COMMAND ----------


--MERGE INTO dwhdb.Models_Customers_Data_2_Days e
--USING Customers_data_for_all_insert i
--ON e.AccountNumber = i.AccountNumber 
--WHEN  MATCHED  THEN UPDATE SET *   
--WHEN NOT MATCHED THEN INSERT *  

-- COMMAND ----------

select count(*) from dwhdb.Models_Customers_Data_2_Days

-- COMMAND ----------

