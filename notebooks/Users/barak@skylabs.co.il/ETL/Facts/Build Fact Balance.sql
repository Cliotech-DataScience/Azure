-- Databricks notebook source
-- MAGIC %python
-- MAGIC from_date_day = sql("select cast(max(Event_Date_Day) - Interval 7 Days as string) as From_Date_Day from prod_dwh.fact_balance where Event_Date_Year >= cast(year(now())-1 as string) ").collect()[0]["From_Date_Day"]
-- MAGIC #to_date_month = result["To_Date_Month"]
-- MAGIC #print("from_date: ", from_date, ", from_date_month: ", from_date_month,", to_date_month: ", to_date_month)
-- MAGIC #print("from_date_month: ", from_date_month)
-- MAGIC # sql_rates = "select * from prod_dwh.fact_rates_full where TTime_Month between '" + from_date_month + "' and '"+ to_date_month + "' and TTime >= '" + from_date + "'"
-- MAGIC #sql_rates = "select * from prod_dwh.fact_rates_full where TTime_Month between '" + from_date_month + "' and '"+ to_date_month + "'"
-- MAGIC ##sql_rates = "select * from prod_dwh.fact_rates_full where TTime_Month >= '" + from_date_month + "'"
-- MAGIC print (from_date_day)
-- MAGIC ##sql(sql_rates).createOrReplaceTempView("v_rates_period")
-- MAGIC from_date_month = from_date_day[0:7]
-- MAGIC print (from_date_month)

-- COMMAND ----------

-- DBTITLE 1,get period data
-- MAGIC %python
-- MAGIC sqlcmd="""
-- MAGIC select Event_Date_Month, Event_Date_Day, Accountnumber, upper(Currency) as Currency, Amount  
-- MAGIC from prod_dwh.fact_events 
-- MAGIC where Event_Source_System =  'FXNET' 
-- MAGIC   and Event_Source = 'Accountcard'
-- MAGIC   and Event_Date_Day >= cast('""" + from_date_day +  """' as date)
-- MAGIC   and Event_Date_Month >= '""" + from_date_month + """'
-- MAGIC     """
-- MAGIC print(sqlcmd)
-- MAGIC sql(sqlcmd).createOrReplaceTempView("vac_for_date")

-- COMMAND ----------

-- DBTITLE 1,get prev date data
-- MAGIC %python
-- MAGIC sqlcmd="""
-- MAGIC select *
-- MAGIC from prod_dwh.fact_balance
-- MAGIC where Event_Date_Day = cast('""" + from_date_day + """' - Interval 1 day as date)
-- MAGIC   and Event_Date_Year = left('""" + from_date_day + """' - Interval 1 day, 4)
-- MAGIC     """
-- MAGIC print(sqlcmd)
-- MAGIC sql(sqlcmd).createOrReplaceTempView("vbalance_prev")

-- COMMAND ----------

create or replace temp view vbalance_period
as
with b as
(
  -- get prev data as change for calculating commulative balance
  select 'Prev' as State, Event_Date_Day, Accountnumber, Currency, Balance as Balance_Change
  from vbalance_prev
    union all
  -- get all account transactions
  select 'Current' as State, Event_Date_Day, Accountnumber, Currency, Sum(Amount) as Balance_Change
  from vac_for_date
  group by Event_Date_Day, Accountnumber, Currency
),
ac_dates as
(
  -- get min and max date per account and currency
  SELECT Accountnumber,Currency, min(Event_Date_Day) as Start_Date--, max(Event_Date_Day) as End_Date
  from b
  group by Accountnumber, Currency
),
ac_d as
(
  -- build range for account and currency ()
--  SELECT Accountnumber,Currency,explode( sequence(Start_Date, End_Date, interval 1 day)) as Event_Date_Day 
  SELECT Accountnumber,Currency,explode( sequence(Start_Date, cast(now() as date), interval 1 day)) as Event_Date_Day 
  from ac_dates
), 
res as
(
  -- create balance list
  select nvl(b.State, 'Generated') as State, ac_d.Event_Date_Day, ac_d.Accountnumber, ac_d.Currency, 
     Sum(b.Balance_Change) over(partition by ac_d.Accountnumber, ac_d.Currency order by ac_d.Event_Date_Day) as Balance,
     nvl(b.Balance_Change,0) as Balance_Change
  from ac_d left join  b on ac_d.Event_Date_Day = b.Event_Date_Day and ac_d.Accountnumber = b.Accountnumber and ac_d.Currency = b.Currency
)
select *
from res
where ( abs(Balance_Change) > 0.01 or abs(Balance) > 0.01)
  and State <> 'Prev'


-- COMMAND ----------

-- DBTITLE 1,calculate conversions
create or replace temp view v_balance_converted 
as
-- Balance Conversion
with b as
(
  -- find rates for date and currency
  select /*+ BROADCAST(prod_dwh.fact_daily_rates) */
    fb.State,
    fb.Event_Date_Day, fb.AccountNumber, fb.Currency, fb.Balance, fb.Balance_Change,
    case 
          when fb.Currency = 'USD' then 1
          when fb.Currency = upper(r.BaseCCY) then r.Close_Mid
          when fb.Currency = upper(r.OtherCCY) then 1 / r.Close_Mid
    end  as Convert_To_Usd_Rate
  from vbalance_period as fb
    left join prod_dwh.fact_rates_daily r on r.Date = fb.Event_Date_Day 
      and ((upper(r.BaseCCY) = fb.Currency and upper(r.OtherCCY) = 'USD') or (upper(r.OtherCCY) = fb.Currency and upper(r.BaseCCY) = 'USD'))
)
, bc as
(
  -- use rate for conversion
  select b.State,
    b.Event_Date_Day, b.AccountNumber, b.Currency,
    b.Balance,
    b.Balance * b.Convert_To_Usd_Rate as Balance_Usd,
    b.Balance_Change,
    b.Balance_Change * b.Convert_To_Usd_Rate as Balance_Change_Usd,
    b.Convert_To_Usd_Rate
  from b

    union all
  -- get prev data to calc difference in usd
  select 'Prev' as State,
    p.Event_Date_Day, p.AccountNumber, p.Currency,
    p.Balance,
    p.Balance_Usd,
    p.Balance_Change,
    p.Balance_Change_Usd,
    p.Convert_To_Usd_Rate
  from vbalance_prev p
)
, res as
(
  -- calc the diff from prev date
  select State, bc.Event_Date_Day, bc.AccountNumber, bc.Currency, 
    bc.Balance, bc.Balance_Usd,
    bc.Balance_Change, bc.Balance_Change_Usd,
    bc.Balance_Usd - lag(bc.Balance_Usd, 1, 0) over (partition by  bc.AccountNumber, bc.Currency order by bc.Event_Date_Day) as Balance_Usd_Change,
    bc.Convert_To_Usd_Rate
  from bc
)
select r.Event_Date_Day, r.AccountNumber, r.Currency, 
    r.Balance, r.Balance_Usd,
    r.Balance_Change, r.Balance_Change_Usd,
    r.Balance_Usd_Change,
    r.Convert_To_Usd_Rate
from res r
where r.State <> 'Prev'

-- COMMAND ----------

drop table if exists prod_dwh.stg_fact_balance

-- COMMAND ----------

create table prod_dwh.stg_fact_balance
using delta
as
select *,left(Event_Date_Day,4) as Event_Date_Year
from v_balance_converted


-- COMMAND ----------

MERGE INTO prod_dwh.fact_balance as f
USING prod_dwh.stg_fact_balance  as v
ON v.Accountnumber = f.Accountnumber and v.Currency = f.Currency and v.Event_Date_Day=f.Event_Date_Day and v.Event_Date_Year = f.Event_Date_Year
WHEN MATCHED 
  THEN UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *
  



-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false;
OPTIMIZE prod_dwh.fact_balance where Event_Date_Year >= cast(year(now())-1 as string)  zorder by Accountnumber, Event_Date_Day;
VACUUM  prod_dwh.fact_balance retain 0 hours;