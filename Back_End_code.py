# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import json
from pyspark.sql.functions import when

# COMMAND ----------

dbutils.widgets.text('user_input', "[{'city_id': 1, 'year': 2017, 'month': 1, 'day': 1, 'hour': 100}, {'city_id': 1, 'end_year': 2017, 'end_month': 1, 'end_day': 2, 'end_hour': 600}]")
input=dbutils.widgets.get("user_input")
#dbutils.widgets.removeAll()

# COMMAND ----------

l1=eval(input)

# COMMAND ----------

l1[0]

# COMMAND ----------

l1[1]

# COMMAND ----------

city_id=l1[0]["city_id"]
start_year=(l1[0]["year"])
end_year=(l1[1]["end_year"])
start_month=l1[0]["month"]
end_month=l1[1]["end_month"]
start_hour=l1[0]["hour"]
end_hour=l1[1]["end_hour"]
start_day=l1[0]["day"]
end_day=l1[1]["end_day"]

# COMMAND ----------

dbutils.fs.ls("/mnt/input")

# COMMAND ----------

df1=spark.read.option("inferschema",True).option("header",True).csv("dbfs:/mnt/input/weather_data.csv")

# COMMAND ----------

df1=df1.withColumn("year",year("date"))

# COMMAND ----------

df1=df1.withColumn("day",dayofmonth("date"))

# COMMAND ----------

df1=df1.withColumn("month",month("date"))

# COMMAND ----------

df1.display()

# COMMAND ----------

if start_month<9:
    start_month1='0'+str(start_month)
if start_day<9:
    start_day1='0'+str(start_day)

start_date=str(start_year)+'-'+str(start_month1)+'-'+str(start_day1)

# COMMAND ----------

if end_month<9:
    end_month1='0'+str(end_month)
if end_day<9:
    end_day1='0'+str(end_day)

end_date=str(end_year)+'-'+str(end_month1)+'-'+str(end_day1)

# COMMAND ----------

df2=df1.filter((df1.city_id==city_id)&(df1.date>=(lit(start_date)) ) & (df1.date<=lit(end_date)) )

# COMMAND ----------

df2.display()

# COMMAND ----------

df3=df2.withColumn("cond", when ((df2.date==start_date) & (df2.hour< start_hour), 1).when ((df2.date==end_date) & (df2.hour>end_hour),1).otherwise(0))

# COMMAND ----------

df3.display()

# COMMAND ----------

df3=df3.filter(df3.cond==0)

# COMMAND ----------

df3.display()

# COMMAND ----------

from datetime import *

# COMMAND ----------

current_timestamp=datetime.today()

# COMMAND ----------

current_timestamp=str(current_timestamp).replace(":","")

# COMMAND ----------

df3.write.save("/mnt/replication/weather_request"+current_timestamp)
