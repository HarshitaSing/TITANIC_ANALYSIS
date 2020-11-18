# Databricks notebook source
airbnb_df = spark.read.format('csv').option("header","true").option("inferSchema","true").load('/FileStore/tables/listings-1.csv')
display(airbnb_df.limit(15))

# COMMAND ----------

airbnb_df.na.drop()
airbnb_df.count()

# COMMAND ----------

#neighbourhood_group column is not completely null
display(airbnb_df.filter("neighbourhood_group is not null"))

# COMMAND ----------

# DBTITLE 1,Duplicate host_ids
#count(host_id) > 1 dataframe
airbnb_df.createOrReplaceTempView("host_id_count")
host_id_count = spark.sql("select host_id,count(*) as count from host_id_count group by host_id having count > 1 ")
display(host_id_count.limit(10))

# COMMAND ----------

import pyspark.sql.functions as f

max_booking_host_id = host_id_count.agg({"host_id":"max"}).collect()[0]
print (max_booking_host_id)

# COMMAND ----------

display(airbnb_df.filter(f.col("host_id") == 'Hotel'))

# COMMAND ----------


