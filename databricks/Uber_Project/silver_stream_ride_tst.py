# Databricks notebook source
# MAGIC %md
# MAGIC # To test the stream rides (rides_raw)record change the json string field rides to json file and then to a proper schema record

# COMMAND ----------

# MAGIC %md
# MAGIC ## this is just for testing, the actual code will run from ELT pipeline - **silver_stg**

# COMMAND ----------

from pyspark.sql.types import*
from pyspark.sql.functions import*

# COMMAND ----------

rides_schema = StructType([StructField('base_fare', DoubleType(), True), StructField('booking_timestamp', StringType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('confirmation_number', StringType(), True), StructField('distance_fare', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('driver_id', StringType(), True), StructField('driver_license', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_phone', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_city_id', LongType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_location_id', StringType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('duration_minutes', LongType(), True), StructField('license_plate', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_id', StringType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('payment_method_id', LongType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_city_id', LongType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_location_id', StringType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('pickup_timestamp', StringType(), True), StructField('rating', LongType(), True), StructField('ride_id', StringType(), True), StructField('ride_status_id', LongType(), True), StructField('subtotal', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('vehicle_color', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('vehicle_make_id', LongType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_type_id', LongType(), True)])

# COMMAND ----------

df = spark.read.table("uber_cat.bronze.rides_raw")
df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))\
    .select ("parsed_rides.*")
display(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### pick the schema of the bulk ride record and use it for the rides column to create schema of the json 

# COMMAND ----------

df = spark.sql("select * from uber_cat.bronze.bulk_rides")
df.schema