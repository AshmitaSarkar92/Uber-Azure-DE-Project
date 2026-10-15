# Databricks notebook source
# MAGIC %sql
# MAGIC select * from uber_cat.silver.silver_obt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_cat.silver.stream_rides_stg where ride_id in ("92fe3165-c139-432d-90f5-d498d85bb17d",
# MAGIC "50457c8e-fa1a-4557-8739-ab942cc482a6",
# MAGIC "a21525c9-3bcb-49c9-95fd-637a226aa3c6",
# MAGIC "c869acb1-78f8-46f0-b0f6-0f24417367ec")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_cat.silver.stream_rides_stg

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table uber_cat.silver.stream_rides_stg

# COMMAND ----------

# MAGIC %sql select * from uber_cat.silver.silver_obt where city_updated_at is not null

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Query for the View creation for the gold Dimension Table source--Check the model_gold.py file

# COMMAND ----------

# MAGIC %md
# MAGIC ## View for DimPassenger

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(passenger_id) as passenger_id, passenger_name, passenger_email, passenger_phone from uber_cat.silver.silver_obt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_cat.gold.dim_passenger

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_cat.bronze.map_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     stream_rides_stg.ride_id, stream_rides_stg.confirmation_number, stream_rides_stg.passenger_id, stream_rides_stg.driver_id, stream_rides_stg.vehicle_id, stream_rides_stg.pickup_location_id, stream_rides_stg.dropoff_location_id, stream_rides_stg.vehicle_type_id, stream_rides_stg.vehicle_make_id, stream_rides_stg.payment_method_id, stream_rides_stg.ride_status_id, stream_rides_stg.pickup_city_id, stream_rides_stg.dropoff_city_id, stream_rides_stg.cancellation_reason_id, stream_rides_stg.passenger_name, stream_rides_stg.passenger_email, stream_rides_stg.passenger_phone, stream_rides_stg.driver_name, stream_rides_stg.driver_rating, stream_rides_stg.driver_phone, stream_rides_stg.driver_license, stream_rides_stg.vehicle_model, stream_rides_stg.vehicle_color, stream_rides_stg.license_plate, stream_rides_stg.pickup_address, stream_rides_stg.pickup_latitude, stream_rides_stg.pickup_longitude, stream_rides_stg.dropoff_address, stream_rides_stg.dropoff_latitude, stream_rides_stg.dropoff_longitude, stream_rides_stg.distance_miles, stream_rides_stg.duration_minutes, stream_rides_stg.booking_timestamp, stream_rides_stg.pickup_timestamp, stream_rides_stg.dropoff_timestamp, stream_rides_stg.base_fare, stream_rides_stg.distance_fare, stream_rides_stg.time_fare, stream_rides_stg.surge_multiplier, stream_rides_stg.subtotal, stream_rides_stg.tip_amount, stream_rides_stg.total_fare, stream_rides_stg.rating,
# MAGIC     map_vehicle_makes.vehicle_make,
# MAGIC     map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute,
# MAGIC     map_ride_statuses.ride_status,
# MAGIC     map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth,
# MAGIC     map_cities.city AS pickup_city, map_cities.state, map_cities.region,map_cities.updated_at as city_updated_at,
# MAGIC     map_cancellation_reasons.cancellation_reason
# MAGIC FROM
# MAGIC     uber_cat.silver.stream_rides_stg
# MAGIC     LEFT JOIN uber_cat.bronze.map_vehicle_makes map_vehicle_makes ON stream_rides_stg.vehicle_make_id = map_vehicle_makes.vehicle_make_id
# MAGIC     LEFT JOIN uber_cat.bronze.map_vehicle_types map_vehicle_types ON stream_rides_stg.vehicle_type_id = map_vehicle_types.vehicle_type_id
# MAGIC     LEFT JOIN uber_cat.bronze.map_ride_statuses map_ride_statuses ON stream_rides_stg.ride_status_id = map_ride_statuses.ride_status_id
# MAGIC     LEFT JOIN uber_cat.bronze.map_payment_methods map_payment_methods ON stream_rides_stg.payment_method_id = map_payment_methods.payment_method_id
# MAGIC     LEFT JOIN uber_cat.bronze.map_cities map_cities ON stream_rides_stg.pickup_city_id = map_cities.city_id
# MAGIC     LEFT JOIN uber_cat.bronze.map_cancellation_reasons map_cancellation_reasons ON stream_rides_stg.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
# MAGIC     

# COMMAND ----------

# MAGIC %sql select  distinct(pickup_city_id) as pickup_city_id , pickup_city ,city_updated_at,region,state from uber_cat.silver.silver_obt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_cat.gold.dim_location

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_cat.gold.fact

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fact.ride_id, fact.base_fare, dim.region
# MAGIC FROM uber_cat.gold.fact AS fact
# MAGIC LEFT JOIN uber_cat.gold.dim_location AS dim
# MAGIC   ON fact.pickup_city_id = dim.pickup_city_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fact.ride_id, fact.base_fare, dim.region
# MAGIC FROM uber_cat.gold.fact AS fact
# MAGIC LEFT JOIN uber_cat.gold.dim_location AS dim
# MAGIC   ON fact.pickup_city_id = dim.pickup_city_id
# MAGIC   AND dim.__END_AT IS NULL