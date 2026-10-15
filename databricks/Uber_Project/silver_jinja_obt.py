# Databricks notebook source
# MAGIC %md
# MAGIC # Jinja Template for OBT

# COMMAND ----------

# MAGIC %pip install jinja2

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

jinja_config = [
    {
        "table"  : "uber_cat.silver.stream_rides_stg",
        "alias"  : "stream_rides_stg",
        "select" : "stream_rides_stg.ride_id, stream_rides_stg.confirmation_number, stream_rides_stg.passenger_id, stream_rides_stg.driver_id, stream_rides_stg.vehicle_id, stream_rides_stg.pickup_location_id, stream_rides_stg.dropoff_location_id, stream_rides_stg.vehicle_type_id, stream_rides_stg.vehicle_make_id, stream_rides_stg.payment_method_id, stream_rides_stg.ride_status_id, stream_rides_stg.pickup_city_id, stream_rides_stg.dropoff_city_id, stream_rides_stg.cancellation_reason_id, stream_rides_stg.passenger_name, stream_rides_stg.passenger_email, stream_rides_stg.passenger_phone, stream_rides_stg.driver_name, stream_rides_stg.driver_rating, stream_rides_stg.driver_phone, stream_rides_stg.driver_license, stream_rides_stg.vehicle_model, stream_rides_stg.vehicle_color, stream_rides_stg.license_plate, stream_rides_stg.pickup_address, stream_rides_stg.pickup_latitude, stream_rides_stg.pickup_longitude, stream_rides_stg.dropoff_address, stream_rides_stg.dropoff_latitude, stream_rides_stg.dropoff_longitude, stream_rides_stg.distance_miles, stream_rides_stg.duration_minutes, stream_rides_stg.booking_timestamp, stream_rides_stg.pickup_timestamp, stream_rides_stg.dropoff_timestamp, stream_rides_stg.base_fare, stream_rides_stg.distance_fare, stream_rides_stg.time_fare, stream_rides_stg.surge_multiplier, stream_rides_stg.subtotal, stream_rides_stg.tip_amount, stream_rides_stg.total_fare, stream_rides_stg.rating",
        "where"  : ""
    },
    {
        "table"  : "uber_cat.bronze.map_vehicle_makes",
        "alias"  : "map_vehicle_makes",
        "select" : "map_vehicle_makes.vehicle_make",
        "where"  : "",
        "on"     : "stream_rides_stg.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table"  : "uber_cat.bronze.map_vehicle_types",
        "alias"  : "map_vehicle_types",
        "select" : "map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute",
        "where"  : "",
        "on"     : "stream_rides_stg.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    },
    {
        "table"  : "uber_cat.bronze.map_ride_statuses",
        "alias"  : "map_ride_statuses",
        "select" : "map_ride_statuses.ride_status",
        "where"  : "",
        "on"     : "stream_rides_stg.ride_status_id = map_ride_statuses.ride_status_id"
    },
    {
        "table"  : "uber_cat.bronze.map_payment_methods",
        "alias"  : "map_payment_methods",
        "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
        "where"  : "",
        "on"     : "stream_rides_stg.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table"  : "uber_cat.bronze.map_cities",
        "alias"  : "map_cities",
        "select" : "map_cities.city AS pickup_city, map_cities.state, map_cities.region",
        "where"  : "",
        "on"     : "stream_rides_stg.pickup_city_id = map_cities.city_id"
    },
    {
        "table"  : "uber_cat.bronze.map_cancellation_reasons",
        "alias"  : "map_cancellation_reasons",
        "select" : "map_cancellation_reasons.cancellation_reason",
        "where"  : "",
        "on"     : "stream_rides_stg.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Template

# COMMAND ----------

from jinja2 import Template

jinja_str = """
SELECT
    {{ jinja_config | map(attribute='select') | join(',\n    ') }}
FROM
{%- for config in jinja_config %}
    {% if loop.first -%}
    {{ config.table }} {{ config.alias }}
    {%- else -%}
    LEFT JOIN {{ config.table }} {{ config.alias }} ON {{ config.on }}
    {%- endif %}
{%- endfor %}
{%- set wheres = jinja_config | selectattr('where') | map(attribute='where') | list %}
{%- if wheres %}
WHERE {{ wheres | join(' AND ') }}
{%- endif %}
"""

template = Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config)
print(rendered_template)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

display(spark.sql(rendered_template))

# COMMAND ----------

# MAGIC %sql select  current_timestamp()