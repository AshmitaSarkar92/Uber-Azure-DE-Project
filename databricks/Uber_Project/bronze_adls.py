# Databricks notebook source
# MAGIC %md
# MAGIC # Creating the tables for the raw- bulk and map from raw adls to adb records

# COMMAND ----------

BASE_PATH = "abfss://raw@uberprojectdatalake.dfs.core.windows.net/ingestion"

files = [
    {"file": "map_cities"},
    {"file": "map_cancellation_reasons"},
    {"file": "map_payment_methods"},
    {"file": "map_ride_statuses"},
    {"file": "map_vehicle_makes"},
    {"file": "map_vehicle_types"}
]

# ---- Helper: read JSON and detect array vs JSON Lines ----
def read_json(path):
    first_char = dbutils.fs.head(path, 100).lstrip()[:1]
    is_multiline = "true" if first_char == "[" else "false"
    return spark.read.format("json")\
            .option("multiLine", is_multiline)\
            .load(path)


# ---- Mapping files: full refresh every run ----
for file in files:

    path = f"{BASE_PATH}/{file['file']}.json"
    df = read_json(path)

    # Writing Data To the Bronze Layer
    df.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(f"uber_cat.bronze.{file['file']}")

    print(f"Loaded: uber_cat.bronze.{file['file']}")


# ---- Bulk rides: one-time historical load ----
if not spark.catalog.tableExists("uber_cat.bronze.bulk_rides"):

    df = read_json(f"{BASE_PATH}/bulk_rides.json")

    df.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("uber_cat.bronze.bulk_rides")

    print("Loaded: uber_cat.bronze.bulk_rides (this will not run more than 1 time)")
else:
    print("Skipped: uber_cat.bronze.bulk_rides already exists")

# COMMAND ----------

# MAGIC %sql SHOW TABLES IN uber_cat.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name
# MAGIC FROM uber_cat.information_schema.columns
# MAGIC WHERE table_schema = 'bronze' AND column_name = '_corrupt_record';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_cat.bronze.rides_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_cat.bronze.map_cities