# Databricks notebook source
# MAGIC %md
# MAGIC * Project: Ontology Management Platform
# MAGIC * Notebook: Read Stearming Data from EventHub & load raw data to Bronze layer
# MAGIC * Author: Ullas Vashista
# MAGIC * Last Update: 05/01/2024

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import re
import configparser

# COMMAND ----------

# ------------------- CONFIG -------------------
# Create parser and read config file
config = configparser.ConfigParser()
config.read("../config/config.ini")

# Read values
# EventHub - Source
usage_connection_str = config.get("SourceEventHub", "usage_connection_str")
usage_consumer_group = config.get("SourceEventHub", "usage_consumer_group")
usage_eventhub_name      = config.get("SourceEventHub", "usage_eventhub_name")

# Storage - Target
logm_storage_account_name = config.get("TargetStorage", "account_name")
logm_container_name       = config.get("TargetStorage", "container_name")
logm_mount_name           = config.get("TargetStorage", "mount_name")

#Key Vault Scope Name
KeyVaultScope = config.get("KeyVaultScope", "scope_name")
tenant_id = config.get("KeyVaultScope", "tenant_id") # This retrieves a secret value (e.g., from a key vault). It won't display the actual value when printed (shows [REDACTED]) but works correctly when used in code. For testing or display purposes, use: tenant_id = "<actual_tenant_id>"

# Define path to your Delta table & checkpoint
usage_raw_table           = config.get("TargetStorage", "usage_raw_table")
usage_raw_checkpoint           = config.get("TargetStorage", "usage_raw_checkpoint")

# COMMAND ----------

event_hub_conf = {
    'eventhubs.connectionString': sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(usage_connection_str),
    'eventhubs.consumerGroup': usage_consumer_group
}

# COMMAND ----------

# MAGIC %md
# MAGIC # ------------------- MAIN -------------------

# COMMAND ----------

# Define schema
schema = StructType() \
    .add("start_date", StringType()) \
    .add("start_time", StringType()) \
    .add("id", StringType()) \
    .add("ontology", StringType()) \
    .add("activity", StringType())

# Generate timestamp for metadata
cur_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Read stream from Event Hub
event_hub_stream_df = (
    spark.readStream
    .format("eventhubs")
    .options(**event_hub_conf)
    .load()
)

# Parse the JSON payload
json_df = (
    event_hub_stream_df
    .selectExpr("cast(body as string) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("TimeGenerated", current_timestamp())
    .withColumn("input_file_path", lit(f"{usage_eventhub_name}-{cur_time}"))
)

# Write to Delta table
query = (
    json_df.writeStream
    .format("delta")
    .outputMode("append")  # or "update"/"complete" depending on logic
    .option("checkpointLocation", usage_raw_checkpoint)  # Required for fault tolerance
    .start(usage_raw_table)  # Path to Delta table location
)

# Optional: Await termination if running in a script
query.awaitTermination()