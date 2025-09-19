# Databricks notebook source
# MAGIC %md
# MAGIC * Project: Ontology Management Platform
# MAGIC * Author: Ullas Vashista
# MAGIC * Last Update: 05/01/2024

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import re
import configparser
import time
from delta.tables import DeltaTable
import os
import asyncio
from azure.eventhub.aio import EventHubConsumerClient
import pandas as pd
import json

# COMMAND ----------

# ------------------- CONFIG -------------------
# Create parser and read config file
config = configparser.ConfigParser()
config.read("../config/config.ini")

# Read values
# EventHub - Source
metadata_connection_str = config.get("SourceEventHub", "metadata_connection_str")
metadata_consumer_group = config.get("SourceEventHub", "metadata_consumer_group")
metadata_eventhub_name      = config.get("SourceEventHub", "metadata_eventhub_name")

# Storage - Target
logm_storage_account_name = config.get("TargetStorage", "account_name")
logm_container_name       = config.get("TargetStorage", "container_name")
logm_mount_name           = config.get("TargetStorage", "mount_name")

#Key Vault Scope Name
KeyVaultScope = config.get("KeyVaultScope", "scope_name")
tenant_id = config.get("KeyVaultScope", "tenant_id") # This retrieves a secret value (e.g., from a key vault). It won't display the actual value when printed (shows [REDACTED]) but works correctly when used in code. For testing or display purposes, use: tenant_id = "<actual_tenant_id>"

# Define path to your Delta table
metadata_final_table           = config.get("TargetStorage", "metadata_final_table")

# COMMAND ----------

# Secrets
client_id     = dbutils.secrets.get(scope=KeyVaultScope, key="adls-client-id")
client_secret = dbutils.secrets.get(scope=KeyVaultScope, key="adls-client-secret")

# COMMAND ----------

# Spark Configuration for ADLS Mounting
def configure_spark_for_adls_oauth(storage_account_name, client_id, client_secret, tenant_id):
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", 
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", 
                   f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# Message buffer
messages = []

# COMMAND ----------

# Event handler
async def on_event(partition_context, event):
    try:
        if event is None:
            print("Received None as event.")
            return

        body_str = event.body_as_str()
        #print("Received raw body:", body_str)

        try:
            body = json.loads(body_str)
            messages.append(body)
        except json.JSONDecodeError:
            print(f"Skipping invalid JSON: {body_str}")

        await partition_context.update_checkpoint(event)

    except Exception as e:
        print(f"Error processing message: {e}")

# COMMAND ----------

# Receiver
async def main():
    client = EventHubConsumerClient.from_connection_string(
        conn_str=metadata_connection_str,
        consumer_group=metadata_consumer_group
    )

    async with client:
        print("Listening for events...")
        await client.receive(
            on_event=on_event,
            starting_position="-1"
        )

# COMMAND ----------

# ✅ Force the listener to stop after 10 seconds
try:
    await asyncio.wait_for(main(), timeout=10)
except asyncio.TimeoutError:
    print("⏰ Done listening. Moving to DataFrame creation.")

# Convert to Pandas DataFrame
df = pd.DataFrame(messages)

# COMMAND ----------

def transform_load():
    raw_df = spark.createDataFrame(df)

    # Add source_dir
    raw_df = raw_df.withColumn("TimeGenerated", current_timestamp())
    cur_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    raw_df = raw_df.withColumn("input_file_path", lit(f"{metadata_eventhub_name}-{cur_time}"))

    #Add Metadata Columns
    final_df = (
        raw_df
        .withColumn("body", to_json(struct(*raw_df.columns)))
        .withColumn("MetadataLogId", sha2(col("body"), 512).cast("string"))
        .drop("body")
        .withColumn("MetadataLogType", lit("logm.omp.metadata"))
        .withColumn("MetadataLogTimeGenerated", col("TimeGenerated").cast(TimestampType()))
        .withColumn("MetadataLogDate", date_format(col("TimeGenerated"), "yyyyMMddHHmmssSSS").cast(LongType()))
        .drop("TimeGenerated")
        .withColumn("TimeGenerated", current_timestamp().cast(StringType()))
        .withColumn("MetadataLogWindow", date_format(from_utc_timestamp(col("MetadataLogTimeGenerated"), "UTC"), "yyMMddHHmm").cast(LongType()))
        .withColumn("MetadataLogGuid", concat(col("MetadataLogWindow"), lpad(monotonically_increasing_id(), 9, "0")).cast(LongType()))
        .drop("MetadataLogWindow")
        .withColumn("MetadataLogFileName", col("input_file_path"))
        .drop("input_file_path")
        .withColumn("TenantId", lit(tenant_id))
        .withColumn("Type", lit("LOGM_OMP_MEATADATA_CL"))
        )
    
    # Write to Delta
    configure_spark_for_adls_oauth(logm_storage_account_name, client_id, client_secret, tenant_id)

    # Check if Delta table exists
    if not DeltaTable.isDeltaTable(spark, metadata_final_table):
        # Table doesn't exist – write initial data
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(metadata_final_table)

    else:
        # Table exists – perform MERGE
        delta_table = DeltaTable.forPath(spark, metadata_final_table)

        delta_table.alias("target").merge(
            source=final_df.alias("source"),
            condition="target.ontology = source.ontology"
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # ------------------- MAIN -------------------

# COMMAND ----------

try:
    transform_load()
    print("New Data Loaded")
except Exception as e:
    print("No new data from EventHub")