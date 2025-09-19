# Databricks notebook source
# MAGIC %md
# MAGIC * Project: Ontology Management Platform
# MAGIC * Notebook: Read Data from Trusted layer, apply minor transformation, join with metadata and load final data to Unified Layer.
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
# Storage - Target
logm_storage_account_name = config.get("TargetStorage", "account_name")
logm_container_name       = config.get("TargetStorage", "container_name")
logm_mount_name           = config.get("TargetStorage", "mount_name")

#Key Vault Scope Name
KeyVaultScope = config.get("KeyVaultScope", "scope_name")
tenant_id = config.get("KeyVaultScope", "tenant_id") # This retrieves a secret value (e.g., from a key vault). It won't display the actual value when printed (shows [REDACTED]) but works correctly when used in code. For testing or display purposes, use: tenant_id = "<actual_tenant_id>"

# Define path to your Delta table & checkpoint
usage_trusted_table           = config.get("TargetStorage", "usage_trusted_table")

metadata_final_table           = config.get("TargetStorage", "metadata_final_table")
usage_final_table           = config.get("TargetStorage", "usage_final_table")

# COMMAND ----------

# Read Delta Trusted table in batch mode
gold_usage_df = spark.read.format("delta").load(usage_trusted_table)
gold_metadata_df = spark.read.format("delta").load(metadata_final_table)

# COMMAND ----------

#add final transfomation to Usage
gold_usage_df = (
    gold_usage_df
    .withColumn("body", to_json(struct(*gold_usage_df.columns)))
    .withColumn("MetadataLogId", sha2(col("body"), 512).cast("string"))
    .drop("body")
    .withColumn("MetadataLogWindow", date_format(from_utc_timestamp(col("MetadataLogTimeGenerated"), "UTC"), "yyMMddHHmm").cast(LongType()))
    .withColumn("MetadataLogGuid", concat(col("MetadataLogWindow"), lpad(monotonically_increasing_id(), 9, "0")).cast(LongType()))
    .drop("MetadataLogWindow")
)

# COMMAND ----------

# joining metadata to usage data
#Select required columns from metadata and create JKey
gold_metadata_trimmed = (
    gold_metadata_df
    .select("ontology", "filename", "size_in_kb")
    .withColumn("JKey", concat(col("ontology"), lit("Download")))
)

# Create JKey in usage table
gold_usage_with_key = (
    gold_usage_df
    .withColumn("JKey", concat(col("ontology"), col("activity")))
)

# Broadcast join on JKey
final_df = (
    gold_usage_with_key
    .join(broadcast(gold_metadata_trimmed), on="JKey", how="left")
    .drop(gold_metadata_trimmed["ontology"])
    .drop("JKey")
)

# COMMAND ----------

#write final data into Unified layer
final_df.write.format("delta").mode("overwrite").save(usage_final_table)