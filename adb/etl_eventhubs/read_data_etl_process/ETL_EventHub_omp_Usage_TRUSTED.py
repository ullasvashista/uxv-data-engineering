# Databricks notebook source
# MAGIC %md
# MAGIC * Project: Ontology Management Platform
# MAGIC * Notebook: Read Stearming Data from EventHub & Bronze layer data to Trusted Layer with major Transformation
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
usage_raw_table           = config.get("TargetStorage", "usage_raw_table")

usage_trusted_table           = config.get("TargetStorage", "usage_trusted_table")
usage_trusted_checkpoint           = config.get("TargetStorage", "usage_trusted_checkpoint")

# COMMAND ----------

#Read streaming data from the Bronze Delta table
bronze_df = (
    spark.readStream
    .format("delta")
    .load(usage_raw_table)
)

#Apply transformations
transformed_df = (
    bronze_df
    .withColumn("MetadataLogType", lit("logm.omp.metadata"))
    .withColumn("MetadataLogTimeGenerated", col("TimeGenerated").cast(TimestampType()))
    .withColumn("MetadataLogDate", date_format(col("TimeGenerated"), "yyyyMMddHHmmssSSS").cast(LongType()))
    .drop("TimeGenerated")
    .withColumn("TimeGenerated", current_timestamp().cast(StringType()))
    .withColumn("MetadataLogFileName", col("input_file_path"))
    .drop("input_file_path")
    .withColumn("TenantId", lit(tenant_id))
    .withColumn("Type", lit("LOGM_OMP_MEATADATA_CL"))
)

#Write the transformed stream to the Silver Delta table
silver_query = (
    transformed_df.writeStream
    .format("delta")
    .outputMode("append")  # Typically 'append' for streaming
    .option("checkpointLocation", usage_trusted_checkpoint)
    .start(usage_trusted_table)
)

# Optional: Wait for termination
silver_query.awaitTermination()

# COMMAND ----------



# COMMAND ----------

