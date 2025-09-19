📄 Denodo Query Logs Ingestion Pipeline (Azure Databricks)
 
This repository contains a Spark-based ETL pipeline written in PySpark that reads Denodo query logs stored in Azure Data Lake Storage (ADLS), processes them, and writes the results into a Delta Lake table. It also maintains checkpointing to ensure only new records are processed in subsequent runs.
 
💼 Project Overview
 
This ETL pipeline processes Denodo query logs from two sources (AVDP1, AVDP2) stored in JSON format. The logs are split and parsed into structured fields, enriched with metadata, and persisted into a Delta table in a target ADLS container.
 
⚙️ Requirements
 
Before running the notebook or script, ensure you have the following:
 
✅ Databricks Runtime
 
Databricks Runtime (DBR) with Apache Spark 3.x
 
Delta Lake enabled
 
✅ Python Libraries
 
pyspark
 
configparser
 
re (regular expressions)
 
✅ Azure Resources
 
Azure Data Lake Storage Gen2 (source & target)
 
Azure Active Directory (AAD) App Registration with client credentials
 
✅ Secrets Configuration
 
Store the following secrets in Databricks Secrets scope (codvmtlogmus6kvdevtst01):
 
adls-client-id
 
adls-client-secret
 
tenant-id
 
🔐 Secrets & Configuration
 
Secrets are used to securely access ADLS using OAuth2 authentication:
 
client_id     = dbutils.secrets.get(scope="uxvus6kvdevtst01", key="adls-client-id")
client_secret = dbutils.secrets.get(scope="uxvus6kvdevtst01", key="adls-client-secret")
tenant_id     = dbutils.secrets.get(scope="uxvus6kvdevtst01", key="tenant-id")
 
 
The storage details are managed in a config file:
 
📄 config/config.ini:
 
[SourceStorage]
account_name = <source_storage_account>
container_name = <source_container>
mount_name = <source_directory>
 
[TargetStorage]
account_name = <target_storage_account>
container_name = <target_container>
mount_name = <target_directory>
 
🛠️ Pipeline Workflow
1. 🔐 Authentication & Configuration
 
Set up Spark session to authenticate with Azure Data Lake using OAuth2 client credentials.
 
configure_spark_for_adls_oauth(...)
 
2. 📌 Checkpoint Management
 
Load the latest checkpoint from Delta table to determine the last processed date and timestamp.
 
file_date, last_timegenerated = get_checkpoint()
 
 
Fallback to a manual start date (09102025) if no checkpoint is found.
 
3. 📂 File Discovery
 
Based on the checkpoint or default date, build a list of expected log files from AVDP1 and AVDP2 directories for each date in range.
 
paths_to_read = list_latest_file_paths(...)
 
4. 📥 Log Reading
 
Read the discovered log files (in JSON format). If no data is available, exit the pipeline.
 
raw_df = spark.read.option("mode", "PERMISSIVE").json(paths_to_read)
 
5. 🧪 Data Transformation
 
Extract the directory name from the file path (AVDP1, AVDP2)
 
Cast @timestamp to Spark Timestamp
 
Filter records where TimeGenerated > last_timegenerated
 
raw_df = raw_df.withColumn("TimeGenerated", col("@timestamp").cast(TimestampType()))
 
6. 🧬 Metadata Enrichment
 
Split the log column by tab (\t) into 24+ fields.
 
Enrich with metadata like hash, GUIDs, file paths, and log type.
 
Add tenant ID, log type, and system timestamp.
 
final_df = final_df.withColumn("MetadataLogId", sha2(...))
 
7. 💾 Write to Delta Lake
 
Persist the transformed and enriched data into a Delta Lake table at the target path:
 
final_df.write.format("delta").mode("append").save(delta_table_path)
 
8. ✅ Checkpoint Update
 
Extract:
 
Latest MetadataLogTimeGenerated
 
Maximum date from input file names
 
Then update the checkpoint Delta table:
 
update_checkpoint(file_date, last_timegenerated)
 
📁 Directory Structure
/
├── config/
│   └── config.ini          # Configuration for storage accounts and directories
├── notebook.py             # Main ETL logic
└── README.md               # You're here!
 
📤 Output Schema
 
The final schema contains both original log data and enriched metadata:
 
Column Name	Description
ServerName	Denodo server name
Host	Hostname of the request
Port	Connection port
Id_, UserName, Database	Request identification fields
StartTime, EndTime	Query timing info
Duration, WaitingTime	Performance stats
Query	Actual SQL query
MetadataLogId	SHA-512 hash of the log body
MetadataLogType	Fixed value: logm.denodo.queries
MetadataLogTimeGenerated	Timestamp from log (@timestamp)
MetadataLogDate	Numeric timestamp for tracking
MetadataLogGuid	Unique identifier for log row
MetadataLogFileName	Full input path of log file
TenantId	Azure Tenant ID
Type	Fixed: LOGM_DENODO_QUERIES_CL
TimeGenerated	Pipeline ingestion time
📎 Notes
 
Empty file handling: If no logs or no new records are found, the notebook exits with status "EMPTY".
 
File Naming: Logs follow this format:
 
dev_azure_vdp1-queries.log.MMDDYYYY
 
 
Daily Append: This notebook is meant to be scheduled daily or hourly, depending on log generation.
 
🧠 Best Practices
 
Use job clusters with auto-termination.
 
Secure all credentials using secret scopes.
 
Monitor Delta table growth and optimize (ZORDER, VACUUM, etc.)

