# Ontology Management Platform – Data Pipeline

## 📌 Overview
This project implements a streaming data pipeline on Azure Databricks for managing ontology metadata and usage activity.  
The pipeline ingests mock data into Azure Event Hub, processes it through Bronze → Trusted → Unified Delta layers, and validates the output in the final step.  

---

## 📂 Notebooks Summary

### 1. **Mock Metadata Generator**
- **File**: `adb/etl_eventhubs/write_mock_data_to_eventhub/Mock_EventHub_Metadata.py`
- **Purpose**: Simulates ontology metadata events and publishes them to **Azure Event Hub**.
- **Key Points**:
  - Generates ontology ID, random filename, and file size.
  - Sends **20 metadata events** once per day.
  - Helps test downstream ingestion without relying on real-time systems.

---

### 2. **Mock Usage Generator**
- **File**: `adb/etl_eventhubs/write_mock_data_to_eventhub/Mock_EventHub_Usage_data.py`
- **Purpose**: Simulates **user activity logs** and publishes them to **Azure Event Hub**.
- **Key Points**:
  - Generates login, API access, query, upload, or download activities.
  - Includes event timestamp and random user/session ID.
  - Sends **1 usage event every 30 minutes**.

---

### 3. **Metadata Consumer & Loader**
- **File**: `adb/etl_eventhubs/read_data_etl_process/ETL_EventHub_omp_Metadata.py`
- **Purpose**: Reads metadata events from Event Hub and stores them in **Delta Lake**.
- **Steps**:
  1. Listens to Event Hub for a fixed duration (10 sec).
  2. Buffers JSON messages and converts them into a DataFrame.
  3. Adds metadata fields (`LogId`, `LogDate`, `LogGuid`, etc.).
  4. Writes into **Delta Table** (`metadata_final_table`).
  5. Uses **MERGE** for upserts (update if ontology exists, insert otherwise).

---

### 4. **Usage – Bronze Layer Loader**
- **File**: `adb/etl_eventhubs/read_data_etl_process/ETL_EventHub_omp_Usage_RAW.py`
- **Purpose**: Streams raw usage activity from Event Hub to **Bronze Delta layer**.
- **Steps**:
  1. Reads from Event Hub continuously using Spark Structured Streaming.
  2. Parses raw JSON into structured schema (date, time, id, ontology, activity).
  3. Adds metadata (`TimeGenerated`, `input_file_path`).
  4. Writes streaming data to **Delta (Bronze table)** with checkpointing for reliability.

---

### 5. **Usage – Trusted Layer Transformer**
- **File**: `adb/etl_eventhubs/read_data_etl_process/ETL_EventHub_omp_Usage_TRUSTED.py`
- **Purpose**: Cleans and enriches Bronze data → loads into **Trusted (Silver) Delta layer**.
- **Steps**:
  1. Reads from Bronze Delta table as streaming input.
  2. Adds consistent metadata fields (`LogType`, `LogDate`, `LogGuid`, `TenantId`).
  3. Writes transformed events to **Trusted Delta table** in append mode.
  4. Uses checkpoints to ensure fault-tolerant streaming.

---

### 6. **Usage – Unified Layer Enrichment**
- **File**: `adb/etl_eventhubs/read_data_etl_process/ETL_EventHub_omp_Usage_UNIFIED.py`
- **Purpose**: Final processing to combine **usage logs** with **metadata**.
- **Steps**:
  1. Reads Trusted usage data and Metadata final table (batch mode).
  2. Adds unique identifiers (`LogId`, `LogGuid`).
  3. Prepares join key (`ontology + activity`) in usage and (`ontology + Download`) in metadata.
  4. Performs **broadcast join** to enrich usage events with metadata attributes (`filename`, `size_in_kb`).
  5. Writes final **Unified Delta table** (`usage_final_table`).

---

### 7. **Stream Validation**
- **File**: `Stream Validation.ipynb`
- **Purpose**: Final validation of data pipeline output.
- **Details**:
  - Reads **all final Delta tables** (metadata, trusted usage, unified usage).
  - Performs validation checks on schema, counts, sample data.
  - Ensures that the streaming pipeline produces expected results.

---

## 📊 Data Flow

(Mock Generators)
│
├── Metadata Generator → Event Hub → Metadata Consumer Loader → Delta (Metadata Final Table)
│
└── Usage Generator → Event Hub → Bronze Delta → Trusted Delta → Unified Delta


- **Metadata Final Table**: Contains ontology file details (ID, name, size).
- **Usage Final Table (Unified)**: Contains user activity logs enriched with ontology metadata.
- **Stream Validation**: Confirms correctness and completeness of the final output.

---

## ⚙️ Technologies Used
- **Azure Databricks (PySpark, Delta Lake)**
- **Azure Event Hub**
- **Azure Data Lake Storage (ADLS)**
- **Azure Key Vault (for secrets management)**
- **Python / Pandas for intermediate handling**

---

## 🚀 Execution Order
To run the pipeline end-to-end:
1. Run **Mock_EventHub_Metadata.pyr** and **Mock_EventHub_Usage_data.py** to push events into Event Hub.  
2. Run **ETL_EventHub_omp_Metadata.py** to capture metadata into Delta.  
3. Run **ETL_EventHub_omp_Usage_RAW.py** to continuously capture usage logs.  
4. Run **ETL_EventHub_omp_Usage_TRUSTED.py** to clean and enrich Bronze usage logs.  
5. Run **ETL_EventHub_omp_Usage_UNIFIED.py** to join usage logs with metadata.  
6. Finally, run **Stream Validation.ipynb** to validate the outputs.  

---

## ✅ Final Output
- **Metadata Final Table** → Contains latest ontology file information.  
- **Usage Trusted Table** → Cleansed, standardized usage logs.  
- **Usage Final (Unified) Table** → Usage logs enriched with ontology metadata, ready for analytics.  
- **Stream Validation** → Confirms end-to-end pipeline integrity.  

---
