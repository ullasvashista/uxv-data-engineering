# Databricks notebook source
import configparser
import random
import time
from datetime import datetime
import json
from azure.eventhub import EventHubProducerClient, EventData

# COMMAND ----------

# ------------------- CONFIG -------------------
# Create parser and read config file
config = configparser.ConfigParser()
config.read("../config/config.ini")

# Read values
# EventHub - Source
metadata_connection_str = config.get("SourceEventHub", "metadata_connection_str")
metadata_eventhub_name      = config.get("SourceEventHub", "metadata_eventhub_name")

# COMMAND ----------

# Function to generate mock ontology data
def generate_mock_data():
    return {
        'ontology': f"onto_{random.randint(1, 20)}",
        'filename': f"file_{random.randint(1, 20)}.owl",
        'size_in_kb': f"{random.randint(100, 1000)}"
    }

# Initialize Event Hub Producer
producer = EventHubProducerClient.from_connection_string(conn_str=metadata_connection_str, eventhub_name=metadata_eventhub_name)

# Send messages continuously
try:
    while True:
        for i in range (1, 21):
            data = {
                'ontology': f"onto_{i}",
                'filename': f"file_{random.randint(1, 20)}.owl",
                'size_in_kb': f"{random.randint(100, 1000)}"
                }
            print(f"Sending: {data}")
            
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(data)))
            producer.send_batch(event_data_batch)
        time.sleep(1*24*60*60)  # Wait 1 Day before sending next event

except KeyboardInterrupt:
    print("Stopped sending mock data.")

finally:
    producer.close()

# COMMAND ----------

