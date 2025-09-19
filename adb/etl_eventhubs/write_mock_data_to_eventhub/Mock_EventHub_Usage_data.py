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
usage_connection_str = config.get("SourceEventHub", "usage_connection_str")
usage_eventhub_name      = config.get("SourceEventHub", "usage_eventhub_name")

# COMMAND ----------

# Function to generate mock ontology data
def generate_mock_data():
    now = datetime.now()
    return {
        'start_date': now.strftime('%d/%b/%Y'),
        'start_time': now.strftime('%H:%M'),
        'id': f"np{random.randint(10000, 99999)}",
        'ontology': f"onto_{random.randint(1, 20)}",
        'activity': random.choice(['API Access', 'Login', 'Data Query', 'Download', 'Upload'])
    }

# Initialize Event Hub Producer
producer = EventHubProducerClient.from_connection_string(conn_str=usage_connection_str, eventhub_name=usage_eventhub_name)

# Send messages continuously
try:
    while True:
        data = generate_mock_data()
        print(f"Sending: {data}")
        
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(data)))
        
        producer.send_batch(event_data_batch)
        time.sleep(30*60)  # Wait 30 minutes before sending next event

except KeyboardInterrupt:
    print("Stopped sending mock data.")

finally:
    producer.close()

# COMMAND ----------

