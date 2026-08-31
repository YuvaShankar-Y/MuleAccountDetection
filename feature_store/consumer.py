import json
import time
from kafka import KafkaConsumer
from feast import FeatureStore
import pandas as pd
from datetime import datetime

print("Starting Feast CDC Consumer...")

# Wait for Feast registry and Redis to be ready
time.sleep(5)

store = FeatureStore(repo_path=".")

# Configure the Kafka consumer
consumer = KafkaConsumer(
    'graph-mutations-log',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='feast-cdc-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for Neo4j CDC messages on 'graph-mutations-log'...")
for message in consumer:
    event = message.value
    print(f"Received CDC event: {event}")
    
    try:
        # Parse official Neo4j 5.x CDC payload structure:
        # {
        #   "payload": {
        #     "op": "create", // or "update"
        #     "type": "node",
        #     "after": {
        #       "labels": ["Account"],
        #       "properties": {
        #         "account_id": "ACC123",
        #         "transaction_volume_30d": 15000.0,
        #         "node_degree": 5
        #       }
        #     }
        #   }
        # }
        payload = event.get("payload", {})
        after = payload.get("after", {})
        
        # If 'after' is null, check for direct event or state formatting
        if not after and "event" in event:
            after = event.get("event", {}).get("state", {}).get("after", {})
            
        labels = after.get("labels", [])
        
        # We only care about Account node mutations for these features
        if "Account" in labels:
            properties = after.get("properties", {}) if "properties" in after else after
            account_id = properties.get("account_id")
            
            if account_id:
                feature_data = {
                    "account_id": [str(account_id)],
                    "transaction_volume_30d": [float(properties.get("transaction_volume_30d", 0.0))],
                    "node_degree": [int(properties.get("node_degree", 0))],
                    "event_timestamp": [datetime.utcnow()]
                }
                
                df = pd.DataFrame(feature_data)
                print(f"Pushing Account features to Feast: {feature_data}")
                store.push("graph_mutations_push_source", df)
                
    except Exception as e:
        print(f"Error parsing/pushing CDC event: {e}")
