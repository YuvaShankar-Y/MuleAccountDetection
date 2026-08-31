from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Float32, Int64, String
from feast.data_format import JsonFormat
from feast.infra.offline_stores.file_source import FileSource
from feast.data_source import KafkaSource

# Define an entity for the account
account_entity = Entity(
    name="account_id",
    description="The ID of the bank account",
    value_type=String
)

# Define the Kafka source that reads from the Debezium/Neo4j CDC topic
# Assuming the CDC JSON payload has 'account_id' and features
graph_mutations_source = KafkaSource(
    name="graph_mutations_source",
    kafka_bootstrap_servers="kafka:29092",
    topic="graph-mutations-log",
    message_format=JsonFormat(
        schema_json="""{
            "type": "record",
            "name": "GraphMutation",
            "fields": [
                {"name": "account_id", "type": "string"},
                {"name": "transaction_volume_30d", "type": "float"},
                {"name": "node_degree", "type": "int"},
                {"name": "event_timestamp", "type": "string"}
            ]
        }"""
    ),
    watermark_delay_threshold=timedelta(minutes=1),
    timestamp_field="event_timestamp",
)

# Define the Feature View that materializes the stream data
@stream_feature_view(
    entities=[account_entity],
    ttl=timedelta(days=30),
    mode="spark",
    schema=[
        Field(name="transaction_volume_30d", dtype=Float32),
        Field(name="node_degree", dtype=Int64),
    ],
    source=graph_mutations_source,
    online=True,
    description="Real-time graph features for AML GNN models",
)
def account_graph_features(df):
    return df
