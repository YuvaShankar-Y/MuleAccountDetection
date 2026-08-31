import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
import json

def calculate_velocity():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Required JAR for Kafka connection (ensure it's in the Flink lib or downloaded)
    # env.add_jars("file:///opt/flink/usrlib/flink-sql-connector-kafka-1.17.1.jar")

    # Kafka and mTLS Configuration
    kafka_brokers = "kafka:9093"
    topic = "banking-events"
    
    # Path to certs inside the Flink container
    keystore_path = "/opt/flink/certs/client.keystore.jks"
    truststore_path = "/opt/flink/certs/client.truststore.jks"
    cert_password = "changeit"

    # 1. Setup Kafka Source with strict mTLS
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_brokers) \
        .set_topics(topic) \
        .set_group_id("velocity-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_property("security.protocol", "SSL") \
        .set_property("ssl.keystore.location", keystore_path) \
        .set_property("ssl.keystore.password", cert_password) \
        .set_property("ssl.key.password", cert_password) \
        .set_property("ssl.truststore.location", truststore_path) \
        .set_property("ssl.truststore.password", cert_password) \
        .build()

    # 2. Ingest Data Stream
    stream = env.from_source(
        kafka_source, 
        WatermarkStrategy.no_watermarks(), 
        "Kafka Source"
    )

    # 3. Parse JSON and map to (source_account, 1)
    # Expected JSON schema: 
    # {"timestamp": 1690000000, "source_account": "ACC123", "target_account": "ACC456", "amount": 500, "currency": "USD", "device_id": "DEV99"}
    def parse_and_map(json_str):
        try:
            data = json.loads(json_str)
            return (data.get("source_account", "UNKNOWN"), 1)
        except Exception:
            return ("ERROR", 0)

    parsed_stream = stream.map(parse_and_map, output_type=Types.TUPLE([Types.STRING(), Types.INT()]))

    # 4. Apply Sliding Window: Count transactions per account in a 5-minute window, sliding every 1 minute
    # In production, use EventTime. Using ProcessingTime here for simplicity of demonstration.
    windowed_stream = parsed_stream \
        .key_by(lambda x: x[0]) \
        .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.minutes(1))) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    # 5. Filter for velocity metric: Flag if more than 10 transactions
    flagged_stream = windowed_stream.filter(lambda x: x[1] > 10)

    # 6. Sink output (Print to TaskManager logs for Phase 1)
    # In a real scenario, this would write to another Kafka topic or a graph DB (like Neo4j)
    flagged_stream.print()

    # Execute Job
    env.execute("Velocity Metric Calculation Job")

if __name__ == '__main__':
    calculate_velocity()
