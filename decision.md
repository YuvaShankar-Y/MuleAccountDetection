# Architectural Decisions

This document tracks all meaningful architectural and technical decisions made during the development of the high-velocity data ingestion pipeline (Phase 1).

## 1. Apache Kafka in KRaft Mode
**Decision:** We are deploying Apache Kafka using KRaft (Kafka Raft metadata) mode instead of Zookeeper.
**Reasoning:** KRaft is the modern consensus protocol for Kafka, removing the dependency on Zookeeper. This simplifies the infrastructure, reduces the operational burden of managing two distributed systems, and improves scalability by allowing Kafka brokers to manage metadata internally.

## 2. Apache Flink with PyFlink
**Decision:** We are using Apache Flink for stream processing and implementing the logic using PyFlink (Python API).
**Reasoning:** 
- Flink provides robust, exactly-once processing semantics and advanced state management (like sliding windows), which are essential for calculating precise real-time velocity metrics.
- Python was chosen over Java/Scala because the subsequent phases of the pipeline will involve Graph Neural Networks (GNNs) and Topological Data Analysis (TDA), which are heavily reliant on Python ecosystems (e.g., PyTorch Geometric, NetworkX). Using PyFlink allows for seamless ML integration natively in Python without complex language boundaries.

## 3. Mutual TLS (mTLS) for Security
**Decision:** Strict mTLS is enforced for communication between Kafka brokers and Flink workers.
**Reasoning:** Security is a primary mandate (zero-trust). By using mTLS, we ensure that:
- Data in transit is encrypted.
- Only authenticated and authorized clients (possessing the correct signed certificate) can connect to the Kafka cluster, preventing unauthorized data injection or extraction.

## 4. Containerized Local Development
**Decision:** The local infrastructure is managed via Docker Compose.
**Reasoning:** Provides a reproducible, isolated environment that mimics the production containerized deployment (e.g., Kubernetes). Certificates are generated locally and injected via Docker volume mounts.

## 5. Neo4j as the Graph Database (Phase 2)
**Decision:** Selected Neo4j over TigerGraph/NebulaGraph for the Graph Database.
**Reasoning:** Neo4j has massive adoption in the financial sector for forensics, robust Cypher pattern matching for AML rings, and most importantly, it provides a native Kafka Source Connector that perfectly fulfills our CDC requirements.

## 6. Neo4j Native CDC via Kafka Connect (Phase 2)
**Decision:** Used the official Neo4j Kafka Connect plugin instead of Debezium.
**Reasoning:** Debezium excels with relational databases but lacks an official Neo4j connector. Neo4j introduced native CDC in 5.13+, which integrates seamlessly with Kafka Connect to stream node/edge mutations. This fulfills the CDC architectural pattern natively.

## 7. Feast Feature Store with Redis (Phase 2)
**Decision:** Selected Feast using Redis as the online store and configured a stream FeatureView.
**Reasoning:** Feast supports `KafkaSource`, allowing it to consume the real-time `graph-mutations-log` directly and materialize features (e.g., node degree, transaction volume) into Redis for ultra-low-latency model serving.
