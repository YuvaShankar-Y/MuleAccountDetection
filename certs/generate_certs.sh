#!/bin/bash

# generate_certs.sh
# This script generates self-signed certificates and keystores/truststores 
# for strict mTLS communication between Kafka brokers and Flink clients.

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "=== Generating Certificates for Kafka and Flink ==="
echo "Cleaning up old certificates..."
rm -f *.crt *.key *.srl *.jks *.pem *.csr

PASSWORD="changeit"
VALIDITY=365

# 1. Generate a CA (Certificate Authority)
echo "1. Generating CA..."
openssl req -new -newkey rsa:4096 -days $VALIDITY -x509 -subj "/CN=Kafka-CA" \
    -keyout ca-key.pem -out ca-cert.pem -nodes

# Import CA to broker truststore
keytool -keystore broker.truststore.jks -alias CARoot -import -file ca-cert.pem \
    -storepass $PASSWORD -noprompt

# Import CA to client truststore
keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert.pem \
    -storepass $PASSWORD -noprompt

# 2. Generate Broker Certificate
echo "2. Generating Broker Keystore and Certificate..."
# Generate broker keystore
keytool -genkey -keystore broker.keystore.jks -alias localhost -validity $VALIDITY \
    -keyalg RSA -storepass $PASSWORD -keypass $PASSWORD \
    -dname "CN=kafka, OU=Broker, O=Org, L=City, ST=State, C=US" \
    -ext SAN=DNS:kafka,DNS:localhost

# Create a certificate signing request (CSR) for the broker
keytool -keystore broker.keystore.jks -alias localhost -certreq -file broker.csr \
    -storepass $PASSWORD -keypass $PASSWORD

# Sign the broker CSR with the CA
openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem -in broker.csr -out broker-cert-signed.pem \
    -days $VALIDITY -CAcreateserial

# Import the CA and signed cert into the broker keystore
keytool -keystore broker.keystore.jks -alias CARoot -import -file ca-cert.pem \
    -storepass $PASSWORD -noprompt
keytool -keystore broker.keystore.jks -alias localhost -import -file broker-cert-signed.pem \
    -storepass $PASSWORD -noprompt

# 3. Generate Client (Flink) Certificate
echo "3. Generating Client Keystore and Certificate..."
# Generate client keystore
keytool -genkey -keystore client.keystore.jks -alias client -validity $VALIDITY \
    -keyalg RSA -storepass $PASSWORD -keypass $PASSWORD \
    -dname "CN=flink-client, OU=Client, O=Org, L=City, ST=State, C=US"

# Create a CSR for the client
keytool -keystore client.keystore.jks -alias client -certreq -file client.csr \
    -storepass $PASSWORD -keypass $PASSWORD

# Sign the client CSR with the CA
openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem -in client.csr -out client-cert-signed.pem \
    -days $VALIDITY -CAcreateserial

# Import the CA and signed cert into the client keystore
keytool -keystore client.keystore.jks -alias CARoot -import -file ca-cert.pem \
    -storepass $PASSWORD -noprompt
keytool -keystore client.keystore.jks -alias client -import -file client-cert-signed.pem \
    -storepass $PASSWORD -noprompt

echo "=== Certificate Generation Complete ==="
echo "Generated files:"
ls -l *.jks
