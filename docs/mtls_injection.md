# mTLS Certificate Injection Strategy

Ensuring strict mTLS communication requires both the server (Kafka) and client (Flink) to have access to valid keystores and truststores. In Phase 1, we use Docker Compose for local orchestration, which dictates our initial injection strategy.

## Phase 1: Local Docker Compose Strategy
Locally, certificates are injected into the containers using **Host Volume Mounts**.

1. **Generation:** The `certs/generate_certs.sh` script executes on the host machine to generate the necessary `.jks` (Java KeyStore) and `.pem` files.
2. **Mounting:** In `docker-compose.yml`, the local `./certs` directory is mounted into the containers as read-only (`ro`):
   - **Kafka Broker:** `- ./certs:/etc/kafka/secrets:ro`
   - **Flink JobManager/TaskManager:** `- ./certs:/opt/flink/certs:ro`
3. **Environment Injection:** The Kafka broker consumes these paths via environment variables (e.g., `KAFKA_SSL_KEYSTORE_LOCATION`). Flink consumes them directly within the PyFlink script.

## Production Blueprint: Secure Secret Injection
For production deployment (e.g., Kubernetes), host mounts are insecure and anti-pattern. We recommend transitioning to a robust secret management system:

### 1. HashiCorp Vault Integration
Instead of manual generation, use HashiCorp Vault's PKI engine.
- A Vault sidecar or init-container authenticates (e.g., via Kubernetes Service Accounts) and requests short-lived, dynamically generated certificates.
- These certificates are placed into a shared, in-memory volume (e.g., `tmpfs` or `emptyDir` in K8s) accessible only to the Kafka or Flink process.
- **Advantage:** Automatic rotation of certificates without manual intervention and secure ephemeral storage.

### 2. Kubernetes Secrets
If Vault is too complex for initial production:
- Generate certificates in a secure CI/CD pipeline.
- Store them as native Kubernetes `Secret` resources.
- Mount the Secrets as files into the pods or inject them as environment variables (though file mounts are required for JKS files).
- **Advantage:** Native to K8s, easier setup, but requires manual handling of rotation and strong RBAC over who can read the Secrets.

### Summary
The current host-mount approach is strictly for rapid prototyping and local validation of the mTLS handshakes. Transitioning to dynamic secret injection (like Vault) is mandatory before handling real banking data.
