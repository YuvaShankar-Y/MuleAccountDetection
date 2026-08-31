// Create constraints to ensure uniqueness and fast lookups
CREATE CONSTRAINT account_id_unique IF NOT EXISTS FOR (n:Account) REQUIRE n.account_id IS UNIQUE;
CREATE CONSTRAINT device_id_unique IF NOT EXISTS FOR (d:Device) REQUIRE d.device_id IS UNIQUE;

// Create indexes on properties that might be heavily filtered (optional but good practice)
CREATE INDEX account_status_index IF NOT EXISTS FOR (n:Account) ON (n.status);

// Enable Change Data Capture (CDC) tracking explicitly for these labels (Requires Neo4j 5.13+)
// The connector will listen to these changes.
