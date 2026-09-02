# Mule Account Detection Platform

A real-time, explainable AI platform designed to detect and visualize complex money laundering networks and "Mule Accounts" using Graph Analytics, GNNs, XGBoost, and SHAP. Built for Smart India Hackathon (SIH).

##  How to Run the Application

### Prerequisites
- Python 3.8+
- Neo4j Database (Desktop or Docker instance running on port 7687)
- Apache Kafka (Optional for demo, fallback mock producer is included)

### 1. Install Dependencies
```bash
pip install fastapi uvicorn neo4j kafka-python shap numpy
```

### 2. Configure Neo4j
Ensure you have a Neo4j database running. Update the default connection string in your environment if needed (the default is usually `bolt://localhost:7687`, `neo4j`, `password`):
```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password"
```

### 3. Seed the Database
Populate the Neo4j database with our realistic test presentation data. This script generates the hidden criminal structures, circular money flows, and normal background transactions:
```bash
python3 graph_db/seed_presentation_data.py
```

### 4. Start the Backend Server
Launch the FastAPI application:
```bash
uvicorn api.feedback:app --reload
```

### 5. Open the Dashboard
Open your web browser and navigate to the Analyst Workbench:
```
http://localhost:8000/dashboard
```

---

##  System Architecture & Workflow

How does the application actually catch money launderers? Here is the flow from transaction ingestion to human-in-the-loop retraining:

### 1. Data Ingestion & Graph Representation (Neo4j)
Traditional anti-money laundering (AML) systems treat transactions as isolated tabular events. Our system ingests streaming transaction data into a **Neo4j Graph Database**, allowing us to track the complex, multi-hop pathways money takes as it flows between individuals, shell companies, and banking institutions.

### 2. Threat Detection (Hybrid GNN + XGBoost)
To detect mule accounts and criminal syndicates, we use a hybrid AI approach:
*   **Topological Data Analysis (TDA) & Graph Neural Networks (GNN):** We extract structural features from the transaction network (e.g., circular loops, rapid pass-throughs, integration hubs).
*   **XGBoost:** A highly scalable decision tree model evaluates these structural graph embeddings alongside traditional tabular data (transaction velocity, account age) to output a highly accurate **Fraud Probability Score**.

### 3. Explainable AI (SHAP)
Black-box AI is not acceptable in FinTech compliance. When our model flags an account, it uses **SHAP (SHapley Additive exPlanations)** to break down *exactly why* the AI made that decision. 
The dashboard translates this complex mathematical vector into human-readable **Risk Topologies**:
*   `Circular Money Flow`: The money forms a complete loop (detected via TDA cycles).
*   `Rapid Fund Transfers`: The account acts as an instant pass-through hub.
*   `High 30-Day Volume`: Massive amounts of money are being integrated or layered.

### 4. Interactive Analyst Workbench
Fraud investigators access our Web Dashboard to review flagged accounts. The interface provides:
*   A **Live Network Graph** (Vis.js) visualizing the direct transactional neighborhood of the suspect.
*   **Dynamic Visual Proof:** Selecting a high-risk account physically highlights the AI's evidence directly on the graph (e.g., illuminating the actual circular money loops in red).
*   A **Node Inspector** displaying exact account metadata alongside the SHAP AI Explainer breakdown.

### 5. Human-in-the-Loop Active Learning (Kafka)
When an analyst makes a final decision (`Flag as True Positive` or `Dismiss`), the verdict and their investigation notes are instantly pushed to an **Apache Kafka** event stream (`analyst-feedback`).

This live feedback stream is continuously consumed by an automated background MLOps pipeline. The XGBoost model dynamically retrains itself based on the human investigator's ground-truth input, ensuring the AI actively gets smarter and adapts to newly evolving money laundering typologies in real-time.
