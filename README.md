````markdown
# Graph-Based AML Detection System

## Overview

This project implements a real-time graph-based Anti-Money Laundering (AML) detection system using Kafka, Neo4j, FastAPI, and Machine Learning.

The system ingests financial transactions, models them as a graph, engineers graph-based fraud features, and predicts suspicious activity using a trained ML model. It also exposes a real-time fraud scoring API with explainable outputs.

The project focuses on detecting hidden transactional relationships such as:

- Multi-hop laundering
- Circular money movement
- Fan-out fraud patterns
- Suspicious account connectivity
- Transaction flow imbalance

---

# Why This Project Exists

Traditional fraud detection systems usually analyze transactions independently. However, money laundering typically occurs through connected transaction structures and hidden relationships between accounts.

This project demonstrates how graph databases and machine learning can be combined to detect these complex fraud patterns more effectively than traditional row-based systems.

The goal of the project is not only prediction accuracy, but also designing a scalable AML architecture similar to real-world financial monitoring systems.

---

# Technologies Used

- Python
- FastAPI
- Uvicorn
- Apache Kafka
- Zookeeper
- Neo4j
- Scikit-learn
- Pandas
- NumPy
- Joblib
- Docker

---

# Project Architecture

```text
Dataset
   ↓
Ingestion Service (FastAPI)
   ↓
Kafka Producer
   ↓
Graph Builder Consumer
   ↓
Neo4j Transaction Graph
   ↓
Feature Engineering
   ↓
Machine Learning Model
   ↓
Real-Time Scoring API
```

---

# Features

## Graph-Based Features

- Neighbor fraud risk
- Two-hop fraud exposure
- Sender/receiver diversity
- Flow imbalance ratio
- Degree ratio
- Burst transaction ratio

## Fraud Pattern Simulation

- Chain laundering
- Circular laundering
- Fan-out fraud structures

## Machine Learning

- Random Forest classifier
- Imbalanced data handling
- Threshold tuning
- Model persistence

## Real-Time API

- Fraud scoring endpoint
- Fraud probability prediction
- Explainable predictions

---

# Project Structure

```text
aml-tx-detection/
│
├── services/
│   ├── ingestion-service/
│   ├── graph-builder/
│   └── scoring-service/
│
├── scripts/
│   └── load_aml_dataset.py
│
├── ml/
│   ├── export.py
│   ├── train_model.py
│   ├── load_model.py
│   ├── aml_features.csv
│   └── aml_model.pkl
│
├── data/
│   └── aml/
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

# Installation

## 1. Clone Repository

```bash
git clone <your-repository-url>
cd aml-tx-detection
```

---

## 2. Create Virtual Environment

### Linux/macOS

```bash
python3 -m venv venv
source venv/bin/activate
```

### Windows PowerShell

```powershell
python -m venv venv
venv\Scripts\activate
```

---

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

# How to Run the Project

The project requires multiple terminals running simultaneously.

---

## Terminal 1 — Start Neo4j

```bash
docker run -d \
  --name neo4j \
  -p7474:7474 \
  -p7687:7687 \
  -e NEO4J_AUTH=neo4j/test1234 \
  neo4j
```

### Neo4j Browser

```text
http://localhost:7474
```

### Login Credentials

```text
Username: neo4j
Password: test1234
```

---

## Terminal 2 — Start Zookeeper

```bash
zookeeper-server-start.sh config/zookeeper.properties
```

---

## Terminal 3 — Start Kafka

```bash
kafka-server-start.sh config/server.properties
```

---

## Terminal 4 — Start Ingestion Service

```bash
cd services/ingestion-service
uvicorn app.main:app --reload --port 8000
```

---

## Terminal 5 — Start Graph Builder

```bash
cd services/graph-builder
python -m app.main
```

---

## Terminal 6 — Load Dataset

```bash
python scripts/load_aml_dataset.py
```

This loads:

- Normal transactions
- Chain laundering patterns
- Circular laundering patterns
- Fan-out fraud structures

---

## Terminal 7 — Export Features

```bash
python ml/export.py
```

---

## Terminal 8 — Train ML Model

```bash
python ml/train_model.py
```

The trained model is saved as:

```text
ml/aml_model.pkl
```

---

## Terminal 9 — Start Real-Time Scoring API

```bash
cd services/scoring-service
uvicorn app.main:app --reload --port 8003
```

---

# How to Use the System

## Open Swagger Documentation

```text
http://127.0.0.1:8003/docs
```

You can test the fraud scoring API directly from the browser.

---

# Fraud Scoring API

## Endpoint

```http
POST /score
```

---

## Example Request

```bash
curl -X POST http://127.0.0.1:8003/score \
-H "Content-Type: application/json" \
-d '{
  "out_degree": 5,
  "in_degree": 2,
  "total_sent": 5000,
  "total_received": 100,
  "tx_count_60s": 3,
  "unique_receivers": 4,
  "unique_senders": 1,
  "neighbor_risk_ratio": 0.4,
  "two_hop_risk": 0.2,
  "flow_ratio": 50,
  "burst_ratio": 0.6,
  "degree_ratio": 2
}'
```

---

## Example Response

```json
{
  "fraud_probability": 0.93,
  "is_fraud": 1,
  "top_reasons": [
    ["total_sent", 164.49],
    ["total_received", 8.96],
    ["flow_ratio", 3.34]
  ]
}
```

---

# Explainability

The scoring API provides explainable predictions using feature contribution approximations to indicate why a transaction was flagged as suspicious.

---

# Notes

- Fraud structures are partially simulated to validate graph-based fraud detection logic.
- The project prioritizes scalable system design and graph analytics over massive-scale training data.
- The architecture is designed to resemble real-world AML monitoring pipelines.

---

# Future Improvements

- Kafka-based automatic live scoring
- Frontend dashboard for fraud monitoring
- SHAP-based explainability
- Graph Neural Network integration
- Docker Compose deployment

---

# Author

**Rahul Chaturvedi**
````
