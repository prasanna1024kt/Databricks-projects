# 🌦️ Weather API Streaming Project on Azure
This project demonstrates two approaches to ingest and process **real-time weather data** from a public Weather API in Azure.  
It leverages **Databricks**, **Event Hubs**, **Azure Key Vault**, and **Azure Functions** to create a scalable and secure data pipeline.

## 🚀 Architecture Overview

### Approach 1: Databricks + Event Hubs + Key Vault
1. **Databricks Notebook** fetches data from the Weather API.
2. **Azure Key Vault** securely stores API keys and Event Hub connection strings.
3. **Event Hub** ingests the weather data as a streaming source.
4. Databricks Structured Streaming job pushes data into Event Hub **every 30 seconds**.
5. Downstream consumers (like storage, analytics, or Power BI) can subscribe to the Event Hub.

### Approach 2: Azure Functions + Timer Trigger + Key Vault
1. **Azure Function (Timer Trigger)** runs every **30 seconds**.
2. Function retrieves **Weather API key** and **Event Hub credentials** from **Azure Key Vault**.
3. Calls the Weather API, collects the latest weather data.
4. Sends the payload to Event Hub (or logs/outputs for downstream processing).

---

## 🔑 Key Azure Components Used
- **Azure Databricks**: Data ingestion & streaming with Spark Structured Streaming.
- **Azure Event Hubs**: Message broker for real-time weather data.
- **Azure Key Vault**: Secure storage of API keys, secrets, and connection strings.
- **Azure Functions (Timer Trigger)**: Lightweight serverless streaming alternative.
- **Azure Storage / Lakehouse (optional)**: For persisting data.

---

## ⚙️ Deployment Steps

### 1. Prerequisites
- Azure subscription with access to create resources.
- Provision:
  - Databricks workspace
  - Event Hub namespace + Event Hub instance
  - Azure Key Vault
  - Function App (for timer trigger approach)

### 2. Store Secrets in Key Vault
- Add:
  - `WeatherApiKeylatest`
  - `EventHubConnectionString`
  - Any other required credentials

### 3. Databricks Approach
- Import the Databricks notebook into your workspace.
- Configure Key Vault-backed secret scope.
- Read secrets into the notebook.
- Schedule notebook to run continuously, pushing data into Event Hub every 30 seconds.

### 4. Azure Functions Approach
- Create an **Azure Function App** with **Timer Trigger**.
- Configure Managed Identity for Key Vault access.
- Fetch secrets inside the function code.
- Implement API call + Event Hub push.
- Set CRON schedule in `function.json` for 30-second intervals:
  ```json
  "schedule": "*/30 * * * * *"