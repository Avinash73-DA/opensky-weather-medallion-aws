# ✈️🌦️ AWS Data Engineering Project – OpenSky & OpenWeather Analytics  

A complete **AWS Data Engineering pipeline** that ingests, processes, and analyzes **live flight and weather data** using **serverless architecture** with **medallion design pattern (Bronze → Silver → Gold)**.  

---

## 📑 Table of Contents  
- [🚀 Overview](#-overview)  
- [⚙️ Tech Stack](#️-tech-stack)  
- [📂 Project Hierarchy](#-project-hierarchy)  
- [🔄 Process Flow](#-process-flow)  
  - [🥉 Bronze Layer](#-bronze-layer)  
  - [🥈 Silver Layer](#-silver-layer)  
  - [🥇 Gold Layer](#-gold-layer)  
  - [🔍 Athena](#-athena)  
  - [📡 Airflow Orchestration](#-airflow-orchestration)  
- [🧪 Testing](#-testing)  
- [📊 Architecture & Diagrams](#-architecture--diagrams)  
- [📸 Sample Data](#-sample-data)  
- [📌 Key Features](#-key-features)  

---

## 🚀 Overview  

This project demonstrates how to build a **real-time Data Engineering pipeline** using AWS services.  
It ingests **live weather data** (OpenWeather API 🌦️) and **live flight data** (OpenSky API ✈️), processes them with **PySpark on AWS Glue**, enriches the data using the **Haversine formula**, and serves them for analytics using **Athena**.  

👉 The entire orchestration is handled using **Airflow DAGs** hosted on **EC2**.  

📷 *Project Overview Diagram:*  
![Overview Diagram](img_Src/overview_diagram.png)  

---

## ⚙️ Tech Stack  

- **AWS Lambda** + **Lambda Layers** → API Ingestion  
- **AWS Glue (Script + Notebook + Crawler)** → ETL & Schema Management  
- **Amazon S3** → Bronze (JSON), Silver (Parquet), Gold (Parquet Tables)  
- **Apache Airflow** (EC2 + Local) → Orchestration  
- **Athena** → Querying & Analytics  
- **PySpark** → Transformation, Joins, Broadcast Joins, Partitioning  
- **OpenWeather API 🌦️** & **OpenSky API ✈️** → Source Data  

---

## 📂 Project Hierarchy  

```bash
├── airflow_dags/              # Airflow DAGs for orchestration
├── bronze/
│   └── lambda_script/          # Lambda ingestion scripts
├── silver/
│   └── glue_Scripts/           # Glue ETL scripts for Silver layer
├── gold/
│   └── glue_notebook/          # Glue notebooks for Gold layer
├── google_Colab/               # Testing & validation in Colab
├── img_Src/                    # Architecture & pipeline diagrams
├── sample_layer_data/          # Sample outputs
│   ├── bronze/
│   │   ├── opensky_api/
│   │   └── openweather_api/
│   ├── silver/
│   │   ├── planes_data/year=2025/month=08/
│   │   ├── plane_weather_enriched/year=2025/month=08/
│   │   └── weather_data/country=AE, AF
│   └── gold/
│       ├── CityWeatherHistory/country=AE, AF
│       ├── flight_weather_snapshot/
│       └── weather_Impact_flights/

## 🔄 Process Flow  

### 🥉 Bronze Layer (Lambda Ingestion)  
- **Weather Data**: Pulling **1500 cities live weather data** from OpenWeather API → Stored as JSON in **S3 Bronze**.  
- **Flight Data**: Pulling **live plane positions** from OpenSky API → Stored as JSON in **S3 Bronze**.  

📷 ![Bronze Layer](img_Src/bronze_layer.png)  

---

### 🥈 Silver Layer (Glue Script & Crawler)  
- Defined **structured schema** using PySpark `StructType`.  
- Converted **JSON → Parquet** with partitioning:  
  - Weather → partitioned by **Country**  
  - Flights → partitioned by **Year & Month**  
- **Enriched Table**: Linked weather & flights using **Haversine formula** (closest city to flight coordinates).  
- Enabled **Glue Job Bookmark** → Avoids reprocessing old files.  
- Implemented **deduplication** at Silver layer.  
- Created Athena schema with **Glue Crawler**.  

📷 ![Silver Layer](img_Src/silver_layer.png)  

---

### 🥇 Gold Layer (Glue Notebook)  
- Built **3 curated datasets**:  
  1. **Flight + Weather Combined Data** → partitioned by *Year, Month*  
  2. **Weather History (Avg, Min, Max)** → partitioned by *Country*  
  3. **Weather Impact on Flights** → aggregated snapshot of flights stuck in storms, delays, etc.  
- Stored as **Parquet tables** in **Gold S3 bucket**.  

📷 ![Gold Layer](img_Src/gold_layer.png)  

---

### 🔍 Athena  
- Queryable datasets for **analytics and reporting**.  
- Partition pruning enabled for **faster queries**.  

📷 ![Athena](img_Src/athena.png)  

---

### 📡 Airflow Orchestration  
- **Airflow DAG** orchestrates ingestion & transformation every **1 hour**.  
- Flow:  

```python
# Step 1: Bronze ingestion triggers Silver jobs
lambda_flights >> process_plane_data
lambda_weather >> process_weather_data

# Step 2: Enrichment after initial Silver jobs
[process_plane_data, process_weather_data] >> process_enriched_data

# Step 3: Gold jobs in parallel after enrichment
process_enriched_data >> [gold_weather_impact, flight_weather, weather_history]
