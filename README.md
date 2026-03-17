# Real-Time Social Media Sentiment Pipeline

## 📌 Overview
This project builds an end-to-end real-time data pipeline to analyze public sentiment surrounding trending global events. The system ingests live data from social media APIs, processes text using NLP techniques, and visualizes sentiment trends over time.

The goal is to demonstrate a scalable big data architecture where data is continuously collected, processed, and served for analysis.

---

## 🏗️ Architecture

The pipeline consists of the following components:

1. **Data Source**
   - Live data stream from Reddit API (or news APIs)

2. **Ingestion Layer**
   - Apache Kafka for streaming data ingestion

3. **Processing Layer**
   - PySpark (Spark Streaming)
   - Text cleaning and sentiment classification (positive, negative, neutral)

4. **Storage Layer**
   - Amazon S3 (or local storage for testing)
   - Stores both raw and processed data

5. **Output Layer**
   - Jupyter Notebook for visualization
   - Displays sentiment trends over time


