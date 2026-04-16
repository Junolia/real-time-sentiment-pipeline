# Real-Time Reddit Sentiment Pipeline: Macro-Level Analysis

## Overview

### Problem Statement
Monitoring and analyzing platform-wide sentiment trends in real-time across dozens of distinct online communities presents significant engineering challenges. Public APIs enforce strict rate limits that prevent at-scale ingestion, while rendering real-time analytics on a single frontend often leads to interface stuttering and state-loss. This project solves these issues by simulating a high-throughput data stream using a Lambda architecture, processing over 2GB of text data, and serving the results through a dual-speed decoupled dashboard.

### System Architecture
The system utilizes a modern Big Data streaming pipeline:
1. **Ingestion:** High-throughput Python producer reading from a local amplified dataset.
2. **Message Broker:** Apache Kafka for robust data buffering.
3. **Processing:** PySpark Structured Streaming for text cleaning and VADER sentiment analysis.
4. **Storage (Cold Path):** AWS S3 for long-term Parquet storage.
5. **Serving (Hot Path):** A secondary Kafka topic feeding a Streamlit frontend.

*( insert architecture diagram )*

---

## Data

### Source and Collection
The core dataset is the **[1 Million Reddit Comments from 40 Subreddits](https://www.kaggle.com/datasets/smagnan/1-million-reddit-comments-from-40-subreddits/data)** available on Kaggle. 

To demonstrate the pipeline's capability to operate at Big Data scale (>2GB), the original dataset undergoes a simulated load-testing amplification process. A local script (`data_amplifier.py`) magnifies the base dataset by a factor of 50, creating a massive, continuous stream of realistic comment data without triggering API rate limits.

### Schema and Metadata
The data is mapped and injected into Kafka using the following JSON schema:
* `subreddit` (String): The community the comment originated from.
* `author` (String): The user who posted the comment.
* `content` (String): The cleaned body text of the comment.
* `parent_post` (String): The title of the thread.
* `type` (String): Categorized as 'comment'.
* `created_utc` (Float): Time-shifted UNIX timestamp to simulate real-time ingestion.
* `parent_id` / `comment_id` / `link_id` (Strings): Metadata for thread aggregation.

---

## Process

### 1. Ingestion (`macro_producer.py`)
Acts as the load injector. It parses the 2GB+ CSV file line-by-line, performs schema mapping, applies time-shifting to the timestamps, and pushes thousands of records per second into the `reddit_sentiment` Kafka topic.

### 2. Processing Engine (`spark_consumer.py`)
A PySpark application that subscribes to the Kafka topic.
* **Inputs:** Raw JSON strings from Kafka.
* **Transformations:** Applies regex to strip URLs and special characters, executes VADER sentiment analysis (assigning a compound score from -1.0 to 1.0), and categorizes the sentiment.
* **Outputs:** * Appends processed data to an AWS S3 bucket in `.parquet` format for historical batch processing.
    * Streams the enriched data back to Kafka into the `dashboard_feed` topic.

---

## Results

### Dual-Speed Streamlit Dashboard (`dashboard.py`)
The final output is rendered via a Streamlit application engineered with a dual-buffer state management system to prevent UI flickering under heavy loads.

* **Fast Tier (Real-Time Flow):** A scatter plot and line graph that update every 0.5 seconds, showing the live distribution of sentiment and comment volume across the 40 subreddits.
* **Slow Tier (Aggregated Analytics):** Updates at a controlled interval (e.g., every 1-10 minutes) to display statistically significant metrics:
    * Global Platform Sentiment Average.
    * Top 5 Positive and Top 5 Negative Subreddits (Horizontal Bar Charts).
    * Natural Language Processing (NLP) insights detailing the most frequently mentioned words (excluding standard stop-words).

---

## Reproduce Guidelines

### Prerequisites
* **Java 17:** PySpark 3.x/4.x requires Java 17. Newer versions (Java 21/25) will cause `UnsupportedOperationException: getSubject` errors.
* **Apache Kafka:** Must be installed and running locally on port `9092`.
* **Python 3.12+:** With virtual environment containing `pyspark`, `kafka-python`, `streamlit`, `pandas`, `plotly`, and `vaderSentiment`.
* **AWS Credentials:** Configured locally or passed via environment variables for S3 access.

### Execution Steps

**1. Lock Java Environment**
Ensure your terminal sessions are utilizing Java 17:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
```

**2. Generate the Load-Testing Dataset**
Download the Kaggle dataset to the project root, then run the amplifier to build the 2GB file:
```bash
python data_amplifier.py
```

**3. Start the Spark Engine**
Initialize the processing layer (requires Kafka and Hadoop-AWS packages):
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.2 \
  spark_consumer.py
```

**4. Launch the Visualization Dashboard**
In a new terminal (with Java 17 exported), start Streamlit:
```bash
streamlit run dashboard.py
```

**5. Initiate the Data Stream**
Finally, start the load injector to begin pushing the 2GB dataset through the pipeline:
```bash
python macro_producer.py
```