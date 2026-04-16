import os
from pyspark.sql.functions import to_json, struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lower,
    regexp_replace,
    trim,
    current_timestamp,
    udf,
    when,
    from_unixtime,
    to_timestamp,
    window,
    avg,
    count,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# ----------------------------
# Kafka message schema
# ----------------------------
schema = StructType([
    StructField("subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("content", StringType(), True),
    StructField("parent_post", StringType(), True),
    StructField("type", StringType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("parent_id", StringType(), True),
    StructField("comment_id", StringType(), True),
    StructField("link_id", StringType(), True),
])

KAFKA_BROKER = "127.0.0.1:9092"
TOPIC = "reddit_sentiment"


# ----------------------------
# Text cleaning
# ----------------------------
def clean_text(column):
    cleaned = column

    # Remove URLs
    cleaned = regexp_replace(cleaned, r"https?://\S+", "")

    # Remove Reddit user/subreddit mentions
    cleaned = regexp_replace(cleaned, r"/?(u|r)/[A-Za-z0-9_-]+", "")

    # Replace newlines/tabs with spaces
    cleaned = regexp_replace(cleaned, r"[\n\r\t]+", " ")

    # Keep letters, numbers, spaces, and some punctuation
    cleaned = regexp_replace(cleaned, r"[^a-zA-Z0-9\s.,!?'\-]", "")

    # Collapse repeated spaces
    cleaned = regexp_replace(cleaned, r"\s+", " ")

    # Lowercase and trim
    cleaned = lower(trim(cleaned))

    return cleaned


# ----------------------------
# VADER setup
# ----------------------------
analyzer = SentimentIntensityAnalyzer()


def get_vader_score(text):
    if text is None:
        return 0.0

    text = text.strip()
    if text == "":
        return 0.0

    return float(analyzer.polarity_scores(text)["compound"])


vader_udf = udf(get_vader_score, DoubleType())


# ----------------------------
# Main streaming app
# ----------------------------
def main():
    spark = (
        SparkSession.builder
        .appName("RedditSentimentConsumer")
        .getOrCreate()
    )

    # AWS configuration for s3a
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
    hadoop_conf.set("fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    hadoop_conf.set("fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN"))
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark.sparkContext.setLogLevel("WARN")

    S3_BUCKET = "s3a://reddit-sentiment-pipeline-selena"

    # Read raw Kafka stream
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON from Kafka value
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    # Clean text
    cleaned = (
        parsed
        .withColumn("content_clean", clean_text(col("content")))
        .withColumn("parent_post_clean", clean_text(col("parent_post")))
        .filter(col("content_clean").isNotNull())
        .filter(trim(col("content_clean")) != "")
        .withColumn("processed_at", current_timestamp())
    )

    # Run VADER sentiment
    scored = (
        cleaned
        .withColumn("sentiment_score", vader_udf(col("content_clean")))
        .withColumn(
            "sentiment_label",
            when(col("sentiment_score") >= 0.05, "positive")
            .when(col("sentiment_score") <= -0.05, "negative")
            .otherwise("neutral")
        )
        .withColumn(
            "weight",
            when(col("type") == "comment", 1.5).otherwise(1.0)
        )
        .withColumn("weighted_score", col("sentiment_score") * col("weight"))
    )

    # Convert unix timestamp to Spark timestamp
    scored_with_time = (
        scored
        .withColumn(
            "event_time",
            to_timestamp(from_unixtime(col("created_utc").cast("long")))
        )
        .filter(col("event_time").isNotNull())
    )

    # packaged the cleaned/scored data to JSON format for downstream processing
    kafka_output = scored_with_time.select(
        to_json(struct("*")).alias("value")
    )

    # route 1: backup to S3 for long term analysis

    s3_query = (
        scored_with_time.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{S3_BUCKET}/processed/raw_scored_stream/")
        .option("checkpointLocation", "./checkpoints/s3_cold_storage")
        .start()
    )

    # route 2: wrap up real-time data and send back to Kafka for real-time visualization in Streamlit
    kafka_query = (
        kafka_output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", "dashboard_feed")
        .option("checkpointLocation", "./checkpoints/kafka_dashboard_feed")
        .trigger(processingTime='1 second')
        .start()
    )

    print(f"Spark Processing Engine Active.")
    print(f"-> Routing Cold Data to S3 ({S3_BUCKET})")
    print(f"-> Routing Hot Stream to Kafka Topic: dashboard_feed")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()