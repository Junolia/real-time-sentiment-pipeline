from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lower, regexp_replace, trim, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# Schema matching the messages produced by reddit_producer.py
schema = StructType([
    StructField("subreddit", StringType()),
    StructField("author", StringType()),
    StructField("content", StringType()),
    StructField("parent_post", StringType()),
    StructField("type", StringType()),
    StructField("created_utc", LongType()),
])

KAFKA_BROKER = "127.0.0.1:9092"
TOPIC = "reddit_sentiment"


def clean_text(column):
    """Apply text-cleaning transformations to a string column."""
    cleaned = column
    # Remove URLs
    cleaned = regexp_replace(cleaned, r"https?://\S+", "")
    # Remove Reddit user/subreddit mentions (/u/name, /r/name, u/name, r/name)
    cleaned = regexp_replace(cleaned, r"/?(u|r)/[A-Za-z0-9_-]+", "")
    # Keep only alphanumeric characters, basic punctuation, and spaces
    cleaned = regexp_replace(cleaned, r"[^a-zA-Z0-9\s.,!?'-]", "")
    # Collapse multiple spaces into one
    cleaned = regexp_replace(cleaned, r"\s+", " ")
    # Strip leading/trailing whitespace and lowercase
    cleaned = lower(trim(cleaned))
    return cleaned


def main():
    spark = (
        SparkSession.builder
        .appName("RedditSentimentConsumer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse the JSON value into structured columns
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    # Clean text columns
    cleaned = (
        parsed
        .withColumn("content", clean_text(col("content")))
        .withColumn("parent_post", clean_text(col("parent_post")))
        .filter(
            (col("content").isNotNull()) & (trim(col("content")) != "")
        )
        .withColumn("cleaned_at", current_timestamp())
    )

    # Write to console for debugging / demo
    query = (
        cleaned.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    print("Spark consumer running — waiting for messages on topic:", TOPIC)
    query.awaitTermination()


if __name__ == "__main__":
    main()
