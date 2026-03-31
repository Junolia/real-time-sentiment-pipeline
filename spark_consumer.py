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

    spark.sparkContext.setLogLevel("WARN")

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

    # ----------------------------
    # Aggregation 1: time-window + subreddit
    # ----------------------------
    subreddit_aggregated = (
        scored_with_time
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("subreddit")
        )
        .agg(
            avg("sentiment_score").alias("avg_sentiment_score"),
            avg("weighted_score").alias("avg_weighted_score"),
            count("*").alias("total_messages"),
            spark_sum(
                when(col("sentiment_label") == "positive", 1).otherwise(0)
            ).alias("positive_count"),
            spark_sum(
                when(col("sentiment_label") == "negative", 1).otherwise(0)
            ).alias("negative_count"),
            spark_sum(
                when(col("sentiment_label") == "neutral", 1).otherwise(0)
            ).alias("neutral_count")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("subreddit"),
            col("avg_sentiment_score"),
            col("avg_weighted_score"),
            col("total_messages"),
            col("positive_count"),
            col("negative_count"),
            col("neutral_count")
        )
    )

    # ----------------------------
    # Aggregation 2: thread-level by link_id
    # ----------------------------
    thread_aggregated = (
        scored_with_time
        .filter(col("link_id").isNotNull())
        .groupBy(
            col("link_id"),
            col("parent_post_clean"),
            col("subreddit")
        )
        .agg(
            avg("sentiment_score").alias("avg_sentiment_score"),
            avg("weighted_score").alias("avg_weighted_score"),
            count("*").alias("total_messages"),
            spark_sum(
                when(col("sentiment_label") == "positive", 1).otherwise(0)
            ).alias("positive_count"),
            spark_sum(
                when(col("sentiment_label") == "negative", 1).otherwise(0)
            ).alias("negative_count"),
            spark_sum(
                when(col("sentiment_label") == "neutral", 1).otherwise(0)
            ).alias("neutral_count")
        )
    )

    # ----------------------------
    # Output 1: subreddit aggregation to console
    # ----------------------------
    subreddit_query = (
        subreddit_aggregated.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .option("checkpointLocation", "./checkpoints/reddit_subreddit_agg_v2")
        .queryName("subreddit_aggregation")
        .start()
    )

    # ----------------------------
    # Output 2: thread aggregation to console
    # ----------------------------
    thread_query = (
        thread_aggregated.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .option("checkpointLocation", "./checkpoints/reddit_thread_agg_v2")
        .queryName("thread_aggregation")
        .start()
    )

    print(f"Spark consumer running. Listening on Kafka topic: {TOPIC}")
    print("Streaming query 1: 5-minute sentiment by subreddit")
    print("Streaming query 2: sentiment by thread/link_id")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()