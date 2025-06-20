# spark-apps/spark_etl_job.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp, length, udf
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType
from textblob import TextBlob
import time

def main():
    """
    Main function to run the Spark Streaming ETL job.
    """
    print("🚀 Starting Spark Streaming ETL Job...")

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")


    required_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    spark_packages = ",".join(required_packages)

    
    spark = SparkSession.builder \
        .appName("AutomatedAmazonReviewsETL") \
        .config("spark.jars.packages", spark_packages) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint_url) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session initialized successfully.")

    # Schema for Kafka data
    review_schema = StructType([
        StructField("Id", LongType(), True),
        StructField("ProductId", StringType(), True),
        StructField("UserId", StringType(), True),
        StructField("ProfileName", StringType(), True),
        StructField("HelpfulnessNumerator", IntegerType(), True),
        StructField("HelpfulnessDenominator", IntegerType(), True),
        StructField("Score", IntegerType(), True),
        StructField("Time", LongType(), True),
        StructField("Summary", StringType(), True),
        StructField("Text", StringType(), True)
    ])

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")) \
        .option("subscribe", "ecommerce-events") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df.select(from_json(col("value").cast("string"), review_schema).alias("data")).select("data.*")
    print("✅ Kafka stream connected.")

    # --- Define and Start All Streams ---

    # Bronze Stream
    bronze_query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://bronze/checkpoints/amazon_reviews") \
        .start("s3a://bronze/amazon_reviews")
    print("✅ Bronze stream started.")

    # Silver Stream
    silver_df = parsed_df \
        .withColumn("review_timestamp", to_timestamp(col("Time"))) \
        .withColumn("helpfulness_ratio",
            when(col("HelpfulnessDenominator") > 0, col("HelpfulnessNumerator") / col("HelpfulnessDenominator"))
            .otherwise(0).cast(DoubleType())
        ).drop("Time", "ProfileName")
    
    silver_query = silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/checkpoints/amazon_reviews") \
        .start("s3a://silver/amazon_reviews")
    print("✅ Silver stream started.")

    # Gold Stream
    def get_sentiment_score(text):
        if text: return TextBlob(text).sentiment.polarity
        return 0.0
    sentiment_udf = udf(get_sentiment_score, DoubleType())
    gold_df = silver_df.withColumn("sentiment_score", sentiment_udf(col("Text")))

    gold_query = gold_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://gold/checkpoints/amazon_reviews_sentiment") \
        .start("s3a://gold/amazon_reviews_sentiment")
    print("✅ Gold stream started.")

    # Wait for all streams to terminate
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()