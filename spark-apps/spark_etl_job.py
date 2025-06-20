import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp, length, regexp_count, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

def main():
    """
    Main function to run the Spark Streaming ETL job.
    """
    print("🚀 Starting Spark Streaming ETL Job...")

    # Get S3 configuration from environment variables
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

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
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session initialized successfully.")

    # Define the schema for incoming Kafka messages
    schema = StructType([
        StructField("Id", StringType(), True),
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

    try:
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", "ecommerce-events") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        print("✅ Kafka stream connected.")

        # Parse JSON data
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")

        # Bronze Layer - Raw data with minimal processing
        bronze_query = parsed_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints/bronze") \
            .option("path", "s3a://bronze/amazon_reviews") \
            .trigger(processingTime='30 seconds') \
            .start()

        print("✅ Bronze stream started.")

        # Silver Layer - Cleaned and enriched data with simple sentiment analysis
        # Using rule-based sentiment scoring instead of TextBlob UDF
        silver_df = parsed_df \
            .filter(col("Text").isNotNull() & (length(col("Text")) > 10)) \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("positive_words", 
                       regexp_count(col("Text").lower(), 
                                  "good|great|excellent|amazing|wonderful|fantastic|love|best|perfect|awesome")) \
            .withColumn("negative_words", 
                       regexp_count(col("Text").lower(), 
                                  "bad|terrible|awful|hate|worst|horrible|disgusting|disappointing|useless|poor")) \
            .withColumn("sentiment_score", 
                       (col("positive_words") - col("negative_words")).cast("double")) \
            .withColumn("sentiment_label", 
                       when(col("sentiment_score") > 0, "positive")
                       .when(col("sentiment_score") < 0, "negative")
                       .otherwise("neutral"))

        silver_query = silver_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints/silver") \
            .option("path", "s3a://silver/amazon_reviews") \
            .trigger(processingTime='30 seconds') \
            .start()

        print("✅ Silver stream started.")

        # Gold Layer - Simplified processed data
        gold_df = silver_df.select("ProductId", "Score", "sentiment_label", "sentiment_score", "processed_timestamp")

        gold_query = gold_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoints/gold") \
            .option("path", "s3a://gold/amazon_reviews_sentiment") \
            .trigger(processingTime='60 seconds') \
            .start()

        print("✅ Gold stream started.")
        print("🔄 Streaming ETL pipeline is running...")
        print("📊 Data flows: Kafka → Bronze → Silver → Gold")

        # Keep the streaming job running
        bronze_query.awaitTermination()

    except Exception as e:
        print(f"❌ Error in streaming ETL: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()