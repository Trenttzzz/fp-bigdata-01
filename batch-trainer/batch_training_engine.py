import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import mlflow
import mlflow.spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class BatchTrainingEngine:
    def __init__(self):
        """Initializes the training engine and sets up MLflow."""
        logger.info("Initializing Batch Training Engine...")
        
        # Define attributes before they are used
        self.mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
        self.s3_endpoint_url = os.getenv("MLFLOW_S3_ENDPOINT_URL")
        self.silver_table_path = "s3a://silver/amazon_reviews"
        self.model_name = "sentiment-logistic-regression"
        
        # Now, setup Spark session which depends on the attributes above
        self.spark = self.setup_spark_session()
        
        # Configure MLflow after Spark is ready
        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        mlflow.set_experiment("Amazon Review Sentiment")
        logger.info(f"MLflow tracking URI set to {self.mlflow_tracking_uri}")

    def setup_spark_session(self):
        """Configures and creates a Spark session with S3 and Delta Lake support."""
        logger.info("Setting up Spark session...")
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        return SparkSession.builder \
            .appName("BatchTrainingEngine") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
            .config("spark.hadoop.fs.s3a.endpoint", self.s3_endpoint_url) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    def initial_training(self):
        """Performs the initial model training on service startup."""
        logger.info("🚀 Kicking off initial model training...")
        self.full_retrain()
        logger.info("✅ Initial model training completed.")

    def check_and_train(self):
        """Scheduled job to check for new data and retrain."""
        logger.info("🔎 Checking for new data to trigger training (currently runs full retrain)...")
        self.full_retrain()

    def wait_for_data(self, max_retries=10, retry_delay=60):
        """
        Wait for the Silver table to be available and contain data.
        Returns the DataFrame if successful, None if failed.
        """
        logger.info(f"⏳ Waiting for Silver table data to be available...")
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/{max_retries}: Checking Silver table at {self.silver_table_path}")
                
                # Try to read the Delta table
                df = self.spark.read.format("delta").load(self.silver_table_path)
                
                # Check if data exists
                row_count = df.count()
                if row_count == 0:
                    raise Exception(f"Silver table exists but contains no data (0 rows)")
                
                logger.info(f"✅ Found {row_count} rows in Silver table")
                return df
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"⏳ Silver table not ready (attempt {attempt + 1}/{max_retries}): {e}")
                    logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"❌ Failed to load data after {max_retries} attempts: {e}")
                    return None
        
        return None

    def full_retrain(self):
        """Loads data, trains a model, evaluates it, and logs it to MLflow."""
        logger.info("🔄 Starting full model retraining...")
        
        try:
            # 1. Wait for and load data from the silver Delta table
            df = self.wait_for_data()
            if df is None:
                logger.error("❌ Unable to proceed with training - no data available")
                return

            # 2. Preprocess data: create a binary label and select columns
            logger.info("🔧 Preprocessing data...")
            df = df.withColumn("label", when(col("Score") > 3, 1.0).otherwise(0.0))
            df = df.select("Text", "label").na.drop()

            # Check if we have enough data after preprocessing
            processed_count = df.count()
            logger.info(f"Data after preprocessing: {processed_count} rows")
            
            if processed_count < 100:  # Minimum threshold for training
                logger.warning(f"⚠️ Insufficient data for training (only {processed_count} rows). Skipping training.")
                return

            # 3. Split data into training and testing sets
            (train_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)
            train_count = train_data.count()
            test_count = test_data.count()
            logger.info(f"Data split into {train_count} training and {test_count} test records.")

            if train_count < 50 or test_count < 10:
                logger.warning(f"⚠️ Insufficient data for reliable training/testing. Skipping training.")
                return

            # Start a new MLflow run
            with mlflow.start_run(run_name="logistic_regression_training") as run:
                logger.info(f"📊 Starting MLflow run: {run.info.run_id}")
                
                # 4. Define the ML pipeline
                logger.info("🔧 Building ML pipeline...")
                tokenizer = Tokenizer(inputCol="Text", outputCol="words")
                hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="rawFeatures", numFeatures=10000)
                idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")
                lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
                pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, lr])

                # 5. Train the model
                logger.info("🎯 Training the Logistic Regression model...")
                model = pipeline.fit(train_data)
                logger.info("✅ Model training completed")

                # 6. Evaluate the model
                logger.info("📈 Evaluating the model on the test set...")
                predictions = model.transform(test_data)
                
                # Calculate multiple metrics
                evaluator_auc = BinaryClassificationEvaluator(
                    rawPredictionCol="rawPrediction", 
                    labelCol="label", 
                    metricName="areaUnderROC"
                )
                evaluator_pr = BinaryClassificationEvaluator(
                    rawPredictionCol="rawPrediction", 
                    labelCol="label", 
                    metricName="areaUnderPR"
                )
                
                auc = evaluator_auc.evaluate(predictions)
                pr_auc = evaluator_pr.evaluate(predictions)
                
                logger.info(f"📊 Model Performance:")
                logger.info(f"   - Area Under ROC: {auc:.4f}")
                logger.info(f"   - Area Under PR:  {pr_auc:.4f}")

                # 7. Log parameters, metrics, and the model to MLflow
                mlflow.log_param("model_type", "LogisticRegression")
                mlflow.log_param("training_rows", train_count)
                mlflow.log_param("test_rows", test_count)
                mlflow.log_param("num_features", 10000)
                mlflow.log_param("max_iter", 10)
                
                mlflow.log_metric("auc_roc", auc)
                mlflow.log_metric("auc_pr", pr_auc)

                # Log the model
                logger.info("💾 Logging model to MLflow registry...")
                mlflow.spark.log_model(
                    spark_model=model,
                    artifact_path="model",
                    registered_model_name=self.model_name
                )
                
                logger.info(f"✅ Model training complete!")
                logger.info(f"📊 Run ID: {run.info.run_id}")
                logger.info(f"🎯 AUC-ROC: {auc:.4f}")
                logger.info(f"📈 AUC-PR: {pr_auc:.4f}")

        except Exception as e:
            logger.error(f"❌ An error occurred during model retraining: {e}", exc_info=True)
            raise

    def cleanup(self):
        """Cleanup resources"""
        if hasattr(self, 'spark'):
            logger.info("🧹 Cleaning up Spark session...")
            self.spark.stop()