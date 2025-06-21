# batch-trainer/batch_training_engine.py

import os
import logging
import mlflow
import mlflow.sklearn
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, length
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

class BatchTrainingEngine:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark = self.setup_spark_session()
        mlflow.set_tracking_uri("http://mlflow:5000")
        self.logger.info("✅ Batch training engine initialized")

    def setup_spark_session(self):
        return SparkSession.builder.appName("BatchTrainingEngine").getOrCreate()

    def load_training_data(self):
        try:
            silver_path = "s3a://silver/amazon_reviews"
            self.logger.info(f"📥 Loading data from Silver Delta table: {silver_path}")
            
            df_silver = self.spark.read.format("delta").load(silver_path)

            df_with_target = df_silver.withColumn("is_helpful",
                when((col("helpfulness_ratio") > 0.75) & (col("HelpfulnessDenominator") >= 5), 1)
                .otherwise(0)
            )
            df_with_features = df_with_target.withColumn("review_length", length(col("Text")))
            df_filtered = df_with_features.filter(col("HelpfulnessDenominator") >= 5)
            df_model_data = df_filtered.select("Text", "review_length", "Score", "is_helpful")

            record_count = df_model_data.count()
            self.logger.info(f"📊 Found {record_count} reviews with >= 5 votes.")

            if record_count < 100: # Set a minimum threshold
                self.logger.warning("⚠️ Insufficient data for training.")
                return None
            
            sample_fraction = min(1.0, 50000 / record_count) # Use a larger sample for automated job
            pandas_df = df_model_data.sample(fraction=sample_fraction, seed=42).toPandas()
            
            pandas_df.dropna(subset=['Text', 'is_helpful'], inplace=True)
            return pandas_df
            
        except Exception as e:
            self.logger.error(f"❌ Error loading training data: {e}", exc_info=True)
            return None

    def train_model(self):
        df = self.load_training_data()
        if df is None:
            self.logger.warning("Skipping training run as no data was loaded.")
            return False # Return False to indicate failure/skip


        mlflow.set_experiment("Helpfulness_Prediction_Automated")
        
        with mlflow.start_run(run_name=f"Automated_LR_{datetime.now().strftime('%Y%m%d-%H%M%S')}"):
            self.logger.info("🚀 Starting model training workflow...")
            X = df[['Text', 'review_length', 'Score']]
            y = df['is_helpful']
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)
            
            preprocessor = ColumnTransformer(
                transformers=[
                    # CORRECTED: Added 'Text' as the column for the TfidfVectorizer
                    ('tfidf', TfidfVectorizer(stop_words='english', max_features=5000, ngram_range=(1,2)), 'Text'),
                    ('scaler', StandardScaler(), ['review_length', 'Score'])
                ])

            pipeline = Pipeline(steps=[
                ('preprocessor', preprocessor),
                ('classifier', LogisticRegression(random_state=42, class_weight='balanced', C=0.5))
            ])
            
            pipeline.fit(X_train, y_train)
            y_pred = pipeline.predict(X_test)
            
            f1 = f1_score(y_test, y_pred)
            self.logger.info(f"✅ Model Evaluation Complete - F1-Score: {f1:.4f}")
            
            mlflow.log_metric("f1_score", f1)
            
            mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="helpfulness_model",
                registered_model_name="review-helpfulness-classifier"
            )
            self.logger.info("📦 Model successfully logged to MLflow.")
            return True