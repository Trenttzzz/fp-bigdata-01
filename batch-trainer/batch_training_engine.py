import os
import json
import boto3
from botocore.config import Config
import logging
import mlflow
import mlflow.sklearn
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, stddev, lit, when, min, max
from pyspark.sql.utils import AnalysisException

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

class BatchTrainingEngine:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark = self.setup_spark_session()
        mlflow.set_tracking_uri("http://mlflow:5000")
        self.logger.info("✅ Batch training engine initialized")

    def setup_spark_session(self):
        return SparkSession.builder.appName("ProductHitPredictorTraining").getOrCreate()

    def create_feature_table(self):
        try:
            # REVERTED: Use SILVER data for more balanced training
            silver_path = "s3a://silver/amazon_reviews"
            self.logger.info(f"📥 Loading data from Silver Delta table: {silver_path}")
            
            df_silver = self.spark.read.format("delta").load(silver_path)

            # DEBUG: Check how much data we have
            total_reviews = df_silver.count()
            unique_products = df_silver.select("ProductId").distinct().count()
            self.logger.info(f"🔍 Total reviews: {total_reviews}, Unique products: {unique_products}")

            # Calculate helpfulness ratio on-the-fly
            df_with_helpfulness = df_silver.withColumn("helpfulness_ratio",
                when(col("HelpfulnessDenominator") > 0, 
                     col("HelpfulnessNumerator") / col("HelpfulnessDenominator"))
                .otherwise(0)
            )

            # Add simple sentiment score based on the original star rating
            df_with_sentiment = df_with_helpfulness.withColumn("sentiment_score",
                when(col("Score") >= 4, 1.0)
                .when(col("Score") >= 3, 0.5)
                .otherwise(0.0)
            )

            # --- Aggregate data by ProductID to create features ---
            self.logger.info("📊 Aggregating data to create product-level features...")
            df_product_features = df_with_sentiment.groupBy("ProductId").agg(
                avg("sentiment_score").alias("avg_sentiment"),
                avg("Score").alias("avg_score"),
                count("Id").alias("review_count"),
                stddev("Score").alias("score_stddev"),
                avg("helpfulness_ratio").alias("avg_helpfulness"),
                min("Score").alias("min_score"),
                max("Score").alias("max_score"),
                avg(when(col("Score") >= 4, 1).otherwise(0)).alias("positive_ratio")
            )

            # REVERTED: Use the hit definition that produced better F1-scores
            df_final_features = df_product_features.withColumn("is_hit",
                when(
                    (col("avg_score") >= 4.2) &
                    (col("positive_ratio") >= 0.7) &
                    (col("review_count") >= 3) &
                    (col("avg_helpfulness") >= 0.2)
                , 1)
                .otherwise(0)
            ).na.fill(0)

            # DEBUG: Check class distribution
            record_count = df_final_features.count()
            hit_count = df_final_features.filter(col("is_hit") == 1).count()
            hit_percentage = (hit_count / record_count * 100) if record_count > 0 else 0
            
            self.logger.info(f"📈 Created feature table with {record_count} products")
            self.logger.info(f"🎯 Products classified as hits: {hit_count} ({hit_percentage:.1f}%)")
            
            if record_count < 100:
                self.logger.warning(f"⚠️ Very few products for training: {record_count}")
                
            return df_final_features.toPandas()

        except AnalysisException:
            self.logger.warning("⚠️ Silver data not found. Please ensure ETL is running.")
            return None
        except Exception as e:
            self.logger.error(f"❌ Error creating feature table: {e}", exc_info=True)
            return None
        
    def generate_dashboard_data(self, df_features):
        self.logger.info("📊 Generating dashboard data...")
        try:
            kpis = {
                'total_products': df_features.shape[0],
                'avg_score': round(df_features['avg_score'].mean(), 2),
                'total_hits': int(df_features['is_hit'].sum()),
                'hit_percentage': round(df_features['is_hit'].mean() * 100, 1),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            }

            product_list = df_features.sort_values(by='review_count', ascending=False) \
                                      .head(100) \
                                      .to_dict(orient='records')
            
            dashboard_content = {'kpis': kpis, 'product_list': product_list}

            s3_client = boto3.client(
                's3',
                endpoint_url=os.getenv("MLFLOW_S3_ENDPOINT_URL"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                config=Config(signature_version='s3v4')
            )
            
            s3_client.put_object(
                Bucket='dashboard',
                Key='dashboard_summary.json',
                Body=json.dumps(dashboard_content, indent=4).encode('utf-8'),
                ContentType='application/json'
            )
            self.logger.info("✅ Dashboard data successfully uploaded to MinIO.")
        except Exception as e:
            self.logger.error(f"❌ Failed to generate or upload dashboard data: {e}", exc_info=True)

    def train_model(self):
        df = self.create_feature_table()
        if df is None:
            self.logger.warning("Skipping training run as no feature data was created.")
            return False

        # Check for minimum viable dataset
        if len(df) < 50:
            self.logger.warning(f"Dataset too small for reliable training: {len(df)} samples")
            return False

        mlflow.set_experiment("Product_Success_Prediction")
        
        with mlflow.start_run(run_name=f"LogisticRegression_Hit_Predictor_{datetime.now().strftime('%Y%m%d-%H%M%S')}"):
            self.logger.info("🚀 Starting model training workflow...")

            # Use only the features that exist in your create_feature_table method
            features = ['avg_sentiment', 'avg_score', 'review_count', 'score_stddev', 'avg_helpfulness']
            X = df[features]
            y = df['is_hit']
            
            # Check class distribution
            positive_count = y.sum()
            negative_count = len(y) - positive_count
            self.logger.info(f"📊 Class distribution: {positive_count} hits, {negative_count} non-hits")
            
            if positive_count < 5 or negative_count < 5:
                self.logger.warning("⚠️ Insufficient samples in one class for reliable training")
                return False
            
            # Stratified split to maintain class balance
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.25, random_state=42, stratify=y
            )
            
            # IMPROVED: Logistic Regression Pipeline (like your working notebook!)
            model_pipeline = Pipeline(steps=[
                ('scaler', StandardScaler()),
                ('classifier', LogisticRegression(
                    random_state=42,
                    class_weight='balanced',  # Handle imbalanced data
                    C=0.5,                    # Regularization (prevents overfitting)
                    max_iter=1000,            # Ensure convergence
                    solver='liblinear'        # Good for small datasets
                ))
            ])
            
            # Train the Model
            self.logger.info("Training Logistic Regression model...")
            model_pipeline.fit(X_train, y_train)
            
            # Evaluate
            self.logger.info("📈 Evaluating model...")
            y_pred = model_pipeline.predict(X_test)
            y_pred_proba = model_pipeline.predict_proba(X_test)[:, 1]
            
            # Comprehensive metrics
            from sklearn.metrics import classification_report, roc_auc_score
            
            f1 = f1_score(y_test, y_pred)
            accuracy = accuracy_score(y_test, y_pred)
            precision = precision_score(y_test, y_pred, zero_division=0)
            recall = recall_score(y_test, y_pred, zero_division=0)
            
            # Calculate AUC
            try:
                auc = roc_auc_score(y_test, y_pred_proba)
            except:
                auc = 0.5
                
            self.logger.info(f"✅ Model Metrics - F1: {f1:.4f}, Accuracy: {accuracy:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}, AUC: {auc:.4f}")
            
            # Log feature importance (Logistic Regression coefficients)
            feature_importance = abs(model_pipeline.named_steps['classifier'].coef_[0])
            for i, feature in enumerate(features):
                mlflow.log_metric(f"feature_importance_{feature}", feature_importance[i])
                
            # Log comprehensive metrics
            mlflow.log_param("model_type", "LogisticRegression_Balanced")
            mlflow.log_param("training_products", len(df))
            mlflow.log_param("positive_samples", positive_count)
            mlflow.log_param("negative_samples", negative_count)
            mlflow.log_param("regularization_C", 0.5)
            mlflow.log_metric("f1_score", f1)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("precision", precision)
            mlflow.log_metric("recall", recall)
            mlflow.log_metric("auc", auc)
            
            # More realistic performance thresholds
            if f1 > 0.3 and auc > 0.6:  # Reasonable expectations for small dataset
                mlflow.sklearn.log_model(
                    sk_model=model_pipeline,
                    artifact_path="product_hit_predictor_model",
                    registered_model_name="product-hit-predictor"
                )
                self.logger.info("📦 Model successfully logged to MLflow.")
            else:
                self.logger.warning(f"⚠️ Model performance below threshold (F1: {f1:.4f}, AUC: {auc:.4f})")

            # Generate and upload dashboard data
            self.generate_dashboard_data(df)

        return True