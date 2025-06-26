# api/main.py
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import mlflow
import pandas as pd
from typing import Dict, Any

app = FastAPI(title="Lakehouse ML API", version="1.0.0")

# --- Add CORS Middleware ---
origins = [
    "http://localhost:3000", # The origin for dashboard
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CORRECTED API Data Models ---
# This schema now matches the features our new model was trained on.
class PredictionFeatures(BaseModel):
    avg_sentiment: float
    avg_score: float
    review_count: int
    score_stddev: float
    avg_helpfulness: float

class PredictionRequest(BaseModel):
    features: PredictionFeatures

class PredictionResponse(BaseModel):
    prediction: int # 1 for "hit", 0 for "not a hit"
    model_version: str

# --- MLflow Setup ---
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# --- API Endpoints ---
@app.post("/predict/{model_name}", response_model=PredictionResponse)
async def predict(model_name: str, request: PredictionRequest):
    try:
        versions = mlflow.tracking.MlflowClient().get_latest_versions(model_name, stages=["Production", "Staging"])
        if not versions:
            raise HTTPException(status_code=404, detail=f"No model version for '{model_name}' found in Production/Staging.")
        
        model_version = versions[0]
        model_uri = f"models:/{model_name}/{model_version.version}"
        model = mlflow.pyfunc.load_model(model_uri)
        
        # The input data now directly matches what the model expects.
        features_dict = request.features.model_dump()
        input_data = pd.DataFrame([features_dict])
        
        # Ensure the column order is the same as during training
        # This is a good practice for robustness.
        feature_order = ['avg_sentiment', 'avg_score', 'review_count', 'score_stddev', 'avg_helpfulness']
        input_data = input_data[feature_order]

        # Make prediction
        prediction = model.predict(input_data)
        
        return PredictionResponse(
            prediction=int(prediction[0]),
            model_version=model_version.version
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
