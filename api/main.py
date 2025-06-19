# api/main.py
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import mlflow
import pandas as pd
from typing import List, Dict, Any, Optional

app = FastAPI(title="Lakehouse ML API", version="1.0.0")

# --- API Data Models ---
class PredictionFeatures(BaseModel):
    Text: str
    Score: int = Field(..., gt=0, lt=6) # Score must be between 1 and 5

class PredictionRequest(BaseModel):
    features: PredictionFeatures

class PredictionResponse(BaseModel):
    prediction: int
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
            raise HTTPException(status_code=404, detail=f"No model version for '{model_name}' found.")
        
        model_version = versions[0]
        model_uri = f"models:/{model_name}/{model_version.version}"
        model = mlflow.pyfunc.load_model(model_uri)
        
        # --- Feature Engineering at Inference Time ---
        # The user only provides Text and Score. We calculate review_length.
        features_dict = request.features.model_dump()
        features_dict['review_length'] = len(features_dict['Text'])
        
        # Create a DataFrame in the correct format for the model
        input_data = pd.DataFrame([features_dict])
        
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