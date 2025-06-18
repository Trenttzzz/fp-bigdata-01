from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import os
import boto3
from mlflow.tracking import MlflowClient

app = FastAPI(title="Lakehouse ML API", version="1.0.0")

# Configuration
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000')
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
client = MlflowClient()

class PredictionRequest(BaseModel):
    features: Dict[str, Any]

class PredictionResponse(BaseModel):
    prediction: float
    probability: List[float] = None
    model_version: str

@app.get("/")
async def root():
    return {"message": "Lakehouse ML API"}

@app.get("/models")
async def list_models():
    try:
        models = client.search_registered_models()
        return [{"name": model.name, "version": model.latest_versions[0].version if model.latest_versions else "N/A"} for model in models]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/predict/{model_name}", response_model=PredictionResponse)
async def predict(model_name: str, request: PredictionRequest):
    try:
        # Step 1: Search for model versions
        versions = client.get_latest_versions(model_name, stages=["Production", "Staging"])
        
        # Step 2: Validate the result before using it
        if not versions:
            # If no versions are found, raise a 404 Not Found error
            raise HTTPException(
                status_code=404,
                detail=f"No model version for '{model_name}' found in 'Production' or 'Staging' stages."
            )
        
        # If validation passes, proceed with the first version found
        model_version = versions[0]
        model = mlflow.pyfunc.load_model(f"models:/{model_name}/{model_version.version}")
        
        # Prepare input data
        input_data = pd.DataFrame([request.features])
        
        # Make prediction
        prediction = model.predict(input_data)
        
        response = PredictionResponse(
            prediction=float(prediction[0]),
            model_version=model_version.version
        )
        
        return response
    except HTTPException as http_exc:
        # Re-raise HTTPExceptions to let FastAPI handle them
        raise http_exc
    except Exception as e:
        # Catch other, truly unexpected server errors
        # Optional: Add logging here for unexpected errors
        # logger.error(f"Unexpected error during prediction for {model_name}: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/health")
async def health_check():
    return {"status": "healthy"}