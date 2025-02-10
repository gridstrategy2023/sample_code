# Step 1: install dependencies
#!pip install fastapi uvicorn numpy scikit-learn pydantic joblib

# Step 2: Train and Save Model (train_model.py)
import pickle
import numpy as np
from sklearn.linear_model import LinearRegression

# Generate dummy training data
X = np.array([[1], [2], [3], [4], [5]])  # Features
y = np.array([2, 4, 6, 8, 10])  # Labels

# Train a simple model
model = LinearRegression()
model.fit(X, y)

# Save the model
with open("model.pkl", "wb") as f:
    pickle.dump(model, f)

print("Model trained and saved as model.pkl")

#Step 3: Deploy model with FastAPI
from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np
import uvicorn
import asyncio
import sys

# Load the trained model
with open("model.pkl", "rb") as f:
    model = pickle.load(f)

# Define the input schema
from typing import List

class InputData(BaseModel):
    features: List[float]  # Ensure it's explicitly a list of floats

app = FastAPI()

@app.post("/predict")
def predict(data: InputData):
    features = np.array(data.features).reshape(1, -1)
    prediction = model.predict(features).tolist()
    return {"prediction": prediction}

if __name__ == "__main__":
    if sys.platform.startswith("win") and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    uvicorn.run(app, host="0.0.0.0", port=8000)