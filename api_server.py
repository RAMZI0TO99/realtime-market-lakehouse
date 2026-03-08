import pandas as pd
import logging
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from deltalake import DeltaTable
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

app = FastAPI(title="Market AI Anomaly Detector")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize model
model = IsolationForest(contamination=0.05, random_state=42)
MODEL_FITTED = False # Flag to prevent IndexError before training

def get_latest_gold_data():
    try:
        # Safety check for Delta Lake folder
        if not os.path.exists("./data/gold_market_metrics/_delta_log"):
            return pd.DataFrame()
            
        dt = DeltaTable("./data/gold_market_metrics")
        df = dt.to_pandas()
        return df.sort_values(by="window_start", ascending=False)
    except Exception as e:
        logging.error(f"Delta Read Error: {e}")
        return pd.DataFrame()

@app.get("/api/v1/market/status")
def get_market_status():
    global MODEL_FITTED
    df = get_latest_gold_data()
    
    if df.empty or len(df) < 2:
        return {"status": "waiting", "message": "Awaiting more data windows..."}

    latest_bar = df.iloc[0].to_dict()
    anomaly_flag = False

    try:
        # Use 'close' and 'total_volume' as features
        # We keep it as a DataFrame to avoid the "Feature Names" UserWarning
        features = df[['close', 'total_volume']].fillna(0)
        
        # We need enough data to establishment a baseline (at least 10 rows)
        if len(df) >= 10:
            model.fit(features)
            MODEL_FITTED = True
            
            # Predict only on the most recent row
            # We wrap it in a DataFrame to keep feature names consistent
            current_row = features.iloc[[0]] 
            prediction = model.predict(current_row)
            anomaly_flag = bool(prediction[0] == -1)
            
    except Exception as e:
        logging.warning(f"AI Inference skipped: {e}")

    return {
        "timestamp": str(latest_bar["window_start"]),
        "symbol": latest_bar["symbol"],
        "metrics": {
            "open": latest_bar["open"],
            "high": latest_bar["high"],
            "low": latest_bar["low"],
            "close": latest_bar["close"],
            "volume": latest_bar["total_volume"]
        },
        "ai_analysis": {
            "anomaly_detected": anomaly_flag,
            "status": "Warning: Unusual Activity!" if anomaly_flag else "Normal"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)