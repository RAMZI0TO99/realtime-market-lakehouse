import pandas as pd
import logging
import os
import requests
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from deltalake import DeltaTable
from sklearn.ensemble import IsolationForest

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

app = FastAPI(title="Market AI Anomaly Detector")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

model = IsolationForest(contamination=0.05, random_state=42)
MODEL_FITTED = False

# Configure your bot here
TELEGRAM_TOKEN = "YOUR_BOT_TOKEN_HERE"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID_HERE"

def send_telegram_alert(symbol: str, price: float, status: str):
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN_HERE":
        return 
    msg = f"🚨 *NEXUS AI ALERT*\n\nAsset: {symbol}\nStatus: {status}\nPrice: ${price:,.2f}"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"})
    except Exception as e:
        logging.error(f"Failed to send Telegram alert: {e}")

def get_latest_gold_data():
    try:
        if not os.path.exists("./data/gold_market_metrics/_delta_log"):
            return pd.DataFrame()
        dt = DeltaTable("./data/gold_market_metrics")
        return dt.to_pandas().sort_values(by="window_start", ascending=False)
    except Exception as e:
        return pd.DataFrame()

@app.get("/api/v1/market/status/{symbol}")
def get_market_status(symbol: str, background_tasks: BackgroundTasks):
    global MODEL_FITTED
    symbol = symbol.upper()
    df = get_latest_gold_data()
    
    if df.empty:
        return {"status": "waiting", "symbol": symbol}

    df_symbol = df[df['symbol'] == symbol]
    if df_symbol.empty or len(df_symbol) < 2:
        return {"status": "waiting", "symbol": symbol}

    latest_bar = df_symbol.iloc[0].to_dict()
    anomaly_flag = False

    try:
        features = df_symbol[['close', 'total_volume']].fillna(0)
        if len(df_symbol) >= 10:
            model.fit(features)
            MODEL_FITTED = True
            current_row = features.iloc[[0]] 
            prediction = model.predict(current_row)
            anomaly_flag = bool(prediction[0] == -1)
            
            if anomaly_flag:
                background_tasks.add_task(send_telegram_alert, symbol, latest_bar["close"], "Unusual Volatility Detected")
    except Exception as e:
        logging.warning(f"AI Inference skipped: {e}")

    return {
        "timestamp": str(latest_bar["window_start"]),
        "symbol": latest_bar["symbol"],
        "metrics": {
            "open": latest_bar["open"], "high": latest_bar["high"],
            "low": latest_bar["low"], "close": latest_bar["close"],
            "volume": latest_bar["total_volume"]
        },
        "ai_analysis": {
            "anomaly_detected": anomaly_flag,
            "status": "Warning: Unusual Activity!" if anomaly_flag else "Normal"
        }
    }

@app.get("/api/v1/market/versions")
def get_table_versions():
    try:
        dt = DeltaTable("./data/gold_market_metrics")
        latest_version = dt.history()[0]["version"]
        return {"latest_version": latest_version}
    except Exception as e:
        return {"latest_version": 0, "error": str(e)}

@app.get("/api/v1/market/timetravel/{symbol}/{version}")
def get_time_travel_data(symbol: str, version: int):
    symbol = symbol.upper()
    try:
        dt = DeltaTable("./data/gold_market_metrics", version=version)
        df_symbol = dt.to_pandas()[dt.to_pandas()['symbol'] == symbol].sort_values(by="window_start")
        
        if df_symbol.empty:
            return {"status": "empty", "data": []}
            
        formatted_data = [{"time": pd.to_datetime(row["window_start"]).strftime("%I:%M %p"), "price": row["close"]} 
                          for _, row in df_symbol.tail(20).iterrows()]
            
        return {"status": "success", "version": version, "data": formatted_data}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)