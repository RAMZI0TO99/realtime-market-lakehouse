# 🚀 Real-Time Market Lakehouse

An enterprise-grade real-time data pipeline and AI analytics platform. This project ingests live cryptocurrency trade data, processes it through a **Medallion Architecture**, and provides real-time AI anomaly detection via a high-performance dashboard.

---

## 🛠️ Tech Stack

- **Ingestion:** Apache Kafka (KRaft Mode), Python (WebSockets)
- **Processing:** Apache Spark Structured Streaming (PySpark)
- **Storage:** Delta Lake (ACID Transactions, Time-Travel)
- **API/Inference:** FastAPI, Scikit-Learn (Isolation Forest)
- **Frontend:** Next.js 14, Tailwind CSS, Recharts
- **Infrastructure:** Docker, GitHub Codespaces

---

## 🏗️ Architecture: The Medallion Flow

This project follows the **Lakehouse** design pattern to ensure data reliability and scalability:

1.  **Bronze (Raw):** Live trades from Binance are streamed into Kafka as immutable JSON.
2.  **Silver (Cleaned):** Spark cleans the data, casts types, and enforces schemas, saving to Delta Lake.
3.  **Gold (Aggregated):** Spark aggregates 1-second trades into 1-minute OHLCV bars.
4.  **Platinum (AI Inference):** A FastAPI layer runs an **Isolation Forest** ML model to detect market anomalies in real-time.

```mermaid
graph LR
    A[Binance WS] --> B(Kafka Bronze)
    B --> C{Spark Streaming}
    C --> D[(Delta Lake Silver)]
    D --> E{Spark Aggregator}
    E --> F[(Delta Lake Gold)]
    F --> G[FastAPI + AI]
    G --> H[Next.js Dashboard]
🤖 AI Anomaly Detection
The platform doesn't just show data—it analyzes it. Using the Isolation Forest algorithm, the system monitors:

Volume Surges: Detecting unusual buying/selling pressure.

Price Variance: Identifying flash crashes or "fat finger" trades.

Alert System: The frontend dynamically pulses red when the anomaly score exceeds the set threshold.

🚀 Getting Started
1. Start Infrastructure
Bash
docker-compose up -d
2. Run the Pipeline (in separate terminals)
Bash
python ingestion_producer.py   # Ingest raw data
python spark_orchestrator.py  # Process Silver & Gold
python api_server.py           # Launch AI Inference API
3. Launch Frontend
Bash
cd frontend
npm run dev
📈 Future Roadmap
[ ] Support for multiple symbols (ETH, SOL, etc.)

[ ] Integration with Snowflake for long-term historical storage.

[ ] Advanced sentiment analysis via LLM on live news feeds.