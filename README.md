🚀 Real-Time Market Lakehouse
An enterprise-grade real-time data pipeline and AI analytics platform. This project ingests live cryptocurrency trade data, processes it through a Medallion Architecture, and provides real-time AI anomaly detection via a high-performance dashboard.

🏗️ Architecture: The Medallion Flow
This project follows the Lakehouse design pattern to ensure data reliability, ACID transactions, and sub-second analytical performance:

graph TD
    subgraph Data_Ingestion
    A[Binance WebSockets] --> B(Kafka Bronze Topic)
    end
    
    subgraph Lakehouse_Processing
    B --> C{Spark Structured Streaming}
    C --> D[(Delta Lake Silver: Cleaned)]
    D --> E{Spark Aggregator}
    E --> F[(Delta Lake Gold: OHLCV)]
    end
    
    subgraph Serving_Layer
    F --> G[FastAPI + Scikit-Learn AI]
    G --> H[Next.js 14 Dashboard]
    end


Bronze (Raw): Live trades from Binance are streamed into Kafka as immutable JSON strings.

Silver (Cleaned): Spark cleans the data, casts types (Decimal/Timestamp), and enforces schemas, saving to Delta Lake.

Gold (Aggregated): Spark aggregates raw trades into 1-minute OHLCV (Open, High, Low, Close, Volume) bars using watermarking.

Platinum (AI Inference): A FastAPI layer runs an Isolation Forest ML model to detect market anomalies (Flash crashes/Volume spikes) in real-time.

🤖 AI Anomaly Detection
The platform doesn't just show data—it analyzes it. Using the Isolation Forest algorithm, the system monitors:

Volume Surges: Automatically detecting unusual buying/selling pressure.

Price Variance: Identifying flash crashes or "fat finger" trades before they hit the chart.

Alert System: The frontend dynamically pulses red when the anomaly score exceeds the set threshold.

🛠️ Tech Stack
Ingestion: Apache Kafka (KRaft Mode), Python (WebSockets)

Processing: Apache Spark 3.5.0 (PySpark)

Storage: Delta Lake 3.1.0 (ACID Transactions, Time-Travel)

API/Inference: FastAPI, Scikit-Learn (Isolation Forest)

Frontend: Next.js 14 (Turbopack), Tailwind CSS, Recharts

Infrastructure: Docker, GitHub Codespaces (Java 11/17 compatibility)

🚀 Getting Started
1. Start Infrastructure
Launch the Kafka brokers and Docker environment:
    docker-compose up -d
2. Run the Pipeline (Separate Terminals)
Run the unified orchestrator to manage the entire data lifecycle:
    python ingestion_producer.py      # Ingest raw data
    python medallion_orchestrator.py  # Unified Silver & Gold Processing
    python api_server.py              # Launch AI Inference API (Port 8000)
3. Launch Frontend
    cd frontend
    npm install
    npm run dev                       # Launch Dashboard (Port 3000) 

📊 Technical Highlights & Challenges
Schema Bootstrapping: Implemented a custom Delta Lake bootstrap to prevent "Chicken and Egg" schema errors during concurrent Read/Write operations.

Memory Management: Optimized Spark configuration for restricted environments (Codespaces) using local[1] master and spark.driver.memory caps.

Fault Tolerance: Integrated failOnDataLoss: false and Spark Checkpointing to ensure the pipeline resumes seamlessly after Kafka retention cycles.

📈 Future Roadmap
[ ] Support for multiple symbols (ETH, SOL, etc.) using Spark multi-stream grouping.

[ ] Integration with Snowflake for long-term historical storage.

[ ] Advanced sentiment analysis via LLM (Llama 3) on live Twitter/X news feeds.