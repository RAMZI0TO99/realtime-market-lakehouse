import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, first, last, max, min, sum

# 1. CREATE SESSION (Cleaned for Java 11)
spark = SparkSession.builder \
    .appName("MarketGoldAggregator") \
    .config("spark.ui.port", "4041") \
    .config("spark.master", "local[1]") \
    .config("spark.jars.packages", 
            "io.delta:delta-spark_2.12:3.1.0," + 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. DATA PIPELINE LOGIC
try:
    print("✨ Reading Silver Delta Table...")
    silver_stream_df = spark.readStream \
        .format("delta") \
        .load("./data/silver_market_trades")

    # Grouping by symbol and 1-minute time window for OHLCV
    gold_df = silver_stream_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "1 minute"), 
            col("symbol")
        ) \
        .agg(
            first("price").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            last("price").alias("close"),
            sum("volume").alias("volume")
        )

    print("🚀 Gold Aggregator started. Writing to Delta...")
    
    query = gold_df.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "./checkpoints/gold") \
        .start("./data/gold_market_metrics")

    query.awaitTermination()
except Exception as e:
    print(f"❌ Error: {e}")