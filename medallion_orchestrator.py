import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, first, last, max, min, sum
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, TimestampType

# 1. SESSION SETUP
spark = SparkSession.builder \
    .appName("MedallionOrchestrator") \
    .config("spark.master", "local[2]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

silver_path = "./data/silver_market_trades"
gold_path = "./data/gold_market_metrics"

silver_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("m", BooleanType(), True)
])

gold_schema = StructType([
    StructField("window_start", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("total_volume", DoubleType(), True)
])

# 2. BOOTSTRAP DELTA TABLES
for path, schema, label in [(silver_path, silver_schema, "Silver"), (gold_path, gold_schema, "Gold")]:
    if not os.path.exists(os.path.join(path, "_delta_log")):
        print(f"📦 Bootstrapping {label} Delta table...")
        if label == "Gold":
            # ✅ FIX: Tell the bootstrap to partition the Gold table by symbol
            spark.createDataFrame([], schema).write.format("delta").partitionBy("symbol").mode("overwrite").save(path)
        else:
            spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)
# 3. SILVER STREAM
bronze_schema = StructType([
    StructField("s", StringType(), True), StructField("E", LongType(), True),
    StructField("p", StringType(), True), StructField("q", StringType(), True),
    StructField("m", BooleanType(), True)
])

silver_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:29092") \
    .option("subscribe", "bronze_market_trades") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

silver_processed = silver_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bronze_schema).alias("data")).select("data.*") \
    .withColumn("timestamp", to_timestamp(col("E") / 1000)) \
    .withColumn("price", col("p").cast("double")) \
    .withColumn("volume", col("q").cast("double")) \
    .select("timestamp", col("s").alias("symbol"), "price", "volume", "m")

silver_query = silver_processed.writeStream.format("delta") \
    .option("checkpointLocation", "./checkpoints/silver") \
    .trigger(processingTime='5 seconds') \
    .start(silver_path)

# 4. GOLD STREAM
print("✨ Initializing Gold Stream...")
gold_source = spark.readStream.format("delta").load(silver_path)

gold_aggregated = gold_source \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute"), col("symbol")) \
    .agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("volume").alias("total_volume")
    ) \
    .select(col("window.start").alias("window_start"), "symbol", "open", "high", "low", "close", "total_volume")

gold_query = gold_aggregated.writeStream.format("delta") \
    .partitionBy("symbol") \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoints/gold") \
    .option("mergeSchema", "true") \
    .trigger(processingTime='10 seconds') \
    .start(gold_path)

print("🚀 Medallion Pipeline is LIVE.")
spark.streams.awaitAnyTermination()