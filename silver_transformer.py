import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

# 1. CLEAN SESSION (Matching your working Gold config)
spark = SparkSession.builder \
    .appName("SilverTransformer") \
    .config("spark.ui.port", "4040") \
    .config("spark.master", "local[1]") \
    .config("spark.jars.packages", 
            "io.delta:delta-spark_2.12:3.1.0," + 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. SCHEMA
bronze_schema = StructType([
    StructField("s", StringType(), True),
    StructField("E", LongType(), True),
    StructField("p", StringType(), True),
    StructField("q", StringType(), True),
    StructField("m", BooleanType(), True)
])

try:
    # 3. THROTTLED KAFKA READ
    # maxOffsetsPerTrigger prevents the JVM from exploding on startup
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:29092") \
        .option("subscribe", "bronze_market_trades") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()

    silver_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), bronze_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("E") / 1000)) \
        .withColumn("price", col("p").cast("double")) \
        .withColumn("volume", col("q").cast("double")) \
        .select("timestamp", col("s").alias("symbol"), "price", "volume", "m")

    print("🚀 Silver stream starting with backpressure safety...")
    
    # 4. WRITE WITH TRIGGER
    query = silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoints/silver") \
        .trigger(processingTime='2 seconds') \
        .start("./data/silver_market_trades")

    query.awaitTermination()
except Exception as e:
    print(f"❌ Silver Error: {e}")
finally:
    spark.stop()