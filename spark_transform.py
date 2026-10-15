from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def main():
    print("🚀 Initializing Incremental Apache Spark Session...")
    
    spark = SparkSession.builder \
        .appName("CryptoIncrementalTransformation") \
        .master("spark://spark-master:7077") \
        .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/crypto_data.bitcoin_prices") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    # 1. QUERY POSTGRESQL FOR THE LATEST RECORD TIMESTAMP (The Delta Marker)
    print("🔍 Fetching the latest watermark timestamp from PostgreSQL Warehouse...")
    try:
        # Querying an inline subquery to get the max extraction_time
        watermark_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/data_warehouse") \
            .option("dbtable", "(SELECT MAX(extraction_time) as max_time FROM fact_bitcoin_rates) as watermark") \
            .option("user", "de_user") \
            .option("password", "de_password") \
            .option("driver", "org.postgresql.Driver") \
            .load()
            
        latest_timestamp = watermark_df.first()["max_time"]
    except Exception as e:
        print("ℹ️ Destination table might be empty or uninitialized. Defaulting to full load extraction.")
        latest_timestamp = None

    # 2. EXTRACT ONLY NEW RECORDS FROM MONGODB (Incremental Pushdown Filter)
    print(f"📥 Extracting records from MongoDB (Watermark Filter: > {latest_timestamp})...")
    
    if latest_timestamp:
        # Convert the timestamp object into the exact ISO string format matching your MongoDB keys
        ts_string = latest_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Spark pushes this filter down directly to MongoDB so only matching documents cross the network!
        raw_df = spark.read.format("mongodb") \
            .load() \
            .filter(col("timestamp") > ts_string)
    else:
        # Fallback to full load extraction if PostgreSQL is completely blank
        raw_df = spark.read.format("mongodb").load()

    # 3. VERIFY IF NEW DELTA DATA EXISTS
    if raw_df.isEmpty():
        print("💤 No new crypto metrics found since the last execution. Pipeline shutting down cleanly.")
        spark.stop()
        return

    #Set a flag to cache results to memory after the next action count()
    raw_df.cache()

    # 4. Transform and flatten the fresh records (Omit 'id' so PostgreSQL auto-increments it)
    print(f"⚡ Processing {raw_df.count()} new delta rows...")
    transformed_df = raw_df.select(
        col("timestamp").cast("timestamp").alias("extraction_time"),
        col("currency"),
        col("rate_usd").cast("double").alias("price_usd"),
        col("rate_gbp").cast("double").alias("price_gbp")
    ).withColumn("processed_at", current_timestamp())

    # 5. Append rows directly into the final analytical table
    # (The database constraint acts as a defensive guard and will flag duplicates here)
    print("📤 Loading clean incremental rows into PostgreSQL Data Warehouse...")
    transformed_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/data_warehouse") \
        .option("dbtable", "fact_bitcoin_rates") \
        .option("user", "de_user") \
        .option("password", "de_password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print("✅ Incremental batch transformation sequence finished successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
