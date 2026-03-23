# spark_stream.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import psycopg2
from dotenv import load_dotenv

# ----------------------------------------
# Load environment variables
# ----------------------------------------
load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB", "banking")
POSTGRES_USER = os.getenv("POSTGRES_USER", "nikitapatel")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Akash29")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", 5432)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

# ----------------------------------------
# Ensure Postgres Tables Exist
# ----------------------------------------
def ensure_tables_exist():
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cur = conn.cursor()

    # Transactions table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        transaction_id INT,
        user_id INT,
        amount INT,
        country VARCHAR(10),
        risk_level VARCHAR(20),
        timestamp TIMESTAMP
    )
    """)

    # Fraud alerts table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fraud_alerts (
        id SERIAL PRIMARY KEY,
        transaction_id INT,
        user_id INT,
        amount INT,
        country VARCHAR(10),
        risk_level VARCHAR(20),
        timestamp TIMESTAMP
    )
    """)

    conn.commit()
    cur.close()
    conn.close()

ensure_tables_exist()

# ----------------------------------------
# Spark Session
# ----------------------------------------
spark = SparkSession.builder.appName("Banking Fraud Detection Pipeline").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------
# Kafka Transaction Schema
# ----------------------------------------
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("risk_level", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# ----------------------------------------
# Read Stream From Kafka
# ----------------------------------------
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# ----------------------------------------
# Convert Kafka Value to JSON
# ----------------------------------------
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .select(
        col("transaction_id").cast("int"),
        col("user_id").cast("int"),
        col("amount").cast("int"),
        col("country"),
        col("risk_level"),
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").alias("timestamp")
    )

# ----------------------------------------
# Function to Write Batch to Postgres
# ----------------------------------------
def write_to_postgres(batch_df, batch_id, table_name):
    if batch_df.count() > 0:
        print(f"\nProcessing batch {batch_id} for table {table_name}")
        batch_df.show(truncate=False)
        batch_df.write.format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

# ----------------------------------------
# Write all transactions to `transactions`
# ----------------------------------------
all_txn_query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "transactions")) \
    .start()

# ----------------------------------------
# Filter Fraud Transactions for `fraud_alerts`
# ----------------------------------------
fraud_df = parsed_df.filter(col("amount") > 3000)

fraud_query = fraud_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "fraud_alerts")) \
    .start()

# ----------------------------------------
# Keep Streams Running
# ----------------------------------------
spark.streams.awaitAnyTermination(timeout=120)
