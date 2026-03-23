from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import psycopg2

# ----------------------------------------
# 0️⃣ Ensure Postgres Tables Exist
# ----------------------------------------
def ensure_tables_exist():
    conn = psycopg2.connect(
        dbname="banking",
        user="nikitapatel",
        password="Akash29",
        host="localhost"
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
# 1️⃣ Spark Session
# ----------------------------------------
spark = SparkSession.builder \
    .appName("Banking Fraud Detection Pipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------
# 2️⃣ Kafka Transaction Schema
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
# 3️⃣ Read Stream From Kafka
# ----------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# ----------------------------------------
# 4️⃣ Convert Kafka Value to JSON
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
# 5️⃣ Function to Write Batch to Postgres
# ----------------------------------------
def write_to_postgres(batch_df, batch_id, table_name):
    if batch_df.count() > 0:
        print(f"\nProcessing batch {batch_id} for table {table_name}")
        batch_df.show(truncate=False)
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/banking") \
            .option("dbtable", table_name) \
            .option("user", "nikitapatel") \
            .option("password", "Akash29") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

# ----------------------------------------
# 6️⃣ Write all transactions to `transactions`
# ----------------------------------------
all_txn_query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "transactions")) \
    .start()

# ----------------------------------------
# 7️⃣ Filter Fraud Transactions for `fraud_alerts`
# ----------------------------------------
fraud_df = parsed_df.filter(col("amount") > 3000)

fraud_query = fraud_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "fraud_alerts")) \
    .start()

# ----------------------------------------
# 8️⃣ Keep Streams Running
# ----------------------------------------
#all_txn_query.awaitTermination(120)
#fraud_query.awaitTermination(120)

spark.streams.awaitAnyTermination(timeout=120)


