import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructType,
)

# ---------------------------------------------------------
# Windows Native Hadoop Setup (winutils.exe & hadoop.dll)
# ---------------------------------------------------------
if os.name == "nt":
    hadoop_dir = os.path.join(os.path.expanduser("~"), "hadoop")
    bin_dir = os.path.join(hadoop_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    os.environ["HADOOP_HOME"] = hadoop_dir

    # Append hadoop/bin to system PATH so hadoop.dll is visible
    if bin_dir not in os.environ["PATH"]:
        os.environ["PATH"] = bin_dir + ";" + os.environ["PATH"]

    base_url = "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/"
    for binary_name in ["winutils.exe", "hadoop.dll"]:
        target_path = os.path.join(bin_dir, binary_name)
        if not os.path.exists(target_path):
            print(f"Downloading {binary_name} for Windows compatibility...")
            try:
                urllib.request.urlretrieve(base_url + binary_name, target_path)
                print(f"{binary_name} downloaded successfully!")
            except Exception as e:
                print(f"Error downloading {binary_name}: {e}")

# ---------------------------------------------------------
# Initialize Spark Session
# ---------------------------------------------------------
spark = (
    SparkSession.builder.appName("FlightStreamConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.postgresql:postgresql:42.6.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# Define Schema for Kafka Flight Events
# ---------------------------------------------------------
schema = (
    StructType()
    .add("icao24", StringType())
    .add("callsign", StringType())
    .add("origin_country", StringType())
    .add("time_position", DoubleType())
    .add("longitude", DoubleType())
    .add("latitude", DoubleType())
    .add("baro_altitude", DoubleType())
    .add("on_ground", BooleanType())
    .add("velocity", DoubleType())
)

# ---------------------------------------------------------
# Read from Kafka Stream
# ---------------------------------------------------------
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "flight-events")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON payload
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ---------------------------------------------------------
# Micro-Batch Sink to PostgreSQL
# ---------------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.write.format("jdbc").option(
            "url", "jdbc:postgresql://localhost:5432/flightdb"
        ).option("dbtable", "live_flights").option("user", "postgres").option(
            "password", "postgres"
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()
        print(f"--- Micro-batch {batch_id} written to PostgreSQL ---")

# Explicit checkpoint directory
checkpoint_dir = os.path.join(os.getcwd(), "spark_checkpoint")

query = (
    parsed_df.writeStream.foreachBatch(write_to_postgres)
    .option("checkpointLocation", checkpoint_dir)
    .outputMode("append")
    .start()
)

print("Consumer started. Waiting for incoming flight data from Kafka...")
query.awaitTermination()