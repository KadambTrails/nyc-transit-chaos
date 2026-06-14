import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, when

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_URL = os.getenv("DB_URL")

def write_to_postgis(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    batch_df.select(
        col("train_id"),
        col("route_id"),
        col("current_status"),
        col("current_stop_id"),
        col("event_timestamp"),
        col("final_lat"),
        col("final_lon")
    ).write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "staging_subway_stream") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("NYC-Transit-Chaos-Consumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    kafka_schema = StructType([
        StructField("train_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("current_status", StringType(), True),
        StructField("current_stop_id", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])

    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "mta-subway-raw") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), kafka_schema).alias("data")) \
        .select("data.*")

    # Dynamic loading for the static lookup table as well
    static_stops_df = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "stops") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    enriched_stream = parsed_stream.join(
        static_stops_df, 
        parsed_stream.current_stop_id == static_stops_df.stop_id, 
        "left"
    ).withColumn(
        "final_lat", 
        when(col("latitude") == 0.0, col("static_lat")).otherwise(col("latitude"))
    ).withColumn(
        "final_lon", 
        when(col("longitude") == 0.0, col("static_lon")).otherwise(col("longitude"))
    )

    query = enriched_stream.writeStream \
        .foreachBatch(write_to_postgis) \
        .outputMode("append") \
        .start()

    query.awaitTermination()