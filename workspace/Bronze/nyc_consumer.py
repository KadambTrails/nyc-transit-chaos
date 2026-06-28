import os
import redis
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_URL = os.getenv("DB_URL")


redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0, socket_timeout=2)

def write_to_postgis(batch_df, batch_id):
    if not batch_df.take(1):
        return

    transformed_df = batch_df.select(
        col("train_id"),
        col("route_id"),
        col("current_status"),
        col("current_stop_id"),
        from_unixtime(col("timestamp")).cast("timestamp").alias("event_timestamp"),
        col("final_lat"),
        col("final_lon")
    )

    try:
       
        transformed_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "staging_subway_stream") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
            
        print(f"Batch {batch_id}: Successfully processed by PostGIS Game Engine.")
        
        
        backlog_length = redis_client.llen("subway_chaos_backlog")
        if backlog_length > 0:
            print(f"[SELF-HEALING] Found {backlog_length} records inside the Redis backup tank. Initiating BULK recovery...")
            
            bulk_records = []
            # Pull everything out of Redis into a local memory array quickly
            while redis_client.llen("subway_chaos_backlog") > 0:
                raw_record = redis_client.lpop("subway_chaos_backlog")
                if raw_record:
                    bulk_records.append(json.loads(raw_record))
            
            if bulk_records:
                # cast the event_timestamp column back into a proper timestamp type during recovery
                drain_df = spark.createDataFrame(bulk_records, batch_df.schema) \
                    .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
                
                drain_df.select(
                    col("train_id"), col("route_id"), col("current_status"), col("current_stop_id"),
                    col("event_timestamp"), col("final_lat"), col("final_lon")
                ).write \
                    .format("jdbc") \
                    .option("url", DB_URL) \
                    .option("dbtable", "staging_subway_stream") \
                    .option("user", DB_USER) \
                    .option("password", DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                    
                print(f"[SELF-HEALING] Successfully flushed {len(bulk_records)} backup rows to PostGIS in a single batch transaction!")

    except Exception as database_crash_error:
        
        print(f"PostGIS is down! Diverting Batch {batch_id} directly to Redis RAM storage...")
        
        records_to_save = [row.asDict() for row in transformed_df.collect()]
        
        for record in records_to_save:
            # Convert datetime to string for JSON compliance
            if record.get('event_timestamp'):
                record['event_timestamp'] = record['event_timestamp'].isoformat()
            redis_client.rpush("subway_chaos_backlog", json.dumps(record))
            
        print(f"Successfully secured {len(records_to_save)} records safely inside Redis.")    # Peek at the memory bits to verify if data actually exists in this micro-batch
    if not batch_df.take(1):
        return

    # Transform the dataframe batch into an optimized string-ready format
    transformed_df = batch_df.select(
        col("train_id"),
        col("route_id"),
        col("current_status"),
        col("current_stop_id"),
        from_unixtime(col("timestamp")).cast("timestamp").alias("event_timestamp"),
        col("final_lat"),
        col("final_lon")
    )

    try:
        transformed_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "staging_subway_stream") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
            
        print(f"Batch {batch_id}: Successfully processed by PostGIS Game Engine.")
        
        # check if Redis contains a backlog from a previous crash
        backlog_length = redis_client.llen("subway_chaos_backlog")
        if backlog_length > 0:
            print(f"Found {backlog_length} records inside the Redis backup tank. Initiating recovery drain...")
            
            # Continuously pop the oldest records out from the front of the queue (FIFO)
            while redis_client.llen("subway_chaos_backlog") > 0:
                raw_record = redis_client.lpop("subway_chaos_backlog")
                parsed_record = json.loads(raw_record)
                
                
                drain_df = spark.createDataFrame([parsed_record], transformed_df.schema)
                
                drain_df.write \
                    .format("jdbc") \
                    .option("url", DB_URL) \
                    .option("dbtable", "staging_subway_stream") \
                    .option("user", DB_USER) \
                    .option("password", DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                    
            print("Redis holding tank completely drained. Pipeline caught up with zero data loss!")

    except Exception as database_crash_error:
        print(f"PostGIS is down, Diverting Batch {batch_id} directly to Redis RAM storage...")
        
        # Collect the dataframe rows back to the driver as Python objects
        records_to_save = [row.asDict() for row in transformed_df.collect()]
        
        for record in records_to_save:
            # Convert datetime objects to clean strings so JSON serializer doesn't break
            if record.get('event_timestamp'):
                record['event_timestamp'] = record['event_timestamp'].isoformat()
                
            # Efficiently push serialized JSON strings to the end of our Redis queue
            redis_client.rpush("subway_chaos_backlog", json.dumps(record))
            
        print(f" Successfully secured {len(records_to_save)} records safely inside Redis.")

if __name__ == "__main__":
    
    # Spark Session configured with custom performance parameters
    spark = SparkSession.builder \
        .appName("NYC-Transit-Chaos-Consumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.6.0")\
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.streaming.minBatchesToRetain", "10") \
        .config("spark.kafka.consumer.cache.capacity", "64") \
        .config("spark.sql.streaming.stopTimeout", "15s") \
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
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "mta-subway-raw") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), kafka_schema).alias("data")) \
        .select("data.*")

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
        when(col("latitude") == 0.0, col("stop_lat")).otherwise(col("latitude"))
    ).withColumn(
        "final_lon", 
        when(col("longitude") == 0.0, col("stop_lon")).otherwise(col("longitude"))
    )

    query = enriched_stream.writeStream \
        .foreachBatch(write_to_postgis) \
        .outputMode("append") \
        .trigger(processingTime="1 second") \
        .start()

    query.awaitTermination()