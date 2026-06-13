from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

DB_URL = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

DB_PROPERTIES = {
    "user":db_user,
    "password":db_password,
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder.appName("MTA-Bulk-Static-Seeder") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

GTFS_Folder = r"C:\Users\archit_sharma\Documents\projack\RAWDATA\gtfs"

gtfs_manifest = {
    "agency.txt":"agency",
    "routes.txt":"routes",
    "shapes.txt":"shapes",
    "stops.txt":"stops",
    "transfers.txt":"transfers",
    "trips.txt":"trips"
}

print ("starting bulk master data ingestion ...")

for file_name, table_name in gtfs_manifest.items():
    file_path = os.path.join(GTFS_Folder,file_name)

    if os.path.exists(file_path):
        print(f"Processing {file_name} -> Loading into database table: {table_name}...")
        df = spark.read.option("header","true")\
            .option("inferSchema","true")\
            .csv(file_path)
        
        df.write.jdbc(
            url=DB_URL,
            table=table_name, 
            mode="overwrite", 
            properties=DB_PROPERTIES
        )
    else:
        print(f"File missing: {file_name}, skipping...")

print("All staging tables populated successfully!")