import os
import time
import json
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "mta-subway-raw"

MTA_FEED_URLS = {
    "1234567_G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "ACE": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "NQRW": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "BDFM": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm"
}

def initialise_producer():
    print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=5000,
        max_block_ms=5000
    )

def stream_mta_data(producer):
    print("MTA Live Ingestion Stream Started...")

    while True:
        start_time = time.time()
        total_records = 0

        for feed_name, url in MTA_FEED_URLS.items():
            try:
                response = requests.get(url,timeout=5)
                if response.status_code != 200:
                    print(f" Feed {feed_name} returned status code {response.status_code}")
                    continue

                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)

                for entity in feed.entity:
                    if entity.HasField('vehicle'):
                        v_data = entity.vehicle

                        payload = {
                            "train_id": v_data.trip.trip_id,
                            "route_id": v_data.trip.route_id,
                            "latitude": float(v_data.position.latitude),
                            "longitude": float(v_data.position.longitude),
                            "current_status": gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(v_data.current_status),
                            "timestamp": int(v_data.timestamp) if v_data.timestamp else int(time.time())
                        }

                        producer.send(TOPIC_NAME, value=payload)
                        total_records += 1

            except KafkaError as ke:
                print(f"[KAFKA ERROR] Broker unreachable: {ke}. Backing off...")
                time.sleep(5)
                break
            except Exception as e:
                print(f"Error processing feed {feed_name}: {e}")
                
        print(f"[INGEST] Successfully pushed {total_records} updates to Kafka. Sleeping for 15s...")
        
        # Enforce polite polling loop matching MTA update schedules
        elapsed = time.time() - start_time
        time.sleep(max(1, 15 - elapsed))

if __name__ == "__main__":
    try:
        producer_instance = initialise_producer()
        stream_mta_data(producer_instance)
    except KeyboardInterrupt:
        print("\nIngestor safely shut down.")