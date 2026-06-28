import time
import random
import docker

try:
    client = docker.from_env()
except Exception as e:
    print("Could not connect to Docker Engine.")
    exit(1)

def attack():
    
    while True:
        # Wait a random duration between 45 to 90 seconds before attacking
        sleep_time = random.randint(45, 90)
        print(f"\nAttack stopped for {sleep_time} seconds")
        time.sleep(sleep_time)
        
        try:
            # Locate our active spatial database container
            postgis_container = client.containers.get("nyc-transit-chaos-postgis-1")
            
            print("\n killing the PostGIS Database Container!")
            postgis_container.stop()
            print("PostGIS container successfully killed mid-stream.")
            
            print("Leaving database offline for 30 seconds to test resilience...")
            time.sleep(30)
            
            print("Auto-Recovery initiated. Resurrecting PostGIS Container...")
            postgis_container.start()
            print("PostGIS container is back online and running healthy.\n")
            
        except docker.errors.NotFound:
            print("couldn't find a running container")
        except Exception as e:
            print(f"encountered an error: {str(e)}")

if __name__ == "__main__":
    attack()