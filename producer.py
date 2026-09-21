import json
import time
import requests
from kafka import KafkaProducer

# Initialize Kafka Producer pointing to the local broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'flight-events'
# OpenSky Network public API for live flight data
url = "https://opensky-network.org/api/states/all"

print("Connecting to OpenSky Network API for live flight data...")

try:
    while True:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            states = data.get('states', [])
            
            if states:
                # Take a batch of the first 10 live flights for streaming
                for state in states[:10]:
                    flight_event = {
                        "icao24": state[0],
                        "callsign": state[1].strip() if state[1] else "UNKNOWN",
                        "origin_country": state[2],
                        "time_position": state[3],
                        "longitude": state[5],
                        "latitude": state[6],
                        "baro_altitude": state[7],
                        "on_ground": state[8],
                        "velocity": state[9]
                    }
                    
                    # Send event to Kafka
                    producer.send(topic_name, value=flight_event)
                    print(f"Sent live flight: {flight_event['callsign']} from {flight_event['origin_country']} at velocity {flight_event['velocity']} m/s")
            else:
                print("No active flights found at the moment.")
        else:
            print(f"API Error: {response.status_code}")
            
        # OpenSky API has rate limits, so we wait 15 seconds
        time.sleep(15)
            
except KeyboardInterrupt:
    print("\nFlight Producer stopped.")
    producer.close()