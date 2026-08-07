from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PUNE_ZONES = [
    "Hinjawadi", "Baner", "Wakad", "Aundh", "Shivajinagar",
    "Kothrud", "Swargate", "Katraj", "Hadapsar", "Kharadi",
    "PCMC", "Camp"
]

INCIDENT_TYPES = [
    "accident",
    "pothole",
    "traffic_jam",
    "police_check",
    "roadblock",
    "toll_plaza_delay",
    "vip_movement"
]

def generate_emergency_data():
    return {
        "id": random.randint(1, 1000),
        "incidentId": random.choice(INCIDENT_TYPES),
        "deviceId": f"VH{random.randint(1,5)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latitude": round(random.uniform(18.4, 18.7), 6),
        "longitude": round(random.uniform(73.7, 74.0), 6),
        "status": random.choice(["active", "resolved"]),
        "area": random.choice(PUNE_ZONES)
    }

while True:
    data = generate_emergency_data()
    producer.send("emergency_data", value=data)
    print("Sent:", data)
    time.sleep(4)