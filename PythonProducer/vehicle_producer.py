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

def generate_vehicle_data():
    return {
        "id": random.randint(1, 1000),
        "deviceId": f"VH{random.randint(1,5)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latitude": round(random.uniform(18.4, 18.7), 6),
        "longitude": round(random.uniform(73.7, 74.0), 6),
        "speed": random.randint(20, 80),
        "direction": random.choice(["N", "S", "E", "W"]),
        "brand": random.choice(["Tata", "Hyundai", "Mahindra","BMW","Mercedes"]),
        "model": random.choice(["X1", "X2", "X3"]),
        "year": random.randint(2015, 2023),
        "fuelType": random.choice(["Petrol", "Diesel"]),
        "area": random.choice(PUNE_ZONES)
    }

while True:
    data = generate_vehicle_data()
    producer.send("vehicle_data", value=data)
    print("Sent:", data)
    time.sleep(5)