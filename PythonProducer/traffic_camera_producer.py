from kafka import KafkaProducer
import json
import time
import random
import base64
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

def generate_snapshot():
    dummy_text = f"traffic_image_{random.randint(1,100)}"
    return base64.b64encode(dummy_text.encode()).decode()

def generate_camera_data():
    return {
        "id": random.randint(1, 1000),
        "deviceId": f"VH{random.randint(1,5)}",
        "cameraId": f"CAM{random.randint(100,105)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latitude": round(random.uniform(18.4, 18.7), 6),
        "longitude": round(random.uniform(73.7, 74.0), 6),
        "snapshot": generate_snapshot(),
        "area": random.choice(PUNE_ZONES)
    }

while True:
    data = generate_camera_data()
    producer.send("traffic_camera_data", value=data)
    print("Sent:", data)
    time.sleep(5)