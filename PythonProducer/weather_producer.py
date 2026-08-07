from kafka import KafkaProducer
import json
import time
import random
import requests
from datetime import datetime
import os

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


API_KEY = os.getenv("OPENWEATHER_API_KEY", "MY_OPENWEATHER_API_KEY")

PUNE_ZONES = [
    "Hinjawadi", "Baner", "Wakad", "Aundh", "Shivajinagar",
    "Kothrud", "Swargate", "Katraj", "Hadapsar", "Kharadi",
    "PCMC", "Camp"
]

LAT = 18.5204
LON = 73.8567

def get_weather():
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()

    return {
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather_condition": data["weather"][0]["main"],
        "wind_speed": data["wind"]["speed"]
    }

def generate_weather_data():
    api_data = get_weather()

   
    temperature = api_data["temperature"] + random.uniform(-1.5, 1.5)
    humidity = api_data["humidity"] + random.randint(-5, 5)
    wind_speed = api_data["wind_speed"] + random.uniform(-1, 1)

    weather_condition = random.choice([
        api_data["weather_condition"],
        "Clouds",
        "Clear",
        "Rain"
    ])

    return {
        "id": random.randint(1, 1000),
        "deviceId": f"WH{random.randint(1,3)}",
        "area": random.choice(PUNE_ZONES),
        "temperature": round(temperature, 2),
        "humidity": max(0, min(100, humidity)),  # keep valid range
        "weather_condition": weather_condition,
        "wind_speed": round(wind_speed, 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

while True:
    data = generate_weather_data()
    producer.send("weather_punedata", value=data)
    print("Sent:", data)
    time.sleep(10)