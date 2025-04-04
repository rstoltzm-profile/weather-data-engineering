import requests
import pandas as pd
import datetime
import time
import yaml
import logging
from kafka import KafkaProducer
import json
from typing import Any, Dict, Optional

# Configuration constants
CONFIG_FILE = '/app/config.yaml'
CITY = 'Phoenix'
URL_TEMPLATE = 'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
SCHEMA_FILE = '/app/schema.yaml'
KAFKA_TOPIC = 'weather_data'
KAFKA_SERVER = '172.17.0.3:9092'
SLEEP_INTERVAL = 60  # in seconds

CITIES = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "San Jose"
]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file: str) -> Dict[str, Any]:
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        raise

def load_schema(schema_file: str) -> Dict[str, Any]:
    try:
        with open(schema_file, 'r') as file:
            schema = yaml.safe_load(file)
        return schema['schema']
    except Exception as e:
        logging.error(f"Error loading schema: {e}")
        raise

def get_weather_data(url: str) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        return None

def send_to_kafka(producer: KafkaProducer, topic: str, data: Dict[str, Any]) -> None:
    try:
        producer.send(topic, value=data)
        producer.flush()
    except Exception as e:
        logging.error(f"Error sending data to Kafka: {e}")
        raise

def collect_weather_data() -> None:
    config = load_config(CONFIG_FILE)
    api_key = config['api_key']
    schema = load_schema(SCHEMA_FILE)
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    while True:
        for city in CITIES:
            url = URL_TEMPLATE.format(city=city, api_key=api_key)
            data = get_weather_data(url)
            if data:
                weather = {
                    'city': data['name'],
                    'temperature': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'pressure': data['main']['pressure'],
                    'weather': data['weather'][0]['description'],
                    'main': data['weather'][0]['main'],
                    'temp_min': data['main']['temp_min'],
                    'temp_max': data['main']['temp_max'],
                    'wind_speed': data['wind']['speed'],
                    'rain_1h': data['rain']['1h'] if 'rain' in data else 0,
                    'snow_1h': data['snow']['1h'] if 'snow' in data else 0,
                    'coord_lon': data['coord']['lon'],
                    'coord_lat': data['coord']['lat'],
                    'date': datetime.datetime.now().isoformat()  # Convert datetime to ISO format string
                }
                send_to_kafka(producer, KAFKA_TOPIC, weather)
                logging.info("Data collected for %s and sent to Kafka topic %s", city, KAFKA_TOPIC)
            else:
                logging.warning("Failed to retrieve data for %s", city)
        
        # Wait for the specified interval before collecting data again
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    collect_weather_data()