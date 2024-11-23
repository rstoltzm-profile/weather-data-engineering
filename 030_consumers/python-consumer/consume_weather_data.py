from kafka import KafkaConsumer
import json
import pandas as pd
import logging
from typing import Any, Dict

# Configuration constants
KAFKA_TOPIC = 'weather_data'
KAFKA_SERVER = '172.17.0.3:9092'
DATA_FILE_PATH = '/app/data/transformed_weather_data.csv'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data(data: Dict[str, Any]) -> pd.DataFrame:
    # Example transformation: Convert temperature from Kelvin to Celsius
    data['temperature'] = data['temperature'] - 273.15
    return pd.DataFrame([data])

def write_data(df: pd.DataFrame, file_path: str) -> None:
    try:
        df.to_csv(file_path, mode='a', header=not pd.io.common.file_exists(file_path), index=False)
    except Exception as e:
        logging.error(f"Error writing data to file: {e}")
        raise

def consume_and_transform() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for message in consumer:
        data = message.value
        df = transform_data(data)
        write_data(df, DATA_FILE_PATH)
        logging.info("Data transformed and saved to %s", DATA_FILE_PATH)

if __name__ == "__main__":
    consume_and_transform()