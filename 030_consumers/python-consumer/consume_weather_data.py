from kafka import KafkaConsumer, KafkaProducer
import json
import logging
from typing import Any, Dict

# Configuration constants
KAFKA_CONSUME_TOPIC = 'weather_data'
KAFKA_PRODUCE_TOPIC = 'weather_data_processed'
KAFKA_SERVER = '172.17.0.3:9092'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data(data: Dict[str, Any]) -> Dict[str, Any]:
    # Example transformation: Convert temperature from Kelvin to Celsius
    data['temperature'] = data['temperature'] - 273.15
    return data

def consume_and_transform() -> None:
    consumer = KafkaConsumer(
        KAFKA_CONSUME_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for message in consumer:
        data = message.value
        transformed_data = transform_data(data)
        producer.send(KAFKA_PRODUCE_TOPIC, value=transformed_data)
        producer.flush()
        logging.info("Data transformed and sent to Kafka topic %s", KAFKA_PRODUCE_TOPIC)

if __name__ == "__main__":
    consume_and_transform()