from kafka import KafkaProducer
import requests
import time
import json
import logging

KAFKA_BROKER = 'kafka:9092'
TOPIC = 'input'
STREAM_URL = 'https://stream.wikimedia.org/v2/stream/page-create'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_stream_data():
    while True:
        try:
            response = requests.get(STREAM_URL, stream=True, timeout=10)
            for line in response.iter_lines():
                if line:
                    yield line
        except requests.exceptions.RequestException as e:
            logger.error(f"Error during streaming: {e}")
            logger.info("Retrying in 5 seconds...")
            time.sleep(5)

def transform_to_json(byte_data):
    str_data = byte_data.decode('utf-8')
    if str_data.startswith("data: "):
        json_str = str_data[6:]
        json_data = json.loads(json_str)
        return json_data
    return None

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for byte_data in get_stream_data():
        json_data = transform_to_json(byte_data)
        if json_data:
            producer.send(TOPIC, json_data)
        time.sleep(1)

if __name__ == "__main__":
    main()
