import os
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "vitals_input")
HEALTHY_TOPIC = os.getenv("HEALTHY_TOPIC", "healthy_vitals")
UNHEALTHY_TOPIC = os.getenv("UNHEALTHY_TOPIC", "unhealthy_vitals")
GROUP_ID = os.getenv("GROUP_ID", "vitals_health_group")
SASL_USERNAME = os.getenv("SASL_USERNAME", "")
SASL_PASSWORD = os.getenv("SASL_PASSWORD", "")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_PLAINTEXT")
SASL_MECHANISM = os.getenv("SASL_MECHANISM", "PLAIN")

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Consumer configuration
consumer_config = {
    'bootstrap_servers': KAFKA_BROKER,
    'group_id': GROUP_ID,
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'security_protocol': SECURITY_PROTOCOL,
    'sasl_mechanism': SASL_MECHANISM,
    'sasl_plain_username': SASL_USERNAME,
    'sasl_plain_password': SASL_PASSWORD,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8'))
}

# Kafka Producer configuration
producer_config = {
    'bootstrap_servers': KAFKA_BROKER,
    'security_protocol': SECURITY_PROTOCOL,
    'sasl_mechanism': SASL_MECHANISM,
    'sasl_plain_username': SASL_USERNAME,
    'sasl_plain_password': SASL_PASSWORD,
    'value_serializer': lambda x: json.dumps(x).encode('utf-8')
}

def is_healthy(vitals):
    """
    Classifies vitals data as healthy or unhealthy based on predefined thresholds.
    """
    body_temp = vitals.get('body_temp')
    heart_rate = vitals.get('heart_rate')
    systolic = vitals.get('systolic')
    diastolic = vitals.get('diastolic')
    breaths = vitals.get('breaths')
    oxygen = vitals.get('oxygen')
    glucose = vitals.get('glucose')

    if not all(isinstance(v, (int, float)) for v in [body_temp, heart_rate, systolic, diastolic, breaths, oxygen, glucose]):
        logging.warning(f"Invalid vitals data: {vitals}.  All values must be numeric.")
        return False

    healthy = (
        97 <= body_temp <= 99 and
        60 <= heart_rate <= 100 and
        90 <= systolic <= 120 and
        60 <= diastolic <= 80 and
        12 <= breaths <= 20 and
        oxygen >= 94 and
        70 <= glucose <= 140
    )
    return healthy

def main():
    """
    Main function to consume messages from Kafka, classify vitals,
    and produce messages to different topics.
    """
    try:
        consumer = KafkaConsumer(INPUT_TOPIC, **consumer_config)
        producer = KafkaProducer(**producer_config)
        logging.info(f"Connected to Kafka broker: {KAFKA_BROKER}")

        for message in consumer:
            try:
                vitals_data = message.value
                logging.info(f"Received vitals data: {vitals_data}")

                if is_healthy(vitals_data):
                    producer.send(HEALTHY_TOPIC, value=vitals_data)
                    logging.info(f"Sent healthy vitals to {HEALTHY_TOPIC}: {vitals_data}")
                else:
                    producer.send(UNHEALTHY_TOPIC, value=vitals_data)
                    logging.info(f"Sent unhealthy vitals to {UNHEALTHY_TOPIC}: {vitals_data}")

                producer.flush()

            except Exception as e:
                logging.error(f"Error processing message: {e}", exc_info=True)

    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}", exc_info=True)
    finally:
        try:
            consumer.close()
        except:
            pass
        try:
            producer.close()
        except:
            pass

if __name__ == "__main__":
    main()
