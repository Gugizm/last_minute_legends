import logging
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure Logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaEventPublisher:
    def __init__(self, kafka_config):
        """Initializes Kafka Producer"""
        self.producer = Producer(kafka_config)
        self.serializers = {}  # Cache for Avro serializers

    def send_event(self, topic, event_data):
        """Publishes event to Kafka"""
        key = str(event_data.get("user_id", event_data.get("movie_id", "unknown")))
        key_bytes = key.encode("utf-8")

        self.producer.produce(topic, key=key_bytes, value=str(event_data))
        self.producer.flush()
        logger.info(f"✅ Sent event to {topic}: {event_data}")
