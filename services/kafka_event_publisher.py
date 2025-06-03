import logging
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configure Logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaEventPublisher:
    def __init__(self, kafka_config, schema_registry_url, auth_user_info):
        """Initializes Kafka Producer with Avro Schema Registry"""
        self.producer = Producer(kafka_config)

        
        self.schema_registry_client = SchemaRegistryClient({
            "url": schema_registry_url,
            "basic.auth.user.info": auth_user_info
        })
        
        self.serializers = {}  # Cache for Avro serializers

    def get_serializer(self, topic):
        """Fetch and cache Avro serializer for a given topic"""
        if topic in self.serializers:
            return self.serializers[topic]

   
        subject = f"{topic}-value"
        schema = self.schema_registry_client.get_latest_version(subject).schema.schema_str

   
        avro_serializer = AvroSerializer(
            self.schema_registry_client,
            schema,
            lambda obj, ctx: obj  # No transformation needed
        )
        self.serializers[topic] = avro_serializer
        return avro_serializer

    def send_event(self, topic, event_data):
        """Publishes Avro-serialized event to Kafka"""
        key = str(event_data.get("user_id", event_data.get("movie_id", "unknown")))
        key_bytes = key.encode("utf-8")

        avro_serializer = self.get_serializer(topic)

        self.producer.produce(
            topic=topic,
            key=key_bytes,
            value=avro_serializer(event_data, SerializationContext(topic, MessageField.VALUE))
        )
        self.producer.flush()
        logger.info(f"Sent Avro event to {topic}: {event_data}")
