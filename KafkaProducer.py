from confluent_kafka import Producer
import logging
from AvroSerializationManager import AvroSerializationManager

class KafkaProducer:
    def __init__(self, config, schema_registry_url, auth_user_info):
        """
        Initialize the Kafka producer with the provided configuration.

        :param config: Dictionary containing Kafka configuration settings
        :param schema_registry_url: URL of the Schema Registry
        :param auth_user_info: Authentication details for Schema Registry
        """
        self.config = config
        self.producer = None
        self.schema_registry_url = schema_registry_url
        self.auth_user_info = auth_user_info
        self._initialize_producer()
        self.serializers = {}

    def _initialize_producer(self):
        """Initialize the Kafka producer with the configuration."""
        try:
            self.producer = Producer(self.config)
            logging.info("Kafka Producer initialized successfully.")
        except Exception as e:
            logging.error(f"Error initializing Kafka Producer: {e}")
            raise

    def register_schema(self, topic, to_dict):
        """
        Registers a schema and serializer for a specific topic.

        :param topic: Kafka topic associated with the schema
        :param schema_name: Name of the schema file (without .avsc extension)
        :param to_dict: Function to convert an event to a dictionary
        """
        try:
            serializer = AvroSerializationManager(
                self.schema_registry_url, self.auth_user_info, topic, to_dict
            )
            self.serializers[topic] = serializer
            logging.info(f"Registered Avro schema for topic: {topic}")
        except Exception as e:
            logging.error(f"Error registering schema for {topic}: {e}")

    def produce_message(self, topic, message_key, message_value):
        """
        Produce an Avro-serialized message to Kafka.

        :param topic: Kafka topic to send the message to
        :param message_key: Key of the message (optional)
        :param message_value: Dictionary containing the actual event data
        """
        try:
            if topic in self.serializers:
                # avro_serializer = self.serializers.get(topic)
                # logging.error(2)
                # serialized_value = avro_serializer.avro_serializer(message_value)
                # logging.error(3)
                self.producer.produce(topic, key=message_key, value=message_value, callback=self._delivery_report)
                self.producer.poll(1)
                logging.error(f"Sent Avro message to topic {topic}: {message_value}")
            else:
                logging.warning(f"No serializer found for topic: {topic}. Message not sent.")

        except Exception as e:
            logging.error(f"Error producing Avro message: {e}")


    def close(self):
        """Close the producer connection."""
        if self.producer:
            self.producer.flush()
            self._process_delivery_reports()
            logging.info("Kafka Producer connection closed.")
        else:
            logging.warning("Producer is not initialized.")

    @staticmethod
    def _delivery_report(err, msg):
        """Callback function to handle message delivery report."""
        if err is not None:
            logging.error(f"Message delivery failed: {err}")
        else:
            logging.info(f"Message sent to topic {msg.topic()} with key {msg.key()} at offset {msg.offset()}.")

    def _process_delivery_reports(self):
        """Process remaining delivery reports."""
        self.producer.poll(0)
