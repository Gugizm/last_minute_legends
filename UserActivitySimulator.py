import time
import random
import logging
from KafkaProducer import KafkaProducer
from UserEvents import (
    UserRegistrationEvent, 
    SignInEvent, 
    SignOutEvent, 
    ItemViewEvent, 
    AddToCartEvent, 
    CheckoutEvent
)
from UserManager import UserManager
from confluent_kafka.serialization import SerializationContext, MessageField
from pyspark.sql import SparkSession

class UserActivitySimulator:
    """Simulates user activity events and sends to Kafka"""

    def __init__(self, kafka_producer, spark):
        """
        :param kafka_producer: KafkaProducer instance
        :param spark: SparkSession instance
        """
        self.kafka_producer = kafka_producer
        self.spark = spark
        self.cart_ids = []  # Store cart IDs for checkout events
        self.user_manager = UserManager()  # Ensure user uniqueness
        self.movies = self.load_movies()  # Fetch movies dynamically

    def load_movies(self):
        """Fetch movies from Spark DataFrame"""
        try:
            df = self.spark.read.csv("movies.csv", header=True)
            return df.select("movie_id").rdd.flatMap(lambda x: x).collect()
        except Exception as e:
            logging.error(f"❌ Error loading movies: {e}")
            return ["default-movie"]  # Fallback if loading fails

    def generate_event(self):
        """Randomly selects and sends an event"""
        event_classes = [
            UserRegistrationEvent,
            SignInEvent,
            SignOutEvent,
            lambda: ItemViewEvent(self.movies),
            lambda: AddToCartEvent(self.movies),
            lambda: CheckoutEvent(self.cart_ids)
        ]

        event_class = random.choice(event_classes)
        event = event_class() if callable(event_class) else event_class

        # Store cart IDs for checkout use
        if isinstance(event, AddToCartEvent):
            self.cart_ids.append(event.cart_id)

        # Send event to Kafka
        topic_mapping = {
            "consumer_registration": "consumer_registration_topic",
            "sign_in": "sign_in_topic",
            "sign_out": "sign_out_topic",
            "item_view": "item_view_topic",
            "added_to_cart": "added_to_cart_topic",
            "checkout_to_cart": "checkout_to_cart_topic"
        }
        topic = topic_mapping.get(event.event_name)
        self.kafka_producer.register_schema(topic, event.to_dict)
        avro_manager = self.kafka_producer.serializers[topic]
        string_serializer = avro_manager.string_serializer
        key = string_serializer(str(event.user_id))
        serialized_value = avro_manager.avro_serializer(event, SerializationContext(topic, MessageField.VALUE))

        self.kafka_producer.produce_message(topic=topic, message_key=key, message_value=serialized_value)
        logging.info(f"✅ Sent event to {topic}: {event}")

    def start_simulation(self, delay=2):
        """Continuously generate user activity events"""
        try:
            while True:
                self.generate_event()
                time.sleep(delay)
        except KeyboardInterrupt:
            logging.info("🛑 Simulation stopped by user.")

if __name__ == "__main__":
    from Config import producer_conf, schema_registry_url, auth_user_info

    # Start Spark session
    spark = SparkSession.builder.appName("UserActivitySimulator").getOrCreate()
    
    # Start Kafka Producer
    producer = KafkaProducer(producer_conf, schema_registry_url, auth_user_info)

    simulator = UserActivitySimulator(producer, spark)
    simulator.start_simulation(delay=1)

    producer.close()
    spark.stop()
