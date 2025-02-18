import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType
from services.snowflake_query_service import SnowflakeQueryService
from services.kafka_event_publisher import KafkaEventPublisher

# Configure Logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkStreamingProcessor:
    def __init__(self, kafka_publisher: KafkaEventPublisher):
        """Initializes Spark Streaming Processor"""
        self.spark = SparkSession.builder.appName("UserActivityStreaming").getOrCreate()
        self.kafka_publisher = kafka_publisher
        self.snowflake_service = SnowflakeQueryService()

        # ✅ Define the schema for Kafka messages
        self.event_schema = StructType() \
            .add("timestamp", StringType()) \
            .add("event_name", StringType()) \
            .add("user_id", StringType()) \
            .add("item_id", StringType(), True) \
            .add("cart_id", StringType(), True) \
            .add("payment_method", StringType(), True) \
            .add("title", StringType(), True) \
            .add("genre", StringType(), True) \
            .add("list_price", FloatType(), True)

    def process_event(self, event):
        """Processes incoming Kafka events"""
        event_type = event["event_name"]
        user_id = event["user_id"]
        item_id = event.get("item_id")

        # ✅ Ensure the user exists in Snowflake
        if event_type in ["sign_in", "sign_out"]:
            result = self.snowflake_service.fetch_data(f"SELECT 1 FROM KAFKA_EVENTS.STREAMING_DATA.consumer_events WHERE user_id = '{user_id}'")
            if not result:
                logger.warning(f"❌ Unregistered user {user_id} attempted {event_type}!")
                return

            logger.info(f"✅ User {user_id} {event_type} event processed.")

        # ✅ Ensure the movie exists in Snowflake before processing item-related events
        elif event_type in ["item_view", "added_to_cart", "checkout_to_cart"]:
            result = self.snowflake_service.fetch_data(f"SELECT 1 FROM KAFKA_EVENTS.STREAMING_DATA.movie_catalog_events WHERE movie_id = '{item_id}'")
            if not result:
                logger.warning(f"❌ Invalid Movie {item_id} in event {event_type}")
                return

            logger.info(f"✅ Movie {item_id} exists, processing {event_type} event.")

        # ✅ Send processed event to Kafka
        self.kafka_publisher.send_event("processed_events_topic", event)

        # ✅ Save the event into Snowflake
        insert_query = f"""
        INSERT INTO KAFKA_EVENTS.STREAMING_DATA.{event_type}_events 
        (timestamp, event_name, user_id, item_id, cart_id, payment_method, title, genre, list_price)
        VALUES 
        ('{event['timestamp']}', '{event['event_name']}', '{event['user_id']}', 
        '{event.get('item_id', 'NULL')}', '{event.get('cart_id', 'NULL')}', '{event.get('payment_method', 'NULL')}',
        '{event.get('title', 'NULL')}', '{event.get('genre', 'NULL')}', {event.get('list_price', 0.0)});
        """
        self.snowflake_service.execute_query(insert_query)
        logger.info(f"✅ Saved {event_type} event into Snowflake.")

    def run(self):
        """Starts Spark Streaming"""
        df = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "user_activity_topic") \
            .load()

        # ✅ Parse Kafka messages as JSON
        parsed_df = df.selectExpr("CAST(value AS STRING)").alias("event")
        parsed_df = parsed_df.withColumn("event", from_json(col("event"), self.event_schema))

        # ✅ Process each event
        parsed_df.foreach(lambda row: self.process_event(row["event"]))

        self.spark.streams.awaitAnyTermination()
