import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from services.snowflake_query_service import SnowflakeQueryService
from services.kafka_event_publisher import KafkaEventPublisher

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkStreamingProcessor:
    def __init__(self, kafka_publisher: KafkaEventPublisher):
        """Initializes Spark Streaming Processor"""
        self.spark = SparkSession.builder.appName("UserActivityStreaming").getOrCreate()
        self.kafka_publisher = kafka_publisher
        self.snowflake_service = SnowflakeQueryService()

    def process_event(self, event):
        """Processes incoming Kafka events"""
        event_type = event["event_name"]
        user_id = event["user_id"]
        item_id = event.get("item_id")

        if event_type == "sign_in":
            # ✅ Ensure the user exists in Snowflake
            result = self.snowflake_service.fetch_data(f"SELECT 1 FROM users WHERE user_id = '{user_id}'")
            if not result:
                logger.warning(f"❌ Unregistered user {user_id} tried to sign in!")
                return
            
            logger.info(f"User {user_id} signed in.")

        elif event_type == "item_view":
            # ✅ Ensure the movie exists in Snowflake
            result = self.snowflake_service.fetch_data(f"SELECT 1 FROM movies_catalog WHERE movie_id = '{item_id}'")
            if not result:
                logger.warning(f"❌ Invalid Movie {item_id} in event {event_type}")
                return

        self.kafka_publisher.send_event(event)

    def run(self):
        """Starts Spark Streaming"""
        df = self.spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092") \
               .option("subscribe", "user_activity_topic").load()

        parsed_df = df.selectExpr("CAST(value AS STRING)").alias("event")
        parsed_df.foreach(self.process_event)

        self.spark.streams.awaitAnyTermination()
