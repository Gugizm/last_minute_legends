import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType

from services.snowflake_query_service import SnowflakeQueryService
from services.kafka_event_publisher import KafkaEventPublisher
from services.Config import producer_conf, schema_registry_url, auth_user_info

# Configure Logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkStreamingProcessor:
    def __init__(self, kafka_publisher: KafkaEventPublisher):
        """Initializes Spark Streaming Processor"""

        jars = [
            "/usr/local/app/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar",
            "/usr/local/app/jars/snowflake-jdbc-3.22.0.jar",
            "/usr/local/app/jars/spark-snowflake_2.12-3.1.1.jar",
        ]

        self.spark = SparkSession.builder \
            .appName("UserActivityStreaming") \
            .config("spark.jars", ",".join(jars)) \
            .config("spark.driver.extraClassPath", "/usr/local/app/jars/*") \
            .config("spark.executor.extraClassPath", "/usr/local/app/jars/*") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()

        self.kafka_publisher = kafka_publisher
        self.snowflake_service = SnowflakeQueryService()

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

    def run(self):
        """Starts Spark Streaming - Keeps Running"""
        logger.info("Starting Spark Streaming Processor...")

        while True:
            try:
                df = self.spark.readStream.format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:9092") \
                    .option("subscribe", "user_activity_topic") \
                    .load()

                # ✅ Use the correct column name
                parsed_df = df.selectExpr("CAST(value AS STRING) as value")
                parsed_df = parsed_df.withColumn("event", from_json(col("value"), self.event_schema))

                logger.info("✅ Listening for new events...")

                self.spark.streams.awaitAnyTermination()  # Keeps running

            except Exception as e:
                logger.error(f"❌ Streaming Error: {e}", exc_info=True)
                logger.info("🔄 Restarting Spark Streaming...")


if __name__ == "__main__":

    kafka_publisher = KafkaEventPublisher(producer_conf, schema_registry_url, auth_user_info)
    spark_processor = SparkStreamingProcessor(kafka_publisher)
    spark_processor.run()