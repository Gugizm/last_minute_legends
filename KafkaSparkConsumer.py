from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

# Snowflake Configuration
SNOWFLAKE_PARAMS = {
    "sfURL": os.getenv("SNOWFLAKE_URL"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
}

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaSparkConsumer:
    def __init__(self, kafka_broker, snowflake_params, topics):
        """Initializes Kafka-Spark Consumer to process real-time events into Snowflake"""
        self.kafka_broker = kafka_broker
        self.snowflake_params = snowflake_params
        self.topics = topics
        self.spark = self._create_spark_session()
        self.schema_map = self._define_schemas()

    def _create_spark_session(self):
        """Create and configure Spark session for Kafka and Snowflake integration"""
        return (
            SparkSession.builder
            .appName("Kafka-Spark-Snowflake-Consumer")
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,"
                    "net.snowflake:spark-snowflake_2.12:2.12.0,"
                    "net.snowflake:snowflake-jdbc:3.13.14")
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.snowflake.sfURL", self.snowflake_params["sfURL"])
            .config("spark.snowflake.user", self.snowflake_params["user"])
            .config("spark.snowflake.password", self.snowflake_params["password"])
            .config("spark.snowflake.database", self.snowflake_params["sfDatabase"])
            .config("spark.snowflake.schema", self.snowflake_params["sfSchema"])
            .config("spark.snowflake.warehouse", self.snowflake_params["sfWarehouse"])
            .getOrCreate()
        )

    def _define_schemas(self):
        """Define JSON Schemas for different Kafka event types"""
        return {
            "added_to_cart_topic": StructType([
                StructField("timestamp", StringType(), True),
                StructField("event_name", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("item_id", StringType(), True),
                StructField("cart_id", StringType(), True),
            ]),
            "checkout_to_cart_topic": StructType([
                StructField("timestamp", StringType(), True),
                StructField("event_name", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("cart_id", StringType(), True),
                StructField("payment_method", StringType(), True),
            ]),
            "consumer_registration_topic": StructType([
                StructField("timestamp", StringType(), True),
                StructField("event_name", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("age", StringType(), True),
                StructField("masked_email", StringType(), True),
                StructField("preferred_language", StringType(), True),
            ]),
            "item_view_topic": StructType([
                StructField("timestamp", StringType(), True),
                StructField("event_name", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("item_id", StringType(), True),
            ]),
            "movies_catalog_enriched": StructType([
                StructField("timestamp", StringType(), True),
                StructField("event_name", StringType(), True),
                StructField("movie_id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("genre", StringType(), True),
                StructField("list_price", FloatType(), True),
            ]),
            "sign_in_topic": StructType([
                StructField("timestamp", StringType(), True),
                StructField("event_name", StringType(), True),
                StructField("user_id", StringType(), True),
            ]),
            "sign_out_topic": StructType([
                StructField("timestamp", StringType(), True),
                StructField("event_name", StringType(), True),
                StructField("user_id", StringType(), True),
            ]),
        }

    def read_kafka_stream(self, topic, schema):
        """Read a Kafka stream and apply the correct schema"""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_broker)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")  # Change to "earliest" for testing full history
            .load()
            .selectExpr("CAST(value AS STRING) as json_str")
            .withColumn("data", from_json(col("json_str"), schema))
            .select("data.*")  # Extract nested fields
        )

    def write_to_snowflake(self, df, snowflake_table):
        """Write streaming data to Snowflake"""
        return (
            df.writeStream
            .outputMode("append")
            .format("snowflake")
            .options(**self.snowflake_params)
            .option("dbtable", snowflake_table)
            .option("checkpointLocation", f"/tmp/snowflake_checkpoint/{snowflake_table}")  # Ensure fault-tolerance
            .start()
        )

    def run(self):
        """Start streaming from Kafka to Snowflake"""
        queries = []
        for topic in self.topics:
            schema = self.schema_map.get(topic)
            if schema:
                df = self.read_kafka_stream(topic, schema)
                snowflake_table = topic.replace("_topic", "")  # Derive table name from topic name
                query = self.write_to_snowflake(df, snowflake_table)
                queries.append(query)
                logger.info(f"✅ Streaming from Kafka topic `{topic}` to Snowflake table `{snowflake_table}`")

        for query in queries:
            query.awaitTermination()

if __name__ == "__main__":
    consumer = KafkaSparkConsumer(KAFKA_BROKER, SNOWFLAKE_PARAMS, [
        "consumer_registration_topic", 
        "sign_in_topic", 
        "sign_out_topic", 
        "item_view_topic", 
        "added_to_cart_topic", 
        "checkout_to_cart_topic", 
        "movies_catalog_enriched"
    ])
    consumer.run()
