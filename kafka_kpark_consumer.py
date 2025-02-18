from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.avro.functions import from_avro

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToSnowflake") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,net.snowflake:spark-snowflake_2.12:2.11.0") \
    .getOrCreate()

# Kafka Broker & Topics
KAFKA_BOOTSTRAP_SERVERS = "your_kafka_broker:9092"
TOPICS = [
    "consumer_registration_topic",
    "sign_in_topic",
    "item_view_topic",
    "checkout_topic",
    "added_to_cart",
    "checkout_to_cart",
    "movies_catalog_enriched"
]

# Define Kafka Schema (AVRO Representation)
event_schemas = {
    "consumer_registration_topic": StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("masked_email", StringType(), True),
        StructField("preferred_language", StringType(), True),
    ]),
    
    "sign_in_topic": StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("user_id", StringType(), True),
    ]),
    
    "item_view_topic": StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("item_id", StringType(), True),
    ]),

    "checkout_topic": StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("cart_id", StringType(), True),
        StructField("payment_method", StringType(), True),
    ]),

    "added_to_cart": StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("item_id", StringType(), True),
        StructField("cart_id", StringType(), True),
    ]),

    "checkout_to_cart": StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("cart_id", StringType(), True),
    ]),

    "movies_catalog_enriched": StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("movie_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("list_price", FloatType(), True),
    ])
}

# Read Kafka Stream
def read_kafka_topic(topic):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df = df.selectExpr("CAST(value AS STRING) as json_value")
    return df.select(from_json(col("json_value"), event_schemas[topic]).alias("data")).select("data.*")


# Process Each Topic Stream
streams = {}
for topic in TOPICS:
    streams[topic] = read_kafka_topic(topic)

# Snowflake Connection Options
sf_options = {
    "sfURL": "https://your_snowflake_account.snowflakecomputing.com",
    "sfDatabase": "KAFKA_EVENTS",
    "sfSchema": "STREAMING_DATA",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "user": "your_username",
    "password": "your_password"
}

# Write to Snowflake Function
def write_to_snowflake(df, table_name):
    return df.writeStream \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name) \
        .option("checkpointLocation", f"/tmp/snowflake_checkpoint/{table_name}") \
        .outputMode("append") \
        .start()


# Start Streaming & Load Data to Snowflake
write_to_snowflake(streams["consumer_registration_topic"], "consumer_events")
write_to_snowflake(streams["sign_in_topic"], "user_auth_events")
write_to_snowflake(streams["item_view_topic"], "item_view_events")
write_to_snowflake(streams["checkout_topic"], "checkout_events")
write_to_snowflake(streams["added_to_cart"], "cart_events")
write_to_snowflake(streams["checkout_to_cart"], "checkout_to_cart_events")
write_to_snowflake(streams["movies_catalog_enriched"], "movie_catalog_events")

# Await Termination
spark.streams.awaitAnyTermination()
