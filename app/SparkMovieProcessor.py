from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, first, expr, last, regexp_extract, coalesce, lit, trim, lower
from pyspark.sql.window import Window
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os
from typing import Dict, Any
import logging

def to_dict(obj: Any, ctx: Any) -> Dict[str, Any]:
    """Convert object to dictionary for Avro serialization."""
    return obj if isinstance(obj, dict) else obj.__dict__

class SparkMovieProcessor:
    def __init__(self, file_path: str, kafka_bootstrap_servers: str, schema_registry_url: str, schema_subject: str):
        """Initializes SparkMovieProcessor."""
        self.file_path = file_path
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.schema_subject = schema_subject
        
        self.spark = SparkSession.builder \
            .appName("MoviesProcessor") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def process_movies(self) -> None:
        """Reads Movies.txt, cleans data, and sends events to Kafka using Avro."""
        try:
            self.logger.info(f"Reading data from {self.file_path}")
            df = self.spark.read.text(self.file_path)

            # Extract movie_id and fix spacing issue (handles "ITEM1" and "ITEM 1")
            df = df.withColumn(
                "movie_id", 
                expr("CASE WHEN value RLIKE '(?i)^ITEM\\s*\\d+' THEN regexp_extract(value, '(?i)^ITEM\\s*(\\d+)', 1) ELSE NULL END")
            )

            # Forward fill movie_id for grouping
            window_spec = Window.orderBy("value").rowsBetween(Window.unboundedPreceding, 0)
            df = df.withColumn("movie_id", last("movie_id", ignorenulls=True).over(window_spec))

            # Extract key-value pairs dynamically
            df = df.withColumn("key_value", split(col("value"), "=")) \
                   .filter(col("key_value").getItem(1).isNotNull())

            df = df.select(
                col("movie_id"),
                trim(col("key_value").getItem(0)).alias("key"),
                trim(col("key_value").getItem(1)).alias("value")
            )

            # Select only relevant keys (case-insensitive)
            relevant_keys = ["title", "genre", "listprice"]
            df = df.filter(lower(col("key")).isin([k.lower() for k in relevant_keys]))

            # Pivot to convert key-value pairs into structured format
            df = df.groupBy("movie_id").pivot("key").agg(first("value"))

            # Extract price using regex (handles USD format and "$" format)
            df = df.withColumn(
                "list_price",
                coalesce(
                    regexp_extract(col("ListPrice"), r'\$(\d+\.?\d*)', 1).cast("float"),
                    regexp_extract(col("ListPrice"), r'(\d+\.?\d*)USD', 1).cast("float"),
                    lit(0.0)
                )
            )

            # Ensure all fields have valid values
            final_df = df.select(
                coalesce(col("movie_id"), lit("unknown")).alias("movie_id"),
                coalesce(col("Title"), lit("Unknown Title")).alias("title"),
                coalesce(col("Genre"), lit("Unknown Genre")).alias("genre"),
                coalesce(col("list_price"), lit(0.0)).alias("list_price"),
                expr("current_timestamp()").alias("timestamp")
            ).withColumn("event_name", lit("movies_catalog_enriched"))

            # Filter out any remaining invalid movie_ids
            final_df = final_df.filter(col("movie_id").isNotNull() & (col("movie_id") != "unknown"))

            # Convert to row list
            movies = final_df.collect()
            
            self.logger.info(f"Processing {len(movies)} movies")

            # Process in batches for Kafka
            batch_size = 100
            for i in range(0, len(movies), batch_size):
                batch = movies[i:i + batch_size]
                send_batch_to_kafka(
                    batch,
                    self.kafka_bootstrap_servers,
                    self.schema_registry_url,
                    self.schema_subject
                )
            
            self.logger.info("✅ Successfully processed all movies!")
            
        except Exception as e:
            self.logger.error(f"❌ Error processing movies: {str(e)}")
            raise
        finally:
            df.unpersist()

def send_batch_to_kafka(batch, kafka_bootstrap_servers: str, 
                       schema_registry_url: str, schema_subject: str) -> None:
    """Sends a batch of movies to Kafka."""
    logger = logging.getLogger(__name__)
    
    try:
        schema_registry_client = SchemaRegistryClient({
            "url": schema_registry_url,
            "basic.auth.user.info": os.getenv("SCHEMA_REGISTRY_AUTH")
        })

        schema_metadata = schema_registry_client.get_latest_version(schema_subject)
        avro_schema = schema_metadata.schema.schema_str

        avro_serializer = AvroSerializer(
            schema_registry_client,
            avro_schema,
            to_dict
        )

        producer_config = {
            "bootstrap.servers": kafka_bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.getenv("KAFKA_SASL_USERNAME"),
            "sasl.password": os.getenv("KAFKA_SASL_PASSWORD"),
            "value.serializer": avro_serializer,
            "delivery.timeout.ms": 30000,
            "retry.backoff.ms": 1000,
            "message.timeout.ms": 10000
        }
        
        producer = SerializingProducer(producer_config)
        
        for row in batch:
            try:
                # Ensure valid movie_id
                if row["movie_id"] and str(row["movie_id"]).strip():
                    event = {
                        "timestamp": str(row["timestamp"]),
                        "event_name": str(row["event_name"]),
                        "movie_id": str(row["movie_id"]).strip(),
                        "title": str(row["title"]),
                        "genre": str(row["genre"]),
                        "list_price": float(row["list_price"])
                    }
                    
                    producer.produce(
                        topic="movies_catalog_enriched",
                        key=str(row["movie_id"]),
                        value=event,
                        on_delivery=lambda err, msg: _delivery_callback(err, msg, logger)
                    )
                    producer.poll(0)
                else:
                    logger.warning(f"⚠️ Skipping row with invalid movie_id: {row}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing row {row.get('movie_id', 'unknown')}: {str(e)}")
        
        producer.flush()
        
    except Exception as e:
        logger.error(f"❌ Error in Kafka producer setup: {str(e)}")
        raise

def _delivery_callback(err: Any, msg: Any, logger: logging.Logger) -> None:
    """Callback function for Kafka message delivery reports."""
    if err:
        logger.error(f'❌ Message delivery failed: {str(err)}')
    else:
        logger.debug(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

if __name__ == "__main__":
    from Config import producer_conf, schema_registry_url, schema_subject

    processor = SparkMovieProcessor(
        "Movies.txt",
        producer_conf["bootstrap.servers"],
        schema_registry_url,
        schema_subject
    )
    processor.process_movies()
