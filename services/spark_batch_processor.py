# SparkBatchProcessor.py
import os
import time
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, split, when, first
from services.snowflake_query_service import SnowflakeQueryService
from services.kafka_event_publisher import KafkaEventPublisher
from UserEvents import MovieCatalogEvent

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkBatchProcessor:
    def __init__(self, file_path: str, kafka_publisher: KafkaEventPublisher):
        """Initializes Spark Batch Processor"""
        self.spark = SparkSession.builder.appName("MoviesBatchProcessing").getOrCreate()
        self.file_path = file_path
        self.kafka_publisher = kafka_publisher
        self.snowflake_service = SnowflakeQueryService()

    def read_movies_data(self):
        """Reads Movies.txt and extracts structured data"""
        logger.info("Reading Movies.txt file...")
        df = self.spark.read.text(self.file_path).filter(col("value").isNotNull() & (col("value") != ""))
        
        # Add partitioning by a computed column to improve window performance
        df = df.withColumn("partition_id", F.spark_partition_id())
        
        window_spec = Window.partitionBy("partition_id").orderBy(F.monotonically_increasing_id())
        df = df.withColumn("ItemID", F.sum(when(col("value").startswith("ITEM "), 1).otherwise(0))
                           .over(window_spec))

        df = df.filter(col("value").contains("=")).withColumn("split_array", split(col("value"), "=")) \
               .withColumn("Column", F.trim(col("split_array").getItem(0))) \
               .withColumn("Value", F.trim(col("split_array").getItem(1))) \
               .drop("split_array", "partition_id")

        df = df.groupBy("ItemID").pivot("Column").agg(first(col("Value")))
        return df

    def clean_data(self, df):
        """
        Cleans extracted data:
        - Extracts numeric ListPrice values.
        - Handles missing values.
        - Ensures correct data types.
        """
        def extract_price(price):
            """Extracts and cleans the numeric price from strings like '899USD$8.99'"""
            if not price:
                return 0.0
            parts = price.split("$")  # ✅ Split by `$`
            try:
                return float(parts[-1]) if len(parts) > 1 else 0.0  # ✅ Get last numeric value
            except ValueError:
                return 0.0  # ✅ Default if conversion fails

        logger.info("Applying price extraction transformation...")

        # ✅ Apply Price Extraction Using UDF
        price_udf = F.udf(lambda price: extract_price(price), "float")

        if "ListPrice" in df.columns:
            df = df.withColumn("ListPrice", price_udf(col("ListPrice")))

        # ✅ Ensure required fields exist
        df = df.fillna({
            "Title": "Unknown",
            "ListPrice": 0.0,
            "Genre": "Unknown Genre"
        })

        logger.info("✅ Data cleaning completed.")
        return df

    def process_new_movies(self):
        """Checks for new movies, cleans data, and sends events to Kafka"""
        df = self.read_movies_data()
        df = self.clean_data(df)

        query = "SELECT movie_id FROM KAFKA_EVENTS.STREAMING_DATA.movie_catalog_events;"
        existing_movies = {row["movie_id"] for row in self.snowflake_service.fetch_data(query)}
        new_movies_df = df.filter(~col("ItemID").isin(existing_movies))

        if new_movies_df.count() == 0:
            logger.info("No new movies found. Skipping processing.")
            return

        # Process in batches to improve performance
        for row in new_movies_df.collect():
            movie_event = MovieCatalogEvent(
                movie_id=str(row["ItemID"]),
                title=row["Title"],
                genre=row["Genre"],
                list_price=row["ListPrice"]
            )
            # Call to_dict() without arguments - it will use self internally
            self.kafka_publisher.send_event("movies_catalog_enriched", movie_event())

        logger.info(f"✅ Published {new_movies_df.count()} new movie events to Kafka.")

    def run(self):
        """Runs batch processing every 60 seconds"""
        while True:
            try:
                self.process_new_movies()
            except Exception as e:
                logger.error(f"Error in batch processing: {str(e)}")
            time.sleep(60)  # Changed from 2 to 60 seconds for production

if __name__ == "__main__":
    from Config import producer_conf, schema_registry_url, auth_user_info
    
    kafka_publisher = KafkaEventPublisher(producer_conf, schema_registry_url, auth_user_info)
    processor = SparkBatchProcessor("/app/data/raw/Movies.txt", kafka_publisher)
    processor.run()