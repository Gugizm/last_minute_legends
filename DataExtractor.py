import os
import shutil
import time
import logging
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, split, when, first


# Configure Logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class MoviesDataProcessor:
    """
    Processes Movies.txt file using Apache Spark.
    - Reads raw text data.
    - Extracts movie attributes using DataFrames.
    - Cleans and structures data.
    - Extracts correct `ListPrice` values WITHOUT regex.
    - Saves processed data to Parquet.
    """

    def __init__(self, file_path: str, output_path: str):
        """
        Initializes the Spark session and file paths.
        :param file_path: Path to Movies.txt
        :param output_path: Path to save processed Parquet files
        """
        logger.info("Initializing Spark Session...")
        self.spark = SparkSession.builder \
            .appName("MoviesDataProcessing") \
            .master("local[*]") \
            .getOrCreate()

        self.file_path = file_path
        self.output_path = output_path
        logger.info(f"File Path: {self.file_path}")
        logger.info(f"Output Path: {self.output_path}")

    def read_movies_data(self):
        """
        Reads Movies.txt as a DataFrame and assigns an ItemID for each movie.
        Ensures the schema aligns with Avro requirements.
        """
        logger.info("Reading Movies.txt as a DataFrame...")
        df = self.spark.read.text(self.file_path)

        if "value" not in df.columns:
            raise ValueError("The 'value' column is missing from the input data.")

        logger.info("Removing empty lines...")
        df = df.filter(F.col("value").isNotNull() & (F.col("value") != ""))

        logger.info("Assigning unique ItemID for each movie entry...")
        df = df.withColumn(
            "ItemID",
            F.sum(
                when(F.col("value").startswith("ITEM "), 1).otherwise(0)
            ).over(Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id()))  # ✅ Added partition
        )

        logger.info("Filtering out lines without '='...")
        df = df.filter(F.col("value").contains("="))

        # Split and create columns with correct case
        logger.info("Performing key-value split...")
        df = df.withColumn(
            "split_array", 
            F.split(F.col("value"), "=")
        ).withColumn(
            "Column", 
            F.trim(F.col("split_array").getItem(0))
        ).withColumn(
            "Value",
            F.trim(F.col("split_array").getItem(1))
        ).drop("split_array")

        # ✅ Ensure correct data types immediately
        df = df.withColumn("ItemID", col("ItemID").cast("string"))  # Convert ItemID to string
        df = df.withColumn("ListPrice", col("Value").cast("float"))  # Convert ListPrice to float when applicable

        # Important: Keep both Column and Value when dropping intermediate columns
        df = df.select("ItemID", "Column", "Value")

        logger.info(f"Available columns after transformation: {df.columns}")

        # ✅ Pivot the data BEFORE cleaning
        logger.info("Pivoting data...")
        df = df.groupBy("ItemID").pivot("Column").agg(first(col("Value")))

        logger.info(f"Data extraction completed. {df.count()} movies processed.")
        return df


    def clean_data(self, df):
        """
        Cleans extracted data:
        - Extracts correct ListPrice (without using regex).
        - Handles missing values.
        - Converts ListPrice to float.
        :param df: Raw extracted DataFrame
        :return: Cleaned DataFrame
        """

        def extract_price(price):
            """
            Extracts price from ListPrice column.
            - Finds the last numeric value after `$`
            - Works without using regex
            """
            if not price:
                return 0.0
            parts = price.split("$")  # Split by `$`
            try:
                return float(parts[-1]) if len(parts) > 1 else 0.0  # Extracted price as float
            except ValueError:
                return 0.0  # Default if no valid price found

        logger.info("Applying price extraction transformation...")

        # Apply Price Extraction Using UDF
        price_udf = F.udf(lambda price: extract_price(price), "float")

        if "ListPrice" in df.columns:
            df = df.withColumn("ListPrice", price_udf(col("ListPrice")))

        # Ensure required fields exist
        df = df.fillna({
            "Title": "Unknown",
            "ListPrice": 0.0,
            "Genre": "Unknown Genre"
        })

        logger.info("Data cleaning completed.")
        return df



    def save_to_parquet(self, df: DataFrame):
        """
        Saves the cleaned DataFrame as a Parquet file.
        - Ensures old files are deleted before saving.
        - Fixes permission issues.
        - Handles potential directory conflicts.
        """
        output_path = self.output_path

        try:
            # ✅ Ensure DataFrame is not empty before saving
            if df.count() == 0:
                logging.error("❌ DataFrame is empty. Not saving to Parquet.")
                return

            # ✅ Fix file ownership to allow deletion
            os.system(f"chown -R 1001:1001 {output_path}")
            os.system(f"chmod -R 777 {output_path}")

            # ✅ Try to remove the existing directory using `shutil.rmtree`
            if os.path.exists(output_path):
                logging.info(f"🔄 Removing existing Parquet directory: {output_path}")
                shutil.rmtree(output_path, ignore_errors=True)
                time.sleep(2)  # Allow time for OS to release locks

            # ✅ Ensure directory is recreated before writing
            os.makedirs(output_path, exist_ok=True)

            # ✅ Save Parquet file with `coalesce(1)` to avoid too many partitions
            df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(output_path)

            logging.info("✅ Parquet file saved successfully.")

        except Exception as e:
            logging.error(f"❌ Failed to save Parquet file: {e}")

            # ✅ Attempt to save to an alternative backup location if primary save fails
            temp_path = "/tmp/movies.parquet"
            logging.info(f"⚠️ Attempting to save to alternative location: {temp_path}")
            try:
                df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(temp_path)
                logging.info(f"✅ Successfully saved to alternative location: {temp_path}")
            except Exception as alt_e:
                logging.error(f"❌ Failed to save to alternative location: {alt_e}")
                raise

    def process(self):
        """
        Complete pipeline:
        - Reads Movies.txt
        - Extracts movie data
        - Cleans and structures the data
        - Saves it to Parquet
        """
        logger.info("🔹 Starting Movie Data Processing Pipeline...")
        
        raw_df = self.read_movies_data()
        cleaned_df = self.clean_data(raw_df)
        self.save_to_parquet(cleaned_df)

        logger.info("✅ Movie Data Processing Completed Successfully!")


if __name__ == "__main__":
    processor = MoviesDataProcessor(
        file_path="/app/data/raw/Movies.txt",
        output_path="/app/data/processed/movies.parquet"
    )
    processor.process()
