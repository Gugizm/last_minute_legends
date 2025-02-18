import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.avro.functions import from_avro

from kafka.Config import producer_conf


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    kafka_options = {
        "kafka.bootstrap.servers": producer_conf["bootstrap.servers"],
        "kafka.security.protocol": producer_conf["security.protocol"],
        "kafka.sasl.mechanism": producer_conf["sasl.mechanism"],
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required'
                                  f'username="{producer_conf["sasl.username"]}" password="{producer_conf["sasl.password"]}";',
        "subscribe": "checkout_to_cart_topic",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false"
    }

    jars = [
        "/usr/local/app/jars/kafka-clients-3.5.0.jar",
        "/usr/local/app/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
        "/usr/local/app/jars/commons-pool2-2.11.1.jar",
        "/usr/local/app/jars/spark-avro_2.12-3.5.4.jar",
        "/usr/local/app/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar"
    ]

    spark = SparkSession \
        .builder \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.driver.extraClassPath", "/usr/local/app/jars/*") \
        .config("spark.executor.extraClassPath", "/usr/local/app/jars/*") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .appName("KafkaStreamingReader") \
        .getOrCreate()

    try:
        df = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()

        raw_df = df.select(
            f.col("key").cast("string"),
            f.col("value"),
            f.col("topic"),
            f.col("partition"),
            f.col("offset")
        )

        avro_schema = """
        {
          "type": "record",
          "name": "AddedToCartEvent",
          "fields": [
            { "name": "timestamp", "type": "string" },
            { "name": "event_name", "type": "string" },
            { "name": "user_id", "type": "string" },
            { "name": "item_id", "type": "string" },
            { "name": "cart_id", "type": "string" }
          ]
        }
        """

        deserialized_df = raw_df.select(
            from_avro(
                f.col("value"),
                avro_schema,
                {"mode": "PERMISSIVE"}
            ).alias("data")
        ).select("data.*")

        query = deserialized_df.writeStream \
            .outputMode("append") \
            .option("truncate", "false") \
            .format("console") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()

        logger.info("Streaming query started successfully")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error in streaming application: {e}", exc_info=True)
    finally:
        logger.info("Stopping Spark session")
        spark.stop()


if __name__ == "__main__":
    main()