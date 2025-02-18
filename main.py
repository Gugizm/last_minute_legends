import multiprocessing
from services.spark_batch_processor import SparkBatchProcessor
from services.spark_streaming_processor import SparkStreamingProcessor
from services.kafka_event_publisher import KafkaEventPublisher
from Config import kafka_config

def start_batch_processor():
    """Runs Spark Batch Processor"""
    kafka_publisher = KafkaEventPublisher(kafka_config)
    processor = SparkBatchProcessor("/app/data/raw/Movies.txt", "/app/data/processed/movies.parquet", kafka_publisher)
    processor.run()

def start_streaming_processor():
    """Runs Spark Streaming Processor"""
    kafka_publisher = KafkaEventPublisher(kafka_config)
    processor = SparkStreamingProcessor(kafka_publisher)
    processor.run()

if __name__ == "__main__":
    """Orchestrates both batch & streaming processors in parallel"""
    batch_process = multiprocessing.Process(target=start_batch_processor)
    streaming_process = multiprocessing.Process(target=start_streaming_processor)

    batch_process.start()
    streaming_process.start()

    batch_process.join()
    streaming_process.join()
