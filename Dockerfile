# Use Bitnami Spark image (includes Java + Spark)
FROM bitnami/spark:latest

# Switch to root user to install system dependencies
USER root

# Ensure apt works and install Python dependencies
RUN apt-get clean && apt-get update && apt-get install -y python3 python3-pip curl

# ✅ Create a writable directory for JARs and set ownership to non-root user
RUN mkdir -p /app/jars && chown -R 1001:1001 /app/jars

# ✅ Download Correct Snowflake & Kafka JARs
RUN curl -o /app/jars/snowflake-jdbc.jar https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.14/snowflake-jdbc-3.13.14.jar && \
    curl -o /app/jars/spark-snowflake.jar https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.9.1/spark-snowflake_2.12-2.9.1.jar && \
    curl -o /app/jars/spark-sql-kafka-0-10.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar

# ✅ Install Snowflake Connector for Python
RUN pip install snowflake-connector-python

# Switch back to non-root user for security
USER 1001

# Set working directory inside the container
WORKDIR /app

# Copy project files into the container
COPY . .
# ✅ Download Spark & Kafka JARs

# Install required Python packages
RUN pip3 install --no-cache-dir -r requirements.txt pyspark

# ✅ Set Spark to use `/app/jars` for Snowflake & Kafka connectors
ENV SPARK_CLASSPATH="/app/jars/snowflake-jdbc.jar:/app/jars/spark-snowflake.jar:/app/jars/spark-sql-kafka-0-10.jar"

# ✅ Run Spark Streaming
CMD ["sh", "-c", "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /app/services/spark_streaming_processor.py"]
