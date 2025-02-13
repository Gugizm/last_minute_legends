# Use Bitnami Spark image (includes Java + Spark)
FROM bitnami/spark:latest

# Switch to root user to install system dependencies
USER root

# Ensure apt works and install Python dependencies
RUN apt-get clean && apt-get update && apt-get install -y python3 python3-pip

# Switch back to non-root user for security
USER 1001

# Set working directory inside the container
WORKDIR /app

# Copy project files into the container
COPY . .

# Install PySpark and required Python packages
RUN pip3 install --no-cache-dir -r requirements.txt pyspark

# Run both scripts (Movie Processing + User Activity)
CMD ["sh", "-c", "python3 SparkMovieProcessor.py && python3 UserActivitySimulator.py"]
