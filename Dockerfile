FROM apache/airflow:2.10.4

WORKDIR /usr/local/app
ENV PYTHONPATH="/usr/local/app"
COPY app .

USER root

RUN apt-get update \
  && apt-get install -y openjdk-17-jre-headless \
  && apt-get clean \
  && java -version

USER airflow

ENV SPARK_CLASSPATH="/usr/local/app/jars/snowflake-jdbc.jar:/app/jars/spark-snowflake.jar"

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN pip install --no-cache-dir -r /usr/local/app/requirements.txt