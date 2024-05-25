FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.2.0
ARG HADOOP_VERSION=3.2
RUN curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt/ \
    && rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH=$PATH:$SPARK_HOME/bin

RUN pip install pyspark==${SPARK_VERSION}
RUN pip install kafka-python
RUN pip install requests
RUN pip install cassandra-driver
RUN pip install pandas
COPY kafka_to_kafka.py /opt/app/kafka_to_kafka.py

WORKDIR /opt/app

CMD ["python", "kafka_to_kafka.py"]
