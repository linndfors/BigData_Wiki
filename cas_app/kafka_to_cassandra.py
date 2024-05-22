from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, TimestampType, BooleanType, IntegerType


spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

schema = StructType() \
    .add("domain", StringType()) \
    .add("created_at", StringType()) \
    .add("page_title", StringType()) \
    .add("user_id", IntegerType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "processed") \
    .load()


df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


df = df.withColumn("created_at", col("created_at").cast(TimestampType()))

query = df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "hw12_nahurna") \
    .option("table", "main_table") \
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_cassandra") \
    .start()

query.awaitTermination()
