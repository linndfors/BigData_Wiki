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
    .add("page_id", StringType()) \
    .add("user_text", StringType ()) \
    .add("user_id", StringType()) \
    .add("user_is_bot", StringType()) \
    .add("page_title", StringType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "processed") \
    .load()


df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


df = df.withColumn("created_at", col("created_at").cast(TimestampType()))

df_1 = df.select("page_id",
    "user_is_bot",
    "created_at",
    "domain")

# df_2 = df.select("page_id",
#     "user_text", "user_id",
#     "created_at",
#     "page_title")

df_3 = df.select("page_id",
    "domain", "user_id",
    "created_at",
    "page_title")

query = df_1.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "fancy_keyspace") \
    .option("table", "created_pages") \
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_cassandra") \
    .start()

# query2 = df_2.writeStream \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("keyspace", "fancy_keyspace") \
#     .option("table", "for_user") \
#     .option("checkpointLocation", "/tmp/checkpoints/kafka_to_cassandra_2") \
#     .start()

query3 = df_3.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "fancy_keyspace") \
    .option("table", "pages") \
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_cassandra_3") \
    .start()

# query2.awaitTermination()
query3.awaitTermination()
query.awaitTermination()
