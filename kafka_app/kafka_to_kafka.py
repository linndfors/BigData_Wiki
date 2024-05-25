from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, ArrayType

spark = SparkSession.builder \
    .appName("KafkaToKafka") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

performer_schema = StructType([
    StructField("user_text", StringType()),
    StructField("user_groups", ArrayType(StringType())),
    StructField("user_is_bot", BooleanType()),
    StructField("user_id", IntegerType()),
    StructField("user_registration_dt", StringType()),
    StructField("user_edit_count", IntegerType())
])

rev_slots_schema = StructType([
    StructField("main", StructType([
        StructField("rev_slot_content_model", StringType()),
        StructField("rev_slot_sha1", StringType()),
        StructField("rev_slot_size", IntegerType()),
        StructField("rev_slot_origin_rev_id", IntegerType())
    ]))
])

meta_schema = StructType([
    StructField("uri", StringType()),
    StructField("request_id", StringType()),
    StructField("id", StringType()),
    StructField("dt", StringType()),
    StructField("domain", StringType()),
    StructField("stream", StringType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", IntegerType())
])

schema = StructType([
    StructField("$schema", StringType()),
    StructField("meta", meta_schema),
    StructField("database", StringType()),
    StructField("page_id", IntegerType()),
    StructField("page_title", StringType()),
    StructField("page_namespace", IntegerType()),
    StructField("rev_id", IntegerType()),
    StructField("rev_timestamp", StringType()),
    StructField("rev_sha1", StringType()),
    StructField("rev_minor_edit", BooleanType()),
    StructField("rev_len", IntegerType()),
    StructField("rev_content_model", StringType()),
    StructField("rev_content_format", StringType()),
    StructField("performer", performer_schema),
    StructField("page_is_redirect", BooleanType()),
    StructField("comment", StringType()),
    StructField("parsedcomment", StringType()),
    StructField("dt", StringType()),
    StructField("rev_slots", rev_slots_schema)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "input") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = df.select(from_json(col("json_str"), schema).alias("data"))

flattened_df = parsed_df.select("data.*")

selected_df = flattened_df.select(
    col("meta.domain").alias("domain"),
    col("meta.dt").alias("created_at"),
    col("page_id"), col ("page_title"),
    col("performer.user_is_bot"),
    col("performer.user_id"),
    col("performer.user_text")
)

# filtered_df = selected_df.filter(
#     (col("domain").isin("en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org")) &
#     (col("user_is_bot") == False)
# )

# filtered_df = filtered_df.drop("user_is_bot")

query = selected_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "processed") \
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_kafka") \
    .start()

query.awaitTermination()
