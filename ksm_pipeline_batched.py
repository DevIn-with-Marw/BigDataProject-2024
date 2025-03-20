from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, FloatType, StringType, StructField

spark = SparkSession.\
        builder.\
        appName("streamingExampleWrite").\
        config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.4.0').\
        config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0').\
        getOrCreate()

kafka_stream = spark \
  .readStream \
  .format("kafka") \
  .option("startingOffsets", "earliest") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "vehicle-positions") \
  .load()

#INPUT SCHEMA DEFINITION
schema_in = StructType([
    StructField("name", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("time", StringType(), True),
    StructField("link", StringType(), True),
    StructField("position", FloatType(), True),
    StructField("spacing", FloatType(), True),
    StructField("speed", FloatType(), True)
])

# ISOLATE THE JSON FIELDS: kafka_stream has id, topic, value blah blah blah... 
json_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_in).alias("data")).selectExpr('data.*')


json_stream_tmstmp = json_stream.select( col("name"), col("origin"), col("destination"), to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss").alias("time") , 
                                        col("link"), col("position"), col("spacing"), col("speed"))


# DATAFRAME CREATION WITH OUTPUT SCHEMA 
#retain only the fields of schema in, necessary for the aggregations
processed_df = json_stream_tmstmp.select(
    col("time"),
    col("link"),
    col("speed")
)

# WATERMARK ADDITION FOR THE AGGREGATION
processed_df_with_watermark = processed_df \
    .withWatermark("time", "1 hour")

# AGGREGATION OPERATIONS
aggregated_df = processed_df_with_watermark \
    .groupBy("time", "link") \
    .agg(
        count("*").alias("vcount"),
        expr("avg(speed)").alias("vspeed")
    )

# STORE DATA IN MONGODB: AGGREGATED DATA
def write_to_mongodb(batch_df, batch_id):
    batch_df.write \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", "mongodb://localhost:27017/project.spark_aggregated") \
        .option("spark.mongodb.database", "project") \
        .option("spark.mongodb.collection", "spark_aggregated") \
        .mode("append") \
        .save()
    
query1 = (
    aggregated_df.writeStream
        .foreachBatch(write_to_mongodb) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()
)

# STORE DATA IN MONGODB: RAW DATA
def write_raw_to_mongodb(batch_df, batch_id):
    batch_df.write \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", "mongodb://localhost:27017/project.spark_raw") \
        .option("spark.mongodb.database", "project") \
        .option("spark.mongodb.collection", "spark_raw") \
        .mode("append") \
        .save()

query2 = (
    json_stream_tmstmp.writeStream
        .foreachBatch(write_raw_to_mongodb) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
)

query1.awaitTermination()
query2.awaitTermination()
