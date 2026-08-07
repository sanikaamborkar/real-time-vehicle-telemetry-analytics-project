from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from config import configuration

# -------------------------------
# 1. CREATE SPARK SESSION (WITH DEPENDENCIES)
# -------------------------------
# spark = SparkSession.builder \
#     .appName("KafkaToS3Streaming") \
#     .config("spark.jars.packages",
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
#             "org.apache.hadoop:hadoop-aws:3.3.1") \
#     .getOrCreate()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# spark = SparkSession.builder \
#     .appName("KafkaToS3Streaming") \
#     .config("spark.sql.streaming.metricsEnabled", "false") \
#     .config("spark.kafka.metrics.enabled", "false") \
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("KafkaToS3Streaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.metricsEnabled", "false") \
    .config("spark.kafka.metrics.enabled", "false") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 2. S3 CONFIGURATION (IMPORTANT)
# -------------------------------
# spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))
# spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))
# spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
# spark._jsc.hadoopConfiguration().set(
#     "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
# )

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set(                   
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

# -------------------------------
# 3. SCHEMAS
# -------------------------------

#vehicle schema
vehicleSchema = StructType([
    StructField("id", StringType()),
    StructField("deviceId", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("direction", StringType()),
    StructField("brand", StringType()),
    StructField("model", StringType()),
    StructField("year", IntegerType()),
    StructField("fuelType", StringType()),
    StructField("area", StringType())
])

#gps schema
gpsSchema = StructType([
    StructField("id", StringType()),
    StructField("deviceId", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("direction", StringType()),
    StructField("vehicleType", StringType()),
    StructField("area", StringType())
])

#traffic schema
trafficSchema = StructType([
    StructField("id", StringType()),
    StructField("deviceId", StringType()),
    StructField("cameraId", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("snapshot", StringType()),
    StructField("area", StringType())
])

#weather Schema
weatherSchema = StructType([
    StructField("id", StringType()),
    StructField("deviceId", StringType()),
    StructField("area", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("weather_condition", StringType()),
    StructField("wind_speed", DoubleType()),
    StructField("timestamp", TimestampType())
])

#emergency Schema
emergencySchema = StructType([
    StructField("id", StringType()),
    StructField("incidentId", StringType()),
    StructField("deviceId", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("status", StringType()),
    StructField("area", StringType())
])

# -------------------------------
# 4. READ KAFKA FUNCTION
# -------------------------------
#using startingOffsets = earliest for testing and debugging later change it to [latest]

def read_kafka(topic, schema):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", "2 minutes")
       
    return df 

# -------------------------------
# 5. READ ALL STREAMS
# -------------------------------
vehicleDF = read_kafka("vehicle_data", vehicleSchema)
gpsDF = read_kafka("gps_data", gpsSchema)
trafficDF = read_kafka("traffic_camera_data", trafficSchema)
weatherDF = read_kafka("weather_punedata", weatherSchema)
emergencyDF = read_kafka("emergency_data", emergencySchema)

# -------------------------------
# 6. WRITE FUNCTION (UPGRADED)
# -------------------------------
def write_stream(df, path, checkpoint):
    return df.writeStream \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", checkpoint) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

# -------------------------------
# 7. WRITE TO S3 (BRONZE LAYER)
# -------------------------------
write_stream(vehicleDF,
             "s3a://spark-streaming-data-bigdata-project/data/vehicle_data",
             "s3a://spark-streaming-data-bigdata-project/checkpoints/vehicle_data")

write_stream(gpsDF,
             "s3a://spark-streaming-data-bigdata-project/data/gps_data",
             "s3a://spark-streaming-data-bigdata-project/checkpoints/gps_data")

write_stream(trafficDF,
             "s3a://spark-streaming-data-bigdata-project/data/traffic_data",
             "s3a://spark-streaming-data-bigdata-project/checkpoints/traffic_data")

write_stream(weatherDF,
             "s3a://spark-streaming-data-bigdata-project/data/weather_data",
             "s3a://spark-streaming-data-bigdata-project/checkpoints/weather_data")

write_stream(emergencyDF,
             "s3a://spark-streaming-data-bigdata-project/data/emergency_data",
             "s3a://spark-streaming-data-bigdata-project/checkpoints/emergency_data")


# -------------------------------
# 8. KEEP STREAM RUNNING
# -------------------------------
spark.streams.awaitAnyTermination()