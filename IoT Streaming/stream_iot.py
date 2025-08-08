from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

IOT_JSON = "iot_input"

spark = (SparkSession.builder
         .appName("IOT Streaming")
         .master("local[*]")
         .getOrCreate()
         )

schema = (StructType().
          add("deviceId", StringType())
          .add("timestamp", TimestampType())
          .add("temperature", DoubleType())
          .add("humidity", DoubleType())
          )

streaming_df = (spark.readStream.schema(schema)
                .json(IOT_JSON)
                )

streaming_df.printSchema()

# temp_25_above_df = streaming_df.filter("temperature > 25")
#
# query = temp_25_above_df.writeStream.format("csv") \
#     .option("path", "iot_output") \
#     .option("checkpointLocation", "checkpoint") \
#     .outputMode("append") \
#     .start()

# average temperature per device every minute
aggregated_df = (streaming_df
                 .withWatermark("timestamp", "1 minute")
                 .groupby(col("deviceId"), window("timestamp", "1 minute"))
                 .agg(avg("temperature").alias("average_column")))

query = aggregated_df.writeStream.format("console").outputMode("complete").option("truncate", False).start()

query.awaitTermination()
