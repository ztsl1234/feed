
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("RiskTradesStreamingJob") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

trade_schema = StructSchema = StructType([
    StructField("trade_id", StringType()),
    StructField("counterparty", StringType()),
    StructField("notional", DoubleType()),
    StructField("currency", StringType()),
    StructField("timestamp", TimestampType())
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "risk_trades") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka value is bytes → cast to string
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(
    from_json(col("json_str"), trade_schema).alias("data")
).select("data.*")

windowed_agg = (
    parsed_df
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("currency")
    )
    .sum("notional")
    .withColumnRenamed("sum(notional)", "total_notional")
)

alert_stream = parsed_df.filter(
    (col("notional") > 2_000_000) & (col("currency") == "USD")
)

agg_query = (
    windowed_agg.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/delta/checkpoints/risk_window_agg")
    .start("/delta/risk_window_agg")
)

alert_query = (
    alert_stream.writeStream
    .format("delta")     
    .outputMode("append")
    .option("checkpointLocation", "/delta/checkpoints/alerts")
    .start("/delta/alerts")


agg_query.awaitTermination()
alert_query.awaitTermination()
