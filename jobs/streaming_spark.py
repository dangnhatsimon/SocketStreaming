import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, when, udf
import logging
from config.config import config


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


def streaming_spark(
    spark,
    host="localhost",
    port=9999
):
    try:
        stream_df = (
            spark.readStream.format("socket")
                            .option("host", host)
                            .option("port", port)
                            .load()
        )

        schema = StructType(
            [
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType()),
            ]
        )
        stream_df = stream_df.select(from_json(col("value"), schema).alias("data")).select(("data.*"))

        # query = (
        #     stream_df.writeStream
        #     .outputMode("append")
        #     .format("console")
        #     .option("truncate", "false")
        #     .start()
        # )
        # query.awaitTermination()

        kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
        query = (
            kafka_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", config["kafka"]["bootstrap.servers"])
            .option("kafka.security.protocol", config["kafka"]["security.protocol"])
            .option("kafka.sasl.mechanisms", config["kafka"]["sasl.mechanisms"])
            .option('kafka.sasl.jaas.config',
                    'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                    'password="{password}";'.format(
                        username=config['kafka']['sasl.username'],
                        password=config['kafka']['sasl.password']
                    ))
            .option('checkpointLocation', '/tmp/checkpoint')
            .option('topic', topic)
            .start()
            .awaitTermination()
        )
    except Exception as e:
        logging.error(f"Unexpected errors: {e}")


if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    streaming_spark(spark_conn)
