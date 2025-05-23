import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, when, udf, to_json, struct
import logging
import openai
from time import sleep
from dotenv import load_dotenv
import os


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


load_dotenv(dotenv_path="/opt/spark/spark-config/.env")


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
        stream_df = (
            stream_df
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )

        query = (
            stream_df.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .start()
        )
        query.awaitTermination()

    except Exception as e:
        logging.error(f"Unexpected errors: {e}")


def streaming_spark_kafka(
    spark,
    topic: str,
    host="localhost",
    port=9999
):
    while True:
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
            stream_df = (
                stream_df
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
            )

            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

            query = (
                (
                    kafka_df.writeStream.format("kafka")
                    .option("kafka.bootstrap.servers", os.getenv("bootstrap.servers"))
                    .option("kafka.security.protocol", os.getenv("security.protocol"))
                    .option("kafka.sasl.mechanisms", os.getenv("sasl.mechanisms"))
                    .option("kafka.sasl.jaas.config", f"""org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("sasl.username")}" password="{os.getenv("sasl.password")}";""")
                )
                .option("checkpointLocation", "/opt/spark/spark-checkpoint")
                .option("topic", topic)
                .start()
            )
            query.awaitTermination()
        except Exception as e:
            logging.error(f"Unexpected errors: {e}")
            sleep(10)


def sentimental_analysis(comment: str) -> str:
    if comment:
        openai.api_key = os.getenv("openai")
        completion = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": f"""
                        You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                        You are to respond with one word from the option specified above, do not add anything else.
                        Here is the comment:
                        {comment}
                        """
                }
            ]
        )
        return completion.choices[0].message["content"]
    return "EMPTY"


def streaming_spark_kafka_sentimental(
    spark,
    topic: str,
    host="localhost",
    port=9999
):
    while True:
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
            stream_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
            sentimental_analysis_udf = udf(sentimental_analysis, StringType())
            stream_df = stream_df.withColumn(
                "feedback",
                when(col("text").isNotNull(), sentimental_analysis_udf(col("text")))
                .otherwise(None)
            )
            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
            query = (
                (
                    kafka_df.writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", os.getenv("bootstrap.servers"))
                    .option("kafka.security.protocol", os.getenv("security.protocol"))
                    .option("kafka.sasl.mechanisms", os.getenv("sasl.mechanisms"))
                    .option("kafka.sasl.jaas.config", f"""org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("sasl.username")}" password="{os.getenv("sasl.password")}";""")
                )
                .option("checkpointLocation", "/opt/spark/spark-checkpoint")
                .option("topic", topic)
                .start()
            )
            query.awaitTermination()
        except Exception as e:
            logging.error(f"Unexpected errors: {e}")


if __name__ == "__main__":
    spark_conn = (
        SparkSession.builder
        .appName("SocketStreamConsumer")
        .config("spark.driver.cores", 2)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
        .config("spark.submit.deployMode", "client")
        .config("spark.log.level", "ALL")
        .getOrCreate()
    )

    streaming_spark(spark_conn, host="socketstreaming-spark-master", port=9999)

    # streaming_spark_kafka(spark_conn, topic="customers_review", host="socketstreaming-spark-master", port=9999)
    # streaming_spark_kafka_sentimental(spark_conn, topic="customers_review", host="0.0.0.0", port=9999)
    
# docker exec -it socketstreaming-spark-master spark-submit --master spark://socketstreaming-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3 jobs/streaming_spark.py
