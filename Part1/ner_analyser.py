from configparser import ConfigParser

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc, to_json, struct, col

_config = ConfigParser()
_config.read(["./config.ini"])

bootstrapServers = _config.get("KAFKA", "bootstrap_servers")
input_topic = _config.get("REDDIT", "input_topic")
output_topic = _config.get("REDDIT", "output_topic")

spark = (
    SparkSession
    .builder
    .appName("assignment3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("/tmp")


if __name__ == "__main__":
    lines = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", input_topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    named_entities = (
        lines
        #regex remover
        #stopwords remover
        #named entities identifier
        .select(
            explode(split(lines.value, ' '))
            .alias('word')
        )
        .groupBy('word')
        .count()
        .orderBy(desc("count"))
    )

    """named_entities = (
        wordCounts
    )"""

    # wordCounts = words.groupBy('word').count().orderBy(desc("count"))

    query = (
        named_entities
        .select(to_json(struct(col("word"), col("count"))).alias("value"))
        .writeStream
        .format("kafka")
        .outputMode("complete")
        .option("checkpointLocation", "./tmp")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("topic", output_topic)
        .start()
    )
    query.awaitTermination()
