import nltk
nltk.download("punkt")
nltk.download("averaged_perceptron_tagger")
nltk.download("maxent_ne_chunker")
nltk.download("words")

from configparser import ConfigParser
from nltk import ne_chunk, pos_tag, word_tokenize, Tree
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, desc, col, lower, regexp_replace, udf, trim, to_json, struct

_config = ConfigParser()
_config.read(["./config.ini"])

bootstrapServers = _config.get("KAFKA", "bootstrap_servers")
input_topic = _config.get("TOPICS", "input_topic")
output_topic = _config.get("TOPICS", "output_topic")

spark = (
    SparkSession
    .builder
    .appName("assignment3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir("/tmp")


@udf()
def extract_named_entities(x):
    chunked = ne_chunk(pos_tag(word_tokenize(x)))
    continuous_chunk = []
    current_chunk = []
    for i in chunked:
        if type(i) == Tree:
            current_chunk.append(" ".join([token for token, pos in i.leaves()]))
        if current_chunk:
            named_entity = " ".join(current_chunk)
            if named_entity not in continuous_chunk and len(named_entity.split()) > 0:
                continuous_chunk.append(named_entity)
                current_chunk = []
        else:
            continue
    return continuous_chunk


if __name__ == "__main__":
    articles = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", input_topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    named_entities = (
        articles
        .select(extract_named_entities(col("value")).alias("value"))
        .withColumn('value', lower('value'))
        .withColumn("value", regexp_replace("value", r"[^ a-zA-Z0-9]+", ""))
    )

    output = (
        named_entities
        .select(
            explode(split(named_entities.value, ' '))
            .alias('word')
        )
        .withColumn("word", regexp_replace("word", r"^\s+$", ""))
        .filter(trim(col("word"))!="")
        .groupBy('word')
        .count()
        .orderBy(desc("count"))
    )
    """
    query = output \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()
    """
    # wordCounts = words.groupBy('word').count().orderBy(desc("count"))

    query = (
        output
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
