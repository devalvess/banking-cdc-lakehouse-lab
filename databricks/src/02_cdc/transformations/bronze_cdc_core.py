"""Bronze: eventos Debezium do core bancário, exatamente como chegaram do Kafka on-prem."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP = spark.conf.get("kafka_bootstrap")


@dp.table(
    name="bronze.cdc_eventos_core",
    comment="Eventos de CDC (formato Debezium) das tabelas CORE.* do Db2. Append-only; "
    "payload JSON preservado em ds_valor para reprocessamento e auditoria.",
    cluster_by=["ds_topico"],
)
def cdc_eventos_core():
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", r"corebank\.CORE\..*")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
        .select(
            F.col("topic").alias("ds_topico"),
            F.col("partition").alias("nr_particao"),
            F.col("offset").alias("nr_offset"),
            F.col("timestamp").alias("dt_kafka"),
            F.col("key").cast("string").alias("ds_chave"),
            F.col("value").cast("string").alias("ds_valor"),
            F.current_timestamp().alias("dt_ingestao"),
        )
    )
