"""Silver: aplica os eventos de CDC com AUTO CDC.

- clientes   -> SCD tipo 2 (histórico cadastral: exigência de auditoria)
- contas     -> SCD tipo 1 (estado atual)
- transacoes -> SCD tipo 1 (fato; deletes também são respeitados)

A ordem dos eventos vem do LSN do Db2 (posição no log), não do horário de chegada:
eventos fora de ordem ou reprocessados não sobrescrevem um estado mais novo.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

# Debezium com decimal.handling.mode=string e time.precision.mode=connect:
# DECIMAL chega como texto, DATE como dias desde 1970 e TIMESTAMP como epoch em ms.
LINHA = {
    "clientes": StructType(
        [
            StructField("ID_CLIENTE", IntegerType()),
            StructField("NR_CPF", StringType()),
            StructField("NM_CLIENTE", StringType()),
            StructField("DS_EMAIL", StringType()),
            StructField("NR_TELEFONE", StringType()),
            StructField("DT_NASCIMENTO", IntegerType()),
            StructField("NM_CIDADE", StringType()),
            StructField("SG_UF", StringType()),
            StructField("VL_RENDA_MENSAL", StringType()),
            StructField("CD_AGENCIA", IntegerType()),
            StructField("DT_CADASTRO", LongType()),
            StructField("DT_ATUALIZACAO", LongType()),
        ]
    ),
    "contas": StructType(
        [
            StructField("ID_CONTA", IntegerType()),
            StructField("ID_CLIENTE", IntegerType()),
            StructField("CD_AGENCIA", IntegerType()),
            StructField("TP_CONTA", StringType()),
            StructField("VL_SALDO", StringType()),
            StructField("ST_CONTA", StringType()),
            StructField("DT_ABERTURA", IntegerType()),
            StructField("DT_ATUALIZACAO", LongType()),
        ]
    ),
    "transacoes": StructType(
        [
            StructField("ID_TRANSACAO", LongType()),
            StructField("ID_CONTA", IntegerType()),
            StructField("TP_TRANSACAO", StringType()),
            StructField("VL_TRANSACAO", StringType()),
            StructField("DT_TRANSACAO", LongType()),
        ]
    ),
}

FONTE = StructType(
    [
        StructField("ts_ms", LongType()),
        StructField("snapshot", StringType()),
        StructField("commit_lsn", StringType()),
        StructField("change_lsn", StringType()),
    ]
)


def eventos(tabela: str):
    """Lê da Bronze os eventos de uma tabela e devolve a imagem da linha + metadados de CDC."""
    envelope = StructType(
        [
            StructField("before", LINHA[tabela]),
            StructField("after", LINHA[tabela]),
            StructField("op", StringType()),
            StructField("source", FONTE),
        ]
    )
    return (
        spark.readStream.table("bronze.cdc_eventos_core")
        .where(F.col("ds_topico") == f"corebank.CORE.{tabela.upper()}")
        .withColumn("e", F.from_json("ds_valor", envelope))
        .where("e.op IS NOT NULL")
        # Em delete o "after" é nulo: a chave vem do "before".
        .withColumn("linha", F.when(F.col("e.op") == "d", F.col("e.before")).otherwise(F.col("e.after")))
        .select(
            "linha.*",
            F.col("e.op").alias("ds_operacao"),
            F.timestamp_millis("e.source.ts_ms").alias("dt_evento_origem"),
            # Sequência = posição no log do Db2 (+ offset como desempate dentro do snapshot).
            F.concat_ws(
                "|",
                F.coalesce("e.source.commit_lsn", F.lit("")),
                F.coalesce("e.source.change_lsn", F.lit("")),
                F.lpad(F.col("nr_offset").cast("string"), 20, "0"),
            ).alias("ds_sequencia"),
        )
    )


def data_debezium(coluna):
    return F.date_add(F.lit("1970-01-01").cast("date"), F.col(coluna))


# ---------------------------------------------------------------------------------------------
# clientes — SCD tipo 2
# ---------------------------------------------------------------------------------------------

@dp.temporary_view()
def clientes_cdc():
    return eventos("clientes").select(
        F.col("ID_CLIENTE").alias("id_cliente"),
        F.col("NR_CPF").alias("nr_cpf"),
        F.col("NM_CLIENTE").alias("nm_cliente"),
        F.col("DS_EMAIL").alias("ds_email"),
        F.col("NR_TELEFONE").alias("nr_telefone"),
        data_debezium("DT_NASCIMENTO").alias("dt_nascimento"),
        F.col("NM_CIDADE").alias("nm_cidade"),
        F.col("SG_UF").alias("sg_uf"),
        F.col("VL_RENDA_MENSAL").cast("decimal(15,2)").alias("vl_renda_mensal"),
        F.col("CD_AGENCIA").alias("cd_agencia"),
        F.timestamp_millis("DT_CADASTRO").alias("dt_cadastro"),
        F.timestamp_millis("DT_ATUALIZACAO").alias("dt_atualizacao"),
        "dt_evento_origem",
        "ds_operacao",
        "ds_sequencia",
    )


dp.create_streaming_table(
    name="silver.clientes",
    comment="Cadastro de clientes com histórico (SCD tipo 2). Contém PII sem máscara: "
    "acesso restrito à engenharia; consumo via gold.dim_cliente.",
)
dp.create_auto_cdc_flow(
    target="silver.clientes",
    source="clientes_cdc",
    keys=["id_cliente"],
    sequence_by="ds_sequencia",
    apply_as_deletes=F.expr("ds_operacao = 'd'"),
    except_column_list=["ds_operacao", "ds_sequencia"],
    stored_as_scd_type=2,
    track_history_except_column_list=["dt_atualizacao", "dt_evento_origem"],
)

# ---------------------------------------------------------------------------------------------
# contas — SCD tipo 1
# ---------------------------------------------------------------------------------------------

@dp.temporary_view()
def contas_cdc():
    return eventos("contas").select(
        F.col("ID_CONTA").alias("id_conta"),
        F.col("ID_CLIENTE").alias("id_cliente"),
        F.col("CD_AGENCIA").alias("cd_agencia"),
        F.col("TP_CONTA").alias("tp_conta"),
        F.col("VL_SALDO").cast("decimal(15,2)").alias("vl_saldo"),
        F.col("ST_CONTA").alias("st_conta"),
        data_debezium("DT_ABERTURA").alias("dt_abertura"),
        F.timestamp_millis("DT_ATUALIZACAO").alias("dt_atualizacao"),
        "dt_evento_origem",
        "ds_operacao",
        "ds_sequencia",
    )


dp.create_streaming_table(name="silver.contas", comment="Contas correntes e poupança — estado atual.")
dp.create_auto_cdc_flow(
    target="silver.contas",
    source="contas_cdc",
    keys=["id_conta"],
    sequence_by="ds_sequencia",
    apply_as_deletes=F.expr("ds_operacao = 'd'"),
    except_column_list=["ds_operacao", "ds_sequencia"],
    stored_as_scd_type=1,
)

# ---------------------------------------------------------------------------------------------
# transacoes — SCD tipo 1
# ---------------------------------------------------------------------------------------------

@dp.temporary_view()
def transacoes_cdc():
    return eventos("transacoes").select(
        F.col("ID_TRANSACAO").alias("id_transacao"),
        F.col("ID_CONTA").alias("id_conta"),
        F.col("TP_TRANSACAO").alias("tp_transacao"),
        F.col("VL_TRANSACAO").cast("decimal(15,2)").alias("vl_transacao"),
        F.timestamp_millis("DT_TRANSACAO").alias("dt_transacao"),
        "dt_evento_origem",
        "ds_operacao",
        "ds_sequencia",
    )


dp.create_streaming_table(name="silver.transacoes", comment="Transações das contas (PIX, débito, salário, tarifa).")
dp.create_auto_cdc_flow(
    target="silver.transacoes",
    source="transacoes_cdc",
    keys=["id_transacao"],
    sequence_by="ds_sequencia",
    apply_as_deletes=F.expr("ds_operacao = 'd'"),
    except_column_list=["ds_operacao", "ds_sequencia"],
    stored_as_scd_type=1,
)
