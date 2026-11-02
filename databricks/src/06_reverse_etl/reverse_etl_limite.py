# Databricks notebook source
# MAGIC %md
# MAGIC # Reverse ETL: `gold.limite_pre_aprovado` → sistema de crédito on-premises (Db2)
# MAGIC
# MAGIC | Etapa | Onde | Por quê |
# MAGIC |---|---|---|
# MAGIC | 1. Fixa a versão Delta da Gold | Databricks | O que foi enviado é reproduzível (`VERSION AS OF`) |
# MAGIC | 2. Registra o lote em `ops.reverse_etl_execucoes` | Databricks | Auditoria e idempotência |
# MAGIC | 3. Grava na **staging** via JDBC | Db2 `CREDITO.STG_*` | A carga pesada nunca toca a tabela de produção |
# MAGIC | 4. `CALL SP_APLICAR_LIMITE` | Db2 | Valida contagem/soma e aplica com MERGE em lotes, commit por lote e lock timeout de 5 s |
# MAGIC | 5. Fecha a auditoria dos dois lados | Databricks + `CREDITO.CTL_CARGA` | O time on-prem audita sem acessar o Databricks |
# MAGIC
# MAGIC `simular_falha_transmissao = sim` descarta 1% das linhas antes do envio: a procedure rejeita
# MAGIC o lote e a produção não é alterada.

# COMMAND ----------

# MAGIC %run ../comum/conexao_onprem

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("tamanho_lote", "200")
dbutils.widgets.dropdown("simular_falha_transmissao", "nao", ["nao", "sim"])

TABELA = "gold.limite_pre_aprovado"
TAMANHO_LOTE = int(dbutils.widgets.get("tamanho_lote"))
SIMULAR_FALHA = dbutils.widgets.get("simular_falha_transmissao") == "sim"
USUARIO = spark.sql("SELECT current_user()").first()[0]

# COMMAND ----------

# MAGIC %md ## 1. Fixa a versão e monta o lote

# COMMAND ----------

versao = spark.sql(f"DESCRIBE HISTORY {TABELA} LIMIT 1").first()["version"]
id_lote = f"LIMITE_v{versao}" + ("_falha" if SIMULAR_FALHA else "")

ja_aplicado = spark.sql(f"""
  SELECT 1 FROM ops.reverse_etl_execucoes WHERE id_lote = '{id_lote}' AND st_execucao = 'APLICADO'
""").count() > 0
if ja_aplicado:
    dbutils.notebook.exit(f"Lote {id_lote} já aplicado — nada a enviar (idempotência).")

lote = (
    spark.read.option("versionAsOf", versao).table(TABELA)
    .select(
        F.lit(id_lote).alias("ID_LOTE"),
        F.col("id_cliente").alias("ID_CLIENTE"),
        F.col("vl_limite").alias("VL_LIMITE"),
        F.col("ds_faixa_risco").alias("DS_FAIXA_RISCO"),
        F.col("dt_validade").alias("DT_VALIDADE"),
        # Hash dos campos de negócio: a procedure só altera linhas que realmente mudaram.
        F.sha2(F.concat_ws("|", "vl_limite", "ds_faixa_risco", "dt_validade"), 256).alias("DS_HASH_LINHA"),
    )
    .cache()
)
esperado = lote.agg(F.count("*").alias("qt"), F.sum("VL_LIMITE").alias("vl")).first()
QT_ESPERADA, VL_ESPERADO = esperado["qt"], esperado["vl"]
print(f"Lote {id_lote}: versão {versao}, {QT_ESPERADA} linhas, soma {VL_ESPERADO}")

# COMMAND ----------

# MAGIC %md ## 2. Abre a auditoria

# COMMAND ----------

# Uma tentativa anterior deste lote que caiu no meio fica registrada como interrompida.
spark.sql(
    "UPDATE ops.reverse_etl_execucoes SET st_execucao = 'INTERROMPIDO', dt_fim = current_timestamp() "
    "WHERE id_lote = :lote AND st_execucao = 'INICIADO'",
    args={"lote": id_lote},
)
spark.sql(f"""
  INSERT INTO ops.reverse_etl_execucoes
  VALUES ('{id_lote}', '{TABELA}', {versao}, {QT_ESPERADA}, {VL_ESPERADO}, 'INICIADO',
          NULL, NULL, '{USUARIO}', current_timestamp(), NULL)
""")

# COMMAND ----------

# MAGIC %md ## 3 e 4. Envia para a staging e aplica na produção on-prem

# COMMAND ----------

def executa_sql(conn, sql, *params):
    stmt = conn.prepareStatement(sql)
    for i, p in enumerate(params, start=1):
        stmt.setString(i, p)
    stmt.executeUpdate()
    stmt.close()


def envia_staging():
    conn = conexao_db2()
    try:  # reenvio do mesmo lote (ex.: após falha) começa com a staging limpa
        executa_sql(conn, "DELETE FROM CREDITO.STG_LIMITE_PRE_APROVADO WHERE ID_LOTE = ?", id_lote)
    finally:
        conn.close()

    envio = lote.sample(fraction=0.99, seed=42) if SIMULAR_FALHA else lote
    (
        envio.repartition(2)                     # no máximo 2 conexões simultâneas no Db2
        .write.format("jdbc")
        .option("url", DB2_URL)
        .option("driver", DB2_DRIVER)
        .option("user", DB2_USUARIO)
        .option("password", DB2_SENHA)
        .option("dbtable", "CREDITO.STG_LIMITE_PRE_APROVADO")
        .option("batchsize", 1000)
        .mode("append")
        .save()
    )


def aplica_producao():
    jvm = spark._jvm
    conn = conexao_db2()
    try:
        call = conn.prepareCall("CALL CREDITO.SP_APLICAR_LIMITE(?, ?, ?, ?, ?, ?, ?)")
        call.setString(1, id_lote)
        call.setLong(2, versao)
        call.setInt(3, QT_ESPERADA)
        call.setBigDecimal(4, jvm.java.math.BigDecimal(str(VL_ESPERADO)))
        call.setInt(5, TAMANHO_LOTE)
        call.registerOutParameter(6, jvm.java.sql.Types.VARCHAR)
        call.registerOutParameter(7, jvm.java.sql.Types.INTEGER)
        call.execute()
        status, qt_aplicadas = call.getString(6), call.getInt(7)
        call.close()

        ctl = conn.prepareStatement("SELECT DS_MENSAGEM FROM CREDITO.CTL_CARGA WHERE ID_LOTE = ?")
        ctl.setString(1, id_lote)
        rs = ctl.executeQuery()
        mensagem = rs.getString(1) if rs.next() else None
        rs.close()
        ctl.close()

        if status == "APLICADO":   # staging limpa: o histórico fica em CTL_CARGA e no Databricks
            executa_sql(conn, "DELETE FROM CREDITO.STG_LIMITE_PRE_APROVADO WHERE ID_LOTE = ?", id_lote)
        return status, qt_aplicadas, mensagem
    finally:
        conn.close()


try:
    envia_staging()
    status, qt_aplicadas, mensagem = aplica_producao()
except Exception as erro:  # qualquer falha fica na auditoria, não só no log do job
    status, qt_aplicadas, mensagem = "ERRO", None, str(erro)[:500]

print(f"Resultado: {status} | linhas aplicadas: {qt_aplicadas} | {mensagem or ''}")

# COMMAND ----------

# MAGIC %md ## 5. Fecha a auditoria

# COMMAND ----------

spark.sql(
    """
  UPDATE ops.reverse_etl_execucoes
     SET st_execucao = :status, qt_linhas_aplicadas = :qt, ds_mensagem = :msg, dt_fim = current_timestamp()
   WHERE id_lote = :lote AND st_execucao = 'INICIADO'
""",
    args={"status": status, "qt": qt_aplicadas or 0, "msg": mensagem or "", "lote": id_lote},
)
display(spark.sql(f"SELECT * FROM ops.reverse_etl_execucoes WHERE id_lote = '{id_lote}'"))

if status not in ("APLICADO", "JA_APLICADO"):
    raise Exception(f"Reverse ETL {status}: {mensagem}")
