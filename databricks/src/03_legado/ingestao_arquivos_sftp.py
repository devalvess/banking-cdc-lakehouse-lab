# Databricks notebook source
# MAGIC %md
# MAGIC # Arquivos do batch noturno (CSV) via SFTP
# MAGIC Modelo **pull**: o Databricks busca os arquivos no servidor on-prem — o on-prem não guarda
# MAGIC credencial da nuvem. Depois, Auto Loader carrega na Bronze; arquivo já processado não duplica.

# COMMAND ----------

# MAGIC %run ../comum/conexao_onprem

# COMMAND ----------

import os

import paramiko
from pyspark.sql import functions as F

VOLUME = f"/Volumes/{CATALOGO}/landing/arquivos_batch"
SFTP_SENHA = dbutils.secrets.get("onprem", "sftp-senha")

transporte = paramiko.Transport((ONPREM_HOST, 2222))
transporte.connect(username="transfer", password=SFTP_SENHA)
sftp = paramiko.SFTPClient.from_transport(transporte)

ja_copiados = set(os.listdir(VOLUME)) if os.path.exists(VOLUME) else set()
copiados = []
for nome in sorted(sftp.listdir("batch")):
    # Só arquivos completos (.csv): o gerador grava .tmp e renomeia ao final.
    if nome.endswith(".csv") and nome not in ja_copiados:
        sftp.get(f"batch/{nome}", f"{VOLUME}/{nome}")
        copiados.append(nome)
sftp.close()
transporte.close()
print(f"Arquivos copiados: {copiados or 'nenhum novo'}")

# COMMAND ----------

consulta = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("sep", ";")
    .option("rescuedDataColumn", "_dados_resgatados")  # linhas fora do layout não se perdem
    .schema("DT_REFERENCIA DATE, ID_CONTA INT, CD_AGENCIA INT, TP_CONTA STRING, VL_SALDO_FECHAMENTO DECIMAL(15,2)")
    .load(VOLUME)
    .select(
        F.col("DT_REFERENCIA").alias("dt_referencia"),
        F.col("ID_CONTA").alias("id_conta"),
        F.col("CD_AGENCIA").alias("cd_agencia"),
        F.col("TP_CONTA").alias("tp_conta"),
        F.col("VL_SALDO_FECHAMENTO").alias("vl_saldo_fechamento"),
        "_dados_resgatados",
        F.col("_metadata.file_name").alias("nm_arquivo"),
        F.current_timestamp().alias("dt_ingestao"),
    )
    .writeStream.option("checkpointLocation", f"/Volumes/{CATALOGO}/landing/checkpoints/saldos_diarios")
    .trigger(availableNow=True)
    .toTable("bronze.saldos_diarios")
)
consulta.awaitTermination()
