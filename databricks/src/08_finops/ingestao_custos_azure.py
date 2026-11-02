# Databricks notebook source
# MAGIC %md
# MAGIC # FinOps: custos do Azure (Cost Management API)
# MAGIC
# MAGIC Custo diário real (o que vai para a fatura, em BRL) de tudo que o projeto usa no Azure:
# MAGIC
# MAGIC | Origem do custo | Como é identificado |
# MAGIC |---|---|
# MAGIC | Servidor on-prem simulado (VM, disco, IP, VNet) | Resource group `rg-onprem-simulado` |
# MAGIC | DBUs e VMs dos clusters | Tags `projeto` e `carga`, que o Databricks propaga para a cobrança |
# MAGIC | Rede e storage gerenciados do workspace (NAT gateway, IP, storage) | Resource group gerenciado: custo compartilhado |
# MAGIC
# MAGIC **De onde vem a resposta da API** (`origem`):
# MAGIC - `volume` (padrão): o script `scripts/coletar_custos_azure.ps1` consulta a API com a identidade
# MAGIC   do `az login` e grava a resposta no volume `finops.custos_azure`. Serve para tenants onde não
# MAGIC   é possível criar um service principal.
# MAGIC - `api`: o próprio job consulta a API com um service principal com o papel **Cost Management
# MAGIC   Reader** (secret scope `finops`). É o caminho de produção.
# MAGIC
# MAGIC O Azure fecha o custo com 8 a 24 h de atraso e reajusta dias recentes: cada carga regrava a
# MAGIC janela inteira (`replaceWhere`), o que também a torna idempotente.

# COMMAND ----------

import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import requests
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "banking_lab")
dbutils.widgets.dropdown("origem", "volume", ["volume", "api"])
dbutils.widgets.text("dias", "35")
dbutils.widgets.text("rg_onprem", "rg-onprem-simulado")
dbutils.widgets.text("rg_gerenciado", "databricks-rg-adb_databricks_dev1-otdiskxfuqjga")
dbutils.widgets.text("tag_projeto", "banking-cdc-lakehouse-lab")

CATALOGO = dbutils.widgets.get("catalogo")
ORIGEM = dbutils.widgets.get("origem")
DIAS = int(dbutils.widgets.get("dias"))
RG_ONPREM = dbutils.widgets.get("rg_onprem").lower()
RG_GERENCIADO = dbutils.widgets.get("rg_gerenciado").lower()
TAG_PROJETO = dbutils.widgets.get("tag_projeto")
VOLUME = f"/Volumes/{CATALOGO}/finops/custos_azure"

spark.sql(f"USE CATALOG {CATALOGO}")

# COMMAND ----------

# MAGIC %md ## Leitura da resposta da API

# COMMAND ----------

def corpo_consulta(inicio, fim):
    """Mesma consulta do script local: custo diário por recurso, medidor e tag carga."""
    return {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": f"{inicio}T00:00:00Z", "to": f"{fim}T23:59:59Z"},
        "dataset": {
            "granularity": "Daily",
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
            "grouping": [
                {"type": "Dimension", "name": "ResourceId"},
                {"type": "Dimension", "name": "MeterCategory"},
                {"type": "TagKey", "name": "carga"},
            ],
            "filter": {"or": [
                {"dimensions": {"name": "ResourceGroupName", "operator": "In",
                                "values": [RG_ONPREM, RG_GERENCIADO]}},
                {"tags": {"name": "projeto", "operator": "In", "values": [TAG_PROJETO]}},
            ]},
        },
    }


def consulta_api():
    tenant = dbutils.secrets.get("finops", "tenant-id")
    assinatura = dbutils.secrets.get("finops", "subscription-id")
    token = requests.post(
        f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": dbutils.secrets.get("finops", "client-id"),
            "client_secret": dbutils.secrets.get("finops", "client-secret"),
            "scope": "https://management.azure.com/.default",
        },
        timeout=30,
    )
    token.raise_for_status()

    fim = datetime.now(timezone.utc).date()
    inicio = fim - timedelta(days=DIAS)
    url = (f"https://management.azure.com/subscriptions/{assinatura}"
           "/providers/Microsoft.CostManagement/query?api-version=2023-11-01")
    cabecalho = {"Authorization": f"Bearer {token.json()['access_token']}"}
    colunas, linhas = None, []
    while url:
        for _ in range(6):
            resposta = requests.post(url, json=corpo_consulta(inicio, fim), headers=cabecalho, timeout=60)
            if resposta.status_code != 429:
                break
            # A API tem cota baixa por minuto e diz nos cabeçalhos quanto esperar.
            esperas = [int(v) for k, v in resposta.headers.items()
                       if k.lower().endswith("retry-after") and v.isdigit()]
            time.sleep(max(esperas, default=20) + 1)
        resposta.raise_for_status()
        props = resposta.json()["properties"]
        colunas = [c["name"] for c in props["columns"]]
        linhas += props["rows"]
        url = props.get("nextLink")
    return {"inicio": str(inicio), "fim": str(fim), "colunas": colunas, "linhas": linhas}


def ultima_coleta_do_volume():
    arquivos = sorted(f for f in os.listdir(VOLUME) if f.endswith(".json"))
    if not arquivos:
        raise Exception(f"Nenhuma coleta em {VOLUME}: rode scripts/coletar_custos_azure.ps1.")
    print(f"Arquivo: {arquivos[-1]}")
    with open(f"{VOLUME}/{arquivos[-1]}", encoding="utf-8-sig") as arquivo:
        return json.load(arquivo)


coleta = consulta_api() if ORIGEM == "api" else ultima_coleta_do_volume()

# COMMAND ----------

# MAGIC %md ## Classificação e gravação

# COMMAND ----------

def camada(resource_group, categoria):
    if resource_group == RG_ONPREM:
        return "On-prem simulado"
    if categoria == "Azure Databricks":
        return "Databricks · DBUs"
    if categoria == "Virtual Machines":
        return "Databricks · VMs dos clusters"
    return "Databricks · infraestrutura compartilhada"


registros = []
for valores in coleta["linhas"]:
    linha = dict(zip(coleta["colunas"], valores))
    id_recurso = linha["ResourceId"].lower()
    resource_group = id_recurso.split("/")[4] if id_recurso.count("/") >= 4 else ""
    dia = str(linha["UsageDate"])
    registros.append((
        date(int(dia[:4]), int(dia[4:6]), int(dia[6:])),
        resource_group,
        id_recurso.rsplit("/", 1)[-1],
        id_recurso,
        linha["MeterCategory"],
        camada(resource_group, linha["MeterCategory"]),
        linha.get("TagValue"),
        Decimal(str(round(linha["Cost"], 6))),
        linha["Currency"],
    ))

custos = spark.createDataFrame(
    registros,
    "dt_uso DATE, nm_resource_group STRING, nm_recurso STRING, id_recurso STRING, "
    "ds_categoria_medidor STRING, ds_camada STRING, ds_carga STRING, vl_custo DECIMAL(18,6), cd_moeda STRING",
).withColumn("dt_ingestao", F.current_timestamp())

(
    custos.write.mode("overwrite")
    .option("replaceWhere", f"dt_uso >= '{coleta['inicio']}'")
    .saveAsTable("finops.custo_azure_diario")
)

print(f"{len(registros)} linhas de custo entre {coleta['inicio']} e {coleta['fim']}")
display(
    custos.groupBy("ds_camada", "cd_moeda")
    .agg(F.sum("vl_custo").alias("vl_custo"))
    .orderBy(F.desc("vl_custo"))
)
