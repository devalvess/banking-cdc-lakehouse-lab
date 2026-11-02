# Databricks notebook source
# MAGIC %md
# MAGIC # Conexão com o on-premises
# MAGIC Usado via `%run`. Credenciais vêm do secret scope `onprem` (em produção: Azure Key Vault).
# MAGIC O usuário `SVCDBX` só lê o legado e só escreve na staging do sistema de crédito.

# COMMAND ----------

dbutils.widgets.text("catalogo", "banking_lab")
dbutils.widgets.text("onprem_host", "10.50.1.10")

CATALOGO = dbutils.widgets.get("catalogo")
ONPREM_HOST = dbutils.widgets.get("onprem_host")

DB2_URL = f"jdbc:db2://{ONPREM_HOST}:50000/BANCO"
DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver"
DB2_USUARIO = dbutils.secrets.get("onprem", "db2-usuario")
DB2_SENHA = dbutils.secrets.get("onprem", "db2-senha")

spark.sql(f"USE CATALOG {CATALOGO}")


def ler_db2(query, **opcoes):
    """Leitura JDBC do Db2. Poucas conexões e fetch em blocos: o legado não aguenta a
    concorrência de um cluster Spark inteiro."""
    leitor = (
        spark.read.format("jdbc")
        .option("url", DB2_URL)
        .option("driver", DB2_DRIVER)
        .option("user", DB2_USUARIO)
        .option("password", DB2_SENHA)
        .option("query", query)
        .option("fetchsize", 5000)
    )
    for chave, valor in opcoes.items():
        leitor = leitor.option(chave, valor)
    return leitor.load()


def conexao_db2():
    """Conexão JDBC direta (para DELETE na staging e CALL da procedure).
    O driver vem de uma biblioteca Maven do cluster, visível só pelo classloader do Spark —
    Class.forName/DriverManager chamados via Py4J não o encontram."""
    jvm = spark._jvm
    classe = jvm.org.apache.spark.util.Utils.classForName(DB2_DRIVER, True, False)
    driver = classe.newInstance()  # getDeclaredConstructor() é varargs e não é chamável via Py4J
    propriedades = jvm.java.util.Properties()
    propriedades.setProperty("user", DB2_USUARIO)
    propriedades.setProperty("password", DB2_SENHA)
    return driver.connect(DB2_URL, propriedades)
