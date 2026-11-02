#!/bin/bash
# Roda como db2inst1, uma única vez. Uso: db2_setup.sh <DBNAME>
DB=$1
. ~/sqllib/db2profile

until db2 connect to "$DB" >/dev/null 2>&1; do echo "[setup] aguardando ${DB}..."; sleep 5; done

# 1. Log arquivado em disco: pré-requisito do CDC por log. É este diretório que cresce
#    quando o leitor do log (ASN Capture) fica para trás — ver README, cenário 1.
db2 update db cfg for "$DB" using LOGARCHMETH1 "DISK:/database/archive/"
db2 connect reset
db2 force applications all
db2 deactivate db "$DB"
db2 backup db "$DB" to /dev/null    # obrigatório após mudar para log arquivado
db2 activate db "$DB"
db2 connect to "$DB"

# 2. Permite que o JDBC leia o catálogo (usado pelo Debezium e pelo Spark).
# (sem subshell: o CLP do Db2 amarra a conexão ao processo do shell)
cd ~/sqllib/bnd
db2 bind db2schema.bnd blocking all grant public sqlerror continue
cd ~

# 3. Tabelas de controle do ASN Capture + procedures ADDTABLE/REMOVETABLE (Debezium).
db2 -tvmf /setup/asncdc/asncdctables.sql
db2 -tvmf /setup/asncdc/asncdcaddremove.sql

# 4. Sistemas do banco: core (com CDC), legado (sem CDC) e crédito (destino do reverse ETL).
for f in /setup/sql/*.sql; do
  echo "[setup] executando $f"
  db2 -td@ -vmf "$f"
done

db2 connect reset
echo "[setup] concluído"
