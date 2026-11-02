#!/bin/bash
# Roda como db2inst1. Sobe o ASN Capture (leitor do log do Db2) se ele não estiver rodando
# e ativa os registros de CDC pendentes. Uso: start_capture.sh <DBNAME>
DB=$1
. ~/sqllib/db2profile

inicia_capture() {
  cd /database/asncap
  nohup asncap capture_schema=ASNCDC capture_server="$DB" capture_path=/database/asncap \
    > /database/asncap/asncap.out 2>&1 &
  echo "[capture] ASN Capture iniciado (pid $!)"
  sleep 15
}

until db2 connect to "$DB" >/dev/null 2>&1; do sleep 5; done
db2 connect reset >/dev/null

if pgrep -u db2inst1 -x asncap >/dev/null; then
  echo "[capture] ASN Capture já está rodando"
else
  inicia_capture
fi

# A conexão do CLP pertence ao shell que a abriu: db2 dentro de $( ) roda num subshell sem
# conexão (SQL1024N). Por isso a consulta grava em arquivo e só a leitura fica no $( ).
conta_registros() {
  db2 -x "SELECT COUNT(*) FROM ASNCDC.IBMSNAP_REGISTER WHERE GLOBAL_RECORD = 'N' AND STATE = '$1'" \
    > /tmp/registros_$1.txt
}

db2 connect to "$DB" >/dev/null
conta_registros I
PENDENTES=$(tr -d ' \n' < /tmp/registros_I.txt)

if [ "$PENDENTES" -gt 0 ]; then
  echo "[capture] ativando ${PENDENTES} registro(s) com CAPSTART"
  db2 -tf /setup/ativa_registros.sql >/dev/null
  sleep 20
  # O CAPSTART preenche CD_OLD_SYNCHPOINT, que o Debezium interpreta como o fim de uma instância
  # de captura antiga — e fica esperando para sempre (loop "Schema will be changed").
  # Limpa com o Capture parado, porque ele mantém lock na IBMSNAP_REGISTER.
  db2 connect reset >/dev/null
  asnccmd capture_schema=ASNCDC capture_server="$DB" stop >/dev/null
  sleep 10
  db2 connect to "$DB" >/dev/null
  db2 "UPDATE ASNCDC.IBMSNAP_REGISTER SET CD_OLD_SYNCHPOINT = NULL WHERE GLOBAL_RECORD = 'N' AND STATE = 'A'" >/dev/null
  db2 connect reset >/dev/null
  inicia_capture
  db2 connect to "$DB" >/dev/null
fi

conta_registros A
echo "[capture] registros ativos: $(tr -d ' \n' < /tmp/registros_A.txt)"
db2 connect reset >/dev/null
