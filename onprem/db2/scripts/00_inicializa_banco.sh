#!/bin/bash
# Executado como root pela imagem do Db2 depois que a instância sobe (scripts em /var/custom).
# Na primeira vez cria schemas, tabelas, CDC e usuários; em toda inicialização sobe o ASN Capture.

FLAG=/database/config/.setup_banco_ok

# Usuário técnico do Databricks (autenticação do Db2 é feita pelo sistema operacional).
# Recriado a cada start porque /etc/passwd não fica no volume persistente.
if ! id svcdbx >/dev/null 2>&1; then
  useradd -M -s /sbin/nologin svcdbx
  echo "svcdbx:${SVCDBX_PASSWORD}" | chpasswd
fi

# Diretórios do log arquivado e do ASN Capture (o db2inst1 não pode criar na raiz de /database).
mkdir -p /database/archive /database/asncap
chown db2inst1:db2iadm1 /database/archive /database/asncap

if [ ! -f "$FLAG" ]; then
  echo "[setup] Primeira inicialização: criando estrutura do banco ${DBNAME}"
  su - db2inst1 -c "bash /setup/db2_setup.sh ${DBNAME}" && touch "$FLAG"
fi

su - db2inst1 -c "bash /setup/start_capture.sh ${DBNAME}"
