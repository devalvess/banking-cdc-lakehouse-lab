#!/bin/bash
# Gera senhas aleatórias (.env e secrets/) na primeira execução. Nada disso vai para o git.
set -euo pipefail
cd "$(dirname "$0")"

gera_senha() { tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24; }

if [ ! -f .env ]; then
  VM_IP=$(hostname -I | awk '{print $1}')
  cat > .env <<EOF
VM_PRIVATE_IP=${VM_IP}
DB2INST1_PASSWORD=$(gera_senha)
SVCDBX_PASSWORD=$(gera_senha)
SFTP_PASSWORD=$(gera_senha)
SIMULADOR_INTERVALO_SEG=2
EOF
  chmod 600 .env
  echo ".env criado (IP privado: ${VM_IP})"
fi

mkdir -p secrets
grep DB2INST1_PASSWORD .env > secrets/db2.properties
chmod 644 secrets/db2.properties
