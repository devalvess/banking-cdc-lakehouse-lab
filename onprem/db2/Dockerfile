# Db2 Community Edition com o ASN Capture preparado para o Debezium.
# Os scripts em asncdc/ vêm do projeto Debezium (Apache 2.0):
# https://github.com/debezium/debezium-examples/tree/main/tutorial/debezium-db2-init/db2server
FROM icr.io/db2_community/db2:11.5.9.0

COPY asncdc/ /setup/asncdc/
COPY sql/    /setup/sql/
COPY scripts/db2_setup.sh scripts/start_capture.sh scripts/ativa_registros.sql /setup/
COPY scripts/00_inicializa_banco.sh /var/custom/

RUN chmod -R 755 /setup /var/custom
