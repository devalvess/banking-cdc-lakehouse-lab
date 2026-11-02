# Servidor on-premises simulado

Pilha que roda na VM `vm-onprem-db2` (criada por [`infra/terraform`](../infra/terraform)):

| Serviço | Papel no banco simulado | Porta |
|---|---|---|
| `db2` | IBM Db2 11.5 — schemas `CORE` (com CDC), `LEGADO` (sem CDC) e `CREDITO` (destino do reverse ETL) | 50000 (rede privada) |
| `kafka` | Broker Kafka (KRaft, 1 nó) | 9092 (rede privada) |
| `kafka-connect` | Debezium Db2 connector: lê as tabelas CD do ASN Capture e publica no Kafka | 8083 (localhost) |
| `kafka-ui` | Interface para ver tópicos e lag | 8080 (localhost) |
| `sftp` | Servidor de arquivos com os CSVs do batch noturno | 2222 (rede privada) |
| `simulador` | Gera movimento contínuo (clientes, contas, transações, contratos) | — |

## Subir o ambiente

```bash
# na sua máquina, a partir da raiz do repositório
scp -r onprem azureuser@<IP_ADMIN>:/opt/

# na VM
ssh azureuser@<IP_ADMIN>
cd /opt/onprem
bash preparar.sh                 # gera .env e secrets/ com senhas aleatórias
docker compose up -d --build     # 1ª subida do Db2 leva ~5-10 min
docker logs -f db2               # aguarde "[setup] concluído" e "[capture] ASN Capture iniciado"
bash connect/registrar_conector.sh
```

Kafka UI no seu navegador: `ssh -L 8080:localhost:8080 azureuser@<IP_ADMIN>` e abra http://localhost:8080.

## Comandos úteis para a demonstração

```bash
# Rajada de 2.000 operações (gera bastante log)
docker compose exec simulador python gerador.py rajada 2000

# Gerar o CSV do dia
docker compose exec simulador python gerador.py csv

# Demo "o CDC pesa na origem": pare o consumidor do log e veja a origem crescer
docker compose stop kafka-connect
docker compose exec simulador python gerador.py rajada 2000
docker exec -it db2 su - db2inst1 -c "db2 connect to BANCO && db2 \"SELECT COUNT(*) AS LINHAS_CD FROM ASNCDC.CDC_CORE_TRANSACOES\""
docker exec -it db2 du -sh /database/archive
docker compose start kafka-connect
```
