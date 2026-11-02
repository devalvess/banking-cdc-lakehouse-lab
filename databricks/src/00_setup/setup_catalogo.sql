-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup do catálogo `banking_lab`
-- MAGIC Cria catálogo, schemas, volume e as tabelas operacionais (`ops`). Idempotente.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'banking_lab';

-- COMMAND ----------

-- O catálogo é pré-requisito (depende da storage de cada ambiente) — ver README, "Como executar".
USE CATALOG IDENTIFIER(:catalogo);

CREATE SCHEMA IF NOT EXISTS landing    COMMENT 'Arquivos brutos recebidos do on-premises.';
CREATE SCHEMA IF NOT EXISTS bronze     COMMENT 'Dados como chegaram da origem, append-only, com metadados de ingestão.';
CREATE SCHEMA IF NOT EXISTS silver     COMMENT 'Dados limpos e com histórico. Área técnica: acesso restrito à engenharia.';
CREATE SCHEMA IF NOT EXISTS gold       COMMENT 'Tabelas de negócio para consumo, com PII mascarada e descrições de coluna.';
CREATE SCHEMA IF NOT EXISTS ops        COMMENT 'Controle operacional: marca d''água, reconciliação e auditoria do reverse ETL.';
CREATE SCHEMA IF NOT EXISTS governanca COMMENT 'Funções de máscara e filtro de linha.';

CREATE VOLUME IF NOT EXISTS landing.arquivos_batch
  COMMENT 'CSVs do batch noturno, copiados do servidor SFTP on-prem.';

CREATE VOLUME IF NOT EXISTS landing.checkpoints
  COMMENT 'Checkpoints das ingestões em streaming (Auto Loader).';

-- COMMAND ----------

-- MAGIC %md ## Tabelas operacionais

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ops.controle_watermark (
  nm_tabela        STRING    COMMENT 'Tabela da origem (schema.tabela no Db2).',
  dt_marca_dagua   TIMESTAMP COMMENT 'Maior DT_ATUALIZACAO já carregada.',
  qt_linhas_lidas  BIGINT    COMMENT 'Linhas lidas na última execução.',
  dt_execucao      TIMESTAMP COMMENT 'Quando a carga rodou.'
) COMMENT 'Marca d''água das cargas incrementais via JDBC.';

CREATE TABLE IF NOT EXISTS ops.reconciliacao (
  id_execucao      STRING        COMMENT 'Identificador da execução da carga.',
  nm_tabela        STRING        COMMENT 'Tabela reconciliada.',
  ds_metrica       STRING        COMMENT 'Métrica comparada (quantidade, soma de valor...).',
  vl_origem        DECIMAL(38,2) COMMENT 'Valor calculado no sistema de origem.',
  vl_lakehouse     DECIMAL(38,2) COMMENT 'Valor calculado na Silver.',
  vl_diferenca     DECIMAL(38,2) COMMENT 'Origem menos Lakehouse.',
  fl_ok            BOOLEAN       COMMENT 'Verdadeiro quando a diferença é zero.',
  dt_execucao      TIMESTAMP     COMMENT 'Quando a reconciliação rodou.'
) COMMENT 'Prova de integridade: origem x Lakehouse por contagem e somatório.';

CREATE TABLE IF NOT EXISTS ops.reverse_etl_execucoes (
  id_lote              STRING        COMMENT 'Lote enviado. Formato <tabela>_v<versão Delta>.',
  nm_tabela_origem     STRING        COMMENT 'Tabela Gold de origem.',
  nr_versao_origem     BIGINT        COMMENT 'Versão Delta enviada — reproduzível com VERSION AS OF.',
  qt_linhas            BIGINT        COMMENT 'Linhas enviadas.',
  vl_soma              DECIMAL(38,2) COMMENT 'Soma de VL_LIMITE enviada.',
  st_execucao          STRING        COMMENT 'INICIADO | APLICADO | REJEITADO | ERRO | JA_APLICADO.',
  qt_linhas_aplicadas  BIGINT        COMMENT 'Linhas inseridas ou alteradas na produção on-prem.',
  ds_mensagem          STRING        COMMENT 'Detalhe do resultado.',
  nm_usuario           STRING        COMMENT 'Identidade que executou o envio.',
  dt_inicio            TIMESTAMP,
  dt_fim               TIMESTAMP
) COMMENT 'Auditoria do reverse ETL para o sistema de crédito on-premises.';
