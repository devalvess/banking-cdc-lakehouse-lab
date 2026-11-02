-- Databricks notebook source
-- MAGIC %md
-- MAGIC # FinOps: schema, volume de entrada e tabela de custos
-- MAGIC
-- MAGIC O custo vem da **Cost Management API do Azure**, que é a fatura real (BRL). Ela já inclui as
-- MAGIC DBUs do Databricks, as VMs dos clusters, a VM on-prem e a rede. As tags `projeto` e `carga`,
-- MAGIC aplicadas pelo Terraform e pelos jobs, dividem esse custo por carga.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'banking_lab';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);

CREATE SCHEMA IF NOT EXISTS finops COMMENT 'Custos do projeto, lidos da fatura do Azure.';

CREATE VOLUME IF NOT EXISTS finops.custos_azure
  COMMENT 'Respostas brutas da Cost Management API, enviadas por scripts/coletar_custos_azure.ps1.';

CREATE TABLE IF NOT EXISTS finops.custo_azure_diario (
  dt_uso                DATE          COMMENT 'Dia do consumo (UTC).',
  nm_resource_group     STRING        COMMENT 'Resource group do recurso no Azure.',
  nm_recurso            STRING        COMMENT 'Nome do recurso (VM, NAT gateway, storage, workspace...).',
  id_recurso            STRING        COMMENT 'Resource ID completo no Azure.',
  ds_categoria_medidor  STRING        COMMENT 'Categoria do medidor do Azure (Virtual Machines, Azure Databricks, NAT Gateway...).',
  ds_camada             STRING        COMMENT 'Onde o custo nasce: On-prem simulado, Databricks · DBUs, Databricks · VMs dos clusters ou Databricks · infraestrutura compartilhada.',
  ds_carga              STRING        COMMENT 'Valor da tag carga (job ou pipeline que gerou o custo). Nulo para custos sem tag, como a rede compartilhada.',
  vl_custo              DECIMAL(18,6) COMMENT 'Custo real do dia, na moeda da fatura.',
  cd_moeda              STRING        COMMENT 'Moeda da fatura (BRL).',
  dt_ingestao           TIMESTAMP     COMMENT 'Quando o custo foi carregado.'
) COMMENT 'Custo diário real do projeto no Azure. Os dias da janela de cada coleta são regravados, porque o Azure reajusta dias recentes.';
