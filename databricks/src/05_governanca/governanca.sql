-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Governança: máscaras de PII, filtro por região e grants
-- MAGIC Grupos: `grp_engenharia` (tudo), `grp_pii_leitura` (vê PII sem máscara),
-- MAGIC `grp_analistas` (Gold mascarada, filtrada pela região em `governanca.acesso_regiao`).

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'banking_lab';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);

-- Quem vê dados pessoais sem máscara
CREATE OR REPLACE FUNCTION governanca.pode_ver_pii()
  RETURNS BOOLEAN
  COMMENT 'Quem vê dados pessoais sem máscara.'
  RETURN is_account_group_member('grp_pii_leitura') OR is_account_group_member('grp_engenharia');

CREATE OR REPLACE FUNCTION governanca.fn_mascara_cpf(nr_cpf STRING)
  RETURNS STRING
  COMMENT 'Exibe só os 6 dígitos centrais do CPF (padrão LGPD de mascaramento).'
  RETURN CASE WHEN governanca.pode_ver_pii() THEN nr_cpf
              ELSE concat('***.', substr(nr_cpf, 4, 3), '.', substr(nr_cpf, 7, 3), '-**') END;

CREATE OR REPLACE FUNCTION governanca.fn_mascara_nome(nm STRING)
  RETURNS STRING
  COMMENT 'Mantém só o primeiro nome.'
  RETURN CASE WHEN governanca.pode_ver_pii() THEN nm ELSE concat(split(nm, ' ')[0], ' ***') END;

CREATE OR REPLACE FUNCTION governanca.fn_mascara_email(ds STRING)
  RETURNS STRING
  RETURN CASE WHEN governanca.pode_ver_pii() THEN ds ELSE regexp_replace(ds, '^(.).*(@.*)$', '$1***$2') END;

CREATE OR REPLACE FUNCTION governanca.fn_mascara_telefone(nr STRING)
  RETURNS STRING
  RETURN CASE WHEN governanca.pode_ver_pii() THEN nr ELSE concat('*****', right(nr, 4)) END;

CREATE OR REPLACE FUNCTION governanca.fn_mascara_data(dt DATE)
  RETURNS DATE
  COMMENT 'Data de nascimento reduzida ao ano.'
  RETURN CASE WHEN governanca.pode_ver_pii() THEN dt ELSE make_date(year(dt), 1, 1) END;

-- COMMAND ----------

-- Filtro de linha por região: o mapeamento grupo -> região fica numa tabela, não no código.
CREATE TABLE IF NOT EXISTS governanca.acesso_regiao (
  nm_grupo  STRING COMMENT 'Grupo de conta do Databricks.',
  ds_regiao STRING COMMENT 'Região que o grupo pode ver.'
) COMMENT 'Mapa de acesso por região usado pelo filtro de linha das tabelas Gold.';

MERGE INTO governanca.acesso_regiao t
USING (SELECT 'grp_analistas' AS nm_grupo, 'SUDESTE' AS ds_regiao) s
ON t.nm_grupo = s.nm_grupo AND t.ds_regiao = s.ds_regiao
WHEN NOT MATCHED THEN INSERT *;

CREATE OR REPLACE FUNCTION governanca.fn_filtro_regiao(p_regiao STRING)
  RETURNS BOOLEAN
  COMMENT 'Engenharia e PII veem tudo; analistas só as regiões mapeadas em acesso_regiao.'
  RETURN governanca.pode_ver_pii()
      OR EXISTS (SELECT 1 FROM governanca.acesso_regiao a
                  WHERE a.ds_regiao = p_regiao AND is_account_group_member(a.nm_grupo));

