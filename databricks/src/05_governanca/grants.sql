-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Grants (somente a grupos, nunca a usuários)
-- MAGIC Pré-requisito: os grupos **de conta** `grp_engenharia`, `grp_analistas` e `grp_pii_leitura`
-- MAGIC (Account Console → User management → Groups) atribuídos ao workspace. Grupos locais do
-- MAGIC workspace não são reconhecidos pelo Unity Catalog. Em produção, vêm do Entra ID via SCIM.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'banking_lab';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);

-- GRANT em catálogo não aceita IDENTIFIER(): o comando é montado numa variável.
DECLARE OR REPLACE VARIABLE comando STRING;
SET VAR comando = 'GRANT ALL PRIVILEGES ON CATALOG ' || current_catalog() || ' TO `grp_engenharia`';
EXECUTE IMMEDIATE comando;
SET VAR comando = 'GRANT USE CATALOG ON CATALOG ' || current_catalog() || ' TO `grp_analistas`';
EXECUTE IMMEDIATE comando;
SET VAR comando = 'GRANT USE CATALOG ON CATALOG ' || current_catalog() || ' TO `grp_pii_leitura`';
EXECUTE IMMEDIATE comando;

GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `grp_analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `grp_pii_leitura`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA governanca TO `grp_analistas`;
GRANT USE SCHEMA, EXECUTE ON SCHEMA governanca TO `grp_pii_leitura`;
-- silver/bronze/ops: sem acesso para analistas (área técnica).
