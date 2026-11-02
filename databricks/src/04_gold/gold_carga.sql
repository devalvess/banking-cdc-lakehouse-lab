-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: carga diária
-- MAGIC Só roda depois que a reconciliação do legado passou (condição no job).

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'banking_lab';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);

CREATE OR REPLACE TEMPORARY VIEW clientes_atuais AS
SELECT c.*, a.nm_agencia, a.ds_regiao
  FROM silver.clientes c
  LEFT JOIN silver.agencias a USING (cd_agencia)
 WHERE c.`__END_AT` IS NULL;   -- versão vigente do SCD tipo 2

-- COMMAND ----------

INSERT OVERWRITE gold.dim_cliente
SELECT id_cliente, nr_cpf, nm_cliente, ds_email, nr_telefone, dt_nascimento,
       CAST(floor(months_between(current_date(), dt_nascimento) / 12) AS INT) AS nr_idade,
       nm_cidade, sg_uf, vl_renda_mensal,
       CASE WHEN vl_renda_mensal <= 3000  THEN 'ATE_3K'
            WHEN vl_renda_mensal <= 10000 THEN '3K_A_10K'
            WHEN vl_renda_mensal <= 20000 THEN '10K_A_20K'
            ELSE 'ACIMA_20K' END AS ds_faixa_renda,
       cd_agencia, nm_agencia, ds_regiao, dt_cadastro
  FROM clientes_atuais;

-- COMMAND ----------

-- Calculada a partir da Silver (sem filtro de linha), e reaproveitada pelas outras tabelas:
-- a carga não depende de quem a executa enxergar todas as regiões.
CREATE OR REPLACE TEMPORARY VIEW posicao_calc AS
WITH contas AS (
  SELECT id_cliente, COUNT(*) AS qt_contas, SUM(vl_saldo) AS vl_saldo_contas
    FROM silver.contas WHERE st_conta = 'ATIVA' GROUP BY id_cliente
), credito AS (
  SELECT id_cliente, COUNT(*) AS qt_contratos_ativos, SUM(vl_saldo_devedor) AS vl_saldo_devedor,
         MAX(nr_dias_atraso) AS nr_maior_atraso_dias
    FROM silver.contratos_credito WHERE NOT fl_excluido GROUP BY id_cliente
), movimento AS (
  SELECT ct.id_cliente, COUNT(*) AS qt_transacoes_30d, SUM(t.vl_transacao) AS vl_movimentado_30d
    FROM silver.transacoes t JOIN silver.contas ct USING (id_conta)
   WHERE t.dt_transacao >= current_date() - INTERVAL 30 DAYS
   GROUP BY ct.id_cliente
)
SELECT c.id_cliente, c.cd_agencia, c.ds_regiao,
       CAST(COALESCE(ct.qt_contas, 0) AS INT)                     AS qt_contas,
       CAST(COALESCE(ct.vl_saldo_contas, 0) AS DECIMAL(17,2))     AS vl_saldo_contas,
       CAST(COALESCE(cr.qt_contratos_ativos, 0) AS INT)           AS qt_contratos_ativos,
       CAST(COALESCE(cr.vl_saldo_devedor, 0) AS DECIMAL(17,2))    AS vl_saldo_devedor,
       COALESCE(cr.nr_maior_atraso_dias, 0)                       AS nr_maior_atraso_dias,
       CAST(COALESCE(m.qt_transacoes_30d, 0) AS INT)              AS qt_transacoes_30d,
       CAST(COALESCE(m.vl_movimentado_30d, 0) AS DECIMAL(17,2))   AS vl_movimentado_30d,
       current_date()                                             AS dt_referencia
  FROM clientes_atuais c
  LEFT JOIN contas    ct USING (id_cliente)
  LEFT JOIN credito   cr USING (id_cliente)
  LEFT JOIN movimento m  USING (id_cliente);

INSERT OVERWRITE gold.posicao_cliente SELECT * FROM posicao_calc;

-- COMMAND ----------

-- Regra de crédito (ilustrativa): risco pelo atraso e pelo endividamento em relação à renda.
INSERT OVERWRITE gold.limite_pre_aprovado
WITH base AS (
  SELECT p.id_cliente, c.vl_renda_mensal, p.vl_saldo_devedor, p.nr_maior_atraso_dias,
         CASE WHEN p.nr_maior_atraso_dias > 90 OR p.vl_saldo_devedor > 12 * c.vl_renda_mensal THEN 'ALTO'
              WHEN p.nr_maior_atraso_dias > 15 OR p.vl_saldo_devedor >  6 * c.vl_renda_mensal THEN 'MEDIO'
              ELSE 'BAIXO' END AS ds_faixa_risco
    FROM posicao_calc p JOIN clientes_atuais c USING (id_cliente)
)
SELECT id_cliente,
       CAST(GREATEST(0, floor((CASE ds_faixa_risco WHEN 'BAIXO' THEN 3.0 * vl_renda_mensal
                                                   WHEN 'MEDIO' THEN 1.5 * vl_renda_mensal
                                                   ELSE 0 END
                               - 0.1 * vl_saldo_devedor) / 100) * 100) AS DECIMAL(15,2)) AS vl_limite,
       ds_faixa_risco,
       current_date() + INTERVAL 30 DAYS AS dt_validade,
       current_timestamp() AS dt_calculo
  FROM base;

-- COMMAND ----------

INSERT OVERWRITE gold.resumo_agencia
SELECT a.cd_agencia, a.nm_agencia, a.nm_cidade, a.sg_uf, a.ds_regiao,
       COUNT(p.id_cliente)                                AS qt_clientes,
       COALESCE(SUM(p.vl_saldo_contas), 0)                AS vl_saldo_total,
       COALESCE(SUM(p.vl_saldo_devedor), 0)               AS vl_carteira_credito,
       COALESCE(SUM(inad.qt), 0)                          AS qt_contratos_inadimplentes,
       CAST(100 * COALESCE(SUM(inad.qt), 0) / NULLIF(SUM(p.qt_contratos_ativos), 0) AS DECIMAL(5,2))
                                                          AS pc_inadimplencia,
       COALESCE(SUM(p.vl_movimentado_30d), 0)             AS vl_movimentado_30d,
       current_date()                                     AS dt_referencia
  FROM silver.agencias a
  LEFT JOIN posicao_calc p ON p.cd_agencia = a.cd_agencia
  LEFT JOIN (SELECT id_cliente, COUNT(*) AS qt FROM silver.contratos_credito
              WHERE NOT fl_excluido AND nr_dias_atraso > 90 GROUP BY id_cliente) inad
         ON inad.id_cliente = p.id_cliente
 GROUP BY ALL;
