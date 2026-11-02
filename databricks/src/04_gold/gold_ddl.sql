-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold: definição das tabelas
-- MAGIC Criadas uma vez, com comentário em toda coluna, máscara de PII e filtro por região.
-- MAGIC A carga diária usa `INSERT OVERWRITE`, que preserva comentários, máscaras e grants.
-- MAGIC Nomes e descrições claros preparam a Gold para um Genie Space responder perguntas corretamente.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'banking_lab';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);

CREATE TABLE IF NOT EXISTS gold.dim_cliente (
  id_cliente       INT     COMMENT 'Identificador único do cliente no core bancário.',
  nr_cpf           STRING  MASK governanca.fn_mascara_cpf      COMMENT 'CPF do cliente (11 dígitos). Mascarado para quem não pertence ao grupo grp_pii_leitura.',
  nm_cliente       STRING  MASK governanca.fn_mascara_nome     COMMENT 'Nome completo do cliente. Mascarado (só primeiro nome) sem permissão de PII.',
  ds_email         STRING  MASK governanca.fn_mascara_email    COMMENT 'E-mail de contato. Mascarado sem permissão de PII.',
  nr_telefone      STRING  MASK governanca.fn_mascara_telefone COMMENT 'Telefone celular. Mascarado sem permissão de PII.',
  dt_nascimento    DATE    MASK governanca.fn_mascara_data     COMMENT 'Data de nascimento. Sem permissão de PII, mostra só o ano (1º de janeiro).',
  nr_idade         INT     COMMENT 'Idade do cliente em anos completos.',
  nm_cidade        STRING  COMMENT 'Cidade de residência.',
  sg_uf            STRING  COMMENT 'Sigla da unidade federativa (estado) de residência.',
  vl_renda_mensal  DECIMAL(15,2) COMMENT 'Renda mensal declarada, em reais.',
  ds_faixa_renda   STRING  COMMENT 'Faixa de renda: ATE_3K, 3K_A_10K, 10K_A_20K ou ACIMA_20K (reais por mês).',
  cd_agencia       INT     COMMENT 'Código da agência de relacionamento.',
  nm_agencia       STRING  COMMENT 'Nome da agência de relacionamento.',
  ds_regiao        STRING  COMMENT 'Região do Brasil da agência: SUDESTE, SUL, NORDESTE, CENTRO-OESTE ou NORTE.',
  dt_cadastro      TIMESTAMP COMMENT 'Data e hora em que o cliente foi cadastrado.'
)
WITH ROW FILTER governanca.fn_filtro_regiao ON (ds_regiao)
COMMENT 'Cadastro atual dos clientes (uma linha por cliente ativo). PII mascarada conforme o grupo de acesso.';

CREATE TABLE IF NOT EXISTS gold.posicao_cliente (
  id_cliente              INT           COMMENT 'Identificador do cliente. Relaciona com gold.dim_cliente.',
  cd_agencia              INT           COMMENT 'Código da agência de relacionamento.',
  ds_regiao               STRING        COMMENT 'Região do Brasil da agência.',
  qt_contas               INT           COMMENT 'Quantidade de contas ativas do cliente.',
  vl_saldo_contas         DECIMAL(17,2) COMMENT 'Soma dos saldos atuais de todas as contas do cliente, em reais.',
  qt_contratos_ativos     INT           COMMENT 'Quantidade de contratos de crédito ativos (não excluídos na origem).',
  vl_saldo_devedor        DECIMAL(17,2) COMMENT 'Soma do saldo devedor dos contratos de crédito, em reais.',
  nr_maior_atraso_dias    INT           COMMENT 'Maior número de dias de atraso entre os contratos do cliente. 0 = em dia.',
  qt_transacoes_30d       INT           COMMENT 'Quantidade de transações nos últimos 30 dias.',
  vl_movimentado_30d      DECIMAL(17,2) COMMENT 'Soma dos valores das transações dos últimos 30 dias (entradas e saídas), em reais.',
  dt_referencia           DATE          COMMENT 'Data de referência do cálculo.'
)
WITH ROW FILTER governanca.fn_filtro_regiao ON (ds_regiao)
COMMENT 'Posição financeira consolidada por cliente: contas, crédito e movimentação.';

CREATE TABLE IF NOT EXISTS gold.limite_pre_aprovado (
  id_cliente      INT           COMMENT 'Identificador do cliente.',
  vl_limite       DECIMAL(15,2) COMMENT 'Limite de crédito pré-aprovado, em reais. 0 = não elegível.',
  ds_faixa_risco  STRING        COMMENT 'Faixa de risco: BAIXO, MEDIO ou ALTO.',
  dt_validade     DATE          COMMENT 'Data até a qual o limite é válido.',
  dt_calculo      TIMESTAMP     COMMENT 'Quando o limite foi calculado.'
)
COMMENT 'Limite pré-aprovado por cliente. É o dado devolvido ao sistema de crédito on-premises (reverse ETL).';

CREATE TABLE IF NOT EXISTS gold.resumo_agencia (
  cd_agencia                  INT           COMMENT 'Código da agência.',
  nm_agencia                  STRING        COMMENT 'Nome da agência.',
  nm_cidade                   STRING        COMMENT 'Cidade da agência.',
  sg_uf                       STRING        COMMENT 'Estado (UF) da agência.',
  ds_regiao                   STRING        COMMENT 'Região do Brasil: SUDESTE, SUL, NORDESTE, CENTRO-OESTE ou NORTE.',
  qt_clientes                 INT           COMMENT 'Quantidade de clientes ativos da agência.',
  vl_saldo_total              DECIMAL(17,2) COMMENT 'Soma dos saldos das contas dos clientes da agência, em reais.',
  vl_carteira_credito         DECIMAL(17,2) COMMENT 'Soma do saldo devedor dos contratos de crédito da agência, em reais.',
  qt_contratos_inadimplentes  INT           COMMENT 'Contratos com mais de 90 dias de atraso.',
  pc_inadimplencia            DECIMAL(5,2)  COMMENT 'Percentual de contratos com mais de 90 dias de atraso sobre o total de contratos ativos.',
  vl_movimentado_30d          DECIMAL(17,2) COMMENT 'Valor total transacionado pelos clientes da agência nos últimos 30 dias, em reais.',
  dt_referencia               DATE          COMMENT 'Data de referência do cálculo.'
)
WITH ROW FILTER governanca.fn_filtro_regiao ON (ds_regiao)
COMMENT 'Indicadores agregados por agência, sem dados pessoais. Preparada para ser a base de um Genie Space.';
