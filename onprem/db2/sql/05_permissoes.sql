-- Usuário técnico do Databricks (SVCDBX): mínimo privilégio.
--  * Legado: somente leitura.
--  * Crédito: escreve SÓ na staging e executa a procedure. Não tem UPDATE na tabela de produção —
--    a procedure (SQL estático) executa com a autoridade de quem a criou.

GRANT CONNECT ON DATABASE TO USER SVCDBX@

GRANT SELECT ON LEGADO.AGENCIAS          TO USER SVCDBX@
GRANT SELECT ON LEGADO.CONTRATOS_CREDITO TO USER SVCDBX@

GRANT SELECT, INSERT, DELETE ON CREDITO.STG_LIMITE_PRE_APROVADO TO USER SVCDBX@
GRANT SELECT ON CREDITO.CTL_CARGA                                TO USER SVCDBX@
GRANT EXECUTE ON PROCEDURE CREDITO.SP_APLICAR_LIMITE             TO USER SVCDBX@
