-- Sistema de crédito on-premises: DESTINO do reverse ETL.
-- O Databricks só escreve na staging; quem altera a tabela de produção é a procedure,
-- em lotes curtos e com timeout de lock — a aplicação que lê LIMITE_PRE_APROVADO não trava.

CREATE SCHEMA CREDITO@

-- Tabela de produção, lida pelo sistema de concessão de crédito.
CREATE TABLE CREDITO.LIMITE_PRE_APROVADO (
    ID_CLIENTE       INTEGER       NOT NULL,
    VL_LIMITE        DECIMAL(15,2) NOT NULL,
    DS_FAIXA_RISCO   VARCHAR(10)   NOT NULL,   -- BAIXO | MEDIO | ALTO
    DT_VALIDADE      DATE          NOT NULL,
    DS_HASH_LINHA    CHAR(64)      NOT NULL,   -- SHA-256 dos campos de negócio (detecta mudança)
    ID_LOTE          VARCHAR(40)   NOT NULL,   -- último lote que alterou a linha
    DT_ATUALIZACAO   TIMESTAMP     NOT NULL DEFAULT CURRENT TIMESTAMP,
    CONSTRAINT PK_LIMITE PRIMARY KEY (ID_CLIENTE)
)@

-- Área de pouso do reverse ETL. Ninguém além da carga lê esta tabela.
CREATE TABLE CREDITO.STG_LIMITE_PRE_APROVADO (
    ID_LOTE          VARCHAR(40)   NOT NULL,
    ID_CLIENTE       INTEGER       NOT NULL,
    VL_LIMITE        DECIMAL(15,2) NOT NULL,
    DS_FAIXA_RISCO   VARCHAR(10)   NOT NULL,
    DT_VALIDADE      DATE          NOT NULL,
    DS_HASH_LINHA    CHAR(64)      NOT NULL,
    CONSTRAINT PK_STG_LIMITE PRIMARY KEY (ID_LOTE, ID_CLIENTE)
)@

-- Trilha de auditoria do lado on-prem: o time do sistema de crédito audita sem acessar o Databricks.
CREATE TABLE CREDITO.CTL_CARGA (
    ID_LOTE              VARCHAR(40)   NOT NULL,
    NR_VERSAO_ORIGEM     BIGINT,                  -- versão Delta da tabela Gold enviada
    QT_LINHAS_ESPERADA   INTEGER,
    VL_SOMA_ESPERADA     DECIMAL(31,2),
    QT_LINHAS_STAGING    INTEGER,
    VL_SOMA_STAGING      DECIMAL(31,2),
    QT_LINHAS_APLICADAS  INTEGER,
    ST_CARGA             VARCHAR(20)   NOT NULL,  -- VALIDANDO | APLICANDO | APLICADO | REJEITADO | ERRO
    DS_MENSAGEM          VARCHAR(500),
    DT_INICIO            TIMESTAMP     NOT NULL DEFAULT CURRENT TIMESTAMP,
    DT_FIM               TIMESTAMP,
    CONSTRAINT PK_CTL_CARGA PRIMARY KEY (ID_LOTE)
)@

CREATE OR REPLACE PROCEDURE CREDITO.SP_APLICAR_LIMITE (
    IN  P_ID_LOTE        VARCHAR(40),
    IN  P_VERSAO_ORIGEM  BIGINT,
    IN  P_QT_ESPERADA    INTEGER,
    IN  P_VL_ESPERADO    DECIMAL(31,2),
    IN  P_TAMANHO_LOTE   INTEGER,
    OUT P_ST_CARGA       VARCHAR(20),
    OUT P_QT_APLICADAS   INTEGER
)
LANGUAGE SQL
MODIFIES SQL DATA
BEGIN
    DECLARE V_QT_STG  INTEGER;
    DECLARE V_VL_STG  DECIMAL(31,2);
    DECLARE V_MIN_ID  INTEGER;
    DECLARE V_MAX_ID  INTEGER;
    DECLARE V_INICIO  INTEGER;
    DECLARE V_LINHAS  INTEGER DEFAULT 0;
    DECLARE V_MSG     VARCHAR(1000);

    -- Qualquer erro (inclusive timeout de lock) encerra a carga sem deixar transação aberta.
    -- Lotes já confirmados permanecem: a reexecução é segura porque o MERGE compara hash.
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS EXCEPTION 1 V_MSG = MESSAGE_TEXT;
        ROLLBACK;
        UPDATE CREDITO.CTL_CARGA
           SET ST_CARGA = 'ERRO', DS_MENSAGEM = SUBSTR(V_MSG, 1, 500),
               QT_LINHAS_APLICADAS = P_QT_APLICADAS, DT_FIM = CURRENT TIMESTAMP
         WHERE ID_LOTE = P_ID_LOTE;
        COMMIT;
        SET P_ST_CARGA = 'ERRO';
    END;

    -- Se a tabela de produção estiver bloqueada por mais de 5 s, desiste em vez de enfileirar.
    SET CURRENT LOCK TIMEOUT = 5;
    SET P_QT_APLICADAS = 0;

    -- Idempotência: lote já aplicado não é reaplicado.
    IF EXISTS (SELECT 1 FROM CREDITO.CTL_CARGA
                WHERE ID_LOTE = P_ID_LOTE AND ST_CARGA = 'APLICADO') THEN
        SET P_ST_CARGA = 'JA_APLICADO';
        RETURN;
    END IF;

    SELECT COUNT(*), COALESCE(SUM(VL_LIMITE), 0), MIN(ID_CLIENTE), MAX(ID_CLIENTE)
      INTO V_QT_STG, V_VL_STG, V_MIN_ID, V_MAX_ID
      FROM CREDITO.STG_LIMITE_PRE_APROVADO
     WHERE ID_LOTE = P_ID_LOTE;

    MERGE INTO CREDITO.CTL_CARGA C
    USING (VALUES (P_ID_LOTE)) AS N(ID_LOTE) ON C.ID_LOTE = N.ID_LOTE
    WHEN MATCHED THEN UPDATE SET
         NR_VERSAO_ORIGEM = P_VERSAO_ORIGEM, QT_LINHAS_ESPERADA = P_QT_ESPERADA,
         VL_SOMA_ESPERADA = P_VL_ESPERADO, QT_LINHAS_STAGING = V_QT_STG,
         VL_SOMA_STAGING = V_VL_STG, ST_CARGA = 'VALIDANDO', DS_MENSAGEM = NULL,
         DT_INICIO = CURRENT TIMESTAMP, DT_FIM = NULL
    WHEN NOT MATCHED THEN INSERT
         (ID_LOTE, NR_VERSAO_ORIGEM, QT_LINHAS_ESPERADA, VL_SOMA_ESPERADA,
          QT_LINHAS_STAGING, VL_SOMA_STAGING, ST_CARGA)
         VALUES (P_ID_LOTE, P_VERSAO_ORIGEM, P_QT_ESPERADA, P_VL_ESPERADO,
                 V_QT_STG, V_VL_STG, 'VALIDANDO');
    COMMIT;

    -- Validação: o que chegou na staging tem de bater com o que o Databricks diz ter enviado.
    IF V_QT_STG <> P_QT_ESPERADA OR V_VL_STG <> P_VL_ESPERADO THEN
        UPDATE CREDITO.CTL_CARGA
           SET ST_CARGA = 'REJEITADO',
               DS_MENSAGEM = 'Divergência na staging: esperado ' || P_QT_ESPERADA || ' linhas / '
                             || P_VL_ESPERADO || ', recebido ' || V_QT_STG || ' / ' || V_VL_STG
                             || '. Produção não foi alterada.',
               DT_FIM = CURRENT TIMESTAMP
         WHERE ID_LOTE = P_ID_LOTE;
        COMMIT;
        SET P_ST_CARGA = 'REJEITADO';
        RETURN;
    END IF;

    UPDATE CREDITO.CTL_CARGA SET ST_CARGA = 'APLICANDO' WHERE ID_LOTE = P_ID_LOTE;
    COMMIT;

    -- Aplicação em faixas de ID com commit por faixa: locks de linha curtos, sem escalar para tabela.
    SET V_INICIO = V_MIN_ID;
    WHILE V_INICIO <= V_MAX_ID DO
        MERGE INTO CREDITO.LIMITE_PRE_APROVADO T
        USING (SELECT ID_CLIENTE, VL_LIMITE, DS_FAIXA_RISCO, DT_VALIDADE, DS_HASH_LINHA
                 FROM CREDITO.STG_LIMITE_PRE_APROVADO
                WHERE ID_LOTE = P_ID_LOTE
                  AND ID_CLIENTE >= V_INICIO
                  AND ID_CLIENTE <  V_INICIO + P_TAMANHO_LOTE) S
           ON T.ID_CLIENTE = S.ID_CLIENTE
        WHEN MATCHED AND T.DS_HASH_LINHA <> S.DS_HASH_LINHA THEN UPDATE SET
             VL_LIMITE = S.VL_LIMITE, DS_FAIXA_RISCO = S.DS_FAIXA_RISCO,
             DT_VALIDADE = S.DT_VALIDADE, DS_HASH_LINHA = S.DS_HASH_LINHA,
             ID_LOTE = P_ID_LOTE, DT_ATUALIZACAO = CURRENT TIMESTAMP
        WHEN NOT MATCHED THEN INSERT
             (ID_CLIENTE, VL_LIMITE, DS_FAIXA_RISCO, DT_VALIDADE, DS_HASH_LINHA, ID_LOTE)
             VALUES (S.ID_CLIENTE, S.VL_LIMITE, S.DS_FAIXA_RISCO, S.DT_VALIDADE,
                     S.DS_HASH_LINHA, P_ID_LOTE);

        GET DIAGNOSTICS V_LINHAS = ROW_COUNT;
        SET P_QT_APLICADAS = P_QT_APLICADAS + V_LINHAS;
        COMMIT;
        SET V_INICIO = V_INICIO + P_TAMANHO_LOTE;
    END WHILE;

    UPDATE CREDITO.CTL_CARGA
       SET ST_CARGA = 'APLICADO', QT_LINHAS_APLICADAS = P_QT_APLICADAS, DT_FIM = CURRENT TIMESTAMP
     WHERE ID_LOTE = P_ID_LOTE;
    COMMIT;
    SET P_ST_CARGA = 'APLICADO';
END@
