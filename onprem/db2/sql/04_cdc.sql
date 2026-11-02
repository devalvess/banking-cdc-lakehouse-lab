-- Coloca APENAS as tabelas do core em CDC (DATA CAPTURE CHANGES + registro no ASN Capture).
-- Capturar só o necessário é a primeira defesa contra o acúmulo de log na origem.

CALL ASNCDC.ADDTABLE('CORE', 'CLIENTES')@
CALL ASNCDC.ADDTABLE('CORE', 'CONTAS')@
CALL ASNCDC.ADDTABLE('CORE', 'TRANSACOES')@
