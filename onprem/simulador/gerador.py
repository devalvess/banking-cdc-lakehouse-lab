"""Simulador do banco on-premises: gera movimento contínuo no Db2 e o arquivo CSV noturno.

Comandos:
    python gerador.py run          # carga inicial (se vazio) + movimento contínuo
    python gerador.py csv [data]   # gera saldos_diarios_AAAAMMDD.csv em DIR_BATCH
    python gerador.py rajada N     # N operações seguidas, sem pausa (demo de acúmulo de log)
    python gerador.py rotina-antiga N  # N pagamentos que NÃO atualizam DT_ATUALIZACAO (demo de reconciliação)

Todos os dados são sintéticos (Faker pt_BR).
"""

import csv
import os
import random
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import ibm_db
from faker import Faker

fake = Faker("pt_BR")

AGENCIAS = [
    (1, "Paulista", "São Paulo", "SP", "SUDESTE"),
    (2, "Faria Lima", "São Paulo", "SP", "SUDESTE"),
    (3, "Campinas Centro", "Campinas", "SP", "SUDESTE"),
    (4, "Copacabana", "Rio de Janeiro", "RJ", "SUDESTE"),
    (5, "Savassi", "Belo Horizonte", "MG", "SUDESTE"),
    (6, "Vitória Praia", "Vitória", "ES", "SUDESTE"),
    (7, "Batel", "Curitiba", "PR", "SUL"),
    (8, "Moinhos", "Porto Alegre", "RS", "SUL"),
    (9, "Centro Floripa", "Florianópolis", "SC", "SUL"),
    (10, "Boa Viagem", "Recife", "PE", "NORDESTE"),
    (11, "Pituba", "Salvador", "BA", "NORDESTE"),
    (12, "Aldeota", "Fortaleza", "CE", "NORDESTE"),
    (13, "Asa Sul", "Brasília", "DF", "CENTRO-OESTE"),
    (14, "Setor Bueno", "Goiânia", "GO", "CENTRO-OESTE"),
    (15, "Adrianópolis", "Manaus", "AM", "NORTE"),
    (16, "Umarizal", "Belém", "PA", "NORTE"),
]
PRODUTOS = ["CONSIGNADO", "PESSOAL", "VEICULO", "CARTAO_ROTATIVO"]


def conectar():
    conn_str = (
        f"DATABASE={os.environ['DB2_DATABASE']};HOSTNAME={os.environ['DB2_HOST']};"
        f"PORT={os.environ['DB2_PORT']};PROTOCOL=TCPIP;"
        f"UID={os.environ['DB2_USER']};PWD={os.environ['DB2_PASSWORD']};"
    )
    while True:
        try:
            conn = ibm_db.connect(conn_str, "", "")
            ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
            return conn
        except Exception as exc:  # Db2 leva alguns minutos para subir na primeira vez
            print(f"aguardando Db2... ({str(exc)[:80]})", flush=True)
            time.sleep(15)


def executar(conn, sql, params=()):
    stmt = ibm_db.prepare(conn, sql)
    # Decimal vai como texto: preserva os centavos exatos (nada de float com dinheiro).
    ibm_db.execute(stmt, tuple(str(p) if isinstance(p, Decimal) else p for p in params))
    return stmt


def um_valor(conn, sql, params=()):
    row = ibm_db.fetch_tuple(executar(conn, sql, params))
    return row[0] if row else None


def todos(conn, sql, params=()):
    stmt = executar(conn, sql, params)
    rows, row = [], ibm_db.fetch_tuple(stmt)
    while row:
        rows.append(row)
        row = ibm_db.fetch_tuple(stmt)
    return rows


def dinheiro(minimo, maximo):
    return Decimal(str(round(random.uniform(minimo, maximo), 2)))


# --- Core (com CDC) ---------------------------------------------------------------------------

def novo_cliente(conn):
    agencia = random.choice(AGENCIAS)
    id_cliente = um_valor(
        conn,
        """SELECT ID_CLIENTE FROM FINAL TABLE (
             INSERT INTO CORE.CLIENTES (NR_CPF, NM_CLIENTE, DS_EMAIL, NR_TELEFONE, DT_NASCIMENTO,
                                        NM_CIDADE, SG_UF, VL_RENDA_MENSAL, CD_AGENCIA)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?))""",
        (
            fake.cpf().replace(".", "").replace("-", ""),
            fake.name(),
            fake.email(),
            fake.cellphone_number(),
            fake.date_of_birth(minimum_age=18, maximum_age=80),
            agencia[2],
            agencia[3],
            dinheiro(1500, 35000),
            agencia[0],
        ),
    )
    for tipo in random.sample(["CORRENTE", "POUPANCA"], k=random.choice([1, 1, 2])):
        executar(
            conn,
            "INSERT INTO CORE.CONTAS (ID_CLIENTE, CD_AGENCIA, TP_CONTA, VL_SALDO) VALUES (?, ?, ?, ?)",
            (id_cliente, agencia[0], tipo, dinheiro(0, 20000)),
        )
    return id_cliente


def nova_transacao(conn):
    id_conta = um_valor(
        conn,
        "SELECT ID_CONTA FROM CORE.CONTAS WHERE ST_CONTA = 'ATIVA' ORDER BY RAND() FETCH FIRST 1 ROW ONLY",
    )
    if id_conta is None:
        return
    tipo = random.choices(
        ["PIX_ENVIADO", "PIX_RECEBIDO", "DEBITO", "SALARIO", "TARIFA"], weights=[35, 30, 25, 5, 5]
    )[0]
    valor = {
        "PIX_ENVIADO": dinheiro(10, 3000),
        "PIX_RECEBIDO": dinheiro(10, 3000),
        "DEBITO": dinheiro(5, 800),
        "SALARIO": dinheiro(1500, 15000),
        "TARIFA": dinheiro(5, 60),
    }[tipo]
    sinal = 1 if tipo in ("PIX_RECEBIDO", "SALARIO") else -1
    # Transação e saldo no mesmo commit: o CDC entrega os dois eventos com o mesmo commit LSN.
    executar(
        conn,
        "INSERT INTO CORE.TRANSACOES (ID_CONTA, TP_TRANSACAO, VL_TRANSACAO) VALUES (?, ?, ?)",
        (id_conta, tipo, valor),
    )
    executar(
        conn,
        "UPDATE CORE.CONTAS SET VL_SALDO = VL_SALDO + ?, DT_ATUALIZACAO = CURRENT TIMESTAMP WHERE ID_CONTA = ?",
        (valor * sinal, id_conta),
    )


def atualizar_cliente(conn):
    """Mudança cadastral: gera histórico SCD tipo 2 na Silver."""
    id_cliente = um_valor(conn, "SELECT ID_CLIENTE FROM CORE.CLIENTES ORDER BY RAND() FETCH FIRST 1 ROW ONLY")
    if id_cliente is None:
        return
    if random.random() < 0.5:
        executar(
            conn,
            "UPDATE CORE.CLIENTES SET VL_RENDA_MENSAL = ?, DT_ATUALIZACAO = CURRENT TIMESTAMP WHERE ID_CLIENTE = ?",
            (dinheiro(1500, 35000), id_cliente),
        )
    else:
        executar(
            conn,
            "UPDATE CORE.CLIENTES SET DS_EMAIL = ?, NR_TELEFONE = ?, DT_ATUALIZACAO = CURRENT TIMESTAMP WHERE ID_CLIENTE = ?",
            (fake.email(), fake.cellphone_number(), id_cliente),
        )


def encerrar_cliente(conn):
    """Delete físico no core: só o CDC por log enxerga."""
    id_cliente = um_valor(conn, "SELECT ID_CLIENTE FROM CORE.CLIENTES ORDER BY RAND() FETCH FIRST 1 ROW ONLY")
    if id_cliente is None:
        return
    executar(conn, "DELETE FROM CORE.CONTAS WHERE ID_CLIENTE = ?", (id_cliente,))
    executar(conn, "DELETE FROM CORE.CLIENTES WHERE ID_CLIENTE = ?", (id_cliente,))


# --- Legado (sem CDC) -------------------------------------------------------------------------

def novo_contrato(conn, id_cliente=None):
    if id_cliente is None:
        id_cliente = um_valor(conn, "SELECT ID_CLIENTE FROM CORE.CLIENTES ORDER BY RAND() FETCH FIRST 1 ROW ONLY")
    valor = dinheiro(2000, 80000)
    executar(
        conn,
        """INSERT INTO LEGADO.CONTRATOS_CREDITO
             (ID_CLIENTE, TP_PRODUTO, VL_CONTRATO, VL_SALDO_DEVEDOR, QT_PARCELAS, DT_CONTRATACAO)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (id_cliente, random.choice(PRODUTOS), valor, valor, random.choice([12, 24, 36, 48, 60]),
         fake.date_between(start_date="-3y", end_date="today")),
    )


def pagar_parcela(conn, atualiza_marca_dagua=True):
    """Pagamento de parcela. Com atualiza_marca_dagua=False simula a rotina batch antiga que
    altera o contrato SEM mexer em DT_ATUALIZACAO — a carga incremental não vê; a reconciliação vê."""
    row = todos(
        conn,
        """SELECT ID_CONTRATO, VL_SALDO_DEVEDOR, QT_PARCELAS, QT_PARCELAS_PAGAS FROM LEGADO.CONTRATOS_CREDITO
           WHERE QT_PARCELAS_PAGAS < QT_PARCELAS ORDER BY RAND() FETCH FIRST 1 ROW ONLY""",
    )
    if not row:
        return
    id_contrato, saldo, qt_parcelas, qt_pagas = row[0]
    parcela = (Decimal(saldo) / max(qt_parcelas - qt_pagas, 1)).quantize(Decimal("0.01"))
    atraso = random.choices([0, 0, 0, 15, 45, 95], k=1)[0]
    marca = ", DT_ATUALIZACAO = CURRENT TIMESTAMP" if atualiza_marca_dagua else ""
    executar(
        conn,
        f"""UPDATE LEGADO.CONTRATOS_CREDITO
              SET VL_SALDO_DEVEDOR = VL_SALDO_DEVEDOR - ?, QT_PARCELAS_PAGAS = QT_PARCELAS_PAGAS + 1,
                  NR_DIAS_ATRASO = ? {marca}
            WHERE ID_CONTRATO = ?""",
        (parcela, atraso, id_contrato),
    )


def excluir_contrato(conn):
    """Delete físico no legado: invisível para a carga incremental por marca d'água."""
    executar(
        conn,
        """DELETE FROM LEGADO.CONTRATOS_CREDITO WHERE ID_CONTRATO =
             (SELECT ID_CONTRATO FROM LEGADO.CONTRATOS_CREDITO ORDER BY RAND() FETCH FIRST 1 ROW ONLY)""",
    )


# --- Carga inicial e loop ---------------------------------------------------------------------

def aguardar_estrutura(conn):
    """Na primeira subida, o Db2 aceita conexões antes do setup terminar. Espera até o CDC das
    três tabelas do core estar ativo, para nenhuma mudança escapar da captura."""
    sql = "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = 'ASNCDC' AND TABNAME = 'IBMSNAP_REGISTER'"
    ativos = "SELECT COUNT(*) FROM ASNCDC.IBMSNAP_REGISTER WHERE STATE = 'A'"
    while um_valor(conn, sql) == 0 or um_valor(conn, ativos) < 3:
        print("aguardando setup do banco...", flush=True)
        time.sleep(15)


def carga_inicial(conn, qt_clientes=500):
    aguardar_estrutura(conn)
    if um_valor(conn, "SELECT COUNT(*) FROM CORE.CLIENTES") > 0:
        return
    print("carga inicial...", flush=True)
    for ag in AGENCIAS:
        executar(
            conn,
            "INSERT INTO LEGADO.AGENCIAS (CD_AGENCIA, NM_AGENCIA, NM_CIDADE, SG_UF, DS_REGIAO) VALUES (?, ?, ?, ?, ?)",
            ag,
        )
    for i in range(qt_clientes):
        id_cliente = novo_cliente(conn)
        if random.random() < 0.6:
            novo_contrato(conn, id_cliente)
        if i % 50 == 0:
            ibm_db.commit(conn)
    for _ in range(qt_clientes * 4):
        nova_transacao(conn)
    ibm_db.commit(conn)
    print("carga inicial concluída", flush=True)


OPERACOES = [
    (nova_transacao, 70),
    (atualizar_cliente, 8),
    (novo_cliente, 5),
    (pagar_parcela, 10),
    (novo_contrato, 3),
    (encerrar_cliente, 1),
    (excluir_contrato, 1),
]


def uma_operacao(conn):
    funcao = random.choices([o[0] for o in OPERACOES], weights=[o[1] for o in OPERACOES])[0]
    try:
        funcao(conn)
        ibm_db.commit(conn)
    except Exception as exc:
        ibm_db.rollback(conn)
        print(f"erro em {getattr(funcao, '__name__', 'operacao')}: {exc}", flush=True)


def run():
    conn = conectar()
    carga_inicial(conn)
    intervalo = float(os.environ.get("INTERVALO_SEG", "2"))
    dia_csv = None
    n = 0
    while True:
        uma_operacao(conn)
        n += 1
        if n % 100 == 0:
            print(f"{datetime.now():%H:%M:%S} {n} operações", flush=True)
        # Uma vez por dia, gera o arquivo do batch noturno (referência: ontem).
        if dia_csv != date.today():
            gerar_csv(conn)
            dia_csv = date.today()
        time.sleep(intervalo)


def rajada(qt):
    conn = conectar()
    for _ in range(qt):
        uma_operacao(conn)
    print(f"rajada de {qt} operações concluída", flush=True)


def gerar_csv(conn, dt_referencia=None):
    """Arquivo de saldos de fechamento, como o extrato que o mainframe gera à noite."""
    dt_referencia = dt_referencia or date.today()
    destino = Path(os.environ.get("DIR_BATCH", "/dados/batch"))
    destino.mkdir(parents=True, exist_ok=True)
    arquivo = destino / f"saldos_diarios_{dt_referencia:%Y%m%d}.csv"
    linhas = todos(conn, "SELECT ID_CONTA, CD_AGENCIA, TP_CONTA, VL_SALDO FROM CORE.CONTAS ORDER BY ID_CONTA")
    temporario = arquivo.with_suffix(".tmp")
    with temporario.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["DT_REFERENCIA", "ID_CONTA", "CD_AGENCIA", "TP_CONTA", "VL_SALDO_FECHAMENTO"])
        for id_conta, agencia, tipo, saldo in linhas:
            w.writerow([f"{dt_referencia:%Y-%m-%d}", id_conta, agencia, tipo, saldo])
    temporario.rename(arquivo)  # publica o arquivo só quando completo
    print(f"arquivo gerado: {arquivo} ({len(linhas)} linhas)", flush=True)


if __name__ == "__main__":
    comando = sys.argv[1] if len(sys.argv) > 1 else "run"
    if comando == "run":
        run()
    elif comando == "csv":
        dt = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else None
        gerar_csv(conectar(), dt)
    elif comando == "rajada":
        rajada(int(sys.argv[2]) if len(sys.argv) > 2 else 1000)
    elif comando == "rotina-antiga":
        conn = conectar()
        for _ in range(int(sys.argv[2]) if len(sys.argv) > 2 else 5):
            pagar_parcela(conn, atualiza_marca_dagua=False)
        ibm_db.commit(conn)
        print("rotina antiga executada: contratos alterados sem atualizar DT_ATUALIZACAO", flush=True)
    else:
        sys.exit(__doc__)
