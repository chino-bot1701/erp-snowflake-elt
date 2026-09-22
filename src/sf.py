# -*- coding: utf-8 -*-
"""
sf.py — Conexión a Snowflake + MERGE incremental por ROW_HASH
=============================================================
Proyecto: cargadirecta_snowflake (INMOGES -> Snowflake)

Hereda el patrón YA VALIDADO en producción
(`ProyectoALMENA/ingesta_entidades/ingesta_inmoges_entidades.py`, probado en
INMOGES_CONTRATO) y le agrega UNA corrección importante:

  ⚠ MANEJO DE COLUMNAS VARIANT
  `write_pandas` NO sabe escribir un VARIANT desde un string de pandas: guardaría
  el JSON como una CADENA dentro del VARIANT, y entonces `RAW_JSON:campo`
  devolvería NULL. Las tablas viejas no sufrían esto porque su RAW_JSON era texto.
  Las RAW_ERP_* SÍ son VARIANT (para poder navegar el JSON con SQL).

  Solución: la tabla STAGE se crea LIKE el destino y luego se le cambian las
  columnas VARIANT a VARCHAR. En el MERGE se envuelven con PARSE_JSON().
  Es automático: se detectan solas leyendo INFORMATION_SCHEMA.

El esquema destino está PARAMETRIZADO (variable ESQUEMA). El día que se migre a
SCH_PLD se cambia aquí y en ningún otro lado.
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass


import hashlib
import json
import os

import pandas as pd

# --------------------------------------------------------------------------- #
#  Configuración
#
#  El esquema destino esta PARAMETRIZADO. Migrar de esquema es cambiar
#  SCHEMA aqui y en ningun otro lado.
#
#  El esquema destino comparte espacio con otro pipeline en produccion,
#  por eso todas las tablas de este proyecto llevan el prefijo RAW_ERP_*:
#  evita cualquier choque de nombres.
# --------------------------------------------------------------------------- #
DATABASE = "DB_ANALYTICS"
SCHEMA   = "SCH_PLD"               # <- migrado el 2026-08-28 (antes SCH_CORE)
ESQUEMA  = f"{DATABASE}.{SCHEMA}"

# `` es el mismo account que ``
# (CURRENT_ACCOUNT() = ). Por eso el clone entre esquemas fue posible.
SF = dict(account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
          user=os.environ.get("SNOWFLAKE_USER", "SVC_ANALYTICS"),
          warehouse="WH_ANALYTICS", database=DATABASE,
          schema=SCHEMA, role="ROLE_DEV")

# Conexión ANTERIOR, por si hiciera falta volver al esquema viejo:
#   account="", user="SVC_ANALYTICS",
#   warehouse="WH_ANALYTICS", role="ROLE_ANALYTICS", schema="SCH_CORE"
#   llave: %LOCALAPPDATA%\erp_sf_key.pem
SF_ANTERIOR = dict(account="", user="SVC_ANALYTICS",
                   warehouse="WH_ANALYTICS", database=DATABASE,
                   schema="SCH_CORE", role="ROLE_ANALYTICS")

# Donde se busca la llave privada, en orden. La ruta se toma de
# SNOWFLAKE_PRIVATE_KEY_PATH; si no esta, se cae a una copia local.
# La llave NUNCA se versiona: ver .gitignore y .env.example.
_AQUI = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PEM_CANDIDATOS = [
    os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", ""),
    os.path.join(os.environ.get("LOCALAPPDATA", ""), "erp_sf_key.pem"),
]


def _pem() -> str:
    for p in _PEM_CANDIDATOS:
        if os.path.exists(p):
            return p
    raise FileNotFoundError("No se encontro la llave .pem: " + " | ".join(_PEM_CANDIDATOS))


def conectar():
    """Conexión key-pair. Mismo patrón que el pipeline PLD en producción."""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    with open(_pem(), "rb") as f:
        pkey = serialization.load_pem_private_key(f.read(), password=None)
    pkb = pkey.private_bytes(encoding=serialization.Encoding.DER,
                             format=serialization.PrivateFormat.PKCS8,
                             encryption_algorithm=serialization.NoEncryption())
    return snowflake.connector.connect(private_key=pkb, **SF)


# --------------------------------------------------------------------------- #
#  Huella de contenido — la señal de cambio del MERGE
# --------------------------------------------------------------------------- #
def row_hash(reg: dict) -> str:
    """MD5 del contenido de NEGOCIO, EXCLUYENDO createdDate/modifiedDate.

    Por qué se excluyen: INMOGES RE-SELLA modifiedDate EN LOTE (se midió: 4,676 de
    4,970 contratos con el mismo timestamp). Si el hash los incluyera, cada
    corrida dispararía miles de UPDATE espurios y la idempotencia se rompería.
    """
    if not isinstance(reg, dict):
        reg = {"_": reg}
    limpio = {k: v for k, v in reg.items() if k not in ("createdDate", "modifiedDate")}
    payload = json.dumps(limpio, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
#  Metadatos del destino
# --------------------------------------------------------------------------- #
def columnas_de(conn, tabla: str) -> list[tuple[str, str]]:
    """[(COLUMN_NAME, DATA_TYPE), ...] en orden. Base del MERGE dinámico."""
    cur = conn.cursor()
    cur.execute(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        (SCHEMA, tabla))
    out = [(c[0], c[1]) for c in cur.fetchall()]
    cur.close()
    if not out:
        raise RuntimeError(f"La tabla {ESQUEMA}.{tabla} no existe (o no hay permisos).")
    return out


# --------------------------------------------------------------------------- #
#  MERGE incremental por ROW_HASH  (el corazón)
# --------------------------------------------------------------------------- #
def merge_upsert(conn, tabla: str, filas: list[dict], llaves: list[str],
                 soft_delete: bool = False, verbose: bool = True) -> tuple[int, int]:
    """Sincroniza `filas` contra `tabla` comparando ROW_HASH.

        INSERT  los que no existen
        UPDATE  los que existen y cambiaron de hash
        NO TOCA los iguales   <- esto es lo que hace la corrida idempotente

    soft_delete=True marca ACTIVO=FALSE los que ya no vinieron. SOLO usarlo cuando
    el pull fue COMPLETO (un catálogo entero), nunca en una ventana de fechas —
    si no, marcaría de baja todo lo que está fuera de la ventana.

    Devuelve (insertados, actualizados).
    """
    if not filas:
        if verbose:
            print(f"    {tabla}: 0 filas en el pull; no se hace MERGE")
        return (0, 0)

    from snowflake.connector.pandas_tools import write_pandas

    cols_tipos = columnas_de(conn, tabla)
    cols = [c for c, _ in cols_tipos]
    variantes = [c for c, t in cols_tipos if t == "VARIANT"]

    tipos = dict(cols_tipos)
    #  Columnas de fecha: se reciben como TEXTO en la stage y se castean con
    #  TRY_TO_* en el MERGE.
    #  ⚠ POR QUÉ (medido 2026-08-12): INMOGES devuelve "0000-00-00" como fecha
    #  vacía (nulo estilo MySQL). Un cast directo lanza
    #  «Failed to cast variant value "0000-00-00" to DATE» y MATA la carga del
    #  mes completo. Con TRY_TO_* ese valor queda NULL, el resto de la ventana
    #  entra bien, y el literal original sigue intacto dentro de RAW_JSON.
    #  Regla: un dato sucio degrada UNA celda, nunca tumba un job.
    fechas = [c for c, t in cols_tipos
              if t in ("DATE", "TIME") or str(t).startswith("TIMESTAMP")]
    como_texto = list(variantes) + fechas

    cur = conn.cursor()
    stg = f"{tabla}_STG"

    # 1) STAGE con la misma forma que el destino...
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {ESQUEMA}.{stg} LIKE {ESQUEMA}.{tabla}")
    # ...pero con VARIANT y FECHAS como VARCHAR (write_pandas manda strings).
    for c in como_texto:
        cur.execute(f"ALTER TABLE {ESQUEMA}.{stg} DROP COLUMN {c}")
        cur.execute(f"ALTER TABLE {ESQUEMA}.{stg} ADD COLUMN {c} VARCHAR")

    # 2) Cargar. El DataFrame se alinea a las columnas reales del destino.
    df = pd.DataFrame(filas)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    sobra = [c for c in df.columns if c not in cols]
    if sobra:
        raise RuntimeError(f"{tabla}: el mapeo produjo columnas inexistentes: {sobra}")
    df = df[cols]
    write_pandas(conn, df, stg, database=DATABASE, schema=SCHEMA,
                 quote_identifiers=False)

    # 3) MERGE dinámico.
    #    PARSE_JSON reconstruye el VARIANT; TRY_TO_* castea fechas sin tumbar
    #    la carga si el valor viene inválido (ver nota arriba).
    def src(c):
        #  ⚠ BUG 28 (medido 2026-09-08): LOAD_TS estaba VACIO en las 30M de filas,
        #    en TODAS las tablas. La DDL lo declara
        #        LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        #    pero el MERGE lista TODAS las columnas e inserta el LOAD_TS que trae
        #    la stage, que viene en NULL. **Un INSERT explicito de NULL le gana al
        #    DEFAULT**: Snowflake solo aplica el DEFAULT si la columna se omite.
        #    Consecuencia: nos quedamos sin saber cuando cambio cada fila.
        #    Se sella aqui, tanto al insertar como al actualizar, asi LOAD_TS
        #    significa "la ultima vez que ESTA fila cambio".
        if c == "LOAD_TS":
            return "CURRENT_TIMESTAMP()"
        if c in variantes:
            return f"PARSE_JSON(S.{c})"
        t = str(tipos.get(c, ""))
        if t == "DATE":
            return f"TRY_TO_DATE(S.{c})"
        if t.startswith("TIMESTAMP"):
            return f"TRY_TO_TIMESTAMP_NTZ(S.{c})"
        if t == "TIME":
            return f"TRY_TO_TIME(S.{c})"
        return f"S.{c}"

    on    = " AND ".join(f"T.{k}=S.{k}" for k in llaves)
    part  = ", ".join(llaves)
    setc  = ", ".join(f"T.{c}={src(c)}" for c in cols if c not in llaves)
    ins_c = ", ".join(cols)
    ins_v = ", ".join(src(c) for c in cols)

    cur.execute(f"""
        MERGE INTO {ESQUEMA}.{tabla} T
        USING (
            SELECT * FROM {ESQUEMA}.{stg}
            -- Si una llave viene repetida DENTRO del mismo lote hay que quedarse
            -- con una sola, o Snowflake aborta con "nondeterministic merge".
            -- ⚠ Antes ordenaba por LOAD_TS DESC, que estaba SIEMPRE EN NULL
            --   (bug 28): el desempate era arbitrario y NO reproducible.
            --   ROW_HASH no dice cual es "la mas nueva" —eso no se puede saber
            --   dentro de un lote— pero al menos hace que la misma entrada de
            --   siempre el mismo resultado. Las repetidas vienen del mismo pull,
            --   asi que su contenido es normalmente identico.
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY ROW_HASH) = 1
        ) S
        ON {on}
        WHEN MATCHED AND (T.ROW_HASH IS DISTINCT FROM S.ROW_HASH) THEN UPDATE SET {setc}
        WHEN NOT MATCHED THEN INSERT ({ins_c}) VALUES ({ins_v})
    """)
    res = cur.fetchone() or (0, 0)
    insertados, actualizados = int(res[0] or 0), int(res[1] or 0)

    # 4) Soft-delete: lo que ya no vino en un pull COMPLETO se marca inactivo.
    bajas = 0
    if soft_delete:
        cond = " AND ".join(f"T.{k}=S.{k}" for k in llaves)
        cur.execute(f"""
            UPDATE {ESQUEMA}.{tabla} T
               SET ACTIVO = FALSE, FECHA_BAJA = CURRENT_TIMESTAMP()
             WHERE T.ACTIVO = TRUE
               AND NOT EXISTS (SELECT 1 FROM {ESQUEMA}.{stg} S WHERE {cond})
        """)
        bajas = int((cur.fetchone() or [0])[0] or 0)

    cur.close()
    if verbose:
        extra = f" · bajas {bajas}" if soft_delete else ""
        print(f"    {tabla}: +{insertados} nuevos · ~{actualizados} cambiados{extra} "
              f"(de {len(filas)} en el pull)")
    return (insertados, actualizados)


# --------------------------------------------------------------------------- #
#  Bitácora de jobs (ERP_SYNC_CONTROL)
# --------------------------------------------------------------------------- #
def job_registrar(conn, entidad, empresa=None, ini=None, fin=None,
                  granularidad="FULL", corrida_id=None):
    """Da de alta un job en PENDIENTE (idempotente: no duplica si ya existe)."""
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {ESQUEMA}.ERP_SYNC_CONTROL
               (ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN, GRANULARIDAD, ESTADO, CORRIDA_ID)
        SELECT %s, %s, %s, %s, %s, 'PENDIENTE', %s
        WHERE NOT EXISTS (
            SELECT 1 FROM {ESQUEMA}.ERP_SYNC_CONTROL
            WHERE ENTIDAD = %s
              AND EQUAL_NULL(EMPRESA, %s)
              AND EQUAL_NULL(VENTANA_INI, TO_DATE(%s))
              AND EQUAL_NULL(VENTANA_FIN, TO_DATE(%s))
              AND ESTADO IN ('COMPLETADO', 'VACIO', 'PENDIENTE', 'EN_CURSO')
        )
    """, (entidad, empresa, ini, fin, granularidad, corrida_id,
          entidad, empresa, ini, fin))
    cur.close()


def job_cerrar(conn, entidad, empresa, ini, fin, estado, filas=0, nuevas=0,
               cambiadas=0, paginas=0, mensaje=None, seg=None,
               ram_pico=None, ram_delta=None):
    """Cierra el job con su resultado (COMPLETADO | VACIO | ERROR).

    Guarda también el consumo de RAM: con el historial se puede responder
    "¿qué job fue el más pesado?" y calibrar LOTE_FILAS con datos, no a ojo.
    """
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE {ESQUEMA}.ERP_SYNC_CONTROL
           SET ESTADO=%s, FILAS=%s, FILAS_NUEVAS=%s, FILAS_CAMBIADAS=%s,
               PAGINAS=%s, MENSAJE=%s, TS_FIN=CURRENT_TIMESTAMP(),
               DURACION_SEG=%s, INTENTOS=COALESCE(INTENTOS,0)+1,
               RAM_PICO_MB=%s, RAM_DELTA_MB=%s
         WHERE ENTIDAD=%s AND EQUAL_NULL(EMPRESA,%s)
           AND EQUAL_NULL(VENTANA_INI, TO_DATE(%s)) AND EQUAL_NULL(VENTANA_FIN, TO_DATE(%s))
    """, (estado, filas, nuevas, cambiadas, paginas,
          (mensaje or "")[:3900], seg, ram_pico, ram_delta,
          entidad, empresa, ini, fin))
    cur.close()
