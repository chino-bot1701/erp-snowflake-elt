# -*- coding: utf-8 -*-
r"""
sync.py — SYNC INCREMENTAL diario INMOGES -> Snowflake
====================================================
Proyecto: cargadirecta_snowflake                          (paso 2 del roadmap)

PARA QUÉ
--------
El backfill pregunta por FECHA DEL DOCUMENTO, y eso obliga a barrer ~200 meses ×
50 empresas = 10,550 preguntas (~7 h). Para mantener la data al día eso es
absurdo. Este módulo pregunta por FECHA DE MODIFICACIÓN:

    "dame lo que cambió desde la última vez"   -> UNA ventana, toda la historia

    carga completa    ~7 h · 10,550 preguntas
    sync incremental   minutos · decenas de preguntas

NO DUPLICA, NUNCA
-----------------
No hace INSERT: hace MERGE comparando `ROW_HASH`.
    llave nueva                        -> inserta
    llave existente, huella igual      -> no hace nada
    llave existente, huella distinta   -> ACTUALIZA la fila (no crea otra)
Por eso es seguro correrlo mil veces. Verificado sobre 30M de filas:
`COUNT(*) = COUNT(DISTINCT llave)` en todas las tablas grandes.

⚠⚠ LAS DOS TRAMPAS QUE SE MIDIERON (2026-08-28) — NO REPETIRLAS
---------------------------------------------------------------
1. **`fecha_fin_modificacion` es EXCLUSIVO** ("hasta ese día a las 00:00").
   Medido en EDN:
       [2026-08-01 .. 2026-08-28] -> 665 facturas · NO trae la modificada 08-28 13:58
       [2026-08-01 .. 2026-08-29] -> 907 facturas · SÍ la trae   (+242)
   Un sync con `fin = hoy` habría perdido TODO lo de hoy, todos los días, en
   silencio. Por eso aquí `hasta` SIEMPRE es **mañana**.

2. **El troceo por días del backfill NO sirve con este filtro.** `iter_rango`
   parte la ventana en días y genera `[D, D]`, que con fin exclusivo devuelve
   CERO siempre: la primera versión daba 0 en vez de 907. Por eso el sync lee la
   ventana COMPLETA con `iter_ventana_segura` (que igual protege contra la
   paginación inestable). Se activa pasando `campo_fechas=` a `extraer_hecho`.

QUÉ HACE CON CADA ENTIDAD
-------------------------
    cfdi, pago                         filtro de MODIFICACIÓN, por empresa
    movimiento_bancario, gasto,        la API NO expone fecha de modificación
      pago_gasto                       -> barrido corto por fecha de documento
    catálogos (empresa, unidad,        pull completo: son chicos (~13k filas
      contrato, arrendatario, ...)        entre todos) y así no hay que confiar
                                          en filtros que no podemos verificar

SOLAPE A PROPÓSITO
------------------
La ventana arranca en (última corrida OK − MARGEN_DIAS). Repetir días no cuesta
nada (el MERGE es idempotente) y cubre el hueco si una corrida se cae a medias o
si INMOGES registra la modificación con retraso. **Es más barato repetir que perder.**

USO
---
    py src\sync.py                      # lo normal: desde la última corrida
    py src\sync.py --dry-run            # no escribe nada, solo dice qué traería
    py src\sync.py --desde 2026-08-01   # forzar una ventana
    py src\sync.py --entidades cfdi,pago
    py src\sync.py --solo-hechos        # saltarse los catálogos
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

import argparse
import time
import traceback
from datetime import date, datetime, timedelta

import erp_client as bc
import extraer
import sf
from entidades import ENTIDADES

# --------------------------------------------------------------------------- #
#  Parámetros
# --------------------------------------------------------------------------- #
MODIF = ("fecha_inicio_modificacion", "fecha_fin_modificacion")

#  Cuántos días hacia atrás se repiten respecto de la última corrida OK.
#  Repetir es gratis (MERGE idempotente); perder, no.
MARGEN_DIAS = 2

#  Si NUNCA se ha corrido el sync, desde cuándo arrancar.
PRIMERA_VEZ_DIAS = 7

#  Entidades sin `fecha_*_modificacion` en la API: se barre por fecha de
#  documento una ventana corta. Un movimiento bancario viejo casi nunca cambia.
VENTANA_SIN_DELTA = 60

TABLA_CTL = "ERP_SYNC_INCREMENTAL"


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def banner(t: str) -> None:
    print("\n" + "=" * 78 + f"\n  {t}\n" + "=" * 78, flush=True)


# --------------------------------------------------------------------------- #
#  Tabla de control
# --------------------------------------------------------------------------- #
def asegurar_control(conn) -> None:
    conn.cursor().execute(f"""
        CREATE TABLE IF NOT EXISTS {sf.ESQUEMA}.{TABLA_CTL} (
            CORRIDA_ID   VARCHAR(32),
            ENTIDAD      VARCHAR(60),
            EMPRESA      VARCHAR(60),
            MODO         VARCHAR(20),     -- MODIFICACION | DOCUMENTO | COMPLETO
            VENTANA_INI  DATE,
            VENTANA_FIN  DATE,            -- ⚠ EXCLUSIVA en modo MODIFICACION
            INICIO       TIMESTAMP_NTZ,
            FIN          TIMESTAMP_NTZ,
            FILAS_API    NUMBER,
            INSERTADAS   NUMBER,
            ACTUALIZADAS NUMBER,
            ESTADO       VARCHAR(15),     -- OK | ERROR
            MENSAJE      VARCHAR(2000)
        ) COMMENT = 'Bitacora del sync incremental (sync.py). Una fila por entidad/empresa.'
    """)


def ultima_corrida_ok(conn, entidad: str) -> date | None:
    """La ventana más reciente que cerró OK para esta entidad.

    Se toma el MIN de los FIN de la última corrida completa, no el MAX: si una
    empresa se quedó atrás, la próxima ventana debe cubrirla también.
    """
    cur = conn.cursor()
    cur.execute(f"""
        SELECT MIN(VENTANA_FIN) FROM {sf.ESQUEMA}.{TABLA_CTL}
        WHERE ENTIDAD = %s AND ESTADO = 'OK'
          AND CORRIDA_ID = (SELECT MAX(CORRIDA_ID) FROM {sf.ESQUEMA}.{TABLA_CTL}
                            WHERE ENTIDAD = %s AND ESTADO = 'OK')
    """, (entidad, entidad))
    r = cur.fetchone()
    return r[0] if r and r[0] else None


def registrar(conn, corrida: str, entidad: str, empresa: str, modo: str,
              ini: str | None, fin: str | None, t0: float, filas: int,
              ins: int, upd: int, estado: str, mensaje: str = "") -> None:
    """Escribe una fila en la bitácora.

    ⚠ DOS COSAS APRENDIDAS A GOLPES AQUÍ MISMO (2026-08-31):

    1. `TRY_TO_DATE(%s)` con un None de Python revienta con
       «Function TRY_CAST cannot be used with arguments of types NULL and DATE»:
       Snowflake no sabe de qué tipo es ese NULL. Los catálogos no tienen ventana,
       así que siempre mandan None. Se arregla casteando a VARCHAR primero.

    2. **Esta función NUNCA debe lanzar.** Se la llama desde el `except` de
       `sync_catalogo`; si falla ahí, tumba TODA la corrida mientras intentaba
       anotar un error menor. Fue exactamente lo que pasó: un catálogo falló,
       el registro del fallo reventó, y los 11 catálogos siguientes no se
       corrieron. Un registrador de errores que rompe la corrida es peor que no
       tener registrador.
    """
    try:
        conn.cursor().execute(f"""
            INSERT INTO {sf.ESQUEMA}.{TABLA_CTL}
                (CORRIDA_ID, ENTIDAD, EMPRESA, MODO, VENTANA_INI, VENTANA_FIN,
                 INICIO, FIN, FILAS_API, INSERTADAS, ACTUALIZADAS, ESTADO, MENSAJE)
            SELECT %s, %s, %s, %s,
                   TRY_TO_DATE(%s::VARCHAR), TRY_TO_DATE(%s::VARCHAR),
                   TO_TIMESTAMP_NTZ(%s::VARCHAR), CURRENT_TIMESTAMP(),
                   %s, %s, %s, %s, %s
        """, (corrida, entidad, empresa, modo, ini, fin,
              datetime.fromtimestamp(t0).strftime("%Y-%m-%d %H:%M:%S"),
              filas, ins, upd, estado, (mensaje or "")[:2000]))
    except Exception as e:
        # se avisa y se sigue: la bitácora es importante, pero no tanto como
        # que la corrida termine.
        log(f"  (no se pudo escribir en la bitacora: {type(e).__name__}: "
            f"{str(e)[:90]})")


# --------------------------------------------------------------------------- #
#  Piezas del sync
# --------------------------------------------------------------------------- #
def empresas(conn) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"SELECT ALIAS FROM {sf.ESQUEMA}.RAW_ERP_EMPRESA "
                f"WHERE ALIAS IS NOT NULL ORDER BY ALIAS")
    return [r[0] for r in cur.fetchall()]


def sync_hecho(cli, conn, corrida, nombre, ent, emps, desde, hasta,
               dry_run) -> tuple[int, int, int, int]:
    """Un hecho, empresa por empresa.

    Si la entidad declara `campo_delta` se usa el filtro de MODIFICACIÓN y
    `hasta` es EXCLUSIVO (por eso viene ya con el +1 día). Si no, se barre por
    fecha de documento, que es inclusiva en los dos extremos.
    """
    tiene_delta = bool(ent.get("campo_delta"))
    modo = "MODIFICACION" if tiene_delta else "DOCUMENTO"
    campos = MODIF if tiene_delta else None
    if not tiene_delta:
        # sin filtro de modificación: ventana corta por fecha de documento,
        # y el extremo es INCLUSIVO -> se resta el día que se había sumado.
        hasta = (date.fromisoformat(hasta) - timedelta(days=1)).isoformat()
        desde = (date.fromisoformat(hasta)
                 - timedelta(days=VENTANA_SIN_DELTA)).isoformat()

    banner(f"{nombre}  ·  modo {modo}  ·  {desde} .. {hasta}"
           + ("   (fin EXCLUSIVO)" if tiene_delta else ""))
    tot = ins_t = upd_t = 0
    errores = 0
    for i, emp in enumerate(emps, 1):
        t0 = time.time()
        try:
            n, ins, upd = extraer.extraer_hecho(
                cli, conn, nombre, ent, emp, desde, hasta,
                dry_run=dry_run, campo_fechas=campos)
            tot += n; ins_t += ins; upd_t += upd
            if not dry_run:
                registrar(conn, corrida, nombre, emp, modo, desde, hasta,
                          t0, n, ins, upd, "OK")
            if n:
                log(f"  [{i:>2}/{len(emps)}] {emp:<6} {n:>6,} filas · "
                    f"+{ins:,} nuevas · ~{upd:,} cambiadas")
        except Exception as e:
            errores += 1
            msg = f"{type(e).__name__}: {e}"
            log(f"  [{i:>2}/{len(emps)}] {emp:<6} !! {msg[:110]}")
            if not dry_run:
                registrar(conn, corrida, nombre, emp, modo, desde, hasta,
                          t0, 0, 0, 0, "ERROR", msg)
    return tot, ins_t, upd_t, errores


def sync_catalogo(cli, conn, corrida, nombre, ent, dry_run) -> tuple[int, int, int, int]:
    """Pull COMPLETO. Los catálogos son chicos (~13k filas entre todos) y así no
    dependemos de filtros que no hemos podido verificar uno por uno."""
    t0 = time.time()
    try:
        n, ins, upd = extraer.extraer_catalogo(cli, conn, nombre, ent, dry_run=dry_run)
        if not dry_run:
            registrar(conn, corrida, nombre, "-", "COMPLETO", None, None,
                      t0, n, ins, upd, "OK")
        log(f"  {nombre:<24} {n:>6,} filas · +{ins:,} nuevas · ~{upd:,} cambiadas")
        return n, ins, upd, 0
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        log(f"  {nombre:<24} !! {msg[:110]}")
        if not dry_run:
            registrar(conn, corrida, nombre, "-", "COMPLETO", None, None,
                      t0, 0, 0, 0, "ERROR", msg)
        return 0, 0, 0, 1


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description="Sync incremental INMOGES -> Snowflake")
    ap.add_argument("--desde", help="YYYY-MM-DD; por defecto, la última corrida OK")
    ap.add_argument("--entidades", help="lista separada por coma")
    ap.add_argument("--empresas", help="lista separada por coma")
    ap.add_argument("--dry-run", action="store_true", help="no escribe nada")
    ap.add_argument("--solo-hechos", action="store_true")
    ap.add_argument("--margen", type=int, default=MARGEN_DIAS,
                    help=f"días de solape hacia atrás (default {MARGEN_DIAS})")
    a = ap.parse_args()

    corrida = datetime.now().strftime("%Y%m%d%H%M%S")
    #  ⚠ SIEMPRE mañana: `fecha_fin_modificacion` es EXCLUSIVO. Ver la cabecera.
    hasta = (date.today() + timedelta(days=1)).isoformat()

    HECHOS = ["cfdi", "pago", "movimiento_bancario", "gasto", "pago_gasto"]
    CATALOGOS = ["empresa", "propietario", "inmueble", "unidad", "contrato",
                 "arrendatario", "sucursal_arrendatario", "cuenta_bancaria",
                 "cuenta_contable", "centro_costos", "producto", "pld"]
    if a.entidades:
        pedidas = [x.strip() for x in a.entidades.split(",")]
        HECHOS = [e for e in HECHOS if e in pedidas]
        CATALOGOS = [e for e in CATALOGOS if e in pedidas]
    if a.solo_hechos:
        CATALOGOS = []

    cli = bc.InmogesClient()
    conn = sf.conectar()
    asegurar_control(conn)
    emps = ([x.strip() for x in a.empresas.split(",")] if a.empresas
            else empresas(conn))

    banner(f"SYNC INCREMENTAL · corrida {corrida}"
           + ("   [DRY-RUN: no escribe]" if a.dry_run else ""))
    log(f"esquema {sf.ESQUEMA} · {len(emps)} empresas · hasta {hasta} (exclusivo)")

    t_ini = time.time()
    tot = ins_t = upd_t = err_t = 0
    for nombre in HECHOS:
        ent = ENTIDADES.get(nombre)
        if not ent:
            continue
        if a.desde:
            desde = a.desde
        else:
            ult = ultima_corrida_ok(conn, nombre)
            desde = ((ult - timedelta(days=a.margen)).isoformat() if ult
                     else (date.today() - timedelta(days=PRIMERA_VEZ_DIAS)).isoformat())
            if not ult:
                log(f"  ({nombre}: primera corrida -> se arranca "
                    f"{PRIMERA_VEZ_DIAS} días atrás)")
        n, i, u, e = sync_hecho(cli, conn, corrida, nombre, ent, emps,
                                desde, hasta, a.dry_run)
        tot += n; ins_t += i; upd_t += u; err_t += e

    if CATALOGOS:
        banner("catálogos · pull completo (son chicos)")
        for nombre in CATALOGOS:
            ent = ENTIDADES.get(nombre)
            if not ent:
                continue
            n, i, u, e = sync_catalogo(cli, conn, corrida, nombre, ent, a.dry_run)
            tot += n; ins_t += i; upd_t += u; err_t += e

    mins = (time.time() - t_ini) / 60
    banner("RESUMEN")
    print(f"  filas vistas      {tot:>10,}")
    if a.dry_run:
        # ⚠ En dry-run el motor devuelve ins=upd=0 SIEMPRE porque no escribe.
        #   Imprimir "sin cambio: N" aquí sería una MEDICIÓN QUE MIENTE (el bug
        #   24 nos costó 5 h por confiar en una). Se omite a propósito.
        print("  insertadas/actualizadas: no aplica en DRY-RUN (no se escribió)")
    else:
        print(f"  INSERTADAS        {ins_t:>10,}   <- registros NUEVOS")
        print(f"  ACTUALIZADAS      {upd_t:>10,}   <- ya existían y CAMBIARON")
        print(f"  sin cambio        {tot - ins_t - upd_t:>10,}   <- el MERGE no las tocó")
    print(f"  errores           {err_t:>10}")
    print(f"  duración          {mins:>10.1f} min · {cli.paginas:,} llamadas a la API")
    if a.dry_run:
        print("\n  (DRY-RUN: no se escribió nada en Snowflake)")
    conn.close()
    return 1 if err_t else 0


if __name__ == "__main__":
    try:
        _sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrumpido por el usuario")
        _sys.exit(130)
    except Exception:
        traceback.print_exc()
        _sys.exit(1)
