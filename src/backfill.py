# -*- coding: utf-8 -*-
"""
backfill.py — CARGA HISTÓRICA REANUDABLE Y PARALELA
===================================================
Proyecto: cargadirecta_snowflake

UN JOB = (ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN)
porque /cfdi, /pago, /movimiento_bancario y /gasto exigen `empresa` con UN SOLO
valor -> la extracción es un producto cartesiano (50 empresas x ~17 años).

Cada job se registra en ERP_SYNC_CONTROL y se cierra por separado:
    PENDIENTE -> EN_CURSO -> COMPLETADO | VACIO | ERROR
Reanudar = volver a correr; solo se consumen los que NO están COMPLETADO/VACIO.

PARALELISMO: los jobs son independientes, así que se reparten en N hilos. Cada
hilo abre su PROPIA conexión a Snowflake y su PROPIA sesión HTTP (requests.Session
no es thread-safe si se comparte).

⚠ ANTES DE CORRER EL BACKFILL COMPLETO: avisar a INMOGES. N hilos sostenidos
durante horas contra su API pueden verse como un ataque y degradar el servicio
para el resto de Almena.

USO
    py src/backfill.py --generar --entidad cfdi --desde 2010-01-01 --hasta 2026-12-31
    py src/backfill.py --estado
    py src/backfill.py --run --hilos 4
    py src/backfill.py --run --hilos 4 --entidad cfdi --limite 20
    py src/backfill.py --escalamiento --entidad cfdi      (mide 1 vs 2 vs 4 hilos)
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass


import argparse
import gc
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])

import erp_client as bc
import memoria
import sf
from entidades import ENTIDADES
from extraer import extraer_hecho, extraer_catalogo

# Granularidad por entidad. cfdi va por MES porque con partidas un mes de una
# empresa grande ya tarda ~4 min; años enteros harían jobs de ~50 min y perder
# uno a la mitad costaría caro.
GRANULARIDAD = {"cfdi": "MES", "pago": "MES"}
DEFECTO = "ANIO"

_imprimir = threading.Lock()


def log(msg: str):
    with _imprimir:
        print(msg, flush=True)


# --------------------------------------------------------------------------- #
#  Generación de la cola de jobs
# --------------------------------------------------------------------------- #
def ventanas(desde: str, hasta: str, gran: str) -> list[tuple[str, str]]:
    d0, d1 = date.fromisoformat(desde), date.fromisoformat(hasta)
    out = []
    if gran == "ANIO":
        for a in range(d0.year, d1.year + 1):
            out.append((max(date(a, 1, 1), d0).isoformat(),
                        min(date(a, 12, 31), d1).isoformat()))
    else:  # MES
        a, m = d0.year, d0.month
        while (a, m) <= (d1.year, d1.month):
            ini = date(a, m, 1)
            fin = date(a + (m == 12), (m % 12) + 1, 1) - __import__("datetime").timedelta(days=1)
            out.append((max(ini, d0).isoformat(), min(fin, d1).isoformat()))
            a, m = (a + 1, 1) if m == 12 else (a, m + 1)
    return out


def generar(entidad: str, desde: str, hasta: str, empresas: list[str] | None = None):
    ent = ENTIDADES[entidad]
    conn = sf.conectar()
    try:
        if not ent.get("requiere_empresa"):
            sf.job_registrar(conn, entidad, None, None, None, "FULL")
            log(f"  1 job (catálogo, pull completo)")
            return

        if empresas is None:
            cli = bc.InmogesClient()
            empresas = cli.empresas_alias()
        gran = GRANULARIDAD.get(entidad, DEFECTO)
        vs = ventanas(desde, hasta, gran)

        # ⚠ CARGA MASIVA, no fila por fila.
        # La versión anterior llamaba a sf.job_registrar() por cada job: 10,200
        # round-trips a Snowflake = ~90 minutos SOLO para armar la cola
        # (medido 2026-08-12: 11% en 10 min). Ahora se sube todo de un golpe con
        # write_pandas + un único INSERT ... SELECT que descarta los ya existentes.
        # Segundos en vez de hora y media.
        import pandas as pd
        from snowflake.connector.pandas_tools import write_pandas

        filas = [{"ENTIDAD": entidad, "EMPRESA": emp,
                  "VENTANA_INI": ini, "VENTANA_FIN": fin,
                  "GRANULARIDAD": gran, "ESTADO": "PENDIENTE",
                  "FILAS": 0, "FILAS_NUEVAS": 0, "FILAS_CAMBIADAS": 0,
                  "PAGINAS": 0, "INTENTOS": 0}
                 for emp in empresas for ini, fin in vs]
        log(f"  preparando {len(filas):,} jobs ({len(empresas)} empresas x "
            f"{len(vs)} ventanas {gran})...")

        cur = conn.cursor()
        stg = "ERP_SYNC_STG"
        cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {sf.ESQUEMA}.{stg} ("
                    f"ENTIDAD VARCHAR, EMPRESA VARCHAR, VENTANA_INI DATE, "
                    f"VENTANA_FIN DATE, GRANULARIDAD VARCHAR, ESTADO VARCHAR, "
                    f"FILAS NUMBER, FILAS_NUEVAS NUMBER, FILAS_CAMBIADAS NUMBER, "
                    f"PAGINAS NUMBER, INTENTOS NUMBER)")
        write_pandas(conn, pd.DataFrame(filas), stg, database=sf.DATABASE,
                     schema=sf.SCHEMA, quote_identifiers=False)

        # Solo se insertan los que NO existen ya (así relanzar es idempotente
        # y no pisa el avance de jobs ya completados).
        cur.execute(f"""
            INSERT INTO {sf.ESQUEMA}.ERP_SYNC_CONTROL
                (ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN, GRANULARIDAD,
                 ESTADO, FILAS, FILAS_NUEVAS, FILAS_CAMBIADAS, PAGINAS, INTENTOS)
            SELECT s.ENTIDAD, s.EMPRESA, s.VENTANA_INI, s.VENTANA_FIN,
                   s.GRANULARIDAD, s.ESTADO, 0, 0, 0, 0, 0
            FROM {sf.ESQUEMA}.{stg} s
            WHERE NOT EXISTS (
                SELECT 1 FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL c
                WHERE c.ENTIDAD = s.ENTIDAD
                  AND EQUAL_NULL(c.EMPRESA, s.EMPRESA)
                  AND EQUAL_NULL(c.VENTANA_INI, s.VENTANA_INI)
                  AND EQUAL_NULL(c.VENTANA_FIN, s.VENTANA_FIN))
        """)
        nuevos = cur.rowcount
        conn.commit()
        cur.close()
        log(f"  {nuevos:,} jobs nuevos encolados "
            f"({len(filas) - nuevos:,} ya existían y se respetaron)")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
#  Consumo de la cola
# --------------------------------------------------------------------------- #
def pendientes(entidad: str | None = None, limite: int | None = None) -> list[tuple]:
    conn = sf.conectar()
    try:
        cur = conn.cursor()
        q = (f"SELECT ENTIDAD, EMPRESA, TO_CHAR(VENTANA_INI,'YYYY-MM-DD'), "
             f"TO_CHAR(VENTANA_FIN,'YYYY-MM-DD') "
             f"FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL "
             f"WHERE ESTADO IN ('PENDIENTE','EN_CURSO','ERROR') AND INTENTOS < 3 ")
        p = []
        if entidad:
            q += "AND ENTIDAD = %s "
            p.append(entidad)
        q += "ORDER BY ENTIDAD, VENTANA_INI, EMPRESA"
        if limite:
            q += f" LIMIT {int(limite)}"
        cur.execute(q, tuple(p))
        return cur.fetchall()
    finally:
        conn.close()


#  Techo de RAM por job. Si se cruza, el job se aborta SOLO y queda reintentable,
#  en vez de que el sistema operativo mate el backfill completo de 20 h.
#  Con 32 GB de RAM, 4 GB por job deja margen amplio incluso con 2 hilos.
LIMITE_MB_JOB = 4096


def correr_job(job: tuple) -> dict:
    """Ejecuta UN job con su propia conexión, su cliente HTTP y su techo de RAM."""
    entidad, empresa, ini, fin = job
    ent = ENTIDADES[entidad]
    t0 = time.perf_counter()
    cli = bc.InmogesClient()
    conn = sf.conectar()
    pico = memoria.mb()
    try:
        if ent["tipo"] == "catalogo":
            n, ins, upd = extraer_catalogo(cli, conn, entidad, ent,
                                           limite_mb=LIMITE_MB_JOB)
        else:
            n, ins, upd = extraer_hecho(cli, conn, entidad, ent, empresa, ini, fin,
                                        limite_mb=LIMITE_MB_JOB)
        pico = memoria.mb()
        estado = "COMPLETADO" if n else "VACIO"
        sf.job_cerrar(conn, entidad, empresa, ini, fin, estado, n, ins, upd,
                      cli.paginas, None, round(time.perf_counter() - t0, 2),
                      ram_pico=round(pico, 1))
        conn.commit()
        return dict(job=job, estado=estado, filas=n,
                    seg=time.perf_counter() - t0, ram=pico)
    except memoria.LimiteMemoria as e:
        # No es un error de datos: es protección. Se deja PENDIENTE, no ERROR,
        # para que el reintento lo tome (idealmente con un LOTE_FILAS menor).
        try:
            sf.job_cerrar(conn, entidad, empresa, ini, fin, "PENDIENTE", 0, 0, 0,
                          cli.paginas, f"ABORTADO POR RAM: {e}",
                          round(time.perf_counter() - t0, 2),
                          ram_pico=round(memoria.mb(), 1))
            conn.commit()
        except Exception:
            pass
        log(f"  ⚠ RAM {entidad}/{empresa} {ini}: abortado y devuelto a PENDIENTE")
        return dict(job=job, estado="RAM", filas=0,
                    seg=time.perf_counter() - t0, ram=memoria.mb())
    except Exception as e:
        try:
            sf.job_cerrar(conn, entidad, empresa, ini, fin, "ERROR", 0, 0, 0,
                          cli.paginas, f"{type(e).__name__}: {e}",
                          round(time.perf_counter() - t0, 2),
                          ram_pico=round(memoria.mb(), 1))
            conn.commit()
        except Exception:
            pass
        log(f"  ERROR {entidad}/{empresa} {ini}: {type(e).__name__}: {str(e)[:120]}")
        # `mensaje` viaja de vuelta para que quien llame pueda distinguir un fallo
        # AJENO (504 de INMOGES) de uno del motor. El canario lo usa: confundirlos
        # abortó una corrida entera el 2026-08-21.
        return dict(job=job, estado="ERROR", filas=0, mensaje=f"{type(e).__name__}: {e}",
                    seg=time.perf_counter() - t0, ram=memoria.mb())
    finally:
        conn.close()
        gc.collect()


def run(hilos: int = 4, entidad: str | None = None, limite: int | None = None):
    jobs = pendientes(entidad, limite)
    if not jobs:
        log("No hay jobs pendientes.")
        return
    log(f"\n{'='*72}\n  {len(jobs):,} jobs pendientes · {hilos} hilos\n{'='*72}")
    t0 = time.perf_counter()
    hechos = filas = errores = 0

    with ThreadPoolExecutor(max_workers=hilos) as ex:
        futs = {ex.submit(correr_job, j): j for j in jobs}
        for fut in as_completed(futs):
            r = fut.result()
            hechos += 1
            filas += r["filas"]
            errores += (r["estado"] == "ERROR")
            transcurrido = time.perf_counter() - t0
            ritmo = hechos / transcurrido if transcurrido else 0
            falta = (len(jobs) - hechos) / ritmo / 3600 if ritmo else 0
            log(f"  [{hechos:>5}/{len(jobs)}] {r['estado']:<11} "
                f"{r['filas']:>7,} filas · {r['seg']:>6.1f}s · "
                f"RAM {r.get('ram',0):>6,.0f} MB · ETA {falta:.1f} h · err {errores}")

    log(f"\n  TOTAL: {filas:,} filas · {hechos} jobs · "
        f"{(time.perf_counter()-t0)/60:.1f} min · errores {errores}")


# --------------------------------------------------------------------------- #
#  Prueba de escalamiento — ¿de verdad escala con más hilos?
# --------------------------------------------------------------------------- #
def escalamiento(entidad: str, muestras: int = 4):
    """Mide la MISMA carga con 1, 2 y 4 hilos.

    Por qué: el paralelismo NO siempre escala lineal. Si el cuello es el servidor
    de INMOGES construyendo JSON, más hilos pueden no ayudar — o degradar. Medirlo
    con 4 ventanas es barato; descubrirlo a la hora 5 de un backfill, no.
    """
    jobs = pendientes(entidad, muestras)
    if len(jobs) < 2:
        log("No hay jobs suficientes para medir. Genera primero.")
        return
    log(f"\nPrueba de escalamiento con {len(jobs)} jobs de '{entidad}'")
    log("(los jobs se marcan COMPLETADO; el MERGE por ROW_HASH los hace idempotentes)\n")
    base = None
    for h in (1, 2, 4):
        js = pendientes(entidad, muestras)
        if not js:
            log("  (ya no quedan pendientes; usa --generar para más)")
            break
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=h) as ex:
            list(ex.map(correr_job, js))
        seg = time.perf_counter() - t0
        base = base or seg
        log(f"  {h} hilo(s): {seg:6.1f} s   speedup {base/seg:.2f}x   "
            f"eficiencia {(base/seg)/h*100:5.1f}%")


# --------------------------------------------------------------------------- #
def estado():
    conn = sf.conectar()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {sf.ESQUEMA}.VW_ERP_SYNC_RESUMEN")
    filas = cur.fetchall()
    cols = [c[0] for c in cur.description]
    if not filas:
        print("SYNC_CONTROL vacío: genera jobs con --generar")
    else:
        print(" · ".join(cols))
        for f in filas:
            print("   " + " · ".join(str(x) for x in f))
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--generar", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--estado", action="store_true")
    ap.add_argument("--escalamiento", action="store_true")
    ap.add_argument("--entidad")
    ap.add_argument("--desde", default="2010-01-01")
    ap.add_argument("--hasta", default=date.today().isoformat())
    ap.add_argument("--empresas", help="lista separada por coma (default: las 50)")
    ap.add_argument("--hilos", type=int, default=4)
    ap.add_argument("--limite", type=int)
    a = ap.parse_args()

    if a.estado:
        estado()
    elif a.generar:
        if not a.entidad:
            print("--generar requiere --entidad"); sys.exit(1)
        emp = a.empresas.split(",") if a.empresas else None
        generar(a.entidad, a.desde, a.hasta, emp)
    elif a.escalamiento:
        escalamiento(a.entidad or "cfdi", a.limite or 4)
    elif a.run:
        run(a.hilos, a.entidad, a.limite)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
