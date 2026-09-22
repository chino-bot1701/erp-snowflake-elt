# -*- coding: utf-8 -*-
r"""
reconciliar.py — LA RED DE SEGURIDAD: INMOGES es la fuente de la verdad
====================================================================
Proyecto: cargadirecta_snowflake                          (paso 2b del roadmap)

PARA QUÉ
--------
El sync diario (`sync.py`) descubre cambios preguntando **por fecha de
modificación**: "dame lo que tocaste desde ayer". Es rapidísimo, pero descansa
sobre una suposición: **que INMOGES sella esa fecha cada vez que toca un dato.**

Este módulo NO supone nada. Vuelve a pedirle a INMOGES ventanas de historia y
compara la huella (`ROW_HASH`) de cada registro contra la que tenemos guardada:

    huella igual      -> no se toca la fila (la inmensa mayoría)
    huella distinta   -> se ACTUALIZA esa fila, y solo esa
    llave que no está -> se inserta

Es el mismo MERGE de siempre, así que es idempotente y quirúrgico por
construcción. Lo nuevo aquí es **a qué ventanas se le vuelve a preguntar y en
qué orden**, para que con el tiempo quede cubierta TODA la historia sin volver a
pagar las 7 h del backfill de un jalón.

⚠ POR QUÉ EXISTE ESTO — MEDIDO EL 2026-09-07
--------------------------------------------
Se midió deriva REAL con `sondas/deriva.py`:

    cfdi EDN 2024-03   367 registros de INMOGES  ->  6 con huella distinta (1.6%)

Y con `sondas/deriva_detalle.py` se vio QUÉ había cambiado. No era ruido de
formato: era el campo **`contrato`**.

    id 285695   contrato 3340 -> 5212
    id 278949   contrato 3233 -> 5208

Ese campo es justo por el que agrupa el pipeline PLD (1 aviso por contrato-mes),
así que la deriva movería avisos.

La causa NO fue que INMOGES mintiera: sí había sellado `modifiedDate`
(2026-08-20/21). Fue un problema de COBERTURA — el sync arrancó el 31-ago
mirando 7 días atrás, y lo que INMOGES tocó entre nuestra extracción y esa fecha
se quedó viejo, en silencio.

    leccion: un filtro de "lo que cambió" solo cubre desde que lo enciendes.
             La historia anterior necesita que alguien la vuelva a mirar.

QUÉ VENTANAS RECORRE
--------------------
No se inventan rangos. Se usan **las mismas ventanas que cargó el backfill**,
leídas de `ERP_SYNC_CONTROL`. Así la cobertura es exactamente la que se
cargó, ni más ni menos:

    cfdi                  3,685 ventanas de MES con datos
    pago                  3,181 ventanas de MES
    movimiento_bancario     413 ventanas de AÑO
    gasto                    52 ventanas de AÑO
                          -----
                          7,331 ventanas

⚠ SE OMITEN A PROPÓSITO las ventanas que quedaron VACÍAS (0 filas): no hay nada
que pueda derivar en ellas. Si en una ventana vacía aparecen filas nuevas, las
caza `sync.py` por fecha de modificación. Se dice aquí para que quede por
escrito y nadie crea que "recorrió todo".

EN QUÉ ORDEN
------------
1. Las que **nunca se han revisado**, y dentro de esas:
2. ⭐ las entidades **sin `fecha_modificacion` en la API** (`movimiento_bancario`,
   `gasto`, `pago_gasto`), porque el sync diario NO las cubre y dependen
   enteramente de este módulo;
3. y al final, siempre **la que lleva más tiempo sin revisarse**.

Corriendo un rato cada noche, la historia completa queda cubierta y luego el
ciclo se repite solo.

⚠ El punto 2 se agregó el 2026-09-21 tras MEDIR que no pasaba. Ver `plan()`.

USO
---
    py src\reconciliar.py --horas 3                  # lo normal: 3 h y para
    py src\reconciliar.py --ventanas 200             # exactamente 200 ventanas
    py src\reconciliar.py --entidades cfdi
    py src\reconciliar.py --dry-run --ventanas 10    # no escribe: solo mide
    py src\reconciliar.py --estado                   # cuánto lleva cubierto
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

import argparse
import time
import traceback
from datetime import datetime

import erp_client as bc
import extraer
import sf
from entidades import ENTIDADES

TABLA_CTL = "ERP_RECONCILIACION"

#  Entidades que tiene sentido reconciliar: las que se cargaron por ventanas de
#  fecha de documento. Los catálogos NO van aquí porque `sync.py` ya los jala
#  COMPLETOS todos los días — ahí no puede haber deriva escondida.
RECONCILIABLES = ["cfdi", "pago", "movimiento_bancario", "gasto"]


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def banner(t: str) -> None:
    print("\n" + "=" * 78 + f"\n  {t}\n" + "=" * 78, flush=True)


# --------------------------------------------------------------------------- #
#  Bitácora
# --------------------------------------------------------------------------- #
def asegurar_control(conn) -> None:
    conn.cursor().execute(f"""
        CREATE TABLE IF NOT EXISTS {sf.ESQUEMA}.{TABLA_CTL} (
            CORRIDA_ID   VARCHAR(32),
            ENTIDAD      VARCHAR(60),
            EMPRESA      VARCHAR(60),
            VENTANA_INI  DATE,
            VENTANA_FIN  DATE,
            INICIO       TIMESTAMP_NTZ,
            FIN          TIMESTAMP_NTZ,
            FILAS_API    NUMBER,
            INSERTADAS   NUMBER,
            ACTUALIZADAS NUMBER,      -- <- LA DERIVA: filas que INMOGES cambio
            ESTADO       VARCHAR(15),
            MENSAJE      VARCHAR(2000)
        ) COMMENT = 'Bitacora del reconciliador por huella (reconciliar.py)'
    """)


def registrar(conn, corrida, entidad, empresa, ini, fin, t0,
              filas, ins, upd, estado, mensaje="") -> None:
    """⚠ NUNCA debe lanzar: se la llama desde el `except`. El 2026-08-31 un
    registrador que reventó dentro de un `except` tumbó la corrida entera y dejó
    11 catálogos sin ejecutar. Ver `sync.registrar`."""
    try:
        conn.cursor().execute(f"""
            INSERT INTO {sf.ESQUEMA}.{TABLA_CTL}
                (CORRIDA_ID, ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN,
                 INICIO, FIN, FILAS_API, INSERTADAS, ACTUALIZADAS, ESTADO, MENSAJE)
            SELECT %s, %s, %s,
                   TRY_TO_DATE(%s::VARCHAR), TRY_TO_DATE(%s::VARCHAR),
                   TO_TIMESTAMP_NTZ(%s::VARCHAR), CURRENT_TIMESTAMP(),
                   %s, %s, %s, %s, %s
        """, (corrida, entidad, empresa, ini, fin,
              datetime.fromtimestamp(t0).strftime("%Y-%m-%d %H:%M:%S"),
              filas, ins, upd, estado, (mensaje or "")[:2000]))
    except Exception as e:
        log(f"  (no se pudo escribir en la bitacora: {type(e).__name__}: "
            f"{str(e)[:90]})")


# --------------------------------------------------------------------------- #
#  Qué reconciliar y en qué orden
# --------------------------------------------------------------------------- #
def plan(conn, entidades: list[str], limite: int,
         empresas: list[str] | None = None,
         desde: str | None = None, hasta: str | None = None) -> list[tuple]:
    """Las `limite` ventanas que llevan más tiempo sin revisarse.

    La lista de ventanas NO se inventa: sale de las que cargó el backfill
    (`ERP_SYNC_CONTROL`), así la cobertura es la real. Las que nunca se
    han reconciliado van primero (NULLS FIRST).

    Los filtros opcionales (`empresas`, `desde`, `hasta`) sirven para atacar un
    tramo concreto —p. ej. revisar solo EDN 2024— sin esperar a que le toque su
    turno en la rotación.
    """
    marcas = ",".join(["%s"] * len(entidades))
    args: list = list(entidades)
    extra = ""
    if empresas:
        extra += f" AND EMPRESA IN ({','.join(['%s'] * len(empresas))})"
        args += empresas
    if desde:
        extra += " AND VENTANA_INI >= TRY_TO_DATE(%s::VARCHAR)"
        args.append(desde)
    if hasta:
        extra += " AND VENTANA_FIN <= TRY_TO_DATE(%s::VARCHAR)"
        args.append(hasta)

    #  ⭐ PRIORIDAD (2026-09-21): las entidades SIN `campo_delta` van PRIMERO.
    #
    #  POR QUE: `movimiento_bancario`, `gasto` y `pago_gasto` no tienen fecha de
    #  modificacion en la API, asi que el sync diario NO las cubre (solo mira 60
    #  dias por fecha de documento). Dependen ENTERAMENTE de este reconciliador.
    #
    #  ⚠ Y estaban las ULTIMAS de la fila. Medido el 2026-09-21, tras 2 corridas:
    #        cfdi  310/3,685 (8%)   ·  pago 129/3,181 (4%)
    #        movimiento_bancario 0/413   ·  gasto 0/52    <- nunca les tocaba
    #  Causa: al empatar todas en REVISADA=NULL, desempataba VENTANA_INI DESC, y
    #  esas dos se cargaron en ventanas de AÑO (la mas nueva empieza 2026-01-01)
    #  mientras cfdi/pago tienen ventanas de MES de 2026-09. Habia 487 ventanas
    #  de cfdi/pago por delante de la primera de movimiento_bancario.
    #
    #  Son solo 465 ventanas entre las dos: un par de corridas y quedan cubiertas.
    #  Despues el orden vuelve solo a "la que lleva mas tiempo sin revisarse".
    sin_delta = [e for e in entidades if not ENTIDADES[e].get("campo_delta")]
    if sin_delta:
        lista = ", ".join(f"'{e}'" for e in sin_delta)
        prioridad = f"CASE WHEN v.ENTIDAD IN ({lista}) THEN 0 ELSE 1 END,"
    else:
        prioridad = ""

    cur = conn.cursor()
    cur.execute(f"""
        WITH ventanas AS (
            SELECT ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN, MAX(FILAS) AS FILAS
              FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL
             WHERE ESTADO = 'COMPLETADO'
               AND ENTIDAD IN ({marcas})
               AND FILAS > 0                     -- una ventana vacia no puede derivar
               {extra}
             GROUP BY 1,2,3,4
        ),
        ultima AS (
            SELECT ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN, MAX(FIN) AS REVISADA
              FROM {sf.ESQUEMA}.{TABLA_CTL}
             WHERE ESTADO = 'OK'
             GROUP BY 1,2,3,4
        )
        SELECT v.ENTIDAD, v.EMPRESA, v.VENTANA_INI::VARCHAR, v.VENTANA_FIN::VARCHAR,
               v.FILAS, u.REVISADA
          FROM ventanas v
          LEFT JOIN ultima u
                 ON u.ENTIDAD = v.ENTIDAD AND u.EMPRESA = v.EMPRESA
                AND u.VENTANA_INI = v.VENTANA_INI AND u.VENTANA_FIN = v.VENTANA_FIN
         ORDER BY u.REVISADA ASC NULLS FIRST,
                  {prioridad}
                  v.VENTANA_INI DESC
         LIMIT {int(limite)}
    """, args)
    return cur.fetchall()


def cobertura(conn, entidades: list[str]) -> None:
    marcas = ",".join(["%s"] * len(entidades))
    cur = conn.cursor()
    cur.execute(f"""
        WITH ventanas AS (
            SELECT ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN
              FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL
             WHERE ESTADO='COMPLETADO' AND ENTIDAD IN ({marcas}) AND FILAS > 0
             GROUP BY 1,2,3,4
        ),
        ultima AS (
            SELECT ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN, MAX(FIN) AS REVISADA
              FROM {sf.ESQUEMA}.{TABLA_CTL} WHERE ESTADO='OK' GROUP BY 1,2,3,4
        )
        SELECT v.ENTIDAD, COUNT(*) AS total, COUNT(u.REVISADA) AS revisadas,
               MIN(u.REVISADA)::VARCHAR AS la_mas_vieja
          FROM ventanas v LEFT JOIN ultima u
                 ON u.ENTIDAD=v.ENTIDAD AND u.EMPRESA=v.EMPRESA
                AND u.VENTANA_INI=v.VENTANA_INI AND u.VENTANA_FIN=v.VENTANA_FIN
         GROUP BY 1 ORDER BY 1
    """, entidades)
    print(f"\n  {'entidad':<22}{'ventanas':>10}{'revisadas':>11}{'avance':>9}"
          f"   revisada mas antigua")
    print("  " + "-" * 76)
    for ent, tot, rev, vieja in cur.fetchall():
        pct = (rev / tot * 100) if tot else 0
        print(f"  {ent:<22}{tot:>10,}{rev:>11,}{pct:>8.0f}%   {str(vieja)[:19]}")

    cur.execute(f"""
        SELECT ENTIDAD, SUM(ACTUALIZADAS), SUM(INSERTADAS), SUM(FILAS_API)
          FROM {sf.ESQUEMA}.{TABLA_CTL} WHERE ESTADO='OK' GROUP BY 1 ORDER BY 1
    """)
    filas = cur.fetchall()
    if filas:
        print(f"\n  {'entidad':<22}{'DERIVA hallada':>16}{'insertadas':>12}"
              f"{'filas vistas':>14}")
        print("  " + "-" * 76)
        for ent, upd, ins, api in filas:
            print(f"  {ent:<22}{(upd or 0):>16,}{(ins or 0):>12,}{(api or 0):>14,}")


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Reconciliador por huella INMOGES -> Snowflake")
    ap.add_argument("--ventanas", type=int, help="cuantas ventanas revisar")
    ap.add_argument("--horas", type=float,
                    help="presupuesto de tiempo; para al agotarlo")
    ap.add_argument("--entidades", help="lista separada por coma")
    ap.add_argument("--empresas", help="lista separada por coma; solo esas")
    ap.add_argument("--desde", help="YYYY-MM-DD; solo ventanas desde esa fecha")
    ap.add_argument("--hasta", help="YYYY-MM-DD; solo ventanas hasta esa fecha")
    ap.add_argument("--dry-run", action="store_true", help="no escribe nada")
    ap.add_argument("--estado", action="store_true",
                    help="solo dice cuanto lleva cubierto y se sale")
    a = ap.parse_args()

    entidades = ([x.strip() for x in a.entidades.split(",")] if a.entidades
                 else list(RECONCILIABLES))
    entidades = [e for e in entidades if e in ENTIDADES]
    if not entidades:
        print("no hay entidades validas que reconciliar"); return 1

    conn = sf.conectar()
    asegurar_control(conn)

    if a.estado:
        banner("COBERTURA DEL RECONCILIADOR")
        cobertura(conn, entidades)
        print()
        conn.close()
        return 0

    #  Sin límite explícito: se pide un lote generoso y manda el reloj.
    limite = a.ventanas or (20000 if a.horas else 100)
    emps = [x.strip() for x in a.empresas.split(",")] if a.empresas else None
    pendientes = plan(conn, entidades, limite, emps, a.desde, a.hasta)
    if not pendientes:
        print("no hay ventanas que reconciliar"); conn.close(); return 0

    corrida = datetime.now().strftime("%Y%m%d%H%M%S")
    cli = bc.InmogesClient()
    tope = time.time() + a.horas * 3600 if a.horas else None

    banner(f"RECONCILIACION POR HUELLA · corrida {corrida}"
           + ("   [DRY-RUN: no escribe]" if a.dry_run else ""))
    log(f"esquema {sf.ESQUEMA} · {len(pendientes):,} ventanas en cola"
        + (f" · tope {a.horas} h" if a.horas else ""))
    log("INMOGES es la fuente de la verdad: si una huella difiere, gana INMOGES")

    t_ini = time.time()
    hechas = tot = ins_t = upd_t = err_t = 0
    deriva_por_entidad: dict[str, int] = {}

    for entidad, empresa, ini, fin, filas_prev, revisada in pendientes:
        if tope and time.time() > tope:
            log(f"se agoto el tiempo ({a.horas} h). Se para aqui; "
                f"la proxima corrida sigue por donde iba.")
            break

        ent = ENTIDADES[entidad]
        t0 = time.time()
        try:
            n, ins, upd = extraer.extraer_hecho(
                cli, conn, entidad, ent, empresa, ini, fin, dry_run=a.dry_run)
            tot += n; ins_t += ins; upd_t += upd; hechas += 1
            if upd or ins:
                deriva_por_entidad[entidad] = deriva_por_entidad.get(entidad, 0) + upd
                log(f"  {entidad:<20} {empresa:<5} {ini}..{fin}  "
                    f"{n:>6,} filas · ~{upd:,} CAMBIADAS · +{ins:,} nuevas")
            if not a.dry_run:
                registrar(conn, corrida, entidad, empresa, ini, fin,
                          t0, n, ins, upd, "OK")
        except Exception as e:
            err_t += 1
            msg = f"{type(e).__name__}: {e}"
            log(f"  {entidad:<20} {empresa:<5} {ini}..{fin}  !! {msg[:100]}")
            if not a.dry_run:
                registrar(conn, corrida, entidad, empresa, ini, fin,
                          t0, 0, 0, 0, "ERROR", msg)

        if hechas and hechas % 50 == 0:
            mins = (time.time() - t_ini) / 60
            log(f"  ... {hechas:,}/{len(pendientes):,} ventanas · {mins:.0f} min · "
                f"deriva acumulada {upd_t:,}")

    mins = (time.time() - t_ini) / 60
    banner("RESUMEN")
    print(f"  ventanas revisadas {hechas:>10,}")
    print(f"  filas vistas       {tot:>10,}")
    if a.dry_run:
        #  ⚠ En dry-run el motor devuelve ins=upd=0 SIEMPRE porque no escribe.
        #    Imprimir la deriva aqui seria una MEDICION QUE MIENTE (bug 24).
        print("  deriva: no aplica en DRY-RUN (no se escribio nada)")
    else:
        print(f"  DERIVA CORREGIDA   {upd_t:>10,}   <- filas que INMOGES habia cambiado")
        print(f"  insertadas         {ins_t:>10,}   <- registros que nos faltaban")
        print(f"  sin cambio         {tot - ins_t - upd_t:>10,}   <- ya estaban bien")
        for e, n in sorted(deriva_por_entidad.items(), key=lambda x: -x[1]):
            if n:
                print(f"      {e:<22} {n:>8,} filas actualizadas")
    print(f"  errores            {err_t:>10}")
    print(f"  duracion           {mins:>10.1f} min · {cli.paginas:,} llamadas API")

    if not a.dry_run:
        cobertura(conn, entidades)
    conn.close()
    print()
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
