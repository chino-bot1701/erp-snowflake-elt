# -*- coding: utf-8 -*-
"""
verificar.py — RECONCILIACIÓN API vs SNOWFLAKE (la prueba de que no falta nada)
==============================================================================
Proyecto: cargadirecta_snowflake

POR QUÉ EXISTE (2026-08-17)
---------------------------
El bug de convergencia enseñó la lección más cara del proyecto: **un job en
COMPLETADO no prueba nada**. 1,128 días se perdieron y SYNC_CONTROL se veía
impecable. `reencolar.py` repara lo que los LOGS delataron, pero los logs son
frágiles: se rotan, se pierden en un apagón, y no existen para las corridas que
nadie capturó.

Esto es la verificación que no depende de nadie: para cada ventana ya cargada
pregunta a la API **cuántos registros debería haber** y lo compara contra lo que
hay en Snowflake. Si no cuadra, el job vuelve a PENDIENTE.

POR QUÉ ES BARATO
-----------------
El conteo usa la SONDA LIGERA (`incluir_*=false`). Medido: 1.4 MB / 3.6 s por
mes, contra 77.6 MB / 249 s con partidas (55x menos peso, 69x menos tiempo).
Y el lado de Snowflake es UN SOLO GROUP BY para todos los meses de golpe.

CÓMO CUENTA (y por qué así)
---------------------------
No se usa `registros_encontrados` (son las filas de LA PÁGINA, no el total) ni
`iter_paginas` a secas (la paginación por offset de INMOGES omite registros). Se
usa `iter_ventana_segura`, que garantiza el conjunto completo, y se cuentan IDs
ÚNICOS. Es el mismo criterio con el que se carga: comparar manzanas con manzanas.

USO
    py src/verificar.py --entidad cfdi --empresas HPI --limite 12
    py src/verificar.py --entidad cfdi                       # todo (largo)
    py src/verificar.py --entidad cfdi --reencolar           # + repara lo que no cuadre
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

import argparse
import sys
import time

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])

import erp_client as bc
import sf
from entidades import ENTIDADES

#  Columna de fecha de cada entidad: es la que la API filtra con
#  fecha_inicio/fecha_fin, así que es la que define a qué ventana pertenece
#  cada fila. Usar otra desalinearía la comparación.
COL_FECHA = {"cfdi": "FECHA", "pago": "FECHA", "movimiento_bancario": "FECHA",
             "gasto": "FECHA"}

# Tamaño máximo de la ventana con la que se INTERROGA a la API al verificar.
# Medido (bug 26): con 366 días la API omite filas y ni siquiera es determinista;
# con 31 cuadra exacto y cuesta lo mismo. Ver `contar_api()`.
VENTANA_CHK_DIAS = 31


def conteos_snowflake(conn, ent: dict, entidad: str) -> dict[tuple[str, str], int]:
    """{(empresa, 'YYYY-MM-DD'): ids_unicos} de UNA sola consulta.

    ⚠ BUG 24 (2026-08-27): esto agrupaba por MES y el comparador buscaba el
    cubo `TO_CHAR(VENTANA_INI,'YYYY-MM')`. Funciona si el job es mensual
    (`cfdi`, `pago`) pero **`movimiento_bancario`, `gasto` y `pago_gasto` tienen
    ventanas ANUALES** (236-366 días) → comparaba el conteo de la API del AÑO
    ENTERO contra el de Snowflake de ENERO SOLO.
    Resultado: 371 de 900 ventanas marcadas con hueco y **182,500 registros
    "faltantes" que en realidad estaban ahí**, más ~5 h de recargas inútiles.
    Caso testigo: VVD 2023 → API 775 · enero 63 · **año completo 775 = cuadra**.

    Ahora se agrupa por DÍA y el comparador SUMA el rango real del job.
    """
    col = COL_FECHA.get(entidad, "FECHA")
    llave = ent["llaves"][0]
    cur = conn.cursor()
    cur.execute(f"""
        SELECT EMPRESA, TO_CHAR({col}, 'YYYY-MM-DD') AS DIA, COUNT(DISTINCT {llave})
        FROM {sf.ESQUEMA}.{ent['tabla']}
        WHERE {col} IS NOT NULL
        GROUP BY 1, 2
    """)
    return {(r[0], r[1]): int(r[2]) for r in cur.fetchall()}


def contar_sf_rango(sfc: dict, empresa: str, ini: str, fin: str) -> int:
    """Suma los ids de Snowflake en [ini, fin] — el rango REAL del job, sea de
    un día, de un mes o de un año. Sustituye al cubo mensual del bug 24."""
    from datetime import date, timedelta
    d = date.fromisoformat(ini)
    hasta = date.fromisoformat(fin)
    total = 0
    while d <= hasta:
        total += sfc.get((empresa, d.isoformat()), 0)
        d += timedelta(days=1)
    return total


def jobs_cargados(conn, entidad: str, empresas: list[str] | None,
                  limite: int | None) -> list[tuple]:
    """Los jobs que se dan por buenos hoy. Son justo los que hay que auditar:
    un job PENDIENTE ya se va a correr, no hace falta comprobarlo."""
    cur = conn.cursor()
    q = (f"SELECT EMPRESA, TO_CHAR(VENTANA_INI,'YYYY-MM-DD'), "
         f"TO_CHAR(VENTANA_FIN,'YYYY-MM-DD'), ESTADO, FILAS "
         f"FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL "
         f"WHERE ENTIDAD = %s AND ESTADO IN ('COMPLETADO','VACIO') ")
    p: list = [entidad]
    if empresas:
        q += f"AND EMPRESA IN ({','.join(['%s']*len(empresas))}) "
        p += empresas
    q += "ORDER BY EMPRESA, VENTANA_INI"
    if limite:
        q += f" LIMIT {int(limite)}"
    cur.execute(q, tuple(p))
    return cur.fetchall()


def contar_api(cli, ent: dict, empresa: str, ini: str, fin: str) -> int:
    """IDs únicos que la API dice que hay en la ventana. Si la ventana ni siquiera
    se puede contar, se propaga la excepción: 'no se pudo verificar' NO es
    'está bien'.

    ⚠ BUG 26 (2026-08-27): esto pedía el rango ENTERO de un tirón
    (`iter_ventana_segura` sobre [ini, fin]). El CARGADOR no hace eso: usa
    `iter_rango`, que trocea según `ventana_dias` de la entidad (7 días en
    `movimiento_bancario`). Con jobs ANUALES el verificador pedía 365 días de
    golpe y **la API omitía registros en silencio** (bug 3, paginación inestable).
    Resultado: 17 ventanas marcadas como "sobran" — es decir, acusaba a Snowflake
    de tener basura cuando en realidad Snowflake estaba MÁS COMPLETO que la API.

    Caso testigo **MDI 2024**: pidiendo el año → 3,722. Snowflake → 3,736.
    Los 14 de diferencia son todos del **2024-09-30**, y preguntando por ESE DÍA
    la API sí los devuelve (44 ese día, incluidos los 14).

    Ahora se cuenta **troceando**, igual que carga el motor.

    ### Por qué 31 días (medido, no supuesto) — MDI 2024, Snowflake = 3,736
    ```
    troceo de 366 días -> 3,732 ids · 21.4 s · 23 llamadas   ✗ (y otra corrida dio 3,722)
    troceo de  31 días -> 3,736 ids · 20.4 s · 25 llamadas   ✓ CUADRA
    troceo de   7 días -> 3,736 ids · 26.4 s · 54 llamadas   ✓ CUADRA, pero 2x llamadas
    ```
    31 días **cuesta lo mismo que pedir el año entero y sí cuadra**. Además el
    rango anual **ni siquiera es determinista** (3,722 vs 3,732 en dos lecturas).

    Se usa 31 fijo, no el `ventana_dias` de la entidad, para no penalizar a `cfdi`
    y `pago`: sus jobs ya son mensuales, así que siguen costando UNA llamada
    (`ventana_dias=1` los habría vuelto 31x más lentos sin ganar nada).
    """
    ids: set = set()
    for lote in cli.iter_rango(ent["endpoint"],
                               {**cli._ligeros(ent["params"]), "empresa": empresa},
                               ini, fin,
                               page_size=ent.get("page_size", 100),
                               etiqueta=f"chk {empresa} {ini}",
                               ventana_dias=VENTANA_CHK_DIAS,
                               llave=ent["llave_api"],
                               max_page_size=ent.get("max_page_size", 500)):
        for r in lote:
            k = str(r.get(ent["llave_api"]))
            if k and k != "None":
                ids.add(k)
    return len(ids)


def verificar(entidad: str, empresas: list[str] | None = None,
              limite: int | None = None, reencolar: bool = False):
    ent = ENTIDADES[entidad]
    cli = bc.InmogesClient()
    conn = sf.conectar()
    t0 = time.perf_counter()
    try:
        jobs = jobs_cargados(conn, entidad, empresas, limite)
        if not jobs:
            print("No hay jobs COMPLETADO/VACIO que verificar.")
            return
        sfc = conteos_snowflake(conn, ent, entidad)
        print(f"\n{'='*84}")
        print(f"  VERIFICACIÓN {entidad} · {len(jobs):,} ventanas cargadas")
        print(f"{'='*84}")
        print(f"  {'empresa':<8} {'ventana':<10} {'API':>8} {'SNOWFLAKE':>10} "
              f"{'dif':>8}  veredicto")
        print("  " + "-" * 80)

        malos: list[tuple[str, str, str]] = []
        ok = revisados = 0
        falta_total = 0
        for empresa, ini, fin, estado, filas in jobs:
            mes = ini[:7]
            try:
                n_api = contar_api(cli, ent, empresa, ini, fin)
            except Exception as e:
                print(f"  {empresa:<8} {mes:<10} {'?':>8} {'?':>10} {'?':>8}  "
                      f"NO SE PUDO CONTAR ({type(e).__name__})")
                malos.append((entidad, empresa, mes))
                continue
            n_sf = contar_sf_rango(sfc, empresa, ini, fin)   # rango REAL (bug 24)
            revisados += 1
            dif = n_api - n_sf
            if dif == 0:
                ok += 1
                continue                      # solo se imprime lo que NO cuadra
            falta_total += max(dif, 0)
            veredicto = "FALTAN EN SNOWFLAKE" if dif > 0 else "sobran (revisar ventana)"
            print(f"  {empresa:<8} {mes:<10} {n_api:>8,} {n_sf:>10,} {dif:>+8,}  {veredicto}")
            if dif > 0:
                malos.append((entidad, empresa, mes))

        print("  " + "-" * 80)
        print(f"  cuadran {ok:,}/{revisados:,} · con hueco {len(malos):,} · "
              f"registros faltantes {falta_total:,} · "
              f"{(time.perf_counter()-t0)/60:.1f} min · {cli.paginas:,} llamadas")

        if malos and reencolar:
            import reencolar as rq
            print()
            rq.reencolar(sorted(set(malos)), aplicar=True)
        elif malos:
            print(f"\n  usa --reencolar para devolverlos a PENDIENTE")
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entidad", default="cfdi")
    ap.add_argument("--empresas", help="lista separada por coma")
    ap.add_argument("--limite", type=int)
    ap.add_argument("--reencolar", action="store_true",
                    help="devolver a PENDIENTE los jobs que no cuadren")
    a = ap.parse_args()
    emp = [e.strip() for e in a.empresas.split(",")] if a.empresas else None
    verificar(a.entidad, emp, a.limite, a.reencolar)


if __name__ == "__main__":
    main()
