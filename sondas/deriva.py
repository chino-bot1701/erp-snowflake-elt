# -*- coding: utf-8 -*-
r"""
deriva.py — ¿se nos escapan cambios de INMOGES?   (SONDA DE SOLO LECTURA)
======================================================================
Proyecto: cargadirecta_snowflake

LA PREGUNTA
-----------
El sync diario descubre cambios preguntando por FECHA DE MODIFICACION. Eso
funciona **solo si INMOGES re-sella `modifiedDate` cada vez que toca un campo.**
Si INMOGES rellena campos vacios con un UPDATE masivo de base de datos y no mueve
esa fecha, el cambio es INVISIBLE para nosotros y nadie se entera nunca.

Esta sonda contesta esa pregunta con una medicion, no con una suposicion:

    1. Vuelve a pedirle a INMOGES una ventana VIEJA por fecha de DOCUMENTO
       (justo lo que el sync incremental jamas vuelve a mirar).
    2. Calcula el ROW_HASH de cada registro EXACTAMENTE igual que el pipeline.
    3. Lo compara contra el ROW_HASH guardado en Snowflake.

    todos los hashes iguales -> el filtro de modificacion no se pierde nada
    algun hash distinto      -> DERIVA SILENCIOSA: hay que reconciliar por hash

NO ESCRIBE NADA. Ni un INSERT, ni un UPDATE, ni en la bitacora.

USO
---
    py sondas\deriva.py --entidad cfdi --empresa EDN --ini 2024-03-01 --fin 2024-03-31
    py sondas\deriva.py --entidad movimiento_bancario --empresa EDN \
                        --ini 2024-03-01 --fin 2024-03-31
    py sondas\deriva.py --muestra          # barrido corto ya configurado
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

import argparse
import os
import time

_AQUI = os.path.dirname(os.path.abspath(__file__))
_sys.path.insert(0, os.path.join(os.path.dirname(_AQUI), "src"))

import erp_client as bc      # noqa: E402
import sf                      # noqa: E402
from entidades import ENTIDADES  # noqa: E402


def comparar(cli, conn, entidad: str, empresa: str, ini: str, fin: str) -> dict:
    """Devuelve el veredicto de una ventana. Solo lectura."""
    ent = ENTIDADES[entidad]
    llave_api = ent["llave_api"]
    col_llave = ent["llaves"][0]
    tabla = ent["tabla"]

    params = dict(ent.get("params") or {})
    if ent.get("requiere_empresa"):
        params["empresa"] = empresa

    # --- 1. lo que dice INMOGES HOY, por fecha de documento ------------------
    vivo: dict[str, str] = {}
    t0 = time.time()
    for pagina in cli.iter_rango(
            ent["endpoint"], params, ini, fin,
            page_size=ent["page_size"], etiqueta=f"deriva {entidad}/{empresa}",
            ventana_dias=ent.get("ventana_dias", 1), llave=llave_api,
            max_page_size=ent.get("max_page_size", 500),
            rescate=ent.get("rescate")):
        for reg in pagina:
            k = reg.get(llave_api)
            if k is None:
                continue
            vivo[str(k)] = sf.row_hash(reg)
    seg = time.time() - t0

    if not vivo:
        return dict(entidad=entidad, empresa=empresa, ini=ini, fin=fin,
                    api=0, sf=0, iguales=0, distintos=0, solo_api=0,
                    solo_sf=0, segundos=seg, ejemplos=[])

    # --- 2. lo que tenemos guardado ----------------------------------------
    cur = conn.cursor()
    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE _DERIVA_K (K VARCHAR, H VARCHAR)
    """)
    vals = list(vivo.items())
    for i in range(0, len(vals), 5000):
        lote = vals[i:i + 5000]
        cur.executemany("INSERT INTO _DERIVA_K (K, H) VALUES (%s, %s)", lote)

    cur.execute(f"""
        SELECT
          COUNT_IF(T.{col_llave} IS NOT NULL AND T.ROW_HASH = S.H)  AS iguales,
          COUNT_IF(T.{col_llave} IS NOT NULL AND T.ROW_HASH <> S.H) AS distintos,
          COUNT_IF(T.{col_llave} IS NULL)                           AS solo_api
        FROM _DERIVA_K S
        LEFT JOIN {sf.ESQUEMA}.{tabla} T
               ON T.{col_llave}::VARCHAR = S.K
    """)
    iguales, distintos, solo_api = cur.fetchone()

    ejemplos = []
    if distintos:
        cur.execute(f"""
            SELECT S.K, T.ROW_HASH, S.H
            FROM _DERIVA_K S
            JOIN {sf.ESQUEMA}.{tabla} T ON T.{col_llave}::VARCHAR = S.K
            WHERE T.ROW_HASH <> S.H
            LIMIT 5
        """)
        ejemplos = [r[0] for r in cur.fetchall()]

    return dict(entidad=entidad, empresa=empresa, ini=ini, fin=fin,
                api=len(vivo), iguales=iguales, distintos=distintos,
                solo_api=solo_api, segundos=seg, ejemplos=ejemplos)


def imprimir(v: dict) -> None:
    estado = "OK" if v["distintos"] == 0 and v["solo_api"] == 0 else "!! DERIVA"
    print(f"  {v['entidad']:<20} {v['empresa']:<5} {v['ini']}..{v['fin']}  "
          f"API {v['api']:>6,} · iguales {v['iguales']:>6,} · "
          f"CAMBIADOS {v['distintos']:>5,} · faltan {v['solo_api']:>4,} · "
          f"{v['segundos']:>5.1f}s  {estado}")
    if v["ejemplos"]:
        print(f"        ejemplos de llave con hash distinto: {v['ejemplos']}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Sonda de deriva (solo lectura)")
    ap.add_argument("--entidad")
    ap.add_argument("--empresa")
    ap.add_argument("--ini")
    ap.add_argument("--fin")
    ap.add_argument("--muestra", action="store_true",
                    help="barrido corto ya configurado")
    a = ap.parse_args()

    cli = bc.InmogesClient()
    conn = sf.conectar()

    print(f"\nSONDA DE DERIVA · esquema {sf.ESQUEMA} · SOLO LECTURA\n")

    if a.muestra:
        casos = [
            # entidad,               empresa, ini,          fin
            ("cfdi",                 "EDN", "2024-03-01", "2024-03-31"),
            ("cfdi",                 "DHU", "2023-06-01", "2023-06-30"),
            ("movimiento_bancario",  "EDN", "2024-03-01", "2024-03-31"),
            ("pago",                 "EDN", "2024-03-01", "2024-03-31"),
        ]
    else:
        if not (a.entidad and a.ini and a.fin):
            ap.error("hacen falta --entidad --ini --fin (o usa --muestra)")
        casos = [(a.entidad, a.empresa or "", a.ini, a.fin)]

    for entidad, empresa, ini, fin in casos:
        try:
            imprimir(comparar(cli, conn, entidad, empresa, ini, fin))
        except Exception as e:
            print(f"  {entidad:<20} {empresa:<5} !! {type(e).__name__}: {str(e)[:100]}")

    conn.close()
    print()
    return 0


if __name__ == "__main__":
    _sys.exit(main())
