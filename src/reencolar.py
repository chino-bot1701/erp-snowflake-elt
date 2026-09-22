# -*- coding: utf-8 -*-
"""
reencolar.py — devuelve a PENDIENTE los jobs que se cerraron con huecos
=======================================================================
Proyecto: cargadirecta_snowflake

POR QUÉ EXISTE (2026-08-17)
--------------------------
`iter_ventana_segura` avisaba de las ventanas que no lograba traer, pero solo
con un `print`. El job seguía y se cerraba como COMPLETADO. Resultado: 1,128
días de CFDI perdidos que en SYNC_CONTROL se veían perfectos.

El bug ya está arreglado en `erp_client.py` (rescate vía /partida + excepción
`VentanaIncompleta`). Falta REPARAR lo ya cargado: este script lee los logs,
saca los (empresa, día) que fallaron, los traduce a los jobs de MES que los
contienen y los devuelve a PENDIENTE con INTENTOS=0.

Volver a correrlos es seguro e idempotente: el MERGE va por ROW_HASH, así que
lo que ya está no se duplica — solo se COMPLETA lo que falta.

USO
    py src/reencolar.py --de-logs                 # ver qué se va a re-encolar
    py src/reencolar.py --de-logs --aplicar       # aplicarlo
    py src/reencolar.py --entidad cfdi --empresas HPI,MDI --aplicar
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

import argparse
import os
import re
import sys

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])

import sf

LOGS = [os.path.join(os.environ.get("LOCALAPPDATA", ""), n) for n in
        ("erp_nocturno.log", "erp_maraton.log", "erp_maraton_stdout.log",
         "erp_contrato.log")]

#  ⚠ [cfdi/HPI 2022-07-21] NO CONVERGIÓ en 6 pasadas (0 ids).
PAT = re.compile(r"\[(\w+)/(\w+) (\d{4})-(\d{2})-(\d{2})\]\s*NO CONVERGI")


def de_logs() -> dict[tuple[str, str, str], set[str]]:
    """{(entidad, empresa, 'YYYY-MM'): {dias...}} leído de los logs."""
    out: dict[tuple[str, str, str], set[str]] = {}
    for ruta in LOGS:
        if not os.path.exists(ruta):
            continue
        with open(ruta, "r", encoding="utf-8", errors="replace") as fh:
            for linea in fh:
                m = PAT.search(linea)
                if m:
                    ent, emp, a, mm, dd = m.groups()
                    out.setdefault((ent, emp, f"{a}-{mm}"), set()).add(f"{a}-{mm}-{dd}")
    return out


def reencolar(claves: list[tuple[str, str, str]], aplicar: bool) -> int:
    """Devuelve a PENDIENTE los jobs (entidad, empresa, mes) indicados."""
    if not claves:
        print("  nada que re-encolar.")
        return 0
    conn = sf.conectar()
    try:
        cur = conn.cursor()
        # Se identifica el job por el MES de VENTANA_INI: así funciona tanto si
        # el job es de mes (cfdi/pago) como si fuera de año.
        cond = " OR ".join(
            "(ENTIDAD = %s AND EMPRESA = %s AND TO_CHAR(VENTANA_INI,'YYYY-MM') = %s)"
            for _ in claves)
        params = [x for c in claves for x in c]

        cur.execute(f"SELECT COUNT(*) FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL "
                    f"WHERE {cond}", params)
        encontrados = cur.fetchone()[0]
        print(f"  jobs en SYNC_CONTROL que coinciden: {encontrados:,}")

        if not aplicar:
            print("  (simulación — usa --aplicar para escribir)")
            return encontrados

        cur.execute(f"""
            UPDATE {sf.ESQUEMA}.ERP_SYNC_CONTROL
               SET ESTADO = 'PENDIENTE',
                   INTENTOS = 0,
                   MENSAJE = 'RE-ENCOLADO 2026-08-17: la ventana se cerró '
                                || 'con días no convergidos (bug de convergencia). '
                                || 'Estado previo: ' || COALESCE(ESTADO,'?')
             WHERE {cond}
        """, params)
        n = cur.rowcount
        conn.commit()
        print(f"  {n:,} jobs devueltos a PENDIENTE (INTENTOS=0)")
        return n
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--de-logs", action="store_true",
                    help="deduce los jobs afectados de los logs de las corridas")
    ap.add_argument("--entidad", help="re-encolar a mano: entidad")
    ap.add_argument("--empresas", help="re-encolar a mano: lista separada por coma")
    ap.add_argument("--aplicar", action="store_true", help="escribir de verdad")
    a = ap.parse_args()

    if a.de_logs:
        d = de_logs()
        if not d:
            print("No se encontraron avisos 'NO CONVERGIÓ' en los logs.")
            return
        dias = sum(len(v) for v in d.values())
        print(f"\n  {dias:,} días con hueco · {len(d):,} jobs (entidad, empresa, mes)\n")
        por_emp: dict[str, list[int]] = {}
        for (ent, emp, mes), v in d.items():
            por_emp.setdefault(f"{ent}/{emp}", [0, 0])
            por_emp[f"{ent}/{emp}"][0] += 1
            por_emp[f"{ent}/{emp}"][1] += len(v)
        print(f"    {'entidad/empresa':<22} {'jobs':>6} {'días':>7}")
        print("    " + "-" * 37)
        for k, (nj, nd) in sorted(por_emp.items(), key=lambda x: -x[1][1]):
            print(f"    {k:<22} {nj:>6,} {nd:>7,}")
        print()
        reencolar(sorted(d.keys()), a.aplicar)

    elif a.entidad and a.empresas:
        conn = sf.conectar()
        cur = conn.cursor()
        emps = [e.strip() for e in a.empresas.split(",")]
        marca = ",".join(["%s"] * len(emps))
        if not a.aplicar:
            cur.execute(f"SELECT COUNT(*) FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL "
                        f"WHERE ENTIDAD=%s AND EMPRESA IN ({marca})",
                        [a.entidad] + emps)
            print(f"  coinciden {cur.fetchone()[0]:,} jobs "
                  f"(simulación — usa --aplicar)")
        else:
            cur.execute(f"UPDATE {sf.ESQUEMA}.ERP_SYNC_CONTROL "
                        f"SET ESTADO='PENDIENTE', INTENTOS=0 "
                        f"WHERE ENTIDAD=%s AND EMPRESA IN ({marca})",
                        [a.entidad] + emps)
            print(f"  {cur.rowcount:,} jobs devueltos a PENDIENTE")
            conn.commit()
        conn.close()
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
