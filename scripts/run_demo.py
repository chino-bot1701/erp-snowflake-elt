# -*- coding: utf-8 -*-
"""
End-to-end demo. No credentials, no network, no Snowflake.

    python scripts/run_demo.py

Four passes against a SQLite stand-in, to show what the row-hash merge buys:

  1. First load          — everything is new
  2. Same data again     — zero writes (idempotent)
  3. Source re-stamps    — every modifiedDate changes, nothing else:
                           still zero writes, because the hash ignores them
  4. Real edits          — only the edited rows are updated

Pass 3 is the one that matters. The source ERP re-stamps modification dates in
bulk overnight; a pipeline that trusted those dates would rewrite the whole
table every morning.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "src"))
sys.path.insert(0, str(RAIZ))

# The engine is imported unchanged from src/ — only the two boundaries
# (the API client and the warehouse) are swapped for demo doubles.
import entidades as E                       # noqa: E402  the declarative catalog
from extraer import construir_fila          # noqa: E402  the real flattener
from demo import fake_api                   # noqa: E402
from demo.warehouse import Warehouse        # noqa: E402

ENTIDADES_DEMO = ["empresa", "inmueble", "unidad", "arrendatario", "contrato"]
FILAS = {"empresa": 6, "inmueble": 40, "unidad": 320,
         "arrendatario": 260, "contrato": 300}
DB = RAIZ / "out" / "demo.sqlite"


def linea(c: str = "-") -> None:
    print(c * 72)


def cargar(wh: Warehouse, fuente: dict[str, list[dict]], etiqueta: str) -> dict:
    """One full pass over every demo entity. Returns per-entity results."""
    print(f"\n{etiqueta}")
    linea()
    print(f"  {'entity':<16}{'table':<26}{'result'}")
    resultados = {}
    for nombre in ENTIDADES_DEMO:
        ent = E.ENTIDADES[nombre]
        filas = [construir_fila(reg, ent) for reg in fuente[nombre]]
        res = wh.merge_upsert(ent["tabla"], filas, ent["llaves"])
        resultados[nombre] = res
        print(f"  {nombre:<16}{ent['tabla']:<26}{res}")
    total = sum(r.escrituras for r in resultados.values())
    print(f"  {'':<42}{'writes: ' + str(total)}")
    return resultados


def main() -> int:
    DB.parent.mkdir(exist_ok=True)
    if DB.exists():
        DB.unlink()

    print(__doc__.strip().split("\n")[0])
    linea("=")
    print(f"  catalog        : {len(E.ENTIDADES)} entities declared in src/entidades.py")
    print(f"  demo subset    : {', '.join(ENTIDADES_DEMO)}")
    print(f"  warehouse      : SQLite at {DB.relative_to(RAIZ)}")

    fuente = {n: fake_api.generar(n, E.ENTIDADES[n], FILAS[n])
              for n in ENTIDADES_DEMO}
    wh = Warehouse(str(DB))

    # ---- pass 1 ---------------------------------------------------- #
    r1 = cargar(wh, fuente, "PASS 1 — first load")
    assert all(r.actualizadas == 0 for r in r1.values())

    # ---- pass 2 ---------------------------------------------------- #
    r2 = cargar(wh, fuente, "PASS 2 — identical source, run again")
    escrituras2 = sum(r.escrituras for r in r2.values())

    # ---- pass 3 ---------------------------------------------------- #
    resellado = {n: fake_api.resellar_fechas(v) for n, v in fuente.items()}
    r3 = cargar(wh, resellado,
                "PASS 3 — source re-stamps every modifiedDate, no real change")
    escrituras3 = sum(r.escrituras for r in r3.values())

    # ---- pass 4 ---------------------------------------------------- #
    mutado, esperados = {}, {}
    for n, v in resellado.items():
        cuantos = max(1, len(v) // 20)          # about 5% of rows
        mutado[n], indices = fake_api.mutar(v, E.ENTIDADES[n], cuantos)
        esperados[n] = len(indices)
    r4 = cargar(wh, mutado, "PASS 4 — 5% of records genuinely edited")

    # ---- verdict --------------------------------------------------- #
    print()
    linea("=")
    print("  RESULT")
    linea()
    ok = True

    def check(cond: bool, texto: str) -> None:
        nonlocal ok
        ok = ok and cond
        print(f"  [{'PASS' if cond else 'FAIL'}]  {texto}")

    check(escrituras2 == 0,
          f"re-running identical data wrote {escrituras2} rows (expected 0)")
    check(escrituras3 == 0,
          f"bulk re-stamped dates wrote {escrituras3} rows (expected 0) "
          "— ROW_HASH excludes createdDate/modifiedDate")
    for n in ENTIDADES_DEMO:
        check(r4[n].actualizadas == esperados[n] and r4[n].insertadas == 0,
              f"{n}: {r4[n].actualizadas} rows updated, expected {esperados[n]}; "
              f"{r4[n].insertadas} inserted, expected 0")

    print()
    print("  Final row counts")
    for nombre in ENTIDADES_DEMO:
        tabla = E.ENTIDADES[nombre]["tabla"]
        print(f"    {tabla:<26}{wh.contar(tabla):>7}")
    wh.close()

    print()
    print(f"  Inspect it:  sqlite3 {DB.relative_to(RAIZ)} "
          f'"SELECT * FROM RAW_ERP_CONTRATO LIMIT 5"')
    linea("=")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
