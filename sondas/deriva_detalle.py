# -*- coding: utf-8 -*-
r"""
deriva_detalle.py — QUE campo cambio exactamente   (SONDA DE SOLO LECTURA)
=========================================================================
`deriva.py` dice CUANTOS registros cambiaron. Esta dice QUE cambio en cada uno,
campo por campo, comparando el JSON que INMOGES devuelve HOY contra el RAW_JSON
que guardamos el dia de la carga.

POR QUE HACE FALTA ESTA SEGUNDA MEDICION
----------------------------------------
Un hash distinto NO prueba que INMOGES actualizo el dato. Tambien cambia el hash
si la API devuelve lo mismo en otro orden, con otro formateo de decimales, o con
un campo nuevo vacio. Si reconciliaramos por hash sin mirar esto, podriamos
estar reescribiendo millones de filas todos los dias para nada.

    cambio de NEGOCIO  (un campo vacio que ahora tiene dato)  -> hay que traerlo
    ruido de la API    (orden, formato, campo nuevo en null)  -> hay que ignorarlo

NO ESCRIBE NADA.

USO
---
    py sondas\deriva_detalle.py --empresa EDN --ids 285695,278949,278950
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

import argparse
import json
import os

_AQUI = os.path.dirname(os.path.abspath(__file__))
_sys.path.insert(0, os.path.join(os.path.dirname(_AQUI), "src"))

import erp_client as bc      # noqa: E402
import sf                      # noqa: E402
from entidades import ENTIDADES  # noqa: E402

IGNORAR = ("createdDate", "modifiedDate")   # los mismos que excluye row_hash


def aplanar(d, pref=""):
    """dict/list anidado -> {'ruta.al.campo': valor}. Las listas se indexan."""
    out = {}
    if isinstance(d, dict):
        for k, v in d.items():
            if k in IGNORAR:
                continue
            out.update(aplanar(v, f"{pref}{k}."))
    elif isinstance(d, list):
        for i, v in enumerate(d):
            out.update(aplanar(v, f"{pref}[{i}]."))
    else:
        out[pref.rstrip(".")] = d
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entidad", default="cfdi")
    ap.add_argument("--empresa", required=True)
    ap.add_argument("--ids", required=True, help="separados por coma")
    a = ap.parse_args()

    ent = ENTIDADES[a.entidad]
    ids = [x.strip() for x in a.ids.split(",")]
    col = ent["llaves"][0]

    conn = sf.conectar(); cur = conn.cursor()
    cur.execute(f"""SELECT {col}::VARCHAR, FECHA::VARCHAR, RAW_JSON
                    FROM {sf.ESQUEMA}.{ent['tabla']}
                    WHERE {col}::VARCHAR IN ({','.join(['%s'] * len(ids))})""", ids)
    guardado = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    conn.close()

    if not guardado:
        print("no se encontraron esos ids en Snowflake"); return 1

    cli = bc.InmogesClient()
    params = dict(ent.get("params") or {})
    if ent.get("requiere_empresa"):
        params["empresa"] = a.empresa

    # se pide el DIA de cada registro y se busca el id dentro
    dias = sorted({v[0][:10] for v in guardado.values() if v[0]})
    vivo = {}
    for d in dias:
        for pagina in cli.iter_rango(ent["endpoint"], params, d, d,
                                     page_size=ent["page_size"],
                                     etiqueta=f"detalle {d}",
                                     ventana_dias=1, llave=ent["llave_api"]):
            for reg in pagina:
                k = str(reg.get(ent["llave_api"]))
                if k in guardado:
                    vivo[k] = reg

    print(f"\n{'='*78}\n  QUE CAMBIO ENTRE LO GUARDADO Y LO QUE INMOGES DICE HOY\n{'='*78}")
    resumen = {}
    for k in ids:
        if k not in guardado:
            print(f"\n  id {k}: no esta en Snowflake"); continue
        if k not in vivo:
            print(f"\n  id {k}: INMOGES ya no lo devuelve en su fecha"); continue

        antes = aplanar(json.loads(guardado[k][1]))
        ahora = aplanar(vivo[k])
        difs = []
        for campo in sorted(set(antes) | set(ahora)):
            va, vb = antes.get(campo, "<no estaba>"), ahora.get(campo, "<ya no viene>")
            if va != vb:
                difs.append((campo, va, vb))
                resumen[campo] = resumen.get(campo, 0) + 1

        print(f"\n  --- id {k}  ({len(difs)} campos distintos) ---")
        for campo, va, vb in difs[:25]:
            print(f"      {campo}")
            print(f"         guardado : {str(va)[:90]}")
            print(f"         INMOGES hoy: {str(vb)[:90]}")
        if len(difs) > 25:
            print(f"      ... y {len(difs)-25} campos mas")

    if resumen:
        print(f"\n{'='*78}\n  CAMPOS QUE MAS CAMBIAN\n{'='*78}")
        for campo, n in sorted(resumen.items(), key=lambda x: -x[1]):
            print(f"    {n:>3}x  {campo}")
    print()
    return 0


if __name__ == "__main__":
    _sys.exit(main())
