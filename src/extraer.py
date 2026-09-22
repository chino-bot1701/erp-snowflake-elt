# -*- coding: utf-8 -*-
"""
extraer.py — MOTOR de extracción INMOGES -> Snowflake
===================================================
Proyecto: cargadirecta_snowflake

Un solo motor para las ~24 entidades. Lee la definición de `entidades.py`,
llama a la API con `erp_client.py` y sincroniza con `sf.merge_upsert()`.

USO
    py src/extraer.py --entidad empresa
    py src/extraer.py --entidad empresa --dry-run       (no escribe en Snowflake)
    py src/extraer.py --entidad cfdi --empresa EDN --ini 2026-07-01 --fin 2026-07-31
    py src/extraer.py --listar
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass


import argparse
import gc
import json
import sys
import time

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])

import erp_client as bc
import memoria
import sf
from entidades import ENTIDADES


# --------------------------------------------------------------------------- #
#  Mapeo declarativo: registro de la API -> fila de Snowflake
# --------------------------------------------------------------------------- #
def construir_fila(reg: dict, ent: dict) -> dict:
    """Aplana el registro segun `mapa`, convierte tipos y agrega el contrato RAW."""
    fila: dict = {}
    num_c, fec_c, int_c = set(ent["num"]), set(ent["fec"]), set(ent["int_"])

    for col, campo in ent["mapa"].items():
        v = reg.get(campo)
        if col in num_c:
            fila[col] = bc.num(v)
        elif col in fec_c:
            fila[col] = bc.fecha(v)
        elif col in int_c:
            fila[col] = bc.entero(v)
        else:
            fila[col] = bc.txt(v)

    for col, fn in (ent.get("derivados") or {}).items():
        try:
            fila[col] = fn(reg)
        except Exception:
            fila[col] = None

    # contrato RAW comun a todas las tablas
    fila["RAW_JSON"] = _raw_json(reg, ent)
    fila["ROW_HASH"] = sf.row_hash(reg)
    fila["ORIGEN"] = "API"
    fila["ACTIVO"] = True
    fila["FECHA_BAJA"] = None
    return fila


#  Techo de un valor en Snowflake: 16,777,216 bytes (16 MB). Se deja holgura.
LIMITE_RAW_JSON = 15_000_000
#  Por debajo de esto ni se mide: evita gastar un encode() en 10 millones de filas.
_UMBRAL_MEDIR = 3_000_000


def _raw_json(reg: dict, ent: dict) -> str:
    """El JSON íntegro del registro... salvo que NO QUEPA en Snowflake.

    ⚠ MEDIDO 2026-08-19: dos jobs murieron con
        ProgrammingError 100074: User character length limit (16777216) exceeded
    (HPI 2024-12 y HPI 2023-12, ambos DICIEMBRE = cierre de año). Causa: una
    factura de estacionamiento con ~9,000 partidas anidadas pesa más de 16 MB,
    que es el tamaño máximo de UN valor en Snowflake. No hay forma de guardarla
    entera en una sola celda.

    Qué se hace: si el JSON no cabe, se quitan del RAW_JSON del PADRE los arrays
    de hijos declarados en `hijos` (para /cfdi: `partida`) y se deja constancia
    de cuántos eran y dónde están.

    ⚠ ESTO NO VIOLA LA REGLA DE "NO OMITIR DATOS": esas partidas SÍ se guardan,
    completas y una por una, en su propia tabla (RAW_ERP_CFDI_PARTIDA)
    en la misma pasada. No se pierde ni un renglón: se deja de duplicar dentro
    del padre algo que ya vive normalizado en la tabla hija.

    La alternativa —perder la factura entera— sí violaría la regla.
    """
    s = json.dumps(reg, ensure_ascii=False, default=str)
    if len(s) < _UMBRAL_MEDIR or len(s.encode("utf-8")) <= LIMITE_RAW_JSON:
        return s

    claves = [h["clave"] for h in (ent.get("hijos") or [])]
    podado = {k: v for k, v in reg.items() if k not in claves}
    for c in claves:
        n = len(reg.get(c) or [])
        if n:
            podado[f"_{c}_movido_a_tabla_hija"] = n
    podado["_raw_json_podado"] = (
        f"el JSON completo superaba {LIMITE_RAW_JSON:,} bytes (límite de Snowflake); "
        f"los arrays {claves} viven íntegros en su tabla hija")
    s2 = json.dumps(podado, ensure_ascii=False, default=str)
    print(f"      ⚠ RAW_JSON de {ent['tabla']} demasiado grande "
          f"({len(s.encode('utf-8'))/1e6:.1f} MB) -> se poda {claves} "
          f"(quedan en la tabla hija). Ahora {len(s2.encode('utf-8'))/1e6:.2f} MB")
    del s
    return s2


def _sin_llave(filas: list[dict], llaves: list[str]) -> int:
    return sum(1 for f in filas if any(f.get(k) in (None, "") for k in llaves))


def cosechar_hijos(reg: dict, fila_padre: dict, ent: dict,
                   acumulador: dict[str, list]) -> None:
    """Materializa los arreglos ANIDADOS del JSON en sus propias tablas.

    Aplica RECURSIVAMENTE, asi que soporta nietos:
        pago -> pago[] (abonos) -> documento_relacionado_pago[]

    `hereda` copia columnas YA CONVERTIDAS de la fila padre (empresa, folio
    fiscal, fecha...) para que la tabla hija se pueda consultar sola sin JOIN.

    Por que importa: las partidas vienen en el MISMO JSON de /cfdi
    (incluir_partidas=true). Materializarlas aqui es GRATIS; hacerlo despues
    obligaria a re-procesar 241,855 VARIANTs o a una segunda pasada a la API.
    """
    for h in (ent.get("hijos") or []):
        items = reg.get(h["clave"]) or []
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            continue

        destino = acumulador.setdefault(h["tabla"], [])
        for it in items:
            if not isinstance(it, dict):
                continue
            fila = construir_fila(it, h)
            for col_hijo, col_padre in (h.get("hereda") or {}).items():
                fila[col_hijo] = fila_padre.get(col_padre)
            destino.append(fila)
            # nietos
            cosechar_hijos(it, fila, h, acumulador)


def _dedup(filas: list[dict], llaves: list[str],
           rechazos: list[dict] | None = None, contexto: dict | None = None
           ) -> list[dict]:
    """Separa lo insertable de lo que no tiene llave natural.

    REGLA DURA DEL PROYECTO: no se omite NADA en silencio. Lo que no tiene llave
    NO se descarta: se manda a RAW_ERP_RECHAZOS con su JSON completo para
    poder auditarlo y recuperarlo después.

    Los duplicados por llave SÍ se colapsan: no es perder datos, es el MISMO
    registro devuelto dos veces (pasa cuando el troceo adaptativo solapa
    ventanas). El MERGE haría exactamente lo mismo.
    """
    vistos, out = set(), []
    for f in filas:
        if any(f.get(k) in (None, "") for k in llaves):
            if rechazos is not None:
                rechazos.append({**(contexto or {}),
                                 "MOTIVO": f"sin llave natural ({','.join(llaves)})",
                                 "RAW_JSON": f.get("RAW_JSON"),
                                 "ROW_HASH": f.get("ROW_HASH")})
            continue
        k = tuple(f[k] for k in llaves)
        if k not in vistos:
            vistos.add(k)
            out.append(f)
    return out


def guardar_rechazos(conn, rechazos: list[dict]) -> None:
    """Persiste la cuarentena. Si esto falla, se avisa fuerte: significa que
    hay registros de la API que no quedaron en ningún lado."""
    if not rechazos or conn is None:
        return
    try:
        sf.merge_upsert(conn, "RAW_ERP_RECHAZOS", rechazos,
                        ["ROW_HASH"], soft_delete=False, verbose=False)
        print(f"  ⚠ {len(rechazos)} registro(s) SIN LLAVE -> RAW_ERP_RECHAZOS "
              f"(no se perdieron; revisar)")
    except Exception as e:
        print(f"  !!! NO SE PUDO GUARDAR LA CUARENTENA ({e}). "
              f"{len(rechazos)} registros en riesgo de perderse.")


def _tablas_hijas(ent: dict) -> dict[str, list[str]]:
    """{tabla_hija: llaves} recorriendo el arbol de hijos."""
    out = {}
    for h in (ent.get("hijos") or []):
        out[h["tabla"]] = h["llaves"]
        out.update(_tablas_hijas(h))
    return out


# --------------------------------------------------------------------------- #
#  Extracción de un CATÁLOGO (pull completo)
# --------------------------------------------------------------------------- #
def extraer_catalogo(cli, conn, nombre: str, ent: dict, dry_run=False,
                     limite_mb: float | None = 4096):
    t0 = time.perf_counter()
    print(f"\n=== CATALOGO: {nombre}  ->  {ent['tabla']} ===")

    if ent.get("trocear_por"):
        # Un pull completo de estas entidades muere por OFFSET PROFUNDO (la API
        # materializa offset+limit). Se parte en trozos para que cada consulta
        # quepa en 1-2 páginas. Mismo principio que ventana_dias en los hechos.
        #   trocear_por="empresa" -> contrato, cuenta_bancaria
        #   trocear_por="tipo"    -> actualizacion (el param no acepta lista)
        #
        # ⚠ EN STREAMING: se escribe trozo por trozo y se libera. La versión
        # que acumulaba las 50 empresas llegó a 3.1 GB de RAM (medido 2026-08-11).
        return _catalogo_troceado(cli, conn, nombre, ent, dry_run, t0, limite_mb)

    crudos = cli.paginar(ent["endpoint"], dict(ent["params"]),
                         page_size=ent["page_size"], etiqueta=nombre)
    print(f"  API: {len(crudos):,} registros  (esperado: {ent.get('esperado', 's/d')})")
    if not crudos:
        return 0, 0, 0

    filas = [construir_fila(r, ent) for r in crudos]

    huerfanos = _sin_llave(filas, ent["llaves"])
    if huerfanos:
        print(f"  ! {huerfanos} registros SIN llave natural -> se descartan")
        filas = [f for f in filas if all(f.get(k) not in (None, "") for k in ent["llaves"])]

    # de-dup por llave dentro del mismo pull
    vistos, unicas = set(), []
    for f in filas:
        k = tuple(f[k] for k in ent["llaves"])
        if k not in vistos:
            vistos.add(k)
            unicas.append(f)
    if len(unicas) != len(filas):
        print(f"  ! {len(filas)-len(unicas)} duplicados por llave en el pull -> se colapsan")
    filas = unicas

    if dry_run:
        print(f"  [DRY-RUN] no se escribe. Muestra de la primera fila:")
        ej = {k: v for k, v in list(filas[0].items())[:10]}
        for k, v in ej.items():
            print(f"      {k:<28} {str(v)[:60]}")
        print(f"      ROW_HASH                     {filas[0]['ROW_HASH']}")
        return len(filas), 0, 0

    ins, upd = sf.merge_upsert(conn, ent["tabla"], filas, ent["llaves"],
                               soft_delete=ent.get("soft_delete", False))
    print(f"  tiempo: {time.perf_counter()-t0:.1f}s")
    return len(filas), ins, upd


# --------------------------------------------------------------------------- #
#  Extracción de un HECHO (una ventana de una empresa)
# --------------------------------------------------------------------------- #
def _catalogo_troceado(cli, conn, nombre, ent, dry_run, t0,
                       limite_mb: float | None = 4096):
    """Catálogo grande: se recorre por empresa, escribiendo y LIBERANDO cada una.

    Antes acumulaba las 50 empresas en una lista -> 3.1 GB de RAM.
    Ahora el pico es UNA empresa (~100 contratos) -> decenas de MB.
    """
    if ent.get("trocear_por") == "tipo":
        # `/actualizacion` no acepta lista de tipos: hay que pedir uno por uno.
        empresas = ent.get("tipos", [])
        param_nombre = "tipo"
        print(f"  troceando por TIPO ({len(empresas)}): {', '.join(empresas)}")
    else:
        empresas = cli.empresas_alias()
        param_nombre = "empresa"
        print(f"  troceando por empresa ({len(empresas)}) — escribe y libera en cada una")
    llaves = ent["llaves"]
    ctx = dict(ENTIDAD=nombre, TABLA_DESTINO=ent["tabla"])
    vistos: set = set()
    n_api = n_esc = ins_tot = upd_tot = 0
    rechazos: list[dict] = []
    fallidas: list[str] = []

    with memoria.Vigilante(f"catalogo/{nombre}", limite_mb) as vig:
        for i, emp in enumerate(empresas, 1):
            p = {**ent["params"], param_nombre: emp}
            # ⚠ LECTURA SEGURA también aquí. El 2026-08-12 se detectó que este
            # bloque usaba `iter_paginas` (insegura) + `list()`: heredaba EL MISMO
            # defecto que hacía perder registros en /cfdi — la paginación por
            # offset de INMOGES duplica unos y omite otros — y encima anulaba el
            # streaming al materializar todas las páginas de la empresa.
            # Ahora: generador (sin list) + iter_ventana_segura si hay llave_api.
            llave_api = ent.get("llave_api") or (
                ent["mapa"].get(llaves[0]) if llaves else None)
            try:
                if llave_api:
                    fuente = cli.iter_ventana_segura(ent["endpoint"], p, llave_api,
                                                     ent["page_size"], 500, emp)
                else:
                    fuente = cli.iter_paginas(ent["endpoint"], p,
                                              ent["page_size"], emp)
                buf = []
                for lote in fuente:                     # streaming: 1 página a la vez
                    n_api += len(lote)
                    for r in lote:
                        f = construir_fila(r, ent)
                        k = tuple(f.get(x) for x in llaves)
                        if any(v in (None, "") for v in k):
                            rechazos.append({**ctx, "MOTIVO": "sin llave natural",
                                             "RAW_JSON": f.get("RAW_JSON"),
                                             "ROW_HASH": f.get("ROW_HASH")})
                            continue
                        if k in vistos:
                            continue
                        vistos.add(k)
                        buf.append(f)
                    del lote                            # <- soltar el crudo
            except Exception as e2:
                fallidas.append(emp)
                print(f"    !! {emp}: {type(e2).__name__}: {str(e2)[:80]}")
                continue

            if buf and not dry_run:
                i_, u_ = sf.merge_upsert(conn, ent["tabla"], buf, llaves,
                                         soft_delete=False, verbose=False)
                ins_tot += i_
                upd_tot += u_
            n_esc += len(buf)
            del buf                                     # <- liberar
            gc.collect()
            vig.verificar()
            if i % 10 == 0:
                print(f"    {i}/{len(empresas)} empresas · {n_esc:,} filas · "
                      f"RAM {memoria.mb():,.0f} MB")

    if rechazos and not dry_run:
        guardar_rechazos(conn, rechazos)
    print(f"  API={n_api:,} · escritos={n_esc:,} (esperado {ent.get('esperado','s/d')}) "
          f"· +{ins_tot:,} nuevos · ~{upd_tot:,} cambiados · cuarentena={len(rechazos)}")
    print(f"  tiempo: {time.perf_counter()-t0:.1f}s · {vig.resumen()}")
    if fallidas:
        # ⚠ ANTES esto solo se imprimía y el job se cerraba COMPLETADO: un
        # catálogo al que le faltaban empresas enteras se daba por bueno.
        # Mismo pecado que el bug de convergencia (2026-08-17). Ahora revienta.
        raise bc.VentanaIncompleta(
            f"{nombre}: {len(fallidas)} de {len(empresas)} trozos fallaron "
            f"({', '.join(fallidas[:12])}{'...' if len(fallidas) > 12 else ''}). "
            f"Se escribió lo que sí vino; el job queda ERROR para reintentarlo.")
    return n_esc, ins_tot, upd_tot


#  Tamaño del micro-lote: cuántas filas PADRE se acumulan antes de escribir y
#  liberar. Es la perilla que fija el techo de RAM. 500 CFDI con partidas
#  pesan ~100 MB de JSON -> pico acotado y predecible.
LOTE_FILAS = 500
LOTE_HIJOS = 20_000          # las partidas explotan: 1 CFDI puede traer 2,761


def extraer_hecho(cli, conn, nombre: str, ent: dict, empresa: str,
                  ini: str, fin: str, dry_run=False, limite_mb: float | None = 4096,
                  campo_fechas: tuple[str, str] | None = None):
    """Extracción de un hecho por (empresa, ventana) — EN STREAMING.

    Diseño de memoria (refactor 2026-08-11):
      * `iter_rango()` hace yield PÁGINA POR PÁGINA: nunca existe la lista completa.
      * Se acumula en micro-lotes (LOTE_FILAS) y se ESCRIBE + LIBERA.
      * Se sueltan las referencias al crudo apenas se transforma.
      * Un Vigilante mide el pico real y ABORTA el job si cruza `limite_mb`,
        para que muera UN job (reintentable) y no todo el backfill de 20 h.

    Resultado: el pico de RAM NO depende del tamaño del job.
    """
    t0 = time.perf_counter()
    print(f"\n=== HECHO: {nombre} · {empresa} · {ini}..{fin} ===")

    params = {**ent["params"], "empresa": empresa}
    llaves_hijas = _tablas_hijas(ent)
    ctx = dict(ENTIDAD=nombre, TABLA_DESTINO=ent["tabla"], EMPRESA=empresa,
               VENTANA_INI=ini, VENTANA_FIN=fin)

    buf: list[dict] = []
    buf_hijos: dict[str, list] = {}
    rechazos: list[dict] = []
    n_api = n_padres = ins_tot = upd_tot = n_hijos_tot = 0
    vistos: set = set()          # de-dup por llave a lo largo de TODO el job

    def descargar(forzar=False):
        """Escribe el micro-lote y LIBERA. El corazón del control de memoria."""
        nonlocal buf, buf_hijos, ins_tot, upd_tot, n_hijos_tot, n_padres
        pend_h = sum(len(v) for v in buf_hijos.values())
        if not forzar and len(buf) < LOTE_FILAS and pend_h < LOTE_HIJOS:
            return
        if buf:
            n_padres += len(buf)
            if not dry_run:
                i, u = sf.merge_upsert(conn, ent["tabla"], buf, ent["llaves"],
                                       soft_delete=False, verbose=False)
                ins_tot += i
                upd_tot += u
        for tabla, fh in buf_hijos.items():
            if fh:
                n_hijos_tot += len(fh)
                if not dry_run:
                    sf.merge_upsert(conn, tabla, fh, llaves_hijas[tabla],
                                    soft_delete=False, verbose=False)
        buf = []                      # <- liberar
        buf_hijos = {}                # <- liberar
        gc.collect()

    # ¿Cómo se recorre la ventana? Depende del filtro:
    #
    #  · POR FECHA DEL DOCUMENTO (backfill, lo de siempre) -> `iter_rango`, que
    #    trocea adaptativamente por días. Rango INCLUSIVO en los dos extremos.
    #
    #  · POR FECHA DE MODIFICACIÓN (sync incremental, 2026-08-28) -> NO se puede
    #    usar el mismo troceo. Medido: `fecha_fin_modificacion` es EXCLUSIVO
    #    ("hasta ese día a las 00:00"), así que una ventana de un solo día
    #    [D, D] devuelve SIEMPRE CERO. Al trocear por días, el resultado
    #    completo daba 0 en vez de 907.
    #    Se lee la ventana COMPLETA con `iter_ventana_segura`, que igual protege
    #    contra la paginación inestable (converge por pasadas).
    if campo_fechas is None:
        _paginas = cli.iter_rango(ent["endpoint"], params, ini, fin,
                                  page_size=ent["page_size"],
                                  etiqueta=f"{nombre}/{empresa}",
                                  ventana_dias=ent.get("ventana_dias", 30),
                                  llave=ent.get("llave_api"),
                                  rescate=ent.get("rescate"))
    else:
        _ci, _cf = campo_fechas
        _paginas = cli.iter_ventana_segura(
            ent["endpoint"], {**params, _ci: ini, _cf: fin},
            llave=ent["llave_api"], page_size=ent["page_size"],
            max_page_size=ent.get("max_page_size", 500),
            etiqueta=f"{nombre}/{empresa} mod", rescate=ent.get("rescate"))

    with memoria.Vigilante(f"{nombre}/{empresa} {ini}", limite_mb) as vig:
        # llave_api activa la LECTURA SEGURA contra la paginacion inestable
        for pagina in _paginas:
            n_api += len(pagina)
            for r in pagina:
                f = construir_fila(r, ent)
                k = tuple(f.get(k) for k in ent["llaves"])
                if any(v in (None, "") for v in k):
                    rechazos.append({**ctx, "MOTIVO": "sin llave natural",
                                     "RAW_JSON": f.get("RAW_JSON"),
                                     "ROW_HASH": f.get("ROW_HASH")})
                    continue
                if k in vistos:          # mismo registro devuelto 2 veces
                    continue
                vistos.add(k)
                buf.append(f)
                cosechar_hijos(r, f, ent, buf_hijos)
            del pagina                   # <- soltar el crudo de la API
            descargar()
            vig.verificar()              # aborta si la RAM se disparó
        descargar(forzar=True)

    if rechazos and not dry_run:
        guardar_rechazos(conn, rechazos)

    seg = time.perf_counter() - t0
    print(f"  API={n_api:,} padres · escritos={n_padres:,} · anidados={n_hijos_tot:,} "
          f"· cuarentena={len(rechazos)}")
    print(f"  +{ins_tot:,} nuevos · ~{upd_tot:,} cambiados · {seg:.1f}s · "
          f"páginas {cli.paginas}")
    print(f"  {vig.resumen()}")
    return n_padres, ins_tot, upd_tot


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Motor de extracción INMOGES -> Snowflake")
    ap.add_argument("--entidad")
    ap.add_argument("--empresa", help="alias de empresa (obligatorio en hechos)")
    ap.add_argument("--ini", help="YYYY-MM-DD")
    ap.add_argument("--fin", help="YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true", help="no escribe en Snowflake")
    ap.add_argument("--listar", action="store_true")
    ap.add_argument("--sandbox", action="store_true")
    a = ap.parse_args()

    if a.listar:
        print(f"{'entidad':<24} {'tipo':<10} {'tabla':<40} {'esperado':>10}")
        print("-" * 88)
        for n, e in ENTIDADES.items():
            print(f"{n:<24} {e['tipo']:<10} {e['tabla']:<40} {str(e.get('esperado','s/d')):>10}")
        return

    if not a.entidad or a.entidad not in ENTIDADES:
        print(f"Entidad desconocida. Usa --listar. ({a.entidad})")
        sys.exit(1)

    ent = ENTIDADES[a.entidad]
    cli = bc.InmogesClient(bc.TOKEN_SANDBOX if a.sandbox else bc.TOKEN_PROD)
    conn = None if a.dry_run else sf.conectar()

    try:
        if ent["tipo"] == "catalogo":
            n, ins, upd = extraer_catalogo(cli, conn, a.entidad, ent, a.dry_run)
        else:
            if not (a.empresa and a.ini and a.fin):
                print("Los hechos requieren --empresa --ini --fin")
                sys.exit(1)
            n, ins, upd = extraer_hecho(cli, conn, a.entidad, ent,
                                        a.empresa, a.ini, a.fin, a.dry_run)
        print(f"\nRESUMEN  filas={n:,}  insertadas={ins:,}  actualizadas={upd:,}"
              f"  páginas_api={cli.paginas}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
