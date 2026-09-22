# -*- coding: utf-8 -*-
"""
erp_client.py — Cliente GET de la API oficial de INMOGES
=======================================================
Proyecto: cargadirecta_snowflake

Adaptado de `Cobranza/inmoges_api.py` (validado en producción) con los hallazgos
de la FASE 0:

  * PAGE_SIZE ya NO es fijo en 100. `/cfdi` aguanta 1000/página SI se pide
    `incluir_xml_code=false` (el límite viejo era por arrastrar el XML del SAT).
    `/pago` no pasa de ~500. Cada entidad declara el suyo en entidades.py.
  * `registros_encontrados` de la API son las filas de LA PÁGINA, no el total
    -> nunca se usa para saber cuánto falta; se pagina hasta agotar.
  * `errors` llega como array [] en unos endpoints y como string "" en otros.
  * process=false con "no se encontraron..." NO es error: es página vacía.
  * "el tamaño del diccionario excede lo permitido" -> partir la ventana a la
    mitad (troceo adaptativo recursivo).
"""
from __future__ import annotations

import os

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass


import time
from datetime import date, timedelta
from typing import Any, Iterator

import requests

BASE_URL = os.environ.get("ERP_API_BASE", "")
TOKEN_PROD    = os.environ.get("ERP_API_TOKEN", "")
TOKEN_SANDBOX = os.environ.get("ERP_API_TOKEN_SANDBOX", "")          # mismo host; no timbra ante el SAT

TIMEOUT = (30, 180)          # (conexión, lectura). API Gateway corta ~29 s por request.
PAUSA   = 0.10               # cortesía entre páginas

#  Si una ventana trae <= este número de registros, se pide COMPLETA en una
#  sola llamada en vez de trocearla por días. Calibrado 2026-08-12: el 69% de
#  los jobs con datos caen aquí y gastaban ~30 llamadas para traer <50 filas.
#  Conservador a propósito: las facturas de estacionamiento traen hasta 2,761
#  partidas cada una, así que 40 x 2,761 ya reventaría el límite de 30 s
#  -> en ese caso el TamanoExcedido devuelve el control al troceo normal.
UMBRAL_SIN_TROCEAR = 40


class TamanoExcedido(RuntimeError):
    """La API rechazó la consulta: 'el tamaño del diccionario excede lo permitido'."""


class ErrorTransporte(RuntimeError):
    """La petición no llegó a completarse: 504/502/503, timeout o corte de red.

    Se distingue del resto de errores a propósito. Un 504 NO significa que el
    código esté mal: significa que el servidor de INMOGES no alcanzó a construir
    la respuesta dentro de su límite de 30 s. Es un problema de TAMAÑO DE
    VENTANA disfrazado de problema de red, y por eso se trata como tal: se parte
    la ventana en dos y se reintenta (ver `_iter_adaptativo`).

    ⚠ Confundirlo con "el motor está roto" costó una corrida de fin de semana
    completa el 2026-08-21: el canario tomó 3 jobs que fallaban con 504, los dio
    por prueba de que el código estaba mal, y abortó.
    """


class VentanaIncompleta(RuntimeError):
    """No se pudo GARANTIZAR que la ventana viniera completa.

    ⚠ EXISTE POR UNA REGLA DURA DEL PROYECTO: no se omite ningún dato.

    Antes (bug medido 2026-08-17) una ventana que no se lograba traer solo
    imprimía un aviso y seguía: el job se cerraba como COMPLETADO y quedaban
    huecos SILENCIOSOS. Fueron 1,128 días perdidos (828 HPI · 189 MDI · 91 EDN
    · 20 MEO) que parecían cargados.

    Ahora se lanza esta excepción: `backfill.correr_job` la atrapa, marca el job
    como ERROR y lo deja reintentable. Un hueco visible es reparable; uno
    silencioso, no.
    """


class InmogesClient:
    def __init__(self, token: str = TOKEN_PROD, base_url: str = BASE_URL):
        self.base_url = base_url
        self.ses = requests.Session()
        self.ses.headers.update({"Accept": "application/json",
                                 "Authorization-token": token})
        self.paginas = 0        # contador de la corrida (para SYNC_CONTROL)

    # ---------------------------------------------------------------- HTTP --
    def _get(self, endpoint: str, params: dict, reintentos: int = 4) -> dict:
        ultimo = None
        for intento in range(1, reintentos + 1):
            try:
                r = self.ses.get(f"{self.base_url}/{endpoint}", params=params,
                                 timeout=TIMEOUT)
                if r.status_code in (429, 502, 503, 504):
                    raise requests.HTTPError(f"{r.status_code} {r.reason}", response=r)
                r.raise_for_status()
                self.paginas += 1
                return r.json()
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
                ultimo = e
                if intento == reintentos:
                    break
                espera = 3 * intento          # backoff lineal: 3, 6, 9 s
                print(f"      ! {type(e).__name__} -> reintento {intento+1}/{reintentos} en {espera}s")
                time.sleep(espera)
        raise ErrorTransporte(f"/{endpoint} falló tras {reintentos} intentos: {ultimo}")

    # ------------------------------------------------------------ envelope --
    @staticmethod
    def desenvolver(payload: dict) -> list[dict]:
        """{result:{process,data},errors} -> lista de registros.

        `data` cambia de forma según el endpoint:
            lista directa      /cfdi, /inmueble
            dict con la lista  /contrato -> contratos[] · /pago -> cfdis[]
            dict simple        /empresa, /arrendatario, /unidad  (1 objeto)
        """
        result = payload.get("result") or {}
        if not result.get("process"):
            errs = payload.get("errors") or result.get("data") or ""
            texto = " ".join(map(str, errs)) if isinstance(errs, list) else str(errs)
            low = texto.lower()
            if "excede" in low:
                raise TamanoExcedido(texto)
            # "No se encontraron registros con los parámetros ingresados" = vacío, no error.
            if "no se encontr" in low and "empresa" not in low and "cat" not in low:
                return []
            raise RuntimeError(f"API process=false: {texto[:300]}")

        d = result.get("data")
        if d is None:
            return []
        if isinstance(d, list):
            return d
        if isinstance(d, dict):
            for k in ("cfdis", "contratos", "unidades", "partida", "partidas",
                      "sucursal", "sucursales", "data", "registros"):
                if isinstance(d.get(k), list):
                    return d[k]

            # ⚠ GENÉRICO (2026-08-26). La lista de llaves de arriba estaba escrita
            # a mano y NO cubría todos los endpoints:
            #   /movimiento_bancario  ->  {"movimientos_bancarios": [...]}
            #   /gasto                ->  {"gastos": [...]}
            # Al no reconocerlas, se devolvía EL SOBRE como si fuera un registro.
            # Resultado: 850 jobs cerrados como "VACIO" trayendo 0 filas de las
            # 177,733 esperadas, y 368 sobres a cuarentena. Ningún dato se perdió
            # (la cuarentena los atrapó) pero el hueco parecía "sin datos".
            #
            # Ahora: cualquier dict que traiga UNA lista es un sobre; se devuelve
            # esa lista. Así funcionan también los endpoints que aún no tocamos.
            # `registros_encontrados` se ignora: es el contador que viaja al lado.
            listas = [v for k, v in d.items()
                      if isinstance(v, list) and k != "registros_encontrados"]
            if len(listas) == 1:
                return listas[0]

            # dict simple = un solo registro (empresa, arrendatario, unidad)
            return [d] if d else []
        return []

    # ---------------------------------------------------------- paginación --
    def paginar(self, endpoint: str, params: dict, page_size: int = 100,
                etiqueta: str = "", verbose: bool = True) -> list[dict]:
        """Pagina hasta agotar. Una página incompleta significa fin del conjunto
        (la API no da un total confiable: `registros_encontrados` son las filas
        de LA PÁGINA)."""
        out: list[dict] = []
        pagina = 1
        while True:
            p = dict(params)
            p["no_registros_x_pagina"] = page_size
            p["no_pagina"] = pagina
            lote = self.desenvolver(self._get(endpoint, p))
            if not lote:
                break
            out.extend(lote)
            if verbose and pagina % 5 == 0:
                print(f"      [{etiqueta}] pág {pagina} · acum {len(out):,}")
            if len(lote) < page_size:
                break
            pagina += 1
            time.sleep(PAUSA)
        return out

    # --------------------------------------------- ventana con troceo auto --
    def rango_adaptativo(self, endpoint: str, params: dict, ini: str, fin: str,
                         page_size: int = 100, etiqueta: str = "",
                         campo_ini: str = "fecha_inicio",
                         campo_fin: str = "fecha_fin") -> list[dict]:
        """Trae [ini, fin]. Si la API dice 'excede', parte la ventana a la mitad
        recursivamente hasta que quepa (o hasta 1 día, donde ya no se puede partir)."""
        p = {**params, campo_ini: ini, campo_fin: fin}
        try:
            return self.paginar(endpoint, p, page_size, f"{etiqueta} {ini}..{fin}")
        except TamanoExcedido:
            if ini == fin:
                print(f"      ! {ini}: el día solo excede el límite — resultado parcial")
                try:
                    return self.paginar(endpoint, p, max(25, page_size // 4), etiqueta)
                except TamanoExcedido:
                    return []
            d0, d1 = date.fromisoformat(ini), date.fromisoformat(fin)
            mid = d0 + (d1 - d0) // 2
            izq = self.rango_adaptativo(endpoint, params, ini, mid.isoformat(),
                                        page_size, etiqueta, campo_ini, campo_fin)
            der = self.rango_adaptativo(endpoint, params,
                                        (mid + timedelta(days=1)).isoformat(), fin,
                                        page_size, etiqueta, campo_ini, campo_fin)
            return izq + der

    # ======================================================================= #
    #  API DE STREAMING (generadores) — la que debe usarse para VOLUMEN
    #
    #  Diferencia con paginar()/consultar_rango(), que devuelven una LISTA:
    #  estos hacen `yield` de cada página. El consumidor procesa y suelta, así
    #  que el pico de RAM es UNA página, no todo el job.
    #
    #      lista:      pico de RAM = tamaño del job   (crece sin techo)
    #      generador:  pico de RAM = 1 página         (techo FIJO)
    #
    #  Medido 2026-08-11: el pull de `contrato` acumulando 50 empresas llegó a
    #  3.1 GB de RAM. Con streaming, el mismo trabajo cabe en decenas de MB.
    # ======================================================================= #

    def iter_paginas(self, endpoint: str, params: dict, page_size: int = 100,
                     etiqueta: str = "") -> Iterator[list[dict]]:
        """Hace yield de CADA PÁGINA. Nunca construye la lista completa.

        ⚠ NO USAR DIRECTAMENTE PARA CARGAS DONDE IMPORTE LA COMPLETITUD.
        Ver `iter_ventana_segura()`: la paginación por offset de INMOGES es
        INESTABLE y esta función puede omitir registros.
        """
        pagina = 1
        while True:
            p = dict(params)
            p["no_registros_x_pagina"] = page_size
            p["no_pagina"] = pagina
            lote = self.desenvolver(self._get(endpoint, p))
            if not lote:
                return
            yield lote
            # Fin del conjunto: SOLO se sigue paginando si la página vino
            # EXACTAMENTE llena.
            #
            # ⚠ El `!=` en vez de `<` es deliberado (bug medido 2026-08-12):
            # `/contrato` IGNORA `no_registros_x_pagina` y `no_pagina` — con
            # page_size=50 devuelve los 1,050 contratos de la empresa, y con 200
            # devuelve los mismos 1,050. Con la condición `<` nunca se cumplía la
            # salida y se pedía la misma página para siempre: BUCLE INFINITO.
            # Si el endpoint devuelve MÁS de lo pedido, es que no pagina -> fin.
            if len(lote) != page_size:
                return
            pagina += 1
            time.sleep(PAUSA)

    # ======================================================================= #
    #  ⚠⚠ LECTURA SEGURA — contra la paginación inestable de INMOGES
    #
    #  PROBLEMA MEDIDO (2026-08-11, EDN 2026-07-01):
    #    308 registros pedidos en 4 páginas de 100
    #      -> 7 registros llegaron DOS VECES
    #      -> 7 registros DISTINTOS no llegaron NUNCA
    #    Confirmado comparando dos corridas del mismo job: la segunda devolvió
    #    359 ids únicos donde la primera había devuelto 366.
    #
    #  CAUSA: la API pagina por OFFSET sin ORDER BY determinista. Entre la
    #  petición de la página N y la N+1 el orden interno cambia, así que hay
    #  filas que caen en dos páginas y filas que se escapan entre ambas.
    #
    #  ESTRATEGIA (en este orden):
    #    1. Intentar que TODA la ventana quepa en UNA SOLA página subiendo el
    #       page_size. Sin segunda página no hay offset -> no hay pérdida.
    #    2. Si no cabe (topa con el límite de 30 s / "tamaño excedido"),
    #       paginar con PASADAS REPETIDAS y unir por id, hasta que dos pasadas
    #       consecutivas no aporten ningún id nuevo (convergencia).
    #    3. Si NADA de lo anterior funciona, PLAN B (`rescate`): pedir la
    #       cabecera sin anidados y traer los hijos por su propio endpoint.
    #    4. Si tampoco: `VentanaIncompleta` -> el job muere como ERROR.
    #       NUNCA se cierra en silencio una ventana que no se pudo garantizar.
    # ======================================================================= #

    def iter_ventana_segura(self, endpoint: str, params: dict, llave: str,
                            page_size: int = 100, max_page_size: int = 500,
                            etiqueta: str = "", max_pasadas: int = 6,
                            rescate: dict | None = None
                            ) -> Iterator[list[dict]]:
        """Trae una ventana GARANTIZANDO completitud. Hace yield de lotes.

        `llave`   campo id del registro (ej. 'id_cfdi'): sirve para de-duplicar
                  y para saber si una pasada aportó algo nuevo.
        `rescate` config del PLAN B (ver `_iter_rescate`). Si es None y la
                  ventana no se puede traer, se lanza `VentanaIncompleta`.
        """
        vistos: set = set()

        # --- 1) ¿cabe todo en una sola página? -----------------------------
        ps = page_size
        lote = []
        while ps <= max_page_size:
            try:
                lote = self.desenvolver(self._get(
                    endpoint, {**params, "no_registros_x_pagina": ps, "no_pagina": 1}))
            except (TamanoExcedido, ErrorTransporte):
                # ⚠ El 504 se trata IGUAL que "tamaño excedido" (2026-08-21).
                # Son el mismo problema visto de dos formas: la respuesta que
                # pedimos es demasiado grande para que INMOGES la arme en 30 s.
                # A veces contesta "excede lo permitido" y a veces simplemente
                # no contesta. Reintentar igual no sirve; hay que pedir menos.
                break                       # ya no se puede crecer más
            if len(lote) != ps:             # != y no <: ver nota en iter_paginas.
                # Menos de lo pedido = está todo.
                # MÁS de lo pedido = el endpoint ignora el page_size (no pagina)
                # y ya devolvió el conjunto completo. En ambos casos: listo.
                if lote:
                    yield lote
                return
            ps *= 2                         # página EXACTAMENTE llena -> hay más
        del lote

        # --- 2) no cabe: pasadas repetidas hasta convergencia --------------
        ps = min(max(ps // 2, 1), max_page_size)
        pasada = encogidas = 0
        while pasada < max_pasadas:
            pasada += 1
            nuevos = 0
            try:
                for lote in self.iter_paginas(endpoint, params, ps, etiqueta):
                    frescos = []
                    for r in lote:
                        k = str(r.get(llave))
                        if k and k != "None" and k not in vistos:
                            vistos.add(k)
                            frescos.append(r)
                    nuevos += len(frescos)
                    if frescos:
                        yield frescos
                    del lote, frescos
            except (TamanoExcedido, ErrorTransporte):
                # (el 504 va por aquí también: ver nota en el paso 1)
                # ⚠ AQUÍ ESTABA EL BUG (medido 2026-08-17). Antes: `max(25, ps//2)`.
                # Ese piso de 25 daba por hecho que SIEMPRE existe un page_size
                # que cabe. No es cierto: hay CFDI cuya sola respuesta ya excede
                # el límite (id_cfdi=472620 tiene 4,980 partidas). Con el piso,
                # las 6 pasadas se gastaban reintentando 25 y la ventana salía
                # con "0 ids" — sin datos y sin error.
                if ps <= 1:
                    break                   # ni UN registro cabe -> al PLAN B
                ps = max(1, ps // 2)
                pasada -= 1                 # encoger no consume una pasada...
                encogidas += 1
                if encogidas > 12:          # ...pero sí tiene tope, por si acaso
                    break
                continue
            if nuevos == 0:                 # una pasada sin nada nuevo = listo
                if pasada > 1:
                    print(f"      [{etiqueta}] convergió en {pasada} pasadas "
                          f"({len(vistos):,} ids únicos)")
                return
            if pasada > 1:
                print(f"      [{etiqueta}] pasada {pasada}: +{nuevos} ids que la "
                      f"pasada anterior había OMITIDO")

        # --- 3) PLAN B: cabecera ligera + hijos por su propio endpoint ------
        if rescate:
            print(f"      [{etiqueta}] ventana indomable -> RESCATE vía "
                  f"/{rescate['endpoint']}")
            antes = len(vistos)
            for lote in self._iter_rescate(endpoint, params, llave, rescate,
                                           etiqueta, max_page_size, vistos):
                yield lote
            print(f"      [{etiqueta}] RESCATE OK: {len(vistos)-antes:,} registros "
                  f"reensamblados ({len(vistos):,} en la ventana)")
            return

        # --- 4) no hay forma: que se vea. -----------------------------------
        raise VentanaIncompleta(
            f"[{etiqueta}] no se pudo garantizar la ventana completa "
            f"({len(vistos):,} ids tras {max_pasadas} pasadas, page_size mínimo {ps}). "
            f"El job se marca ERROR para no dar por buenos datos incompletos.")

    def _iter_rescate(self, endpoint: str, params: dict, llave: str,
                      rescate: dict, etiqueta: str, max_page_size: int,
                      vistos: set) -> Iterator[list[dict]]:
        """PLAN B — separar lo que la API no puede entregar junto.

        DIAGNÓSTICO (2026-08-17, HPI 2026-07-03):
            el día tiene 5 CFDI. Uno de ellos, `id_cfdi=472620`, trae 4,980
            partidas (facturación de estacionamiento: un renglón por ticket).
            Con `incluir_partidas=true` la API responde «el tamaño del
            diccionario excede lo permitido» AUNQUE se pida
            `no_registros_x_pagina=1`. No existe page_size que funcione:
            el problema no es cuántas facturas caben, es que UNA no cabe.

        LA SALIDA: pedir por separado lo que junto no cabe.
            cabeceras -> /cfdi   con incluir_*=false   (pesa ~55x menos)
            partidas  -> /partida?id_cfdi=N            (paginado, lectura segura)
        y reensamblar el registro con la MISMA forma que habría tenido de venir
        completo, para que `entidades.py` lo mapee sin cambios.

        EQUIVALENCIA VERIFICADA (2026-08-17, id_cfdi=472557, 500 partidas):
            35 campos comunes -> valores IDÉNTICOS uno a uno
            /partida trae 5 campos DE MÁS (createdDate, modifiedDate, fecha,
                fecha_timbre, fecha_cancelacion)
            /partida NO trae `dimension_financiera_concepto`, que resultó ser
                "None" en el 100% de una muestra de 500,000 partidas ya
                cargadas -> no se pierde información real.
            Ninguna columna del mapeo de entidades.py queda sin fuente.
        """
        # -- cabeceras: sin anidados SIEMPRE caben ---------------------------
        cabeceras: list[dict] = []
        for lote in self.iter_ventana_segura(endpoint, self._ligeros(params), llave,
                                             page_size=max_page_size,
                                             max_page_size=max_page_size,
                                             etiqueta=f"{etiqueta}/cabeceras"):
            cabeceras.extend(lote)

        base = {k: v for k, v in params.items()
                if k in rescate.get("hereda_params", ("empresa",))}
        param_padre = rescate["param_padre"]        # 'id_cfdi'

        # -- hijos, de uno en uno: el pico de RAM es UN registro -------------
        for cab in cabeceras:
            k = str(cab.get(llave))
            if not k or k == "None" or k in vistos:
                continue
            pid = cab.get(param_padre)
            hijos: list[dict] = []
            for lote in self.iter_ventana_segura(
                    rescate["endpoint"], {**base, param_padre: pid},
                    rescate["llave_hijo"],
                    page_size=rescate.get("page_size", 500),
                    max_page_size=rescate.get("max_page_size", 1000),
                    etiqueta=f"{etiqueta}/{rescate['clave']} {pid}"):
                hijos.extend(lote)
            cab[rescate["clave"]] = hijos
            vistos.add(k)
            yield [cab]
            del hijos

    def iter_rango(self, endpoint: str, params: dict, ini: str, fin: str,
                   page_size: int = 100, etiqueta: str = "",
                   ventana_dias: int = 1, llave: str | None = None,
                   max_page_size: int = 500,
                   rescate: dict | None = None,
                   campo_fechas: tuple[str, str] = ("fecha_inicio", "fecha_fin"),
                   ) -> Iterator[list[dict]]:
        """Streaming sobre [ini, fin] con TROCEO ADAPTATIVO (grueso -> fino).

        ⚠ POR QUÉ ADAPTATIVO (medido 2026-08-12):
        La versión anterior partía SIEMPRE en ventanas de 1 día. Un mes vacío
        costaba 31 llamadas HTTP para traer CERO registros. Con 10,200 jobs de
        mes, eso son ~316,000 llamadas y ~26 HORAS gastadas en nada, porque la
        mayoría de (empresa, mes) del histórico están vacíos: las empresas no
        existían en 2010.

        Ahora: se prueba la ventana COMPLETA de un tirón.
            vacía o cabe en una página -> 1 sola llamada, listo
            no cabe                    -> se parte a la mitad y se repite
        El refinamiento ocurre SOLO donde hay volumen real.

        `campo_fechas` (2026-08-28): con qué par de parámetros se filtra.
            ("fecha_inicio", "fecha_fin")                          -> por FECHA DEL DOCUMENTO (backfill)
            ("fecha_inicio_modificacion","fecha_fin_modificacion") -> por FECHA DE MODIFICACIÓN (sync)
        Toda la maquinaria (sonda barata, troceo adaptativo, rescate, convergencia)
        funciona igual con cualquiera de los dos.
        """
        yield from self._iter_adaptativo(endpoint, params, ini, fin, page_size,
                                         etiqueta, llave, max_page_size,
                                         ventana_dias, rescate, campo_fechas)

    @staticmethod
    def _ligeros(params: dict) -> dict:
        """Los mismos filtros pero SIN los anidados pesados.

        `incluir_partidas`, `incluir_movimiento_bancario`, `incluir_detalle_pago`
        multiplican el peso de la respuesta por ~55×. Para una SONDA (solo
        queremos saber si hay algo y cuánto) no hacen ninguna falta.
        """
        return {k: ("false" if k.startswith("incluir_") else v)
                for k, v in params.items()}

    def _iter_adaptativo(self, endpoint, params, ini, fin, page_size, etiqueta,
                         llave, max_page_size, ventana_dias,
                         rescate=None,
                         campo_fechas=("fecha_inicio", "fecha_fin"),
                         ) -> Iterator[list[dict]]:
        """SONDA BARATA primero, extracción real solo si hay datos.

        Medido 2026-08-12 (EDN, un mes):
            con partidas   : 77.6 MB · 249 s
            sin partidas   :  1.4 MB ·   3.6 s     <- la sonda usa ESTO

        Así, un (empresa, mes) vacío —el caso MAYORITARIO del histórico, porque
        casi ninguna empresa existía en 2010— cuesta UNA llamada de 0.7 s en vez
        de 31 llamadas pesadas.
        """
        _ci, _cf = campo_fechas
        p = {**params, _ci: ini, _cf: fin}

        # --- 1) SONDA: ¿hay algo aquí? (barata, sin anidados) --------------
        try:
            sonda = self.desenvolver(self._get(
                endpoint, {**self._ligeros(p),
                           "no_registros_x_pagina": max_page_size, "no_pagina": 1}))
        except TamanoExcedido:
            sonda = None                    # muchísimo: seguro hay datos
        except ErrorTransporte:
            # ⚠ 504 = INMOGES no alcanzó a construir la respuesta en sus 30 s.
            # NO es un problema de red: es que la ventana pedida es demasiado
            # grande para que su servidor la arme a tiempo. La cura es la misma
            # que para "tamaño excedido": PARTIRLA EN DOS.
            #
            # Medido 2026-08-21: MDI 2022-05, 2022-07 y 2022-08 fallaban los 4
            # intentos con 504 y quedaban en ERROR de forma permanente (1,423
            # facturas sin cargar). Reintentar la MISMA ventana no ayuda nunca:
            # va a tardar lo mismo las cuatro veces.
            if ini == fin:
                raise                       # un solo día: ya no se puede partir
            d0, d1 = date.fromisoformat(ini), date.fromisoformat(fin)
            mid = d0 + (d1 - d0) // 2
            print(f"      ! [{etiqueta}] 504 en {ini}..{fin} -> se parte en dos "
                  f"({ini}..{mid.isoformat()} y {(mid+timedelta(days=1)).isoformat()}..{fin})")
            # ⚠ propagar `campo_fechas` en las recursivas: si no, al partirse la
            # ventana se volvería en silencio al filtro por fecha de documento.
            yield from self._iter_adaptativo(endpoint, params, ini, mid.isoformat(),
                                             page_size, etiqueta, llave,
                                             max_page_size, ventana_dias, rescate,
                                             campo_fechas)
            yield from self._iter_adaptativo(endpoint, params,
                                             (mid + timedelta(days=1)).isoformat(), fin,
                                             page_size, etiqueta, llave,
                                             max_page_size, ventana_dias, rescate,
                                             campo_fechas)
            return

        if sonda is not None and not sonda:
            return                          # VACÍO -> 1 sola llamada, siguiente

        # --- 1-BIS) POCOS registros: traer la ventana ENTERA de un tirón ---
        #
        # ⚠ MEDIDO 2026-08-12 sobre 1,104 jobs reales con datos:
        #   el 69% (758 jobs) tienen <=50 CFDI, y aun así gastaban ~30 llamadas
        #   cada uno (una por día) para traer 7,676 filas en total.
        #   Un mes con UNA factura costaba 30 llamadas.
        #
        # La sonda YA nos dijo cuántos hay. Si son pocos, el mes completo cabe
        # en una sola petición aunque venga con los anidados pesados.
        # Si resulta que no cabe (facturas de estacionamiento con 2,761 partidas),
        # el TamanoExcedido nos manda al troceo normal sin haber perdido nada.
        if sonda is not None and len(sonda) <= UMBRAL_SIN_TROCEAR:
            try:
                lote = self.desenvolver(self._get(
                    endpoint, {**p, "no_registros_x_pagina": max(len(sonda) + 10, 100),
                               "no_pagina": 1}))
                if len(lote) >= len(sonda):      # vino completo
                    if lote:
                        yield lote
                    return
            except TamanoExcedido:
                pass                              # pesa demasiado -> trocear

        # --- 2) muchos datos: extracción real, troceada por ventana_dias ---
        cur = date.fromisoformat(ini)
        tope = date.fromisoformat(fin)
        paso = max(1, ventana_dias)
        while cur <= tope:
            hasta = min(cur + timedelta(days=paso - 1), tope)
            pv = {**params, campo_fechas[0]: cur.isoformat(),
                  campo_fechas[1]: hasta.isoformat()}
            if llave:
                yield from self.iter_ventana_segura(endpoint, pv, llave, page_size,
                                                    max_page_size,
                                                    f"{etiqueta} {cur.isoformat()}",
                                                    rescate=rescate)
            else:
                yield from self.iter_paginas(endpoint, pv, page_size, etiqueta)
            cur = hasta + timedelta(days=1)

    def _iter_ventana(self, endpoint, params, ini, fin, page_size, etiqueta
                      ) -> Iterator[list[dict]]:
        """Una ventana, con troceo adaptativo, en streaming."""
        _ci, _cf = campo_fechas
        p = {**params, _ci: ini, _cf: fin}
        try:
            for lote in self.iter_paginas(endpoint, p, page_size, f"{etiqueta} {ini}"):
                yield lote
            return
        except TamanoExcedido:
            pass  # se parte abajo
        if ini == fin:
            print(f"      ! {ini}: el día solo excede el límite; se baja el page_size")
            try:
                for lote in self.iter_paginas(endpoint, p, max(10, page_size // 4),
                                              etiqueta):
                    yield lote
            except TamanoExcedido:
                print(f"      !! {ini}: NO SE PUDO TRAER — queda registrado como ERROR")
            return
        d0, d1 = date.fromisoformat(ini), date.fromisoformat(fin)
        mid = d0 + (d1 - d0) // 2
        yield from self._iter_ventana(endpoint, params, ini, mid.isoformat(),
                                      page_size, etiqueta)
        yield from self._iter_ventana(endpoint, params,
                                      (mid + timedelta(days=1)).isoformat(), fin,
                                      page_size, etiqueta)

    # ---- API de LISTA (se conserva para catálogos chicos y compatibilidad) --
    def consultar_rango(self, endpoint: str, params: dict, ini: str, fin: str,
                        page_size: int = 100, etiqueta: str = "",
                        ventana_dias: int = 1) -> list[dict]:
        """Recorre [ini, fin] TROCEANDO PROACTIVAMENTE en ventanas de N días.

        ⚠ ESTA ES LA RECETA QUE EVITA EL 504. Medido 2026-08-11:

            page_size=1000, ventana=1 mes  ->  504 Gateway Timeout / TamanoExcedido
            page_size=100,  ventana=1 día  ->  6/6 OK, 0 errores

        Por qué: la paginación de INMOGES es por OFFSET y materializa internamente
        `offset + limit` registros. La página 3 con tamaño 250 obliga al servidor
        a materializar 750 -> "el tamaño del diccionario excede lo permitido".
        Si cada ventana cabe en 1 sola página, nunca hay offset profundo.

        No es lentitud: es el MISMO trabajo repartido en requests que sí caben.
        """
        out: list[dict] = []
        cur = date.fromisoformat(ini)
        tope = date.fromisoformat(fin)
        while cur <= tope:
            hasta = min(cur + timedelta(days=ventana_dias - 1), tope)
            out.extend(self.rango_adaptativo(endpoint, params, cur.isoformat(),
                                             hasta.isoformat(), page_size, etiqueta))
            cur = hasta + timedelta(days=1)
        return out

    # ------------------------------------------------------------ utilidad --
    def empresas_alias(self) -> list[str]:
        """Los alias de empresa. Son el eje del backfill: /cfdi, /pago,
        /movimiento_bancario y /gasto exigen `empresa` con UN SOLO valor."""
        regs = self.paginar("empresa", {}, page_size=1000, etiqueta="empresa",
                            verbose=False)
        alias = [str(r.get("alias")).strip() for r in regs
                 if r.get("alias") not in (None, "", "None")]
        return sorted(set(alias))


# ------------------------------------------------------------- conversores --
def txt(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return None if s in ("", "None", "null") else s


def num(v: Any) -> float | None:
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError, AttributeError):
        return None


#  Valores que INMOGES usa como "fecha vacía" y que Snowflake NO puede castear.
#  "0000-00-00" es el nulo de MySQL: tumbó un job entero con
#  «Failed to cast variant value "0000-00-00" to DATE» (medido 2026-08-12).
_FECHAS_NULAS = ("0000-00-00", "0000-00-00 00:00:00", "00/00/0000",
                 "1900-01-01", "0001-01-01")


def fecha(v: Any) -> str | None:
    """Fecha como texto ISO; Snowflake la castea al insertar en DATE/TIMESTAMP.

    Si viene basura -> None. El literal original SIEMPRE queda preservado dentro
    de RAW_JSON, así que no se pierde información: solo se evita que un valor
    inválido tumbe la carga de todo un mes.
    """
    s = txt(v)
    if not s:
        return None
    s = s.replace("T", " ").strip()
    if s in _FECHAS_NULAS or s.startswith("0000"):
        return None
    return s[:19] if len(s) >= 19 else s[:10]


def entero(v: Any) -> int | None:
    n = num(v)
    return int(n) if n is not None else None
