# -*- coding: utf-8 -*-
"""
A stand-in for the ERP's REST API, backed by generated records.

It reads the same declarative catalog the production engine reads
(`src/entidades.py`) and emits a record shaped like the real endpoint would
return: the API field names come straight from each entity's `mapa`, so the
generator never drifts from the catalog. Add an entity to the catalog and this
file produces data for it with no changes.

Everything here is invented. No real company, tenant, property or amount.
"""
from __future__ import annotations

import random
from datetime import date, timedelta

SEED = 1701

# --- The fictional group -------------------------------------------------- #
EMPRESAS = [
    ("AAI", "ALMENA ADMINISTRACION INMOBILIARIA SA DE CV", "AAI180312K72"),
    ("ADP", "ALMENA DESARROLLOS Y PROYECTOS SA DE CV", "ADP150822J41"),
    ("ILP", "INMOBILIARIA LOMA PRIETA SA DE CV", "ILP090714R58"),
    ("EDP", "EDIFICIOS DE LA PRADERA SA DE CV", "EDP120405M90"),
    ("BSM", "BIENES SAN MARCOS SA DE CV", "BSM170228T13"),
    ("OCI", "OPERADORA REGIONAL DE INMUEBLES SA DE CV", "ORI200119B65"),
]

PLAZAS = [
    ("Paseo Altamira", "Queretaro", "QRO", "76000"),
    ("Plaza Bernal", "Aguascalientes", "AGS", "20000"),
    ("Paseo San Isidro", "Leon", "GTO", "37000"),
    ("Paseo La Rioja", "Merida", "YUC", "97000"),
    ("Paseo Los Alamos", "Puebla", "PUE", "72000"),
    ("Punto Alameda", "Culiacan", "SIN", "80000"),
]

ARRENDATARIOS = [
    "Cafe Bonanza", "Farmacia Lucero", "Banco del Istmo", "Gimnasio Vertice",
    "Telecom Sierra", "Burger Nogal", "Optica Clara", "Tiendas Almendro",
    "Sushi Kaiso", "Papeleria Nova", "Academia Lincoln", "Viajes Orion",
]

ESTATUS = ["Vigente", "Vencido", "Cancelado", "En renovacion"]


def _rfc(rng: random.Random, prefijo: str) -> str:
    """Valid shape, invented value."""
    d = date(2005, 1, 1) + timedelta(days=rng.randint(0, 7000))
    homo = "".join(rng.choice("0123456789ABCDEFGHJKLMNPQRSTUVWXYZ") for _ in range(3))
    return f"{prefijo}{d:%y%m%d}{homo}"


def _valor(campo: str, i: int, rng: random.Random):
    """One generated value for an API field name, by what the name means."""
    c = campo.lower()
    emp = EMPRESAS[i % len(EMPRESAS)]
    plaza = PLAZAS[i % len(PLAZAS)]

    if c.startswith("id_") or c == "id":
        return f"{c.replace('id_', '').upper()[:3]}-{i:05d}"
    if "rfc" in c:
        return _rfc(rng, emp[0] + "X")
    if "razon_social" in c or "nombre_comercial" in c:
        return emp[1]
    if "alias" in c:
        return emp[0]
    if "nombre" in c or "arrendatario" in c:
        return rng.choice(ARRENDATARIOS)
    if "inmueble" in c or "plaza" in c:
        return plaza[0]
    if "municipio" in c or "ciudad" in c:
        return plaza[1]
    if "estado" in c and "estatus" not in c:
        return plaza[2]
    if "codigo_postal" in c or c.endswith("_cp"):
        return plaza[3]
    if "calle" in c or "direccion" in c or "domicilio" in c:
        return f"Av. Principal {rng.randint(100, 4999)}"
    if "colonia" in c:
        return rng.choice(["Centro", "Del Valle", "Las Lomas", "San Angel"])
    if "pais" in c:
        return "Mexico"
    if "correo" in c or "email" in c:
        return f"contacto{i:03d}@almena.mx"
    if "telefono" in c or "tel" == c:
        return f"55{rng.randint(10000000, 99999999)}"
    if "estatus" in c or "status" in c:
        return rng.choice(ESTATUS)
    if "moneda" in c:
        return rng.choice(["MXN", "MXN", "MXN", "USD"])
    if "fecha" in c or c.endswith("date") or "vigencia" in c:
        return (date(2024, 1, 1) + timedelta(days=rng.randint(0, 900))).isoformat()
    if any(k in c for k in ("monto", "importe", "total", "renta", "subtotal",
                            "saldo", "precio", "iva")):
        return round(rng.uniform(5_000, 480_000), 2)
    if any(k in c for k in ("metros", "superficie", "area")):
        return round(rng.uniform(25, 2_400), 2)
    if c.startswith("num") or c.startswith("no_") or "folio" in c:
        return rng.randint(1, 99_999)
    if c.startswith("es_") or c.startswith("activo") or "flag" in c:
        return rng.choice([True, False])
    if "tipo" in c or "clave" in c or "regimen" in c or "giro" in c:
        return rng.choice(["A", "B", "C", "D"])
    return f"{campo}_{i:04d}"


def generar(nombre: str, ent: dict, n: int, semilla: int = SEED) -> list[dict]:
    """`n` records for one entity, shaped by its `mapa` in the catalog."""
    rng = random.Random(f"{semilla}-{nombre}")
    campos = list(ent["mapa"].values())
    registros = []
    for i in range(n):
        reg = {campo: _valor(campo, i, rng) for campo in campos}
        # The engine excludes these from ROW_HASH on purpose: the real ERP
        # re-stamps them in bulk. The demo reproduces that so the third pass
        # can prove the hash ignores them.
        reg["createdDate"] = "2026-01-15T09:00:00"
        reg["modifiedDate"] = "2026-09-01T03:00:00"
        registros.append(reg)
    return registros


def resellar_fechas(registros: list[dict]) -> list[dict]:
    """What the real ERP does nightly: touch every modifiedDate, change nothing."""
    sellados = []
    for r in registros:
        r = dict(r)
        r["modifiedDate"] = "2026-09-22T03:00:00"
        sellados.append(r)
    return sellados


def mutar(registros: list[dict], ent: dict, cuantos: int,
          semilla: int = SEED) -> tuple[list[dict], list[int]]:
    """Change a real business field on `cuantos` records. Returns the indices."""
    rng = random.Random(semilla + 7)
    campos = [c for c in ent["mapa"].values()
              if "id" not in c.lower() and "fecha" not in c.lower()]
    if not campos:
        return registros, []
    indices = sorted(rng.sample(range(len(registros)),
                                min(cuantos, len(registros))))
    mutados = [dict(r) for r in registros]
    for i in indices:
        campo = rng.choice(campos)
        mutados[i][campo] = f"CAMBIADO_{rng.randint(1000, 9999)}"
    return mutados, indices


class FakeClient:
    """Same surface the engine uses, without the network."""

    def __init__(self, registros_por_entidad: dict[str, list[dict]]):
        self._datos = registros_por_entidad
        self.llamadas = 0

    def paginar(self, endpoint: str, params: dict, page_size: int = 100,
                **kwargs) -> list[dict]:
        self.llamadas += 1
        return list(self._datos.get(endpoint, []))

    def iter_paginas(self, endpoint: str, params: dict, page_size: int = 100,
                     **kwargs):
        registros = self._datos.get(endpoint, [])
        for inicio in range(0, len(registros), page_size):
            self.llamadas += 1
            yield registros[inicio:inicio + page_size]

    def empresas_alias(self) -> list[str]:
        return [e[0] for e in EMPRESAS]
