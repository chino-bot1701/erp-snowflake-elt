# -*- coding: utf-8 -*-
"""
A SQLite stand-in for the Snowflake target, with the same merge semantics.

Production uses `src/sf.merge_upsert()`: a dynamic Snowflake `MERGE` against a
transient stage table, with VARIANT handling. That SQL cannot run on SQLite, so
the demo reimplements the *decision rule* — which is the part worth showing —
and reuses the real `ROW_HASH` from `src/sf.py` to make it.

    key not present        -> INSERT
    key present, same hash -> leave the row alone   (no write at all)
    key present, diff hash -> UPDATE that row only

That rule is the whole idempotency story. The Snowflake implementation of it
stays visible in `src/sf.py`.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field


@dataclass
class Resultado:
    insertadas: int = 0
    actualizadas: int = 0
    sin_cambio: int = 0
    llaves_tocadas: list[str] = field(default_factory=list)

    @property
    def escrituras(self) -> int:
        return self.insertadas + self.actualizadas

    def __str__(self) -> str:
        return (f"{self.insertadas:>5} insert  "
                f"{self.actualizadas:>5} update  "
                f"{self.sin_cambio:>5} unchanged")


class Warehouse:
    def __init__(self, ruta: str = ":memory:"):
        self.conn = sqlite3.connect(ruta)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    # ---------------------------------------------------------------- #
    def crear_tabla(self, tabla: str, columnas: list[str],
                    llaves: list[str]) -> None:
        cols = ", ".join(f'"{c}" TEXT' for c in columnas)
        pk = ", ".join(f'"{k}"' for k in llaves)
        self.conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{tabla}" ({cols}, PRIMARY KEY ({pk}))')
        self.conn.commit()

    def merge_upsert(self, tabla: str, filas: list[dict],
                     llaves: list[str]) -> Resultado:
        """Same contract as sf.merge_upsert, decided by ROW_HASH."""
        if not filas:
            return Resultado()

        columnas = list(filas[0].keys())
        self.crear_tabla(tabla, columnas, llaves)

        clave_sql = " AND ".join(f'"{k}" = ?' for k in llaves)
        cur = self.conn.cursor()
        res = Resultado()

        for fila in filas:
            valores_llave = [str(fila[k]) for k in llaves]
            actual = cur.execute(
                f'SELECT ROW_HASH FROM "{tabla}" WHERE {clave_sql}',
                valores_llave).fetchone()

            if actual is None:
                cur.execute(
                    f'INSERT INTO "{tabla}" ({", ".join(chr(34) + c + chr(34) for c in columnas)}) '
                    f'VALUES ({", ".join("?" for _ in columnas)})',
                    [_texto(fila[c]) for c in columnas])
                res.insertadas += 1
                res.llaves_tocadas.append("|".join(valores_llave))

            elif actual["ROW_HASH"] != fila["ROW_HASH"]:
                set_cols = [c for c in columnas if c not in llaves]
                cur.execute(
                    f'UPDATE "{tabla}" SET '
                    f'{", ".join(chr(34) + c + chr(34) + " = ?" for c in set_cols)} '
                    f'WHERE {clave_sql}',
                    [_texto(fila[c]) for c in set_cols] + valores_llave)
                res.actualizadas += 1
                res.llaves_tocadas.append("|".join(valores_llave))

            else:
                # Same fingerprint: the row is not touched at all.
                res.sin_cambio += 1

        self.conn.commit()
        return res

    # ---------------------------------------------------------------- #
    def contar(self, tabla: str) -> int:
        try:
            return self.conn.execute(
                f'SELECT COUNT(*) FROM "{tabla}"').fetchone()[0]
        except sqlite3.OperationalError:
            return 0

    def tablas(self) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]


def _texto(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)
