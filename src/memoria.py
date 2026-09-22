# -*- coding: utf-8 -*-
"""
memoria.py — Instrumentación de RAM del pipeline
================================================
Proyecto: cargadirecta_snowflake

Por qué existe: el 2026-08-11 descubrimos que el pull de `contrato` había
llegado a 3.1 GB de RAM... y nos enteramos de casualidad, mirando `tasklist`.
Un pipeline de datos serio no adivina su consumo: lo MIDE y lo REGISTRA.

Este módulo da:
  * `mb()`             cuánta RAM usa el proceso ahora mismo
  * `Vigilante`        mide el PICO durante un bloque y avisa/aborta si se pasa
  * `LimiteMemoria`    excepción cuando se cruza el umbral

Funciona sin dependencias externas (usa el propio proceso de Windows vía
ctypes si psutil no está instalado), para no añadir requisitos al entorno.
"""
from __future__ import annotations

import gc
import os
import threading
import time


class LimiteMemoria(RuntimeError):
    """El proceso superó el techo de RAM configurado para un job."""


# --------------------------------------------------------------------------- #
#  Lectura de memoria — psutil si está; si no, API nativa de Windows
# --------------------------------------------------------------------------- #
try:
    import psutil
    _PROC = psutil.Process(os.getpid())

    def mb() -> float:
        """RAM residente del proceso, en MB."""
        return _PROC.memory_info().rss / 1_048_576

except ImportError:                                        # pragma: no cover
    # Respaldo nativo de Windows, sin dependencias.
    # ⚠ OJO (bug real encontrado 2026-08-11): SIN declarar argtypes/restype,
    # ctypes trata el HANDLE como int de 32 bits y en Windows 64-bit lo TRUNCA
    # -> la llamada falla y devuelve 0 MB en silencio. Un medidor que miente es
    # peor que no tener medidor. Por eso las firmas van declaradas explícitas.
    import ctypes
    from ctypes import wintypes

    class _PMC(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t)]

    _K32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _K32.GetCurrentProcess.restype = wintypes.HANDLE
    _K32.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE,
                                             ctypes.POINTER(_PMC),
                                             wintypes.DWORD]
    _K32.K32GetProcessMemoryInfo.restype = wintypes.BOOL

    def mb() -> float:
        c = _PMC()
        c.cb = ctypes.sizeof(c)
        if not _K32.K32GetProcessMemoryInfo(_K32.GetCurrentProcess(),
                                            ctypes.byref(c), c.cb):
            raise OSError(f"No se pudo leer la memoria del proceso "
                          f"(error {ctypes.get_last_error()})")
        return c.WorkingSetSize / 1_048_576


# --------------------------------------------------------------------------- #
#  Vigilante de pico
# --------------------------------------------------------------------------- #
class Vigilante:
    """Mide el PICO de RAM durante un bloque `with` y opcionalmente aborta.

        with Vigilante("cfdi/EDN 2026-07", limite_mb=2000) as v:
            ...trabajo pesado...
        print(v.pico_mb)

    `limite_mb` protege la máquina: si el proceso lo cruza, lanza LimiteMemoria
    para que el JOB muera solo y quede registrado como ERROR reintentable —
    en vez de que el sistema operativo mate todo el backfill de 20 horas.
    """

    def __init__(self, etiqueta: str = "", limite_mb: float | None = None,
                 intervalo: float = 0.5):
        self.etiqueta = etiqueta
        self.limite_mb = limite_mb
        self.intervalo = intervalo
        self.inicio_mb = 0.0
        self.pico_mb = 0.0
        self.excedido = False
        self._parar = threading.Event()
        self._hilo: threading.Thread | None = None

    def _muestrear(self):
        while not self._parar.is_set():
            actual = mb()
            if actual > self.pico_mb:
                self.pico_mb = actual
            if self.limite_mb and actual > self.limite_mb:
                self.excedido = True
            self._parar.wait(self.intervalo)

    def __enter__(self):
        gc.collect()
        self.inicio_mb = self.pico_mb = mb()
        self._hilo = threading.Thread(target=self._muestrear, daemon=True)
        self._hilo.start()
        return self

    def __exit__(self, *exc):
        self._parar.set()
        if self._hilo:
            self._hilo.join(timeout=2)
        gc.collect()
        self.final_mb = mb()
        return False

    @property
    def delta_mb(self) -> float:
        """Cuánto CRECIÓ la memoria durante el bloque (lo que costó el job)."""
        return self.pico_mb - self.inicio_mb

    def resumen(self) -> str:
        aviso = "  ⚠ LIMITE EXCEDIDO" if self.excedido else ""
        return (f"RAM: inicio {self.inicio_mb:,.0f} MB · pico {self.pico_mb:,.0f} MB "
                f"· delta +{self.delta_mb:,.0f} MB{aviso}")

    def verificar(self):
        """Lanza si se cruzó el umbral. Llamar dentro del bucle de trabajo."""
        if self.excedido:
            raise LimiteMemoria(
                f"{self.etiqueta}: la RAM superó el límite de {self.limite_mb:,.0f} MB "
                f"(pico {self.pico_mb:,.0f} MB). Job abortado para proteger la máquina; "
                f"queda PENDIENTE y se puede reintentar con lote más chico.")


def formato(v: float) -> str:
    return f"{v/1024:,.1f} GB" if v >= 1024 else f"{v:,.0f} MB"
