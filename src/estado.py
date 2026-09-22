# -*- coding: utf-8 -*-
"""
estado.py — SEMAFORO de la corrida: ¿está trabajando o está caída?
==================================================================
Proyecto: cargadirecta_snowflake

POR QUÉ EXISTE (2026-08-24)
---------------------------
La corrida murió a las 11:19 y nadie se enteró hasta las 15:30. Cuatro horas
perdidas. Antes se había perdido un fin de semana entero por lo mismo: no hay
forma de saber de un vistazo si el proceso está vivo.

Esto lo resuelve. Se ejecuta con DOBLE CLIC en `VER_ESTADO.bat` y contesta una
sola pregunta con letras grandes: **¿hay que hacer algo o no?**

El criterio es la FRESCURA DEL LOG, no que el proceso exista: un proceso vivo
pero congelado es igual de inútil que uno muerto. Si el log no crece en
`MINUTOS_ALERTA`, es problema.
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass

import os
import subprocess
import sys
from datetime import datetime

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])

LOG = os.path.join(os.environ.get("LOCALAPPDATA", "."), "erp_maraton.log")
PID_FILE = os.path.join(os.environ.get("LOCALAPPDATA", "."), "erp_maraton.pid")

#  Un job normal tarda de 5 s a 35 min (los peores de HPI, con rescate de
#  partidas). 45 min sin escribir NADA en el log ya es anormal.
MINUTOS_ALERTA = 45


def _proceso_vivo(pid: int) -> bool:
    try:
        r = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                           capture_output=True, text=True, timeout=20)
        return str(pid) in r.stdout
    except Exception:
        return False


def _pid_guardado() -> int | None:
    try:
        with open(PID_FILE) as f:
            return int(f.read().strip())
    except Exception:
        return None


def main():
    print()
    print("=" * 70)
    print("   ESTADO DE LA CARGA INMOGES -> SNOWFLAKE")
    print(f"   {datetime.now():%d/%m/%Y %H:%M:%S}")
    print("=" * 70)

    # ---------------------------------------------------------- ¿está viva? --
    pid = _pid_guardado()
    vivo = _proceso_vivo(pid) if pid else False

    if os.path.exists(LOG):
        mod = datetime.fromtimestamp(os.path.getmtime(LOG))
        mins = (datetime.now() - mod).total_seconds() / 60
    else:
        mod, mins = None, 9999

    print()
    if pid is None:
        print("   proceso            : (sin archivo de PID; se juzga por el log)")
    else:
        print(f"   proceso            : {'VIVO (PID ' + str(pid) + ')' if vivo else 'NO SE ENCUENTRA (PID ' + str(pid) + ')'}")
    print(f"   ultima actividad   : {mod:%d/%m %H:%M:%S} ({mins:,.0f} min)"
          if mod else "   ultima actividad   : (sin log)")

    # ------------------------------------------------------------ veredicto --
    #
    # ⚠ MANDA EL LOG, NO EL PID (corregido 2026-08-24).
    # La primera versión daba "DETENIDA" solo porque no existía el archivo de
    # PID —que las corridas viejas no escriben— aunque el log se estuviera
    # actualizando en ese mismo instante. Un falso "está caída" es tan dañino
    # como un falso "todo bien": ambos hacen que el operador actúe mal.
    #
    # La verdad es: si el log CRECE, la carga está trabajando. Punto.
    trabajando = mins < MINUTOS_ALERTA and (vivo or pid is None)

    print()
    if trabajando:
        print("   " + "#" * 62)
        print("   #                                                            #")
        print("   #        TODO BIEN - la carga esta trabajando                #")
        print("   #        No hay que hacer nada.                              #")
        print("   #                                                            #")
        print("   " + "#" * 62)
    elif vivo or pid is None:
        print("   " + "!" * 62)
        print("   !  EL PROCESO EXISTE PERO NO ESCRIBE HACE RATO               !")
        print(f"   !  {mins:,.0f} minutos sin actividad (lo normal es < {MINUTOS_ALERTA}).       !")
        print("   !  Espera 15 min y vuelve a mirar. Si sigue igual:           !")
        print("   !     doble clic en LANZAR_MARATON.bat                       !")
        print("   " + "!" * 62)
    else:
        print("   " + "!" * 62)
        print("   !                                                            !")
        print("   !        LA CARGA ESTA DETENIDA                              !")
        print("   !                                                            !")
        print("   !        QUE HACER:  doble clic en LANZAR_MARATON.bat        !")
        print("   !        (no se pierde nada: retoma donde iba)               !")
        print("   !                                                            !")
        print("   " + "!" * 62)

    # ------------------------------------------------- avance en Snowflake --
    print()
    print("-" * 70)
    print("   AVANCE")
    print("-" * 70)
    try:
        import sf
        conn = sf.conectar()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT ENTIDAD,
                   COUNT_IF(ESTADO IN ('COMPLETADO','VACIO')) HECHOS,
                   COUNT(*) TOTAL,
                   COUNT_IF(ESTADO='ERROR') ERRORES
            FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL
            GROUP BY 1 ORDER BY 3 DESC
        """)
        print(f"   {'entidad':<22} {'avance':>18} {'errores':>9}")
        for e, h, t, err in cur.fetchall():
            print(f"   {e:<22} {h:>7,}/{t:<7,} {h/t*100:>4.0f}% {err:>9,}")

        # BUG 27 (2026-09-03): el esquema iba escrito a mano y quedo apuntando al
        # RESPALDO tras la migracion -> VER_ESTADO.bat mostraba cifras congeladas.
        cur.execute(f"""
            SELECT TABLE_NAME, ROW_COUNT
            FROM DB_ANALYTICS.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{sf.SCHEMA}' AND TABLE_NAME LIKE 'ERP_RAW%'
              AND TABLE_TYPE='BASE TABLE' AND ROW_COUNT > 0
            ORDER BY ROW_COUNT DESC LIMIT 6
        """)
        print()
        print("   tablas mas grandes:")
        for n, rc in cur.fetchall():
            print(f"     {n:<40} {rc or 0:>13,}")

        cur.execute(f"""SELECT COUNT(*) FROM {sf.ESQUEMA}.RAW_ERP_RECHAZOS""")
        q = cur.fetchone()[0]
        print()
        print(f"   en cuarentena: {q:,}   {'(bien: nada se descarto)' if q == 0 else '<-- REVISAR'}")
        conn.close()
    except Exception as e:
        print(f"   (no se pudo consultar Snowflake: {type(e).__name__})")

    # ------------------------------------------------- ultimas lineas del log
    print()
    print("-" * 70)
    print("   ULTIMO QUE HIZO")
    print("-" * 70)
    try:
        with open(LOG, encoding="utf-8", errors="replace") as f:
            lineas = f.readlines()
        for l in lineas[-6:]:
            print("   " + l.rstrip()[:100])
    except Exception:
        print("   (sin log)")
    print()


if __name__ == "__main__":
    main()
