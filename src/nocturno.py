# -*- coding: utf-8 -*-
"""
nocturno.py — CORRIDA DESATENDIDA por fases, en orden de riesgo creciente
=========================================================================
Proyecto: cargadirecta_snowflake · creado 2026-08-11 18:45

Pensado para dejarlo trabajando sin nadie enfrente.

FILOSOFÍA: primero lo que VALIDA, después lo que TARDA.
Si la fase de validación falla, ABORTA — mejor perder 20 minutos que
descubrir en la mañana que 14 horas corrieron sobre código roto.

FASES
  1. Recargar 6 jobs conocidos (EDN 2026 ene-jun) con lectura segura.
     Es el canario: si esto falla, algo se rompió en el refactor.
  2. Cargar `contrato` (troceo por empresa + streaming). Nunca se ha corrido.
     Si falla, se registra y se CONTINÚA (no bloquea el histórico).
  3. Generar la cola completa de cfdi (2010 -> hoy).
  4. Backfill de cfdi hasta la hora límite, AÑOS RECIENTES PRIMERO.

TODO queda registrado en ERP_SYNC_CONTROL y es REANUDABLE: si algo se
corta, relanzar retoma donde iba sin repetir ni duplicar.

USO
    py src/nocturno.py --hasta-hora 08:30
    py src/nocturno.py --hasta-hora 08:30 --hilos 1
"""
from __future__ import annotations

import sys as _sys
for _s in (_sys.stdout, _sys.stderr):
    try: _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception: pass


import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

sys.path.insert(0, __file__.rsplit("\\", 1)[0].rsplit("/", 1)[0])

import backfill
import erp_client as bc
import memoria
import sf
from entidades import ENTIDADES


def ahora() -> str:
    return datetime.now().strftime("%H:%M:%S")


#  ⚠ El log va a DISCO LOCAL, no a la carpeta del proyecto.
#  Motivo (2026-08-11): el log del primer nocturno se escribió en `r:\Mi unidad\`
#  = Google Drive. Al apagarse el equipo de golpe, los buffers del archivo
#  sincronizado se perdieron: quedaron 15 líneas de cientos. Los datos en
#  Snowflake sobrevivieron (son transaccionales); la bitácora no.
LOG_LOCAL = os.path.join(os.environ.get("LOCALAPPDATA", "."), "erp_nocturno.log")


def log(msg: str = ""):
    linea = f"[{ahora()}] {msg}"
    print(linea, flush=True)
    try:
        with open(LOG_LOCAL, "a", encoding="utf-8") as f:
            f.write(linea + "\n")
            f.flush()
            os.fsync(f.fileno())        # fuerza escritura fisica: sobrevive apagones
    except Exception:
        pass


def banner(t: str):
    log()
    log("=" * 68)
    log(f"  {t}")
    log("=" * 68)


def jobs_canario(n: int = 4) -> list[tuple]:
    """Jobs VÍRGENES (INTENTOS=0) de la entidad que toca, para probar el motor.

    ⚠ POR QUÉ ASÍ (lección cara, 2026-08-21):
    El canario pedía `backfill.pendientes("cfdi", limite=4)`. Cuando CFDI terminó,
    los ÚNICOS jobs de cfdi que quedaban en la cola eran los 3 de MDI que ya
    sabíamos rotos (504 del servidor). El canario los probó, fallaron los 3,
    y como la regla era "abortar si ninguno pasa", **abortó la corrida entera
    9 minutos después de arrancar y se perdió el fin de semana completo**.

    Dos defectos, los dos corregidos aquí:
      1. estaba clavado en `cfdi` aunque la cadena ya fuera por otra entidad
      2. tomaba jobs con intentos fallidos previos — una muestra formada SOLO
         por casos malos conocidos no prueba nada del motor

    Ahora se piden jobs con INTENTOS=0 (nunca intentados) recorriendo la cadena
    en orden. Si no hay ninguno, no hay canario que valga: se salta.
    """
    conn = sf.conectar()
    try:
        cur = conn.cursor()
        for entidad, _ in CADENA:
            cur.execute(f"""
                SELECT ENTIDAD, EMPRESA, TO_CHAR(VENTANA_INI,'YYYY-MM-DD'),
                       TO_CHAR(VENTANA_FIN,'YYYY-MM-DD')
                FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL
                WHERE ENTIDAD = %s AND ESTADO = 'PENDIENTE' AND INTENTOS = 0
                ORDER BY VENTANA_INI DESC, EMPRESA
                LIMIT {int(n)}
            """, (entidad,))
            filas = cur.fetchall()
            if filas:
                return filas
        return []
    finally:
        conn.close()


#  Fallos que NO son culpa del motor: el servidor de INMOGES no alcanzó a responder.
#  Un 504 significa "INMOGES tardó demasiado", no "nuestro código está roto".
_FALLA_AJENA = ("504", "502", "503", "Gateway", "Timeout", "ConnectionError",
                "ErrorTransporte")


def fase1_canario() -> bool:
    """Comprueba que el motor está sano antes de comprometer días de corrida.

    Solo aborta si el motor está REALMENTE roto. Un 504 de INMOGES no lo está.
    """
    banner("FASE 1 — CANARIO: probar el motor con jobs nunca intentados")
    pend = jobs_canario(4)
    if not pend:
        log("  no hay jobs vírgenes que usar de canario; se salta y se continúa")
        return True
    log(f"  {len(pend)} jobs de '{pend[0][0]}' · se espera 0 errores")
    ok = ajenos = rotos = filas = 0
    for j in pend:
        r = backfill.correr_job(j)
        if r["estado"] in ("COMPLETADO", "VACIO"):
            ok += 1
            filas += r["filas"]
        elif any(t.lower() in str(r.get("mensaje", "")).lower() or
                 t.lower() in str(r.get("estado", "")).lower()
                 for t in _FALLA_AJENA):
            ajenos += 1
        else:
            rotos += 1
        log(f"    {j[0]}/{j[1]} {j[2]} -> {r['estado']} · {r['filas']:,} filas · "
            f"{r['seg']:.0f}s · RAM {r.get('ram', 0):,.0f} MB")
    log(f"  resultado: {ok} ok · {ajenos} fallo ajeno (servidor) · "
        f"{rotos} sospechosos · {filas:,} filas")

    if ok == 0 and rotos == len(pend):
        log("  !! EL CANARIO FALLÓ POR COMPLETO y ninguno fue culpa del servidor")
        log("  !! -> se aborta: parece que el motor sí está roto.")
        return False
    if ok == 0:
        log("  ⚠ ningún job pasó, pero los fallos apuntan al servidor de INMOGES, "
            "no al motor. SE CONTINÚA (esos jobs quedan reintentables).")
    elif ok < len(pend):
        log(f"  canario con tropiezos pero {ok} OK: el motor responde, se continúa.")
    else:
        log("  canario OK: el motor está sano.")
    return True


def fase2_contrato(saltar=True):
    """Carga `contrato`. Nunca se ha corrido con streaming: si truena, se
    registra y se sigue — no debe bloquear el histórico, que es lo valioso."""
    banner("FASE 2 — catálogo `contrato`")
    if saltar:
        log("  ya cargado (4,982/4,982) — se salta")
        return
    conn = sf.conectar()
    try:
        import extraer
        cli = bc.InmogesClient()
        n, ins, upd = extraer.extraer_catalogo(cli, conn, "contrato",
                                               ENTIDADES["contrato"],
                                               limite_mb=6144)
        conn.commit()
        log(f"  contrato: {n:,} filas · +{ins:,} nuevos · ~{upd:,} cambiados")
        log(f"  esperado ~4,982 -> {'OK' if n >= 4900 else 'REVISAR: quedó corto'}")
    except Exception as e:
        log(f"  !! contrato falló: {type(e).__name__}: {str(e)[:160]}")
        log("  se CONTINÚA con el histórico (contrato se retoma mañana)")
    finally:
        conn.close()


def fase3_cola(desde: str, hasta: str):
    banner("FASE 3 — generar la cola de jobs del histórico")
    for ent in ("cfdi",):
        try:
            backfill.generar(ent, desde, hasta)
        except Exception as e:
            log(f"  !! no se pudo generar la cola de {ent}: {e}")


#  Orden en que se atacan las entidades cuando hay una ventana larga (fin de
#  semana). En cuanto una se termina, la corrida sigue SOLA con la siguiente.
#  Está ordenado por valor: primero el núcleo transaccional, al final los
#  catálogos chicos que se cargan en minutos.
CADENA = [
    ("cfdi", True),                  # (entidad, requiere_cola_por_ventana)
    ("pago", True),
    ("movimiento_bancario", True),
    ("gasto", True),
    ("pago_gasto", True),
    ("proveedor", False),            # catálogos: pull completo, sin cola
    ("direccion_fiscal", False),
    ("cuenta_bancaria", False),
    ("cuenta_contable", False),
    ("centro_costos", False),
    ("concepto", False),
    ("producto", False),
    ("orden_trabajo", False),
    ("actualizacion", False),
    ("pld", False),                  # avisos PLD (doc 11 §1) — agregado 27-ago
]


def maraton(limite: datetime, hilos: int, desde: str, hasta: str):
    """Corrida LARGA: encadena entidades hasta agotar la lista o la hora límite.

    Pensada para ventanas de fin de semana. Si CFDI termina el sábado, la
    máquina no se queda parada: sigue con pago, luego movimiento_bancario, y
    al final los catálogos menores.

    Todo sigue siendo reanudable: cada entidad lleva su propia cola en
    ERP_SYNC_CONTROL y el MERGE por ROW_HASH nunca duplica.
    """
    for entidad, con_cola in CADENA:
        if datetime.now() >= limite:
            log("  hora límite alcanzada; la cadena se detiene aquí.")
            return
        if entidad not in ENTIDADES:
            continue

        banner(f"CADENA · {entidad}")
        try:
            if con_cola:
                backfill.generar(entidad, desde, hasta)
                fase4_backfill(limite, hilos, entidad)
            else:
                # catálogo: pull completo, una sola pasada
                import extraer
                conn = sf.conectar()
                try:
                    cli = bc.InmogesClient()
                    n, ins, upd = extraer.extraer_catalogo(
                        cli, conn, entidad, ENTIDADES[entidad], limite_mb=6144)
                    conn.commit()
                    log(f"  {entidad}: {n:,} filas · +{ins:,} nuevos · ~{upd:,} cambiados")
                finally:
                    conn.close()
        except Exception as e:
            log(f"  !! {entidad} falló: {type(e).__name__}: {str(e)[:160]}")
            log("  se CONTINÚA con la siguiente entidad de la cadena")

    fase5_verificar(limite, hilos)


def fase5_verificar(limite: datetime, hilos: int, rondas: int = 2):
    """CIERRE DEL CICLO: comprobar contra la API que no falta nada, y si falta,
    volver a cargarlo.

    POR QUÉ (2026-08-17): terminar la cola NO es lo mismo que tener los datos.
    El bug de convergencia cerró 1,128 días como COMPLETADO estando vacíos.
    `verificar.py` le pregunta a la API cuántos registros debería haber en cada
    ventana ya cargada y lo compara contra Snowflake; lo que no cuadra vuelve a
    PENDIENTE y se recarga aquí mismo.

    Es un lazo cerrado —cargar, verificar, reparar, recargar— con tope de
    `rondas` para que no se quede dando vueltas si algo no se puede traer.

    ⚠ FIX 2026-08-27: antes decía `vf.verificar("cfdi")` con la entidad ESCRITA A
    MANO, así que `pago` y `movimiento_bancario` — 357,609 filas entre las dos —
    NUNCA se contrastaron contra la API. Ahora recorre TODAS las entidades con
    cola. El bug 13 enseñó que terminar la cola NO es lo mismo que tener los datos.
    """
    import verificar as vf

    # las entidades con cola por ventana, en el mismo orden de la CADENA
    entidades = [e for e, con_cola in CADENA if con_cola and e in ENTIDADES]

    for entidad in entidades:
        for ronda in range(1, rondas + 1):
            if datetime.now() >= limite:
                log("  hora límite: la verificación queda pendiente.")
                return
            banner(f"FASE 5 — verificación {entidad} (ronda {ronda}/{rondas})")
            try:
                vf.verificar(entidad, reencolar=True)
            except Exception as e:
                log(f"  !! verificación de {entidad} falló: "
                    f"{type(e).__name__}: {str(e)[:160]}")
                break          # se sigue con la SIGUIENTE entidad, no se aborta
            # ¿quedó algo re-encolado? Si sí, se recarga y se vuelve a verificar.
            conn = sf.conectar()
            cur = conn.cursor()
            cur.execute(f"""SELECT COUNT(*) FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL
                            WHERE ENTIDAD=%s AND ESTADO IN ('PENDIENTE','ERROR')
                              AND INTENTOS < 3""", (entidad,))
            quedan = cur.fetchone()[0]
            conn.close()
            if not quedan:
                log(f"  ✓ {entidad}: la API y Snowflake cuadran ventana por ventana.")
                break
            log(f"  {entidad}: {quedan:,} ventanas con hueco -> se recargan")
            fase4_backfill(limite, hilos, entidad)


def fase4_backfill(limite: datetime, hilos: int, entidad: str = "cfdi"):
    """Consume la cola hasta la hora límite. AÑOS RECIENTES PRIMERO: si no
    termina, lo que queda cargado es lo más útil (PLD y validación usan lo
    reciente)."""
    banner(f"backfill de {entidad} hasta {limite:%d-%b %H:%M}")
    conn = sf.conectar()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT ENTIDAD, EMPRESA, TO_CHAR(VENTANA_INI,'YYYY-MM-DD'),
               TO_CHAR(VENTANA_FIN,'YYYY-MM-DD')
        FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL
        WHERE ENTIDAD=%s AND ESTADO IN ('PENDIENTE','EN_CURSO','ERROR')
          AND INTENTOS < 3
        ORDER BY VENTANA_INI DESC, EMPRESA       -- lo reciente primero
    """, (entidad,))
    jobs = cur.fetchall()
    cur.close()
    conn.close()
    log(f"  {len(jobs):,} jobs en cola · {hilos} hilo(s)")
    if not jobs:
        return

    hechos = filas = errores = vacios = 0
    t0 = time.perf_counter()
    for j in jobs:
        if datetime.now() >= limite:
            log(f"  hora límite alcanzada; se detiene ordenadamente.")
            break
        r = backfill.correr_job(j)
        hechos += 1
        filas += r["filas"]
        errores += (r["estado"] == "ERROR")
        vacios += (r["estado"] == "VACIO")
        if hechos % 5 == 0 or r["filas"]:
            trans = time.perf_counter() - t0
            ritmo = hechos / trans * 3600 if trans else 0
            restan = (limite - datetime.now()).total_seconds() / 3600
            log(f"  [{hechos:>5}/{len(jobs)}] {j[1]:<6} {j[2]} {r['estado']:<11} "
                f"{r['filas']:>6,} filas · {r['seg']:>5.0f}s · RAM {r.get('ram',0):>5,.0f} MB "
                f"· {ritmo:.0f} jobs/h · quedan {restan:.1f} h")
    log(f"\n  FASE 4 cerrada: {hechos:,} jobs · {filas:,} filas · "
        f"{vacios} vacíos · {errores} errores · {(time.perf_counter()-t0)/3600:.1f} h")


def resumen_final():
    banner("RESUMEN PARA LA MAÑANA")
    conn = sf.conectar()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {sf.ESQUEMA}.VW_ERP_SYNC_RESUMEN")
    cols = [c[0] for c in cur.description]
    for f in cur.fetchall():
        log("  " + " · ".join(f"{c}={v}" for c, v in zip(cols, f)))
    # BUG 27 (2026-09-03): aqui decia TABLE_SCHEMA='SCH_CORE' escrito a mano.
    # Tras migrar a SCH_PLD el resumen final seguia contando el esquema de
    # RESPALDO (congelado el 28-ago), asi que reportaba 30,117,034 filas mientras
    # el esquema vivo ya llevaba ~30.21M. La carga estaba BIEN; mentia el reporte.
    # Misma familia que los bugs 24/25/26: una medicion que miente.
    cur.execute(f"""
        SELECT TABLE_NAME, ROW_COUNT
        FROM DB_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='{sf.SCHEMA}' AND TABLE_NAME LIKE 'ERP%'
          AND TABLE_TYPE='BASE TABLE' AND ROW_COUNT > 0
        ORDER BY ROW_COUNT DESC
    """)
    log()
    tot = 0
    for n, rc in cur.fetchall():
        tot += rc or 0
        log(f"    {n:<42} {rc or 0:>12,}")
    log(f"    {'TOTAL':<42} {tot:>12,}")
    cur.execute(f"""SELECT COUNT(*) FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL
                    WHERE ESTADO='ERROR'""")
    log(f"\n  jobs en ERROR (reintentables): {cur.fetchone()[0]}")
    cur.execute(f"""SELECT ROUND(MAX(RAM_PICO_MB),0), ROUND(AVG(RAM_PICO_MB),0)
                    FROM {sf.ESQUEMA}.ERP_SYNC_CONTROL WHERE RAM_PICO_MB IS NOT NULL""")
    mx, av = cur.fetchone()
    log(f"  RAM por job: pico máximo {mx or 0:,.0f} MB · promedio {av or 0:,.0f} MB")
    cur.close()
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hasta-hora", default="08:30", help="HH:MM en que debe parar")
    ap.add_argument("--hasta-fecha", help="YYYY-MM-DD (para corridas de varios dias)")
    #  ⚠ PREFERIR --horas SOBRE --hasta-fecha (lección del 2026-08-17):
    #  run_maraton.bat traía la fecha tope escrita a mano. Al llegar ese día, la
    #  Tarea Programada arrancaba y se moría en el acto porque el límite YA había
    #  pasado — sin avisar de nada. Un tope RELATIVO no puede caducar.
    ap.add_argument("--horas", type=float,
                    help="horas de corrida desde AHORA (tope relativo, no caduca)")
    ap.add_argument("--maraton", action="store_true",
                    help="encadena TODAS las entidades, no solo cfdi")
    ap.add_argument("--hilos", type=int, default=1)
    ap.add_argument("--desde", default="2010-01-01")
    ap.add_argument("--hasta", default=datetime.now().strftime("%Y-%m-%d"))
    a = ap.parse_args()

    h, m = map(int, a.hasta_hora.split(":"))
    if a.horas:
        limite = datetime.now() + timedelta(hours=a.horas)
    elif a.hasta_fecha:
        d = datetime.strptime(a.hasta_fecha, "%Y-%m-%d")
        limite = d.replace(hour=h, minute=m)
    else:
        limite = datetime.now().replace(hour=h, minute=m, second=0, microsecond=0)
        if limite <= datetime.now():
            limite += timedelta(days=1)

    #  ⚠ ARCHIVO DE PID (2026-08-24). Sin esto no hay forma FIABLE de matar la
    #  maratón sin matar de paso el Streamlit y el pipeline PLD de José, que
    #  también son python.exe. `taskkill /IM python.exe` los mata a todos, y el
    #  filtro por título de ventana ya falló una vez.
    #  Lo usa LANZAR_MARATON.bat para reiniciar limpio con un doble clic.
    pid_file = os.path.join(os.environ.get("LOCALAPPDATA", "."), "erp_maraton.pid")
    try:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))
    except Exception:
        pass

    banner("CORRIDA NOCTURNA — cargadirecta_snowflake")
    log(f"  inicio  : {datetime.now():%Y-%m-%d %H:%M:%S}")
    log(f"  PID     : {os.getpid()}  (anotado en erp_maraton.pid)")
    log(f"  límite  : {limite:%Y-%m-%d %H:%M:%S} "
        f"({(limite-datetime.now()).total_seconds()/3600:.1f} horas)")
    log(f"  RAM     : {memoria.mb():,.0f} MB al arrancar")
    log(f"  destino : {sf.ESQUEMA}")

    try:
        if not fase1_canario():
            resumen_final()
            return
        fase2_contrato()
        if a.maraton:
            maraton(limite, a.hilos, a.desde, a.hasta)
        else:
            fase3_cola(a.desde, a.hasta)
            fase4_backfill(limite, a.hilos)
    except KeyboardInterrupt:
        log("\n  interrumpido a mano — todo lo hecho está guardado y es reanudable")
    except Exception:
        log("\n  !! ERROR NO ESPERADO:")
        traceback.print_exc()
    finally:
        resumen_final()
        log("\n  Fin. Para retomar:  py src/backfill.py --run --hilos 1")


if __name__ == "__main__":
    main()
