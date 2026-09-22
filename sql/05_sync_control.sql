-- ============================================================================
--  05_sync_control.sql — BITÁCORA DE JOBS (hace el backfill REANUDABLE)
--  Proyecto: cargadirecta_snowflake · 2026-08-11
--  Destino:  DB_ANALYTICS.SCH_CORE  (rol ROLE_ANALYTICS)
--
--  UN JOB = (ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN)
--  Necesario porque /cfdi, /pago, /movimiento_bancario y /gasto exigen `empresa`
--  con UN SOLO valor -> 50 empresas x ~17 años = producto cartesiano de jobs.
--
--  Ciclo:  PENDIENTE -> EN_CURSO -> COMPLETADO | VACIO | ERROR
--  Reanudar = volver a correr solo los PENDIENTE / EN_CURSO / ERROR.
-- ============================================================================

USE DATABASE DB_ANALYTICS;
USE SCHEMA SCH_CORE;

CREATE TABLE IF NOT EXISTS ERP_SYNC_CONTROL (
    -- identidad del job -------------------------------------------------------
    ENTIDAD         VARCHAR(60)   NOT NULL,   -- 'cfdi', 'pago', 'contrato'...
    EMPRESA         VARCHAR(60),              -- alias de empresa; NULL en catálogos globales
    VENTANA_INI     DATE,                     -- NULL en catálogos (pull completo)
    VENTANA_FIN     DATE,
    GRANULARIDAD    VARCHAR(10),              -- 'FULL' | 'ANIO' | 'MES' | 'DIA'

    -- estado ------------------------------------------------------------------
    ESTADO          VARCHAR(15)   NOT NULL DEFAULT 'PENDIENTE',
                                              -- PENDIENTE|EN_CURSO|COMPLETADO|VACIO|ERROR
    FILAS           NUMBER(18,0)  DEFAULT 0,  -- filas devueltas por la API
    FILAS_NUEVAS    NUMBER(18,0)  DEFAULT 0,  -- insertadas por el MERGE
    FILAS_CAMBIADAS NUMBER(18,0)  DEFAULT 0,  -- actualizadas por el MERGE (hash distinto)
    PAGINAS         NUMBER(9,0)   DEFAULT 0,
    INTENTOS        NUMBER(4,0)   DEFAULT 0,
    MENSAJE         VARCHAR(4000),            -- traza del error, si lo hubo

    -- trazabilidad ------------------------------------------------------------
    CORRIDA_ID      VARCHAR(40),              -- agrupa todos los jobs de una ejecución
    TS_INICIO       TIMESTAMP_NTZ,
    TS_FIN          TIMESTAMP_NTZ,
    DURACION_SEG    NUMBER(12,2),
    LOAD_TS         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Bitacora de jobs de ingesta INMOGES. Un job = (entidad, empresa, ventana). Hace el backfill reanudable y audita la cobertura. Proyecto cargadirecta_snowflake.';


-- ---------------------------------------------------------------------------
--  Vistas de operación (para no escribir el mismo SQL cada vez)
-- ---------------------------------------------------------------------------

-- ¿Qué falta por correr?  (esto es lo que consume backfill.py --run)
CREATE OR REPLACE VIEW VW_ERP_SYNC_PENDIENTE AS
SELECT ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN, GRANULARIDAD,
       ESTADO, INTENTOS, MENSAJE, TS_FIN
FROM   ERP_SYNC_CONTROL
WHERE  ESTADO IN ('PENDIENTE', 'EN_CURSO', 'ERROR')
ORDER BY ENTIDAD, EMPRESA, VENTANA_INI;

-- Resumen por entidad: ¿cómo va la cobertura?
CREATE OR REPLACE VIEW VW_ERP_SYNC_RESUMEN AS
SELECT ENTIDAD,
       COUNT(*)                                              AS JOBS,
       COUNT_IF(ESTADO = 'COMPLETADO')                       AS COMPLETADOS,
       COUNT_IF(ESTADO = 'VACIO')                            AS VACIOS,
       COUNT_IF(ESTADO = 'ERROR')                            AS ERRORES,
       COUNT_IF(ESTADO IN ('PENDIENTE','EN_CURSO'))          AS PENDIENTES,
       SUM(FILAS)                                            AS FILAS_TOTAL,
       SUM(FILAS_NUEVAS)                                     AS FILAS_NUEVAS,
       SUM(FILAS_CAMBIADAS)                                  AS FILAS_CAMBIADAS,
       ROUND(SUM(DURACION_SEG)/60, 1)                        AS MINUTOS,
       MIN(TS_INICIO)                                        AS DESDE,
       MAX(TS_FIN)                                           AS HASTA
FROM   ERP_SYNC_CONTROL
GROUP BY ENTIDAD
ORDER BY ENTIDAD;

-- Los jobs que fallaron, con su error (para diagnosticar sin abrir logs)
CREATE OR REPLACE VIEW VW_ERP_SYNC_ERRORES AS
SELECT ENTIDAD, EMPRESA, VENTANA_INI, VENTANA_FIN, INTENTOS,
       LEFT(MENSAJE, 200) AS ERROR, TS_FIN
FROM   ERP_SYNC_CONTROL
WHERE  ESTADO = 'ERROR'
ORDER BY INTENTOS DESC, ENTIDAD, EMPRESA;
