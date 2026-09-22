-- ============================================================================
--  01_schema_y_grants.sql — SOLICITUD DE ESQUEMA EXCLUSIVO PARA INMOGES
--  Proyecto: cargadirecta_snowflake (ingesta masiva INMOGES -> Snowflake)
--  Fecha: 2026-08-06 · Solicita: José Rangel · Ejecuta: responsable de permisos
--
--  QUÉ PEDIMOS: un esquema nuevo y exclusivo donde vivirá TODA la data ingestada
--  de INMOGES (capa cruda RAW_* + vistas STG_* + tabla de control SYNC_CONTROL).
--  El rol de ingesta es ROLE_ANALYTICS (usuario SVC_ANALYTICS, key-pair).
--
--  NOTA: si prefieren otro nombre de esquema, cualquier nombre sirve — solo
--  avisar cuál quedó para parametrizar la tubería (src/sf.py usa una variable).
-- ============================================================================

USE DATABASE DB_ANALYTICS;

-- 1) El esquema exclusivo -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS DB_ANALYTICS.SCH_INMOGES
  COMMENT = 'Capa de ingesta exclusiva de INMOGES (API). RAW_* = crudo por entidad; STG_* = vistas; SYNC_CONTROL = bitacora de corridas. Proyecto cargadirecta_snowflake (Jose Rangel).';

-- 2) Permisos para el rol de INGESTA (crea tablas/vistas y escribe) -----------
GRANT USAGE ON SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ROLE_ANALYTICS;
GRANT CREATE TABLE  ON SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ROLE_ANALYTICS;
GRANT CREATE VIEW   ON SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ROLE_ANALYTICS;
GRANT CREATE STAGE  ON SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ROLE_ANALYTICS;  -- write_pandas usa stage temporal
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON FUTURE TABLES IN SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ROLE_ANALYTICS;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ROLE_ANALYTICS;

-- 3) Permisos de LECTURA para consumidores (dashboard / analítica) ------------
--    (mismo patrón que ya se usa con ALMENA_DASH en SCH_PLD)
GRANT USAGE ON SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ALMENA_DASH;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ALMENA_DASH;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA DB_ANALYTICS.SCH_INMOGES TO ROLE ALMENA_DASH;

-- 4) Verificación (la corre quien ejecuta, y luego nosotros desde la tubería) --
-- SHOW GRANTS ON SCHEMA DB_ANALYTICS.SCH_INMOGES;
-- USE ROLE ROLE_ANALYTICS;
-- CREATE TABLE DB_ANALYTICS.SCH_INMOGES._PING (X INT);  -- debe funcionar
-- DROP TABLE DB_ANALYTICS.SCH_INMOGES._PING;
