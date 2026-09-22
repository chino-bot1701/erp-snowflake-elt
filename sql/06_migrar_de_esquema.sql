-- ============================================================================
--  06_migrar_a_sch_almena.sql — MOVER LAS TABLAS AL ESQUEMA FINAL
--  Proyecto: cargadirecta_snowflake · 2026-08-11
--
--  SITUACION: construimos en SCH_CORE porque ahi tenemos .key + permisos.
--             El destino FINAL que quiere Jose es SCH_PLD.
--
--  BUENA NOTICIA: en Snowflake, mover tablas entre esquemas de la MISMA base de
--  datos es INSTANTANEO y NO cuesta almacenamiento extra — aunque sean 600,000
--  filas. No hay que "volver a cargar" nada, ni exportar, ni pasar por CSV.
--
--  ZERO-COPY CLONE: el clon comparte las mismas micro-particiones que el
--  original. No se copian bytes. Solo cuando una de las dos tablas cambia se
--  empieza a pagar por lo que difiere. Por eso clonar 241,855 CFDI tarda
--  SEGUNDOS y cuesta ~0 MB.
--
--  NO EJECUTAR TODAVIA. Este archivo es para el dia en que exista la credencial
--  (o el grant) sobre SCH_PLD.
-- ============================================================================

USE DATABASE DB_ANALYTICS;

-- ---------------------------------------------------------------------------
--  PRE-REQUISITO: el rol necesita poder crear en SCH_PLD
--  (esto lo ejecuta quien administra permisos, UNA sola vez)
-- ---------------------------------------------------------------------------
-- GRANT USAGE        ON SCHEMA DB_ANALYTICS.SCH_PLD TO ROLE ROLE_ANALYTICS;
-- GRANT CREATE TABLE ON SCHEMA DB_ANALYTICS.SCH_PLD TO ROLE ROLE_ANALYTICS;
-- GRANT CREATE VIEW  ON SCHEMA DB_ANALYTICS.SCH_PLD TO ROLE ROLE_ANALYTICS;


-- ===========================================================================
--  OPCION A (RECOMENDADA) — CLONE: copia instantanea, el origen SIGUE VIVO
--  Permite validar en SCH_PLD antes de soltar nada. Reversible.
-- ===========================================================================

-- control
CREATE TABLE IF NOT EXISTS SCH_PLD.ERP_SYNC_CONTROL
    CLONE SCH_CORE.ERP_SYNC_CONTROL;

-- catalogo
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_EMPRESA               CLONE SCH_CORE.RAW_ERP_EMPRESA;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_PROPIETARIO           CLONE SCH_CORE.RAW_ERP_PROPIETARIO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_INMUEBLE              CLONE SCH_CORE.RAW_ERP_INMUEBLE;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_UNIDAD                CLONE SCH_CORE.RAW_ERP_UNIDAD;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_CONTRATO              CLONE SCH_CORE.RAW_ERP_CONTRATO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_ARRENDATARIO          CLONE SCH_CORE.RAW_ERP_ARRENDATARIO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_SUCURSAL_ARRENDATARIO CLONE SCH_CORE.RAW_ERP_SUCURSAL_ARRENDATARIO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_PROVEEDOR             CLONE SCH_CORE.RAW_ERP_PROVEEDOR;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_DIRECCION_FISCAL      CLONE SCH_CORE.RAW_ERP_DIRECCION_FISCAL;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_CUENTA_BANCARIA       CLONE SCH_CORE.RAW_ERP_CUENTA_BANCARIA;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_CUENTA_CONTABLE       CLONE SCH_CORE.RAW_ERP_CUENTA_CONTABLE;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_CENTRO_COSTOS         CLONE SCH_CORE.RAW_ERP_CENTRO_COSTOS;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_CONCEPTO              CLONE SCH_CORE.RAW_ERP_CONCEPTO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_PRODUCTO              CLONE SCH_CORE.RAW_ERP_PRODUCTO;

-- transaccional (aqui esta el volumen; el clone sigue siendo instantaneo)
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_CFDI                  CLONE SCH_CORE.RAW_ERP_CFDI;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_CFDI_PARTIDA          CLONE SCH_CORE.RAW_ERP_CFDI_PARTIDA;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_PAGO                  CLONE SCH_CORE.RAW_ERP_PAGO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_PAGO_ABONO            CLONE SCH_CORE.RAW_ERP_PAGO_ABONO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_PAGO_DOC_RELACIONADO  CLONE SCH_CORE.RAW_ERP_PAGO_DOC_RELACIONADO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_MOVIMIENTO_BANCARIO   CLONE SCH_CORE.RAW_ERP_MOVIMIENTO_BANCARIO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_GASTO                 CLONE SCH_CORE.RAW_ERP_GASTO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_PAGO_GASTO            CLONE SCH_CORE.RAW_ERP_PAGO_GASTO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_ORDEN_TRABAJO         CLONE SCH_CORE.RAW_ERP_ORDEN_TRABAJO;
CREATE TABLE IF NOT EXISTS SCH_PLD.RAW_ERP_ACTUALIZACION         CLONE SCH_CORE.RAW_ERP_ACTUALIZACION;


-- ---------------------------------------------------------------------------
--  VERIFICACION: los conteos deben ser IDENTICOS a los del origen
-- ---------------------------------------------------------------------------
SELECT 'CFDI'        AS TABLA,
       (SELECT COUNT(*) FROM SCH_CORE.RAW_ERP_CFDI) AS ORIGEN,
       (SELECT COUNT(*) FROM SCH_PLD.RAW_ERP_CFDI)      AS DESTINO
UNION ALL
SELECT 'PAGO',
       (SELECT COUNT(*) FROM SCH_CORE.RAW_ERP_PAGO),
       (SELECT COUNT(*) FROM SCH_PLD.RAW_ERP_PAGO)
UNION ALL
SELECT 'CONTRATO',
       (SELECT COUNT(*) FROM SCH_CORE.RAW_ERP_CONTRATO),
       (SELECT COUNT(*) FROM SCH_PLD.RAW_ERP_CONTRATO);


-- ===========================================================================
--  OPCION B — RENAME: MUEVE la tabla (no deja copia). Tambien instantaneo.
--  Usar solo cuando ya se valido con la Opcion A y se quiere una sola copia.
--  OJO: rompe cualquier consulta que apunte al nombre viejo.
-- ===========================================================================
-- ALTER TABLE SCH_CORE.RAW_ERP_CFDI
--     RENAME TO SCH_PLD.RAW_ERP_CFDI;


-- ===========================================================================
--  OPCION C — dejar los datos donde estan y exponer VISTAS en SCH_PLD.
--  Cero duplicacion, una sola fuente de verdad, y los consumidores leen desde
--  SCH_PLD sin enterarse. Es lo mas limpio si nadie EXIGE tablas fisicas.
-- ===========================================================================
-- CREATE OR REPLACE VIEW SCH_PLD.RAW_ERP_CFDI AS
--     SELECT * FROM SCH_CORE.RAW_ERP_CFDI;


-- ---------------------------------------------------------------------------
--  DESPUES DE MIGRAR: apuntar la tuberia al esquema nuevo
--  En src/sf.py cambiar una sola variable:  ESQUEMA = "DB_ANALYTICS.SCH_PLD"
--  (por eso el esquema esta parametrizado y no hardcodeado en cada script)
-- ---------------------------------------------------------------------------
