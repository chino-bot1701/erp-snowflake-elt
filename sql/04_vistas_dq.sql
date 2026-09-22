-- ===========================================================================
--  04_vistas_dq.sql — VISTAS DE CALIDAD Y DE ARCHIVOS
--  Proyecto: cargadirecta_snowflake                    (paso 3 del roadmap)
--  Creado 2026-09-08
--
--  PARA QUE
--  --------
--  Que Jose pueda contestar solo "esta bien mi informacion?" sin pedirle a
--  nadie que corra un script. Se consultan como cualquier tabla.
--
--  ⚠ REGLA: estas vistas NO corrigen ni maquillan nada. Solo MIDEN.
--     Si un numero sale feo, el numero se queda feo. Una vista que embellece
--     es una medicion que miente (ver bugs 24 y 27 del proyecto).
-- ===========================================================================

-- ---------------------------------------------------------------------------
--  1. ARCHIVOS DE CADA FACTURA  (PDF / XML / QR)
--
--  HALLAZGO 2026-09-08: la API de /cfdi SI devuelve las URLs de los archivos,
--  en los campos `pdf`, `xml` y `qr`. Nunca les habiamos hecho caso porque no
--  tenian columna propia, pero SIEMPRE estuvieron guardadas dentro de RAW_JSON.
--  Probado: la URL del PDF responde HTTP 200 y abre en el navegador.
--
--  ⚠ CUIDADO AL CONTARLAS — es la misma trampa que con `folio_fiscal`:
--    INMOGES no manda NULL cuando no hay archivo, manda **el texto "None"**. Un
--    `COUNT(RAW_JSON:pdf)` cuenta ese texto como si fuera un dato y da 100%.
--    Hay que preguntar por `ILIKE 'http%'`. Medido de verdad:
--
--        217,558 de 245,623  (88.6%)  tienen URL real
--         22,970             ( 9.4%)  traen el texto "None"
--
--    Y de las que NO tienen URL:
--        14,947 "No aplica" + 9,356 "Sin timbrar"  -> logico, no hay PDF
--         3,728 **TIMBRADAS sin PDF**              -> eso si es anomalia de INMOGES
--
--    Por eso esta vista devuelve NULL en vez del texto "None": el literal
--    original sigue intacto en RAW_JSON, aqui solo se limpia para poder usarlo.
--
--  ⚠ Las URLs traen ESPACIOS en el nombre del archivo. Al pegarlas en un
--    navegador funcionan, pero si se piden por codigo hay que codificarlas
--    (espacio -> %20) o algunas librerias fallan.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW DB_ANALYTICS.SCH_PLD.VW_ERP_CFDI_ARCHIVOS
COMMENT = 'URLs de PDF, XML y QR de cada CFDI. Salen de RAW_JSON (campos pdf/xml/qr de la API).'
AS
SELECT
    ID_CFDI,
    EMPRESA,
    FOLIO_FISCAL,
    SERIE,
    FOLIO,
    FECHA,
    RAZON_RECEPTOR,
    RFC_RECEPTOR,
    TOTAL,
    MONEDA,
    ESTATUS_TIMBRE,
    IFF(RAW_JSON:pdf::VARCHAR ILIKE 'http%', RAW_JSON:pdf::VARCHAR, NULL) AS URL_PDF,
    IFF(RAW_JSON:xml::VARCHAR ILIKE 'http%', RAW_JSON:xml::VARCHAR, NULL) AS URL_XML,
    IFF(RAW_JSON:qr::VARCHAR  ILIKE 'http%', RAW_JSON:qr::VARCHAR,  NULL) AS URL_QR
FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_CFDI;


-- ---------------------------------------------------------------------------
--  2. CALIDAD DE CFDI, POR EMPRESA Y AÑO
--
--  ⚠ SOBRE LA COLUMNA `DESCUADRE_IMPORTE`:
--    La formula ingenua  subtotal + iva = total  falla en 3,736 facturas, y
--    NO porque el dato este mal. Medido en la factura 186752:
--
--        subtotal   187,210.21
--        iva         29,953.63
--        retenidos  -38,690.17     <- vive en RAW_JSON:impuestos_retenidos
--        --------------------
--        total      178,473.67     <- CUADRA AL CENTAVO
--
--    Por eso aqui se usan los impuestos del JSON y no solo las columnas.
--    La leccion: cuando un chequeo acusa a los datos, primero hay que
--    sospechar del chequeo.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW DB_ANALYTICS.SCH_PLD.DQ_ERP_CFDI
COMMENT = 'Calidad de CFDI por empresa y anio. Solo mide, no corrige.'
AS
SELECT
    EMPRESA,
    ANIO,
    COUNT(*)                                                   AS FILAS,
    COUNT(DISTINCT ID_CFDI)                                    AS LLAVES_UNICAS,
    COUNT(*) - COUNT(DISTINCT ID_CFDI)                         AS DUPLICADOS,
    COUNT_IF(FOLIO_FISCAL IS NULL)                             AS SIN_FOLIO_FISCAL,
    COUNT_IF(FECHA IS NULL)                                    AS SIN_FECHA,
    COUNT_IF(TOTAL IS NULL)                                    AS SIN_TOTAL,
    COUNT_IF(NOT (RAW_JSON:pdf::VARCHAR ILIKE 'http%'))         AS SIN_URL_PDF,
    --  Una factura timbrada por el SAT DEBERIA tener su PDF. Si no lo tiene,
    --  es una anomalia de INMOGES, no nuestra. Medido: 3,728 en total.
    COUNT_IF(ESTATUS_TIMBRE = 'Timbrada'
             AND NOT (RAW_JSON:pdf::VARCHAR ILIKE 'http%'))     AS TIMBRADA_SIN_PDF,
    COUNT_IF(NUM_PARTIDAS = 0)                                 AS SIN_PARTIDAS,
    COUNT_IF(
        ABS( COALESCE(SUBTOTAL, 0)
           - COALESCE(DESCUENTO, 0)
           + COALESCE(TRY_TO_DOUBLE(RAW_JSON:impuestos_trasladados::VARCHAR), 0)
           + COALESCE(TRY_TO_DOUBLE(RAW_JSON:impuestos_retenidos::VARCHAR), 0)
           - COALESCE(TOTAL, 0) ) > 1
    )                                                          AS DESCUADRE_IMPORTE,
    MIN(FECHA)                                                 AS PRIMERA_FACTURA,
    MAX(FECHA)                                                 AS ULTIMA_FACTURA,
    MAX(LOAD_TS)                                               AS ULTIMA_ACTUALIZACION
FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_CFDI
GROUP BY EMPRESA, ANIO;


-- ---------------------------------------------------------------------------
--  3. SEMAFORO: una sola fila por tabla, para mirar de un vistazo
--
--  ⚠ SOBRE `FILAS_CON_SELLO` — HAY QUE LEERLA CON CONTEXTO (bug 28):
--    Hasta el 2026-09-08, `LOAD_TS` estaba VACIO en las 30M de filas. La DDL lo
--    declaraba con DEFAULT CURRENT_TIMESTAMP(), pero el MERGE insertaba
--    explicitamente el NULL que traia la stage, y **un NULL explicito le gana al
--    DEFAULT**. Ya esta arreglado en `sf.merge_upsert`.
--
--    Por eso NO se rellenaron las filas viejas a posteriori: ponerles la fecha
--    de hoy seria inventar que cambiaron hoy. Se lee asi:
--
--        LOAD_TS en NULL  ->  esa fila NO ha cambiado desde el 2026-09-08
--        LOAD_TS con dato ->  esa es la ultima vez que la fila cambio
--
--    O sea que FILAS_CON_SELLO va a ir creciendo sola conforme INMOGES mueva cosas.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW DB_ANALYTICS.SCH_PLD.DQ_ERP_RESUMEN
COMMENT = 'Semaforo general por tabla. LOAD_TS en NULL = la fila no ha cambiado desde el fix del 2026-09-08 (bug 28).'
AS
SELECT 'CFDI' AS TABLA, COUNT(*) AS FILAS,
       COUNT(*) - COUNT(DISTINCT ID_CFDI) AS DUPLICADOS,
       COUNT(LOAD_TS) AS FILAS_CON_SELLO,
       MAX(LOAD_TS) AS ULTIMO_CAMBIO
  FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_CFDI
UNION ALL
SELECT 'CFDI_PARTIDA', COUNT(*), 0, COUNT(LOAD_TS), MAX(LOAD_TS)
  FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_CFDI_PARTIDA
UNION ALL
SELECT 'PAGO', COUNT(*), 0, COUNT(LOAD_TS), MAX(LOAD_TS)
  FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_PAGO
UNION ALL
SELECT 'MOVIMIENTO_BANCARIO', COUNT(*), 0, COUNT(LOAD_TS), MAX(LOAD_TS)
  FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_MOVIMIENTO_BANCARIO
UNION ALL
SELECT 'CONTRATO', COUNT(*), 0, COUNT(LOAD_TS), MAX(LOAD_TS)
  FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_CONTRATO
UNION ALL
SELECT 'ARRENDATARIO', COUNT(*), 0, COUNT(LOAD_TS), MAX(LOAD_TS)
  FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_ARRENDATARIO
UNION ALL
SELECT 'UNIDAD', COUNT(*), 0, COUNT(LOAD_TS), MAX(LOAD_TS)
  FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_UNIDAD
UNION ALL
SELECT 'INMUEBLE', COUNT(*), 0, COUNT(LOAD_TS), MAX(LOAD_TS)
  FROM DB_ANALYTICS.SCH_PLD.RAW_ERP_INMUEBLE;
