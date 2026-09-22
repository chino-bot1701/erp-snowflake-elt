-- ============================================================================
--  03_raw_hechos.sql — TABLAS RAW TRANSACCIONALES (el núcleo: ~597k filas)
--  Proyecto: cargadirecta_snowflake · 2026-08-11
--  Destino:  DB_ANALYTICS.SCH_CORE  (rol ROLE_ANALYTICS)
--            -> destino FINAL previsto: SCH_PLD (ver 06_migrar_a_sch_almena.sql)
--
--  Estas son las tablas que HOY NO EXISTEN en ningun esquema. Es el trabajo real:
--     CFDI               241,855      (~2010 -> hoy)
--     PAGO (REP)         177,730
--     MOVIMIENTO_BANCARIO 177,733
--     GASTO                   51
--
--  Se extraen por JOB = (entidad, empresa, ventana) porque la API exige `empresa`
--  con UN SOLO valor. El avance se registra en ERP_SYNC_CONTROL.
--
--  Estas entidades SI MUTAN (estatus, saldo, cancelacion) -> MERGE por llave + ROW_HASH,
--  nunca INSERT ciego.
-- ============================================================================

USE DATABASE DB_ANALYTICS;
USE SCHEMA SCH_CORE;

-- ---------------------------------------------------------------------------
-- 1) CFDI  (/cfdi)  — 241,855 esperados · LA TABLA MAS IMPORTANTE
--    Params obligatorios de extraccion:
--      incluir_partidas=true   -> las partidas vienen en el MISMO JSON
--      incluir_xml_code=false  -> PERF 10x (el default true arrastra el XML completo)
--      no_registros_x_pagina=1000
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_CFDI (
    ID_CFDI             VARCHAR(40)   NOT NULL,
    FOLIO_FISCAL        VARCHAR(60),          -- UUID del SAT
    EMPRESA             VARCHAR(120)  NOT NULL,
    RFC_EMISOR          VARCHAR(100),
    RAZON_EMISOR        VARCHAR(400),
    ARRENDATARIO        VARCHAR(400),
    ID_ARRENDATARIO     VARCHAR(40),
    RFC_RECEPTOR        VARCHAR(100),
    RAZON_RECEPTOR      VARCHAR(400),
    SUCURSAL            VARCHAR(300),
    DOMICILIO_FISCAL    VARCHAR(400),
    INMUEBLE            VARCHAR(300),
    ID_INMUEBLE         VARCHAR(40),
    UNIDAD              VARCHAR(300),
    ID_UNIDAD           VARCHAR(40),
    CONTRATO            VARCHAR(80),
    -- fechas
    FECHA               DATE,
    FECHA_LIMITE        DATE,
    FECHA_TIMBRE        TIMESTAMP_NTZ,
    FECHA_CANCELACION   TIMESTAMP_NTZ,        -- != NULL  => CFDI cancelado
    -- estatus
    ESTATUS_DOCUMENTO   VARCHAR(60),
    TIPO_DOCUMENTO      VARCHAR(60),
    TIPO_COMPROBANTE    VARCHAR(40),
    ESTATUS_TIMBRE      VARCHAR(60),
    ESTATUS_VARIABLE    VARCHAR(60),
    ESTATUS_CONECTOR    VARCHAR(60),
    -- fiscales
    SERIE               VARCHAR(40),
    FOLIO               VARCHAR(40),
    FORMA_PAGO          VARCHAR(80),          -- texto con clave: "03 - Transferencia"
    METODO_PAGO         VARCHAR(40),          -- PUE / PPD
    USO_CFDI            VARCHAR(80),
    CONDICION_PAGO      VARCHAR(80),
    NUMERO_PARCIALIDAD  NUMBER(9,0),
    -- montos
    MONEDA              VARCHAR(100),
    TIPO_CAMBIO         NUMBER(18,6),
    SUBTOTAL            NUMBER(38,6),
    DESCUENTO           NUMBER(38,6),
    IVA                 NUMBER(38,6),
    RETENCION_IVA       NUMBER(38,6),
    ISR                 NUMBER(38,6),
    TOTAL               NUMBER(38,6),
    SALDO               NUMBER(38,6),
    TOTAL_MONEDA_BASE   NUMBER(38,6),
    SALDO_MONEDA_BASE   NUMBER(38,6),
    NOTA_CREDITO        NUMBER(38,6),
    -- derivados de control
    ANIO                NUMBER(4,0),          -- YEAR(FECHA): filtro barato para el backfill
    MES                 NUMBER(2,0),
    NUM_PARTIDAS        NUMBER(9,0),          -- ARRAY_SIZE(partida) — conciliacion rapida
    RAW_JSON            VARIANT,              -- SIN xml_code (se pide false)
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /cfdi — 241,855 esperados (~2010->hoy). Extraido con incluir_partidas=true e incluir_xml_code=false (perf 10x). Las partidas vienen anidadas y se materializan en RAW_ERP_CFDI_PARTIDA en la MISMA pasada. FECHA_CANCELACION != NULL => cancelado.';

-- ---------------------------------------------------------------------------
-- 2) CFDI_PARTIDA — el desglose por concepto
--    NO se llama /partida aparte: se materializa del anidado partida[] de /cfdi
--    en la misma pasada (gratis). Materializada y no vista porque un LATERAL
--    FLATTEN sobre 241k VARIANTs en cada consulta seria caro.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_CFDI_PARTIDA (
    ID_PARTIDA          VARCHAR(40)   NOT NULL,
    ID_CFDI             VARCHAR(40)   NOT NULL,
    EMPRESA             VARCHAR(120),
    FOLIO_FISCAL        VARCHAR(60),
    FECHA               DATE,
    DESCRIPCION         VARCHAR(1000),
    CONCEPTO            VARCHAR(300),
    ID_CONCEPTO         VARCHAR(40),
    TIPO_CONCEPTO       VARCHAR(120),         -- 'Renta' / 'Renta Variable' / servicios...
    PRODUCTO_SERVICIO   VARCHAR(120),
    UNIDAD_MEDIDA       VARCHAR(80),
    CUENTA_PREDIAL      VARCHAR(120),
    VALOR_UNITARIO      NUMBER(38,6),
    CANTIDAD            NUMBER(18,6),
    DESCUENTO           NUMBER(38,6),
    SUBTOTAL            NUMBER(38,6),
    IVA                 NUMBER(38,6),
    ISR                 NUMBER(38,6),
    RETENCION_IVA       NUMBER(38,6),
    TOTAL               NUMBER(38,6),
    SALDO               NUMBER(38,6),
    ESTATUS_VARIABLE    VARCHAR(60),
    MINIMO_VARIABLE     NUMBER(38,6),
    PORCENTAJE_VARIABLE NUMBER(18,6),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW partidas de CFDI, materializadas del anidado partida[] de /cfdi (NO se llama /partida). TIPO_CONCEPTO distingue Renta / Renta Variable / servicios — es la base de la logica PLD.';

-- ---------------------------------------------------------------------------
-- 3) PAGO (complemento de pago / REP)  (/pago)  — 177,730 esperados
--    Extraer con incluir_movimiento_bancario=true. Max ~500/pagina
--    ("el tamano del diccionario excede lo permitido" si se pide mas).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_PAGO (
    ID_CFDI             VARCHAR(40)   NOT NULL,   -- el REP es un CFDI de tipo pago
    FOLIO_FISCAL        VARCHAR(60),
    EMPRESA             VARCHAR(120)  NOT NULL,
    RFC_EMISOR          VARCHAR(100),
    RAZON_EMISOR        VARCHAR(400),
    ARRENDATARIO        VARCHAR(400),
    RFC_RECEPTOR        VARCHAR(100),
    RAZON_RECEPTOR      VARCHAR(400),
    SUCURSAL            VARCHAR(300),
    DOMICILIO_FISCAL    VARCHAR(400),
    INMUEBLE            VARCHAR(300),
    ID_INMUEBLE         VARCHAR(40),
    UNIDAD              VARCHAR(300),
    ID_UNIDAD           VARCHAR(40),
    CONTRATO            VARCHAR(80),
    FECHA               DATE,
    FECHA_TIMBRE        TIMESTAMP_NTZ,
    FECHA_CANCELACION   TIMESTAMP_NTZ,        -- REP CANCELADO -> saltar (si no, 2x techo)
    ESTATUS_DOCUMENTO   VARCHAR(60),
    TIPO_DOCUMENTO      VARCHAR(60),
    ESTATUS_TIMBRE      VARCHAR(60),
    ESTATUS_CONECTOR    VARCHAR(60),
    SERIE               VARCHAR(40),
    FOLIO               VARCHAR(40),
    USO_CFDI            VARCHAR(80),
    TIPO_CAMBIO         NUMBER(18,6),
    ANIO                NUMBER(4,0),
    MES                 NUMBER(2,0),
    NUM_ABONOS          NUMBER(9,0),          -- ARRAY_SIZE(pago)
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /pago = complemento de pago (REP). 177,730 esperados. Max ~500/pagina. OJO: FECHA_CANCELACION != NULL debe saltarse en la logica PLD o se duplica el techo. Anidados: pago[] (abonos) y documento_relacionado_pago[] -> se materializan abajo.';

-- ---------------------------------------------------------------------------
-- 4) PAGO_ABONO — cada abono individual (anidado pago[] del REP)
--    Este es el GRANO del libro mayor de cobranza.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_PAGO_ABONO (
    ID_PAGO             VARCHAR(40)   NOT NULL,
    ID_CFDI             VARCHAR(40)   NOT NULL,   -- el REP al que pertenece
    EMPRESA             VARCHAR(120),
    FOLIO_FISCAL_REP    VARCHAR(60),
    FECHA               DATE,
    HORA                VARCHAR(100),
    MONTO               NUMBER(38,6),
    MONEDA              VARCHAR(100),
    TIPO_CAMBIO         NUMBER(18,6),
    FORMA_PAGO          VARCHAR(80),          -- texto con clave: "01 - Efectivo"
    TIPO_PAGO           VARCHAR(80),
    ESTATUS_PAGO        VARCHAR(60),
    ESTATUS_TIMBRE      VARCHAR(60),
    NUMERO_OPERACION    VARCHAR(120),
    CUENTA_ORIGEN       VARCHAR(60),
    BANCO_ORIGEN        VARCHAR(200),
    CUENTA_BENEFICIARIO VARCHAR(60),
    BANCO_BENEFICIARIO  VARCHAR(200),
    ID_MOVIMIENTO_BANCARIO VARCHAR(40),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW abonos individuales (anidado pago[] del REP). Es el GRANO del libro mayor de cobranza: FORMA_PAGO real llega aqui como texto con clave.';

-- ---------------------------------------------------------------------------
-- 5) PAGO_DOC_RELACIONADO — qué factura paga cada abono y con qué parcialidad
--    Llave compuesta (UUID de la factura pagada, numero de parcialidad).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_PAGO_DOC_RELACIONADO (
    ID_DOC_RELACIONADO  VARCHAR(40)   NOT NULL,
    ID_PAGO             VARCHAR(40)   NOT NULL,
    ID_CFDI_PAGANDO     VARCHAR(40),          -- la FACTURA que se esta pagando
    EMPRESA             VARCHAR(120),
    FOLIO_FISCAL        VARCHAR(60),          -- UUID de la factura pagada
    SERIE               VARCHAR(40),
    FOLIO               VARCHAR(40),
    METODO_PAGO         VARCHAR(40),          -- PPD / PUE
    NUMERO_PARCIALIDAD  NUMBER(9,0),
    TIPO_CAMBIO         NUMBER(18,6),
    SALDO_ANTERIOR      NUMBER(38,6),
    IMPORTE_PAGADO      NUMBER(38,6),
    IMPORTE_INSOLUTO    NUMBER(38,6),
    IVA_DOC_RELACIONADO NUMBER(38,6),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW documento_relacionado_pago[]: el puente ABONO -> FACTURA con numero de parcialidad. Base de la logica de parcialidades del PLD (llave UUID + PARCIALIDAD).';

-- ---------------------------------------------------------------------------
-- 6) MOVIMIENTO_BANCARIO  (/movimiento_bancario)  — 177,733 esperados
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_MOVIMIENTO_BANCARIO (
    ID_MOVIMIENTO_BANCARIO VARCHAR(40) NOT NULL,
    EMPRESA             VARCHAR(120)  NOT NULL,
    ARRENDATARIO        VARCHAR(400),
    SUCURSAL            VARCHAR(300),
    DOMICILIO_FISCAL    VARCHAR(400),
    FECHA               DATE,
    MONTO               NUMBER(38,6),
    MONEDA              VARCHAR(100),
    TIPO_CAMBIO         NUMBER(18,6),
    ESTATUS             VARCHAR(60),
    ESTATUS_CONECTOR    VARCHAR(60),
    CUENTA              VARCHAR(60),
    BANCO               VARCHAR(200),
    REFERENCIA          VARCHAR(300),
    ANIO                NUMBER(4,0),
    MES                 NUMBER(2,0),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /movimiento_bancario — 177,733 esperados. Extraer con incluir_detalle_pago=true. Es la fecha REAL de cobro (vs la fecha del REP).';

-- ---------------------------------------------------------------------------
-- 7) GASTO / PAGO_GASTO / ORDEN_TRABAJO / ACTUALIZACION  (egresos y operacion)
--    Bajo volumen. /gasto NO tiene fecha_*_modificacion -> full por ventana.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_GASTO (
    ID_GASTO VARCHAR(40) NOT NULL, EMPRESA VARCHAR(120) NOT NULL,
    PROVEEDOR VARCHAR(400), TIPO_GASTO VARCHAR(120), NUMERO_FACTURA VARCHAR(80),
    UUID VARCHAR(60), INMUEBLE VARCHAR(300), ID_INMUEBLE VARCHAR(40),
    UNIDAD VARCHAR(300), ID_UNIDAD VARCHAR(40), ADMINISTRADOR VARCHAR(200),
    FECHA DATE, FECHA_TIMBRE TIMESTAMP_NTZ, FECHA_CANCELACION TIMESTAMP_NTZ,
    ESTATUS VARCHAR(60), ESTATUS_TIMBRE VARCHAR(60),
    MONEDA VARCHAR(100), TIPO_CAMBIO NUMBER(18,6),
    SUBTOTAL NUMBER(38,6), IVA NUMBER(38,6), TOTAL NUMBER(38,6),
    ANIO NUMBER(4,0), MES NUMBER(2,0),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /gasto (egresos) — solo 51 esperados. SIN filtro fecha_*_modificacion -> siempre por ventana de fecha.';

CREATE TABLE IF NOT EXISTS RAW_ERP_PAGO_GASTO (
    ID_PAGO_GASTO VARCHAR(40) NOT NULL, EMPRESA VARCHAR(120) NOT NULL,
    PROVEEDOR VARCHAR(400), FECHA DATE, MONTO NUMBER(38,6), MONEDA VARCHAR(100),
    FORMA_PAGO VARCHAR(80), ESTATUS_PAGO VARCHAR(60), ESTATUS_TIMBRE VARCHAR(60),
    ANIO NUMBER(4,0), MES NUMBER(2,0),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /pago_gasto (pagos a proveedores).';

CREATE TABLE IF NOT EXISTS RAW_ERP_ORDEN_TRABAJO (
    ID_ORDEN_TRABAJO VARCHAR(40) NOT NULL, INMUEBLE VARCHAR(300), ID_INMUEBLE VARCHAR(40),
    UNIDAD VARCHAR(300), ID_UNIDAD VARCHAR(40), PROVEEDOR VARCHAR(400),
    SUPERVISOR VARCHAR(200), RESPONSABLE VARCHAR(200), TIPO_MANTENIMIENTO VARCHAR(120),
    CATEGORIA VARCHAR(120), EQUIPO VARCHAR(200), ESTATUS VARCHAR(60), FECHA DATE,
    ANIO NUMBER(4,0), MES NUMBER(2,0),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /orden_trabajo (mantenimiento). Max 50/pagina si se envia `buscar`.';

CREATE TABLE IF NOT EXISTS RAW_ERP_ACTUALIZACION (
    ID_ACTUALIZACION VARCHAR(40), TIPO VARCHAR(120), FECHA DATE, VALOR NUMBER(38,6),
    INMUEBLE VARCHAR(300), UNIDAD VARCHAR(300),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /actualizacion (Predial, Valor comercial, Agua, Luz, Afluencia). Sonda 2026-07-28: tipo=Valor comercial vino VACIO -> el valor de referencia NO sale de aqui.';
