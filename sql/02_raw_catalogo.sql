-- ============================================================================
--  02_raw_catalogo.sql — TABLAS RAW DE CATÁLOGO
--  Proyecto: cargadirecta_snowflake · 2026-08-11
--  Destino:  DB_ANALYTICS.SCH_CORE  (rol ROLE_ANALYTICS)
--
--  IMPORTANTE: estas tablas son NUEVAS (prefijo RAW_ERP_). Las tablas
--  INMOGES_<entidad> que ya existen pertenecen al pipeline PLD en PRODUCCIÓN
--  y NO SE TOCAN.
--
--  CONTRATO DE COLUMNAS (todas las RAW_* lo cumplen):
--    <llaves naturales>  la llave del MERGE
--    <negocio nivel 1>   campos que se filtran/consultan seguido
--    RAW_JSON  VARIANT   el JSON COMPLETO tal cual lo devolvió la API
--    ROW_HASH  VARCHAR   MD5(RAW_JSON sin created/modifiedDate) -> detecta cambios reales
--    ORIGEN    VARCHAR   API | PORTAL_EXCEL | PORTAL_JSON  (arquitectura híbrida)
--    LOAD_TS / ACTIVO / FECHA_BAJA
--
--  Por qué RAW_JSON siempre: si mañana falta un campo que no aplanamos, se saca
--  con SQL sobre el VARIANT — sin volver a llamar a la API 5 horas.
--
--  NOTA DE TIPOS: las fechas se cargan con TRY_TO_DATE / TRY_TO_TIMESTAMP.
--  Si un valor viene raro queda NULL (no truena la ingesta) y el literal
--  original SIEMPRE queda preservado dentro de RAW_JSON.
-- ============================================================================

USE DATABASE DB_ANALYTICS;
USE SCHEMA SCH_CORE;

-- ---------------------------------------------------------------------------
-- 1) EMPRESA  (/empresa)  — 50 esperadas
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_EMPRESA (
    ID_EMPRESA          VARCHAR(40)   NOT NULL,
    ALIAS               VARCHAR(200),
    RAZON_SOCIAL        VARCHAR(400),
    RFC                 VARCHAR(100),
    TIPO_PERSONA        VARCHAR(40),
    REGIMEN_SOCIETARIO  VARCHAR(120),
    ESTATUS_MODULO      VARCHAR(40),
    ID_EXTERNO          VARCHAR(80),
    DOMICILIO_FISCAL_PRINCIPAL VARCHAR(400),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /empresa. Llave: ID_EMPRESA. Anidado en RAW_JSON: domicilio_fiscal[] (con regimen_fiscal, correos_cfdi, cuenta bancaria).';

-- ---------------------------------------------------------------------------
-- 2) PROPIETARIO  (/propietario)  — 118 esperados
--    OJO: este endpoint NO tiene fecha_*_modificacion -> siempre pull completo.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_PROPIETARIO (
    ID_PROPIETARIO      VARCHAR(40)   NOT NULL,
    ALIAS               VARCHAR(200),
    RAZON_SOCIAL        VARCHAR(400),
    RFC                 VARCHAR(100),
    TIPO_PERSONA        VARCHAR(40),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /propietario. Trae la ficha PLD (pld_entidad[] con CURP) dentro de RAW_JSON. SIN filtro de modificacion -> pull completo siempre (118 filas, es barato). Calidad conocida: 22 sin RFC y 5 RFC genericos XAXX -> el RFC NO sirve como llave.';

-- ---------------------------------------------------------------------------
-- 3) INMUEBLE  (/inmueble)  — 334 esperados (325 activos + 9 inactivos)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_INMUEBLE (
    ID_INMUEBLE         VARCHAR(40)   NOT NULL,
    INMUEBLE            VARCHAR(300),
    FOLIO               VARCHAR(80),          -- suele venir NULL (hueco PLD)
    TIPO_INMUEBLE       VARCHAR(120),         -- suele venir NULL (hueco PLD)
    CLAVE_CATASTRAL     VARCHAR(120),         -- suele venir NULL (lo resuelve el Excel el proveedor del portal)
    CUENTA_PREDIAL      VARCHAR(120),
    ID_EXTERNO          VARCHAR(80),
    ESTATUS             VARCHAR(40),
    ADMINISTRADOR       VARCHAR(200),
    CENTRO_COSTOS       VARCHAR(120),
    CALLE               VARCHAR(300),
    NO_EXTERIOR         VARCHAR(40),
    NO_INTERIOR         VARCHAR(40),
    COLONIA             VARCHAR(200),
    CODIGO_POSTAL       VARCHAR(50),          -- suele venir NULL (hueco PLD)
    CIUDAD              VARCHAR(120),
    ESTADO              VARCHAR(120),
    PAIS                VARCHAR(80),
    ZONA                VARCHAR(120),
    REGION              VARCHAR(120),
    PLAZA               VARCHAR(120),
    M2_RENTABLES        NUMBER(18,4),
    M2_CONSTRUCCION     NUMBER(18,4),
    M2_TERRENO          NUMBER(18,4),
    PRECIO_RENTA        NUMBER(38,6),
    PRECIO_M2_RENTA     NUMBER(38,6),
    MERCADO             NUMBER(38,6),         -- proxy parcial de "valor de referencia"
    CATASTRO            NUMBER(38,6),
    ADQUISICION         NUMBER(38,6),
    FECHA_ADQUISICION   DATE,
    SEGMENTO_NEGOCIO    VARCHAR(120),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /inmueble. FOLIO, TIPO_INMUEBLE, CLAVE_CATASTRAL y CODIGO_POSTAL suelen venir NULL por la API -> se completan con el Excel _general de el proveedor del portal (ORIGEN=PORTAL_EXCEL). Anidado: documentos[].';

-- ---------------------------------------------------------------------------
-- 4) UNIDAD  (/unidad)  — 2,711 esperadas
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_UNIDAD (
    ID_UNIDAD           VARCHAR(40)   NOT NULL,
    UNIDAD              VARCHAR(300),
    INMUEBLE            VARCHAR(300),
    USO                 VARCHAR(120),
    ESTATUS             VARCHAR(40),
    CUENTA_PREDIAL      VARCHAR(120),
    CLAVE_CATASTRAL     VARCHAR(120),
    PRECIO_RENTA        NUMBER(38,6),
    PRECIO_M2_RENTA     NUMBER(38,6),
    PRECIO_MANTENIMIENTO NUMBER(38,6),
    PRECIO_DEPOSITO_GARANTIA NUMBER(38,6),
    CALLE               VARCHAR(300),
    NO_EXTERIOR         VARCHAR(40),
    NO_INTERIOR         VARCHAR(40),
    COLONIA             VARCHAR(200),
    CODIGO_POSTAL       VARCHAR(50),
    CIUDAD              VARCHAR(120),
    ESTADO              VARCHAR(120),
    PAIS                VARCHAR(80),
    ZONA                VARCHAR(120),
    REGION              VARCHAR(120),
    PLAZA               VARCHAR(120),
    M2_RENTABLES        NUMBER(18,4),
    M2_CONSTRUCCION     NUMBER(18,4),
    PRECIO_MERCADO      NUMBER(38,6),
    CATASTRO            NUMBER(38,6),
    ADQUISICION         NUMBER(38,6),
    FECHA_ADQUISICION   DATE,
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /unidad (el local/espacio que se renta). 2,711 esperadas — cuadra exacto con el portal. Anidado: documentos[], portales[].';

-- ---------------------------------------------------------------------------
-- 5) CONTRATO  (/contrato)  — 4,982 esperados (TODOS los estatus)
--    OJO: el portal muestra 1,637 (solo Vigente+Renovacion+Vencido). Aqui van todos.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_CONTRATO (
    ID_CONTRATO         VARCHAR(40)   NOT NULL,
    ID_EXTERNO          VARCHAR(80),
    EMPRESA             VARCHAR(120),
    ARRENDATARIO        VARCHAR(400),
    INMUEBLE            VARCHAR(300),
    ID_INMUEBLE         VARCHAR(40),
    UNIDAD              VARCHAR(300),
    ID_UNIDAD           VARCHAR(40),
    SUCURSAL            VARCHAR(300),
    DOMICILIO_FISCAL    VARCHAR(400),
    GIRO                VARCHAR(200),         -- actividad economica (util para el aviso PLD)
    GRUPO_AVISO         VARCHAR(200),         -- agrupacion de avisos PLD
    ESTATUS             VARCHAR(60),
    ESTATUS_ADICIONAL   VARCHAR(60),
    FECHA_INICIAL       DATE,
    FECHA_FINAL         DATE,
    FECHA_FIRMA         DATE,
    FECHA_RENOVACION    DATE,
    FECHA_ENTREGA       DATE,
    FECHA_OPERACION     DATE,
    FRECUENCIA          VARCHAR(60),
    MONEDA              VARCHAR(100),
    TIPO_CAMBIO         NUMBER(18,6),
    SUBTOTAL            NUMBER(38,6),
    DESCUENTO           NUMBER(38,6),
    IVA                 NUMBER(38,6),
    RETENCION_IVA       NUMBER(38,6),
    ISR                 NUMBER(38,6),
    TOTAL               NUMBER(38,6),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /contrato — TODOS los estatus (4,982), no la vista del portal (1,637). GIRO y GRUPO_AVISO son utiles para PLD y hoy no se leian. Anidado: concepto_contrato[] -> se explota en ERP_STG_CONTRATO_CONCEPTO.';

-- ---------------------------------------------------------------------------
-- 6) ARRENDATARIO  (/arrendatario)  — 4,989 esperados
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_ARRENDATARIO (
    ID_ARRENDATARIO     VARCHAR(40)   NOT NULL,
    ALIAS               VARCHAR(300),
    RAZON_SOCIAL        VARCHAR(400),
    RFC                 VARCHAR(100),
    TIPO_PERSONA        VARCHAR(40),
    MEDIO_CONTACTO      VARCHAR(200),
    ESTATUS_MODULO      VARCHAR(40),
    CONTACTO_PRINCIPAL  VARCHAR(300),
    SUCURSAL_PRINCIPAL  VARCHAR(300),
    FECHA               DATE,
    TIENE_FICHA_PLD     BOOLEAN,              -- derivado: pld_entidad[] no vacio (~3.5%)
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /arrendatario = el CLIENTE. Anidados clave en RAW_JSON: sucursales[] (unico lugar con codigo_postal), contactos[] (telefono), pld_entidad[] (ficha PLD con CURP, ~3.5% poblada). Se explotan en vistas STG_*.';

-- ---------------------------------------------------------------------------
-- 7) SUCURSAL_ARRENDATARIO  (/sucursal_arrendatario)  — 5,355 esperadas
--    OJO: esta version es PLANA y NO trae codigo_postal. Para el domicilio con
--    CP hay que usar el anidado sucursales[] de /arrendatario.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_SUCURSAL_ARRENDATARIO (
    ID_SUCURSAL         VARCHAR(40)   NOT NULL,
    ARRENDATARIO        VARCHAR(400),
    RFC_ARRENDATARIO    VARCHAR(100),
    ALIAS               VARCHAR(300),
    ID_EXTERNO_SUCURSAL VARCHAR(80),
    RAW_JSON            VARIANT,
    ROW_HASH            VARCHAR(32),
    ORIGEN              VARCHAR(20)   DEFAULT 'API',
    LOAD_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACTIVO              BOOLEAN       DEFAULT TRUE,
    FECHA_BAJA          TIMESTAMP_NTZ
) COMMENT = 'RAW /sucursal_arrendatario (version PLANA, SIN codigo_postal — por eso el CP salia 0%). Para domicilio completo usar ERP_STG_ARRENDATARIO_SUCURSAL (del anidado de /arrendatario).';

-- ---------------------------------------------------------------------------
-- 8) CATÁLOGOS MENORES — mismo contrato, pocos campos aplanados
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_ERP_PROVEEDOR (
    ID_PROVEEDOR VARCHAR(40) NOT NULL, ALIAS VARCHAR(300), RAZON_SOCIAL VARCHAR(400),
    RFC VARCHAR(100), ID_EXTERNO VARCHAR(80),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /proveedor.';

CREATE TABLE IF NOT EXISTS RAW_ERP_DIRECCION_FISCAL (
    ID_DIRECCION_FISCAL VARCHAR(40) NOT NULL, ALIAS VARCHAR(300),
    PROVEEDOR VARCHAR(400), RFC_PROVEEDOR VARCHAR(100), ID_EXTERNO VARCHAR(80),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /direccion_fiscal.';

CREATE TABLE IF NOT EXISTS RAW_ERP_CUENTA_BANCARIA (
    ID_CUENTA_BANCARIA VARCHAR(40) NOT NULL, ALIAS VARCHAR(300),
    EMPRESA VARCHAR(120), NUMERO_CUENTA VARCHAR(60), BANCO VARCHAR(200),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /cuenta_bancaria.';

CREATE TABLE IF NOT EXISTS RAW_ERP_CUENTA_CONTABLE (
    CUENTA_CONTABLE VARCHAR(80) NOT NULL, NOMBRE VARCHAR(300),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /cuenta_contable.';

CREATE TABLE IF NOT EXISTS RAW_ERP_CENTRO_COSTOS (
    ID_CENTRO_COSTOS VARCHAR(40) NOT NULL, NOMBRE VARCHAR(300),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /centro_costos.';

CREATE TABLE IF NOT EXISTS RAW_ERP_CONCEPTO (
    ID_CONCEPTO VARCHAR(40) NOT NULL, ALIAS VARCHAR(300), ID_EXTERNO VARCHAR(80),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /concepto.';

CREATE TABLE IF NOT EXISTS RAW_ERP_PRODUCTO (
    PRODUCTO VARCHAR(300) NOT NULL, ID_EXTERNO VARCHAR(80),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /producto.';

-- /pld = AVISOS PLD presentados, con la URL de su XML en S3.
-- Agregado 2026-08-27 (gap analysis, doc 11 §1): el endpoint existe desde
-- siempre pero nunca se habia extraido. NO filtrar por `empresa`: con ese
-- parametro la API devuelve otra forma y rompe el parser. Pull completo.
CREATE TABLE IF NOT EXISTS RAW_ERP_PLD_AVISO (
    ID_AVISO VARCHAR(40) NOT NULL, FECHA DATE, EMPRESA VARCHAR(120),
    ESTATUS VARCHAR(200), DOCUMENTOS VARCHAR(4000),
    RAW_JSON VARIANT, ROW_HASH VARCHAR(32), ORIGEN VARCHAR(20) DEFAULT 'API',
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), ACTIVO BOOLEAN DEFAULT TRUE,
    FECHA_BAJA TIMESTAMP_NTZ
) COMMENT = 'RAW /pld — avisos PLD presentados + URL del XML en S3.';
