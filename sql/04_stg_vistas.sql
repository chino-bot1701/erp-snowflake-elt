-- ============================================================================
--  04_stg_vistas.sql — CAPA STG: explota el JSON anidado del RAW
--  Proyecto: cargadirecta_snowflake · 2026-08-12
--  Destino:  DB_ANALYTICS.SCH_CORE
--
--  QUÉ RESUELVE: las tablas RAW_ERP_* guardan el JSON COMPLETO en la
--  columna RAW_JSON (VARIANT). Ahí dentro viven arreglos anidados que hoy
--  NADIE puede consultar con SQL normal:
--
--      arrendatario -> sucursales[]     <- EL UNICO lugar con codigo_postal
--      arrendatario -> contactos[]      <- telefonos y correos
--      arrendatario -> pld_entidad[]    <- la ficha PLD con CURP
--      empresa      -> domicilio_fiscal[]  <- regimen_fiscal
--      contrato     -> concepto_contrato[]
--      inmueble/unidad -> documentos[]
--
--  Son VISTAS, no tablas: siempre reflejan el RAW actual, no hay que
--  re-procesar nada cuando la ingesta avanza, y no ocupan almacenamiento.
--
--  LATERAL FLATTEN es el operador de Snowflake que convierte un arreglo del
--  VARIANT en filas. `outer => TRUE` conserva el padre aunque el arreglo venga
--  vacío (importante: si no, se perderían los arrendatarios sin sucursales).
-- ============================================================================

USE DATABASE DB_ANALYTICS;
USE SCHEMA SCH_CORE;

-- ---------------------------------------------------------------------------
-- 1) SUCURSALES DEL ARRENDATARIO  ⭐ la más valiosa
--    /sucursal_arrendatario (la tabla plana) NO trae codigo_postal — por eso el
--    CP salía en 0%. El CP SOLO existe aquí, en el anidado de /arrendatario.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ERP_STG_ARRENDATARIO_SUCURSAL AS
SELECT
    a.ID_ARRENDATARIO,
    a.RAZON_SOCIAL                             AS ARRENDATARIO,
    a.RFC                                      AS RFC_ARRENDATARIO,
    a.TIPO_PERSONA,
    s.value:id_sucursal::STRING                AS ID_SUCURSAL,
    s.value:alias::STRING                      AS SUCURSAL,
    s.value:calle::STRING                      AS CALLE,
    s.value:no_exterior::STRING                AS NO_EXTERIOR,
    s.value:no_interior::STRING                AS NO_INTERIOR,
    s.value:colonia::STRING                    AS COLONIA,
    s.value:codigo_postal::STRING              AS CODIGO_POSTAL,   -- ⭐ solo aquí
    s.value:ciudad::STRING                     AS CIUDAD,
    s.value:estado::STRING                     AS ESTADO,
    s.value:pais::STRING                       AS PAIS,
    s.value:direccion::STRING                  AS DIRECCION,
    s.value:forma_pago_clave::STRING           AS FORMA_PAGO_CLAVE,
    s.value:forma_pago::STRING                 AS FORMA_PAGO,
    s.value:metodo_pago_clave::STRING          AS METODO_PAGO_CLAVE,
    s.value:metodo_pago::STRING                AS METODO_PAGO,
    s.value:uso_cfdi_clave::STRING             AS USO_CFDI_CLAVE,
    s.value:banco::STRING                      AS BANCO,
    s.value:cuenta_bancaria::STRING            AS CUENTA_BANCARIA,
    s.value:referencia::STRING                 AS REFERENCIA,
    s.value:cuenta_contable::STRING            AS CUENTA_CONTABLE,
    a.LOAD_TS, a.ACTIVO
FROM RAW_ERP_ARRENDATARIO a,
     LATERAL FLATTEN(input => a.RAW_JSON:sucursales, outer => TRUE) s
WHERE s.value IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 2) CONTACTOS DEL ARRENDATARIO — teléfono y correo (obligatorios en el portal de avisos UIF)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ERP_STG_ARRENDATARIO_CONTACTO AS
SELECT
    a.ID_ARRENDATARIO,
    a.RAZON_SOCIAL                             AS ARRENDATARIO,
    a.RFC                                      AS RFC_ARRENDATARIO,
    c.value:id_contacto::STRING                AS ID_CONTACTO,
    c.value:nombre::STRING                     AS NOMBRE,
    c.value:telefono::STRING                   AS TELEFONO,
    c.value:telefono_adicional::STRING         AS TELEFONO_ADICIONAL,
    c.value:extension::STRING                  AS EXTENSION,
    c.value:correo::STRING                     AS CORREO,
    c.value:puesto::STRING                     AS PUESTO,
    TRY_TO_DATE(c.value:fecha_alta::STRING)    AS FECHA_ALTA,
    c.value:notas::STRING                      AS NOTAS,
    a.LOAD_TS, a.ACTIVO
FROM RAW_ERP_ARRENDATARIO a,
     LATERAL FLATTEN(input => a.RAW_JSON:contactos, outer => TRUE) c
WHERE c.value IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 3) FICHA PLD DEL ARRENDATARIO  ⭐ el molde exacto que pide el portal de avisos UIF
--    Cobertura conocida: ~3.5% de los arrendatarios la tienen poblada.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ERP_STG_ARRENDATARIO_PLD AS
SELECT
    a.ID_ARRENDATARIO,
    a.RAZON_SOCIAL                             AS ARRENDATARIO,
    a.RFC                                      AS RFC_ARRENDATARIO,
    p.value:tipo_persona::STRING               AS TIPO_PERSONA,
    -- persona física
    p.value:pf_nombre::STRING                  AS PF_NOMBRE,
    p.value:pf_apellido_paterno::STRING        AS PF_APELLIDO_PATERNO,
    p.value:pf_apellido_materno::STRING        AS PF_APELLIDO_MATERNO,
    TRY_TO_DATE(p.value:pf_fecha_nacimiento::STRING) AS PF_FECHA_NACIMIENTO,
    UPPER(p.value:pf_curp::STRING)             AS PF_CURP,
    -- persona moral
    p.value:actividad_economica::STRING        AS ACTIVIDAD_ECONOMICA,
    TRY_TO_DATE(p.value:fecha_constitucion::STRING) AS FECHA_CONSTITUCION,
    p.value:giro_mercantil::STRING             AS GIRO_MERCANTIL,
    -- representante
    p.value:rpm_nombre::STRING                 AS RPM_NOMBRE,
    p.value:rpm_apellido_paterno::STRING       AS RPM_APELLIDO_PATERNO,
    p.value:rpm_apellido_materno::STRING       AS RPM_APELLIDO_MATERNO,
    TRY_TO_DATE(p.value:rpm_fecha_nacimiento::STRING) AS RPM_FECHA_NACIMIENTO,
    UPPER(p.value:rpm_rfc::STRING)             AS RPM_RFC,
    UPPER(p.value:rpm_curp::STRING)            AS RPM_CURP,
    -- domicilio + contacto de la ficha
    p.value:calle::STRING                      AS CALLE,
    p.value:colonia::STRING                    AS COLONIA,
    p.value:no_exterior::STRING                AS NO_EXTERIOR,
    p.value:no_interior::STRING                AS NO_INTERIOR,
    p.value:pais::STRING                       AS PAIS,
    p.value:clave::STRING                      AS CODIGO_POSTAL,
    p.value:telefono::STRING                   AS TELEFONO,
    UPPER(p.value:correo::STRING)              AS CORREO,
    a.LOAD_TS, a.ACTIVO
FROM RAW_ERP_ARRENDATARIO a,
     LATERAL FLATTEN(input => a.RAW_JSON:pld_entidad, outer => TRUE) p
WHERE p.value IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 4) FICHA PLD DEL PROPIETARIO — mismo molde (aquí sí suele venir poblada)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ERP_STG_PROPIETARIO_PLD AS
SELECT
    pr.ID_PROPIETARIO,
    pr.RAZON_SOCIAL                            AS PROPIETARIO,
    pr.RFC                                     AS RFC_PROPIETARIO,
    p.value:tipo_persona::STRING               AS TIPO_PERSONA,
    p.value:pf_nombre::STRING                  AS PF_NOMBRE,
    p.value:pf_apellido_paterno::STRING        AS PF_APELLIDO_PATERNO,
    p.value:pf_apellido_materno::STRING        AS PF_APELLIDO_MATERNO,
    TRY_TO_DATE(p.value:pf_fecha_nacimiento::STRING) AS PF_FECHA_NACIMIENTO,
    UPPER(p.value:pf_curp::STRING)             AS PF_CURP,
    p.value:actividad_economica::STRING        AS ACTIVIDAD_ECONOMICA,
    p.value:giro_mercantil::STRING             AS GIRO_MERCANTIL,
    UPPER(p.value:rpm_rfc::STRING)             AS RPM_RFC,
    UPPER(p.value:rpm_curp::STRING)            AS RPM_CURP,
    p.value:calle::STRING                      AS CALLE,
    p.value:colonia::STRING                    AS COLONIA,
    p.value:clave::STRING                      AS CODIGO_POSTAL,
    p.value:telefono::STRING                   AS TELEFONO,
    UPPER(p.value:correo::STRING)              AS CORREO,
    pr.LOAD_TS, pr.ACTIVO
FROM RAW_ERP_PROPIETARIO pr,
     LATERAL FLATTEN(input => pr.RAW_JSON:pld_entidad, outer => TRUE) p
WHERE p.value IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 5) DOMICILIOS FISCALES DE LA EMPRESA — trae regimen_fiscal
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ERP_STG_EMPRESA_DOMICILIO AS
SELECT
    e.ID_EMPRESA,
    e.ALIAS                                    AS EMPRESA,
    e.RAZON_SOCIAL,
    e.RFC,
    d.value:id_domicilio_fiscal::STRING        AS ID_DOMICILIO_FISCAL,
    d.value:alias::STRING                      AS DOMICILIO_ALIAS,
    d.value:regimen_fiscal::STRING             AS REGIMEN_FISCAL,   -- ⭐ p/ el aviso
    d.value:correos_cfdi::STRING               AS CORREOS_CFDI,
    d.value:cuenta_bancaria_principal::STRING  AS CUENTA_BANCARIA,
    d.value:uso_cfdi::STRING                   AS USO_CFDI,
    d.value:forma_pago::STRING                 AS FORMA_PAGO,
    d.value:metodo_pago::STRING                AS METODO_PAGO,
    d.value:cuenta_contable::STRING            AS CUENTA_CONTABLE,
    d.value:calle::STRING                      AS CALLE,
    d.value:no_exterior::STRING                AS NO_EXTERIOR,
    d.value:no_interior::STRING                AS NO_INTERIOR,
    d.value:colonia::STRING                    AS COLONIA,
    d.value:ciudad::STRING                     AS CIUDAD,
    d.value:estado::STRING                     AS ESTADO,
    d.value:pais::STRING                       AS PAIS,
    e.LOAD_TS, e.ACTIVO
FROM RAW_ERP_EMPRESA e,
     LATERAL FLATTEN(input => e.RAW_JSON:domicilio_fiscal, outer => TRUE) d
WHERE d.value IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 6) CONCEPTOS DEL CONTRATO — el desglose de lo que se cobra por contrato
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ERP_STG_CONTRATO_CONCEPTO AS
SELECT
    c.ID_CONTRATO,
    c.EMPRESA,
    c.ARRENDATARIO,
    c.INMUEBLE,
    c.UNIDAD,
    c.ESTATUS                                  AS ESTATUS_CONTRATO,
    k.value:id_concepto_contrato::STRING       AS ID_CONCEPTO_CONTRATO,
    k.value:descripcion::STRING                AS DESCRIPCION,
    k.value:tipo_concepto::STRING              AS TIPO_CONCEPTO,
    k.value:producto_servicio::STRING          AS PRODUCTO_SERVICIO,
    k.value:unidad_medida::STRING              AS UNIDAD_MEDIDA,
    k.value:referencia::STRING                 AS REFERENCIA,
    TRY_TO_DATE(k.value:fecha_inicial::STRING) AS FECHA_INICIAL,
    TRY_TO_DATE(k.value:fecha_final::STRING)   AS FECHA_FINAL,
    k.value:moneda::STRING                     AS MONEDA,
    TRY_TO_NUMBER(k.value:tipo_cambio::STRING, 18, 6)   AS TIPO_CAMBIO,
    TRY_TO_NUMBER(k.value:valor_unitario::STRING, 38, 6) AS VALOR_UNITARIO,
    TRY_TO_NUMBER(k.value:cantidad::STRING, 18, 6)      AS CANTIDAD,
    TRY_TO_NUMBER(k.value:descuento::STRING, 38, 6)     AS DESCUENTO,
    TRY_TO_NUMBER(k.value:subtotal::STRING, 38, 6)      AS SUBTOTAL,
    TRY_TO_NUMBER(k.value:iva::STRING, 38, 6)           AS IVA,
    TRY_TO_NUMBER(k.value:isr::STRING, 38, 6)           AS ISR,
    TRY_TO_NUMBER(k.value:retencion_iva::STRING, 38, 6) AS RETENCION_IVA,
    TRY_TO_NUMBER(k.value:total::STRING, 38, 6)         AS TOTAL,
    c.LOAD_TS, c.ACTIVO
FROM RAW_ERP_CONTRATO c,
     LATERAL FLATTEN(input => c.RAW_JSON:concepto_contrato, outer => TRUE) k
WHERE k.value IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 7) IMPUESTOS POR PARTIDA — el desglose fiscal más fino que existe
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ERP_STG_PARTIDA_IMPUESTO AS
SELECT
    p.ID_PARTIDA,
    p.ID_CFDI,
    p.EMPRESA,
    p.FOLIO_FISCAL,
    p.FECHA,
    p.TIPO_CONCEPTO,
    i.value:id_impuesto_partida::STRING        AS ID_IMPUESTO_PARTIDA,
    i.value:tipo_impuesto::STRING              AS TIPO_IMPUESTO,
    i.value:clave_impuesto::STRING             AS CLAVE_IMPUESTO,   -- 002 = IVA
    i.value:impuesto::STRING                   AS IMPUESTO,          -- "IVA 16%"
    TRY_TO_NUMBER(i.value:porcentaje::STRING, 18, 6) AS PORCENTAJE,
    TRY_TO_NUMBER(i.value:monto::STRING, 38, 6)      AS MONTO,
    i.value:cuenta_contable_impuesto::STRING   AS CUENTA_CONTABLE,
    p.LOAD_TS, p.ACTIVO
FROM RAW_ERP_CFDI_PARTIDA p,
     LATERAL FLATTEN(input => p.RAW_JSON:impuestos_partida, outer => TRUE) i
WHERE i.value IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 8) DOCUMENTOS de inmueble y unidad (adjuntos)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ERP_STG_DOCUMENTO AS
SELECT 'INMUEBLE' AS ORIGEN_ENTIDAD, i.ID_INMUEBLE AS ID_ENTIDAD,
       i.INMUEBLE AS ENTIDAD,
       d.value:titulo::STRING AS TITULO, d.value:tipo::STRING AS TIPO,
       TRY_TO_DATE(d.value:fecha::STRING) AS FECHA,
       d.value:documento::STRING AS DOCUMENTO, i.LOAD_TS
FROM RAW_ERP_INMUEBLE i,
     LATERAL FLATTEN(input => i.RAW_JSON:documentos, outer => TRUE) d
WHERE d.value IS NOT NULL
UNION ALL
SELECT 'UNIDAD', u.ID_UNIDAD, u.UNIDAD,
       d.value:titulo::STRING, d.value:tipo::STRING,
       TRY_TO_DATE(d.value:fecha::STRING),
       d.value:documentos::STRING, u.LOAD_TS
FROM RAW_ERP_UNIDAD u,
     LATERAL FLATTEN(input => u.RAW_JSON:documentos, outer => TRUE) d
WHERE d.value IS NOT NULL;
