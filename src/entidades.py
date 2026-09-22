# -*- coding: utf-8 -*-
"""
entidades.py — REGISTRO DECLARATIVO de todo lo que se extrae
============================================================
Proyecto: cargadirecta_snowflake

Esta es la pieza central del diseño: **un solo diccionario** describe las ~24
entidades. Nadie escribe 24 scripts parecidos; el motor (`extraer.py`) lee de
aquí. Agregar una entidad nueva = agregar una entrada.

Cada entrada declara:
    endpoint          ruta de la API (sin '/')
    tabla             tabla destino RAW_ERP_*
    tipo              'catalogo' (pull completo)  |  'hecho' (por ventana+empresa)
    llaves            columnas del MERGE
    page_size         filas por página (FASE 0: cfdi=1000, pago=500, resto=100)
    params            parámetros fijos de la llamada
    requiere_empresa  True  -> se itera por las 50 empresas (la API lo exige)
    campo_delta       (ini, fin) para el sync incremental; None si no lo soporta
    mapa              {COLUMNA_SNOWFLAKE: 'campo_api'}  aplanado nivel 1
    num / fec / int   qué columnas convertir (el resto va como texto)
    soft_delete       marcar ACTIVO=FALSE lo que ya no vino (solo en pull completo)
"""
from __future__ import annotations

# --------------------------------------------------------------------------- #
#  CATÁLOGOS — pull completo, MERGE por hash, soft-delete permitido
# --------------------------------------------------------------------------- #
CATALOGOS = {

    "empresa": dict(
        endpoint="empresa", tabla="RAW_ERP_EMPRESA", tipo="catalogo",
        llaves=["ID_EMPRESA"], page_size=1000, params={}, requiere_empresa=False,
        campo_delta=None, soft_delete=True, esperado=50,
        mapa={
            "ID_EMPRESA": "id_empresa", "ALIAS": "alias",
            "RAZON_SOCIAL": "razon_social", "RFC": "rfc",
            "TIPO_PERSONA": "tipo_persona", "REGIMEN_SOCIETARIO": "regimen_societario",
            "ESTATUS_MODULO": "estatus_modulo", "ID_EXTERNO": "id_externo",
            "DOMICILIO_FISCAL_PRINCIPAL": "domicilio_fiscal_principal",
        },
        num=[], fec=[], int_=[],
    ),

    "propietario": dict(
        # OJO: este endpoint NO tiene fecha_*_modificacion -> siempre completo.
        endpoint="propietario", tabla="RAW_ERP_PROPIETARIO", tipo="catalogo",
        llaves=["ID_PROPIETARIO"], page_size=1000, params={}, requiere_empresa=False,
        campo_delta=None, soft_delete=True, esperado=118,
        mapa={
            "ID_PROPIETARIO": "id_propietario", "ALIAS": "alias",
            "RAZON_SOCIAL": "razon_social", "RFC": "rfc",
            "TIPO_PERSONA": "tipo_persona",
        },
        num=[], fec=[], int_=[],
    ),

    "inmueble": dict(
        endpoint="inmueble", tabla="RAW_ERP_INMUEBLE", tipo="catalogo",
        llaves=["ID_INMUEBLE"], page_size=500, params={}, requiere_empresa=False,
        campo_delta=("fecha_inicio_modificacion", "fecha_fin_modificacion"),
        soft_delete=True, esperado=334,
        mapa={
            "ID_INMUEBLE": "id_inmueble", "INMUEBLE": "inmueble", "FOLIO": "folio",
            "TIPO_INMUEBLE": "tipo_inmueble", "CLAVE_CATASTRAL": "clave_catastral",
            "CUENTA_PREDIAL": "cuenta_predial", "ID_EXTERNO": "id_externo",
            "ESTATUS": "estatus", "ADMINISTRADOR": "administrador",
            "CENTRO_COSTOS": "centro_costos", "CALLE": "calle",
            "NO_EXTERIOR": "no_exterior", "NO_INTERIOR": "no_interior",
            "COLONIA": "colonia", "CODIGO_POSTAL": "codigo_postal",
            "CIUDAD": "ciudad", "ESTADO": "estado", "PAIS": "pais",
            "ZONA": "zona", "REGION": "region", "PLAZA": "plaza",
            "M2_RENTABLES": "m2_rentables", "M2_CONSTRUCCION": "m2_construccion",
            "M2_TERRENO": "m2_terreno", "PRECIO_RENTA": "precio_renta",
            "PRECIO_M2_RENTA": "precio_m2_renta", "MERCADO": "mercado",
            "CATASTRO": "catastro", "ADQUISICION": "adquisicion",
            "FECHA_ADQUISICION": "fecha_adquisicion",
            "SEGMENTO_NEGOCIO": "segmento_negocio",
        },
        num=["M2_RENTABLES", "M2_CONSTRUCCION", "M2_TERRENO", "PRECIO_RENTA",
             "PRECIO_M2_RENTA", "MERCADO", "CATASTRO", "ADQUISICION"],
        fec=["FECHA_ADQUISICION"], int_=[],
    ),

    "unidad": dict(
        endpoint="unidad", tabla="RAW_ERP_UNIDAD", tipo="catalogo",
        llaves=["ID_UNIDAD"], page_size=500, params={}, requiere_empresa=False,
        campo_delta=("fecha_inicio_modificacion", "fecha_fin_modificacion"),
        soft_delete=True, esperado=2711,
        mapa={
            "ID_UNIDAD": "id_unidad", "UNIDAD": "unidad", "INMUEBLE": "inmueble",
            "USO": "uso", "ESTATUS": "estatus", "CUENTA_PREDIAL": "cuenta_predial",
            "CLAVE_CATASTRAL": "clave_catastral", "PRECIO_RENTA": "precio_renta",
            "PRECIO_M2_RENTA": "precio_m2_renta",
            "PRECIO_MANTENIMIENTO": "precio_mantenimiento",
            "PRECIO_DEPOSITO_GARANTIA": "precio_deposito_garantia",
            "CALLE": "calle", "NO_EXTERIOR": "no_exterior",
            "NO_INTERIOR": "no_interior", "COLONIA": "colonia",
            "CODIGO_POSTAL": "codigo_postal", "CIUDAD": "ciudad",
            "ESTADO": "estado", "PAIS": "pais", "ZONA": "zona",
            "REGION": "region", "PLAZA": "plaza",
            "M2_RENTABLES": "m2_rentables", "M2_CONSTRUCCION": "m2_construccion",
            "PRECIO_MERCADO": "precio_mercado", "CATASTRO": "catastro",
            "ADQUISICION": "adquisicion", "FECHA_ADQUISICION": "fecha_adquisicion",
        },
        num=["PRECIO_RENTA", "PRECIO_M2_RENTA", "PRECIO_MANTENIMIENTO",
             "PRECIO_DEPOSITO_GARANTIA", "M2_RENTABLES", "M2_CONSTRUCCION",
             "PRECIO_MERCADO", "CATASTRO", "ADQUISICION"],
        fec=["FECHA_ADQUISICION"], int_=[],
    ),

    "contrato": dict(
        # 4,982 = TODOS los estatus (el portal solo muestra 1,637).
        endpoint="contrato", tabla="RAW_ERP_CONTRATO", tipo="catalogo",
        llaves=["ID_CONTRATO"], page_size=200, params={}, requiere_empresa=False,
        # MEDIDO 2026-08-11: el pull completo (25 páginas) muere con TamanoExcedido
        # por offset profundo. Se trocea por empresa (~100 contratos c/u = 1 página).
        trocear_por="empresa",
        campo_delta=("fecha_inicio_modificacion", "fecha_fin_modificacion"),
        soft_delete=True, esperado=4982,
        mapa={
            "ID_CONTRATO": "id_contrato", "ID_EXTERNO": "id_externo",
            "EMPRESA": "empresa", "ARRENDATARIO": "arrendatario",
            "INMUEBLE": "inmueble", "ID_INMUEBLE": "id_inmueble",
            "UNIDAD": "unidad", "ID_UNIDAD": "id_unidad", "SUCURSAL": "sucursal",
            "DOMICILIO_FISCAL": "domicilio_fiscal", "GIRO": "giro",
            "GRUPO_AVISO": "grupo_aviso", "ESTATUS": "estatus",
            "ESTATUS_ADICIONAL": "estatus_adicional",
            "FECHA_INICIAL": "fecha_inicial", "FECHA_FINAL": "fecha_final",
            "FECHA_FIRMA": "fecha_firma", "FECHA_RENOVACION": "fecha_renovacion",
            "FECHA_ENTREGA": "fecha_entrega", "FECHA_OPERACION": "fecha_operacion",
            "FRECUENCIA": "frecuencia", "MONEDA": "moneda",
            "TIPO_CAMBIO": "tipo_cambio", "SUBTOTAL": "subtotal",
            "DESCUENTO": "descuento", "IVA": "iva",
            "RETENCION_IVA": "retencion_iva", "ISR": "isr", "TOTAL": "total",
        },
        num=["TIPO_CAMBIO", "SUBTOTAL", "DESCUENTO", "IVA", "RETENCION_IVA",
             "ISR", "TOTAL"],
        fec=["FECHA_INICIAL", "FECHA_FINAL", "FECHA_FIRMA", "FECHA_RENOVACION",
             "FECHA_ENTREGA", "FECHA_OPERACION"],
        int_=[],
    ),

    "arrendatario": dict(
        endpoint="arrendatario", tabla="RAW_ERP_ARRENDATARIO", tipo="catalogo",
        llaves=["ID_ARRENDATARIO"], page_size=200, params={}, requiere_empresa=False,
        campo_delta=("fecha_inicio_modificacion", "fecha_fin_modificacion"),
        soft_delete=True, esperado=4989,
        mapa={
            "ID_ARRENDATARIO": "id_arrendatario", "ALIAS": "alias",
            "RAZON_SOCIAL": "razon_social", "RFC": "rfc",
            "TIPO_PERSONA": "tipo_persona", "MEDIO_CONTACTO": "medio_contacto",
            "ESTATUS_MODULO": "estatus_modulo",
            "CONTACTO_PRINCIPAL": "contacto_principal",
            "SUCURSAL_PRINCIPAL": "sucursal_principal", "FECHA": "fecha",
        },
        num=[], fec=["FECHA"], int_=[],
        # derivado: ¿trae ficha PLD? (~3.5% la tienen)
        derivados={"TIENE_FICHA_PLD": lambda r: bool(r.get("pld_entidad"))},
    ),

    "sucursal_arrendatario": dict(
        # Versión PLANA: NO trae codigo_postal (por eso el CP salía en 0%).
        endpoint="sucursal_arrendatario", tabla="RAW_ERP_SUCURSAL_ARRENDATARIO",
        tipo="catalogo", llaves=["ID_SUCURSAL"], page_size=500, params={},
        requiere_empresa=False, campo_delta=None, soft_delete=True, esperado=5355,
        mapa={
            "ID_SUCURSAL": "id_sucursal", "ARRENDATARIO": "arrendatario",
            "RFC_ARRENDATARIO": "rfc_arrendatario", "ALIAS": "alias",
            "ID_EXTERNO_SUCURSAL": "id_externo_sucursal",
        },
        num=[], fec=[], int_=[],
    ),

    # ----------------------------------------------------------------------- #
    #  CATÁLOGOS MENORES
    #
    #  ⚠ NOTA SOBRE EL APLANADO CORTO: estos endpoints devuelven schema `Empty`
    #  en el swagger (10 de los 28 GET), así que los campos reales solo se
    #  conocen por sonda en vivo. Se aplana lo que se sabe seguro y TODO LO DEMÁS
    #  QUEDA EN RAW_JSON.
    #
    #  Esto NO pierde información: el JSON entra íntegro y navegable
    #  (`RAW_JSON:campo`). Cuando veamos los datos reales podremos ampliar el
    #  aplanado con un ALTER TABLE + UPDATE desde el propio VARIANT — sin volver
    #  a llamar a la API. Ese es justo el motivo de guardar el JSON completo.
    # ----------------------------------------------------------------------- #

    "proveedor": dict(
        endpoint="proveedor", tabla="RAW_ERP_PROVEEDOR", tipo="catalogo",
        llaves=["ID_PROVEEDOR"], page_size=200, params={}, requiere_empresa=False,
        campo_delta=("fecha_inicio_modificacion", "fecha_fin_modificacion"),
        soft_delete=True, esperado=None,
        mapa={"ID_PROVEEDOR": "id_proveedor", "ALIAS": "alias",
              "RAZON_SOCIAL": "razon_social", "RFC": "rfc",
              "ID_EXTERNO": "id_externo"},
        num=[], fec=[], int_=[],
    ),

    "direccion_fiscal": dict(
        endpoint="direccion_fiscal", tabla="RAW_ERP_DIRECCION_FISCAL",
        tipo="catalogo", llaves=["ID_DIRECCION_FISCAL"], page_size=200, params={},
        requiere_empresa=False, campo_delta=None, soft_delete=True, esperado=None,
        mapa={"ID_DIRECCION_FISCAL": "id_direccion_fiscal", "ALIAS": "alias",
              "PROVEEDOR": "proveedor", "RFC_PROVEEDOR": "rfc_proveedor",
              "ID_EXTERNO": "id_externo"},
        num=[], fec=[], int_=[],
    ),

    "cuenta_bancaria": dict(
        # /cuenta_bancaria filtra por empresa/numero/alias -> se trocea por empresa
        endpoint="cuenta_bancaria", tabla="RAW_ERP_CUENTA_BANCARIA",
        tipo="catalogo", llaves=["ID_CUENTA_BANCARIA"], page_size=200, params={},
        requiere_empresa=False, trocear_por="empresa",
        campo_delta=None, soft_delete=True, esperado=None,
        mapa={"ID_CUENTA_BANCARIA": "id_cuenta_bancaria", "ALIAS": "alias",
              "EMPRESA": "empresa", "NUMERO_CUENTA": "numero_cuenta",
              "BANCO": "banco"},
        num=[], fec=[], int_=[],
    ),

    "cuenta_contable": dict(
        endpoint="cuenta_contable", tabla="RAW_ERP_CUENTA_CONTABLE",
        tipo="catalogo", llaves=["CUENTA_CONTABLE"], page_size=200, params={},
        requiere_empresa=False, campo_delta=None, soft_delete=True, esperado=None,
        mapa={"CUENTA_CONTABLE": "cuenta_contable", "NOMBRE": "nombre"},
        num=[], fec=[], int_=[],
    ),

    "centro_costos": dict(
        endpoint="centro_costos", tabla="RAW_ERP_CENTRO_COSTOS",
        tipo="catalogo", llaves=["ID_CENTRO_COSTOS"], page_size=200, params={},
        requiere_empresa=False, campo_delta=None, soft_delete=True, esperado=None,
        mapa={"ID_CENTRO_COSTOS": "id_centro_costos", "NOMBRE": "nombre"},
        num=[], fec=[], int_=[],
    ),

    "concepto": dict(
        endpoint="concepto", tabla="RAW_ERP_CONCEPTO", tipo="catalogo",
        llaves=["ID_CONCEPTO"], page_size=200, params={}, requiere_empresa=False,
        campo_delta=None, soft_delete=True, esperado=None,
        mapa={"ID_CONCEPTO": "id_concepto", "ALIAS": "alias",
              "ID_EXTERNO": "id_externo"},
        num=[], fec=[], int_=[],
    ),

    "producto": dict(
        endpoint="producto", tabla="RAW_ERP_PRODUCTO", tipo="catalogo",
        llaves=["PRODUCTO"], page_size=200, params={}, requiere_empresa=False,
        campo_delta=None, soft_delete=True, esperado=None,
        mapa={"PRODUCTO": "producto", "ID_EXTERNO": "id_externo"},
        num=[], fec=[], int_=[],
    ),

    # AVISOS PLD ya presentados, con la URL de su XML en S3.
    # Agregado 2026-08-27 (gap analysis, doc 11 §1). El endpoint existia desde
    # siempre y nunca se habia extraido; la referencia decia "SIN registros" y
    # era falso: hay 7 avisos de HPI (jul-sep 2025).
    # ⚠ NO pasar `empresa`: con ese parametro la API devuelve otra forma y
    #   revienta el parser ('str' object has no attribute 'get'). Pull completo.
    "pld": dict(
        endpoint="pld", tabla="RAW_ERP_PLD_AVISO", tipo="catalogo",
        llaves=["ID_AVISO"], page_size=200, params={}, requiere_empresa=False,
        campo_delta=None, soft_delete=True, esperado=None,
        mapa={"ID_AVISO": "id_aviso", "FECHA": "fecha", "EMPRESA": "empresa",
              "ESTATUS": "estatus", "DOCUMENTOS": "documentos"},
        num=[], fec=["FECHA"], int_=[],
    ),
}

# --------------------------------------------------------------------------- #
#  HECHOS — por (empresa, ventana). MUTAN -> MERGE por llave + hash, sin soft-delete
# --------------------------------------------------------------------------- #
HECHOS = {

    "cfdi": dict(
        llave_api="id_cfdi",
        endpoint="cfdi", tabla="RAW_ERP_CFDI", tipo="hecho",
        llaves=["ID_CFDI"], page_size=100, ventana_dias=1,
        # MEDIDO 2026-08-11: con partidas, page_size=1000 -> 504/TamanoExcedido.
        # 100 + ventana de 1 dia = 0 errores. El 1000 de FASE 0 era SIN partidas.
        params={"incluir_partidas": "true", "incluir_xml_code": "false"},
        requiere_empresa=True,
        campo_delta=("fecha_inicio_modificacion", "fecha_fin_modificacion"),
        soft_delete=False, esperado=241855,
        mapa={
            "ID_CFDI": "id_cfdi", "FOLIO_FISCAL": "folio_fiscal",
            "EMPRESA": "empresa", "RFC_EMISOR": "rfc_emisor",
            "RAZON_EMISOR": "razon_emisor", "ARRENDATARIO": "arrendatario",
            "ID_ARRENDATARIO": "id_arrendatario", "RFC_RECEPTOR": "rfc_receptor",
            "RAZON_RECEPTOR": "razon_receptor", "SUCURSAL": "sucursal",
            "DOMICILIO_FISCAL": "domicilio_fiscal", "INMUEBLE": "inmueble",
            "ID_INMUEBLE": "id_inmueble", "UNIDAD": "unidad",
            "ID_UNIDAD": "id_unidad", "CONTRATO": "contrato",
            "FECHA": "fecha", "FECHA_LIMITE": "fecha_limite",
            "FECHA_TIMBRE": "fecha_timbre", "FECHA_CANCELACION": "fecha_cancelacion",
            "ESTATUS_DOCUMENTO": "estatus_documento",
            "TIPO_DOCUMENTO": "tipo_documento", "TIPO_COMPROBANTE": "tipo_comprobante",
            "ESTATUS_TIMBRE": "estatus_timbre", "ESTATUS_VARIABLE": "estatus_variable",
            "ESTATUS_CONECTOR": "estatus_conector", "SERIE": "serie", "FOLIO": "folio",
            "FORMA_PAGO": "forma_pago", "METODO_PAGO": "metodo_pago",
            "USO_CFDI": "uso_cfdi", "CONDICION_PAGO": "condicion_pago",
            "NUMERO_PARCIALIDAD": "numero_parcialidad", "MONEDA": "moneda",
            "TIPO_CAMBIO": "tipo_cambio", "SUBTOTAL": "subtotal",
            "DESCUENTO": "descuento", "IVA": "iva", "RETENCION_IVA": "retencion_iva",
            "ISR": "isr", "TOTAL": "total", "SALDO": "saldo",
            "TOTAL_MONEDA_BASE": "total_moneda_base",
            "SALDO_MONEDA_BASE": "saldo_moneda_base", "NOTA_CREDITO": "nota_credito",
        },
        num=["TIPO_CAMBIO", "SUBTOTAL", "DESCUENTO", "IVA", "RETENCION_IVA", "ISR",
             "TOTAL", "SALDO", "TOTAL_MONEDA_BASE", "SALDO_MONEDA_BASE", "NOTA_CREDITO"],
        fec=["FECHA", "FECHA_LIMITE", "FECHA_TIMBRE", "FECHA_CANCELACION"],
        int_=["NUMERO_PARCIALIDAD"],
        derivados={
            "ANIO": lambda r: _anio(r.get("fecha")),
            "MES":  lambda r: _mes(r.get("fecha")),
            "NUM_PARTIDAS": lambda r: len(r.get("partida") or []),
        },
        # PLAN B para los días que la API NO puede entregar juntos.
        #
        # MEDIDO 2026-08-17: hay CFDI que por sí solos exceden el límite de la
        # API (id_cfdi=472620 -> 4,980 partidas). Con incluir_partidas=true
        # revienta incluso pidiendo no_registros_x_pagina=1, así que NINGÚN
        # page_size sirve. Eso perdió 1,128 días en silencio.
        #
        # El rescate pide la cabecera sin anidados y las partidas por /partida
        # con id_cfdi, y reensambla el registro con la MISMA forma. Equivalencia
        # verificada campo por campo (ver `_iter_rescate` en erp_client.py).
        rescate=dict(
            endpoint="partida",        # de dónde salen los hijos
            param_padre="id_cfdi",     # filtro del hijo == campo id del padre
            clave="partida",           # dónde se pegan dentro del JSON padre
            llave_hijo="id_partida",   # id del hijo (para la lectura segura)
            hereda_params=("empresa",),  # /partida exige `empresa`
            page_size=500, max_page_size=1000,
        ),
        # Las partidas viajan ANIDADAS en el mismo JSON (incluir_partidas=true):
        # se materializan en la MISMA pasada, sin llamar /partida aparte.
        # `hereda` copia columnas ya convertidas de la fila PADRE.
        hijos=[dict(
            clave="partida", tabla="RAW_ERP_CFDI_PARTIDA",
            llaves=["ID_PARTIDA"],
            hereda={"ID_CFDI": "ID_CFDI", "EMPRESA": "EMPRESA",
                    "FOLIO_FISCAL": "FOLIO_FISCAL", "FECHA": "FECHA"},
            mapa={
                "ID_PARTIDA": "id_partida", "DESCRIPCION": "descripcion",
                "CONCEPTO": "concepto", "ID_CONCEPTO": "id_concepto",
                "TIPO_CONCEPTO": "tipo_concepto",
                "PRODUCTO_SERVICIO": "producto_servicio",
                "UNIDAD_MEDIDA": "unidad_medida", "CUENTA_PREDIAL": "cuenta_predial",
                "VALOR_UNITARIO": "valor_unitario", "CANTIDAD": "cantidad",
                "DESCUENTO": "descuento", "SUBTOTAL": "subtotal", "IVA": "iva",
                "ISR": "isr", "RETENCION_IVA": "retencion_iva", "TOTAL": "total",
                "SALDO": "saldo", "ESTATUS_VARIABLE": "estatus_variable",
                "MINIMO_VARIABLE": "minimo_variable",
                "PORCENTAJE_VARIABLE": "porcentaje_variable",
            },
            num=["VALOR_UNITARIO", "CANTIDAD", "DESCUENTO", "SUBTOTAL", "IVA", "ISR",
                 "RETENCION_IVA", "TOTAL", "SALDO", "MINIMO_VARIABLE",
                 "PORCENTAJE_VARIABLE"],
            fec=[], int_=[],
        )],
    ),

    "pago": dict(
        llave_api="id_cfdi",
        endpoint="pago", tabla="RAW_ERP_PAGO", tipo="hecho",
        llaves=["ID_CFDI"], page_size=100, ventana_dias=1,   # 500+ revienta ("excede")
        params={"incluir_movimiento_bancario": "true"},
        requiere_empresa=True,
        campo_delta=("fecha_inicio_modificacion", "fecha_fin_modificacion"),
        soft_delete=False, esperado=177730,
        mapa={
            "ID_CFDI": "id_cfdi", "FOLIO_FISCAL": "folio_fiscal", "EMPRESA": "empresa",
            "RFC_EMISOR": "rfc_emisor", "RAZON_EMISOR": "razon_emisor",
            "ARRENDATARIO": "arrendatario", "RFC_RECEPTOR": "rfc_receptor",
            "RAZON_RECEPTOR": "razon_receptor", "SUCURSAL": "sucursal",
            "DOMICILIO_FISCAL": "domicilio_fiscal", "INMUEBLE": "inmueble",
            "ID_INMUEBLE": "id_inmueble", "UNIDAD": "unidad", "ID_UNIDAD": "id_unidad",
            "CONTRATO": "contrato", "FECHA": "fecha", "FECHA_TIMBRE": "fecha_timbre",
            "FECHA_CANCELACION": "fecha_cancelacion",
            "ESTATUS_DOCUMENTO": "estatus_documento", "TIPO_DOCUMENTO": "tipo_documento",
            "ESTATUS_TIMBRE": "estatus_timbre", "ESTATUS_CONECTOR": "estatus_conector",
            "SERIE": "serie", "FOLIO": "folio", "USO_CFDI": "uso_cfdi",
            "TIPO_CAMBIO": "tipo_cambio",
        },
        num=["TIPO_CAMBIO"],
        fec=["FECHA", "FECHA_TIMBRE", "FECHA_CANCELACION"], int_=[],
        derivados={
            "ANIO": lambda r: _anio(r.get("fecha")),
            "MES":  lambda r: _mes(r.get("fecha")),
            "NUM_ABONOS": lambda r: len(r.get("pago") or []),
        },
        # pago[] = los ABONOS; y dentro de cada abono,
        # documento_relacionado_pago[] = a QUE FACTURA se aplico (NIETO, 2 niveles).
        hijos=[dict(
            clave="pago", tabla="RAW_ERP_PAGO_ABONO", llaves=["ID_PAGO"],
            hereda={"ID_CFDI": "ID_CFDI", "EMPRESA": "EMPRESA",
                    "FOLIO_FISCAL_REP": "FOLIO_FISCAL"},
            mapa={
                "ID_PAGO": "id_pago", "FECHA": "fecha", "HORA": "hora",
                "MONTO": "monto", "MONEDA": "moneda", "TIPO_CAMBIO": "tipo_cambio",
                "FORMA_PAGO": "forma_pago", "TIPO_PAGO": "tipo_pago",
                "ESTATUS_PAGO": "estatus_pago", "ESTATUS_TIMBRE": "estatus_timbre",
                "NUMERO_OPERACION": "numero_operacion",
                "CUENTA_ORIGEN": "cuenta_origen", "BANCO_ORIGEN": "banco_origen",
                "CUENTA_BENEFICIARIO": "cuenta_beneficiario",
                "BANCO_BENEFICIARIO": "banco_beneficiario",
                "ID_MOVIMIENTO_BANCARIO": "id_movimiento_bancario",
            },
            num=["MONTO", "TIPO_CAMBIO"], fec=["FECHA"], int_=[],
            hijos=[dict(
                clave="documento_relacionado_pago",
                tabla="RAW_ERP_PAGO_DOC_RELACIONADO",
                llaves=["ID_DOC_RELACIONADO"],
                hereda={"ID_PAGO": "ID_PAGO", "EMPRESA": "EMPRESA"},
                mapa={
                    "ID_DOC_RELACIONADO": "id_documento_relacionado_pago",
                    "ID_CFDI_PAGANDO": "id_cfdi_pagando",
                    "FOLIO_FISCAL": "folio_fiscal", "SERIE": "serie", "FOLIO": "folio",
                    "METODO_PAGO": "metodo_pago",
                    "NUMERO_PARCIALIDAD": "numero_parcialidad",
                    "TIPO_CAMBIO": "tipo_cambio", "SALDO_ANTERIOR": "saldo_anterior",
                    "IMPORTE_PAGADO": "importe_pagado",
                    "IMPORTE_INSOLUTO": "importe_insoluto",
                    "IVA_DOC_RELACIONADO": "iva_documento_relacionado_pago",
                },
                num=["TIPO_CAMBIO", "SALDO_ANTERIOR", "IMPORTE_PAGADO",
                     "IMPORTE_INSOLUTO", "IVA_DOC_RELACIONADO"],
                fec=[], int_=["NUMERO_PARCIALIDAD"],
            )],
        )],
    ),

    "movimiento_bancario": dict(
        llave_api="id_movimiento_bancario",
        endpoint="movimiento_bancario", tabla="RAW_ERP_MOVIMIENTO_BANCARIO",
        tipo="hecho", llaves=["ID_MOVIMIENTO_BANCARIO"], page_size=200, ventana_dias=7,
        params={"incluir_detalle_pago": "true"}, requiere_empresa=True,
        campo_delta=None, soft_delete=False, esperado=177733,
        mapa={
            "ID_MOVIMIENTO_BANCARIO": "id_movimiento_bancario", "EMPRESA": "empresa",
            "ARRENDATARIO": "arrendatario", "SUCURSAL": "sucursal",
            "DOMICILIO_FISCAL": "domicilio_fiscal", "FECHA": "fecha",
            "MONTO": "monto", "MONEDA": "moneda", "TIPO_CAMBIO": "tipo_cambio",
            "ESTATUS": "estatus", "ESTATUS_CONECTOR": "estatus_conector",
            "CUENTA": "cuenta", "BANCO": "banco", "REFERENCIA": "referencia",
        },
        num=["MONTO", "TIPO_CAMBIO"], fec=["FECHA"], int_=[],
        derivados={"ANIO": lambda r: _anio(r.get("fecha")),
                   "MES":  lambda r: _mes(r.get("fecha"))},
    ),

    "gasto": dict(
        llave_api="id_gasto",
        endpoint="gasto", tabla="RAW_ERP_GASTO", tipo="hecho",
        llaves=["ID_GASTO"], page_size=200, ventana_dias=30, params={}, requiere_empresa=True,
        campo_delta=None, soft_delete=False, esperado=51,   # sin fecha_modificacion
        mapa={
            "ID_GASTO": "id_gasto", "EMPRESA": "empresa", "PROVEEDOR": "proveedor",
            "TIPO_GASTO": "tipo_gasto", "NUMERO_FACTURA": "numero_factura",
            "UUID": "uuid", "INMUEBLE": "inmueble", "ID_INMUEBLE": "id_inmueble",
            "UNIDAD": "unidad", "ID_UNIDAD": "id_unidad",
            "ADMINISTRADOR": "administrador", "FECHA": "fecha",
            "FECHA_TIMBRE": "fecha_timbre", "FECHA_CANCELACION": "fecha_cancelacion",
            "ESTATUS": "estatus", "ESTATUS_TIMBRE": "estatus_timbre",
            "MONEDA": "moneda", "TIPO_CAMBIO": "tipo_cambio",
            "SUBTOTAL": "subtotal", "IVA": "iva", "TOTAL": "total",
        },
        num=["TIPO_CAMBIO", "SUBTOTAL", "IVA", "TOTAL"],
        fec=["FECHA", "FECHA_TIMBRE", "FECHA_CANCELACION"], int_=[],
        derivados={"ANIO": lambda r: _anio(r.get("fecha")),
                   "MES":  lambda r: _mes(r.get("fecha"))},
    ),
}

HECHOS.update({

    "pago_gasto": dict(
        llave_api="id_pago_gasto",
        endpoint="pago_gasto", tabla="RAW_ERP_PAGO_GASTO", tipo="hecho",
        llaves=["ID_PAGO_GASTO"], page_size=200, ventana_dias=30, params={},
        requiere_empresa=True, campo_delta=None, soft_delete=False, esperado=0,
        mapa={"ID_PAGO_GASTO": "id_pago_gasto", "EMPRESA": "empresa",
              "PROVEEDOR": "proveedor", "FECHA": "fecha", "MONTO": "monto",
              "MONEDA": "moneda", "FORMA_PAGO": "forma_pago",
              "ESTATUS_PAGO": "estatus_pago", "ESTATUS_TIMBRE": "estatus_timbre"},
        num=["MONTO"], fec=["FECHA"], int_=[],
        derivados={"ANIO": lambda r: _anio(r.get("fecha")),
                   "MES": lambda r: _mes(r.get("fecha"))},
    ),

    "orden_trabajo": dict(
        # NO requiere empresa (filtra por inmueble/proveedor/etc.) -> pull completo.
        # Máx 50/página si se envía `buscar`; sin `buscar` admite más.
        llave_api="id_orden_trabajo",
        endpoint="orden_trabajo", tabla="RAW_ERP_ORDEN_TRABAJO",
        tipo="catalogo", llaves=["ID_ORDEN_TRABAJO"], page_size=100, params={},
        requiere_empresa=False, campo_delta=None, soft_delete=True, esperado=None,
        mapa={"ID_ORDEN_TRABAJO": "id_orden_trabajo", "INMUEBLE": "inmueble",
              "ID_INMUEBLE": "id_inmueble", "UNIDAD": "unidad",
              "ID_UNIDAD": "id_unidad", "PROVEEDOR": "proveedor",
              "SUPERVISOR": "supervisor", "RESPONSABLE": "responsable",
              "TIPO_MANTENIMIENTO": "tipo_mantenimiento", "CATEGORIA": "categoria",
              "EQUIPO": "equipo", "ESTATUS": "estatus", "FECHA": "fecha"},
        num=[], fec=["FECHA"], int_=[],
        derivados={"ANIO": lambda r: _anio(r.get("fecha")),
                   "MES": lambda r: _mes(r.get("fecha"))},
    ),

    "actualizacion": dict(
        # Sonda 2026-07-28: tipo="Valor comercial" vino VACIO. Tipos validos
        # confirmados: Predial, Luz, Afluencia, Acercamiento, Amortizacion...
        # Se recorren varios tipos porque el parametro NO acepta lista.
        endpoint="actualizacion", tabla="RAW_ERP_ACTUALIZACION",
        tipo="catalogo", llaves=["ID_ACTUALIZACION"], page_size=200, params={},
        requiere_empresa=False, campo_delta=None, soft_delete=False, esperado=None,
        trocear_por="tipo",
        tipos=["Predial", "Valor comercial", "Luz", "Agua", "Afluencia",
               "Acercamiento", "Amortizacion"],
        mapa={"ID_ACTUALIZACION": "id_actualizacion", "TIPO": "tipo",
              "FECHA": "fecha", "VALOR": "valor", "INMUEBLE": "inmueble",
              "UNIDAD": "unidad"},
        num=["VALOR"], fec=["FECHA"], int_=[],
    ),
})

ENTIDADES = {**CATALOGOS, **HECHOS}


# --------------------------------------------------------------------------- #
#  helpers de derivados
# --------------------------------------------------------------------------- #
def _anio(v):
    s = str(v or "")[:4]
    return int(s) if s.isdigit() else None


def _mes(v):
    s = str(v or "")
    return int(s[5:7]) if len(s) >= 7 and s[5:7].isdigit() else None
