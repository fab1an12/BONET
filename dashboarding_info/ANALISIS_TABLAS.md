# 📊 Análisis de Tablas para el Dashboard de Trazabilidad y Producción

## Resumen Ejecutivo

Tras analizar la base de datos **BONETV9**, he identificado las tablas necesarias para:
1. La procedure `AT_GETTRAZA` (trazabilidad documental)
2. Responder las preguntas del dashboard (producción, lotes, calibres, stock)

---

## 🎯 Lista Definitiva de Tablas a Migrar a ClickHouse

### **GRUPO 1: Tablas de la Procedure AT_GETTRAZA (Trazabilidad Documental)**

| Tabla | Registros | Descripción | Columna Fecha |
|-------|-----------|-------------|---------------|
| `CabeFacV` | - | Cabecera Facturas Venta | `Fecha` |
| `LineFact` | - | Líneas de Facturas | - |
| `CabeAlbV` | - | Cabecera Albaranes Venta | `Fecha` |
| `CabeAlbC` | - | Cabecera Albaranes Compra | `Fecha` |
| `LineAlba` | - | Líneas de Albaranes (Venta/Compra) | - |
| `__CabeDepV` | - | Cabecera Depósitos Venta | `Fecha` |
| `__CabeDepC` | - | Cabecera Depósitos Compra | `Fecha` |
| `__LineDepo` | - | Líneas de Depósitos | - |
| `CabeTras` | - | Cabecera Traspasos | `Fecha` |
| `LineTras` | - | Líneas de Traspasos | - |
| `CabeRegu` | - | Cabecera Regularizaciones | `Fecha` |
| `LineRegu` | - | Líneas de Regularizaciones | - |
| `CabeInve` | - | Cabecera Inventarios | `Fecha` |
| `LineInve` | - | Líneas de Inventarios | - |

### **GRUPO 2: Tablas Maestras**

| Tabla | Registros | Descripción | Carga |
|-------|-----------|-------------|-------|
| `Articulo` | ~5,000 | Maestro de artículos (productos, MP, cajas) | Completa |
| `Almacen` | 25 | Maestro de almacenes y tolvas | Completa |
| `AT_TIPOARTICULO` | 6 | Tipos: CAJA, MA, MP, PALET, PROD, SERV | Completa |
| `AT_CALIBRES` | 8 | Calibres: Platillo, Mitjana, Grossa, Carril, Rebuig 1/2/3, Residual | Completa |

### **GRUPO 3: Tablas de Producción y Stock (CLAVE PARA EL DASHBOARD)**

| Tabla | Registros | Descripción | Columna Incremental |
|-------|-----------|-------------|---------------------|
| `AT_PRODUCCION` | 103,937 | Producciones por lote | `IDPRODUCCION`, `FECHA` |
| `AT_PRODUCCIONES` | 540,237 | Histórico detallado de producciones | `IDALBC`, `FECHA` |
| `AT_STOCK` | 26,130 | Stock actual por lote/almacén | `ID` |
| `AT_STOCK_IDENTIFICADOR` | 17,638 | Stock por identificador/ubicación | `ID` |
| `STOCKALM` | 144,858 | Stock por almacén | `ID` |
| `AT_SUBPRODUCTO` | - | Subproductos generados | `IDSUBPRODUCTO` |
| `AT_TRASPASOS` | - | Traspasos entre tolvas/almacenes | `IDTRANS`, `FECHA` |
| `AT_TRAZABILIDAD` | - | Trazabilidad completa | - |
| `AT_TRANSACCION_DET` | 11,765 | Detalle de transacciones | `ID` |
| `AT_IDENTIFICADORES_DET` | 156,328 | Detalle de identificadores | `IDIDENTIFICADOR` |

### **GRUPO 4: Tablas de Relaciones/Control**

| Tabla | Registros | Descripción |
|-------|-----------|-------------|
| `AT_REGISTROS_REL` | - | Relaciones de registros APPCC |
| `VINCULOS` | - | Vínculos entre documentos |
| `AT_CALIBRES_REL` | - | Relaciones entre calibres |

---

## 📏 Calibres Disponibles (para las preguntas del dashboard)

| ID | Nombre | Prefijo | Descripción según documento |
|----|--------|---------|----------------------------|
| 1 | Platillo | PLA | menos de 45mm |
| 2 | Mitjana | MIT | de 45mm a 50mm |
| 8 | Carril | CAR | de 50mm a 65mm |
| 3 | Grossa | GRO | más de 80mm |
| 4 | Rebuig 1 | RE1 | - |
| 5 | Rebuig 2 | RE2 | No conforme, se puede volver a aprovechar |
| 6 | Rebuig 3 | RE3 | No conforme, NO se puede aprovechar |
| 7 | Residual | R | - |

---

## 🏭 Almacenes/Tolvas Disponibles

| Código | Descripción | Tipo |
|--------|-------------|------|
| 1 | MAGATZEM CENTRAL | Almacén principal |
| T1-T6 | T01 a T06 | Tolvas de producción |
| S1, S2 | xS1, xS2 | Silos |
| CC, CF, OT | zCC, zCF, zOT | Zonas especiales |
| DES | Desecho | Residuos |
| R | Rebuig | Rechazos |
| D, DEV | Devoluciones | Devoluciones |
| TRC1, TRC2 | Tolva Recepción Cebolla | Recepción |

---

## 🔄 Estrategia de Carga Incremental

### Tablas con carga incremental (por fecha/ID):
- **Por FECHA**: `CabeFacV`, `CabeAlbV`, `CabeAlbC`, `CabeTras`, `CabeRegu`, `CabeInve`, `AT_PRODUCCION`, `AT_PRODUCCIONES`, `AT_TRASPASOS`
- **Por ID**: `AT_STOCK`, `AT_STOCK_IDENTIFICADOR`, `STOCKALM`, `AT_TRANSACCION_DET`, `AT_IDENTIFICADORES_DET`

### Tablas con carga completa (maestras pequeñas):
- `Articulo`, `Almacen`, `AT_TIPOARTICULO`, `AT_CALIBRES`, `AT_CALIBRES_REL`

---

## 📈 Mapeo Preguntas Dashboard → Tablas

| Pregunta | Tablas Necesarias |
|----------|-------------------|
| 1. Aprovechamiento del Lote | `AT_PRODUCCION`, `AT_STOCK`, `AT_STOCK_IDENTIFICADOR`, `Almacen` |
| 2. Producción obtenida del lote | `AT_PRODUCCION`, `AT_PRODUCCIONES`, `Articulo` |
| 3. Subproductos por calibre | `AT_SUBPRODUCTO`, `AT_CALIBRES`, `AT_TRANSACCION_DET` |
| 4. Stock en tolva X | `AT_STOCK`, `AT_STOCK_IDENTIFICADOR`, `Almacen` (WHERE CodAlm IN ('T1'-'T6')) |
| 5. Stock en almacén | `AT_STOCK`, `STOCKALM`, `Almacen` (WHERE CodAlm = '1') |
| 6. Gráficos de aprovechamiento % | `AT_PRODUCCION`, `AT_STOCK`, cálculos de porcentaje |

---

## 📋 Total: 28 Tablas a Migrar

```
Grupo 1 (Procedure):     14 tablas
Grupo 2 (Maestras):       4 tablas
Grupo 3 (Producción):    10 tablas
Grupo 4 (Relaciones):     3 tablas (algunas ya contadas)
```

**Nota**: Algunas tablas de líneas (como `LineAlba`, `__LineDepo`) se reutilizan para múltiples cabeceras.
