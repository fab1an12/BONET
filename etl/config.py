"""
Configuración de conexiones a las bases de datos
Soporta variables de entorno para Docker
"""
import os

# SQL Server (origen)
# Soporta instancias nombradas con puerto dinámico via ODBC Driver
SQLSERVER_CONFIG = {
    "server": os.getenv("MSSQL_HOST", "localhost"),
    "port": os.getenv("MSSQL_PORT", ""),  # Vacío para puerto dinámico
    "instance": os.getenv("MSSQL_INSTANCE", ""),  # Ej: A3ERP
    "database": os.getenv("MSSQL_DATABASE", "BONETV9"),
    "user": os.getenv("MSSQL_USER", "SA"),
    "password": os.getenv("MSSQL_PASSWORD", "Passw0rd!"),
    "driver": os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
    "trust_certificate": os.getenv("MSSQL_TRUST_CERT", "yes"),  # Para certificados auto-firmados
}

# ClickHouse (destino)
CLICKHOUSE_CONFIG = {
    "host": os.getenv("CH_HOST", "localhost"),
    "port": int(os.getenv("CH_PORT", 8123)),
    "database": os.getenv("CH_DATABASE", "bonet_analytics"),
    "username": os.getenv("CH_USER", "bonet_user"),
    "password": os.getenv("CH_PASSWORD", "Bonet2024!")
}

# =============================================================================
# TABLAS OPTIMIZADAS PARA EL DASHBOARD DE METABASE
# Solo se cargan las 11 tablas necesarias para los gráficos
# Última actualización: 2026-01-24
# =============================================================================
TABLES_CONFIG = {
    # GRUPO 1: Tablas de Documentos (para info del lote: proveedor, fecha entrada)
    "document_tables": [
        {"name": "CabeAlbC", "incremental_column": "IDALBC", "type": "id"},  # G9: Info Lote (proveedor) - Usa ID, no fecha
        {"name": "LineAlba", "incremental_column": "IDLIN", "type": "id"},         # G9: Líneas albaranes (IDLIN siempre poblado)
    ],
    
    # GRUPO 2: Tablas Maestras (carga completa siempre)
    "master_tables": [
        {"name": "Articulo", "incremental_column": None, "type": "full"},      # G4, G10: Catálogo artículos
        {"name": "Almacen", "incremental_column": None, "type": "full"},       # G2: Catálogo almacenes/tolvas
        {"name": "AT_CALIBRES", "incremental_column": None, "type": "full"},   # G3, G10: Catálogo calibres
    ],
    
    # GRUPO 3: Tablas de Producción y Stock (核心 del dashboard)
    "production_tables": [
        {"name": "AT_PRODUCCION", "incremental_column": "IDPRODUCCION", "type": "id"},     # G6, G7: Aprovechamiento
        {"name": "AT_PRODUCCIONES", "incremental_column": "IDALBC", "type": "id"},         # G4, G8: Producción detalle
        {"name": "AT_STOCK", "incremental_column": None, "type": "full"},                  # G1, G2, G6, G7: Stock actual (full - registros se eliminan)
        {"name": "AT_STOCK_IDENTIFICADOR", "incremental_column": None, "type": "full"},    # G3: Stock con identificador (full - registros se eliminan)
        {"name": "AT_SUBPRODUCTO", "incremental_column": "IDSUBPRODUCTO", "type": "id"},   # G3, G10: Subproductos
        {"name": "AT_TRASPASOS", "incremental_column": "IDTRASPASO", "type": "id"},        # G5: Historial traspasos
    ],
}

def get_all_tables():
    """Obtiene la lista plana de todas las tablas"""
    all_tables = []
    for group in TABLES_CONFIG.values():
        all_tables.extend(group)
    return all_tables


def get_incremental_tables():
    """Obtiene solo las tablas con carga incremental"""
    return [t for t in get_all_tables() if t["type"] != "full"]


def get_full_load_tables():
    """Obtiene solo las tablas con carga completa"""
    return [t for t in get_all_tables() if t["type"] == "full"]
