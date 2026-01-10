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

# Tablas a migrar organizadas por grupos
TABLES_CONFIG = {
    # GRUPO 1: Tablas de la Procedure AT_GETTRAZA (Documentos)
    # Nota: columnas en MAYÚSCULAS porque así se almacenan en ClickHouse
    "procedure_tables": [
        {"name": "CabeFacV", "incremental_column": "FECHA", "type": "datetime"},
        {"name": "LineFact", "incremental_column": "IDFACV", "type": "id"},
        {"name": "CabeAlbV", "incremental_column": "FECHA", "type": "datetime"},
        {"name": "CabeAlbC", "incremental_column": "FECHA", "type": "datetime"},
        {"name": "LineAlba", "incremental_column": "IDALBV", "type": "id"},
        {"name": "__CabeDepV", "incremental_column": "FECHA", "type": "datetime"},
        {"name": "__CabeDepC", "incremental_column": "FECHA", "type": "datetime"},
        {"name": "__LineDepo", "incremental_column": "IDDEPV", "type": "id"},
        {"name": "CabeTras", "incremental_column": "FECHA", "type": "datetime"},
        {"name": "LineTras", "incremental_column": "IDTRA", "type": "id"},
        {"name": "CabeRegu", "incremental_column": "FECHA", "type": "datetime"},
        {"name": "LineRegu", "incremental_column": "IDREG", "type": "id"},
        {"name": "CabeInve", "incremental_column": "FECHA", "type": "datetime"},
        {"name": "LineInve", "incremental_column": "IDINVEN", "type": "id"},
    ],
    
    # GRUPO 2: Tablas Maestras (carga completa siempre)
    "master_tables": [
        {"name": "Articulo", "incremental_column": None, "type": "full"},
        {"name": "Almacen", "incremental_column": None, "type": "full"},
        {"name": "AT_TIPOARTICULO", "incremental_column": None, "type": "full"},
        {"name": "AT_CALIBRES", "incremental_column": None, "type": "full"},
        {"name": "AT_CALIBRES_REL", "incremental_column": None, "type": "full"},
    ],
    
    # GRUPO 3: Tablas de Producción y Stock
    "production_tables": [
        {"name": "AT_PRODUCCION", "incremental_column": "IDPRODUCCION", "type": "id"},
        {"name": "AT_PRODUCCIONES", "incremental_column": "IDALBC", "type": "id"},
        {"name": "AT_STOCK", "incremental_column": "ID", "type": "id"},
        {"name": "AT_STOCK_IDENTIFICADOR", "incremental_column": "ID", "type": "id"},
        {"name": "STOCKALM", "incremental_column": "ID", "type": "id"},
        {"name": "AT_SUBPRODUCTO", "incremental_column": "IDSUBPRODUCTO", "type": "id"},
        {"name": "AT_TRASPASOS", "incremental_column": "IDTRASPASO", "type": "id"},
        {"name": "AT_TRAZABILIDAD", "incremental_column": None, "type": "full"},
        {"name": "AT_TRANSACCION_DET", "incremental_column": "ID_DETALLE", "type": "id"},
        {"name": "AT_IDENTIFICADORES_DET", "incremental_column": "IDIDENTIFICADOR", "type": "id"},
    ],
    
    # GRUPO 4: Tablas de Relaciones
    "relation_tables": [
        {"name": "AT_REGISTROS_REL", "incremental_column": None, "type": "full"},
        {"name": "VINCULOS", "incremental_column": None, "type": "full"},
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
