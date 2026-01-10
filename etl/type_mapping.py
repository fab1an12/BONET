"""
Mapeo de tipos de datos de SQL Server a ClickHouse
"""

# Mapeo de tipos SQL Server -> ClickHouse
TYPE_MAPPING = {
    # Numéricos enteros
    "int": "Int32",
    "bigint": "Int64",
    "smallint": "Int16",
    "tinyint": "UInt8",
    "bit": "UInt8",
    
    # Numéricos decimales
    "decimal": "Decimal(18, 4)",
    "numeric": "Decimal(18, 4)",
    "money": "Decimal(19, 4)",
    "smallmoney": "Decimal(10, 4)",
    "float": "Float64",
    "real": "Float32",
    
    # Cadenas
    "char": "String",
    "varchar": "String",
    "nchar": "String",
    "nvarchar": "String",
    "text": "String",
    "ntext": "String",
    
    # Fecha y hora
    "date": "Date",
    "datetime": "DateTime",
    "datetime2": "DateTime64(3)",
    "smalldatetime": "DateTime",
    "time": "String",  # ClickHouse no tiene tipo time nativo
    "datetimeoffset": "DateTime64(3)",
    
    # Binarios
    "binary": "String",
    "varbinary": "String",
    "image": "String",
    
    # Otros
    "uniqueidentifier": "UUID",
    "xml": "String",
}

def get_clickhouse_type(sql_type: str, precision: int = None, scale: int = None) -> str:
    """
    Convierte un tipo de SQL Server a su equivalente en ClickHouse
    
    Args:
        sql_type: Tipo de datos de SQL Server
        precision: Precisión para tipos decimal/numeric
        scale: Escala para tipos decimal/numeric
    
    Returns:
        Tipo de datos equivalente en ClickHouse
    """
    sql_type_lower = sql_type.lower()
    
    # Tipos con precisión específica
    if sql_type_lower in ("decimal", "numeric") and precision and scale:
        return f"Decimal({precision}, {scale})"
    
    # Buscar en el mapeo
    ch_type = TYPE_MAPPING.get(sql_type_lower, "String")
    
    return ch_type

def make_nullable(ch_type: str) -> str:
    """Convierte un tipo ClickHouse a Nullable"""
    if ch_type.startswith("Nullable"):
        return ch_type
    return f"Nullable({ch_type})"
