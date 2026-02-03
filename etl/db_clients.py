"""
Clientes de conexión a las bases de datos
"""

import pyodbc
import clickhouse_connect
from typing import List, Dict
from .config import SQLSERVER_CONFIG, CLICKHOUSE_CONFIG


class SQLServerClient:
    """Cliente para SQL Server usando ODBC (soporta instancias nombradas)"""

    def __init__(self):
        self.config = SQLSERVER_CONFIG
        self.conn = None

    def _build_connection_string(self) -> str:
        """Construye la cadena de conexión ODBC"""
        server = self.config["server"]
        instance = self.config.get("instance", "")
        port = self.config.get("port", "")
        
        # Construir el servidor con instancia/puerto
        if instance:
            # Instancia nombrada: SERVER\INSTANCE
            server_str = f"{server}\\{instance}"
        elif port:
            # Puerto específico: SERVER,PORT
            server_str = f"{server},{port}"
        else:
            server_str = server
        
        conn_str = (
            f"DRIVER={{{self.config['driver']}}};"
            f"SERVER={server_str};"
            f"DATABASE={self.config['database']};"
            f"UID={self.config['user']};"
            f"PWD={self.config['password']};"
            f"TrustServerCertificate={self.config.get('trust_certificate', 'yes')};"
        )
        return conn_str

    def connect(self):
        """Establece conexión con SQL Server"""
        conn_str = self._build_connection_string()
        self.conn = pyodbc.connect(conn_str)
        return self

    def disconnect(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query: str) -> List[Dict]:
        """Ejecuta una consulta y devuelve los resultados como lista de diccionarios"""
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        cursor.close()
        return results

    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Obtiene el esquema de una tabla"""
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query)

    def get_table_count(self, table_name: str) -> int:
        """Obtiene el número de registros de una tabla"""
        query = f"SELECT COUNT(*) as cnt FROM [{table_name}]"
        result = self.execute_query(query)
        return result[0]["cnt"] if result else 0

    def table_exists(self, table_name: str) -> bool:
        """Verifica si una tabla existe"""
        query = f"""
        SELECT COUNT(*) as cnt 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{table_name}'
        """
        result = self.execute_query(query)
        return result[0]["cnt"] > 0 if result else False

    def fetch_data_in_batches(self, table_name: str, batch_size: int = 50000):
        """
        Generator que obtiene datos de una tabla en batches

        Args:
            table_name: Nombre de la tabla
            batch_size: Tamaño del batch

        Yields:
            Lista de diccionarios con los datos del batch
        """
        # Primary keys conocidas para ORDER BY estable (evita duplicados en OFFSET/FETCH)
        # AT_PRODUCCIONES no tiene PK única, usamos combinación para estabilidad
        PRIMARY_KEYS = {
            "CabeAlbC": "IDALBC",
            "LineAlba": "IDLIN",
            "AT_PRODUCCION": "IDPRODUCCION",
            "AT_PRODUCCIONES": "IDALBC, FECHA, CODART, LOTE",
            "AT_STOCK": "ID",
            "AT_STOCK_IDENTIFICADOR": "ID",
            "AT_SUBPRODUCTO": "IDSUBPRODUCTO",
            "AT_TRASPASOS": "IDTRASPASO",
            "Articulo": "CODART",
            "Almacen": "CODALM",
            "AT_CALIBRES": "IDCALIBRE",
        }
        
        total = self.get_table_count(table_name)
        offset = 0

        # Usar primary key conocida o fallback a primera columna
        order_col = PRIMARY_KEYS.get(table_name)
        
        # Si la columna tiene comas, es una lista de columnas (validar nombres)
        if order_col and "," in order_col:
            cols = [c.strip() for c in order_col.split(",")]
            order_clause = ", ".join([f"[{c}]" for c in cols])
        elif order_col:
            order_clause = f"[{order_col}]"
        else:
            # Fallback automático
            schema = self.get_table_schema(table_name)
            if not schema:
                return
            order_clause = f"[{schema[0]['COLUMN_NAME']}]"

        while offset < total:
            query = f"""
            SELECT * FROM [{table_name}]
            ORDER BY {order_clause}
            OFFSET {offset} ROWS
            FETCH NEXT {batch_size} ROWS ONLY
            """
            batch = self.execute_query(query)
            if not batch:
                break
            yield batch
            offset += batch_size

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class ClickHouseClient:
    """Cliente para ClickHouse"""

    def __init__(self):
        self.config = CLICKHOUSE_CONFIG
        self.client = None

    def connect(self):
        """Establece conexión con ClickHouse"""
        self.client = clickhouse_connect.get_client(**self.config)
        return self

    def disconnect(self):
        """Cierra la conexión"""
        if self.client:
            self.client.close()
            self.client = None

    def execute_query(self, query: str):
        """Ejecuta una consulta"""
        return self.client.query(query)

    def execute_command(self, command: str):
        """Ejecuta un comando DDL"""
        return self.client.command(command)

    def create_table(
        self, table_name: str, columns: List[Dict], engine: str = "MergeTree()"
    ):
        """
        Crea una tabla en ClickHouse

        Args:
            table_name: Nombre de la tabla
            columns: Lista de columnas con nombre y tipo
            engine: Motor de ClickHouse (default: MergeTree)
        """
        cols_def = ",\n    ".join([f"`{col['name']}` {col['type']}" for col in columns])

        # Encontrar una columna NO NULLABLE para ORDER BY (preferir fecha o ID)
        order_col = None

        # Primero buscar columnas no-nullable con fecha o ID
        for col in columns:
            col_lower = col["name"].lower()
            is_nullable = "Nullable" in col["type"]

            if not is_nullable:
                if "fecha" in col_lower or "date" in col_lower:
                    order_col = col["name"]
                    break
                elif col_lower.startswith("id") or col_lower.endswith("id"):
                    if order_col is None:
                        order_col = col["name"]

        # Si no encontramos una buena columna no-nullable, buscar cualquier no-nullable
        if not order_col:
            for col in columns:
                if "Nullable" not in col["type"]:
                    order_col = col["name"]
                    break

        # Si todas son nullable, usar tuple() vacío (no ordenado)
        if not order_col:
            order_clause = "ORDER BY tuple()"
        else:
            order_clause = f"ORDER BY (`{order_col}`)"

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {cols_def}
        ) ENGINE = {engine}
        {order_clause}
        SETTINGS allow_nullable_key = 1
        """

        self.execute_command(query)

    def drop_table(self, table_name: str):
        """Elimina una tabla si existe"""
        self.execute_command(f"DROP TABLE IF EXISTS {table_name}")

    def insert_data(self, table_name: str, data: List[Dict], column_names: List[str]):
        """
        Inserta datos en una tabla

        Args:
            table_name: Nombre de la tabla
            data: Lista de diccionarios con los datos
            column_names: Lista de nombres de columnas
        """
        if not data:
            return 0

        # Convertir a lista de listas
        rows = [[row.get(col) for col in column_names] for row in data]

        self.client.insert(table_name, rows, column_names=column_names)

        return len(rows)

    def get_table_count(self, table_name: str) -> int:
        """Obtiene el número de registros de una tabla"""
        result = self.execute_query(f"SELECT count() FROM {table_name}")
        return result.result_rows[0][0] if result.result_rows else 0

    def table_exists(self, table_name: str) -> bool:
        """Verifica si una tabla existe"""
        result = self.execute_query(f"EXISTS TABLE {table_name}")
        return result.result_rows[0][0] == 1 if result.result_rows else False

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
