"""
Módulo ETL para migración de datos SQL Server → ClickHouse
"""
from .config import (
    SQLSERVER_CONFIG,
    CLICKHOUSE_CONFIG,
    TABLES_CONFIG,
    get_all_tables,
    get_incremental_tables,
    get_full_load_tables,
)
from .db_clients import SQLServerClient, ClickHouseClient
from .type_mapping import get_clickhouse_type, make_nullable
from .initial_load import run_initial_load, migrate_table
from .incremental_load import run_incremental_load

__all__ = [
    'SQLSERVER_CONFIG',
    'CLICKHOUSE_CONFIG',
    'TABLES_CONFIG',
    'get_all_tables',
    'get_incremental_tables',
    'get_full_load_tables',
    'SQLServerClient',
    'ClickHouseClient',
    'get_clickhouse_type',
    'make_nullable',
    'run_initial_load',
    'migrate_table',
    'run_incremental_load',
]
