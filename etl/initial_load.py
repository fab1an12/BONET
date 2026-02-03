"""
Script de carga inicial - Migra todas las tablas de SQL Server a ClickHouse
"""

import time
from datetime import datetime
from typing import List, Dict

from .config import get_all_tables
from .db_clients import SQLServerClient, ClickHouseClient
from .type_mapping import get_clickhouse_type, make_nullable


def convert_schema_to_clickhouse(sql_schema: List[Dict]) -> List[Dict]:
    """
    Convierte el esquema de SQL Server al formato de ClickHouse

    Args:
        sql_schema: Esquema de la tabla en SQL Server

    Returns:
        Lista de columnas en formato ClickHouse
    """
    ch_columns = []

    for col in sql_schema:
        ch_type = get_clickhouse_type(
            col["DATA_TYPE"], col.get("NUMERIC_PRECISION"), col.get("NUMERIC_SCALE")
        )

        is_date_column = (
            "date" in col["DATA_TYPE"].lower() or "time" in col["DATA_TYPE"].lower()
        )

        # Hacer nullable:
        # - TODAS las columnas de fecha (para manejar fechas inválidas/NULL)
        # - Columnas que permiten NULL (excepto primera columna para ORDER BY)
        should_be_nullable = is_date_column or (
            col["ORDINAL_POSITION"] > 1 and col["IS_NULLABLE"] == "YES"
        )

        if should_be_nullable:
            ch_type = make_nullable(ch_type)

        ch_columns.append(
            {
                "name": col["COLUMN_NAME"],
                "type": ch_type,
                "original_type": col["DATA_TYPE"],
            }
        )

    return ch_columns


def clean_data_for_clickhouse(data: List[Dict], schema: List[Dict]) -> List[Dict]:
    """
    Limpia y prepara los datos para insertar en ClickHouse

    Args:
        data: Datos a limpiar
        schema: Esquema de la tabla

    Returns:
        Datos limpiados
    """
    # Identificar columnas de fecha/tiempo y si son nullable
    date_columns = {}
    for col in schema:
        if (
            "date" in col["original_type"].lower()
            or "time" in col["original_type"].lower()
        ):
            is_nullable = "Nullable" in col["type"]
            date_columns[col["name"]] = is_nullable

    # Fecha mínima válida para ClickHouse DateTime (1970-01-01 00:00:01)
    min_valid_date = datetime(1970, 1, 1, 0, 0, 1)
    # Fecha máxima válida para ClickHouse DateTime (2106-02-07)
    max_valid_date = datetime(2100, 12, 31, 23, 59, 59)

    cleaned = []
    for row in data:
        clean_row = {}
        for key, value in row.items():
            if key in date_columns:
                is_nullable = date_columns[key]
                clean_row[key] = _clean_date_value(
                    value, is_nullable, min_valid_date, max_valid_date
                )
            elif value is None:
                clean_row[key] = None
            elif isinstance(value, bytes):
                # Convertir bytes a string
                try:
                    clean_row[key] = value.decode("utf-8", errors="replace")
                except Exception:
                    clean_row[key] = str(value)
            else:
                clean_row[key] = value
        cleaned.append(clean_row)

    return cleaned


def _clean_date_value(value, is_nullable: bool, min_date: datetime, max_date: datetime):
    """
    Limpia un valor de fecha para ClickHouse
    """
    if value is None:
        return None if is_nullable else min_date

    # Si ya es datetime
    if isinstance(value, datetime):
        # Verificar rango válido
        if value < min_date:
            return None if is_nullable else min_date
        elif value > max_date:
            return None if is_nullable else max_date
        return value

    # Si es date (sin tiempo)
    if hasattr(value, "year"):
        try:
            if value.year < 1970:
                return None if is_nullable else min_date
            elif value.year > 2100:
                return None if is_nullable else max_date
            # Convertir date a datetime
            return datetime(value.year, value.month, value.day)
        except Exception:
            return None if is_nullable else min_date

    # Otros casos - intentar mantener el valor o usar mínimo
    return None if is_nullable else min_date


def migrate_table(
    sql_client: SQLServerClient,
    ch_client: ClickHouseClient,
    table_name: str,
    drop_existing: bool = True,
    batch_size: int = 50000,
) -> Dict:
    """
    Migra una tabla completa de SQL Server a ClickHouse

    Args:
        sql_client: Cliente de SQL Server
        ch_client: Cliente de ClickHouse
        table_name: Nombre de la tabla
        drop_existing: Si True, elimina la tabla existente antes de crear
        batch_size: Tamaño del batch para la inserción

    Returns:
        Diccionario con estadísticas de la migración
    """
    stats = {
        "table": table_name,
        "status": "pending",
        "source_count": 0,
        "migrated_count": 0,
        "duration_seconds": 0,
        "error": None,
    }

    start_time = time.time()

    try:
        # Verificar que la tabla existe en SQL Server
        if not sql_client.table_exists(table_name):
            stats["status"] = "skipped"
            stats["error"] = f"Tabla {table_name} no existe en SQL Server"
            print(f"  ⚠️  {table_name}: No existe en SQL Server")
            return stats

        # Obtener esquema
        print(f"  📋 Obteniendo esquema de {table_name}...")
        sql_schema = sql_client.get_table_schema(table_name)

        if not sql_schema:
            stats["status"] = "error"
            stats["error"] = f"No se pudo obtener el esquema de {table_name}"
            print(f"  ❌ {table_name}: No se pudo obtener el esquema")
            return stats

        # Convertir esquema
        ch_columns = convert_schema_to_clickhouse(sql_schema)
        column_names = [col["name"] for col in ch_columns]

        # Contar registros origen
        stats["source_count"] = sql_client.get_table_count(table_name)
        print(f"  📊 Registros en origen: {stats['source_count']:,}")

        if stats["source_count"] == 0:
            stats["status"] = "skipped"
            stats["error"] = "Tabla vacía"
            print(f"  ⚠️  {table_name}: Tabla vacía, saltando...")
            return stats

        # Crear tabla en ClickHouse
        ch_table_name = table_name.replace("__", "_")  # Normalizar nombre

        # Si no se fuerza el drop, verificar si ya existe y tiene datos
        if not drop_existing and ch_client.table_exists(ch_table_name):
            current_count = ch_client.get_table_count(ch_table_name)
            if current_count > 0:
                stats["status"] = "skipped"
                stats["migrated_count"] = current_count
                stats["error"] = "Ya existe y tiene datos"
                print(f"  ⏭️  {table_name}: Ya existe ({current_count:,} registros). Saltando...")
                return stats

        if drop_existing:
            print(" 🗑️  Eliminando tabla existente...")
            ch_client.drop_table(ch_table_name)

        print(" 🏗️  Creando tabla en ClickHouse...")
        ch_client.create_table(ch_table_name, ch_columns)

        # Migrar datos en batches
        print(" 📦 Migrando datos en batches de {batch_size:,}...")
        total_migrated = 0
        batch_num = 0

        for batch in sql_client.fetch_data_in_batches(table_name, batch_size):
            batch_num += 1

            # Limpiar datos
            clean_batch = clean_data_for_clickhouse(batch, ch_columns)

            # Insertar en ClickHouse
            inserted = ch_client.insert_data(ch_table_name, clean_batch, column_names)
            total_migrated += inserted

            progress = (total_migrated / stats["source_count"]) * 100
            print(f"      Batch {batch_num}: {inserted:,} registros ({progress:.1f}%)")

        stats["migrated_count"] = total_migrated
        stats["status"] = "success"

        # Verificar conteo final
        final_count = ch_client.get_table_count(ch_table_name)
        print(f"  ✅ {table_name}: {final_count:,} registros migrados")

    except Exception as e:
        stats["status"] = "error"
        stats["error"] = str(e)
        print(f"  ❌ {table_name}: Error - {e}")

    finally:
        stats["duration_seconds"] = round(time.time() - start_time, 2)

    return stats


def run_initial_load(tables: List[Dict] = None, drop_existing: bool = False):
    """
    Ejecuta la carga inicial de todas las tablas

    Args:
        tables: Lista de tablas a migrar (None = todas)
        drop_existing: Si True, elimina tablas existentes
    """
    if tables is None:
        tables = get_all_tables()

    print("=" * 80)
    print("🚀 CARGA INICIAL - SQL Server → ClickHouse")
    print("=" * 80)
    print(f"📅 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Tablas a migrar: {len(tables)}")
    print("=" * 80)

    all_stats = []
    start_time = time.time()

    with SQLServerClient() as sql_client, ClickHouseClient() as ch_client:
        for i, table_config in enumerate(tables, 1):
            table_name = table_config["name"]
            print(f"\n[{i}/{len(tables)}] 📋 Procesando: {table_name}")
            print("-" * 40)

            stats = migrate_table(
                sql_client, ch_client, table_name, drop_existing=drop_existing
            )
            all_stats.append(stats)

    # Resumen final
    total_duration = round(time.time() - start_time, 2)
    success = sum(1 for s in all_stats if s["status"] == "success")
    skipped = sum(1 for s in all_stats if s["status"] == "skipped")
    errors = sum(1 for s in all_stats if s["status"] == "error")
    total_records = sum(s["migrated_count"] for s in all_stats)

    print("\n" + "=" * 80)
    print("📊 RESUMEN DE LA CARGA INICIAL")
    print("=" * 80)
    print(f"✅ Exitosas:  {success}")
    print(f"⚠️  Saltadas:  {skipped}")
    print(f"❌ Errores:   {errors}")
    print(f"📊 Total registros migrados: {total_records:,}")
    print(
        f"⏱️  Duración total: {total_duration:.2f} segundos ({total_duration / 60:.1f} minutos)"
    )
    print("=" * 80)

    # Mostrar errores si los hay
    if errors > 0:
        print("\n❌ TABLAS CON ERRORES:")
        for s in all_stats:
            if s["status"] == "error":
                print(f"   - {s['table']}: {s['error']}")

    return all_stats


if __name__ == "__main__":
    run_initial_load()
