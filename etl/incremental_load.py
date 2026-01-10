"""
Script de carga incremental - Solo carga registros nuevos desde SQL Server a ClickHouse
"""
import time
from datetime import datetime
from typing import List, Dict, Optional

from .config import get_incremental_tables, get_full_load_tables
from .db_clients import SQLServerClient, ClickHouseClient
from .initial_load import convert_schema_to_clickhouse, clean_data_for_clickhouse


def get_last_value_from_clickhouse(
    ch_client: ClickHouseClient,
    table_name: str,
    column_name: str,
    column_type: str
) -> Optional[any]:
    """
    Obtiene el último valor de la columna incremental en ClickHouse
    
    Args:
        ch_client: Cliente de ClickHouse
        table_name: Nombre de la tabla
        column_name: Nombre de la columna incremental
        column_type: Tipo de la columna ('datetime' o 'id')
        
    Returns:
        Último valor o None si no hay datos
    """
    ch_table_name = table_name.replace('__', '_')
    
    # Verificar si la tabla existe
    if not ch_client.table_exists(ch_table_name):
        return None
    
    try:
        result = ch_client.execute_query(
            f"SELECT MAX(`{column_name}`) FROM {ch_table_name}"
        )
        
        if result.result_rows and result.result_rows[0][0] is not None:
            return result.result_rows[0][0]
    except Exception as e:
        print(f"    ⚠️  Error obteniendo último valor: {e}")
    
    return None


def fetch_new_records(
    sql_client: SQLServerClient,
    table_name: str,
    column_name: str,
    column_type: str,
    last_value: any
) -> List[Dict]:
    """
    Obtiene los registros nuevos desde SQL Server
    
    Args:
        sql_client: Cliente de SQL Server
        table_name: Nombre de la tabla
        column_name: Nombre de la columna incremental
        column_type: Tipo de la columna ('datetime' o 'id')
        last_value: Último valor conocido en ClickHouse
        
    Returns:
        Lista de nuevos registros
    """
    if last_value is None:
        # Si no hay datos previos, obtener todo (no debería pasar en incremental)
        query = f"SELECT * FROM [{table_name}]"
    else:
        # Formatear el valor según el tipo
        if column_type == 'datetime':
            if isinstance(last_value, datetime):
                formatted_value = last_value.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_value = str(last_value)
            query = f"SELECT * FROM [{table_name}] WHERE [{column_name}] > '{formatted_value}'"
        else:
            # ID numérico
            query = f"SELECT * FROM [{table_name}] WHERE [{column_name}] > {last_value}"
    
    return sql_client.execute_query(query)


def migrate_incremental_table(
    sql_client: SQLServerClient,
    ch_client: ClickHouseClient,
    table_config: Dict
) -> Dict:
    """
    Migra solo los registros nuevos de una tabla
    
    Args:
        sql_client: Cliente de SQL Server
        ch_client: Cliente de ClickHouse
        table_config: Configuración de la tabla
        
    Returns:
        Estadísticas de la migración
    """
    table_name = table_config['name']
    column_name = table_config['incremental_column']
    column_type = table_config['type']
    ch_table_name = table_name.replace('__', '_')
    
    stats = {
        'table': table_name,
        'status': 'pending',
        'new_records': 0,
        'duration_seconds': 0,
        'error': None
    }
    
    start_time = time.time()
    
    try:
        # Verificar que la tabla existe en SQL Server
        if not sql_client.table_exists(table_name):
            stats['status'] = 'skipped'
            stats['error'] = "No existe en SQL Server"
            print(f"  ⚠️  {table_name}: No existe en SQL Server")
            return stats
        
        # Verificar que la tabla existe en ClickHouse
        if not ch_client.table_exists(ch_table_name):
            stats['status'] = 'skipped'
            stats['error'] = "Tabla no existe en ClickHouse (ejecutar carga inicial)"
            print(f"  ⚠️  {table_name}: No existe en ClickHouse, saltar")
            return stats
        
        # Obtener último valor en ClickHouse
        last_value = get_last_value_from_clickhouse(
            ch_client, table_name, column_name, column_type
        )
        
        if last_value is None:
            # Si no hay último valor, la tabla puede estar vacía o hubo error
            # Saltar en lugar de cargar todo
            stats['status'] = 'skipped'
            stats['error'] = "Sin datos previos o error de columna"
            print(f"  ⚠️  {table_name}: Sin datos previos, saltar")
            return stats
        
        print(f"  📍 Último valor en ClickHouse: {last_value}")
        
        # Obtener nuevos registros (solo los posteriores al último)
        new_records = fetch_new_records(
            sql_client, table_name, column_name, column_type, last_value
        )
        
        if not new_records:
            stats['status'] = 'success'
            stats['new_records'] = 0
            print(f"  ✅ {table_name}: Sin registros nuevos")
            return stats
        
        # Obtener esquema para mapeo de tipos
        sql_schema = sql_client.get_table_schema(table_name)
        ch_columns = convert_schema_to_clickhouse(sql_schema)
        column_names = [col['name'] for col in ch_columns]
        
        # Limpiar datos
        clean_records = clean_data_for_clickhouse(new_records, ch_columns)
        
        # Insertar nuevos registros
        inserted = ch_client.insert_data(ch_table_name, clean_records, column_names)
        
        stats['new_records'] = inserted
        stats['status'] = 'success'
        print(f"  ✅ {table_name}: {inserted:,} registros nuevos insertados")
        
    except Exception as e:
        stats['status'] = 'error'
        stats['error'] = str(e)
        print(f"  ❌ {table_name}: Error - {e}")
    
    finally:
        stats['duration_seconds'] = round(time.time() - start_time, 2)
    
    return stats


def migrate_full_table(
    sql_client: SQLServerClient,
    ch_client: ClickHouseClient,
    table_config: Dict
) -> Dict:
    """
    Hace carga completa de una tabla maestra (truncate + insert)
    
    Args:
        sql_client: Cliente de SQL Server
        ch_client: Cliente de ClickHouse  
        table_config: Configuración de la tabla
        
    Returns:
        Estadísticas de la migración
    """
    table_name = table_config['name']
    ch_table_name = table_name.replace('__', '_')
    
    stats = {
        'table': table_name,
        'status': 'pending',
        'new_records': 0,
        'duration_seconds': 0,
        'error': None
    }
    
    start_time = time.time()
    
    try:
        # Verificar que la tabla existe en SQL Server
        if not sql_client.table_exists(table_name):
            stats['status'] = 'skipped'
            stats['error'] = "No existe en SQL Server"
            print(f"  ⚠️  {table_name}: No existe en SQL Server")
            return stats
        
        # Obtener todos los registros de SQL Server
        all_records = sql_client.execute_query(f"SELECT * FROM [{table_name}]")
        
        if not all_records:
            stats['status'] = 'skipped'
            stats['error'] = "Tabla vacía"
            print(f"  ⚠️  {table_name}: Tabla vacía")
            return stats
        
        # Obtener esquema
        sql_schema = sql_client.get_table_schema(table_name)
        ch_columns = convert_schema_to_clickhouse(sql_schema)
        column_names = [col['name'] for col in ch_columns]
        
        # Truncar tabla en ClickHouse
        if ch_client.table_exists(ch_table_name):
            ch_client.execute_command(f"TRUNCATE TABLE {ch_table_name}")
            print("  🗑️  Tabla truncada")
        
        # Limpiar e insertar
        clean_records = clean_data_for_clickhouse(all_records, ch_columns)
        inserted = ch_client.insert_data(ch_table_name, clean_records, column_names)
        
        stats['new_records'] = inserted
        stats['status'] = 'success'
        print(f"  ✅ {table_name}: {inserted:,} registros (carga completa)")
        
    except Exception as e:
        stats['status'] = 'error'
        stats['error'] = str(e)
        print(f"  ❌ {table_name}: Error - {e}")
    
    finally:
        stats['duration_seconds'] = round(time.time() - start_time, 2)
    
    return stats


def run_incremental_load():
    """
    Ejecuta la carga incremental de todas las tablas
    """
    incremental_tables = get_incremental_tables()
    full_tables = get_full_load_tables()
    
    print("=" * 80)
    print("🔄 CARGA INCREMENTAL - SQL Server → ClickHouse")
    print("=" * 80)
    print(f"📅 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Tablas incrementales: {len(incremental_tables)}")
    print(f"📊 Tablas full reload: {len(full_tables)}")
    print("=" * 80)
    
    all_stats = []
    start_time = time.time()
    
    with SQLServerClient() as sql_client, ClickHouseClient() as ch_client:
        # Procesar tablas incrementales
        print("\n📈 TABLAS INCREMENTALES:")
        print("-" * 40)
        
        for i, table_config in enumerate(incremental_tables, 1):
            print(f"\n[{i}/{len(incremental_tables)}] {table_config['name']}")
            stats = migrate_incremental_table(sql_client, ch_client, table_config)
            all_stats.append(stats)
        
        # Procesar tablas con carga completa
        print("\n📦 TABLAS MAESTRAS (FULL RELOAD):")
        print("-" * 40)
        
        for i, table_config in enumerate(full_tables, 1):
            print(f"\n[{i}/{len(full_tables)}] {table_config['name']}")
            stats = migrate_full_table(sql_client, ch_client, table_config)
            all_stats.append(stats)
    
    # Resumen final
    total_duration = round(time.time() - start_time, 2)
    success = sum(1 for s in all_stats if s['status'] == 'success')
    skipped = sum(1 for s in all_stats if s['status'] == 'skipped')
    errors = sum(1 for s in all_stats if s['status'] == 'error')
    total_new = sum(s['new_records'] for s in all_stats)
    
    print("\n" + "=" * 80)
    print("📊 RESUMEN DE LA CARGA INCREMENTAL")
    print("=" * 80)
    print(f"✅ Exitosas:  {success}")
    print(f"⚠️  Saltadas:  {skipped}")
    print(f"❌ Errores:   {errors}")
    print(f"📊 Total registros nuevos: {total_new:,}")
    print(f"⏱️  Duración total: {total_duration:.2f} segundos")
    print("=" * 80)
    
    # Mostrar errores si los hay
    if errors > 0:
        print("\n❌ TABLAS CON ERRORES:")
        for s in all_stats:
            if s['status'] == 'error':
                print(f"   - {s['table']}: {s['error']}")
    
    return all_stats


if __name__ == "__main__":
    run_incremental_load()
