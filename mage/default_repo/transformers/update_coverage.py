if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
import pandas as pd
from datetime import datetime

import snowflake.connector
from mage_ai.data_preparation.shared.secrets import get_secret_value

COVERAGE_PATH = "/home/src/docs/coverage_matrix.csv"

def _conn():
    return snowflake.connector.connect(
        account=get_secret_value('SNOWFLAKE_ACCOUNT'),
        user=get_secret_value('SNOWFLAKE_USER'),
        password=get_secret_value('SNOWFLAKE_PASSWORD'),
        role=get_secret_value('SNOWFLAKE_ROLE'),
        warehouse=get_secret_value('SNOWFLAKE_WAREHOUSE'),
        database=get_secret_value('SNOWFLAKE_DATABASE'),
        schema=get_secret_value('SNOWFLAKE_SCHEMA_RAW'),
        client_session_keep_alive=False,
    )

def _read_existing_csv():
    if os.path.exists(COVERAGE_PATH):
        try:
            return pd.read_csv(COVERAGE_PATH)
        except Exception:
            pass
    return pd.DataFrame(columns=[
        'year','month','service_type','has_parquet','load_status',
        'row_count','notes','updated_at'
    ])

def _counts_from_snowflake():
    db = get_secret_value('SNOWFLAKE_DATABASE')
    sch = get_secret_value('SNOWFLAKE_SCHEMA_RAW')

    conn = _conn()
    try:
        q = f"""
        with y as (
          select 'yellow' as service_type, year, month, count(*) as row_count
          from {db}.{sch}.yellow_trips
          group by 1,2,3
        ),
        g as (
          select 'green' as service_type, year, month, count(*) as row_count
          from {db}.{sch}.green_trips
          group by 1,2,3
        )
        select * from y
        union all
        select * from g
        """
        df = pd.read_sql(q, conn)
        # asegurar tipos
        if not df.empty:
            df['year'] = df['year'].astype(int)
            df['month'] = df['month'].astype(int)
            df['service_type'] = df['service_type'].astype(str)
        return df
    except Exception as e:
        print("Error consultando conteos en Snowflake:", e)
        return pd.DataFrame(columns=['service_type','year','month','row_count'])
    finally:
        conn.close()

@transformer
def transform(availability_df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    availability_df: output del bloque 2 con columnas:
      ['year','month','service_type','url','has_parquet','http_status','content_length','checked_at','notes']

    Efecto:
      - Escribe/actualiza docs/coverage_matrix.csv
    Retorna:
      - El DataFrame completo de cobertura actualizado (útil para inspección en UI).
    """
    if availability_df is None or availability_df.empty:
        raise ValueError("No llegó availability_df desde el bloque anterior.")

    # Normaliza columnas importantes
    avail = availability_df[['year','month','service_type','has_parquet','notes']].copy()
    avail['year'] = avail['year'].astype(int)
    avail['month'] = avail['month'].astype(int)
    avail['service_type'] = avail['service_type'].astype(str)

    # Conteos actuales en BRONZE
    counts = _counts_from_snowflake()

    # Merge availability + counts
    cov = avail.merge(
        counts,
        on=['service_type','year','month'],
        how='left'
    )
    cov['row_count'] = cov['row_count'].fillna(0).astype(int)

    # Determinar load_status
    def decide_status(row):
        if pd.notnull(row.get('notes')) and str(row['notes']).startswith('error'):
            return 'error'
        if not row['has_parquet']:
            return 'missing'
        # has_parquet True:
        return 'ok' if row['row_count'] > 0 else 'pending'

    cov['load_status'] = cov.apply(decide_status, axis=1)
    cov['updated_at'] = datetime.utcnow().isoformat(timespec='seconds') + 'Z'

    # Cargar cobertura previa y hacer upsert por clave (y,m,service)
    prev = _read_existing_csv()
    key_cols = ['year','month','service_type']
    if not prev.empty:
        # drop duplicados en prev y cov, priorizando los nuevos
        prev = prev.drop_duplicates(subset=key_cols, keep='last')
        cov = cov.drop_duplicates(subset=key_cols, keep='last')
        merged = pd.concat([prev, cov], ignore_index=True)
        merged = merged.sort_values('updated_at').drop_duplicates(subset=key_cols, keep='last')
    else:
        merged = cov

    # Guardar CSV
    os.makedirs(os.path.dirname(COVERAGE_PATH), exist_ok=True)
    merged = merged[['year','month','service_type','has_parquet','load_status','row_count','notes','updated_at']]
    merged.to_csv(COVERAGE_PATH, index=False)

    print(f"Cobertura actualizada: {COVERAGE_PATH} ({len(merged)} filas)")
    return merged

@test
def test_output(output: pd.DataFrame, *args) -> None:
    assert output is not None and not output.empty, "Output vacío."
    for c in ['year','month','service_type','has_parquet','load_status','row_count','updated_at']:
        assert c in output.columns, f"Falta columna {c}"
