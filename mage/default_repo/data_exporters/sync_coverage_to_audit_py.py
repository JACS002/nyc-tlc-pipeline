# --- guard del template de Mage ---
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
from mage_ai.settings.repo import get_repo_path

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def _conn():
    return snowflake.connector.connect(
        account=get_secret_value('SNOWFLAKE_ACCOUNT'),
        user=get_secret_value('SNOWFLAKE_USER'),
        password=get_secret_value('SNOWFLAKE_PASSWORD'),
        role=get_secret_value('SNOWFLAKE_ROLE'),
        warehouse=get_secret_value('SNOWFLAKE_WAREHOUSE'),
        database=get_secret_value('SNOWFLAKE_DATABASE'),
        schema=get_secret_value('SNOWFLAKE_SCHEMA_RAW'),  # BRONZE
        client_session_keep_alive=False,
        ocsp_fail_open=True,
        insecure_mode=True,
    )

DDL_AUDIT = """
create table if not exists {db}.{schema}.load_audit (
  service_type string,
  year int,
  month int,
  row_count number,
  latest_ingest_ts timestamp,
  status string,   -- OK | PENDING | MISSING
  note string
);
"""

@data_exporter
def export_data(*args, **kwargs) -> None:
    """
    Lee coverage_matrix.csv y sincroniza BRONZE.LOAD_AUDIT:
      - MISSING: has_parquet=false
      - PENDING: has_parquet=true y row_count=0
      - OK:      has_parquet=true y row_count>0
    No reingesta datos; sólo consulta conteos en BRONZE.Y/G y reconstruye LOAD_AUDIT.
    """
    DB = get_secret_value('SNOWFLAKE_DATABASE')
    SCHEMA = get_secret_value('SNOWFLAKE_SCHEMA_RAW')  # BRONZE

    # 1) Cargar coverage_matrix.csv del repo
    repo = get_repo_path()
    coverage_path = os.path.join(repo, 'coverage_matrix.csv')
    if not os.path.exists(coverage_path):
        raise FileNotFoundError(f"No se encontró {coverage_path}. Ejecuta el bloque que genera la matriz de cobertura.")

    cov = pd.read_csv(coverage_path)
    # columnas esperadas: service_type, year, month, url, has_parquet, http_status, content_length, checked_at
    cov['service_type'] = cov['service_type'].str.lower()
    cov['year'] = cov['year'].astype(int)
    cov['month'] = cov['month'].astype(int)
    cov['has_parquet'] = cov['has_parquet'].astype(bool)

    # 2) Consultar conteos por mes (sólo para los que tienen parquet)
    conn = _conn()
    cs = conn.cursor()
    try:
        cs.execute(DDL_AUDIT.format(db=DB, schema=SCHEMA))

        # armar DF audit
        records = []
        # Preparamos listas de meses a contar por servicio
        to_check_yellow = cov[(cov['service_type']=='yellow') & (cov['has_parquet']==True)][['year','month']].drop_duplicates()
        to_check_green  = cov[(cov['service_type']=='green')  & (cov['has_parquet']==True)][['year','month']].drop_duplicates()

        # Hacemos una sola query por servicio (más eficiente)
        def fetch_counts(table_name: str):
            q = f"""
            select year, month,
                   count(*) as row_count,
                   max(try_to_timestamp(ingest_ts)) as latest_ingest_ts
            from {DB}.{SCHEMA}.{table_name}
            group by 1,2
            """
            cs.execute(q)
            rows = cs.fetchall()
            # dict {(year,month): (row_count, latest_ingest_ts)}
            return {(r[0], r[1]): (int(r[2] or 0), r[3]) for r in rows}

        yellow_counts = fetch_counts('yellow_trips')
        green_counts  = fetch_counts('green_trips')

        # 3) Determinar status fila por fila
        for idx, r in cov.iterrows():
            svc = r['service_type']; y = int(r['year']); m = int(r['month'])
            if not r['has_parquet']:
                records.append({
                    'service_type': svc,
                    'year': y, 'month': m,
                    'row_count': 0,
                    'latest_ingest_ts': None,
                    'status': 'MISSING',
                    'note': 'No Parquet available'
                })
            else:
                if svc == 'yellow':
                    rc, ts = yellow_counts.get((y,m), (0, None))
                else:
                    rc, ts = green_counts.get((y,m), (0, None))
                status = 'OK' if rc > 0 else 'PENDING'
                records.append({
                    'service_type': svc,
                    'year': y, 'month': m,
                    'row_count': rc,
                    'latest_ingest_ts': ts,
                    'status': status,
                    'note': None
                })

        audit_df = pd.DataFrame(records)
        audit_df.sort_values(['service_type','year','month'], inplace=True)

        # 4) Reemplazar contenido de LOAD_AUDIT
        cs.execute(f"truncate table {DB}.{SCHEMA}.load_audit")
        ok, nchunks, nrows, _ = write_pandas(
            conn,
            audit_df,
            table_name='load_audit',
            database=DB,
            schema=SCHEMA,
            quote_identifiers=False,
            chunk_size=50_000,
        )
        print(f"[audit] Actualizado ok={ok}, filas={nrows}, chunks={nchunks}")

    finally:
        try: cs.close()
        except Exception: pass
        conn.close()
