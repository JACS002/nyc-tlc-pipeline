# --- guard del template de Mage ---
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
from mage_ai.settings.repo import get_repo_path

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime

# ============== Conexión ==============
def _conn(schema_override=None):
    # SIN fallback: siempre RAW (o lo que pases explícitamente en schema_override)
    schema = schema_override or get_secret_value('SNOWFLAKE_SCHEMA_RAW')
    return snowflake.connector.connect(
        account=get_secret_value('SNOWFLAKE_ACCOUNT'),
        user=get_secret_value('SNOWFLAKE_USER'),
        password=get_secret_value('SNOWFLAKE_PASSWORD'),
        role=get_secret_value('SNOWFLAKE_ROLE'),
        warehouse=get_secret_value('SNOWFLAKE_WAREHOUSE'),
        database=get_secret_value('SNOWFLAKE_DATABASE'),
        schema=schema,
        client_session_keep_alive=False,
        ocsp_fail_open=True,
        insecure_mode=True,
    )

# ============== DDLs mínimas + ensure columns ==============
DDL_AUDIT_MIN = """
create table if not exists {db}.{schema}.load_audit (
  service_type string,
  year int,
  month int
);
"""

DDL_COVERAGE_MIN = """
create table if not exists {db}.{schema}.coverage_matrix (
  service_type string,
  year int,
  month int
);
"""

def _ensure_audit_columns(conn, db, schema, table='load_audit'):
    fq = f"{db}.{schema}.{table}"
    cur = conn.cursor()
    try:
        cur.execute(f"alter table if exists {fq} add column if not exists row_count number")
        cur.execute(f"alter table if exists {fq} add column if not exists latest_ingest_ts timestamp_ntz")
        cur.execute(f"alter table if exists {fq} add column if not exists status string")  # OK | MISSING
        cur.execute(f"alter table if exists {fq} add column if not exists note string")
    finally:
        cur.close()

def _ensure_coverage_columns(conn, db, schema, table='coverage_matrix'):
    fq = f"{db}.{schema}.{table}"
    cur = conn.cursor()
    try:
        cur.execute(f"alter table if exists {fq} add column if not exists url string")
        cur.execute(f"alter table if exists {fq} add column if not exists has_parquet boolean")
        cur.execute(f"alter table if exists {fq} add column if not exists http_status int")
        cur.execute(f"alter table if exists {fq} add column if not exists content_length number(38,0)")
        cur.execute(f"alter table if exists {fq} add column if not exists checked_at timestamp_ntz")
        cur.execute(f"alter table if exists {fq} add column if not exists notes string")
    finally:
        cur.close()

# ============== Helpers ==============
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
def _build_url(service: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{service}_tripdata_{year}-{month:02d}.parquet"

def _grid_to_df(services, years_from, years_to):
    rows = []
    for svc in services:
        for y in range(int(years_from), int(years_to) + 1):
            for m in range(1, 13):
                rows.append((svc, y, m))
    return pd.DataFrame(rows, columns=['service_type','year','month'])

def _values_rows_int(int_iterable):
    # genera: (2015),(2016),...  -> múltiples filas (correcto para Snowflake VALUES)
    return ", ".join(f"({int(v)})" for v in int_iterable)

# ============== Exportador principal ==============
@data_exporter
def export_data(*args, **kwargs) -> None:
    """
    Construye/actualiza LOAD_AUDIT y COVERAGE_MATRIX directamente desde RAW.
    kwargs:
      - years_from: int (default 2015)
      - years_to:   int (default 2025)
      - services:   list[str] (default ['green','yellow'])
      - schema:     str (default: secreto SNOWFLAKE_SCHEMA_RAW)  -> SIN fallback
      - truncate:   bool (default True)  -> TRUNCATE + INSERT
      - write_csv:  bool (default True)  -> guarda coverage_matrix.csv en el repo
    """
    DB = get_secret_value('SNOWFLAKE_DATABASE')
    SCHEMA = kwargs.get('schema') or get_secret_value('SNOWFLAKE_SCHEMA_RAW')  # SIN fallback

    years_from = int(kwargs.get('years_from', 2015))
    years_to   = int(kwargs.get('years_to', 2025))
    services   = [s.lower() for s in kwargs.get('services', ['green','yellow']) if s.lower() in ('green','yellow')]
    if not services:
        services = ['green','yellow']
    truncate   = bool(kwargs.get('truncate', True))
    write_csv  = bool(kwargs.get('write_csv', True))

    # 1) Armar malla completa
    base = _grid_to_df(services, years_from, years_to)

    # 2) SQL de conteos desde RAW (VALUES como filas)
    services_vals = ", ".join([f"('{s}')" for s in services])
    years_rows  = _values_rows_int(range(years_from, years_to + 1))  # -> (2015),(2016),...
    months_rows = _values_rows_int(range(1, 13))                     # -> (1),(2),...

    sql_counts = f"""
with services(service_type) as (
  select column1 from values {services_vals}
),
years(year) as (
  select column1 from values {years_rows}
),
months(month) as (
  select column1 from values {months_rows}
),
base as (
  select s.service_type, y.year, m.month
  from services s
  cross join years y
  cross join months m
),
counts as (
  select 'green' as service_type, year, month,
         count(*) as row_count,
         max(try_to_timestamp(ingest_ts)) as latest_ingest_ts
  from {DB}.{SCHEMA}.green_trips
  where year between {years_from} and {years_to}
  group by 1,2,3
  union all
  select 'yellow' as service_type, year, month,
         count(*) as row_count,
         max(try_to_timestamp(ingest_ts)) as latest_ingest_ts
  from {DB}.{SCHEMA}.yellow_trips
  where year between {years_from} and {years_to}
  group by 1,2,3
)
select
  b.service_type,
  b.year,
  b.month,
  coalesce(c.row_count, 0) as row_count,
  c.latest_ingest_ts as latest_ingest_ts
from base b
left join counts c
  on b.service_type = c.service_type
 and b.year = c.year
 and b.month = c.month
order by b.service_type, b.year, b.month
"""

    conn = _conn(schema_override=SCHEMA)
    cur = conn.cursor()
    try:
        # 3) Ejecutar conteos
        cur.execute(sql_counts)
        df_counts: pd.DataFrame = cur.fetch_pandas_all()
        df_counts.columns = [c.lower() for c in df_counts.columns]
        df_counts['row_count'] = df_counts['row_count'].fillna(0).astype(int)

        # 4) Construir LOAD_AUDIT
        audit_df = df_counts.copy()
        audit_df['status'] = audit_df['row_count'].apply(lambda x: 'OK' if x > 0 else 'MISSING')
        audit_df['note'] = pd.NA
        audit_df = audit_df[['service_type','year','month','row_count','latest_ingest_ts','status','note']]

        # 5) Construir COVERAGE_MATRIX (basado en audit_df)
        now_ntz = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        cov_df = audit_df[['service_type','year','month','row_count']].copy()
        cov_df['url'] = cov_df.apply(lambda r: _build_url(r['service_type'], int(r['year']), int(r['month'])), axis=1)
        cov_df['has_parquet'] = cov_df['row_count'] > 0
        cov_df['http_status'] = cov_df['has_parquet'].apply(lambda v: 200 if bool(v) else None)
        cov_df['content_length'] = pd.NA
        cov_df['checked_at'] = pd.to_datetime(now_ntz)  # NTZ
        cov_df['notes'] = 'from_raw'
        cov_df = cov_df[['service_type','year','month','url','has_parquet','http_status','content_length','checked_at','notes']]

        # 6) Asegurar tablas y columnas
        cur.execute(DDL_AUDIT_MIN.format(db=DB, schema=SCHEMA))
        _ensure_audit_columns(conn, DB, SCHEMA)

        cur.execute(DDL_COVERAGE_MIN.format(db=DB, schema=SCHEMA))
        _ensure_coverage_columns(conn, DB, SCHEMA)

        # 7) Escribir en Snowflake (TRUNCATE + INSERT por defecto)
        fq_audit = f"{DB}.{SCHEMA}.load_audit"
        fq_cov   = f"{DB}.{SCHEMA}.coverage_matrix"

        if truncate:
            cur.execute(f"truncate table {fq_audit}")
            cur.execute(f"truncate table {fq_cov}")
        else:
            # delete selectivo para la malla solicitada
            keys = ", ".join([f"('{r.service_type}',{int(r.year)},{int(r.month)})" for r in base.itertuples(index=False)])
            if keys:
                cur.execute(f"delete from {fq_audit} where (service_type,year,month) in ({keys})")
                cur.execute(f"delete from {fq_cov}   where (service_type,year,month) in ({keys})")

        ok1, c1, n1, _ = write_pandas(conn, audit_df, table_name='load_audit', database=DB, schema=SCHEMA, quote_identifiers=False, chunk_size=100_000)
        ok2, c2, n2, _ = write_pandas(conn, cov_df,   table_name='coverage_matrix', database=DB, schema=SCHEMA, quote_identifiers=False, chunk_size=100_000)
        print(f"[load_audit] ok={ok1}, rows={n1}, chunks={c1}")
        print(f"[coverage_matrix] ok={ok2}, rows={n2}, chunks={c2}")

        # 8) (Opcional) Guardar coverage_matrix.csv en el repo
        if write_csv:
            out_path = os.path.join(get_repo_path(), 'coverage_matrix.csv')
            tmp = out_path + ".tmp"
            cov_df.to_csv(tmp, index=False)
            os.replace(tmp, out_path)
            print(f"[coverage_matrix] CSV escrito en {out_path}")

    finally:
        try: cur.close()
        except Exception: pass
        conn.close()
