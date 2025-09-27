# --- guard del template de Mage ---
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value

from pandas import DataFrame
import pandas as pd
import uuid
import time, requests, tempfile, os, logging, math

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pyarrow.parquet as pq
import pyarrow as pa

# Silenciar logs ruidosos de Snowflake
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
logging.getLogger('snowflake.connector.ocsp_snowflake').setLevel(logging.ERROR)
logging.getLogger('snowflake.connector.file_transfer_agent').setLevel(logging.ERROR)

# ===================== DDL BRONZE (ingest_ts como STRING) =====================
YELLOW_DDL = """
create table if not exists {db}.{schema}.yellow_trips (
    vendorid integer,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count integer,
    trip_distance float,
    ratecodeid integer,
    store_and_fwd_flag string,
    pulocationid integer,
    dolocationid integer,
    payment_type integer,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge float,
    airport_fee float,
    cbd_congestion_fee float,
    -- metadatos
    run_id string,
    ingest_ts string,    -- ISO string
    year int,
    month int,
    service_type string,
    source_url string
);
"""

GREEN_DDL = """
create table if not exists {db}.{schema}.green_trips (
    vendorid integer,
    lpep_pickup_datetime timestamp,
    lpep_dropoff_datetime timestamp,
    passenger_count integer,
    trip_distance float,
    ratecodeid integer,
    store_and_fwd_flag string,
    pulocationid integer,
    dolocationid integer,
    payment_type integer,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge float,
    trip_type integer,
    cbd_congestion_fee float,
    ehail_fee float,
    -- metadatos
    run_id string,
    ingest_ts string,    -- ISO string
    year int,
    month int,
    service_type string,
    source_url string
);
"""

# ===================== Columnas esperadas =====================
YELLOW_COLS = [
    'vendorid','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance',
    'ratecodeid','store_and_fwd_flag','pulocationid','dolocationid','payment_type','fare_amount',
    'extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount',
    'congestion_surcharge','airport_fee','cbd_congestion_fee'
]
GREEN_COLS = [
    'vendorid','lpep_pickup_datetime','lpep_dropoff_datetime','passenger_count','trip_distance',
    'ratecodeid','store_and_fwd_flag','pulocationid','dolocationid','payment_type','fare_amount',
    'extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount',
    'congestion_surcharge','trip_type','cbd_congestion_fee','ehail_fee'
]
META_COLS = ['run_id','ingest_ts','year','month','service_type','source_url']

# ===================== Conexión Snowflake =====================
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
        ocsp_fail_open=True,
        insecure_mode=True,
    )

# ===================== Utilidades =====================
def _download_parquet(url: str, timeout_connect=8, timeout_read=90) -> str:
    headers = {'User-Agent': 'mage-ai/nyc-tlc-pipeline'}
    with requests.get(url, headers=headers, stream=True, timeout=(timeout_connect, timeout_read)) as r:
        r.raise_for_status()
        fd, tmp_path = tempfile.mkstemp(suffix='.parquet'); os.close(fd)
        with open(tmp_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk: f.write(chunk)
    return tmp_path

def _normalize_trip_datetimes(pdf: pd.DataFrame, service: str) -> None:
    """
    Convierte pickup/dropoff a 'YYYY-MM-DD HH:MM:SS' como string (Snowflake TIMESTAMP_NTZ friendly).
    """
    if service == 'yellow':
        dt_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    else:
        dt_cols = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    for c in dt_cols:
        dt = pd.to_datetime(pdf[c], errors='coerce', utc=False)
        iso = dt.dt.strftime('%Y-%m-%d %H:%M:%S')
        pdf[c] = iso
        pdf.loc[dt.isna(), c] = None

# ===================== Exportador principal =====================
@data_exporter
def export_data(df: DataFrame, **kwargs) -> None:
    """
    Input (desde bloque 2): ['year','month','service_type','url','has_parquet', ...]
    - Crea tablas con ingest_ts como STRING (ISO)
    - Idempotencia por (service, year, month): DELETE previo
    - Descarga parquet, lee por row group, sube en micro-batches
    - Normaliza columnas y fechas (pickup/dropoff ISO; ingest_ts ISO)
    kwargs:
      - batch_size_yellow (int, default 100_000)
      - batch_size_green  (int, default 600_000)
    """
    if df is None or len(df) == 0:
        print('No hay filas de entrada.'); return
    df = df[df['has_parquet'] == True].copy()
    if df.empty:
        print('No hay archivos Parquet disponibles para cargar.'); return

    DB = get_secret_value('SNOWFLAKE_DATABASE')
    SCHEMA_RAW = get_secret_value('SNOWFLAKE_SCHEMA_RAW')

    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)
    df['service_type'] = df['service_type'].astype(str)

    bs_yellow = int(kwargs.get('batch_size_yellow', 400_000))
    bs_green  = int(kwargs.get('batch_size_green',  600_000))

    conn = _conn()
    try:
        cs = conn.cursor()
        # Crear tablas si no existen
        cs.execute(YELLOW_DDL.format(db=DB, schema=SCHEMA_RAW))
        cs.execute(GREEN_DDL.format(db=DB, schema=SCHEMA_RAW))
        # Asegurar columnas recientes
        cs.execute(f"alter table if exists {DB}.{SCHEMA_RAW}.yellow_trips add column if not exists cbd_congestion_fee float")
        cs.execute(f"alter table if exists {DB}.{SCHEMA_RAW}.green_trips  add column if not exists cbd_congestion_fee float")
        cs.execute(f"alter table if exists {DB}.{SCHEMA_RAW}.green_trips  add column if not exists ehail_fee float")

        for (service, year, month), part in df.groupby(['service_type', 'year', 'month']):
            urls = part['url'].tolist()
            table_name = f'{service}_trips'
            fq_table = f'{DB}.{SCHEMA_RAW}.{table_name}'

            # Cursor fresco por iteración
            try:
                cs = conn.cursor()
            except Exception:
                try: conn.close()
                except Exception: pass
                conn = _conn(); cs = conn.cursor()

            # Idempotencia por lote (replace de partición natural)
            cs.execute(
                f"delete from {fq_table} where year = %s and month = %s and service_type = %s",
                (year, month, service)
            )

            run_id = str(uuid.uuid4())
            total_rows = 0

            for url in urls:
                try:
                    print(f"[{service} {year}-{month:02d}] Descargando: {url}")
                    local_path = _download_parquet(url)

                    pf = pq.ParquetFile(local_path)
                    num_groups = pf.num_row_groups
                    print(f"[{service} {year}-{month:02d}] Row groups: {num_groups}")

                    for rg in range(num_groups):
                        tbl: pa.Table = pf.read_row_group(rg)
                        num_rows = tbl.num_rows
                        batch_size = bs_yellow if service == 'yellow' else bs_green
                        num_batches = max(1, math.ceil(num_rows / batch_size))

                        for b in range(num_batches):
                            t0 = time.time()
                            start = b * batch_size
                            end = min((b + 1) * batch_size, num_rows)
                            slice_tbl: pa.Table = tbl.slice(offset=start, length=end - start)
                            pdf = slice_tbl.to_pandas(split_blocks=True, self_destruct=True)

                            # normalizar columnas
                            pdf.columns = [str(c).lower() for c in pdf.columns]
                            base_cols = YELLOW_COLS if service == 'yellow' else GREEN_COLS
                            for c in base_cols:
                                if c not in pdf.columns:
                                    pdf[c] = pd.NA

                            # metadatos (ingest_ts ISO string)
                            pdf['run_id'] = run_id
                            pdf['ingest_ts'] = pd.Timestamp.utcnow().tz_localize(None).strftime('%Y-%m-%d %H:%M:%S')
                            pdf['year'] = year
                            pdf['month'] = month
                            pdf['service_type'] = service
                            pdf['source_url'] = url

                            # normalizar fechas pickup/dropoff a ISO
                            _normalize_trip_datetimes(pdf, service)

                            # orden final
                            pdf = pdf[base_cols + META_COLS]

                            ok, nchunks, nrows, _ = write_pandas(
                                conn, pdf,
                                table_name=table_name,
                                database=DB,
                                schema=SCHEMA_RAW,
                                quote_identifiers=False,
                                chunk_size=100_000,
                            )
                            total_rows += nrows
                            print(f"[{service} {year}-{month:02d}] RG {rg+1}/{num_groups} | batch {b+1}/{num_batches} → rows={nrows} ({round(time.time()-t0,1)}s)")

                    os.remove(local_path)
                except Exception as e:
                    print(f"[{service} {year}-{month:02d}] Error: {e}")

            print(f"[{service} {year}-{month:02d}] Total subido: {total_rows} filas")

    finally:
        try: cs.close()
        except Exception: pass
        conn.close()
