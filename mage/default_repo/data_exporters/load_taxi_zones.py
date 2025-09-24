# --- guard del template de Mage ---
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value

import pandas as pd
import requests, tempfile, os, logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

# URLs candidatas (la CDN de TLC a veces cambia el nombre)
CANDIDATE_URLS = [
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv",  # fallback
]

DDL_ZONES = """
create table if not exists {db}.{schema}.taxi_zones (
    locationid int,
    borough string,
    zone string,
    service_zone string
);
"""

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

def _download_csv() -> str:
    last_err = None
    for url in CANDIDATE_URLS:
        try:
            r = requests.get(url, timeout=(5, 60))
            r.raise_for_status()
            fd, tmp = tempfile.mkstemp(suffix='.csv'); os.close(fd)
            with open(tmp, 'wb') as f:
                f.write(r.content)
            print(f"[taxi_zones] Descargado desde: {url}")
            return tmp
        except Exception as e:
            last_err = e
            print(f"[taxi_zones] Intento fallido {url}: {e}")
    raise RuntimeError(f"No se pudo descargar Taxi Zones: {last_err}")

@data_exporter
def export_data(*args, **kwargs) -> None:
    DB = get_secret_value('SNOWFLAKE_DATABASE')
    SCHEMA = get_secret_value('SNOWFLAKE_SCHEMA_RAW')  # BRONZE

    # 1) Descargar CSV
    csv_path = _download_csv()

    # 2) Leer y normalizar
    df = pd.read_csv(csv_path)
    os.remove(csv_path)

    # normalizar nombres
    df.columns = [c.strip().lower() for c in df.columns]
    # renombrar si vinieran con mayúsculas o espacios
    rename_map = {
        'locationid': 'locationid',
        'borough': 'borough',
        'zone': 'zone',
        'service_zone': 'service_zone',
    }
    df = df.rename(columns=rename_map)

    # asegurar columnas
    for c in ['locationid', 'borough', 'zone', 'service_zone']:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[['locationid', 'borough', 'zone', 'service_zone']].copy()

    # tipificar
    df['locationid'] = pd.to_numeric(df['locationid'], errors='coerce').astype('Int64')

    # 3) Crear tabla si no existe
    conn = _conn()
    cs = conn.cursor()
    try:
        cs.execute(DDL_ZONES.format(db=DB, schema=SCHEMA))

        # 4) Idempotencia: reemplazar contenido
        cs.execute(f"truncate table {DB}.{SCHEMA}.taxi_zones")

        # 5) Cargar
        ok, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name='taxi_zones',
            database=DB,
            schema=SCHEMA,
            quote_identifiers=False,
            chunk_size=10_000,
        )
        print(f"[taxi_zones] Cargado ok={ok}, filas={nrows}, chunks={nchunks}")
    finally:
        try: cs.close()
        except Exception: pass
        conn.close()
