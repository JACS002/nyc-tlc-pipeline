if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

import pandas as pd
import requests
from datetime import datetime
from mage_ai.settings.repo import get_repo_path
import os

# Constantes
SERVICES = ['yellow', 'green']
YEARS = list(range(2015, 2026))
MONTHS = list(range(1, 13))
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def _build_url(service: str, year: int, month: int) -> str:
    """Construye la URL oficial del parquet de NYC TLC."""
    return f"{BASE_URL}/{service}_tripdata_{year}-{month:02d}.parquet"

def _check_parquet(url: str, timeout=(4, 15)):
    """
    Verifica si existe un archivo parquet usando HEAD.
    Devuelve: (has_parquet: bool, http_status: int|None, content_length: int|None, notes: str|None)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; checker/1.0)'}
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        status = r.status_code

        content_length = None
        if 'Content-Length' in r.headers:
            try:
                content_length = int(r.headers.get('Content-Length') or 0)
            except Exception:
                content_length = None

        notes = None
        has_parquet = False
        if status == 200 and (content_length is None or content_length > 0):
            has_parquet = True
        elif status in (403, 404):
            has_parquet = False
            notes = 'missing'
        else:
            has_parquet = False
            notes = f'unexpected_status_{status}'

        return has_parquet, status, content_length, notes

    except Exception as e:
        return False, None, None, f'error:{type(e).__name__}'

@transformer
def transform(*args, **kwargs) -> pd.DataFrame:
    """
    Genera un coverage matrix de archivos parquet disponibles en NYC TLC.
    Guarda CSV en repo y devuelve DataFrame listo para análisis.
    """
    rows = []
    now_iso = datetime.utcnow().isoformat(timespec='seconds') + 'Z'

    for service in SERVICES:
        for year in YEARS:
            for month in MONTHS:
                url = _build_url(service, year, month)
                has_parquet, http_status, content_length, notes = _check_parquet(url)
                rows.append({
                    'service_type': service,
                    'year': year,
                    'month': month,
                    'url': url,
                    'has_parquet': has_parquet,
                    'http_status': http_status,
                    'content_length': content_length,
                    'checked_at': now_iso,
                    'notes': notes,
                })

    df = pd.DataFrame(rows)

    # Guardar CSV en el repo
    try:
        repo = get_repo_path()
        out_path = os.path.join(repo, 'coverage_matrix.csv')
        df.to_csv(out_path, index=False)
        print(f"[coverage] Escribí {len(df)} filas en {out_path}")
    except Exception as e:
        print(f"[coverage][warning] No pude escribir CSV: {e}")

    return df
