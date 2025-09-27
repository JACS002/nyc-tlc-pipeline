if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

import pandas as pd
import requests
from datetime import datetime
from mage_ai.settings.repo import get_repo_path
import os
import time
import random

# Constantes
SERVICES_ORDER = ['green', 'yellow']   # procesar green primero, luego yellow
YEARS = list(range(2015, 2026))
MONTHS = list(range(1, 13))
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def _build_url(service: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{service}_tripdata_{year}-{month:02d}.parquet"

def _check_parquet_with_retries(url: str, max_attempts=3, timeout=(4,15), base_sleep=0.5):
    """
    HEAD con reintentos/backoff. Devuelve (has_parquet, status, content_length, notes).
    Considera reintentar en caso de 403/5xx/transitorios.
    """
    headers = {
        # usar User-Agent "razonable" para evitar bloqueos básicos
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/116.0 Safari/537.36'
    }

    attempt = 0
    last_exc = None
    while attempt < max_attempts:
        attempt += 1
        try:
            r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
            status = r.status_code
            content_length = None
            if 'Content-Length' in r.headers:
                try:
                    content_length = int(r.headers.get('Content-Length') or 0)
                except Exception:
                    content_length = None

            # Heurística: si 200 y content_length>0 (o None) -> OK
            if status == 200 and (content_length is None or content_length > 0):
                return True, status, content_length, None
            if status in (403, 404):
                # Puede ser transitorio: reintentamos algunas veces para 403, pero después lo marcamos como missing
                notes = 'missing' if status == 404 else 'forbidden'
                # reintentar si no es el último intento y es 403 (posible bloqueo temporal)
                if status == 403 and attempt < max_attempts:
                    sleep = base_sleep * (2 ** (attempt - 1)) + random.random() * 0.5
                    time.sleep(sleep)
                    continue
                return False, status, content_length, notes

            # Otros códigos (5xx etc) -> reintentar
            notes = f'unexpected_status_{status}'
            if 500 <= status < 600 and attempt < max_attempts:
                sleep = base_sleep * (2 ** (attempt - 1)) + random.random() * 0.5
                time.sleep(sleep)
                continue
            return False, status, content_length, notes

        except Exception as e:
            last_exc = e
            # reintentar en caso de excepciones transitorias
            if attempt < max_attempts:
                sleep = base_sleep * (2 ** (attempt - 1)) + random.random() * 0.5
                time.sleep(sleep)
                continue
            return False, None, None, f'error:{type(e).__name__}'

@transformer
def transform(*args, **kwargs) -> pd.DataFrame:
    """
    Coverage matrix: procesa green primero, luego yellow; reintentos para 403/transitorios;
    guarda un único CSV unificado en repo ('coverage_matrix.csv').
    """
    rows = []
    now_iso = datetime.utcnow().isoformat(timespec='seconds') + 'Z'

    for service in SERVICES_ORDER:
        for year in YEARS:
            for month in MONTHS:
                url = _build_url(service, year, month)
                has_parquet, http_status, content_length, notes = _check_parquet_with_retries(url)
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

                # pausa corta para evitar bursts y throttling
                time.sleep(0.08)  # ~80ms entre requests; ajusta si necesitas más lento

    df = pd.DataFrame(rows)

    # Guardar CSV en el repo (unificado)
    try:
        repo = get_repo_path()
        out_path = os.path.join(repo, 'coverage_matrix.csv')
        df.to_csv(out_path, index=False)
        print(f"[coverage] Escribí {len(df)} filas en {out_path}")
    except Exception as e:
        print(f"[coverage][warning] No pude escribir CSV: {e}")

    return df
