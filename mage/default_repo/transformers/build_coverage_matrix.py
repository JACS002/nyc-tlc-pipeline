if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

import pandas as pd
import requests
from datetime import datetime
from mage_ai.settings.repo import get_repo_path
import os
import time
import random
from typing import Iterable, Tuple, List, Optional

# Defaults (puedes sobrescribirlos por kwargs)
DEFAULT_SERVICES = ['yellow']
DEFAULT_YEARS = list(range(2015, 2016))
DEFAULT_MONTHS = list(range(1, 2))
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def _build_url(service: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{service}_tripdata_{year}-{month:02d}.parquet"

def _check_parquet_with_retries(url: str, max_attempts=3, timeout=(4, 15), base_sleep=0.5):
    """
    HEAD con reintentos/backoff. Devuelve (has_parquet, status, content_length, notes).
    Reintenta en 403/5xx/transitorios.
    """
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/116.0 Safari/537.36'
        )
    }

    attempt = 0
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

            if status == 200 and (content_length is None or content_length > 0):
                return True, status, content_length, None

            if status in (403, 404):
                notes = 'missing' if status == 404 else 'forbidden'
                if status == 403 and attempt < max_attempts:
                    sleep = base_sleep * (2 ** (attempt - 1)) + random.random() * 0.5
                    time.sleep(sleep)
                    continue
                return False, status, content_length, notes

            # 5xx u otros
            notes = f'unexpected_status_{status}'
            if 500 <= status < 600 and attempt < max_attempts:
                sleep = base_sleep * (2 ** (attempt - 1)) + random.random() * 0.5
                time.sleep(sleep)
                continue
            return False, status, content_length, notes

        except Exception as e:
            if attempt < max_attempts:
                sleep = base_sleep * (2 ** (attempt - 1)) + random.random() * 0.5
                time.sleep(sleep)
                continue
            return False, None, None, f'error:{type(e).__name__}'

def _atomic_write_csv(df: pd.DataFrame, path: str):
    """Escritura atómica para evitar archivos truncos."""
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def _coerce_params(
    services: Optional[Iterable[str]],
    years: Optional[Iterable[int]],
    months: Optional[Iterable[int]],
    pairs: Optional[Iterable[Tuple[int, int]]]
) -> List[Tuple[str, int, int]]:
    """
    Normaliza la selección:
      - Si pasas 'pairs' como [(2020,1),(2020,2)], usa eso.
      - Si no, usa el producto cartesiano services x years x months.
    """
    if pairs:
        pairs = list(pairs)
    services = list(services) if services is not None else DEFAULT_SERVICES
    years = list(years) if years is not None else DEFAULT_YEARS
    months = list(months) if months is not None else DEFAULT_MONTHS

    targets = []
    if pairs:
        # Repite pairs para cada servicio elegido
        for svc in services:
            for (y, m) in pairs:
                targets.append((svc, y, m))
    else:
        for svc in services:
            for y in years:
                for m in months:
                    targets.append((svc, y, m))
    return targets

@transformer
def transform(*args, **kwargs) -> pd.DataFrame:
    """
    Crea/actualiza 'coverage_matrix.csv' sin borrar lo previo.
    - Recalcula solo los (service, year, month) que pidas.
    - Conservará los registros existentes para combinaciones no tocadas.
    - Parámetros opcionales por kwargs:
        services=['green'] o ['yellow'] o ambas
        years=[2020,2021]
        months=[1,2,3]
        pairs=[(2020,1),(2020,2)]  # útil para rangos pequeños
        throttle_ms=80  # pausa entre requests
    """
    services = kwargs.get('services', None)          # iterable[str] o None
    years = kwargs.get('years', None)                # iterable[int] o None
    months = kwargs.get('months', None)              # iterable[int] o None
    pairs = kwargs.get('pairs', None)                # iterable[(int,int)] o None
    throttle_ms = int(kwargs.get('throttle_ms', 80))

    # Generar las combinaciones a consultar
    targets = _coerce_params(services, years, months, pairs)

    now_iso = datetime.utcnow().isoformat(timespec='seconds') + 'Z'
    new_rows = []
    for service, year, month in targets:
        url = _build_url(service, year, month)
        has_parquet, http_status, content_length, notes = _check_parquet_with_retries(url)
        new_rows.append({
            'service_type': service,
            'year': int(year),
            'month': int(month),
            'url': url,
            'has_parquet': bool(has_parquet),
            'http_status': http_status,
            'content_length': content_length,
            'checked_at': now_iso,
        })
        time.sleep(throttle_ms / 1000.0)

    df_new = pd.DataFrame(new_rows)
    keys = ['service_type', 'year', 'month']

    # Ruta del CSV en el repo
    repo = get_repo_path()
    out_path = os.path.join(repo, 'coverage_matrix.csv')

    # Si existe, fusionamos; si no, creamos desde cero
    if os.path.exists(out_path):
        try:
            df_old = pd.read_csv(out_path)
            # Asegurar tipos básicos por si vienen como strings
            if not df_old.empty:
                if 'year' in df_old.columns: df_old['year'] = df_old['year'].astype(int, errors='ignore')
                if 'month' in df_old.columns: df_old['month'] = df_old['month'].astype(int, errors='ignore')
        except Exception as e:
            print(f"[coverage][warning] No pude leer CSV existente, se recreará: {e}")
            df_old = pd.DataFrame(columns=df_new.columns)

        # Indexar y hacer upsert (update + insert)
        df_old_idx = df_old.set_index(keys, drop=False)
        df_new_idx = df_new.set_index(keys, drop=False)

        # Copia para actualizar coincidencias
        df_merged = df_old_idx.copy()
        # 'update' reemplaza columnas de filas coincidentes con los valores de df_new
        df_merged.update(df_new_idx)

        # Agregar filas nuevas que no existían antes
        missing_idx = df_new_idx.index.difference(df_old_idx.index)
        if len(missing_idx) > 0:
            df_merged = pd.concat([df_merged, df_new_idx.loc[missing_idx]], axis=0)

        # Reset index y ordenar por llave
        df_final = df_merged.reset_index(drop=True).sort_values(keys).reset_index(drop=True)
    else:
        df_final = df_new.sort_values(keys).reset_index(drop=True)

    # Escritura atómica
    try:
        _atomic_write_csv(df_final, out_path)
        print(f"[coverage] CSV actualizado en {out_path} | total_filas={len(df_final)} | nuevas_o_actualizadas={len(df_new)}")
    except Exception as e:
        print(f"[coverage][warning] No pude escribir CSV: {e}")

    # Devolvemos SOLO lo recién consultado
    return df_new
