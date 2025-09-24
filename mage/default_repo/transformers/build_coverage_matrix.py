if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

import pandas as pd
import requests
from datetime import datetime
from mage_ai.settings.repo import get_repo_path
import os

SERVICES = ['yellow', 'green']
YEARS = list(range(2015, 2026))
MONTHS = list(range(1, 13))

def _url(service, year, month):
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{month:02d}.parquet"

def _check_parquet(url, timeout=(4, 15)):
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        status = r.status_code
        clen = int(r.headers.get('Content-Length', 0) or 0)
        return (status == 200 and clen > 0), status, clen
    except Exception:
        return False, None, None

@transformer
def transform(*args, **kwargs):
    rows = []
    for s in SERVICES:
        for y in YEARS:
            for m in MONTHS:
                url = _url(s, y, m)
                has_parquet, http_status, content_length = _check_parquet(url)
                rows.append({
                    'service_type': s,
                    'year': y,
                    'month': m,
                    'url': url,
                    'has_parquet': has_parquet,
                    'http_status': http_status,
                    'content_length': content_length,
                    'checked_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                })

    df = pd.DataFrame(rows)

    # Guarda CSV en el repo (VSCode)
    repo = get_repo_path()
    out_path = os.path.join(repo, 'coverage_matrix.csv')
    df.to_csv(out_path, index=False)
    print(f"[coverage] Escribí {len(df)} filas en {out_path}")

    # Devolver DF por si quieres encadenar o visualizar en Mage
    return df
