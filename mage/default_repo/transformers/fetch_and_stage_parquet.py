if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import requests
from datetime import datetime

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def _build_url(service: str, year: int, month: int) -> str:
    # Formato oficial: yellow_tripdata_YYYY-MM.parquet / green_tripdata_YYYY-MM.parquet
    return f"{BASE_URL}/{service}_tripdata_{year}-{month:02d}.parquet"

@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Verifica si existe el archivo Parquet para cada (year, month, service_type).
    NO descarga; usa peticiones HEAD para ahorrar tiempo/ancho de banda.

    Input:
        data: DataFrame del bloque generate_months con columnas:
              ['year', 'month', 'service_type']

    Output:
        DataFrame con columnas:
          ['year','month','service_type','url','has_parquet','http_status',
           'content_length','checked_at','notes']
    """
    rows_out = []
    now_iso = datetime.utcnow().isoformat(timespec='seconds') + 'Z'

    # Seguridad: asegurarnos de que vienen las columnas esperadas
    required_cols = {'year', 'month', 'service_type'}
    missing = required_cols - set(data.columns)
    if missing:
        raise ValueError(f"Faltan columnas en la entrada del bloque: {missing}")

    # FILTRO
    data = data.copy()
    #Query de prueba para 2019
    #data = data.query("year == 2019").copy()



    for _, r in data.iterrows():
        year = int(r['year'])
        month = int(r['month'])
        service = str(r['service_type']).lower().strip()

        url = _build_url(service, year, month)
        http_status = None
        has_parquet = False
        content_length = None
        notes = None

        try:
            resp = requests.head(url, allow_redirects=True, timeout=10)
            http_status = resp.status_code
            # Algunos endpoints devuelven Content-Length:
            if 'Content-Length' in resp.headers:
                try:
                    content_length = int(resp.headers['Content-Length'])
                except Exception:
                    content_length = None

            if resp.status_code == 200:
                has_parquet = True
            elif resp.status_code in (403, 404):
                has_parquet = False
                notes = 'missing'
            else:
                has_parquet = False
                notes = f'unexpected_status_{resp.status_code}'
        except Exception as e:
            http_status = None
            has_parquet = False
            notes = f'error:{type(e).__name__}'

        rows_out.append({
            'year': year,
            'month': month,
            'service_type': service,
            'url': url,
            'has_parquet': has_parquet,
            'http_status': http_status,
            'content_length': content_length,
            'checked_at': now_iso,
            'notes': notes,
        })

    return pd.DataFrame(rows_out)


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Validaciones básicas del resultado.
    """
    assert output is not None, 'El output es None'
    assert len(output) > 0, 'El output está vacío'

    expected_cols = {
        'year', 'month', 'service_type', 'url',
        'has_parquet', 'http_status', 'content_length',
        'checked_at', 'notes'
    }
    missing = expected_cols - set(output.columns)
    assert not missing, f'Faltan columnas esperadas: {missing}'

    # Tipos básicos
    assert output['year'].dtype.kind in 'iu', 'year debe ser entero'
    assert output['month'].dtype.kind in 'iu', 'month debe ser entero'
    assert output['service_type'].dtype.kind in 'OSU', 'service_type debe ser texto'
    assert output['url'].str.contains('http').all(), 'Hay URLs inválidas'
