if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@transformer
def transform(*args, **kwargs):
    """
    Genera la matriz de (year, month, service_type) para 2015–2025.
    Esto servirá como entrada para los siguientes bloques de ingesta.
    """
    years = range(2015, 2026)  # 2015–2025 inclusive
    services = ['yellow', 'green']
    rows = []

    for y in years:
        for m in range(1, 13):
            for s in services:
                rows.append({'year': y, 'month': m, 'service_type': s})

    # Devolvemos un DataFrame para que Mage lo pueda manejar abajo
    return pd.DataFrame(rows)


@test
def test_output(output, *args) -> None:
    """
    Verifica que el bloque produjo datos válidos.
    """
    # Debe haber 11 años * 12 meses * 2 servicios = 264 filas
    assert len(output) == 264, f'Esperaba 264 filas, encontré {len(output)}'

    # Validar que contiene las columnas esperadas
    for col in ['year', 'month', 'service_type']:
        assert col in output.columns, f'Falta columna {col}'
