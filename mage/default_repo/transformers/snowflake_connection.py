if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
from mage_ai.data_preparation.shared.secrets import get_secret_value
import os
import snowflake.connector as sf

@transformer
def test_snowflake_connection(*args, **kwargs):
    conn = sf.connect(
        account=get_secret_value('SNOWFLAKE_ACCOUNT'),
        user=get_secret_value('SNOWFLAKE_USER'),
        password=get_secret_value('SNOWFLAKE_PASSWORD'),
        role=get_secret_value('SNOWFLAKE_ROLE'),
        warehouse=get_secret_value('SNOWFLAKE_WAREHOUSE'),
        database=get_secret_value('SNOWFLAKE_DATABASE'),
        schema=get_secret_value('SNOWFLAKE_SCHEMA_SILVER'),
        insecure_mode=True,   # igual que en ingest
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
            print('[SF CONNECTED]', cur.fetchone())
    finally:
        conn.close()
    return "OK"

