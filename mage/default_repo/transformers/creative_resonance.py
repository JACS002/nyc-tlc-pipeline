if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

from mage_ai.data_preparation.shared.secrets import get_secret_value
import snowflake.connector

@transformer
def transform(*args, **kwargs):
    """
    Prueba conexión a Snowflake con Mage Secrets.
    Retorna información básica de la sesión actual.
    """
    conn = snowflake.connector.connect(
        account=get_secret_value('SNOWFLAKE_ACCOUNT'),
        user=get_secret_value('SNOWFLAKE_USER'),
        password=get_secret_value('SNOWFLAKE_PASSWORD'),
        role=get_secret_value('SNOWFLAKE_ROLE'),
        warehouse=get_secret_value('SNOWFLAKE_WAREHOUSE'),
        database=get_secret_value('SNOWFLAKE_DATABASE'),
        schema=get_secret_value('SNOWFLAKE_SCHEMA_RAW'),
    )
    cs = conn.cursor()
    try:
        cs.execute("""
            select current_user(), current_role(), current_warehouse(),
                   current_database(), current_schema();
        """)
        row = cs.fetchone()
        print("Conexión exitosa ✅")
        print("User:", row[0])
        print("Role:", row[1])
        print("Warehouse:", row[2])
        print("Database:", row[3])
        print("Schema:", row[4])
        return {
            'user': row[0],
            'role': row[1],
            'warehouse': row[2],
            'database': row[3],
            'schema': row[4],
        }
    finally:
        cs.close()
        conn.close()
