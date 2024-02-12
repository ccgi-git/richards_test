import pytest
from snowflake.snowpark.session import Session
@pytest.fixture(scope = "session")
def snowflake():
    snowflake_conn_prop = {
        'user': 'rchuang',
        'password': '',
        'account': 'AT38648',
        'warehouse': 'COMPUTE_WH',
        'protocol': 'https'
    }
    #session = Session.builder.configs(snowflake_conn_prop).create()

    return snowflake_conn_prop