from test_package.example import add_one
from conftest import get_aws_session, get_secret
import pytest

@pytest.mark.parametrize("number, expected",
                          [(1,2),
                           (3,4),
                           (5,6)
                           ])
def test_func(number, expected):
    assert add_one(number) == expected

def test_connection(snowflake):
    your_secrets = get_secret("snowflake_credentials", get_aws_session())
    snowflake(your_secrets)
