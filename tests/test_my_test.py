from test_package.example import add_one

import pytest

@pytest.mark.parametrize("number, expected",
                          [(1,2),
                           (3,4),
                           (5,6)
                           ])
def test_func(number, expected):
    assert add_one(number) == expected

def test_connection(snowflake):
    snowflake
