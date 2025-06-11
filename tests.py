import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn
