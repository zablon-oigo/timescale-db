import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn

def test_on_event_price_inserts_when_batch_full(monkeypatch, mock_conn):
    pipeline = WebsocketPipeline(mock_conn)
    pipeline.MAX_BATCH_SIZE = 2
