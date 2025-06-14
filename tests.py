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

    sample_event = {
        "event": "price",
        "timestamp": datetime.now(tz=timezone.utc).timestamp(),
        "symbol": "BTC/USD",
        "price": 50000.0,
        "day_volume": 1000,
        "exchange": "NASDAQ"
    }

    pipeline._on_event(sample_event)
    assert len(pipeline.current_batch) == 1

    pipeline._on_event(sample_event)
    assert len(pipeline.current_batch) == 0

def test_on_event_non_price_event_is_ignored(mock_conn):
    pipeline = WebsocketPipeline(mock_conn)
    pipeline._on_event({"event": "heartbeat"})
