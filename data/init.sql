CREATE TABLE crypto_ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION,
    exchange TEXT
);

SELECT create_hypertable(
    'crypto_ticks',
    'time',
    partitioning_column => 'symbol',
    number_partitions => 10,
    if_not_exists => TRUE
);
