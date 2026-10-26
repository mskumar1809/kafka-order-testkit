-- Orders written by the processor. order_id is the primary key, which is what makes
-- the consumer idempotent: a redelivered message hits ON CONFLICT DO NOTHING.
CREATE TABLE IF NOT EXISTS orders (
    order_id        uuid PRIMARY KEY,
    client_order_id text           NOT NULL,
    account         text           NOT NULL,
    symbol          text           NOT NULL,
    side            text           NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty             bigint         NOT NULL CHECK (qty > 0),
    price           numeric(18, 6) NOT NULL CHECK (price > 0),
    notional        numeric(24, 6) NOT NULL,
    order_ts        timestamptz    NOT NULL,
    status          text           NOT NULL CHECK (status IN ('ACCEPTED', 'REJECTED')),
    reject_reason   text,
    correlation_id  text,
    -- Where the order came from in Kafka; useful for ordering checks and incident triage.
    kafka_partition integer        NOT NULL,
    kafka_offset    bigint         NOT NULL,
    processed_at    timestamptz    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_account_symbol_idx ON orders (account, symbol);

-- Net signed position per account and symbol, from accepted orders only.
-- Reconciliation tests compare this against totals computed independently.
CREATE OR REPLACE VIEW positions AS
SELECT account,
       symbol,
       SUM(CASE side WHEN 'BUY' THEN qty ELSE -qty END) AS net_qty
FROM orders
WHERE status = 'ACCEPTED'
GROUP BY account, symbol;
