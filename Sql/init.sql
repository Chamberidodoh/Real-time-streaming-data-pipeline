CREATE TABLE raw_events (
  event_id    TEXT PRIMARY KEY,
  user_id     INT,
  event_type  TEXT,
  product_id  INT,
  price       NUMERIC(10,2),
  event_time  TIMESTAMP,
  ingested_at TIMESTAMP DEFAULT now()
);

CREATE TABLE agg_per_minute (
  window_start TIMESTAMP,
  event_type   TEXT,
  event_count  BIGINT,
  revenue      NUMERIC(12,2),
  PRIMARY KEY (window_start, event_type)
);

CREATE TABLE dead_letter (
  id          SERIAL PRIMARY KEY,
  raw_value   TEXT,
  failed_at   TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_raw_event_time ON raw_events (event_time);