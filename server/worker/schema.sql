-- server/db/schema.sql
CREATE TABLE IF NOT EXISTS places(
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  address_norm TEXT NOT NULL UNIQUE,
  city TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  manual_override BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS events(
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  starts_at DATE NOT NULL,
  place_id INTEGER NOT NULL REFERENCES places(id) ON DELETE RESTRICT,
  source TEXT,
  source_post_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_starts_at ON events(starts_at);
