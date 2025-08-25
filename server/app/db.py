# server/app/db.py
# Простое подключение к PostgreSQL через psycopg
# Комментарии даны «для живых людей» — не бойтесь править.

import os
import psycopg

DB_URL = os.getenv("DB_URL", "postgresql://meow:meow@db:5432/meow")

# Подключаться «по требованию», чтобы не держать постоянный коннект

def get_conn():
    return psycopg.connect(DB_URL)

# Применяем минимальную схему при старте
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS places (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  address_norm TEXT NOT NULL,
  city TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  accuracy TEXT,
  manual_override BOOLEAN DEFAULT FALSE,
  updated_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  starts_at DATE NOT NULL,
  ends_at DATE,
  place_id INTEGER REFERENCES places(id),
  source TEXT,
  source_post_id TEXT,
  url TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS geocode_cache (
  address_norm TEXT PRIMARY KEY,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  provider TEXT,
  updated_at TIMESTAMP DEFAULT NOW()
);
"""

def apply_schema():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            conn.commit()