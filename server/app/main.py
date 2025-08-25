# server/app/main.py
# Минимальный API: /health, /events (по дате), /places (поиск)

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import date
from .db import get_conn, apply_schema

app = FastAPI(title="MeowAfisha API")
apply_schema()  # создаём таблицы, если их ещё нет

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/events")
def list_events(
    date_from: date = Query(..., description="включительно"),
    date_to:   date = Query(..., description="включительно"),
):
    sql = """
        SELECT e.id, e.title, e.starts_at AS date, p.lat, p.lon, p.address_norm AS location
        FROM events e
        LEFT JOIN places p ON p.id = e.place_id
        WHERE e.starts_at BETWEEN %s AND %s
        ORDER BY e.starts_at, e.title
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (date_from, date_to))
            rows = cur.fetchall()
    cols = ["id","title","date","lat","lon","location"]
    return [dict(zip(cols, r)) for r in rows]

@app.get("/places")
def list_places(q: str = ""):
    sql = """
        SELECT id, name, lat, lon, address_norm FROM places
        WHERE name ILIKE %s OR address_norm ILIKE %s
        ORDER BY name
        LIMIT 100
    """
    like = f"%{q}%"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (like, like))
            rows = cur.fetchall()
    cols = ["id","name","lat","lon","address_norm"]
    return [dict(zip(cols, r)) for r in rows]