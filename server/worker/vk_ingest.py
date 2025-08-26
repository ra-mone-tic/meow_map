# server/worker/vk_ingest.py
# Сбор постов VK → извлечение даты/локации → геокодирование → UPSERT в БД (опционально)
# + экспорт совместимого с фронтом JSON (events.json) для index.html

import os, re, time, json, datetime, requests
from pathlib import Path
from hashlib import sha1

from geopy.geocoders import ArcGIS, Nominatim
from geopy.extra.rate_limiter import RateLimiter

USE_DB = True
try:
    import psycopg
except Exception:
    USE_DB = False  # позволяем работать без psycopg/БД

# ──────────────── ENV / пути ────────────────
DB_URL    = os.getenv("DB_URL", "postgresql://meow:meow@db:5432/meow")
VK_TOKEN  = os.getenv("VK_TOKEN")
VK_DOMAIN = os.getenv("VK_DOMAIN", "meowafisha")

# repo_root = …/server/worker/ -> подняться на 2 уровня → корень репо
REPO_ROOT   = Path(__file__).resolve().parents[2]
PATH_WWW    = REPO_ROOT / "www"
PATH_WWW.mkdir(parents=True, exist_ok=True)
PATH_JSON   = PATH_WWW / "events.json"          # именно этот файл ждёт фронт
PATH_OVR    = REPO_ROOT / "data" / "manual_overrides.json"
PATH_CACHE  = REPO_ROOT / "data" / "geocode_cache.json"
PATH_BAD    = REPO_ROOT / "logs" / "bad_addresses.log"
PATH_BAD.parent.mkdir(parents=True, exist_ok=True)

YEAR_DEFAULT = str(datetime.date.today().year)

# Геокодеры (ArcGIS → OSM) + rate limit
arc = ArcGIS(timeout=10)
osm = Nominatim(user_agent="meowafisha", timeout=10)
geo_arc = RateLimiter(arc.geocode, min_delay_seconds=1.0)
geo_osm = RateLimiter(osm.geocode, min_delay_seconds=1.0)

# ──────────────── Утилиты ────────────────
def norm_addr(addr: str) -> str:
    s = addr.lower().strip()
    s = re.sub(r"\s*[\(\[\{].*?[\)\]\}]\s*", " ", s)
    s = re.sub(r"[.,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not re.search(r"(калининград|пионерский|светлогорск|гурьевск|янтарный|балтийск)", s):
        s += " калининград"
    return s

def load_json(p: Path, default):
    if p.exists():
        try:
            return json.loads(p.read_text("utf-8"))
        except:
            return default
    return default

def save_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(p)

def smart_geocode(raw_addr: str, overrides, cache):
    # 1) ручные координаты по raw-ключу
    if raw_addr in overrides:
        d = overrides[raw_addr]
        cache[norm_addr(raw_addr)] = {**d, "provider":"manual", "ts": datetime.date.today().isoformat()}
        return d["lat"], d["lon"], "manual"
    # 2) кэш по нормализованному адресу
    key = norm_addr(raw_addr)
    if key in cache:
        d = cache[key]
        return d["lat"], d["lon"], d.get("provider","cache")
    # 3) ArcGIS → OSM
    for prov, fn in (("arcgis", geo_arc), ("osm", geo_osm)):
        try:
            g = fn(raw_addr)
            if g:
                lat, lon = float(g.latitude), float(g.longitude)
                cache[key] = {"lat":lat, "lon":lon, "provider":prov, "ts": datetime.date.today().isoformat()}
                return lat, lon, prov
        except Exception:
            pass
    return None, None, None

def vk_wall(offset: int, batch: int = 100):
    url = "https://api.vk.com/method/wall.get"
    params = dict(domain=VK_DOMAIN, offset=offset, count=batch, access_token=VK_TOKEN, v="5.199")
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    j = r.json()
    if "response" not in j:
        return []
    return j["response"]["items"]

def extract(text: str):
    # DD.MM + «📍 …»
    m_date = re.search(r"\b(\d{2})\.(\d{2})\b", text)
    m_loc  = re.search(r"📍\s*(.+)", text)
    if not (m_date and m_loc):
        return None
    date  = f"{YEAR_DEFAULT}-{m_date.group(2)}-{m_date.group(1)}"
    loc   = m_loc.group(1).split('➡️')[0].strip()
    title = re.sub(r"^\d{2}\.\d{2}\s*\|\s*", "", text.split('\n')[0]).strip()
    return dict(title=title, date=date, location=loc)

def event_id(source: str, post_id: str, title: str, date: str) -> str:
    raw = f"{source}|{post_id}|{title}|{date}".encode("utf-8")
    return sha1(raw).hexdigest()

# ──────────────── БД ────────────────
DDL_PLACES = """
CREATE TABLE IF NOT EXISTS places(
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  address_norm TEXT NOT NULL UNIQUE,
  city TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  manual_override BOOLEAN DEFAULT FALSE
);
"""
DDL_EVENTS = """
CREATE TABLE IF NOT EXISTS events(
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  starts_at DATE NOT NULL,
  place_id INTEGER NOT NULL REFERENCES places(id) ON DELETE RESTRICT,
  source TEXT,
  source_post_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_starts_at ON events(starts_at);
"""

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_PLACES)
        cur.execute(DDL_EVENTS)
        conn.commit()

def upsert_event(conn, ev, lat, lon, provider):
    address_norm = norm_addr(ev["location"])
    with conn.cursor() as cur:
        # places: уникальность по address_norm
        cur.execute("""
            INSERT INTO places(name, address_norm, city, lat, lon, manual_override)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (address_norm) DO UPDATE
            SET name = EXCLUDED.name,
                lat  = EXCLUDED.lat,
                lon  = EXCLUDED.lon,
                manual_override = EXCLUDED.manual_override
            RETURNING id
        """, (ev["location"], address_norm, None, lat, lon, provider=="manual"))
        place_id = cur.fetchone()[0]
        # events: идемпотентный insert по детерминированному id
        cur.execute("""
            INSERT INTO events(id, title, starts_at, place_id, source, source_post_id)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
        """, (ev["id"], ev["title"], ev["date"], place_id, "vk", ev["post_id"]))
        conn.commit()

def export_events_json_from_db(conn, out_path: Path):
    q = """
    SELECT e.id, e.title, e.starts_at::text AS date,
           p.name AS location, p.lat, p.lon
    FROM events e
    JOIN places p ON p.id = e.place_id
    WHERE e.starts_at >= CURRENT_DATE - INTERVAL '60 days'
    ORDER BY e.starts_at ASC;
    """
    with conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    data = [dict(zip(cols, r)) for r in rows]
    # фронт ждёт поля: title, date(YYYY-MM-DD), location, lat, lon (см. index.html) :contentReference[oaicite:1]{index=1}
    save_json(out_path, data)

def export_events_json_direct(records_with_coords, out_path: Path):
    # Фолбэк: если БД не используется — пишем JSON напрямую из собранных данных
    data = [
        {
            "id": r["id"],
            "title": r["title"],
            "date": r["date"],
            "location": r["location"],
            "lat": r["lat"],
            "lon": r["lon"],
        }
        for r in records_with_coords
    ]
    data.sort(key=lambda x: x["date"])
    save_json(out_path, data)

# ──────────────── main ────────────────
def main():
    overrides = load_json(PATH_OVR, {})
    cache     = load_json(PATH_CACHE, {})

    records, off, BATCH = [], 0, 100
    while off < 2000:
        items = vk_wall(off, BATCH)
        if not items:
            break
        for it in items:
            text = it.get("text", "")
            ev = extract(text)
            if not ev:
                continue
            ev["post_id"] = str(it.get("id", ""))
            ev["id"] = event_id("vk", ev["post_id"], ev["title"], ev["date"])
            records.append(ev)
        off += BATCH
        time.sleep(1.1)

    bad = set()
    # Геокодировать и (опционально) писать в БД
    if USE_DB:
        with psycopg.connect(DB_URL) as conn:
            ensure_schema(conn)
            enriched = []
            for ev in records:
                lat, lon, prov = smart_geocode(ev["location"], overrides, cache)
                if lat is None:
                    bad.add(ev["location"]); continue
                upsert_event(conn, ev, lat, lon, prov)
                enriched.append({**ev, "lat": lat, "lon": lon})
            # Экспорт JSON из БД (а не из RAM), чтобы точно совпадало с реальным состоянием
            export_events_json_from_db(conn, PATH_JSON)
    else:
        # Без БД — просто геокодим и экспортим JSON
        enriched = []
        for ev in records:
            lat, lon, prov = smart_geocode(ev["location"], overrides, cache)
            if lat is None:
                bad.add(ev["location"]); continue
            enriched.append({**ev, "lat": lat, "lon": lon})
        export_events_json_direct(enriched, PATH_JSON)

    # Сохранить кэш и «плохие» адреса
    save_json(PATH_CACHE, cache)
    if bad:
        with PATH_BAD.open("a", encoding="utf-8") as f:
            for a in sorted(bad):
                f.write(f"[{datetime.date.today().isoformat()}] {a}\n")

if __name__ == "__main__":
    main()
