# server/worker/vk_ingest.py
# Задача: забрать посты VK, вытащить дату/локацию, получить координаты,
#         сохранить в БД (без дублей), логировать «плохие» адреса.

import os, re, time, json, datetime, requests
from pathlib import Path
from geopy.geocoders import ArcGIS, Nominatim
from geopy.extra.rate_limiter import RateLimiter
import psycopg
from hashlib import sha1

DB_URL = os.getenv("DB_URL", "postgresql://meow:meow@db:5432/meow")
VK_TOKEN = os.getenv("VK_TOKEN")           # положите в .env
VK_DOMAIN = os.getenv("VK_DOMAIN","meowafisha")

PATH_OVR   = Path("/app/data/manual_overrides.json")
PATH_CACHE = Path("/app/data/geocode_cache.json")
PATH_BAD   = Path("/app/logs/bad_addresses.log")
PATH_BAD.parent.mkdir(parents=True, exist_ok=True)

# Год по умолчанию — текущий
YEAR_DEFAULT = str(datetime.date.today().year)

# Геокодеры + ограничение частоты, чтобы не ловить баны
arc = ArcGIS(timeout=10)
osm = Nominatim(user_agent="meowafisha", timeout=10)
geo_arc = RateLimiter(arc.geocode, min_delay_seconds=1.0)
geo_osm = RateLimiter(osm.geocode, min_delay_seconds=1.0)

# Полезные функции

def norm_addr(addr: str) -> str:
    s = addr.lower().strip()
    s = re.sub(r"\s*[\(\[\{].*?[\)\]\}]\s*", " ", s)
    s = re.sub(r"[.,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Подставим город, если не указан
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
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(p)


def smart_geocode(raw_addr: str, overrides, cache):
    # 1) ручные координаты
    if raw_addr in overrides:
        d = overrides[raw_addr]
        cache[norm_addr(raw_addr)] = {**d, "provider":"manual", "ts": datetime.date.today().isoformat()}
        return d["lat"], d["lon"], "manual"
    # 2) кэш
    key = norm_addr(raw_addr)
    if key in cache:
        d = cache[key]
        return d["lat"], d["lon"], d.get("provider","cache")
    # 3) ArcGIS -> OSM
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
    return r.json()["response"]["items"]


def extract(text: str):
    m_date = re.search(r"\b(\d{2})\.(\d{2})\b", text)  # DD.MM
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


def upsert_event(conn, ev, lat, lon, provider):
    # 1) площадка (upsert по address_norm)
    address_norm = norm_addr(ev["location"])
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO places(name, address_norm, city, lat, lon, manual_override)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
        """, (ev["location"], address_norm, None, lat, lon, provider=="manual"))
        row = cur.fetchone()
        if row:
            place_id = row[0]
        else:
            # найти существующий по address_norm
            cur.execute("SELECT id FROM places WHERE address_norm=%s", (address_norm,))
            place_id = cur.fetchone()[0]
        # 2) событие upsert
        cur.execute("""
            INSERT INTO events(id, title, starts_at, place_id, source, source_post_id)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO NOTHING
        """, (ev["id"], ev["title"], ev["date"], place_id, "vk", ev["post_id"]))
        conn.commit()


def main():
    overrides = load_json(PATH_OVR, {})
    cache     = load_json(PATH_CACHE, {})

    records, off, BATCH = [], 0, 100
    while off < 2000:
        items = vk_wall(off, BATCH)
        if not items:
            break
        for it in items:
            ev = extract(it.get("text", ""))
            if not ev:
                continue
            ev["post_id"] = str(it["id"])
            ev["id"] = event_id("vk", ev["post_id"], ev["title"], ev["date"])  # детерминированный ключ
            records.append(ev)
        off += BATCH
        time.sleep(1.1)

    bad = set()
    with psycopg.connect(DB_URL) as conn:
        for ev in records:
            lat, lon, prov = smart_geocode(ev["location"], overrides, cache)
            if lat is None:
                bad.add(ev["location"])
                continue
            upsert_event(conn, ev, lat, lon, prov)

    # сохранить кэш и плохие адреса
    save_json(PATH_CACHE, cache)
    if bad:
        with PATH_BAD.open("a", encoding="utf-8") as f:
            for a in sorted(bad):
                f.write(f"[{datetime.date.today().isoformat()}] {a}\n")

if __name__ == "__main__":
    main()