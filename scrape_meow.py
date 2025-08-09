import os, json, time, math, csv, re, hashlib, html
from datetime import datetime, timedelta, timezone
from dateutil import tz
import requests
from rapidfuzz import fuzz
from unidecode import unidecode

# === Config ===
VK_DOMAIN = os.environ.get("VK_DOMAIN", "meow_records")  # домен сообщества без https://vk.com/
VK_TOKEN = os.environ.get("VK_TOKEN", "")
VK_API_VERSION = "5.199"
MAX_POSTS = int(os.environ.get("MAX_POSTS", "400"))
REQUESTS_PER_CALL = 100
SLEEP_BETWEEN_CALLS = 0.34  # чтобы не спамить

TZ = tz.gettz("Europe/Kaliningrad")
SCHEMA_VERSION = "2.0.0"

ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.join(ROOT, "data")
VENUES_PATH = os.path.join(DATA_DIR, "venues.json")
OVERRIDES_CSV = os.path.join(DATA_DIR, "geocode_overrides.csv")
CACHE_PATH = os.path.join(DATA_DIR, "geocode_cache.json")
EVENTS_OUT = os.path.join(ROOT, "events.json")

# === Helpers ===
def now_iso():
    return datetime.now(tz=TZ).isoformat()

def slug(s: str) -> str:
    s = unidecode(s or "").lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:80]

def hash8(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R*c

# === Load data ===
def load_venues():
    if os.path.exists(VENUES_PATH):
        with open(VENUES_PATH, "r", encoding="utf-8") as f: return json.load(f)
    return []

def load_overrides():
    d = {}
    if os.path.exists(OVERRIDES_CSV):
        with open(OVERRIDES_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d[row["raw_address"].strip()] = (float(row["lat"]), float(row["lon"]), "override")
    return d

def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "r", encoding="utf-8") as f: return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

# === VK fetch ===
def fetch_vk_posts(domain: str, token: str, max_posts=400) -> list:
    items = []
    offset = 0
    while offset < max_posts:
        count = min(REQUESTS_PER_CALL, max_posts - offset)
        params = {
            "access_token": token,
            "v": VK_API_VERSION,
            "domain": domain,
            "count": count,
            "offset": offset
        }
        r = requests.get("https://api.vk.com/method/wall.get", params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise RuntimeError(f"VK error: {data['error']}")
        batch = data["response"]["items"]
        if not batch:
            break
        items.extend(batch)
        offset += count
        time.sleep(SLEEP_BETWEEN_CALLS)
    return items

# === Parsing post content ===
MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12
}

def extract_date_time(text: str):
    text = text or ""
    # 1) DD.MM[.YYYY]
    m = re.search(r"(\b\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?", text)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), m.group(3)
        year = int(y) if y else datetime.now(TZ).year
        t = re.search(r"(\b[01]?\d|2[0-3])[:.](\d{2})", text)
        if t:
            hh, mm = int(t.group(1)), int(t.group(2))
            dt = datetime(year, mo, d, hh, mm, tzinfo=TZ)
            return dt, False
        else:
            dt = datetime(year, mo, d, 0, 0, tzinfo=TZ)
            return dt, True
    # 2) "15 августа" etc.
    m2 = re.search(r"\b(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\b", text, re.I)
    if m2:
        d = int(m2.group(1)); mo = MONTHS_RU[m2.group(2).lower()]; year = datetime.now(TZ).year
        t = re.search(r"(\b[01]?\d|2[0-3])[:.](\d{2})", text)
        if t:
            hh, mm = int(t.group(1)), int(t.group(2))
            dt = datetime(year, mo, d, hh, mm, tzinfo=TZ)
            return dt, False
        else:
            dt = datetime(year, mo, d, 0, 0, tzinfo=TZ)
            return dt, True
    return None, True

VENUE_HINTS = ["ул.", "улица", "просп.", "проспект", "пл.", "площадь", "пер.", "переулок", "д.", "дом", "к.", "корп.", "корпус", "пар", "бар", "клуб", "пляж", "станция"]

def extract_venue_and_address(text: str):
    text = text or ""
    # ищем строку с адресными подсказками
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for ln in lines:
        low = ln.lower()
        if any(h in low for h in VENUE_HINTS):
            # эвристика: до тире — venue, после — адрес; иначе вся строка — адрес
            if " — " in ln or " - " in ln:
                parts = re.split(r"\s[—-]\s", ln, maxsplit=1)
                venue_name = parts[0].strip()
                addr = parts[1].strip() if len(parts) > 1 else parts[0].strip()
            else:
                venue_name, addr = "", ln
            return venue_name, addr
    # fallback: первая строка как название места, без уверенности
    return "", ""

def extract_title(text: str):
    text = text or ""
    # заголовок = первая непустая строка, укоротить
    line = next((l.strip() for l in text.splitlines() if l.strip()), "")
    return line[:140]

def extract_images(attachments):
    urls = []
    if not attachments:
        return urls
    for a in attachments:
        if a.get("type") == "photo":
            sizes = a["photo"].get("sizes", [])
            best = max(sizes, key=lambda s: s.get("width",0)*s.get("height",0)) if sizes else None
            if best and best.get("url"):
                urls.append(best["url"])
    return urls

def post_permalink(owner_id, post_id):
    # owner_id отрицательный для групп
    domain = VK_DOMAIN
    return f"https://vk.com/{domain}?w=wall{owner_id}_{post_id}"

# === Geocoding ===
from geopy.geocoders import ArcGIS, Nominatim
from geopy.extra.rate_limiter import RateLimiter

arc = ArcGIS(timeout=10)
nom = Nominatim(user_agent="meowafisha_bot", timeout=10)
arc_rl = RateLimiter(arc.geocode, min_delay_seconds=0.8, swallow_exceptions=True)
nom_rl = RateLimiter(nom.geocode, min_delay_seconds=1.1, swallow_exceptions=True)

def norm_address(addr: str) -> str:
    return re.sub(r"\s+", " ", (addr or "").strip())

def geocode_chain(addr: str, venues, overrides, cache):
    # 1) venues alias exact
    for v in venues:
        names = [v.get("name","")] + v.get("aliases", [])
        if any(addr and name and name.lower() in addr.lower() for name in names):
            return v["lat"], v["lon"], "manual", "manual", v["venue_id"], v.get("address", addr)

    # 2) overrides exact match
    if addr in overrides:
        lat, lon, tag = overrides[addr]
        return lat, lon, "exact", tag, None, addr

    # 3) cache
    nkey = norm_address(addr)
    if nkey in cache:
        c = cache[nkey]
        return c["lat"], c["lon"], c.get("geo_accuracy","approx"), "cache", None, addr

    # 4) arcgis
    loc = arc_rl(addr + ", Калининград")
    if not loc:
        loc = arc_rl(addr)
    if loc:
        lat, lon = float(loc.latitude), float(loc.longitude)
        cache[nkey] = {"lat": lat, "lon": lon, "geocoder": "arcgis", "geo_accuracy": "approx", "updated_at": now_iso()}
        return lat, lon, "approx", "arcgis", None, addr

    # 5) nominatim
    loc = nom_rl(addr + ", Калининград")
    if not loc:
        loc = nom_rl(addr)
    if loc:
        lat, lon = float(loc.latitude), float(loc.longitude)
        cache[nkey] = {"lat": lat, "lon": lon, "geocoder": "nominatim", "geo_accuracy": "fallback", "updated_at": now_iso()}
        return lat, lon, "fallback", "nominatim", None, addr

    return None, None, "fallback", "n/a", None, addr

# === Dedup ===
def same_event(a, b):
    # date within 1 day
    da = datetime.fromisoformat(a["start"])
    db = datetime.fromisoformat(b["start"])
    date_close = abs((da.date() - db.date()).days) <= 1

    # geo within 200m
    try:
        geo_close = haversine_m(a["lat"], a["lon"], b["lat"], b["lon"]) <= 200
    except Exception:
        geo_close = False

    # title similarity
    t1 = (a["title"] or "").lower()
    t2 = (b["title"] or "").lower()
    sim = fuzz.token_sort_ratio(t1, t2) / 100.0
    title_close = sim >= 0.85

    # Need 2 of 3
    conds = [date_close, geo_close, title_close]
    return sum(1 for c in conds if c) >= 2

def merge_events(events):
    # choose best by geocoder priority
    prio = {"manual": 3, "arcgis": 2, "nominatim": 1, "cache": 1, "override": 3}
    res = events[0].copy()
    for e in events[1:]:
        # geo
        if prio.get(e["geocoder"],0) > prio.get(res["geocoder"],0):
            res["lat"], res["lon"] = e["lat"], e["lon"]
            res["geocoder"], res["geo_accuracy"] = e["geocoder"], e["geo_accuracy"]
            res["address"] = e.get("address") or res.get("address")
            res["venue_id"] = e.get("venue_id") or res.get("venue_id")
            res["venue_name"] = e.get("venue_name") or res.get("venue_name")
        # description
        if len((e.get("description") or "")) > len((res.get("description") or "")):
            res["description"] = e["description"]
        # images/tags
        imgs = set(res.get("images") or [])
        imgs.update(e.get("images") or [])
        res["images"] = list(imgs)
        tags = set(res.get("tags") or [])
        tags.update(e.get("tags") or [])
        res["tags"] = list(tags)
    return res

def deduplicate(events):
    used = [False]*len(events)
    out = []
    for i, ev in enumerate(events):
        if used[i]: continue
        bucket = [ev]
        used[i] = True
        for j in range(i+1, len(events)):
            if used[j]: continue
            if same_event(ev, events[j]):
                bucket.append(events[j]); used[j] = True
        out.append(merge_events(bucket))
    return out

# === Main pipeline ===
def main():
    if not VK_TOKEN:
        raise SystemExit("VK_TOKEN is required")

    venues = load_venues()
    overrides = load_overrides()
    cache = load_cache()

    posts = fetch_vk_posts(VK_DOMAIN, VK_TOKEN, MAX_POSTS)
    events = []

    for p in posts:
        text = p.get("text","")
        dt, time_unknown = extract_date_time(text)
        if not dt:
            continue  # skip posts без даты

        title = extract_title(text)
        venue_name, raw_addr = extract_venue_and_address(text)
        images = extract_images(p.get("attachments"))
        start_iso = dt.isoformat()

        lat = lon = None
        geocoder = geo_accuracy = None
        venue_id = None
        address = raw_addr

        if raw_addr:
            lat, lon, geo_accuracy, geocoder, vid, norm_addr = geocode_chain(raw_addr, venues, overrides, cache)
            address = norm_addr or raw_addr
            if vid: venue_id = vid

        if lat is None or lon is None:
            # skip events без координат (пока что)
            continue

        date_key = dt.date().isoformat()
        eid = f"{date_key}_{slug(venue_name or 'venue')}_{slug(title)}_{hash8(str(p.get('id')))}"

        ev = {
            "schema_version": SCHEMA_VERSION,
            "id": eid,
            "title": title,
            "description": text,
            "start": start_iso,
            "end": None,
            "timezone": "Europe/Kaliningrad",
            "time_unknown": time_unknown,
            "status": "scheduled",
            "venue_id": venue_id,
            "venue_name": venue_name or None,
            "address": address or None,
            "lat": float(lat),
            "lon": float(lon),
            "geo_accuracy": geo_accuracy,
            "geocoder": geocoder,
            "source": "vk",
            "source_url": post_permalink(p.get("owner_id"), p.get("id")),
            "tags": [],
            "price": None,
            "age": None,
            "images": images,
            "updated_at": now_iso()
        }
        events.append(ev)

    # Aggressive dedup
    events = deduplicate(events)

    # Sort by start
    events.sort(key=lambda e: e["start"])

    # Save
    with open(EVENTS_OUT, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

    save_cache(cache)
    print(f"Saved {len(events)} events -> {EVENTS_OUT}")

if __name__ == "__main__":
    main()
