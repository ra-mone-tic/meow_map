# scrape_vk.py  ▸  Python 3.10+
# Сбор анонсов VK + «умный» геокодер (ArcGIS → Nominatim → ручной справочник)

import re, time, math, requests, pandas as pd
from geopy.geocoders import ArcGIS, Nominatim
from geopy.extra.rate_limiter import RateLimiter

# ───────────────────────── НАСТРОЙКИ ─────────────────────────
TOKEN        = "vk1.a.az68uTKtqRoSRW7ud6qQBHBYEDX-IoB3n-raywQhtxBEfNEsIncINjLHPtxM6GA0lLaf_FjOY9H89xHnTq3c65AtgZzZ4BNxzgC8ThX56PY52S8yf3cZxbS4Utojhi83OiCUofSgsuBRXJldpIHtzuO70BBV8wR1DM9bdSMUhknZ7kb1e6ib9XBG2vnJSjXjhQ76en7c7VDcIMv-PqeCfQ"  # ← свой
DOMAIN       = "meowrecords"
MAX_POSTS    = 2000
BATCH        = 100
WAIT_REQ     = 1.1                # пауза между wall.get
YEAR_DEFAULT = "2025"

# ручные фиксы
MANUAL_FIX = {
    "галицкого 18, калининград": (54.71428885362268, 20.49684969892652),
    "баранова 43а, калининград": (54.720663512417474, 20.507042646865408),
    "сёрф-станция рассвет, пионерский пляж": (54.95460237461884, 20.218043935955457)
}

# центры городов для выбора «лепшего»
CENTER = {
    "калининград": (54.7100, 20.5100),
    "пионерский":  (54.9560, 20.2270)
}

# геокодеры с RateLimiter
gis = RateLimiter(ArcGIS(timeout=10).geocode,                min_delay_seconds=1.0)
osm = RateLimiter(Nominatim(user_agent="meowafisha").geocode, min_delay_seconds=1.0)

vk_api = "https://api.vk.com/method/wall.get"

# ───────────────────────── VK парсинг ─────────────────────────
def vk_wall(offset: int):
    prm = dict(domain=DOMAIN, offset=offset, count=BATCH,
               access_token=TOKEN, v="5.199")
    return requests.get(vk_api, params=prm, timeout=15).json()["response"]["items"]

def extract(text: str):
    m_date = re.search(r"\b(\d{2})\.(\d{2})\b", text)
    m_loc  = re.search(r"📍\s*(.+)", text)
    if not (m_date and m_loc):
        return None
    date  = f"{YEAR_DEFAULT}-{m_date.group(2)}-{m_date.group(1)}"
    loc   = m_loc.group(1).split('➡️')[0].strip()
    if not re.search(r"(калининград|пионер)", loc, re.I):
        loc += ", Калининград"
    title = re.sub(r"^\d{2}\.\d{2}\s*\|\s*", "", text.split('\n')[0]).strip()
    return dict(title=title, date=date, location=loc)

records, off = [], 0
while off < MAX_POSTS:
    items = vk_wall(off)
    if not items:
        break
    for it in items:
        evt = extract(it["text"])
        if evt:
            records.append(evt)
    off += BATCH
    time.sleep(WAIT_REQ)

print("Анонсов найдено:", len(records))
df = pd.DataFrame(records).drop_duplicates()

# ─────────────────────── Геокодер «умный» ─────────────────────
def haversine(a, b):
    lat1, lon1 = a
    lat2, lon2 = b
    R = 6_371_000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    return 2 * R * math.asin(math.sqrt(
        math.sin(dlat / 2) ** 2 +
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
        math.sin(dlon / 2) ** 2))

def pick_best(addr, cands):
    city = "пионерский" if "пионер" in addr else "калининград"
    center = CENTER[city]
    return min(cands, key=lambda p: haversine(center, p))

def normalize(addr: str) -> str:
    addr = addr.lower().strip()
    addr = re.sub(r"\(.*?\)", "", addr)
    if "пионер" in addr and "пионерский" not in addr:
        addr += ", Пионерский"
    if not re.search(r"(калининград|пионерский)", addr):
        addr += ", Калининград"
    return addr

def smart_geocode(addr: str):
    norm = normalize(addr)
    if norm in MANUAL_FIX:
        return MANUAL_FIX[norm]
    cands = []
    try:
        g = gis(norm)
        if g:
            cands.append((g.latitude, g.longitude))
    except Exception:
        pass
    try:
        n = osm(norm)
        if n:
            cands.append((n.latitude, n.longitude))
    except Exception:
        pass
    if not cands:
        return (None, None)
    return pick_best(norm, cands)

# ───────────────────────── Применяем ─────────────────────────
df[["lat", "lon"]] = df["location"].apply(lambda a: pd.Series(smart_geocode(a)))
bad_cnt = df["lat"].isna().sum()
df = df.dropna(subset=["lat", "lon"])

print(f"С координатами: {len(df)} | без координат: {bad_cnt}")

df[["title", "date", "location", "lat", "lon"]].to_json(
    "events.json", orient="records", force_ascii=False, indent=2)

print("✅  events.json создан")
