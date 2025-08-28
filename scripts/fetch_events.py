# Python 3.10+
import os, re, time, json
from pathlib import Path

import requests
import pandas as pd
from geopy.geocoders import ArcGIS
from geopy.extra.rate_limiter import RateLimiter

# ─────────── НАСТРОЙКИ ───────────
TOKEN        = os.getenv("VK_TOKEN")                 # ⬅️ добавь секрет в GitHub → Settings → Secrets → Actions
DOMAIN       = os.getenv("VK_DOMAIN", "meowafisha")  # паблик ВК (можно переопределить секретом/варом)
MAX_POSTS    = int(os.getenv("VK_MAX_POSTS", "2000"))
BATCH        = 100
WAIT_REQ     = 1.1                                   # пауза между wall.get (~1 rps)
WAIT_GEO     = 1.0                                   # пауза ArcGIS (≈1000/сутки)
YEAR_DEFAULT = os.getenv("YEAR_DEFAULT", "2025")

OUTPUT_JSON  = Path("events.json")                   # важно: лежит там, откуда его раздаст Pages
CACHE_FILE   = Path("geocode_cache.json")            # коммитим кэш — экономит лимит

assert TOKEN, "VK_TOKEN не задан"

# ─────────── ИНИЦИАЛИЗАЦИЯ ───────────
vk_url = "https://api.vk.com/method/wall.get"
geo = RateLimiter(ArcGIS(timeout=10).geocode, min_delay_seconds=WAIT_GEO)

# Кэш адрес→(lat, lon)
if CACHE_FILE.exists():
    geocache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
else:
    geocache = {}

def vk_wall(offset: int):
    params = dict(domain=DOMAIN, offset=offset, count=BATCH,
                  access_token=TOKEN, v="5.199")
    r = requests.get(vk_url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    # fail-safe: у VK ошибки бывают в "error"
    if "error" in data:
        raise RuntimeError(f"VK API error: {data['error']}")
    return data["response"]["items"]

CITY_WORDS = r"(калининград|гурьевск|светлогорск|янтарный)"

def extract(text: str):
    # дата в формате ДД.ММ
    m_date = re.search(r"\b(\d{2})\.(\d{2})\b", text)
    # локация после 📍
    m_loc  = re.search(r"📍\s*(.+)", text)
    if not (m_date and m_loc):
        return None

    date  = f"{YEAR_DEFAULT}-{m_date.group(2)}-{m_date.group(1)}"
    loc   = m_loc.group(1).split('➡️')[0].strip()
    if not re.search(CITY_WORDS, loc, re.I):
        loc += ", Калининград"

    # заголовок = первая строка без "ДД.ММ |"
    first_line = text.split('\n', 1)[0]
    title = re.sub(r"^\s*\d{2}\.\d{2}\s*\|\s*", "", first_line).strip()

    return dict(title=title, date=date, location=loc)

def geocode(addr: str):
    if addr in geocache:
        return geocache[addr]
    try:
        g = geo(addr)
        if g:
            geocache[addr] = [g.latitude, g.longitude]
        else:
            geocache[addr] = [None, None]
    except Exception:
        geocache[addr] = [None, None]
    return geocache[addr]

# ─────────── СБОР ПОСТОВ ───────────
records, off = [], 0
while off < MAX_POSTS:
    items = vk_wall(off)
    if not items:
        break
    for it in items:
        text = it.get("text") or ""
        evt = extract(text)
        if evt:
            records.append(evt)
    off += BATCH
    time.sleep(WAIT_REQ)

print("Анонсов найдено:", len(records))
if not records:
    # ничего не ломаем: сохраняем пустой массив
    OUTPUT_JSON.write_text("[]", encoding="utf-8")
    raise SystemExit(0)

df = pd.DataFrame(records).drop_duplicates()

# ─────────── ГЕОКОДИНГ ───────────
lats, lons = [], []
for addr in df["location"]:
    lat, lon = geocode(addr)
    lats.append(lat); lons.append(lon)
df["lat"] = lats; df["lon"] = lons

bad_cnt = int(df["lat"].isna().sum())
df = df.dropna(subset=["lat", "lon"])

print(f"С координатами: {len(df)} | без координат: {bad_cnt}")

# ─────────── СОХРАНЕНИЕ ───────────
df = df[["title","date","location","lat","lon"]].sort_values("date")
OUTPUT_JSON.write_text(df.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")

# кэш тоже сохраним — это экономит лимиты
CACHE_FILE.write_text(json.dumps(geocache, ensure_ascii=False, indent=2), encoding="utf-8")

print("✅  events.json создан/обновлён")
