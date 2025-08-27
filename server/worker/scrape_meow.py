# Python 3.10+
import re, time, requests, pandas as pd
from geopy.geocoders import ArcGIS
from geopy.extra.rate_limiter import RateLimiter

# ─────────────────────── НАСТРОЙКИ ───────────────────────
TOKEN        = os.getenv("VK_TOKEN")
DOMAIN       = "meowafisha"       # паблик ВК
MAX_POSTS    = 2000                # сколько всего тянуть (offset’ами)
BATCH        = 100                 # максимум за 1 вызов API
WAIT_REQ     = 1.1                 # пауза между вызовами wall.get  (1 rps)
WAIT_GEO     = 1.0                 # пауза ArcGIS                 (≈1000/сутки)
YEAR_DEFAULT = "2025"              # дописываем к dd.mm

geo = RateLimiter(ArcGIS(timeout=10).geocode, min_delay_seconds=WAIT_GEO)
vk  = "https://api.vk.com/method/wall.get"

def vk_wall(offset: int):
    params = dict(domain=DOMAIN, offset=offset, count=BATCH,
                  access_token=TOKEN, v="5.199")
    return requests.get(vk, params=params, timeout=15).json()["response"]["items"]

def extract(text: str):
    m_date = re.search(r"\b(\d{2})\.(\d{2})\b", text)
    m_loc  = re.search(r"📍\s*(.+)", text)
    if not (m_date and m_loc):
        return None
    date  = f"{YEAR_DEFAULT}-{m_date.group(2)}-{m_date.group(1)}"
    loc   = m_loc.group(1).split('➡️')[0].strip()
    if not re.search(r"(калининград|гурьевск|светлогорск|янтарный)", loc, re.I):
        loc += ", Калининград"
    title = re.sub(r"^\d{2}\.\d{2}\s*\|\s*", "", text.split('\n')[0]).strip()
    return dict(title=title, date=date, location=loc)

# ────────────────────── СБОР ПОСТОВ ──────────────────────
records, off = [], 0
while off < MAX_POSTS:
    items = vk_wall(off)
    if not items: break
    for it in items:
        evt = extract(it["text"])
        if evt: records.append(evt)
    off += BATCH
    time.sleep(WAIT_REQ)

print("Анонсов найдено:", len(records))
df = pd.DataFrame(records).drop_duplicates()

# ────────────────────── ГЕОКОДИНГ ────────────────────────
def to_latlon(addr: str):
    try:
        g = geo(addr)
        return (g.latitude, g.longitude) if g else (None, None)
    except: return (None, None)

df[["lat", "lon"]] = df["location"].apply(lambda a: pd.Series(to_latlon(a)))
bad_cnt = df["lat"].isna().sum()
df = df.dropna(subset=["lat", "lon"])

print(f"С координатами: {len(df)} | без координат: {bad_cnt}")

# ────────────────────── СОХРАНЯЕМ ───────────────────────
df[["title","date","location","lat","lon"]].to_json(
    "events.json", orient="records", force_ascii=False, indent=2)
print("✅  events.json создан")