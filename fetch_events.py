# Python 3.10+
"""
MeowAfisha · fetch_events.py
Каскадный геокодинг: ArcGIS → Yandex → Nominatim
- Кэш: geocode_cache.json (коммитим — экономит лимиты)
- Логи: печать в stdout + (опционально) geocode_log.json при GEOCODE_SAVE_LOG=1
- Рейт-лимиты: min_delay_seconds для каждого провайдера (env)
"""

import os, re, time, json, sys
from pathlib import Path

# ── опциональная загрузка .env при локальном запуске
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except Exception:
    pass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from geopy.geocoders import ArcGIS, Yandex, Nominatim
from geopy.extra.rate_limiter import RateLimiter

# ─────────── НАСТРОЙКИ ───────────
TOKEN        = os.getenv("VK_TOKEN")                       # ⬅️ секрет VK (обязателен)
DOMAIN       = os.getenv("VK_DOMAIN", "meowafisha")        # паблик ВК
MAX_POSTS    = int(os.getenv("VK_MAX_POSTS", "2000"))
BATCH        = 100
WAIT_REQ     = float(os.getenv("VK_WAIT_REQ", "1.1"))      # пауза между wall.get (~1 rps)
YEAR_DEFAULT = os.getenv("YEAR_DEFAULT", "2025")

# Геокодеры: задержки (сек) между запросами
ARCGIS_MIN_DELAY    = float(os.getenv("ARCGIS_MIN_DELAY", "1.0"))
YANDEX_MIN_DELAY    = float(os.getenv("YANDEX_MIN_DELAY", "1.0"))
NOMINATIM_MIN_DELAY = float(os.getenv("NOMINATIM_MIN_DELAY", "1.0"))

# Yandex / Nominatim ключи/параметры
YANDEX_KEY = os.getenv("YANDEX_KEY")  # ⬅️ обязательный для Яндекса
NOMINATIM_USER_AGENT = os.getenv("NOMINATIM_USER_AGENT", "meowafisha-bot")
# если в перспективе будет свой Nominatim: NOMINATIM_URL="nominatim.example.com"
NOMINATIM_URL = os.getenv("NOMINATIM_URL", "").strip()

# Логирование в файл (кроме stdout)
GEOCODE_SAVE_LOG = os.getenv("GEOCODE_SAVE_LOG", "1") == "1"

OUTPUT_JSON  = Path("events.json")           # отсюда читает фронт
CACHE_FILE   = Path("geocode_cache.json")    # коммитим — экономит лимиты
LOG_FILE     = Path("geocode_log.json")      # в .gitignore

assert TOKEN, "VK_TOKEN не задан (секрет репозитория или .env)"

# ─────────── ИНИЦИАЛИЗАЦИЯ ───────────
vk_url = "https://api.vk.ru/method/wall.get"

def init_session() -> requests.Session:
    """Create a requests session with retry logic."""
    sess = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


session = init_session()


# Геокодеры
arcgis = ArcGIS(timeout=10)  # публичный ArcGIS, без ключа
# Яндекс может быть отключён, если нет ключа
yandex = Yandex(api_key=YANDEX_KEY, timeout=10, user_agent="meowafisha-script") if YANDEX_KEY else None

# Nominatim: по умолчанию публичный; если есть свой — передаём domain
if NOMINATIM_URL:
    nominatim = Nominatim(user_agent=NOMINATIM_USER_AGENT, timeout=10, domain=NOMINATIM_URL)
else:
    nominatim = Nominatim(user_agent=NOMINATIM_USER_AGENT, timeout=10)

# Рейт-лимитеры (бережно по 1 req/s на сервис)
arcgis_geocode    = RateLimiter(arcgis.geocode, min_delay_seconds=ARCGIS_MIN_DELAY)
yandex_geocode    = RateLimiter(yandex.geocode, min_delay_seconds=YANDEX_MIN_DELAY) if yandex else None
nominatim_geocode = RateLimiter(nominatim.geocode, min_delay_seconds=NOMINATIM_MIN_DELAY)

# список провайдеров с их функциями геокодирования (в порядке приоритетов)
GEOCODERS = [
    {"name": "ArcGIS", "func": arcgis_geocode},
    {"name": "Yandex", "func": yandex_geocode},
    {"name": "Nominatim", "func": nominatim_geocode},
]

# ─────────── КЭШ ───────────
def load_cache() -> dict:
    """Загрузить кэш геокодинга из файла.

    Если файл повреждён или отсутствует — вернуть пустой словарь,
    чтобы запросы к API выполнялись только при необходимости.
    """
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_cache(cache: dict) -> None:
    """Сохранить текущий кэш геокодинга на диск."""
    CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# Кэш адрес→[lat, lon]
geocache = load_cache()

geolog = {}  # адрес → {'arcgis':..., 'yandex':..., 'nominatim':...}

def vk_wall(offset: int, attempts: int = 3):
    params = dict(domain=DOMAIN, offset=offset, count=BATCH,
                  access_token=TOKEN, v="5.199")
    for attempt in range(1, attempts + 1):
        try:
            r = session.get(vk_url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                raise RuntimeError(f"VK API error: {data['error']}")
            return data["response"]["items"]
        except Exception as e:
            if attempt == attempts:
                raise
            time.sleep(WAIT_REQ)

CITY_WORDS = r"(калининград|гурьевск|светлогорск|янтарный|зеленоградск|пионерский|балтийск|поселок|пос\.|г\.)"

def extract(text: str):
    # дата ДД.ММ
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

def _log(addr: str, provider: str, ok: bool, detail: str = ""):
    print(f"[{provider:9}] {'OK ' if ok else 'N/A'} | {addr} {('→ ' + detail) if detail else ''}")
    if addr not in geolog:
        geolog[addr] = {}
    geolog[addr][provider] = {"ok": ok, "detail": detail}

def geocode_addr(addr: str):
    """Каскадное геокодирование: ArcGIS → Yandex → Nominatim.
       Возвращает [lat, lon] или [None, None]. Все результаты пишем в кэш.
    """
    if addr in geocache:
        return geocache[addr]

    for provider in GEOCODERS:
        name, func = provider["name"], provider["func"]
        if not func:
            detail = "нет ключа" if name == "Yandex" else ""
            _log(addr, name, False, detail)
            continue
        try:
            loc = func(addr)
            if loc:
                res = [loc.latitude, loc.longitude]
                geocache[addr] = res
                _log(addr, name, True, f"{res[0]:.6f},{res[1]:.6f}")
                return res
            _log(addr, name, False)
        except Exception as e:
            _log(addr, name, False, f"err: {e}")

    geocache[addr] = [None, None]
    return geocache[addr]

def main():
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
        OUTPUT_JSON.write_text("[]", encoding="utf-8")
        sys.exit(0)

    df = pd.DataFrame(records).drop_duplicates()

    # ─────────── ГЕОКОДИНГ ───────────
    lats, lons = [], []
    for addr in df["location"]:
        lat, lon = geocode_addr(addr)
        lats.append(lat); lons.append(lon)

    df["lat"] = lats; df["lon"] = lons

    bad = df[df["lat"].isna()]
    bad_cnt = int(bad.shape[0])
    if bad_cnt:
        missed = ", ".join(sorted(set(map(str, bad["location"].tolist()))))
        print(f"⚠️  Не найдены координаты для {bad_cnt} адрес(ов): {missed[:800]}{' …' if len(missed)>800 else ''}")

    # фильтруем точки без координат из отдачи на фронт
    df = df.dropna(subset=["lat", "lon"])

    print(f"С координатами: {len(df)} | без координат: {bad_cnt}")

    # ─────────── СОХРАНЕНИЕ ───────────
    df = df[["title","date","location","lat","lon"]].sort_values("date")
    OUTPUT_JSON.write_text(
        df.to_json(orient="records", force_ascii=False, indent=2),
        encoding="utf-8"
    )

    # кэш сохраняем всегда — экономит лимиты
    save_cache(geocache)

    if GEOCODE_SAVE_LOG:
        try:
            LOG_FILE.write_text(json.dumps(geolog, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"Не удалось сохранить {LOG_FILE}: {e}")

    print("✅  events.json создан/обновлён")
    session.close()


if __name__ == "__main__":
    main()
