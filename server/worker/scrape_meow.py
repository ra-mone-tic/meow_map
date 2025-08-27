#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scrape_meow.py
================

Этот скрипт — упрощённая и обратно‑совместимая версия исходного
`scrape_meow.py`. Он собирает события из VK, извлекает дату и локацию,
геокодирует адрес (с учётом ручных override и кеша) и формирует
`www/events.json`, который читает фронт. База данных здесь не используется.

Переменные окружения:
    VK_TOKEN   – токен доступа к VK (обязателен).
    VK_DOMAIN  – домен сообщества (по умолчанию «meowafisha»).
    YEAR_DEFAULT – год по умолчанию для дат вида «DD.MM» (берётся текущий).

Файлы данных:
    data/manual_overrides.json – необязательная карта «сырой адрес → координаты».
    data/geocode_cache.json    – кеш нормализованных адресов.
    logs/bad_addresses.log     – лог неподдавшихся геокоду адресов.

Вывод:
    www/events.json – массив объектов {title, date, location, lat, lon} для фронта.
"""

import os
import re
import time
import json
import datetime
import requests
from hashlib import sha1
from pathlib import Path

from geopy.geocoders import ArcGIS, Nominatim
from geopy.extra.rate_limiter import RateLimiter

# ──────────────── Конфигурация ────────────────
VK_TOKEN  = os.getenv("VK_TOKEN")
VK_DOMAIN = os.getenv("VK_DOMAIN", "meowafisha")

# Год по умолчанию
YEAR_DEFAULT = str(datetime.date.today().year)

# Определяем корень репозитория и пути
REPO_ROOT = Path(__file__).resolve().parents[0]
while not (REPO_ROOT / "www").exists() and REPO_ROOT != REPO_ROOT.parent:
    REPO_ROOT = REPO_ROOT.parent

PATH_WWW   = REPO_ROOT / "www"
PATH_JSON  = PATH_WWW / "events.json"
PATH_OVR   = REPO_ROOT / "data" / "manual_overrides.json"
PATH_CACHE = REPO_ROOT / "data" / "geocode_cache.json"
PATH_BAD   = REPO_ROOT / "logs" / "bad_addresses.log"

# Убеждаемся, что каталоги существуют
PATH_WWW.mkdir(parents=True, exist_ok=True)
PATH_OVR.parent.mkdir(parents=True, exist_ok=True)
PATH_CACHE.parent.mkdir(parents=True, exist_ok=True)
PATH_BAD.parent.mkdir(parents=True, exist_ok=True)

# Инициализируем геокодеры и лимитер скорости
arc = ArcGIS(timeout=10)
osm = Nominatim(user_agent="meowafisha", timeout=10)
geo_arc = RateLimiter(arc.geocode, min_delay_seconds=1.0)
geo_osm = RateLimiter(osm.geocode, min_delay_seconds=1.0)

# ──────────────── Утилиты ────────────────
def norm_addr(addr: str) -> str:
    """Нормализует адрес для кеша: убирает скобки, пунктуацию, приводит к нижнему регистру и добавляет «Калининград» при отсутствии города."""
    s = addr.lower().strip()
    s = re.sub(r"\s*[\(\[\{].*?[\)\]\}]\s*", " ", s)
    s = re.sub(r"[.,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not re.search(r"(калининград|пионерский|светлогорск|гурьевск|янтарный|балтийск)", s):
        s += " калининград"
    return s

def load_json(p: Path, default):
    """Читает JSON из файла, возвращает default при ошибке или отсутствии."""
    if p.exists():
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception:
            return default
    return default

def save_json(p: Path, obj):
    """Атомарно пишет объект obj как JSON в файл p."""
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(p)

def smart_geocode(raw_addr: str, overrides: dict, cache: dict):
    """
    Возвращает (lat, lon, provider) для raw_addr.
    Проверяет: 1) ручные override; 2) кеш нормализованного адреса; 3) ArcGIS и OSM.
    """
    # ручной override
    if raw_addr in overrides:
        d = overrides[raw_addr]
        cache[norm_addr(raw_addr)] = {**d, "provider": "manual", "ts": datetime.date.today().isoformat()}
        return d.get("lat"), d.get("lon"), "manual"

    # кеш
    key = norm_addr(raw_addr)
    if key in cache:
        d = cache[key]
        return d.get("lat"), d.get("lon"), d.get("provider", "cache")

    # геокодеры
    for prov, fn in (("arcgis", geo_arc), ("osm", geo_osm)):
        try:
            g = fn(raw_addr)
            if g:
                lat, lon = float(g.latitude), float(g.longitude)
                cache[key] = {"lat": lat, "lon": lon, "provider": prov, "ts": datetime.date.today().isoformat()}
                return lat, lon, prov
        except Exception:
            pass
    return None, None, None

def vk_wall(offset: int, batch: int = 100):
    """Получает пачку постов со стены VK."""
    url = "https://api.vk.com/method/wall.get"
    params = dict(domain=VK_DOMAIN, offset=offset, count=batch, access_token=VK_TOKEN, v="5.199")
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    j = r.json()
    if "response" not in j:
        return []
    return j["response"].get("items", [])

def extract(text: str):
    """Извлекает данные события из текста поста. Ожидает дату «DD.MM» и локацию после 📍."""
    m_date = re.search(r"\b(\d{2})\.(\d{2})\b", text)
    m_loc  = re.search(r"📍\s*(.+)", text)
    if not (m_date and m_loc):
        return None
    date  = f"{YEAR_DEFAULT}-{m_date.group(2)}-{m_date.group(1)}"
    loc   = m_loc.group(1).split('➡️')[0].strip()
    title = re.sub(r"^\d{2}\.\d{2}\s*\|\s*", "", text.split('\n')[0]).strip()
    return dict(title=title, date=date, location=loc)

def event_id(source: str, post_id: str, title: str, date: str) -> str:
    """Генерирует детерминированный SHA‑1 для события (не используется в JSON, но оставлен для совместимости)."""
    raw = f"{source}|{post_id}|{title}|{date}".encode("utf-8")
    return sha1(raw).hexdigest()

def export_events_json_direct(records_with_coords, out_path: Path = PATH_JSON):
    """Сохраняет список событий в JSON для фронта."""
    data = [
        {
            "id": r.get("id"),
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

# ──────────────── Основной поток ────────────────
def main():
    overrides = load_json(PATH_OVR, {})
    cache     = load_json(PATH_CACHE, {})

    # сбор постов
    records = []
    offset, BATCH = 0, 100
    while offset < 2000:
        items = vk_wall(offset, BATCH)
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
        offset += BATCH
        time.sleep(1.1)  # пауза во избежание 429

    # геокодирование
    enriched = []
    bad = set()
    for ev in records:
        lat, lon, prov = smart_geocode(ev["location"], overrides, cache)
        if lat is None or lon is None:
            bad.add(ev["location"])
            continue
        enriched.append({**ev, "lat": lat, "lon": lon})

    # запись JSON
    export_events_json_direct(enriched, PATH_JSON)

    # обновление кеша
    save_json(PATH_CACHE, cache)

    # логирование плохих адресов
    if bad:
        with PATH_BAD.open("a", encoding="utf-8") as f:
            for a in sorted(bad):
                f.write(f"[{datetime.date.today().isoformat()}] {a}\n")

    print(f"Processed {len(records)} posts; exported {len(enriched)} events to {PATH_JSON}")

if __name__ == "__main__":
    main()
