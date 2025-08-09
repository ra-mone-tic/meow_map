# Как активировать изменения (короткая шпаргалка)

1) Скопируйте эти файлы в корень вашего репозитория:
   - `scrape_meow.py`
   - `index.html`
   - `requirements.txt`
   - `data/venues.json`
   - `data/geocode_overrides.csv`
   - `data/geocode_cache.json` (может быть пустым)
   - `schemas/events_v2.schema.json`
   - `scripts/validate_events.py`
   - `.github/workflows/update.yml`

2) Локальный запуск:
   ```bash
   python -m venv .venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   set VK_TOKEN=...   # Windows; macOS/Linux: export VK_TOKEN=...
   python scrape_meow.py
   python scripts/validate_events.py
   open index.html    # или просто открыть в браузере
   ```

3) GitHub Actions:
   - В репозитории GitHub откройте **Settings → Secrets and variables → Actions → New repository secret**.
   - Создайте секрет `VK_TOKEN` и вставьте токен VK.
   - После пуша workflow сам запустится каждые 6 часов (или вручную через **Actions → Run workflow**).

4) Где править:
   - Справочник площадок: `data/venues.json` (постоянные точки, алиасы).
   - Разовые адресные фиксы: `data/geocode_overrides.csv` (одна строка — один адрес).
   - Кеш геокодера: `data/geocode_cache.json` (создаётся и обновляется автоматически).
   - Схема данных: `schemas/events_v2.schema.json`.
   - Валидатор: `scripts/validate_events.py`.

5) Поведение времени:
   - Если в посте нет времени — событие отмечено `time_unknown=true` и **не** попадает в «фильтр по часу» (в будущем).
   - В карточке пишется «Время не указано».

6) Дедупликация (агрессивная):
   - Склейка, если совпадают 2 из 3: дата±1 день, расстояние ≤200 м, похожесть названий ≥0.85.
   - Берётся лучшая гео-точка (manual > ArcGIS > Nominatim), объединяются картинки/теги.

Удачи!
