import importlib, sys, pathlib
import pytest

@pytest.fixture
def fe(monkeypatch):
    monkeypatch.setenv("VK_TOKEN", "test")
    sys.modules.pop("fetch_events", None)
    monkeypatch.syspath_prepend(str(pathlib.Path(__file__).resolve().parents[1]))
    import fetch_events
    return fetch_events


def test_provider_order(monkeypatch, fe):
    calls = []

    class Loc:
        latitude = 1.0
        longitude = 2.0

    def make(name, ret):
        def _f(addr):
            calls.append(name)
            return ret
        return _f

    geocoders = [
        {"name": "ArcGIS", "func": make("ArcGIS", None)},
        {"name": "Yandex", "func": make("Yandex", None)},
        {"name": "Nominatim", "func": make("Nominatim", Loc())},
    ]
    monkeypatch.setattr(fe, "GEOCODERS", geocoders)
    fe.geocache.clear()

    res = fe.geocode_addr("адрес")
    assert res == [1.0, 2.0]
    assert calls == ["ArcGIS", "Yandex", "Nominatim"]


def test_cache(monkeypatch, fe):
    class Loc:
        latitude = 3
        longitude = 4

    calls = {"cnt": 0}

    def arcgis(addr):
        calls["cnt"] += 1
        return Loc()

    geocoders = [{"name": "ArcGIS", "func": arcgis}]
    monkeypatch.setattr(fe, "GEOCODERS", geocoders)
    fe.geocache.clear()

    assert fe.geocode_addr("addr") == [3, 4]
    assert fe.geocode_addr("addr") == [3, 4]
    assert calls["cnt"] == 1