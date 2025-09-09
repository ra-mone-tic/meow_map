
// ===== КОНСТАНТЫ/НАСТРОЙКИ =====
// Загружаем список событий без параметра для принудительного обновления,
// чтобы браузер мог кэшировать данные и ускорять повторные загрузки.
const JSON_URL = 'events.json';

// Координаты границ Калининградской области
// [minLng, minLat, maxLng, maxLat]
const REGION_BBOX = [19.30, 54.00, 23.10, 55.60];

// ===== КАРТА =====
const MAP_OPTS = {
  container: 'map',
  // Используем простые растровые тайлы OSM и ограничиваем запросы областью
  style: {
    version: 8,
    sources: {
      'osm-raster': {
        type: 'raster',
        tiles: [
          'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
          'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
          'https://c.tile.openstreetmap.org/{z}/{x}/{y}.png'
        ],
        tileSize: 256,
        bounds: REGION_BBOX,
        attribution: '© OpenStreetMap contributors'
      }
    },
    layers: [
      { id: 'osm', type: 'raster', source: 'osm-raster' }
    ]
  },
  center: [20.45, 54.71],
  zoom: 10,
  antialias: false,
  maxZoom: 17,
  maxBounds: [
    [REGION_BBOX[0], REGION_BBOX[1]],
    [REGION_BBOX[2], REGION_BBOX[3]]
  ],
  renderWorldCopies: false
};

let map;
let isMapLibre = false;

if (maplibregl && maplibregl.supported()) {
  isMapLibre = true;
  map = new maplibregl.Map(MAP_OPTS);

  let styleErrorShown = false;
  map.on('error', e => {
    if (styleErrorShown) return;
    styleErrorShown = true;
    console.error('Map style load error', e.error);
  });

  map.addControl(new maplibregl.NavigationControl(), 'top-right');
  map.addControl(new maplibregl.GeolocateControl({
    positionOptions: { enableHighAccuracy: true },
    showUserLocation: true
  }), 'top-right');

  map.dragRotate.disable();
  map.touchZoomRotate.disableRotation();
} else {
  // Фолбэк на Leaflet при отсутствии WebGL
  const bounds = [[REGION_BBOX[1], REGION_BBOX[0]], [REGION_BBOX[3], REGION_BBOX[2]]];
  map = L.map('map', { maxBounds: bounds }).setView([MAP_OPTS.center[1], MAP_OPTS.center[0]], MAP_OPTS.zoom);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: MAP_OPTS.maxZoom,
    bounds: bounds,
    noWrap: true,
    attribution: '&copy; OpenStreetMap contributors'
  }).addTo(map);
}

// Ускорение отрисовки карты
// helper для контроля частоты вызовов
function debounce(fn, delay) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), delay); };
}

const resizeMap = debounce(() => {
  if (isMapLibre) map.resize(); else map.invalidateSize();
}, 100);

if (isMapLibre) {
  map.on('load', () => { setTimeout(resizeMap, 100); });
} else {
  setTimeout(resizeMap, 100);
}

// ===== МАРКЕРЫ =====
let markers = [];
function clearMarkers() {
  if (isMapLibre) {
    markers.forEach(m => m.remove());}
  }
  