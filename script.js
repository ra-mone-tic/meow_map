
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
    markers.forEach(m => m.remove());
  } else {
    markers.forEach(m => map.removeLayer(m));
  }
  markers = [];
}
function addMarker(ev) {
  if (isMapLibre) {
    const pop = new maplibregl.Popup({ offset: 25 })
      .setHTML(`<b>${ev.title}</b><br>${ev.location}<br>${ev.date}`);
    const m = new maplibregl.Marker().setLngLat([ev.lon, ev.lat]).setPopup(pop).addTo(map);
    markers.push(m);
  } else {
    const m = L.marker([ev.lat, ev.lon]).addTo(map)
      .bindPopup(`<b>${ev.title}</b><br>${ev.location}<br>${ev.date}`);
    markers.push(m);
  }
}

// ===== ДАННЫЕ И РЕНДЕР =====
fetch(JSON_URL).then(r=>r.json()).then(events=>{
  events.sort((a,b)=>a.date.localeCompare(b.date));

  const input=document.getElementById('event-date');
  input.min=events[0].date; input.max=events[events.length-1].date;

  const today=new Date().toISOString().slice(0,10);
  const first=events.find(e=>e.date>=today)?today:events[0].date;
  input.value=first;

  function render(dateStr){
    clearMarkers();
    const todays=events.filter(e=>e.date===dateStr);
    todays.forEach(addMarker);
    // document.getElementById('count').textContent=todays.length;
    if(todays.length){
      if(isMapLibre) map.flyTo({center:[todays[0].lon,todays[0].lat],zoom:12});
      else map.setView([todays[0].lat,todays[0].lon],12);
    }
  }

  const upcoming=events.filter(e=>new Date(e.date)>=new Date(today)).slice(0,100);
  const upDiv=document.getElementById('upcoming');
  if(!upcoming.length){
    upDiv.textContent='События не найдены';
  }else{
    upcoming.forEach(e=>{
      const d=document.createElement('div');
      d.className='item';
      d.innerHTML=`<strong>${e.title}</strong><br>${e.location}<br><i>${e.date}</i>`;
      d.onclick=()=>{
        render(e.date);
        setTimeout(()=>{
          const m = markers.find(mk => {
            if(isMapLibre){
              const p = mk.getLngLat();
              return Math.abs(p.lat - e.lat) < 1e-5 && Math.abs(p.lng - e.lon) < 1e-5;
            } else {
              const p = mk.getLatLng();
              return Math.abs(p.lat - e.lat) < 1e-5 && Math.abs(p.lng - e.lon) < 1e-5;
            }
          });
          if (m) {
            if(isMapLibre){
              map.flyTo({center:[e.lon,e.lat],zoom:14});
              m.togglePopup();
            } else {
              map.setView([e.lat,e.lon],14);
              m.openPopup();
            }
          }
          document.getElementById('sidebar').classList.remove('open');
        },100);
      };
      upDiv.appendChild(d);
    });
  }

  render(first);
  input.onchange=ev=>{
    const d=new Date(ev.target.value).toISOString().slice(0,10);
    render(d);
  };
}).catch(err=>{
  console.error('Ошибка загрузки данных', err);
  clearMarkers();
  const upDiv=document.getElementById('upcoming');
  upDiv.innerHTML='';
  upDiv.textContent='Ошибка загрузки событий';
});

// ===== UI: Бургер / Закрыть =====
const sidebar=document.getElementById('sidebar');
const burger=document.getElementById('burger');
const closeBtn=document.getElementById('closeSidebar');
burger.onclick=()=>sidebar.classList.toggle('open');
closeBtn.onclick=()=>sidebar.classList.remove('open');

document.addEventListener('click', e => {
  if (sidebar.classList.contains('open') && !sidebar.contains(e.target) && e.target !== burger) {
    sidebar.classList.remove('open');
  }
});
