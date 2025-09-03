// ===== КОНСТАНТЫ/НАСТРОЙКИ =====
// Загружаем список событий без параметра для принудительного обновления,
// чтобы браузер мог кэшировать данные и ускорять повторные загрузки.
const JSON_URL = 'events.json';
const MAPTILER_KEY = (typeof process !== 'undefined' && process.env && process.env.MAPTILER_KEY) || globalThis.MAPTILER_KEY || '';

if (!MAPTILER_KEY) {
  console.warn('MAPTILER_KEY is not defined. Set it in config.js or as environment variable.');
}

// ===== КАРТА =====
const MAP_OPTS = {
  container: 'map',
  // Используем более лёгкий стиль карты для ускорения загрузки
  style: `https://api.maptiler.com/maps/streets/style.json?key=${MAPTILER_KEY}`,
  center: [20.45, 54.71],
  zoom: 10,
  antialias: false,
  maxZoom: 17
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
    alert('Не удалось загрузить стиль карты. Проверьте ключ MapTiler и подключение к интернету.');
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
  map = L.map('map').setView([MAP_OPTS.center[1], MAP_OPTS.center[0]], MAP_OPTS.zoom);
  L.tileLayer(`https://api.maptiler.com/maps/streets/256/{z}/{x}/{y}.png?key=${MAPTILER_KEY}`, {
    maxZoom: MAP_OPTS.maxZoom,
    attribution: '&copy; MapTiler & OpenStreetMap contributors'
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
burger.onclick=()=>sidebar.classList.toggle('open');