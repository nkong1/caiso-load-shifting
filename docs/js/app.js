// app.js - expects docs/lmp_price_scores.json (array of {time, records})
(async function(){
  const map = L.map('map').setView([37.5, -119.5], 6);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO'
  }).addTo(map);

  // load JSON (cache-bust to avoid stale cached file)
  async function loadData(){
    const resp = await fetch('lmp_price_scores.json?ts=' + Date.now());
    return resp.json();
  }

  const data = await loadData(); // array
  if(!Array.isArray(data) || data.length === 0){
    alert('No data found (docs/lmp_price_scores.json).');
    return;
  }

  // UI elements
  const slider = document.getElementById('time-slider');
  const timeLabel = document.getElementById('timeLabel');
  const playBtn = document.getElementById('play');

  slider.max = data.length - 1;
  let idx = 0;
  let playing = false;
  let timer = null;

  // marker layer
  const markerLayer = L.layerGroup().addTo(map);

  function getColor(score){
    if (score === null || score === undefined) return '#999';
    if (score >= 0.9) return '#d73027';
    if (score >= 0.5) return '#fee08b';
    return '#91cf60';
  }

  function renderIndex(i){
    idx = i;
    const step = data[i];
    timeLabel.textContent = step.time;
    markerLayer.clearLayers();

    for(const rec of step.records){
      if(rec.lat == null || rec.lon == null) continue;
      const col = getColor(rec.score);
      const marker = L.circleMarker([rec.lat, rec.lon], {
        radius: 6,
        fillColor: col,
        color:'#333',
        weight:0.5,
        fillOpacity:0.9
      });
      marker.bindPopup(`<b>${rec.node_id}</b><br>Price: ${rec.price ?? 'n/a'}<br>Score: ${rec.score ?? 'n/a'}`);
      markerLayer.addLayer(marker);
    }
  }

  // slider change
  slider.addEventListener('input', e => {
    renderIndex(+e.target.value);
  });

  // play/pause
  playBtn.addEventListener('click', () => {
    playing = !playing;
    playBtn.textContent = playing ? '⏸' : '▶';
    if(playing){
      timer = setInterval(() => {
        idx = (idx + 1) % data.length;
        slider.value = idx;
        renderIndex(idx);
      }, 1000);
    } else {
      clearInterval(timer);
    }
  });

  // initial render
  renderIndex(0);
})();
