import { fetchJSON } from './api.js';

export async function loadStats(){
    const data = await fetchJSON('/stats');

    renderStats(data);
    updateKPIs(data);
}

function updateKPIs(data){

  document.getElementById('kpi-links').textContent = data.length;
  const avg = data.reduce((s, r) => s + (r.vspeed || 0), 0) / data.length;

  document.getElementById('kpi-speed').textContent = avg.toFixed(1);
  const total = data.reduce((s, r) => s + (r.vcount || 0), 0);

  document.getElementById('kpi-vehicles').textContent = total;
}


function renderStats(data){

  if (!data.length){
    document.getElementById('statsTableArea').innerHTML =
      '<div class="empty">No data</div>';
    return;
  }

  const rows = data.map(r => `
    <tr>
      <td>${r.link}</td>
      <td>${r.vcount}</td>
      <td>${r.vspeed.toFixed(2)}</td>
      <td>${r.max_speed.toFixed(2)}</td>
      <td>${r.min_speed.toFixed(2)}</td>
    </tr>
  `).join('');

  document.getElementById('statsTableArea').innerHTML = `
    <table>

      <thead>
        <tr>
          <th>Link</th>
          <th>Vehicles</th>
          <th>Avg Speed</th>
          <th>Max</th>
          <th>Min</th>
        </tr>
      </thead>

      <tbody>
        ${rows}
      </tbody>

    </table>
  `;
}