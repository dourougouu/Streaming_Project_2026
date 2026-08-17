import { fetchJSON } from './api.js';

export async function loadSummary() {

  const data = await fetchJSON('/summary');

  document.getElementById('summaryGrid').innerHTML =
    data.map(r => `
      <div class="summary-card">

        <h3>${r.link}</h3>

        <p>Avg: ${r.overall_avg_speed.toFixed(1)}</p>

        <p>Max: ${r.overall_max_speed.toFixed(1)}</p>

        <p>Vehicles: ${r.total_vehicle_obs}</p>

      </div>
    `).join('');
}