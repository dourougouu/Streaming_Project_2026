import { fetchJSON } from './api.js';

export async function loadWindowed() {

  const link =
    document.getElementById('windowedLink').value;

  const data = await fetchJSON(
    '/windowed?limit=100' +
    (link ? '&link=' + encodeURIComponent(link) : '')
  );

  const rows = data.map(r => `
    <tr>
      <td>${r.link}</td>
      <td>${r.vcount}</td>
      <td>${r.vspeed.toFixed(2)}</td>
    </tr>
  `).join('');

  document.getElementById('windowedTableArea').innerHTML = `
    <table>

      <thead>
        <tr>
          <th>Link</th>
          <th>Count</th>
          <th>Avg Speed</th>
        </tr>
      </thead>

      <tbody>
        ${rows}
      </tbody>

    </table>
  `;
}