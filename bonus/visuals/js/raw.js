import { fetchJSON } from './api.js';

export async function loadRaw() {

  const limit =
    document.getElementById('rawLimit').value;

  const skip =
    document.getElementById('rawSkip').value;

  const link =
    document.getElementById('rawLink').value;

  const data = await fetchJSON(
    `/raw?limit=${limit}&skip=${skip}` +
    (link ? '&link=' + encodeURIComponent(link) : '')
  );

  const rows = data.map(r => `
    <tr>
      <td>${r.time}</td>
      <td>${r.link}</td>
      <td>${r.name}</td>
      <td>${r.v}</td>
    </tr>
  `).join('');

  document.getElementById('rawTableArea').innerHTML = `
    <table>

      <thead>
        <tr>
          <th>Time</th>
          <th>Link</th>
          <th>Vehicle</th>
          <th>Speed</th>
        </tr>
      </thead>

      <tbody>
        ${rows}
      </tbody>

    </table>
  `;
}