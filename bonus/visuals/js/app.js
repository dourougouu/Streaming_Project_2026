import { setupTabs } from './ui.js';
import { checkHealth } from './health.js';
import { loadStats } from './stats.js';
import { loadWindowed } from './windowed.js';
import { loadRaw } from './raw.js';
import { loadSummary } from './summary.js';

setupTabs();

document
  .getElementById('healthRefreshButton')
  .addEventListener('click', checkHealth);

document
  .getElementById('statsRefreshButton')
  .addEventListener('click', loadStats);

document
  .getElementById('windowedRefreshButton')
  .addEventListener('click', loadWindowed);

document
  .getElementById('rawRefreshButton')        // ← ήταν 'healthRefreshButton' λάθος!
  .addEventListener('click', loadRaw);

document
  .getElementById('summaryRefreshButton')
  .addEventListener('click', loadSummary);

checkHealth();
loadStats();