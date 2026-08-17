import {fetchJSON} from './api.js';
import {log} from './ui.js';

export async function checkHealth(){
    try{
        const d = await fetchJSON('/health');

        document.getElementById('statusDot').className = 'status-dot okay';
        document.getElementById('statusText').textContent = 'ONLINE';

        renderHealth(d);
        log('Health OKAY :)');
    }
    catch (e){

        document.getElementById('statusDot').className = 'status-dot error';
        document.getElementById('statusText').textContent = 'OFFLINE';

        log('Health FAILED :(');
    }
}

function renderHealth(d){
    document.getElementById('healthGrid').innerHTML = `
    <div class="summary-card">
        <h2><b> MongoDB </b></h3>
        <p>${d.mongodb}</p>
    </div>

    <div class="summary-card">
        <h3>Stats Docs</h3>
        <p>${d.collections.stats}</p>
    </div>

     `;
}