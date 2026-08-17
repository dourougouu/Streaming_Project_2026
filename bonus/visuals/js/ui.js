export function setupTabs(){
    const buttons = document.querySelectorAll('nav button');

    buttons.forEach(btn => {
        btn.addEventListener('click', () => {
            
            document .querySelectorAll('nav button')
                     .forEach(b => b.classList.remove('active'));
            
            document .querySelectorAll('.panel')
                     .forEach(b => b.classList.remove('active'));

            btn.classList.add('active');

            document .getElementById('panel-' + btn.dataset.panel)
                     .classList.add('active');
        });
    });
}

export function log(msg){
    const t = document.getElementById('logTerminal');
    const div = document.createElement('div');

    div.innerHTML = `[${new Date().toLocaleTimeString()}] ${msg}`;
    t.appendChild(div);
    t.scrollTop = t.scrollHeight;
}