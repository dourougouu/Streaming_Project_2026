export function api(path){
    return document .getElementById('apiBase')
                    .value.replace(/\/+$/, '') + path;
}

export async function fetchJSON(path){
    const res = await fetch(api(path));
        if(!res.ok){
            throw new Error(`HTTP ${res.status}`);
        }
    return res.json();
}