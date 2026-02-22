#!/usr/bin/env python3
import json
import subprocess
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

try:
    from gpiozero import Button
except Exception:
    Button = None

CONFIG_PATH = Path('/home/eamondoherty618/zone_node_config.json')
W1_BASE = Path('/sys/bus/w1/devices')
DEFAULT_PORT = 8090
SAMPLE_INTERVAL_SECONDS = 5
NET_HELPER = '/usr/local/bin/zone-node-net.sh'

stop_event = threading.Event()
cache_lock = threading.Lock()
latest_payload = None


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def run_net_helper(*args, timeout=20):
    cmd = ['sudo', NET_HELPER, *args]
    try:
        cp = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {
            'ok': cp.returncode == 0,
            'code': cp.returncode,
            'stdout': cp.stdout.strip(),
            'stderr': cp.stderr.strip(),
        }
    except Exception as e:
        return {'ok': False, 'code': -1, 'stdout': '', 'stderr': str(e)}


def list_probes():
    if not W1_BASE.exists():
        return []
    return sorted(p.name for p in W1_BASE.glob('28-*') if (p / 'w1_slave').exists())


def probe_choices():
    out = []
    for sid in list_probes():
        t, e = read_temp_f(sid)
        out.append({
            "sensor_id": sid,
            "temp_f": t,
            "error": e,
        })
    return out


def default_config():
    probes = list_probes()
    feed_id = probes[0] if len(probes) > 0 else ''
    ret_id = probes[1] if len(probes) > 1 else ''
    return {
        'configured': False,
        'unit_name': 'New Zone Node',
        'zone_id': '',
        'parent_zone': 'Zone 1',
        'feed_sensor_id': feed_id,
        'return_sensor_id': ret_id,
        'call_gpio': 17,
        'call_active_mode': 'dry_contact_to_gnd',
        'port': DEFAULT_PORT,
        'wifi': {
            'ssid': '',
            'password': '',
            'hotspot_ssid': '',
            'hotspot_password': ''
        },
        'hub': {
            'hub_url': '',
            'account_label': '',
            'enroll_token': ''
        },
        'alerts': {
            'low_temp_enabled': True,
            'low_temp_threshold_f': 35.0
        }
    }


def load_config():
    if not CONFIG_PATH.exists():
        cfg = default_config()
        save_config(cfg)
        return cfg
    try:
        cfg = json.loads(CONFIG_PATH.read_text())
        d = default_config()
        for k, v in cfg.items():
            if k not in {'hub', 'wifi'}:
                d[k] = v
        hub = d.get('hub', {})
        if isinstance(cfg.get('hub'), dict):
            hub.update(cfg['hub'])
        d['hub'] = hub
        wifi = d.get('wifi', {})
        if isinstance(cfg.get('wifi'), dict):
            wifi.update(cfg['wifi'])
        d['wifi'] = wifi
        alerts = d.get('alerts', {})
        if isinstance(cfg.get('alerts'), dict):
            alerts.update(cfg['alerts'])
        d['alerts'] = alerts
        return d
    except Exception:
        cfg = default_config()
        save_config(cfg)
        return cfg


def save_config(cfg):
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2) + '\n')


def read_temp_f(sensor_id):
    if not sensor_id:
        return None, 'unassigned'
    f = W1_BASE / sensor_id / 'w1_slave'
    if not f.exists():
        return None, 'missing'
    lines = f.read_text().strip().splitlines()
    if len(lines) < 2 or not lines[0].endswith('YES'):
        return None, 'crc'
    try:
        c = float(lines[1].split('t=')[-1]) / 1000.0
        return round(c * 9.0 / 5.0 + 32.0, 2), None
    except Exception:
        return None, 'parse'


def build_call_reader(cfg):
    gpio = int(cfg.get('call_gpio', 17) or 17)
    if Button is None:
        return None, 'gpiozero unavailable'
    try:
        return Button(gpio, pull_up=True), None
    except Exception as e:
        return None, str(e)


def compute_payload(cfg, call_reader=None, call_reader_err=None):
    feed_id = cfg.get('feed_sensor_id', '')
    ret_id = cfg.get('return_sensor_id', '')
    feed_f, feed_err = read_temp_f(feed_id)
    ret_f, ret_err = read_temp_f(ret_id)

    heating_call = None
    call_status = '24VAC Signal Unknown'
    if call_reader is not None:
        try:
            heating_call = bool(call_reader.is_pressed)
            call_status = '24VAC Present (Calling)' if heating_call else 'No 24VAC (Idle)'
        except Exception:
            heating_call = None
            call_status = '24VAC Signal Unknown'
    elif call_reader_err:
        call_status = '24VAC Signal Error'

    wifi_ssid = ''
    if isinstance(cfg.get('wifi'), dict):
        wifi_ssid = cfg['wifi'].get('ssid') or ''

    alerts_cfg = cfg.get('alerts', {}) if isinstance(cfg.get('alerts'), dict) else {}
    low_temp_enabled = bool(alerts_cfg.get('low_temp_enabled', True))
    try:
        low_temp_threshold_f = float(alerts_cfg.get('low_temp_threshold_f', 35.0))
    except Exception:
        low_temp_threshold_f = 35.0
    low_temp_hits = []
    if low_temp_enabled:
        if feed_f is not None and float(feed_f) <= low_temp_threshold_f:
            low_temp_hits.append(f"feed ({feed_f:.2f}F)")
        if ret_f is not None and float(ret_f) <= low_temp_threshold_f:
            low_temp_hits.append(f"return ({ret_f:.2f}F)")
    low_temp_alert = len(low_temp_hits) > 0
    low_temp_status = "Low Temperature Detected - Check Equipment (Freeze Risk)" if low_temp_alert else "Normal"
    if low_temp_alert:
        low_temp_detail = (
            f"Low temp detected on {cfg.get('unit_name') or 'zone node'}: "
            + ", ".join(low_temp_hits)
            + f" at or below {low_temp_threshold_f:.1f}F. Check equipment to prevent freezing and damage."
        )
    else:
        low_temp_detail = ""

    return {
        'configured': bool(cfg.get('configured')),
        'unit_name': cfg.get('unit_name') or 'Unnamed Zone Node',
        'zone_id': cfg.get('zone_id') or '',
        'parent_zone': cfg.get('parent_zone') or '',
        'feed_f': feed_f,
        'return_f': ret_f,
        'feed_sensor_id': feed_id,
        'return_sensor_id': ret_id,
        'feed_error': feed_err,
        'return_error': ret_err,
        'heating_call': heating_call,
        'call_status': call_status,
        'call_gpio': cfg.get('call_gpio', 17),
        'call_active_mode': 'dry_contact_to_gnd',
        'probes_detected': list_probes(),
        'wifi_ssid': wifi_ssid,
        'low_temp_enabled': low_temp_enabled,
        'low_temp_threshold_f': low_temp_threshold_f,
        'low_temp_alert': low_temp_alert,
        'low_temp_status': low_temp_status,
        'low_temp_detail': low_temp_detail,
        'hub': cfg.get('hub', {}),
        'alerts': alerts_cfg,
        'updated_utc': now_utc_iso(),
    }


def sample_loop():
    current_gpio = None
    call_reader = None
    call_reader_err = None
    while not stop_event.is_set():
        try:
            cfg = load_config()
            next_gpio = int(cfg.get('call_gpio', 17) or 17)
            if next_gpio != current_gpio or call_reader is None:
                call_reader = None
                current_gpio = next_gpio
                call_reader, call_reader_err = build_call_reader(cfg)
            payload = compute_payload(cfg, call_reader, call_reader_err)
            with cache_lock:
                global latest_payload
                latest_payload = payload
        except Exception:
            pass
        stop_event.wait(SAMPLE_INTERVAL_SECONDS)


def get_payload():
    with cache_lock:
        if isinstance(latest_payload, dict):
            return dict(latest_payload)
    cfg = load_config()
    return compute_payload(cfg)


def html_page():
    return '''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Zone Setup</title>
<style>
:root{--bg:#eef4f7;--panel:#fff;--ink:#10242b;--muted:#5b717a;--line:#d8e2e7;--brand:#114b5f;}
body{margin:0;padding:20px;background:linear-gradient(180deg,#eef4f7,#e7f1ef);font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--ink)}
.wrap{max-width:980px;margin:0 auto}
.hero{background:linear-gradient(130deg,#0f172a,#114b5f 55%,#0f766e);color:#effcff;border-radius:16px;padding:18px 20px;box-shadow:0 10px 30px rgba(0,0,0,.15)}
.hero h1{margin:0;font-size:1.35rem}.hero p{margin:.35rem 0 0;opacity:.95}
.grid{display:grid;grid-template-columns:1.2fr .8fr;gap:14px;margin-top:14px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px;box-shadow:0 6px 18px rgba(10,31,44,.08)}
.card h2{margin:0 0 10px;font-size:1.02rem}
label{display:block;font-size:.88rem;color:var(--muted);font-weight:700;margin:10px 0 4px}
input,select{width:100%;padding:10px;border:1px solid var(--line);border-radius:10px;font-size:15px;background:#fff}
button{margin-top:10px;background:#114b5f;color:#fff;border:0;padding:10px 14px;border-radius:10px;font-weight:700;cursor:pointer}
button.secondary{background:#fff;color:#114b5f;border:1px solid var(--line)}
button.warn{background:#8a2d12}
.row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
pre{background:#f7fbfd;border:1px solid var(--line);padding:10px;border-radius:10px;overflow:auto;font-size:.83rem;max-height:520px}
.badge{display:inline-block;padding:4px 8px;border-radius:999px;background:#edf6f9;border:1px solid #cfe0e8;color:#114b5f;font-weight:700;font-size:.8rem}
small{color:var(--muted)} .pill{display:inline-block;border:1px solid var(--line);padding:5px 8px;border-radius:999px;background:#f7fbfd;margin-right:6px;font-size:.85rem}
#msg{margin-top:10px;font-weight:700;color:#114b5f;white-space:pre-wrap}
.hidden{display:none}
.success-box{margin-top:12px;padding:12px;border:1px solid #bfe3d6;background:#eefbf5;border-radius:12px}
.success-box h3{margin:0 0 6px;font-size:1rem;color:#0c5a48}
.tabrow{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0 8px}
.itab{border:1px solid var(--line);background:#fff;color:#114b5f;padding:6px 10px;border-radius:999px;font-weight:700;cursor:pointer}
.itab.active{background:#114b5f;color:#fff;border-color:#114b5f}
.itpanel{display:none;background:#f7fbfd;border:1px solid var(--line);border-radius:10px;padding:10px;font-size:.92rem}
.itpanel.active{display:block}
@media (max-width:760px){.grid,.row{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <h1>Zone Setup</h1>
    <p>Phone-friendly setup for feed/return probe mapping, dry-contact call input, Wi-Fi assignment, and hub linking.</p>
  </div>
  <div class="grid">
    <div class="card">
      <h2>Device Setup</h2>
      <div class="badge" id="cfgState">Loading...</div>
      <div style="margin-top:8px">
        <span class="pill" id="wifiStatus">Wi-Fi: --</span>
        <span class="pill" id="apStatus">Setup Hotspot: --</span>
      </div>
      <form id="setupForm">
        <label>Unit Name</label>
        <input name="unit_name" placeholder="AV Room Coil" required>

        <div class="row">
          <div>
            <label>Zone ID</label>
            <input name="zone_id" placeholder="zone1-av-room">
          </div>
          <div>
            <label>Parent Zone</label>
            <select name="parent_zone">
              <option>Zone 1</option>
              <option>Zone 2</option>
              <option>Zone 3</option>
              <option>Other</option>
            </select>
          </div>
        </div>

        <div class="row">
          <div>
            <label>Feed Probe</label>
            <select name="feed_sensor_id" id="feedSel"></select>
          </div>
          <div>
            <label>Return Probe</label>
            <select name="return_sensor_id" id="returnSel"></select>
          </div>
        </div>
        <div>
          <button type="button" class="secondary" id="swapProbesBtn">Swap Feed / Return</button>
        </div>

        <div class="row">
          <div>
            <label>Heating Call GPIO</label>
            <input type="number" name="call_gpio" min="2" max="27" value="17">
          </div>
          <div>
            <label>Call Input Mode</label>
            <input value="Dry contact to GND" disabled>
          </div>
        </div>

        <h2 style="margin-top:14px">Alerts</h2>
        <div class="row">
          <div>
            <label>Low Temp Alert Threshold (F)</label>
            <input type="number" step="0.5" name="low_temp_threshold_f" value="35">
          </div>
          <div>
            <label>Low Temp Alerts</label>
            <select name="low_temp_enabled">
              <option value="true">Enabled</option>
              <option value="false">Disabled</option>
            </select>
          </div>
        </div>

        <h2 style="margin-top:14px">Building Wi-Fi</h2>
        <div class="row">
          <div>
            <label>Wi-Fi SSID</label>
            <select name="wifi_ssid" id="wifiSsidSel"></select>
            <div style="margin-top:6px"><button type="button" class="secondary" id="scanWifiBtn">Scan Wi-Fi Networks</button></div>
            <label style="margin-top:8px">Or Enter Wi-Fi SSID Manually</label>
            <input name="wifi_ssid_manual" placeholder="Type Wi-Fi name if scan is unavailable">
          </div>
          <div>
            <label>Wi-Fi Password</label>
            <input name="wifi_password" type="password" placeholder="Wi-Fi password">
          </div>
        </div>

        <label>Hub URL (optional for now)</label>
        <input name="hub_url" placeholder="https://165-boiler.tail58e171.ts.net">

        <div class="row">
          <div>
            <label>Account Label</label>
            <input name="account_label" placeholder="165 Water Street">
          </div>
          <div>
            <label>Enroll Token (optional)</label>
            <input name="enroll_token" placeholder="future hub enrollment token">
          </div>
        </div>

        <button type="submit">Save Configuration (and Join Wi-Fi)</button>
        <button type="button" class="secondary" id="refreshBtn">Refresh Detected Probes</button>
      </form>
      <div id="msg"></div>
      <div id="setupSuccess" class="success-box hidden">
        <h3>Setup Successful</h3>
        <div id="setupSuccessText">This zone node is connected and reporting.</div>
        <div class="tabrow">
          <button type="button" class="secondary" id="addAnotherBtn">Add Another Device</button>
          <button type="button" id="showInstructionsBtn">Setup Instructions</button>
        </div>
        <div id="instructionsWrap" class="hidden">
          <div class="tabrow">
            <button type="button" class="itab active" data-itab="wiring">Wiring</button>
            <button type="button" class="itab" data-itab="feedreturn">Feed vs Return</button>
            <button type="button" class="itab" data-itab="probe">Probe Placement</button>
            <button type="button" class="itab" data-itab="mounting">Securing Probe</button>
            <button type="button" class="itab" data-itab="troubleshoot">Troubleshooting</button>
          </div>
          <div class="itpanel active" data-ipanel="wiring">
            <strong>Wiring (placeholder)</strong><br>
            Add wiring diagrams and terminal references for dry-contact relay, GPIO input, and probe lead routing.
          </div>
          <div class="itpanel" data-ipanel="feedreturn">
            <strong>Feed vs Return (placeholder)</strong><br>
            Add field guidance for identifying flow direction, reading temperatures, and marking pipes before assigning probes.
          </div>
          <div class="itpanel" data-ipanel="probe">
            <strong>Probe Placement (placeholder)</strong><br>
            Add instructions for best sensing location, insulation, and avoiding false ambient readings.
          </div>
          <div class="itpanel" data-ipanel="mounting">
            <strong>Securing Probe (placeholder)</strong><br>
            Add acceptable fastening methods, insulation wrap recommendations, and strain relief details.
          </div>
          <div class="itpanel" data-ipanel="troubleshoot">
            <strong>Troubleshooting (placeholder)</strong><br>
            Add checks for no probe reading, swapped feed/return, no 24VAC call, and Wi-Fi/Tailscale connectivity.
          </div>
        </div>
      </div>
      <small>Setup hotspot stays on during onboarding. Connect in iPhone Wi-Fi settings to <code>HeatingHub-Setup-xxxx</code>, then open <code>http://10.42.0.1:8090</code>. After Wi-Fi credentials are saved and the node joins your network, setup hotspot can be shut off automatically in the next deploy.</small>
    </div>
    <div class="card">
      <h2>Live Device Readings</h2>
      <pre id="live">Loading...</pre>
    </div>
  </div>
</div>
<script>
async function j(url, opts){ const r = await fetch(url, opts); const txt=await r.text(); let data={}; try{data=txt?JSON.parse(txt):{}}catch(e){throw new Error(txt||('HTTP '+r.status))}; if(!r.ok) throw new Error(data.error || txt || ('HTTP '+r.status)); return data; }
function fillSelect(sel, probes, value){
  sel.innerHTML='';
  const blank=document.createElement('option');
  blank.value='';
  blank.textContent=(probes && probes.length) ? '-- Select Probe --' : '-- No Probes Detected --';
  sel.appendChild(blank);
  for(const p of (probes||[])){
    const sid = (typeof p === 'string') ? p : p.sensor_id;
    const temp = (p && p.temp_f !== undefined && p.temp_f !== null) ? `${Number(p.temp_f).toFixed(1)}°F` : '--';
    const err = (p && p.error) ? ` [${p.error}]` : '';
    const o=document.createElement('option');
    o.value=sid;
    o.textContent = (typeof p === 'string') ? sid : `${sid} (${temp})${err}`;
    sel.appendChild(o);
  }
  if(value!==undefined&&value!==null) sel.value=value;
}
function fillWifiSelect(sel, networks, current){
  sel.innerHTML='';
  const blank=document.createElement('option');
  blank.value='';
  blank.textContent='-- Select Wi-Fi Network --';
  sel.appendChild(blank);
  for(const n of (networks||[])){
    const o=document.createElement('option');
    o.value=n.ssid;
    const sig = (n.signal===null || n.signal===undefined) ? '--' : n.signal;
    o.textContent = `${n.ssid} (${sig}%)`;
    sel.appendChild(o);
  }
  if(current) sel.value = current;
}
function setMsg(t){ document.getElementById('msg').textContent = t || ''; }
function showSetupSuccess(show, text){
  const box = document.getElementById('setupSuccess');
  const txt = document.getElementById('setupSuccessText');
  if(!box) return;
  if(text) txt.textContent = text;
  box.classList.toggle('hidden', !show);
}
function showInstructions(show){
  const el = document.getElementById('instructionsWrap');
  if(!el) return;
  el.classList.toggle('hidden', !show);
}
function activateInstructionTab(name){
  document.querySelectorAll('[data-itab]').forEach(b => b.classList.toggle('active', b.dataset.itab === name));
  document.querySelectorAll('[data-ipanel]').forEach(p => p.classList.toggle('active', p.dataset.ipanel === name));
}
async function refreshWifiStatus(){
  try {
    const w = await j('/api/wifi/status?_=' + Date.now());
    document.getElementById('wifiStatus').textContent = 'Wi-Fi: ' + (w.connected_ssid || 'not connected');
    document.getElementById('apStatus').textContent = 'Setup Hotspot: ' + (w.ap_active ? (w.ap_ssid || 'active') : 'off');
  } catch (e) {
    document.getElementById('wifiStatus').textContent = 'Wi-Fi: status unavailable';
    document.getElementById('apStatus').textContent = 'Setup Hotspot: status unavailable';
  }
}
async function refreshWifiScan(){
  try {
    const s = await j('/api/wifi/scan?_=' + Date.now());
    const f = document.getElementById('setupForm');
    const current = (f.wifi_ssid_manual.value || f.wifi_ssid.value || '').trim();
    fillWifiSelect(document.getElementById('wifiSsidSel'), s.networks || [], current);
  } catch (e) {
    setMsg('Wi-Fi scan unavailable (common while setup hotspot is active). You can type the Wi-Fi name manually.');
  }
}
async function refreshAll(){
  const s = await j('/api/setup-state?_=' + Date.now());
  document.getElementById('cfgState').textContent = s.configured ? 'Configured' : 'Setup Required';
  const f = document.getElementById('setupForm');
  f.unit_name.value = s.config.unit_name || '';
  f.zone_id.value = s.config.zone_id || '';
  f.parent_zone.value = s.config.parent_zone || 'Zone 1';
  f.call_gpio.value = s.config.call_gpio || 17;
  f.low_temp_threshold_f.value = (s.config.alerts && s.config.alerts.low_temp_threshold_f != null) ? s.config.alerts.low_temp_threshold_f : 35;
  f.low_temp_enabled.value = (s.config.alerts && s.config.alerts.low_temp_enabled === false) ? 'false' : 'true';
  fillWifiSelect(document.getElementById('wifiSsidSel'), [], (s.config.wifi && s.config.wifi.ssid) || '');
  f.wifi_ssid_manual.value = (s.config.wifi && s.config.wifi.ssid) || '';
  f.wifi_password.value = '';
  f.hub_url.value = (s.config.hub && s.config.hub.hub_url) || '';
  f.account_label.value = (s.config.hub && s.config.hub.account_label) || '';
  f.enroll_token.value = (s.config.hub && s.config.hub.enroll_token) || '';
  fillSelect(document.getElementById('feedSel'), s.probe_choices || s.probes_detected, s.config.feed_sensor_id || '');
  fillSelect(document.getElementById('returnSel'), s.probe_choices || s.probes_detected, s.config.return_sensor_id || '');
  if (s.configured) {
    showSetupSuccess(true, 'This zone node is configured. You can review instructions or set up another device.');
  }
}
async function refreshLive(){
  try { const data = await j('/api/zone?_=' + Date.now()); document.getElementById('live').textContent = JSON.stringify(data, null, 2); }
  catch (e) { document.getElementById('live').textContent = 'Error: ' + e.message; }
}
document.getElementById('setupForm').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const f = ev.target;
  const payload = {
    unit_name: f.unit_name.value.trim(),
    zone_id: f.zone_id.value.trim(),
    parent_zone: f.parent_zone.value.trim(),
    feed_sensor_id: f.feed_sensor_id.value,
    return_sensor_id: f.return_sensor_id.value,
    call_gpio: Number(f.call_gpio.value || 17),
    alerts: { low_temp_threshold_f: Number(f.low_temp_threshold_f.value || 35), low_temp_enabled: (f.low_temp_enabled.value === 'true') },
    wifi: { ssid: (f.wifi_ssid_manual.value.trim() || f.wifi_ssid.value.trim()), password: f.wifi_password.value },
    hub: {
      hub_url: f.hub_url.value.trim(),
      account_label: f.account_label.value.trim(),
      enroll_token: f.enroll_token.value.trim(),
    }
  };
  if (payload.feed_sensor_id && payload.feed_sensor_id === payload.return_sensor_id) { setMsg('Feed and Return probes must be different.'); return; }
  setMsg('Saving...');
  try {
    const res = await j('/api/setup', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
    setMsg((res.message || 'Saved') + (res.wifi_apply && res.wifi_apply.ok ? '\nWi-Fi apply started.' : (res.wifi_apply ? ('\nWi-Fi apply issue: ' + (res.wifi_apply.stderr || res.wifi_apply.stdout || 'unknown')) : '')));
    if (res.configured) {
      const wifiOk = !res.wifi_apply || !!res.wifi_apply.ok;
      showSetupSuccess(true, wifiOk ? 'Setup Successful. This zone node is now configured and reporting.' : 'Configuration saved. Verify Wi-Fi connection, then continue.');
    }
    await refreshAll(); await refreshWifiStatus(); await refreshLive();
  } catch (e) { setMsg('Save failed: ' + e.message); }
});
document.getElementById('refreshBtn').addEventListener('click', async ()=>{ await refreshAll(); await refreshWifiStatus(); await refreshLive(); });
document.getElementById('scanWifiBtn').addEventListener('click', async ()=>{ setMsg('Scanning Wi-Fi networks...'); await refreshWifiScan(); setMsg('Wi-Fi scan complete.'); });
document.getElementById('wifiSsidSel').addEventListener('change', ()=>{
  const f = document.getElementById('setupForm');
  if (f.wifi_ssid.value) f.wifi_ssid_manual.value = f.wifi_ssid.value;
});
document.getElementById('swapProbesBtn').addEventListener('click', ()=>{
  const f = document.getElementById('feedSel');
  const r = document.getElementById('returnSel');
  const a = f.value;
  f.value = r.value;
  r.value = a;
  setMsg('Feed and Return probe selections swapped. Save to apply.');
});
document.getElementById('addAnotherBtn').addEventListener('click', ()=>{
  showInstructions(false);
  showSetupSuccess(false);
  setMsg('Ready to configure another device. Update Unit Name, Zone ID, and probe assignment.');
  window.scrollTo({top: 0, behavior: 'smooth'});
});
document.getElementById('showInstructionsBtn').addEventListener('click', ()=>{
  showInstructions(true);
  activateInstructionTab('wiring');
  const hubUrl = document.getElementById('setupForm').hub_url.value.trim();
  setMsg(hubUrl ? `Setup instructions shown below. Hub link saved: ${hubUrl}` : 'Setup instructions shown below.');
});
document.querySelectorAll('[data-itab]').forEach(btn => {
  btn.addEventListener('click', ()=> activateInstructionTab(btn.dataset.itab));
});
refreshAll(); refreshWifiStatus(); refreshLive(); refreshWifiScan();
setInterval(refreshLive, 5000); setInterval(refreshWifiStatus, 10000);
</script>
</body>
</html>'''


class H(BaseHTTPRequestHandler):
    def _send_json(self, obj, code=200):
        b = json.dumps(obj).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _send_html(self, text, code=200):
        b = text.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        route = urlparse(self.path).path
        if route in ('/', '/setup'):
            return self._send_html(html_page())
        if route == '/api/setup-state':
            cfg = load_config()
            safe_cfg = json.loads(json.dumps(cfg))
            if isinstance(safe_cfg.get('wifi'), dict) and safe_cfg['wifi'].get('password'):
                safe_cfg['wifi']['password'] = ''
            return self._send_json({'configured': bool(cfg.get('configured')), 'config': safe_cfg, 'probes_detected': list_probes(), 'probe_choices': probe_choices(), 'updated_utc': now_utc_iso()})
        if route == '/api/zone':
            return self._send_json(get_payload())
        if route == '/api/probes':
            return self._send_json({'probes': list_probes(), 'updated_utc': now_utc_iso()})
        if route == '/api/wifi/status':
            status = run_net_helper('status')
            payload = {'updated_utc': now_utc_iso(), **status}
            if status.get('ok') and status.get('stdout'):
                try:
                    payload.update(json.loads(status['stdout']))
                except Exception:
                    pass
            return self._send_json(payload)
        if route == '/api/wifi/scan':
            scan = run_net_helper('scan')
            payload = {'updated_utc': now_utc_iso(), **scan}
            if scan.get('ok') and scan.get('stdout'):
                try:
                    payload.update(json.loads(scan['stdout']))
                except Exception:
                    pass
            return self._send_json(payload)
        return self._send_json({'error': 'not found'}, 404)

    def do_POST(self):
        route = urlparse(self.path).path
        if route in ('/api/wifi/start-hotspot', '/api/wifi/stop-hotspot'):
            action = 'ap-on' if route.endswith('start-hotspot') else 'ap-off'
            res = run_net_helper(action)
            msg = 'Setup hotspot started' if action == 'ap-on' else 'Setup hotspot stopped'
            if res.get('ok'):
                return self._send_json({'ok': True, 'message': msg, 'result': res})
            return self._send_json({'error': res.get('stderr') or res.get('stdout') or 'wifi helper failed', 'result': res}, 500)

        if route != '/api/setup':
            return self._send_json({'error': 'not found'}, 404)
        try:
            n = int(self.headers.get('Content-Length', '0'))
        except Exception:
            n = 0
        raw = self.rfile.read(n) if n > 0 else b'{}'
        try:
            body = json.loads(raw.decode('utf-8') or '{}')
        except Exception:
            return self._send_json({'error': 'invalid json'}, 400)

        cfg = load_config()
        cfg['unit_name'] = str(body.get('unit_name') or cfg.get('unit_name') or '').strip() or 'Unnamed Zone Node'
        cfg['zone_id'] = str(body.get('zone_id') or '').strip()
        cfg['parent_zone'] = str(body.get('parent_zone') or cfg.get('parent_zone') or 'Zone 1').strip()
        cfg['feed_sensor_id'] = str(body.get('feed_sensor_id') or '').strip()
        cfg['return_sensor_id'] = str(body.get('return_sensor_id') or '').strip()
        try:
            cfg['call_gpio'] = int(body.get('call_gpio', cfg.get('call_gpio', 17)) or 17)
        except Exception:
            cfg['call_gpio'] = 17

        hub = cfg.get('hub', {}) if isinstance(cfg.get('hub'), dict) else {}
        body_hub = body.get('hub', {}) if isinstance(body.get('hub'), dict) else {}
        hub['hub_url'] = str(body_hub.get('hub_url') or hub.get('hub_url') or '').strip()
        hub['account_label'] = str(body_hub.get('account_label') or hub.get('account_label') or '').strip()
        hub['enroll_token'] = str(body_hub.get('enroll_token') or hub.get('enroll_token') or '').strip()
        cfg['hub'] = hub

        alerts = cfg.get('alerts', {}) if isinstance(cfg.get('alerts'), dict) else {}
        body_alerts = body.get('alerts', {}) if isinstance(body.get('alerts'), dict) else {}
        if 'low_temp_enabled' in body_alerts:
            alerts['low_temp_enabled'] = bool(body_alerts.get('low_temp_enabled'))
        if 'low_temp_threshold_f' in body_alerts:
            try:
                alerts['low_temp_threshold_f'] = float(body_alerts.get('low_temp_threshold_f'))
            except Exception:
                pass
        cfg['alerts'] = alerts

        wifi = cfg.get('wifi', {}) if isinstance(cfg.get('wifi'), dict) else {}
        body_wifi = body.get('wifi', {}) if isinstance(body.get('wifi'), dict) else {}
        if 'ssid' in body_wifi:
            wifi['ssid'] = str(body_wifi.get('ssid') or '').strip()
        if 'password' in body_wifi and str(body_wifi.get('password') or ''):
            wifi['password'] = str(body_wifi.get('password') or '')
        cfg['wifi'] = wifi

        if cfg['feed_sensor_id'] and cfg['feed_sensor_id'] == cfg['return_sensor_id']:
            return self._send_json({'error': 'feed and return probes must differ'}, 400)

        cfg['configured'] = bool(cfg.get('unit_name')) and bool(cfg.get('feed_sensor_id')) and bool(cfg.get('return_sensor_id'))
        save_config(cfg)

        wifi_apply = None
        ssid = (cfg.get('wifi') or {}).get('ssid', '').strip()
        psk = (cfg.get('wifi') or {}).get('password', '')
        if ssid and psk:
            # Try applying Wi-Fi after config save. This may interrupt current connection if run over Wi-Fi.
            wifi_apply = run_net_helper('connect', ssid, psk, timeout=35)
            if wifi_apply.get('ok'):
                run_net_helper('ap-off', timeout=10)

        return self._send_json({'ok': True, 'configured': cfg['configured'], 'message': 'Configuration saved', 'config': cfg, 'wifi_apply': wifi_apply})


if __name__ == '__main__':
    load_config()
    sampler = threading.Thread(target=sample_loop, daemon=True)
    sampler.start()
    port = int(load_config().get('port', DEFAULT_PORT) or DEFAULT_PORT)
    ThreadingHTTPServer(('0.0.0.0', port), H).serve_forever()
