#!/usr/bin/env python3
import json
import subprocess
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen, Request

try:
    from gpiozero import Button
except Exception:
    Button = None

CONFIG_PATH = Path('/home/eamondoherty618/zone_node_config.json')
W1_BASE = Path('/sys/bus/w1/devices')
DEFAULT_PORT = 8090
SAMPLE_INTERVAL_SECONDS = 5
NET_HELPER = '/usr/local/bin/zone-node-net.sh'
KNOWN_PUBLIC_HUB_URL = 'https://165-boiler.tail58e171.ts.net/'

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


def hub_discovery_candidates():
    cfg = load_config()
    cfg_url = ''
    try:
        cfg_url = ((cfg.get('hub') or {}).get('hub_url') or '').strip()
    except Exception:
        cfg_url = ''
    candidates = [
        cfg_url,
        KNOWN_PUBLIC_HUB_URL,
        'http://165-boiler.local:8080',
        'http://165water-boiler.local:8080',
        'http://165-boiler:8080',
        'http://165water-boiler:8080',
        'http://100.100.143.19:8080',
        'http://192.168.4.181:8080',
    ]
    seen = set()
    out = []
    for c in candidates:
        c = (c or '').strip().rstrip('/')
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def probe_hub(base_url, timeout=0.8):
    base_url = (base_url or '').strip().rstrip('/')
    if not base_url:
        return None
    # Prefer explicit discovery endpoint
    for path in ('/api/discovery', '/api/hub'):
        try:
            req = Request(base_url + path, headers={'User-Agent': 'zone-node-discovery/1.0'})
            with urlopen(req, timeout=timeout) as r:
                raw = r.read().decode('utf-8', errors='replace')
            data = json.loads(raw or '{}')
            if path == '/api/discovery':
                if isinstance(data, dict) and data.get('service') == 'heating_hub':
                    return {
                        'ok': True,
                        'hub_url': base_url,
                        'name': data.get('name') or 'Heating Hub',
                        'public_url': data.get('public_url') or '',
                        'matched': path,
                    }
            else:
                if isinstance(data, dict) and ('main' in data or 'downstream' in data):
                    return {
                        'ok': True,
                        'hub_url': base_url,
                        'name': 'Heating Hub',
                        'public_url': '',
                        'matched': path,
                    }
        except Exception:
            continue
    return None


def discover_hubs():
    tried = []
    matches = []
    seen_urls = set()
    for c in hub_discovery_candidates():
        tried.append(c)
        hit = probe_hub(c)
        if not hit:
            continue
        hub_url = (hit.get('hub_url') or '').rstrip('/')
        if not hub_url or hub_url in seen_urls:
            continue
        seen_urls.add(hub_url)
        matches.append(hit)
        if len(matches) >= 4:
            break
    if matches:
        return {
            'ok': True,
            'hub_url': matches[0].get('hub_url', ''),
            'matches': matches,
            'tried': tried,
            'updated_utc': now_utc_iso(),
        }
    return {'ok': False, 'error': 'hub not found', 'matches': [], 'tried': tried, 'updated_utc': now_utc_iso()}


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
        'timezone': 'America/New_York',
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
            'low_temp_threshold_f': 35.0,
            'probe_swap_idle_delta_threshold_f': 5.0,
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


def reset_to_adoption_defaults(start_hotspot=True):
    cfg = default_config()
    # Keep hardware-fixed behavior.
    cfg['call_gpio'] = 17
    cfg['call_active_mode'] = 'dry_contact_to_gnd'
    save_config(cfg)
    hotspot_result = None
    if start_hotspot:
        hotspot_result = run_net_helper('ap-on', timeout=20)
    return cfg, hotspot_result


def recalc_configured(cfg):
    cfg['configured'] = bool(cfg.get('unit_name')) and bool(cfg.get('feed_sensor_id')) and bool(cfg.get('return_sensor_id'))
    return cfg


def update_config_fields(cfg, body):
    if not isinstance(body, dict):
        return cfg
    if 'unit_name' in body:
        cfg['unit_name'] = str(body.get('unit_name') or '').strip() or cfg.get('unit_name') or 'Unnamed Zone Node'
    if 'zone_id' in body:
        cfg['zone_id'] = str(body.get('zone_id') or '').strip()
    if 'parent_zone' in body:
        cfg['parent_zone'] = str(body.get('parent_zone') or cfg.get('parent_zone') or 'Zone 1').strip()
    if 'timezone' in body:
        cfg['timezone'] = str(body.get('timezone') or cfg.get('timezone') or 'America/New_York').strip() or 'America/New_York'
    if 'feed_sensor_id' in body:
        cfg['feed_sensor_id'] = str(body.get('feed_sensor_id') or '').strip()
    if 'return_sensor_id' in body:
        cfg['return_sensor_id'] = str(body.get('return_sensor_id') or '').strip()
    if 'hub' in body and isinstance(body.get('hub'), dict):
        hub = cfg.get('hub', {}) if isinstance(cfg.get('hub'), dict) else {}
        bh = body.get('hub') or {}
        for k in ('hub_url', 'account_label', 'enroll_token'):
            if k in bh:
                hub[k] = str(bh.get(k) or '').strip()
        cfg['hub'] = hub
    if 'alerts' in body and isinstance(body.get('alerts'), dict):
        alerts = cfg.get('alerts', {}) if isinstance(cfg.get('alerts'), dict) else {}
        ba = body.get('alerts') or {}
        if 'low_temp_enabled' in ba:
            alerts['low_temp_enabled'] = bool(ba.get('low_temp_enabled'))
        if 'low_temp_threshold_f' in ba:
            try:
                alerts['low_temp_threshold_f'] = float(ba.get('low_temp_threshold_f'))
            except Exception:
                pass
        if 'probe_swap_idle_delta_threshold_f' in ba:
            try:
                alerts['probe_swap_idle_delta_threshold_f'] = float(ba.get('probe_swap_idle_delta_threshold_f'))
            except Exception:
                pass
        cfg['alerts'] = alerts
    cfg['call_gpio'] = 17
    cfg['call_active_mode'] = 'dry_contact_to_gnd'
    return recalc_configured(cfg)


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

    probes = list_probes()
    diagnostics_issues = []
    probe_swap_suspected = False
    probe_swap_detail = ""
    try:
        probe_swap_idle_delta_f = float(alerts_cfg.get('probe_swap_idle_delta_threshold_f', 5.0))
    except Exception:
        probe_swap_idle_delta_f = 5.0
    if len(probes) < 2:
        diagnostics_issues.append(f"Only {len(probes)} probe(s) detected")
    if not feed_id:
        diagnostics_issues.append("Supply probe not assigned")
    if not ret_id:
        diagnostics_issues.append("Return probe not assigned")
    if feed_id and ret_id and feed_id == ret_id:
        diagnostics_issues.append("Supply and return assigned to same probe")
    if feed_err and feed_err != 'unassigned':
        diagnostics_issues.append(f"Supply probe error: {feed_err}")
    if ret_err and ret_err != 'unassigned':
        diagnostics_issues.append(f"Return probe error: {ret_err}")
    if call_reader is None and call_reader_err:
        diagnostics_issues.append(f"Call input error: {call_reader_err}")
    # Field rule: during idle (no 24VAC call), feed should normally be the hotter pipe.
    # If return is hotter by a meaningful margin, flag a likely swapped probe assignment.
    if heating_call is False and feed_f is not None and ret_f is not None:
        try:
            idle_delta = float(ret_f) - float(feed_f)
            if idle_delta >= probe_swap_idle_delta_f:
                probe_swap_suspected = True
                probe_swap_detail = (
                    f"Probe swap suspected: return is hotter than feed during idle "
                    f"by {idle_delta:.1f}F (threshold {probe_swap_idle_delta_f:.1f}F). "
                    "Verify probe placement or use Swap Supply / Return."
                )
                diagnostics_issues.append(probe_swap_detail)
        except Exception:
            pass
    diagnostics = {
        'probe_count_detected': len(probes),
        'probe_assignment_ok': bool(feed_id and ret_id and feed_id != ret_id),
        'feed_probe_ok': feed_err is None,
        'return_probe_ok': ret_err is None,
        'call_input_ok': call_reader is not None and call_reader_err is None,
        'probe_swap_suspected': probe_swap_suspected,
        'probe_swap_detail': probe_swap_detail,
        'probe_swap_idle_delta_threshold_f': probe_swap_idle_delta_f,
        'hardware_fault': len(diagnostics_issues) > 0,
        'hardware_faults': diagnostics_issues,
        'self_diagnostic_status': 'Attention Required' if diagnostics_issues else 'Normal',
    }

    return {
        'configured': bool(cfg.get('configured')),
        'unit_name': cfg.get('unit_name') or 'Unnamed Zone Node',
        'zone_id': cfg.get('zone_id') or '',
        'parent_zone': cfg.get('parent_zone') or '',
        'timezone': cfg.get('timezone') or 'America/New_York',
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
        'diagnostics': diagnostics,
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
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
:root{--bg:#edf3f6;--panel:#fff;--ink:#10242b;--muted:#5b717a;--line:#d8e2e7;--brand:#114b5f;--brand2:#0f766e}
body{margin:0;padding:0;background:linear-gradient(180deg,#eef4f7,#e6efee);font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--ink)}
.wrap{max-width:1100px;margin:0 auto;padding:1rem}
.hero{background:radial-gradient(500px 200px at 5% 0%,rgba(255,255,255,.12),transparent 60%),linear-gradient(130deg,#0f172a,#114b5f 55%,#0f766e);color:#effcff;border-radius:18px;padding:20px 22px;box-shadow:0 14px 32px rgba(0,0,0,.14);border:1px solid rgba(255,255,255,.08)}
.hero h1{margin:0;font-size:1.35rem}.hero p{margin:.35rem 0 0;opacity:.95}
.grid{display:grid;grid-template-columns:1.2fr .8fr;gap:14px;margin-top:14px}
.card{background:linear-gradient(180deg,#fff,#fbfdff);border:1px solid var(--line);border-radius:16px;padding:14px;box-shadow:0 10px 24px rgba(10,31,44,.07)}
.card h2{margin:0 0 10px;font-size:1.02rem}
label{display:block;font-size:.82rem;color:var(--muted);font-weight:700;margin:10px 0 4px;text-transform:uppercase;letter-spacing:.03em}
input{width:100%;padding:10px;border:1px solid var(--line);border-radius:12px;font-size:15px;background:#fff}
select.form-select{font-size:15px;border-radius:12px;border-color:var(--line);padding-top:.55rem;padding-bottom:.55rem}
input:focus,select.form-select:focus{outline:none;border-color:#8bc2d3;box-shadow:0 0 0 4px rgba(22,138,173,.12)}
button{margin-top:10px;background:linear-gradient(120deg,var(--brand),var(--brand2));color:#fff;border:0;padding:10px 14px;border-radius:12px;font-weight:700;cursor:pointer;box-shadow:0 8px 20px rgba(17,75,95,.15)}
button.secondary{background:#fff;color:#114b5f;border:1px solid var(--line);box-shadow:none}
button.warn{background:#8a2d12}
.row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
pre{background:#f7fbfd;border:1px solid var(--line);padding:12px;border-radius:12px;overflow:auto;font-size:.82rem;max-height:560px}
.badge{display:inline-block;padding:6px 10px;border-radius:999px;background:#edf6f9;border:1px solid #cfe0e8;color:#114b5f;font-weight:700;font-size:.8rem}
small{color:var(--muted)} .pill{display:inline-block;border:1px solid var(--line);padding:6px 10px;border-radius:999px;background:#f7fbfd;margin-right:6px;font-size:.85rem}
#msg{margin-top:10px;font-weight:700;color:#114b5f;white-space:pre-wrap}
.hidden{display:none}
.success-box{margin-top:12px;padding:12px;border:1px solid #bfe3d6;background:linear-gradient(180deg,#eefbf5,#f7fffb);border-radius:14px}
.success-box h3{margin:0 0 6px;font-size:1rem;color:#0c5a48}
.tabrow{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0 8px}
.segrow{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;padding:4px;background:#f3f7fa;border:1px solid var(--line);border-radius:12px;width:max-content;max-width:100%}
.segbtn{margin-top:0;background:transparent;color:#114b5f;border:1px solid transparent;padding:5px 10px;border-radius:9px;font-weight:700;font-size:.85rem;line-height:1.1;cursor:pointer;box-shadow:none}
.segbtn.active{background:#ffffff;color:#0f4c5c;border-color:#d6e2e8;box-shadow:0 1px 4px rgba(15,76,92,.08)}
.itab{border:1px solid var(--line);background:#fff;color:#114b5f;padding:6px 10px;border-radius:999px;font-weight:700;cursor:pointer;box-shadow:none}
.itab.active{background:linear-gradient(120deg,var(--brand),var(--brand2));color:#fff;border-color:transparent}
.itpanel{display:none;background:#f7fbfd;border:1px solid var(--line);border-radius:10px;padding:10px;font-size:.92rem}
.itpanel.active{display:block}
.live-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.live-item{border:1px solid var(--line);border-radius:12px;padding:10px;background:#f9fcfe}
.live-k{font-size:.78rem;color:var(--muted);font-weight:700;text-transform:uppercase;letter-spacing:.04em}
.live-v{margin-top:4px;font-weight:700;font-size:1rem;color:#10242b}
.live-v.temp{font-size:1.2rem}
.ok-pill,.warn-pill{display:inline-block;padding:4px 8px;border-radius:999px;font-size:.8rem;font-weight:700}
.ok-pill{background:#edf9f4;border:1px solid #bfe3d6;color:#0c5a48}
.warn-pill{background:#fff2f2;border:1px solid #f0c6c6;color:#a61b1b}
@media (max-width:760px){.wrap{padding:.75rem}.grid,.row{grid-template-columns:1fr}.hero{padding:16px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <h1>Zone Setup</h1>
    <p>Phone-friendly setup for supply/return probe mapping, dry-contact call input, Wi-Fi assignment, and hub linking.</p>
  </div>
  <div class="grid">
    <div class="card">
      <h2>Device Setup</h2>
      <div class="badge" id="cfgState">Loading...</div>
      <div style="margin-top:8px">
        <span class="pill" id="wifiStatus">Wi-Fi: --</span>
        <span class="pill" id="apStatus">Setup Hotspot: --</span>
      </div>
      <form id="setupForm" autocomplete="off">
        <label>Unit Name</label>
        <input name="unit_name" placeholder="AV Room Coil" required>

        <div class="row">
          <div>
            <label>Zone ID</label>
            <input name="zone_id" placeholder="zone1-av-room">
          </div>
          <div>
            <label>Parent Zone</label>
            <select class="form-select" name="parent_zone">
              <option>Zone 1</option>
              <option>Zone 2</option>
              <option>Zone 3</option>
              <option>Other</option>
            </select>
          </div>
        </div>
        <div class="row">
          <div>
            <label>Time Zone</label>
            <select class="form-select" name="timezone">
              <option value="America/New_York">Eastern (America/New_York)</option>
              <option value="America/Chicago">Central (America/Chicago)</option>
              <option value="America/Denver">Mountain (America/Denver)</option>
              <option value="America/Los_Angeles">Pacific (America/Los_Angeles)</option>
              <option value="UTC">UTC</option>
            </select>
          </div>
          <div>
            <label>Time Display</label>
            <input value="Readable Local Time" disabled>
          </div>
        </div>

        <div class="row">
          <div>
            <label>Supply Probe</label>
            <select class="form-select" name="feed_sensor_id" id="feedSel"></select>
          </div>
          <div>
            <label>Return Probe</label>
            <select class="form-select" name="return_sensor_id" id="returnSel"></select>
          </div>
        </div>
        <div>
          <button type="button" class="secondary" id="swapProbesBtn">Swap Supply / Return</button>
        </div>

        <h2 style="margin-top:14px">Alerts</h2>
        <div class="row">
          <div>
            <label>Low Temp Alert Threshold (F)</label>
            <input type="number" step="0.5" name="low_temp_threshold_f" value="35">
          </div>
          <div>
            <label>Low Temp Alerts</label>
            <select class="form-select" name="low_temp_enabled">
              <option value="true">Enabled</option>
              <option value="false">Disabled</option>
            </select>
          </div>
        </div>

        <h2 style="margin-top:14px">Building Wi-Fi</h2>
        <div class="row">
          <div>
            <label>Wi-Fi SSID</label>
            <div class="segrow">
              <button type="button" class="segbtn active" id="wifiModeSelectBtn">Select Network</button>
              <button type="button" class="segbtn" id="wifiModeManualBtn">Type Manually</button>
            </div>
            <div id="wifiSelectWrap">
              <select class="form-select" name="wifi_ssid" id="wifiSsidSel"></select>
              <div style="margin-top:6px"><button type="button" class="secondary" id="scanWifiBtn">Scan Wi-Fi Networks</button></div>
            </div>
            <div id="wifiManualWrap" class="hidden">
              <label style="margin-top:8px">Wi-Fi Name (SSID)</label>
              <input name="wifi_ssid_manual" placeholder="Type Wi-Fi name if scan is unavailable">
            </div>
          </div>
          <div>
            <label>Wi-Fi Password</label>
            <input name="wifi_key" type="text" inputmode="text" placeholder="Enter Wi-Fi password" autocomplete="off" autocapitalize="none" autocorrect="off" spellcheck="false" data-lpignore="true" readonly onfocus="this.removeAttribute('readonly');">
          </div>
        </div>

        <label>Hub URL (optional for now)</label>
        <div class="row">
          <div>
            <input name="hub_url" placeholder="https://165-boiler.tail58e171.ts.net">
          </div>
          <div>
            <button type="button" class="secondary" id="findHubBtn" style="width:100%">Find Hub</button>
          </div>
        </div>
        <div id="hubChoicesWrap" class="hidden" style="margin-top:8px">
          <small>Select detected hub:</small>
          <div id="hubChoices" class="tabrow" style="margin-top:6px"></div>
        </div>

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
            <a id="viewHubBtn" href="#" target="_blank" rel="noopener" class="itab hidden" style="text-decoration:none">View Hub</a>
          </div>
        <div id="instructionsWrap" class="hidden">
          <div class="tabrow">
            <button type="button" class="itab active" data-itab="wiring">Wiring</button>
            <button type="button" class="itab" data-itab="feedreturn">Supply vs Return</button>
            <button type="button" class="itab" data-itab="probe">Probe Placement</button>
            <button type="button" class="itab" data-itab="mounting">Securing Probe</button>
            <button type="button" class="itab" data-itab="troubleshoot">Troubleshooting</button>
          </div>
          <div class="itpanel active" data-ipanel="wiring">
            <strong>Wiring (placeholder)</strong><br>
            Add wiring diagrams and terminal references for dry-contact relay, GPIO input, and probe lead routing.
          </div>
          <div class="itpanel" data-ipanel="feedreturn">
            <strong>Supply vs Return (placeholder)</strong><br>
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
            Add checks for no probe reading, swapped supply/return, no 24VAC call, and Wi-Fi/Tailscale connectivity.
          </div>
        </div>
      </div>
      <div style="margin-top:10px">
        <small>Setup hotspot stays on during onboarding. Connect in iPhone Wi-Fi settings to <code>HeatingHub-Setup-xxxx</code>, then open <code>http://10.42.0.1:8090</code>. If Wi-Fi scan is unavailable in hotspot mode, use the manual SSID field.</small>
        <div style="margin-top:8px">
          <small>Hub Link: <a id="hubUrlLink" href="#" target="_blank" rel="noopener">Not set</a></small>
        </div>
      </div>
      <div style="margin-top:14px;border-top:1px solid var(--line);padding-top:12px">
        <h2 style="margin:0 0 8px">Advanced / Service</h2>
        <small>Use this only when repurposing the device for a new install.</small>
        <div style="margin-top:8px">
          <button type="button" class="warn" id="resetDeviceBtn">Reset Device for New Install</button>
        </div>
      </div>
    </div>
    <div class="card">
      <h2>Live Device Readings</h2>
      <div id="liveSummary" class="live-grid">
        <div class="live-item"><div class="live-k">Loading</div><div class="live-v">Waiting for device data...</div></div>
      </div>
      <pre id="live" class="hidden">Loading...</pre>
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
  (probes||[]).forEach((p, idx) => {
    const sid = (typeof p === 'string') ? p : p.sensor_id;
    const temp = (p && p.temp_f !== undefined && p.temp_f !== null) ? `${Number(p.temp_f).toFixed(1)}°F` : '--';
    const err = (p && p.error) ? ` [${p.error}]` : '';
    const o=document.createElement('option');
    o.value=sid;
    const friendly = `Temp Probe ${idx + 1}`;
    o.textContent = (typeof p === 'string')
      ? `${friendly}`
      : `${friendly} (${temp})${err}`;
    sel.appendChild(o);
  });
  if(value!==undefined&&value!==null) sel.value=value;
}
function fillWifiSelect(sel, networks, current){
  sel.innerHTML='';
  const blank=document.createElement('option');
  blank.value='';
  blank.textContent='-- Select Wi-Fi Network --';
  sel.appendChild(blank);
  const bars = (sig) => {
    const n = Number(sig);
    if (!Number.isFinite(n)) return '⋯';
    if (n >= 75) return '▂▄▆█';
    if (n >= 55) return '▂▄▆_';
    if (n >= 35) return '▂▄__';
    if (n > 0) return '▂___';
    return '⋯';
  };
  for(const n of (networks||[])){
    const o=document.createElement('option');
    o.value=n.ssid;
    const sec = n.security ? ` · ${n.security}` : '';
    o.textContent = `${bars(n.signal)}  ${n.ssid}${sec}`;
    sel.appendChild(o);
  }
  if(current) sel.value = current;
}
function fmtDateTime(iso, tz){
  if(!iso) return '--';
  try {
    return new Intl.DateTimeFormat([], {
      timeZone: tz || undefined,
      weekday: 'short',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      second: '2-digit',
      timeZoneName: 'short'
    }).format(new Date(iso));
  } catch (e) {
    try { return new Date(iso).toLocaleString(); } catch (_) { return iso; }
  }
}
function setMsg(t){ document.getElementById('msg').textContent = t || ''; }
function hubRouteLabel(url){
  const u = (url || '').trim().toLowerCase();
  if (!u) return '';
  if (u.startsWith('https://') && u.includes('.ts.net')) return 'Public (Tailscale Funnel)';
  if (u.startsWith('http://100.')) return 'Tailscale (Private)';
  if (u.startsWith('http://192.168.') || u.startsWith('http://10.') || u.startsWith('http://172.16.') || u.startsWith('http://172.17.') || u.startsWith('http://172.18.') || u.startsWith('http://172.19.') || u.startsWith('http://172.2') || u.startsWith('http://172.30.') || u.startsWith('http://172.31.')) return 'Local LAN';
  if (u.includes('.local') || u.includes('165-boiler') || u.includes('165water-boiler')) return 'Local Hostname';
  if (u.startsWith('https://')) return 'HTTPS';
  if (u.startsWith('http://')) return 'HTTP';
  return 'Hub';
}
function renderHubChoices(matches){
  const wrap = document.getElementById('hubChoicesWrap');
  const box = document.getElementById('hubChoices');
  if(!wrap || !box) return;
  box.innerHTML = '';
  const arr = Array.isArray(matches) ? matches : [];
  if(!arr.length){
    wrap.classList.add('hidden');
    return;
  }
  wrap.classList.remove('hidden');
  arr.forEach((m, idx) => {
    const url = (m && m.hub_url ? m.hub_url : '').trim();
    if(!url) return;
    const routeKind = hubRouteLabel(url);
    const card = document.createElement('button');
    card.type = 'button';
    card.className = 'secondary';
    card.style.marginTop = '0';
    card.style.textAlign = 'left';
    card.style.display = 'flex';
    card.style.flexDirection = 'column';
    card.style.gap = '2px';
    card.title = url;
    const title = document.createElement('span');
    title.textContent = `${m.name || 'Heating Hub'} ${arr.length > 1 ? `(${idx + 1})` : ''}`;
    title.style.fontWeight = '700';
    const sub = document.createElement('span');
    sub.textContent = routeKind + (m.public_url && m.public_url.trim() === url ? '' : ` · ${url}`);
    sub.style.fontSize = '.78rem';
    sub.style.opacity = '.8';
    card.appendChild(title);
    card.appendChild(sub);
    card.addEventListener('click', () => {
      const f = document.getElementById('setupForm');
      f.hub_url.value = url;
      updateHubLinkUI(url);
      setMsg(`Hub selected: ${url}`);
    });
    box.appendChild(card);
  });
  if(!box.children.length){
    wrap.classList.add('hidden');
  }
}
function updateHubLinkUI(url){
  const link = document.getElementById('hubUrlLink');
  const btn = document.getElementById('viewHubBtn');
  const clean = (url || '').trim();
  if(link){
    if(clean){
      link.href = clean;
      link.textContent = clean;
    } else {
      link.href = '#';
      link.textContent = 'Not set';
    }
  }
  if(btn){
    if(clean){
      btn.href = clean;
      btn.classList.remove('hidden');
    } else {
      btn.href = '#';
      btn.classList.add('hidden');
    }
  }
}
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
function setWifiMode(mode){
  const selectMode = mode !== 'manual';
  document.getElementById('wifiSelectWrap').classList.toggle('hidden', !selectMode);
  document.getElementById('wifiManualWrap').classList.toggle('hidden', selectMode);
  document.getElementById('wifiModeSelectBtn').classList.toggle('active', selectMode);
  document.getElementById('wifiModeManualBtn').classList.toggle('active', !selectMode);
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
    setWifiMode('manual');
    setMsg('Wi-Fi scan unavailable (common while setup hotspot is active). Use the Type Manually tab for Wi-Fi name.');
  }
}
async function refreshAll(){
  const s = await j('/api/setup-state?_=' + Date.now());
  document.getElementById('cfgState').textContent = s.configured ? 'Configured' : 'Setup Required';
  const f = document.getElementById('setupForm');
  f.unit_name.value = s.config.unit_name || '';
  f.zone_id.value = s.config.zone_id || '';
  f.parent_zone.value = s.config.parent_zone || 'Zone 1';
  f.timezone.value = s.config.timezone || 'America/New_York';
  f.low_temp_threshold_f.value = (s.config.alerts && s.config.alerts.low_temp_threshold_f != null) ? s.config.alerts.low_temp_threshold_f : 35;
  f.low_temp_enabled.value = (s.config.alerts && s.config.alerts.low_temp_enabled === false) ? 'false' : 'true';
  fillWifiSelect(document.getElementById('wifiSsidSel'), [], (s.config.wifi && s.config.wifi.ssid) || '');
  f.wifi_ssid_manual.value = (s.config.wifi && s.config.wifi.ssid) || '';
  setWifiMode((s.config.wifi && s.config.wifi.ssid) ? 'manual' : 'select');
  f.wifi_key.value = '';
  f.hub_url.value = (s.config.hub && s.config.hub.hub_url) || '';
  updateHubLinkUI(f.hub_url.value);
  renderHubChoices([]);
  f.account_label.value = (s.config.hub && s.config.hub.account_label) || '';
  f.enroll_token.value = (s.config.hub && s.config.hub.enroll_token) || '';
  fillSelect(document.getElementById('feedSel'), s.probe_choices || s.probes_detected, s.config.feed_sensor_id || '');
  fillSelect(document.getElementById('returnSel'), s.probe_choices || s.probes_detected, s.config.return_sensor_id || '');
  if (s.configured) {
    showSetupSuccess(true, 'This zone node is configured. You can review instructions or set up another device.');
  }
}
async function refreshLive(){
  try {
    const data = await j('/api/zone?_=' + Date.now());
    document.getElementById('live').textContent = JSON.stringify(data, null, 2);
    const fmt = (v) => (v === null || v === undefined ? '--' : `${Number(v).toFixed(1)} °F`);
    const feedErr = data.feed_error ? ` [${data.feed_error}]` : '';
    const retErr = data.return_error ? ` [${data.return_error}]` : '';
    const lowTempBadge = data.low_temp_alert
      ? `<span class="warn-pill">Low Temp Warning</span>`
      : `<span class="ok-pill">Normal</span>`;
    const configuredBadge = data.configured
      ? `<span class="ok-pill">Configured</span>`
      : `<span class="warn-pill">Setup Required</span>`;
    document.getElementById('liveSummary').innerHTML = `
      <div class="live-item">
        <div class="live-k">Device</div>
        <div class="live-v">${data.unit_name || '--'}</div>
        <div class="live-k" style="margin-top:8px">Status</div>
        <div class="live-v">${configuredBadge}</div>
      </div>
      <div class="live-item">
        <div class="live-k">Zone / Parent</div>
        <div class="live-v">${data.zone_id || '--'} / ${data.parent_zone || '--'}</div>
        <div class="live-k" style="margin-top:8px">Call Status</div>
        <div class="live-v">${data.call_status || '--'}</div>
      </div>
      <div class="live-item">
        <div class="live-k">Supply Temperature</div>
        <div class="live-v temp">${fmt(data.feed_f)}</div>
        <div class="live-k" style="margin-top:6px">Probe</div>
        <div class="live-v" style="font-size:.88rem;font-weight:600">${data.feed_sensor_id || '--'}${feedErr}</div>
      </div>
      <div class="live-item">
        <div class="live-k">Return Temperature</div>
        <div class="live-v temp">${fmt(data.return_f)}</div>
        <div class="live-k" style="margin-top:6px">Probe</div>
        <div class="live-v" style="font-size:.88rem;font-weight:600">${data.return_sensor_id || '--'}${retErr}</div>
      </div>
      <div class="live-item">
        <div class="live-k">Low Temp Status</div>
        <div class="live-v">${lowTempBadge}</div>
        <div class="live-k" style="margin-top:8px">Threshold</div>
        <div class="live-v">${data.low_temp_threshold_f ?? '--'} °F</div>
      </div>
      <div class="live-item">
        <div class="live-k">Updated (${data.timezone || 'Local'})</div>
        <div class="live-v" style="font-size:.9rem">${fmtDateTime(data.updated_utc, data.timezone)}</div>
        <div class="live-k" style="margin-top:8px">Wi-Fi</div>
        <div class="live-v" style="font-size:.9rem">${data.wifi_ssid || '(not connected)'}</div>
      </div>
    `;
  }
  catch (e) {
    document.getElementById('live').textContent = 'Error: ' + e.message;
    document.getElementById('liveSummary').innerHTML = `<div class="live-item"><div class="live-k">Error</div><div class="live-v">${e.message}</div></div>`;
  }
}
let hubAutoDiscoverTried = false;
async function discoverHub(autoMode){
  try {
    if (!autoMode) setMsg('Looking for hub on local network / Tailscale...');
    const res = await j('/api/hub/discover?_=' + Date.now());
    const matches = Array.isArray(res && res.matches) ? res.matches : [];
    if (matches.length > 1) {
      renderHubChoices(matches);
      if (!autoMode) setMsg(`Multiple hubs found (${matches.length}). Select the correct hub.`);
      return true;
    }
    if (res && res.ok && res.hub_url) {
      const f = document.getElementById('setupForm');
      f.hub_url.value = res.hub_url;
      updateHubLinkUI(res.hub_url);
      renderHubChoices(matches);
      if (!autoMode) {
        setMsg(`Hub found: ${res.hub_url}`);
      }
      return true;
    }
    renderHubChoices([]);
    if (!autoMode) setMsg('Hub not found automatically. Enter Hub URL manually.');
    return false;
  } catch (e) {
    renderHubChoices([]);
    if (!autoMode) setMsg('Hub lookup failed. Enter Hub URL manually.');
    return false;
  }
}
document.getElementById('setupForm').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const f = ev.target;
  const payload = {
    unit_name: f.unit_name.value.trim(),
    zone_id: f.zone_id.value.trim(),
    parent_zone: f.parent_zone.value.trim(),
    timezone: f.timezone.value.trim(),
    feed_sensor_id: f.feed_sensor_id.value,
    return_sensor_id: f.return_sensor_id.value,
    call_gpio: 17,
    alerts: { low_temp_threshold_f: Number(f.low_temp_threshold_f.value || 35), low_temp_enabled: (f.low_temp_enabled.value === 'true') },
    wifi: { ssid: (f.wifi_ssid_manual.value.trim() || f.wifi_ssid.value.trim()), password: f.wifi_key.value },
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
    setMsg((res.message || 'Saved') + (res.wifi_apply && res.wifi_apply.ok ? '\\nWi-Fi apply started.' : (res.wifi_apply ? ('\\nWi-Fi apply issue: ' + (res.wifi_apply.stderr || res.wifi_apply.stdout || 'unknown')) : '')));
    if (res.configured) {
      const wifiOk = !res.wifi_apply || !!res.wifi_apply.ok;
      showSetupSuccess(true, wifiOk ? 'Setup Successful. This zone node is now configured and reporting.' : 'Configuration saved. Verify Wi-Fi connection, then continue.');
    }
    await refreshAll(); await refreshWifiStatus(); await refreshLive();
  } catch (e) { setMsg('Save failed: ' + e.message); }
});
document.getElementById('refreshBtn').addEventListener('click', async ()=>{ await refreshAll(); await refreshWifiStatus(); await refreshLive(); });
document.getElementById('scanWifiBtn').addEventListener('click', async ()=>{ setMsg('Scanning Wi-Fi networks...'); await refreshWifiScan(); setMsg('Wi-Fi scan complete.'); });
document.getElementById('setupForm').hub_url.addEventListener('input', (e)=> updateHubLinkUI(e.target.value));
document.getElementById('findHubBtn').addEventListener('click', async ()=> { await discoverHub(false); });
document.getElementById('wifiSsidSel').addEventListener('change', ()=>{
  const f = document.getElementById('setupForm');
  if (f.wifi_ssid.value) f.wifi_ssid_manual.value = f.wifi_ssid.value;
});
document.getElementById('wifiModeSelectBtn').addEventListener('click', ()=> setWifiMode('select'));
document.getElementById('wifiModeManualBtn').addEventListener('click', ()=> setWifiMode('manual'));
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
document.getElementById('resetDeviceBtn').addEventListener('click', async ()=>{
  const ok = confirm('Reset this device to adoption defaults? This clears unit name, zone ID, probe assignment, Wi-Fi, and hub settings, and returns it to setup hotspot mode.');
  if(!ok) return;
  setMsg('Resetting device to adoption defaults...');
  try {
    const res = await j('/api/reset-device', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ start_hotspot: true }) });
    showSetupSuccess(false);
    showInstructions(false);
    setWifiMode('manual');
    setMsg((res.message || 'Device reset complete.') + '\\nReconnect to the setup hotspot and continue adoption.');
    await refreshAll(); await refreshWifiStatus(); await refreshLive();
  } catch (e) {
    setMsg('Reset failed: ' + e.message);
  }
});
refreshAll().then(async ()=>{
  const currentHub = (document.getElementById('setupForm').hub_url.value || '').trim();
  if (!currentHub && !hubAutoDiscoverTried) {
    hubAutoDiscoverTried = true;
    await discoverHub(true);
  }
});
refreshWifiStatus(); refreshLive(); refreshWifiScan();
setInterval(refreshLive, 5000); setInterval(refreshWifiStatus, 10000);
</script>
</body>
</html>'''


def html_demo_page():
    demo = html_page()
    demo = demo.replace('<title>Zone Setup</title>', '<title>Zone Setup Demo</title>', 1)
    demo = demo.replace('<h1>Zone Setup</h1>', '<h1>Zone Setup Demo</h1>', 1)
    demo = demo.replace(
        '<p>Phone-friendly setup for supply/return probe mapping, dry-contact call input, Wi-Fi assignment, and hub linking.</p>',
        '<p>Read-only demo preview of the installer onboarding page. This view does not save changes or control any device.</p>',
        1,
    )
    for a, b in (
        ("'/api/setup-state", "'/demo/api/setup-state"),
        ('"/api/setup-state', '"/demo/api/setup-state'),
        ("'/api/zone", "'/demo/api/zone"),
        ('"/api/zone', '"/demo/api/zone'),
        ("'/api/wifi/status", "'/demo/api/wifi/status"),
        ('"/api/wifi/status', '"/demo/api/wifi/status'),
        ("'/api/wifi/scan", "'/demo/api/wifi/scan"),
        ('"/api/wifi/scan', '"/demo/api/wifi/scan'),
        ("'/api/setup'", "'/demo/api/setup'"),
        ('"/api/setup"', '"/demo/api/setup"'),
        ("'/api/hub/discover", "'/demo/api/hub/discover"),
        ('"/api/hub/discover', '"/demo/api/hub/discover'),
    ):
        demo = demo.replace(a, b)
    demo_inject = r"""
<script>
(() => {
  window.addEventListener('load', () => {
    const msg = document.getElementById('msg');
    if (msg) msg.textContent = 'Demo Mode: safe preview only. Changes are not saved.';
    const badge = document.getElementById('cfgState');
    if (badge) badge.textContent = 'Demo Preview';
    const saveBtn = document.querySelector('#setupForm button[type="submit"]');
    if (saveBtn) saveBtn.textContent = 'Preview Save (Demo Only)';
  });
})();
</script>
"""
    if "<script>\nasync function j(" in demo:
        return demo.replace("<script>\nasync function j(", demo_inject + "\n<script>\nasync function j(", 1)
    return demo.replace("</body>", demo_inject + "\n</body>", 1)


def demo_setup_state():
    return {
        'configured': False,
        'config': {
            'configured': False,
            'unit_name': '',
            'zone_id': '',
            'parent_zone': 'Zone 1',
            'timezone': 'America/New_York',
            'feed_sensor_id': '',
            'return_sensor_id': '',
            'call_gpio': 17,
            'alerts': {'low_temp_enabled': True, 'low_temp_threshold_f': 35},
            'wifi': {'ssid': '', 'password': ''},
            'hub': {'hub_url': 'https://165-boiler.tail58e171.ts.net/', 'account_label': '165 Water Street', 'enroll_token': ''},
        },
        'probes_detected': ['28-0417701d59ff', '28-041770d515ff'],
        'probe_choices': [
            {'sensor_id': '28-0417701d59ff', 'temp_f': 118.6, 'error': None},
            {'sensor_id': '28-041770d515ff', 'temp_f': 110.2, 'error': None},
        ],
        'updated_utc': now_utc_iso(),
    }


def demo_zone_payload():
    return {
        'configured': False,
        'unit_name': 'Example Zone Node',
        'zone_id': 'zone1-av-room',
        'parent_zone': 'Zone 1',
        'timezone': 'America/New_York',
        'feed_f': 118.6,
        'return_f': 110.2,
        'feed_sensor_id': '28-0417701d59ff',
        'return_sensor_id': '28-041770d515ff',
        'feed_error': None,
        'return_error': None,
        'heating_call': True,
        'call_status': '24VAC Present (Calling)',
        'low_temp_enabled': True,
        'low_temp_threshold_f': 35.0,
        'low_temp_alert': False,
        'low_temp_status': 'Normal',
        'low_temp_detail': '',
        'wifi_ssid': 'Example-Building-WiFi',
        'updated_utc': now_utc_iso(),
    }


def demo_wifi_status():
    return {
        'ok': True,
        'connected_ssid': 'Example-Building-WiFi',
        'ap_active': False,
        'ap_ssid': '',
        'updated_utc': now_utc_iso(),
    }


def demo_wifi_scan():
    return {
        'ok': True,
        'updated_utc': now_utc_iso(),
        'networks': [
            {'ssid': 'Example-Building-WiFi', 'signal': 78, 'security': 'WPA2'},
            {'ssid': 'Guest-WiFi', 'signal': 51, 'security': 'WPA2'},
            {'ssid': 'MechanicalRoom', 'signal': 32, 'security': 'WPA2'},
        ],
    }


def demo_hub_discover():
    return {
        'ok': True,
        'hub_url': KNOWN_PUBLIC_HUB_URL.rstrip('/'),
        'matches': [{
            'ok': True,
            'hub_url': KNOWN_PUBLIC_HUB_URL.rstrip('/'),
            'name': '165 Water Street Heating Hub',
            'public_url': KNOWN_PUBLIC_HUB_URL,
            'matched': '/api/discovery',
        }],
        'name': '165 Water Street Heating Hub',
        'public_url': KNOWN_PUBLIC_HUB_URL,
        'matched': '/api/discovery',
        'tried': [KNOWN_PUBLIC_HUB_URL.rstrip('/')],
        'updated_utc': now_utc_iso(),
    }


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
        if route == '/demo':
            return self._send_html(html_demo_page())
        if route == '/demo/api/setup-state':
            return self._send_json(demo_setup_state())
        if route == '/demo/api/zone':
            return self._send_json(demo_zone_payload())
        if route == '/demo/api/wifi/status':
            return self._send_json(demo_wifi_status())
        if route == '/demo/api/wifi/scan':
            return self._send_json(demo_wifi_scan())
        if route == '/demo/api/hub/discover':
            return self._send_json(demo_hub_discover())
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
        if route == '/api/hub/discover':
            return self._send_json(discover_hubs())
        if route == '/api/manage/status':
            cfg = load_config()
            safe_cfg = json.loads(json.dumps(cfg))
            if isinstance(safe_cfg.get('wifi'), dict):
                safe_cfg['wifi']['password'] = ''
            return self._send_json({
                'config': safe_cfg,
                'zone': get_payload(),
                'setup_state': {'configured': bool(cfg.get('configured')), 'probes_detected': list_probes(), 'probe_choices': probe_choices()},
                'updated_utc': now_utc_iso(),
            })
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

        if route == '/demo/api/setup':
            return self._send_json({
                'ok': True,
                'configured': True,
                'message': 'Demo mode: configuration preview only (nothing was saved)',
                'wifi_apply': {'ok': True, 'stdout': 'demo', 'stderr': ''},
            })

        if route == '/api/reset-device':
            try:
                n = int(self.headers.get('Content-Length', '0'))
            except Exception:
                n = 0
            raw = self.rfile.read(n) if n > 0 else b'{}'
            try:
                body = json.loads(raw.decode('utf-8') or '{}')
            except Exception:
                body = {}
            start_hotspot = True
            if isinstance(body, dict) and 'start_hotspot' in body:
                start_hotspot = bool(body.get('start_hotspot'))
            cfg, hotspot_result = reset_to_adoption_defaults(start_hotspot=start_hotspot)
            return self._send_json({
                'ok': True,
                'message': 'Device reset to adoption defaults',
                'config': cfg,
                'hotspot': hotspot_result,
                'updated_utc': now_utc_iso(),
            })

        if route in ('/api/manage/swap-probes', '/api/manage/update'):
            try:
                n = int(self.headers.get('Content-Length', '0'))
            except Exception:
                n = 0
            raw = self.rfile.read(n) if n > 0 else b'{}'
            try:
                body = json.loads(raw.decode('utf-8') or '{}')
            except Exception:
                body = {}
            cfg = load_config()
            if route == '/api/manage/swap-probes':
                a = str(cfg.get('feed_sensor_id') or '')
                b = str(cfg.get('return_sensor_id') or '')
                cfg['feed_sensor_id'], cfg['return_sensor_id'] = b, a
                recalc_configured(cfg)
                save_config(cfg)
                return self._send_json({'ok': True, 'message': 'Feed/return probes swapped', 'config': cfg, 'zone': get_payload(), 'updated_utc': now_utc_iso()})
            cfg = update_config_fields(cfg, body if isinstance(body, dict) else {})
            if cfg.get('feed_sensor_id') and cfg.get('return_sensor_id') and cfg.get('feed_sensor_id') == cfg.get('return_sensor_id'):
                return self._send_json({'error': 'feed and return probes must differ'}, 400)
            save_config(cfg)
            return self._send_json({'ok': True, 'message': 'Zone device settings updated', 'config': cfg, 'zone': get_payload(), 'updated_utc': now_utc_iso()})

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
        cfg['timezone'] = str(body.get('timezone') or cfg.get('timezone') or 'America/New_York').strip() or 'America/New_York'
        cfg['feed_sensor_id'] = str(body.get('feed_sensor_id') or '').strip()
        cfg['return_sensor_id'] = str(body.get('return_sensor_id') or '').strip()
        cfg['call_gpio'] = 17
        cfg['call_active_mode'] = 'dry_contact_to_gnd'

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
