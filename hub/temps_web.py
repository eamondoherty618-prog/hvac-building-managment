#!/usr/bin/env python3
import json
import threading
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse, urlencode
from urllib.request import Request, urlopen

try:
    from gpiozero import Button
except Exception:
    Button = None

LOCAL_SENSORS = {
    "28-0317621335ff": "feed_1",
    "28-03176218f4ff": "return_1",
    "28-041770d777ff": "feed_2",
    "28-041770e1f3ff": "return_2",
}
W1_BASE = Path("/sys/bus/w1/devices")
CONFIG_PATH = Path("/home/eamondoherty618/downstream_zones.json")
HISTORY_FILE = Path("/home/eamondoherty618/hub_history.jsonl")
SAMPLE_INTERVAL_SECONDS = 10
HISTORY_SAMPLE_EVERY_LOOPS = 6  # 10s sampler -> 60s history points
RETENTION_DAYS = 31
ZONE1_DELTA_THRESHOLD_F = 10.0
ALERT_CONFIG_PATH = Path("/home/eamondoherty618/alert_config.json")
ALERT_LOG_PATH = Path("/home/eamondoherty618/alert_events.log")
DEFAULT_ALERT_COOLDOWN_MIN = 120
ZONE1_CALL_ALERT_GRACE_SECONDS = 120
PUBLIC_HUB_URL = "https://165-boiler.tail58e171.ts.net/"
HUB_SITE_NAME = "165 Water Street Heating Hub"

DEFAULT_CONFIG = {
    "zones": [
        {
            "id": "zone1-av-room",
            "name": "AV Room Coil",
            "parent_zone": "Zone 1",
            "source_url": "http://100.100.100.100:8090/api/zone",
            "delta_ok_f": 8.0,
        },
        {
            "id": "zone1-first-floor-prod",
            "name": "1st Floor Production",
            "parent_zone": "Zone 1",
            "source_url": "http://100.100.100.101:8090/api/zone",
            "delta_ok_f": 8.0,
        },
        {
            "id": "zone2-example",
            "name": "Zone 2 Example",
            "parent_zone": "Zone 2",
            "source_url": "http://100.100.100.102:8090/api/zone",
            "delta_ok_f": 8.0,
        },
    ]
}

stop_event = threading.Event()
last_alert_sent_at = None
last_alert_key = None
downstream_cache_lock = threading.Lock()
downstream_cache = []
downstream_cache_updated_utc = None
local_cache_lock = threading.Lock()
local_cache = None
local_cache_updated_utc = None
main_call_state_lock = threading.Lock()
main_call_prev_active = None
main_call_active_since_utc = None

main_call_input = None
if Button is not None:
    try:
        # Dry contact on GPIO17 to GND (pin 6): closed means call active
        main_call_input = Button(17, pull_up=True)
    except Exception:
        main_call_input = None


def main_call_state():
    if main_call_input is None:
        return None, "24VAC Signal Unknown"
    active = bool(main_call_input.is_pressed)
    return (active, "24VAC Present (Calling)" if active else "No 24VAC (Idle)")


def load_alert_config():
    if not ALERT_CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(ALERT_CONFIG_PATH.read_text())
    except Exception:
        return {}


def save_alert_config(cfg):
    try:
        ALERT_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        ALERT_CONFIG_PATH.write_text(json.dumps(cfg, indent=2) + "\n")
        return True
    except Exception:
        return False


def public_notifications_config():
    cfg = load_alert_config()
    email_cfg = cfg.get("email", {}) if isinstance(cfg, dict) else {}
    return {
        "updated_utc": now_utc_iso(),
        "cooldown_minutes": cfg.get("cooldown_minutes", DEFAULT_ALERT_COOLDOWN_MIN) if isinstance(cfg, dict) else DEFAULT_ALERT_COOLDOWN_MIN,
        "email": {
            "enabled": bool(email_cfg.get("enabled")),
            "to": email_cfg.get("to", []),
            "from": email_cfg.get("from", email_cfg.get("username", "")),
            "smtp_host": email_cfg.get("smtp_host", ""),
            "smtp_port": email_cfg.get("smtp_port", 587),
            "username": email_cfg.get("username", ""),
            "has_password": bool(email_cfg.get("password")),
        },
    }


def log_alert_event(msg):
    try:
        ALERT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ALERT_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(f"{now_utc_iso()} {msg}\n")
    except Exception:
        pass


def send_email_notification(cfg, subject, body):
    email_cfg = cfg.get("email", {}) if isinstance(cfg, dict) else {}
    if not email_cfg.get("enabled"):
        return False

    to_list = email_cfg.get("to", [])
    if isinstance(to_list, str):
        to_list = [to_list]
    if not to_list:
        return False

    host = email_cfg.get("smtp_host")
    port = int(email_cfg.get("smtp_port", 587))
    username = email_cfg.get("username")
    password = email_cfg.get("password")
    from_addr = email_cfg.get("from", username)
    use_tls = bool(email_cfg.get("use_tls", True))
    if not host or not from_addr:
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port, timeout=10) as server:
            if use_tls:
                server.starttls()
            if username and password:
                server.login(username, password)
            server.send_message(msg)
        return True
    except Exception as e:
        log_alert_event(f"email_send_failed: {e}")
        return False


def send_sms_notification(cfg, body):
    sms_cfg = cfg.get("sms", {}) if isinstance(cfg, dict) else {}
    if not sms_cfg.get("enabled"):
        return False

    provider = (sms_cfg.get("provider") or "").lower()
    if provider != "twilio":
        return False

    sid = sms_cfg.get("account_sid")
    token = sms_cfg.get("auth_token")
    from_num = sms_cfg.get("from")
    to_list = sms_cfg.get("to", [])
    if isinstance(to_list, str):
        to_list = [to_list]

    if not sid or not token or not from_num or not to_list:
        return False

    any_sent = False
    for to_num in to_list:
        try:
            data = urlencode({"From": from_num, "To": to_num, "Body": body}).encode("utf-8")
            req = Request(
                f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json",
                data=data,
                method="POST",
                headers={"Authorization": "Basic " + __import__("base64").b64encode(f"{sid}:{token}".encode()).decode()},
            )
            with urlopen(req, timeout=10):
                any_sent = True
        except Exception as e:
            log_alert_event(f"sms_send_failed({to_num}): {e}")
    return any_sent


def evaluate_zone1_main_status(readings, call_active, threshold_f=ZONE1_DELTA_THRESHOLD_F):
    feed = readings.get("feed_1", {}).get("temp_f") if isinstance(readings, dict) else None
    ret = readings.get("return_1", {}).get("temp_f") if isinstance(readings, dict) else None

    if call_active is True:
        if feed is None or ret is None:
            return "Notify Admin", "Call active, missing feed/return temperature", True
        delta = abs(float(feed) - float(ret))
        if delta <= threshold_f:
            return "Calling, Normal", f"delta {delta:.1f}F <= {threshold_f:.1f}F", False
        return "Notify Admin", f"delta {delta:.1f}F > {threshold_f:.1f}F", True

    if call_active is False:
        return "Idle", "No 24VAC", False

    return "Signal Unknown", "24VAC signal unavailable", False


def update_main_call_timing(call_active):
    global main_call_prev_active, main_call_active_since_utc
    now = now_utc()
    with main_call_state_lock:
        if call_active is True:
            if main_call_prev_active is not True:
                main_call_active_since_utc = now
        else:
            main_call_active_since_utc = None
        main_call_prev_active = call_active


def zone1_call_grace_remaining_seconds(call_active):
    if call_active is not True:
        return 0
    with main_call_state_lock:
        started = main_call_active_since_utc
    if started is None:
        return ZONE1_CALL_ALERT_GRACE_SECONDS
    elapsed = (now_utc() - started).total_seconds()
    return max(0, int(ZONE1_CALL_ALERT_GRACE_SECONDS - elapsed))


def zone1_alert_fault_key(zone1_status, detail):
    if zone1_status != "Notify Admin":
        return zone1_status
    d = (detail or "").lower()
    if "missing" in d:
        return "Notify Admin:temp-missing"
    if "delta" in d:
        return "Notify Admin:delta-out-of-range"
    return "Notify Admin:generic"


def maybe_send_admin_alert(zone1_status, detail, readings, call_status, call_active=None):
    global last_alert_sent_at, last_alert_key

    if zone1_status != "Notify Admin":
        return

    grace_remaining = zone1_call_grace_remaining_seconds(call_active)
    if call_active is True and grace_remaining > 0:
        log_alert_event(
            f"alert_suppressed grace_period remaining_sec={grace_remaining} status={zone1_status} detail={detail}"
        )
        return

    cfg = load_alert_config()
    cooldown_min = cfg.get("cooldown_minutes", DEFAULT_ALERT_COOLDOWN_MIN) if isinstance(cfg, dict) else DEFAULT_ALERT_COOLDOWN_MIN
    try:
        cooldown_sec = max(60, int(cooldown_min) * 60)
    except Exception:
        cooldown_sec = DEFAULT_ALERT_COOLDOWN_MIN * 60

    now = now_utc()
    alert_key = zone1_alert_fault_key(zone1_status, detail)
    if last_alert_sent_at is not None and last_alert_key == alert_key:
        if (now - last_alert_sent_at).total_seconds() < cooldown_sec:
            return

    feed = readings.get("feed_1", {}).get("temp_f") if isinstance(readings, dict) else None
    ret = readings.get("return_1", {}).get("temp_f") if isinstance(readings, dict) else None
    subject = "165 Water Street Heating Hub - Notify Admin"
    body = (
        f"165 Water Street Heating Hub Alert\n\n"
        f"Zone: Zone 1 (Main)\n"
        f"Status: {zone1_status}\n"
        f"Call Status: {call_status}\n"
        f"Feed: {feed} F\n"
        f"Return: {ret} F\n"
        f"Error Description: {detail}\n"
        f"Threshold: +/- {ZONE1_DELTA_THRESHOLD_F:.1f}F\n"
        f"Time: {now_utc_iso()}\n\n"
        f"Live Hub: {PUBLIC_HUB_URL}\n"
    )

    sent_email = send_email_notification(cfg, subject, body)
    sent_sms = send_sms_notification(cfg, body)
    log_alert_event(f"alert_dispatched email={sent_email} sms={sent_sms} key={alert_key} detail={detail}")

    last_alert_sent_at = now
    last_alert_key = alert_key


def now_utc():
    return datetime.now(timezone.utc)


def now_utc_iso():
    return now_utc().isoformat()


def set_local_cache(local):
    global local_cache, local_cache_updated_utc
    with local_cache_lock:
        local_cache = dict(local) if isinstance(local, dict) else local
        local_cache_updated_utc = now_utc_iso()


def get_local_cached():
    with local_cache_lock:
        return dict(local_cache) if isinstance(local_cache, dict) else None


def local_payload_placeholder():
    readings = {}
    for sid, name in LOCAL_SENSORS.items():
        readings[name] = {"sensor_id": sid, "temp_f": None, "error": "pending"}
    call_active, call_status = main_call_state()
    return {
        "readings": readings,
        "main_call_24vac": call_active,
        "main_call_status": call_status,
        "zone1_status_label": "Initializing",
        "zone1_status_detail": "Waiting for first sensor sample",
        "zone1_delta_threshold_f": ZONE1_DELTA_THRESHOLD_F,
        "updated_utc": now_utc_iso(),
    }


def read_temp_f(sensor_id):
    f = W1_BASE / sensor_id / "w1_slave"
    if not f.exists():
        return None, "missing"
    lines = f.read_text().strip().splitlines()
    if len(lines) < 2 or not lines[0].endswith("YES"):
        return None, "crc"
    try:
        c = float(lines[1].split("t=")[-1]) / 1000.0
        return (c * 9.0 / 5.0 + 32.0), None
    except Exception:
        return None, "parse"


def local_payload():
    out = {}
    for sid, name in LOCAL_SENSORS.items():
        t, e = read_temp_f(sid)
        out[name] = {
            "sensor_id": sid,
            "temp_f": round(t, 2) if t is not None else None,
            "error": e,
        }
    call_active, call_status = main_call_state()
    update_main_call_timing(call_active)
    zone1_status_label, zone1_status_detail, _ = evaluate_zone1_main_status(out, call_active)
    return {"readings": out, "main_call_24vac": call_active, "main_call_status": call_status, "zone1_status_label": zone1_status_label, "zone1_status_detail": zone1_status_detail, "zone1_delta_threshold_f": ZONE1_DELTA_THRESHOLD_F, "updated_utc": now_utc_iso()}


def trigger_zone1_alert_from_local(local):
    try:
        if not isinstance(local, dict):
            return
        status = local.get("zone1_status_label")
        detail = local.get("zone1_status_detail")
        readings = local.get("readings", {})
        call_status = local.get("main_call_status")
        call_active = local.get("main_call_24vac")
        t = threading.Thread(
            target=maybe_send_admin_alert,
            args=(status, detail, readings, call_status, call_active),
            daemon=True,
        )
        t.start()
    except Exception:
        pass


def ensure_config():
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, indent=2) + "\n")


def load_config():
    ensure_config()
    try:
        data = json.loads(CONFIG_PATH.read_text())
        zones = data.get("zones", [])
        if isinstance(zones, list):
            return zones
    except Exception:
        pass
    return []


def as_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def as_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        x = v.strip().lower()
        if x in {"1", "true", "yes", "on", "open", "call"}:
            return True
        if x in {"0", "false", "no", "off", "closed", "idle"}:
            return False
    return None


def fetch_remote_json(url):
    try:
        with urlopen(url, timeout=2.5) as r:
            return json.loads(r.read().decode("utf-8")), None
    except (HTTPError, URLError, TimeoutError, OSError) as e:
        return None, str(e)
    except Exception as e:
        return None, f"bad_json: {e}"


def parse_remote_payload(obj):
    data = obj
    if isinstance(obj, dict):
        if isinstance(obj.get("data"), dict):
            data = obj["data"]
        elif isinstance(obj.get("readings"), dict):
            data = obj["readings"]

    if not isinstance(data, dict):
        return None, None, None, None

    feed = as_float(data.get("feed_f", data.get("feed_temp_f", data.get("feed"))))
    ret = as_float(data.get("return_f", data.get("return_temp_f", data.get("return"))))
    call = as_bool(data.get("heating_call", data.get("call_24vac", data.get("call"))))
    updated = data.get("updated_utc") or (obj.get("updated_utc") if isinstance(obj, dict) else None)
    return feed, ret, call, updated


def call_state_label(heating_call):
    if heating_call is True:
        return "24VAC Present (Calling)"
    if heating_call is False:
        return "No 24VAC (Idle)"
    return "24VAC Signal Unknown"


def evaluate_status(feed_f, return_f, heating_call, delta_ok_f):
    if heating_call is True:
        if feed_f is None or return_f is None:
            return "trouble", "Call Active - Temp Data Missing", "verify probe readings"
        delta = abs(feed_f - return_f)
        if delta <= delta_ok_f:
            return "normal", "Calling Active", f"delta {delta:.1f}F (within {delta_ok_f:.1f}F)"
        return "trouble", "Call Active - No Temperature Response", f"delta {delta:.1f}F exceeds {delta_ok_f:.1f}F"
    if heating_call is False:
        return "standby", "Idle", "no heat call"
    return "trouble", "Signal Unknown", "24VAC call state unavailable"


def downstream_payload():
    zones = []
    for z in load_config():
        zone_id = str(z.get("id", "unknown"))
        name = str(z.get("name", zone_id))
        parent_zone = str(z.get("parent_zone", "Unknown"))
        source_url = str(z.get("source_url", ""))
        delta_ok_f = as_float(z.get("delta_ok_f"))
        if delta_ok_f is None:
            delta_ok_f = 8.0

        item = {
            "id": zone_id,
            "name": name,
            "parent_zone": parent_zone,
            "source_url": source_url,
            "delta_ok_f": delta_ok_f,
            "feed_f": None,
            "return_f": None,
            "heating_call": None,
            "call_status": "24VAC Signal Unknown",
            "status": "trouble",
            "zone_status": "Signal Unknown",
            "status_note": "no source configured",
            "updated_utc": None,
        }

        if source_url:
            raw, err = fetch_remote_json(source_url)
            if err:
                item["status"] = "trouble"
                item["call_status"] = "24VAC Signal Unknown"
                item["zone_status"] = "Source Unreachable"
                item["status_note"] = f"source error: {err}"
            else:
                feed_f, return_f, heating_call, updated = parse_remote_payload(raw)
                status, zone_status, note = evaluate_status(feed_f, return_f, heating_call, delta_ok_f)
                item.update(
                    {
                        "feed_f": feed_f,
                        "return_f": return_f,
                        "heating_call": heating_call,
                        "call_status": call_state_label(heating_call),
                        "status": status,
                        "zone_status": zone_status,
                        "status_note": note,
                        "updated_utc": updated,
                    }
                )

        zones.append(item)
    return zones


def refresh_downstream_cache():
    global downstream_cache, downstream_cache_updated_utc
    zones = downstream_payload()
    with downstream_cache_lock:
        downstream_cache = zones
        downstream_cache_updated_utc = now_utc_iso()
    return zones


def get_downstream_cached():
    with downstream_cache_lock:
        return list(downstream_cache)


def make_sample(local=None, downstream=None):
    if local is None:
        local = get_local_cached() or local_payload_placeholder()
    main = local.get("readings", {})
    if downstream is None:
        downstream = get_downstream_cached()
    downstream_map = {}
    for z in downstream:
        downstream_map[z.get("id", "unknown")] = {
            "feed_f": z.get("feed_f"),
            "return_f": z.get("return_f"),
        }
    return {
        "ts": now_utc_iso(),
        "main": {
            "feed_1": main.get("feed_1", {}).get("temp_f"),
            "return_1": main.get("return_1", {}).get("temp_f"),
            "feed_2": main.get("feed_2", {}).get("temp_f"),
            "return_2": main.get("return_2", {}).get("temp_f"),
        },
        "downstream": downstream_map,
    }


def append_sample(sample):
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(sample) + "\n")


def prune_history():
    if not HISTORY_FILE.exists():
        return
    cutoff = now_utc() - timedelta(days=RETENTION_DAYS)
    keep = []
    with HISTORY_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                ts = datetime.fromisoformat(obj.get("ts", ""))
            except Exception:
                continue
            if ts >= cutoff:
                keep.append(obj)
    with HISTORY_FILE.open("w", encoding="utf-8") as f:
        for obj in keep:
            f.write(json.dumps(obj) + "\n")


def history_for_window(window_key):
    windows = {
        "1h": timedelta(hours=1),
        "6h": timedelta(hours=6),
        "12h": timedelta(hours=12),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
    }
    window = windows.get(window_key, windows["24h"])
    cutoff = now_utc() - window
    out = []
    if not HISTORY_FILE.exists():
        return out
    with HISTORY_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                ts = datetime.fromisoformat(obj.get("ts", ""))
            except Exception:
                continue
            if ts >= cutoff:
                out.append(obj)
    return out


def sample_loop():
    loops = 0
    while not stop_event.is_set():
        try:
            local = local_payload()
            set_local_cache(local)
            trigger_zone1_alert_from_local(local)
            try:
                downstream = refresh_downstream_cache()
            except Exception:
                downstream = get_downstream_cached()
            loops += 1
            if loops % HISTORY_SAMPLE_EVERY_LOOPS == 0:
                append_sample(make_sample(local=local, downstream=downstream))
            if loops % 60 == 0:
                prune_history()
        except Exception:
            pass
        stop_event.wait(SAMPLE_INTERVAL_SECONDS)


def hub_payload():
    local = local_payload()
    downstream = get_downstream_cached()
    trouble_count = sum(1 for z in downstream if z.get("status") == "trouble")
    return {
        "main": local,
        "downstream": downstream,
        "trouble_count": trouble_count,
        "downstream_cache_updated_utc": downstream_cache_updated_utc,
        "updated_utc": now_utc_iso(),
    }


HTML = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>165 Water Street Heating Hub</title>
  <link href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css\" rel=\"stylesheet\">
  <style>
    :root {
      --bg:#edf3f6; --ink:#0f2029; --panel:#ffffff; --muted:#5e7480; --border:#d7e3ea;
      --ok:#0f766e; --warn:#b45309; --bad:#b91c1c; --brand:#0f4c5c; --brand2:#168aad;
    }
    * { box-sizing:border-box; }
    body {
      margin:0; min-height:100vh;
      font-family:Inter, ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      color:var(--ink);
      background:
        radial-gradient(900px 500px at -10% -20%, #d6e9f1 0%, transparent 60%),
        radial-gradient(900px 500px at 120% 120%, #d7ede6 0%, transparent 60%),
        var(--bg);
    }
    .wrap { max-width:1180px; margin:0 auto; padding:1.1rem; }
    .head {
      background:
        radial-gradient(550px 220px at 10% 0%, rgba(255,255,255,.12), transparent 60%),
        linear-gradient(130deg,#0b1320,#123447 55%,#0f766e);
      color:#f2fbff; border-radius:16px; padding:18px 20px;
      border:1px solid rgba(255,255,255,.08);
      box-shadow:0 16px 34px rgba(12,30,41,.22);
      display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;
    }
    h1 { margin:0; font-size:clamp(1.55rem,2.8vw,2.2rem); }
    .meta { opacity:.9; font-size:.92rem; margin-top:4px; }
    .pill { border:1px solid rgba(255,255,255,.26); border-radius:999px; padding:7px 12px; font-weight:700; background:rgba(255,255,255,.1); backdrop-filter: blur(4px); }

    .main-grid { margin-top:16px; display:grid; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); gap:14px; }
    .card { background:linear-gradient(180deg,#ffffff,#fbfdff); border:1px solid var(--border); border-radius:16px; padding:14px; box-shadow:0 10px 24px rgba(10,31,44,.08); }
    .card h2 { margin:0 0 10px; font-size:1.16rem; }
    .row { display:grid; grid-template-columns:1fr auto; gap:8px; align-items:center; border-top:1px solid var(--border); padding:10px 0; }
    .row:first-of-type { border-top:0; }
    .label { color:var(--muted); font-weight:700; }
    .sid { font-size:.78rem; color:var(--muted); margin-top:2px; }
    .temp { font-size:1.7rem; font-weight:800; letter-spacing:-0.02em; }
    .unit { color:var(--muted); margin-left:5px; }

    .chart-wrap { margin-top:18px; background:linear-gradient(180deg,#ffffff,#fbfdff); border:1px solid var(--border); border-radius:16px; box-shadow:0 10px 24px rgba(10,31,44,.08); padding:14px; }
    .chart-head { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px; }
    .btns,.tabs { display:flex; gap:8px; flex-wrap:wrap; }
    .btn,.tab {
      border:1px solid var(--border); border-radius:999px; padding:6px 12px; font-weight:700; background:#fff; color:#1f3340; cursor:pointer;
      box-shadow:0 2px 8px rgba(10,31,44,.04);
    }
    .btn.active,.tab.active { background:linear-gradient(120deg,var(--brand),var(--brand2)); color:#fff; border-color:transparent; }
    canvas { width:100%; height:300px; margin-top:12px; border:1px solid var(--border); border-radius:10px; background:#fbfeff; }
    .legend { display:flex; gap:12px; flex-wrap:wrap; margin-top:8px; font-size:.88rem; color:var(--muted); }
    .dot { display:inline-block; width:10px; height:10px; border-radius:50%; margin-right:5px; }

    .subzones { margin-top:18px; background:linear-gradient(180deg,#ffffff,#fbfdff); border:1px solid var(--border); border-radius:16px; box-shadow:0 10px 24px rgba(10,31,44,.08); overflow:hidden; }
    .subzones-head { padding:12px 14px; border-bottom:1px solid var(--border); display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px; }
    .subzones-head h3 { margin:0; font-size:1.08rem; }
    table { width:100%; border-collapse:collapse; font-size:.93rem; }
    th,td { padding:10px; border-bottom:1px solid var(--border); text-align:left; vertical-align:top; }
    th { color:var(--muted); font-weight:700; background:#f8fbfd; }
    tr:last-child td { border-bottom:0; }
    .status { font-weight:700; }
    .st-normal { color:var(--ok); }
    .st-standby { color:var(--warn); }
    .st-trouble { color:var(--bad); }

    .status-chip{display:inline-flex;align-items:center;gap:.4rem;padding:.2rem .55rem;border-radius:999px;background:#f3f8fb;border:1px solid var(--border)}
    .form-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .form-grid .full{grid-column:1 / -1}
    .input,.select{width:100%;padding:10px 12px;border:1px solid var(--border);border-radius:10px;background:#fff}
    .input:focus,.select:focus{outline:none;border-color:#8bc2d3;box-shadow:0 0 0 4px rgba(22,138,173,.12)}
    .smallnote{font-size:.82rem;color:var(--muted)}
    .msg{margin-top:8px;font-weight:700;color:#114b5f;white-space:pre-wrap}
    @media (max-width:760px) {
      .wrap { padding:.75rem; }
      .head { padding:14px; border-radius:14px; }
      .temp { font-size:1.35rem; }
      .form-grid{grid-template-columns:1fr}
      table,thead,tbody,th,td,tr { display:block; }
      thead { display:none; }
      tr { border-bottom:1px solid var(--border); padding:8px 10px; }
      td { border:0; padding:4px 0; }
      td::before { content:attr(data-k) ": "; color:var(--muted); font-weight:700; }
    }
  </style>
</head>
<body class=\"text-body\">
  <div class=\"wrap\">
    <header class=\"head\">
      <div>
        <h1>165 Water Street Heating Hub</h1>
        <div class=\"meta\">Main boiler zones + downstream sub-zones</div>
      </div>
      <div id=\"alertPill\" class=\"pill\">Trouble: --</div>
    </header>

    <section class=\"main-grid\">
      <article class=\"card\">
        <h2>Zone 1 (Main)</h2>
        <div class=\"row\"><div><div class=\"label\">Feed</div><div id=\"feed_1_sid\" class=\"sid\">--</div></div><div><span id=\"feed_1\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Return</div><div id=\"return_1_sid\" class=\"sid\">--</div></div><div><span id=\"return_1\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Call Status</div><div class=\"sid\">GPIO17 Dry Contact</div></div><div><span id=\"zone1_call_status\" class=\"temp\" style=\"font-size:1.0rem;\">--</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Zone 1 Status</div><div class=\"sid\">Call + feed/return check</div></div><div><span id=\"zone1_status\" class=\"temp\" style=\"font-size:1.0rem;\">--</span></div></div>
      </article>
      <article class=\"card\">
        <h2>Zone 2 (Main)</h2>
        <div class=\"row\"><div><div class=\"label\">Feed</div><div id=\"feed_2_sid\" class=\"sid\">--</div></div><div><span id=\"feed_2\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Return</div><div id=\"return_2_sid\" class=\"sid\">--</div></div><div><span id=\"return_2\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
      </article>
    </section>

    <section class=\"chart-wrap\">
      <div class=\"chart-head\">
        <h3 id=\"graphTitle\">Zone Graph</h3>
        <div class=\"btns\">
          <button class=\"btn active\" data-range=\"live\">LIVE</button>
          <button class=\"btn\" data-range=\"1h\">1H</button>
          <button class=\"btn\" data-range=\"6h\">6H</button>
          <button class=\"btn\" data-range=\"12h\">12H</button>
          <button class=\"btn\" data-range=\"24h\">24H</button>
          <button class=\"btn\" data-range=\"7d\">7D</button>
          <button class=\"btn\" data-range=\"30d\">30D</button>
        </div>
      </div>
      <div id=\"zoneTabs\" class=\"tabs\" style=\"margin-top:10px\"></div>
      <canvas id=\"historyChart\" width=\"1040\" height=\"320\"></canvas>
      <div class=\"legend\">
        <span><span class=\"dot\" style=\"background:#0f766e\"></span>Feed</span>
        <span><span class=\"dot\" style=\"background:#ef4444\"></span>Return</span>
      </div>
    </section>

    <section class=\"subzones\">
      <div class=\"subzones-head\">
        <h3>Downstream Sub-Zones</h3>
        <div id=\"updated\" class=\"meta\">Updated: --</div>
      </div>
      <div class=\"table-responsive\">
      <table class=\"table table-hover align-middle mb-0\">
        <thead>
          <tr><th>Parent</th><th>Sub-Zone</th><th>Heat Call (24VAC)</th><th>Feed</th><th>Return</th><th>Status</th><th>Note</th></tr>
        </thead>
        <tbody id=\"subzoneBody\"></tbody>
      </table>
      </div>
    </section>

    <section class=\"chart-wrap\">
      <div class=\"chart-head\">
        <h3>Notifications / Account</h3>
        <div class=\"smallnote\">Configure alert recipients on the hub (shared by all zone devices)</div>
      </div>
      <form id=\"notifForm\" class=\"form-grid\" style=\"margin-top:10px\">
        <div class=\"full\">
          <label class=\"label\" for=\"notifEmails\">Alert Email Recipients</label>
          <input id=\"notifEmails\" class=\"input\" placeholder=\"eamon@easternstandard.co, manager@example.com\">
          <div class=\"smallnote\">Comma-separated email addresses</div>
        </div>
        <div>
          <label class=\"label\" for=\"notifCooldown\">Repeat Alert Cooldown (minutes)</label>
          <input id=\"notifCooldown\" class=\"input\" type=\"number\" min=\"1\" step=\"1\" value=\"120\">
        </div>
        <div>
          <label class=\"label\" for=\"notifEmailEnabled\">Email Alerts</label>
          <select id=\"notifEmailEnabled\" class=\"select\">
            <option value=\"true\">Enabled</option>
            <option value=\"false\">Disabled</option>
          </select>
        </div>
        <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
          <button type=\"button\" class=\"btn active\" id=\"saveNotifBtn\">Save Notification Settings</button>
          <button type=\"button\" class=\"btn\" id=\"testNotifBtn\">Send Test Email</button>
        </div>
      </form>
      <div id=\"notifMsg\" class=\"msg\"></div>
    </section>
  </div>

  <script>
    let activeRange = "live";
    let currentTabId = "zone1_main";
    let tabDefs = [];
    let historyPoints = [];
    const livePoints = [];
    const LIVE_MAX_POINTS = 240;

    function fmtTemp(v) {
      if (v === null || v === undefined) return "--";
      return Number(v).toFixed(1) + " °F";
    }

    function setMain(name, obj) {
      const t = document.getElementById(name);
      const sid = document.getElementById(name + "_sid");
      if (!t || !sid) return;
      sid.textContent = obj && obj.sensor_id ? obj.sensor_id : "--";
      t.textContent = obj && obj.temp_f !== null && obj.temp_f !== undefined ? Number(obj.temp_f).toFixed(1) : "--";
    }

    function setZone1CallStatus(label) {
      const el = document.getElementById("zone1_call_status");
      if (!el) return;
      el.textContent = label || "24VAC Signal Unknown";
    }

    function setZone1StatusLabel(label) {
      const el = document.getElementById("zone1_status");
      if (!el) return;
      el.textContent = label || "--";
    }

    function evaluateZone1Status(readings, callActive) {
      const feed = readings && readings.feed_1 ? readings.feed_1.temp_f : null;
      const ret = readings && readings.return_1 ? readings.return_1.temp_f : null;
      const deltaOkF = 10.0;

      if (callActive === true) {
        if (feed === null || feed === undefined || ret === null || ret === undefined) {
          return "Calling, Trouble";
        }
        const delta = Math.abs(Number(feed) - Number(ret));
        if (delta <= deltaOkF) {
          return "Calling, Normal";
        }
        return "Calling, Alert";
      }
      if (callActive === false) {
        return "Idle";
      }
      return "Signal Unknown";
    }

    function statusClass(status) {
      if (status === "normal") return "st-normal";
      if (status === "standby") return "st-standby";
      return "st-trouble";
    }

    function renderRows(rows) {
      const body = document.getElementById("subzoneBody");
      body.innerHTML = "";
      if (!rows || rows.length === 0) {
        const tr = document.createElement("tr");
        tr.innerHTML = '<td colspan="7">No downstream zones configured yet. Edit <code>/home/eamondoherty618/downstream_zones.json</code>.</td>';
        body.appendChild(tr);
        return;
      }
      for (const z of rows) {
        const tr = document.createElement("tr");
        const call = z.call_status || (z.heating_call === true ? "24VAC Present (Calling)" : (z.heating_call === false ? "No 24VAC (Idle)" : "24VAC Signal Unknown"));
        tr.innerHTML = `
          <td data-k="Parent">${z.parent_zone || "--"}</td>
          <td data-k="Sub-Zone"><strong>${z.name || "--"}</strong><br><span class="sid">${z.id || ""}</span></td>
          <td data-k="Heat Call">${call}</td>
          <td data-k="Feed">${fmtTemp(z.feed_f)}</td>
          <td data-k="Return">${fmtTemp(z.return_f)}</td>
          <td data-k="Status" class="status ${statusClass(z.status)}">${z.zone_status || z.status || "--"}</td>
          <td data-k="Note">${z.status_note || ""}</td>
        `;
        body.appendChild(tr);
      }
    }

    function updateTabs(downstreamRows) {
      const defs = [
        { id: "zone1_main", label: "Zone 1 (Main)", type: "main", key: "zone1" },
        { id: "zone2_main", label: "Zone 2 (Main)", type: "main", key: "zone2" },
      ];
      (downstreamRows || []).forEach((z) => {
        defs.push({ id: "ds_" + z.id, label: z.name || z.id, type: "downstream", zoneId: z.id });
      });
      tabDefs = defs;
      if (!tabDefs.some((t) => t.id === currentTabId)) currentTabId = "zone1_main";

      const tabWrap = document.getElementById("zoneTabs");
      tabWrap.innerHTML = "";
      for (const t of tabDefs) {
        const b = document.createElement("button");
        b.className = "tab" + (t.id === currentTabId ? " active" : "");
        b.textContent = t.label;
        b.addEventListener("click", () => {
          currentTabId = t.id;
          updateTabs(downstreamRows);
          renderSelectedGraph();
        });
        tabWrap.appendChild(b);
      }
      const current = tabDefs.find((t) => t.id === currentTabId);
      document.getElementById("graphTitle").textContent = (current ? current.label : "Zone") + " Graph";
    }

    function appendLivePoint(mainReadings, downstreamRows) {
      const p = {
        ts: new Date().toISOString(),
        main: {
          feed_1: mainReadings.feed_1 && mainReadings.feed_1.temp_f !== undefined ? mainReadings.feed_1.temp_f : null,
          return_1: mainReadings.return_1 && mainReadings.return_1.temp_f !== undefined ? mainReadings.return_1.temp_f : null,
          feed_2: mainReadings.feed_2 && mainReadings.feed_2.temp_f !== undefined ? mainReadings.feed_2.temp_f : null,
          return_2: mainReadings.return_2 && mainReadings.return_2.temp_f !== undefined ? mainReadings.return_2.temp_f : null,
        },
        downstream: {},
      };
      (downstreamRows || []).forEach((z) => {
        p.downstream[z.id] = { feed_f: z.feed_f, return_f: z.return_f };
      });
      livePoints.push(p);
      if (livePoints.length > LIVE_MAX_POINTS) {
        livePoints.splice(0, livePoints.length - LIVE_MAX_POINTS);
      }
    }

    function seriesFromSample(sample, tabId) {
      const out = { feed: null, ret: null };
      if (!sample) return out;

      if (tabId === "zone1_main") {
        if (sample.main) {
          out.feed = sample.main.feed_1;
          out.ret = sample.main.return_1;
        } else {
          out.feed = sample.feed_1;
          out.ret = sample.return_1;
        }
        return out;
      }

      if (tabId === "zone2_main") {
        if (sample.main) {
          out.feed = sample.main.feed_2;
          out.ret = sample.main.return_2;
        } else {
          out.feed = sample.feed_2;
          out.ret = sample.return_2;
        }
        return out;
      }

      if (tabId.startsWith("ds_")) {
        const zoneId = tabId.slice(3);
        if (sample.downstream && sample.downstream[zoneId]) {
          out.feed = sample.downstream[zoneId].feed_f;
          out.ret = sample.downstream[zoneId].return_f;
        }
      }

      return out;
    }

    function drawHistory(points, tabId) {
      const canvas = document.getElementById("historyChart");
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#fbfeff";
      ctx.fillRect(0, 0, w, h);

      if (!points || points.length < 2) {
        ctx.fillStyle = "#536b75";
        ctx.font = "16px sans-serif";
        ctx.fillText("Not enough data yet.", 20, 30);
        return;
      }

      const transformed = points.map((p) => {
        const s = seriesFromSample(p, tabId);
        return { ts: p.ts, feed: s.feed, ret: s.ret };
      });

      const vals = [];
      transformed.forEach((p) => {
        if (p.feed !== null && p.feed !== undefined) vals.push(Number(p.feed));
        if (p.ret !== null && p.ret !== undefined) vals.push(Number(p.ret));
      });
      if (vals.length === 0) {
        ctx.fillStyle = "#536b75";
        ctx.font = "16px sans-serif";
        ctx.fillText("No values for selected zone yet.", 20, 30);
        return;
      }

      let minV = Math.min(...vals);
      let maxV = Math.max(...vals);
      if (maxV - minV < 5) { minV -= 2.5; maxV += 2.5; }

      const padL = 48, padR = 14, padT = 12, padB = 26;
      const plotW = w - padL - padR;
      const plotH = h - padT - padB;

      ctx.strokeStyle = "#d6e2e8";
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i++) {
        const y = padT + (plotH * i / 4);
        ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
      }

      ctx.fillStyle = "#536b75";
      ctx.font = "12px sans-serif";
      for (let i = 0; i <= 4; i++) {
        const y = padT + (plotH * i / 4);
        const v = (maxV - (maxV - minV) * i / 4).toFixed(0);
        ctx.fillText(v, 8, y + 4);
      }

      const n = transformed.length;
      const xAt = (idx) => padL + (plotW * idx / (n - 1));
      const yAt = (v) => padT + (maxV - v) * plotH / (maxV - minV);

      const lines = [
        { key: "feed", color: "#0f766e" },
        { key: "ret", color: "#ef4444" },
      ];

      for (const line of lines) {
        ctx.beginPath();
        ctx.strokeStyle = line.color;
        ctx.lineWidth = 2;
        let started = false;
        for (let i = 0; i < n; i++) {
          const v = transformed[i][line.key];
          if (v === null || v === undefined) continue;
          const x = xAt(i);
          const y = yAt(Number(v));
          if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); }
        }
        ctx.stroke();
      }
    }

    function renderSelectedGraph() {
      if (activeRange === "live") {
        drawHistory(livePoints, currentTabId);
      } else {
        drawHistory(historyPoints, currentTabId);
      }
    }

    async function refreshHistory() {
      if (activeRange === "live") {
        renderSelectedGraph();
        return;
      }
      try {
        const res = await fetch(`/api/history?range=${activeRange}&_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        historyPoints = data.points || [];
        renderSelectedGraph();
      } catch (e) {
        historyPoints = [];
        renderSelectedGraph();
      }
    }

    async function refreshMain() {
      try {
        const res = await fetch(`/api/temps?_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        const r = data.readings || {};

        setMain("feed_1", r.feed_1 || {});
        setMain("return_1", r.return_1 || {});
        setMain("feed_2", r.feed_2 || {});
        setMain("return_2", r.return_2 || {});

        const zone1CallActive = (data.main_call_24vac !== undefined) ? data.main_call_24vac : null;
        setZone1CallStatus(data.main_call_status || "24VAC Signal Unknown");
        const zone1StatusFromApi = data.zone1_status_label || null;
        setZone1StatusLabel(zone1StatusFromApi || evaluateZone1Status(r, zone1CallActive));

        // Keep live graph moving even when /api/hub is slow.
        appendLivePoint(r, []);
        if (activeRange === "live") renderSelectedGraph();
      } catch (e) {
        // leave last values in place
      }
    }

    async function refreshHub() {
      try {
        const res = await fetch(`/api/hub?_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        const r = (data.main && data.main.readings) ? data.main.readings : {};
        const downstream = data.downstream || [];

        // Main readings are updated by refreshMain() so they remain visible even if hub polling is slow.
        appendLivePoint(r, downstream);
        updateTabs(downstream);
        renderRows(downstream);
        if (activeRange === "live") renderSelectedGraph();

        const trouble = Number(data.trouble_count || 0);
        const pill = document.getElementById("alertPill");
        pill.textContent = `Trouble: ${trouble}`;
        pill.style.borderColor = trouble > 0 ? "rgba(255,130,130,.8)" : "rgba(180,255,210,.8)";

        document.getElementById("updated").textContent = "Updated: " + (data.updated_utc || new Date().toISOString());
      } catch (e) {
        document.getElementById("updated").textContent = "Updated: connection lost";
      }
    }

    function initButtons() {
      document.querySelectorAll(".btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          activeRange = btn.dataset.range;
          document.querySelectorAll(".btn").forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          refreshHistory();
        });
      });
    }

    function setNotifMsg(msg) {
      const el = document.getElementById("notifMsg");
      if (el) el.textContent = msg || "";
    }

    async function loadNotifications() {
      try {
        const res = await fetch(`/api/notifications?_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        const email = data.email || {};
        document.getElementById("notifEmails").value = Array.isArray(email.to) ? email.to.join(", ") : (email.to || "");
        document.getElementById("notifCooldown").value = Number(data.cooldown_minutes || 120);
        document.getElementById("notifEmailEnabled").value = email.enabled ? "true" : "false";
      } catch (e) {
        setNotifMsg("Failed to load notification settings.");
      }
    }

    async function saveNotifications() {
      const emails = document.getElementById("notifEmails").value
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      const cooldown = Number(document.getElementById("notifCooldown").value || 120);
      const emailEnabled = document.getElementById("notifEmailEnabled").value === "true";
      setNotifMsg("Saving notification settings...");
      try {
        const res = await fetch("/api/notifications", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            cooldown_minutes: cooldown,
            email: { enabled: emailEnabled, to: emails }
          })
        });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "save failed");
        setNotifMsg("Notification settings saved.");
        await loadNotifications();
      } catch (e) {
        setNotifMsg("Save failed: " + e.message);
      }
    }

    async function testNotifications() {
      setNotifMsg("Sending test email...");
      try {
        const res = await fetch("/api/notifications/test-email", { method: "POST" });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "test failed");
        setNotifMsg(data.message || "Test email sent.");
      } catch (e) {
        setNotifMsg("Test email failed: " + e.message);
      }
    }

    initButtons();
    document.getElementById("saveNotifBtn").addEventListener("click", saveNotifications);
    document.getElementById("testNotifBtn").addEventListener("click", testNotifications);
    refreshMain();
    refreshHub();
    refreshHistory();
    loadNotifications();
    setInterval(refreshMain, 3000);
    setInterval(refreshHub, 10000);
    setInterval(refreshHistory, 60000);
  </script>
</body>
</html>
"""


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        route = parsed.path
        query = parse_qs(parsed.query)

        if route == "/":
            b = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/temps":
            b = json.dumps(get_local_cached() or local_payload_placeholder()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/hub":
            b = json.dumps(hub_payload()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/history":
            window = query.get("range", ["24h"])[0]
            points = history_for_window(window)
            b = json.dumps({"range": window, "points": points, "updated_utc": now_utc_iso()}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/notifications":
            b = json.dumps(public_notifications_config()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/discovery":
            b = json.dumps({
                "service": "heating_hub",
                "name": HUB_SITE_NAME,
                "public_url": PUBLIC_HUB_URL,
                "hub_api": "/api/hub",
                "version": 1,
                "updated_utc": now_utc_iso(),
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        route = parsed.path

        if route not in ("/api/notifications", "/api/notifications/test-email"):
            self.send_response(404)
            self.end_headers()
            return

        if route == "/api/notifications/test-email":
            cfg = load_alert_config()
            subject = "165 Water Street Heating Hub - Test Email"
            body = (
                "This is a test email from the 165 Water Street Heating Hub.\n\n"
                f"Time: {now_utc_iso()}\n"
                f"Live Hub: {PUBLIC_HUB_URL}\n"
            )
            sent = send_email_notification(cfg, subject, body)
            b = json.dumps({
                "ok": bool(sent),
                "message": "Test email sent." if sent else "Email test failed. Check SMTP settings on the hub."
            }).encode("utf-8")
            self.send_response(200 if sent else 500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        try:
            n = int(self.headers.get("Content-Length", "0"))
        except Exception:
            n = 0
        raw = self.rfile.read(n) if n > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            b = json.dumps({"error": "invalid json"}).encode("utf-8")
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        cfg = load_alert_config()
        if not isinstance(cfg, dict):
            cfg = {}

        try:
            cooldown = int(body.get("cooldown_minutes", cfg.get("cooldown_minutes", DEFAULT_ALERT_COOLDOWN_MIN)))
        except Exception:
            cooldown = DEFAULT_ALERT_COOLDOWN_MIN
        cfg["cooldown_minutes"] = max(1, cooldown)

        email_cfg = cfg.get("email", {})
        if not isinstance(email_cfg, dict):
            email_cfg = {}
        body_email = body.get("email", {})
        if isinstance(body_email, dict):
            if "enabled" in body_email:
                email_cfg["enabled"] = bool(body_email.get("enabled"))
            if "to" in body_email:
                to_list = body_email.get("to", [])
                if isinstance(to_list, str):
                    to_list = [to_list]
                if isinstance(to_list, list):
                    email_cfg["to"] = [str(x).strip() for x in to_list if str(x).strip()]
        cfg["email"] = email_cfg

        if not save_alert_config(cfg):
            b = json.dumps({"error": "failed to save alert config"}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        b = json.dumps({"ok": True, "message": "Notification settings saved", "config": public_notifications_config()}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)


if __name__ == "__main__":
    ensure_config()
    prune_history()
    sampler = threading.Thread(target=sample_loop, daemon=True)
    sampler.start()
    ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()
