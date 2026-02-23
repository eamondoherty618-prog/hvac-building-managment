#!/usr/bin/env python3
import json
import threading
import smtplib
import uuid
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
CALL_CYCLES_FILE = Path("/home/eamondoherty618/hub_call_cycles.jsonl")
SAMPLE_INTERVAL_SECONDS = 10
HISTORY_SAMPLE_EVERY_LOOPS = 6  # 10s sampler -> 60s history points
RETENTION_DAYS = 31
ZONE1_DELTA_THRESHOLD_F = 10.0
SHORT_CYCLE_THRESHOLD_SECONDS = 300
RESPONSE_LAG_ALERT_THRESHOLD_SECONDS = 120
DEVICE_OFFLINE_ALERT_THRESHOLD_SECONDS = 300
ALERT_CONFIG_PATH = Path("/home/eamondoherty618/alert_config.json")
ALERT_LOG_PATH = Path("/home/eamondoherty618/alert_events.log")
USERS_CONFIG_PATH = Path("/home/eamondoherty618/hub_users.json")
ALERT_EVENTS_JSONL_PATH = Path("/home/eamondoherty618/hub_alert_events.jsonl")
SITE_MAP_CONFIG_PATH = Path("/home/eamondoherty618/hub_site_map.json")
DEFAULT_ALERT_COOLDOWN_MIN = 120
ZONE1_CALL_ALERT_GRACE_SECONDS = 120
PUBLIC_HUB_URL = "https://165-boiler.tail58e171.ts.net/"
HUB_SITE_NAME = "165 Water Street Heating Hub"
DEFAULT_ALERT_PREFS = {
    "temp_response": True,
    "short_cycle": True,
    "response_lag": True,
    "no_response": True,
    "low_temp": True,
    "device_offline": True,
    "device_recovered": True,
}

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
cycle_state_lock = threading.Lock()
cycle_trackers = {}
perf_alert_last_sent = {}
downstream_offline_state = {}

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


def default_users_config():
    return {
        "users": [
            {
                "id": "eamon-admin",
                "name": "Eamon",
                "email": "eamon@easternstandard.co",
                "role": "admin",
                "alerts": dict(DEFAULT_ALERT_PREFS),
                "enabled": True,
            }
        ]
    }


def load_users_config():
    if not USERS_CONFIG_PATH.exists():
        cfg = default_users_config()
        save_users_config(cfg)
        return cfg
    try:
        data = json.loads(USERS_CONFIG_PATH.read_text())
        if not isinstance(data, dict):
            raise ValueError("users config not dict")
        users = data.get("users", [])
        if not isinstance(users, list):
            users = []
        norm = []
        for u in users:
            if not isinstance(u, dict):
                continue
            alerts = dict(DEFAULT_ALERT_PREFS)
            if isinstance(u.get("alerts"), dict):
                for k in DEFAULT_ALERT_PREFS:
                    if k in u["alerts"]:
                        alerts[k] = bool(u["alerts"][k])
            norm.append({
                "id": str(u.get("id") or f"user-{uuid.uuid4().hex[:8]}"),
                "name": str(u.get("name") or "").strip(),
                "email": str(u.get("email") or "").strip(),
                "role": str(u.get("role") or "viewer").strip() or "viewer",
                "alerts": alerts,
                "enabled": bool(u.get("enabled", True)),
            })
        return {"users": norm}
    except Exception:
        cfg = default_users_config()
        save_users_config(cfg)
        return cfg


def save_users_config(cfg):
    try:
        USERS_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        USERS_CONFIG_PATH.write_text(json.dumps(cfg, indent=2) + "\n")
        return True
    except Exception:
        return False


def public_users_config():
    cfg = load_users_config()
    return {"users": cfg.get("users", []), "updated_utc": now_utc_iso()}


def default_site_map_config():
    return {
        "address": "",
        "center": {"lat": 41.307, "lng": -72.927},
        "zoom": 16,
        "pins": {},
        "updated_utc": now_utc_iso(),
    }


def load_site_map_config():
    if not SITE_MAP_CONFIG_PATH.exists():
        cfg = default_site_map_config()
        save_site_map_config(cfg)
        return cfg
    try:
        data = json.loads(SITE_MAP_CONFIG_PATH.read_text())
        if not isinstance(data, dict):
            raise ValueError("not a dict")
        cfg = default_site_map_config()
        cfg["address"] = str(data.get("address") or "")
        center = data.get("center") if isinstance(data.get("center"), dict) else {}
        try:
            cfg["center"] = {
                "lat": float(center.get("lat", cfg["center"]["lat"])),
                "lng": float(center.get("lng", cfg["center"]["lng"])),
            }
        except Exception:
            pass
        try:
            cfg["zoom"] = int(data.get("zoom", cfg["zoom"]))
        except Exception:
            pass
        pins_in = data.get("pins", {}) if isinstance(data.get("pins"), dict) else {}
        pins_out = {}
        for zid, p in pins_in.items():
            if not isinstance(p, dict):
                continue
            try:
                lat = float(p.get("lat"))
                lng = float(p.get("lng"))
            except Exception:
                continue
            pins_out[str(zid)] = {
                "lat": lat,
                "lng": lng,
                "label": str(p.get("label") or str(zid)),
                "color": str(p.get("color") or "red"),
                "equipment_type": str(p.get("equipment_type") or ""),
                "room_area_name": str(p.get("room_area_name") or ""),
                "access_instructions": str(p.get("access_instructions") or ""),
                "installed_by": str(p.get("installed_by") or ""),
                "install_date": str(p.get("install_date") or ""),
                "last_service_date": str(p.get("last_service_date") or ""),
                "thermostat_location": str(p.get("thermostat_location") or ""),
                "valve_actuator_type": str(p.get("valve_actuator_type") or ""),
                "circuit_breaker_ref": str(p.get("circuit_breaker_ref") or ""),
                "description": str(p.get("description") or ""),
                "photo_data_url": str(p.get("photo_data_url") or ""),
                "updated_utc": str(p.get("updated_utc") or now_utc_iso()),
            }
        cfg["pins"] = pins_out
        cfg["updated_utc"] = str(data.get("updated_utc") or now_utc_iso())
        return cfg
    except Exception:
        cfg = default_site_map_config()
        save_site_map_config(cfg)
        return cfg


def save_site_map_config(cfg):
    try:
        SITE_MAP_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        cfg_out = default_site_map_config()
        if isinstance(cfg, dict):
            cfg_out["address"] = str(cfg.get("address") or "")
            c = cfg.get("center", {})
            if isinstance(c, dict):
                try:
                    cfg_out["center"] = {"lat": float(c.get("lat")), "lng": float(c.get("lng"))}
                except Exception:
                    pass
            try:
                cfg_out["zoom"] = max(1, min(22, int(cfg.get("zoom", cfg_out["zoom"]))))
            except Exception:
                pass
            pins = cfg.get("pins", {})
            if isinstance(pins, dict):
                cfg_out["pins"] = {}
                for zid, p in pins.items():
                    if not isinstance(p, dict):
                        continue
                    try:
                        lat = float(p.get("lat"))
                        lng = float(p.get("lng"))
                    except Exception:
                        continue
                    cfg_out["pins"][str(zid)] = {
                        "lat": lat,
                        "lng": lng,
                        "label": str(p.get("label") or str(zid)),
                        "color": str(p.get("color") or "red"),
                        "equipment_type": str(p.get("equipment_type") or ""),
                        "room_area_name": str(p.get("room_area_name") or ""),
                        "access_instructions": str(p.get("access_instructions") or ""),
                        "installed_by": str(p.get("installed_by") or ""),
                        "install_date": str(p.get("install_date") or ""),
                        "last_service_date": str(p.get("last_service_date") or ""),
                        "thermostat_location": str(p.get("thermostat_location") or ""),
                        "valve_actuator_type": str(p.get("valve_actuator_type") or ""),
                        "circuit_breaker_ref": str(p.get("circuit_breaker_ref") or ""),
                        "description": str(p.get("description") or ""),
                        "photo_data_url": str(p.get("photo_data_url") or ""),
                        "updated_utc": str(p.get("updated_utc") or now_utc_iso()),
                    }
        cfg_out["updated_utc"] = now_utc_iso()
        SITE_MAP_CONFIG_PATH.write_text(json.dumps(cfg_out, indent=2) + "\n")
        return True
    except Exception:
        return False


def recipients_for_alert_code(alert_code, alert_cfg):
    users_cfg = load_users_config()
    out = []
    for u in users_cfg.get("users", []):
        if not isinstance(u, dict) or not u.get("enabled"):
            continue
        email = str(u.get("email") or "").strip()
        if not email:
            continue
        prefs = u.get("alerts", {}) if isinstance(u.get("alerts"), dict) else {}
        if prefs.get(alert_code):
            out.append(email)
    if out:
        seen = set()
        dedup = []
        for e in out:
            if e.lower() in seen:
                continue
            seen.add(e.lower())
            dedup.append(e)
        return dedup
    # fallback to legacy recipient list
    email_cfg = alert_cfg.get("email", {}) if isinstance(alert_cfg, dict) else {}
    to_list = email_cfg.get("to", [])
    if isinstance(to_list, str):
        to_list = [to_list]
    return [str(x).strip() for x in to_list if str(x).strip()]


def append_structured_alert_event(event):
    try:
        ALERT_EVENTS_JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ALERT_EVENTS_JSONL_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    except Exception:
        pass


def record_alert_dispatch_event(category, zone_id, zone_label, subject, detail, alert_key, recipients, sent_email, sent_sms):
    append_structured_alert_event({
        "id": f"al_{uuid.uuid4().hex[:12]}",
        "created_utc": now_utc_iso(),
        "category": category,
        "zone_id": zone_id,
        "zone_label": zone_label,
        "subject": subject,
        "detail": detail,
        "alert_key": alert_key,
        "recipients": recipients or [],
        "sent_email": bool(sent_email),
        "sent_sms": bool(sent_sms),
        "acknowledged": False,
        "acknowledged_utc": None,
        "acknowledged_by": "",
        "ack_note": "",
    })


def recent_alert_events(limit=100):
    out = []
    if not ALERT_EVENTS_JSONL_PATH.exists():
        return out
    try:
        with ALERT_EVENTS_JSONL_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
        out = [x for x in out if isinstance(x, dict)]
        out.sort(key=lambda x: x.get("created_utc") or "", reverse=True)
        return out[: max(1, int(limit))]
    except Exception:
        return []


def unacknowledged_alert_count(limit=1000):
    try:
        return sum(1 for ev in recent_alert_events(limit=limit) if isinstance(ev, dict) and not ev.get("acknowledged"))
    except Exception:
        return 0


def acknowledge_alert_event(event_id, who="", note=""):
    if not ALERT_EVENTS_JSONL_PATH.exists():
        return False, "alert log not found"
    changed = False
    rows = []
    try:
        with ALERT_EVENTS_JSONL_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict) and str(obj.get("id")) == str(event_id):
                    obj["acknowledged"] = True
                    obj["acknowledged_utc"] = now_utc_iso()
                    obj["acknowledged_by"] = str(who or "").strip()
                    obj["ack_note"] = str(note or "").strip()
                    changed = True
                rows.append(obj)
        if changed:
            with ALERT_EVENTS_JSONL_PATH.open("w", encoding="utf-8") as f:
                for obj in rows:
                    f.write(json.dumps(obj) + "\n")
            return True, "acknowledged"
        return False, "alert id not found"
    except Exception as e:
        return False, str(e)


def log_alert_event(msg):
    try:
        ALERT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ALERT_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(f"{now_utc_iso()} {msg}\n")
    except Exception:
        pass


def send_email_notification(cfg, subject, body, recipients=None):
    email_cfg = cfg.get("email", {}) if isinstance(cfg, dict) else {}
    if not email_cfg.get("enabled"):
        return False

    to_list = recipients if recipients is not None else email_cfg.get("to", [])
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


def dispatch_alert_by_category(category, zone_id, zone_label, subject, body, alert_key, detail):
    cfg = load_alert_config()
    recipients = recipients_for_alert_code(category, cfg)
    sent_email = send_email_notification(cfg, subject, body, recipients=recipients)
    sent_sms = send_sms_notification(cfg, body)
    log_alert_event(f"alert_dispatched email={sent_email} sms={sent_sms} key={alert_key} category={category} detail={detail}")
    record_alert_dispatch_event(category, zone_id, zone_label, subject, detail, alert_key, recipients, sent_email, sent_sms)
    return sent_email, sent_sms


def alert_cooldown_seconds(cfg):
    cooldown_min = cfg.get("cooldown_minutes", DEFAULT_ALERT_COOLDOWN_MIN) if isinstance(cfg, dict) else DEFAULT_ALERT_COOLDOWN_MIN
    try:
        return max(60, int(cooldown_min) * 60)
    except Exception:
        return DEFAULT_ALERT_COOLDOWN_MIN * 60


def should_send_performance_alert(alert_key, cooldown_sec):
    now = now_utc()
    last = perf_alert_last_sent.get(alert_key)
    if last and (now - last).total_seconds() < cooldown_sec:
        return False, now
    perf_alert_last_sent[alert_key] = now
    return True, now


def maybe_send_device_offline_alerts(downstream_rows):
    global downstream_offline_state
    rows = downstream_rows if isinstance(downstream_rows, list) else []
    cfg = load_alert_config()
    cooldown_sec = alert_cooldown_seconds(cfg)
    now = now_utc()
    for z in rows:
        if not isinstance(z, dict):
            continue
        zone_id = str(z.get("id") or "").strip()
        if not zone_id:
            continue
        zone_label = str(z.get("name") or zone_id)
        is_offline = str(z.get("zone_status") or "") == "Source Unreachable"
        state = downstream_offline_state.get(zone_id, {"offline": False, "offline_since": None, "last_sent": None, "alerted": False})
        if is_offline:
            if not state.get("offline"):
                state["offline_since"] = now
                state["alerted"] = False
            offline_since = state.get("offline_since") if isinstance(state.get("offline_since"), datetime) else now
            offline_elapsed = max(0, (now - offline_since).total_seconds())
            last_sent = state.get("last_sent")
            if (not state.get("alerted")) and offline_elapsed >= DEVICE_OFFLINE_ALERT_THRESHOLD_SECONDS and not (
                isinstance(last_sent, datetime) and (now - last_sent).total_seconds() < cooldown_sec
            ):
                subject = f"165 Water Street Heating Hub - Notify Admin ({zone_label})"
                detail = z.get("status_note") or "source unreachable"
                body = (
                    "165 Water Street Heating Hub Alert\n\n"
                    f"Zone: {zone_label}\n"
                    f"Zone ID: {zone_id}\n"
                    "Alert Type: Device Offline\n"
                    f"Status: Source Unreachable\n"
                    f"Offline Duration: {round(offline_elapsed/60.0,2)} min (threshold {DEVICE_OFFLINE_ALERT_THRESHOLD_SECONDS/60:.1f} min)\n"
                    f"Error Description: {detail}\n"
                    f"Time: {now.isoformat()}\n\n"
                    f"Live Hub: {PUBLIC_HUB_URL}\n"
                )
                dispatch_alert_by_category("device_offline", zone_id, zone_label, subject, body, f"offline:{zone_id}", detail)
                state["last_sent"] = now
                state["alerted"] = True
        if not is_offline and state.get("offline"):
            offline_since = state.get("offline_since") if isinstance(state.get("offline_since"), datetime) else None
            offline_minutes = round(max(0, (now - offline_since).total_seconds()) / 60.0, 2) if offline_since else None
            log_alert_event(f"device_recovered zone={zone_id}")
            if state.get("alerted"):
                subject = f"165 Water Street Heating Hub - Device Recovered ({zone_label})"
                detail = "source reachable again"
                body = (
                    "165 Water Street Heating Hub Alert\n\n"
                    f"Zone: {zone_label}\n"
                    f"Zone ID: {zone_id}\n"
                    "Alert Type: Device Recovered\n"
                    "Status: Source Reachable\n"
                    + (f"Previous Offline Duration: {offline_minutes} min\n" if offline_minutes is not None else "")
                    + f"Time: {now.isoformat()}\n\n"
                    + f"Live Hub: {PUBLIC_HUB_URL}\n"
                )
                dispatch_alert_by_category("device_recovered", zone_id, zone_label, subject, body, f"recovered:{zone_id}", detail)
                state["last_sent"] = now
            state["offline_since"] = None
            state["alerted"] = False
        state["offline"] = is_offline
        downstream_offline_state[zone_id] = state


def maybe_send_cycle_performance_alert(event):
    if not isinstance(event, dict):
        return
    zone_id = str(event.get("zone") or "").strip() or "unknown"
    zone_label = str(event.get("zone_label") or zone_id)
    duration_s = int(event.get("duration_s", 0) or 0)
    short_cycle = bool(event.get("short_cycle"))
    response_reached = bool(event.get("response_reached"))
    response_lag_s = event.get("response_lag_s")
    try:
        response_lag_s = None if response_lag_s is None else int(response_lag_s)
    except Exception:
        response_lag_s = None

    issues = []
    if short_cycle:
        issues.append("short_cycle")
    if response_reached and response_lag_s is not None and response_lag_s > RESPONSE_LAG_ALERT_THRESHOLD_SECONDS:
        issues.append("response_lag")
    if not response_reached:
        issues.append("no_response")
    if not issues:
        return

    cfg = load_alert_config()
    cooldown_sec = alert_cooldown_seconds(cfg)
    key = f"perf:{zone_id}:{'+'.join(sorted(issues))}"
    ok, now = should_send_performance_alert(key, cooldown_sec)
    if not ok:
        log_alert_event(f"perf_alert_suppressed cooldown key={key}")
        return

    issue_lines = []
    if "short_cycle" in issues:
        issue_lines.append(f"- Short Cycling: cycle runtime {round(duration_s/60.0,2)} min (threshold <= {SHORT_CYCLE_THRESHOLD_SECONDS/60:.1f} min)")
    if "response_lag" in issues:
        issue_lines.append(f"- Response Lag: {round((response_lag_s or 0)/60.0,2)} min (alert threshold > {RESPONSE_LAG_ALERT_THRESHOLD_SECONDS/60:.1f} min)")
    if "no_response" in issues:
        issue_lines.append("- No Temperature Response detected during call cycle")

    subject = f"165 Water Street Heating Hub - Notify Admin ({zone_label})"
    body = (
        "165 Water Street Heating Hub Alert\n\n"
        f"Zone: {zone_label}\n"
        f"Zone ID: {zone_id}\n"
        f"Alert Type: Cycle Performance\n"
        f"Cycle Start: {event.get('start_ts')}\n"
        f"Cycle End: {event.get('end_ts')}\n"
        f"Cycle Duration: {round(duration_s/60.0,2)} min\n"
        + ("\n".join(issue_lines) + "\n\n")
        + "Check system operation to avoid equipment wear, poor heat delivery, or freeze risk.\n\n"
        + f"Live Hub: {PUBLIC_HUB_URL}\n"
        + f"Time: {now.isoformat()}\n"
    )
    primary_category = "no_response" if "no_response" in issues else ("response_lag" if "response_lag" in issues else "short_cycle")
    dispatch_alert_by_category(primary_category, zone_id, zone_label, subject, body, key, ",".join(issues))


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
    cooldown_sec = alert_cooldown_seconds(cfg)

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

    dispatch_alert_by_category("temp_response", "zone1_main", "Zone 1 (Main)", subject, body, alert_key, detail)

    last_alert_sent_at = now
    last_alert_key = alert_key


def now_utc():
    return datetime.now(timezone.utc)


def now_utc_iso():
    return now_utc().isoformat()


def iso_to_dt(s):
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


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


def append_call_cycle_event(event):
    CALL_CYCLES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with CALL_CYCLES_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def zone1_response_ok(local):
    try:
        readings = (local or {}).get("readings", {})
        feed = readings.get("feed_1", {}).get("temp_f")
        ret = readings.get("return_1", {}).get("temp_f")
        if feed is None or ret is None:
            return False
        return abs(float(feed) - float(ret)) <= float(ZONE1_DELTA_THRESHOLD_F)
    except Exception:
        return False


def downstream_response_ok(zone_row):
    try:
        if not isinstance(zone_row, dict):
            return False
        feed = zone_row.get("feed_f")
        ret = zone_row.get("return_f")
        delta_ok = as_float(zone_row.get("delta_ok_f"))
        if delta_ok is None:
            delta_ok = 8.0
        if feed is None or ret is None:
            return False
        return abs(float(feed) - float(ret)) <= float(delta_ok)
    except Exception:
        return False


def _tracker_for(zone_id):
    t = cycle_trackers.get(zone_id)
    if not isinstance(t, dict):
        t = {
            "prev_call_active": None,
            "active_started_at": None,
            "response_lag_s": None,
            "response_reached": False,
        }
        cycle_trackers[zone_id] = t
    return t


def update_cycle_tracking_for_zone(zone_id, call_active, sample_ts, response_ok=None):
    if sample_ts is None:
        sample_ts = now_utc()
    with cycle_state_lock:
        t = _tracker_for(zone_id)
        prev = t.get("prev_call_active")
        if call_active is True and prev is not True:
            t["active_started_at"] = sample_ts
            t["response_lag_s"] = None
            t["response_reached"] = False
        if call_active is True and t.get("active_started_at") is not None:
            if bool(response_ok) and not t.get("response_reached"):
                lag = max(0, int((sample_ts - t["active_started_at"]).total_seconds()))
                t["response_lag_s"] = lag
                t["response_reached"] = True
        elif call_active is not True and prev is True:
            started = t.get("active_started_at") or sample_ts
            if sample_ts < started:
                sample_ts = started
            duration_s = max(0, int((sample_ts - started).total_seconds()))
            event = {
                "zone": zone_id,
                "zone_label": t.get("zone_label") or zone_id,
                "start_ts": started.isoformat(),
                "end_ts": sample_ts.isoformat(),
                "duration_s": duration_s,
                "short_cycle": bool(duration_s <= SHORT_CYCLE_THRESHOLD_SECONDS),
                "response_reached": bool(t.get("response_reached")),
                "response_lag_s": t.get("response_lag_s"),
                "completed": True,
            }
            append_call_cycle_event(event)
            try:
                maybe_send_cycle_performance_alert(event)
            except Exception:
                pass
            t["active_started_at"] = None
            t["response_lag_s"] = None
            t["response_reached"] = False
        t["prev_call_active"] = call_active


def update_all_cycle_tracking(local, downstream):
    local_ts = iso_to_dt((local or {}).get("updated_utc")) if isinstance(local, dict) else None
    if local_ts is None:
        local_ts = now_utc()
    local_call = (local or {}).get("main_call_24vac") if isinstance(local, dict) else None
    with cycle_state_lock:
        _tracker_for("zone1_main")["zone_label"] = "Zone 1 (Main)"
    update_cycle_tracking_for_zone("zone1_main", local_call, local_ts, zone1_response_ok(local))
    for z in (downstream or []):
        if not isinstance(z, dict):
            continue
        zid = str(z.get("id") or "").strip()
        if not zid:
            continue
        with cycle_state_lock:
            _tracker_for(zid)["zone_label"] = str(z.get("name") or zid)
        z_ts = iso_to_dt(z.get("updated_utc")) or local_ts
        update_cycle_tracking_for_zone(zid, z.get("heating_call"), z_ts, downstream_response_ok(z))


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


def prune_call_cycles():
    if not CALL_CYCLES_FILE.exists():
        return
    cutoff = now_utc() - timedelta(days=RETENTION_DAYS)
    keep = []
    with CALL_CYCLES_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                end_ts = iso_to_dt(obj.get("end_ts", "")) or iso_to_dt(obj.get("start_ts", ""))
            except Exception:
                continue
            if end_ts and end_ts >= cutoff:
                keep.append(obj)
    with CALL_CYCLES_FILE.open("w", encoding="utf-8") as f:
        for obj in keep:
            f.write(json.dumps(obj) + "\n")


def cycle_analytics_for_window(window_key, zone_id="zone1_main"):
    windows = {
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
    }
    window = windows.get(window_key, windows["24h"])
    now = now_utc()
    cutoff = now - window
    if window_key == "24h":
        bucket_step = timedelta(hours=1)
        bucket_fmt = "%H:00"
    else:
        bucket_step = timedelta(days=1)
        bucket_fmt = "%b %d"
    bucket_count = int(window.total_seconds() // bucket_step.total_seconds())
    if bucket_count <= 0:
        bucket_count = 1
    bucket_start0 = now - (bucket_step * bucket_count)
    buckets = []
    for i in range(bucket_count):
        bs = bucket_start0 + bucket_step * i
        buckets.append({
            "start_ts": bs.isoformat(),
            "label": bs.strftime(bucket_fmt),
            "cycles": 0,
            "runtime_minutes": 0.0,
        })

    total_cycles = 0
    total_runtime_s = 0.0
    short_cycle_count = 0
    response_samples = []
    no_response_count = 0
    if CALL_CYCLES_FILE.exists():
        with CALL_CYCLES_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if str(obj.get("zone", "zone1_main")) != str(zone_id):
                        continue
                    end_ts = iso_to_dt(obj.get("end_ts", ""))
                    duration_s = float(obj.get("duration_s", 0) or 0)
                except Exception:
                    continue
                if end_ts is None or end_ts < cutoff:
                    continue
                total_cycles += 1
                total_runtime_s += max(0.0, duration_s)
                if bool(obj.get("short_cycle")):
                    short_cycle_count += 1
                if obj.get("response_lag_s") is not None:
                    try:
                        response_samples.append(float(obj.get("response_lag_s")))
                    except Exception:
                        pass
                elif obj.get("response_reached") is False:
                    no_response_count += 1
                idx = int((end_ts - bucket_start0).total_seconds() // bucket_step.total_seconds())
                if 0 <= idx < len(buckets):
                    buckets[idx]["cycles"] += 1
                    buckets[idx]["runtime_minutes"] += max(0.0, duration_s) / 60.0

    open_cycle = None
    with cycle_state_lock:
        t = dict(_tracker_for(zone_id))
        started = t.get("active_started_at")
        active_now = t.get("prev_call_active") is True
    if active_now and started is not None:
        if started < cutoff:
            started_for_window = cutoff
        else:
            started_for_window = started
        open_runtime_s = max(0.0, (now - started_for_window).total_seconds())
        total_runtime_s += open_runtime_s
        idx = int((now - bucket_start0).total_seconds() // bucket_step.total_seconds())
        if 0 <= idx < len(buckets):
            buckets[idx]["runtime_minutes"] += open_runtime_s / 60.0
        open_cycle = {
            "active": True,
            "start_ts": started.isoformat(),
            "runtime_minutes": round(open_runtime_s / 60.0, 2),
            "response_reached": bool(t.get("response_reached")),
            "response_lag_minutes": round((float(t.get("response_lag_s")) / 60.0), 2) if t.get("response_lag_s") is not None else None,
        }

    for b in buckets:
        b["runtime_minutes"] = round(b["runtime_minutes"], 2)

    window_seconds = max(1.0, window.total_seconds())
    avg_cycle_minutes = (total_runtime_s / 60.0 / total_cycles) if total_cycles else 0.0
    duty_cycle_pct = (total_runtime_s / window_seconds) * 100.0
    avg_response_lag_min = (sum(response_samples) / len(response_samples) / 60.0) if response_samples else 0.0
    short_cycle_pct = (short_cycle_count / total_cycles * 100.0) if total_cycles else 0.0
    return {
        "zone": zone_id,
        "range": window_key,
        "updated_utc": now.isoformat(),
        "summary": {
            "cycle_count": int(total_cycles),
            "total_runtime_minutes": round(total_runtime_s / 60.0, 2),
            "avg_cycle_minutes": round(avg_cycle_minutes, 2),
            "duty_cycle_pct": round(duty_cycle_pct, 2),
            "short_cycle_count": int(short_cycle_count),
            "short_cycle_pct": round(short_cycle_pct, 2),
            "avg_response_lag_minutes": round(avg_response_lag_min, 2),
            "no_response_count": int(no_response_count),
            "short_cycle_threshold_minutes": round(SHORT_CYCLE_THRESHOLD_SECONDS / 60.0, 1),
        },
        "open_cycle": open_cycle,
        "points": buckets,
    }


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
            try:
                downstream = refresh_downstream_cache()
            except Exception:
                downstream = get_downstream_cached()
            try:
                maybe_send_device_offline_alerts(downstream)
            except Exception:
                pass
            update_all_cycle_tracking(local, downstream)
            trigger_zone1_alert_from_local(local)
            loops += 1
            if loops % HISTORY_SAMPLE_EVERY_LOOPS == 0:
                append_sample(make_sample(local=local, downstream=downstream))
            if loops % 60 == 0:
                prune_history()
                prune_call_cycles()
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
        "unack_alert_count": unacknowledged_alert_count(),
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
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" crossorigin=\"\"/>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\" crossorigin=\"\"></script>
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
    .head-pill-btn{border:1px solid rgba(255,255,255,.26);border-radius:999px;padding:7px 12px;font-weight:700;background:rgba(255,255,255,.1);color:#f2fbff;cursor:pointer}
    .head-pill-btn:hover{background:rgba(255,255,255,.16)}
    .head-pill-btn .sub{display:block;font-size:.72rem;font-weight:600;opacity:.9;line-height:1.1}

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
    .perf-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-top:10px}
    .kpi{border:1px solid var(--border);border-radius:12px;padding:10px;background:#f8fbfd}
    .kpi .k{font-size:.78rem;color:var(--muted);font-weight:700;text-transform:uppercase;letter-spacing:.03em}
    .kpi .v{font-size:1.25rem;font-weight:800;margin-top:4px}
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
    .hidden{display:none !important}
    .fab-top{
      position:fixed; right:18px; bottom:18px; z-index:1200;
      border:1px solid var(--border); border-radius:999px; padding:10px 14px;
      background:linear-gradient(120deg,var(--brand),var(--brand2)); color:#fff;
      font-weight:700; box-shadow:0 10px 24px rgba(10,31,44,.18); cursor:pointer;
      opacity:0; transform:translateY(8px); pointer-events:none; transition:opacity .18s ease, transform .18s ease;
    }
    .fab-top.show{opacity:1; transform:translateY(0); pointer-events:auto}
    #siteMapCanvas{width:100%;height:420px;border:1px solid var(--border);border-radius:12px;overflow:hidden;background:#eef6fb}
    .site-map-tools{display:grid;grid-template-columns:2fr 1fr auto auto;gap:10px;align-items:end}
    .site-map-subtools{display:grid;grid-template-columns:1fr 1fr auto;gap:10px;align-items:end;margin-top:10px}
    .pin-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:8px;margin-top:10px}
    .pin-chip{border:1px solid var(--border);border-radius:10px;padding:8px 10px;background:#f8fbfd}
    .pin-chip .t{font-weight:700}
    .pin-chip .s{color:var(--muted);font-size:.86rem}
    .pin-dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:middle;border:1px solid rgba(0,0,0,.15)}
    .zone-info-modal{position:fixed;inset:0;background:rgba(5,12,18,.45);display:none;align-items:center;justify-content:center;padding:16px;z-index:2000}
    .zone-info-modal.show{display:flex}
    .zone-info-card{max-width:760px;width:min(100%,760px);max-height:88vh;overflow:auto;background:#fff;border-radius:16px;border:1px solid var(--border);box-shadow:0 18px 36px rgba(10,31,44,.24);padding:14px}
    .zone-info-grid{display:grid;grid-template-columns:1.2fr 1fr;gap:12px}
    .zone-info-photo{width:100%;aspect-ratio:4/3;object-fit:cover;border:1px solid var(--border);border-radius:12px;background:#f2f7fa}
    @media (max-width:760px){.zone-info-grid{grid-template-columns:1fr}}
    @media (max-width:760px) {
      .wrap { padding:.75rem; }
      .head { padding:14px; border-radius:14px; }
      .temp { font-size:1.35rem; }
      .form-grid{grid-template-columns:1fr}
      .site-map-tools,.site-map-subtools{grid-template-columns:1fr}
      #siteMapCanvas{height:320px}
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
    <header class=\"head\" id=\"mainTop\">
      <div>
        <h1>165 Water Street Heating Hub</h1>
        <div class=\"meta\">Main Boiler Zone(s) + Heating Zones</div>
      </div>
      <div style=\"display:flex;gap:8px;flex-wrap:wrap\">
        <button id=\"alertPill\" type=\"button\" class=\"head-pill-btn\">Trouble Zones: --<span class=\"sub\">Open Trouble Log</span></button>
        <button id=\"unackPill\" type=\"button\" class=\"head-pill-btn\">Alerts To Acknowledge: --<span class=\"sub\">Open Alert Log</span></button>
        <button id=\"addDevicePill\" type=\"button\" class=\"head-pill-btn\">Add Device<span class=\"sub\">Open Setup New Device</span></button>
      </div>
    </header>

    <section class=\"main-grid\">
      <article class=\"card\">
        <div style=\"display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px\">
          <h2 style=\"margin:0\">Zone 1 (Main)</h2>
          <button type=\"button\" class=\"btn\" data-zone-info=\"zone1_main\" style=\"padding:4px 10px\">Zone Info</button>
        </div>
        <div class=\"row\"><div><div class=\"label\">Feed</div><div id=\"feed_1_sid\" class=\"sid\">--</div></div><div><span id=\"feed_1\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Return</div><div id=\"return_1_sid\" class=\"sid\">--</div></div><div><span id=\"return_1\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Call Status</div><div class=\"sid\">GPIO17 Dry Contact</div></div><div><span id=\"zone1_call_status\" class=\"temp\" style=\"font-size:1.0rem;\">--</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Zone 1 Status</div><div class=\"sid\">Call + feed/return check</div></div><div><span id=\"zone1_status\" class=\"temp\" style=\"font-size:1.0rem;\">--</span></div></div>
      </article>
      <article class=\"card\">
        <div style=\"display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px\">
          <h2 style=\"margin:0\">Zone 2 (Main)</h2>
          <button type=\"button\" class=\"btn\" data-zone-info=\"zone2_main\" style=\"padding:4px 10px\">Zone Info</button>
        </div>
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

    <section class=\"chart-wrap\">
      <div class=\"chart-head\">
        <h3>Call Cycles & Performance (Zone 1 Main)</h3>
        <div class=\"btns\" id=\"cycleBtns\">
          <button class=\"btn active\" data-cycle-range=\"24h\">24H</button>
          <button class=\"btn\" data-cycle-range=\"7d\">7D</button>
          <button class=\"btn\" data-cycle-range=\"30d\">30D</button>
        </div>
      </div>
      <div style=\"display:flex;justify-content:flex-end;margin-top:8px\">
        <button type=\"button\" class=\"btn\" data-return-main>Return to Main</button>
      </div>
      <div id=\"cycleZoneTabs\" class=\"tabs\" style=\"margin-top:10px\"></div>
      <div class=\"perf-grid\">
        <div class=\"kpi\"><div class=\"k\">Cycles</div><div class=\"v\" id=\"cycleCountKpi\">--</div></div>
        <div class=\"kpi\"><div class=\"k\">Total Runtime</div><div class=\"v\" id=\"cycleRuntimeKpi\">--</div></div>
        <div class=\"kpi\"><div class=\"k\">Average Call Length</div><div class=\"v\" id=\"cycleAvgKpi\">--</div></div>
        <div class=\"kpi\"><div class=\"k\">Heating Run Time %</div><div class=\"v\" id=\"cycleDutyKpi\">--</div></div>
        <div class=\"kpi\"><div class=\"k\">Short Cycles</div><div class=\"v\" id=\"cycleShortKpi\">--</div></div>
        <div class=\"kpi\"><div class=\"k\">Average Temp Response Time</div><div class=\"v\" id=\"cycleRespLagKpi\">--</div></div>
        <div class=\"kpi\"><div class=\"k\">Calls With No Temp Response</div><div class=\"v\" id=\"cycleNoRespKpi\">--</div></div>
      </div>
      <canvas id=\"cycleChart\" width=\"1040\" height=\"280\"></canvas>
      <div class=\"legend\">
        <span><span class=\"dot\" style=\"background:#2563eb\"></span>Cycles</span>
        <span><span class=\"dot\" style=\"background:#b45309\"></span>Runtime (min)</span>
      </div>
      <div class=\"smallnote\" id=\"cycleOpenNote\" style=\"margin-top:6px\"></div>
    </section>

    <section class=\"subzones\">
      <div class=\"subzones-head\">
        <h3>Downstream Sub-Zones</h3>
        <div style=\"display:flex;gap:8px;align-items:center;flex-wrap:wrap\">
          <div id=\"updated\" class=\"meta\">Updated: --</div>
          <button type=\"button\" class=\"btn\" data-return-main style=\"padding:4px 10px\">Return to Main</button>
        </div>
      </div>
      <div class=\"table-responsive\">
      <table class=\"table table-hover align-middle mb-0\">
        <thead>
          <tr><th>Parent</th><th>Sub-Zone</th><th>Heat Call (24VAC)</th><th>Feed</th><th>Return</th><th>Status</th><th>Note</th><th>Info</th></tr>
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
      <div style=\"display:flex;justify-content:flex-end;margin-top:8px\">
        <button type=\"button\" class=\"btn\" data-return-main>Return to Main</button>
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

    <section class=\"chart-wrap\">
      <div class=\"chart-head\">
        <h3>Team, Roles & Alarm Acknowledge</h3>
        <div class=\"smallnote\">Manage who receives which alerts and acknowledge dispatched alarms</div>
      </div>
      <div style=\"display:flex;justify-content:flex-end;margin-top:8px\">
        <button type=\"button\" class=\"btn\" data-return-main>Return to Main</button>
      </div>
      <div class=\"tabs\" id=\"adminTabs\" style=\"margin-top:10px\">
        <button class=\"tab active\" data-admin-tab=\"users\">Users / Roles</button>
        <button class=\"tab\" data-admin-tab=\"alerts\">Alarm Acknowledge</button>
        <button class=\"tab\" data-admin-tab=\"map\">Site Map</button>
        <button class=\"tab\" data-admin-tab=\"setup\">Setup New Device</button>
      </div>

      <div id=\"adminUsersPanel\" style=\"margin-top:12px\">
        <div class=\"form-grid\">
          <div>
            <label class=\"label\" for=\"userName\">Name</label>
            <input id=\"userName\" class=\"input\" placeholder=\"Property Manager / Tech\">
          </div>
          <div>
            <label class=\"label\" for=\"userEmail\">Email</label>
            <input id=\"userEmail\" class=\"input\" placeholder=\"name@example.com\">
          </div>
          <div>
            <label class=\"label\" for=\"userRole\">Role</label>
            <select id=\"userRole\" class=\"select\">
              <option value=\"admin\">Admin</option>
              <option value=\"tech\">Tech</option>
              <option value=\"manager\">Property Manager</option>
              <option value=\"viewer\">Viewer</option>
            </select>
          </div>
          <div>
            <label class=\"label\" for=\"userEnabled\">User Status</label>
            <select id=\"userEnabled\" class=\"select\">
              <option value=\"true\">Enabled</option>
              <option value=\"false\">Disabled</option>
            </select>
          </div>
          <div class=\"full\">
            <div class=\"label\" style=\"margin-bottom:6px\">Alert Types</div>
            <div class=\"d-flex flex-wrap gap-3\" id=\"userAlertPrefs\">
              <label><input type=\"checkbox\" data-alert-pref=\"temp_response\" checked> Temp Response Faults</label>
              <label><input type=\"checkbox\" data-alert-pref=\"short_cycle\" checked> Short Cycling</label>
              <label><input type=\"checkbox\" data-alert-pref=\"response_lag\" checked> Response Lag</label>
              <label><input type=\"checkbox\" data-alert-pref=\"no_response\" checked> No Temperature Response</label>
              <label><input type=\"checkbox\" data-alert-pref=\"low_temp\" checked> Low Temp</label>
              <label><input type=\"checkbox\" data-alert-pref=\"device_offline\" checked> Device Offline</label>
              <label><input type=\"checkbox\" data-alert-pref=\"device_recovered\" checked> Device Recovered</label>
            </div>
          </div>
          <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
            <button type=\"button\" class=\"btn active\" id=\"saveUserBtn\">Save User</button>
            <button type=\"button\" class=\"btn\" id=\"clearUserFormBtn\">Clear Form</button>
          </div>
        </div>
        <div id=\"usersMsg\" class=\"msg\"></div>
        <div class=\"table-responsive\" style=\"margin-top:10px\">
          <table class=\"table table-hover align-middle mb-0\">
            <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Subscribed Alerts</th><th>Status</th><th>Actions</th></tr></thead>
            <tbody id=\"usersBody\"></tbody>
          </table>
        </div>
      </div>

      <div id=\"adminAlertsPanel\" class=\"hidden\" style=\"margin-top:12px\">
        <div class=\"form-grid\">
          <div>
            <label class=\"label\" for=\"ackBy\">Acknowledge As</label>
            <input id=\"ackBy\" class=\"input\" placeholder=\"Your name / initials\">
          </div>
          <div>
            <label class=\"label\" for=\"alertLimit\">Rows</label>
            <select id=\"alertLimit\" class=\"select\">
              <option value=\"25\">25</option>
              <option value=\"50\" selected>50</option>
              <option value=\"100\">100</option>
            </select>
          </div>
          <div class=\"full\">
            <label class=\"label\" for=\"ackNote\">Acknowledge Note</label>
            <input id=\"ackNote\" class=\"input\" placeholder=\"Tech dispatched / monitoring / resolved cause identified\">
          </div>
          <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
            <button type=\"button\" class=\"btn\" id=\"refreshAlertsBtn\">Refresh Alerts</button>
          </div>
          <div class=\"full\">
            <div class=\"tabs\" id=\"alertLogFilterTabs\" style=\"margin-top:4px\">
              <button class=\"tab active\" data-alert-log-filter=\"trouble\">Trouble Log</button>
              <button class=\"tab\" data-alert-log-filter=\"all\">Alert Log</button>
              <button class=\"tab\" data-alert-log-filter=\"unack\">Needs Acknowledgement</button>
            </div>
          </div>
        </div>
        <div id=\"alertsMsg\" class=\"msg\"></div>
        <div class=\"table-responsive\" style=\"margin-top:10px\">
          <table class=\"table table-hover align-middle mb-0\">
            <thead><tr><th>Time</th><th>Zone</th><th>Type</th><th>Detail</th><th>Status</th><th>Actions</th></tr></thead>
            <tbody id=\"alertsBody\"></tbody>
          </table>
        </div>
      </div>

      <div id=\"adminMapPanel\" class=\"hidden\" style=\"margin-top:12px\">
        <div class=\"card\" style=\"padding:12px;border-radius:12px;border:1px solid var(--border);background:#f9fcfe;box-shadow:none\">
          <h4 style=\"margin:0 0 8px\">Building / Zone Device Map</h4>
          <div class=\"smallnote\">Enter the building address, zoom in, then select a zone/device and click the map to place a rough pin location.</div>
          <div class=\"site-map-tools\" style=\"margin-top:10px\">
            <div>
              <label class=\"label\" for=\"siteAddress\">Building Address</label>
              <input id=\"siteAddress\" class=\"input\" placeholder=\"165 Water Street, New Haven, CT\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapZoneSelect\">Zone / Device</label>
              <select id=\"siteMapZoneSelect\" class=\"select\"></select>
            </div>
            <button type=\"button\" class=\"btn\" id=\"siteMapFindBtn\">Find Address</button>
            <button type=\"button\" class=\"btn active\" id=\"siteMapSaveBtn\">Save Map</button>
          </div>
          <div class=\"site-map-subtools\">
            <div>
              <label class=\"label\" for=\"siteMapZoom\">Zoom</label>
              <input id=\"siteMapZoom\" class=\"input\" type=\"number\" min=\"1\" max=\"22\" step=\"1\" value=\"16\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapCenter\">Map Center</label>
              <input id=\"siteMapCenter\" class=\"input\" readonly placeholder=\"Lat, Lng\">
            </div>
            <button type=\"button\" class=\"btn\" id=\"siteMapClearPinBtn\">Clear Selected Pin</button>
          </div>
          <div class=\"form-grid\" style=\"margin-top:10px\">
            <div>
              <label class=\"label\" for=\"siteMapPinColor\">Pin Color / Floor Group</label>
              <select id=\"siteMapPinColor\" class=\"select\">
                <option value=\"red\">Red</option>
                <option value=\"blue\">Blue</option>
                <option value=\"green\">Green</option>
                <option value=\"orange\">Orange</option>
                <option value=\"purple\">Purple</option>
                <option value=\"gray\">Gray</option>
              </select>
            </div>
            <div>
              <label class=\"label\" for=\"siteMapEquipType\">Equipment Type</label>
              <input id=\"siteMapEquipType\" class=\"input\" placeholder=\"Fan coil / Unit heater / Air handler coil\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapRoomArea\">Room / Area Name</label>
              <input id=\"siteMapRoomArea\" class=\"input\" placeholder=\"AV Room / 1st Floor Production / Basement Mech Room\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapThermostatLoc\">Thermostat Location</label>
              <input id=\"siteMapThermostatLoc\" class=\"input\" placeholder=\"South wall by door / office interior wall\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapValveType\">Valve / Actuator Type</label>
              <input id=\"siteMapValveType\" class=\"input\" placeholder=\"2-way zone valve / Belimo actuator / Taco zone valve\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapBreakerRef\">Circuit / Breaker Reference</label>
              <input id=\"siteMapBreakerRef\" class=\"input\" placeholder=\"Panel L1 / Breaker 12\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapInstalledBy\">Installed By</label>
              <input id=\"siteMapInstalledBy\" class=\"input\" placeholder=\"Tech / contractor name\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapInstallDate\">Install Date</label>
              <input id=\"siteMapInstallDate\" class=\"input\" type=\"date\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapLastServiceDate\">Last Service Date</label>
              <input id=\"siteMapLastServiceDate\" class=\"input\" type=\"date\">
            </div>
            <div class=\"full\">
              <label class=\"label\" for=\"siteMapDesc\">Location Description</label>
              <textarea id=\"siteMapDesc\" class=\"input\" rows=\"3\" placeholder=\"AV coil is above drop ceiling near north wall access panel.\"></textarea>
            </div>
            <div class=\"full\">
              <label class=\"label\" for=\"siteMapAccessInstructions\">Access Instructions</label>
              <textarea id=\"siteMapAccessInstructions\" class=\"input\" rows=\"2\" placeholder=\"Use 8' ladder, remove 2nd ceiling tile from east wall, power panel access required.\"></textarea>
            </div>
            <div>
              <label class=\"label\" for=\"siteMapPhotoInput\">Zone Photo</label>
              <input id=\"siteMapPhotoInput\" class=\"input\" type=\"file\" accept=\"image/*\" capture=\"environment\">
              <div class=\"smallnote\">On mobile, this can open the camera.</div>
            </div>
            <div style=\"display:flex;align-items:end;gap:8px\">
              <button type=\"button\" class=\"btn\" id=\"siteMapClearPhotoBtn\">Clear Photo</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapApplyDetailsBtn\">Apply Details to Selected Zone</button>
            </div>
            <div class=\"full\">
              <img id=\"siteMapPhotoPreview\" alt=\"Zone photo preview\" style=\"max-width:280px;width:100%;display:none;border:1px solid var(--border);border-radius:10px;background:#f4f8fb\"/>
            </div>
          </div>
          <div id=\"siteMapCanvas\" style=\"margin-top:10px\"></div>
          <div class=\"smallnote\" style=\"margin-top:8px\">Tip: Choose a zone/device, then tap the map where that unit is roughly located.</div>
          <div id=\"siteMapMsg\" class=\"msg\"></div>
          <div id=\"siteMapPinsList\" class=\"pin-list\"></div>
        </div>
      </div>

      <div id=\"adminSetupPanel\" class=\"hidden\" style=\"margin-top:12px\">
        <div class=\"card\" style=\"padding:12px;border-radius:12px;border:1px solid var(--border);background:#f9fcfe;box-shadow:none\">
          <h4 style=\"margin:0 0 8px\">New Zone Device Onboarding</h4>
          <div class=\"smallnote\">Use this when installing another Raspberry Pi zone node (feed/return probes + dry-contact call sensing).</div>
          <div class=\"tabrow\" style=\"margin-top:10px\">
            <a class=\"itab\" href=\"https://flooroneproduction.tail58e171.ts.net/demo\" target=\"_blank\" rel=\"noopener\" style=\"text-decoration:none\">Setup Page Demo</a>
            <a class=\"itab\" href=\"https://github.com/eamondoherty618-prog/hvac-building-managment\" target=\"_blank\" rel=\"noopener\" style=\"text-decoration:none\">Code / GitHub</a>
          </div>
          <div style=\"margin-top:10px\">
            <div class=\"label\">Installer Flow (Recommended)</div>
            <ol style=\"margin:.4rem 0 0 1.1rem;padding:0\">
              <li>Power new zone node and wire probes + dry contact relay.</li>
              <li>Connect phone to setup hotspot (`HeatingHub-Setup-xxxx`).</li>
              <li>Open local setup page (`http://10.42.0.1:8090`).</li>
              <li>Assign unit name, zone ID, feed/return probes, and Wi-Fi.</li>
              <li>Use `Find Hub` or paste Hub URL, then save configuration.</li>
              <li>Confirm device appears in downstream zones on this hub.</li>
            </ol>
          </div>
          <div style=\"margin-top:10px\">
            <div class=\"label\">Hub URL (Use on Setup Page)</div>
            <div><code>https://165-boiler.tail58e171.ts.net/</code></div>
          </div>
          <div style=\"margin-top:10px\">
            <div class=\"label\">Phone Setup Links</div>
            <div class=\"smallnote\">On hotspot: <code>http://10.42.0.1:8090</code></div>
            <div class=\"smallnote\">On Tailscale/LAN (example node): <code>http://100.77.13.12:8090</code></div>
          </div>
        </div>
      </div>
    </section>
  </div>
  <button id=\"floatingTopBtn\" type=\"button\" class=\"fab-top\" aria-label=\"Back to top\">Back to Top</button>
  <div id=\"zoneInfoModal\" class=\"zone-info-modal\" aria-hidden=\"true\">
    <div class=\"zone-info-card\">
      <div style=\"display:flex;justify-content:space-between;align-items:center;gap:10px\">
        <h4 id=\"zoneInfoTitle\" style=\"margin:0\">Zone Information</h4>
        <button type=\"button\" class=\"btn\" id=\"zoneInfoCloseBtn\">Close</button>
      </div>
      <div class=\"zone-info-grid\" style=\"margin-top:10px\">
        <div>
          <img id=\"zoneInfoPhoto\" class=\"zone-info-photo\" alt=\"Zone equipment photo\">
        </div>
        <div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Zone / Device</div></div><div id=\"zoneInfoLabel\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Room / Area</div></div><div id=\"zoneInfoRoomArea\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Equipment Type</div></div><div id=\"zoneInfoEquip\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Thermostat Location</div></div><div id=\"zoneInfoThermostat\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Valve / Actuator</div></div><div id=\"zoneInfoValve\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Circuit / Breaker</div></div><div id=\"zoneInfoBreaker\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Installed By</div></div><div id=\"zoneInfoInstalledBy\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Install Date</div></div><div id=\"zoneInfoInstallDate\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Last Service Date</div></div><div id=\"zoneInfoLastServiceDate\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Pin / Floor Color</div></div><div id=\"zoneInfoColor\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Location</div></div><div id=\"zoneInfoCoords\">--</div></div>
          <div class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Last Updated</div></div><div id=\"zoneInfoUpdated\">--</div></div>
        </div>
      </div>
      <div style=\"margin-top:12px\">
        <div class=\"label\">Location Description / Access Notes</div>
        <div id=\"zoneInfoDesc\" style=\"margin-top:6px;white-space:pre-wrap;border:1px solid var(--border);border-radius:10px;padding:10px;background:#f8fbfd\">--</div>
      </div>
      <div style=\"margin-top:12px\">
        <div class=\"label\">Access Instructions</div>
        <div id=\"zoneInfoAccess\" style=\"margin-top:6px;white-space:pre-wrap;border:1px solid var(--border);border-radius:10px;padding:10px;background:#f8fbfd\">--</div>
      </div>
      <div id=\"zoneInfoLive\" class=\"smallnote\" style=\"margin-top:10px\"></div>
    </div>
  </div>

  <script>
    let activeRange = "live";
    let activeCycleRange = "24h";
    let activeCycleZoneId = "zone1_main";
    let cycleZoneDefs = [];
    let currentTabId = "zone1_main";
    let tabDefs = [];
    let historyPoints = [];
    let cycleData = null;
    let usersCache = [];
    let alertEventsCache = [];
    let activeAlertLogFilter = "trouble";
    let lastHubPayload = null;
    let siteMapConfig = null;
    let siteMapMap = null;
    let siteMapMarkers = {};
    let siteMapReady = false;
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

    function pinColorHex(name) {
      const m = {
        red: "#dc2626",
        blue: "#2563eb",
        green: "#16a34a",
        orange: "#ea580c",
        purple: "#7c3aed",
        gray: "#6b7280",
      };
      return m[String(name || "").toLowerCase()] || m.red;
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
        tr.innerHTML = '<td colspan="8">No downstream zones configured yet. Edit <code>/home/eamondoherty618/downstream_zones.json</code>.</td>';
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
          <td data-k="Info"><button type="button" class="btn" data-zone-info="${z.id || ""}" style="padding:4px 10px">Zone Info</button></td>
        `;
        body.appendChild(tr);
      }
      bindZoneInfoButtons();
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

    function updateCycleTabs(downstreamRows) {
      const defs = [
        { id: "zone1_main", label: "Zone 1 (Main)" },
      ];
      (downstreamRows || []).forEach((z) => {
        defs.push({ id: z.id, label: z.name || z.id });
      });
      cycleZoneDefs = defs;
      if (!cycleZoneDefs.some((z) => z.id === activeCycleZoneId)) activeCycleZoneId = "zone1_main";
      const wrap = document.getElementById("cycleZoneTabs");
      if (!wrap) return;
      wrap.innerHTML = "";
      defs.forEach((z) => {
        const b = document.createElement("button");
        b.className = "tab" + (z.id === activeCycleZoneId ? " active" : "");
        b.textContent = z.label;
        b.addEventListener("click", () => {
          activeCycleZoneId = z.id;
          updateCycleTabs(downstreamRows);
          refreshCycles();
        });
        wrap.appendChild(b);
      });
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

    function drawCycleChart(data) {
      const canvas = document.getElementById("cycleChart");
      if (!canvas) return;
      const ctx = canvas.getContext("2d");
      const w = canvas.width, h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#fbfeff";
      ctx.fillRect(0, 0, w, h);
      const pts = (data && data.points) ? data.points : [];
      if (pts.length === 0) {
        ctx.fillStyle = "#536b75";
        ctx.font = "16px sans-serif";
        ctx.fillText("No cycle data yet.", 20, 30);
        return;
      }
      const maxCycles = Math.max(1, ...pts.map(p => Number(p.cycles || 0)));
      const maxRuntime = Math.max(1, ...pts.map(p => Number(p.runtime_minutes || 0)));
      const padL = 46, padR = 14, padT = 12, padB = 42;
      const plotW = w - padL - padR;
      const plotH = h - padT - padB;
      const n = pts.length;
      const xAt = (i) => padL + (n > 1 ? (plotW * i / (n - 1)) : plotW / 2);
      const yCycles = (v) => padT + (1 - (Number(v || 0) / maxCycles)) * plotH;
      const yRuntime = (v) => padT + (1 - (Number(v || 0) / maxRuntime)) * plotH;

      ctx.strokeStyle = "#d6e2e8";
      for (let i = 0; i <= 4; i++) {
        const y = padT + (plotH * i / 4);
        ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
      }

      ctx.fillStyle = "#536b75";
      ctx.font = "11px sans-serif";
      for (let i = 0; i < n; i++) {
        if (n > 12 && i % Math.ceil(n / 8) !== 0 && i !== n - 1) continue;
        const x = xAt(i);
        const label = String(pts[i].label || "");
        ctx.save();
        ctx.translate(x, h - 8);
        ctx.rotate(-0.35);
        ctx.fillText(label, 0, 0);
        ctx.restore();
      }

      // Runtime line
      ctx.beginPath();
      ctx.strokeStyle = "#b45309";
      ctx.lineWidth = 2;
      pts.forEach((p, i) => {
        const x = xAt(i), y = yRuntime(p.runtime_minutes);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.stroke();

      // Cycle count bars
      const barW = Math.max(4, Math.min(18, plotW / Math.max(1, n) * 0.55));
      ctx.fillStyle = "rgba(37,99,235,0.75)";
      pts.forEach((p, i) => {
        const x = xAt(i) - barW / 2;
        const y = yCycles(p.cycles);
        ctx.fillRect(x, y, barW, (padT + plotH) - y);
      });
    }

    function renderCycleSummary(data) {
      const s = (data && data.summary) ? data.summary : {};
      document.getElementById("cycleCountKpi").textContent = s.cycle_count ?? "--";
      document.getElementById("cycleRuntimeKpi").textContent = (s.total_runtime_minutes ?? "--") + ((s.total_runtime_minutes ?? null) !== null ? " min" : "");
      document.getElementById("cycleAvgKpi").textContent = (s.avg_cycle_minutes ?? "--") + ((s.avg_cycle_minutes ?? null) !== null ? " min" : "");
      document.getElementById("cycleDutyKpi").textContent = (s.duty_cycle_pct ?? "--") + ((s.duty_cycle_pct ?? null) !== null ? "%" : "");
      document.getElementById("cycleShortKpi").textContent = `${s.short_cycle_count ?? "--"}${(s.short_cycle_count ?? null) !== null ? ` (${s.short_cycle_pct ?? 0}%)` : ""}`;
      document.getElementById("cycleRespLagKpi").textContent = `${s.avg_response_lag_minutes ?? "--"}${(s.avg_response_lag_minutes ?? null) !== null ? " min" : ""}`;
      document.getElementById("cycleNoRespKpi").textContent = s.no_response_count ?? "--";
      const open = data && data.open_cycle;
      document.getElementById("cycleOpenNote").textContent = (open && open.active)
        ? `Current call active: ${open.runtime_minutes} min (open cycle)` + (open.response_reached ? ` · Response reached in ${open.response_lag_minutes} min` : "")
        : "";
    }

    async function refreshCycles() {
      try {
        const res = await fetch(`/api/cycles?range=${activeCycleRange}&zone=${encodeURIComponent(activeCycleZoneId)}&_=${Date.now()}`, { cache: "no-store" });
        cycleData = await res.json();
      } catch (e) {
        cycleData = { points: [], summary: {} };
      }
      renderCycleSummary(cycleData);
      drawCycleChart(cycleData);
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
        lastHubPayload = data;
        const r = (data.main && data.main.readings) ? data.main.readings : {};
        const downstream = data.downstream || [];

        // Main readings are updated by refreshMain() so they remain visible even if hub polling is slow.
        appendLivePoint(r, downstream);
        updateTabs(downstream);
        updateCycleTabs(downstream);
        renderRows(downstream);
        refreshSiteMapZoneSelect();
        if (activeRange === "live") renderSelectedGraph();

        const trouble = Number(data.trouble_count || 0);
        const pill = document.getElementById("alertPill");
        pill.childNodes[0].nodeValue = `Trouble Zones: ${trouble}`;
        pill.style.borderColor = trouble > 0 ? "rgba(255,130,130,.8)" : "rgba(180,255,210,.8)";
        const unack = Number(data.unack_alert_count || 0);
        const upill = document.getElementById("unackPill");
        if (upill) {
          upill.childNodes[0].nodeValue = `Alerts To Acknowledge: ${unack}`;
          upill.style.borderColor = unack > 0 ? "rgba(255,130,130,.8)" : "rgba(180,255,210,.8)";
        }

        document.getElementById("updated").textContent = "Updated: " + (data.updated_utc || new Date().toISOString());
      } catch (e) {
        document.getElementById("updated").textContent = "Updated: connection lost";
      }
    }

    function siteMapZoneOptions() {
      const out = [
        { id: "zone1_main", label: "Zone 1 (Main)" },
        { id: "zone2_main", label: "Zone 2 (Main)" },
      ];
      const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
      ds.forEach((z) => {
        const zid = String((z && z.id) || "").trim();
        if (!zid) return;
        out.push({ id: zid, label: String(z.name || zid) });
      });
      return out;
    }

    function refreshSiteMapZoneSelect() {
      const sel = document.getElementById("siteMapZoneSelect");
      if (!sel) return;
      const prev = sel.value;
      const opts = siteMapZoneOptions();
      sel.innerHTML = "";
      opts.forEach((o) => {
        const op = document.createElement("option");
        op.value = o.id;
        op.textContent = o.label;
        sel.appendChild(op);
      });
      if (opts.some((o) => o.id === prev)) sel.value = prev;
      syncSiteMapDetailsFormFromSelected();
    }

    function applySiteMapStateToForm() {
      if (!siteMapConfig) return;
      document.getElementById("siteAddress").value = siteMapConfig.address || "";
      if (siteMapConfig.center && Number.isFinite(Number(siteMapConfig.center.lat)) && Number.isFinite(Number(siteMapConfig.center.lng))) {
        document.getElementById("siteMapCenter").value = `${Number(siteMapConfig.center.lat).toFixed(5)}, ${Number(siteMapConfig.center.lng).toFixed(5)}`;
      }
      if (siteMapConfig.zoom !== undefined && siteMapConfig.zoom !== null) {
        document.getElementById("siteMapZoom").value = String(Number(siteMapConfig.zoom));
      }
      syncSiteMapDetailsFormFromSelected();
    }

    function selectedSiteMapZoneId() {
      const sel = document.getElementById("siteMapZoneSelect");
      return sel ? String(sel.value || "") : "";
    }

    function getSiteMapPin(zoneId) {
      if (!zoneId || !siteMapConfig || !siteMapConfig.pins || typeof siteMapConfig.pins !== "object") return null;
      return siteMapConfig.pins[zoneId] || null;
    }

    function setSiteMapPhotoPreview(dataUrl) {
      const img = document.getElementById("siteMapPhotoPreview");
      if (!img) return;
      if (dataUrl) {
        img.src = dataUrl;
        img.style.display = "block";
      } else {
        img.removeAttribute("src");
        img.style.display = "none";
      }
    }

    function syncSiteMapDetailsFormFromSelected() {
      const zoneId = selectedSiteMapZoneId();
      const pin = getSiteMapPin(zoneId) || {};
      const colorEl = document.getElementById("siteMapPinColor");
      const equipEl = document.getElementById("siteMapEquipType");
      const roomEl = document.getElementById("siteMapRoomArea");
      const thermoEl = document.getElementById("siteMapThermostatLoc");
      const valveEl = document.getElementById("siteMapValveType");
      const breakerEl = document.getElementById("siteMapBreakerRef");
      const installedByEl = document.getElementById("siteMapInstalledBy");
      const installDateEl = document.getElementById("siteMapInstallDate");
      const lastServiceDateEl = document.getElementById("siteMapLastServiceDate");
      const descEl = document.getElementById("siteMapDesc");
      const accessEl = document.getElementById("siteMapAccessInstructions");
      if (colorEl) colorEl.value = pin.color || "red";
      if (equipEl) equipEl.value = pin.equipment_type || "";
      if (roomEl) roomEl.value = pin.room_area_name || "";
      if (thermoEl) thermoEl.value = pin.thermostat_location || "";
      if (valveEl) valveEl.value = pin.valve_actuator_type || "";
      if (breakerEl) breakerEl.value = pin.circuit_breaker_ref || "";
      if (installedByEl) installedByEl.value = pin.installed_by || "";
      if (installDateEl) installDateEl.value = pin.install_date || "";
      if (lastServiceDateEl) lastServiceDateEl.value = pin.last_service_date || "";
      if (descEl) descEl.value = pin.description || "";
      if (accessEl) accessEl.value = pin.access_instructions || "";
      setSiteMapPhotoPreview(pin.photo_data_url || "");
      const photoInput = document.getElementById("siteMapPhotoInput");
      if (photoInput) photoInput.value = "";
    }

    function renderSiteMapPinsList() {
      const el = document.getElementById("siteMapPinsList");
      if (!el) return;
      const pins = (siteMapConfig && siteMapConfig.pins && typeof siteMapConfig.pins === "object") ? siteMapConfig.pins : {};
      const labels = {};
      siteMapZoneOptions().forEach((o) => { labels[o.id] = o.label; });
      const entries = Object.entries(pins);
      if (!entries.length) {
        el.innerHTML = '<div class="smallnote">No zone/device pins saved yet.</div>';
        return;
      }
      el.innerHTML = "";
      entries.forEach(([zid, p]) => {
        const row = document.createElement("div");
        row.className = "pin-chip";
        row.innerHTML = `<div class="t"><span class="pin-dot" style="background:${pinColorHex(p.color)}"></span>${labels[zid] || p.label || zid}</div><div class="s">${Number(p.lat).toFixed(5)}, ${Number(p.lng).toFixed(5)}${p.room_area_name ? ` · ${p.room_area_name}` : ""}${p.equipment_type ? ` · ${p.equipment_type}` : ""}</div>`;
        el.appendChild(row);
      });
    }

    function renderSiteMapMarkers() {
      if (!siteMapMap || !window.L) return;
      Object.values(siteMapMarkers).forEach((m) => { try { siteMapMap.removeLayer(m); } catch (e) {} });
      siteMapMarkers = {};
      const pins = (siteMapConfig && siteMapConfig.pins && typeof siteMapConfig.pins === "object") ? siteMapConfig.pins : {};
      const labels = {};
      siteMapZoneOptions().forEach((o) => { labels[o.id] = o.label; });
      Object.entries(pins).forEach(([zid, p]) => {
        const marker = L.circleMarker([Number(p.lat), Number(p.lng)], {
          radius: 8,
          color: pinColorHex(p.color),
          weight: 2,
          fillColor: pinColorHex(p.color),
          fillOpacity: 0.85,
        }).addTo(siteMapMap);
        marker.bindPopup(`<b>${labels[zid] || p.label || zid}</b>${p.equipment_type ? `<br>${p.equipment_type}` : ""}`);
        siteMapMarkers[zid] = marker;
      });
      renderSiteMapPinsList();
    }

    function snapshotSiteMapViewport() {
      if (!siteMapMap) return;
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      const c = siteMapMap.getCenter();
      siteMapConfig.center = { lat: c.lat, lng: c.lng };
      siteMapConfig.zoom = siteMapMap.getZoom();
      document.getElementById("siteMapCenter").value = `${c.lat.toFixed(5)}, ${c.lng.toFixed(5)}`;
      document.getElementById("siteMapZoom").value = String(siteMapConfig.zoom);
    }

    function ensureSiteMapReady() {
      if (siteMapReady) return;
      refreshSiteMapZoneSelect();
      if (!window.L) { setSiteMapMsg("Map library failed to load."); return; }
      const base = siteMapConfig || { center: { lat: 41.307, lng: -72.927 }, zoom: 16, pins: {} };
      siteMapMap = L.map("siteMapCanvas");
      siteMapMap.setView([Number(base.center.lat), Number(base.center.lng)], Number(base.zoom || 16));
      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        maxZoom: 20,
        attribution: "&copy; OpenStreetMap contributors"
      }).addTo(siteMapMap);
      siteMapMap.on("click", (e) => {
        const sel = document.getElementById("siteMapZoneSelect");
        const zoneId = sel ? sel.value : "";
        if (!zoneId) { setSiteMapMsg("Select a zone/device first."); return; }
        if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
        if (!siteMapConfig.pins || typeof siteMapConfig.pins !== "object") siteMapConfig.pins = {};
        const labels = {};
        siteMapZoneOptions().forEach((o) => { labels[o.id] = o.label; });
        siteMapConfig.pins[zoneId] = {
          lat: e.latlng.lat,
          lng: e.latlng.lng,
          label: labels[zoneId] || zoneId,
          color: (getSiteMapPin(zoneId) || {}).color || (document.getElementById("siteMapPinColor")?.value || "red"),
          equipment_type: (getSiteMapPin(zoneId) || {}).equipment_type || "",
          room_area_name: (getSiteMapPin(zoneId) || {}).room_area_name || "",
          access_instructions: (getSiteMapPin(zoneId) || {}).access_instructions || "",
          installed_by: (getSiteMapPin(zoneId) || {}).installed_by || "",
          install_date: (getSiteMapPin(zoneId) || {}).install_date || "",
          last_service_date: (getSiteMapPin(zoneId) || {}).last_service_date || "",
          thermostat_location: (getSiteMapPin(zoneId) || {}).thermostat_location || "",
          valve_actuator_type: (getSiteMapPin(zoneId) || {}).valve_actuator_type || "",
          circuit_breaker_ref: (getSiteMapPin(zoneId) || {}).circuit_breaker_ref || "",
          description: (getSiteMapPin(zoneId) || {}).description || "",
          photo_data_url: (getSiteMapPin(zoneId) || {}).photo_data_url || "",
          updated_utc: new Date().toISOString()
        };
        snapshotSiteMapViewport();
        renderSiteMapMarkers();
        setSiteMapMsg(`Placed pin for ${labels[zoneId] || zoneId}. Click Save Map to store.`);
      });
      siteMapMap.on("moveend", snapshotSiteMapViewport);
      siteMapMap.on("zoomend", snapshotSiteMapViewport);
      siteMapReady = true;
      renderSiteMapMarkers();
      snapshotSiteMapViewport();
    }

    async function loadSiteMapConfig() {
      try {
        const res = await fetch(`/api/site-map?_=${Date.now()}`, { cache: "no-store" });
        siteMapConfig = await res.json();
        if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
        applySiteMapStateToForm();
        if (siteMapMap && siteMapConfig.center) {
          siteMapMap.setView([Number(siteMapConfig.center.lat), Number(siteMapConfig.center.lng)], Number(siteMapConfig.zoom || 16));
          renderSiteMapMarkers();
        }
      } catch (e) {
        setSiteMapMsg("Failed to load site map.");
      }
    }

    async function saveSiteMapConfig() {
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      siteMapConfig.address = document.getElementById("siteAddress").value.trim();
      snapshotSiteMapViewport();
      setSiteMapMsg("Saving map...");
      try {
        const res = await fetch("/api/site-map", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(siteMapConfig)
        });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "save failed");
        siteMapConfig = data;
        applySiteMapStateToForm();
        renderSiteMapMarkers();
        setSiteMapMsg("Map saved.");
      } catch (e) {
        setSiteMapMsg("Save failed: " + e.message);
      }
    }

    async function geocodeSiteAddress() {
      const q = document.getElementById("siteAddress").value.trim();
      if (!q) { setSiteMapMsg("Enter a building address first."); return; }
      setSiteMapMsg("Finding address...");
      try {
        const res = await fetch(`/api/geocode?q=${encodeURIComponent(q)}&_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        const hit = Array.isArray(data.results) ? data.results[0] : null;
        if (!hit) throw new Error("No matches found");
        if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
        siteMapConfig.address = q;
        siteMapConfig.center = { lat: Number(hit.lat), lng: Number(hit.lng) };
        siteMapConfig.zoom = Math.max(16, Number(siteMapConfig.zoom || 16));
        applySiteMapStateToForm();
        ensureSiteMapReady();
        if (siteMapMap) siteMapMap.setView([siteMapConfig.center.lat, siteMapConfig.center.lng], Number(siteMapConfig.zoom || 16));
        setSiteMapMsg("Address found. Click the map to place a zone/device pin.");
      } catch (e) {
        setSiteMapMsg("Address lookup failed: " + e.message);
      }
    }

    function clearSelectedSiteMapPin() {
      const sel = document.getElementById("siteMapZoneSelect");
      const zoneId = sel ? sel.value : "";
      if (!zoneId || !siteMapConfig || !siteMapConfig.pins || !siteMapConfig.pins[zoneId]) {
        setSiteMapMsg("No pin set for selected zone/device.");
        return;
      }
      delete siteMapConfig.pins[zoneId];
      renderSiteMapMarkers();
      syncSiteMapDetailsFormFromSelected();
      setSiteMapMsg("Selected pin removed. Click Save Map to store.");
    }

    function applySelectedSiteMapDetails() {
      const zoneId = selectedSiteMapZoneId();
      if (!zoneId) { setSiteMapMsg("Select a zone/device first."); return; }
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      if (!siteMapConfig.pins || typeof siteMapConfig.pins !== "object") siteMapConfig.pins = {};
      const pin = siteMapConfig.pins[zoneId] || { lat: null, lng: null, label: zoneId };
      pin.label = (siteMapZoneOptions().find((o) => o.id === zoneId) || {}).label || pin.label || zoneId;
      pin.color = document.getElementById("siteMapPinColor").value || "red";
      pin.equipment_type = document.getElementById("siteMapEquipType").value.trim();
      pin.room_area_name = document.getElementById("siteMapRoomArea").value.trim();
      pin.thermostat_location = document.getElementById("siteMapThermostatLoc").value.trim();
      pin.valve_actuator_type = document.getElementById("siteMapValveType").value.trim();
      pin.circuit_breaker_ref = document.getElementById("siteMapBreakerRef").value.trim();
      pin.installed_by = document.getElementById("siteMapInstalledBy").value.trim();
      pin.install_date = document.getElementById("siteMapInstallDate").value || "";
      pin.last_service_date = document.getElementById("siteMapLastServiceDate").value || "";
      pin.description = document.getElementById("siteMapDesc").value.trim();
      pin.access_instructions = document.getElementById("siteMapAccessInstructions").value.trim();
      const pendingPhoto = document.getElementById("siteMapPhotoPreview").src || "";
      if (pendingPhoto && pendingPhoto.startsWith("data:image/")) {
        pin.photo_data_url = pendingPhoto;
      } else if (!document.getElementById("siteMapPhotoPreview").getAttribute("src")) {
        pin.photo_data_url = "";
      }
      pin.updated_utc = new Date().toISOString();
      siteMapConfig.pins[zoneId] = pin;
      renderSiteMapMarkers();
      setSiteMapMsg(pin.lat === null || pin.lng === null
        ? "Details saved for selected zone/device. Click map to place a pin, then Save Map."
        : "Details applied to selected zone/device. Click Save Map to store.");
    }

    function handleSiteMapPhotoInput(evt) {
      const file = evt && evt.target && evt.target.files ? evt.target.files[0] : null;
      if (!file) return;
      if (!String(file.type || "").startsWith("image/")) {
        setSiteMapMsg("Please choose an image file.");
        return;
      }
      const reader = new FileReader();
      reader.onload = () => {
        const dataUrl = String(reader.result || "");
        setSiteMapPhotoPreview(dataUrl);
        setSiteMapMsg("Photo loaded. Click Apply Details, then Save Map.");
      };
      reader.onerror = () => setSiteMapMsg("Photo read failed.");
      reader.readAsDataURL(file);
    }

    function clearSelectedSiteMapPhoto() {
      setSiteMapPhotoPreview("");
      const input = document.getElementById("siteMapPhotoInput");
      if (input) input.value = "";
      setSiteMapMsg("Photo cleared for selected zone/device. Click Apply Details, then Save Map.");
    }

    function zoneLiveInfoById(zoneId) {
      if (zoneId === "zone1_main") {
        return {
          label: "Zone 1 (Main)",
          equipment_status: document.getElementById("zone1_status")?.textContent || "",
          call: document.getElementById("zone1_call_status")?.textContent || "",
          feed: document.getElementById("feed_1")?.textContent || "--",
          ret: document.getElementById("return_1")?.textContent || "--",
        };
      }
      if (zoneId === "zone2_main") {
        return {
          label: "Zone 2 (Main)",
          equipment_status: "",
          call: "",
          feed: document.getElementById("feed_2")?.textContent || "--",
          ret: document.getElementById("return_2")?.textContent || "--",
        };
      }
      const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
      const z = ds.find((x) => String(x.id || "") === String(zoneId || ""));
      if (!z) return null;
      return {
        label: z.name || z.id || zoneId,
        equipment_status: z.zone_status || z.status || "",
        call: z.call_status || "",
        feed: fmtTemp(z.feed_f),
        ret: fmtTemp(z.return_f),
        note: z.status_note || "",
      };
    }

    function openZoneInfoModal(zoneId) {
      const modal = document.getElementById("zoneInfoModal");
      if (!modal) return;
      const opts = siteMapZoneOptions();
      const opt = opts.find((o) => o.id === zoneId);
      const pin = getSiteMapPin(zoneId) || {};
      const live = zoneLiveInfoById(zoneId) || {};
      document.getElementById("zoneInfoTitle").textContent = (opt && opt.label) ? `${opt.label} Information` : "Zone Information";
      document.getElementById("zoneInfoLabel").textContent = (opt && opt.label) || pin.label || zoneId || "--";
      document.getElementById("zoneInfoRoomArea").textContent = pin.room_area_name || "Not set";
      document.getElementById("zoneInfoEquip").textContent = pin.equipment_type || "Not set";
      document.getElementById("zoneInfoThermostat").textContent = pin.thermostat_location || "Not set";
      document.getElementById("zoneInfoValve").textContent = pin.valve_actuator_type || "Not set";
      document.getElementById("zoneInfoBreaker").textContent = pin.circuit_breaker_ref || "Not set";
      document.getElementById("zoneInfoInstalledBy").textContent = pin.installed_by || "Not set";
      document.getElementById("zoneInfoInstallDate").textContent = pin.install_date || "Not set";
      document.getElementById("zoneInfoLastServiceDate").textContent = pin.last_service_date || "Not set";
      document.getElementById("zoneInfoColor").innerHTML = pin.color ? `<span class="pin-dot" style="background:${pinColorHex(pin.color)}"></span>${pin.color}` : "Not set";
      document.getElementById("zoneInfoCoords").textContent = (pin.lat !== undefined && pin.lng !== undefined) ? `${Number(pin.lat).toFixed(5)}, ${Number(pin.lng).toFixed(5)}` : "Pin not placed";
      document.getElementById("zoneInfoUpdated").textContent = pin.updated_utc ? String(pin.updated_utc).replace("T"," ").replace("+00:00"," UTC") : "Not set";
      document.getElementById("zoneInfoDesc").textContent = pin.description || "No location notes saved yet.";
      document.getElementById("zoneInfoAccess").textContent = pin.access_instructions || "No access instructions saved yet.";
      const img = document.getElementById("zoneInfoPhoto");
      if (pin.photo_data_url) {
        img.src = pin.photo_data_url;
        img.style.display = "block";
      } else {
        img.removeAttribute("src");
        img.style.display = "none";
      }
      const liveParts = [];
      if (live.feed || live.ret) liveParts.push(`Feed ${live.feed || "--"} · Return ${live.ret || "--"}`);
      if (live.call) liveParts.push(`Call: ${live.call}`);
      if (live.equipment_status) liveParts.push(`Status: ${live.equipment_status}`);
      if (live.note) liveParts.push(`Note: ${live.note}`);
      document.getElementById("zoneInfoLive").textContent = liveParts.join(" | ");
      modal.classList.add("show");
      modal.setAttribute("aria-hidden", "false");
    }

    function closeZoneInfoModal() {
      const modal = document.getElementById("zoneInfoModal");
      if (!modal) return;
      modal.classList.remove("show");
      modal.setAttribute("aria-hidden", "true");
    }

    function bindZoneInfoButtons() {
      document.querySelectorAll("[data-zone-info]").forEach((btn) => {
        if (btn.dataset.boundZoneInfo === "1") return;
        btn.dataset.boundZoneInfo = "1";
        btn.addEventListener("click", () => openZoneInfoModal(btn.dataset.zoneInfo || ""));
      });
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
    function initReturnToMainButtons() {
      document.querySelectorAll("[data-return-main]").forEach((btn) => {
        btn.addEventListener("click", () => {
          const top = document.getElementById("mainTop");
          if (top) top.scrollIntoView({ behavior: "smooth", block: "start" });
        });
      });
    }
    function initFloatingTopButton() {
      const btn = document.getElementById("floatingTopBtn");
      if (!btn) return;
      const toTop = () => {
        const top = document.getElementById("mainTop");
        if (top) top.scrollIntoView({ behavior: "smooth", block: "start" });
      };
      btn.addEventListener("click", toTop);
      const update = () => {
        const y = window.scrollY || document.documentElement.scrollTop || 0;
        btn.classList.toggle("show", y > 240);
      };
      window.addEventListener("scroll", update, { passive: true });
      update();
    }

    function setNotifMsg(msg) {
      const el = document.getElementById("notifMsg");
      if (el) el.textContent = msg || "";
    }
    function setUsersMsg(msg) {
      const el = document.getElementById("usersMsg");
      if (el) el.textContent = msg || "";
    }
    function setAlertsMsg(msg) {
      const el = document.getElementById("alertsMsg");
      if (el) el.textContent = msg || "";
    }
    function setSiteMapMsg(msg) {
      const el = document.getElementById("siteMapMsg");
      if (el) el.textContent = msg || "";
    }

    function setAdminTab(which) {
      document.querySelectorAll("[data-admin-tab]").forEach((b) => b.classList.toggle("active", b.dataset.adminTab === which));
      document.getElementById("adminUsersPanel").classList.toggle("hidden", which !== "users");
      document.getElementById("adminAlertsPanel").classList.toggle("hidden", which !== "alerts");
      document.getElementById("adminMapPanel").classList.toggle("hidden", which !== "map");
      document.getElementById("adminSetupPanel").classList.toggle("hidden", which !== "setup");
      if (which === "map") {
        ensureSiteMapReady();
        setTimeout(() => { if (siteMapMap) siteMapMap.invalidateSize(); }, 80);
      }
    }
    function setAlertLogFilter(which) {
      activeAlertLogFilter = which || "trouble";
      document.querySelectorAll("[data-alert-log-filter]").forEach((b) => b.classList.toggle("active", b.dataset.alertLogFilter === activeAlertLogFilter));
      renderAlertEvents(alertEventsCache);
    }

    function formatAlertType(cat) {
      const labels = {
        temp_response: "Temp Response Fault",
        short_cycle: "Short Cycling",
        response_lag: "Response Lag",
        no_response: "No Temperature Response",
        low_temp: "Low Temp",
        device_offline: "Device Offline",
        device_recovered: "Device Recovered",
      };
      return labels[cat] || cat || "--";
    }
    function isTroubleCategory(cat) {
      return String(cat || "") !== "device_recovered";
    }

    function currentUserFormPayload(existingId) {
      const alerts = {};
      document.querySelectorAll("[data-alert-pref]").forEach((cb) => { alerts[cb.dataset.alertPref] = !!cb.checked; });
      return {
        id: existingId || "",
        name: document.getElementById("userName").value.trim(),
        email: document.getElementById("userEmail").value.trim(),
        role: document.getElementById("userRole").value,
        enabled: document.getElementById("userEnabled").value === "true",
        alerts,
      };
    }

    function fillUserForm(u) {
      document.getElementById("userName").value = (u && u.name) || "";
      document.getElementById("userEmail").value = (u && u.email) || "";
      document.getElementById("userRole").value = (u && u.role) || "viewer";
      document.getElementById("userEnabled").value = (u && u.enabled === false) ? "false" : "true";
      const prefs = (u && u.alerts) || {};
      document.querySelectorAll("[data-alert-pref]").forEach((cb) => { cb.checked = !!prefs[cb.dataset.alertPref]; });
      document.getElementById("saveUserBtn").dataset.editingId = (u && u.id) || "";
    }

    function clearUserForm() {
      fillUserForm({ alerts: { temp_response:true, short_cycle:true, response_lag:true, no_response:true, low_temp:true, device_offline:true, device_recovered:true }, role:"viewer", enabled:true });
      document.getElementById("saveUserBtn").dataset.editingId = "";
    }

    function renderUsers(users) {
      usersCache = Array.isArray(users) ? users : [];
      const body = document.getElementById("usersBody");
      body.innerHTML = "";
      if (!usersCache.length) {
        body.innerHTML = '<tr><td colspan="6">No users configured.</td></tr>';
        return;
      }
      usersCache.forEach((u) => {
        const enabled = u.enabled !== false;
        const prefs = u.alerts || {};
        const subscribed = Object.keys(prefs).filter((k) => prefs[k]).map(formatAlertType).join(", ");
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${u.name || "--"}</td>
          <td>${u.email || "--"}</td>
          <td>${u.role || "viewer"}</td>
          <td>${subscribed || "None"}</td>
          <td>${enabled ? "Enabled" : "Disabled"}</td>
          <td></td>
        `;
        const td = tr.lastElementChild;
        const editBtn = document.createElement("button");
        editBtn.type = "button";
        editBtn.className = "btn";
        editBtn.style.padding = "4px 10px";
        editBtn.textContent = "Edit";
        editBtn.addEventListener("click", () => { fillUserForm(u); setAdminTab("users"); setUsersMsg(`Editing ${u.email}`); window.scrollTo({top: document.body.scrollHeight, behavior: "smooth"}); });
        td.appendChild(editBtn);
        const delBtn = document.createElement("button");
        delBtn.type = "button";
        delBtn.className = "btn";
        delBtn.style.padding = "4px 10px";
        delBtn.style.marginLeft = "6px";
        delBtn.textContent = "Delete";
        delBtn.addEventListener("click", async () => {
          if (!confirm(`Delete user ${u.email}?`)) return;
          setUsersMsg("Deleting user...");
          const res = await fetch("/api/users/delete", { method:"POST", headers:{ "Content-Type":"application/json" }, body: JSON.stringify({ id: u.id }) });
          const data = await res.json();
          if (!res.ok || data.error) { setUsersMsg("Delete failed: " + (data.error || "unknown")); return; }
          renderUsers(data.users || []);
          setUsersMsg("User deleted.");
        });
        td.appendChild(delBtn);
        body.appendChild(tr);
      });
    }

    async function loadUsers() {
      try {
        const res = await fetch(`/api/users?_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        renderUsers(data.users || []);
      } catch (e) {
        setUsersMsg("Failed to load users.");
      }
    }

    async function saveUser() {
      const editingId = document.getElementById("saveUserBtn").dataset.editingId || "";
      const user = currentUserFormPayload(editingId);
      if (!user.email) { setUsersMsg("Email is required."); return; }
      setUsersMsg("Saving user...");
      try {
        const res = await fetch("/api/users/save", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify({ user })
        });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "save failed");
        renderUsers(data.users || []);
        clearUserForm();
        setUsersMsg("User saved.");
      } catch (e) {
        setUsersMsg("Save failed: " + e.message);
      }
    }

    function renderAlertEvents(events) {
      alertEventsCache = Array.isArray(events) ? events : [];
      const body = document.getElementById("alertsBody");
      body.innerHTML = "";
      let arr = alertEventsCache.slice();
      if (activeAlertLogFilter === "trouble") {
        arr = arr.filter((ev) => isTroubleCategory(ev.category));
      } else if (activeAlertLogFilter === "unack") {
        arr = arr.filter((ev) => !ev.acknowledged);
      }
      if (!arr.length) {
        body.innerHTML = '<tr><td colspan="6">No alerts in this view.</td></tr>';
        return;
      }
      arr.forEach((ev) => {
        const tr = document.createElement("tr");
        const statusTxt = ev.acknowledged ? `Acknowledged${ev.acknowledged_by ? ` by ${ev.acknowledged_by}` : ""}` : "Unacknowledged";
        tr.innerHTML = `
          <td>${(ev.created_utc || "").replace("T"," ").replace("+00:00"," UTC")}</td>
          <td>${ev.zone_label || ev.zone_id || "--"}</td>
          <td>${formatAlertType(ev.category)}</td>
          <td>${ev.detail || ""}</td>
          <td>${statusTxt}${ev.ack_note ? `<br><span class="smallnote">${ev.ack_note}</span>` : ""}</td>
          <td></td>
        `;
        const td = tr.lastElementChild;
        if (!ev.acknowledged) {
          const b = document.createElement("button");
          b.type = "button";
          b.className = "btn";
          b.style.padding = "4px 10px";
          b.textContent = "Acknowledge";
          b.addEventListener("click", async () => {
            const who = document.getElementById("ackBy").value.trim();
            const note = document.getElementById("ackNote").value.trim();
            setAlertsMsg("Acknowledging alert...");
            const res = await fetch("/api/alerts/ack", {
              method:"POST",
              headers:{ "Content-Type":"application/json" },
              body: JSON.stringify({ id: ev.id, who, note })
            });
            const data = await res.json();
            if (!res.ok || data.error || data.ok === false) {
              setAlertsMsg("Acknowledge failed: " + (data.error || data.message || "unknown"));
              return;
            }
            renderAlertEvents(data.events || []);
            setAlertsMsg("Alert acknowledged.");
          });
          td.appendChild(b);
        } else {
          td.textContent = "—";
        }
      });
    }

    async function loadAlertEvents() {
      const limit = document.getElementById("alertLimit").value || "50";
      try {
        const res = await fetch(`/api/alerts/events?limit=${encodeURIComponent(limit)}&_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        renderAlertEvents(data.events || []);
      } catch (e) {
        setAlertsMsg("Failed to load alert events.");
      }
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
    function initCycleButtons() {
      document.querySelectorAll("#cycleBtns .btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          activeCycleRange = btn.dataset.cycleRange;
          document.querySelectorAll("#cycleBtns .btn").forEach((b) => b.classList.remove("active"));
          btn.classList.add("active");
          refreshCycles();
        });
      });
    }
    function initAdminTabs() {
      document.querySelectorAll("[data-admin-tab]").forEach((btn) => {
        btn.addEventListener("click", () => setAdminTab(btn.dataset.adminTab));
      });
    }
    function initAlertLogFilters() {
      document.querySelectorAll("[data-alert-log-filter]").forEach((btn) => {
        btn.addEventListener("click", () => setAlertLogFilter(btn.dataset.alertLogFilter));
      });
    }
    function openAlertPanelWithFilter(filterName) {
      setAdminTab("alerts");
      setAlertLogFilter(filterName);
      const panel = document.getElementById("adminAlertsPanel");
      if (panel) panel.scrollIntoView({ behavior: "smooth", block: "start" });
      loadAlertEvents();
    }
    function initHeaderActions() {
      const troubleBtn = document.getElementById("alertPill");
      const unackBtn = document.getElementById("unackPill");
      const addDeviceBtn = document.getElementById("addDevicePill");
      if (troubleBtn) troubleBtn.addEventListener("click", () => openAlertPanelWithFilter("trouble"));
      if (unackBtn) unackBtn.addEventListener("click", () => openAlertPanelWithFilter("unack"));
      if (addDeviceBtn) {
        addDeviceBtn.addEventListener("click", () => {
          setAdminTab("setup");
          const panel = document.getElementById("adminSetupPanel");
          if (panel) panel.scrollIntoView({ behavior: "smooth", block: "start" });
        });
      }
    }

    initButtons();
    initReturnToMainButtons();
    initFloatingTopButton();
    initCycleButtons();
    initAdminTabs();
    initAlertLogFilters();
    initHeaderActions();
    bindZoneInfoButtons();
    document.getElementById("siteMapFindBtn").addEventListener("click", geocodeSiteAddress);
    document.getElementById("siteMapSaveBtn").addEventListener("click", saveSiteMapConfig);
    document.getElementById("siteMapClearPinBtn").addEventListener("click", clearSelectedSiteMapPin);
    document.getElementById("siteMapApplyDetailsBtn").addEventListener("click", applySelectedSiteMapDetails);
    document.getElementById("siteMapClearPhotoBtn").addEventListener("click", clearSelectedSiteMapPhoto);
    document.getElementById("siteMapPhotoInput").addEventListener("change", handleSiteMapPhotoInput);
    document.getElementById("siteMapZoneSelect").addEventListener("change", syncSiteMapDetailsFormFromSelected);
    document.getElementById("siteMapZoom").addEventListener("change", () => {
      const z = Number(document.getElementById("siteMapZoom").value || 16);
      if (siteMapMap && Number.isFinite(z)) siteMapMap.setZoom(Math.max(1, Math.min(22, z)));
    });
    document.getElementById("zoneInfoCloseBtn").addEventListener("click", closeZoneInfoModal);
    document.getElementById("zoneInfoModal").addEventListener("click", (e) => {
      if (e.target && e.target.id === "zoneInfoModal") closeZoneInfoModal();
    });
    document.getElementById("saveNotifBtn").addEventListener("click", saveNotifications);
    document.getElementById("testNotifBtn").addEventListener("click", testNotifications);
    document.getElementById("saveUserBtn").addEventListener("click", saveUser);
    document.getElementById("clearUserFormBtn").addEventListener("click", clearUserForm);
    document.getElementById("refreshAlertsBtn").addEventListener("click", loadAlertEvents);
    document.getElementById("alertLimit").addEventListener("change", loadAlertEvents);
    clearUserForm();
    setAlertLogFilter("trouble");
    refreshMain();
    refreshHub();
    refreshHistory();
    refreshCycles();
    loadNotifications();
    loadUsers();
    loadAlertEvents();
    loadSiteMapConfig();
    setInterval(refreshMain, 3000);
    setInterval(refreshHub, 10000);
    setInterval(refreshHistory, 60000);
    setInterval(refreshCycles, 30000);
    setInterval(loadAlertEvents, 30000);
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

        if route == "/api/cycles":
            window = query.get("range", ["24h"])[0]
            zone_id = query.get("zone", ["zone1_main"])[0]
            b = json.dumps(cycle_analytics_for_window(window, zone_id)).encode("utf-8")
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

        if route == "/api/users":
            b = json.dumps(public_users_config()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/alerts/events":
            try:
                limit = int(query.get("limit", ["100"])[0])
            except Exception:
                limit = 100
            b = json.dumps({"events": recent_alert_events(limit=limit), "updated_utc": now_utc_iso()}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/site-map":
            b = json.dumps(load_site_map_config()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/geocode":
            q = str(query.get("q", [""])[0]).strip()
            if not q:
                b = json.dumps({"error": "q required", "results": []}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            try:
                url = "https://nominatim.openstreetmap.org/search?" + urlencode({
                    "q": q,
                    "format": "jsonv2",
                    "limit": 5,
                })
                req = Request(url, headers={"User-Agent": "165-water-street-heating-hub/1.0"})
                with urlopen(req, timeout=8) as resp:
                    raw = resp.read().decode("utf-8")
                parsed_results = json.loads(raw)
                results = []
                if isinstance(parsed_results, list):
                    for r in parsed_results[:5]:
                        if not isinstance(r, dict):
                            continue
                        results.append({
                            "display_name": str(r.get("display_name") or ""),
                            "lat": r.get("lat"),
                            "lng": r.get("lon"),
                        })
                b = json.dumps({"query": q, "results": results}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            except Exception as e:
                b = json.dumps({"error": f"geocode failed: {e}", "results": []}).encode("utf-8")
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        route = parsed.path

        if route not in (
            "/api/notifications",
            "/api/notifications/test-email",
            "/api/users/save",
            "/api/users/delete",
            "/api/alerts/ack",
            "/api/site-map",
        ):
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

        if route == "/api/users/save":
            users_cfg = load_users_config()
            users = users_cfg.get("users", [])
            if not isinstance(users, list):
                users = []
            user = body.get("user", {}) if isinstance(body.get("user"), dict) else body
            name = str(user.get("name") or "").strip()
            email = str(user.get("email") or "").strip()
            role = str(user.get("role") or "viewer").strip() or "viewer"
            if not email:
                b = json.dumps({"error": "email is required"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            alerts_in = user.get("alerts", {}) if isinstance(user.get("alerts"), dict) else {}
            alerts = dict(DEFAULT_ALERT_PREFS)
            for k in DEFAULT_ALERT_PREFS:
                if k in alerts_in:
                    alerts[k] = bool(alerts_in[k])
            enabled = bool(user.get("enabled", True))
            uid = str(user.get("id") or "").strip() or f"user-{uuid.uuid4().hex[:8]}"
            updated = False
            for i, u in enumerate(users):
                if not isinstance(u, dict):
                    continue
                if str(u.get("id")) == uid or (email and str(u.get("email") or "").lower() == email.lower()):
                    users[i] = {"id": uid, "name": name, "email": email, "role": role, "alerts": alerts, "enabled": enabled}
                    updated = True
                    break
            if not updated:
                users.append({"id": uid, "name": name, "email": email, "role": role, "alerts": alerts, "enabled": enabled})
            users_cfg["users"] = users
            if not save_users_config(users_cfg):
                b = json.dumps({"error": "failed to save users"}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            b = json.dumps({"ok": True, "users": public_users_config().get("users", [])}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/users/delete":
            user_id = str(body.get("id") or "").strip()
            if not user_id:
                b = json.dumps({"error": "id is required"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            users_cfg = load_users_config()
            users = [u for u in users_cfg.get("users", []) if not (isinstance(u, dict) and str(u.get("id")) == user_id)]
            users_cfg["users"] = users
            if not save_users_config(users_cfg):
                b = json.dumps({"error": "failed to save users"}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            b = json.dumps({"ok": True, "users": public_users_config().get("users", [])}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/alerts/ack":
            event_id = str(body.get("id") or "").strip()
            who = str(body.get("who") or "").strip()
            note = str(body.get("note") or "").strip()
            ok, msg = acknowledge_alert_event(event_id, who=who, note=note)
            code = 200 if ok else 400
            b = json.dumps({"ok": ok, "message": msg, "events": recent_alert_events(limit=100)}).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/site-map":
            cfg = load_site_map_config()
            if isinstance(body, dict):
                cfg["address"] = str(body.get("address") or cfg.get("address") or "")
                c = body.get("center", {})
                if isinstance(c, dict):
                    try:
                        cfg["center"] = {"lat": float(c.get("lat")), "lng": float(c.get("lng"))}
                    except Exception:
                        pass
                try:
                    cfg["zoom"] = max(1, min(22, int(body.get("zoom", cfg.get("zoom", 16)))))
                except Exception:
                    pass
                pins_in = body.get("pins", {})
                if isinstance(pins_in, dict):
                    pins_out = {}
                    for zid, p in pins_in.items():
                        if not isinstance(p, dict):
                            continue
                        try:
                            lat = float(p.get("lat"))
                            lng = float(p.get("lng"))
                        except Exception:
                            continue
                        pins_out[str(zid)] = {
                            "lat": lat,
                            "lng": lng,
                            "label": str(p.get("label") or str(zid)),
                            "color": str(p.get("color") or "red"),
                            "equipment_type": str(p.get("equipment_type") or ""),
                            "room_area_name": str(p.get("room_area_name") or ""),
                            "access_instructions": str(p.get("access_instructions") or ""),
                            "installed_by": str(p.get("installed_by") or ""),
                            "install_date": str(p.get("install_date") or ""),
                            "last_service_date": str(p.get("last_service_date") or ""),
                            "thermostat_location": str(p.get("thermostat_location") or ""),
                            "valve_actuator_type": str(p.get("valve_actuator_type") or ""),
                            "circuit_breaker_ref": str(p.get("circuit_breaker_ref") or ""),
                            "description": str(p.get("description") or ""),
                            "photo_data_url": str(p.get("photo_data_url") or ""),
                            "updated_utc": str(p.get("updated_utc") or now_utc_iso()),
                        }
                    cfg["pins"] = pins_out
            if not save_site_map_config(cfg):
                b = json.dumps({"error": "failed to save site map"}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            b = json.dumps(load_site_map_config()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
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
    prune_call_cycles()
    sampler = threading.Thread(target=sample_loop, daemon=True)
    sampler.start()
    ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()
