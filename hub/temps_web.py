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
COMMISSIONING_CONFIG_PATH = Path("/home/eamondoherty618/hub_commissioning.json")
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
    "hardware_fault": True,
    "hardware_recovered": True,
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
downstream_hardware_state = {}

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
        "map_layer": "street",
        "building_outline": None,
        "pins": {},
        "updated_utc": now_utc_iso(),
    }


def default_commissioning_config():
    return {"records": {}, "updated_utc": now_utc_iso()}


def load_commissioning_config():
    if not COMMISSIONING_CONFIG_PATH.exists():
        cfg = default_commissioning_config()
        save_commissioning_config(cfg)
        return cfg
    try:
        data = json.loads(COMMISSIONING_CONFIG_PATH.read_text())
        if not isinstance(data, dict):
            raise ValueError("not dict")
        out = default_commissioning_config()
        recs = data.get("records", {})
        if isinstance(recs, dict):
            norm = {}
            for zid, rec in recs.items():
                if not isinstance(rec, dict):
                    continue
                checks = rec.get("checks", {}) if isinstance(rec.get("checks"), dict) else {}
                norm[str(zid)] = {
                    "checks": {
                        "call_input_verified": bool(checks.get("call_input_verified", False)),
                        "feed_return_confirmed": bool(checks.get("feed_return_confirmed", False)),
                        "temp_response_verified": bool(checks.get("temp_response_verified", False)),
                        "photo_added": bool(checks.get("photo_added", False)),
                        "pin_placed": bool(checks.get("pin_placed", False)),
                        "alert_test_completed": bool(checks.get("alert_test_completed", False)),
                    },
                    "notes": str(rec.get("notes") or ""),
                    "completed_by": str(rec.get("completed_by") or ""),
                    "completed_utc": str(rec.get("completed_utc") or ""),
                    "updated_utc": str(rec.get("updated_utc") or now_utc_iso()),
                }
            out["records"] = norm
        out["updated_utc"] = str(data.get("updated_utc") or now_utc_iso())
        return out
    except Exception:
        cfg = default_commissioning_config()
        save_commissioning_config(cfg)
        return cfg


def save_commissioning_config(cfg):
    try:
        COMMISSIONING_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        COMMISSIONING_CONFIG_PATH.write_text(json.dumps(cfg, indent=2) + "\n")
        return True
    except Exception:
        return False


def backup_export_bundle():
    alert_cfg = load_alert_config()
    alert_export = json.loads(json.dumps(alert_cfg)) if isinstance(alert_cfg, dict) else {}
    if isinstance(alert_export.get("email"), dict) and "password" in alert_export["email"]:
        alert_export["email"]["password"] = ""
    if isinstance(alert_export.get("sms"), dict):
        for k in ("auth_token", "token", "account_sid", "sid"):
            if k in alert_export["sms"]:
                alert_export["sms"][k] = ""
    return {
        "format": "hvac-hub-backup-v1",
        "created_utc": now_utc_iso(),
        "hub_site_name": HUB_SITE_NAME,
        "public_hub_url": PUBLIC_HUB_URL,
        "downstream_zones": {"zones": load_config()},
        "users": load_users_config(),
        "site_map": load_site_map_config(),
        "commissioning": load_commissioning_config(),
        "alerts": alert_export,
    }


def restore_from_backup_bundle(bundle):
    if not isinstance(bundle, dict):
        return False, "backup payload must be an object"
    if str(bundle.get("format") or "") != "hvac-hub-backup-v1":
        return False, "unsupported backup format"
    if isinstance(bundle.get("downstream_zones"), dict):
        zones = bundle["downstream_zones"].get("zones", [])
        if isinstance(zones, list):
            if not save_config_zones(zones):
                return False, "failed to restore downstream zones"
    if isinstance(bundle.get("users"), dict):
        if not save_users_config(bundle["users"]):
            return False, "failed to restore users"
    if isinstance(bundle.get("site_map"), dict):
        if not save_site_map_config(bundle["site_map"]):
            return False, "failed to restore site map"
    if isinstance(bundle.get("commissioning"), dict):
        if not save_commissioning_config(bundle["commissioning"]):
            return False, "failed to restore commissioning"
    if isinstance(bundle.get("alerts"), dict):
        current = load_alert_config()
        incoming = json.loads(json.dumps(bundle["alerts"]))
        # Preserve existing secrets unless explicitly supplied.
        if isinstance(incoming.get("email"), dict) and isinstance(current.get("email"), dict):
            if not str(incoming["email"].get("password") or "").strip():
                incoming["email"]["password"] = current["email"].get("password", "")
        if isinstance(incoming.get("sms"), dict) and isinstance(current.get("sms"), dict):
            for k in ("auth_token", "token", "account_sid", "sid"):
                if not str(incoming["sms"].get(k) or "").strip():
                    incoming["sms"][k] = current["sms"].get(k, "")
        if not save_alert_config(incoming):
            return False, "failed to restore alerts config"
    return True, "Backup restored"


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
        cfg["map_layer"] = str(data.get("map_layer") or "street")
        bo = data.get("building_outline")
        if isinstance(bo, dict) and bo.get("type") in ("Polygon", "MultiPolygon"):
            cfg["building_outline"] = bo
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
                "pump_model": str(p.get("pump_model") or ""),
                "pump_voltage": str(p.get("pump_voltage") or ""),
                "pump_speed_mode": str(p.get("pump_speed_mode") or ""),
                "pump_serial": str(p.get("pump_serial") or ""),
                "pump_service_notes": str(p.get("pump_service_notes") or ""),
                "circuit_breaker_ref": str(p.get("circuit_breaker_ref") or ""),
                "description": str(p.get("description") or ""),
                "photo_data_url": str(p.get("photo_data_url") or ""),
                "polygon": p.get("polygon") if isinstance(p.get("polygon"), list) else [],
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
            cfg_out["map_layer"] = str(cfg.get("map_layer") or "street")
            bo = cfg.get("building_outline")
            if isinstance(bo, dict) and bo.get("type") in ("Polygon", "MultiPolygon"):
                cfg_out["building_outline"] = bo
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
                        "pump_model": str(p.get("pump_model") or ""),
                        "pump_voltage": str(p.get("pump_voltage") or ""),
                        "pump_speed_mode": str(p.get("pump_speed_mode") or ""),
                        "pump_serial": str(p.get("pump_serial") or ""),
                        "pump_service_notes": str(p.get("pump_service_notes") or ""),
                        "circuit_breaker_ref": str(p.get("circuit_breaker_ref") or ""),
                        "description": str(p.get("description") or ""),
                        "photo_data_url": str(p.get("photo_data_url") or ""),
                        "polygon": [],
                        "updated_utc": str(p.get("updated_utc") or now_utc_iso()),
                    }
                    poly = p.get("polygon")
                    if isinstance(poly, list):
                        norm_poly = []
                        for pt in poly:
                            if not isinstance(pt, dict):
                                continue
                            try:
                                norm_poly.append({"lat": float(pt.get("lat")), "lng": float(pt.get("lng"))})
                            except Exception:
                                continue
                        cfg_out["pins"][str(zid)]["polygon"] = norm_poly
        cfg_out["updated_utc"] = now_utc_iso()
        SITE_MAP_CONFIG_PATH.write_text(json.dumps(cfg_out, indent=2) + "\n")
        return True
    except Exception:
        return False


def recipients_for_alert_code(alert_code, alert_cfg):
    # Route specialized hardware diagnostics through the existing hardware subscriptions.
    if alert_code == "probe_swap_suspected":
        alert_code = "hardware_fault"
    elif alert_code == "probe_swap_recovered":
        alert_code = "hardware_recovered"
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


def maybe_send_downstream_hardware_alerts(downstream_rows):
    global downstream_hardware_state
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
        diag = z.get("diagnostics", {}) if isinstance(z.get("diagnostics"), dict) else {}
        configured = bool(z.get("configured"))
        if not configured:
            # Ignore unconfigured node hardware faults in operational alerts.
            downstream_hardware_state[zone_id] = {"fault": False, "last_sent": downstream_hardware_state.get(zone_id, {}).get("last_sent"), "alerted": False}
            continue
        faults = []
        if z.get("feed_error"):
            faults.append(f"Feed probe error: {z.get('feed_error')}")
        if z.get("return_error"):
            faults.append(f"Return probe error: {z.get('return_error')}")
        if isinstance(diag.get("hardware_faults"), list):
            for f in diag.get("hardware_faults"):
                fs = str(f or "").strip()
                if fs and fs not in faults:
                    faults.append(fs)
        probe_swap_suspected = bool(diag.get("probe_swap_suspected"))
        probe_swap_detail = str(diag.get("probe_swap_detail") or "").strip()
        fault_active = bool(diag.get("hardware_fault")) or bool(faults)
        state = downstream_hardware_state.get(zone_id, {"fault": False, "last_sent": None, "alerted": False, "kind": None})
        if fault_active and not state.get("fault"):
            last_sent = state.get("last_sent")
            if not (isinstance(last_sent, datetime) and (now - last_sent).total_seconds() < cooldown_sec):
                detail = "; ".join(faults) or "Hardware self-diagnostic reported an issue"
                if probe_swap_suspected:
                    detail = probe_swap_detail or detail or "Probe swap suspected"
                    subject = f"165 Water Street Heating Hub - Probe Swap Suspected ({zone_label})"
                    body = (
                        "165 Water Street Heating Hub Alert\n\n"
                        f"Zone: {zone_label}\n"
                        f"Zone ID: {zone_id}\n"
                        "Alert Type: Probe Swap Suspected\n"
                        f"Error Description: {detail}\n"
                        "Recommendation: Verify feed/return probe placement or use Swap Feed / Return in Zone Management.\n"
                        f"Time: {now.isoformat()}\n\n"
                        f"Live Hub: {PUBLIC_HUB_URL}\n"
                    )
                    dispatch_alert_by_category("probe_swap_suspected", zone_id, zone_label, subject, body, f"probe_swap_suspected:{zone_id}", detail)
                    state["kind"] = "probe_swap"
                else:
                    subject = f"165 Water Street Heating Hub - Hardware Fault ({zone_label})"
                    body = (
                        "165 Water Street Heating Hub Alert\n\n"
                        f"Zone: {zone_label}\n"
                        f"Zone ID: {zone_id}\n"
                        "Alert Type: Hardware Fault\n"
                        f"Error Description: {detail}\n"
                        f"Time: {now.isoformat()}\n\n"
                        f"Live Hub: {PUBLIC_HUB_URL}\n"
                    )
                    dispatch_alert_by_category("hardware_fault", zone_id, zone_label, subject, body, f"hardware_fault:{zone_id}", detail)
                    state["kind"] = "hardware"
                state["last_sent"] = now
                state["alerted"] = True
        elif (not fault_active) and state.get("fault") and state.get("alerted"):
            if state.get("kind") == "probe_swap":
                detail = "Probe swap suspicion cleared"
                subject = f"165 Water Street Heating Hub - Probe Swap Recovered ({zone_label})"
                body = (
                    "165 Water Street Heating Hub Alert\n\n"
                    f"Zone: {zone_label}\n"
                    f"Zone ID: {zone_id}\n"
                    "Alert Type: Probe Swap Recovered\n"
                    "Status: Probe swap diagnostic is now normal\n"
                    f"Time: {now.isoformat()}\n\n"
                    f"Live Hub: {PUBLIC_HUB_URL}\n"
                )
                dispatch_alert_by_category("probe_swap_recovered", zone_id, zone_label, subject, body, f"probe_swap_recovered:{zone_id}", detail)
            else:
                detail = "Hardware self-diagnostic normal"
                subject = f"165 Water Street Heating Hub - Hardware Recovered ({zone_label})"
                body = (
                    "165 Water Street Heating Hub Alert\n\n"
                    f"Zone: {zone_label}\n"
                    f"Zone ID: {zone_id}\n"
                    "Alert Type: Hardware Recovered\n"
                    "Status: Hardware self-diagnostic normal\n"
                    f"Time: {now.isoformat()}\n\n"
                    f"Live Hub: {PUBLIC_HUB_URL}\n"
                )
                dispatch_alert_by_category("hardware_recovered", zone_id, zone_label, subject, body, f"hardware_recovered:{zone_id}", detail)
            state["last_sent"] = now
            state["alerted"] = False
            state["kind"] = None
        state["fault"] = fault_active
        downstream_hardware_state[zone_id] = state


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
            return "Calling, Normal", f"Feed/return temperatures are within expected range ({delta:.1f}F difference)", False
        return "Notify Admin", f"Feed/return temperatures are not matching expected response ({delta:.1f}F difference; alert threshold {threshold_f:.1f}F)", True

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


def save_config_zones(zones):
    try:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps({"zones": zones}, indent=2) + "\n")
        return True
    except Exception:
        return False


def zone_config_entry(zone_id):
    zid = str(zone_id or "").strip()
    if not zid:
        return None
    for z in load_config():
        if isinstance(z, dict) and str(z.get("id") or "").strip() == zid:
            return dict(z)
    return None


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


def post_remote_json(url, payload, timeout=5):
    try:
        data = json.dumps(payload or {}).encode("utf-8")
        req = Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=timeout) as r:
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
            return "normal", "Calling Active", f"Temperatures responding normally ({delta:.1f}F difference)"
        return "trouble", "Call Active - No Temperature Response", f"Temperatures not responding as expected ({delta:.1f}F difference; limit {delta_ok_f:.1f}F)"
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
            "configured": None,
            "feed_error": None,
            "return_error": None,
            "diagnostics": {},
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
                if isinstance(raw, dict):
                    item["configured"] = raw.get("configured")
                    item["feed_error"] = raw.get("feed_error")
                    item["return_error"] = raw.get("return_error")
                    if isinstance(raw.get("diagnostics"), dict):
                        item["diagnostics"] = raw.get("diagnostics")
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
            "call": z.get("heating_call"),
        }
    return {
        "ts": now_utc_iso(),
        "main": {
            "feed_1": main.get("feed_1", {}).get("temp_f"),
            "return_1": main.get("return_1", {}).get("temp_f"),
            "feed_2": main.get("feed_2", {}).get("temp_f"),
            "return_2": main.get("return_2", {}).get("temp_f"),
            "call_zone1": local.get("main_call_24vac") if isinstance(local, dict) else None,
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
            try:
                maybe_send_downstream_hardware_alerts(downstream)
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
    .head-pill-btn.attn{border-color:rgba(255,120,120,.95); background:rgba(165,29,45,.28)}
    .head-pill-btn .badge-dot{display:none;margin-left:6px;min-width:18px;height:18px;border-radius:999px;background:#ff6b6b;color:#fff;font-size:.72rem;line-height:18px;text-align:center;font-weight:800;vertical-align:middle;padding:0 4px}
    .head-pill-btn.attn .badge-dot{display:inline-block}
    .head-action-wrap{position:relative}
    .head-menu{position:absolute;right:0;top:calc(100% + 8px);min-width:240px;background:#fff;color:#16303b;border:1px solid var(--border);border-radius:12px;box-shadow:0 14px 28px rgba(10,31,44,.18);padding:8px;z-index:1500;display:none}
    .head-menu.show{display:block}
    .head-menu .mitem{display:block;width:100%;text-align:left;background:#fff;border:1px solid transparent;border-radius:10px;padding:9px 10px;font-weight:700;color:#1f3340;cursor:pointer}
    .head-menu .mitem:hover{background:#f4fafc;border-color:var(--border)}
    .head-menu .mitem .sub{display:block;font-size:.78rem;color:var(--muted);font-weight:600;margin-top:2px}

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
    .admin-tools-wrap{margin-top:10px}
    .admin-tools-wrap > summary{cursor:pointer;font-weight:700;list-style:none;display:flex;align-items:center;justify-content:space-between;gap:8px;padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:#fff}
    .admin-tools-wrap > summary::-webkit-details-marker{display:none}
    .admin-tools-wrap > summary::after{content:"Open";font-size:.8rem;color:var(--muted);font-weight:600}
    .admin-tools-wrap[open] > summary::after{content:"Hide"}
    .admin-tools-wrap .admin-tools-inner{margin-top:10px}
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
    .overview-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin-top:10px}
    .overview-kpi{border:1px solid var(--border);border-radius:12px;padding:10px;background:#f8fbfd}
    .overview-kpi .k{font-size:.78rem;color:var(--muted);font-weight:700;text-transform:uppercase;letter-spacing:.03em}
    .overview-kpi .v{font-size:1.15rem;font-weight:800;margin-top:4px}
    .overview-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px;margin-top:10px}
    .overview-card{border:1px solid var(--border);border-radius:12px;padding:10px;background:#fff}
    .overview-card .t{font-weight:800}
    .overview-card .s{font-size:.85rem;color:var(--muted)}
    .overview-card .line{font-size:.9rem;margin-top:4px}
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
        <div class=\"head-action-wrap\">
          <button id=\"tasksPill\" type=\"button\" class=\"head-pill-btn\" aria-expanded=\"false\" aria-controls=\"tasksMenu\">Tasks: -- <span class=\"badge-dot\" id=\"tasksPillDot\">!</span><span class=\"sub\">Open Tasks Menu</span></button>
          <div id=\"tasksMenu\" class=\"head-menu\" role=\"menu\" aria-label=\"Tasks Menu\">
            <button type=\"button\" class=\"mitem\" id=\"tasksMenuUnack\" role=\"menuitem\">Acknowledge Alerts<span class=\"sub\">Open alerts that still need acknowledgement</span></button>
            <button type=\"button\" class=\"mitem\" id=\"tasksMenuAckAll\" role=\"menuitem\">Acknowledge All Visible<span class=\"sub\">Bulk acknowledge alerts in the current visible tasks view</span></button>
            <button type=\"button\" class=\"mitem\" id=\"tasksMenuTrouble\" role=\"menuitem\">Trouble Log<span class=\"sub\">Open current trouble events and active issues</span></button>
            <button type=\"button\" class=\"mitem\" id=\"tasksMenuAll\" role=\"menuitem\">Alert Log<span class=\"sub\">Open all recent alert events</span></button>
          </div>
        </div>
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
        <h3>Zone Performance Overview</h3>
        <div class=\"smallnote\">Click a parent zone to see its downstream devices and quick performance stats, then jump to the graph.</div>
      </div>
      <div id=\"overviewParentTabs\" class=\"tabs\" style=\"margin-top:10px\">
        <button class=\"tab active\" data-overview-parent=\"Zone 1\">Zone 1</button>
        <button class=\"tab\" data-overview-parent=\"Zone 2\">Zone 2</button>
      </div>
      <div id=\"overviewKpis\" class=\"overview-grid\"></div>
      <div id=\"overviewCards\" class=\"overview-list\"></div>
    </section>

    <section class=\"chart-wrap\">
      <div class=\"chart-head\">
        <h3>Admin Tools</h3>
        <div class=\"smallnote\">Manage who receives which alerts and acknowledge dispatched alarms</div>
      </div>
      <div style=\"display:flex;justify-content:flex-end;margin-top:8px\">
        <button type=\"button\" class=\"btn\" data-return-main>Return to Main</button>
      </div>
      <details id=\"adminToolsDetails\" class=\"admin-tools-wrap\">
        <summary>Admin Tools</summary>
        <div class=\"admin-tools-inner\">
      <div class=\"tabs\" id=\"adminTabs\" style=\"margin-top:10px\">
        <button class=\"tab active\" data-admin-tab=\"users\">Users</button>
        <button class=\"tab\" data-admin-tab=\"alerts\">Alarm Acknowledge</button>
        <button class=\"tab\" data-admin-tab=\"zones\">Zone Management</button>
        <button class=\"tab\" data-admin-tab=\"commissioning\">Commissioning</button>
        <button class=\"tab\" data-admin-tab=\"backup\">Backup / Restore</button>
        <button class=\"tab\" data-admin-tab=\"map\">Site Map</button>
        <button class=\"tab\" data-admin-tab=\"setup\">Setup New Device</button>
      </div>

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
        <span><span class=\"dot\" style=\"background:#93c5fd\"></span>24VAC Call Active</span>
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

    <section id=\"downstreamEmptyState\" class=\"chart-wrap hidden\">
      <div class=\"chart-head\">
        <h3>Heating Zones</h3>
        <div class=\"smallnote\">No downstream devices adopted yet</div>
      </div>
      <div style=\"margin-top:10px;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap\">
        <div>Set up your first zone device and assign it to a zone to start seeing downstream readings and cycle analytics.</div>
        <button type=\"button\" class=\"btn active\" id=\"firstZoneSetupBtn\">Set Up Your First Zone</button>
      </div>
    </section>

    <section id=\"subzonesSection\" class=\"subzones\">
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
          <label class=\"label\">Alert Recipients</label>
          <div class=\"smallnote\">Recipients are managed in the <strong>Users</strong> tab. Add users there, then choose roles and which alert types each user receives.</div>
          <div style=\"margin-top:8px\">
            <button type=\"button\" class=\"btn\" id=\"openUsersFromNotifBtn\">Manage Users</button>
          </div>
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

      <div id=\"adminUsersPanel\" style=\"margin-top:12px\">
        <div class=\"smallnote\" style=\"margin-bottom:8px\">Manage users, roles, and alert subscriptions.</div>
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
              <label><input type=\"checkbox\" data-alert-pref=\"hardware_fault\" checked> Hardware Fault</label>
              <label><input type=\"checkbox\" data-alert-pref=\"hardware_recovered\" checked> Hardware Recovered</label>
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
            <div class=\"smallnote\">Optional. You can use Quick Ack without entering a name or note.</div>
          </div>
          <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
            <button type=\"button\" class=\"btn\" id=\"refreshAlertsBtn\">Refresh Alerts</button>
            <button type=\"button\" class=\"btn\" id=\"ackAllVisibleBtn\">Acknowledge All Visible</button>
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

      <div id=\"adminZonesPanel\" class=\"hidden\" style=\"margin-top:12px\">
        <div class=\"card\" style=\"padding:12px;border-radius:12px;border:1px solid var(--border);background:#f9fcfe;box-shadow:none\">
          <h4 style=\"margin:0 0 8px\">Zone Management (Adopted Devices)</h4>
          <div class=\"smallnote\">Post-install changes and diagnostics for adopted zone nodes. Use this instead of the setup page for normal service updates.</div>
          <div class=\"form-grid\" style=\"margin-top:10px\">
            <div>
              <label class=\"label\" for=\"zoneMgmtSelect\">Zone Device</label>
              <select id=\"zoneMgmtSelect\" class=\"select\"></select>
            </div>
            <div style=\"display:flex;align-items:end;gap:8px\">
              <button type=\"button\" class=\"btn\" id=\"zoneMgmtRefreshBtn\">Refresh Diagnostics</button>
              <button type=\"button\" class=\"btn\" id=\"zoneMgmtSwapBtn\">Swap Feed / Return</button>
            </div>
            <div>
              <label class=\"label\" for=\"zoneMgmtName\">Display Name</label>
              <input id=\"zoneMgmtName\" class=\"input\" placeholder=\"Production Area Heating Coil 1\">
            </div>
            <div>
              <label class=\"label\" for=\"zoneMgmtParent\">Parent Zone</label>
              <select id=\"zoneMgmtParent\" class=\"select\">
                <option>Zone 1</option>
                <option>Zone 2</option>
                <option>Zone 3</option>
                <option>Other</option>
              </select>
            </div>
            <div>
              <label class=\"label\" for=\"zoneMgmtZoneId\">Zone ID (View Only)</label>
              <input id=\"zoneMgmtZoneId\" class=\"input\" placeholder=\"zone1-production-coil-1\" readonly>
            </div>
            <div>
              <label class=\"label\">Current Probe Assignment</label>
              <div id=\"zoneMgmtProbeSummary\" class=\"smallnote\">--</div>
            </div>
            <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
              <button type=\"button\" class=\"btn active\" id=\"zoneMgmtSaveBtn\">Save Zone Changes</button>
            </div>
            <div class=\"full\">
              <details style=\"border:1px solid var(--border);border-radius:10px;padding:10px;background:#fff\">
                <summary style=\"cursor:pointer;font-weight:600\">Advanced Settings</summary>
                <div class=\"form-grid\" style=\"margin-top:10px\">
                  <div>
                    <label class=\"label\" for=\"zoneMgmtLowTemp\">Low Temp Alert Threshold (F)</label>
                    <input id=\"zoneMgmtLowTemp\" class=\"input\" type=\"number\" step=\"0.5\" min=\"0\">
                  </div>
                  <div>
                    <label class=\"label\" for=\"zoneMgmtLowTempEnabled\">Low Temp Alerts</label>
                    <select id=\"zoneMgmtLowTempEnabled\" class=\"select\">
                      <option value=\"true\">Enabled</option>
                      <option value=\"false\">Disabled</option>
                    </select>
                  </div>
                  <div>
                    <label class=\"label\" for=\"zoneMgmtProbeSwapDelta\">Probe Swap Suspected Threshold (Idle Delta F)</label>
                    <input id=\"zoneMgmtProbeSwapDelta\" class=\"input\" type=\"number\" step=\"0.5\" min=\"0\">
                    <div class=\"smallnote\">During idle (no 24VAC), flag if return is hotter than feed by this amount.</div>
                  </div>
                </div>
              </details>
            </div>
          </div>
          <div id=\"zoneMgmtMsg\" class=\"msg\"></div>
          <div class=\"form-grid\" style=\"margin-top:10px\">
            <div class=\"full\">
              <div class=\"label\">Hardware Self-Diagnostic</div>
              <div id=\"zoneMgmtDiagStatus\" class=\"smallnote\">--</div>
              <div id=\"zoneMgmtDiagIssues\" class=\"smallnote\" style=\"margin-top:6px;white-space:pre-wrap\"></div>
            </div>
            <div>
              <div class=\"label\">Feed Probe</div>
              <div id=\"zoneMgmtFeedDiag\" class=\"smallnote\">--</div>
            </div>
            <div>
              <div class=\"label\">Return Probe</div>
              <div id=\"zoneMgmtReturnDiag\" class=\"smallnote\">--</div>
            </div>
          </div>
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
              <div class=\"smallnote\">Only adopted/assigned zone devices appear here (plus Zone 1 and Zone 2 main).</div>
            </div>
            <button type=\"button\" class=\"btn\" id=\"siteMapFindBtn\">Find Address</button>
            <button type=\"button\" class=\"btn active\" id=\"siteMapSaveBtn\">Save Map</button>
          </div>
          <div class=\"site-map-subtools\">
            <div>
              <label class=\"label\" for=\"siteMapLayer\">Map View</label>
              <select id=\"siteMapLayer\" class=\"select\">
                <option value=\"street\">Street</option>
                <option value=\"satellite\">Satellite</option>
              </select>
            </div>
            <div>
              <label class=\"label\" for=\"siteMapZoom\">Zoom</label>
              <input id=\"siteMapZoom\" class=\"input\" type=\"number\" min=\"1\" max=\"22\" step=\"1\" value=\"16\">
            </div>
            <div>
              <label class=\"label\" for=\"siteMapCenter\">Map Center</label>
              <input id=\"siteMapCenter\" class=\"input\" readonly placeholder=\"Lat, Lng\">
            </div>
            <button type=\"button\" class=\"btn\" id=\"siteMapFootprintBtn\">Load Building Outline</button>
            <button type=\"button\" class=\"btn\" id=\"siteMapClearPinBtn\">Clear Selected Pin</button>
          </div>
          <div class=\"site-map-subtools\" style=\"margin-top:8px\">
            <button type=\"button\" class=\"btn\" id=\"siteMapStartAreaBtn\">Start Zone Area</button>
            <button type=\"button\" class=\"btn\" id=\"siteMapFinishAreaBtn\">Finish Area</button>
            <button type=\"button\" class=\"btn\" id=\"siteMapCancelAreaBtn\">Cancel Area</button>
            <button type=\"button\" class=\"btn\" id=\"siteMapClearAreaBtn\">Clear Selected Area</button>
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
            <div id=\"siteMapThermostatWrap\">
              <label class=\"label\" for=\"siteMapThermostatLoc\">Thermostat Location</label>
              <input id=\"siteMapThermostatLoc\" class=\"input\" placeholder=\"South wall by door / office interior wall\">
            </div>
            <div id=\"siteMapValveWrap\">
              <label class=\"label\" for=\"siteMapValveType\">Valve / Actuator Type</label>
              <input id=\"siteMapValveType\" class=\"input\" placeholder=\"2-way zone valve / Belimo actuator / Taco zone valve\">
            </div>
            <div id=\"siteMapPumpModelWrap\" class=\"hidden\">
              <label class=\"label\" for=\"siteMapPumpModel\">Circulator Pump Model (Optional)</label>
              <input id=\"siteMapPumpModel\" class=\"input\" placeholder=\"Taco 0015e3 / Grundfos UPS15-58FC\">
            </div>
            <div id=\"siteMapPumpVoltageWrap\" class=\"hidden\">
              <label class=\"label\" for=\"siteMapPumpVoltage\">Pump Voltage (Optional)</label>
              <input id=\"siteMapPumpVoltage\" class=\"input\" placeholder=\"120VAC / 208VAC / 24VAC control\">
            </div>
            <div class=\"full\">
              <label class=\"label\" for=\"siteMapDesc\">Location Description</label>
              <textarea id=\"siteMapDesc\" class=\"input\" rows=\"3\" placeholder=\"AV coil is above drop ceiling near north wall access panel.\"></textarea>
            </div>
            <div class=\"full\">
              <label class=\"label\" for=\"siteMapAccessInstructions\">Access Instructions</label>
              <textarea id=\"siteMapAccessInstructions\" class=\"input\" rows=\"2\" placeholder=\"Use 8' ladder, remove 2nd ceiling tile from east wall, power panel access required.\"></textarea>
            </div>
            <div class=\"full\">
              <details style=\"border:1px solid var(--border);border-radius:10px;padding:10px;background:#fff\">
                <summary style=\"cursor:pointer;font-weight:600\">Advanced Details (Optional)</summary>
                <div class=\"form-grid\" style=\"margin-top:10px\">
                  <div id=\"siteMapPumpSpeedModeWrap\" class=\"hidden\">
                    <label class=\"label\" for=\"siteMapPumpSpeedMode\">Pump Speed Setting / ECM Mode</label>
                    <input id=\"siteMapPumpSpeedMode\" class=\"input\" placeholder=\"Speed 2 / Constant Pressure / AutoAdapt\">
                  </div>
                  <div id=\"siteMapPumpSerialWrap\" class=\"hidden\">
                    <label class=\"label\" for=\"siteMapPumpSerial\">Pump Serial #</label>
                    <input id=\"siteMapPumpSerial\" class=\"input\" placeholder=\"Serial number / equipment label\">
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
                  <div id=\"siteMapPumpServiceNotesWrap\" class=\"full hidden\">
                    <label class=\"label\" for=\"siteMapPumpServiceNotes\">Pump Service Notes</label>
                    <textarea id=\"siteMapPumpServiceNotes\" class=\"input\" rows=\"2\" placeholder=\"Speed set to 2 / ECM mode updated / cartridge replaced\"></textarea>
                  </div>
                </div>
              </details>
            </div>
            <div>
              <label class=\"label\" for=\"siteMapPhotoInput\">Zone Photo</label>
              <input id=\"siteMapPhotoInput\" class=\"input\" type=\"file\" accept=\"image/*\" capture=\"environment\">
              <div class=\"smallnote\">On mobile, this can open the camera.</div>
            </div>
            <div style=\"display:flex;align-items:end;gap:8px\">
              <button type=\"button\" class=\"btn\" id=\"siteMapClearPhotoBtn\">Remove Saved Photo</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapApplyDetailsBtn\">Apply Details to Selected Zone</button>
            </div>
            <div class=\"full\">
              <img id=\"siteMapPhotoPreview\" alt=\"Zone photo preview\" style=\"max-width:280px;width:100%;display:none;border:1px solid var(--border);border-radius:10px;background:#f4f8fb\"/>
            </div>
          </div>
          <div id=\"siteMapCanvas\" style=\"margin-top:10px\"></div>
          <div class=\"smallnote\" style=\"margin-top:8px\">Tip: Choose a zone/device, then tap the map to place a pin. Use Start Zone Area to sketch a rough served area polygon.</div>
          <div id=\"siteMapMsg\" class=\"msg\"></div>
          <div id=\"siteMapPinsList\" class=\"pin-list\"></div>
        </div>
      </div>

      <div id=\"adminCommissioningPanel\" class=\"hidden\" style=\"margin-top:12px\">
        <div class=\"card\" style=\"padding:12px;border-radius:12px;border:1px solid var(--border);background:#f9fcfe;box-shadow:none\">
          <h4 style=\"margin:0 0 8px\">Commissioning Checklist</h4>
          <div class=\"smallnote\">Track install verification steps for each main zone or adopted zone device.</div>
          <div class=\"form-grid\" style=\"margin-top:10px\">
            <div>
              <label class=\"label\" for=\"commissionZoneSelect\">Zone / Device</label>
              <select id=\"commissionZoneSelect\" class=\"select\"></select>
            </div>
            <div style=\"display:flex;align-items:end;gap:8px\">
              <button type=\"button\" class=\"btn\" id=\"commissionRefreshBtn\">Load Checklist</button>
            </div>
            <div class=\"full\">
              <div class=\"label\" style=\"margin-bottom:6px\">Checklist</div>
              <div class=\"d-flex flex-wrap gap-3\" id=\"commissionChecks\">
                <label><input type=\"checkbox\" data-commission-check=\"call_input_verified\"> Call Input Verified</label>
                <label><input type=\"checkbox\" data-commission-check=\"feed_return_confirmed\"> Feed / Return Confirmed</label>
                <label><input type=\"checkbox\" data-commission-check=\"temp_response_verified\"> Temp Response Verified</label>
                <label><input type=\"checkbox\" data-commission-check=\"photo_added\"> Photo Added</label>
                <label><input type=\"checkbox\" data-commission-check=\"pin_placed\"> Map Pin Placed</label>
                <label><input type=\"checkbox\" data-commission-check=\"alert_test_completed\"> Alert Test Completed</label>
              </div>
            </div>
            <div>
              <label class=\"label\" for=\"commissionCompletedBy\">Completed By</label>
              <input id=\"commissionCompletedBy\" class=\"input\" placeholder=\"Tech name / initials\">
            </div>
            <div>
              <label class=\"label\">Completion</label>
              <div id=\"commissionSummary\" class=\"smallnote\">0 / 6 complete</div>
            </div>
            <div class=\"full\">
              <label class=\"label\" for=\"commissionNotes\">Commissioning Notes</label>
              <textarea id=\"commissionNotes\" class=\"input\" rows=\"3\" placeholder=\"Notes about verification, observed temps, corrections made, etc.\"></textarea>
            </div>
            <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
              <button type=\"button\" class=\"btn active\" id=\"commissionSaveBtn\">Save Checklist</button>
            </div>
          </div>
          <div id=\"commissionMsg\" class=\"msg\"></div>
        </div>
      </div>

      <div id=\"adminBackupPanel\" class=\"hidden\" style=\"margin-top:12px\">
        <div class=\"card\" style=\"padding:12px;border-radius:12px;border:1px solid var(--border);background:#f9fcfe;box-shadow:none\">
          <h4 style=\"margin:0 0 8px\">Backup / Restore</h4>
          <div class=\"smallnote\">Export or restore hub configuration (zones, users, alerts, site map, commissioning). Secrets are redacted in exports.</div>
          <div class=\"form-grid\" style=\"margin-top:10px\">
            <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
              <button type=\"button\" class=\"btn\" id=\"backupExportBtn\">Generate Backup JSON</button>
              <button type=\"button\" class=\"btn active\" id=\"backupRestoreBtn\">Restore From JSON</button>
            </div>
            <div class=\"full\">
              <label class=\"label\" for=\"backupJson\">Backup JSON</label>
              <textarea id=\"backupJson\" class=\"input\" rows=\"10\" placeholder='{"format":"hvac-hub-backup-v1", ...}'></textarea>
            </div>
            <div class=\"full\">
              <label><input type=\"checkbox\" id=\"backupRestoreConfirm\"> I understand restore will overwrite current hub configuration.</label>
            </div>
          </div>
          <div id=\"backupMsg\" class=\"msg\"></div>
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
        </div>
      </details>
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
          <div id=\"zoneInfoThermostatRow\" class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Thermostat Location</div></div><div id=\"zoneInfoThermostat\">--</div></div>
          <div id=\"zoneInfoValveRow\" class=\"row\" style=\"padding:6px 0\"><div><div class=\"label\">Valve / Actuator</div></div><div id=\"zoneInfoValve\">--</div></div>
          <div id=\"zoneInfoPumpModelRow\" class=\"row hidden\" style=\"padding:6px 0\"><div><div class=\"label\">Circulator Pump Model</div></div><div id=\"zoneInfoPumpModel\">--</div></div>
          <div id=\"zoneInfoPumpVoltageRow\" class=\"row hidden\" style=\"padding:6px 0\"><div><div class=\"label\">Pump Voltage</div></div><div id=\"zoneInfoPumpVoltage\">--</div></div>
          <div id=\"zoneInfoPumpSpeedModeRow\" class=\"row hidden\" style=\"padding:6px 0\"><div><div class=\"label\">Pump Speed / ECM Mode</div></div><div id=\"zoneInfoPumpSpeedMode\">--</div></div>
          <div id=\"zoneInfoPumpSerialRow\" class=\"row hidden\" style=\"padding:6px 0\"><div><div class=\"label\">Pump Serial #</div></div><div id=\"zoneInfoPumpSerial\">--</div></div>
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
      <div id=\"zoneInfoPumpServiceNotesWrap\" class=\"hidden\" style=\"margin-top:12px\">
        <div class=\"label\">Pump Service Notes</div>
        <div id=\"zoneInfoPumpServiceNotes\" style=\"margin-top:6px;white-space:pre-wrap;border:1px solid var(--border);border-radius:10px;padding:10px;background:#f8fbfd\">--</div>
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
    let zoneMgmtStatusCache = null;
    let commissioningCache = { records: {} };
    let lastHubPayload = null;
    let activeOverviewParent = "Zone 1";
    let siteMapConfig = null;
    let siteMapMap = null;
    let siteMapMarkers = {};
    let siteMapZonePolygons = {};
    let siteMapBaseLayers = {};
    let siteMapActiveLayerKey = "street";
    let siteMapBuildingOutlineLayer = null;
    let siteMapDraftAreaPoints = [];
    let siteMapDraftAreaLayer = null;
    let siteMapDrawingArea = false;
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
    function parentZoneColorHex(parentZone) {
      const p = String(parentZone || "").toLowerCase();
      if (p === "zone 1") return "#dc2626";
      if (p === "zone 2") return "#2563eb";
      if (p === "zone 3") return "#16a34a";
      return "#7c3aed";
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

    function isPlaceholderZone(z) {
      const url = String((z && z.source_url) || "");
      return /:\/\/100\.100\.100\.\d+/.test(url);
    }

    function isAdoptedZone(z) {
      const url = String((z && z.source_url) || "").trim();
      if (!url) return false;
      if (isPlaceholderZone(z)) return false;
      return true;
    }

    function realDownstreamRows(rows) {
      return (rows || []).filter((z) => isAdoptedZone(z));
    }

    function ensureAdminToolsOpen() {
      const d = document.getElementById("adminToolsDetails");
      if (d && !d.open) d.open = true;
    }
    function scrollToAdminPanel(panelId) {
      ensureAdminToolsOpen();
      const panel = document.getElementById(panelId);
      const adminWrap = document.getElementById("adminToolsDetails");
      if (adminWrap) adminWrap.scrollIntoView({ behavior: "smooth", block: "start" });
      if (panel) {
        setTimeout(() => {
          panel.scrollIntoView({ behavior: "smooth", block: "start" });
        }, 80);
      }
    }

    function openSetupPanel() {
      ensureAdminToolsOpen();
      setAdminTab("setup");
      scrollToAdminPanel("adminSetupPanel");
    }

    function openUsersPanel() {
      ensureAdminToolsOpen();
      setAdminTab("users");
      scrollToAdminPanel("adminUsersPanel");
    }

    function commissioningZoneOptions() {
      return siteMapZoneOptions().filter((o) => {
        if (o.id === "zone1_main" || o.id === "zone2_main") return true;
        const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
        const row = ds.find((z) => String(z.id || "") === String(o.id));
        return row ? isAdoptedZone(row) : false;
      });
    }

    function refreshCommissioningZoneSelect() {
      const sel = document.getElementById("commissionZoneSelect");
      if (!sel) return;
      const prev = sel.value;
      const opts = commissioningZoneOptions();
      sel.innerHTML = "";
      if (!opts.length) {
        const op = document.createElement("option");
        op.value = "";
        op.textContent = "-- No zones available --";
        sel.appendChild(op);
        return;
      }
      opts.forEach((o) => {
        const op = document.createElement("option");
        op.value = o.id;
        op.textContent = o.label;
        sel.appendChild(op);
      });
      if (opts.some((o) => o.id === prev)) sel.value = prev;
    }

    function currentCommissionChecks() {
      const checks = {};
      document.querySelectorAll("[data-commission-check]").forEach((cb) => {
        checks[cb.dataset.commissionCheck] = !!cb.checked;
      });
      return checks;
    }

    function updateCommissioningSummary() {
      const checks = currentCommissionChecks();
      const keys = Object.keys(checks);
      const total = keys.length;
      const done = keys.filter((k) => checks[k]).length;
      const el = document.getElementById("commissionSummary");
      if (el) el.textContent = `${done} / ${total} complete`;
    }

    function fillCommissioningForm(zoneId, rec) {
      const r = (rec && typeof rec === "object") ? rec : {};
      const checks = (r.checks && typeof r.checks === "object") ? r.checks : {};
      document.querySelectorAll("[data-commission-check]").forEach((cb) => {
        cb.checked = !!checks[cb.dataset.commissionCheck];
      });
      document.getElementById("commissionCompletedBy").value = r.completed_by || "";
      document.getElementById("commissionNotes").value = r.notes || "";
      updateCommissioningSummary();
      if (r.completed_utc) {
        setCommissionMsg(`Checklist loaded. Completed: ${String(r.completed_utc).replace("T", " ").replace("+00:00", " UTC")}`);
      } else {
        setCommissionMsg(zoneId ? "Checklist loaded." : "");
      }
    }

    async function loadCommissioningRecord(zoneId) {
      if (!zoneId) {
        fillCommissioningForm("", {});
        setCommissionMsg("Select a zone/device.");
        return;
      }
      setCommissionMsg("Loading checklist...");
      try {
        const res = await fetch(`/api/commissioning?zone_id=${encodeURIComponent(zoneId)}&_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "load failed");
        commissioningCache = data;
        fillCommissioningForm(zoneId, data.record || {});
      } catch (e) {
        setCommissionMsg("Load failed: " + e.message);
      }
    }

    async function saveCommissioningRecord() {
      const zoneId = String(document.getElementById("commissionZoneSelect").value || "").trim();
      if (!zoneId) { setCommissionMsg("Select a zone/device."); return; }
      setCommissionMsg("Saving checklist...");
      try {
        const res = await fetch("/api/commissioning/save", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            zone_id: zoneId,
            checks: currentCommissionChecks(),
            completed_by: document.getElementById("commissionCompletedBy").value.trim(),
            notes: document.getElementById("commissionNotes").value.trim(),
          })
        });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "save failed");
        commissioningCache = data;
        fillCommissioningForm(zoneId, data.record || {});
        setCommissionMsg(data.message || "Checklist saved.");
      } catch (e) {
        setCommissionMsg("Save failed: " + e.message);
      }
    }

    async function exportBackupJson() {
      setBackupMsg("Generating backup...");
      try {
        const res = await fetch(`/api/backup/export?_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "export failed");
        document.getElementById("backupJson").value = JSON.stringify(data, null, 2);
        setBackupMsg("Backup JSON generated.");
      } catch (e) {
        setBackupMsg("Export failed: " + e.message);
      }
    }

    async function restoreBackupJson() {
      const confirmBox = document.getElementById("backupRestoreConfirm");
      if (!confirmBox || !confirmBox.checked) {
        setBackupMsg("Check the confirmation box before restoring.");
        return;
      }
      let payload;
      try {
        payload = JSON.parse(document.getElementById("backupJson").value || "{}");
      } catch (_) {
        setBackupMsg("Backup JSON is invalid.");
        return;
      }
      setBackupMsg("Restoring backup...");
      try {
        const res = await fetch("/api/backup/restore", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (!res.ok || data.error || data.ok === false) throw new Error(data.error || data.message || "restore failed");
        setBackupMsg(data.message || "Backup restored.");
        await Promise.all([refreshHub(), loadUsers(), loadAlertEvents(), loadNotifications(), loadSiteMapConfig()]);
        refreshCommissioningZoneSelect();
      } catch (e) {
        setBackupMsg("Restore failed: " + e.message);
      }
    }

    function setDownstreamVisibility(hasRealDevices) {
      const sub = document.getElementById("subzonesSection");
      const empty = document.getElementById("downstreamEmptyState");
      if (sub) sub.classList.toggle("hidden", !hasRealDevices);
      if (empty) empty.classList.toggle("hidden", !!hasRealDevices);
    }

    function setOverviewParent(parentName) {
      activeOverviewParent = parentName || "Zone 1";
      document.querySelectorAll("[data-overview-parent]").forEach((b) => {
        b.classList.toggle("active", b.dataset.overviewParent === activeOverviewParent);
      });
      renderZoneOverview();
    }

    function focusZoneGraph(zoneGraphId, cycleZoneId) {
      if (zoneGraphId) currentTabId = zoneGraphId;
      if (cycleZoneId) activeCycleZoneId = cycleZoneId;
      const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
      updateTabs(ds);
      updateCycleTabs(ds);
      renderSelectedGraph();
      refreshCycles();
      const graph = document.getElementById("graphTitle");
      if (graph) graph.scrollIntoView({ behavior: "smooth", block: "start" });
    }

    function renderZoneOverview() {
      const kpiEl = document.getElementById("overviewKpis");
      const cardsEl = document.getElementById("overviewCards");
      if (!kpiEl || !cardsEl) return;
      const hub = lastHubPayload || {};
      const dsAll = realDownstreamRows(Array.isArray(hub.downstream) ? hub.downstream : []);
      const ds = dsAll.filter((z) => String(z.parent_zone || "") === String(activeOverviewParent));
      const troubleCount = ds.filter((z) => String(z.status || "") === "trouble").length;
      const callingCount = ds.filter((z) => z.heating_call === true).length;
      const onlineCount = ds.filter((z) => String(z.zone_status || "") !== "Source Unreachable").length;
      const feedVals = ds.map((z) => Number(z.feed_f)).filter((v) => Number.isFinite(v));
      const retVals = ds.map((z) => Number(z.return_f)).filter((v) => Number.isFinite(v));
      const avg = (arr) => arr.length ? (arr.reduce((a,b)=>a+b,0) / arr.length).toFixed(1) + " °F" : "--";

      let mainFeed = "--", mainRet = "--", mainCall = "--";
      if (hub.main && hub.main.readings) {
        if (activeOverviewParent === "Zone 1") {
          mainFeed = fmtTemp(hub.main.readings.feed_1 && hub.main.readings.feed_1.temp_f);
          mainRet = fmtTemp(hub.main.readings.return_1 && hub.main.readings.return_1.temp_f);
          mainCall = hub.main.main_call_status || "--";
        } else if (activeOverviewParent === "Zone 2") {
          mainFeed = fmtTemp(hub.main.readings.feed_2 && hub.main.readings.feed_2.temp_f);
          mainRet = fmtTemp(hub.main.readings.return_2 && hub.main.readings.return_2.temp_f);
          mainCall = "Main call not monitored";
        }
      }

      kpiEl.innerHTML = `
        <div class="overview-kpi"><div class="k">${activeOverviewParent} Main Feed</div><div class="v">${mainFeed}</div></div>
        <div class="overview-kpi"><div class="k">${activeOverviewParent} Main Return</div><div class="v">${mainRet}</div></div>
        <div class="overview-kpi"><div class="k">Downstream Zones</div><div class="v">${ds.length}</div></div>
        <div class="overview-kpi"><div class="k">Calling Now</div><div class="v">${callingCount}</div></div>
        <div class="overview-kpi"><div class="k">Trouble</div><div class="v">${troubleCount}</div></div>
        <div class="overview-kpi"><div class="k">Online</div><div class="v">${onlineCount}/${ds.length}</div></div>
        <div class="overview-kpi"><div class="k">Avg Feed</div><div class="v">${avg(feedVals)}</div></div>
        <div class="overview-kpi"><div class="k">Avg Return</div><div class="v">${avg(retVals)}</div></div>
      `;

      if (!ds.length) {
        cardsEl.innerHTML = `<div class="smallnote">No adopted downstream devices assigned to ${activeOverviewParent} yet. Use <strong>Add Device</strong> to set up and assign the first zone.</div>`;
        return;
      }
      cardsEl.innerHTML = "";
      ds.forEach((z) => {
        const card = document.createElement("div");
        card.className = "overview-card";
        const status = z.zone_status || z.status || "--";
        const note = z.status_note || "";
        const statusCls = statusClass(z.status);
        card.innerHTML = `
          <div style="display:flex;justify-content:space-between;gap:8px;align-items:start">
            <div>
              <div class="t">${z.name || z.id || "--"}</div>
              <div class="s">${z.id || ""}</div>
            </div>
            <button type="button" class="btn" style="padding:4px 10px">Open Graph</button>
          </div>
          <div class="line">Call: ${z.call_status || (z.heating_call === true ? "24VAC Present (Calling)" : (z.heating_call === false ? "No 24VAC (Idle)" : "24VAC Signal Unknown"))}</div>
          <div class="line">Feed: ${fmtTemp(z.feed_f)} · Return: ${fmtTemp(z.return_f)}</div>
          <div class="line ${statusCls}">Status: ${status}</div>
          ${note ? `<div class="line s">Note: ${note}</div>` : ""}
        `;
        const btn = card.querySelector("button");
        if (btn) btn.addEventListener("click", () => focusZoneGraph("ds_" + (z.id || ""), z.id || ""));
        cardsEl.appendChild(card);
      });
    }

    function renderRows(rows) {
      const body = document.getElementById("subzoneBody");
      body.innerHTML = "";
      const visibleRows = realDownstreamRows(rows);
      if (!visibleRows || visibleRows.length === 0) {
        setDownstreamVisibility(false);
        return;
      }
      setDownstreamVisibility(true);
      for (const z of visibleRows) {
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
      realDownstreamRows(downstreamRows).forEach((z) => {
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
      realDownstreamRows(downstreamRows).forEach((z) => {
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

    function appendLivePoint(mainReadings, downstreamRows, mainCallZone1 = null) {
      const p = {
        ts: new Date().toISOString(),
        main: {
          feed_1: mainReadings.feed_1 && mainReadings.feed_1.temp_f !== undefined ? mainReadings.feed_1.temp_f : null,
          return_1: mainReadings.return_1 && mainReadings.return_1.temp_f !== undefined ? mainReadings.return_1.temp_f : null,
          feed_2: mainReadings.feed_2 && mainReadings.feed_2.temp_f !== undefined ? mainReadings.feed_2.temp_f : null,
          return_2: mainReadings.return_2 && mainReadings.return_2.temp_f !== undefined ? mainReadings.return_2.temp_f : null,
          call_zone1: mainCallZone1,
        },
        downstream: {},
      };
      (downstreamRows || []).forEach((z) => {
        p.downstream[z.id] = { feed_f: z.feed_f, return_f: z.return_f, call: z.heating_call };
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

    function callStateFromSample(sample, tabId) {
      if (!sample) return null;
      if (tabId === "zone1_main") {
        if (sample.main && Object.prototype.hasOwnProperty.call(sample.main, "call_zone1")) return sample.main.call_zone1;
        if (Object.prototype.hasOwnProperty.call(sample, "main_call_24vac")) return sample.main_call_24vac;
        return null;
      }
      if (tabId.startsWith("ds_")) {
        const zoneId = tabId.slice(3);
        const d = sample.downstream && sample.downstream[zoneId];
        if (d && Object.prototype.hasOwnProperty.call(d, "call")) return d.call;
      }
      return null;
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
        return { ts: p.ts, feed: s.feed, ret: s.ret, call: callStateFromSample(p, tabId) };
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

      ctx.fillStyle = "rgba(37,99,235,0.08)";
      for (let i = 0; i < n; i++) {
        if (transformed[i].call !== true) continue;
        const x0 = (i === 0) ? padL : (xAt(i - 1) + xAt(i)) / 2;
        const x1 = (i === n - 1) ? (w - padR) : (xAt(i) + xAt(i + 1)) / 2;
        ctx.fillRect(x0, padT, Math.max(1, x1 - x0), plotH);
      }

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
        appendLivePoint(r, [], zone1CallActive);
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
        appendLivePoint(r, downstream, (data.main && Object.prototype.hasOwnProperty.call(data.main, "main_call_24vac")) ? data.main.main_call_24vac : null);
        updateTabs(downstream);
        updateCycleTabs(downstream);
        renderRows(downstream);
        renderZoneOverview();
        refreshSiteMapZoneSelect();
        refreshCommissioningZoneSelect();
        if (activeRange === "live") renderSelectedGraph();

        const trouble = Number(data.trouble_count || 0);
        const unack = Number(data.unack_alert_count || 0);
        const tasks = Math.max(trouble, unack);
        const tpill = document.getElementById("tasksPill");
        if (tpill) {
          tpill.childNodes[0].nodeValue = `Tasks: ${tasks} `;
          tpill.classList.toggle("attn", (trouble > 0 || unack > 0));
          const dot = document.getElementById("tasksPillDot");
          if (dot) dot.textContent = (unack > 0 ? String(Math.min(99, unack)) : "!");
          const sub = tpill.querySelector(".sub");
          if (sub) {
            if (unack > 0) sub.textContent = `${unack} alert${unack === 1 ? "" : "s"} need acknowledgement`;
            else if (trouble > 0) sub.textContent = `${trouble} trouble zone${trouble === 1 ? "" : "s"} to review`;
            else sub.textContent = "No open tasks";
          }
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
      const opts = siteMapZoneOptions().filter((o) => {
        if (o.id === "zone1_main" || o.id === "zone2_main") return true;
        const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
        const row = ds.find((z) => String(z.id || "") === String(o.id));
        return row ? isAdoptedZone(row) : false;
      });
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
      const layerSel = document.getElementById("siteMapLayer");
      if (layerSel) layerSel.value = siteMapConfig.map_layer || "street";
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
    function isMainZoneId(zoneId) {
      return zoneId === "zone1_main" || zoneId === "zone2_main";
    }
    function updateSiteMapDetailFieldMode(zoneId) {
      const main = isMainZoneId(zoneId || selectedSiteMapZoneId());
      const thermostatWrap = document.getElementById("siteMapThermostatWrap");
      const valveWrap = document.getElementById("siteMapValveWrap");
      const pumpModelWrap = document.getElementById("siteMapPumpModelWrap");
      const pumpVoltageWrap = document.getElementById("siteMapPumpVoltageWrap");
      const pumpSpeedModeWrap = document.getElementById("siteMapPumpSpeedModeWrap");
      const pumpSerialWrap = document.getElementById("siteMapPumpSerialWrap");
      const pumpServiceNotesWrap = document.getElementById("siteMapPumpServiceNotesWrap");
      if (thermostatWrap) thermostatWrap.classList.toggle("hidden", main);
      if (valveWrap) valveWrap.classList.toggle("hidden", main);
      if (pumpModelWrap) pumpModelWrap.classList.toggle("hidden", !main);
      if (pumpVoltageWrap) pumpVoltageWrap.classList.toggle("hidden", !main);
      if (pumpSpeedModeWrap) pumpSpeedModeWrap.classList.toggle("hidden", !main);
      if (pumpSerialWrap) pumpSerialWrap.classList.toggle("hidden", !main);
      if (pumpServiceNotesWrap) pumpServiceNotesWrap.classList.toggle("hidden", !main);
      const equipEl = document.getElementById("siteMapEquipType");
      if (equipEl) {
        equipEl.placeholder = main ? "Circulator pump / Boiler loop pump" : "Fan coil / Unit heater / Air handler coil";
      }
    }

    function getSiteMapPin(zoneId) {
      if (!zoneId || !siteMapConfig || !siteMapConfig.pins || typeof siteMapConfig.pins !== "object") return null;
      return siteMapConfig.pins[zoneId] || null;
    }

    function setSiteMapBaseLayer(layerKey) {
      const nextKey = (layerKey === "satellite") ? "satellite" : "street";
      siteMapActiveLayerKey = nextKey;
      if (siteMapConfig && typeof siteMapConfig === "object") siteMapConfig.map_layer = nextKey;
      if (siteMapMap && siteMapBaseLayers.street && siteMapBaseLayers.satellite) {
        if (siteMapMap.hasLayer(siteMapBaseLayers.street)) siteMapMap.removeLayer(siteMapBaseLayers.street);
        if (siteMapMap.hasLayer(siteMapBaseLayers.satellite)) siteMapMap.removeLayer(siteMapBaseLayers.satellite);
        siteMapBaseLayers[nextKey].addTo(siteMapMap);
      }
      const sel = document.getElementById("siteMapLayer");
      if (sel) sel.value = nextKey;
    }

    function renderSiteMapBuildingOutline() {
      if (!siteMapMap || !window.L) return;
      if (siteMapBuildingOutlineLayer) {
        try { siteMapMap.removeLayer(siteMapBuildingOutlineLayer); } catch (e) {}
        siteMapBuildingOutlineLayer = null;
      }
      const bo = siteMapConfig && siteMapConfig.building_outline;
      if (!bo || typeof bo !== "object") return;
      try {
        siteMapBuildingOutlineLayer = L.geoJSON(bo, {
          style: { color: "#0f172a", weight: 2, fillColor: "#94a3b8", fillOpacity: 0.08 }
        }).addTo(siteMapMap);
      } catch (e) {
        siteMapBuildingOutlineLayer = null;
      }
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
      const pumpModelEl = document.getElementById("siteMapPumpModel");
      const pumpVoltageEl = document.getElementById("siteMapPumpVoltage");
      const pumpSpeedModeEl = document.getElementById("siteMapPumpSpeedMode");
      const pumpSerialEl = document.getElementById("siteMapPumpSerial");
      const breakerEl = document.getElementById("siteMapBreakerRef");
      const installedByEl = document.getElementById("siteMapInstalledBy");
      const installDateEl = document.getElementById("siteMapInstallDate");
      const lastServiceDateEl = document.getElementById("siteMapLastServiceDate");
      const descEl = document.getElementById("siteMapDesc");
      const accessEl = document.getElementById("siteMapAccessInstructions");
      const pumpServiceNotesEl = document.getElementById("siteMapPumpServiceNotes");
      if (colorEl) colorEl.value = pin.color || "red";
      if (equipEl) equipEl.value = pin.equipment_type || "";
      if (roomEl) roomEl.value = pin.room_area_name || "";
      if (thermoEl) thermoEl.value = pin.thermostat_location || "";
      if (valveEl) valveEl.value = pin.valve_actuator_type || "";
      if (pumpModelEl) pumpModelEl.value = pin.pump_model || "";
      if (pumpVoltageEl) pumpVoltageEl.value = pin.pump_voltage || "";
      if (pumpSpeedModeEl) pumpSpeedModeEl.value = pin.pump_speed_mode || "";
      if (pumpSerialEl) pumpSerialEl.value = pin.pump_serial || "";
      if (breakerEl) breakerEl.value = pin.circuit_breaker_ref || "";
      if (installedByEl) installedByEl.value = pin.installed_by || "";
      if (installDateEl) installDateEl.value = pin.install_date || "";
      if (lastServiceDateEl) lastServiceDateEl.value = pin.last_service_date || "";
      if (descEl) descEl.value = pin.description || "";
      if (accessEl) accessEl.value = pin.access_instructions || "";
      if (pumpServiceNotesEl) pumpServiceNotesEl.value = pin.pump_service_notes || "";
      setSiteMapPhotoPreview(pin.photo_data_url || "");
      const photoInput = document.getElementById("siteMapPhotoInput");
      if (photoInput) photoInput.value = "";
      updateSiteMapDetailFieldMode(zoneId);
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

    function renderSiteMapZoneAreas() {
      if (!siteMapMap || !window.L) return;
      Object.values(siteMapZonePolygons).forEach((poly) => { try { siteMapMap.removeLayer(poly); } catch (e) {} });
      siteMapZonePolygons = {};
      const pins = (siteMapConfig && siteMapConfig.pins && typeof siteMapConfig.pins === "object") ? siteMapConfig.pins : {};
      const labels = {};
      siteMapZoneOptions().forEach((o) => { labels[o.id] = o.label; });
      const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
      Object.entries(pins).forEach(([zid, p]) => {
        const polyPts = Array.isArray(p.polygon) ? p.polygon : [];
        if (polyPts.length < 3) return;
        const ll = polyPts
          .map((pt) => [Number(pt.lat), Number(pt.lng)])
          .filter((pt) => Number.isFinite(pt[0]) && Number.isFinite(pt[1]));
        if (ll.length < 3) return;
        const row = ds.find((z) => String(z.id || "") === String(zid));
        const parentColor = row ? parentZoneColorHex(row.parent_zone) : pinColorHex(p.color);
        const fillColor = pinColorHex(p.color);
        const poly = L.polygon(ll, {
          color: parentColor,
          weight: 2,
          fillColor,
          fillOpacity: 0.18,
        }).addTo(siteMapMap);
        poly.bindPopup(`<b>${labels[zid] || p.label || zid}</b><br>${p.room_area_name || p.equipment_type || "Zone area"}`);
        poly.on("click", () => openZoneInfoModal(zid));
        siteMapZonePolygons[zid] = poly;
      });
    }

    function renderSiteMapDraftArea() {
      if (!siteMapMap || !window.L) return;
      if (siteMapDraftAreaLayer) {
        try { siteMapMap.removeLayer(siteMapDraftAreaLayer); } catch (e) {}
        siteMapDraftAreaLayer = null;
      }
      if (!siteMapDrawingArea || siteMapDraftAreaPoints.length === 0) return;
      const latlngs = siteMapDraftAreaPoints.map((p) => [p.lat, p.lng]);
      const color = pinColorHex(document.getElementById("siteMapPinColor")?.value || "red");
      if (latlngs.length >= 3) {
        siteMapDraftAreaLayer = L.polygon(latlngs, { color, weight: 2, dashArray: "6,4", fillColor: color, fillOpacity: 0.08 }).addTo(siteMapMap);
      } else {
        siteMapDraftAreaLayer = L.polyline(latlngs, { color, weight: 2, dashArray: "6,4" }).addTo(siteMapMap);
      }
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
      renderSiteMapBuildingOutline();
      renderSiteMapZoneAreas();
      renderSiteMapDraftArea();
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
      siteMapBaseLayers.street = L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        maxZoom: 20,
        attribution: "&copy; OpenStreetMap contributors"
      });
      siteMapBaseLayers.satellite = L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", {
        maxZoom: 20,
        attribution: "Tiles &copy; Esri"
      });
      setSiteMapBaseLayer((base && base.map_layer) || "street");
      siteMapMap.on("click", (e) => {
        const sel = document.getElementById("siteMapZoneSelect");
        const zoneId = sel ? sel.value : "";
        if (!zoneId) { setSiteMapMsg("Select a zone/device first."); return; }
        if (siteMapDrawingArea) {
          siteMapDraftAreaPoints.push({ lat: e.latlng.lat, lng: e.latlng.lng });
          renderSiteMapDraftArea();
          setSiteMapMsg(`Zone area point ${siteMapDraftAreaPoints.length} added. Add at least 3 points, then click Finish Area.`);
          return;
        }
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
          pump_model: (getSiteMapPin(zoneId) || {}).pump_model || "",
          pump_voltage: (getSiteMapPin(zoneId) || {}).pump_voltage || "",
          pump_speed_mode: (getSiteMapPin(zoneId) || {}).pump_speed_mode || "",
          pump_serial: (getSiteMapPin(zoneId) || {}).pump_serial || "",
          pump_service_notes: (getSiteMapPin(zoneId) || {}).pump_service_notes || "",
          circuit_breaker_ref: (getSiteMapPin(zoneId) || {}).circuit_breaker_ref || "",
          description: (getSiteMapPin(zoneId) || {}).description || "",
          photo_data_url: (getSiteMapPin(zoneId) || {}).photo_data_url || "",
          polygon: Array.isArray((getSiteMapPin(zoneId) || {}).polygon) ? (getSiteMapPin(zoneId) || {}).polygon : [],
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
      siteMapConfig.map_layer = document.getElementById("siteMapLayer")?.value || "street";
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

    async function loadBuildingOutline() {
      const q = document.getElementById("siteAddress").value.trim();
      if (!q) { setSiteMapMsg("Enter a building address first."); return; }
      setSiteMapMsg("Loading building outline...");
      try {
        const res = await fetch(`/api/building-footprint?q=${encodeURIComponent(q)}&_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "outline lookup failed");
        if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
        siteMapConfig.building_outline = data.building_outline || null;
        if (data.center && Number.isFinite(Number(data.center.lat)) && Number.isFinite(Number(data.center.lng))) {
          siteMapConfig.center = { lat: Number(data.center.lat), lng: Number(data.center.lng) };
          if (siteMapMap) siteMapMap.setView([siteMapConfig.center.lat, siteMapConfig.center.lng], Math.max(18, Number(siteMapConfig.zoom || 18)));
        }
        renderSiteMapMarkers();
        setSiteMapMsg("Building outline loaded. Click Save Map to store it.");
      } catch (e) {
        setSiteMapMsg("Building outline lookup failed: " + e.message);
      }
    }

    function startZoneAreaDraw() {
      const zoneId = selectedSiteMapZoneId();
      if (!zoneId) { setSiteMapMsg("Select a zone/device first."); return; }
      siteMapDrawingArea = true;
      siteMapDraftAreaPoints = [];
      renderSiteMapDraftArea();
      setSiteMapMsg("Zone area drawing started. Tap the map to add points, then click Finish Area.");
    }

    function cancelZoneAreaDraw() {
      siteMapDrawingArea = false;
      siteMapDraftAreaPoints = [];
      renderSiteMapDraftArea();
      setSiteMapMsg("Zone area drawing canceled.");
    }

    function finishZoneAreaDraw() {
      const zoneId = selectedSiteMapZoneId();
      if (!zoneId) { setSiteMapMsg("Select a zone/device first."); return; }
      if (siteMapDraftAreaPoints.length < 3) { setSiteMapMsg("Add at least 3 points to create a zone area."); return; }
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      if (!siteMapConfig.pins || typeof siteMapConfig.pins !== "object") siteMapConfig.pins = {};
      const pin = siteMapConfig.pins[zoneId] || { label: zoneId };
      pin.polygon = siteMapDraftAreaPoints.map((p) => ({ lat: Number(p.lat), lng: Number(p.lng) }));
      pin.updated_utc = new Date().toISOString();
      siteMapConfig.pins[zoneId] = pin;
      siteMapDrawingArea = false;
      siteMapDraftAreaPoints = [];
      renderSiteMapMarkers();
      setSiteMapMsg("Zone area saved for selected zone/device. Click Save Map to store.");
    }

    function clearSelectedZoneArea() {
      const zoneId = selectedSiteMapZoneId();
      if (!zoneId || !siteMapConfig || !siteMapConfig.pins || !siteMapConfig.pins[zoneId]) {
        setSiteMapMsg("No selected zone area to clear.");
        return;
      }
      delete siteMapConfig.pins[zoneId].polygon;
      renderSiteMapMarkers();
      setSiteMapMsg("Selected zone area removed. Click Save Map to store.");
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
      pin.pump_model = (document.getElementById("siteMapPumpModel")?.value || "").trim();
      pin.pump_voltage = (document.getElementById("siteMapPumpVoltage")?.value || "").trim();
      pin.pump_speed_mode = (document.getElementById("siteMapPumpSpeedMode")?.value || "").trim();
      pin.pump_serial = (document.getElementById("siteMapPumpSerial")?.value || "").trim();
      pin.circuit_breaker_ref = document.getElementById("siteMapBreakerRef").value.trim();
      pin.installed_by = document.getElementById("siteMapInstalledBy").value.trim();
      pin.install_date = document.getElementById("siteMapInstallDate").value || "";
      pin.last_service_date = document.getElementById("siteMapLastServiceDate").value || "";
      pin.description = document.getElementById("siteMapDesc").value.trim();
      pin.access_instructions = document.getElementById("siteMapAccessInstructions").value.trim();
      pin.pump_service_notes = (document.getElementById("siteMapPumpServiceNotes")?.value || "").trim();
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
      const mainZone = isMainZoneId(zoneId);
      document.getElementById("zoneInfoTitle").textContent = (opt && opt.label) ? `${opt.label} Information` : "Zone Information";
      document.getElementById("zoneInfoLabel").textContent = (opt && opt.label) || pin.label || zoneId || "--";
      document.getElementById("zoneInfoRoomArea").textContent = pin.room_area_name || "Not set";
      document.getElementById("zoneInfoEquip").textContent = pin.equipment_type || "Not set";
      document.getElementById("zoneInfoThermostat").textContent = pin.thermostat_location || "Not set";
      document.getElementById("zoneInfoValve").textContent = pin.valve_actuator_type || "Not set";
      document.getElementById("zoneInfoPumpModel").textContent = pin.pump_model || "Not set";
      document.getElementById("zoneInfoPumpVoltage").textContent = pin.pump_voltage || "Not set";
      document.getElementById("zoneInfoPumpSpeedMode").textContent = pin.pump_speed_mode || "Not set";
      document.getElementById("zoneInfoPumpSerial").textContent = pin.pump_serial || "Not set";
      document.getElementById("zoneInfoThermostatRow").classList.toggle("hidden", mainZone);
      document.getElementById("zoneInfoValveRow").classList.toggle("hidden", mainZone);
      document.getElementById("zoneInfoPumpModelRow").classList.toggle("hidden", !mainZone);
      document.getElementById("zoneInfoPumpVoltageRow").classList.toggle("hidden", !mainZone);
      document.getElementById("zoneInfoPumpSpeedModeRow").classList.toggle("hidden", !mainZone);
      document.getElementById("zoneInfoPumpSerialRow").classList.toggle("hidden", !mainZone);
      document.getElementById("zoneInfoBreaker").textContent = pin.circuit_breaker_ref || "Not set";
      document.getElementById("zoneInfoInstalledBy").textContent = pin.installed_by || "Not set";
      document.getElementById("zoneInfoInstallDate").textContent = pin.install_date || "Not set";
      document.getElementById("zoneInfoLastServiceDate").textContent = pin.last_service_date || "Not set";
      document.getElementById("zoneInfoColor").innerHTML = pin.color ? `<span class="pin-dot" style="background:${pinColorHex(pin.color)}"></span>${pin.color}` : "Not set";
      document.getElementById("zoneInfoCoords").textContent = (pin.lat !== undefined && pin.lng !== undefined) ? `${Number(pin.lat).toFixed(5)}, ${Number(pin.lng).toFixed(5)}` : "Pin not placed";
      document.getElementById("zoneInfoUpdated").textContent = pin.updated_utc ? String(pin.updated_utc).replace("T"," ").replace("+00:00"," UTC") : "Not set";
      document.getElementById("zoneInfoDesc").textContent = pin.description || "No location notes saved yet.";
      document.getElementById("zoneInfoAccess").textContent = pin.access_instructions || "No access instructions saved yet.";
      document.getElementById("zoneInfoPumpServiceNotes").textContent = pin.pump_service_notes || "No pump service notes saved yet.";
      document.getElementById("zoneInfoPumpServiceNotesWrap").classList.toggle("hidden", !mainZone);
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
    function setZoneMgmtMsg(msg) {
      const el = document.getElementById("zoneMgmtMsg");
      if (el) el.textContent = msg || "";
    }
    function setCommissionMsg(msg) {
      const el = document.getElementById("commissionMsg");
      if (el) el.textContent = msg || "";
    }
    function setBackupMsg(msg) {
      const el = document.getElementById("backupMsg");
      if (el) el.textContent = msg || "";
    }

    function setAdminTab(which) {
      ensureAdminToolsOpen();
      document.querySelectorAll("[data-admin-tab]").forEach((b) => b.classList.toggle("active", b.dataset.adminTab === which));
      document.getElementById("adminUsersPanel").classList.toggle("hidden", which !== "users");
      document.getElementById("adminAlertsPanel").classList.toggle("hidden", which !== "alerts");
      document.getElementById("adminZonesPanel").classList.toggle("hidden", which !== "zones");
      document.getElementById("adminCommissioningPanel").classList.toggle("hidden", which !== "commissioning");
      document.getElementById("adminBackupPanel").classList.toggle("hidden", which !== "backup");
      document.getElementById("adminMapPanel").classList.toggle("hidden", which !== "map");
      document.getElementById("adminSetupPanel").classList.toggle("hidden", which !== "setup");
      if (which === "zones") {
        refreshZoneMgmtSelect();
        const sel = document.getElementById("zoneMgmtSelect");
        if (sel && sel.value) loadZoneMgmtStatus(sel.value);
      }
      if (which === "map") {
        ensureSiteMapReady();
        setTimeout(() => { if (siteMapMap) siteMapMap.invalidateSize(); }, 80);
      }
      if (which === "commissioning") {
        refreshCommissioningZoneSelect();
        const sel = document.getElementById("commissionZoneSelect");
        if (sel && sel.value) loadCommissioningRecord(sel.value);
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
        hardware_fault: "Hardware Fault",
        hardware_recovered: "Hardware Recovered",
        probe_swap_suspected: "Probe Swap Suspected",
        probe_swap_recovered: "Probe Swap Recovered",
      };
      return labels[cat] || cat || "--";
    }
    function isTroubleCategory(cat) {
      return !["device_recovered", "hardware_recovered", "probe_swap_recovered"].includes(String(cat || ""));
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
      fillUserForm({ alerts: { temp_response:true, short_cycle:true, response_lag:true, no_response:true, low_temp:true, device_offline:true, device_recovered:true, hardware_fault:true, hardware_recovered:true }, role:"viewer", enabled:true });
      document.getElementById("saveUserBtn").dataset.editingId = "";
    }

    function adoptedDownstreamRows() {
      return realDownstreamRows((lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : []);
    }

    function refreshZoneMgmtSelect() {
      const sel = document.getElementById("zoneMgmtSelect");
      if (!sel) return;
      const rows = adoptedDownstreamRows();
      const prev = sel.value;
      sel.innerHTML = "";
      if (!rows.length) {
        const op = document.createElement("option");
        op.value = "";
        op.textContent = "-- No adopted zone devices --";
        sel.appendChild(op);
        return;
      }
      rows.forEach((z) => {
        const op = document.createElement("option");
        op.value = z.id;
        op.textContent = `${z.name || z.id} (${z.parent_zone || "--"})`;
        sel.appendChild(op);
      });
      if (rows.some((z) => z.id === prev)) sel.value = prev;
    }

    function renderZoneMgmtStatus(data) {
      zoneMgmtStatusCache = data || null;
      const zcfg = (data && data.zone_config) || {};
      const nodeZone = (data && data.node_zone) || {};
      const setup = (data && data.node_setup) || {};
      const cfg = (setup && setup.config) || {};
      const diag = (nodeZone && nodeZone.diagnostics && typeof nodeZone.diagnostics === "object") ? nodeZone.diagnostics : {};
      document.getElementById("zoneMgmtName").value = zcfg.name || nodeZone.unit_name || "";
      document.getElementById("zoneMgmtParent").value = zcfg.parent_zone || cfg.parent_zone || "Zone 1";
      document.getElementById("zoneMgmtZoneId").value = zcfg.id || cfg.zone_id || "";
      document.getElementById("zoneMgmtLowTemp").value = (nodeZone.low_temp_threshold_f ?? ((cfg.alerts || {}).low_temp_threshold_f ?? 35));
      document.getElementById("zoneMgmtLowTempEnabled").value = ((nodeZone.low_temp_enabled ?? ((cfg.alerts || {}).low_temp_enabled)) === false) ? "false" : "true";
      document.getElementById("zoneMgmtProbeSwapDelta").value = ((diag.probe_swap_idle_delta_threshold_f ?? ((cfg.alerts || {}).probe_swap_idle_delta_threshold_f)) ?? 5);
      document.getElementById("zoneMgmtProbeSummary").textContent = `Feed: ${nodeZone.feed_sensor_id || "--"} (${nodeZone.feed_f ?? "--"}F) | Return: ${nodeZone.return_sensor_id || "--"} (${nodeZone.return_f ?? "--"}F)`;
      const diagErr = data && (data.node_zone_error || data.node_setup_error);
      document.getElementById("zoneMgmtDiagStatus").textContent = diagErr
        ? `Node communication issue: ${diagErr}`
        : `${diag.self_diagnostic_status || "Unknown"}${diag.hardware_fault ? " (alerting enabled)" : ""}`;
      const issues = Array.isArray(diag.hardware_faults) ? diag.hardware_faults : [];
      const diagLines = issues.length ? issues.map((x) => `- ${x}`) : ["No hardware self-diagnostic issues reported."];
      if (diag.probe_swap_suspected) {
        diagLines.push("");
        diagLines.push("Recommended action: Use Swap Feed / Return, then verify which pipe stays hotter during idle (no 24VAC).");
      }
      document.getElementById("zoneMgmtDiagIssues").textContent = diagLines.join("\\n");
      document.getElementById("zoneMgmtFeedDiag").textContent = nodeZone.feed_error ? `Error: ${nodeZone.feed_error}` : "Reading OK";
      document.getElementById("zoneMgmtReturnDiag").textContent = nodeZone.return_error ? `Error: ${nodeZone.return_error}` : "Reading OK";
    }

    async function loadZoneMgmtStatus(zoneId) {
      if (!zoneId) { setZoneMgmtMsg("No adopted zone selected."); return; }
      setZoneMgmtMsg("Loading zone diagnostics...");
      try {
        const res = await fetch(`/api/zone-mgmt/status?zone_id=${encodeURIComponent(zoneId)}&_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "load failed");
        renderZoneMgmtStatus(data);
        setZoneMgmtMsg("Zone diagnostics loaded.");
      } catch (e) {
        setZoneMgmtMsg("Load failed: " + e.message);
      }
    }

    async function zoneMgmtSwapProbes() {
      const zoneId = document.getElementById("zoneMgmtSelect").value || "";
      if (!zoneId) { setZoneMgmtMsg("Select an adopted zone device."); return; }
      setZoneMgmtMsg("Swapping feed/return probes on device...");
      try {
        const res = await fetch("/api/zone-mgmt/action", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ zone_id: zoneId, action: "swap_probes" })
        });
        const data = await res.json();
        if (!res.ok || data.error || data.node_error) throw new Error(data.error || data.node_error || "swap failed");
        setZoneMgmtMsg("Feed/return probes swapped on device.");
        await loadZoneMgmtStatus(zoneId);
      } catch (e) {
        setZoneMgmtMsg("Swap failed: " + e.message);
      }
    }

    async function zoneMgmtSave() {
      const zoneId = document.getElementById("zoneMgmtSelect").value || "";
      if (!zoneId) { setZoneMgmtMsg("Select an adopted zone device."); return; }
      const body = {
        zone_id: zoneId,
        action: "save",
        name: document.getElementById("zoneMgmtName").value.trim(),
        parent_zone: document.getElementById("zoneMgmtParent").value.trim(),
        unit_name: document.getElementById("zoneMgmtName").value.trim(),
        alerts: {
          low_temp_enabled: document.getElementById("zoneMgmtLowTempEnabled").value === "true",
          low_temp_threshold_f: Number(document.getElementById("zoneMgmtLowTemp").value || 35),
          probe_swap_idle_delta_threshold_f: Number(document.getElementById("zoneMgmtProbeSwapDelta").value || 5)
        }
      };
      setZoneMgmtMsg("Saving zone changes...");
      try {
        const res = await fetch("/api/zone-mgmt/action", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body)
        });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || data.node_error || "save failed");
        if (data.node_error) setZoneMgmtMsg("Saved hub config, but node update had an issue: " + data.node_error);
        else setZoneMgmtMsg("Zone changes saved.");
        await refreshHub();
        refreshZoneMgmtSelect();
        await loadZoneMgmtStatus(zoneId);
      } catch (e) {
        setZoneMgmtMsg("Save failed: " + e.message);
      }
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

    function filteredAlertEventsForActiveView() {
      let arr = alertEventsCache.slice();
      if (activeAlertLogFilter === "trouble") {
        arr = arr.filter((ev) => isTroubleCategory(ev.category));
      } else if (activeAlertLogFilter === "unack") {
        arr = arr.filter((ev) => !ev.acknowledged);
      }
      return arr;
    }

    async function acknowledgeAlertEvent(ev, useCurrentNote=true) {
      if (!ev || !ev.id) return;
      const who = document.getElementById("ackBy").value.trim();
      const note = useCurrentNote ? document.getElementById("ackNote").value.trim() : "";
      setAlertsMsg("Acknowledging alert...");
      const res = await fetch("/api/alerts/ack", {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({ id: ev.id, who, note })
      });
      const data = await res.json();
      if (!res.ok || data.error || data.ok === false) {
        setAlertsMsg("Acknowledge failed: " + (data.error || data.message || "unknown"));
        return false;
      }
      renderAlertEvents(data.events || []);
      setAlertsMsg("Alert acknowledged.");
      return true;
    }

    async function acknowledgeAllVisibleAlerts() {
      const visible = filteredAlertEventsForActiveView().filter((ev) => !ev.acknowledged);
      if (!visible.length) {
        setAlertsMsg("No unacknowledged alerts in this view.");
        return;
      }
      if (!confirm(`Acknowledge ${visible.length} visible alert(s)?`)) return;
      let okCount = 0;
      for (const ev of visible) {
        try {
          const ok = await acknowledgeAlertEvent(ev, true);
          if (ok) okCount += 1;
        } catch (_) {}
      }
      setAlertsMsg(`Acknowledged ${okCount} alert(s).`);
      await loadAlertEvents();
      await refreshHub();
    }

    function renderAlertEvents(events) {
      alertEventsCache = Array.isArray(events) ? events : [];
      const body = document.getElementById("alertsBody");
      body.innerHTML = "";
      let arr = filteredAlertEventsForActiveView();
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
          const quickBtn = document.createElement("button");
          quickBtn.type = "button";
          quickBtn.className = "btn";
          quickBtn.style.padding = "4px 10px";
          quickBtn.textContent = "Quick Ack";
          quickBtn.addEventListener("click", async () => {
            await acknowledgeAlertEvent(ev, false);
            await refreshHub();
          });
          td.appendChild(quickBtn);
          const noteBtn = document.createElement("button");
          noteBtn.type = "button";
          noteBtn.className = "btn";
          noteBtn.style.padding = "4px 10px";
          noteBtn.style.marginLeft = "6px";
          noteBtn.textContent = "Ack + Note";
          noteBtn.addEventListener("click", async () => {
            await acknowledgeAlertEvent(ev, true);
            await refreshHub();
          });
          td.appendChild(noteBtn);
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
        document.getElementById("notifCooldown").value = Number(data.cooldown_minutes || 120);
        document.getElementById("notifEmailEnabled").value = email.enabled ? "true" : "false";
      } catch (e) {
        setNotifMsg("Failed to load notification settings.");
      }
    }

    async function saveNotifications() {
      const cooldown = Number(document.getElementById("notifCooldown").value || 120);
      const emailEnabled = document.getElementById("notifEmailEnabled").value === "true";
      setNotifMsg("Saving notification settings...");
      try {
        const res = await fetch("/api/notifications", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            cooldown_minutes: cooldown,
            email: { enabled: emailEnabled }
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
      scrollToAdminPanel("adminAlertsPanel");
      loadAlertEvents();
    }
    function closeTasksMenu() {
      const menu = document.getElementById("tasksMenu");
      const btn = document.getElementById("tasksPill");
      if (menu) menu.classList.remove("show");
      if (btn) btn.setAttribute("aria-expanded", "false");
    }
    function toggleTasksMenu() {
      const menu = document.getElementById("tasksMenu");
      const btn = document.getElementById("tasksPill");
      if (!menu || !btn) return;
      const show = !menu.classList.contains("show");
      menu.classList.toggle("show", show);
      btn.setAttribute("aria-expanded", show ? "true" : "false");
    }
    function initHeaderActions() {
      const tasksBtn = document.getElementById("tasksPill");
      const addDeviceBtn = document.getElementById("addDevicePill");
      if (tasksBtn) tasksBtn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        toggleTasksMenu();
      });
      const menuUnack = document.getElementById("tasksMenuUnack");
      const menuAckAll = document.getElementById("tasksMenuAckAll");
      const menuTrouble = document.getElementById("tasksMenuTrouble");
      const menuAll = document.getElementById("tasksMenuAll");
      if (menuUnack) menuUnack.addEventListener("click", () => { closeTasksMenu(); openAlertPanelWithFilter("unack"); });
      if (menuAckAll) menuAckAll.addEventListener("click", async () => {
        closeTasksMenu();
        setAdminTab("alerts");
        setAlertLogFilter("unack");
        scrollToAdminPanel("adminAlertsPanel");
        await loadAlertEvents();
        await acknowledgeAllVisibleAlerts();
      });
      if (menuTrouble) menuTrouble.addEventListener("click", () => { closeTasksMenu(); openAlertPanelWithFilter("trouble"); });
      if (menuAll) menuAll.addEventListener("click", () => { closeTasksMenu(); openAlertPanelWithFilter("all"); });
      document.addEventListener("click", (e) => {
        const wrap = document.querySelector(".head-action-wrap");
        if (!wrap) return;
        if (!wrap.contains(e.target)) closeTasksMenu();
      });
      if (addDeviceBtn) addDeviceBtn.addEventListener("click", openSetupPanel);
    }
    function initOverviewTabs() {
      document.querySelectorAll("[data-overview-parent]").forEach((btn) => {
        btn.addEventListener("click", () => setOverviewParent(btn.dataset.overviewParent || "Zone 1"));
      });
    }

    initButtons();
    initReturnToMainButtons();
    initFloatingTopButton();
    initCycleButtons();
    initAdminTabs();
    initAlertLogFilters();
    initHeaderActions();
    initOverviewTabs();
    bindZoneInfoButtons();
    const firstZoneBtn = document.getElementById("firstZoneSetupBtn");
    if (firstZoneBtn) firstZoneBtn.addEventListener("click", openSetupPanel);
    document.getElementById("siteMapFindBtn").addEventListener("click", geocodeSiteAddress);
    document.getElementById("siteMapFootprintBtn").addEventListener("click", loadBuildingOutline);
    document.getElementById("siteMapSaveBtn").addEventListener("click", saveSiteMapConfig);
    document.getElementById("siteMapClearPinBtn").addEventListener("click", clearSelectedSiteMapPin);
    document.getElementById("siteMapStartAreaBtn").addEventListener("click", startZoneAreaDraw);
    document.getElementById("siteMapFinishAreaBtn").addEventListener("click", finishZoneAreaDraw);
    document.getElementById("siteMapCancelAreaBtn").addEventListener("click", cancelZoneAreaDraw);
    document.getElementById("siteMapClearAreaBtn").addEventListener("click", clearSelectedZoneArea);
    document.getElementById("siteMapApplyDetailsBtn").addEventListener("click", applySelectedSiteMapDetails);
    document.getElementById("siteMapClearPhotoBtn").addEventListener("click", clearSelectedSiteMapPhoto);
    document.getElementById("siteMapPhotoInput").addEventListener("change", handleSiteMapPhotoInput);
    document.getElementById("siteMapZoneSelect").addEventListener("change", syncSiteMapDetailsFormFromSelected);
    document.getElementById("siteMapLayer").addEventListener("change", (e) => setSiteMapBaseLayer((e && e.target && e.target.value) || "street"));
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
    document.getElementById("openUsersFromNotifBtn").addEventListener("click", openUsersPanel);
    document.getElementById("saveUserBtn").addEventListener("click", saveUser);
    document.getElementById("clearUserFormBtn").addEventListener("click", clearUserForm);
    document.getElementById("refreshAlertsBtn").addEventListener("click", loadAlertEvents);
    document.getElementById("ackAllVisibleBtn").addEventListener("click", acknowledgeAllVisibleAlerts);
    document.getElementById("alertLimit").addEventListener("change", loadAlertEvents);
    document.getElementById("zoneMgmtSelect").addEventListener("change", (e) => {
      const zid = (e && e.target && e.target.value) ? e.target.value : "";
      if (zid) loadZoneMgmtStatus(zid);
      else setZoneMgmtMsg("No adopted zone selected.");
    });
    document.getElementById("zoneMgmtRefreshBtn").addEventListener("click", () => {
      const zid = document.getElementById("zoneMgmtSelect").value || "";
      if (!zid) { setZoneMgmtMsg("No adopted zone selected."); return; }
      loadZoneMgmtStatus(zid);
    });
    document.getElementById("zoneMgmtSwapBtn").addEventListener("click", zoneMgmtSwapProbes);
    document.getElementById("zoneMgmtSaveBtn").addEventListener("click", zoneMgmtSave);
    document.getElementById("commissionZoneSelect").addEventListener("change", (e) => {
      const zid = (e && e.target && e.target.value) ? e.target.value : "";
      loadCommissioningRecord(zid);
    });
    document.getElementById("commissionRefreshBtn").addEventListener("click", () => {
      const zid = document.getElementById("commissionZoneSelect").value || "";
      loadCommissioningRecord(zid);
    });
    document.querySelectorAll("[data-commission-check]").forEach((cb) => cb.addEventListener("change", updateCommissioningSummary));
    document.getElementById("commissionSaveBtn").addEventListener("click", saveCommissioningRecord);
    document.getElementById("backupExportBtn").addEventListener("click", exportBackupJson);
    document.getElementById("backupRestoreBtn").addEventListener("click", restoreBackupJson);
    clearUserForm();
    updateCommissioningSummary();
    setAlertLogFilter("trouble");
    refreshMain();
    refreshHub();
    renderZoneOverview();
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

        if route == "/api/commissioning":
            zone_id = str(query.get("zone_id", [""])[0]).strip()
            cfg = load_commissioning_config()
            records = cfg.get("records", {}) if isinstance(cfg, dict) else {}
            if not isinstance(records, dict):
                records = {}
            payload = {
                "record": records.get(zone_id, {}) if zone_id else {},
                "records": records if not zone_id else None,
                "updated_utc": now_utc_iso(),
            }
            b = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/backup/export":
            b = json.dumps(backup_export_bundle()).encode("utf-8")
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

        if route == "/api/zone-mgmt/status":
            zone_id = str(query.get("zone_id", [""])[0]).strip()
            if not zone_id:
                b = json.dumps({"error": "zone_id required"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            zcfg = zone_config_entry(zone_id)
            if not zcfg:
                b = json.dumps({"error": "zone not found"}).encode("utf-8")
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            base = str(zcfg.get("source_url") or "").replace("/api/zone", "").rstrip("/")
            node_zone, node_zone_err = (None, "no source_url")
            node_setup, node_setup_err = (None, "no source_url")
            if base:
                node_zone, node_zone_err = fetch_remote_json(base + "/api/zone")
                node_setup, node_setup_err = fetch_remote_json(base + "/api/setup-state")
            b = json.dumps({
                "zone_config": zcfg,
                "node_base_url": base,
                "node_zone": node_zone,
                "node_zone_error": node_zone_err,
                "node_setup": node_setup,
                "node_setup_error": node_setup_err,
                "updated_utc": now_utc_iso(),
            }).encode("utf-8")
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

        if route == "/api/building-footprint":
            q = str(query.get("q", [""])[0]).strip()
            if not q:
                b = json.dumps({"error": "q required"}).encode("utf-8")
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
                    "limit": 3,
                    "polygon_geojson": 1,
                })
                req = Request(url, headers={"User-Agent": "165-water-street-heating-hub/1.0"})
                with urlopen(req, timeout=10) as resp:
                    raw = resp.read().decode("utf-8")
                parsed_results = json.loads(raw)
                picked = None
                if isinstance(parsed_results, list):
                    for r in parsed_results:
                        if not isinstance(r, dict):
                            continue
                        gj = r.get("geojson")
                        if isinstance(gj, dict) and gj.get("type") in ("Polygon", "MultiPolygon"):
                            picked = {
                                "display_name": str(r.get("display_name") or ""),
                                "lat": r.get("lat"),
                                "lng": r.get("lon"),
                                "geojson": gj,
                            }
                            break
                if not picked:
                    b = json.dumps({"error": "no building outline found", "building_outline": None}).encode("utf-8")
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(b)))
                    self.end_headers()
                    self.wfile.write(b)
                    return
                b = json.dumps({
                    "query": q,
                    "building_outline": picked.get("geojson"),
                    "display_name": picked.get("display_name"),
                    "center": {"lat": picked.get("lat"), "lng": picked.get("lng")},
                    "updated_utc": now_utc_iso(),
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            except Exception as e:
                b = json.dumps({"error": f"footprint lookup failed: {e}"}).encode("utf-8")
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
            "/api/zone-mgmt/action",
            "/api/commissioning/save",
            "/api/backup/restore",
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

        if route == "/api/commissioning/save":
            zone_id = str(body.get("zone_id") or "").strip()
            if not zone_id:
                b = json.dumps({"error": "zone_id is required"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            cfg = load_commissioning_config()
            records = cfg.get("records", {}) if isinstance(cfg.get("records"), dict) else {}
            rec = records.get(zone_id, {}) if isinstance(records.get(zone_id, {}), dict) else {}
            checks_in = body.get("checks", {}) if isinstance(body.get("checks"), dict) else {}
            check_keys = (
                "call_input_verified",
                "feed_return_confirmed",
                "temp_response_verified",
                "photo_added",
                "pin_placed",
                "alert_test_completed",
            )
            checks = {k: bool(checks_in.get(k, False)) for k in check_keys}
            all_done = all(checks.values()) if checks else False
            completed_utc = str(rec.get("completed_utc") or "")
            if all_done and not completed_utc:
                completed_utc = now_utc_iso()
            if not all_done:
                completed_utc = ""
            updated_rec = {
                "checks": checks,
                "notes": str(body.get("notes") or ""),
                "completed_by": str(body.get("completed_by") or ""),
                "completed_utc": completed_utc,
                "updated_utc": now_utc_iso(),
            }
            records[zone_id] = updated_rec
            cfg["records"] = records
            cfg["updated_utc"] = now_utc_iso()
            if not save_commissioning_config(cfg):
                b = json.dumps({"error": "failed to save commissioning checklist"}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            b = json.dumps({
                "ok": True,
                "message": "Commissioning checklist saved.",
                "zone_id": zone_id,
                "record": updated_rec,
                "updated_utc": now_utc_iso(),
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/backup/restore":
            ok, msg = restore_from_backup_bundle(body)
            if ok:
                try:
                    refresh_downstream_cache()
                except Exception:
                    pass
            b = json.dumps({"ok": ok, "message": msg, "updated_utc": now_utc_iso()}).encode("utf-8")
            self.send_response(200 if ok else 400)
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
                cfg["map_layer"] = str(body.get("map_layer") or cfg.get("map_layer") or "street")
                bo = body.get("building_outline")
                if bo is None:
                    cfg["building_outline"] = None
                elif isinstance(bo, dict) and bo.get("type") in ("Polygon", "MultiPolygon"):
                    cfg["building_outline"] = bo
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
                            "pump_model": str(p.get("pump_model") or ""),
                            "pump_voltage": str(p.get("pump_voltage") or ""),
                            "pump_speed_mode": str(p.get("pump_speed_mode") or ""),
                            "pump_serial": str(p.get("pump_serial") or ""),
                            "pump_service_notes": str(p.get("pump_service_notes") or ""),
                            "circuit_breaker_ref": str(p.get("circuit_breaker_ref") or ""),
                            "description": str(p.get("description") or ""),
                            "photo_data_url": str(p.get("photo_data_url") or ""),
                            "polygon": [],
                            "updated_utc": str(p.get("updated_utc") or now_utc_iso()),
                        }
                        poly = p.get("polygon")
                        if isinstance(poly, list):
                            norm_poly = []
                            for pt in poly:
                                if not isinstance(pt, dict):
                                    continue
                                try:
                                    norm_poly.append({"lat": float(pt.get("lat")), "lng": float(pt.get("lng"))})
                                except Exception:
                                    continue
                            pins_out[str(zid)]["polygon"] = norm_poly
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

        if route == "/api/zone-mgmt/action":
            zone_id = str(body.get("zone_id") or "").strip()
            action = str(body.get("action") or "").strip()
            if not zone_id or not action:
                b = json.dumps({"error": "zone_id and action required"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            zones = load_config()
            zcfg = None
            zidx = None
            for i, z in enumerate(zones):
                if isinstance(z, dict) and str(z.get("id") or "").strip() == zone_id:
                    zcfg = dict(z); zidx = i; break
            if zcfg is None:
                b = json.dumps({"error": "zone not found"}).encode("utf-8")
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            base = str(zcfg.get("source_url") or "").replace("/api/zone", "").rstrip("/")
            node_result = None
            node_error = None
            if action == "swap_probes":
                if not base:
                    node_error = "zone has no source_url"
                else:
                    node_result, node_error = post_remote_json(base + "/api/manage/swap-probes", {})
            elif action == "save":
                # Update hub-side display/config first.
                for k in ("name", "parent_zone"):
                    if k in body:
                        zcfg[k] = str(body.get(k) or "").strip()
                if "delta_ok_f" in body:
                    try:
                        zcfg["delta_ok_f"] = float(body.get("delta_ok_f"))
                    except Exception:
                        pass
                if zidx is not None:
                    zones[zidx] = zcfg
                    if not save_config_zones(zones):
                        node_error = "failed to save hub zone config"
                # Then push editable device settings to node (if reachable).
                if base:
                    node_payload = {
                        "unit_name": body.get("unit_name"),
                        "zone_id": body.get("zone_id"),
                        "parent_zone": body.get("parent_zone"),
                    }
                    if isinstance(body.get("alerts"), dict):
                        node_payload["alerts"] = body.get("alerts")
                    node_result, post_err = post_remote_json(base + "/api/manage/update", node_payload)
                    if post_err:
                        node_error = (node_error + "; " if node_error else "") + post_err
                else:
                    node_error = (node_error + "; " if node_error else "") + "zone has no source_url"
            else:
                b = json.dumps({"error": "unsupported action"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            b = json.dumps({
                "ok": node_error is None,
                "zone_config": zcfg,
                "node_result": node_result,
                "node_error": node_error,
                "updated_utc": now_utc_iso(),
            }).encode("utf-8")
            self.send_response(200 if node_error is None else 502)
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
