#!/usr/bin/env python3
import json
import threading
import smtplib
import uuid
import subprocess
import re
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
        "building_floors": 1,
        "building_floor_labels": [],
        "map_layer": "street",
        "building_confirm_pin": None,
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
        try:
            cfg["building_floors"] = max(1, int(data.get("building_floors", cfg["building_floors"])))
        except Exception:
            pass
        labels_in = data.get("building_floor_labels")
        if isinstance(labels_in, list):
            labels_out = []
            for item in labels_in:
                if len(labels_out) >= 24:
                    break
                labels_out.append(str(item or "").strip()[:120])
            cfg["building_floor_labels"] = labels_out
        cfg["map_layer"] = str(data.get("map_layer") or "street")
        bcp = data.get("building_confirm_pin")
        if isinstance(bcp, dict):
            try:
                cfg["building_confirm_pin"] = {
                    "lat": float(bcp.get("lat")),
                    "lng": float(bcp.get("lng")),
                }
            except Exception:
                pass
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
                "main_boiler_floor": str(p.get("main_boiler_floor") or ""),
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
            try:
                cfg_out["building_floors"] = max(1, int(cfg.get("building_floors", cfg_out["building_floors"])))
            except Exception:
                pass
            labels_in = cfg.get("building_floor_labels")
            if isinstance(labels_in, list):
                labels_out = []
                for item in labels_in:
                    if len(labels_out) >= 24:
                        break
                    labels_out.append(str(item or "").strip()[:120])
                cfg_out["building_floor_labels"] = labels_out
            cfg_out["map_layer"] = str(cfg.get("map_layer") or "street")
            bcp = cfg.get("building_confirm_pin")
            if isinstance(bcp, dict):
                try:
                    cfg_out["building_confirm_pin"] = {
                        "lat": float(bcp.get("lat")),
                        "lng": float(bcp.get("lng")),
                    }
                except Exception:
                    pass
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
                        "main_boiler_floor": str(p.get("main_boiler_floor") or ""),
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
            faults.append(f"Supply probe error: {z.get('feed_error')}")
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
                        "Recommendation: Verify supply/return probe placement or use Swap Supply / Return in Zone Management.\n"
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
            return "Notify Admin", "Call active, missing supply/return temperature", True
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
        f"Supply: {feed} F\n"
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


def slugify_zone_id(text):
    s = re.sub(r"[^a-z0-9]+", "-", str(text or "").strip().lower()).strip("-")
    return s or f"zone-{uuid.uuid4().hex[:8]}"


def normalize_zone_node_base_url(url):
    raw = str(url or "").strip()
    if not raw:
        return ""
    if not raw.startswith("http://") and not raw.startswith("https://"):
        raw = "http://" + raw
    parsed = urlparse(raw)
    scheme = parsed.scheme or "http"
    host = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ""
    host = host.rstrip("/")
    if not host:
        return ""
    if not path or path == "/":
        return f"{scheme}://{host}:8090".replace(":8090:8090", ":8090")
    if path.endswith("/api/zone"):
        return f"{scheme}://{host}{path[:-8]}".rstrip("/")
    return f"{scheme}://{host}{path}".rstrip("/")


def tailscale_peer_ips():
    ips = []
    try:
        cp = subprocess.run(["tailscale", "status", "--json"], capture_output=True, text=True, timeout=5)
        if cp.returncode != 0:
            return ips
        data = json.loads(cp.stdout or "{}")
        peers = data.get("Peer", {}) if isinstance(data, dict) else {}
        if isinstance(peers, dict):
            for _, p in peers.items():
                if not isinstance(p, dict):
                    continue
                for ip in p.get("TailscaleIPs", []) or []:
                    s = str(ip or "")
                    if s.startswith("100."):
                        ips.append(s)
        self_info = data.get("Self", {}) if isinstance(data, dict) else {}
        for ip in (self_info.get("TailscaleIPs", []) or []):
            s = str(ip or "")
            if s.startswith("100."):
                ips.append(s)
    except Exception:
        return []
    # preserve order, dedupe
    out, seen = [], set()
    for ip in ips:
        if ip in seen:
            continue
        seen.add(ip)
        out.append(ip)
    return out


def discovered_zone_nodes():
    adopted = {}
    for z in load_config():
        if not isinstance(z, dict):
            continue
        src = str(z.get("source_url") or "").strip()
        base = normalize_zone_node_base_url(src.replace("/api/zone", ""))
        if base:
            adopted[base] = {"zone_id": str(z.get("id") or ""), "name": str(z.get("name") or "")}
    rows = []
    for ip in tailscale_peer_ips():
        base = f"http://{ip}:8090"
        setup, setup_err = fetch_remote_json(base + "/api/setup-state")
        if not isinstance(setup, dict):
            continue
        cfg = setup.get("config", {}) if isinstance(setup.get("config"), dict) else {}
        zone, zone_err = fetch_remote_json(base + "/api/zone")
        zdict = zone if isinstance(zone, dict) else {}
        rows.append({
            "base_url": base,
            "api_url": base + "/api/zone",
            "ip": ip,
            "configured": bool(setup.get("configured")),
            "adopted": base in adopted,
            "adopted_zone_id": adopted.get(base, {}).get("zone_id", ""),
            "adopted_name": adopted.get(base, {}).get("name", ""),
            "unit_name": str((zdict.get("unit_name") or cfg.get("unit_name") or "")).strip(),
            "zone_id": str((zdict.get("zone_id") or cfg.get("zone_id") or "")).strip(),
            "parent_zone": str((zdict.get("parent_zone") or cfg.get("parent_zone") or "Zone 1")).strip() or "Zone 1",
            "supply_f": zdict.get("feed_f"),
            "return_f": zdict.get("return_f"),
            "call_status": zdict.get("call_status"),
            "setup_error": setup_err,
            "zone_error": zone_err,
            "updated_utc": now_utc_iso(),
        })
    rows.sort(key=lambda r: (r.get("adopted", False), r.get("configured", False), (r.get("unit_name") or r.get("ip") or "").lower()))
    return rows


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
  <title>Heating Hub</title>
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
    .site-map-tools > .full{grid-column:1 / -1}
    .site-map-card.building-setup-mode .site-map-tools{grid-template-columns:minmax(0,1fr) auto}
    .site-map-subtools{display:grid;grid-template-columns:1fr 1fr auto;gap:10px;align-items:end;margin-top:10px}
    .site-map-workspace-bar{display:none;align-items:center;justify-content:space-between;gap:10px;margin-top:10px;padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:#fff}
    .site-map-workspace-bar.show{display:flex}
    .site-map-workspace-bar .left{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
    .site-map-workspace-bar .right{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
    .site-map-card.workspace{
      position:fixed; inset:12px; z-index:1900; margin:0 !important; overflow:auto;
      background:#f9fcfe; box-shadow:0 24px 64px rgba(10,31,44,.28);
    }
    .site-map-card.workspace #siteMapCanvas{height:min(62vh,620px)}
    .site-map-card.building-setup-mode #siteMapZoneSelectWrap{display:none}
    .site-map-card.building-setup-mode #siteMapZoneMappingSection{display:none !important}
    .site-map-card.building-setup-mode #siteMapZoneWizard{display:none !important}
    .site-map-card.building-setup-mode #siteMapClearPinBtn,
    .site-map-card.building-setup-mode #siteMapZoneSetupBtn,
    .site-map-card.building-setup-mode #siteMapSaveBtn{display:none}
    .site-map-card.zone-setup-mode .site-map-tools{display:none !important}
    .site-map-card.zone-setup-mode .site-map-subtools{display:none !important}
    .site-map-card.zone-setup-mode #siteMapOutlineWizard{display:none !important}
    .site-map-card.zone-setup-mode #siteMapZoneMappingLockedNote{display:none !important}
    .site-map-zone-wizard{display:none;margin-top:10px;border:1px solid var(--border);border-radius:12px;background:#fff;padding:12px}
    .site-map-zone-wizard.show{display:block}
    .site-map-zone-wizard .mode-tabs{display:flex;gap:8px;flex-wrap:wrap}
    .site-map-zone-wizard .mode-tabs .btn{padding:4px 10px;font-size:.85rem}
    .site-map-zone-wizard .actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
    .site-map-zone-wizard .actions .btn{padding:4px 10px;font-size:.85rem}
    body.map-workspace-open{overflow:hidden}
    .site-map-wizard{display:none;margin-top:10px;border:1px solid var(--border);border-radius:12px;background:#fff;padding:12px}
    .site-map-wizard.show{display:block}
    .site-map-wizard .steps{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;margin-top:8px}
    .site-map-wizard .step{border:1px solid var(--border);border-radius:10px;padding:8px;background:#f8fbfd}
    .site-map-wizard .step.active{border-color:#8bc2d3;background:#eef8fc}
    .site-map-wizard .step.done{border-color:#b8e0c7;background:#f1fbf5}
    .site-map-wizard .step.next-required{border-color:#f59e0b;background:#fff7e6;box-shadow:inset 0 0 0 1px rgba(245,158,11,.25)}
    .site-map-wizard .step .n{font-weight:800;font-size:.75rem;color:var(--muted)}
    .site-map-wizard .step .t{font-weight:700;margin-top:2px}
    .site-map-wizard .step .s{font-size:.82rem;color:var(--muted);margin-top:4px}
    .site-map-wizard .actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
    .site-map-card.workspace .site-map-wizard.show{
      position:sticky; top:70px; z-index:1910; margin-top:8px;
      display:flex; flex-direction:column; gap:8px;
      padding:10px; border-radius:12px;
      box-shadow:0 10px 24px rgba(10,31,44,.12);
    }
    .site-map-card.workspace .site-map-wizard > *{margin-top:0 !important}
    .site-map-card.workspace .site-map-wizard > :nth-child(5){order:2} /* actions */
    .site-map-card.workspace .site-map-wizard > :nth-child(2){order:3} /* status */
    .site-map-card.workspace .site-map-wizard > :nth-child(3){order:4} /* progress */
    .site-map-card.workspace .site-map-wizard > :nth-child(4){order:5} /* steps */
    .site-map-card.workspace .site-map-wizard > :nth-child(6){order:6} /* form grid */
    .site-map-card.workspace .site-map-wizard .steps{
      display:flex; flex-wrap:nowrap; overflow:auto; gap:6px; padding-bottom:2px;
      scrollbar-width:thin;
    }
    .site-map-card.workspace .site-map-wizard .step{
      min-width:150px; padding:6px 8px; background:#f8fbfd;
    }
    .site-map-card.workspace .site-map-wizard .step .s{display:none}
    .site-map-card.workspace .site-map-wizard .step .t{font-size:.86rem}
    .site-map-card.workspace .site-map-wizard .step .n{font-size:.7rem}
    .site-map-card.workspace .site-map-wizard .actions{
      position:sticky; top:0; z-index:1; margin-top:0;
      background:#fff; padding-bottom:2px;
    }
    .site-map-card.workspace .site-map-wizard .actions .btn{padding:3px 8px;font-size:.82rem}
    .site-map-card.workspace .site-map-wizard .form-grid{gap:8px}
    .site-map-card.workspace .site-map-wizard .msg{font-size:.85rem}
    .site-map-card.workspace .site-map-wizard .smallnote{font-size:.78rem}
    .site-map-card.workspace .site-map-wizard.compact-early .form-grid{display:none}
    .site-map-card.workspace .site-map-wizard.compact-early .steps{margin-top:4px}
    .site-map-card.workspace .site-map-wizard.compact-early .step{min-width:130px}
    .site-map-card.workspace .site-map-wizard.compact-early.show{
      position:absolute; top:126px; left:10px; right:10px; z-index:1915;
      max-height:min(34vh,220px); overflow:auto;
      margin-top:0;
    }
    .site-map-card.workspace .site-map-wizard.compact-early.show .steps{display:none}
    .site-map-card.workspace .site-map-wizard.compact-early.show #siteMapOutlineProgress{display:none}
    .site-map-card.workspace .site-map-wizard.compact-early.show .actions{
      position:static; background:transparent; padding-bottom:0;
    }
    .site-map-card.workspace.step-map-focus .site-map-tools{display:none}
    .site-map-card.workspace.step-map-focus #siteMapZoneMappingLockedNote{display:none !important}
    .site-map-card.workspace.step-map-focus #siteMapCanvas{
      display:block !important;
      height:clamp(360px, 58vh, 700px);
      margin-top:8px;
    }
    .site-map-card.workspace.step-map-focus #siteMapPinsList{display:none}
    .site-map-card.workspace.step-map-focus .site-map-subtools{display:none}
    .site-map-card.workspace.step-map-focus .form-grid{display:none}
    .pin-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:8px;margin-top:10px}
    .pin-chip{border:1px solid var(--border);border-radius:10px;padding:8px 10px;background:#f8fbfd}
    .pin-chip .t{font-weight:700}
    .pin-chip .s{color:var(--muted);font-size:.86rem}
    .pin-dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:middle;border:1px solid rgba(0,0,0,.15)}
    .zone-info-modal{position:fixed;inset:0;background:rgba(5,12,18,.45);display:none;align-items:center;justify-content:center;padding:16px;z-index:2000}
    .zone-info-modal.show{display:flex}
    .zone-info-card{max-width:760px;width:min(100%,760px);max-height:88vh;overflow:auto;background:#fff;border-radius:16px;border:1px solid var(--border);box-shadow:0 18px 36px rgba(10,31,44,.24);padding:14px}
    .site-floor-modal{position:fixed;inset:0;background:rgba(5,12,18,.5);display:none;align-items:center;justify-content:center;padding:16px;z-index:2200}
    .site-floor-modal.show{display:flex}
    .site-floor-modal-card{max-width:680px;width:min(100%,680px);max-height:88vh;overflow:auto;background:#fff;border-radius:16px;border:1px solid var(--border);box-shadow:0 18px 36px rgba(10,31,44,.24);padding:14px}
    .site-confirm-modal{position:fixed;inset:0;background:rgba(5,12,18,.5);display:none;align-items:center;justify-content:center;padding:16px;z-index:2200}
    .site-confirm-modal.show{display:flex}
    .site-confirm-modal-card{max-width:620px;width:min(100%,620px);max-height:88vh;overflow:auto;background:#fff;border-radius:16px;border:1px solid var(--border);box-shadow:0 18px 36px rgba(10,31,44,.24);padding:14px}
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
    #graphTitle.flash-target{background:#e0f2fe;border-radius:8px;padding:4px 8px;transition:background .45s ease}
    .map-preview-wrap{margin-top:14px}
    .map-preview-svg{width:100%;height:auto;min-height:260px;border:1px solid var(--border);border-radius:12px;background:#fbfdff;display:block}
    .map-preview-legend{display:flex;gap:10px;flex-wrap:wrap;margin-top:8px}
    .map-preview-legend .chip{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--border);border-radius:999px;padding:4px 8px;background:#fff;font-size:.82rem}
    .map-preview-legend .sw{display:inline-block;width:12px;height:12px;border-radius:999px;border:1px solid rgba(0,0,0,.18)}
    .map-preview-note{margin-top:6px;font-size:.82rem;color:var(--muted)}
    @media (max-width:760px){.zone-info-grid{grid-template-columns:1fr}}
    @media (max-width:760px) {
      .wrap { padding:.75rem; }
      .head { padding:14px; border-radius:14px; }
      .temp { font-size:1.35rem; }
      .form-grid{grid-template-columns:1fr}
      .site-map-tools,.site-map-subtools{grid-template-columns:1fr}
      .site-map-workspace-bar{flex-direction:column;align-items:stretch}
      .site-map-workspace-bar .left,.site-map-workspace-bar .right{width:100%}
      .site-map-card.workspace{inset:8px}
      .site-map-card.workspace .site-map-wizard.show{top:8px}
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
        <h1 id=\"hubTitle\">Heating Hub</h1>
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
        <div class=\"head-action-wrap\">
          <button id=\"adminToolsPill\" type=\"button\" class=\"head-pill-btn\" aria-expanded=\"false\" aria-controls=\"adminToolsMenu\">Admin Tools<span class=\"sub\">Open Admin Tools Menu</span></button>
          <div id=\"adminToolsMenu\" class=\"head-menu\" role=\"menu\" aria-label=\"Admin Tools Menu\">
            <button type=\"button\" class=\"mitem\" id=\"adminMenuUsers\" role=\"menuitem\">Users<span class=\"sub\">Manage users, roles, and alert subscriptions</span></button>
            <button type=\"button\" class=\"mitem\" id=\"adminMenuAlerts\" role=\"menuitem\">Alarm Acknowledge<span class=\"sub\">Trouble log, alert log, acknowledgements</span></button>
            <button type=\"button\" class=\"mitem\" id=\"adminMenuAdopt\" role=\"menuitem\">Adopt Devices<span class=\"sub\">Find unassigned zone nodes and add them to the hub</span></button>
            <button type=\"button\" class=\"mitem\" id=\"adminMenuZones\" role=\"menuitem\">Zone Management<span class=\"sub\">Post-install zone edits and diagnostics</span></button>
            <button type=\"button\" class=\"mitem\" id=\"adminMenuMap\" role=\"menuitem\">Site Map<span class=\"sub\">Building setup and zone mapping</span></button>
            <button type=\"button\" class=\"mitem\" id=\"adminMenuSetup\" role=\"menuitem\">Setup New Device<span class=\"sub\">Installer onboarding and hub link</span></button>
            <button type=\"button\" class=\"mitem\" id=\"adminMenuCommissioning\" role=\"menuitem\">Commissioning<span class=\"sub\">Checklist and install verification</span></button>
            <button type=\"button\" class=\"mitem\" id=\"adminMenuBackup\" role=\"menuitem\">Backup / Restore<span class=\"sub\">Export or restore hub configuration</span></button>
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
        <div class=\"row\"><div><div class=\"label\">Supply</div><div id=\"feed_1_sid\" class=\"sid\">--</div></div><div><span id=\"feed_1\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Return</div><div id=\"return_1_sid\" class=\"sid\">--</div></div><div><span id=\"return_1\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Call Status</div><div class=\"sid\">GPIO17 Dry Contact</div></div><div><span id=\"zone1_call_status\" class=\"temp\" style=\"font-size:1.0rem;\">--</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Zone 1 Status</div><div class=\"sid\">Call + supply/return check</div></div><div><span id=\"zone1_status\" class=\"temp\" style=\"font-size:1.0rem;\">--</span></div></div>
      </article>
      <article class=\"card\">
        <div style=\"display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px\">
          <h2 style=\"margin:0\">Zone 2 (Main)</h2>
          <button type=\"button\" class=\"btn\" data-zone-info=\"zone2_main\" style=\"padding:4px 10px\">Zone Info</button>
        </div>
        <div class=\"row\"><div><div class=\"label\">Supply</div><div id=\"feed_2_sid\" class=\"sid\">--</div></div><div><span id=\"feed_2\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
        <div class=\"row\"><div><div class=\"label\">Return</div><div id=\"return_2_sid\" class=\"sid\">--</div></div><div><span id=\"return_2\" class=\"temp\">--</span><span class=\"unit\">°F</span></div></div>
      </article>
    </section>

    <section class=\"chart-wrap\">
      <div class=\"chart-head\">
        <h3>Zone Performance Overview</h3>
          <div class=\"smallnote\">Click a parent zone to see its downstream devices and quick performance stats, then jump to the graph.</div>
          <div id=\"zonePerfMsg\" class=\"smallnote\" style=\"margin-top:6px\"></div>
      </div>
      <div id=\"overviewParentTabs\" class=\"tabs\" style=\"margin-top:10px\">
        <button class=\"tab active\" data-overview-parent=\"Zone 1\">Zone 1</button>
        <button class=\"tab\" data-overview-parent=\"Zone 2\">Zone 2</button>
      </div>
      <div id=\"overviewKpis\" class=\"overview-grid\"></div>
      <div id=\"overviewCards\" class=\"overview-list\"></div>
    </section>

    <section id=\"mainMapPreviewSection\" class=\"chart-wrap hidden\">
      <div class=\"chart-head\">
        <div>
          <h3>Building Map Preview</h3>
          <div class=\"smallnote\">Satellite overhead preview auto-framed to the saved building outline with color-matched zone pins and branch lines.</div>
        </div>
        <div>
          <button type=\"button\" class=\"btn\" id=\"mainMapPreviewEditZonesBtn\" style=\"padding:4px 10px;margin-right:6px\">Edit Zone Map</button>
          <button type=\"button\" class=\"btn\" id=\"mainMapPreviewEditBtn\" style=\"padding:4px 10px\">Edit Building Map</button>
        </div>
      </div>
      <div class=\"map-preview-wrap\">
        <svg id=\"mainMapPreviewSvg\" class=\"map-preview-svg\" viewBox=\"0 0 1000 560\" preserveAspectRatio=\"xMidYMid meet\" aria-label=\"Building map preview\"></svg>
        <div id=\"mainMapPreviewLegend\" class=\"map-preview-legend\"></div>
        <div id=\"mainMapPreviewNote\" class=\"map-preview-note\"></div>
      </div>
    </section>

    <section id=\"adminToolsSection\" class=\"chart-wrap hidden\">
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
        <button class=\"tab\" data-admin-tab=\"adopt\">Adopt Devices</button>
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
        <span><span class=\"dot\" style=\"background:#0f766e\"></span>Supply</span>
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
          <tr><th>Parent</th><th>Sub-Zone</th><th>Heat Call (24VAC)</th><th>Supply</th><th>Return</th><th>Status</th><th>Note</th><th>Info</th></tr>
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

      <div id=\"adminAdoptPanel\" class=\"hidden\" style=\"margin-top:12px\">
        <div class=\"card\" style=\"padding:12px;border-radius:12px;border:1px solid var(--border);background:#f9fcfe;box-shadow:none\">
          <h4 style=\"margin:0 0 8px\">Unassigned Devices (Tailscale)</h4>
          <div class=\"smallnote\">Scans Tailscale peers for zone nodes on <code>:8090</code>. Adopt a configured node to add it to the hub and assign a parent zone.</div>
          <div class=\"form-grid\" style=\"margin-top:10px\">
            <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
              <button type=\"button\" class=\"btn\" id=\"adoptScanBtn\">Scan for Zone Nodes</button>
              <button type=\"button\" class=\"btn\" id=\"adoptRefreshBtn\">Refresh List</button>
              <button type=\"button\" class=\"btn\" id=\"adoptSelectFirstBtn\">Select First Available</button>
            </div>
            <div id=\"adoptDetectedButtons\" class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\"></div>
          </div>
          <div id=\"adoptMsg\" class=\"msg\"></div>
          <div class=\"smallnote\">Step 1: Scan for Zone Nodes. Step 2: Click Select Device on one row. Step 3: Finish adopt fields and click Adopt Device.</div>
          <div class=\"table-responsive\" style=\"margin-top:10px\">
            <table class=\"table table-hover align-middle mb-0\">
              <thead><tr><th>Device</th><th>IP</th><th>Status</th><th>Parent</th><th>Supply / Return</th><th>Call</th><th>Actions</th></tr></thead>
              <tbody id=\"adoptDevicesBody\"></tbody>
            </table>
          </div>
          <div class=\"form-grid\" style=\"margin-top:12px;border-top:1px solid var(--border);padding-top:12px\">
            <div class=\"full\"><strong>Adopt Selected Device</strong></div>
            <div class=\"full smallnote\">Fields marked with * are required before adoption.</div>
            <div>
              <label class=\"label\" for=\"adoptBaseUrl\">Device URL *</label>
              <input id=\"adoptBaseUrl\" class=\"input\" placeholder=\"http://100.x.x.x:8090\" readonly>
            </div>
            <div>
              <label class=\"label\" for=\"adoptName\">Display Name *</label>
              <input id=\"adoptName\" class=\"input\" placeholder=\"Production Area Heating Coil 1\">
            </div>
            <div>
              <label class=\"label\" for=\"adoptZoneId\">Zone ID *</label>
              <input id=\"adoptZoneId\" class=\"input\" placeholder=\"zone1-production-area-heating-coil-1\">
            </div>
            <div>
              <label class=\"label\" for=\"adoptParentZone\">Parent Zone *</label>
              <select id=\"adoptParentZone\" class=\"select\">
                <option>Zone 1</option>
                <option>Zone 2</option>
              </select>
            </div>
            <div>
              <label class=\"label\" for=\"adoptSupplyProbe\">Supply Probe *</label>
              <select id=\"adoptSupplyProbe\" class=\"select\">
                <option value=\"\">Load device setup first</option>
              </select>
            </div>
            <div>
              <label class=\"label\" for=\"adoptReturnProbe\">Return Probe *</label>
              <select id=\"adoptReturnProbe\" class=\"select\">
                <option value=\"\">Load device setup first</option>
              </select>
            </div>
            <div>
              <label class=\"label\" for=\"adoptWifiSsid\">Wi-Fi Name *</label>
              <input id=\"adoptWifiSsid\" class=\"input\" placeholder=\"Building Wi-Fi SSID\">
            </div>
            <div>
              <label class=\"label\" for=\"adoptWifiPassword\">Wi-Fi Password *</label>
              <input id=\"adoptWifiPassword\" class=\"input\" type=\"password\" placeholder=\"Building Wi-Fi password\">
              <div class=\"smallnote\">Leave blank to keep the device's already-saved Wi-Fi password.</div>
            </div>
            <div>
              <label class=\"label\" for=\"adoptHubUrl\">Hub URL *</label>
              <input id=\"adoptHubUrl\" class=\"input\" placeholder=\"https://your-hub.example.com\">
            </div>
            <div>
              <label class=\"label\" for=\"adoptLowTempEnabled\">Low Temp Alerts</label>
              <select id=\"adoptLowTempEnabled\" class=\"select\">
                <option value=\"true\">Enabled</option>
                <option value=\"false\">Disabled</option>
              </select>
            </div>
            <div>
              <label class=\"label\" for=\"adoptLowTempThreshold\">Low Temp Threshold (F)</label>
              <input id=\"adoptLowTempThreshold\" class=\"input\" type=\"number\" step=\"0.5\" min=\"0\" value=\"35\">
            </div>
            <div>
              <label class=\"label\" for=\"adoptCallSensingEnabled\">24VAC Sensing</label>
              <select id=\"adoptCallSensingEnabled\" class=\"select\">
                <option value=\"true\">Enabled</option>
                <option value=\"false\">Disabled</option>
              </select>
            </div>
            <div>
              <label class=\"label\" for=\"adoptDeltaOk\">Supply/Return Match Threshold (F)</label>
              <input id=\"adoptDeltaOk\" class=\"input\" type=\"number\" step=\"0.5\" min=\"0\" value=\"8\">
            </div>
            <div class=\"full\" style=\"display:flex;gap:8px;flex-wrap:wrap\">
              <button type=\"button\" class=\"btn\" id=\"adoptLoadSetupBtn\">Load Device Setup</button>
              <button type=\"button\" class=\"btn active\" id=\"adoptSaveBtn\">Adopt Device</button>
              <button type=\"button\" class=\"btn\" id=\"adoptClearBtn\">Clear Selection</button>
            </div>
            <div id=\"adoptConfirm\" class=\"full hidden\" style=\"border:2px solid #16a34a;background:#dcfce7;color:#14532d;padding:12px;border-radius:10px;font-weight:700;font-size:15px;box-shadow:0 0 0 2px rgba(22,163,74,.15) inset\"></div>
          </div>
        </div>
      </div>

      <div id=\"adminZonesPanel\" class=\"hidden\" style=\"margin-top:12px\">
        <div id=\"zoneMgmtCard\" class=\"card\" style=\"padding:12px;border-radius:12px;border:1px solid var(--border);background:#f9fcfe;box-shadow:none\">
          <h4 style=\"margin:0 0 8px\">Zone Management (Adopted Devices)</h4>
          <div class=\"smallnote\">Post-install changes and diagnostics for adopted zone nodes. Use this instead of the setup page for normal service updates.</div>
          <div class=\"form-grid\" style=\"margin-top:10px\">
            <div>
              <label class=\"label\" for=\"zoneMgmtSelect\">Zone Device</label>
              <select id=\"zoneMgmtSelect\" class=\"select\"></select>
            </div>
            <div style=\"display:flex;align-items:end;gap:8px\">
              <button type=\"button\" class=\"btn\" id=\"zoneMgmtRefreshBtn\">Refresh Diagnostics</button>
              <button type=\"button\" class=\"btn\" id=\"zoneMgmtSwapBtn\">Swap Supply / Return</button>
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
              <button type=\"button\" class=\"btn\" id=\"zoneMgmtEraseBtn\" style=\"border-color:#fecaca;color:#991b1b;background:#fff5f5\">Erase Zone</button>
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
                    <label class=\"label\" for=\"zoneMgmtCallSensingEnabled\">24VAC Sensing</label>
                    <select id=\"zoneMgmtCallSensingEnabled\" class=\"select\">
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
        <div id=\"siteMapCard\" class=\"card site-map-card\" style=\"padding:12px;border-radius:12px;border:1px solid var(--border);background:#f9fcfe;box-shadow:none\">
          <h4 style=\"margin:0 0 8px\">Building / Zone Device Map</h4>
          <div class=\"smallnote\">Enter the building address, zoom in, then select a zone/device and click the map to place a rough pin location.</div>
          <div class=\"site-map-tools\" style=\"margin-top:10px\">
            <div class=\"full\" style=\"min-width:320px\">
              <label class=\"label\">Building Address (Step 1)</label>
              <div class=\"form-grid\" style=\"margin-top:4px\">
                <div class=\"full\">
                  <label class=\"label\" for=\"siteAddrStreet\">Full Building Address (Number + Street)</label>
                  <input id=\"siteAddrStreet\" class=\"input\" placeholder=\"165 Water Street\">
                </div>
                <div>
                  <label class=\"label\" for=\"siteAddrZip\">ZIP</label>
                  <input id=\"siteAddrZip\" class=\"input\" placeholder=\"06854\" inputmode=\"numeric\" maxlength=\"10\">
                </div>
                <div>
                  <label class=\"label\" for=\"siteAddrState\">State</label>
                  <input id=\"siteAddrState\" class=\"input\" placeholder=\"CT\" maxlength=\"2\" autocapitalize=\"characters\">
                </div>
              </div>
              <input id=\"siteAddress\" class=\"input\" type=\"hidden\" placeholder=\"165 Water Street, New Haven, CT\">
            </div>
            <div id=\"siteMapZoneSelectWrap\">
              <label class=\"label\" for=\"siteMapZoneSelect\">Zone / Device</label>
              <select id=\"siteMapZoneSelect\" class=\"select\"></select>
              <div class=\"smallnote\">Only adopted/assigned zone devices appear here (plus Zone 1 and Zone 2 main).</div>
            </div>
            <button type=\"button\" class=\"btn\" id=\"siteMapFindBtn\">Find Address</button>
            <button type=\"button\" class=\"btn active\" id=\"siteMapSaveBtn\">Save Map</button>
          </div>
          <div id=\"siteMapZoneMappingLockedNote\" class=\"msg\" style=\"display:none;margin-top:8px\">Complete Building Setup first (find address, confirm building pin, draw outline, and save floor count). Zone/device mapping unlocks after that and uses this shared building layout for all zones.</div>
          <div id=\"siteMapZoneMappingSection\">
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
            <button type=\"button\" class=\"btn\" id=\"siteMapFootprintBtn\">Building Setup</button>
            <button type=\"button\" class=\"btn\" id=\"siteMapZoneSetupBtn\">Zone Setup</button>
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
          </div>
          <div id=\"siteMapWorkspaceBar\" class=\"site-map-workspace-bar\">
            <div class=\"left\">
              <strong id=\"siteMapWorkspaceTitle\">Map Workspace</strong>
              <span class=\"smallnote\" id=\"siteMapWorkspaceHint\">Use the map tools, then Save & Finish.</span>
              <span class=\"status-chip\"><span id=\"siteMapWorkspaceStatus\">Selected: --</span></span>
            </div>
            <div class=\"right\">
              <button type=\"button\" class=\"btn\" id=\"siteMapWorkspaceSaveBtn\">Save & Finish</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapWorkspaceBackBtn\">Back / Escape</button>
            </div>
          </div>
          <div id=\"siteMapOutlineWizard\" class=\"site-map-wizard\">
            <div style=\"display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap\">
              <div>
                <strong>Building Outline Setup</strong>
                <div class=\"smallnote\">Find address, click the building to confirm it, draw the outline, set floors, then continue (press Enter).</div>
              </div>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineWizardCloseBtn\">Exit Building Setup</button>
            </div>
            <div id=\"siteMapOutlineWizardStatus\" class=\"msg\" style=\"margin-top:8px\"></div>
            <div id=\"siteMapOutlineProgress\" class=\"smallnote\" style=\"margin-top:6px\"></div>
            <div class=\"steps\" id=\"siteMapOutlineWizardSteps\">
              <div class=\"step\" data-step=\"1\"><div class=\"n\">Step 1</div><div class=\"t\">Find Address</div><div class=\"s\">Locate and center the building on the map.</div></div>
              <div class=\"step\" data-step=\"2\"><div class=\"n\">Step 2</div><div class=\"t\">Confirm Building Pin</div><div class=\"s\">Click the building on the map to confirm the correct structure.</div></div>
              <div class=\"step\" data-step=\"3\"><div class=\"n\">Step 3</div><div class=\"t\">Draw Building Outline</div><div class=\"s\">Auto detect or draw the building boundary on the map.</div></div>
              <div class=\"step\" data-step=\"4\"><div class=\"n\">Step 4</div><div class=\"t\">Set Floor Count</div><div class=\"s\">Enter how many floors the building has.</div></div>
              <div class=\"step\" data-step=\"5\"><div class=\"n\">Step 5</div><div class=\"t\">Assign Zone Colors</div><div class=\"s\">Apply parent-zone colors to downstream zones.</div></div>
              <div class=\"step\" data-step=\"6\"><div class=\"n\">Step 6</div><div class=\"t\">Main Boiler Location</div><div class=\"s\">Mark where the main boiler/circulator is on the map.</div></div>
              <div class=\"step\" data-step=\"7\"><div class=\"n\">Step 7</div><div class=\"t\">Save & Finish</div><div class=\"s\">Save the building setup to the hub.</div></div>
            </div>
            <div class=\"actions\">
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineStepFindBtn\">Find Address</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineStepAutoBtn\">Auto Detect (Optional)</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineStartDrawBtn\">Start Manual Outline</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineEditSavedBtn\">Edit Existing Outline</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineDragBtn\">Select & Drag Outline</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineUndoBtn\">Undo Last Point</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineDeletePointBtn\" style=\"display:none\">Delete Selected Point</button>
              <button type=\"button\" class=\"btn active\" id=\"siteMapOutlineFinishDrawBtn\">Finish Manual Outline</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlineClearBtn\">Clear All Points</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapOutlinePrevStepBtn\">Previous Step</button>
            </div>
            <div class=\"form-grid\" style=\"margin-top:10px\">
              <div>
                <label class=\"label\" for=\"siteMapBuildingFloors\">Building Floors</label>
                <input id=\"siteMapBuildingFloors\" class=\"input\" type=\"number\" min=\"1\" step=\"1\" value=\"1\">
              </div>
              <div style=\"display:flex;align-items:end;gap:8px\">
                <button type=\"button\" class=\"btn\" id=\"siteMapOutlineFloorsSaveBtn\">Save Floor Count</button>
                <button type=\"button\" class=\"btn\" id=\"siteMapOutlineNextStepBtn\">Continue (Enter)</button>
                <button type=\"button\" class=\"btn active\" id=\"siteMapOutlineContinueZoneSetupBtn\" style=\"display:none\">Continue to Zone Setup</button>
              </div>
              <div class=\"full\">
                <div class=\"smallnote\">Zone colors are automatically inherited from the parent zone (Zone 1 / Zone 2) for consistent mapping.</div>
                <div style=\"display:flex;gap:8px;flex-wrap:wrap;margin-top:6px\">
                  <button type=\"button\" class=\"btn\" id=\"siteMapApplyParentColorsBtn\">Apply Parent Colors to Zones</button>
                  <button type=\"button\" class=\"btn\" id=\"siteMapSelectZone1MainBtn\">Select Zone 1 Main Boiler</button>
                  <button type=\"button\" class=\"btn\" id=\"siteMapSelectZone2MainBtn\">Select Zone 2 Main Boiler</button>
                </div>
              </div>
              <div class=\"full\" style=\"display:flex;justify-content:flex-end\">
                <button type=\"button\" class=\"btn\" id=\"siteMapResetBuildingBtn\" style=\"font-size:.82rem;padding:6px 10px\">Reset Building</button>
              </div>
            </div>
          </div>
          <div id=\"siteMapZoneWizard\" class=\"site-map-zone-wizard\">
            <div style=\"display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap\">
              <div>
                <strong>Zone Setup</strong>
                <div class=\"smallnote\">Use the saved building outline as the base. Place all zone pins and areas inside the building outline.</div>
              </div>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardExitBtn\">Exit Zone Setup</button>
            </div>
            <div class=\"mode-tabs\" style=\"margin-top:8px\">
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardMainTabBtn\">Main Zones</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardDownstreamTabBtn\">Downstream Zones</button>
            </div>
            <div id=\"siteMapZoneWizardStatus\" class=\"msg\" style=\"margin-top:8px\"></div>
            <div id=\"siteMapZoneWizardMainActions\" class=\"actions\">
              <button type=\"button\" class=\"btn active\" id=\"siteMapZoneWizardSaveProgressBtn\">Save Progress</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardPinZone1Btn\">Pin Zone 1 Main Boiler</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardPinZone2Btn\">Pin Zone 2 Main Boiler</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardClearOutsidePinsBtn\">Clear Pins Outside Building</button>
            </div>
            <div id=\"siteMapZoneWizardDownstreamActions\" class=\"actions hidden\">
              <button type=\"button\" class=\"btn active\" id=\"siteMapZoneWizardSaveProgressBtnDs\">Save Progress</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardLoadSelectedBtn\">Use Selected Device</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardRefreshZoneBtn\">Refresh Zone Selection</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardStartAreaBtn\">Start Zone Area (Optional)</button>
              <button type=\"button\" class=\"btn\" id=\"siteMapZoneWizardClearOutsidePinsBtnDs\">Clear Pins Outside Building</button>
            </div>
          </div>
          <div id=\"siteMapCanvas\" tabindex=\"0\" style=\"margin-top:10px\"></div>
          <div id=\"siteMapDrawHint\" class=\"smallnote\" style=\"display:none;margin-top:8px;padding:8px 10px;border:1px solid var(--border);border-radius:10px;background:#f8fbff\"></div>
          <div style=\"display:flex;gap:8px;flex-wrap:wrap;margin-top:8px\">
            <button type=\"button\" class=\"btn\" id=\"siteMapAreaUndoBtn\">Undo Last Area Point</button>
          </div>
          <div class=\"smallnote\" style=\"margin-top:8px\">Tip: Complete Building Setup first. In Zone Setup, choose a zone/device and place pins or zone areas inside the saved building outline.</div>
          <div id=\"siteMapPinsList\" class=\"pin-list\"></div>
          <div id=\"siteMapMsg\" class=\"msg\"></div>
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
                <label><input type=\"checkbox\" data-commission-check=\"feed_return_confirmed\"> Supply / Return Confirmed</label>
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
          <div class=\"smallnote\">Use this when installing another Raspberry Pi zone node (supply/return probes + dry-contact call sensing).</div>
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
              <li>Assign unit name, zone ID, supply/return probes, and Wi-Fi.</li>
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
  <div id=\"siteMapFloorModal\" class=\"site-floor-modal\" aria-hidden=\"true\">
    <div class=\"site-floor-modal-card\">
      <div style=\"display:flex;justify-content:space-between;align-items:center;gap:10px\">
        <div>
          <h4 style=\"margin:0\">Confirm Number of Floors</h4>
          <div class=\"smallnote\">Set the total floors before confirming the building outline. Add a short label for each floor (example: Basement, Floor 1).</div>
        </div>
        <button type=\"button\" class=\"btn\" id=\"siteMapFloorModalCloseBtn\">Close</button>
      </div>
      <div class=\"form-grid\" style=\"margin-top:10px\">
        <div>
          <label class=\"label\" for=\"siteMapFloorModalCount\">Number of Floors</label>
          <input id=\"siteMapFloorModalCount\" class=\"input\" type=\"number\" min=\"1\" step=\"1\" value=\"1\">
        </div>
        <div style=\"display:flex;align-items:end;gap:8px;flex-wrap:wrap\">
          <button type=\"button\" class=\"btn\" id=\"siteMapFloorModalBuildRowsBtn\">Update Floor Fields</button>
          <button type=\"button\" class=\"btn active\" id=\"siteMapFloorModalContinueBtn\">Continue to Outline Confirmation</button>
        </div>
        <div class=\"full\">
          <div id=\"siteMapFloorModalRows\" class=\"form-grid\"></div>
        </div>
      </div>
      <div id=\"siteMapFloorModalMsg\" class=\"msg\" style=\"margin-top:8px\"></div>
    </div>
  </div>
  <div id=\"siteMapResetBuildingModal\" class=\"site-confirm-modal\" aria-hidden=\"true\">
    <div class=\"site-confirm-modal-card\">
      <div style=\"display:flex;justify-content:space-between;align-items:center;gap:10px\">
        <div>
          <h4 style=\"margin:0\">Reset Building Setup</h4>
          <div class=\"smallnote\">This clears the saved building setup and zone map pins/areas for this site map.</div>
        </div>
        <button type=\"button\" class=\"btn\" id=\"siteMapResetBuildingCloseBtn\">Close</button>
      </div>
      <div style=\"margin-top:10px\">
        <label style=\"display:flex;gap:8px;align-items:flex-start\">
          <input type=\"checkbox\" id=\"siteMapResetBuildingSaveFirst\" checked>
          <span>Download current building configuration before clearing</span>
        </label>
      </div>
      <div style=\"display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end;margin-top:12px\">
        <button type=\"button\" class=\"btn\" id=\"siteMapResetBuildingCancelBtn\">Cancel</button>
        <button type=\"button\" class=\"btn active\" id=\"siteMapResetBuildingConfirmBtn\">Clear Building Setup</button>
      </div>
      <div id=\"siteMapResetBuildingMsg\" class=\"msg\" style=\"margin-top:8px\"></div>
    </div>
  </div>
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
          <div id=\"zoneInfoMainBoilerFloorRow\" class=\"row hidden\" style=\"padding:6px 0\"><div><div class=\"label\">Main Boiler Floor</div></div><div id=\"zoneInfoMainBoilerFloor\">--</div></div>
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
    let pendingGraphTabId = "";
    let pendingCycleZoneId = "";
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
    let siteMapBuildingConfirmMarker = null;
    let siteMapBaseLayers = {};
    let siteMapActiveLayerKey = "street";
    let siteMapBuildingOutlineLayer = null;
    let siteMapDraftAreaPoints = [];
    let siteMapDraftAreaLayer = null;
    let siteMapDrawingArea = false;
    let siteMapOutlineWizardOpen = false;
    let siteMapOutlineWizardStep = 1;
    let siteMapDrawingBuildingOutline = false;
    let siteMapDraftBuildingOutlinePoints = [];
    let siteMapDraftBuildingOutlineLayer = null;
    let siteMapDraftBuildingOutlinePointsLayer = null;
    let siteMapOutlineDragMode = false;
    let siteMapOutlineDragActive = false;
    let siteMapOutlineDragStartLatLng = null;
    let siteMapOutlineDragBasePoints = [];
    let siteMapBuildingEditOnlyMode = false;
    let siteMapEditingSavedOutlinePoints = false;
    let siteMapSelectedOutlinePointIndex = -1;
    let siteMapOutlinePointDragActive = false;
    let siteMapOutlinePointDragMoved = false;
    let siteMapSuppressNextOutlineClick = false;
    let siteMapMainBoilerFloorPopupZoneId = "";
    let siteMapWorkspaceOpen = false;
    let siteMapSetupMode = "none";
    let siteMapReady = false;
    let mainMapPreviewLastRenderKey = "";
    let siteMapAutoOpenZoneSetupAfterBuildingSave = false;
    let siteMapPendingOutlineConfirmSourceLabel = "";
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
    function parentZoneColorName(parentZone) {
      const p = String(parentZone || "").toLowerCase();
      if (p === "zone 1") return "red";
      if (p === "zone 2") return "blue";
      if (p === "zone 3") return "green";
      return "purple";
    }
    function preferredPinColorForZone(zoneId) {
      if (zoneId === "zone1_main") return "red";
      if (zoneId === "zone2_main") return "blue";
      const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
      const row = ds.find((z) => String(z.id || "") === String(zoneId || ""));
      if (row) return parentZoneColorName(row.parent_zone);
      return "red";
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

    function ensureAdminToolsVisible() {
      const sec = document.getElementById("adminToolsSection");
      if (sec) sec.classList.remove("hidden");
    }
    function ensureAdminToolsOpen() {
      ensureAdminToolsVisible();
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

    function setSiteMapWorkspaceMeta(title, hint) {
      const t = document.getElementById("siteMapWorkspaceTitle");
      const h = document.getElementById("siteMapWorkspaceHint");
      if (t) t.textContent = title || "Map Workspace";
      if (h) h.textContent = hint || "Use the map tools, then Save & Finish.";
      updateSiteMapWorkspaceStatus();
    }

    function setOutlineWizardMsg(msg) {
      const el = document.getElementById("siteMapOutlineWizardStatus");
      if (el) el.textContent = msg || "";
    }

    function renderOutlineWizardProgress(state) {
      const el = document.getElementById("siteMapOutlineProgress");
      if (!el) return;
      const s = state || {};
      const hasAddress = !!s.hasAddress;
      const hasConfirm = !!s.hasConfirm;
      const hasOutline = !!s.hasOutline;
      const drawing = !!s.drawing;
      const floorsSet = Number((siteMapConfig && siteMapConfig.building_floors) || 0) >= 1;
      const colorsAssigned = areParentColorsAppliedToZones();
      const boilerPinned = !!(getSiteMapPin("zone1_main") || getSiteMapPin("zone2_main"));
      const readyToSave = hasAddress && hasConfirm && hasOutline && !drawing && floorsSet && colorsAssigned && boilerPinned;
      const item = (ok, label) => `${ok ? "✓" : "○"} ${label}`;
      el.textContent = [
        item(hasAddress, "Address Found"),
        item(hasConfirm, "Building Confirmed"),
        item(hasOutline, "Building Outline Ready"),
        item(floorsSet, "Floor Count Set"),
        item(colorsAssigned, "Zone Colors Assigned"),
        item(boilerPinned, "Main Boiler Pinned"),
        item(readyToSave, "Ready to Save"),
      ].join("   •   ");
    }

    function renderOutlineWizardSteps() {
      const stepsWrap = document.getElementById("siteMapOutlineWizardSteps");
      const wiz = document.getElementById("siteMapOutlineWizard");
      const card = document.getElementById("siteMapCard");
      if (!stepsWrap) return;
      const hasAddress = !!String(document.getElementById("siteAddress")?.value || "").trim();
      const hasConfirm = !!(siteMapConfig && siteMapConfig.building_confirm_pin);
      const hasOutline = !!(siteMapConfig && siteMapConfig.building_outline);
      const drawing = !!siteMapDrawingBuildingOutline;
      const floorsSet = Number((siteMapConfig && siteMapConfig.building_floors) || 0) >= 1;
      const colorsAssigned = areParentColorsAppliedToZones();
      const boilerPinned = !!(getSiteMapPin("zone1_main") || getSiteMapPin("zone2_main"));
      const doneByStep = {
        1: hasAddress,
        2: hasConfirm,
        3: hasOutline && !drawing,
        4: hasOutline && floorsSet,
        5: colorsAssigned,
        6: boilerPinned,
        7: hasAddress && hasConfirm && hasOutline && !drawing && floorsSet && colorsAssigned && boilerPinned,
      };
      let nextRequired = 7;
      for (let i = 1; i <= 7; i++) {
        if (!doneByStep[i]) { nextRequired = i; break; }
      }
      stepsWrap.querySelectorAll(".step").forEach((el) => {
        const n = Number(el.dataset.step || 0);
        el.classList.toggle("active", n === siteMapOutlineWizardStep);
        el.classList.toggle("done", !!doneByStep[n]);
        el.classList.toggle("next-required", n === nextRequired && !doneByStep[n]);
      });
      if (wiz) wiz.classList.toggle("compact-early", !!siteMapOutlineWizardOpen && Number(siteMapOutlineWizardStep || 1) <= 3);
      if (card) card.classList.toggle("step-map-focus", !!siteMapWorkspaceOpen && !!siteMapOutlineWizardOpen && [2,3].includes(Number(siteMapOutlineWizardStep || 1)));
      renderOutlineWizardProgress({ hasAddress, hasConfirm, hasOutline, drawing });
      updateOutlineWizardActionStates({ hasAddress, hasConfirm, hasOutline, drawing });
    }

    function updateOutlineWizardActionStates(state) {
      const s = state || {};
      const hasAddress = !!s.hasAddress;
      const hasConfirm = !!s.hasConfirm;
      const hasOutline = !!s.hasOutline;
      const drawing = !!s.drawing;
      const editingSavedOutline = !!(drawing && siteMapEditingSavedOutlinePoints);
      const editOnlyOutlineMode = !!(siteMapBuildingEditOnlyMode && editingSavedOutline);
      const btnFind = document.getElementById("siteMapOutlineStepFindBtn");
      const btnAuto = document.getElementById("siteMapOutlineStepAutoBtn");
      const btnStart = document.getElementById("siteMapOutlineStartDrawBtn");
      const btnEditSaved = document.getElementById("siteMapOutlineEditSavedBtn");
      const btnDragOutline = document.getElementById("siteMapOutlineDragBtn");
      const btnUndoOutline = document.getElementById("siteMapOutlineUndoBtn");
      const btnDeletePoint = document.getElementById("siteMapOutlineDeletePointBtn");
      const btnFinish = document.getElementById("siteMapOutlineFinishDrawBtn");
      const btnClear = document.getElementById("siteMapOutlineClearBtn");
      const btnUndoArea = document.getElementById("siteMapAreaUndoBtn");
      const btnContinueZoneSetup = document.getElementById("siteMapOutlineContinueZoneSetupBtn");
      const floorsSet = Number((siteMapConfig && siteMapConfig.building_floors) || 0) >= 1;
      if (btnFind) {
        btnFind.disabled = editOnlyOutlineMode;
        btnFind.classList.toggle("active", !hasAddress && !editOnlyOutlineMode);
      }
      if (btnAuto) {
        btnAuto.disabled = editOnlyOutlineMode || !hasAddress || !hasConfirm || drawing;
        btnAuto.classList.toggle("active", false);
      }
      if (btnStart) {
        btnStart.disabled = !hasAddress || !hasConfirm || drawing;
        btnStart.classList.toggle("active", hasAddress && hasConfirm && !drawing && !hasOutline);
      }
      if (btnEditSaved) {
        btnEditSaved.disabled = !hasOutline || drawing;
        btnEditSaved.classList.toggle("active", editingSavedOutline);
      }
      if (btnDragOutline) {
        btnDragOutline.disabled = !hasOutline || drawing;
        btnDragOutline.classList.toggle("active", !!siteMapOutlineDragMode);
        btnDragOutline.textContent = siteMapOutlineDragMode ? "Finish Drag Outline (Enter)" : "Select & Drag Outline";
      }
      if (btnFinish) {
        btnFinish.disabled = !drawing;
        btnFinish.classList.toggle("active", !!drawing && !editingSavedOutline);
      }
      if (btnUndoOutline) {
        const canReopenSaved = !!(siteMapOutlineWizardOpen && Number(siteMapOutlineWizardStep || 1) === 3 && siteMapConfig && siteMapConfig.building_outline);
        btnUndoOutline.disabled = (!drawing && !canReopenSaved) || (drawing && siteMapDraftBuildingOutlinePoints.length === 0);
      }
      if (btnDeletePoint) {
        const canDelete = !!drawing && !!siteMapEditingSavedOutlinePoints && Number.isInteger(siteMapSelectedOutlinePointIndex) && siteMapSelectedOutlinePointIndex >= 0;
        btnDeletePoint.style.display = (drawing && siteMapEditingSavedOutlinePoints) ? "" : "none";
        btnDeletePoint.disabled = !canDelete;
      }
      if (btnClear) {
        btnClear.disabled = !hasOutline && !drawing;
      }
      if (btnUndoArea) btnUndoArea.disabled = !siteMapDrawingArea || siteMapDraftAreaPoints.length === 0;
      const btnPrev = document.getElementById("siteMapOutlinePrevStepBtn");
      if (btnPrev) {
        btnPrev.textContent = editOnlyOutlineMode ? "Return to Building Lookup" : "Previous Step";
      }
      if (btnContinueZoneSetup) {
        const buildingBaseComplete = hasAddress && !!(siteMapConfig && siteMapConfig.building_confirm_pin) && hasOutline && !drawing && floorsSet;
        btnContinueZoneSetup.style.display = buildingBaseComplete ? "" : "none";
      }
      if (buildingSetupComplete() && Number(siteMapOutlineWizardStep || 1) >= 5 && !drawing) {
        if (btnFind) btnFind.classList.remove("active");
        if (btnStart) btnStart.classList.remove("active");
      }
    }

    function continueToZoneSetupWizard() {
      if (!siteMapConfig || !siteMapConfig.building_outline) {
        setSiteMapMsg("Complete the building outline first.");
        setOutlineWizardStep(3, "Complete and confirm the building outline first.");
        return;
      }
      const floorsSet = Math.max(0, Number((siteMapConfig && siteMapConfig.building_floors) || 0)) >= 1;
      if (!floorsSet) {
        setSiteMapMsg("Save the building floor count first.");
        setOutlineWizardStep(4, "Next: set and save the building floor count.");
        return;
      }
      setOutlineWizardStep(5, "Building setup complete. Continue with zone setup (zone colors and main boiler locations).");
      siteMapBuildingEditOnlyMode = false;
      setSiteMapMsg("Building setup complete. Opening Zone Setup.");
      updateSiteMapWorkspaceStatus("Building setup complete");
      if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
      setTimeout(() => openZoneSetupWorkspace(), 50);
    }

    function centerMapOnAddressOrDefault(preferredZoom = 18) {
      if (!siteMapMap) return;
      const c = siteMapConfig && siteMapConfig.center;
      const lat = c && Number(c.lat);
      const lng = c && Number(c.lng);
      if (Number.isFinite(lat) && Number.isFinite(lng)) {
        siteMapMap.setView([lat, lng], Math.max(16, Number(siteMapConfig?.zoom || preferredZoom || 18)));
      }
    }

    function centerMapOnBuildingOutline() {
      if (!siteMapMap) return false;
      if (siteMapBuildingOutlineLayer) {
        try {
          const b = siteMapBuildingOutlineLayer.getBounds?.();
          if (b && b.isValid && b.isValid()) {
            siteMapMap.fitBounds(b.pad(0.12));
            return true;
          }
        } catch (e) {}
      }
      if (siteMapDraftBuildingOutlineLayer) {
        try {
          const b = siteMapDraftBuildingOutlineLayer.getBounds?.();
          if (b && b.isValid && b.isValid()) {
            siteMapMap.fitBounds(b.pad(0.12));
            return true;
          }
        } catch (e) {}
      }
      return false;
    }

    function centerMapOnZonePin(zoneId) {
      if (!siteMapMap || !zoneId) return false;
      const p = getSiteMapPin(zoneId);
      const lat = p && Number(p.lat);
      const lng = p && Number(p.lng);
      if (Number.isFinite(lat) && Number.isFinite(lng)) {
        siteMapMap.setView([lat, lng], Math.max(18, Number(siteMapConfig?.zoom || 18)));
        return true;
      }
      return false;
    }

    function fitMapToLatLngPoints(points, pad = 0.12) {
      if (!siteMapMap || !window.L || !Array.isArray(points) || !points.length) return false;
      try {
        const ll = points
          .map((p) => [Number(p.lat), Number(p.lng)])
          .filter((pt) => Number.isFinite(pt[0]) && Number.isFinite(pt[1]));
        if (!ll.length) return false;
        if (ll.length === 1) {
          siteMapMap.setView(ll[0], Math.max(19, Number(siteMapMap.getZoom?.() || 19)));
          return true;
        }
        const bounds = L.latLngBounds(ll);
        if (bounds && bounds.isValid && bounds.isValid()) {
          siteMapMap.fitBounds(bounds.pad(pad));
          return true;
        }
      } catch (e) {}
      return false;
    }

    function centerMapOnSelectedZoneArea() {
      const zid = selectedSiteMapZoneId();
      const p = getSiteMapPin(zid);
      const poly = (p && Array.isArray(p.polygon)) ? p.polygon : [];
      return fitMapToLatLngPoints(poly, 0.16);
    }

    function buildSiteAddressQueryFromFields() {
      const street = String(document.getElementById("siteAddrStreet")?.value || "").trim();
      const zip = String(document.getElementById("siteAddrZip")?.value || "").trim();
      const state = String(document.getElementById("siteAddrState")?.value || "").trim().toUpperCase();
      const tail = [state, zip].filter(Boolean).join(" ");
      const q = [street, tail].filter(Boolean).join(", ");
      const hidden = document.getElementById("siteAddress");
      if (hidden) hidden.value = q;
      const stateEl = document.getElementById("siteAddrState");
      if (stateEl) stateEl.value = state;
      return q;
    }

    function initSiteMapAddressStepFields() {
      const street = document.getElementById("siteAddrStreet");
      const zip = document.getElementById("siteAddrZip");
      const state = document.getElementById("siteAddrState");
      const findBtn = document.getElementById("siteMapFindBtn");
      if (street) street.addEventListener("keydown", (e) => {
        if (e.key !== "Enter") return;
        e.preventDefault();
        focusNoScroll(zip || state || findBtn);
      });
      if (zip) zip.addEventListener("keydown", (e) => {
        if (e.key !== "Enter") return;
        e.preventDefault();
        focusNoScroll(state || findBtn);
      });
      if (state) {
        state.addEventListener("input", () => {
          state.value = String(state.value || "").toUpperCase().slice(0, 2);
        });
        state.addEventListener("keydown", (e) => {
          if (e.key !== "Enter") return;
          e.preventDefault();
          if (findBtn) findBtn.click();
        });
      }
    }

    function focusNoScroll(el, opts = {}) {
      if (!el || typeof el.focus !== "function") return;
      try {
        el.focus({ preventScroll: true });
      } catch (_e) {
        try { el.focus(); } catch (__e) {}
      }
      if (opts.select && typeof el.select === "function") {
        try { el.select(); } catch (_e) {}
      }
    }

    function scrollSiteMapWorkspaceToCanvas(opts = {}) {
      const canvas = document.getElementById("siteMapCanvas");
      const card = document.getElementById("siteMapCard");
      if (!canvas) return;
      const behavior = opts.behavior || "smooth";
      const pad = Number.isFinite(Number(opts.pad)) ? Number(opts.pad) : 8;
      if (card && card.classList.contains("workspace")) {
        try {
          const top = Math.max(0, Number(canvas.offsetTop || 0) - pad);
          card.scrollTo({ top, behavior });
        } catch (_e) {
          try { card.scrollTop = Math.max(0, Number(canvas.offsetTop || 0) - pad); } catch (__e) {}
        }
      } else {
        try { canvas.scrollIntoView({ behavior, block: "start" }); } catch (_e) {}
      }
    }

    function applyOutlineWizardStepAssist(step, opts = {}) {
      if (opts && opts.skipAssist) return;
      const s = Number(step || 1);
      setTimeout(() => {
        if (!siteMapOutlineWizardOpen || Number(siteMapOutlineWizardStep || 1) !== s) return;
        if (s >= 2 && s <= 5) {
          if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
        }
        if (s === 1) {
          centerMapOnAddressOrDefault(17);
          focusNoScroll(document.getElementById("siteAddrStreet") || document.getElementById("siteAddress"));
          return;
        }
        if (s === 2) {
          focusNoScroll(document.getElementById("siteMapCanvas"));
          return;
        }
        if (s === 3) {
          if (!siteMapDrawingBuildingOutline) focusNoScroll(document.getElementById("siteMapOutlineStartDrawBtn"));
          else focusNoScroll(document.getElementById("siteMapOutlineFinishDrawBtn"));
          return;
        }
        if (s === 4) {
          focusNoScroll(document.getElementById("siteMapBuildingFloors"), { select: true });
          return;
        }
        if (s === 5) {
          focusNoScroll(document.getElementById("siteMapApplyParentColorsBtn"));
          return;
        }
        if (s === 6) {
          const zid = selectedSiteMapZoneId();
          if (zid !== "zone1_main" && zid !== "zone2_main") {
            selectMainBoilerZone("zone1_main", { preserveStep: true });
          } else {
            if (!centerMapOnZonePin(zid)) {
              if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
            }
            const btnId = zid === "zone2_main" ? "siteMapSelectZone2MainBtn" : "siteMapSelectZone1MainBtn";
            focusNoScroll(document.getElementById(btnId));
          }
          return;
        }
        if (s === 7) {
          if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
          focusNoScroll(document.getElementById("siteMapWorkspaceSaveBtn"));
        }
      }, 20);
    }

    function setOutlineWizardStep(step, msg, opts = {}) {
      siteMapOutlineWizardStep = Math.max(1, Math.min(7, Number(step || 1)));
      renderOutlineWizardSteps();
      updateSiteMapZoneMappingAvailability();
      if (msg !== undefined) setOutlineWizardMsg(msg);
      updateSiteMapWorkspaceStatus();
      applyOutlineWizardStepAssist(siteMapOutlineWizardStep, opts);
    }

    function showOutlineWizard(open=true) {
      siteMapOutlineWizardOpen = !!open;
      const el = document.getElementById("siteMapOutlineWizard");
      if (el) el.classList.toggle("show", siteMapOutlineWizardOpen);
      if (siteMapOutlineWizardOpen) renderOutlineWizardSteps();
      if (siteMapOutlineWizardOpen && buildingSetupComplete() && Number(siteMapOutlineWizardStep || 1) >= 5) {
        setOutlineWizardMsg(buildingSetupSharedCompleteMsg());
      }
      updateSiteMapZoneMappingAvailability();
    }

    function areParentColorsAppliedToZones() {
      const pins = (siteMapConfig && siteMapConfig.pins && typeof siteMapConfig.pins === "object") ? siteMapConfig.pins : {};
      const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
      const adopted = realDownstreamRows(ds);
      if (!adopted.length) return true;
      for (const z of adopted) {
        const p = pins[z.id];
        if (!p) continue;
        if (String(p.color || "") !== String(preferredPinColorForZone(z.id))) return false;
      }
      return true;
    }

    function saveBuildingFloorCount() {
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      const n = Math.max(1, Number(document.getElementById("siteMapBuildingFloors")?.value || 1));
      siteMapConfig.building_floors = Math.round(n);
      if (!Array.isArray(siteMapConfig.building_floor_labels)) siteMapConfig.building_floor_labels = [];
      siteMapConfig.building_floor_labels = siteMapConfig.building_floor_labels.slice(0, Math.round(n));
      if (document.getElementById("siteMapBuildingFloors")) document.getElementById("siteMapBuildingFloors").value = String(Math.round(n));
      setOutlineWizardStep(4, `Building floor count saved: ${Math.round(n)} floor${Math.round(n) === 1 ? "" : "s"}. Building setup complete. Continue to Zone Setup.`);
      updateSiteMapZoneMappingAvailability();
      setSiteMapMsg(`Building floor count set to ${Math.round(n)}. Building setup complete. Click Continue to Zone Setup.`);
      updateSiteMapWorkspaceStatus("Building setup complete");
      if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
    }

    function closeSiteMapFloorModal() {
      const modal = document.getElementById("siteMapFloorModal");
      if (!modal) return;
      modal.classList.remove("show");
      modal.setAttribute("aria-hidden", "true");
      const msg = document.getElementById("siteMapFloorModalMsg");
      if (msg) msg.textContent = "";
    }

    function openResetBuildingModal() {
      const modal = document.getElementById("siteMapResetBuildingModal");
      if (!modal) return;
      const msg = document.getElementById("siteMapResetBuildingMsg");
      if (msg) msg.textContent = "";
      modal.classList.add("show");
      modal.setAttribute("aria-hidden", "false");
    }

    function closeResetBuildingModal() {
      const modal = document.getElementById("siteMapResetBuildingModal");
      if (!modal) return;
      modal.classList.remove("show");
      modal.setAttribute("aria-hidden", "true");
      const msg = document.getElementById("siteMapResetBuildingMsg");
      if (msg) msg.textContent = "";
    }

    function downloadBuildingSetupSnapshot() {
      const cfg = (siteMapConfig && typeof siteMapConfig === "object") ? siteMapConfig : {};
      const out = {
        format: "hvac-building-map-v1",
        exported_utc: new Date().toISOString(),
        address: String(cfg.address || ""),
        center: cfg.center || null,
        zoom: Number(cfg.zoom || 16),
        building_floors: Math.max(1, Number(cfg.building_floors || 1)),
        building_floor_labels: Array.isArray(cfg.building_floor_labels) ? cfg.building_floor_labels : [],
        map_layer: String(cfg.map_layer || "street"),
        building_confirm_pin: cfg.building_confirm_pin || null,
        building_outline: cfg.building_outline || null,
        pins: (cfg.pins && typeof cfg.pins === "object") ? cfg.pins : {},
      };
      const blob = new Blob([JSON.stringify(out, null, 2) + "\\n"], { type: "application/json" });
      const a = document.createElement("a");
      const ts = new Date().toISOString().replace(/[:]/g, "-");
      a.href = URL.createObjectURL(blob);
      a.download = `building-setup-${ts}.json`;
      document.body.appendChild(a);
      a.click();
      setTimeout(() => {
        try { URL.revokeObjectURL(a.href); } catch (_e) {}
        try { a.remove(); } catch (_e) {}
      }, 0);
    }

    async function resetBuildingSetupAndMap() {
      const msg = document.getElementById("siteMapResetBuildingMsg");
      if (msg) msg.textContent = "Clearing building setup...";
      const saveFirst = !!document.getElementById("siteMapResetBuildingSaveFirst")?.checked;
      try {
        if (saveFirst) downloadBuildingSetupSnapshot();
        const defaultCenter = (siteMapConfig && siteMapConfig.center && Number.isFinite(Number(siteMapConfig.center.lat)) && Number.isFinite(Number(siteMapConfig.center.lng)))
          ? { lat: Number(siteMapConfig.center.lat), lng: Number(siteMapConfig.center.lng) }
          : { lat: 41.307, lng: -72.927 };
        siteMapConfig = {
          address: "",
          center: defaultCenter,
          zoom: 16,
          building_floors: 1,
          building_floor_labels: [],
          map_layer: "street",
          building_confirm_pin: null,
          building_outline: null,
          pins: {},
          updated_utc: new Date().toISOString(),
        };
        siteMapDrawingArea = false;
        siteMapDraftAreaPoints = [];
        renderSiteMapDraftArea();
        siteMapDrawingBuildingOutline = false;
        siteMapDraftBuildingOutlinePoints = [];
        siteMapEditingSavedOutlinePoints = false;
        siteMapSelectedOutlinePointIndex = -1;
        siteMapOutlineDragMode = false;
        renderSiteMapDraftBuildingOutline();
        renderSiteMapMarkers();
        if (siteMapMap) {
          setSiteMapBaseLayer("street");
          try { siteMapMap.setView([defaultCenter.lat, defaultCenter.lng], 16); } catch (_e) {}
        }
        applySiteMapStateToForm();
        updateSiteMapZoneMappingAvailability();
        siteMapAutoOpenZoneSetupAfterBuildingSave = false;
        siteMapBuildingEditOnlyMode = false;
        closeResetBuildingModal();
        await saveSiteMapConfig();
        openBuildingOutlineWizard();
        setOutlineWizardStep(1, "Building setup reset. Step 1: enter the building address and click Find Address.");
        setSiteMapMsg("Building setup reset. Start again with the building address.");
      } catch (e) {
        if (msg) msg.textContent = "Reset failed: " + (e && e.message ? e.message : "unknown error");
      }
    }

    function renderSiteMapFloorModalRows() {
      const wrap = document.getElementById("siteMapFloorModalRows");
      const countEl = document.getElementById("siteMapFloorModalCount");
      if (!wrap || !countEl) return;
      const count = Math.max(1, Math.min(24, Math.round(Number(countEl.value || 1))));
      countEl.value = String(count);
      const existing = (siteMapConfig && Array.isArray(siteMapConfig.building_floor_labels)) ? siteMapConfig.building_floor_labels : [];
      const currentInputs = Array.from(wrap.querySelectorAll("input[data-floor-label-idx]"));
      const typed = currentInputs.map((el) => String(el.value || ""));
      wrap.innerHTML = "";
      for (let i = 0; i < count; i += 1) {
        const row = document.createElement("div");
        row.className = "full";
        const label = document.createElement("label");
        label.className = "label";
        label.setAttribute("for", `siteMapFloorLabel_${i + 1}`);
        label.textContent = `Floor ${i + 1} Label / Description`;
        const input = document.createElement("input");
        input.id = `siteMapFloorLabel_${i + 1}`;
        input.className = "input";
        input.type = "text";
        input.setAttribute("data-floor-label-idx", String(i));
        let v = "";
        if (typed[i] !== undefined) v = typed[i];
        else if (existing[i] !== undefined) v = String(existing[i] || "");
        else if (i === 0) v = "Floor 1";
        else v = `Floor ${i + 1}`;
        input.value = v;
        input.placeholder = i === 0 ? "Basement / Floor 1 / Main Level" : `Floor ${i + 1} / Mezzanine / Roof level`;
        row.appendChild(label);
        row.appendChild(input);
        wrap.appendChild(row);
      }
    }

    function saveFloorModalToSiteMapConfig() {
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      const countEl = document.getElementById("siteMapFloorModalCount");
      const count = Math.max(1, Math.min(24, Math.round(Number(countEl?.value || 1))));
      siteMapConfig.building_floors = count;
      const labels = [];
      Array.from(document.querySelectorAll("#siteMapFloorModalRows input[data-floor-label-idx]")).forEach((el, idx) => {
        if (idx < count) labels.push(String(el.value || "").trim());
      });
      while (labels.length < count) labels.push(`Floor ${labels.length + 1}`);
      siteMapConfig.building_floor_labels = labels.slice(0, count);
      const floorsEl = document.getElementById("siteMapBuildingFloors");
      if (floorsEl) floorsEl.value = String(count);
      return count;
    }

    function openSiteMapFloorModalBeforeOutlineConfirm(sourceLabel = "outline") {
      siteMapPendingOutlineConfirmSourceLabel = sourceLabel;
      const modal = document.getElementById("siteMapFloorModal");
      if (!modal) return false;
      const countEl = document.getElementById("siteMapFloorModalCount");
      const currentFloors = Math.max(1, Number((siteMapConfig && siteMapConfig.building_floors) || 1));
      if (countEl) countEl.value = String(Math.round(currentFloors));
      renderSiteMapFloorModalRows();
      const msg = document.getElementById("siteMapFloorModalMsg");
      if (msg) msg.textContent = "Confirm total floors and optional labels before confirming the building outline.";
      modal.classList.add("show");
      modal.setAttribute("aria-hidden", "false");
      setOutlineWizardStep(4, "Confirm number of floors and labels in the popup, then continue outline confirmation.", { skipAssist: true });
      setTimeout(() => focusNoScroll(document.getElementById("siteMapFloorModalCount"), { select: true }), 30);
      return true;
    }

    function buildingSetupComplete() {
      const hasAddress = !!String((siteMapConfig && siteMapConfig.address) || "").trim();
      const hasConfirm = !!(siteMapConfig && siteMapConfig.building_confirm_pin);
      const hasOutline = !!(siteMapConfig && siteMapConfig.building_outline);
      const floors = Math.max(0, Number((siteMapConfig && siteMapConfig.building_floors) || 0));
      return hasAddress && hasConfirm && hasOutline && floors >= 1;
    }

    function buildingSetupSharedCompleteMsg() {
      return "Building setup is complete and shared for all zones/devices. Continue with zone mapping.";
    }

    function openBuildingSetupRequiredPrompt(contextLabel = "map zones") {
      enterSiteMapWorkspace({
        title: "Building Outline Setup",
        hint: "Complete building outline and floors before mapping zone locations.",
        layer: "street",
        outlineWizard: true,
      });
      const hasConfirm = !!(siteMapConfig && siteMapConfig.building_confirm_pin);
      const hasOutline = !!(siteMapConfig && siteMapConfig.building_outline);
      const floors = Math.max(0, Number((siteMapConfig && siteMapConfig.building_floors) || 0));
      if (!String((siteMapConfig && siteMapConfig.address) || "").trim()) setOutlineWizardStep(1, `Complete building setup first, then you can ${contextLabel}.`);
      else if (!hasConfirm) setOutlineWizardStep(2, `Click the building on the map to confirm it before you ${contextLabel}.`, { skipAssist: true });
      else if (!hasOutline) setOutlineWizardStep(3, `Draw the building outline before you ${contextLabel}.`);
      else if (floors < 1) setOutlineWizardStep(4, `Set building floors before you ${contextLabel}.`);
      else setOutlineWizardStep(5, `Finish building setup steps, then you can ${contextLabel}.`);
      setSiteMapMsg("Complete Building Outline Setup first.");
      return false;
    }

    function ensureBuildingSetupReadyForZoneMapping(contextLabel = "map zones") {
      if (buildingSetupComplete()) return true;
      return openBuildingSetupRequiredPrompt(contextLabel);
    }

    function hasRealDownstreamZones() {
      const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
      return realDownstreamRows(ds).length > 0;
    }

    function nextUnpinnedZoneId() {
      const opts = siteMapZoneOptions().filter((o) => {
        if (o.id === "zone1_main" || o.id === "zone2_main") return true;
        const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
        const row = ds.find((z) => String(z.id || "") === String(o.id));
        return row ? isAdoptedZone(row) : false;
      });
      for (const o of opts) {
        if (!getSiteMapPin(o.id)) return o.id;
      }
      return "";
    }

    function openPromptToPinZone(zoneId, msg) {
      if (!ensureBuildingSetupReadyForZoneMapping("place zone and equipment pins")) return;
      const opts = siteMapZoneOptions();
      const hit = opts.find((o) => o.id === zoneId);
      const sel = document.getElementById("siteMapZoneSelect");
      if (sel) {
        sel.value = zoneId;
        syncSiteMapDetailsFormFromSelected();
      }
      enterSiteMapWorkspace({
        title: "Pin Zone / Device Location",
        hint: "Click the map to place the selected zone/device pin, then Save & Finish.",
        layer: "satellite",
      });
      siteMapDrawingBuildingOutline = false;
      siteMapDraftBuildingOutlinePoints = [];
      renderSiteMapDraftBuildingOutline();
      if (siteMapDrawingArea) {
        siteMapDrawingArea = false;
        siteMapDraftAreaPoints = [];
        renderSiteMapDraftArea();
      }
      if (!centerMapOnZonePin(zoneId)) {
        if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
      }
      updateSiteMapWorkspaceStatus();
      if (msg) setSiteMapMsg(msg);
      else if (hit) setSiteMapMsg(`Click the map to place ${hit.label}.`);
    }

    function confirmBuildingOutlineAndProceed(sourceLabel = "outline", opts = {}) {
      if (!(siteMapConfig && siteMapConfig.building_outline)) return false;
      centerMapOnBuildingOutline();
      const floorsAlreadySet = Math.max(0, Number((siteMapConfig && siteMapConfig.building_floors) || 0)) >= 1;
      if (!opts.skipFloorPrompt && !(siteMapBuildingEditOnlyMode && floorsAlreadySet)) {
        openSiteMapFloorModalBeforeOutlineConfirm(sourceLabel);
        return false;
      }
      const ok = window.confirm("Does the building outline look correct?\\n\\nChoose OK to save the outline and continue.");
      if (!ok) {
        setOutlineWizardStep(3, `Review the ${sourceLabel} and adjust it before continuing.`, { skipAssist: true });
        setSiteMapMsg("Building outline not confirmed yet. Adjust the outline and finish again.");
        return false;
      }
      // Persist the building outline immediately so it remains the base setup geometry.
      saveSiteMapConfig();
      const floorsNowSet = Math.max(0, Number((siteMapConfig && siteMapConfig.building_floors) || 0)) >= 1;
      if (siteMapBuildingEditOnlyMode && floorsNowSet) {
        setOutlineWizardStep(3, "Building outline updated and saved. Building setup is already complete.");
        updateSiteMapZoneMappingAvailability();
        updateSiteMapWorkspaceStatus("Building outline updated");
        setSiteMapMsg("Building outline updated and saved. Click Continue to Zone Setup if needed, or Exit Building Setup.");
        centerMapOnBuildingOutline();
        return true;
      }
      if (floorsNowSet) {
        saveBuildingFloorCount();
        setSiteMapMsg("Building outline confirmed and saved. Building setup complete. Continue to Zone Setup.");
      } else {
        setOutlineWizardStep(4, "Building outline saved and confirmed. Next: set and save the building floor count.");
        updateSiteMapZoneMappingAvailability();
        setSiteMapMsg("Building outline confirmed and saved. Next: set the building floor count, then Continue to Zone Setup.");
      }
      centerMapOnBuildingOutline();
      setTimeout(() => focusNoScroll(document.getElementById("siteMapBuildingFloors"), { select: true }), 40);
      return true;
    }

    function runPostBuildingSetupPromptFlow() {
      if (!buildingSetupComplete()) return;
      const z1Pinned = !!getSiteMapPin("zone1_main");
      const z2Pinned = !!getSiteMapPin("zone2_main");
      if (!z1Pinned) {
        if (window.confirm("Building setup is complete. Pin Zone 1 main boiler/circulator location now?")) {
          selectMainBoilerZone("zone1_main");
          return;
        }
      }
      if (!z2Pinned) {
        if (window.confirm("Pin Zone 2 main boiler/circulator location now?")) {
          selectMainBoilerZone("zone2_main");
          return;
        }
      }
      const hasDownstream = hasRealDownstreamZones();
      if (!hasDownstream) {
        if (window.confirm("No additional zone devices are detected yet. Do you want to set up a new device now?")) {
          openSetupPanel();
        }
        return;
      }
      const unpinned = nextUnpinnedZoneId();
      if (unpinned && !isMainZoneId(unpinned)) {
        const label = (siteMapZoneOptions().find((o) => o.id === unpinned) || {}).label || "next zone";
        if (window.confirm(`Building setup is complete. Do you want to place a pin for ${label} now?`)) {
          openPromptToPinZone(unpinned, `Click the map to place ${label}.`);
          return;
        }
      }
    }

    function applyParentColorsToZones() {
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      if (!siteMapConfig.pins || typeof siteMapConfig.pins !== "object") siteMapConfig.pins = {};
      const zoneIds = ["zone1_main", "zone2_main", ...realDownstreamRows((lastHubPayload && lastHubPayload.downstream) || []).map(z => z.id)];
      zoneIds.forEach((zid) => {
        if (!zid) return;
        const p = siteMapConfig.pins[zid];
        if (!p || typeof p !== "object") return;
        p.color = preferredPinColorForZone(zid);
        p.updated_utc = new Date().toISOString();
      });
      renderSiteMapMarkers();
      syncSiteMapDetailsFormFromSelected();
      setOutlineWizardStep(6, "Parent-zone colors applied. Next, pin the main boiler location on the map.");
      setSiteMapMsg("Parent-zone colors applied to mapped zones.");
    }

    function selectMainBoilerZone(zoneId, opts = {}) {
      if (!ensureBuildingSetupReadyForZoneMapping("pin main boiler locations")) return;
      if (!siteMapOutlineWizardOpen) setSiteMapSetupMode("zone-main");
      siteMapDrawingBuildingOutline = false;
      siteMapDraftBuildingOutlinePoints = [];
      renderSiteMapDraftBuildingOutline();
      siteMapDrawingArea = false;
      siteMapDraftAreaPoints = [];
      renderSiteMapDraftArea();
      const sel = document.getElementById("siteMapZoneSelect");
      if (sel) {
        sel.value = zoneId;
        syncSiteMapDetailsFormFromSelected();
      }
      enterSiteMapWorkspace({
        title: "Main Boiler Location",
        hint: `Pin only: place or verify the ${zoneId === "zone1_main" ? "Zone 1" : "Zone 2"} main boiler/circulator location, set floor in popup, then Save & Finish.`,
        layer: "satellite",
        outlineWizard: false,
      });
      const pin = getSiteMapPin(zoneId);
      if (pin && siteMapMap && isPointInsideSavedBuildingOutline({ lat: Number(pin.lat), lng: Number(pin.lng) })) {
        siteMapMap.setView([Number(pin.lat), Number(pin.lng)], Math.max(Number(siteMapConfig?.zoom || 18), 18));
      } else {
        if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
      }
      setOutlineWizardStep(
        6,
        `${zoneId === "zone1_main" ? "Zone 1" : "Zone 2"} main selected. Click the map once to place or update the main boiler pin, then set its floor in the popup.`,
        { skipAssist: !!opts.preserveStep }
      );
      updateSiteMapWorkspaceStatus();
      setSiteMapMsg(`${zoneId === "zone1_main" ? "Zone 1" : "Zone 2"} main boiler mode: pin placement only (not a building outline or zone area).`);
    }

    function advanceOutlineWizardStep() {
      if (!siteMapOutlineWizardOpen) return;
      const cur = Number(siteMapOutlineWizardStep || 1);
      const hasAddress = !!String((siteMapConfig && siteMapConfig.address) || "").trim();
      const hasConfirm = !!(siteMapConfig && siteMapConfig.building_confirm_pin);
      const hasOutline = !!(siteMapConfig && siteMapConfig.building_outline);
      const floorsSet = Math.max(0, Number((siteMapConfig && siteMapConfig.building_floors) || 0)) >= 1;
      if (cur === 1 && !hasAddress) {
        setOutlineWizardMsg("Find the building address first, then press Enter to continue.");
        setSiteMapMsg("Find Address first.");
        return;
      }
      if (cur === 2 && !hasConfirm) {
        setOutlineWizardMsg("Click the building on the map to confirm the correct structure, then press Enter.");
        setSiteMapMsg("Click the building on the map to confirm it.");
        return;
      }
      if (cur === 3 && !hasOutline) {
        setOutlineWizardMsg("Draw the building outline (or use Auto Detect) before continuing.");
        setSiteMapMsg("Complete the building outline first.");
        return;
      }
      if (cur === 4 && !floorsSet) {
        setOutlineWizardMsg("Set and save the building floor count before continuing.");
        setSiteMapMsg("Save the building floor count first.");
        return;
      }
      const next = Math.min(7, cur + 1);
      setOutlineWizardStep(next);
    }

    function goToPreviousOutlineWizardStep() {
      if (!siteMapOutlineWizardOpen) return;
      const cur = Math.max(1, Number(siteMapOutlineWizardStep || 1));
      if (cur <= 1) return;
      if (siteMapBuildingEditOnlyMode && cur === 3) {
        const switchToLookup = window.confirm(
          "Return to Building Lookup?\\n\\n" +
          "Choose OK to leave outline edit mode and go back to Building Lookup.\\n" +
          "Choose Cancel to stay in Edit Existing Outline."
        );
        if (!switchToLookup) {
          setSiteMapMsg("Stayed in Edit Existing Outline.");
          updateSiteMapWorkspaceStatus("Editing saved outline points");
          return;
        }
        const saveBeforeLookup = window.confirm(
          "Save current building setup changes before returning to Building Lookup?\\n\\n" +
          "Choose OK to save now. Choose Cancel to continue without saving."
        );
        if (saveBeforeLookup) {
          try {
            saveSiteMapConfig();
            setSiteMapMsg("Saved current building setup. Returning to Building Lookup.");
          } catch (_e) {
            setSiteMapMsg("Could not save building setup before returning. Continuing to Building Lookup.");
          }
        }
        toggleBuildingOutlineDragMode(false);
        siteMapEditingSavedOutlinePoints = false;
        siteMapSelectedOutlinePointIndex = -1;
        siteMapOutlinePointDragActive = false;
        siteMapDrawingBuildingOutline = false;
        siteMapDraftBuildingOutlinePoints = [];
        renderSiteMapDraftBuildingOutline();
        siteMapBuildingEditOnlyMode = false;
        setSiteMapBaseLayer("street");
        setOutlineWizardStep(1, "Building lookup override enabled. You can find/confirm a different building or continue editing the existing outline.", { skipAssist: true });
        setSiteMapMsg("Returned to Building Setup lookup. Use Find Address to override the current building, or go back to Edit Existing Outline.");
        updateSiteMapWorkspaceStatus("Building lookup override");
        if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
        return;
      }
      const prev = cur - 1;
      setOutlineWizardStep(prev, `Returned to Step ${prev}.`);
      setSiteMapMsg(`Returned to Step ${prev}.`);
    }

    function updateSiteMapWorkspaceStatus(extraText = "") {
      const el = document.getElementById("siteMapWorkspaceStatus");
      if (!el) return;
      const zid = selectedSiteMapZoneId();
      const opt = siteMapZoneOptions().find((o) => o.id === zid);
      const label = (opt && opt.label) ? opt.label : (zid || "--");
      let status = `Selected: ${label}`;
      if (siteMapDrawingBuildingOutline) status += ` | Drawing building outline (${siteMapDraftBuildingOutlinePoints.length} pt${siteMapDraftBuildingOutlinePoints.length === 1 ? "" : "s"})`;
      if (siteMapDrawingArea) status += ` | Drawing area (${siteMapDraftAreaPoints.length} pt${siteMapDraftAreaPoints.length === 1 ? "" : "s"})`;
      if (siteMapOutlineWizardOpen) status += ` | Outline Setup Step ${siteMapOutlineWizardStep}`;
      if (extraText) status += ` | ${extraText}`;
      el.textContent = status;
      updateSiteMapDrawHint();
    }

    function renderSiteMapZoneWizard() {
      const wiz = document.getElementById("siteMapZoneWizard");
      const status = document.getElementById("siteMapZoneWizardStatus");
      const mainActions = document.getElementById("siteMapZoneWizardMainActions");
      const dsActions = document.getElementById("siteMapZoneWizardDownstreamActions");
      const mainTab = document.getElementById("siteMapZoneWizardMainTabBtn");
      const dsTab = document.getElementById("siteMapZoneWizardDownstreamTabBtn");
      const loadSelectedBtn = document.getElementById("siteMapZoneWizardLoadSelectedBtn");
      const startAreaBtn = document.getElementById("siteMapZoneWizardStartAreaBtn");
      if (!wiz) return;
      const isMain = siteMapSetupMode === "zone-main";
      const isDownstream = siteMapSetupMode === "zone-downstream";
      const selId = selectedSiteMapZoneId();
      const hasSelectedDownstream = !!selId && !isMainZoneId(selId);
      wiz.classList.toggle("show", isMain || isDownstream);
      if (mainActions) mainActions.classList.toggle("hidden", !isMain);
      if (dsActions) dsActions.classList.toggle("hidden", !isDownstream);
      if (mainTab) mainTab.classList.toggle("active", isMain);
      if (dsTab) dsTab.classList.toggle("active", isDownstream);
      if (status) {
        if (isMain) {
          status.textContent = "Zone Setup - Main Zones: The saved building outline is locked and shown on the satellite image. Choose Zone 1 or Zone 2, then place the main boiler/circulator pin inside the building outline. Set the floor in the map popup.";
        } else if (isDownstream) {
          status.textContent = hasSelectedDownstream
            ? `Zone Setup - Downstream Zones: Selected device is ready. Place the pin inside the locked building outline, then draw an optional served area.`
            : "Zone Setup - Downstream Zones: Step 1 is choose a downstream zone/device from the Zone / Device dropdown, then click Use Selected Device.";
        } else {
          status.textContent = "";
        }
      }
      if (loadSelectedBtn) loadSelectedBtn.disabled = !isDownstream || !hasSelectedDownstream;
      if (startAreaBtn) startAreaBtn.disabled = !isDownstream || !hasSelectedDownstream;
    }

    function renderSiteMapModeChrome() {
      const addressWrap = document.getElementById("siteAddress")?.closest("div");
      const findBtn = document.getElementById("siteMapFindBtn");
      const footprintBtn = document.getElementById("siteMapFootprintBtn");
      const zoneSetupBtn = document.getElementById("siteMapZoneSetupBtn");
      const saveBtn = document.getElementById("siteMapSaveBtn");
      const lockNote = document.getElementById("siteMapZoneMappingLockedNote");
      const inBuilding = siteMapSetupMode === "building";
      const inZone = siteMapSetupMode === "zone-main" || siteMapSetupMode === "zone-downstream";

      if (addressWrap) addressWrap.classList.toggle("hidden", inZone);
      if (findBtn) findBtn.classList.toggle("hidden", inZone);
      if (footprintBtn) footprintBtn.classList.toggle("hidden", inZone);
      if (zoneSetupBtn) zoneSetupBtn.classList.toggle("hidden", inBuilding);
      if (saveBtn) saveBtn.classList.toggle("hidden", inBuilding);
      if (lockNote && inZone) lockNote.style.display = "none";
    }

    function setSiteMapSetupMode(mode) {
      siteMapSetupMode = String(mode || "none");
      const card = document.getElementById("siteMapCard");
      if (card) {
        card.classList.toggle("building-setup-mode", siteMapSetupMode === "building");
        card.classList.toggle("zone-setup-mode", siteMapSetupMode === "zone-main" || siteMapSetupMode === "zone-downstream");
      }
      renderSiteMapModeChrome();
      renderSiteMapZoneWizard();
    }

    function exitZoneSetupMode() {
      setSiteMapSetupMode("none");
      exitSiteMapWorkspace();
      setSiteMapMsg("Exited Zone Setup mode.");
    }

    function updateSiteMapDrawHint() {
      const el = document.getElementById("siteMapDrawHint");
      if (!el) return;
      let text = "";
      if (siteMapDrawingBuildingOutline) {
        const n = Number(siteMapDraftBuildingOutlinePoints.length || 0);
        text = `Drawing Building Outline: Click to add points • Use Finish Manual Outline button • Undo Last Point${n ? ` • ${n} point${n === 1 ? "" : "s"}` : ""}`;
      } else if (siteMapDrawingArea) {
        const n = Number(siteMapDraftAreaPoints.length || 0);
        text = `Drawing Zone Area: Click to add points • Use Finish Area button • Undo Last Area Point${n ? ` • ${n} point${n === 1 ? "" : "s"}` : ""}`;
      } else if (siteMapOutlineWizardOpen && Number(siteMapOutlineWizardStep || 1) === 6) {
        const zid = selectedSiteMapZoneId();
        if (zid === "zone1_main" || zid === "zone2_main") {
          text = `Main Boiler Pin Mode: Click map once to place pin • Set floor in popup • Save & Finish`;
        }
      }
      if (text) {
        el.textContent = text;
        el.style.display = "block";
      } else {
        el.textContent = "";
        el.style.display = "none";
      }
    }

    function openSiteMapPanel(opts = {}) {
      ensureAdminToolsOpen();
      setAdminTab("map");
      scrollToAdminPanel("adminMapPanel");
      setTimeout(() => {
        if (opts && opts.suppressAutoBuildingPrompt) return;
        if (siteMapOutlineWizardOpen) return;
        if (!buildingSetupComplete()) {
          openBuildingSetupRequiredPrompt("map zone locations");
        }
      }, 120);
    }

    function enterSiteMapWorkspace(opts = {}) {
      if (!siteMapWorkspaceOpen) {
        const mapPanel = document.getElementById("adminMapPanel");
        if (mapPanel && !mapPanel.classList.contains("hidden")) {
          ensureAdminToolsOpen();
          setAdminTab("map");
        } else {
          openSiteMapPanel({ suppressAutoBuildingPrompt: true });
        }
      }
      siteMapWorkspaceOpen = true;
      const card = document.getElementById("siteMapCard");
      const bar = document.getElementById("siteMapWorkspaceBar");
      if (card) card.classList.add("workspace");
      if (bar) bar.classList.add("show");
      document.body.classList.add("map-workspace-open");
      setSiteMapWorkspaceMeta(opts.title, opts.hint);
      if (opts.outlineWizard) showOutlineWizard(true);
      else showOutlineWizard(false);
      if (opts.layer) setSiteMapBaseLayer(opts.layer);
      setTimeout(() => {
        if (siteMapMap) siteMapMap.invalidateSize();
        scrollSiteMapWorkspaceToCanvas({ behavior: "smooth", pad: (opts.outlineWizard ? 8 : 40) });
      }, 80);
    }

    function exitSiteMapWorkspace() {
      siteMapWorkspaceOpen = false;
      setSiteMapSetupMode("none");
      const card = document.getElementById("siteMapCard");
      const bar = document.getElementById("siteMapWorkspaceBar");
      if (card) card.classList.remove("workspace");
      if (card) card.classList.remove("step-map-focus");
      if (bar) bar.classList.remove("show");
      document.body.classList.remove("map-workspace-open");
      if (siteMapDrawingArea) {
        siteMapDrawingArea = false;
        siteMapDraftAreaPoints = [];
        renderSiteMapDraftArea();
      }
      if (siteMapDrawingBuildingOutline) {
        siteMapDrawingBuildingOutline = false;
        siteMapDraftBuildingOutlinePoints = [];
        renderSiteMapDraftBuildingOutline();
      }
      showOutlineWizard(false);
      updateSiteMapWorkspaceStatus();
      setTimeout(() => { if (siteMapMap) siteMapMap.invalidateSize(); }, 50);
    }

    async function saveAndFinishSiteMapWorkspace() {
      const wasOutlineWizard = !!siteMapOutlineWizardOpen;
      if (siteMapOutlineWizardOpen) setOutlineWizardStep(7, "Saving map changes and finishing setup...");
      await saveSiteMapConfig();
      exitSiteMapWorkspace();
      setSiteMapMsg("Map changes saved.");
      if (buildingSetupComplete()) {
        if (wasOutlineWizard && siteMapAutoOpenZoneSetupAfterBuildingSave) {
          siteMapAutoOpenZoneSetupAfterBuildingSave = false;
          setTimeout(() => {
            const z1Pinned = !!getSiteMapPin("zone1_main");
            const z2Pinned = !!getSiteMapPin("zone2_main");
            const nextMode = (z1Pinned && z2Pinned) ? "zone-downstream" : "zone-main";
            setSiteMapMsg("Building setup complete. Opening Zone Setup on the saved building outline.");
            openZoneSetupWorkspace(nextMode);
          }, 150);
          return;
        }
        setTimeout(runPostBuildingSetupPromptFlow, 150);
      }
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
      updateSiteMapWorkspaceStatus();
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
      ensureAdminToolsOpen();
      const requestedTabId = String(zoneGraphId || "").trim();
      if (zoneGraphId) currentTabId = zoneGraphId;
      if (cycleZoneId) activeCycleZoneId = cycleZoneId;
      pendingGraphTabId = String(zoneGraphId || "");
      pendingCycleZoneId = String(cycleZoneId || "");
      activeRange = "live";
      document.querySelectorAll(".btn[data-range]").forEach((b) => b.classList.remove("active"));
      const liveBtn = document.querySelector('.btn[data-range="live"]');
      if (liveBtn) liveBtn.classList.add("active");
      const ds = (lastHubPayload && Array.isArray(lastHubPayload.downstream)) ? lastHubPayload.downstream : [];
      updateTabs(ds);
      if (requestedTabId && !tabDefs.some((t) => String(t.id || "") === requestedTabId)) {
        tabDefs.push({ id: requestedTabId, label: (cycleZoneId ? String(cycleZoneId) : requestedTabId), type: "downstream", zoneId: String(cycleZoneId || "") });
        currentTabId = requestedTabId;
      }
      updateCycleTabs(ds);
      renderSelectedGraph();
      refreshCycles();
      const graph = document.getElementById("graphTitle");
      if (graph) {
        graph.classList.add("flash-target");
        graph.scrollIntoView({ behavior: "smooth", block: "start" });
        window.setTimeout(() => graph.scrollIntoView({ behavior: "smooth", block: "start" }), 140);
        window.setTimeout(() => graph.scrollIntoView({ behavior: "smooth", block: "start" }), 420);
        window.setTimeout(() => graph.classList.remove("flash-target"), 700);
      }
      const perfMsg = document.getElementById("zonePerfMsg");
      if (perfMsg) perfMsg.textContent = `Opened graph for ${cycleZoneId || zoneGraphId || "selected zone"}.`;
    }
    function applyPendingGraphSelection(downstreamRows) {
      const tabId = String(pendingGraphTabId || "").trim();
      if (!tabId) return;
      const hasTab = tabDefs.some((t) => String(t.id || "") === tabId);
      if (!hasTab) return;
      currentTabId = tabId;
      const cycleId = String(pendingCycleZoneId || "").trim();
      if (cycleId) activeCycleZoneId = cycleId;
      updateTabs(downstreamRows || []);
      updateCycleTabs(downstreamRows || []);
      renderSelectedGraph();
      refreshCycles();
      const graph = document.getElementById("graphTitle");
      if (graph) {
        graph.classList.add("flash-target");
        graph.scrollIntoView({ behavior: "smooth", block: "start" });
        window.setTimeout(() => graph.scrollIntoView({ behavior: "smooth", block: "start" }), 140);
        window.setTimeout(() => graph.scrollIntoView({ behavior: "smooth", block: "start" }), 420);
        window.setTimeout(() => graph.classList.remove("flash-target"), 700);
      }
      pendingGraphTabId = "";
      pendingCycleZoneId = "";
    }
    window.openZoneGraphFromButton = function(btnEl) {
      const perfMsg = document.getElementById("zonePerfMsg");
      try {
        ensureAdminToolsOpen();
        if (!btnEl) return false;
        const graph = document.getElementById("graphTitle");
        const chart = document.getElementById("historyChart");
        if (graph && typeof graph.scrollIntoView === "function") graph.scrollIntoView({ behavior: "smooth", block: "start" });
        if (chart && typeof chart.scrollIntoView === "function") window.setTimeout(() => chart.scrollIntoView({ behavior: "smooth", block: "start" }, 120));
        if (graph && typeof graph.scrollIntoView === "function") {
          window.setTimeout(() => graph.scrollIntoView({ behavior: "smooth", block: "start" }), 250);
          window.setTimeout(() => graph.scrollIntoView({ behavior: "smooth", block: "start" }), 650);
        }
        try { window.location.hash = "graphTitle"; } catch (_e) {}
        const tabId = String((btnEl.dataset && btnEl.dataset.openGraphTab) || "").trim();
        const cycId = String((btnEl.dataset && btnEl.dataset.openGraphCycle) || "").trim();
        if (!tabId) return false;
        focusZoneGraph(tabId, cycId);
        refreshHub();
        if (perfMsg) perfMsg.textContent = `Opening graph for ${cycId || tabId}...`;
        return false;
      } catch (e) {
        if (perfMsg) perfMsg.textContent = `Open Graph error: ${e && e.message ? e.message : "unknown"}`;
        return false;
      }
    };

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
        <div class="overview-kpi"><div class="k">${activeOverviewParent} Main Supply</div><div class="v">${mainFeed}</div></div>
        <div class="overview-kpi"><div class="k">${activeOverviewParent} Main Return</div><div class="v">${mainRet}</div></div>
        <div class="overview-kpi"><div class="k">Downstream Zones</div><div class="v">${ds.length}</div></div>
        <div class="overview-kpi"><div class="k">Calling Now</div><div class="v">${callingCount}</div></div>
        <div class="overview-kpi"><div class="k">Trouble</div><div class="v">${troubleCount}</div></div>
        <div class="overview-kpi"><div class="k">Online</div><div class="v">${onlineCount}/${ds.length}</div></div>
        <div class="overview-kpi"><div class="k">Avg Supply</div><div class="v">${avg(feedVals)}</div></div>
        <div class="overview-kpi"><div class="k">Avg Return</div><div class="v">${avg(retVals)}</div></div>
      `;

      if (!ds.length) {
        cardsEl.innerHTML = `
          <div class="overview-card">
            <div class="t">No zones assigned to ${activeOverviewParent} yet</div>
            <div class="s" style="margin-top:6px">Set up and adopt your first downstream zone to see live performance cards here.</div>
            <div style="margin-top:10px"><button type="button" class="btn active" id="overviewAddFirstZoneBtn">Add First Zone</button></div>
          </div>`;
        const cta = document.getElementById("overviewAddFirstZoneBtn");
        if (cta) cta.addEventListener("click", openSetupPanel);
        return;
      }
      cardsEl.innerHTML = "";
      ds.forEach((z) => {
        const card = document.createElement("div");
        card.className = "overview-card";
        const zoneId = String(z.id || "");
        const zoneIdJs = zoneId.replace(/'/g, "\\'");
        const status = z.zone_status || z.status || "--";
        const note = z.status_note || "";
        const statusCls = statusClass(z.status);
        card.innerHTML = `
          <div style="display:flex;justify-content:space-between;gap:8px;align-items:start">
            <div>
              <div class="t">${z.name || z.id || "--"}</div>
              <div class="s">${z.id || ""}</div>
            </div>
            <button type="button" class="btn" data-open-graph-tab="ds_${zoneId}" data-open-graph-cycle="${zoneId}" onclick="return window.openZoneGraphFromButton(this)" style="padding:4px 10px">Open Graph</button>
          </div>
          <div class="line">Call: ${z.call_status || (z.heating_call === true ? "24VAC Present (Calling)" : (z.heating_call === false ? "No 24VAC (Idle)" : "24VAC Signal Unknown"))}</div>
          <div class="line">Supply: ${fmtTemp(z.feed_f)} · Return: ${fmtTemp(z.return_f)}</div>
          <div class="line ${statusCls}">Status: ${status}</div>
          ${note ? `<div class="line s">Note: ${note}</div>` : ""}
        `;
        const btn = card.querySelector("button[data-open-graph-tab]");
        if (btn) {
          btn.addEventListener("click", (ev) => {
            if (ev && typeof ev.preventDefault === "function") ev.preventDefault();
            if (ev && typeof ev.stopPropagation === "function") ev.stopPropagation();
            focusZoneGraph(btn.dataset.openGraphTab || ("ds_" + (z.id || "")), btn.dataset.openGraphCycle || (z.id || ""));
          });
        }
        cardsEl.appendChild(card);
      });
      if (ds.length === 1) {
        const addCard = document.createElement("div");
        addCard.className = "overview-card";
        addCard.innerHTML = `
          <div class="t">Add Another Zone</div>
          <div class="s" style="margin-top:6px">Set up and adopt the next downstream device to expand this parent zone.</div>
          <div style="margin-top:10px"><button type="button" class="btn" style="padding:4px 10px">Add Another Zone</button></div>
        `;
        const btn = addCard.querySelector("button");
        if (btn) btn.addEventListener("click", openSetupPanel);
        cardsEl.appendChild(addCard);
      }
    }

    function siteMapPreviewZones() {
      const hub = lastHubPayload || {};
      const ds = realDownstreamRows(Array.isArray(hub.downstream) ? hub.downstream : []);
      const out = [
        { id: "zone1_main", label: "Zone 1 (Main)", parent_zone: "Zone 1", is_main: true },
        { id: "zone2_main", label: "Zone 2 (Main)", parent_zone: "Zone 2", is_main: true },
      ];
      ds.forEach((z) => out.push({
        id: String(z.id || ""),
        label: String(z.name || z.id || "Zone"),
        parent_zone: String(z.parent_zone || ""),
        is_main: false,
      }));
      return out;
    }

    function renderMainMapPreview() {
      const sec = document.getElementById("mainMapPreviewSection");
      const svg = document.getElementById("mainMapPreviewSvg");
      const legend = document.getElementById("mainMapPreviewLegend");
      const note = document.getElementById("mainMapPreviewNote");
      const editZonesBtn = document.getElementById("mainMapPreviewEditZonesBtn");
      if (!sec || !svg || !legend || !note) return;
      if (editZonesBtn) {
        const canZoneMap = !!buildingSetupComplete();
        editZonesBtn.disabled = !canZoneMap;
        editZonesBtn.style.opacity = canZoneMap ? "" : "0.6";
        editZonesBtn.title = canZoneMap ? "Edit zone locations and areas" : "Complete Building Setup first (address, confirm pin, outline, floors)";
      }
      const cfg = siteMapConfig && typeof siteMapConfig === "object" ? siteMapConfig : null;
      const outline = cfg && cfg.building_outline;
      const pins = (cfg && cfg.pins && typeof cfg.pins === "object") ? cfg.pins : {};
      if (!outline || !Array.isArray(outline.coordinates) || !Array.isArray(outline.coordinates[0]) || outline.coordinates[0].length < 3) {
        sec.classList.remove("hidden");
        svg.innerHTML = `<rect x="0" y="0" width="1000" height="560" fill="#fbfdff"/><text x="500" y="260" text-anchor="middle" font-size="24" font-weight="800" fill="#0f172a">Building Map Preview Unavailable</text><text x="500" y="294" text-anchor="middle" font-size="15" fill="#475569">No saved building outline was found.</text><text x="500" y="320" text-anchor="middle" font-size="15" fill="#475569">Use Edit Building Map to restore or redraw the building outline.</text>`;
        legend.innerHTML = `<span class="chip"><span class="sw" style="background:#ffffff;border-color:#0f172a"></span>No saved building outline</span>`;
        note.textContent = "Click 'Edit Building Map' to open Building Setup and save a building outline. The preview will return after the outline is saved.";
        mainMapPreviewLastRenderKey = "__hidden__";
        return;
      }
      sec.classList.remove("hidden");

      const ring = outline.coordinates[0]
        .map((c) => (Array.isArray(c) && c.length >= 2 ? { lng: Number(c[0]), lat: Number(c[1]) } : null))
        .filter((p) => p && Number.isFinite(p.lat) && Number.isFinite(p.lng));
      if (ring.length < 3) {
        sec.classList.remove("hidden");
        svg.innerHTML = `<rect x="0" y="0" width="1000" height="560" fill="#fbfdff"/><text x="500" y="270" text-anchor="middle" font-size="22" font-weight="800" fill="#0f172a">Building Outline Data Incomplete</text><text x="500" y="304" text-anchor="middle" font-size="15" fill="#475569">Open Edit Building Map and save the outline again.</text>`;
        legend.innerHTML = "";
        note.textContent = "The saved building outline is incomplete and cannot be rendered in the preview yet.";
        mainMapPreviewLastRenderKey = "__hidden__";
        return;
      }
      const zoneDefs = siteMapPreviewZones();
      const previewPinsKey = Object.entries(pins)
        .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
        .map(([zid, p]) => ({
          id: String(zid),
          lat: Number(p && p.lat),
          lng: Number(p && p.lng),
          color: String((p && p.color) || ""),
          floor: String((p && p.main_boiler_floor) || ""),
          polygon: Array.isArray(p && p.polygon)
            ? p.polygon.map((q) => [Number(q && q.lat), Number(q && q.lng)])
            : [],
        }));
      const previewKey = JSON.stringify({
        outline: ring.map((p) => [Number(p.lat), Number(p.lng)]),
        pins: previewPinsKey,
        zones: zoneDefs.map((z) => ({ id: z.id, label: z.label, parent: z.parent_zone, main: !!z.is_main })),
      });
      if (previewKey === mainMapPreviewLastRenderKey) return;
      mainMapPreviewLastRenderKey = previewKey;
      // Frame the preview to the saved building outline only so misplaced pins
      // do not zoom the entire preview far away from the building.
      const ptsAll = [...ring];
      const minLat = Math.min(...ptsAll.map((p) => p.lat));
      const maxLat = Math.max(...ptsAll.map((p) => p.lat));
      const minLng = Math.min(...ptsAll.map((p) => p.lng));
      const maxLng = Math.max(...ptsAll.map((p) => p.lng));
      const W = 1000, H = 560, pad = 44;
      const bbPadLat = Math.max(0.00012, (maxLat - minLat) * 0.15);
      const bbPadLng = Math.max(0.00012, (maxLng - minLng) * 0.15);
      const bgMinLat = minLat - bbPadLat;
      const bgMaxLat = maxLat + bbPadLat;
      const bgMinLng = minLng - bbPadLng;
      const bgMaxLng = maxLng + bbPadLng;
      const dx = Math.max(0.000001, maxLng - minLng);
      const dy = Math.max(0.000001, maxLat - minLat);
      const scale = Math.min((W - pad * 2) / dx, (H - pad * 2) / dy);
      const xOff = (W - dx * scale) / 2;
      const yOff = (H - dy * scale) / 2;
      const project = (lat, lng) => ({
        x: xOff + (Number(lng) - minLng) * scale,
        y: H - (yOff + (Number(lat) - minLat) * scale),
      });
      const esc = (s) => String(s || "").replace(/[&<>"]/g, (m) => {
        if (m === "&") return "&amp;";
        if (m === "<") return "&lt;";
        if (m === ">") return "&gt;";
        return "&quot;";
      });
      const satelliteBgUrl = (() => {
        const q = new URLSearchParams({
          bbox: `${bgMinLng},${bgMinLat},${bgMaxLng},${bgMaxLat}`,
          bboxSR: "4326",
          imageSR: "4326",
          size: `${W},${H}`,
          format: "png32",
          transparent: "false",
          f: "image",
        });
        return `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export?${q.toString()}`;
      })();

      const outlinePath = ring.map((p, i) => {
        const xy = project(p.lat, p.lng);
        return `${i === 0 ? "M" : "L"} ${xy.x.toFixed(1)} ${xy.y.toFixed(1)}`;
      }).join(" ") + " Z";

      const labelsById = {};
      zoneDefs.forEach((z) => { labelsById[z.id] = z; });
      const validPins = {};
      const invalidPreviewPins = [];
      Object.entries(pins).forEach(([zid, p]) => {
        const lat = Number(p && p.lat), lng = Number(p && p.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        if (!pointInPolygonLatLng({ lat, lng }, ring)) {
          invalidPreviewPins.push(String((p && p.label) || (labelsById[zid] && labelsById[zid].label) || zid));
          return;
        }
        validPins[zid] = p;
      });

      const mainPins = {
        "Zone 1": validPins["zone1_main"] || null,
        "Zone 2": validPins["zone2_main"] || null,
      };

      let svgParts = [
        `<rect x="0" y="0" width="${W}" height="${H}" fill="#fbfdff"/>`,
        `<image href="${esc(satelliteBgUrl)}" x="0" y="0" width="${W}" height="${H}" preserveAspectRatio="none" opacity="1"/>`,
        `<rect x="0" y="0" width="${W}" height="${H}" fill="rgba(255,255,255,0.08)"/>`,
        `<path d="${outlinePath}" fill="rgba(148,163,184,0.10)" stroke="#0f172a" stroke-width="4" />`,
        `<g aria-label="North arrow">` +
          `<rect x="${W - 112}" y="18" width="94" height="74" rx="10" ry="10" fill="rgba(255,255,255,0.94)" stroke="#d6e2e8" />` +
          `<text x="${W - 65}" y="40" text-anchor="middle" font-size="14" font-weight="800" fill="#0f172a">NORTH</text>` +
          `<line x1="${W - 65}" y1="74" x2="${W - 65}" y2="50" stroke="#0f172a" stroke-width="3" stroke-linecap="round" />` +
          `<path d="M ${W - 65} 45 L ${W - 73} 57 L ${W - 57} 57 Z" fill="#0f172a" />` +
        `</g>`,
      ];

      Object.entries(validPins).forEach(([zid, p]) => {
        const poly = Array.isArray(p && p.polygon) ? p.polygon : [];
        if (poly.length < 3) return;
        const labelInfo = labelsById[zid] || { parent_zone: "", is_main: isMainZoneId(zid), label: zid };
        const color = pinColorHex(p.color || preferredPinColorForZone(zid));
        const d = poly
          .map((q, i) => {
            const xy = project(Number(q.lat), Number(q.lng));
            return `${i === 0 ? "M" : "L"} ${xy.x.toFixed(1)} ${xy.y.toFixed(1)}`;
          }).join(" ") + " Z";
        svgParts.push(`<path d="${d}" fill="${color}22" stroke="${color}" stroke-width="2" stroke-dasharray="6 4" />`);
      });

      Object.entries(validPins).forEach(([zid, p]) => {
        const lat = Number(p && p.lat), lng = Number(p && p.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        const labelInfo = labelsById[zid] || { parent_zone: "", is_main: isMainZoneId(zid), label: zid };
        if (labelInfo.is_main) return;
        const mainPin = mainPins[String(labelInfo.parent_zone || "")];
        const mlat = Number(mainPin && mainPin.lat), mlng = Number(mainPin && mainPin.lng);
        if (!Number.isFinite(mlat) || !Number.isFinite(mlng)) return;
        const a = project(mlat, mlng);
        const b = project(lat, lng);
        const color = pinColorHex(p.color || preferredPinColorForZone(zid));
        svgParts.push(`<line x1="${a.x.toFixed(1)}" y1="${a.y.toFixed(1)}" x2="${b.x.toFixed(1)}" y2="${b.y.toFixed(1)}" stroke="${color}" stroke-width="2.5" stroke-linecap="round" />`);
      });

      Object.entries(validPins).forEach(([zid, p]) => {
        const lat = Number(p && p.lat), lng = Number(p && p.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        const xy = project(lat, lng);
        const labelInfo = labelsById[zid] || { parent_zone: "", is_main: isMainZoneId(zid), label: zid };
        const color = pinColorHex(p.color || preferredPinColorForZone(zid));
        const r = labelInfo.is_main ? 8 : 6;
        const stroke = labelInfo.is_main ? "#111827" : color;
        const sw = labelInfo.is_main ? 3 : 2;
        svgParts.push(`<circle cx="${xy.x.toFixed(1)}" cy="${xy.y.toFixed(1)}" r="${r}" fill="${color}" stroke="${stroke}" stroke-width="${sw}" />`);
        const floorText = labelInfo.is_main && p.main_boiler_floor ? ` · Floor ${esc(p.main_boiler_floor)}` : "";
        const short = esc(labelInfo.label || zid);
        svgParts.push(`<text x="${(xy.x + 10).toFixed(1)}" y="${(xy.y - 10).toFixed(1)}" font-size="12" font-weight="700" fill="#0f172a">${short}${floorText}</text>`);
      });

      svg.innerHTML = svgParts.join("");

      const z1Color = pinColorHex("red");
      const z2Color = pinColorHex("blue");
      legend.innerHTML = `
        <span class="chip"><span class="sw" style="background:${z1Color}"></span>Zone 1 family (pins + lines)</span>
        <span class="chip"><span class="sw" style="background:${z2Color}"></span>Zone 2 family (pins + lines)</span>
        <span class="chip"><span class="sw" style="background:#f59e0b;border-color:#111827"></span>Main boiler pin (larger marker)</span>
        <span class="chip"><span class="sw" style="background:#ffffff;border-color:#0f172a"></span>Building outline = saved site base</span>
      `;
      note.textContent = "Satellite image is auto-framed from the saved building outline. Pins and branch lines are auto-generated from Site Map locations. North is up in this preview. Use 'Edit Building Map' to adjust the outline and layout." + (invalidPreviewPins.length ? ` ${invalidPreviewPins.length} pin${invalidPreviewPins.length === 1 ? " was" : "s were"} hidden because ${invalidPreviewPins.length === 1 ? "it is" : "they are"} outside the saved building outline.` : "");
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
          <td data-k="Supply">${fmtTemp(z.feed_f)}</td>
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
        applyPendingGraphSelection(downstream);
        renderRows(downstream);
        renderZoneOverview();
        renderMainMapPreview();
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

    function formatHubTitleFromAddress(addr) {
      const base = "Heating Hub";
      const raw = String(addr || "").trim();
      if (!raw) return base;
      const cleaned = raw.replace(/\s+/g, " ").trim();
      if (!cleaned) return base;
      const titleCase = cleaned
        .split(" ")
        .map((part) => {
          if (!part) return part;
          return part.charAt(0).toUpperCase() + part.slice(1);
        })
        .join(" ");
      return `${titleCase} Heating Hub`;
    }

    function refreshHubBranding() {
      const titleText = formatHubTitleFromAddress(siteMapConfig && siteMapConfig.address);
      const h1 = document.getElementById("hubTitle");
      if (h1) h1.textContent = titleText;
      try { document.title = titleText; } catch (_e) {}
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
      const fullAddr = String(siteMapConfig.address || "");
      document.getElementById("siteAddress").value = fullAddr;
      const streetEl = document.getElementById("siteAddrStreet");
      const stateEl = document.getElementById("siteAddrState");
      const zipEl = document.getElementById("siteAddrZip");
      if (streetEl || stateEl || zipEl) {
        const parts = fullAddr.split(",").map((s) => s.trim()).filter(Boolean);
        let street = fullAddr;
        let state = "";
        let zip = "";
        if (parts.length >= 2) {
          street = parts[0];
        }
        const tail = parts.length >= 2 ? parts.slice(1).join(" ") : fullAddr;
        if (tail) {
          const m = tail.match(/([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$/);
          if (m) {
            state = m[1];
            zip = m[2];
            if (parts.length >= 2) street = parts[0];
          } else {
            const m2 = tail.match(/\b([A-Za-z]{2})\b/);
            if (m2) state = String(m2[1]).toUpperCase();
          }
        }
        if (streetEl) streetEl.value = street;
        if (stateEl) stateEl.value = state;
        if (zipEl) zipEl.value = zip;
      }
      const layerSel = document.getElementById("siteMapLayer");
      if (layerSel) layerSel.value = siteMapConfig.map_layer || "street";
      if (siteMapConfig.center && Number.isFinite(Number(siteMapConfig.center.lat)) && Number.isFinite(Number(siteMapConfig.center.lng))) {
        document.getElementById("siteMapCenter").value = `${Number(siteMapConfig.center.lat).toFixed(5)}, ${Number(siteMapConfig.center.lng).toFixed(5)}`;
      }
      if (siteMapConfig.zoom !== undefined && siteMapConfig.zoom !== null) {
        document.getElementById("siteMapZoom").value = String(Number(siteMapConfig.zoom));
      }
      const floorsEl = document.getElementById("siteMapBuildingFloors");
      if (floorsEl) floorsEl.value = String(Math.max(1, Number(siteMapConfig.building_floors || 1)));
      syncSiteMapDetailsFormFromSelected();
      updateSiteMapZoneMappingAvailability();
    }

    function updateSiteMapZoneMappingAvailability() {
      const ready = buildingSetupComplete();
      const wrap = document.getElementById("siteMapZoneMappingSection");
      const lockNote = document.getElementById("siteMapZoneMappingLockedNote");
      const zoneWrap = document.getElementById("siteMapZoneSelectWrap");
      const zoneSetupBtn = document.getElementById("siteMapZoneSetupBtn");
      const zoneWizard = document.getElementById("siteMapZoneWizard");
      if (wrap) wrap.classList.toggle("hidden", !ready);
      if (zoneWrap) zoneWrap.classList.toggle("hidden", !ready);
      if (lockNote) lockNote.style.display = ready ? "none" : "block";
      if (zoneSetupBtn) {
        zoneSetupBtn.disabled = !ready;
        zoneSetupBtn.classList.toggle("active", ready && (siteMapSetupMode === "zone-main" || siteMapSetupMode === "zone-downstream"));
        zoneSetupBtn.title = ready ? "Open Zone Setup" : "Complete Building Setup first";
      }
      if (zoneWizard) zoneWizard.classList.toggle("show", ready && (siteMapSetupMode === "zone-main" || siteMapSetupMode === "zone-downstream"));

      const zoneIds = [
        "siteMapZoneSelect", "siteMapClearPinBtn", "siteMapStartAreaBtn", "siteMapFinishAreaBtn",
        "siteMapCancelAreaBtn", "siteMapClearAreaBtn", "siteMapAreaUndoBtn", "siteMapPinColor",
        "siteMapEquipType", "siteMapRoomArea", "siteMapThermostatLoc", "siteMapValveType",
        "siteMapPumpModel", "siteMapPumpVoltage", "siteMapDesc", "siteMapAccessInstructions",
        "siteMapPumpSpeedMode", "siteMapPumpSerial", "siteMapBreakerRef", "siteMapInstalledBy",
        "siteMapInstallDate", "siteMapLastServiceDate", "siteMapPumpServiceNotes", "siteMapPhotoInput",
        "siteMapClearPhotoBtn", "siteMapApplyDetailsBtn"
      ];
      zoneIds.forEach((id) => {
        const el = document.getElementById(id);
        if (!el) return;
        if ("disabled" in el) el.disabled = !ready;
      });

      if (!ready && siteMapDrawingArea) {
        siteMapDrawingArea = false;
        siteMapDraftAreaPoints = [];
        renderSiteMapDraftArea();
      }
      if (!ready && (siteMapSetupMode === "zone-main" || siteMapSetupMode === "zone-downstream")) {
        setSiteMapSetupMode("none");
      }
      updateSiteMapWorkspaceStatus();
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
      refreshHubBranding();
    }

    function getSiteMapPin(zoneId) {
      if (!zoneId || !siteMapConfig || !siteMapConfig.pins || typeof siteMapConfig.pins !== "object") return null;
      return siteMapConfig.pins[zoneId] || null;
    }

    async function clearPinsOutsideBuildingOutline() {
      if (!siteMapConfig || typeof siteMapConfig !== "object" || !siteMapConfig.pins || typeof siteMapConfig.pins !== "object") {
        setSiteMapMsg("No saved pins found.");
        return;
      }
      const removed = [];
      Object.entries({ ...siteMapConfig.pins }).forEach(([zid, p]) => {
        const lat = Number(p && p.lat), lng = Number(p && p.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        if (!isPointInsideSavedBuildingOutline({ lat, lng })) {
          removed.push(String((p && p.label) || zid));
          delete siteMapConfig.pins[zid];
        }
      });
      if (!removed.length) {
        setSiteMapMsg("No pins outside the building outline were found.");
        return;
      }
      renderSiteMapMarkers();
      renderMainMapPreview();
      updateSiteMapWorkspaceStatus("Removed pins outside building");
      setSiteMapMsg(`Removed ${removed.length} pin${removed.length === 1 ? "" : "s"} outside the building outline. Saving map...`);
      await saveSiteMapConfig();
      setSiteMapMsg(`Removed and saved: ${removed.join(", ")}.`);
    }

    async function saveZoneSetupProgress() {
      if (!(siteMapSetupMode === "zone-main" || siteMapSetupMode === "zone-downstream")) {
        setSiteMapMsg("Zone Setup is not active.");
        return;
      }
      updateSiteMapWorkspaceStatus("Saving zone setup progress");
      await saveSiteMapConfig();
      updateSiteMapWorkspaceStatus(siteMapSetupMode === "zone-downstream" ? "Downstream zone setup mode" : "Main zone setup mode");
      setSiteMapMsg("Zone setup progress saved. Continue mapping or click Save & Finish when done.");
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
      if (siteMapDrawingBuildingOutline && siteMapEditingSavedOutlinePoints) return;
      const bo = siteMapConfig && siteMapConfig.building_outline;
      if (!bo || typeof bo !== "object") return;
      try {
        siteMapBuildingOutlineLayer = L.geoJSON(bo, {
          style: { color: "#0f172a", weight: 4, opacity: 0.95, fillColor: "#94a3b8", fillOpacity: 0.06 }
        }).addTo(siteMapMap);
      } catch (e) {
        siteMapBuildingOutlineLayer = null;
      }
    }

    function renderSiteMapDraftBuildingOutline() {
      if (!siteMapMap || !window.L) return;
      if (siteMapDraftBuildingOutlineLayer) {
        try { siteMapMap.removeLayer(siteMapDraftBuildingOutlineLayer); } catch (e) {}
        siteMapDraftBuildingOutlineLayer = null;
      }
      if (siteMapDraftBuildingOutlinePointsLayer) {
        try { siteMapMap.removeLayer(siteMapDraftBuildingOutlinePointsLayer); } catch (e) {}
        siteMapDraftBuildingOutlinePointsLayer = null;
      }
      if (!siteMapDrawingBuildingOutline || siteMapDraftBuildingOutlinePoints.length === 0) return;
      const latlngs = siteMapDraftBuildingOutlinePoints.map((p) => [p.lat, p.lng]);
      const style = { color: "#0f172a", weight: 2, dashArray: "8,5", fillColor: "#94a3b8", fillOpacity: 0.08 };
      if (latlngs.length >= 3) siteMapDraftBuildingOutlineLayer = L.polygon(latlngs, style).addTo(siteMapMap);
      else siteMapDraftBuildingOutlineLayer = L.polyline(latlngs, style).addTo(siteMapMap);
      const markers = siteMapDraftBuildingOutlinePoints.map((p, idx) =>
        L.circleMarker([Number(p.lat), Number(p.lng)], {
          radius: idx === siteMapSelectedOutlinePointIndex ? 6 : (idx === 0 ? 5 : 4),
          color: "#0f172a",
          weight: 2,
          fillColor: idx === siteMapSelectedOutlinePointIndex ? "#2563eb" : (idx === 0 ? "#f59e0b" : "#ffffff"),
          fillOpacity: 0.95,
        })
      );
      siteMapDraftBuildingOutlinePointsLayer = L.layerGroup(markers).addTo(siteMapMap);
    }

    function nearestDraftOutlinePointIndex(latlng, pxThreshold = 14) {
      if (!siteMapMap || !latlng || !siteMapDrawingBuildingOutline || !siteMapDraftBuildingOutlinePoints.length) return -1;
      const clickPt = siteMapMap.latLngToContainerPoint(latlng);
      let bestIdx = -1;
      let bestDist = Number(pxThreshold) + 0.0001;
      siteMapDraftBuildingOutlinePoints.forEach((p, idx) => {
        const pt = siteMapMap.latLngToContainerPoint([Number(p.lat), Number(p.lng)]);
        const dx = Number(pt.x) - Number(clickPt.x);
        const dy = Number(pt.y) - Number(clickPt.y);
        const d = Math.sqrt(dx * dx + dy * dy);
        if (d <= bestDist) {
          bestDist = d;
          bestIdx = idx;
        }
      });
      return bestIdx;
    }

    function clearSelectedDraftOutlinePoint() {
      siteMapSelectedOutlinePointIndex = -1;
      siteMapOutlinePointDragActive = false;
      siteMapOutlinePointDragMoved = false;
      renderSiteMapDraftBuildingOutline();
      updateOutlineWizardActionStates({
        hasAddress: !!String(document.getElementById("siteAddress")?.value || "").trim(),
        hasConfirm: !!(siteMapConfig && siteMapConfig.building_confirm_pin),
        hasOutline: !!(siteMapConfig && siteMapConfig.building_outline),
        drawing: !!siteMapDrawingBuildingOutline,
      });
    }

    function selectDraftOutlinePoint(idx) {
      if (!siteMapDrawingBuildingOutline) return false;
      if (!Number.isInteger(idx) || idx < 0 || idx >= siteMapDraftBuildingOutlinePoints.length) return false;
      siteMapSelectedOutlinePointIndex = idx;
      renderSiteMapDraftBuildingOutline();
      updateSiteMapWorkspaceStatus(`Outline point ${idx + 1} selected`);
      setSiteMapMsg("Outline point selected. Drag it to reshape the outline, or press Delete / click Delete Selected Point.");
      renderOutlineWizardSteps();
      return true;
    }

    function deleteSelectedDraftOutlinePoint() {
      if (!siteMapDrawingBuildingOutline) {
        setSiteMapMsg("Load or start outline editing first.");
        return false;
      }
      const idx = Number(siteMapSelectedOutlinePointIndex);
      if (!Number.isInteger(idx) || idx < 0 || idx >= siteMapDraftBuildingOutlinePoints.length) {
        setSiteMapMsg("Select an outline point first.");
        return false;
      }
      siteMapDraftBuildingOutlinePoints.splice(idx, 1);
      if (siteMapDraftBuildingOutlinePoints.length === 0) siteMapSelectedOutlinePointIndex = -1;
      else siteMapSelectedOutlinePointIndex = Math.min(idx, siteMapDraftBuildingOutlinePoints.length - 1);
      renderSiteMapDraftBuildingOutline();
      fitMapToLatLngPoints(siteMapDraftBuildingOutlinePoints, 0.18);
      updateSiteMapWorkspaceStatus();
      setSiteMapMsg("Selected outline point deleted. Continue editing, then click Finish Manual Outline.");
      renderOutlineWizardSteps();
      return true;
    }

    function buildingOutlineDraftPointsFromSaved() {
      const bo = siteMapConfig && siteMapConfig.building_outline;
      if (!bo || typeof bo !== "object" || String(bo.type) !== "Polygon") return [];
      const rings = Array.isArray(bo.coordinates) ? bo.coordinates : [];
      const ring = Array.isArray(rings[0]) ? rings[0] : [];
      const pts = ring
        .map((c) => (Array.isArray(c) && c.length >= 2 ? { lat: Number(c[1]), lng: Number(c[0]) } : null))
        .filter((p) => p && Number.isFinite(p.lat) && Number.isFinite(p.lng));
      if (pts.length >= 2) {
        const a = pts[0];
        const b = pts[pts.length - 1];
        if (a && b && a.lat === b.lat && a.lng === b.lng) pts.pop();
      }
      return pts;
    }

    function openSavedBuildingOutlineForEdit() {
      const savedPts = buildingOutlineDraftPointsFromSaved();
      if (!savedPts.length) {
        setSiteMapMsg("No saved building outline found to edit.");
        return false;
      }
      siteMapDrawingArea = false;
      siteMapDraftAreaPoints = [];
      renderSiteMapDraftArea();
      siteMapBuildingEditOnlyMode = !!buildingSetupComplete();
      siteMapEditingSavedOutlinePoints = true;
      siteMapSelectedOutlinePointIndex = -1;
      siteMapOutlinePointDragActive = false;
      siteMapOutlineDragMode = false;
      siteMapOutlineDragActive = false;
      siteMapDrawingBuildingOutline = true;
      siteMapDraftBuildingOutlinePoints = savedPts;
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      renderSiteMapMarkers();
      renderSiteMapDraftBuildingOutline();
      setOutlineWizardStep(3, "Saved outline loaded for editing. Adjust points, then click Finish Manual Outline.");
      updateSiteMapWorkspaceStatus("Editing saved outline points");
      setSiteMapMsg(siteMapBuildingEditOnlyMode
        ? "Editing saved building outline only. Add/remove points, then click Finish Manual Outline. You do not need to repeat the rest of building setup."
        : "Editing saved building outline points. Add/remove points, then click Finish Manual Outline.");
      if (!fitMapToLatLngPoints(siteMapDraftBuildingOutlinePoints, 0.18)) centerMapOnAddressOrDefault(19);
      return true;
    }

    function toggleBuildingOutlineDragMode(forceOn = null) {
      const next = forceOn === null ? !siteMapOutlineDragMode : !!forceOn;
      if (next) {
        if (!siteMapConfig || !siteMapConfig.building_outline) {
          setSiteMapMsg("Save a building outline first, then use Select & Drag Outline.");
          siteMapOutlineDragMode = false;
        } else {
          siteMapBuildingEditOnlyMode = !!buildingSetupComplete();
          enterSiteMapWorkspace({
            title: "Adjust Building Outline",
            hint: "Drag the saved outline over the satellite image. Press Enter or click Finish Drag Outline when done.",
            layer: "satellite",
            outlineWizard: true,
          });
          setSiteMapBaseLayer("satellite");
          siteMapDrawingBuildingOutline = false;
          siteMapDraftBuildingOutlinePoints = [];
          renderSiteMapDraftBuildingOutline();
          siteMapOutlineDragMode = true;
          siteMapOutlineDragActive = false;
          if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(19);
          if (siteMapMap) {
            try { siteMapMap.invalidateSize(); } catch (_e) {}
          }
          setOutlineWizardStep(3, "Select & Drag Outline enabled. Click and drag inside the saved outline to reposition it.");
          updateSiteMapWorkspaceStatus("Outline drag mode");
          setSiteMapMsg(siteMapBuildingEditOnlyMode
            ? "Outline drag mode enabled for building-outline edits. Drag to align, then press Enter or click Finish Drag Outline. You do not need to repeat the rest of building setup."
            : "Outline drag mode enabled. Click and drag inside the saved building outline. Press Enter or click Finish Drag Outline when done.");
        }
      } else {
      if (siteMapOutlineDragActive) stopBuildingOutlineDrag();
      siteMapEditingSavedOutlinePoints = false;
      siteMapSelectedOutlinePointIndex = -1;
      siteMapOutlineDragMode = false;
        siteMapOutlineDragActive = false;
        siteMapOutlineDragStartLatLng = null;
        siteMapOutlineDragBasePoints = [];
        updateSiteMapWorkspaceStatus();
        setSiteMapMsg(siteMapBuildingEditOnlyMode
          ? "Outline drag mode finished. Click Save & Finish to store the updated building outline."
          : "Outline drag mode finished. Click Save & Finish to store the updated outline.");
      }
      const btn = document.getElementById("siteMapOutlineDragBtn");
      if (btn) {
        btn.classList.toggle("active", !!siteMapOutlineDragMode);
        btn.textContent = siteMapOutlineDragMode ? "Finish Drag Outline (Enter)" : "Select & Drag Outline";
      }
    }

    function startBuildingOutlineDrag(latlng) {
      if (!siteMapMap || !siteMapOutlineDragMode) return false;
      const basePts = buildingOutlineDraftPointsFromSaved();
      if (basePts.length < 3) return false;
      if (!pointInPolygonLatLng(latlng, basePts)) return false;
      siteMapOutlineDragActive = true;
      siteMapOutlineDragStartLatLng = { lat: Number(latlng.lat), lng: Number(latlng.lng) };
      siteMapOutlineDragBasePoints = basePts.map((p) => ({ lat: Number(p.lat), lng: Number(p.lng) }));
      try { siteMapMap.dragging.disable(); } catch (_e) {}
      updateSiteMapWorkspaceStatus("Dragging outline");
      setSiteMapMsg("Dragging building outline. Release mouse to finish, then click Save & Finish.");
      return true;
    }

    function applyBuildingOutlineDrag(latlng) {
      if (!siteMapOutlineDragActive || !siteMapOutlineDragStartLatLng || !Array.isArray(siteMapOutlineDragBasePoints)) return false;
      const dLat = Number(latlng.lat) - Number(siteMapOutlineDragStartLatLng.lat);
      const dLng = Number(latlng.lng) - Number(siteMapOutlineDragStartLatLng.lng);
      const movedPts = siteMapOutlineDragBasePoints.map((p) => ({ lat: Number(p.lat) + dLat, lng: Number(p.lng) + dLng }));
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      if (movedPts.length >= 3) {
        const ring = movedPts.map((p) => [Number(p.lng), Number(p.lat)]);
        ring.push([Number(movedPts[0].lng), Number(movedPts[0].lat)]);
        siteMapConfig.building_outline = { type: "Polygon", coordinates: [ring] };
      }
      renderSiteMapMarkers();
      updateSiteMapWorkspaceStatus(`Dragging outline (${movedPts.length} pts)`);
      return true;
    }

    function stopBuildingOutlineDrag() {
      if (!siteMapOutlineDragActive) return;
      siteMapOutlineDragActive = false;
      siteMapOutlineDragStartLatLng = null;
      siteMapOutlineDragBasePoints = [];
      try { if (siteMapMap) siteMapMap.dragging.enable(); } catch (_e) {}
      updateSiteMapWorkspaceStatus("Outline moved");
      setSiteMapMsg("Building outline moved. Click Save & Finish to store the updated outline.");
    }

    function buildingOutlineRingLatLngs() {
      const bo = siteMapConfig && siteMapConfig.building_outline;
      if (!bo || typeof bo !== "object" || String(bo.type) !== "Polygon") return [];
      const rings = Array.isArray(bo.coordinates) ? bo.coordinates : [];
      const ring = Array.isArray(rings[0]) ? rings[0] : [];
      return ring
        .map((c) => (Array.isArray(c) && c.length >= 2 ? { lat: Number(c[1]), lng: Number(c[0]) } : null))
        .filter((p) => p && Number.isFinite(p.lat) && Number.isFinite(p.lng));
    }

    function pointInPolygonLatLng(pt, ring) {
      if (!pt || !Number.isFinite(Number(pt.lat)) || !Number.isFinite(Number(pt.lng))) return false;
      const poly = Array.isArray(ring) ? ring.filter((p) => p && Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng))) : [];
      if (poly.length < 3) return false;
      let inside = false;
      const x = Number(pt.lng), y = Number(pt.lat);
      for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
        const xi = Number(poly[i].lng), yi = Number(poly[i].lat);
        const xj = Number(poly[j].lng), yj = Number(poly[j].lat);
        const intersects = ((yi > y) !== (yj > y)) && (x < ((xj - xi) * (y - yi)) / ((yj - yi) || 1e-12) + xi);
        if (intersects) inside = !inside;
      }
      return inside;
    }

    function isPointInsideSavedBuildingOutline(latlng) {
      const ring = buildingOutlineRingLatLngs();
      if (ring.length < 3) return true; // no saved outline yet; caller may separately gate this
      return pointInPolygonLatLng(latlng, ring);
    }

    function openZoneSetupWorkspace(mode = "zone-main") {
      if (!ensureBuildingSetupReadyForZoneMapping("set up zone and equipment locations")) return;
      setSiteMapSetupMode(mode === "zone-downstream" ? "zone-downstream" : "zone-main");
      siteMapBuildingEditOnlyMode = false;
      enterSiteMapWorkspace({
        title: "Zone Setup",
        hint: "Using the saved building outline and satellite image. Place all zone pins and served areas inside the locked building outline.",
        layer: "satellite",
      });
      showOutlineWizard(false);
      const sel = document.getElementById("siteMapZoneSelect");
      if (siteMapSetupMode === "zone-main") {
        if (sel && !["zone1_main", "zone2_main"].includes(String(sel.value || ""))) sel.value = "";
      } else if (siteMapSetupMode === "zone-downstream") {
        const ds = realDownstreamRows((lastHubPayload && lastHubPayload.downstream) || []);
        if (sel && (!sel.value || isMainZoneId(String(sel.value || "")) || !ds.some((z) => String(z.id) === String(sel.value || "")))) sel.value = "";
      }
      syncSiteMapDetailsFormFromSelected();
      if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
      if (siteMapMap) {
        try { siteMapMap.invalidateSize(); } catch (_e) {}
      }
      setTimeout(() => {
        if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
        if (siteMapMap) {
          try { siteMapMap.invalidateSize(); } catch (_e) {}
        }
      }, 80);
      renderSiteMapZoneWizard();
      updateSiteMapWorkspaceStatus(siteMapSetupMode === "zone-downstream" ? "Downstream zone setup mode" : "Main zone setup mode");
      setSiteMapMsg(siteMapSetupMode === "zone-downstream"
        ? "Downstream Zone Setup: place downstream zone pins/areas inside the saved building outline."
        : "Main Zone Setup: place Zone 1/Zone 2 main boiler pins inside the saved building outline.");
    }

    function useSelectedZoneSetupDevice() {
      if (siteMapSetupMode !== "zone-downstream") return;
      const zid = selectedSiteMapZoneId();
      if (!zid || isMainZoneId(zid)) {
        setSiteMapMsg("Choose a downstream zone/device from the Zone / Device dropdown first.");
        renderSiteMapZoneWizard();
        return;
      }
      syncSiteMapDetailsFormFromSelected();
      if (!centerMapOnZonePin(zid)) {
        if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
      }
      updateSiteMapWorkspaceStatus(`Selected downstream device: ${(siteMapZoneOptions().find((o) => o.id === zid) || {}).label || zid}`);
      setSiteMapMsg("Downstream device selected. Click inside the building outline to place the pin, then draw an optional area.");
      renderSiteMapZoneWizard();
    }

    function openMainBoilerFloorPopup(zoneId) {
      if (!isMainZoneId(zoneId)) return;
      const marker = siteMapMarkers[zoneId];
      if (!marker || !window.L) return;
      const pin = getSiteMapPin(zoneId) || {};
      const floors = Math.max(1, Number((siteMapConfig && siteMapConfig.building_floors) || 1));
      const selected = String(pin.main_boiler_floor || "");
      const opts = ['<option value="">Select floor</option>'];
      for (let i = 1; i <= floors; i++) {
        opts.push(`<option value="${i}"${String(i) === selected ? " selected" : ""}>Floor ${i}</option>`);
      }
      const zoneLabel = zoneId === "zone1_main" ? "Zone 1 Main Boiler" : "Zone 2 Main Boiler";
      marker.bindPopup(
        `<div style="min-width:220px">` +
        `<div style="font-weight:600;margin-bottom:6px">${zoneLabel}</div>` +
        `<div class="smallnote" style="margin-bottom:6px">Set the floor for this main boiler/circulator pin.</div>` +
        `<label class="label" style="display:block;margin-bottom:4px">Main Boiler Floor</label>` +
        `<select id="siteMapMainBoilerFloorPopupSelect" class="select" style="width:100%;margin-bottom:8px">${opts.join("")}</select>` +
        `<div style="display:flex;gap:8px;justify-content:flex-end">` +
        `<button type="button" class="btn" id="siteMapMainBoilerFloorPopupCancelBtn">Cancel</button>` +
        `<button type="button" class="btn btn-primary" id="siteMapMainBoilerFloorPopupSaveBtn">Save Floor</button>` +
        `</div>` +
        `</div>`
      );
      siteMapMainBoilerFloorPopupZoneId = zoneId;
      marker.openPopup();
      setTimeout(() => {
        const sel = document.getElementById("siteMapMainBoilerFloorPopupSelect");
        const saveBtn = document.getElementById("siteMapMainBoilerFloorPopupSaveBtn");
        const cancelBtn = document.getElementById("siteMapMainBoilerFloorPopupCancelBtn");
        if (sel) {
          try { sel.focus({ preventScroll: true }); } catch (_e) { try { sel.focus(); } catch (_e2) {} }
        }
        if (cancelBtn) cancelBtn.onclick = () => { try { marker.closePopup(); } catch (_e) {} };
        if (saveBtn) {
          saveBtn.onclick = () => {
            if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
            if (!siteMapConfig.pins || typeof siteMapConfig.pins !== "object") siteMapConfig.pins = {};
            const existing = siteMapConfig.pins[zoneId] || getSiteMapPin(zoneId) || {};
            const value = String(sel?.value || "").trim();
            existing.main_boiler_floor = value;
            existing.updated_utc = new Date().toISOString();
            siteMapConfig.pins[zoneId] = existing;
            renderSiteMapMarkers();
            syncSiteMapDetailsFormFromSelected();
            setSiteMapMsg(value ? `${zoneLabel} floor set to Floor ${value}. Click Save & Finish to store.` : `${zoneLabel} floor cleared. Click Save & Finish to store.`);
          };
        }
      }, 40);
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
      const preferredColor = preferredPinColorForZone(zoneId);
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
      if (colorEl) {
        colorEl.value = preferredColor || pin.color || "red";
        colorEl.disabled = true;
      }
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
      updateSiteMapWorkspaceStatus();
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
        const floorText = (isMainZoneId(zid) && p.main_boiler_floor) ? ` · Floor ${String(p.main_boiler_floor)}` : "";
        row.innerHTML = `<div class="t"><span class="pin-dot" style="background:${pinColorHex(p.color)}"></span>${labels[zid] || p.label || zid}</div><div class="s">${Number(p.lat).toFixed(5)}, ${Number(p.lng).toFixed(5)}${floorText}${p.room_area_name ? ` · ${p.room_area_name}` : ""}${p.equipment_type ? ` · ${p.equipment_type}` : ""}</div>`;
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

    function renderSiteMapBuildingConfirmPin() {
      if (!siteMapMap || !window.L) return;
      if (siteMapBuildingConfirmMarker) {
        try { siteMapMap.removeLayer(siteMapBuildingConfirmMarker); } catch (e) {}
        siteMapBuildingConfirmMarker = null;
      }
      const p = siteMapConfig && siteMapConfig.building_confirm_pin;
      if (!p || typeof p !== "object") return;
      const lat = Number(p.lat), lng = Number(p.lng);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
      siteMapBuildingConfirmMarker = L.circleMarker([lat, lng], {
        radius: 10,
        color: "#0f172a",
        weight: 2,
        fillColor: "#f59e0b",
        fillOpacity: 0.85,
      }).addTo(siteMapMap);
      siteMapBuildingConfirmMarker.bindPopup("<b>Confirmed Building</b><br>Address confirmation pin");
    }

    function renderSiteMapMarkers() {
      if (!siteMapMap || !window.L) return;
      Object.values(siteMapMarkers).forEach((m) => { try { siteMapMap.removeLayer(m); } catch (e) {} });
      siteMapMarkers = {};
      const pins = (siteMapConfig && siteMapConfig.pins && typeof siteMapConfig.pins === "object") ? siteMapConfig.pins : {};
      const labels = {};
      siteMapZoneOptions().forEach((o) => { labels[o.id] = o.label; });
      Object.entries(pins).forEach(([zid, p]) => {
        const isMain = isMainZoneId(zid);
        const marker = L.circleMarker([Number(p.lat), Number(p.lng)], {
          radius: isMain ? 10 : 8,
          color: isMain ? "#111827" : pinColorHex(p.color),
          weight: isMain ? 3 : 2,
          fillColor: pinColorHex(p.color),
          fillOpacity: isMain ? 0.95 : 0.85,
        }).addTo(siteMapMap);
        const mainFloorLine = (isMainZoneId(zid) && p.main_boiler_floor) ? `<br>Main Boiler Floor: ${String(p.main_boiler_floor)}` : "";
        marker.bindPopup(`<b>${labels[zid] || p.label || zid}</b>${p.equipment_type ? `<br>${p.equipment_type}` : ""}${mainFloorLine}`);
        siteMapMarkers[zid] = marker;
      });
      renderSiteMapBuildingOutline();
      renderSiteMapBuildingConfirmPin();
      renderSiteMapDraftBuildingOutline();
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
      siteMapMap = L.map("siteMapCanvas", { doubleClickZoom: false });
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
        if (siteMapDrawingBuildingOutline && siteMapSuppressNextOutlineClick) {
          siteMapSuppressNextOutlineClick = false;
          return;
        }
        if (siteMapOutlineDragMode) {
          return;
        }
        if (
          siteMapOutlineWizardOpen &&
          Number(siteMapOutlineWizardStep || 1) === 3 &&
          !siteMapDrawingBuildingOutline &&
          !siteMapDrawingArea &&
          !(siteMapConfig && siteMapConfig.building_outline)
        ) {
          siteMapDrawingArea = false;
          siteMapDraftAreaPoints = [];
          renderSiteMapDraftArea();
          siteMapDrawingBuildingOutline = true;
          siteMapDraftBuildingOutlinePoints = [];
          setSiteMapBaseLayer("satellite");
          renderSiteMapDraftBuildingOutline();
          updateSiteMapWorkspaceStatus("Outline tracing started");
          setSiteMapMsg("Outline tracing started. Click around the building corners to place outline points.");
        }
        if (siteMapDrawingBuildingOutline) {
          if (siteMapEditingSavedOutlinePoints) {
            const hitIdx = nearestDraftOutlinePointIndex(e.latlng, 16);
            if (hitIdx >= 0) {
              selectDraftOutlinePoint(hitIdx);
              return;
            }
          }
          siteMapSelectedOutlinePointIndex = -1;
          siteMapDraftBuildingOutlinePoints.push({ lat: e.latlng.lat, lng: e.latlng.lng });
          renderSiteMapDraftBuildingOutline();
          updateSiteMapWorkspaceStatus();
          setOutlineWizardStep(3, `Manual outline point ${siteMapDraftBuildingOutlinePoints.length} added. Add at least 3 points, then click Finish Manual Outline.`, { skipAssist: true });
          setSiteMapMsg(`Building outline point ${siteMapDraftBuildingOutlinePoints.length} added. Finish Manual Outline when complete.`);
          return;
        }
        if (
          siteMapOutlineWizardOpen &&
          Number(siteMapOutlineWizardStep || 1) <= 2 &&
          !siteMapDrawingArea &&
          !siteMapDrawingBuildingOutline &&
          String((siteMapConfig && siteMapConfig.address) || "").trim()
        ) {
          if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
          siteMapConfig.building_confirm_pin = { lat: Number(e.latlng.lat), lng: Number(e.latlng.lng) };
          renderSiteMapMarkers();
          setOutlineWizardStep(3, "Building confirmed. Next: auto detect the outline or start drawing the building boundary.", { skipAssist: true });
          updateSiteMapWorkspaceStatus("Building confirmed");
          setSiteMapMsg("Building confirmed. Start the outline step next.");
          return;
        }
        const sel = document.getElementById("siteMapZoneSelect");
        const zoneId = sel ? sel.value : "";
        if (!zoneId) { setSiteMapMsg("Select a zone/device first."); return; }
        if (siteMapDrawingArea) {
          if (!isPointInsideSavedBuildingOutline(e.latlng)) {
            setSiteMapMsg("Zone area points must be inside the saved building outline.");
            return;
          }
          siteMapDraftAreaPoints.push({ lat: e.latlng.lat, lng: e.latlng.lng });
          renderSiteMapDraftArea();
          fitMapToLatLngPoints(siteMapDraftAreaPoints, 0.2);
          updateSiteMapWorkspaceStatus();
          setSiteMapMsg(`Zone area point ${siteMapDraftAreaPoints.length} added. Add at least 3 points, then click Finish Area.`);
          return;
        }
        if (!ensureBuildingSetupReadyForZoneMapping("place zone and equipment pins")) return;
        if (!isPointInsideSavedBuildingOutline(e.latlng)) {
          setSiteMapMsg("Pins must be placed inside the saved building outline.");
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
          color: preferredPinColorForZone(zoneId),
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
          main_boiler_floor: (getSiteMapPin(zoneId) || {}).main_boiler_floor || "",
          circuit_breaker_ref: (getSiteMapPin(zoneId) || {}).circuit_breaker_ref || "",
          description: (getSiteMapPin(zoneId) || {}).description || "",
          photo_data_url: (getSiteMapPin(zoneId) || {}).photo_data_url || "",
          polygon: Array.isArray((getSiteMapPin(zoneId) || {}).polygon) ? (getSiteMapPin(zoneId) || {}).polygon : [],
          updated_utc: new Date().toISOString()
        };
        snapshotSiteMapViewport();
        renderSiteMapMarkers();
        if (isMainZoneId(zoneId)) {
          openMainBoilerFloorPopup(zoneId);
          setSiteMapMsg(`Placed pin for ${labels[zoneId] || zoneId}. Set the floor in the map popup, then click Save & Finish.`);
        } else {
          setSiteMapMsg(`Placed pin for ${labels[zoneId] || zoneId}. Click Save & Finish to store.`);
        }
      });
      siteMapMap.on("mousedown", (e) => {
        if (siteMapDrawingBuildingOutline && siteMapEditingSavedOutlinePoints) {
          const selectedIdx = Number(siteMapSelectedOutlinePointIndex);
          const hitIdx = nearestDraftOutlinePointIndex(e.latlng, 18);
          if (hitIdx >= 0) {
            siteMapSelectedOutlinePointIndex = hitIdx;
            renderSiteMapDraftBuildingOutline();
            if (hitIdx === selectedIdx || selectedIdx === -1) {
              siteMapOutlinePointDragActive = true;
              siteMapOutlinePointDragMoved = false;
              try { siteMapMap.dragging.disable(); } catch (_e) {}
              updateSiteMapWorkspaceStatus(`Dragging outline point ${hitIdx + 1}`);
              setSiteMapMsg("Dragging outline point. Release to finish, or press Delete to remove selected point.");
              return;
            }
          }
        }
        if (!siteMapOutlineDragMode || siteMapDrawingBuildingOutline || siteMapDrawingArea) return;
        startBuildingOutlineDrag(e.latlng);
      });
      siteMapMap.on("mousemove", (e) => {
        if (siteMapOutlinePointDragActive && siteMapDrawingBuildingOutline) {
          const idx = Number(siteMapSelectedOutlinePointIndex);
          if (Number.isInteger(idx) && idx >= 0 && idx < siteMapDraftBuildingOutlinePoints.length) {
            siteMapDraftBuildingOutlinePoints[idx] = { lat: Number(e.latlng.lat), lng: Number(e.latlng.lng) };
            siteMapOutlinePointDragMoved = true;
            renderSiteMapDraftBuildingOutline();
            updateSiteMapWorkspaceStatus(`Dragging outline point ${idx + 1}`);
          }
          return;
        }
        if (!siteMapOutlineDragActive) return;
        applyBuildingOutlineDrag(e.latlng);
      });
      siteMapMap.on("mouseup", () => {
        if (siteMapOutlinePointDragActive) {
          siteMapOutlinePointDragActive = false;
          try { if (siteMapMap) siteMapMap.dragging.enable(); } catch (_e) {}
          if (siteMapOutlinePointDragMoved) {
            siteMapSuppressNextOutlineClick = true;
            fitMapToLatLngPoints(siteMapDraftBuildingOutlinePoints, 0.18);
            updateSiteMapWorkspaceStatus("Outline point moved");
            setSiteMapMsg("Outline point moved. Continue editing, then click Finish Manual Outline.");
          }
          siteMapOutlinePointDragMoved = false;
          return;
        }
        stopBuildingOutlineDrag();
      });
      siteMapMap.on("mouseout", () => {
        if (siteMapOutlinePointDragActive) {
          siteMapOutlinePointDragActive = false;
          try { if (siteMapMap) siteMapMap.dragging.enable(); } catch (_e) {}
          siteMapOutlinePointDragMoved = false;
        }
        if (siteMapOutlineDragActive) stopBuildingOutlineDrag();
      });
      siteMapMap.on("dblclick", (e) => {
        if (siteMapOutlineDragMode) {
          if (e && e.originalEvent) {
            try { e.originalEvent.preventDefault(); } catch (_err) {}
          }
          return;
        }
        if (siteMapDrawingBuildingOutline) {
          if (e && e.originalEvent) {
            try { e.originalEvent.preventDefault(); } catch (_err) {}
          }
          if (siteMapDraftBuildingOutlinePoints.length >= 3) {
            finishBuildingOutlineDraw();
          } else {
            setSiteMapMsg("Add at least 3 points before finishing the building outline.");
          }
          return;
        }
        if (siteMapDrawingArea) {
          if (e && e.originalEvent) {
            try { e.originalEvent.preventDefault(); } catch (_err) {}
          }
          if (siteMapDraftAreaPoints.length >= 3) {
            finishZoneAreaDraw();
          } else {
            setSiteMapMsg("Add at least 3 points before finishing the zone area.");
          }
        }
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
        updateSiteMapZoneMappingAvailability();
        renderMainMapPreview();
      } catch (e) {
        setSiteMapMsg("Failed to load site map.");
      }
    }

    async function saveSiteMapConfig() {
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      siteMapConfig.address = document.getElementById("siteAddress").value.trim();
      siteMapConfig.map_layer = document.getElementById("siteMapLayer")?.value || "street";
      const invalidPinLabels = [];
      const pinsToValidate = (siteMapConfig && siteMapConfig.pins && typeof siteMapConfig.pins === "object") ? siteMapConfig.pins : {};
      const zoneLabelMap = {};
      siteMapZoneOptions().forEach((o) => { zoneLabelMap[o.id] = o.label; });
      Object.entries(pinsToValidate).forEach(([zid, p]) => {
        const lat = Number(p && p.lat), lng = Number(p && p.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        if (!isPointInsideSavedBuildingOutline({ lat, lng })) {
          invalidPinLabels.push(String((p && p.label) || zoneLabelMap[zid] || zid));
        }
      });
      if (invalidPinLabels.length) {
        setSiteMapMsg(`Cannot save map: ${invalidPinLabels.join(", ")} ${invalidPinLabels.length === 1 ? "is" : "are"} outside the saved building outline. Move ${invalidPinLabels.length === 1 ? "it" : "them"} inside the building outline first.`);
        return;
      }
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
        updateSiteMapZoneMappingAvailability();
        renderMainMapPreview();
        setSiteMapMsg("Map saved.");
      } catch (e) {
        setSiteMapMsg("Save failed: " + e.message);
      }
    }

    async function geocodeSiteAddress() {
      enterSiteMapWorkspace({
        title: "Building Setup - Find Address",
        hint: "Step 1: Find the building address. Then click the map to confirm the building.",
        layer: "street",
        outlineWizard: true,
      });
      setOutlineWizardStep(1, "Finding address and centering the map...", { skipAssist: true });
      const q = buildSiteAddressQueryFromFields() || String(document.getElementById("siteAddress")?.value || "").trim();
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
        siteMapConfig.building_confirm_pin = null;
        applySiteMapStateToForm();
        ensureSiteMapReady();
        if (siteMapMap) siteMapMap.setView([siteMapConfig.center.lat, siteMapConfig.center.lng], Number(siteMapConfig.zoom || 16));
        renderSiteMapMarkers();
        setOutlineWizardStep(2, "Address found. Step 2: click the building on the map to confirm the correct structure.", { skipAssist: true });
        setTimeout(() => {
          if (!siteMapMap) return;
          try { siteMapMap.invalidateSize(); } catch (_e) {}
          scrollSiteMapWorkspaceToCanvas({ behavior: "smooth", pad: 8 });
          const canvas = document.getElementById("siteMapCanvas");
          if (canvas) focusNoScroll(canvas);
        }, 60);
        setSiteMapMsg("Address found. Next: click the building on the map to confirm it, then trace the outline.");
      } catch (e) {
        setSiteMapMsg("Address lookup failed: " + e.message);
      }
    }

    async function loadBuildingOutline() {
      enterSiteMapWorkspace({
        title: "Building Outline Tool",
        hint: "Load the building outline, review it on the map, then Save & Finish.",
        layer: "street",
        outlineWizard: true,
      });
      setOutlineWizardStep(3, "Trying to auto-detect the building outline from map data...");
      const q = buildSiteAddressQueryFromFields() || String(document.getElementById("siteAddress")?.value || "").trim();
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
        centerMapOnBuildingOutline();
        setOutlineWizardStep(3, "Building outline loaded. Review it on the map, then confirm to continue.");
        setSiteMapMsg("Building outline loaded. Review it on the map.");
        confirmBuildingOutlineAndProceed("auto-detected outline");
      } catch (e) {
        setOutlineWizardStep(3, "Auto-detect did not return an outline. Use Start Manual Outline to draw the building boundary.");
        setSiteMapMsg("Building outline lookup failed: " + e.message);
      }
    }

    function openBuildingOutlineWizard() {
      ensureSiteMapReady();
      siteMapBuildingEditOnlyMode = false;
      setSiteMapSetupMode("building");
      const hasSavedOutline = !!(siteMapConfig && siteMapConfig.building_outline);
      enterSiteMapWorkspace({
        title: "Building Outline Setup",
        hint: "Follow the steps to define the building boundary, then Save & Finish.",
        layer: hasSavedOutline ? "satellite" : "street",
        outlineWizard: true,
      });
      setOutlineWizardStep(1, "Step 1: Find the building address, then click the map to confirm the building. Press Enter to continue.");
      setTimeout(() => {
        if (!siteMapMap) return;
        if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
        try { siteMapMap.invalidateSize(); } catch (_e) {}
        const canvas = document.getElementById("siteMapCanvas");
        if (canvas) {
          scrollSiteMapWorkspaceToCanvas({ behavior: "smooth", pad: 8 });
          focusNoScroll(canvas);
        }
      }, 120);
    }

    function exitBuildingSetupMode() {
      toggleBuildingOutlineDragMode(false);
      siteMapEditingSavedOutlinePoints = false;
      siteMapSelectedOutlinePointIndex = -1;
      siteMapOutlinePointDragActive = false;
      if (siteMapDrawingBuildingOutline) {
        siteMapDrawingBuildingOutline = false;
        siteMapDraftBuildingOutlinePoints = [];
        renderSiteMapDraftBuildingOutline();
      }
      showOutlineWizard(false);
      setSiteMapSetupMode("none");
      exitSiteMapWorkspace();
      setSiteMapMsg("Exited Building Setup mode.");
    }

    function startBuildingOutlineDraw() {
      toggleBuildingOutlineDragMode(false);
      siteMapEditingSavedOutlinePoints = false;
      siteMapSelectedOutlinePointIndex = -1;
      siteMapOutlinePointDragActive = false;
      enterSiteMapWorkspace({
        title: "Draw Building Outline",
        hint: "Tap around the building perimeter, then click Finish Manual Outline.",
        layer: "satellite",
        outlineWizard: true,
      });
      siteMapDrawingArea = false;
      siteMapDraftAreaPoints = [];
      renderSiteMapDraftArea();
      siteMapDrawingBuildingOutline = true;
      siteMapDraftBuildingOutlinePoints = [];
      renderSiteMapDraftBuildingOutline();
      if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(19);
      if (siteMapMap) siteMapMap.setZoom(Math.max(19, Number(siteMapMap.getZoom() || 19)));
      setOutlineWizardStep(3, "Manual outline drawing started. Tap the building perimeter points on the map.");
      updateSiteMapWorkspaceStatus();
    }

    function finishBuildingOutlineDraw() {
      if (siteMapDraftBuildingOutlinePoints.length < 3) {
        setOutlineWizardStep(3, "Add at least 3 points to create the building outline.");
        setSiteMapMsg("Add at least 3 points to create the building outline.");
        return;
      }
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      const coords = siteMapDraftBuildingOutlinePoints.map((p) => [Number(p.lng), Number(p.lat)]);
      const first = coords[0];
      const last = coords[coords.length - 1];
      if (!first || !last || first[0] !== last[0] || first[1] !== last[1]) coords.push([first[0], first[1]]);
      siteMapConfig.building_outline = { type: "Polygon", coordinates: [coords] };
      toggleBuildingOutlineDragMode(false);
      siteMapEditingSavedOutlinePoints = false;
      siteMapSelectedOutlinePointIndex = -1;
      siteMapOutlinePointDragActive = false;
      siteMapDrawingBuildingOutline = false;
      siteMapDraftBuildingOutlinePoints = [];
      renderSiteMapMarkers();
      centerMapOnBuildingOutline();
      updateSiteMapWorkspaceStatus();
      setSiteMapMsg("Manual building outline drawn. Review it on the map.");
      confirmBuildingOutlineAndProceed("manual outline");
    }

    function undoBuildingOutlinePoint() {
      if (!siteMapDrawingBuildingOutline) {
        const canReopenSaved = !!(siteMapOutlineWizardOpen && Number(siteMapOutlineWizardStep || 1) === 3 && siteMapConfig && siteMapConfig.building_outline);
        if (!canReopenSaved) {
          setSiteMapMsg("Start Manual Outline first to undo outline points.");
          return;
        }
        const savedPts = buildingOutlineDraftPointsFromSaved();
        if (!savedPts.length) {
          setSiteMapMsg("No saved building outline points to edit.");
          return;
        }
        siteMapDrawingBuildingOutline = true;
        siteMapEditingSavedOutlinePoints = true;
        siteMapDraftBuildingOutlinePoints = savedPts;
        siteMapSelectedOutlinePointIndex = -1;
        renderSiteMapMarkers();
        updateSiteMapWorkspaceStatus("Outline unlocked for edit");
      }
      if (!siteMapDraftBuildingOutlinePoints.length) {
        setSiteMapMsg("No outline points to undo.");
        return;
      }
      siteMapDraftBuildingOutlinePoints.pop();
      renderSiteMapDraftBuildingOutline();
      fitMapToLatLngPoints(siteMapDraftBuildingOutlinePoints, 0.18);
      updateSiteMapWorkspaceStatus();
      setOutlineWizardStep(3, siteMapDraftBuildingOutlinePoints.length ? `Outline point removed. ${siteMapDraftBuildingOutlinePoints.length} point${siteMapDraftBuildingOutlinePoints.length === 1 ? "" : "s"} remaining.` : "Outline points cleared. Tap the map to start drawing again.");
      setSiteMapMsg(siteMapDraftBuildingOutlinePoints.length ? "Removed last building outline point." : "Building outline draft cleared.");
    }

    function clearBuildingOutline() {
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      toggleBuildingOutlineDragMode(false);
      siteMapEditingSavedOutlinePoints = false;
      siteMapSelectedOutlinePointIndex = -1;
      siteMapOutlinePointDragActive = false;
      siteMapConfig.building_outline = null;
      siteMapDrawingBuildingOutline = false;
      siteMapDraftBuildingOutlinePoints = [];
      renderSiteMapMarkers();
      renderSiteMapDraftBuildingOutline();
      setOutlineWizardStep(3, "Building outline cleared. Use Auto Detect or Start Manual Outline.");
      updateSiteMapZoneMappingAvailability();
      updateSiteMapWorkspaceStatus("Outline cleared");
      setSiteMapMsg("Building outline cleared. Click Save & Finish to store.");
    }

    function startZoneAreaDraw() {
      if (!ensureBuildingSetupReadyForZoneMapping("draw zone areas")) return;
      if (!siteMapOutlineWizardOpen) setSiteMapSetupMode("zone-downstream");
      enterSiteMapWorkspace({
        title: "Draw Zone Area",
        hint: "Tap the map to add points, then click Finish Area and Save & Finish.",
        layer: "satellite",
      });
      const zoneId = selectedSiteMapZoneId();
      if (!zoneId) { setSiteMapMsg("Select a zone/device first."); return; }
      if (isMainZoneId(zoneId)) {
        setSiteMapMsg("Main boiler zones use a location pin only. Use 'Select Zone 1 Main Boiler' or 'Select Zone 2 Main Boiler' to place the main boiler pin.");
        selectMainBoilerZone(zoneId, { preserveStep: true });
        return;
      }
      siteMapDrawingArea = true;
      siteMapDraftAreaPoints = [];
      renderSiteMapDraftArea();
      if (!centerMapOnSelectedZoneArea()) {
        if (!centerMapOnZonePin(zoneId)) centerMapOnAddressOrDefault(19);
        if (siteMapMap) siteMapMap.setZoom(Math.max(19, Number(siteMapMap.getZoom() || 19)));
      }
      updateSiteMapWorkspaceStatus();
      setSiteMapMsg("Zone area drawing started. Tap the map to add points, then click Finish Area.");
    }

    function cancelZoneAreaDraw() {
      siteMapDrawingArea = false;
      siteMapDraftAreaPoints = [];
      renderSiteMapDraftArea();
      centerMapOnSelectedZoneArea();
      updateSiteMapWorkspaceStatus();
      setSiteMapMsg("Zone area drawing canceled.");
    }

    function undoZoneAreaPoint() {
      if (!siteMapDrawingArea) {
        setSiteMapMsg("Start Zone Area first to undo area points.");
        return;
      }
      if (!siteMapDraftAreaPoints.length) {
        setSiteMapMsg("No zone area points to undo.");
        return;
      }
      siteMapDraftAreaPoints.pop();
      renderSiteMapDraftArea();
      fitMapToLatLngPoints(siteMapDraftAreaPoints, 0.2);
      updateSiteMapWorkspaceStatus();
      setSiteMapMsg(siteMapDraftAreaPoints.length ? "Removed last zone area point." : "Zone area draft cleared.");
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
      centerMapOnSelectedZoneArea();
      updateSiteMapWorkspaceStatus();
      setSiteMapMsg("Zone area saved for selected zone/device. Click Save & Finish to store.");
    }

    function clearSelectedZoneArea() {
      const zoneId = selectedSiteMapZoneId();
      if (!zoneId || !siteMapConfig || !siteMapConfig.pins || !siteMapConfig.pins[zoneId]) {
        setSiteMapMsg("No selected zone area to clear.");
        return;
      }
      delete siteMapConfig.pins[zoneId].polygon;
      renderSiteMapMarkers();
      updateSiteMapWorkspaceStatus("Area cleared");
      setSiteMapMsg("Selected zone area removed. Click Save & Finish to store.");
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
      setSiteMapMsg("Selected pin removed. Click Save & Finish to store.");
    }

    function applySelectedSiteMapDetails() {
      const zoneId = selectedSiteMapZoneId();
      if (!zoneId) { setSiteMapMsg("Select a zone/device first."); return; }
      if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
      if (!siteMapConfig.pins || typeof siteMapConfig.pins !== "object") siteMapConfig.pins = {};
      const pin = siteMapConfig.pins[zoneId] || { lat: null, lng: null, label: zoneId };
      pin.label = (siteMapZoneOptions().find((o) => o.id === zoneId) || {}).label || pin.label || zoneId;
      pin.color = preferredPinColorForZone(zoneId);
      pin.equipment_type = document.getElementById("siteMapEquipType").value.trim();
      pin.room_area_name = document.getElementById("siteMapRoomArea").value.trim();
      pin.thermostat_location = document.getElementById("siteMapThermostatLoc").value.trim();
      pin.valve_actuator_type = document.getElementById("siteMapValveType").value.trim();
      pin.pump_model = (document.getElementById("siteMapPumpModel")?.value || "").trim();
      pin.pump_voltage = (document.getElementById("siteMapPumpVoltage")?.value || "").trim();
      pin.pump_speed_mode = (document.getElementById("siteMapPumpSpeedMode")?.value || "").trim();
      pin.pump_serial = (document.getElementById("siteMapPumpSerial")?.value || "").trim();
      pin.main_boiler_floor = String(pin.main_boiler_floor || "");
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
        : "Details applied to selected zone/device. Click Save & Finish to store.");
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
      document.getElementById("zoneInfoMainBoilerFloor").textContent = (mainZone && pin.main_boiler_floor) ? `Floor ${pin.main_boiler_floor}` : "Not set";
      document.getElementById("zoneInfoThermostatRow").classList.toggle("hidden", mainZone);
      document.getElementById("zoneInfoValveRow").classList.toggle("hidden", mainZone);
      document.getElementById("zoneInfoPumpModelRow").classList.toggle("hidden", !mainZone);
      document.getElementById("zoneInfoPumpVoltageRow").classList.toggle("hidden", !mainZone);
      document.getElementById("zoneInfoPumpSpeedModeRow").classList.toggle("hidden", !mainZone);
      document.getElementById("zoneInfoPumpSerialRow").classList.toggle("hidden", !mainZone);
      document.getElementById("zoneInfoMainBoilerFloorRow").classList.toggle("hidden", !mainZone);
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
      document.querySelectorAll(".btn[data-range]").forEach((btn) => {
        btn.addEventListener("click", () => {
          activeRange = btn.dataset.range;
          document.querySelectorAll(".btn[data-range]").forEach((b) => b.classList.remove("active"));
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
    function setAdoptMsg(msg) {
      const el = document.getElementById("adoptMsg");
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
      document.getElementById("adminAdoptPanel").classList.toggle("hidden", which !== "adopt");
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
      if (which === "adopt") {
        loadAdoptDevices();
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
    function adminPanelIdForTab(which) {
      const m = {
        users: "adminUsersPanel",
        alerts: "adminAlertsPanel",
        adopt: "adminAdoptPanel",
        zones: "adminZonesPanel",
        commissioning: "adminCommissioningPanel",
        backup: "adminBackupPanel",
        map: "adminMapPanel",
        setup: "adminSetupPanel",
      };
      return m[String(which || "")] || "";
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

    let adoptDevicesCache = [];
    let adoptSelectedBaseUrl = "";

    function slugifyClient(text) {
      return String(text || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || `zone-${Math.random().toString(16).slice(2, 8)}`;
    }

    function clearAdoptForm() {
      adoptSelectedBaseUrl = "";
      const ids = ["adoptBaseUrl","adoptName","adoptZoneId","adoptWifiSsid","adoptWifiPassword","adoptHubUrl"];
      ids.forEach((id) => { const el = document.getElementById(id); if (el) el.value = ""; });
      const p = document.getElementById("adoptParentZone"); if (p) p.value = "Zone 1";
      const d = document.getElementById("adoptDeltaOk"); if (d) d.value = "8";
      const lt = document.getElementById("adoptLowTempThreshold"); if (lt) lt.value = "35";
      const le = document.getElementById("adoptLowTempEnabled"); if (le) le.value = "true";
      const cs = document.getElementById("adoptCallSensingEnabled"); if (cs) cs.value = "true";
      const sp = document.getElementById("adoptSupplyProbe"); if (sp) sp.innerHTML = '<option value="">Load device setup first</option>';
      const rp = document.getElementById("adoptReturnProbe"); if (rp) rp.innerHTML = '<option value="">Load device setup first</option>';
      const ok = document.getElementById("adoptConfirm"); if (ok) { ok.classList.add("hidden"); ok.textContent = ""; }
      const saveBtn = document.getElementById("adoptSaveBtn");
      if (saveBtn) {
        delete saveBtn.dataset.selectedBase;
        delete saveBtn.dataset.passwordSaved;
      }
    }

    function fillProbeSelect(selectId, choices, selectedValue) {
      const sel = document.getElementById(selectId);
      if (!sel) return;
      sel.innerHTML = "";
      if (!Array.isArray(choices) || !choices.length) {
        sel.innerHTML = '<option value="">No probes detected</option>';
        return;
      }
      const blank = document.createElement("option");
      blank.value = "";
      blank.textContent = "-- Select Probe --";
      sel.appendChild(blank);
      choices.forEach((p, idx) => {
        const sid = String((p && p.sensor_id) || "");
        if (!sid) return;
        const tf = (p && p.temp_f != null && Number.isFinite(Number(p.temp_f))) ? `${Number(p.temp_f).toFixed(1)}F` : "--";
        const err = (p && p.error) ? ` • ${p.error}` : "";
        const op = document.createElement("option");
        op.value = sid;
        op.textContent = `Temp Probe ${idx + 1} (${tf})${err}`;
        sel.appendChild(op);
      });
      if (selectedValue && Array.from(sel.options).some((o) => o.value === selectedValue)) sel.value = selectedValue;
    }

    async function loadAdoptDeviceSetup(baseOverride) {
      const base = (baseOverride || document.getElementById("adoptBaseUrl").value || "").trim();
      if (!base) { setAdoptMsg("Select a discovered device first."); return; }
      setAdoptMsg("Loading device setup...");
      try {
        const res = await fetch(`/api/device/setup-state?base_url=${encodeURIComponent(base)}&_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "setup load failed");
        const setup = (data.setup_state && data.setup_state.config) || {};
        const alerts = (setup && setup.alerts) || {};
        const wifi = (setup && setup.wifi) || {};
        const hub = (setup && setup.hub) || {};
        fillProbeSelect("adoptSupplyProbe", data.probe_choices || [], setup.feed_sensor_id || "");
        fillProbeSelect("adoptReturnProbe", data.probe_choices || [], setup.return_sensor_id || "");
        document.getElementById("adoptWifiSsid").value = wifi.ssid || "";
        document.getElementById("adoptHubUrl").value = hub.hub_url || window.location.origin;
        document.getElementById("adoptLowTempThreshold").value = (alerts.low_temp_threshold_f ?? 35);
        document.getElementById("adoptLowTempEnabled").value = (alerts.low_temp_enabled === false) ? "false" : "true";
        document.getElementById("adoptCallSensingEnabled").value = (alerts.call_sensing_enabled === false) ? "false" : "true";
        const pw = document.getElementById("adoptWifiPassword");
        if (pw) pw.value = "";
        const saveBtn = document.getElementById("adoptSaveBtn");
        if (saveBtn) saveBtn.dataset.passwordSaved = (wifi.password_saved === true) ? "true" : "false";
        setAdoptMsg("Device setup loaded. Fill required fields and click Adopt Device.");
      } catch (e) {
        setAdoptMsg("Setup load failed: " + e.message);
      }
    }

    function fillAdoptFormFromDevice(dev) {
      if (!dev) return;
      const base = dev.base_url || "";
      adoptSelectedBaseUrl = base;
      const name = dev.unit_name || dev.adopted_name || "";
      document.getElementById("adoptBaseUrl").value = base;
      document.getElementById("adoptName").value = name;
      document.getElementById("adoptParentZone").value = dev.parent_zone || "Zone 1";
      const suggested = dev.zone_id || slugifyClient(`${dev.parent_zone || "zone1"}-${name || dev.ip || "device"}`);
      document.getElementById("adoptZoneId").value = suggested;
      document.getElementById("adoptDeltaOk").value = "8";
      document.getElementById("adoptSaveBtn").dataset.selectedBase = base;
      setAdoptMsg(`Device selected: ${name || dev.ip || "device"}. Device URL is locked. Continue below to finish adoption.`);
      loadAdoptDeviceSetup(base);
    }

    function renderAdoptDevices(devices) {
      adoptDevicesCache = Array.isArray(devices) ? devices : [];
      const quick = document.getElementById("adoptDetectedButtons");
      if (quick) {
        quick.innerHTML = "";
        if (adoptDevicesCache.length) {
          adoptDevicesCache.forEach((d) => {
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "btn";
            btn.style.padding = "4px 10px";
            const label = (d.unit_name || d.ip || "Device");
            btn.textContent = d.adopted ? `${label} (Adopted)` : label;
            if (d.adopted) {
              btn.disabled = true;
              btn.style.opacity = "0.6";
            } else {
              btn.addEventListener("click", () => {
                fillAdoptFormFromDevice(d);
                const panel = document.getElementById("adoptBaseUrl");
                if (panel && typeof panel.scrollIntoView === "function") panel.scrollIntoView({ behavior: "smooth", block: "center" });
                renderAdoptDevices(adoptDevicesCache);
              });
            }
            if ((d.base_url || "") === adoptSelectedBaseUrl) {
              btn.classList.add("active");
            }
            quick.appendChild(btn);
          });
        }
      }
      const body = document.getElementById("adoptDevicesBody");
      if (!body) return;
      body.innerHTML = "";
      if (!adoptDevicesCache.length) {
        body.innerHTML = '<tr><td colspan="7">No zone nodes found on Tailscale yet.</td></tr>';
        return;
      }
      adoptDevicesCache.forEach((d) => {
        const tr = document.createElement("tr");
        if ((d.base_url || "") === adoptSelectedBaseUrl) tr.style.background = "#eef6ff";
        if (!d.adopted) {
          tr.style.cursor = "pointer";
          tr.title = "Click row to select this device";
          tr.addEventListener("click", () => {
            fillAdoptFormFromDevice(d);
            if (!d.configured) setAdoptMsg("Device selected. Complete required setup fields below, then click Adopt Device.");
            const panel = document.getElementById("adoptBaseUrl");
            if (panel && typeof panel.scrollIntoView === "function") panel.scrollIntoView({ behavior: "smooth", block: "center" });
            renderAdoptDevices(adoptDevicesCache);
          });
        }
        const statusBits = [];
        statusBits.push(d.configured ? "Configured" : "Setup Required");
        if (d.adopted) statusBits.push(`Adopted (${d.adopted_zone_id || "--"})`);
        if (d.zone_error) statusBits.push("Zone API issue");
        const sup = (d.supply_f == null ? "--" : `${Number(d.supply_f).toFixed(1)}°F`);
        const ret = (d.return_f == null ? "--" : `${Number(d.return_f).toFixed(1)}°F`);
        tr.innerHTML = `
          <td>${d.unit_name || "--"}</td>
          <td>${d.ip || "--"}</td>
          <td>${statusBits.join(" • ")}</td>
          <td>${d.parent_zone || "--"}</td>
          <td>${sup} / ${ret}</td>
          <td>${d.call_status || "--"}</td>
          <td></td>
        `;
        const td = tr.lastElementChild;
        if (!d.adopted) {
          const btn = document.createElement("button");
          btn.type = "button";
          btn.className = "btn";
          btn.style.padding = "4px 10px";
          btn.textContent = d.configured ? "Select Device" : "Select + Prepare";
          btn.addEventListener("click", (ev) => {
            if (ev && typeof ev.stopPropagation === "function") ev.stopPropagation();
            fillAdoptFormFromDevice(d);
            if (!d.configured) setAdoptMsg("Device selected. Complete required setup fields below, then click Adopt Device.");
            const panel = document.getElementById("adoptBaseUrl");
            if (panel && typeof panel.scrollIntoView === "function") panel.scrollIntoView({ behavior: "smooth", block: "center" });
            renderAdoptDevices(adoptDevicesCache);
          });
          td.appendChild(btn);
        } else {
          td.textContent = "—";
        }
      });
    }

    async function loadAdoptDevices() {
      setAdoptMsg("Scanning Tailscale for zone nodes...");
      try {
        const res = await fetch(`/api/devices/discover?_=${Date.now()}`, { cache: "no-store" });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "discover failed");
        renderAdoptDevices(data.devices || []);
        const unadopted = (data.devices || []).filter((d) => !d.adopted).length;
        setAdoptMsg(`Scan complete. ${unadopted} device(s) available. Click Select Device on the row you want to adopt.`);
      } catch (e) {
        setAdoptMsg("Scan failed: " + e.message);
      }
    }

    function selectFirstAdoptableDevice() {
      const first = (adoptDevicesCache || []).find((d) => d && !d.adopted);
      if (!first) {
        setAdoptMsg("No unadopted devices available. Run Scan first.");
        return;
      }
      fillAdoptFormFromDevice(first);
      const panel = document.getElementById("adoptBaseUrl");
      if (panel && typeof panel.scrollIntoView === "function") panel.scrollIntoView({ behavior: "smooth", block: "center" });
      renderAdoptDevices(adoptDevicesCache);
    }

    async function adoptSelectedDevice() {
      const applySetup = false; // Safe adopt: do not push /api/setup to node during adoption.
      const base = (document.getElementById("adoptBaseUrl").value || "").trim();
      const name = (document.getElementById("adoptName").value || "").trim();
      const zoneId = (document.getElementById("adoptZoneId").value || "").trim();
      const parentZone = document.getElementById("adoptParentZone").value || "Zone 1";
      const deltaOk = Number(document.getElementById("adoptDeltaOk").value || 8);
      const supplyProbe = (document.getElementById("adoptSupplyProbe").value || "").trim();
      const returnProbe = (document.getElementById("adoptReturnProbe").value || "").trim();
      const wifiSsid = (document.getElementById("adoptWifiSsid").value || "").trim();
      const wifiPassword = (document.getElementById("adoptWifiPassword").value || "");
      const hubUrl = (document.getElementById("adoptHubUrl").value || "").trim();
      const lowTempEnabled = document.getElementById("adoptLowTempEnabled").value === "true";
      const lowTempThresholdF = Number(document.getElementById("adoptLowTempThreshold").value || 35);
      const callSensingEnabled = document.getElementById("adoptCallSensingEnabled").value === "true";
      const saveBtn = document.getElementById("adoptSaveBtn");
      const passwordSaved = !!(saveBtn && saveBtn.dataset && saveBtn.dataset.passwordSaved === "true");
      const missing = [];
      if (!base) missing.push("Device URL");
      if (!name) missing.push("Display Name");
      if (!zoneId) missing.push("Zone ID");
      if (!parentZone) missing.push("Parent Zone");
      if (applySetup) {
        if (!supplyProbe) missing.push("Supply Probe");
        if (!returnProbe) missing.push("Return Probe");
        if (!wifiSsid) missing.push("Wi-Fi Name");
        if (!wifiPassword && !passwordSaved) missing.push("Wi-Fi Password");
        if (!hubUrl) missing.push("Hub URL");
      }
      if (missing.length) { setAdoptMsg(`Required fields missing: ${missing.join(", ")}`); return; }
      if (applySetup && supplyProbe === returnProbe) { setAdoptMsg("Supply Probe and Return Probe must be different."); return; }
      if (!Number.isFinite(deltaOk) || deltaOk < 0) { setAdoptMsg("Supply/Return Match Threshold must be a valid number."); return; }
      if (applySetup && (!Number.isFinite(lowTempThresholdF) || lowTempThresholdF < 0)) { setAdoptMsg("Low Temp Threshold must be a valid number."); return; }
      setAdoptMsg("Adopting device...");
      try {
        const res = await fetch("/api/devices/adopt", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            base_url: base,
            name,
            zone_id: zoneId,
            parent_zone: parentZone,
            delta_ok_f: deltaOk,
            apply_setup: applySetup,
            setup: applySetup ? {
              apply_wifi: false,
              unit_name: name,
              zone_id: zoneId,
              parent_zone: parentZone,
              feed_sensor_id: supplyProbe,
              return_sensor_id: returnProbe,
              wifi: (wifiPassword ? { ssid: wifiSsid, password: wifiPassword } : { ssid: wifiSsid }),
              hub: { hub_url: hubUrl },
              alerts: {
                low_temp_enabled: lowTempEnabled,
                low_temp_threshold_f: lowTempThresholdF,
                call_sensing_enabled: callSensingEnabled,
              },
            } : {},
          })
        });
        const data = await res.json();
        if (!res.ok || data.error) {
          const missingFields = (data && Array.isArray(data.missing_fields) && data.missing_fields.length)
            ? ` (Missing: ${data.missing_fields.join(", ")})`
            : "";
          throw new Error((data && data.error ? data.error : "adopt failed") + missingFields);
        }
        setAdoptMsg(data.message || "Device adopted.");
        clearAdoptForm();
        const ok = document.getElementById("adoptConfirm");
        if (ok) {
          ok.innerHTML = `✅ Device Adopted Successfully<br><span style="font-weight:600">Name:</span> ${name} &nbsp;|&nbsp; <span style="font-weight:600">Zone ID:</span> ${zoneId} &nbsp;|&nbsp; <span style="font-weight:600">Parent:</span> ${parentZone}<br><span style="font-weight:600">Device URL:</span> ${base}`;
          ok.classList.remove("hidden");
          if (typeof ok.scrollIntoView === "function") ok.scrollIntoView({ behavior: "smooth", block: "center" });
        }
        renderAdoptDevices(data.devices || []);
        await refreshHub();
        refreshZoneMgmtSelect();
      } catch (e) {
        setAdoptMsg("Adopt failed: " + e.message);
      }
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
      document.getElementById("zoneMgmtCallSensingEnabled").value = ((diag.call_sensing_enabled ?? ((cfg.alerts || {}).call_sensing_enabled)) === false) ? "false" : "true";
      document.getElementById("zoneMgmtProbeSwapDelta").value = ((diag.probe_swap_idle_delta_threshold_f ?? ((cfg.alerts || {}).probe_swap_idle_delta_threshold_f)) ?? 5);
      document.getElementById("zoneMgmtProbeSummary").textContent = `Supply: ${nodeZone.feed_sensor_id || "--"} (${nodeZone.feed_f ?? "--"}F) | Return: ${nodeZone.return_sensor_id || "--"} (${nodeZone.return_f ?? "--"}F)`;
      const diagErr = data && (data.node_zone_error || data.node_setup_error);
      document.getElementById("zoneMgmtDiagStatus").textContent = diagErr
        ? `Node communication issue: ${diagErr}`
        : `${diag.self_diagnostic_status || "Unknown"}${diag.hardware_fault ? " (alerting enabled)" : ""}`;
      const issues = Array.isArray(diag.hardware_faults) ? diag.hardware_faults : [];
      const diagLines = issues.length ? issues.map((x) => `- ${x}`) : ["No hardware self-diagnostic issues reported."];
      if (diag.probe_swap_suspected) {
        diagLines.push("");
        diagLines.push("Recommended action: Use Swap Supply / Return, then verify which pipe stays hotter during idle (no 24VAC).");
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
      setZoneMgmtMsg("Swapping supply/return probes on device...");
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
          call_sensing_enabled: document.getElementById("zoneMgmtCallSensingEnabled").value === "true",
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

    async function zoneMgmtErase() {
      const zoneId = document.getElementById("zoneMgmtSelect").value || "";
      const zoneName = (document.getElementById("zoneMgmtName").value || zoneId || "this zone").trim();
      if (!zoneId) { setZoneMgmtMsg("Select an adopted zone device."); return; }
      if (!confirm(`Erase downstream zone '${zoneName}' from the hub? This removes its zone assignment and map pin/area data from the hub.`)) return;
      setZoneMgmtMsg("Erasing zone...");
      try {
        const res = await fetch("/api/zone-mgmt/action", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ zone_id: zoneId, action: "erase_zone" })
        });
        const data = await res.json();
        if (!res.ok || data.error) throw new Error(data.error || "erase failed");
        setZoneMgmtMsg(data.message || "Zone erased.");
        await refreshHub();
        refreshZoneMgmtSelect();
        const sel = document.getElementById("zoneMgmtSelect");
        if (sel && sel.value) await loadZoneMgmtStatus(sel.value);
      } catch (e) {
        setZoneMgmtMsg("Erase failed: " + e.message);
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
        btn.addEventListener("click", () => {
          const which = btn.dataset.adminTab;
          setAdminTab(which);
          const panelId = adminPanelIdForTab(which);
          if (panelId) scrollToAdminPanel(panelId);
        });
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
    function closeAdminToolsMenu() {
      const menu = document.getElementById("adminToolsMenu");
      const btn = document.getElementById("adminToolsPill");
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
    function toggleAdminToolsMenu() {
      const menu = document.getElementById("adminToolsMenu");
      const btn = document.getElementById("adminToolsPill");
      if (!menu || !btn) return;
      const show = !menu.classList.contains("show");
      menu.classList.toggle("show", show);
      btn.setAttribute("aria-expanded", show ? "true" : "false");
    }
    function openAdminToolsFromMenu(which) {
      closeAdminToolsMenu();
      ensureAdminToolsOpen();
      setAdminTab(which);
      const panelId = adminPanelIdForTab(which);
      if (panelId) scrollToAdminPanel(panelId);
    }
    function initHeaderActions() {
      const tasksBtn = document.getElementById("tasksPill");
      const adminBtn = document.getElementById("adminToolsPill");
      const addDeviceBtn = document.getElementById("addDevicePill");
      if (tasksBtn) tasksBtn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        closeAdminToolsMenu();
        toggleTasksMenu();
      });
      if (adminBtn) adminBtn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        closeTasksMenu();
        toggleAdminToolsMenu();
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
      const adminUsers = document.getElementById("adminMenuUsers");
      const adminAlerts = document.getElementById("adminMenuAlerts");
      const adminAdopt = document.getElementById("adminMenuAdopt");
      const adminZones = document.getElementById("adminMenuZones");
      const adminMap = document.getElementById("adminMenuMap");
      const adminSetup = document.getElementById("adminMenuSetup");
      const adminCommissioning = document.getElementById("adminMenuCommissioning");
      const adminBackup = document.getElementById("adminMenuBackup");
      if (adminUsers) adminUsers.addEventListener("click", () => openAdminToolsFromMenu("users"));
      if (adminAlerts) adminAlerts.addEventListener("click", () => openAdminToolsFromMenu("alerts"));
      if (adminAdopt) adminAdopt.addEventListener("click", () => openAdminToolsFromMenu("adopt"));
      if (adminZones) adminZones.addEventListener("click", () => openAdminToolsFromMenu("zones"));
      if (adminMap) adminMap.addEventListener("click", () => openAdminToolsFromMenu("map"));
      if (adminSetup) adminSetup.addEventListener("click", () => openAdminToolsFromMenu("setup"));
      if (adminCommissioning) adminCommissioning.addEventListener("click", () => openAdminToolsFromMenu("commissioning"));
      if (adminBackup) adminBackup.addEventListener("click", () => openAdminToolsFromMenu("backup"));
      document.addEventListener("click", (e) => {
        const wraps = Array.from(document.querySelectorAll(".head-action-wrap"));
        if (!wraps.length) return;
        if (!wraps.some((w) => w.contains(e.target))) {
          closeTasksMenu();
          closeAdminToolsMenu();
        }
      });
      if (addDeviceBtn) addDeviceBtn.addEventListener("click", openSetupPanel);
    }

    function openBuildingMapEditorFromPreview() {
      ensureAdminToolsOpen();
      setAdminTab("map");
      scrollToAdminPanel("adminMapPanel");
      setTimeout(() => {
        const hasSavedOutline = !!(siteMapConfig && siteMapConfig.building_outline);
        if (!hasSavedOutline) {
          siteMapAutoOpenZoneSetupAfterBuildingSave = true;
          if (!siteMapConfig || typeof siteMapConfig !== "object") siteMapConfig = { pins: {} };
          siteMapConfig.address = "";
          siteMapConfig.building_confirm_pin = null;
          const streetEl = document.getElementById("siteAddrStreet");
          const cityEl = document.getElementById("siteAddrCity");
          const stateEl = document.getElementById("siteAddrState");
          const zipEl = document.getElementById("siteAddrZip");
          const hiddenAddr = document.getElementById("siteAddress");
          if (streetEl) streetEl.value = "";
          if (cityEl) cityEl.value = "";
          if (stateEl) stateEl.value = "";
          if (zipEl) zipEl.value = "";
          if (hiddenAddr) hiddenAddr.value = "";
        }
        openBuildingOutlineWizard();
        if (hasSavedOutline) {
          siteMapAutoOpenZoneSetupAfterBuildingSave = false;
          window.setTimeout(() => {
            openSavedBuildingOutlineForEdit();
          }, 160);
        }
      }, 100);
    }

    function openZoneMapEditorFromPreview() {
      if (!buildingSetupComplete()) {
        openBuildingMapEditorFromPreview();
        setSiteMapMsg("Complete Building Setup first, then use Edit Zone Map.");
        return;
      }
      ensureAdminToolsOpen();
      setAdminTab("map");
      scrollToAdminPanel("adminMapPanel");
      setTimeout(() => {
        const z1Pinned = !!getSiteMapPin("zone1_main");
        const z2Pinned = !!getSiteMapPin("zone2_main");
        const nextMode = (z1Pinned && z2Pinned) ? "zone-downstream" : "zone-main";
        openZoneSetupWorkspace(nextMode);
        setTimeout(() => {
          try {
            scrollSiteMapWorkspaceToCanvas({ behavior: "smooth", pad: 8 });
          } catch (_e) {}
          if (!centerMapOnBuildingOutline()) centerMapOnAddressOrDefault(18);
          if (!siteMapMap) return;
          try { siteMapMap.invalidateSize(); } catch (_e) {}
          if (!z1Pinned) {
            selectMainBoilerZone("zone1_main", { preserveStep: true });
            setSiteMapMsg("Zone Setup: start by placing the Zone 1 main boiler/circulator pin inside the building outline.");
            return;
          }
          if (!z2Pinned) {
            selectMainBoilerZone("zone2_main", { preserveStep: true });
            setSiteMapMsg("Zone Setup: place the Zone 2 main boiler/circulator pin inside the building outline.");
            return;
          }
          const unpinned = nextUnpinnedZoneId();
          if (unpinned && !isMainZoneId(unpinned)) {
            const sel = document.getElementById("siteMapZoneSelect");
            if (sel) sel.value = String(unpinned);
            syncSiteMapDetailsFormFromSelected();
            useSelectedZoneSetupDevice();
            return;
          }
          setSiteMapMsg("Zone Setup ready. Select a downstream device and click inside the building outline to place a pin.");
        }, 160);
      }, 100);
    }

    function initOverviewTabs() {
      document.querySelectorAll("[data-overview-parent]").forEach((btn) => {
        btn.addEventListener("click", () => setOverviewParent(btn.dataset.overviewParent || "Zone 1"));
      });
    }

    function initOpenGraphDelegation() {
      document.addEventListener("click", (e) => {
        const btn = e.target && e.target.closest ? e.target.closest("[data-open-graph-tab]") : null;
        if (!btn) return;
        e.preventDefault();
        const tabId = String(btn.dataset.openGraphTab || "").trim();
        const cycId = String(btn.dataset.openGraphCycle || "").trim();
        if (!tabId) return;
        focusZoneGraph(tabId, cycId);
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
    initOpenGraphDelegation();
    initSiteMapAddressStepFields();
    bindZoneInfoButtons();
    const firstZoneBtn = document.getElementById("firstZoneSetupBtn");
    if (firstZoneBtn) firstZoneBtn.addEventListener("click", openSetupPanel);
    const previewEditZonesBtn = document.getElementById("mainMapPreviewEditZonesBtn");
    if (previewEditZonesBtn) previewEditZonesBtn.addEventListener("click", openZoneMapEditorFromPreview);
    const previewEditBtn = document.getElementById("mainMapPreviewEditBtn");
    if (previewEditBtn) previewEditBtn.addEventListener("click", openBuildingMapEditorFromPreview);
    document.getElementById("siteMapFindBtn").addEventListener("click", geocodeSiteAddress);
    document.getElementById("siteMapFootprintBtn").addEventListener("click", openBuildingOutlineWizard);
    document.getElementById("siteMapZoneSetupBtn").addEventListener("click", () => openZoneSetupWorkspace("zone-main"));
    document.getElementById("siteMapOutlineWizardCloseBtn").addEventListener("click", exitBuildingSetupMode);
    document.getElementById("siteMapZoneWizardExitBtn").addEventListener("click", exitZoneSetupMode);
    document.getElementById("siteMapZoneWizardMainTabBtn").addEventListener("click", () => openZoneSetupWorkspace("zone-main"));
    document.getElementById("siteMapZoneWizardDownstreamTabBtn").addEventListener("click", () => openZoneSetupWorkspace("zone-downstream"));
    document.getElementById("siteMapZoneWizardPinZone1Btn").addEventListener("click", () => selectMainBoilerZone("zone1_main"));
    document.getElementById("siteMapZoneWizardPinZone2Btn").addEventListener("click", () => selectMainBoilerZone("zone2_main"));
    document.getElementById("siteMapZoneWizardSaveProgressBtn").addEventListener("click", saveZoneSetupProgress);
    document.getElementById("siteMapZoneWizardSaveProgressBtnDs").addEventListener("click", saveZoneSetupProgress);
    document.getElementById("siteMapZoneWizardClearOutsidePinsBtn").addEventListener("click", clearPinsOutsideBuildingOutline);
    document.getElementById("siteMapZoneWizardClearOutsidePinsBtnDs").addEventListener("click", clearPinsOutsideBuildingOutline);
    document.getElementById("siteMapZoneWizardLoadSelectedBtn").addEventListener("click", useSelectedZoneSetupDevice);
    document.getElementById("siteMapZoneWizardRefreshZoneBtn").addEventListener("click", () => {
      refreshSiteMapZoneSelect();
      const sel = document.getElementById("siteMapZoneSelect");
      const ds = realDownstreamRows((lastHubPayload && lastHubPayload.downstream) || []);
      if (sel && ds.length) sel.value = String(ds[0].id);
      syncSiteMapDetailsFormFromSelected();
      setSiteMapMsg(ds.length ? "Downstream zone list refreshed. Select a zone/device and click inside the building outline to place a pin." : "No adopted downstream zones available yet.");
      renderSiteMapZoneWizard();
    });
    document.getElementById("siteMapZoneWizardStartAreaBtn").addEventListener("click", () => {
      const sel = document.getElementById("siteMapZoneSelect");
      const zid = String(sel?.value || "");
      if (!zid || isMainZoneId(zid)) {
        setSiteMapMsg("Select a downstream zone/device first, then start zone area drawing.");
        return;
      }
      startZoneAreaDraw();
    });
    document.getElementById("siteMapOutlineStepFindBtn").addEventListener("click", geocodeSiteAddress);
    document.getElementById("siteMapOutlineStepAutoBtn").addEventListener("click", loadBuildingOutline);
    document.getElementById("siteMapOutlineStartDrawBtn").addEventListener("click", startBuildingOutlineDraw);
    document.getElementById("siteMapOutlineUndoBtn").addEventListener("click", undoBuildingOutlinePoint);
    document.getElementById("siteMapOutlineDeletePointBtn").addEventListener("click", deleteSelectedDraftOutlinePoint);
    document.getElementById("siteMapOutlineFinishDrawBtn").addEventListener("click", finishBuildingOutlineDraw);
    document.getElementById("siteMapOutlineClearBtn").addEventListener("click", clearBuildingOutline);
    document.getElementById("siteMapOutlineEditSavedBtn").addEventListener("click", openSavedBuildingOutlineForEdit);
    document.getElementById("siteMapOutlineDragBtn").addEventListener("click", () => toggleBuildingOutlineDragMode());
    document.getElementById("siteMapOutlineFloorsSaveBtn").addEventListener("click", saveBuildingFloorCount);
    document.getElementById("siteMapOutlineNextStepBtn").addEventListener("click", advanceOutlineWizardStep);
    document.getElementById("siteMapOutlineContinueZoneSetupBtn").addEventListener("click", continueToZoneSetupWizard);
    document.getElementById("siteMapOutlinePrevStepBtn").addEventListener("click", goToPreviousOutlineWizardStep);
    document.getElementById("siteMapResetBuildingBtn").addEventListener("click", openResetBuildingModal);
    document.getElementById("siteMapFloorModalCloseBtn").addEventListener("click", closeSiteMapFloorModal);
    document.getElementById("siteMapFloorModalBuildRowsBtn").addEventListener("click", renderSiteMapFloorModalRows);
    document.getElementById("siteMapFloorModalCount").addEventListener("input", renderSiteMapFloorModalRows);
    document.getElementById("siteMapFloorModalContinueBtn").addEventListener("click", () => {
      const count = saveFloorModalToSiteMapConfig();
      closeSiteMapFloorModal();
      setSiteMapMsg(`Building floor count set to ${count}. Continue confirming the building outline.`);
      confirmBuildingOutlineAndProceed(siteMapPendingOutlineConfirmSourceLabel || "outline", { skipFloorPrompt: true });
    });
    document.getElementById("siteMapResetBuildingCloseBtn").addEventListener("click", closeResetBuildingModal);
    document.getElementById("siteMapResetBuildingCancelBtn").addEventListener("click", closeResetBuildingModal);
    document.getElementById("siteMapResetBuildingConfirmBtn").addEventListener("click", resetBuildingSetupAndMap);
    document.getElementById("siteMapApplyParentColorsBtn").addEventListener("click", applyParentColorsToZones);
    document.getElementById("siteMapSelectZone1MainBtn").addEventListener("click", () => selectMainBoilerZone("zone1_main"));
    document.getElementById("siteMapSelectZone2MainBtn").addEventListener("click", () => selectMainBoilerZone("zone2_main"));
    document.getElementById("siteMapSaveBtn").addEventListener("click", saveSiteMapConfig);
    document.getElementById("siteMapWorkspaceSaveBtn").addEventListener("click", saveAndFinishSiteMapWorkspace);
    document.getElementById("siteMapWorkspaceBackBtn").addEventListener("click", exitSiteMapWorkspace);
    document.getElementById("siteMapClearPinBtn").addEventListener("click", clearSelectedSiteMapPin);
    document.getElementById("siteMapStartAreaBtn").addEventListener("click", startZoneAreaDraw);
    document.getElementById("siteMapFinishAreaBtn").addEventListener("click", finishZoneAreaDraw);
    document.getElementById("siteMapCancelAreaBtn").addEventListener("click", cancelZoneAreaDraw);
    document.getElementById("siteMapAreaUndoBtn").addEventListener("click", undoZoneAreaPoint);
    document.getElementById("siteMapClearAreaBtn").addEventListener("click", clearSelectedZoneArea);
    document.getElementById("siteMapApplyDetailsBtn").addEventListener("click", applySelectedSiteMapDetails);
    document.getElementById("siteMapClearPhotoBtn").addEventListener("click", clearSelectedSiteMapPhoto);
    document.getElementById("siteMapPhotoInput").addEventListener("change", handleSiteMapPhotoInput);
    document.getElementById("siteMapZoneSelect").addEventListener("change", () => {
      syncSiteMapDetailsFormFromSelected();
      updateSiteMapWorkspaceStatus();
      renderSiteMapZoneWizard();
    });
    document.getElementById("siteMapLayer").addEventListener("change", (e) => setSiteMapBaseLayer((e && e.target && e.target.value) || "street"));
    document.addEventListener("keydown", (e) => {
      const floorModalOpen = !!document.getElementById("siteMapFloorModal")?.classList.contains("show");
      if (floorModalOpen) {
        const tag = String((e.target && e.target.tagName) || "").toLowerCase();
        if (e.key === "Escape") {
          e.preventDefault();
          closeSiteMapFloorModal();
          return;
        }
        if (e.key === "Enter" && tag !== "textarea") {
          e.preventDefault();
          document.getElementById("siteMapFloorModalContinueBtn")?.click();
          return;
        }
      }
      const resetBuildingModalOpen = !!document.getElementById("siteMapResetBuildingModal")?.classList.contains("show");
      if (resetBuildingModalOpen) {
        const tag = String((e.target && e.target.tagName) || "").toLowerCase();
        if (e.key === "Escape") {
          e.preventDefault();
          closeResetBuildingModal();
          return;
        }
        if (e.key === "Enter" && tag !== "textarea") {
          e.preventDefault();
          document.getElementById("siteMapResetBuildingConfirmBtn")?.click();
          return;
        }
      }
      if (e.key === "Escape" && siteMapWorkspaceOpen) {
        if (siteMapOutlineDragMode) {
          e.preventDefault();
          toggleBuildingOutlineDragMode(false);
          return;
        }
        e.preventDefault();
        exitSiteMapWorkspace();
      }
      if (e.key === "Enter" && siteMapOutlineWizardOpen) {
        const tag = String((e.target && e.target.tagName) || "").toLowerCase();
        if (tag === "textarea") return;
        if (siteMapOutlineDragMode) {
          e.preventDefault();
          toggleBuildingOutlineDragMode(false);
          return;
        }
        e.preventDefault();
        advanceOutlineWizardStep();
      }
      if ((e.key === "Delete" || e.key === "Backspace") && siteMapOutlineWizardOpen) {
        const tag = String((e.target && e.target.tagName) || "").toLowerCase();
        if (["input", "textarea", "select"].includes(tag)) return;
        if (siteMapDrawingBuildingOutline && siteMapEditingSavedOutlinePoints && Number.isInteger(siteMapSelectedOutlinePointIndex) && siteMapSelectedOutlinePointIndex >= 0) {
          e.preventDefault();
          deleteSelectedDraftOutlinePoint();
          return;
        }
      }
    });
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
    const adoptScanBtn = document.getElementById("adoptScanBtn");
    if (adoptScanBtn) adoptScanBtn.addEventListener("click", loadAdoptDevices);
    const adoptRefreshBtn = document.getElementById("adoptRefreshBtn");
    if (adoptRefreshBtn) adoptRefreshBtn.addEventListener("click", loadAdoptDevices);
    const adoptSelectFirstBtn = document.getElementById("adoptSelectFirstBtn");
    if (adoptSelectFirstBtn) adoptSelectFirstBtn.addEventListener("click", selectFirstAdoptableDevice);
    const adoptSaveBtn = document.getElementById("adoptSaveBtn");
    if (adoptSaveBtn) adoptSaveBtn.addEventListener("click", adoptSelectedDevice);
    const adoptLoadSetupBtn = document.getElementById("adoptLoadSetupBtn");
    if (adoptLoadSetupBtn) adoptLoadSetupBtn.addEventListener("click", () => loadAdoptDeviceSetup());
    const adoptClearBtn = document.getElementById("adoptClearBtn");
    if (adoptClearBtn) adoptClearBtn.addEventListener("click", () => { clearAdoptForm(); setAdoptMsg(""); });
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
    document.getElementById("zoneMgmtEraseBtn").addEventListener("click", zoneMgmtErase);
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

        if route == "/api/devices/discover":
            b = json.dumps({"devices": discovered_zone_nodes(), "updated_utc": now_utc_iso()}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        if route == "/api/device/setup-state":
            base_url = normalize_zone_node_base_url(str(query.get("base_url", [""])[0] or ""))
            if not base_url:
                b = json.dumps({"error": "base_url is required"}).encode("utf-8")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            setup_state, setup_err = fetch_remote_json(base_url + "/api/setup-state")
            zone_payload, zone_err = fetch_remote_json(base_url + "/api/zone")
            if not isinstance(setup_state, dict):
                b = json.dumps({"error": f"setup-state unavailable ({setup_err or 'unknown'})"}).encode("utf-8")
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
            b = json.dumps({
                "base_url": base_url,
                "setup_state": setup_state,
                "zone": zone_payload if isinstance(zone_payload, dict) else None,
                "probe_choices": setup_state.get("probe_choices", []),
                "zone_error": zone_err,
                "updated_utc": now_utc_iso(),
            }).encode("utf-8")
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
            "/api/devices/adopt",
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

        if route == "/api/devices/adopt":
            base_url = normalize_zone_node_base_url(body.get("base_url") or body.get("source_url") or "")
            if not base_url:
                b = json.dumps({"error": "base_url is required"}).encode("utf-8")
                self.send_response(400); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
            setup_body = body.get("setup", {}) if isinstance(body.get("setup"), dict) else {}
            setup_apply_result = None
            setup_warning = None
            apply_setup = bool(body.get("apply_setup", False))
            if setup_body and apply_setup:
                setup_apply_result, setup_apply_err = post_remote_json(base_url + "/api/setup", setup_body, timeout=35)
                # Setup on zone nodes can briefly time out while network settings apply.
                # If that happens, try reading setup-state and continue if fields are present.
                if setup_apply_err and "timed out" in str(setup_apply_err).lower():
                    setup_warning = "Device setup request timed out; continuing adoption and using current device state."
                    recover_state, recover_err = fetch_remote_json(base_url + "/api/setup-state")
                    if isinstance(recover_state, dict):
                        recover_cfg = recover_state.get("config", {}) if isinstance(recover_state.get("config"), dict) else {}
                        req_ok = True
                        for rk in ("unit_name", "feed_sensor_id", "return_sensor_id"):
                            if not str(recover_cfg.get(rk) or "").strip():
                                req_ok = False
                                break
                        hub_cfg = recover_cfg.get("hub", {}) if isinstance(recover_cfg.get("hub"), dict) else {}
                        wifi_cfg = recover_cfg.get("wifi", {}) if isinstance(recover_cfg.get("wifi"), dict) else {}
                        if not str(hub_cfg.get("hub_url") or "").strip():
                            req_ok = False
                        if not str(wifi_cfg.get("ssid") or "").strip():
                            req_ok = False
                        if req_ok:
                            setup_apply_result = {"ok": True, "message": "setup request timed out but device setup appears applied"}
                            setup_apply_err = None
                    else:
                        setup_apply_result = setup_apply_result or {"ok": False, "recover_error": recover_err}
                    # Do not hard-fail adoption on timeout path. Device may have applied config
                    # but dropped/rejoined network before returning HTTP response.
                    setup_apply_err = None
                    if not isinstance(setup_apply_result, dict):
                        setup_apply_result = {"ok": True, "message": "setup response timed out; adoption continued"}
                if setup_apply_err:
                    b = json.dumps({"error": f"device setup failed ({setup_apply_err})", "setup_result": setup_apply_result}).encode("utf-8")
                    self.send_response(502); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
                if not isinstance(setup_apply_result, dict) or not setup_apply_result.get("ok"):
                    err = ""
                    if isinstance(setup_apply_result, dict):
                        err = str(setup_apply_result.get("error") or "")
                        missing_fields = setup_apply_result.get("missing_fields")
                        if isinstance(missing_fields, list) and missing_fields:
                            err = ("missing required setup fields: " + ", ".join(str(x) for x in missing_fields))
                    b = json.dumps({"error": err or "device setup failed", "setup_result": setup_apply_result}).encode("utf-8")
                    self.send_response(400); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
            elif setup_body and not apply_setup:
                setup_warning = "Node setup push skipped during adopt (safe mode)."
            setup_state, setup_err = fetch_remote_json(base_url + "/api/setup-state")
            zone_payload_remote, zone_err = fetch_remote_json(base_url + "/api/zone")
            if not isinstance(setup_state, dict) and not isinstance(zone_payload_remote, dict):
                b = json.dumps({"error": f"device unreachable ({setup_err or zone_err or 'unknown'})"}).encode("utf-8")
                self.send_response(502); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
            cfg = setup_state.get("config", {}) if isinstance(setup_state, dict) and isinstance(setup_state.get("config"), dict) else {}
            zr = zone_payload_remote if isinstance(zone_payload_remote, dict) else {}
            display_name = str(body.get("name") or zr.get("unit_name") or cfg.get("unit_name") or "").strip() or "Unnamed Zone Device"
            parent_zone = str(body.get("parent_zone") or zr.get("parent_zone") or cfg.get("parent_zone") or "Zone 1").strip() or "Zone 1"
            zone_id = str(body.get("zone_id") or zr.get("zone_id") or cfg.get("zone_id") or "").strip()
            if not zone_id:
                zone_id = slugify_zone_id(f"{parent_zone}-{display_name}")
            try:
                delta_ok_f = float(body.get("delta_ok_f", 8.0))
            except Exception:
                delta_ok_f = 8.0
            source_url = base_url.rstrip("/") + "/api/zone"
            zones = load_config()
            for z in zones:
                if not isinstance(z, dict):
                    continue
                if str(z.get("source_url") or "").strip() == source_url:
                    b = json.dumps({"error": "device already adopted", "existing_zone_id": str(z.get("id") or "")}).encode("utf-8")
                    self.send_response(409); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
                if str(z.get("id") or "").strip() == zone_id:
                    b = json.dumps({"error": "zone_id already exists"}).encode("utf-8")
                    self.send_response(409); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
            new_entry = {
                "id": zone_id,
                "name": display_name,
                "parent_zone": parent_zone,
                "source_url": source_url,
                "delta_ok_f": delta_ok_f,
            }
            zones.append(new_entry)
            if not save_config_zones(zones):
                b = json.dumps({"error": "failed to save downstream zone config"}).encode("utf-8")
                self.send_response(500); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b); return
            try:
                refresh_downstream_cache()
            except Exception:
                pass
            b = json.dumps({
                "ok": True,
                "message": "Device adopted and assigned to hub." if not setup_warning else ("Device adopted. " + setup_warning),
                "zone": new_entry,
                "setup_result": setup_apply_result,
                "setup_warning": setup_warning,
                "devices": discovered_zone_nodes(),
                "updated_utc": now_utc_iso(),
            }).encode("utf-8")
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
                try:
                    cfg["building_floors"] = max(1, int(body.get("building_floors", cfg.get("building_floors", 1))))
                except Exception:
                    pass
                bfl = body.get("building_floor_labels")
                if isinstance(bfl, list):
                    labels = []
                    for item in bfl:
                        if len(labels) >= 24:
                            break
                        labels.append(str(item or "").strip()[:120])
                    cfg["building_floor_labels"] = labels
                cfg["map_layer"] = str(body.get("map_layer") or cfg.get("map_layer") or "street")
                bcp = body.get("building_confirm_pin")
                if bcp is None:
                    cfg["building_confirm_pin"] = None
                elif isinstance(bcp, dict):
                    try:
                        cfg["building_confirm_pin"] = {"lat": float(bcp.get("lat")), "lng": float(bcp.get("lng"))}
                    except Exception:
                        pass
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
                            "main_boiler_floor": str(p.get("main_boiler_floor") or ""),
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
            elif action == "erase_zone":
                if zidx is not None:
                    zones.pop(zidx)
                    if not save_config_zones(zones):
                        b = json.dumps({"error": "failed to save hub zone config"}).encode("utf-8")
                        self.send_response(500)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(b)))
                        self.end_headers()
                        self.wfile.write(b)
                        return
                # Clean up optional hub-side metadata for the removed downstream zone.
                try:
                    sm = load_site_map_config()
                    pins = sm.get("pins", {}) if isinstance(sm, dict) and isinstance(sm.get("pins"), dict) else {}
                    if zone_id in pins:
                        pins.pop(zone_id, None)
                        sm["pins"] = pins
                        save_site_map_config(sm)
                except Exception:
                    pass
                try:
                    ccfg = load_commissioning_config()
                    recs = ccfg.get("records", {}) if isinstance(ccfg, dict) and isinstance(ccfg.get("records"), dict) else {}
                    if zone_id in recs:
                        recs.pop(zone_id, None)
                        ccfg["records"] = recs
                        ccfg["updated_utc"] = now_utc_iso()
                        save_commissioning_config(ccfg)
                except Exception:
                    pass
                try:
                    refresh_downstream_cache()
                except Exception:
                    pass
                b = json.dumps({
                    "ok": True,
                    "message": "Zone erased from hub. Device remains running and can be re-adopted later.",
                    "zone_id": zone_id,
                    "updated_utc": now_utc_iso(),
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
                return
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
