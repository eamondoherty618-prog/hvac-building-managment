#!/bin/bash
set -euo pipefail
WLAN_IF="wlan0"
AP_CONN="zone-node-setup"
CLIENT_CONN="zone-node-client"
HOSTNAME_SHORT="$(hostname | tr -cd '[:alnum:]-')"
SUFFIX="${HOSTNAME_SHORT: -4}"
AP_SSID="HeatingHub-Setup-${SUFFIX}"
AP_PSK_DEFAULT="heatsetup123"

json_status() {
  local conn ssid ap_active ap_name ip4
  conn="$(nmcli -t -f NAME,DEVICE connection show --active | awk -F: -v d="$WLAN_IF" '$2==d{print $1; exit}')"
  ssid="$(nmcli -t -f ACTIVE,SSID dev wifi | awk -F: '$1=="yes"{print $2; exit}')"
  ap_active=false
  ap_name=""
  if [[ "$conn" == "$AP_CONN" ]]; then
    ap_active=true
    ap_name="$AP_SSID"
  fi
  ip4="$(ip -4 -brief addr show "$WLAN_IF" | awk '{print $3}')"
  python3 - <<PY
import json
print(json.dumps({
  "ok": True,
  "active_connection": """$conn""",
  "connected_ssid": """$ssid""",
  "ap_active": True if """$ap_active""" == "true" else False,
  "ap_ssid": """$ap_name""",
  "hotspot_ip": """$ip4""",
}))
PY
}

scan_wifi() {
  python3 - <<'PY'
import json, subprocess
try:
    cp = subprocess.run(
        ["nmcli", "-t", "-f", "SSID,SIGNAL,SECURITY", "dev", "wifi", "list", "ifname", "wlan0"],
        capture_output=True, text=True, timeout=10
    )
    rows = []
    seen = set()
    for line in cp.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split(":", 2)
        ssid = (parts[0] or "").strip()
        signal = (parts[1] or "").strip() if len(parts) > 1 else ""
        sec = (parts[2] or "").strip() if len(parts) > 2 else ""
        if not ssid or ssid in seen:
            continue
        seen.add(ssid)
        try:
            signal_i = int(signal)
        except Exception:
            signal_i = None
        rows.append({"ssid": ssid, "signal": signal_i, "security": sec})
    rows.sort(key=lambda r: (r["signal"] is None, -(r["signal"] or 0), r["ssid"].lower()))
    print(json.dumps({"ok": cp.returncode == 0, "networks": rows}))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e), "networks": []}))
PY
}

ensure_ap_profile() {
  if ! nmcli -t -f NAME connection show | grep -Fxq "$AP_CONN"; then
    nmcli connection add type wifi ifname "$WLAN_IF" con-name "$AP_CONN" autoconnect no ssid "$AP_SSID" >/dev/null
    nmcli connection modify "$AP_CONN" \
      802-11-wireless.mode ap \
      802-11-wireless.band bg \
      ipv4.method shared \
      ipv4.addresses 10.42.0.1/24 \
      ipv6.method ignore \
      wifi-sec.key-mgmt wpa-psk \
      wifi-sec.psk "$AP_PSK_DEFAULT" >/dev/null
  else
    nmcli connection modify "$AP_CONN" \
      connection.autoconnect no \
      802-11-wireless.ssid "$AP_SSID" \
      802-11-wireless.mode ap \
      ipv4.method shared \
      ipv4.addresses 10.42.0.1/24 \
      ipv6.method ignore >/dev/null
  fi
}

ap_on() {
  ensure_ap_profile
  nmcli connection up "$AP_CONN" >/dev/null
  echo "AP started: $AP_SSID"
}

ap_off() {
  nmcli connection down "$AP_CONN" >/dev/null 2>&1 || true
  echo "AP stopped"
}

connect_wifi() {
  local ssid="${1:-}"
  local psk="${2:-}"
  if [[ -z "$ssid" || -z "$psk" ]]; then
    echo "usage: connect <ssid> <password>" >&2
    exit 2
  fi
  nmcli connection down "$AP_CONN" >/dev/null 2>&1 || true
  if nmcli -t -f NAME connection show | grep -Fxq "$CLIENT_CONN"; then
    nmcli connection modify "$CLIENT_CONN" 802-11-wireless.ssid "$ssid" wifi-sec.key-mgmt wpa-psk wifi-sec.psk "$psk" connection.autoconnect yes >/dev/null
    nmcli connection up "$CLIENT_CONN" >/dev/null || nmcli dev wifi connect "$ssid" password "$psk" ifname "$WLAN_IF" name "$CLIENT_CONN" >/dev/null
  else
    nmcli dev wifi connect "$ssid" password "$psk" ifname "$WLAN_IF" name "$CLIENT_CONN" >/dev/null
  fi
  echo "Wi-Fi connect requested: $ssid"
}

case "${1:-}" in
  status) json_status ;;
  scan) scan_wifi ;;
  ap-on) ap_on ;;
  ap-off) ap_off ;;
  connect) shift; connect_wifi "$@" ;;
  *) echo "usage: $0 {status|scan|ap-on|ap-off|connect <ssid> <password>}" >&2; exit 2 ;;
esac
