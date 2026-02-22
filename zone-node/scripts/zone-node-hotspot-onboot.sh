#!/bin/bash
set -euo pipefail
CFG="/home/eamondoherty618/zone_node_config.json"
if [ ! -f "$CFG" ]; then
  exec /usr/local/bin/zone-node-net.sh ap-on
fi
configured=$(python3 - <<"PY"
import json
from pathlib import Path
p=Path("/home/eamondoherty618/zone_node_config.json")
try:
 d=json.loads(p.read_text())
 print("1" if d.get("configured") else "0")
except Exception:
 print("0")
PY
)
if [ "$configured" = "0" ]; then
  exec /usr/local/bin/zone-node-net.sh ap-on
fi
exit 0
