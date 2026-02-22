# HVAC Building Managment

Raspberry Pi-based HVAC monitoring and setup tooling for a building heating hub and downstream zone devices.

## Components

- `hub/temps_web.py`: Main heating hub web app (temps, zones, history, alerts, public sharing)
- `hub/temps-web.service`: systemd unit for the hub app
- `hub/downstream_zones.example.json`: Example downstream zone endpoint config
- `hub/alert_config.example.json`: Example email/SMS alert config (no secrets)
- `zone-node/zone_node.py`: Reusable downstream zone node (1-wire probes + GPIO dry contact + setup UI)
- `zone-node/scripts/zone-node-net.sh`: Network helper (hotspot, Wi-Fi connect, scan)
- `zone-node/scripts/zone-node-hotspot-onboot.sh`: Optional hotspot-on-boot helper
- `zone-node/systemd/*.service`: systemd units for zone node and onboarding hotspot
- `zone-node/zone_node_config.example.json`: Example zone node config

## Notes

- Do not commit live credentials (`alert_config.json`) or live Wi-Fi passwords.
- The zone node setup page supports mobile onboarding in hotspot mode and LAN/Tailscale mode.
- Low temperature warning support is built into zone nodes (default threshold `35F`).

## GitHub Push

After creating a GitHub repo (for example `git@github.com:eamondoherty618/hvac-building-managment.git`):

```bash
git remote add origin git@github.com:eamondoherty618/hvac-building-managment.git
git branch -M main
git push -u origin main
```
