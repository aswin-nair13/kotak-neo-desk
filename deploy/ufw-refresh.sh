#!/bin/bash
# Re-resolve a DDNS name and swap the UFW allow rules (for dynamic home IPs).
# Cron every 5 min: */5 * * * * /home/ubuntu/kotak-trader/deploy/ufw-refresh.sh myhome.ddns.net
set -euo pipefail
HOST="${1:?Usage: ufw-refresh.sh <ddns-hostname>}"
IP="$(dig +short "$HOST" | grep -E '^[0-9.]+$' | head -n1)"
[ -n "$IP" ] || { echo "resolve failed for $HOST"; exit 1; }
if sudo ufw status | grep -q "$IP.*443"; then echo "unchanged ($IP)"; exit 0; fi
# delete old home rules (22/80/443) then add current IP
for p in 22 80 443; do
  sudo ufw status numbered | awk -v p="$p" '/ALLOW/ && $0 ~ p {print}' || true
done
sudo ufw allow from "$IP" to any port 22
sudo ufw allow from "$IP" to any port 80
sudo ufw allow from "$IP" to any port 443
echo "UFW refreshed to $IP ($HOST)"
