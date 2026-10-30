#!/bin/bash
# Refresh the direct-SSH allow rule for a dynamic home IP.
#
# Tailscale-only network model: the app itself is never exposed publicly
# (Caddy binds 127.0.0.1 only; `tailscale serve` fronts TLS). The only public
# port is 22, and only during bootstrap. This script manages exactly one rule
# (22/tcp, tagged with the "home-ssh" comment) and never opens 80/443.
#
# Prefer SSH over the tailnet once `tailscale up` is done. If you still need a
# direct SSH bootstrap rule from a changing home IP:
#   */5 * * * * /home/ubuntu/kotak-neo-trader/deploy/ufw-refresh.sh myhome.ddns.net
set -euo pipefail
HOST="${1:?Usage: ufw-refresh.sh <ddns-hostname>}"
IP="$(dig +short -4 "$HOST" | grep -E '^[0-9.]+$' | head -n1)"
[ -n "$IP" ] || { echo "resolve failed for $HOST"; exit 1; }

# Already the current allowlisted IP? Nothing to do.
if sudo ufw status | grep 'home-ssh' | grep -q "$IP"; then
  echo "unchanged ($IP)"
  exit 0
fi

# Delete previous home-ssh rules (highest index first so numbering stays valid),
# then allow the new IP. 80/443 are intentionally never touched.
sudo ufw status numbered \
  | awk -F'[][]' '/home-ssh/ {gsub(/ /, "", $2); print $2}' \
  | sort -rn \
  | while read -r n; do
      [ -n "$n" ] && sudo ufw --force delete "$n"
    done

sudo ufw allow from "$IP" to any port 22 proto tcp comment 'home-ssh'
echo "UFW SSH rule refreshed to $IP ($HOST)"
