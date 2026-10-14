#!/bin/bash
# EC2 setup (Ubuntu 24.04, ap-south-1 Mumbai recommended for NSE latency).
# Your home IP is dynamic, so three supported patterns (pick ONE):
#   1. CIDR (simplest): ./ec2-setup.sh 49.37.0.0/16   — allows your ISP pool; check UFW supports CIDR natively.
#   2. DDNS (tighter): run a no-ip/duckdns updater at home, then open UFW per current IP and
#      re-run ./deploy/ufw-refresh.sh (cron) which re-resolves the name. App layer also
#      re-resolves IP_ALLOWLIST hostnames by itself.
#   3. Tailscale (tightest, recommended if you can install it on PC+EC2): no public 80/443 at all.
#      See deploy/tailscale.md.
set -euo pipefail
ALLOW="${1:?Usage: ./ec2-setup.sh <YOUR_IP_OR_CIDR>  (e.g. 49.37.0.0/16)}"

sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin ufw fail2ban dnsutils
sudo usermod -aG docker ubuntu

sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow from "$ALLOW" to any port 22
sudo ufw allow from "$ALLOW" to any port 80
sudo ufw allow from "$ALLOW" to any port 443
sudo ufw --force enable

echo "UFW done for $ALLOW. Deploy with: docker compose up -d --build"
echo "Set the same range/name in .env IP_ALLOWLIST (defence in depth)."
