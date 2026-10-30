#!/bin/bash
# EC2 setup (Ubuntu 24.04, ap-south-1 Mumbai recommended for NSE latency).
#
# Network model (chosen: Tailscale, option A — see docs/plan/01-network-and-transport-security.md):
#   * No public 80/443. Caddy is published to loopback only (docker-compose.yml).
#   * `tailscale serve` terminates TLS with a trusted *.ts.net cert and proxies to
#     Caddy on 127.0.0.1:80. Only tailnet devices can reach the desk.
#   * SSH: keep your home-IP/CIDR opening below for bootstrap, then prefer SSH over
#     the tailnet. Your home IP is dynamic, so a CIDR (49.37.0.0/16) or DDNS is fine.
#
# Usage: ./ec2-setup.sh <YOUR_IP_OR_CIDR>   (e.g. 49.37.0.0/16)
set -euo pipefail
ALLOW="${1:?Usage: ./ec2-setup.sh <YOUR_IP_OR_CIDR>  (e.g. 49.37.0.0/16)}"

sudo apt-get update
# Ubuntu 24.04 ships Docker Compose v2 as "docker-compose-v2"; the
# "docker-compose-plugin" name only exists in Docker's own apt repository.
sudo apt-get install -y docker.io docker-compose-v2 ufw fail2ban dnsutils curl
sudo usermod -aG docker ubuntu

# Public surface: SSH only (from your IP). 80/443 stay closed — TLS is via Tailscale.
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow from "$ALLOW" to any port 22
sudo ufw --force enable

# Tailscale: install and join the tailnet (interactive `tailscale up` prints a URL).
curl -fsSL https://tailscale.com/install.sh | sh

echo "UFW done (SSH only from $ALLOW; 80/443 closed)."
echo "Next:"
echo "  1. sudo tailscale up"
echo "  2. Deploy: docker compose up -d --build"
echo "  3. sudo tailscale serve --bg --https=443 http://127.0.0.1:80"
echo "  4. Browse https://<machine>.<tailnet>.ts.net"
echo "Set IP_ALLOWLIST to your tailnet range (100.64.0.0/10) or a tighter value in .env."
