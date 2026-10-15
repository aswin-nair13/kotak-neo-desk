# Option A: Tailscale (recommended — encrypted transport, no public ports)

Tailscale terminates TLS with a **trusted** `*.ts.net` certificate, so there is
no self-signed warning and no Certbot/Let's Encrypt setup. Nothing is exposed
beyond your tailnet.

## Setup

1. Install Tailscale on the EC2 box and your PC; join both to the same tailnet.
   `./deploy/ec2-setup.sh <your-ip>` installs it; then run `sudo tailscale up`.
2. Deploy the stack. Caddy is published to loopback only (`127.0.0.1:80`), so it
   is reachable by `tailscale serve` but not from the internet:
   ```bash
   docker compose up -d --build
   ```
3. Front TLS on the tailnet and proxy to Caddy:
   ```bash
   sudo tailscale serve --bg --https=443 http://127.0.0.1:80
   sudo tailscale serve status   # shows the https://<machine>.<tailnet>.ts.net URL
   ```
4. Browse `https://<machine>.<tailnet>.ts.net` from any tailnet device.
5. Keep 80/443 closed publicly (already done by `ec2-setup.sh`); SSH via the
   tailnet too once bootstrapped.
6. Keep the app login + JWT — Tailscale is the network layer, not a substitute
   for auth.
7. `IP_ALLOWLIST` can be your tailnet CIDR (`100.64.0.0/10`) or left tighter.
   With `tailscale serve` proxying locally, the app sees the client's tailnet IP.
