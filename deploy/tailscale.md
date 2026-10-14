# Option 3: Tailscale (best for dynamic home IPs — no public ports)

1. Install Tailscale on EC2 and your PC, join both to the same tailnet.
2. `sudo tailscale serve` the app, or bind Caddy to the tailnet IP only:
   ```
   # Caddyfile
   http://100.x.y.z:8080 {   # your EC2 tailnet IP
       reverse_proxy app:8000
   }
   ```
3. Close 80/443 publicly: `sudo ufw delete allow 80/tcp` etc. SSH via Tailscale too.
4. Keep the app login + JWT — Tailscale is the network layer, not a substitute for auth.
5. `IP_ALLOWLIST` can stay as your tailnet CIDR (`100.64.0.0/10`) or be left strict.
