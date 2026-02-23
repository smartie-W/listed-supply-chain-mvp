#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root: sudo bash scripts/oracle/bootstrap_vm.sh"
  exit 1
fi

apt-get update -y
apt-get install -y curl git nginx ufw build-essential

# Node.js 22 LTS
if ! command -v node >/dev/null 2>&1; then
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -y nodejs
fi

npm install -g pm2

mkdir -p /var/www/listed-supply-chain-mvp
mkdir -p /var/log/listed-supply-chain-mvp
chown -R ubuntu:ubuntu /var/www/listed-supply-chain-mvp /var/log/listed-supply-chain-mvp

cat >/etc/nginx/sites-available/listed-supply-chain-mvp <<'EOF'
server {
    listen 80;
    server_name _;

    client_max_body_size 10m;

    location / {
        proxy_pass http://127.0.0.1:8090;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

ln -sf /etc/nginx/sites-available/listed-supply-chain-mvp /etc/nginx/sites-enabled/listed-supply-chain-mvp
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl enable nginx
systemctl restart nginx

ufw allow OpenSSH || true
ufw allow 'Nginx Full' || true
ufw --force enable || true

echo "bootstrap complete"
