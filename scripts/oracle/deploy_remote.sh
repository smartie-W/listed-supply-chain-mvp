#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash scripts/oracle/deploy_remote.sh <VM_PUBLIC_IP> [SSH_KEY_PATH]"
  exit 1
fi

VM_IP="$1"
SSH_KEY="${2:-$HOME/.ssh/id_rsa}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -i "$SSH_KEY")
REMOTE="ubuntu@${VM_IP}"

echo "[1/4] Sync code to VM..."
rsync -az --delete \
  --exclude ".git" \
  --exclude "node_modules" \
  --exclude "*.log" \
  --exclude ".DS_Store" \
  -e "ssh ${SSH_OPTS[*]}" \
  ./ "${REMOTE}:/var/www/listed-supply-chain-mvp/"

echo "[2/4] Install runtime (idempotent)..."
ssh "${SSH_OPTS[@]}" "${REMOTE}" "sudo bash /var/www/listed-supply-chain-mvp/scripts/oracle/bootstrap_vm.sh"

echo "[3/4] Start service with PM2..."
ssh "${SSH_OPTS[@]}" "${REMOTE}" "cd /var/www/listed-supply-chain-mvp && mkdir -p /var/log/listed-supply-chain-mvp && pm2 startOrReload ecosystem.config.cjs --env production && pm2 save && sudo env PATH=\$PATH:/usr/bin pm2 startup systemd -u ubuntu --hp /home/ubuntu >/tmp/pm2_startup_cmd.txt && bash /tmp/pm2_startup_cmd.txt || true"

echo "[4/4] Health check..."
ssh "${SSH_OPTS[@]}" "${REMOTE}" "curl -s http://127.0.0.1:8090/api/health && echo && curl -s http://127.0.0.1/api/health"

echo "deploy complete: http://${VM_IP}"
