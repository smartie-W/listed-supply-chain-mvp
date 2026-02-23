# Oracle Always Free 部署（关机后可用）

## 目标
- 后端常驻在 Oracle VM（不依赖你本地电脑）
- 前端继续走 GitHub Pages
- 支持约 10 人并发查询（Always Free 上限内尽量优化）

## A. 创建 Oracle VM（仅需一次）
1. 登录 OCI 控制台，进入 `Compute -> Instances -> Create instance`
2. 关键配置：
   - Image: `Ubuntu 22.04`
   - Shape: `VM.Standard.E2.1.Micro`（Always Free）
   - Networking: 分配公网 IP
   - SSH key: 上传你本地公钥
3. 在 VCN 安全列表 / 网络安全组放行：
   - `22/tcp`（SSH）
   - `80/tcp`（HTTP）

## B. 一键部署后端
在你的电脑执行（把 `<VM_PUBLIC_IP>` 改成实际公网 IP）：

```bash
cd /Users/wang/Documents/codex/listed-supply-chain-mvp
bash scripts/oracle/deploy_remote.sh <VM_PUBLIC_IP> ~/.ssh/id_rsa
```

执行完成后后端地址就是：

```text
http://<VM_PUBLIC_IP>
```

## C. 把前端指向 Oracle 后端
编辑 `config.js`：

```js
window.APP_API_BASE = 'http://<VM_PUBLIC_IP>';
```

然后提交并推送：

```bash
git add config.js
git commit -m "chore: point api to oracle vm"
git push
```

GitHub Pages 更新后即可使用。

## D. 运维命令（在 VM）
```bash
pm2 ls
pm2 logs listed-supply-chain-mvp --lines 200
pm2 restart listed-supply-chain-mvp
curl -s http://127.0.0.1:8090/api/health
curl -s http://127.0.0.1/api/health
```

## E. 并发建议（Always Free）
- 该实例规格较小，10 人并发可用但高峰会波动
- 如果后续并发更高，建议迁移到付费规格（至少 2 vCPU）
