# ipaklink 埋点后台单机部署

## 1. 准备环境

安装 Docker Engine、Docker Compose 2.24.4 或更新版本、Nginx 和Node.js 22.12 或更新版本。本文示例代码路径为 `/opt/event_tracking`，域名为 `tracking.example.com`。服务器准备好域名解析和证书后，将示例域名换成实际域名。

在仓库根目录执行 `cp .env.production.example .env.production` 并填写变量。每个密钥分别使用 `openssl rand -hex 32` 生成。PostgreSQL 密码使用十六进制，以便在 DSN 中直接使用。签名密钥应随环境持久保管，旧队列与事件签名依赖它。登录态 App 接入时再填写验签参数。

Compose 内部运行 API、消费者、作业和存储服务。宿主机 API 仅监听回环地址，Nginx 是统一 HTTPS 入口。初次部署使用独立数据卷。

## 2. 初始化和验收

```bash
dc() { docker compose --env-file .env.production -f compose.yaml -f compose.production.yaml "$@"; }
dc config --quiet
dc up -d --build
dc ps -a
curl -fsS http://127.0.0.1:58000/health/ready
npm --prefix web ci
npm --prefix web run build
dc exec api python -m ipaklink_analytics.application.admin_accounts create admin.internal 平台管理员 --platform-admin
```

确认 `migrate`、`kafka-init`、`mongo-init` 和 `object-storage-init` 正常退出。将 [Nginx 模板](../deploy/nginx/tracking.conf)复制到服务器站点配置位置，修改域名、证书路径、静态文件目录和 API 端口，执行 `nginx -t` 后加载。浏览器打开 HTTPS 地址，验证后台登录、看板和导出。

若采用 Docker 桥接时 API 将所有代理请求识别为同一个来源 IP，用 `docker network inspect event-tracking-production_default` 确认宿主机网关，将实际代理对端 IP 加入 `ANALYTICS_PROXY_TRUST`，再重建 API 容器。Nginx 使用连接来源覆盖转发 IP 头，采集限流可按客户端区分。

在测试环境使用相同部署拓扑做采集演练。首次开启实际采集时，经有工单的平台管理员发布策略，并用允许的平台和版本验证一条端到端数据。

## 3. 更新和备份

发布前记录 Git 提交号，备份 PostgreSQL、ClickHouse、MongoDB 及所需的 MinIO 对象。根据缺失数据的补偿窗口保留 Kafka 数据和位点。对变更数据库结构的版本，在维护窗口执行迁移，再更新 API、消费者、作业与 Web 静态文件。更新应用容器时保留数据卷。

```bash
dc build api consumer jobs migrate
dc run --rm --no-deps migrate
dc up -d --no-deps api consumer jobs
npm --prefix web ci
npm --prefix web run build
curl -fsS http://127.0.0.1:58000/health/ready
```

启动器日志用 `dc logs --tail=100 api consumer jobs` 检查。应用结构化日志在共享卷 `/app/logs`。恢复历史数据卷时保留 PostgreSQL 主体清理记录，并按 [主体清理说明](../README.md#行为主体清理)重新应用对应作业，完成后再恢复请求。

## 4. 本地验收边界

业务对账脚本使用 PostgreSQL 临时合成源数据和实际 Kafka、ClickHouse。正式的业务源库查询仍需在真实业务接口确定后接入。当前模板留待实际 Linux 主机配置域名、证书、备份位置并验收投产。
