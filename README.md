# ipaklink Event Tracking

独立的埋点后台工程。ipaklink App 通过版本化采集接口接入。其业务服务和业务数据由原项目维护。

## 当前实现

- Python 3.12 + FastAPI，应用层、领域层、端口与存储适配器独立。
- `GET /health/live`、`GET /health/ready`、ANL_01–03 采集接口，以及页面分析、快照查询与回切和采集配置治理接口。
- PostgreSQL 控制面、权限、审计、作业及快照关系，ClickHouse 原始事件、规范事件视图、事实和聚合结构，以及 MongoDB 语料、作业与出站集合。Redis 与 Kafka 负责采集入口和异步传输。
- 匿名凭据绑定安装和身份阶段，批量上报使用 Redis 限流和 PostgreSQL 幂等受理记录。Kafka 消费进程将安全事件写入 ClickHouse `analytics.events_raw` 后手动提交位点。开发种子策略默认关闭采集。
- 登录态需配置上游签发方、受众和单独的 HS256 验签密钥 `ANALYTICS_APP_JWT_ISSUER`、`ANALYTICS_APP_JWT_AUDIENCE`、`ANALYTICS_APP_JWT_SECRET`。未配置时登录态返回 503，匿名路径可独立联调。卡片曝光和点击支持匿名上报。消费者已支持 K01 的服务端业务事件信封，真实业务侧适配器仍需联调。
- API 写入结构化 JSON Lines `.log`，请求以 `X-Request-Id` 追踪。

## Docker 启动

本地开发配置仅监听回环地址。数据库账号和无认证的中间件设置仅用于本地联调。

```powershell
if (-not (Test-Path .env)) {
    $key = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(48))
    "ANALYTICS_SIGNING_SECRET=$key" | Set-Content -Encoding ascii .env
}
docker compose up -d --build
docker compose exec api python -m ipaklink_analytics.infrastructure.postgres.local_policy enable
docker compose ps -a
Invoke-RestMethod http://127.0.0.1:58000/health/live
Invoke-RestMethod http://127.0.0.1:58000/health/ready
Invoke-RestMethod 'http://127.0.0.1:58000/ipaklink/v1/analytics/config?platform=android&appVersion=1.2.0&installId=3030e190-a030-4bba-9cec-057d5f419f01'
```

OpenAPI 调试入口为 `http://127.0.0.1:58000/docs`。迁移、Kafka Topic 初始化和 MongoDB 副本集初始化是一次性服务，正常完成显示退出码 0。ClickHouse 首次启动创建原始表，后续 ClickHouse、PG 与 MongoDB 模型由 `migrate` 服务追加创建。新迁移可运行 `docker compose run --rm --build migrate`，单环境同时只启一个迁移进程。MongoDB 语料到期索引由后续治理作业读取，保留传播语义。

`events_canonical` 当前为基于 `events_raw` 按事件 ID 取最早一条的视图，覆盖数据卷里已有和新增事件。原始数据保留全部投递记录。事实与聚合表等待报表构建任务写入，不能直接把排序键当唯一约束。构建任务需按固定水位从原始层去重，并生成独立代际。

## 数据库结构文件

数据库结构统一放在 `backend/src/ipaklink_analytics/infrastructure/schema/`。

| 位置 | 用途 |
|---|---|
| `postgres/` | PostgreSQL 编号 SQL 迁移 |
| `clickhouse/` | ClickHouse 编号 SQL 迁移 |
| `clickhouse/bootstrap/001_events.sql` | Docker 首次初始化 ClickHouse 时创建 `events_raw` |
| `mongo.py` | MongoDB 集合、校验规则与索引 |
| `migrate.py` | Compose 的统一迁移入口 |

数据库访问代码继续按组件放在 `infrastructure/` 下。修改 PostgreSQL 或 ClickHouse 的结构时，新增编号 SQL，保留已执行迁移的文件名与内容。已有 PostgreSQL 迁移命令和 Docker 首次初始化流程沿用原入口。

## 页面分析与后台鉴权

在采集事件入仓后运行 `docker compose exec api python -m ipaklink_analytics.application.build_pages`，构建页面访问、列表卡片曝光、搜索、翻译、活跃、询盘和人工回复事实，以及留存和日指标聚合，并发布同一快照。默认最近 7 个完整业务日加当天，可通过 `--from-date YYYY-MM-DD --to-date YYYY-MM-DD` 指定左闭右开范围。`jobs` 服务也会自动构建并发布。搜索、翻译功能分析按快照读事实表，按企业或商品范围查询且无额外目标筛选的 F02 也直接读搜索事实。兼容缺少派生数据集的历史快照。经营总览日趋势已接入日活、匿名 UV 和翻译成功率的日聚合。跨日 UV、无事实覆盖的维度和受控会话仍使用快照水位对应的查询路径。留存查询优先读取该快照的预聚合表，历史快照缺少该数据集时按原始事件计算。页面、曝光、留存的查询结果按快照与实际筛选条件存入 Redis 60–70 秒。本地查询限制单次最多读取 100000 条匹配事件。卡片报表使用 `ANL_23`，本地可通过 `Get-Content -Raw scripts/smoke-cards.py | docker compose exec -T api python -` 验证 20 次曝光、1 次归因点击、区域权限和低样本保护。

后台维护人员使用交互命令开通员工账号，密码使用 Argon2id 存入 PostgreSQL。生产配置使用 HTTPS，`ANALYTICS_ENVIRONMENT` 设为部署环境名，`ANALYTICS_ADMIN_ORIGIN` 设为浏览器实际访问的 HTTPS 源，React 与 API 使用同源代理。

```powershell
docker compose exec api python -m ipaklink_analytics.application.admin_accounts create admin.internal "平台管理员" --platform-admin
docker compose exec api python -m ipaklink_analytics.application.admin_accounts create analyst.internal "分析师"
docker compose exec api python -m ipaklink_analytics.application.admin_accounts approve-ticket admin.internal OPS-127
docker compose exec api python -m ipaklink_analytics.application.admin_accounts disable analyst.internal
```

`approve-ticket` 为已核验的内部工单给指定平台管理员发放 1 小时操作权限。新增或撤销分析师、运营授权，以及发布和回切需要匹配工单。平台管理员账号由维护人员初始化。开户命令通过终端提示读取和确认密码。禁用员工账号后，已有会话下一次请求立即失效。

按 Swagger 的 `POST /ipaklink/v1/internal/analytics/sessions` 登录，浏览器会自动保存会话 Cookie。使用 `DELETE /ipaklink/v1/internal/analytics/sessions/current` 退出。从仓库根运行 `Get-Content -Raw scripts/smoke-reports.py | docker compose exec -T api python -` 验证采集、报表和员工账号链路。脚本禁用本轮创建的测试账号并保留对应审计。

从仓库根目录运行 `pwsh -File scripts/smoke-ingest.ps1`，验证凭据、持久回执、重复上报、非法属性以及实际 ClickHouse 落库。不要删除持久化开发密钥后复用旧数据卷，不然历史匿名身份与 Kafka 签名不再匹配。可运行 `docker compose exec api python -m ipaklink_analytics.infrastructure.postgres.local_policy disable` 关闭采集。Kafka 是至少一次链路，消费者故障重启后原始表可以保留同一事件的重复投递，后续分析按 eventId 去重。

## 业务接口联调

`docker compose up -d --build` 启动 API、消费者和独立 `jobs` 服务。作业服务处理报表快照、导出、修复和语料任务。单轮执行可用 `docker compose exec jobs python -m ipaklink_analytics.application.job_worker --once`，通常使用常驻进程即可。Compose 将新导出 CSV 写入本地 S3 兼容对象存储的 `analytics-exports` 桶。现有短时链接继续由 API 流式下载，作业过期后后台删除对象。旧本地导出在有效期内可继续下载。修复失败会写入本地通知投递记录及 `.log`。

语料导入和审核同步的 `inputRef` 指向维护端预置的 MongoDB `corpus_import_inputs` 文档，并与活跃数据集的修订和用途匹配。审核同步的 `items` 使用 `itemId`、`expectedItemRevision`、`status` 和 `reasonCode`，与 ANL_41 使用相同的状态转换和版本检查。通过 `docker compose exec api python -m ipaklink_analytics.application.queue_review_sync <operatorId> <datasetCode> <purposeCode> <ticketId> <inputRef> <idempotencyKey> <datasetRevision>` 排队，使用 ANL_19 查询结果。接口及事件仅回传审核元数据。审核和删除通过 MongoDB Outbox 传播，审计写入 PostgreSQL，状态事件发送到 Kafka 的 `analytics.corpus.status`。

本地可运行 `Get-Content -Raw scripts/smoke-remaining.py | docker compose exec -T api python -`，逐条联调新增 18 个业务接口。质量端点从采集回执、消费者死信确认和 Kafka 分区定时采样读取受理量、延迟、App 对账、拒绝、重复、限流、重试、死信、积压和停滞分区。观测维度不适用或暂无采样时返回空值。作业进程每分钟检查 Kafka 已提交位点和末端位点，连续两次超过积压阈值或存在停滞时产生本地通知，按 5 分钟窗口去重，阈值可用 `ANALYTICS_QUALITY_LAG_THRESHOLD` 配置。业务权威数据与外部通知渠道需在接入后验收。

使用 `Get-Content -Raw scripts/smoke-architecture.py | docker compose exec -T api python -` 验证翻译任务终态、跨地区 D1 留存、Redis 缓存和质量采样。

使用 `Get-Content -Raw scripts/smoke-business.py | docker compose exec -T api python -` 可通过本地 Kafka 验证已提交询盘、询盘归因、人工回复和收藏分析。脚本使用合成业务事件，真实询盘、聊天与收藏事件由各自的业务后端提供。

## React 管理后台

在本地 API 运行后，从仓库根目录执行以下命令。打开 `http://127.0.0.1:5173`，使用维护命令开通的内部账号登录。Vite 会将后台 API 请求转发到本地 `58000` 端口。

```powershell
Set-Location web
npm ci
npm run dev
```

执行 `npm run build` 可生成 `web/dist`。正式环境将这些静态文件和 API 配在同一个 HTTPS 源下。

## 本地 Python 和测试

```powershell
Set-Location backend
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e '.[test]'
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ipaklink_analytics.api.serve --port 58000
```

测试无需数据库。本地启动器在 Windows 使用 Psycopg 支持的 SelectorEventLoop。若用本地 Python 启动 API，可从仓库根目录运行 `docker compose up -d migrate kafka-init mongo-init redis clickhouse` 初始化依赖，并确认初始化任务退出码为 0。需要切换连接时设置 `ANALYTICS_POSTGRES_DSN`、`ANALYTICS_REDIS_URL`、`ANALYTICS_KAFKA_BOOTSTRAP_SERVERS`、`ANALYTICS_CLICKHOUSE_HOST`、`ANALYTICS_CLICKHOUSE_PORT`、`ANALYTICS_MONGO_URI` 等环境变量。

`docs/` 保存当前冻结功能、技术设计、数据字典和 API 契约的起步版本。

