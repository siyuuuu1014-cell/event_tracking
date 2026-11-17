# ipaklink 埋点系统交接文档

## 1. 接手起点

| 项目 | 当前状态 |
|---|---|
| 代码仓库 | [event_tracking](https://github.com/siyuuuu1014-cell/event_tracking)，交接从 [dev](https://github.com/siyuuuu1014-cell/event_tracking/tree/dev) 开始 |
| 已验证的代码基线 | [2907144](https://github.com/siyuuuu1014-cell/event_tracking/commit/2907144a3005cd3dabc42fc59bff792bc257fbd9)。本文记录该版本的代码、联调结果和待办 |
| 已实现 | 40 个埋点 HTTP 接口。采集、Kafka 消费、ClickHouse 分析、快照、导出、语料治理和行为主体清理已在本地跑通 |
| 已验收 | 从新镜像和空数据卷初始化。用合成数据验收 26 个事件，回归报表、导出、权限与清理。生产 Compose 覆盖和 Nginx 模板已在本地验证 |
| Web 现状 | React 已接员工登录、退出、时区和快照 ID 展示。总览图表、页面停留、曝光、质量和后台管理等完整页面待开发 |
| 外部依赖 | 真实 App 事件、业务服务投递与源库对账、实际告警渠道和远程服务器部署待联调或验收 |

这个仓库是独立埋点后台。App 页面触发代码在 App 仓库实现。询盘、消息和注册等成功事实由原业务服务在持久化后产生。接手人可以先用本仓库的脚本独立验证采集和看板。 [事件验收表](./ipaklink-埋点事件验收表.md)

## 2. 数据流与代码入口

```text
App ── HTTPS → FastAPI → Redis 限流与 PostgreSQL 受理回执 → Kafka analytics.event
业务服务 ── 已提交业务事实 → 业务适配器 → Kafka analytics.event
Kafka analytics.event → consumer → ClickHouse events_raw → jobs 构建并发布快照 → API → React

语料审核、移除和到期 → MongoDB 状态与 Outbox → Kafka analytics.corpus.status
快照发布和回切 → PostgreSQL 快照与 Outbox → Kafka analytics.dashboard.updated.v1
```

| 模块 | 入口与职责 |
|---|---|
| HTTP 采集和报表 | [api/](../backend/src/ipaklink_analytics/api/) 注册采集、报表、导出、鉴权、语料与运维接口。路由的 `operation_id` 对应接口文档中的 `IPK_ANL_*` |
| App 事件接入 | [application/ingest.py](../backend/src/ipaklink_analytics/application/ingest.py) 按 `eventName + schemaVersion` 校验并投递。 [基础 App Schema](../backend/src/ipaklink_analytics/infrastructure/schema/postgres/003_app_schemas.sql) · [卡片 Schema](../backend/src/ipaklink_analytics/infrastructure/schema/postgres/007_card_schemas.sql) |
| Kafka 至仓库 | [consumer.py](../backend/src/ipaklink_analytics/consumer.py) 验证消息并写 ClickHouse，写入确认后提交消费位点。无效消息进入 `analytics.event.dlq.v1`。 [业务事件模型](../backend/src/ipaklink_analytics/domain/business_events.py) |
| 后台作业 | [job_worker.py](../backend/src/ipaklink_analytics/application/job_worker.py) 编排快照、导出、修复、语料、清理和本地告警。 [快照构建](../backend/src/ipaklink_analytics/application/build_pages.py) · [快照通知](../backend/src/ipaklink_analytics/infrastructure/snapshot_outbox.py) · [质量聚合与对账](../backend/src/ipaklink_analytics/application/quality_facts.py) |
| 存储与迁移 | PostgreSQL 管采集回执、员工权限、审计和快照控制信息。ClickHouse 管原始事件、分析事实与聚合。MongoDB 管语料、作业与语料 Outbox。Redis 管限流、会话和缓存。MinIO 保存导出文件。 [统一结构目录](../backend/src/ipaklink_analytics/infrastructure/schema/) · [Compose 服务配置](../compose.yaml) |
| Web | [web/src/main.jsx](../web/src/main.jsx) 是当前员工登录与工作台入口。 [Vite 代理](../web/vite.config.js) 将内部 API 请求转发到本机后端 |

统计业务日使用 `Asia/Tashkent`，日期区间采用左闭右开。Kafka 至少一次投递，`events_raw` 可看到重复副本，指标构建按事件 ID 去重。看板使用 `ANL_10` 获取当前快照，同屏查询复用同一个 `snapshotId`。数据已入仓后，还需等对应日期的快照构建完成才会反映到报表。 [技术设计](./ipaklink-埋点数据后台技术设计.md)

## 3. 接手当天跑起来

Windows 使用 PowerShell 7、Docker Desktop 与 Compose 插件。启动 Web 需要 Node.js 22.12 或更新版本。以下从克隆后的仓库根目录执行。已有工作副本可从 `Set-Location event_tracking` 之后的步骤开始。

```powershell
git clone --branch dev https://github.com/siyuuuu1014-cell/event_tracking.git
Set-Location event_tracking
if (-not (Test-Path .env)) {
    $signingKey = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(48))
    "ANALYTICS_SIGNING_SECRET=$signingKey" | Set-Content -Encoding ascii .env
}
docker compose up -d --build
docker compose ps -a
Invoke-RestMethod http://127.0.0.1:58000/health/ready
```

`migrate`、`kafka-init`、`mongo-init` 和 `object-storage-init` 正常完成时显示 `Exited (0)`。API、消费者和 jobs 应保持运行。健康检查通过后，打开 [本地 Swagger](http://127.0.0.1:58000/docs)。采集策略初始关闭，做本地测试前启用。

```powershell
docker compose exec api python -m ipaklink_analytics.infrastructure.postgres.local_policy enable
docker compose exec api python -m ipaklink_analytics.application.admin_accounts create admin.handoff "平台管理员" --platform-admin
```

创建命令会交互提示输入两次密码，至少 12 个字符。若账号已存在，使用已获授权的账号或选择另一个用户名。新建普通员工时省略 `--platform-admin`，再由管理员经 `ANL_36` 授予地区或数据集。普通员工角色编码为 `analyst`。批准受控操作工单用 `admin_accounts approve-ticket`，有效期为 1 小时。现有旧 `operator` 授权已由迁移归入普通员工。 [账号维护命令](../README.md#页面分析与后台鉴权)

另开终端，在仓库根目录运行以下命令。打开 [本地 React 工作台](http://127.0.0.1:5173/) 登录。登录后可核对统计时区与当前快照。 [Web 启动说明](../README.md#react-管理后台)

```powershell
npm --prefix web ci
npm --prefix web run dev
```

保留与数据卷配套的本地 `.env` 和签名密钥。复用旧数据卷时重新生成密钥，会影响历史匿名身份和 Kafka 签名。员工密码存哈希，由维护人员为接手人新建账号。部署凭据通过公司现有的凭据渠道交接。 [部署环境变量模板](../.env.production.example)

## 4. 本地验收与排查

| 要验证的链路 | 入口与判定 |
|---|---|
| 26 个事件的接入和数据落点 | [smoke-event-catalog.py](../scripts/smoke-event-catalog.py)。检查 19 个 App 事件的逐条回执、6 类业务事实的仓库与死信、1 个语料事件的 Outbox 与 Kafka |
| 登录、授权、报表及回切 | [smoke-reports.py](../scripts/smoke-reports.py)。检查地区范围、重复授权、撤销、审计、登录和回切 |
| 报表导出、语料和修复 | [smoke-remaining.py](../scripts/smoke-remaining.py)。覆盖 S3 导出及过期、语料导入和审核、回放和告警记录等 |
| 页面曝光与 CTR | [smoke-cards.py](../scripts/smoke-cards.py)。检查曝光门槛、点击归因、去重和小样本抑制 |
| 真实业务接入前的本地替代验收 | [smoke-business.py](../scripts/smoke-business.py) 跑合成业务事实与指标。[smoke-business-reconciliation.py](../scripts/smoke-business-reconciliation.py) 比较合成源事实和实际入仓数据，覆盖漏采、多采、重复与水位过滤 |

脚本从仓库根目录执行。例如运行下面的事件验收。连续跑多个采用匿名上报的脚本可能遇到 `429`，按 `Retry-After` 留出限流窗口后再重跑对应脚本。 [完整验收用例及结果](./ipaklink-埋点事件验收表.md)

```powershell
Get-Content -Raw scripts/smoke-event-catalog.py | docker compose exec -T api python -u -
```

| 现象 | 优先检查 |
|---|---|
| Web 打不开或登录后暂无权限 | 检查 `5173` 的 Vite 和 `58000/health/ready`。核对员工账号是否启用，是否有有效地区授权或内置管理员授权 |
| HTTP 200 但事件没显示 | 查看 `events/report` 的逐条 `results`，再按 `eventId` 核对 PostgreSQL 受理回执、Kafka 消费、ClickHouse `events_raw` 和报表快照。 `accepted` 表示采集确认，报表随后构建 |
| 已入仓但看板数不对 | 核对业务日期、应用版本和地区筛选。用 `ANL_10` 检查快照，用 `ANL_17` 查受理、入仓和 Kafka 积压。在测试环境可运行构建命令指定日期重算 |
| 导出过期或旧快照返回 409 | 先用 `ANL_10` 取当前快照，再重新创建导出或查询报表。主体清理会使旧快照和关联导出失效 |
| 告警只在本地出现 | 当前质量告警使用 `localLog`。在 `jobs` 的日志中搜 `quality.alert`，用 `ANL_42` 查投递记录 |

```powershell
docker compose logs --tail 80 api consumer jobs
docker compose exec -T jobs ls /app/logs
docker compose exec -T jobs tail -n 100 /app/logs/jobs-1.log
docker compose exec api python -m ipaklink_analytics.application.build_pages
```

容器应用日志保存在共享的 `api-logs` 数据卷。文件名含进程号，先用 `ls` 确认实际名称。排查采集接口时可用响应的 `X-Request-Id` 对应日志。更新服务时保留数据卷，`docker compose down -v` 会删除数据及日志。

## 5. 需要持续维护的操作

- **数据库变更**。PostgreSQL 和 ClickHouse 新增编号迁移，MongoDB 校验与索引集中维护。已经执行的迁移有校验和，改结构时追加脚本。保留现有数据卷，按部署手册执行迁移和版本更新。 [结构目录](../backend/src/ipaklink_analytics/infrastructure/schema/) · [数据字典](./ipaklink-埋点数据后台数据字典.md)
- **语料到期**。导入时使用数据集 `retentionDays` 计算条目 `expiresAt`。到期任务清除正文或文件引用，保留状态和历史，并通过专用 Topic 投递。验收样例可设一天，实际期限按数据集配置。 [语料作业](../backend/src/ipaklink_analytics/application/corpus_worker.py)
- **行为主体清理**。通过 `subject_erasure` 维护命令按分析 `actor_key` 排队。任务删除采集回执与入仓行为，重算看板，失效旧快照和导出，并抑制后续重传与 Kafka 回放。清理尚未完成时报表会暂不可用，失败可重新排队。恢复历史备份时保留 PostgreSQL 清理记录，并在开放请求前按需用 `--reapply` 重清理。 [主体清理操作](../README.md#行为主体清理)

## 6. 接手后的待办与验收口径

| 接下来做什么 | 前置条件与交付判定 |
|---|---|
| **完整 Web 看板** | 可以立即开始。沿用现有 React 和 API，先接元数据与总览，再接页面、功能、漏斗、留存和质量。用合成数据验证筛选、分页、空态、权限范围及快照切换 |
| **App 匿名采集** | 拿到 App 代码和可运行测试包。在 App 实现 `track()`、本地队列、批量上报和逐条回执处理。先接一页的 `screen_view` / `screen_exit`，验证同一 `pageViewId`、离线补发及停留时长，再接搜索、曝光和其他事件 |
| **App 登录态采集** | 确认上游登录令牌。当前后端支持 HS256 JWT，并核对签发方、受众和账号标识。联调匿名转登录、退出和切号，确认事件按照原身份发送 |
| **业务适配与真实对账** | 与业务后端负责人确认询盘成功、持久化消息和业务浏览等权威事实的生产方式。按契约补齐 `inquiries.created` 适配及业务 Outbox，按同一业务日和水位核对源库与仓库。当前 `ANL_17` 跑通的是 App 回执与入仓对账，正式业务源仍待接入 |
| **外部告警** | 确定实际接收渠道和接收人，以测试异常验证通知送达、重复窗口去重和失败记录 |
| **服务器上线** | 确定 Linux 主机、域名、证书和备份位置，按部署手册启动独立环境，验证健康检查、HTTPS 登录、采集、导出以及备份恢复。现有配置已通过本机验证 |

App 测试包配置埋点后台的可达 HTTPS 地址，依次使用 `GET /ipaklink/v1/analytics/config`、`POST /ipaklink/v1/analytics/anonymous-tokens` 和 `POST /ipaklink/v1/analytics/events/report`。匿名上报带 `X-Analytics-Token`，登录态带 App 的 Bearer。逐条处理 `results`，确认与重复回执出队，拒绝记录原因，重试使用原 `eventId`。登录态验签配置为 `ANALYTICS_APP_JWT_ISSUER`、`ANALYTICS_APP_JWT_AUDIENCE` 和 `ANALYTICS_APP_JWT_SECRET`。 [App 采集契约](./ipaklink-埋点数据后台API接口文档.md#31-采集接入)

交接时请确认 **GitHub 仓库权限、接手人的平台管理员账号、可使用的 App 测试包、业务服务联系人和部署环境负责人**。已有服务器或数据卷需要继续沿用时，同时确认数据归属、备份位置和对应的签名密钥由谁维护。

## 7. 文档索引

- 研发范围、功能编号和验收条件。 [功能清单](./ipaklink-埋点数据后台功能清单.md)
- HTTP 请求响应、字段校验、状态码和 Mock。 [API 接口文档](./ipaklink-埋点数据后台API接口文档.md)
- 原始事件索引、业务事件字段及卡片扩展。 [事件索引](./ipaklink-埋点数据后台接口文档.md#42-事件索引) · [卡片扩展契约](./ipaklink-埋点数据后台API接口文档.md#63-卡片曝光与点击契约)
- 表结构、键、索引与应用分层。 [数据字典](./ipaklink-埋点数据后台数据字典.md) · [技术设计](./ipaklink-埋点数据后台技术设计.md)
- 联调脚本、部署和日常维护。 [README](../README.md) · [26 个事件验收表](./ipaklink-埋点事件验收表.md) · [单机部署手册](./ipaklink-埋点后台部署手册.md)
