# ipaklink 埋点事件验收表

## 1. 范围和运行方式

本地 Docker Compose 合成数据验收覆盖 26 个事件，包括 19 个 App 事件、6 个业务事件和 1 个语料状态事件。真实页面触发、登录态、业务事务投递及指标口径仍需上游联调。本文记录事件接入、拒绝、重投和数据落点验证。

本地采集策略开启后，从仓库根目录运行。

```powershell
Get-Content -Raw scripts/smoke-event-catalog.py | docker compose exec -T api python -u -
```

```bash
docker compose exec -T api python -u - < scripts/smoke-event-catalog.py
```

## 2. 事件验收结果

| 编号 | eventName | 场景 | 生产端与入口 | 缺失项用例 | 预期与实测 |
|---|---|---|---|---|---|
| EVT_01 | `app_session_start` | 会话启动 | App 模拟 → ANL_03 | launchSource | A · 通过 |
| EVT_02 | `app_background` | 进入后台 | App 模拟 → ANL_03 | occurredAt（公共字段） | A · 通过 |
| EVT_03 | `screen_view` | 进入页面 | App 模拟 → ANL_03 | entrySource | A · 通过 |
| EVT_04 | `screen_exit` | 退出页面 | App 模拟 → ANL_03 | visibleMs | A · 通过 |
| EVT_05 | `search_submit` | 提交搜索 | App 模拟 → ANL_03 | searchId | A · 通过 |
| EVT_06 | `search_result` | 搜索成功返回 | App 模拟 → ANL_03 | searchId | A · 通过 |
| EVT_07 | `search_result_open` | 打开搜索结果 | App 模拟 → ANL_03 | targetType | A · 通过 |
| EVT_08 | `entity_detail_view` | 详情可见 | App 模拟 → ANL_03 | targetType | A · 通过 |
| EVT_09 | `business_view_recorded` | 业务浏览入库 | 合成服务端 → Kafka K01 | viewRef 等专用字段 | B · 通过 |
| EVT_10 | `favorite_changed` | 收藏关系变更 | 合成服务端 → Kafka K01 | action 等专用字段 | B · 通过 |
| EVT_11 | `contact_supplier_click` | 点击联系 | App 模拟 → ANL_03 | contactFlowId | A · 通过 |
| EVT_12 | `inquiry_submit` | 发起询盘请求 | App 模拟 → ANL_03 | clientRequestId | A · 通过 |
| EVT_13 | `inquiry_submit_failed` | 询盘请求失败 | App 模拟 → ANL_03 | clientRequestId | A · 通过 |
| EVT_14 | `inquiry_created` | 询盘持久化成功 | 合成服务端 → Kafka K01 | inquiryRef 等专用字段 | B · 通过 |
| EVT_15 | `chat_session_open` | 打开会话 | App 模拟 → ANL_03 | dialogRef | A · 通过 |
| EVT_16 | `message_send_succeeded` | 消息持久化成功 | 合成服务端 → Kafka K01 | messageRef 等专用字段 | B · 通过 |
| EVT_17 | `notification_open` | 打开通知 | App 模拟 → ANL_03 | category | A · 通过 |
| EVT_18 | `message_translation_toggle` | 切换译文 | App 模拟 → ANL_03 | displayMode | A · 通过 |
| EVT_19 | `translate_started` | 启动翻译 | App 模拟 → ANL_03 | taskId | A · 通过 |
| EVT_20 | `translate_completed` | 翻译终态 | App 模拟 → ANL_03 | taskId | A · 通过 |
| EVT_21 | `translate_sync_completed` | 翻译同步终态 | 合成服务端 → Kafka K01 | recordRef 等专用字段 | B · 通过 |
| EVT_22 | `translate_copy` | 复制译文 | App 模拟 → ANL_03 | mode | A · 通过 |
| EVT_23 | `account_registered` | 注册提交成功 | 合成服务端 → Kafka K01 | channel | B · 通过 |
| EVT_24 | `corpus_asset_status_changed` | 语料审核状态变更 | 语料审核 → MongoDB Outbox | reasonCode | C · 通过 |
| EVT_25 | `card_impression` | 卡片达到曝光阈值 | App 模拟 → ANL_03 | targetType | A · 通过 |
| EVT_26 | `card_impression_click` | 点击已曝光卡片 | App 模拟 → ANL_03 | targetType | A · 通过 |

## 3. 预期与实测判定

| 规则 | 有效事件 | 缺失字段 | 重复投递 | 已核对的数据落点 |
|---|---|---|---|---|
| A | 19 条均获 `accepted` | 19 条均获 `rejected` | 原 ID 重传均获 `duplicate` | PostgreSQL 有 19 条已发布回执，ClickHouse 有 19 个对应 ID |
| B | 6 类签名消息均可消费 | 每类缺失必填专用字段的消息均产生与原分区、位点匹配的 DLQ 记录 | 重投产生原始副本，按 `eventId` 核对仍为 6 个不同 ID | Kafka `analytics.event`、ClickHouse `events_raw`、Kafka `analytics.event.dlq.v1` |
| C | 语料审核状态从 pending 到 approved，Kafka 消息包含该状态 | 缺少审核原因的输入校验失败 | 重复审核产生一条状态 Outbox 记录 | MongoDB 已发布状态 Outbox、Kafka `analytics.corpus.status` |

App 缺字段用例每个事件删除一个字段。EVT_02 没有必填专用字段，验证公共字段 `occurredAt`。业务用例清空专用字段对象。语料审核使用真实的域校验、审核事务与后台 Outbox；现有 `smoke-remaining.py` 另行验证审核 HTTP 接口。

## 4. 查找字段及代码

- [事件字段索引与详细契约](./ipaklink-埋点数据后台接口文档.md#42-事件索引)
- [卡片事件契约](./ipaklink-埋点数据后台API接口文档.md#63-卡片曝光与点击契约)
- [App 事件 Schema](../backend/src/ipaklink_analytics/infrastructure/schema/postgres/003_app_schemas.sql) · [卡片 Schema](../backend/src/ipaklink_analytics/infrastructure/schema/postgres/007_card_schemas.sql)
- [业务事件模型](../backend/src/ipaklink_analytics/domain/business_events.py) · [语料审核状态产生代码](../backend/src/ipaklink_analytics/application/corpus_review.py) · [验收脚本](../scripts/smoke-event-catalog.py)
