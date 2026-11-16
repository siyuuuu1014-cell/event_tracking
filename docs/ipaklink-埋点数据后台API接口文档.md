# ipaklink 埋点数据后台 API 接口文档

版本 1.0 · 研发联调与 Mock 契约稿 · 2026-11-16

依据 [冻结功能清单](./ipaklink-埋点数据后台功能清单.md) 和 [数据字典](./ipaklink-埋点数据后台数据字典.md)。本次接口交付覆盖全部 53 项功能。HTTP、Kafka 与内部作业共同承载功能，清单中的后台作业、数据迁移和安全处理不单独暴露数据库接口。本文用于后续人工契约评审和前端 Mock。

## 1. 路由树

统一前缀 `/ipaklink/v1`。现有 ipaklink App 和后台继续使用同一版本根。表中内部后台路由均位于 `/internal/analytics`，App 采集路由位于 `/analytics`。

### 1.1 采集接入

```text
GET   /analytics/config                                                  IPK_ANL_01  获取采集配置
POST  /analytics/anonymous-tokens                                        IPK_ANL_02  签发匿名凭据
POST  /analytics/events/report                                           IPK_ANL_03  批量上报事件
```

### 1.2 经营看板

```text
GET   /internal/analytics/metadata                                       IPK_ANL_10  读取快照和菜单
GET   /internal/analytics/overview                                       IPK_ANL_11  经营总览
GET   /internal/analytics/pages                                          IPK_ANL_12  页面分析
GET   /internal/analytics/features                                       IPK_ANL_13  功能分析
GET   /internal/analytics/funnels/{funnelCode}                           IPK_ANL_14  固定漏斗
GET   /internal/analytics/retention                                      IPK_ANL_15  活跃留存
GET   /internal/analytics/sessions/{sessionId}                           IPK_ANL_16  受控会话时间线
```

### 1.3 扩展报表

```text
GET   /internal/analytics/card-impressions                               IPK_ANL_23  卡片曝光与 CTR
POST  /internal/analytics/export-jobs                                    IPK_ANL_24  创建导出作业
GET   /internal/analytics/export-jobs/{jobId}                            IPK_ANL_25  导出作业详情
```

### 1.4 质量与修复

```text
GET   /internal/analytics/quality                                        IPK_ANL_17  数据质量与对账
POST  /internal/analytics/repair-jobs                                    IPK_ANL_21  创建修复作业
GET   /internal/analytics/repair-jobs/{jobId}                            IPK_ANL_22  修复作业详情
GET   /internal/analytics/repair-jobs                                    IPK_ANL_32  修复作业历史
GET   /internal/analytics/repair-jobs/{jobId}/failures                   IPK_ANL_33  修复失败明细
PUT   /internal/analytics/report-snapshots/current                       IPK_ANL_34  切换当前快照
GET   /internal/analytics/alert-deliveries                               IPK_ANL_42  质量通知投递记录
GET   /internal/analytics/report-snapshots                               IPK_ANL_44  快照列表
```

### 1.5 采集治理

```text
GET   /internal/analytics/event-schemas                                  IPK_ANL_26  事件模式列表
POST  /internal/analytics/event-schemas                                  IPK_ANL_27  创建事件模式
PUT   /internal/analytics/event-schemas/{eventName}/versions/{schemaVersion}/status IPK_ANL_28  发布或停用事件模式
GET   /internal/analytics/collection-policies                            IPK_ANL_29  采集策略列表
POST  /internal/analytics/collection-policies                            IPK_ANL_30  创建采集策略
PUT   /internal/analytics/collection-policies/current                    IPK_ANL_31  设置当前采集策略
GET   /internal/analytics/event-schemas/{eventName}/versions/{schemaVersion} IPK_ANL_43  事件模式详情
GET   /internal/analytics/collection-policies/{policyVersion}            IPK_ANL_45  采集策略详情
```

### 1.6 访问管理

```text
GET   /internal/analytics/access-grants                                  IPK_ANL_35  授权列表
POST  /internal/analytics/access-grants                                  IPK_ANL_36  创建授权
PUT   /internal/analytics/access-grants/{grantId}/status                 IPK_ANL_37  撤销授权
GET   /internal/analytics/audit-records                                  IPK_ANL_38  审计记录
```

### 1.7 语料治理

```text
GET   /internal/analytics/corpus/summary                                 IPK_ANL_18  语料状态汇总
GET   /internal/analytics/corpus/jobs/{jobRef}                           IPK_ANL_19  语料作业详情
POST  /internal/analytics/corpus/jobs                                    IPK_ANL_39  创建语料作业
GET   /internal/analytics/corpus/items                                   IPK_ANL_40  语料审核队列
PUT   /internal/analytics/corpus/items/{itemId}/review-state             IPK_ANL_41  更新语料审核状态
```

## 2. 公共契约

### 2.1 方法与版本

- **GET** 读取资源，路径字段替换占位符，查询字段放 URL query，请求体为空。GET 详情的 Request JSON 用 `path`、`query` 表达字段位置，前端可以据此写 Mock 路由。
- **POST** 接收批次、创建任务或新资源。保留原采集 action `/events/report` 和既有业务路由。对新增持久草稿与授权资源使用 201，异步导出和语料作业使用 202，既有修复作业保留 200 并在 data 里返回 `queued`。客户端依据状态枚举轮询。
- **PUT** 替换已有资源的子状态或当前指针，提供必要期望版本。目标不存在为 404，冲突为 409。重复置位按各接口的规则返回已存在结果。同一动作发生过后再重试，不重复落业务事实或执行状态转换。
- 各接口示例里的 `headers`、`path`、`query`、`body` 是展示结构。实际请求按对应位置发出。例如 Request JSON 中 `headers.Idempotency-Key` 发作真实头，只有 `body` 对象序列化为 JSON 请求体。

方法语义与状态码依据 [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html)。

### 2.2 命名、身份与字段类型

- JSON 键、query 及 path 参数统一 **camelCase**。枚举如业务 `eventName` 的 `screen_view` 是既有字符串值，数据库列用 snake_case，由服务映射。HTTP 标准头保留连字符。
- 普通后台请求使用 `Authorization: Bearer <SSO token>`，页面按角色授权。内部角色认证有效但没有范围或工单权限时返回 403。匿名上报传 `X-Analytics-Token`，登录上报用 Bearer，二者互斥。所有响应返回 `X-Request-Id`，诊断可据此关联。
- `!` 为必填，`?` 为可省。可空字段在对应场景标明；数字为 JSON 数字，Kafka 大 offset 为十进制字符串。错误和状态值使用稳定代码，客户端自行本地化。
- 日期为 `YYYY-MM-DD`，统计时区固定 `Asia/Tashkent`，`fromDate` 含、`toDate` 不含。报表默认最近 7 个完整业务日，通常最长 90 天，接口明细可缩短。若提供一端日期须同时提供另一端。时间戳为 UTC RFC3339，时长为毫秒，比例为 0–1。

### 2.3 响应与分页

统一外层仅有 `code`、`message`、`data`。code 恒等于 HTTP 状态，成功 message 为 `ok`，失败 message 是稳定业务代码、data 为 null。列表响应的 `data` 固定含 `list`、`total`、`page`、`size`。

```json
{"code":200,"message":"ok","data":{"list":[],"total":0,"page":1,"size":20}}
```

分页查询只传 `page`、`size`，默认 1 / 20，size 范围 1–100。API 网关若需兼容旧 `pageSize`，应在旧调用迁移层转换。这份契约的请求与响应均使用 `size`。页超出总数返回 200 和空数组。列表与 total 取相同权限和筛选，带 snapshotId 的续页维持初次查询快照。

### 2.4 查询状态与前端渲染

|值或字段|类型|Mock 渲染规则|
|---|---|---|
|data.meta|object|报表的公共元信息，包含 snapshotId、metricVersion、timezone、generatedAt、sourceWatermark、fromDate、toDate、status、provisional、coverage。首次无快照时 snapshotId、generatedAt、sourceWatermark 为 null。|
|data.meta.status|string|ready 正常、stale 延迟但有旧数据、partial 源覆盖不完整、noData 暂无可发布快照。|
|指标行|object|metricCode、value、unit、status 为固定语义。unit=count/ratio/ms；status=ready/provisional/suppressed/unavailable。分子分母在比率适用时返回。|
|计数 0、值 null|number/null|真实无事件显示 0；缺分母、未成熟、低基数压制或不可计算显示横线。被压制的相关值与可逆的分子分母均返回 null。|
|作业 status|string|queued/running/succeeded/failed。后台详情默认每 3 秒查询一次，终态后停止。失败显示 errorCode，作业查询本身仍为 200。|
|授权空列表|list=[]|展现空态，清楚区分 403 无权限和 404 单资源不存在。|

报表成功响应采用外层 → data → 行或 meta 三层对象结构。序列、分组指标、留存、漏斗分步使用扁平行。数组只作为行集合。批次的事件 properties 是按事件类型定义的末层对象。

### 2.5 错误字典

|HTTP|适用场景|Response JSON|
|---|---|---|
|400|缺必填字段、类型或枚举非法、超长、日期反向、筛选不兼容|`{"code":400,"message":"analytics.invalidRequest","data":null}`|
|401|凭据缺失、无效或过期；返回 WWW-Authenticate|`{"code":401,"message":"analytics.unauthenticated","data":null}`|
|403|已认证但范围、工单或角色拒绝|`{"code":403,"message":"analytics.permissionDenied","data":null}`|
|404|授权范围内资源不存在|`{"code":404,"message":"analytics.notFound","data":null}`|
|409|版本状态冲突、幂等内容变化或快照失效|`{"code":409,"message":"analytics.conflict","data":null}`|
|410|导出文件已经到期|`{"code":410,"message":"analytics.exportExpired","data":null}`|
|413|上传或解压后超过大小限制|`{"code":413,"message":"analytics.batchTooLarge","data":null}`|
|429|达到限流，Retry-After 为等待秒数|`{"code":429,"message":"analytics.rateLimited","data":null}`|
|500|非预期内部异常，保留追踪 ID|`{"code":500,"message":"analytics.internalError","data":null}`|
|503|暂时不可服务，可按退避重试|`{"code":503,"message":"analytics.temporarilyUnavailable","data":null}`|
|400|筛选不适用于当前功能或漏斗|`{"code":400,"message":"analytics.unsupportedFilter","data":null}`|
|403|采集策略关闭|`{"code":403,"message":"analytics.collectionDisabled","data":null}`|
|409|历史快照过期，刷新元数据后重查|`{"code":409,"message":"analytics.snapshotExpired","data":null}`|
|409|同一幂等键的语义负载发生变化|`{"code":409,"message":"analytics.idempotencyConflict","data":null}`|
|409|版本并发冲突或状态转换不允许|`{"code":409,"message":"analytics.versionConflict","data":null}`|

业务错误使用 message 的稳定值，客户端不通过中文文案猜测分支。429 遵循 `Retry-After`，自动重试仅适用于无副作用查询、保留同 eventId 的上报或已约定幂等的写入。[RFC 6585](https://www.rfc-editor.org/rfc/rfc6585.html)

## 3. 逐接口详细契约

### 3.1 采集接入

#### 3.1.1 IPK_ANL_01 获取采集配置

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_01|
|功能名称|获取采集配置|
|功能描述|读取当前采集开关、时间及限额。|
|URL|`/ipaklink/v1/analytics/config`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|App 可选 Bearer|
|功能编号|SDK-01|
|数据落点|analytics_collection_policy、analytics_event_schema|
|参数定义|`query:platform:string!,appVersion:string!,installId:UUID!`|
|异常|400 401 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "platform": "android",
  "appVersion": "1.2.0",
  "installId": "3030e190-a030-4bba-9cec-057d5f419f01"
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "enabled": true,
    "anonymousEnabled": true,
    "policyVersion": 3,
    "schemaVersions": [1],
    "maxBatchSize": 100,
    "maxBatchBytes": 262144,
    "maxAgeSeconds": 604800,
    "flushIntervalMs": 10000,
    "configTtlSeconds": 300
  }
}
```

**校验与渲染** platform 只接受 ios/android，appVersion 最大 40 字符。关闭后 enabled=false，App 在 configTtlSeconds 内可使用同一身份的有效内存缓存。匿名关闭后清理不可继续上传的队列。配置响应 Cache-Control: private, no-store。

#### 3.1.2 IPK_ANL_02 签发匿名凭据

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_02|
|功能名称|签发匿名凭据|
|功能描述|签发绑定安装和身份阶段的匿名凭据。|
|URL|`/ipaklink/v1/analytics/anonymous-tokens`|
|Method|POST|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 201 JSON 示例|
|权限|无需 Bearer|
|功能编号|SDK-02、ING-01|
|数据落点|Redis 限流；匿名凭据由认证服务签发|
|参数定义|`body:installId:UUID!,identityEpoch:UUID!,platform:string!,appVersion:string!`|
|异常|400 403 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "installId": "3030e190-a030-4bba-9cec-057d5f419f01",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419f02",
  "platform": "android",
  "appVersion": "1.2.0"
}
```

**Response JSON · HTTP 201**

```json
{
  "code": 201,
  "message": "ok",
  "data": {
    "analyticsToken": "anon_mock_token",
    "expiresIn": 900,
    "policyVersion": 3
  }
}
```

**校验与渲染** 返回的 analyticsToken 仅限安装、身份阶段和采集用途，有效期单位秒，响应 Cache-Control: no-store。过期可用同一身份阶段重新签发。

#### 3.1.3 IPK_ANL_03 批量上报事件

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_03|
|功能名称|批量上报事件|
|功能描述|提交批次，逐事件返回持久确认、重复、拒绝或重试。|
|URL|`/ipaklink/v1/analytics/events/report`|
|Method|POST|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|App Bearer 或 X-Analytics-Token（二选一）|
|功能编号|SDK-03–06、ING-01–04|
|数据落点|Redis、analytics_ingest_receipt、Kafka analytics.event|
|参数定义|`body:batchId:UUID!,installId:UUID!,identityEpoch:UUID!,identityType:string!,platform:string!,appVersion:string!,appLanguage:string?,regionId:int?,policyVersion:int!,events:Event[]!`|
|异常|400 401 403 413 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "headers": {
    "X-Analytics-Token": "anon_mock_token"
  },
  "body": {
    "batchId": "3030e190-a030-4bba-9cec-057d5f419f05",
    "installId": "3030e190-a030-4bba-9cec-057d5f419f01",
    "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419f02",
    "identityType": "anonymous",
    "platform": "android",
    "appVersion": "1.2.0",
    "appLanguage": "zh",
    "policyVersion": 3,
    "events": [
      {
        "eventId": "3030e190-a030-4bba-9cec-057d5f419f06",
        "eventName": "screen_view",
        "schemaVersion": 1,
        "occurredAt": "2026-11-16T04:00:00Z",
        "sessionId": "3030e190-a030-4bba-9cec-057d5f419f03",
        "sequence": 1,
        "screenName": "FactoryDetail",
        "pageViewId": "3030e190-a030-4bba-9cec-057d5f419f11",
        "properties": {
          "entrySource": "search"
        }
      },
      {
        "eventId": "3030e190-a030-4bba-9cec-057d5f419f07",
        "eventName": "screen_view",
        "schemaVersion": 1,
        "occurredAt": "2026-11-16T04:00:00Z",
        "sessionId": "3030e190-a030-4bba-9cec-057d5f419f03",
        "sequence": 2,
        "screenName": "FactoryDetail",
        "pageViewId": "3030e190-a030-4bba-9cec-057d5f419f12",
        "properties": {
          "entrySource": "search"
        }
      },
      {
        "eventId": "3030e190-a030-4bba-9cec-057d5f419f08",
        "eventName": "screen_view",
        "schemaVersion": 1,
        "occurredAt": "2026-11-16T04:00:00Z",
        "sessionId": "3030e190-a030-4bba-9cec-057d5f419f03",
        "sequence": 3,
        "screenName": "FactoryDetail",
        "pageViewId": "3030e190-a030-4bba-9cec-057d5f419f13",
        "properties": {
          "entrySource": "invalid"
        }
      },
      {
        "eventId": "3030e190-a030-4bba-9cec-057d5f419f09",
        "eventName": "screen_view",
        "schemaVersion": 1,
        "occurredAt": "2026-11-16T04:00:00Z",
        "sessionId": "3030e190-a030-4bba-9cec-057d5f419f03",
        "sequence": 4,
        "screenName": "FactoryDetail",
        "pageViewId": "3030e190-a030-4bba-9cec-057d5f419f14",
        "properties": {
          "entrySource": "search"
        }
      }
    ]
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "batchId": "3030e190-a030-4bba-9cec-057d5f419f05",
    "results": [
      {
        "index": 0,
        "eventId": "3030e190-a030-4bba-9cec-057d5f419f06",
        "status": "accepted"
      },
      {
        "index": 1,
        "eventId": "3030e190-a030-4bba-9cec-057d5f419f07",
        "status": "duplicate"
      },
      {
        "index": 2,
        "eventId": "3030e190-a030-4bba-9cec-057d5f419f08",
        "status": "rejected",
        "errorCode": "invalidProperties"
      },
      {
        "index": 3,
        "eventId": "3030e190-a030-4bba-9cec-057d5f419f09",
        "status": "retry",
        "errorCode": "ackUnknown"
      }
    ]
  }
}
```

**校验与渲染** JSON 解压后最多 256 KiB，单事件最多 8 KiB，批次 1–100 条。事件须按 §6 类型及受启用的白名单校验。HTTP 200 时逐条按 index 处理。accepted、duplicate 清除原事件；rejected 丢弃并记录诊断；retry 保留原 eventId 与内容。429/503 保留整批，分拆重试生成新 batchId。仅持久 ACK 后才确认 accepted。

### 3.2 经营看板

#### 3.2.1 IPK_ANL_10 读取快照和菜单

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_10|
|功能名称|读取快照和菜单|
|功能描述|读取当前可用快照和权限菜单。看板每 60 秒查询。|
|URL|`/ipaklink/v1/internal/analytics/metadata`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.read|
|功能编号|BI-08|
|数据落点|analytics_report_snapshot、analytics_access_grant|
|参数定义|`query:snapshotId:string?`|
|异常|401 403 409 429 503、500，见 §2.5；示例 `{"code":401,"message":"analytics.unauthenticated","data":null}`|

**Request JSON**

```json
{}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "snapshotId": "snap_20261116_0905",
    "metricVersion": 1,
    "timezone": "Asia/Tashkent",
    "generatedAt": "2026-11-16T04:05:00Z",
    "sourceWatermark": "2026-11-16T04:01:00Z",
    "status": "ready",
    "availableReports": [
      "overview",
      "pages",
      "features",
      "funnels",
      "retention",
      "quality",
      "sessions",
      "corpus",
      "cardImpressions",
      "exports"
    ],
    "platforms": [
      "ios",
      "android"
    ],
    "features": [
      "search",
      "chat",
      "translate",
      "favorite"
    ],
    "regions": [
      {
        "regionId": 1,
        "label": "Tashkent"
      }
    ],
    "permissions": [
      "analytics.read",
      "analytics.session.read",
      "analytics.quality.read",
      "analytics.export.read",
      "analytics.export.write",
      "corpus.summary.read"
    ]
  }
}
```

**校验与渲染** 按操作者权限和功能启用配置生成菜单。无可发布快照时 snapshotId、generatedAt、sourceWatermark 为 null、status=noData。已有快照状态 ready/stale/partial。前端每 60 秒读取，快照 ID 不变时维持当前报表。

#### 3.2.2 IPK_ANL_11 经营总览

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_11|
|功能名称|经营总览|
|功能描述|查询活跃、业务浏览、询盘、翻译总览及上期对比。|
|URL|`/ipaklink/v1/internal/analytics/overview`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.read|
|功能编号|BI-01|
|数据落点|agg_metric_day、fact_actor_active_day、fact_inquiry_created|
|参数定义|`query:fromDate:date?,toDate:date?,platform:string?,regionId:int?,appVersion:string?,snapshotId:string?,includePrevious:boolean?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "fromDate": "2026-11-09",
  "toDate": "2026-11-16",
  "includePrevious": true
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "cards": [
      {
        "metricCode": "dau",
        "value": 320,
        "unit": "count",
        "status": "ready"
      },
      {
        "metricCode": "wau",
        "value": 1000,
        "unit": "count",
        "status": "ready"
      },
      {
        "metricCode": "mau",
        "value": 1400,
        "unit": "count",
        "status": "ready"
      },
      {
        "metricCode": "anonymousUv",
        "value": 180,
        "unit": "count",
        "status": "ready"
      },
      {
        "metricCode": "businessViewPv",
        "value": 2512,
        "unit": "count",
        "status": "ready"
      },
      {
        "metricCode": "inquiryCreated",
        "value": 31,
        "unit": "count",
        "status": "ready",
        "previousValue": 25,
        "relativeChange": 0.24
      },
      {
        "metricCode": "translateSuccessRate",
        "value": 0.9,
        "unit": "ratio",
        "status": "ready",
        "numerator": 90,
        "denominator": 100,
        "previousValue": 0.85,
        "differencePercentagePoints": 5
      }
    ],
    "series": [
      {
        "date": "2026-11-15",
        "metricCode": "dau",
        "value": 320,
        "unit": "count",
        "status": "ready"
      }
    ],
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** cards 的固定 metricCode 为 dau、wau、mau、anonymousUv、businessViewPv、inquiryCreated、translateSuccessRate。series 每行一个日期和指标，按日期再按指标编码排序。includePrevious 默认 true。活跃卡按最后选中业务日的窗口计算，比例卡按区间重算分母。相对变化以小数返回，differencePercentagePoints 以百分点返回。

#### 3.2.3 IPK_ANL_12 页面分析

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_12|
|功能名称|页面分析|
|功能描述|查询页面排行及选定页面的停留分布。|
|URL|`/ipaklink/v1/internal/analytics/pages`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.read|
|功能编号|BI-02|
|数据落点|fact_page_visit、agg_metric_day|
|参数定义|`query:fromDate:date?,toDate:date?,platform:string?,regionId:int?,appVersion:string?,snapshotId:string?,screenName:string?,sortBy:string?,page:int?,size:int?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "screenName": "FactoryDetail",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "screenName": "FactoryDetail",
        "pv": 200,
        "uv": 180,
        "closedCount": 190,
        "eligibleDurationCount": 190,
        "closureRate": 0.95,
        "avgVisibleMs": 12200,
        "p50VisibleMs": 9000,
        "p95VisibleMs": 39000,
        "exitRate": 0.2,
        "status": "ready"
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20,
    "durationBuckets": [
      {
        "lowerMs": 0,
        "upperMs": 5000,
        "count": 34
      },
      {
        "lowerMs": 5000,
        "upperMs": 15000,
        "count": 80
      },
      {
        "lowerMs": 15000,
        "upperMs": 30000,
        "count": 50
      },
      {
        "lowerMs": 30000,
        "upperMs": 60000,
        "count": 24
      },
      {
        "lowerMs": 60000,
        "upperMs": null,
        "count": 2
      }
    ],
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** sortBy=pv/uv/p95VisibleMs/exitRate，默认 pv。榜单同值按 screenName 升序。指定 screenName 后返回完整五桶，末桶 upperMs=null；未指定时 durationBuckets=[]。只有闭合且时长有效的访问进入时长分母。闭合率和退出率按各自分母计算。

#### 3.2.4 IPK_ANL_13 功能分析

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_13|
|功能名称|功能分析|
|功能描述|查询搜索、聊天、翻译和收藏的趋势及分组指标。|
|URL|`/ipaklink/v1/internal/analytics/features`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.read|
|功能编号|BI-03、BI-05、BI-11|
|数据落点|fact_search、fact_translate_task、fact_dialog_reply、agg_metric_day|
|参数定义|`query:fromDate:date?,toDate:date?,platform:string?,regionId:int?,appVersion:string?,snapshotId:string?,feature:string!,groupBy:string?,scope:string?,mode:string?,sourceLang:string?,targetLang:string?,errorCode:string?,page:int?,size:int?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "feature": "translate",
  "groupBy": "languagePair",
  "mode": "text",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "feature": "translate",
    "summary": [
      {
        "metricCode": "translateSuccessRate",
        "value": 0.9,
        "unit": "ratio",
        "status": "ready",
        "numerator": 90,
        "denominator": 100
      }
    ],
    "series": [
      {
        "date": "2026-11-15",
        "metricCode": "translateStarted",
        "value": 105,
        "unit": "count",
        "status": "ready"
      }
    ],
    "list": [
      {
        "groupKey": "zh:uz",
        "metricCode": "translateP95Ms",
        "value": 1100,
        "unit": "ms",
        "status": "ready"
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20,
    "coverageStartDate": "2026-11-09",
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** feature=search/chat/translate/favorite。search 支持 groupBy=scope/errorCode/appVersion；chat 支持 entrySource/appVersion；translate 支持 mode/languagePair/errorCode/appVersion；favorite 支持 appVersion。errorCode 仅 search/translate 可用并约束至错误码下钻。无 groupBy 时 list=[]、total=0。分页单位为返回的 groupKey + metricCode 行；UI 按组合并展示。coverageStartDate 是该功能真实采集启用日，未知返回 null。

#### 3.2.5 IPK_ANL_14 固定漏斗

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_14|
|功能名称|固定漏斗|
|功能描述|查询询盘或搜索的固定漏斗。|
|URL|`/ipaklink/v1/internal/analytics/funnels/{funnelCode}`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.read|
|功能编号|BI-04|
|数据落点|fact_inquiry_flow、fact_inquiry_created、fact_search|
|参数定义|`path:funnelCode:string!`<br>`query:fromDate:date?,toDate:date?,platform:string?,regionId:int?,appVersion:string?,snapshotId:string?,entrySource:string?,targetType:string?,searchScope:string?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "path": {
    "funnelCode": "F01"
  },
  "query": {
    "fromDate": "2026-11-09",
    "toDate": "2026-11-16"
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "funnelCode": "F01",
    "windowSeconds": 86400,
    "entityUnit": "actor",
    "steps": [
      {
        "stepCode": "detail",
        "count": 100,
        "uniqueActors": 100,
        "stepRate": 1,
        "totalRate": 1,
        "p50FromPreviousMs": null
      },
      {
        "stepCode": "contact",
        "count": 30,
        "uniqueActors": 30,
        "stepRate": 0.3,
        "totalRate": 0.3,
        "p50FromPreviousMs": 23000
      },
      {
        "stepCode": "submit",
        "count": 18,
        "uniqueActors": 18,
        "stepRate": 0.6,
        "totalRate": 0.18,
        "p50FromPreviousMs": 56000
      },
      {
        "stepCode": "created",
        "count": 16,
        "uniqueActors": 16,
        "stepRate": 0.8889,
        "totalRate": 0.16,
        "p50FromPreviousMs": 700
      }
    ],
    "businessCount": 20,
    "attributedCount": 16,
    "attributionCoverage": 0.8,
    "attributionReasons": [
      {
        "reasonCode": "missingFlow",
        "count": 4
      }
    ],
    "zeroResultCount": null,
    "failedCount": null,
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** funnelCode=F01/F02，日期最长 31 天；targetType、searchScope 仅 F02 可用，company/product 是可用主体，searchScope 允许 all/company/product。F01 以 24 小时和去重用户为单位，四步 detail/contact/submit/created；归因统计按业务提交日。F02 以 15 分钟和 searchId 为单位，四步 submit/hasResults/open/detail。F01 的 zeroResultCount/failedCount=null；F02 的 businessCount/attributedCount/attributionCoverage=null、attributionReasons=[]。分母为 0 时比率为 null。

#### 3.2.6 IPK_ANL_15 活跃留存

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_15|
|功能名称|活跃留存|
|功能描述|查询用户首活队列及观察日留存。|
|URL|`/ipaklink/v1/internal/analytics/retention`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.read|
|功能编号|BI-06|
|数据落点|dim_actor_first_active、agg_retention_day|
|参数定义|`query:fromDate:date?,toDate:date?,platform:string?,regionId:int?,appVersion:string?,snapshotId:string?,offsetDays:string?,page:int?,size:int?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "offsetDays": "1,7,30",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "cohortDate": "2026-11-10",
        "cohortSize": 100,
        "d1RetainedCount": 20,
        "d1Rate": 0.2,
        "d1Mature": true,
        "d1ExpectedMatureDate": "2026-11-12",
        "d7RetainedCount": null,
        "d7Rate": null,
        "d7Mature": false,
        "d7ExpectedMatureDate": "2026-11-18",
        "d30RetainedCount": null,
        "d30Rate": null,
        "d30Mature": false,
        "d30ExpectedMatureDate": "2026-12-11"
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20,
    "unknownCohortCount": 5,
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** offsetDays 是 CSV 含 1、7、30 的非空子集，默认全部。list 每行一个 cohort 日期。未请求的观察日字段仍返回 null（包括 Mature），未成熟的 Mature=false、结果为 null。按首活平台、地区、版本筛选；cohort_date 升序。

#### 3.2.7 IPK_ANL_16 受控会话时间线

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_16|
|功能名称|受控会话时间线|
|功能描述|凭工单查询安全会话时间线。|
|URL|`/ipaklink/v1/internal/analytics/sessions/{sessionId}`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.session.read|
|功能编号|BI-10、GOV-02|
|数据落点|events_canonical、fact_page_visit、analytics_admin_audit|
|参数定义|`path:sessionId:UUID!`<br>`query:ticketId:string!,snapshotId:string?,page:int?,size:int?`|
|异常|400 401 403 404 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "path": {
    "sessionId": "3030e190-a030-4bba-9cec-057d5f419f03"
  },
  "query": {
    "ticketId": "OPS-127",
    "page": 1,
    "size": 20
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "sessionId": "3030e190-a030-4bba-9cec-057d5f419f03",
    "list": [
      {
        "eventName": "screen_view",
        "occurredAt": "2026-11-15T04:00:00Z",
        "sequence": 2,
        "screenName": "FactoryDetail",
        "visibleMs": 35000,
        "closureStatus": "closed",
        "outcome": null,
        "targetType": null,
        "targetRef": null
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20,
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** 仅查询近 30 天，工单、角色和数据范围三者同时校验并记录审计。事件按发生时间、序号和稳定键排序；同 snapshotId 的翻页固定。无权限 403、授权范围内没有记录 404。closureStatus=closed/open/unknown/null；业务字段为空时显示占位符。只返回安全元数据。

### 3.3 扩展报表

#### 3.3.1 IPK_ANL_23 卡片曝光与 CTR

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_23|
|功能名称|卡片曝光与 CTR|
|功能描述|查询列表卡片曝光、已点击曝光和 CTR，支持位置或实体分组。|
|URL|`/ipaklink/v1/internal/analytics/card-impressions`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.read|
|功能编号|BI-13|
|数据落点|fact_card_impression、agg_metric_day|
|参数定义|`query:fromDate:date?,toDate:date?,platform:string?,regionId:int?,appVersion:string?,snapshotId:string?,placementCode:string?,targetType:string?,groupBy:string?,page:int?,size:int?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "placementCode": "searchResults",
  "groupBy": "placement",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "groupKey": "searchResults",
        "impressions": 520,
        "clickedImpressions": 64,
        "ctr": 0.1231,
        "status": "ready"
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1,
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** placementCode 按事件注册表，首版提案 homeRecommendation/searchResults；targetType=company/product；groupBy=placement/targetType，默认 placement。ctr=clickedImpressions/impressions，无分母为空。同 impressionId 的多次点击只计一个已点击曝光。阈值见 §6.3。

#### 3.3.2 IPK_ANL_24 创建导出作业

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_24|
|功能名称|创建导出作业|
|功能描述|按固定快照生成 CSV 导出任务。|
|URL|`/ipaklink/v1/internal/analytics/export-jobs`|
|Method|POST|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 202 JSON 示例|
|权限|analytics.export.write|
|功能编号|BI-12|
|数据落点|analytics_export_job、analytics_admin_audit|
|参数定义|`header:Idempotency-Key:string!`<br>`body:snapshotId:string!,reportCode:string!,fromDate:date!,toDate:date!,platform:string?,regionId:int?,appVersion:string?,format:string!`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "headers": {
    "Idempotency-Key": "export-001"
  },
  "body": {
    "snapshotId": "snap_01",
    "reportCode": "overview",
    "fromDate": "2026-11-09",
    "toDate": "2026-11-16",
    "format": "csv"
  }
}
```

**Response JSON · HTTP 202**

```json
{
  "code": 202,
  "message": "ok",
  "data": {
    "jobId": "export_01",
    "status": "queued",
    "createdAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** format=csv；reportCode=overview/pages/features/funnels/retention/quality/cardImpressions。导出按当前角色、固定快照、窗口和业务维度重跑白名单模板，使用服务端默认模块子用例（features=search、funnels=F01）；所有允许导出范围均沿用脱敏规则。同键异负载 409。 同操作者幂等键与请求摘要在导出创建事务内持久化；可在 analytics_export_job 增加字段和复合唯一索引。

#### 3.3.3 IPK_ANL_25 导出作业详情

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_25|
|功能名称|导出作业详情|
|功能描述|读取作业状态，成功且授权有效时给出短期下载 URL。|
|URL|`/ipaklink/v1/internal/analytics/export-jobs/{jobId}`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.export.read|
|功能编号|BI-12|
|数据落点|analytics_export_job|
|参数定义|`path:jobId:string!`|
|异常|401 403 404 410 429 503、500，见 §2.5；示例 `{"code":401,"message":"analytics.unauthenticated","data":null}`|

**Request JSON**

```json
{
  "path": {
    "jobId": "export_01"
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "jobId": "export_01",
    "status": "succeeded",
    "snapshotId": "snap_01",
    "downloadUrl": "https://files.example.invalid/mock/export.csv",
    "expiresAt": "2026-11-16T05:00:00Z",
    "errorCode": null
  }
}
```

**校验与渲染** queued/running 返回 downloadUrl、expiresAt、errorCode 为 null；failed 仅提供安全错误码；succeeded 在当前授权仍有效时给短期链接；文件失效 410。固定快照过期后已有导出是否仍可用以导出文件自身到期及当前授权为准。

### 3.4 质量与修复

#### 3.4.1 IPK_ANL_17 数据质量与对账

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_17|
|功能名称|数据质量与对账|
|功能描述|查询接入质量、时间桶与业务对账。|
|URL|`/ipaklink/v1/internal/analytics/quality`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.quality.read|
|功能编号|BI-07、DW-06|
|数据落点|agg_quality_minute、fact_reconciliation_day|
|参数定义|`query:fromDate:date?,toDate:date?,platform:string?,appVersion:string?,snapshotId:string?,source:string?,granularity:string?,page:int?,size:int?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "granularity": "day",
  "source": "server",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "accepted": 7400,
    "rejected": 30,
    "duplicates": 29,
    "late": 11,
    "dlq": 0,
    "lagRecords": 89,
    "p95IngestDelayMs": 37000,
    "rateLimited": 10,
    "retry": 3,
    "stalePartitions": 0,
    "series": [
      {
        "bucket": "2026-11-14T19:00:00Z",
        "accepted": 7400,
        "rejected": 30,
        "duplicates": 29,
        "late": 11,
        "dlq": 0,
        "lagRecords": 89,
        "p95IngestDelayMs": 37000,
        "rateLimited": 10,
        "retry": 3,
        "stalePartitions": 0
      }
    ],
    "list": [
      {
        "factCode": "inquiryCreated",
        "date": "2026-11-15",
        "sourceCount": 31,
        "warehouseCount": 31,
        "delta": 0,
        "unattributedCount": 4
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20,
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** granularity=day/minute，默认 day；day 最大 31 天，minute 最大一个业务日。source=app/server，可不传。series 按 UTC 时间升序，日桶是业务日边界对应的 UTC。list 分页仅作用于对账行，delta=warehouseCount-sourceCount。lagRecords 聚合取最新样本；延迟 P95 以样本计算。

#### 3.4.2 IPK_ANL_21 创建修复作业

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_21|
|功能名称|创建修复作业|
|功能描述|提交重算或回放作业，按幂等键受理。|
|URL|`/ipaklink/v1/internal/analytics/repair-jobs`|
|Method|POST|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.repair.write|
|功能编号|DW-07、BI-07|
|数据落点|analytics_repair_job、analytics_admin_audit|
|参数定义|`header:Idempotency-Key:string!`<br>`body:type:string!,fromDate:date!,toDate:date!,reasonCode:string!,ticketId:string!,dryRun:boolean!,topic:string?,partition:int?,startOffset:string?,endOffsetExclusive:string?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "headers": {
    "Idempotency-Key": "repair-001"
  },
  "body": {
    "type": "rebuild",
    "fromDate": "2026-11-12",
    "toDate": "2026-11-13",
    "reasonCode": "lateData",
    "ticketId": "OPS-127",
    "dryRun": true
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "jobId": "repair_01",
    "status": "queued",
    "createdAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** type=rebuild/replay，时间范围最长 31 天。replay 要求 topic、partition、startOffset、endOffsetExclusive，offset 为十进制字符串且尾位点不包含；rebuild 必须省略这些字段。reasonCode=lateData/missingData/schemaFix/reconciliation。dryRun=true 不发布。相同操作者与键同负载返回同一 jobId，不同负载返回 409。

#### 3.4.3 IPK_ANL_22 修复作业详情

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_22|
|功能名称|修复作业详情|
|功能描述|读取修复状态、影响范围与错误码。|
|URL|`/ipaklink/v1/internal/analytics/repair-jobs/{jobId}`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.quality.read 或 analytics.repair.write|
|功能编号|DW-07、BI-07|
|数据落点|analytics_repair_job|
|参数定义|`path:jobId:string!`|
|异常|401 403 404 429 503、500，见 §2.5；示例 `{"code":401,"message":"analytics.unauthenticated","data":null}`|

**Request JSON**

```json
{
  "path": {
    "jobId": "repair_01"
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "jobId": "repair_01",
    "status": "succeeded",
    "type": "rebuild",
    "dryRun": true,
    "scannedCount": 1250,
    "affectedDateCount": 2,
    "publishedSnapshotId": null,
    "errorCode": null,
    "createdAt": "2026-11-16T03:00:00Z",
    "finishedAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** 支持 §2.4 作业状态。dryRun 结果的 publishedSnapshotId 恒为 null。排队或未完成时计数未知填 null；errorCode 用固定字典。记录仍在可读范围但暂无任务时返回 404。

#### 3.4.4 IPK_ANL_32 修复作业历史

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_32|
|功能名称|修复作业历史|
|功能描述|按状态、类型和创建日期查询历史作业。|
|URL|`/ipaklink/v1/internal/analytics/repair-jobs`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.quality.read|
|功能编号|ADM-02|
|数据落点|analytics_repair_job|
|参数定义|`query:status:string?,type:string?,fromDate:date?,toDate:date?,page:int?,size:int?`|
|异常|400 401 403 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "status": "failed",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "jobId": "repair_02",
        "type": "replay",
        "status": "failed",
        "dryRun": false,
        "errorCode": "offsetExpired",
        "createdAt": "2026-11-16T04:05:00Z"
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** status=queued/running/succeeded/failed，type=rebuild/replay；日期按 createdAt 的业务日，最多 31 天。从新到旧，主键二次排序。

#### 3.4.5 IPK_ANL_33 修复失败明细

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_33|
|功能名称|修复失败明细|
|功能描述|分页读取安全失败定位，不返回事件负载。|
|URL|`/ipaklink/v1/internal/analytics/repair-jobs/{jobId}/failures`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.repair.write|
|功能编号|ADM-02|
|数据落点|analytics_repair_job.result、允许的日志索引|
|参数定义|`path:jobId:string!`<br>`query:page:int?,size:int?`|
|异常|400 401 403 404 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "path": {
    "jobId": "repair_02"
  },
  "query": {
    "page": 1,
    "size": 20
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "topic": "analytics.event",
        "partition": 2,
        "offset": "14820",
        "errorCode": "offsetExpired",
        "occurredAt": "2026-11-16T04:05:00Z"
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** 位点为十进制字符串；若某失败非 Kafka 记录，topic/partition/offset 可空。空明细返回 200 和空列表。原始密文或正文不返回。

#### 3.4.6 IPK_ANL_34 切换当前快照

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_34|
|功能名称|切换当前快照|
|功能描述|将当前快照整体切换到仍有效的历史快照。|
|URL|`/ipaklink/v1/internal/analytics/report-snapshots/current`|
|Method|PUT|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.snapshot.write|
|功能编号|ADM-02|
|数据落点|analytics_report_snapshot、analytics_report_snapshot_build、analytics_admin_audit|
|参数定义|`body:snapshotId:string!,expectedSnapshotId:string!,ticketId:string!,reasonCode:string!`|
|异常|400 401 403 404 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "snapshotId": "snap_01",
  "expectedSnapshotId": "snap_02",
  "ticketId": "OPS-127",
  "reasonCode": "reconciliation"
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "snapshotId": "snap_01",
    "previousSnapshotId": "snap_02",
    "status": "published",
    "updatedAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** 目标必须为当前可读取、未过期且通过校验的快照。并发检查 expectedSnapshotId；不匹配或目标不可回滚返回 409。置位成功立即由 ANL_10 读到并写审计；对同一请求安全重试。业务操作通过 HTTPS 轮询传播。

#### 3.4.7 IPK_ANL_42 质量通知投递记录

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_42|
|功能名称|质量通知投递记录|
|功能描述|展示按授权范围投递的质量异常通知及状态。|
|URL|`/ipaklink/v1/internal/analytics/alert-deliveries`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.quality.read|
|功能编号|ADM-05|
|数据落点|analytics_alert_delivery|
|参数定义|`query:status:string?,channelCode:string?,fromDate:date?,toDate:date?,page:int?,size:int?`|
|异常|400 401 403 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "status": "failed",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "deliveryId": "3030e190-a030-4bba-9cec-057d5f419f06",
        "alertKey": "alert_01",
        "channelCode": "ops",
        "status": "failed",
        "attempts": 2,
        "snapshotId": "snap_01",
        "factCode": "inquiryCreated",
        "createdAt": "2026-11-16T04:05:00Z",
        "sentAt": null
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** status=pending/sent/failed，可按 channelCode 筛选。以数据范围授权返回投递诊断；不回传接收地址。factCode 为空时返回 null；sentAt 仅已发送时非空。 factCode 不在现有投递表字段内，须从告警上下文投影；实现时可在该表增加可空 fact_code 或读取既有安全上下文，缺失返回 null。

#### 3.4.8 IPK_ANL_44 快照列表

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_44|
|功能名称|快照列表|
|功能描述|分页列出可读快照及是否可回滚。|
|URL|`/ipaklink/v1/internal/analytics/report-snapshots`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.quality.read|
|功能编号|ADM-02|
|数据落点|analytics_report_snapshot、analytics_report_snapshot_build|
|参数定义|`query:status:string?,page:int?,size:int?`|
|异常|400 401 403 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "status": "archived",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "snapshotId": "snap_01",
        "metricVersion": 1,
        "status": "archived",
        "canRestore": true,
        "generatedAt": "2026-11-16T04:05:00Z",
        "sourceWatermark": "2026-11-16T04:01:00Z"
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** status=published/archived/expired。canRestore 为服务端基于当前权限、保留及校验的布尔结果。仅显示操作者可见快照；回滚请求仍须带工单、原因和期望版本。

### 3.5 采集治理

#### 3.5.1 IPK_ANL_26 事件模式列表

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_26|
|功能名称|事件模式列表|
|功能描述|按事件名、来源或状态查看已登记的模式。|
|URL|`/ipaklink/v1/internal/analytics/event-schemas`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.config.read|
|功能编号|ADM-01|
|数据落点|analytics_event_schema|
|参数定义|`query:eventName:string?,source:string?,status:string?,page:int?,size:int?`|
|异常|400 401 403 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "eventName": "screen_view",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "eventName": "screen_view",
        "schemaVersion": 1,
        "source": "app",
        "status": "active",
        "createdAt": "2026-11-16T04:05:00Z"
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** source=app/server；status=draft/active/retired。分页按 eventName 升序、schemaVersion 降序。列表显示描述字段，完整规则用 ANL_43。

#### 3.5.2 IPK_ANL_27 创建事件模式

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_27|
|功能名称|创建事件模式|
|功能描述|创建事件模式草稿，模式通过校验后入库。|
|URL|`/ipaklink/v1/internal/analytics/event-schemas`|
|Method|POST|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 201 JSON 示例|
|权限|analytics.config.write|
|功能编号|ADM-01|
|数据落点|analytics_event_schema、analytics_admin_audit|
|参数定义|`body:eventName:string!,schemaVersion:int!,source:string!,propertySchemaText:string!`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "eventName": "card_impression",
  "schemaVersion": 1,
  "source": "app",
  "propertySchemaText": "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"impressionId\"],\"properties\":{\"impressionId\":{\"type\":\"string\",\"format\":\"uuid\"}}}"
}
```

**Response JSON · HTTP 201**

```json
{
  "code": 201,
  "message": "ok",
  "data": {
    "eventName": "card_impression",
    "schemaVersion": 1,
    "status": "draft",
    "createdAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** 源为 app/server，版本为正整数；同事件名和版本已存在返回 409。propertySchemaText 是 UTF-8 JSON Schema 文本，解析并校验后存 property_schema，禁止任意拓展字段。样例只演示草稿的字段子集，正式 BI-13 模式须包含 §6.3 全部必填字段。

#### 3.5.3 IPK_ANL_28 发布或停用事件模式

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_28|
|功能名称|发布或停用事件模式|
|功能描述|设置已存在模式的发布状态。|
|URL|`/ipaklink/v1/internal/analytics/event-schemas/{eventName}/versions/{schemaVersion}/status`|
|Method|PUT|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.config.write|
|功能编号|ADM-01|
|数据落点|analytics_event_schema、analytics_admin_audit|
|参数定义|`path:eventName:string!,schemaVersion:int!`<br>`body:status:string!,ticketId:string!`|
|异常|400 401 403 404 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "path": {
    "eventName": "card_impression",
    "schemaVersion": 1
  },
  "body": {
    "status": "active",
    "ticketId": "OPS-127"
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "eventName": "card_impression",
    "schemaVersion": 1,
    "status": "active",
    "activatedAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** 目标状态 active/retired。仅允许合法状态推进。激活前验证必填规则、来源、兼容性及启用策略；相同目标状态重复返回 200、沿用首次时间。审查失败或过时返回 409。

#### 3.5.4 IPK_ANL_29 采集策略列表

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_29|
|功能名称|采集策略列表|
|功能描述|按状态查询采集配置及灰度版本。|
|URL|`/ipaklink/v1/internal/analytics/collection-policies`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.config.read|
|功能编号|ADM-01|
|数据落点|analytics_collection_policy|
|参数定义|`query:status:string?,page:int?,size:int?`|
|异常|400 401 403 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "status": "active",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "policyVersion": 3,
        "status": "active",
        "enabled": true,
        "anonymousEnabled": true,
        "rolloutPercent": 100,
        "platforms": [
          "android",
          "ios"
        ],
        "effectiveAt": "2026-11-16T04:05:00Z"
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** status=draft/active/retired；按 policyVersion 倒序。详情用 ANL_45，平台支持 ios/android。

#### 3.5.5 IPK_ANL_30 创建采集策略

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_30|
|功能名称|创建采集策略|
|功能描述|保存采集策略草稿，由服务端分配版本。|
|URL|`/ipaklink/v1/internal/analytics/collection-policies`|
|Method|POST|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 201 JSON 示例|
|权限|analytics.config.write|
|功能编号|ADM-01|
|数据落点|analytics_collection_policy|
|参数定义|`body:enabled:boolean!,anonymousEnabled:boolean!,rolloutPercent:int!,platforms:string[]!,appVersions:string[]!,enabledEventNames:string[]!`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "enabled": true,
  "anonymousEnabled": true,
  "rolloutPercent": 20,
  "platforms": [
    "android"
  ],
  "appVersions": [
    "1.2.0"
  ],
  "enabledEventNames": [
    "screen_view",
    "screen_exit"
  ]
}
```

**Response JSON · HTTP 201**

```json
{
  "code": 201,
  "message": "ok",
  "data": {
    "policyVersion": 4,
    "status": "draft",
    "createdAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** rolloutPercent 为 0–100 整数，平台为 ios/android，appVersions 为精确版本值，enabledEventNames 必须已登记且不超过冻结功能白名单。列表为空表示不采对应值。创建草稿不改变已生效版本。

#### 3.5.6 IPK_ANL_31 设置当前采集策略

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_31|
|功能名称|设置当前采集策略|
|功能描述|激活一个已校验策略，原子切换当前版本。|
|URL|`/ipaklink/v1/internal/analytics/collection-policies/current`|
|Method|PUT|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.config.write|
|功能编号|ADM-01|
|数据落点|analytics_collection_policy、analytics_admin_audit|
|参数定义|`body:policyVersion:int!,expectedPolicyVersion:int!,ticketId:string!`|
|异常|400 401 403 404 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "policyVersion": 4,
  "expectedPolicyVersion": 3,
  "ticketId": "OPS-127"
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "policyVersion": 4,
    "previousPolicyVersion": 3,
    "status": "active",
    "effectiveAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** expectedPolicyVersion 为当前版本，用于乐观锁；并发变化返回 409。目标草稿必须校验通过。目标已生效时重试返回 200，原 previousPolicyVersion 从审计记录复用。

#### 3.5.7 IPK_ANL_43 事件模式详情

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_43|
|功能名称|事件模式详情|
|功能描述|读取指定事件版本的完整模式，支持审查和发布前比对。|
|URL|`/ipaklink/v1/internal/analytics/event-schemas/{eventName}/versions/{schemaVersion}`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.config.read|
|功能编号|ADM-01|
|数据落点|analytics_event_schema|
|参数定义|`path:eventName:string!,schemaVersion:int!`|
|异常|400 401 403 404 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "path": {
    "eventName": "card_impression",
    "schemaVersion": 1
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "eventName": "card_impression",
    "schemaVersion": 1,
    "source": "app",
    "status": "draft",
    "propertySchemaText": "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"impressionId\"],\"properties\":{\"impressionId\":{\"type\":\"string\",\"format\":\"uuid\"}}}",
    "createdAt": "2026-11-16T04:05:00Z",
    "activatedAt": null
  }
}
```

**校验与渲染** eventName 为合法事件名，schemaVersion 为正整数。需 mode 详情时由前端在模式编辑器解析 propertySchemaText；activatedAt 未启用时为 null。

#### 3.5.8 IPK_ANL_45 采集策略详情

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_45|
|功能名称|采集策略详情|
|功能描述|读取指定采集配置版本的完整规则。|
|URL|`/ipaklink/v1/internal/analytics/collection-policies/{policyVersion}`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.config.read|
|功能编号|ADM-01|
|数据落点|analytics_collection_policy|
|参数定义|`path:policyVersion:int!`|
|异常|400 401 403 404 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "path": {
    "policyVersion": 4
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "policyVersion": 4,
    "status": "draft",
    "enabled": true,
    "anonymousEnabled": true,
    "rolloutPercent": 20,
    "platforms": [
      "android"
    ],
    "appVersions": [
      "1.2.0"
    ],
    "enabledEventNames": [
      "screen_view",
      "screen_exit"
    ],
    "effectiveAt": null
  }
}
```

**校验与渲染** policyVersion 为正整数。状态 draft/active/retired；effectiveAt 未生效时返回 null。灰度和版本规则与 ANL_30 相同。

### 3.6 访问管理

#### 3.6.1 IPK_ANL_35 授权列表

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_35|
|功能名称|授权列表|
|功能描述|查询授权记录的角色、有效期及范围。|
|URL|`/ipaklink/v1/internal/analytics/access-grants`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.grant.read|
|功能编号|ADM-03|
|数据落点|analytics_access_grant、analytics_role|
|参数定义|`query:principalId:string?,roleCode:string?,status:string?,page:int?,size:int?`|
|异常|400 401 403 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "roleCode": "operator",
  "status": "active",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "grantId": "3030e190-a030-4bba-9cec-057d5f419f04",
        "principalId": "sso_01",
        "roleCode": "operator",
        "regionIds": [
          1
        ],
        "datasetCodes": [],
        "status": "active",
        "expiresAt": null
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** status=active/expired/revoked。regionIds 和 datasetCodes 是允许范围，空数组表示该维度未授予具体范围；全域权限由内置角色规则授予。按授权记录 ID 稳定分页。

#### 3.6.2 IPK_ANL_36 创建授权

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_36|
|功能名称|创建授权|
|功能描述|为主体按角色与数据范围创建授权。|
|URL|`/ipaklink/v1/internal/analytics/access-grants`|
|Method|POST|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 201 JSON 示例|
|权限|analytics.grant.write|
|功能编号|ADM-03|
|数据落点|analytics_access_grant、analytics_admin_audit|
|参数定义|`header:Idempotency-Key:string!`<br>`body:principalId:string!,roleCode:string!,regionIds:int[]!,datasetCodes:string[]!,ticketId:string!,expiresAt:datetime?`|
|异常|400 401 403 404 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "headers": {
    "Idempotency-Key": "grant-001"
  },
  "body": {
    "principalId": "sso_01",
    "roleCode": "operator",
    "regionIds": [
      1
    ],
    "datasetCodes": [],
    "ticketId": "OPS-127",
    "expiresAt": null
  }
}
```

**Response JSON · HTTP 201**

```json
{
  "code": 201,
  "message": "ok",
  "data": {
    "grantId": "3030e190-a030-4bba-9cec-057d5f419f04",
    "status": "active",
    "createdAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** roleCode 必须存在且操作者可授予，ticketId 须有效。expiresAt 可为空或未来 UTC 时间。使用 Idempotency-Key；相同键同内容返回原资源，异内容 409。 授权创建的幂等键与请求摘要须在授权事务内持久化，不能只依赖缓存。

#### 3.6.3 IPK_ANL_37 撤销授权

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_37|
|功能名称|撤销授权|
|功能描述|将已存在授权置为撤销，保留审计。|
|URL|`/ipaklink/v1/internal/analytics/access-grants/{grantId}/status`|
|Method|PUT|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.grant.write|
|功能编号|ADM-03|
|数据落点|analytics_access_grant、analytics_admin_audit|
|参数定义|`path:grantId:UUID!`<br>`body:status:string!,ticketId:string!`|
|异常|400 401 403 404 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "path": {
    "grantId": "3030e190-a030-4bba-9cec-057d5f419f04"
  },
  "body": {
    "status": "revoked",
    "ticketId": "OPS-127"
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "grantId": "3030e190-a030-4bba-9cec-057d5f419f04",
    "status": "revoked",
    "revokedAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** status 只接受 revoked，不能通过改状态恢复；需要恢复时新建授权。重复撤销保持原 revokedAt。权限检查与审计绑定同次操作。

#### 3.6.4 IPK_ANL_38 审计记录

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_38|
|功能名称|审计记录|
|功能描述|按工单、动作、操作者和日期查询安全审计。|
|URL|`/ipaklink/v1/internal/analytics/audit-records`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|analytics.audit.read|
|功能编号|ADM-03|
|数据落点|analytics_admin_audit|
|参数定义|`query:ticketId:string?,action:string?,operatorId:string?,fromDate:date?,toDate:date?,page:int?,size:int?`|
|异常|400 401 403 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "ticketId": "OPS-127",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "auditId": "3030e190-a030-4bba-9cec-057d5f419f05",
        "operatorId": "sso_01",
        "action": "snapshotRollback",
        "ticketId": "OPS-127",
        "resultCode": "ok",
        "requestId": "req_01",
        "occurredAt": "2026-11-16T04:05:00Z"
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** 日期按 occurredAt 的业务日，最长 31 天。按时间倒序再按审计 ID，requestId 用于联调追踪。仅返回安全作用域数据。

### 3.7 语料治理

#### 3.7.1 IPK_ANL_18 语料状态汇总

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_18|
|功能名称|语料状态汇总|
|功能描述|查询授权数据集的语料状态汇总。|
|URL|`/ipaklink/v1/internal/analytics/corpus/summary`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|corpus.summary.read|
|功能编号|GOV-04|
|数据落点|corpus_datasets、corpus_items 安全投影|
|参数定义|`query:fromDate:date?,toDate:date?,snapshotId:string?,datasetCode:string?,languagePair:string?,status:string?,page:int?,size:int?`|
|异常|400 401 403 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "datasetCode": "approvedTerms",
  "languagePair": "zh:uz",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "datasetCode": "approvedTerms",
        "languagePair": "zh:uz",
        "status": "approved",
        "itemCount": 350,
        "latestJobRef": "corpus_01"
      }
    ],
    "total": 1,
    "page": 1,
    "size": 20,
    "meta": {
      "snapshotId": "snap_01",
      "metricVersion": 1,
      "timezone": "Asia/Tashkent",
      "generatedAt": "2026-11-16T04:05:00Z",
      "sourceWatermark": "2026-11-16T04:01:00Z",
      "fromDate": "2026-11-09",
      "toDate": "2026-11-16",
      "status": "ready",
      "provisional": false,
      "coverage": 0.99
    }
  }
}
```

**校验与渲染** status=pending/approved/rejected/removed；按条目创建日期筛选，状态取快照。languagePair 使用 source:target。latestJobRef 在无可见作业时为 null。

#### 3.7.2 IPK_ANL_19 语料作业详情

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_19|
|功能名称|语料作业详情|
|功能描述|查询单个语料作业的状态和数量。|
|URL|`/ipaklink/v1/internal/analytics/corpus/jobs/{jobRef}`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|corpus.job.read|
|功能编号|GOV-04、ADM-04|
|数据落点|corpus_jobs、analytics_admin_audit|
|参数定义|`path:jobRef:string!`|
|异常|401 403 404 429 503、500，见 §2.5；示例 `{"code":401,"message":"analytics.unauthenticated","data":null}`|

**Request JSON**

```json
{
  "path": {
    "jobRef": "corpus_01"
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "jobRef": "corpus_01",
    "datasetCode": "approvedTerms",
    "type": "import",
    "status": "succeeded",
    "scannedCount": 20,
    "acceptedCount": 18,
    "rejectedCount": 2,
    "createdAt": "2026-11-16T03:00:00Z",
    "finishedAt": "2026-11-16T04:05:00Z",
    "errorCode": null
  }
}
```

**校验与渲染** type=import/reviewSync/erase；作业状态见 §2.4。queued/running 的 finishedAt 和 errorCode 为空。按数据集授权访问并落审计。

#### 3.7.3 IPK_ANL_39 创建语料作业

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_39|
|功能名称|创建语料作业|
|功能描述|发起限定用途和数据集修订的导入或清理作业。|
|URL|`/ipaklink/v1/internal/analytics/corpus/jobs`|
|Method|POST|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 202 JSON 示例|
|权限|corpus.job.write|
|功能编号|ADM-04、GOV-05|
|数据落点|corpus_datasets、corpus_jobs、analytics_admin_audit|
|参数定义|`header:Idempotency-Key:string!`<br>`body:datasetCode:string!,datasetRevision:int!,type:string!,purposeCode:string!,ticketId:string!,inputRef:string?,sourceRef:string?`|
|异常|400 401 403 404 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "headers": {
    "Idempotency-Key": "corpus-001"
  },
  "body": {
    "datasetCode": "approvedTerms",
    "datasetRevision": 1,
    "type": "import",
    "purposeCode": "terminology",
    "ticketId": "OPS-127",
    "inputRef": "asset_ref_01"
  }
}
```

**Response JSON · HTTP 202**

```json
{
  "code": 202,
  "message": "ok",
  "data": {
    "jobRef": "corpus_01",
    "datasetCode": "approvedTerms",
    "type": "import",
    "status": "queued",
    "createdAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** type=import/erase。import 需要授权 inputRef，erase 需要来源安全引用 sourceRef；另一字段省略。用途、数据集修订和工单共同授权；通过同操作者的 Idempotency-Key 防重。响应只表示已排队。 异步作业需持久化操作者、幂等键和请求摘要，使用复合唯一索引。

#### 3.7.4 IPK_ANL_40 语料审核队列

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_40|
|功能名称|语料审核队列|
|功能描述|列出审核队列的状态和受控引用。|
|URL|`/ipaklink/v1/internal/analytics/corpus/items`|
|Method|GET|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|corpus.item.review|
|功能编号|ADM-04|
|数据落点|corpus_items、corpus_datasets|
|参数定义|`query:datasetCode:string!,datasetRevision:int!,status:string?,languagePair:string?,page:int?,size:int?`|
|异常|400 401 403 404 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "datasetCode": "approvedTerms",
  "datasetRevision": 1,
  "status": "pending",
  "page": 1,
  "size": 20
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "list": [
      {
        "itemId": "corpus_item_01",
        "datasetCode": "approvedTerms",
        "datasetRevision": 1,
        "languagePair": "zh:uz",
        "status": "pending",
        "itemRevision": 2,
        "createdAt": "2026-11-16T04:05:00Z"
      }
    ],
    "page": 1,
    "size": 20,
    "total": 1
  }
}
```

**校验与渲染** status=pending/approved/rejected/removed；itemId 是 MongoDB ObjectId 的对外字符串或服务映射的稳定引用。本列表按 createdAt 倒序、itemId 稳定二次排序，只用于审核队列定位和状态操作。

#### 3.7.5 IPK_ANL_41 更新语料审核状态

|契约项|内容|
|---|---|
|接口编号|IPK_ANL_41|
|功能名称|更新语料审核状态|
|功能描述|更新语料审核状态，保留版本与审计。|
|URL|`/ipaklink/v1/internal/analytics/corpus/items/{itemId}/review-state`|
|Method|PUT|
|Request JSON|见下方请求示例与参数定义|
|Response JSON|见下方 HTTP 200 JSON 示例|
|权限|corpus.item.review|
|功能编号|ADM-04|
|数据落点|corpus_items、analytics_mongo_outbox、analytics_admin_audit|
|参数定义|`path:itemId:string!`<br>`body:status:string!,expectedItemRevision:int!,reasonCode:string!,ticketId:string!`|
|异常|400 401 403 404 409 429 503、500，见 §2.5；示例 `{"code":400,"message":"analytics.invalidRequest","data":null}`|

**Request JSON**

```json
{
  "path": {
    "itemId": "corpus_item_01"
  },
  "body": {
    "status": "approved",
    "expectedItemRevision": 2,
    "reasonCode": "reviewPassed",
    "ticketId": "OPS-127"
  }
}
```

**Response JSON · HTTP 200**

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "itemId": "corpus_item_01",
    "status": "approved",
    "itemRevision": 3,
    "reviewedAt": "2026-11-16T04:05:00Z"
  }
}
```

**校验与渲染** status=approved/rejected，reasonCode 使用治理字典。expectedItemRevision 冲突 409。相同审批请求重试返回既有结果。更新真实状态后才发 EVT_24，并落审计。

## 4. 既有业务接口接入

以下为既有业务 API 的分析增量。原业务 DTO 和响应结构继续由业务 API 管理。增加关联字段时保持存量调用可用。下列 Request JSON 和 Response JSON 只显示分析对接所需字段，不用于替换业务接口的完整 Mock。

|接口编号|功能名称|功能描述|URL|Method|Request JSON|Response JSON|
|---|---|---|---|---|---|---|
|IPK_ANL_B01|询盘|从 PG 已提交行派生 EVT_14；contactFlowId 可选，clientRequestId 既有幂等键|/ipaklink/v1/inquiries|POST|`{"targetCompanyId":"company_01","clientRequestId":"request_01","contactFlowId":"3030e190-a030-4bba-9cec-057d5f419f01"}`|`{"code":200,"message":"ok","data":{"inquiryId":"inquiry_01"}}`|
|IPK_ANL_B02|业务浏览|稳定浏览操作键用于防重；业务 PV 从持久浏览事实计算|/ipaklink/v1/views|POST|`{"targetType":"company","targetId":"company_01"}`|沿用原业务响应，viewRef 是内部安全键|
|IPK_ANL_B03|发送消息|首次持久化消息产生 EVT_16，复用原消息幂等键|/ipaklink/v1/chat/dialogs/{id}/messages|POST|`{"path":{"id":"dialog_01"}}`|沿用原消息成功响应，分析只读取可信消息 ID、类型和事实时间|
|IPK_ANL_B04|同步翻译记录|按记录及状态版本产生 EVT_21|/ipaklink/v1/translate/records|POST|沿用原批量记录 DTO，关联 clientRecordId|沿用记录同步响应，分析只接收 recordRef、mode、outcome|
|IPK_ANL_B05|添加或取消收藏|业务关系实际变更才产生 EVT_10|/ipaklink/v1/favorites；/ipaklink/v1/favorites/cancel|POST|`{"targetType":"company","targetId":"company_01"}`|沿用原收藏成功响应，重复幂等请求不新增分析事实|

翻译四种模式的记录同步继续走各自业务路由。搜索使用既有 `GET /ipaklink/v1/search`，行为归因使用单独的 `searchId`。注册由既有 `POST /ipaklink/v1/auth/register` 的已提交事务派生 EVT_23。聊天业务实时传输沿用业务契约，内部埋点看板自动刷新使用 ANL_10。

## 5. 内部异步契约与功能映射

|编号或范围|生产者和消费者|数据及确认|
|---|---|---|
|IPK_ANL_K01 `analytics.event`|Collector 或业务适配器 → 数仓|App 按 sessionId 分区，服务端按 aggregateRef。稳定 eventId，消费持久写入后推进分区连续位点。|
|IPK_ANL_K02 `inquiries.created`|现有询盘服务 → 事实适配器及原业务消费者|按 inquiryId 定位权威行，写 K01 成功后才推进适配器位点。|
|IPK_ANL_K03 `analytics.event.dlq.v1`|数仓消费者 → 受控修复|携 sourceTopic、partition、offset（字符串）、errorCode、可用的 eventId。永久失败先写安全死信。|
|DW-01–08、ING-02/04–07、GOV-01–03|内部任务和事务|Redis 限流，PG/MongoDB 权威事实与出站，ClickHouse 去重、构建、校验、对账，PG 快照发布、授权、主体清理和日志。HTTP 读取固定快照和角色范围。|
|GOV-04/05、ADM-04/05|治理与值班任务|语料审查触发安全事实和撤回传播；质量告警依据规则窗口生成 alertKey、按授权渠道自动投递，ANL_42 查询诊断。|

下列 K01 消息可作为消费者单元测试输入。

```json
{
  "eventId": "3030e190-a030-4bba-9cec-057d5f419ff0",
  "eventName": "inquiry_created",
  "schemaVersion": 1,
  "occurredAt": "2026-11-16T04:00:00Z",
  "receivedAt": "2026-11-16T04:00:01Z",
  "businessDate": "2026-11-16",
  "source": "server",
  "producer": "analytics-inquiry-adapter",
  "identityType": "authenticated",
  "actorKey": "u_demo",
  "aggregateRef": "inquiry_01",
  "payloadHash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties": {"inquiryRef":"inquiry_01","companyId":"company_01","clientRequestId":"request_01"}
}
```

`payloadHash` 在实际处理时由确认过的规范化负载计算，上例是形状示例。字段细目见 [事件属性数据字典](./ipaklink-埋点数据后台数据字典.md#8-事件属性数据字典) 与 [既有 EVT_01–24 示例](./ipaklink-埋点数据后台接口文档.md#42-事件索引)。

|功能模块|冻结编号|实现入口|
|---|---|---|
|客户端和业务行为|SDK-01–08、BIZ-01–07|ANL_01–03、业务扩展、K01/K02。端侧离线队列和业务成功事件使用原事件专用字段。|
|接入与数仓|ING-01–07、DW-01–08|ANL_03、17、21/22，Kafka、数据库与内部执行器。|
|运营查询与扩展|BI-01–13|ANL_10–17、23–25。收藏复用 ANL_13，卡片事件走 ANL_03。|
|数据范围和治理|GOV-01–05|后台权限与审计、ANL_16、18/19、35–41。|
|后台管理|ADM-01–05|ANL_26–45 中对应配置、修复、授权、语料及告警路由。|

## 6. 采集批次与新增卡片事件

### 6.1 App 事件结构

|位置|类型和要求|
|---|---|
|eventId、sessionId|UUID 必填，eventId 重试不变|
|eventName、schemaVersion|string、正整数，必须匹配已启用登记版本|
|occurredAt、sequence|UTC RFC3339，正整数会话序号，最大 2147483647；首报接受过去 7 天、未来 5 分钟|
|screenName、pageViewId|string、UUID，页面进入、退出、详情曝光、联系点击和卡片事件必填|
|properties|object 必填，各事件字段按白名单；无属性为 {}|
|results[].index、eventId、status、errorCode|index 从 0 开始，非法 eventId 时返回 null；status=accepted/duplicate/rejected/retry；后两者必带固定处理码|

批次 `identityType=anonymous/authenticated`，`platform=ios/android`。把旧事件或新增事件分别按注册版本上报，服务端事实名称不加入 App 白名单。

### 6.2 回执与展示状态

|status|可用 errorCode|客户端处理|
|---|---|---|
|accepted、duplicate|省略|从队列移除原事件|
|rejected|invalidEvent、unknownEvent、unsupportedSchema、invalidProperties、timeOutOfRange、eventIdConflict|移除该事件并增加诊断计数|
|retry|brokerUnavailable、ackUnknown、ingestBusy|保留原内容与 ID，稍后提交|

### 6.3 卡片曝光与点击契约

此处为 BI-13 的接口设计值，活动基线仍覆盖该功能，细节通过人工契约评审确认。建议连续可见达到 **50% 面积且不少于 1 秒**才触发曝光。切后台、失焦或低于阈值时中断连续计时。以 `pageViewId + listRequestId + placementCode + targetType + targetId + position` 划分曝光实例。相同列表批次的虚拟列表重渲染或回滚复用实例，同一页面新请求的列表批次重新计曝光。

|事件|生产时机|properties 字段|业务关联|
|---|---|---|---|
|EVT_25 `card_impression` v1|首次满足可见阈值|impressionId:UUID、listRequestId:UUID、placementCode:string、targetType:string、targetId:string、position:int、visibleMs:int，均必填|写 fact_card_impression，position 从 1 开始|
|EVT_26 `card_impression_click` v1|点击已有有效曝光的卡片|impressionId:UUID、listRequestId:UUID、placementCode:string、targetType:string、targetId:string、position:int，均必填|仅归同页面、同列表批次、同目标的已记录曝光；多次点击在 CTR 分子中计一次|

两事件通过 ANL_03 上报，screenName/pageViewId 同批信封必填，无有效曝光的卡片点击仍可按现有搜索打开契约记录。ANL_23 的分子只计能归因的点击。两个新事件的登记、Schema 和 position 上限在人工评审后同步到数据字典。

示例 `properties`。

```json
{
  "impressionId":"3030e190-a030-4bba-9cec-057d5f419f21",
  "listRequestId":"3030e190-a030-4bba-9cec-057d5f419f22",
  "placementCode":"searchResults",
  "targetType":"company",
  "targetId":"company_01",
  "position":1,
  "visibleMs":1240
}
```

## 7. Mock 用例与人工评审

|Mock 用例|返回数据|页面预期|
|---|---|---|
|首次打开后台、正常加载、快照更新|ANL_10 先返回 snap_01，下一轮返回 snap_02；查询各使用相同 snapshotId|菜单按 availableReports 展示；变更后重新拉当前页，旧请求不能覆盖新筛选|
|成功但无数据|HTTP 200；data.list=[]、data.total=0；报表无快照的 meta.status=noData|空态保留筛选，图区显示暂无数据|
|迟到与低基数|meta.status=stale/partial 或指标 status=suppressed、value=null|保留旧快照提示水位；受压制值使用横线|
|分页与排序|page=1,size=20,total=21；第二页仅 1 行，第三页 0 行|不把 list.length 当总量，不清掉同快照筛选|
|输入错误、权限和资源不存在|400/401/403/404，使用 §2.5 对应 JSON|分别显示字段提示、登录重试、无权限、资源不存在|
|快照过期|409 analytics.snapshotExpired|请求 ANL_10 并使用新 snapshotId 重查一次|
|导出、修复、语料作业|queued → running → succeeded；另建 failed 测试|按 jobId/jobRef 查询并停止终态轮询；导出成功后显示下载按钮|
|导出已到期|410 analytics.exportExpired|隐藏下载按钮并提示重建任务|
|限流与临时故障|429 带 Retry-After 或 503|可重试请求保留参数，测退避、错误页及手动重试|
|批次混合回执|使用 ANL_03 的完整示例|第 0、1 条删除，第 2 条丢弃并计错误，第 3 条原 ID 重试|

```json
{"code":200,"message":"ok","data":{"jobId":"export_01","status":"failed","snapshotId":"snap_01","downloadUrl":null,"expiresAt":null,"errorCode":"queryTimeout"}}
```

### 7.1 F02 搜索漏斗 Mock

使用 ANL_14，路径参数为 `F02`，查询可以附 `searchScope=company`。以下四步和 F01 共用扁平字段。步长比率分母是前一步数量，总转化率分母是第一步数量。

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "funnelCode": "F02",
    "windowSeconds": 900,
    "entityUnit": "search",
    "steps": [
      {"stepCode":"submit","count":100,"uniqueActors":90,"stepRate":1,"totalRate":1,"p50FromPreviousMs":null},
      {"stepCode":"hasResults","count":70,"uniqueActors":65,"stepRate":0.7,"totalRate":0.7,"p50FromPreviousMs":220},
      {"stepCode":"open","count":35,"uniqueActors":32,"stepRate":0.5,"totalRate":0.35,"p50FromPreviousMs":4000},
      {"stepCode":"detail","count":30,"uniqueActors":28,"stepRate":0.8571,"totalRate":0.3,"p50FromPreviousMs":600}
    ],
    "businessCount": null,
    "attributedCount": null,
    "attributionCoverage": null,
    "attributionReasons": [],
    "zeroResultCount": 15,
    "failedCount": 5,
    "meta": {"snapshotId":"snap_01","metricVersion":1,"timezone":"Asia/Tashkent","generatedAt":"2026-11-16T04:05:00Z","sourceWatermark":"2026-11-16T04:01:00Z","fromDate":"2026-11-09","toDate":"2026-11-16","status":"ready","provisional":false,"coverage":0.99}
  }
}
```

**人工评审前需确认** 路由是否沿用现有前缀，旧分页客户端的迁移方式，权限键与 SSO 映射，卡片可见阈值与位置字典，以及创建接口幂等记录的物理落点。评审通过后按本文示例搭建前端 Mock。审批、语料治理和导出下载都按数据范围验证。导出和语料作业的真实执行、下载、SSO 及 Kafka 投递须在联调环境验证。


