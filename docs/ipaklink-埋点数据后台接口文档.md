# ipaklink 埋点数据后台接口文档

版本 2.2 · 2026-11-13 · 配套《ipaklink 埋点数据后台详细设计》。接口与 Topic 为新建契约，业务接口扩展单列。

本次开发统一交付 [冻结功能清单 v4.1](./ipaklink-埋点数据后台功能清单.md#2-功能全清单) 的 53 项功能。HTTP 联调、分页及 Mock 使用 [本次 API 接口文档](./ipaklink-埋点数据后台API接口文档.md)。下文的既有范围标记和旧接口示例用于查阅原技术设计。

## 0 接口导航

|章节|接口或事件编号|
|---|---|
|[1 HTTP 接口清单](#1-http-接口清单)|IPK_ANL_01–22、既有业务扩展 B01–B05|
|[1.3 采集与配置](#13-采集与配置)|IPK_ANL_01、IPK_ANL_02、IPK_ANL_03|
|[1.4 看板配置与经营总览](#14-看板配置与经营总览)|IPK_ANL_10、IPK_ANL_11|
|[1.5 页面、功能、漏斗与留存](#15-页面功能漏斗与留存)|IPK_ANL_12、IPK_ANL_13、IPK_ANL_14、IPK_ANL_15|
|[1.6 会话排查与数据质量](#16-会话排查与数据质量)|IPK_ANL_16、IPK_ANL_17|
|[1.7 语料治理](#17-语料治理)|IPK_ANL_18、IPK_ANL_19|
|[1.8 回放与重算作业](#18-回放与重算作业)|IPK_ANL_21、IPK_ANL_22|
|[2 Kafka 接口清单](#2-kafka-接口清单)|IPK_ANL_K01–K04|
|[3 WebSocket 接口](#3-websocket-接口)|IPK_ANL_WS_01|
|[4 按功能划分的采集字段与接口](#4-按功能划分的采集字段与接口)|EVT_01–EVT_24|
|[5 联调顺序](#5-联调顺序)|联调步骤|

# 1 HTTP 接口清单

## 1.1 全局约定

|项|约定|
|---|---|
|统一前缀|`/ipaklink/v1`。下文 URL 为相对前缀路径。|
|响应体|`{"code":200,"message":"ok","data":{}}`。code 与 HTTP 状态一致，message 为 i18n key，失败 data 省略，可返回 subCode。|
|分页|默认 page=1、pageSize=20，上限 100；data 包含 list、total、page、pageSize。页面总量基于同一 snapshotId 和筛选。|
|命名|HTTP/Kafka JSON、query、路径变量 camelCase；事件名为小写 snake_case。|
|时间|时间戳 UTC RFC3339，毫秒可选。经营统计日期按 Asia/Tashkent，YYYY-MM-DD，fromDate 含、toDate 不含。|
|动词|GET 读、POST 创建和动作、PUT 整体替换。查询使用 GET。|
|认证|App 登录使用 Bearer accessToken，匿名采集使用短时 `X-Analytics-Token`。后台 Bearer 用内部 SSO，服务端从 token 确定角色和授权范围。|
|追踪|服务端生成并响应 `X-Request-Id`。业务关联使用明确字段，requestId 只供诊断。|
|幂等|批次重试保持 batchId，分拆重试生成新 batchId；事件永远保持 eventId。作业创建使用 Idempotency-Key，作用域为操作人和端点。|
|单位|比例取 0–1，时长为毫秒，数量 JSON 整数。没有有效分母时 value=null。|
|默认排序|趋势按日期升序；榜单按指标降序再按主键升序；同一 snapshotId 保持稳定。|
|空数据|列表为空时返回 list=[]、total=0。所有默认日期均取最近 7 个完整业务日，超范围请求返回 400。|

|HTTP|subCode|message|触发与调用方动作|
|---|---:|---|---|
|400|40021|analytics.invalidRequest|缺字段、非法枚举、非法日期或超出查询区间；修正请求|
|400|40022|analytics.unsupportedFilter|过滤字段或指标组合不在白名单|
|401|40121|analytics.unauthenticated|无有效凭据；完成授权后重试同一身份|
|403|40321|analytics.permissionDenied|访问角色、工单或数据范围不足|
|403|40322|analytics.collectionDisabled|采集策略关闭，清理该身份禁采队列|
|404|40421|analytics.notFound|授权范围内资源不存在|
|409|40921|analytics.idempotencyConflict|同一键请求负载冲突|
|409|40922|analytics.snapshotExpired|快照已过期，清空快照参数重查|
|413|41321|analytics.batchTooLarge|超过事件数、字节数或解压限额，拆小后重试|
|429|42921|analytics.rateLimited|返回 Retry-After 秒数|
|503|50321|analytics.temporarilyUnavailable|入口依赖失败，可退避重试|
|503|50322|analytics.queryTimeout|查询超时，重试或缩小范围|

示例错误 `{"code":429,"message":"analytics.rateLimited","subCode":42921}`，HTTP 响应头 `Retry-After: 5`。

## 1.2 公共请求与响应结构

|字段|类型|必填|约束|
|---|---|---|---|
|fromDate / toDate|string 日期|成对可选|指定日区间；通用最多 90 天；漏斗和质量最多 31 天；留存 cohort 最多 90 天|
|platform|string|否|ios / android|
|regionId|int|否|既有地区字典；需在后台角色范围内|
|appVersion|string|否|精确版本，1–40 字符|
|snapshotId|string|否|已公布发布清单 ID；缺省使用最新，内部快照保留至少两代|
|page / pageSize|int|列表可选|默认 1 / 20；pageSize 1–100|

各接口只允许本节及其明细列出的筛选。若提供日期，只接受同一业务时区。返回报表的 `data.meta` 使用统一结构。

接口表中的 receive 数据按参数位置阅读。GET 的 query 示例放入 URL 查询字符串，path 示例替换 URL 占位符。POST 的 JSON body 直接写入请求体。示例若分列 headers 和 body，按各自位置发送。鉴权失败统一适用全局 401，表内错误响应补充各接口的业务分支。

```json
{
  "snapshotId":"snap_20261113_0905", "metricVersion":1, "timezone":"Asia/Tashkent",
  "generatedAt":"2026-11-13T04:05:00Z", "sourceWatermark":"2026-11-13T04:01:00Z",
  "fromDate":"2026-11-06", "toDate":"2026-11-13", "status":"ready",
  "provisional":false, "coverage":0.99
}
```

status 为 ready / stale / partial。coverage 为与该查询相关的已验证数据覆盖率，无法计算返回 null。日期序列每点用 date，指标对象通用为 `{metricCode,value,unit,numerator?,denominator?,status}`。unit 为 count / ratio / ms；status 为 ready / provisional / suppressed / unavailable；suppressed 时数值及可逆推出的分子分母均为 null。response 示例中的示例 ID 为非生产数据。

## 1.3 采集与配置

```text
GET    /analytics/config                              IPK_ANL_01  获取采集配置
POST   /analytics/anonymous-tokens                    IPK_ANL_02  创建匿名采集凭据
POST   /analytics/events/report                       IPK_ANL_03  批量上报行为
```

### 1.3.1 IPK_ANL_01 获取采集配置

|**IPK_ANL_01**||
|---|---|
|功能名称|获取采集配置|
|Description|启动、登录变更或配置过期时读取策略。支持未登录请求，登录传 Bearer。客户端按 returned policyVersion 保存策略，不可用时沿用未过期缓存。|
|URL|`/ipaklink/v1/analytics/config`|
|UI设计页面|App 采集 SDK|
|METHOD|GET|
|鉴权|可匿名；登录时传 Bearer|
|parameters|见下方请求参数表|
|receive数据|query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;platform&quot;: &quot;android&quot;,<br>&nbsp;&nbsp;&quot;appVersion&quot;: &quot;1.2.0&quot;,<br>&nbsp;&nbsp;&quot;installId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419f9d&quot;<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;enabled&quot;: true,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;anonymousEnabled&quot;: true,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;policyVersion&quot;: 3,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;schemaVersions&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;1<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;maxBatchSize&quot;: 100,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;maxBatchBytes&quot;: 262144,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;maxAgeSeconds&quot;: 604800,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;flushIntervalMs&quot;: 10000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;configTtlSeconds&quot;: 300<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|enabled、anonymousEnabled 为布尔值；policyVersion 和 schemaVersions 表示版本；maxBatchSize、maxBatchBytes、maxAgeSeconds、flushIntervalMs、configTtlSeconds 为整数。|
|错误响应|400 参数错误；401 已传凭据无效；429 限流；503 配置不可用。|
|业务规则|App 解析配置。版本禁用返回 200 enabled=false。配置响应使用 private 缓存或 no-store。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|platform|query|string|是|ios / android|
|appVersion|query|string|是|1–40 字符|
|installId|query|UUID|是|安装随机标识|

### 1.3.2 IPK_ANL_02 创建匿名采集凭据

|**IPK_ANL_02**||
|---|---|
|功能名称|创建匿名采集凭据|
|Description|为匿名安装阶段签发采集凭据。签名绑定环境、installId、identityEpoch、用途、失效时间。经 Redis IP/安装限流。|
|URL|`/ipaklink/v1/analytics/anonymous-tokens`|
|UI设计页面|App 采集 SDK|
|METHOD|POST|
|鉴权|匿名；按 IP 和安装限流|
|parameters|见下方请求参数表|
|receive数据|JSON body<br><code>{<br>&nbsp;&nbsp;&quot;installId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419f9d&quot;,<br>&nbsp;&nbsp;&quot;identityEpoch&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419f9e&quot;,<br>&nbsp;&nbsp;&quot;platform&quot;: &quot;android&quot;,<br>&nbsp;&nbsp;&quot;appVersion&quot;: &quot;1.2.0&quot;<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;analyticsToken&quot;: &quot;signed-example&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;expiresIn&quot;: 900,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;policyVersion&quot;: 3<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|analyticsToken:string、expiresIn:int（秒）、policyVersion:int。|
|错误响应|400 参数错误；403 禁用匿名采集；429 限流；503 依赖不可用。|
|业务规则|响应 no-store。凭据只允许匿名行为上报。匿名禁用 403，过期时以同 epoch 重取。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|installId、identityEpoch|body|UUID / UUID|是|小写标准格式|
|platform|body|string|是|ios / android|
|appVersion|body|string|是|1–40 字符|

### 1.3.3 IPK_ANL_03 批量上报行为

|**IPK_ANL_03**||
|---|---|
|功能名称|批量上报行为|
|Description|单请求只允许一个安装和身份阶段。批次外层错误返回 HTTP 错误；事件逐条验证与 ACK，成功响应逐条列出状态。|
|URL|`/ipaklink/v1/analytics/events/report`|
|UI设计页面|App 采集 SDK|
|METHOD|POST|
|鉴权|匿名凭据或登录 Bearer，二选一|
|parameters|见下方请求参数表|
|receive数据|JSON body<br><code>{<br>&nbsp;&nbsp;&quot;batchId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419f90&quot;,<br>&nbsp;&nbsp;&quot;installId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419f9d&quot;,<br>&nbsp;&nbsp;&quot;identityEpoch&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419f9e&quot;,<br>&nbsp;&nbsp;&quot;identityType&quot;: &quot;anonymous&quot;,<br>&nbsp;&nbsp;&quot;platform&quot;: &quot;android&quot;,<br>&nbsp;&nbsp;&quot;appVersion&quot;: &quot;1.2.0&quot;,<br>&nbsp;&nbsp;&quot;appLanguage&quot;: &quot;zh&quot;,<br>&nbsp;&nbsp;&quot;policyVersion&quot;: 3,<br>&nbsp;&nbsp;&quot;events&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa1&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventName&quot;: &quot;screen_view&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;schemaVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;occurredAt&quot;: &quot;2026-11-13T04:00:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sessionId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa2&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sequence&quot;: 2,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;screenName&quot;: &quot;FactoryDetail&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageViewId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa3&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;properties&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;entrySource&quot;: &quot;search&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa4&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventName&quot;: &quot;screen_view&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;schemaVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;occurredAt&quot;: &quot;2026-11-13T04:00:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sessionId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa2&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sequence&quot;: 3,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;screenName&quot;: &quot;FactoryDetail&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageViewId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa3&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;properties&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;entrySource&quot;: &quot;search&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa5&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventName&quot;: &quot;screen_view&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;schemaVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;occurredAt&quot;: &quot;2026-11-13T04:00:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sessionId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa2&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sequence&quot;: 4,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;screenName&quot;: &quot;FactoryDetail&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageViewId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa3&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;properties&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;entrySource&quot;: &quot;badValue&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa6&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventName&quot;: &quot;screen_view&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;schemaVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;occurredAt&quot;: &quot;2026-11-13T04:00:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sessionId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa2&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sequence&quot;: 5,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;screenName&quot;: &quot;FactoryDetail&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageViewId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa3&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;properties&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;entrySource&quot;: &quot;search&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;]<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;batchId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419f90&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;results&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;index&quot;: 0,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa1&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;accepted&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;index&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa4&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;duplicate&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;index&quot;: 2,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa5&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;rejected&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;errorCode&quot;: &quot;invalidProperties&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;index&quot;: 3,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa6&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;retry&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;errorCode&quot;: &quot;ackUnknown&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;]<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|batchId:UUID；results 每项为 index:int、eventId:UUID/null、status:string、errorCode?:string。|
|错误响应|400 外层非法；401 无有效凭据；403 停止采集；413 超限；429 整批限流；503 入口不可用。HTTP 200 仍需逐条判断。|
|业务规则|accepted 或 duplicate 可删队列，rejected 丢弃并计端侧质量，retry 保留。同批重复 ID 按 index 返回，身份不匹配拒绝整批。 逐条字段及状态见本条下方的采集数据与逐条回执。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|batchId、installId、identityEpoch|body|UUID|是|同一安装与身份阶段|
|identityType|body|string|是|anonymous / authenticated|
|platform、appVersion|body|string|是|ios / android；版本 1–40 字符|
|appLanguage、regionId|body|string / int|否|业务字典|
|policyVersion|body|int|是|正整数|
|events|body|object[]|是|1–100 条，单事件不超过 8 KiB，解压后不超过 256 KiB|

#### 1.3.3.1 IPK_ANL_03 采集数据与逐条回执

|层级|字段|类型及要求|
|---|---|---|
|批次|batchId、installId、identityEpoch|必填 UUID，小写标准格式|
|批次|identityType、platform、appVersion、policyVersion|必填，anonymous/authenticated、ios/android、1–40 字符、正整数；两类身份凭据互斥|
|批次|appLanguage、regionId|可选，zh/en/ru/uz 和地区字典整数 ID，未传归 unknown|
|批次|events|必填数组，1–100 条|
|事件|eventId、sessionId|必填 UUID；重试保持不变|
|事件|eventName、schemaVersion|必填，白名单字符串与正整数版本|
|事件|occurredAt|必填 UTC；首次受理前 7 天至未来 5 分钟|
|事件|sequence|必填整数 1–2147483647，会话内递增|
|事件|screenName、pageViewId|页面进入、退出、实体展示、联系点击必填，稳定路由名和 UUID|
|事件|properties|必填对象，无属性用空对象，未知字段拒绝|
|逐条回执|index、eventId、status、errorCode|index 从 0 开始，非法 ID 时 eventId=null；失败或重试带 errorCode|

|status|errorCode|处理|
|---|---|---|
|accepted|省略|Kafka 持久 ACK 后清除队列事件|
|duplicate|省略|同身份、同 ID、同语义内容已经持久确认，可清除|
|rejected|invalidEvent、unknownEvent、unsupportedSchema、invalidProperties、timeOutOfRange、eventIdConflict|永久错误，丢弃并计诊断数|
|retry|brokerUnavailable、ackUnknown、ingestBusy|保留事件 ID 与原内容，退避重试|

200 可包含任意逐条状态。身份不匹配整批拒绝，HTTP 429/503 重试整批。ACK 未知或 ACK 后状态更新失败均可返回 retry。重复检查排除 batchId、requestId、接收时间等传输字段，使用原始身份和规范化语义字段；App 禁止自报 source、actorKey、receivedAt、producer。后台计算哈希使用确定性排序的 JSON 后做 SHA-256。

App 可上报事件的字段白名单、触发点和请求样例见[按功能划分的采集字段与接口](#4-按功能划分的采集字段与接口)。服务端事件提交至本接口时按不在 App 白名单处理，逐条返回 rejected 和 unknownEvent。

## 1.4 看板配置与经营总览

```text
GET    /internal/analytics/metadata                   IPK_ANL_10  获取快照和看板元数据
GET    /internal/analytics/overview                   IPK_ANL_11  经营总览
```

### 1.4.1 IPK_ANL_10 获取快照和看板元数据

|**IPK_ANL_10**||
|---|---|
|功能名称|获取快照和看板元数据|
|Description|提供最新可用快照、授权页面、过滤字典和订阅信息。WS 断线时客户端轮询本接口。|
|URL|`/ipaklink/v1/internal/analytics/metadata`|
|UI设计页面|获取快照和看板元数据|
|METHOD|GET|
|鉴权|analytics.read|
|parameters|见下方请求参数表|
|receive数据|query 参数，GET 无请求体<br><code>{}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;availableReports&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;overview&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;pages&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;features&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;funnels&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;retention&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;dimensions&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;platform&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;ios&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;android&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;searchScope&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;all&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;company&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;product&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;user&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;translateMode&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;text&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;dialog&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;image&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;file&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]<br>&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;streamEvent&quot;: &quot;ipaklink.analytics.snapshot.updated.emit&quot;<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|snapshotId、timezone、generatedAt、streamEvent 为字符串；metricVersion 为整数；availableReports 为字符串数组；dimensions 为维度映射。|
|错误响应|403 无权限；409 快照过期；503 服务暂不可用。|
|业务规则|按操作者范围返回可用页面、指标/维度枚举。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|snapshotId|query|string|否|默认最新快照，过期返回 409|

### 1.4.2 IPK_ANL_11 经营总览

|**IPK_ANL_11**||
|---|---|
|功能名称|经营总览|
|Description|返回活跃、业务浏览、询盘、翻译成功和同长度上期趋势。|
|URL|`/ipaklink/v1/internal/analytics/overview`|
|UI设计页面|经营总览|
|METHOD|GET|
|鉴权|analytics.read|
|parameters|见下方请求参数表|
|receive数据|query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&quot;platform&quot;: &quot;android&quot;<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;cards&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;dau&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 320,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;count&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;previousValue&quot;: 300,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;relativeChange&quot;: 0.0667<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;wau&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 1000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;count&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;mau&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 1400,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;count&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;anonymousUv&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 180,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;count&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;businessViewPv&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 2512,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;count&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;inquiryCreated&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 31,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;count&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;previousValue&quot;: 25,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;relativeChange&quot;: 0.24<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;translateSuccessRate&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 0.9,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;ratio&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;numerator&quot;: 90,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;denominator&quot;: 100,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;previousValue&quot;: 0.85,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;differencePercentagePoints&quot;: 5<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;series&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;date&quot;: &quot;2026-11-12&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metrics&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;dau&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 320,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;count&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;meta&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceWatermark&quot;: &quot;2026-11-13T04:01:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;provisional&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.99<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|cards 为指标对象数组，可附 previousValue:number/null、relativeChange:number/null；series 为 date:string、metrics:Metric[] 数组。必备卡片 dau、wau、mau、anonymousUv、businessViewPv、inquiryCreated、translateSuccessRate；比率卡可附 previousValue 和 differencePercentagePoints:number/null。 同时包含 data.meta，结构见全局约定。|
|错误响应|400 筛选无效；403 越权；409 快照过期；503 查询超时。|
|业务规则|活跃卡取最后选中日的窗口值；流量和业务计数卡为区间合计，转化率重算分子分母。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|fromDate、toDate|query|date / date|成对可选|起日含、止日不含，默认最近 7 个完整业务日|
|platform|query|string|否|ios / android|
|regionId|query|int|否|现有地区字典 ID，按角色范围校验|
|appVersion|query|string|否|精确匹配，1–40 字符|
|snapshotId|query|string|否|默认最新快照，过期返回 409|
|includePrevious|query|boolean|否|默认 true|

## 1.5 页面、功能、漏斗与留存

```text
GET    /internal/analytics/pages                      IPK_ANL_12  页面分析
GET    /internal/analytics/features                   IPK_ANL_13  功能分析
GET    /internal/analytics/funnels/{funnelCode}       IPK_ANL_14  固定漏斗
GET    /internal/analytics/retention                  IPK_ANL_15  留存 Cohort
```

### 1.5.1 IPK_ANL_12 页面分析

|**IPK_ANL_12**||
|---|---|
|功能名称|页面分析|
|Description|页面排行及单页停留分布。|
|URL|`/ipaklink/v1/internal/analytics/pages`|
|UI设计页面|页面分析|
|METHOD|GET|
|鉴权|analytics.read|
|parameters|见下方请求参数表|
|receive数据|query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;screenName&quot;: &quot;FactoryDetail&quot;,<br>&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&quot;pageSize&quot;: 20<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;list&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;screenName&quot;: &quot;FactoryDetail&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;pv&quot;: 200,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;uv&quot;: 180,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;closedCount&quot;: 190,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eligibleDurationCount&quot;: 190,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;closureRate&quot;: 0.95,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;avgVisibleMs&quot;: 12200,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;p50VisibleMs&quot;: 9000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;p95VisibleMs&quot;: 39000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;exitRate&quot;: 0.2<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;total&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageSize&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;durationBuckets&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;lowerMs&quot;: 0,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;upperMs&quot;: 5000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 34<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;lowerMs&quot;: 5000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;upperMs&quot;: 15000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 80<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;lowerMs&quot;: 15000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;upperMs&quot;: 30000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 50<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;lowerMs&quot;: 30000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;upperMs&quot;: 60000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 24<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;lowerMs&quot;: 60000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;upperMs&quot;: null,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 2<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;meta&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceWatermark&quot;: &quot;2026-11-13T04:01:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;provisional&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.99<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|list 为页面行数组；每行 screenName:string；pv、uv、closedCount、eligibleDurationCount:int/null；closureRate、exitRate:number/null；avgVisibleMs、p50VisibleMs、p95VisibleMs:number/null。durationBuckets 每项 lowerMs:int、upperMs:int/null、count:int/null。 同时包含 data.meta，结构见全局约定。|
|错误响应|400 页面、排序或筛选无效；403 越权；409 快照过期；503 查询超时。|
|业务规则|durationBuckets 仅指定 screenName 时返回完整五桶，最后一桶 upperMs=null。列表退出率只使用成熟会话。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|fromDate、toDate|query|date / date|成对可选|起日含、止日不含，默认最近 7 个完整业务日|
|platform|query|string|否|ios / android|
|regionId|query|int|否|现有地区字典 ID，按角色范围校验|
|appVersion|query|string|否|精确匹配，1–40 字符|
|snapshotId|query|string|否|默认最新快照，过期返回 409|
|screenName|query|string|否|已注册页面名|
|sortBy|query|string|否|pv / uv / p95VisibleMs / exitRate，默认 pv|
|page、pageSize|query|int / int|否|默认 1 / 20，pageSize 上限 100|

### 1.5.2 IPK_ANL_13 功能分析

|**IPK_ANL_13**||
|---|---|
|功能名称|功能分析|
|Description|搜索、收藏、聊天、翻译各模块的总量、日趋势、状态及可分页分组。|
|URL|`/ipaklink/v1/internal/analytics/features`|
|UI设计页面|功能分析|
|METHOD|GET|
|鉴权|analytics.read|
|parameters|见下方请求参数表|
|receive数据|query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;feature&quot;: &quot;translate&quot;,<br>&nbsp;&nbsp;&quot;groupBy&quot;: &quot;languagePair&quot;,<br>&nbsp;&nbsp;&quot;mode&quot;: &quot;text&quot;,<br>&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&quot;pageSize&quot;: 20<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;feature&quot;: &quot;translate&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;summary&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;translateSuccessRate&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 0.9,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;ratio&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;numerator&quot;: 90,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;denominator&quot;: 100,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;series&quot;: [],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;list&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;groupKey&quot;: &quot;zh:uz&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metrics&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricCode&quot;: &quot;translateP95Ms&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;value&quot;: 1100,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unit&quot;: &quot;ms&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;total&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageSize&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;meta&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceWatermark&quot;: &quot;2026-11-13T04:01:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;provisional&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.99<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|summary:Metric[]；series:{date,metrics}[]；list:{groupKey,metrics}[]。search 输出 searchSubmitted、searchZeroResultRate、searchResultOpenRate、searchFailureRate、searchP95Ms；favorite 输出 favoriteAdded、favoriteRemoved、favoriteActors；chat 输出 chatOpened、messageSent、message24hReplyRate、messageReplyP95Ms；translate 输出 translateStarted、translateCompleted、translateSuccessRate、translateP95Ms、translateSyncFailureRate。 同时包含 data.meta，结构见全局约定。|
|错误响应|400 模块或分组不匹配；403 越权；409 快照过期；503 查询超时。|
|业务规则|支持下钻失败码。search 支持 scope/errorCode/appVersion；favorite 支持 appVersion；chat 支持 entrySource/appVersion；translate 支持 mode/languagePair/errorCode/appVersion。无 groupBy 时 list=[]。 支持错误码下钻时可用 groupBy=errorCode，仅返回固定枚举。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|fromDate、toDate|query|date / date|成对可选|起日含、止日不含，默认最近 7 个完整业务日|
|platform|query|string|否|ios / android|
|regionId|query|int|否|现有地区字典 ID，按角色范围校验|
|appVersion|query|string|否|精确匹配，1–40 字符|
|snapshotId|query|string|否|默认最新快照，过期返回 409|
|feature|query|string|是|search / favorite / chat / translate|
|groupBy|query|string|否|仅允许所属功能的分组，见业务规则|
|scope|query|string|否|仅 search，all / company / product / user|
|mode|query|string|否|仅 translate，text / dialog / image / file|
|sourceLang、targetLang|query|string|否|仅 translate，业务语言字典|
|page、pageSize|query|int / int|否|默认 1 / 20，pageSize 上限 100|

### 1.5.3 IPK_ANL_14 固定漏斗

|**IPK_ANL_14**||
|---|---|
|功能名称|固定漏斗|
|Description|查询 F01 详情到询盘或 F02 搜索到详情。|
|URL|`/ipaklink/v1/internal/analytics/funnels/{funnelCode}`|
|UI设计页面|固定漏斗|
|METHOD|GET|
|鉴权|analytics.read|
|parameters|见下方请求参数表|
|receive数据|path / query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;path&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;funnelCode&quot;: &quot;F01&quot;<br>&nbsp;&nbsp;},<br>&nbsp;&nbsp;&quot;query&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;entrySource&quot;: &quot;search&quot;<br>&nbsp;&nbsp;}<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;funnelCode&quot;: &quot;F01&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;windowSeconds&quot;: 86400,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;entityUnit&quot;: &quot;actor&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;steps&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;stepCode&quot;: &quot;detail&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 100,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;uniqueActors&quot;: 100,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;stepRate&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;totalRate&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;p50FromPreviousMs&quot;: null<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;stepCode&quot;: &quot;contact&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 30,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;uniqueActors&quot;: 30,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;stepRate&quot;: 0.3,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;totalRate&quot;: 0.3,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;p50FromPreviousMs&quot;: 23000<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;stepCode&quot;: &quot;submit&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 18,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;uniqueActors&quot;: 18,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;stepRate&quot;: 0.6,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;totalRate&quot;: 0.18,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;p50FromPreviousMs&quot;: 56000<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;stepCode&quot;: &quot;created&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 16,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;uniqueActors&quot;: 16,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;stepRate&quot;: 0.8889,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;totalRate&quot;: 0.16,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;p50FromPreviousMs&quot;: 700<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;attribution&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;businessCount&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;attributedCount&quot;: 16,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.8,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;reasons&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;reasonCode&quot;: &quot;missingFlow&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;count&quot;: 4<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]<br>&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;meta&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceWatermark&quot;: &quot;2026-11-13T04:01:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;provisional&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.99<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|funnelCode:string、windowSeconds:int、entityUnit:string、steps:array；步骤含 stepCode:string、count/uniqueActors:int/null、stepRate/totalRate/p50FromPreviousMs:number/null。F01 attribution 含 businessCount、attributedCount:int/null、coverage:number/null、reasons:{reasonCode,count}[]。F02 返回 zeroResultCount、failedCount:int/null。 同时包含 data.meta，结构见全局约定。|
|错误响应|400 漏斗或筛选无效；403 越权；409 快照过期；503 查询超时。|
|业务规则|F02 entityUnit=search，steps 为 submit / hasResults / open / detail，另返零结果和失败数量。F01 attribution 按提交日单列，不同于按锚点日分组的漏斗。 F01 attribution.reasons 的 reasonCode 为 missingFlow / subjectMismatch / outsideWindow / invalidSequence。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|funnelCode|path|string|是|F01 / F02|
|fromDate、toDate|query|date / date|否|成对提供时最多 31 天；默认最近 7 个完整业务日|
|platform|query|string|否|ios / android|
|regionId|query|int|否|现有地区字典 ID，按角色范围校验|
|appVersion|query|string|否|精确匹配，1–40 字符|
|snapshotId|query|string|否|默认最新快照，过期返回 409|
|entrySource|query|string|否|入口枚举|
|targetType|query|string|否|仅 F02，company / product|
|searchScope|query|string|否|仅 F02，all / company / product|

### 1.5.4 IPK_ANL_15 留存 Cohort

|**IPK_ANL_15**||
|---|---|
|功能名称|留存 Cohort|
|Description|返回首活 cohort 的 D1/D7/D30 及成熟度。|
|URL|`/ipaklink/v1/internal/analytics/retention`|
|UI设计页面|留存 Cohort|
|METHOD|GET|
|鉴权|analytics.read|
|parameters|见下方请求参数表|
|receive数据|query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;offsetDays&quot;: &quot;1,7,30&quot;,<br>&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&quot;pageSize&quot;: 20<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;list&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;cohortDate&quot;: &quot;2026-11-10&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;cohortSize&quot;: 100,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;points&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;offsetDay&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;retainedCount&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;rate&quot;: 0.2,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;mature&quot;: true,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;expectedMatureDate&quot;: &quot;2026-11-12&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;offsetDay&quot;: 7,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;retainedCount&quot;: null,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;rate&quot;: null,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;mature&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;expectedMatureDate&quot;: &quot;2026-11-18&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;offsetDay&quot;: 30,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;retainedCount&quot;: null,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;rate&quot;: null,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;mature&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;expectedMatureDate&quot;: &quot;2026-12-11&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;]<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;total&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageSize&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;unknownCohortCount&quot;: 5,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;meta&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceWatermark&quot;: &quot;2026-11-13T04:01:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;provisional&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.99<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|list 每项含 cohortDate:string、cohortSize:int/null、points 数组。点含 offsetDay:int、retainedCount:int/null、rate:number/null、mature:boolean、expectedMatureDate:string。unknownCohortCount:int/null。 同时包含 data.meta，结构见全局约定。|
|错误响应|400 日期或观察日无效；403 越权；409 快照过期；503 查询超时。|
|业务规则|expectedMatureDate 为最早跨过观察日的日期，最终还需源水位满足。 platform、regionId、appVersion 使用首次活跃快照。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|fromDate、toDate|query|date / date|成对可选|起日含、止日不含，默认最近 7 个完整业务日|
|platform|query|string|否|ios / android|
|regionId|query|int|否|现有地区字典 ID，按角色范围校验|
|appVersion|query|string|否|精确匹配，1–40 字符|
|snapshotId|query|string|否|默认最新快照，过期返回 409|
|offsetDays|query|string|否|逗号分隔，1,7,30 的非空子集，默认全部|
|page、pageSize|query|int / int|否|默认 1 / 20，pageSize 上限 100|

## 1.6 会话排查与数据质量

```text
GET    /internal/analytics/sessions/{sessionId}       IPK_ANL_16  受控会话时间线
GET    /internal/analytics/quality                    IPK_ANL_17  采集与仓库质量
```

### 1.6.1 IPK_ANL_16 受控会话时间线

|**IPK_ANL_16**||
|---|---|
|功能名称|受控会话时间线|
|Description|返回可见行为、页面停留和允许的业务事件。读取前校验工单并落审计。|
|URL|`/ipaklink/v1/internal/analytics/sessions/{sessionId}`|
|UI设计页面|受控会话时间线|
|METHOD|GET|
|鉴权|analytics.session.read + 工单授权|
|parameters|见下方请求参数表|
|receive数据|path / query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;path&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;sessionId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa2&quot;<br>&nbsp;&nbsp;},<br>&nbsp;&nbsp;&quot;query&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;ticketId&quot;: &quot;OPS-127&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageSize&quot;: 20<br>&nbsp;&nbsp;}<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;sessionId&quot;: &quot;3030e190-a030-4bba-9cec-057d5f419fa2&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;list&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;eventName&quot;: &quot;screen_view&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;occurredAt&quot;: &quot;2026-11-12T04:00:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sequence&quot;: 2,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;screenName&quot;: &quot;FactoryDetail&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;visibleMs&quot;: 35000,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;closureStatus&quot;: &quot;closed&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;total&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageSize&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;meta&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceWatermark&quot;: &quot;2026-11-13T04:01:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;provisional&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.99<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|sessionId:string；list 每项 eventName:string、occurredAt:string、sequence:int/null、screenName:string/null、visibleMs:int/null、closureStatus:string/null。closureStatus=closed/open/unknown。允许的安全业务摘要可用 details:{outcome?,targetType?,targetRef?}。 同时包含 data.meta，结构见全局约定。|
|错误响应|403 工单或权限不足；404 会话不存在；409 快照过期；503 审计或查询不可用。|
|业务规则|只查授权范围内近 30 天；同一请求不返回实际用户 ID、聊天正文、译文。 同一 snapshotId 保证翻页顺序。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|sessionId|path|UUID|是|分析会话 ID|
|ticketId|query|string|是|有效用途工单|
|snapshotId|query|string|否|默认最新快照，过期返回 409|
|page、pageSize|query|int / int|否|默认 1 / 20，pageSize 上限 100|

### 1.6.2 IPK_ANL_17 采集与仓库质量

|**IPK_ANL_17**||
|---|---|
|功能名称|采集与仓库质量|
|Description|提供分钟或日质量指标及来源对账。|
|URL|`/ipaklink/v1/internal/analytics/quality`|
|UI设计页面|采集与仓库质量|
|METHOD|GET|
|鉴权|analytics.quality.read|
|parameters|见下方请求参数表|
|receive数据|query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;granularity&quot;: &quot;day&quot;,<br>&nbsp;&nbsp;&quot;source&quot;: &quot;server&quot;,<br>&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&quot;pageSize&quot;: 20<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;summary&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;accepted&quot;: 7400,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;rejected&quot;: 30,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;duplicates&quot;: 29,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;late&quot;: 11,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;dlq&quot;: 0,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;lagRecords&quot;: 89,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;p95IngestDelayMs&quot;: 37000<br>&nbsp;&nbsp;&nbsp;&nbsp;},<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;series&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;bucket&quot;: &quot;2026-11-12T19:00:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;accepted&quot;: 7400,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;rejected&quot;: 30<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;list&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;factCode&quot;: &quot;inquiryCreated&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;date&quot;: &quot;2026-11-12&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceCount&quot;: 31,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;warehouseCount&quot;: 31,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;delta&quot;: 0,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;unattributedCount&quot;: 4<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;total&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageSize&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;meta&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceWatermark&quot;: &quot;2026-11-13T04:01:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;provisional&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.99<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|summary 及 series 按相同字段输出，可加入 rateLimited、retry、stalePartitions 整数计数；series 含 bucket:string。对账行 factCode/date:string、sourceCount/warehouseCount/delta/unattributedCount:int/null。 同时包含 data.meta，结构见全局约定。|
|错误响应|400 日期或粒度非法；403 越权；409 快照过期；503 查询超时。|
|业务规则|delta=warehouseCount-sourceCount；质量数据过期显示 stale。 分钟桶为 UTC 时间戳；日桶为业务时区日边界对应的 UTC 时间戳。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|fromDate、toDate|query|date / date|否|day 最多 31 天；minute 最多 1 天，默认最近一个完整业务日|
|platform|query|string|否|ios / android|
|appVersion|query|string|否|精确匹配，1–40 字符|
|snapshotId|query|string|否|默认最新快照，过期返回 409|
|source|query|string|否|app / server|
|granularity|query|string|否|minute / day，默认 day；minute 最多 1 天|
|page、pageSize|query|int / int|否|默认 1 / 20，pageSize 上限 100|

## 1.7 语料治理

```text
GET    /internal/analytics/corpus/summary             IPK_ANL_18  语料治理汇总
GET    /internal/analytics/corpus/jobs/{jobRef}       IPK_ANL_19  查询语料作业状态
```

### 1.7.1 IPK_ANL_18 语料治理汇总

|**IPK_ANL_18**||
|---|---|
|功能名称|语料治理汇总|
|Description|提供 Mongo 语料元数据的已发布聚合。|
|URL|`/ipaklink/v1/internal/analytics/corpus/summary`|
|UI设计页面|语料治理汇总|
|METHOD|GET|
|鉴权|corpus.summary.read + 数据集权限|
|parameters|见下方请求参数表|
|receive数据|query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;datasetCode&quot;: &quot;approved_terms&quot;,<br>&nbsp;&nbsp;&quot;languagePair&quot;: &quot;zh:uz&quot;,<br>&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&quot;pageSize&quot;: 20<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;list&quot;: [<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;datasetCode&quot;: &quot;approved_terms&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;languagePair&quot;: &quot;zh:uz&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;approved&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;itemCount&quot;: 350,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;latestJobRef&quot;: &quot;job_corpus_91&quot;<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;&nbsp;&nbsp;],<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;total&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;page&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;pageSize&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;meta&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;snapshotId&quot;: &quot;snap_20261113_0905&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;metricVersion&quot;: 1,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;timezone&quot;: &quot;Asia/Tashkent&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;generatedAt&quot;: &quot;2026-11-13T04:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;sourceWatermark&quot;: &quot;2026-11-13T04:01:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-06&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-13&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;ready&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;provisional&quot;: false,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&quot;coverage&quot;: 0.99<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|list 每项 datasetCode、languagePair、status:string，itemCount:int/null，latestJobRef:string/null。 同时包含 data.meta，结构见全局约定。|
|错误响应|400 筛选无效；403 数据集越权；409 快照过期；503 查询超时。|
|业务规则|日期筛选按语料条目创建日，状态取快照时状态。返回范围按数据集授权，只输出治理元数据。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|fromDate、toDate|query|date / date|成对可选|起日含、止日不含，默认最近 7 个完整业务日|
|snapshotId|query|string|否|默认最新快照，过期返回 409|
|datasetCode|query|string|否|授权数据集编号|
|languagePair|query|string|否|源语言:目标语言|
|status|query|string|否|pending / approved / rejected / removed|
|page、pageSize|query|int / int|否|默认 1 / 20，pageSize 上限 100|

### 1.7.2 IPK_ANL_19 查询语料作业状态

|**IPK_ANL_19**||
|---|---|
|功能名称|查询语料作业状态|
|Description|按 IPK_ANL_18 返回的 latestJobRef 查看导入、审核同步或清理作业进度。|
|URL|`/ipaklink/v1/internal/analytics/corpus/jobs/{jobRef}`|
|UI设计页面|查询语料作业状态|
|METHOD|GET|
|鉴权|corpus.job.read + 数据集权限|
|parameters|见下方请求参数表|
|receive数据|path / query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;path&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;jobRef&quot;: &quot;job_corpus_91&quot;<br>&nbsp;&nbsp;}<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;jobRef&quot;: &quot;job_corpus_91&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;datasetCode&quot;: &quot;approved_terms&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;type&quot;: &quot;import&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;succeeded&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;scannedCount&quot;: 20,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;acceptedCount&quot;: 18,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;rejectedCount&quot;: 2,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;createdAt&quot;: &quot;2026-11-13T03:00:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;finishedAt&quot;: &quot;2026-11-13T03:05:00Z&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;errorCode&quot;: null<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|jobRef、datasetCode、type、status、createdAt 为字符串；scannedCount、acceptedCount、rejectedCount 为整数；finishedAt、errorCode 可空。|
|错误响应|403 数据集越权；404 作业不存在；503 查询不可用。|
|业务规则|type 为 import/reviewSync/erase，status 为 queued/running/succeeded/failed。运行中 finishedAt=null；失败 errorCode 使用固定枚举 sourceUnavailable/policyDenied/validationFailed/internalError，避免返回处理日志原文。读取记录进审计。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|jobRef|path|string|是|1–80 字符|

## 1.8 回放与重算作业

```text
POST   /internal/analytics/repair-jobs                IPK_ANL_21  创建回放或重算作业
GET    /internal/analytics/repair-jobs/{jobId}        IPK_ANL_22  查询修复作业
```

### 1.8.1 IPK_ANL_21 创建回放或重算作业

|**IPK_ANL_21**||
|---|---|
|功能名称|创建回放或重算作业|
|Description|创建受控作业。状态先 queued，后台完成校验、构建和发布。|
|URL|`/ipaklink/v1/internal/analytics/repair-jobs`|
|UI设计页面|创建回放或重算作业|
|METHOD|POST|
|鉴权|analytics.repair.write + 工单授权|
|parameters|见下方请求参数表|
|receive数据|JSON body<br><code>{<br>&nbsp;&nbsp;&quot;headers&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;Idempotency-Key&quot;: &quot;repair-example-001&quot;<br>&nbsp;&nbsp;},<br>&nbsp;&nbsp;&quot;body&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;type&quot;: &quot;rebuild&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;fromDate&quot;: &quot;2026-11-10&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;toDate&quot;: &quot;2026-11-12&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;reasonCode&quot;: &quot;lateData&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;ticketId&quot;: &quot;OPS-127&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;dryRun&quot;: true<br>&nbsp;&nbsp;}<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;jobId&quot;: &quot;job_repair_101&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;queued&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;createdAt&quot;: &quot;2026-11-13T04:07:00Z&quot;<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|jobId:string、status:string、createdAt:string。首次响应状态为 queued。|
|错误响应|400 范围或重放坐标非法；403 工单或权限不足；409 幂等冲突；429 作业限流；503 创建失败。|
|业务规则|请求量上限每人每小时 5 次。重放 Offset 用十进制字符串，避免 JS 精度丢失。只允许分析输入白名单 Topic，实际发布与回放需工单授权。 reasonCode 为 lateData / missingData / schemaFix / reconciliation。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|Idempotency-Key|header|string|是|同一操作者同一路径下唯一|
|type|body|string|是|rebuild / replay|
|fromDate、toDate|body|date / date|是|业务日期，区间最多 31 天|
|reasonCode、ticketId|body|string / string|是|固定原因码及有效工单|
|dryRun|body|boolean|是|true 时只校验与估算|
|topic、partition|body|string / int|条件|replay 必填，白名单 Topic 和非负分区|
|startOffset、endOffsetExclusive|body|string|条件|replay 必填，十进制非负整数，结束值大于起始值|

### 1.8.2 IPK_ANL_22 查询修复作业

|**IPK_ANL_22**||
|---|---|
|功能名称|查询修复作业|
|Description|按 ID 查看运行状态、计数和关联快照。|
|URL|`/ipaklink/v1/internal/analytics/repair-jobs/{jobId}`|
|UI设计页面|查询修复作业|
|METHOD|GET|
|鉴权|analytics.quality.read 或 analytics.repair.write|
|parameters|见下方请求参数表|
|receive数据|path / query 参数，GET 无请求体<br><code>{<br>&nbsp;&nbsp;&quot;path&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;jobId&quot;: &quot;job_repair_101&quot;<br>&nbsp;&nbsp;}<br>}</code>|
|return数据|HTTP 200<br><code>{<br>&nbsp;&nbsp;&quot;code&quot;: 200,<br>&nbsp;&nbsp;&quot;message&quot;: &quot;ok&quot;,<br>&nbsp;&nbsp;&quot;data&quot;: {<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;jobId&quot;: &quot;job_repair_101&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;status&quot;: &quot;succeeded&quot;,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;dryRun&quot;: true,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;scannedCount&quot;: 1250,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;affectedDateCount&quot;: 2,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;publishedSnapshotId&quot;: null,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;errorCode&quot;: null,<br>&nbsp;&nbsp;&nbsp;&nbsp;&quot;finishedAt&quot;: &quot;2026-11-13T04:10:00Z&quot;<br>&nbsp;&nbsp;}<br>}</code>|
|返回字段|jobId、status 为字符串；dryRun 为布尔值；scannedCount、affectedDateCount 为整数；publishedSnapshotId、errorCode、finishedAt 可空。|
|错误响应|403 越权；404 作业不存在；503 查询不可用。|
|业务规则|状态 queued/running/succeeded/failed，成功 dryRun 的 publishedSnapshotId 为 null。|

**请求参数**

|字段|位置|类型|必填|说明|
|---|---|---|---|---|
|jobId|path|string|是|修复作业 ID|

## 1.9 既有业务接口扩展

|编号|既有协议|新增契约|
|---|---|---|
|IPK_ANL_B01|POST /inquiries|增加可选 contactFlowId UUID，沿用 clientRequestId 幂等。PG 提交后产生稳定事实。客户端 dialogId 与接口示例 sessionId 在联调时统一 DTO，保持旧端成功语义。|
|IPK_ANL_B02|POST /views|沿用业务入口，重试沿用同一操作键，有意再次浏览用新键；持久记录对应稳定 viewRef。|
|IPK_ANL_B03|POST /chat/dialogs/{id}/messages|消息首次写 Mongo 后产生一次事实，补发沿用业务键；dialogRef 在 App 网关与业务侧用同一假名算法。|
|IPK_ANL_B04|POST /translate/records|Mongo 持久同步状态变化后投递，按记录 ID 与状态版本去重。|
|IPK_ANL_B05|POST /favorites、POST /favorites/cancel|实际关系状态变化后投递，重复的幂等成功无新事实。|

# 2 Kafka 接口清单

|编号|Topic|生产者 / 消费者|key|保留|
|---|---|---|---|---|
|IPK_ANL_K01|analytics.event|Collector 与事实适配器 / analytics-warehouse-v1|App sessionId、服务端 aggregateRef|7 天|
|IPK_ANL_K02|inquiries.created|询盘服务 / 现有通知及 analytics-inquiry-adapter-v1|inquiryId|既有业务设置|
|IPK_ANL_K03|analytics.event.dlq.v1|仓库消费者 / analytics-repair-v1|原 key 或安全指纹|14 天|
|IPK_ANL_K04|analytics.dashboard.updated.v1|快照发布 outbox / WS 网关|reportScope|24 小时|

## 2.1 Kafka 全局约定

消息是 UTF-8 JSON。headers 含 schemaVersion、contentType、traceId、producer、可选 replayRunId。K01 初始 12 分区，RF=3、min ISR=2、acks=all；分区内有序，跨分区由事件时间及业务键关联。K02 适配器取得 inquiryId 后从 PG 补取可信 buyerId、companyId、productId、createdAt、clientRequestId 和 contactFlowId；PG 暂不可用重试，事实确定不存在则隔离。适配器收到 K01 持久 ACK 才提交 K02 位点，保留已有通知消费组。

## 2.2 Kafka Topic 明细

### 2.2.1 IPK_ANL_K01 分析消息字段

|**IPK_ANL_K01**||
|---|---|
|功能名称|分析事件投递|
|Description|接收 App 行为和已验证的服务端事实，供仓库消费。|
|Topic|`analytics.event`|
|生产者 / 消费者|Collector、业务事实适配器 / analytics-warehouse-v1|
|parameters|App 以 sessionId 为 key，服务端以 aggregateRef 为 key；详细字段见下。|
|receive数据|见本条 JSON 消息示例。|
|return数据|Broker 持久 ACK；仓库消费结果在质量看板查询。|

|字段|类型|约束|
|---|---|---|
|eventId、eventName、schemaVersion|UUID/string/int|稳定 ID、名称和版本|
|occurredAt、receivedAt、businessDate|string|UTC 时间及规范化业务日期，重放保留|
|source、producer|string|app/server；服务身份|
|actorKey、anonKey、identityType|string 或 null|分析假名，identityType=anonymous/authenticated/system，系统 actorKey 可空|
|sessionId、pageViewId、screenName、sequence|条件|App 会话字段；服务端仅由可信上下文补齐|
|platform、appVersion、appLanguage、regionId|条件|经验证快照，可空|
|aggregateRef|string|服务端必填安全引用|
|policyVersion、requestId、ingestBatchId|条件|App 受理元数据，服务端无批次则省略|
|payloadHash|string|规范化语义内容 SHA-256 十六进制|
|properties|object|按事件字段表，禁止正文和原始身份|

```json
{
  "eventId":"3030e190-a030-4bba-9cec-057d5f419fb0","eventName":"inquiry_created","schemaVersion":1,
  "occurredAt":"2026-11-13T04:00:00Z","receivedAt":"2026-11-13T04:00:01Z","businessDate":"2026-11-13",
  "source":"server","producer":"analytics-inquiry-adapter","actorKey":"u_example","identityType":"authenticated",
  "aggregateRef":"inquiry_example",
  "payloadHash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties":{"inquiryRef":"inquiry_example","companyId":"company_example","contactFlowId":"3030e190-a030-4bba-9cec-057d5f419fa5","clientRequestId":"req_example"}
}
```

7 项服务端事件的字段、业务来源、去重键与完整消息见[事件索引](#42-事件索引)，选择生产端为服务端的条目。

### 2.2.2 IPK_ANL_K02 询盘事实接入

|**IPK_ANL_K02**||
|---|---|
|功能名称|复用已提交询盘事件|
|Description|适配器从既有业务 Topic 提取询盘事实，投影为 K01 的 inquiry_created。|
|Topic|`inquiries.created`|
|生产者 / 消费者|询盘服务 / analytics-inquiry-adapter-v1，既有通知消费组独立运行|
|parameters|key 为 inquiryId。原消息字段和版本遵循既有业务契约，分析侧联调时确认。|
|receive数据|适配器从原消息定位 inquiryId，从 PG 权威记录补充 buyerId、companyId、productId、createdAt、clientRequestId 和 contactFlowId。|
|return数据|向 K01 投递安全事件，收到持久 ACK 后提交 K02 消费位点。PG 暂不可读时重试，事实确认不存在时隔离。|

### 2.2.3 IPK_ANL_K03 死信及重放

|**IPK_ANL_K03**||
|---|---|
|功能名称|仓库消费死信|
|Description|记录永久失败的安全定位信息，供受控修复使用。|
|Topic|`analytics.event.dlq.v1`|
|生产者 / 消费者|仓库消费者 / analytics-repair-v1|
|parameters|key 为原 key 或安全指纹，offset 采用十进制字符串。|
|receive数据|见本条 JSON 消息示例。|
|return数据|Broker 持久 ACK 后推进原消费位点；修复结果通过 IPK_ANL_22 查询。|

```json
{
  "sourceTopic":"analytics.event","partition":3,"offset":"23091",
  "eventId":"3030e190-a030-4bba-9cec-057d5f419fb0","schemaVersion":1,
  "errorCode":"unsupportedSchema","failedAt":"2026-11-13T04:03:00Z","attempts":1,"canReplay":true
}
```

永久错误经清洗后写 DLQ 并提交原位点。疑似敏感内容只留定位、错误码和摘要，canReplay=false。允许重放时读取原坐标或可信脱敏原始记录，使用十进制字符串表示 offset，保留 eventId、业务时间及身份，加 replayRunId，先检查删除抑制清单。

### 2.2.4 IPK_ANL_K04 发布通知

|**IPK_ANL_K04**||
|---|---|
|功能名称|看板快照更新通知|
|Description|通知 WebSocket 网关已有可查询的报表快照。|
|Topic|`analytics.dashboard.updated.v1`|
|生产者 / 消费者|快照发布 Outbox / WebSocket 网关|
|parameters|key 为 reportScope，以 snapshotId 去重。|
|receive数据|见本条 JSON 消息示例。|
|return数据|网关按授权范围发 IPK_ANL_WS_01 通知；客户端经 IPK_ANL_10 查询当前快照。|

```json
{
  "eventId":"3030e190-a030-4bba-9cec-057d5f419fb1","snapshotId":"snap_20261113_0905","metricVersion":1,
  "reportScope":"analytics","reports":["overview","pages","features","funnels","retention","quality"],
  "generatedAt":"2026-11-13T04:05:00Z","sourceWatermark":"2026-11-13T04:01:00Z"
}
```

PG 发布清单及 K04 待发送记录同事务提交。按 snapshotId 去重；旧通知或重连由 metadata 校准。

# 3 WebSocket 接口

```text
WS   /ipaklink/v1/internal/analytics/ws   IPK_ANL_WS_01  看板快照更新
```

## 3.1 IPK_ANL_WS_01 看板快照更新

|**IPK_ANL_WS_01**||
|---|---|
|功能名称|看板快照更新|
|Description|WS 网关消费 K04 并按权限通知内部 Web。看板数据由 HTTPS GET 返回。|
|URL|`/ipaklink/v1/internal/analytics/ws`|
|METHOD|WebSocket over TLS|
|parameters|同源 Secure/HttpOnly SSO 会话 Cookie，握手验证 Origin 和 analytics.read|
|receive数据|连接后按当前权限自动订阅|
|return数据|ipaklink.analytics.snapshot.updated.emit|

```json
{
  "data":{"snapshotId":"snap_20261113_0905","reports":["overview","pages"],"metricVersion":1},
  "meta":{"event":"ipaklink.analytics.snapshot.updated.emit","serverTs":1794542700000,"needAck":false,"version":"1.0","replyTo":"","seq":102}
}
```

seq 在连接内递增。重连先查 metadata，断线每 60 秒轮询。前端去重 snapshotId，并忽略筛选条件更新前发出的旧请求响应。

# 4 按功能划分的采集字段与接口

本章逐项定义 24 个事件。每项列出字段的实际位置、类型、必填规则和取值。App 示例是 `POST /ipaklink/v1/analytics/events/report` 的完整请求体，单条事件也必须放入 `events`。服务端示例是 Kafka `analytics.event` 的完整消息 value，key 为该条的 `aggregateRef`。下文的各事件示例独立，若组合为同一条链路，应使用对应的关联键和实际发生时间。

表中的「条件」按说明校验，缺少可选字段时省略。`string` 业务 ID 长度为 1–80 字符，UUID 为小写标准格式。业务 URL 默认前缀 `/ipaklink/v1`，端侧操作直接使用采集接口，服务端事件由业务提交后投递。24 项是当前方案定义的采集范围，具体字段不沿用业务 API 的整包 DTO。

**错误码字典** `networkTimeout`、`unavailable`、`validationFailed`、`permissionDenied`、`modelMissing`、`unsupportedLanguage`、`noTextDetected`、`engineFailed`、`unknown`。事件的 `properties.errorCode` 表示业务结果，IPK_ANL_03 的 `results[].errorCode` 表示采集处理结果。

## 4.1 功能和接口对照

表中 SDK、BIZ、GOV 编号对应[功能清单第 2 章](./ipaklink-埋点数据后台功能清单.md#2-功能全清单)。按事件拆分的验收项见[功能清单第 3 章](./ipaklink-埋点数据后台功能清单.md#3-采集事件子清单)。

|功能清单|具体采集环节|业务入口|后台查询|
|---|---|---|---|
|SDK-03 会话和前后台|EVT_01 会话建立、EVT_02 切后台|App 生命周期|IPK_ANL_11 总览、IPK_ANL_16 会话路径|
|SDK-04 页面|EVT_03 页面进入、EVT_04 退出与停留|App 导航与可见性|IPK_ANL_12 页面分析|
|BIZ-01 搜索|EVT_05 提交、EVT_06 首屏结果、EVT_07 打开结果|GET /search|IPK_ANL_13 搜索、IPK_ANL_14 F02|
|BIZ-02 详情和浏览|EVT_08 实体详情曝光、EVT_09 业务浏览持久化|GET /companies/{companyId}、GET /products/{productId}、POST /views|IPK_ANL_11 业务 PV、IPK_ANL_14 漏斗、IPK_ANL_17 对账|
|BIZ-03 收藏|EVT_10 实际添加或取消|POST /favorites、POST /favorites/cancel|IPK_ANL_13 收藏|
|BIZ-04 询盘|EVT_11 联系点击、EVT_12 真实提交、EVT_13 请求失败、EVT_14 成功入库|详情进入 InquiryEdit、POST /inquiries、inquiries.created|IPK_ANL_11 询盘总量、IPK_ANL_14 F01、IPK_ANL_17 对账|
|BIZ-05 站内消息|EVT_15 打开会话、EVT_16 首次持久化消息|聊天页、POST /chat/dialogs/{id}/messages|IPK_ANL_13 聊天、IPK_ANL_17 对账|
|SDK-07 通知|EVT_17 点击打开通知|通知中心或系统推送|事件入仓；当前未定义通知专用看板查询|
|SDK-08 聊天译文与复制|EVT_18 切换译文、EVT_22 复制译文|聊天或翻译结果页控件|事件入仓；当前未定义使用深度专用看板查询|
|BIZ-06 翻译|EVT_19 任务开始、EVT_20 执行终态、EVT_21 记录同步|端侧翻译执行、四种翻译记录同步接口|IPK_ANL_13 翻译|
|BIZ-07 注册|EVT_23 注册事务提交|POST /auth/register|事件入仓；当前未定义注册渠道专用看板查询|
|GOV-04 语料治理|EVT_24 条目状态变更|内部治理作业|IPK_ANL_18 汇总、IPK_ANL_19 作业状态|

## 4.2 事件索引

|章节|事件编号|功能模块|eventName|生产端|优先级|分析接入|
|---|---|---|---|---|---|---|
|4.3.1|[EVT_01](#431-evt_01-app_session_start)|会话与页面|`app_session_start`|App|P0|IPK_ANL_03 → K01|
|4.3.2|[EVT_02](#432-evt_02-app_background)|会话与页面|`app_background`|App|P0|IPK_ANL_03 → K01|
|4.3.3|[EVT_03](#433-evt_03-screen_view)|会话与页面|`screen_view`|App|P0|IPK_ANL_03 → K01|
|4.3.4|[EVT_04](#434-evt_04-screen_exit)|会话与页面|`screen_exit`|App|P0|IPK_ANL_03 → K01|
|4.4.1|[EVT_05](#441-evt_05-search_submit)|搜索与详情|`search_submit`|App|P0|IPK_ANL_03 → K01|
|4.4.2|[EVT_06](#442-evt_06-search_result)|搜索与详情|`search_result`|App|P0|IPK_ANL_03 → K01|
|4.4.3|[EVT_07](#443-evt_07-search_result_open)|搜索与详情|`search_result_open`|App|P0|IPK_ANL_03 → K01|
|4.4.4|[EVT_08](#444-evt_08-entity_detail_view)|搜索与详情|`entity_detail_view`|App|P0|IPK_ANL_03 → K01|
|4.5.1|[EVT_09](#451-evt_09-business_view_recorded)|浏览与收藏|`business_view_recorded`|服务端|P0|IPK_ANL_K01|
|4.5.2|[EVT_10](#452-evt_10-favorite_changed)|浏览与收藏|`favorite_changed`|服务端|P0|IPK_ANL_K01|
|4.6.1|[EVT_11](#461-evt_11-contact_supplier_click)|询盘|`contact_supplier_click`|App|P0|IPK_ANL_03 → K01|
|4.6.2|[EVT_12](#462-evt_12-inquiry_submit)|询盘|`inquiry_submit`|App|P0|IPK_ANL_03 → K01|
|4.6.3|[EVT_13](#463-evt_13-inquiry_submit_failed)|询盘|`inquiry_submit_failed`|App|P0|IPK_ANL_03 → K01|
|4.6.4|[EVT_14](#464-evt_14-inquiry_created)|询盘|`inquiry_created`|服务端|P0|IPK_ANL_K01|
|4.7.1|[EVT_15](#471-evt_15-chat_session_open)|消息与通知|`chat_session_open`|App|P0|IPK_ANL_03 → K01|
|4.7.2|[EVT_16](#472-evt_16-message_send_succeeded)|消息与通知|`message_send_succeeded`|服务端|P0|IPK_ANL_K01|
|4.7.3|[EVT_17](#473-evt_17-notification_open)|消息与通知|`notification_open`|App|P1|IPK_ANL_03 → K01|
|4.7.4|[EVT_18](#474-evt_18-message_translation_toggle)|消息与通知|`message_translation_toggle`|App|P1|IPK_ANL_03 → K01|
|4.8.1|[EVT_19](#481-evt_19-translate_started)|翻译|`translate_started`|App|P0|IPK_ANL_03 → K01|
|4.8.2|[EVT_20](#482-evt_20-translate_completed)|翻译|`translate_completed`|App|P0|IPK_ANL_03 → K01|
|4.8.3|[EVT_21](#483-evt_21-translate_sync_completed)|翻译|`translate_sync_completed`|服务端|P0|IPK_ANL_K01|
|4.8.4|[EVT_22](#484-evt_22-translate_copy)|翻译|`translate_copy`|App|P1|IPK_ANL_03 → K01|
|4.9.1|[EVT_23](#491-evt_23-account_registered)|账号与语料|`account_registered`|服务端|P1|IPK_ANL_K01|
|4.9.2|[EVT_24](#492-evt_24-corpus_asset_status_changed)|账号与语料|`corpus_asset_status_changed`|服务端|P1|IPK_ANL_K01|

## 4.3 会话与页面

### 4.3.1 EVT_01 app_session_start

|项目|定义|
|---|---|
|功能模块|会话与页面|
|生产方与优先级|App · P0|
|页面或服务|App.tsx / AppState 与导航初始化|
|业务接口|端侧生命周期|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|新 sessionId 建立后首次进入前台。冷启动、后台超时后恢复、身份切换均可建立新会话。|
|去重与关联|sessionId 一次；离线补发沿用 eventId。|
|统计用途|会话数、启动入口|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `app_session_start`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.launchSource`|string|是|coldStart / foreground / deepLink / push / accountSwitch / unknown；取本次会话的建立原因|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f01",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f01",
      "eventName": "app_session_start",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 1,
      "properties": {
        "launchSource": "coldStart"
      }
    }
  ]
}
```

### 4.3.2 EVT_02 app_background

|项目|定义|
|---|---|
|功能模块|会话与页面|
|生产方与优先级|App · P0|
|页面或服务|App.tsx / AppState|
|业务接口|端侧生命周期|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|从 active 切到非 active 时记录，随后连续 inactive/background 回调合并。|
|去重与关联|每次真实状态边沿一次；同一会话允许多次。|
|统计用途|前后台诊断|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `app_background`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|固定空对象 {}|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|白名单叶子页；示例 HomeMain|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f02",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f02",
      "eventName": "app_background",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 2,
      "screenName": "HomeMain",
      "properties": {}
    }
  ]
}
```

### 4.3.3 EVT_03 screen_view

|项目|定义|
|---|---|
|功能模块|会话与页面|
|生产方与优先级|App · P0|
|页面或服务|App.tsx / NavigationContainer 叶子路由|
|业务接口|端侧导航|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|白名单叶子页面首次获得焦点且 App 在前台。首屏由 onReady 触发。|
|去重与关联|每个 pageViewId 一次；短时后台返回同一访问只续时。|
|统计用途|页面 PV、UV、路径|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `screen_view`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|是|白名单叶子页；示例 FactoryDetail|
|`events[].pageViewId`|UUID|是|对应页面访问的稳定 ID|
|`events[].properties.entrySource`|string|是|tab / home / search / detail / message / push / deepLink / unknown|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f03",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f03",
      "eventName": "screen_view",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 3,
      "screenName": "FactoryDetail",
      "pageViewId": "3030e190-a030-4bba-9cec-057d5f419fc2",
      "properties": {
        "entrySource": "search"
      }
    }
  ]
}
```

### 4.3.4 EVT_04 screen_exit

|项目|定义|
|---|---|
|功能模块|会话与页面|
|生产方与优先级|App · P0|
|页面或服务|App.tsx / 页面可见性计时|
|业务接口|端侧导航|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|访问结束时结算所有有效前台片段。进程异常退出时缺失闭合可保留。|
|去重与关联|每个 pageViewId 首个有效结算；与 screen_view 共享 pageViewId。|
|统计用途|页面停留、闭合率|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `screen_exit`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|是|白名单叶子页；示例 FactoryDetail|
|`events[].pageViewId`|UUID|是|对应页面访问的稳定 ID|
|`events[].properties.visibleMs`|int|是|0–28,800,000 毫秒，使用单调时钟累计前台可见时长|
|`events[].properties.exitReason`|string|是|navigate / sessionTimeout / accountSwitch / appClose|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f04",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f04",
      "eventName": "screen_exit",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 4,
      "screenName": "FactoryDetail",
      "pageViewId": "3030e190-a030-4bba-9cec-057d5f419fc2",
      "properties": {
        "visibleMs": 35000,
        "exitReason": "navigate"
      }
    }
  ]
}
```

## 4.4 搜索与详情

### 4.4.1 EVT_05 search_submit

|项目|定义|
|---|---|
|功能模块|搜索与详情|
|生产方与优先级|App · P0|
|页面或服务|GlobalSearchScreen / 搜索请求发起处|
|业务接口|GET /search|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|实际发起一次新的搜索请求后上报。翻页和同次请求内部重试沿用 searchId。|
|去重与关联|一个 searchId 一次；同一关键词再次提交可生成新 ID。|
|统计用途|搜索请求量|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `search_submit`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.searchId`|UUID|是|每次新逻辑搜索由 App 生成|
|`events[].properties.scope`|string|是|all / company / product / user|
|`events[].properties.queryLengthBucket`|string|是|0 / 1-3 / 4-10 / 11-30 / 31+；端侧按去除首尾空白后的字符数分桶，不上传搜索词|
|`events[].properties.filterCount`|int|是|0–50，已生效的筛选项数，不上传筛选原文|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f05",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f05",
      "eventName": "search_submit",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 5,
      "properties": {
        "searchId": "3030e190-a030-4bba-9cec-057d5f419fa1",
        "scope": "all",
        "queryLengthBucket": "4-10",
        "filterCount": 2
      }
    }
  ]
}
```

### 4.4.2 EVT_06 search_result

|项目|定义|
|---|---|
|功能模块|搜索与详情|
|生产方与优先级|App · P0|
|页面或服务|GlobalSearchScreen / 首次结果处理|
|业务接口|GET /search|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|同一搜索的首屏有效响应或最终失败时触发。成功返回零条时照常上报。|
|去重与关联|同一 searchId 取首个有效首屏终态；分页结果不产生该事件。|
|统计用途|零结果率、请求失败率、耗时|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `search_result`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.searchId`|UUID|是|沿用搜索提交 ID|
|`events[].properties.scope`|string|是|all / company / product / user，与提交一致|
|`events[].properties.outcome`|string|是|success / failed|
|`events[].properties.resultCount`|int|成功时必填|0–1,000,000；当前搜索范围的首屏结果条数，失败时省略|
|`events[].properties.latencyMs`|int|是|请求发出到首屏处理结束的毫秒数，0–86,400,000|
|`events[].properties.errorCode`|string|失败时必填|固定错误字典，成功时省略|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f06",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f06",
      "eventName": "search_result",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 6,
      "properties": {
        "searchId": "3030e190-a030-4bba-9cec-057d5f419fa1",
        "scope": "all",
        "outcome": "success",
        "resultCount": 8,
        "latencyMs": 430
      }
    }
  ]
}
```

失败时将 `outcome` 置为 `failed`，添加 `properties.errorCode` 并省略 `properties.resultCount`。成功返回零条时填写 `resultCount: 0`。

### 4.4.3 EVT_07 search_result_open

|项目|定义|
|---|---|
|功能模块|搜索与详情|
|生产方与优先级|App · P0|
|页面或服务|GlobalSearchScreen / 结果卡片点击回调|
|业务接口|GET /search 返回结果；后续进入对应详情|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|点击有效结果并触发打开时上报。|
|去重与关联|每次实际打开使用独立 eventId，漏斗按 searchId 和目标匹配。|
|统计用途|结果打开率、位置分析|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `search_result_open`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.searchId`|UUID|是|结果所属搜索 ID|
|`events[].properties.targetType`|string|是|company / product / user|
|`events[].properties.targetId`|string|是|结果实体业务 ID；user ID 由网关假名化后进入 Kafka|
|`events[].properties.position`|int|是|卡片序号，从 1 开始，计入分页偏移|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f07",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f07",
      "eventName": "search_result_open",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 7,
      "properties": {
        "searchId": "3030e190-a030-4bba-9cec-057d5f419fa1",
        "targetType": "company",
        "targetId": "company_demo",
        "position": 3
      }
    }
  ]
}
```

### 4.4.4 EVT_08 entity_detail_view

|项目|定义|
|---|---|
|功能模块|搜索与详情|
|生产方与优先级|App · P0|
|页面或服务|BizDetailScreen、ProductDetailScreen / 对象加载后可见|
|业务接口|`GET /companies/{companyId}`、`GET /products/{productId}`；各自响应的实体 ID 映射到事件字段。|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|详情业务数据加载完成且页面可见。|
|去重与关联|同一 pageViewId + targetType + targetId 一次。|
|统计用途|详情曝光、询盘和搜索归因|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `entity_detail_view`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|是|白名单叶子页；示例 FactoryDetail|
|`events[].pageViewId`|UUID|是|对应页面访问的稳定 ID|
|`events[].properties.targetType`|string|是|company / product|
|`events[].properties.targetId`|string|是|企业用 companyId，产品用 productId|
|`events[].properties.companyId`|string|是|当前企业或商品归属企业 ID|
|`events[].properties.productId`|string|产品时必填|当前产品 ID，企业详情省略|
|`events[].properties.searchId`|UUID|否|从搜索结果进入且有有效链路时传|
|`events[].properties.entrySource`|string|是|tab / home / search / detail / message / push / deepLink / unknown|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f08",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f08",
      "eventName": "entity_detail_view",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 8,
      "screenName": "FactoryDetail",
      "pageViewId": "3030e190-a030-4bba-9cec-057d5f419fc2",
      "properties": {
        "targetType": "company",
        "targetId": "company_demo",
        "companyId": "company_demo",
        "searchId": "3030e190-a030-4bba-9cec-057d5f419fa1",
        "entrySource": "search"
      }
    }
  ]
}
```

## 4.5 浏览与收藏

### 4.5.1 EVT_09 business_view_recorded

|项目|定义|
|---|---|
|功能模块|浏览与收藏|
|生产方与优先级|服务端 · P0|
|页面或服务|业务浏览服务 / MongoDB company_view_raw|
|业务接口|POST /views；IPK_ANL_B02|
|分析接入|`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|
|触发条件|业务浏览明细成功持久化。|
|去重与关联|以持久浏览记录的 viewRef 去重；用户再次有效浏览使用新操作键。|
|统计用途|业务浏览 PV 及对账|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`eventName`|string|是|固定 `business_view_recorded`|
|`eventId`|UUID|是|由本条业务事实键稳定派生|
|`schemaVersion`|int|是|本条契约版本固定为 1|
|`occurredAt`|RFC3339|是|业务事实提交时刻，UTC|
|`properties`|object|是|仅接受本表列出的专用字段|
|`receivedAt`|RFC3339|是|首次进入分析链路的 UTC 时间|
|`businessDate`|date|是|按业务时区规范化的事件日期|
|`source`|string|是|固定 server|
|`producer`|string|是|受信任的服务身份，示例 analytics-view-adapter|
|`identityType`|string|是|取可信主体类型，authenticated / anonymous / system|
|`actorKey`|string / null|是|服务端生成的主体假名；无主体的系统事实可填 null|
|`aggregateRef`|string|是|用于 Kafka 分区的安全引用，示例 view_demo|
|`payloadHash`|string|是|规范化语义内容的 SHA-256 十六进制摘要；示例为占位值|
|`anonKey`|string / null|否|可信匿名主体假名，适用时补充|
|`sessionId、pageViewId、screenName、sequence`|UUID / UUID / string / int|否|仅有可信链路上下文时补充|
|`platform、appVersion、appLanguage、regionId`|string / string / string / int|否|有经验证的业务快照时补充|
|`policyVersion、requestId、ingestBatchId`|int / string / UUID|否|有对应受理元数据时补充；服务端通常省略 ingestBatchId|
|`properties.viewRef`|string|是|业务浏览记录稳定引用|
|`properties.targetType`|string|是|company / product，按业务类别映射|
|`properties.targetId`|string|是|浏览主体 ID|

**Kafka 消息 value**

```json
{
  "eventId": "3030e190-a030-4bba-9cec-057d5f419f09",
  "eventName": "business_view_recorded",
  "schemaVersion": 1,
  "occurredAt": "2026-11-13T04:00:00Z",
  "receivedAt": "2026-11-13T04:00:01Z",
  "businessDate": "2026-11-13",
  "source": "server",
  "producer": "analytics-view-adapter",
  "identityType": "authenticated",
  "actorKey": "u_demo",
  "aggregateRef": "view_demo",
  "payloadHash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties": {
    "viewRef": "view_demo",
    "targetType": "company",
    "targetId": "company_demo"
  }
}
```

### 4.5.2 EVT_10 favorite_changed

|项目|定义|
|---|---|
|功能模块|浏览与收藏|
|生产方与优先级|服务端 · P0|
|页面或服务|收藏服务 / user_actions 与业务出站|
|业务接口|POST /favorites；POST /favorites/cancel；IPK_ANL_B05|
|分析接入|`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|
|触发条件|关系真实新增或取消并成功提交。|
|去重与关联|关系 ID + 状态版本；重复幂等成功不再产生事件。|
|统计用途|收藏、取消次数及人数|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`eventName`|string|是|固定 `favorite_changed`|
|`eventId`|UUID|是|由本条业务事实键稳定派生|
|`schemaVersion`|int|是|本条契约版本固定为 1|
|`occurredAt`|RFC3339|是|业务事实提交时刻，UTC|
|`properties`|object|是|仅接受本表列出的专用字段|
|`receivedAt`|RFC3339|是|首次进入分析链路的 UTC 时间|
|`businessDate`|date|是|按业务时区规范化的事件日期|
|`source`|string|是|固定 server|
|`producer`|string|是|受信任的服务身份，示例 analytics-favorite-adapter|
|`identityType`|string|是|取可信主体类型，authenticated / anonymous / system|
|`actorKey`|string / null|是|服务端生成的主体假名；无主体的系统事实可填 null|
|`aggregateRef`|string|是|用于 Kafka 分区的安全引用，示例 favorite_relation_demo|
|`payloadHash`|string|是|规范化语义内容的 SHA-256 十六进制摘要；示例为占位值|
|`anonKey`|string / null|否|可信匿名主体假名，适用时补充|
|`sessionId、pageViewId、screenName、sequence`|UUID / UUID / string / int|否|仅有可信链路上下文时补充|
|`platform、appVersion、appLanguage、regionId`|string / string / string / int|否|有经验证的业务快照时补充|
|`policyVersion、requestId、ingestBatchId`|int / string / UUID|否|有对应受理元数据时补充；服务端通常省略 ingestBatchId|
|`properties.targetType`|string|是|company / product；取业务接口的 targetType|
|`properties.targetId`|string|是|被收藏或取消的实体 ID|
|`properties.action`|string|是|add / remove|

**Kafka 消息 value**

```json
{
  "eventId": "3030e190-a030-4bba-9cec-057d5f419f0a",
  "eventName": "favorite_changed",
  "schemaVersion": 1,
  "occurredAt": "2026-11-13T04:00:00Z",
  "receivedAt": "2026-11-13T04:00:01Z",
  "businessDate": "2026-11-13",
  "source": "server",
  "producer": "analytics-favorite-adapter",
  "identityType": "authenticated",
  "actorKey": "u_demo",
  "aggregateRef": "favorite_relation_demo",
  "payloadHash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties": {
    "targetType": "product",
    "targetId": "product_demo",
    "action": "add"
  }
}
```

## 4.6 询盘

### 4.6.1 EVT_11 contact_supplier_click

|项目|定义|
|---|---|
|功能模块|询盘|
|生产方与优先级|App · P0|
|页面或服务|FactoryDetailScreen、SupplierDetailScreen、ShopDetailScreen / 询盘入口|
|业务接口|页面跳转 InquiryEdit，无独立写接口|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|点击联系供应商并创建表单关联链路。|
|去重与关联|每次独立联系动作生成新的 contactFlowId；同一动作重试沿用 eventId。|
|统计用途|询盘漏斗联系步骤|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `contact_supplier_click`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|是|白名单叶子页；示例 FactoryDetail|
|`events[].pageViewId`|UUID|是|对应页面访问的稳定 ID|
|`events[].properties.contactFlowId`|UUID|是|点击时生成，并传给询盘表单|
|`events[].properties.companyId`|string|是|被联系企业 ID|
|`events[].properties.productId`|string|否|从商品相关入口进入且已选商品时传|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f0b",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f0b",
      "eventName": "contact_supplier_click",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 11,
      "screenName": "FactoryDetail",
      "pageViewId": "3030e190-a030-4bba-9cec-057d5f419fc2",
      "properties": {
        "contactFlowId": "3030e190-a030-4bba-9cec-057d5f419fa2",
        "companyId": "company_demo",
        "productId": "product_demo"
      }
    }
  ]
}
```

### 4.6.2 EVT_12 inquiry_submit

|项目|定义|
|---|---|
|功能模块|询盘|
|生产方与优先级|App · P0|
|页面或服务|InquiryEditScreen / inquiryApi.create 调用点|
|业务接口|POST /inquiries；IPK_ANL_B01|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|表单通过校验并实际发出请求。|
|去重与关联|clientRequestId 表示一次逻辑提交；网络重试沿用。|
|统计用途|有效提交、表单转化|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `inquiry_submit`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.contactFlowId`|UUID|否|沿用联系入口，未建立归因链路时省略|
|`events[].properties.clientRequestId`|string|是|稳定的业务幂等键，和业务请求一致|
|`events[].properties.companyId`|string|是|询盘目标企业，取业务 targetCompanyId|
|`events[].properties.productId`|string|否|业务请求有产品 ID 时传|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f0c",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f0c",
      "eventName": "inquiry_submit",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 12,
      "properties": {
        "contactFlowId": "3030e190-a030-4bba-9cec-057d5f419fa2",
        "clientRequestId": "req_demo",
        "companyId": "company_demo",
        "productId": "product_demo"
      }
    }
  ]
}
```

询盘的数量、价格、交货地、备注、附件和联系人信息由原业务接口处理，不进入此事件。

### 4.6.3 EVT_13 inquiry_submit_failed

|项目|定义|
|---|---|
|功能模块|询盘|
|生产方与优先级|App · P0|
|页面或服务|InquiryEditScreen / 请求失败分支|
|业务接口|POST /inquiries；IPK_ANL_B01|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|用户实际看到一次请求失败时触发，未发出请求的表单提示不进入该事件。|
|去重与关联|clientRequestId + attemptNo；后续网络尝试递增。|
|统计用途|询盘失败原因|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `inquiry_submit_failed`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.clientRequestId`|string|是|沿用提交幂等键|
|`events[].properties.attemptNo`|int|是|从 1 开始的实际网络尝试序号|
|`events[].properties.errorCode`|string|是|固定错误字典，禁止错误原文|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f0d",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f0d",
      "eventName": "inquiry_submit_failed",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 13,
      "properties": {
        "clientRequestId": "req_demo",
        "attemptNo": 1,
        "errorCode": "networkTimeout"
      }
    }
  ]
}
```

### 4.6.4 EVT_14 inquiry_created

|项目|定义|
|---|---|
|功能模块|询盘|
|生产方与优先级|服务端 · P0|
|页面或服务|询盘服务 / PostgreSQL inquiries、询盘事实适配器|
|业务接口|POST /inquiries；inquiries.created（K02）；IPK_ANL_B01|
|分析接入|`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|
|触发条件|询盘在 PG 最终提交后由适配器投影。|
|去重与关联|按权威 inquiryId 派生 eventId；无归因仍计业务总量。|
|统计用途|询盘成功数、F01 归因|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`eventName`|string|是|固定 `inquiry_created`|
|`eventId`|UUID|是|由本条业务事实键稳定派生|
|`schemaVersion`|int|是|本条契约版本固定为 1|
|`occurredAt`|RFC3339|是|业务事实提交时刻，UTC|
|`properties`|object|是|仅接受本表列出的专用字段|
|`receivedAt`|RFC3339|是|首次进入分析链路的 UTC 时间|
|`businessDate`|date|是|按业务时区规范化的事件日期|
|`source`|string|是|固定 server|
|`producer`|string|是|受信任的服务身份，示例 analytics-inquiry-adapter|
|`identityType`|string|是|取可信主体类型，authenticated / anonymous / system|
|`actorKey`|string / null|是|服务端生成的主体假名；无主体的系统事实可填 null|
|`aggregateRef`|string|是|用于 Kafka 分区的安全引用，示例 inquiry_demo|
|`payloadHash`|string|是|规范化语义内容的 SHA-256 十六进制摘要；示例为占位值|
|`anonKey`|string / null|否|可信匿名主体假名，适用时补充|
|`sessionId、pageViewId、screenName、sequence`|UUID / UUID / string / int|否|仅有可信链路上下文时补充|
|`platform、appVersion、appLanguage、regionId`|string / string / string / int|否|有经验证的业务快照时补充|
|`policyVersion、requestId、ingestBatchId`|int / string / UUID|否|有对应受理元数据时补充；服务端通常省略 ingestBatchId|
|`properties.inquiryRef`|string|是|已提交询盘稳定引用|
|`properties.companyId`|string|是|PG 询盘目标企业 ID|
|`properties.productId`|string|否|PG 询盘有关联产品时传|
|`properties.contactFlowId`|UUID|否|仅从已验证的业务关联读取|
|`properties.clientRequestId`|string|否|已有业务请求幂等键时补充|

**Kafka 消息 value**

```json
{
  "eventId": "3030e190-a030-4bba-9cec-057d5f419f0e",
  "eventName": "inquiry_created",
  "schemaVersion": 1,
  "occurredAt": "2026-11-13T04:00:00Z",
  "receivedAt": "2026-11-13T04:00:01Z",
  "businessDate": "2026-11-13",
  "source": "server",
  "producer": "analytics-inquiry-adapter",
  "identityType": "authenticated",
  "actorKey": "u_demo",
  "aggregateRef": "inquiry_demo",
  "payloadHash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties": {
    "inquiryRef": "inquiry_demo",
    "companyId": "company_demo",
    "productId": "product_demo",
    "contactFlowId": "3030e190-a030-4bba-9cec-057d5f419fa2",
    "clientRequestId": "req_demo"
  }
}
```

## 4.7 消息与通知

### 4.7.1 EVT_15 chat_session_open

|项目|定义|
|---|---|
|功能模块|消息与通知|
|生产方与优先级|App · P0|
|页面或服务|ChatScreen / 真实会话页面可见|
|业务接口|GET /chat/dialogs/{id}/messages 是相关读取接口；进入页触发分析|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|真实会话页面获得焦点并可见，排除示例会话。|
|去重与关联|同次可见访问一次；重新进入生成新 eventId。|
|统计用途|对话入口、会话打开次数|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `chat_session_open`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|白名单叶子页；示例 Chat|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.dialogRef`|string|是|App 传真实业务会话 ID，Collector 映射为 Kafka 安全假名|
|`events[].properties.entrySource`|string|是|tab / home / search / detail / message / push / deepLink / unknown|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f0f",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f0f",
      "eventName": "chat_session_open",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 15,
      "screenName": "Chat",
      "properties": {
        "dialogRef": "dialog_demo",
        "entrySource": "message"
      }
    }
  ]
}
```

### 4.7.2 EVT_16 message_send_succeeded

|项目|定义|
|---|---|
|功能模块|消息与通知|
|生产方与优先级|服务端 · P0|
|页面或服务|消息服务 / MongoDB messages|
|业务接口|POST /chat/dialogs/{id}/messages；IPK_ANL_B03|
|分析接入|`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|
|触发条件|消息首次持久化后由业务出站发布。|
|去重与关联|按业务 messageId 派生 eventId；补发不新增事实。|
|统计用途|发送量、24 小时人工回复|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`eventName`|string|是|固定 `message_send_succeeded`|
|`eventId`|UUID|是|由本条业务事实键稳定派生|
|`schemaVersion`|int|是|本条契约版本固定为 1|
|`occurredAt`|RFC3339|是|业务事实提交时刻，UTC|
|`properties`|object|是|仅接受本表列出的专用字段|
|`receivedAt`|RFC3339|是|首次进入分析链路的 UTC 时间|
|`businessDate`|date|是|按业务时区规范化的事件日期|
|`source`|string|是|固定 server|
|`producer`|string|是|受信任的服务身份，示例 analytics-message-adapter|
|`identityType`|string|是|取可信主体类型，authenticated / anonymous / system|
|`actorKey`|string / null|是|服务端生成的主体假名；无主体的系统事实可填 null|
|`aggregateRef`|string|是|用于 Kafka 分区的安全引用，示例 msg_ref_demo|
|`payloadHash`|string|是|规范化语义内容的 SHA-256 十六进制摘要；示例为占位值|
|`anonKey`|string / null|否|可信匿名主体假名，适用时补充|
|`sessionId、pageViewId、screenName、sequence`|UUID / UUID / string / int|否|仅有可信链路上下文时补充|
|`platform、appVersion、appLanguage、regionId`|string / string / string / int|否|有经验证的业务快照时补充|
|`policyVersion、requestId、ingestBatchId`|int / string / UUID|否|有对应受理元数据时补充；服务端通常省略 ingestBatchId|
|`properties.dialogRef`|string|是|会话安全引用，与 chat_session_open 入仓引用一致|
|`properties.messageRef`|string|是|已持久消息的安全引用|
|`properties.messageType`|string|是|text / image / file / audio / card / other|
|`properties.senderKind`|string|是|human / system / inquiryCard；回复指标仅使用 human|

**Kafka 消息 value**

```json
{
  "eventId": "3030e190-a030-4bba-9cec-057d5f419f10",
  "eventName": "message_send_succeeded",
  "schemaVersion": 1,
  "occurredAt": "2026-11-13T04:00:00Z",
  "receivedAt": "2026-11-13T04:00:01Z",
  "businessDate": "2026-11-13",
  "source": "server",
  "producer": "analytics-message-adapter",
  "identityType": "authenticated",
  "actorKey": "u_demo",
  "aggregateRef": "msg_ref_demo",
  "payloadHash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties": {
    "dialogRef": "dialog_ref_demo",
    "messageRef": "msg_ref_demo",
    "messageType": "text",
    "senderKind": "human"
  }
}
```

### 4.7.3 EVT_17 notification_open

|项目|定义|
|---|---|
|功能模块|消息与通知|
|生产方与优先级|App · P1|
|页面或服务|站内通知或系统推送点击路由|
|业务接口|GET /notifications 为列表读取；有已读动作时沿用 POST /notifications/read|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|用户实际点击并开始打开通知内容，收到推送本身不触发。|
|去重与关联|同次点击一次，站内跳转和已读回调不重复发送。|
|统计用途|通知召回|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `notification_open`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.category`|string|是|comments / likes / follows / inquiries / system|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f11",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f11",
      "eventName": "notification_open",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 17,
      "properties": {
        "category": "inquiries"
      }
    }
  ]
}
```

### 4.7.4 EVT_18 message_translation_toggle

|项目|定义|
|---|---|
|功能模块|消息与通知|
|生产方与优先级|App · P1|
|页面或服务|ChatScreen / 译文展示控件|
|业务接口|端侧显示；如需同步译文仍走既有 POST /chat/messages/{id}/translations|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|原文与译文显示状态实际切换时触发。|
|去重与关联|每次有效状态切换一次；渲染、重复点击同状态不触发。|
|统计用途|聊天跨语功能使用|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `message_translation_toggle`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.displayMode`|string|是|original / translated，记录切换后的状态|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f12",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f12",
      "eventName": "message_translation_toggle",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 18,
      "properties": {
        "displayMode": "translated"
      }
    }
  ]
}
```

## 4.8 翻译

### 4.8.1 EVT_19 translate_started

|项目|定义|
|---|---|
|功能模块|翻译|
|生产方与优先级|App · P0|
|页面或服务|TextTranslateScreen、DialogTranslateScreen、CameraTranslateScreen、DocTranslateScreen / 实际任务开始|
|业务接口|端侧翻译执行；记录同步独立|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|文本、对话、图片或文件任务真正开始执行后触发。|
|去重与关联|每次执行生成 taskId；用户主动重试是新任务，上报重试保留原 ID。|
|统计用途|翻译任务需求|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `translate_started`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.taskId`|UUID|是|本次执行任务 ID|
|`events[].properties.mode`|string|是|text / dialog / image / file|
|`events[].properties.sourceLang`|string|是|业务字典中的源语言，未知 und|
|`events[].properties.targetLang`|string|是|业务字典中的目标语言，未知 und|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f13",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f13",
      "eventName": "translate_started",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 19,
      "properties": {
        "taskId": "3030e190-a030-4bba-9cec-057d5f419fa3",
        "mode": "text",
        "sourceLang": "zh",
        "targetLang": "uz"
      }
    }
  ]
}
```

### 4.8.2 EVT_20 translate_completed

|项目|定义|
|---|---|
|功能模块|翻译|
|生产方与优先级|App · P0|
|页面或服务|各翻译模式的执行完成回调|
|业务接口|端侧翻译执行；记录同步独立|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|开始过的任务进入成功、失败或取消终态时触发。|
|去重与关联|同一 taskId 的首个有效终态；与 translate_started 关联。|
|统计用途|成功率、失败率、耗时|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `translate_completed`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.taskId`|UUID|是|沿用任务开始 ID|
|`events[].properties.mode`|string|是|text / dialog / image / file，与开始一致|
|`events[].properties.sourceLang`|string|是|任务的源语言；自动识别可填识别结果|
|`events[].properties.targetLang`|string|是|任务的目标语言|
|`events[].properties.outcome`|string|是|success / failed / cancelled|
|`events[].properties.latencyMs`|int|是|任务运行时长，0–86,400,000 毫秒|
|`events[].properties.errorCode`|string|失败时必填|固定错误字典，成功或取消时省略|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f14",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f14",
      "eventName": "translate_completed",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 20,
      "properties": {
        "taskId": "3030e190-a030-4bba-9cec-057d5f419fa3",
        "mode": "text",
        "sourceLang": "zh",
        "targetLang": "uz",
        "outcome": "failed",
        "latencyMs": 920,
        "errorCode": "engineFailed"
      }
    }
  ]
}
```

成功或取消时分别填写 `success` 或 `cancelled`，省略 `properties.errorCode`。

### 4.8.3 EVT_21 translate_sync_completed

|项目|定义|
|---|---|
|功能模块|翻译|
|生产方与优先级|服务端 · P0|
|页面或服务|翻译记录服务 / MongoDB 同步状态|
|业务接口|POST /translate/records、/translate/dialog-sessions、/translate/image-records、/translate/file-jobs；IPK_ANL_B04|
|分析接入|`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|
|触发条件|同步首次成功持久化，或形成可持久追踪的失败状态时触发。|
|去重与关联|记录 ID + 状态版本；同状态幂等同步不重复。|
|统计用途|记录同步质量|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`eventName`|string|是|固定 `translate_sync_completed`|
|`eventId`|UUID|是|由本条业务事实键稳定派生|
|`schemaVersion`|int|是|本条契约版本固定为 1|
|`occurredAt`|RFC3339|是|业务事实提交时刻，UTC|
|`properties`|object|是|仅接受本表列出的专用字段|
|`receivedAt`|RFC3339|是|首次进入分析链路的 UTC 时间|
|`businessDate`|date|是|按业务时区规范化的事件日期|
|`source`|string|是|固定 server|
|`producer`|string|是|受信任的服务身份，示例 analytics-translation-adapter|
|`identityType`|string|是|取可信主体类型，authenticated / anonymous / system|
|`actorKey`|string / null|是|服务端生成的主体假名；无主体的系统事实可填 null|
|`aggregateRef`|string|是|用于 Kafka 分区的安全引用，示例 rec_demo|
|`payloadHash`|string|是|规范化语义内容的 SHA-256 十六进制摘要；示例为占位值|
|`anonKey`|string / null|否|可信匿名主体假名，适用时补充|
|`sessionId、pageViewId、screenName、sequence`|UUID / UUID / string / int|否|仅有可信链路上下文时补充|
|`platform、appVersion、appLanguage、regionId`|string / string / string / int|否|有经验证的业务快照时补充|
|`policyVersion、requestId、ingestBatchId`|int / string / UUID|否|有对应受理元数据时补充；服务端通常省略 ingestBatchId|
|`properties.recordRef`|string|是|同步记录的稳定安全引用|
|`properties.mode`|string|是|text / dialog / image / file，对应业务记录类型|
|`properties.outcome`|string|是|success / failed；表达同步结果|

**Kafka 消息 value**

```json
{
  "eventId": "3030e190-a030-4bba-9cec-057d5f419f15",
  "eventName": "translate_sync_completed",
  "schemaVersion": 1,
  "occurredAt": "2026-11-13T04:00:00Z",
  "receivedAt": "2026-11-13T04:00:01Z",
  "businessDate": "2026-11-13",
  "source": "server",
  "producer": "analytics-translation-adapter",
  "identityType": "authenticated",
  "actorKey": "u_demo",
  "aggregateRef": "rec_demo",
  "payloadHash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties": {
    "recordRef": "rec_demo",
    "mode": "text",
    "outcome": "success"
  }
}
```

### 4.8.4 EVT_22 translate_copy

|项目|定义|
|---|---|
|功能模块|翻译|
|生产方与优先级|App · P1|
|页面或服务|支持复制译文的翻译结果页|
|业务接口|端侧剪贴板操作|
|分析接入|`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|
|触发条件|成功将译文交给剪贴板后触发。|
|去重与关联|每次成功复制一次，复制失败不触发。|
|统计用途|译文使用深度|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`batchId`|UUID|是|当前批次 ID；整批重试沿用|
|`installId`|UUID|是|安装随机标识|
|`identityEpoch`|UUID|是|当前身份阶段|
|`identityType`|string|是|anonymous / authenticated，与认证方式一致|
|`platform`|string|是|ios / android|
|`appVersion`|string|是|运行版本，1–40 字符|
|`appLanguage`|string|否|zh / en / ru / uz|
|`regionId`|int|否|现有业务地区字典 ID|
|`policyVersion`|int|是|生效采集策略的正整数版本|
|`events`|object[]|是|1–100 个事件；本示例为单事件|
|`events[].eventName`|string|是|固定 `translate_copy`|
|`events[].eventId`|UUID|是|App 生成；补传沿用同一 ID|
|`events[].schemaVersion`|int|是|本条契约版本固定为 1|
|`events[].occurredAt`|RFC3339|是|行为实际发生时刻，UTC|
|`events[].properties`|object|是|仅接受本表列出的专用字段|
|`events[].sessionId`|UUID|是|当前 App 会话 ID|
|`events[].sequence`|int|是|1–2,147,483,647；在该会话内递增|
|`events[].screenName`|string|否|有可靠页面上下文时可传|
|`events[].pageViewId`|UUID|否|有可靠页面访问上下文时可传|
|`events[].properties.mode`|string|是|text / dialog / image / file|

**HTTP 请求体**

```json
{
  "batchId": "3030e190-a030-4bba-9cec-057d5f419f16",
  "installId": "3030e190-a030-4bba-9cec-057d5f419fd1",
  "identityEpoch": "3030e190-a030-4bba-9cec-057d5f419fd2",
  "identityType": "authenticated",
  "platform": "android",
  "appVersion": "1.2.0",
  "appLanguage": "zh",
  "policyVersion": 3,
  "events": [
    {
      "eventId": "3030e190-a030-4bba-9cec-057d5f419f16",
      "eventName": "translate_copy",
      "schemaVersion": 1,
      "occurredAt": "2026-11-13T04:00:00Z",
      "sessionId": "3030e190-a030-4bba-9cec-057d5f419fc1",
      "sequence": 22,
      "properties": {
        "mode": "text"
      }
    }
  ]
}
```

## 4.9 账号与语料

### 4.9.1 EVT_23 account_registered

|项目|定义|
|---|---|
|功能模块|账号与语料|
|生产方与优先级|服务端 · P1|
|页面或服务|认证服务 / PostgreSQL users 与业务出站|
|业务接口|POST /auth/register|
|分析接入|`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|
|触发条件|新账号注册事务首次成功提交。|
|去重与关联|按新建 userId 派生事件 ID；账户标识只以 actorKey 假名进入 Kafka。|
|统计用途|注册人数、渠道|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`eventName`|string|是|固定 `account_registered`|
|`eventId`|UUID|是|由本条业务事实键稳定派生|
|`schemaVersion`|int|是|本条契约版本固定为 1|
|`occurredAt`|RFC3339|是|业务事实提交时刻，UTC|
|`properties`|object|是|仅接受本表列出的专用字段|
|`receivedAt`|RFC3339|是|首次进入分析链路的 UTC 时间|
|`businessDate`|date|是|按业务时区规范化的事件日期|
|`source`|string|是|固定 server|
|`producer`|string|是|受信任的服务身份，示例 analytics-auth-adapter|
|`identityType`|string|是|取可信主体类型，authenticated / anonymous / system|
|`actorKey`|string / null|是|服务端生成的主体假名；无主体的系统事实可填 null|
|`aggregateRef`|string|是|用于 Kafka 分区的安全引用，示例 u_ref_demo|
|`payloadHash`|string|是|规范化语义内容的 SHA-256 十六进制摘要；示例为占位值|
|`anonKey`|string / null|否|可信匿名主体假名，适用时补充|
|`sessionId、pageViewId、screenName、sequence`|UUID / UUID / string / int|否|仅有可信链路上下文时补充|
|`platform、appVersion、appLanguage、regionId`|string / string / string / int|否|有经验证的业务快照时补充|
|`policyVersion、requestId、ingestBatchId`|int / string / UUID|否|有对应受理元数据时补充；服务端通常省略 ingestBatchId|
|`properties.channel`|string|是|phone / email，取实际注册渠道|

**Kafka 消息 value**

```json
{
  "eventId": "3030e190-a030-4bba-9cec-057d5f419f17",
  "eventName": "account_registered",
  "schemaVersion": 1,
  "occurredAt": "2026-11-13T04:00:00Z",
  "receivedAt": "2026-11-13T04:00:01Z",
  "businessDate": "2026-11-13",
  "source": "server",
  "producer": "analytics-auth-adapter",
  "identityType": "authenticated",
  "actorKey": "u_demo",
  "aggregateRef": "u_ref_demo",
  "payloadHash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties": {
    "channel": "phone"
  }
}
```

### 4.9.2 EVT_24 corpus_asset_status_changed

|项目|定义|
|---|---|
|功能模块|账号与语料|
|生产方与优先级|服务端 · P1|
|页面或服务|语料治理作业 / MongoDB corpus_items|
|业务接口|内部治理作业；IPK_ANL_18、19 为查询接口|
|分析接入|`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|
|触发条件|语料条目进入实际审核或移除状态，变更成功持久化。|
|去重与关联|条目 ID + 状态版本；同状态重复写入不产生事件。|
|统计用途|语料审核与状态覆盖|

|字段路径|类型|必填|取值与来源|
|---|---|---|---|
|`eventName`|string|是|固定 `corpus_asset_status_changed`|
|`eventId`|UUID|是|由本条业务事实键稳定派生|
|`schemaVersion`|int|是|本条契约版本固定为 1|
|`occurredAt`|RFC3339|是|业务事实提交时刻，UTC|
|`properties`|object|是|仅接受本表列出的专用字段|
|`receivedAt`|RFC3339|是|首次进入分析链路的 UTC 时间|
|`businessDate`|date|是|按业务时区规范化的事件日期|
|`source`|string|是|固定 server|
|`producer`|string|是|受信任的服务身份，示例 analytics-corpus-adapter|
|`identityType`|string|是|系统治理事实填 system|
|`actorKey`|string / null|是|服务端生成的主体假名；无主体的系统事实可填 null|
|`aggregateRef`|string|是|用于 Kafka 分区的安全引用，示例 corpus_item_ref_demo|
|`payloadHash`|string|是|规范化语义内容的 SHA-256 十六进制摘要；示例为占位值|
|`anonKey`|string / null|否|可信匿名主体假名，适用时补充|
|`sessionId、pageViewId、screenName、sequence`|UUID / UUID / string / int|否|仅有可信链路上下文时补充|
|`platform、appVersion、appLanguage、regionId`|string / string / string / int|否|有经验证的业务快照时补充|
|`policyVersion、requestId、ingestBatchId`|int / string / UUID|否|有对应受理元数据时补充；服务端通常省略 ingestBatchId|
|`properties.datasetCode`|string|是|已注册的数据集代码|
|`properties.languagePair`|string|是|源语言:目标语言，使用业务语言字典|
|`properties.status`|string|是|pending / approved / rejected / removed|
|`properties.reasonCode`|string|否|治理作业注册的固定原因码；禁止审核意见原文|

**Kafka 消息 value**

```json
{
  "eventId": "3030e190-a030-4bba-9cec-057d5f419f18",
  "eventName": "corpus_asset_status_changed",
  "schemaVersion": 1,
  "occurredAt": "2026-11-13T04:00:00Z",
  "receivedAt": "2026-11-13T04:00:01Z",
  "businessDate": "2026-11-13",
  "source": "server",
  "producer": "analytics-corpus-adapter",
  "identityType": "system",
  "actorKey": null,
  "aggregateRef": "corpus_item_ref_demo",
  "payloadHash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "properties": {
    "datasetCode": "approved_terms",
    "languagePair": "zh:uz",
    "status": "approved",
    "reasonCode": "reviewPassed"
  }
}
```

# 5 联调顺序

1. 先对接采集凭据、批次幂等、限流、混合回执及 Kafka ACK。
2. 用同一 inquiryId 贯通 B01、K02、K01、总览和 F01，核对未归因业务事实。
3. 用迟到、重放、过期快照验证 409、K04、WebSocket 和轮询恢复。
4. 用不同角色验证会话工单、语料数据集授权和 `.log` 审计。

