# ipaklink 埋点数据后台功能清单

版本 4.1 · 统一交付范围冻结版 · 2026-11-16

原 Must 与 Should 共 53 项，统一纳入本次交付。本清单用于开发、联调与验收，任务按功能编号关联接口、数据模型和验收记录。

依据：[本次 API 接口文档](./ipaklink-埋点数据后台API接口文档.md) · [数据字典](./ipaklink-埋点数据后台数据字典.md)

## 1. 冻结规则

功能范围以本清单为准。既有功能编号、事件名称和接口编号继续使用。新增技术契约先在 API 和数据字典中确认，再进入实现与联调。若变更功能范围或验收口径，需记录项目负责人决定并同步关联文档。

## 2. 功能全清单

共 53 项，按模块归类。主责栏由第一个角色牵头，ANL 指 IPK_ANL。

### 2.1 客户端采集

|功能编号|功能|主责|数据落点|接口或事件|业务价值|验收条件|
|---|---|---|---|---|---|---|
|SDK-01|配置读取与采集关停|端 / 采集|analytics_event_schema、端侧配置缓存|ANL_01|没有生效策略和关停能力，本次上线后无法控制采集范围|能按发布配置关停采集；App 在 TTL 内使用有效缓存；配置编辑与灰度发布见 ADM-01|
|SDK-02|匿名凭据及账号隔离|端 / 采集|installId、identityEpoch、analytics_ingest_receipt|ANL_02、03|身份边界错误会把旧队列归给新账号，活跃与漏斗失真|切换账号后旧队列不进入新账号；匿名补报按原 epoch|
|SDK-03|会话和前后台|端|端侧 SQLite 队列、events_canonical|app_session_start、app_background<br>[EVT_01](./ipaklink-埋点数据后台接口文档.md#431-evt_01-app_session_start)、[EVT_02](./ipaklink-埋点数据后台接口文档.md#432-evt_02-app_background)|会话 ID 和顺序支撑活跃、停留及会话级关联|冷启动、超时、切号新建会话；重复 AppState 回调不重复上报|
|SDK-04|页面进入与退出|端 / 数据|fact_page_visit|screen_view、screen_exit<br>[EVT_03](./ipaklink-埋点数据后台接口文档.md#433-evt_03-screen_view)、[EVT_04](./ipaklink-埋点数据后台接口文档.md#434-evt_04-screen_exit)|没有页面进入与结算，PV、UV 和停留无法计算|叶子路由正确；重复回调不加 PV；20s 前台加 10s 前台的结果为 30s|
|SDK-05|离线队列和批次回执|端 / 采集|独立 SQLite 采集队列|ANL_03|断网时没有缓冲会系统性丢失移动端事件|断网重启后可补发；按事件回执逐条删除或保留；同一 eventId 重试稳定|
|SDK-06|采集字段白名单|端 / 采集|analytics_event_schema|ANL_03|缺少字段白名单时无法保证采集口径与内容隔离|任意新增属性和非法枚举返回 rejected；正文、凭据和完整联系方式不入队|
|SDK-07|通知点击行为|端|events_canonical|notification_open<br>[EVT_17](./ipaklink-埋点数据后台接口文档.md#473-evt_17-notification_open)|用于通知归因，本次搜索和询盘链路可独立计算|从通知实际打开时计一次，按通知类别入仓校验|
|SDK-08|翻译使用深度|端|events_canonical|message_translation_toggle、translate_copy<br>[EVT_18](./ipaklink-埋点数据后台接口文档.md#474-evt_18-message_translation_toggle)、[EVT_22](./ipaklink-埋点数据后台接口文档.md#484-evt_22-translate_copy)|用于译文复用深度，本次执行与同步质量指标已有独立事件|实际切换译文或成功复制才上报|

### 2.2 业务采集

|功能编号|功能|主责|数据落点|接口或事件|业务价值|验收条件|
|---|---|---|---|---|---|---|
|BIZ-01|搜索全链路|端 / 数据|fact_search|GET /search、ANL_03；EVT_05–07|缺任一步都无法区分搜索失败、零结果和后续打开|通过 searchId 串起提交、首屏结果和打开；翻页不创建新搜索；纯用户搜索排除出企业/商品漏斗|
|BIZ-02|实体曝光与业务浏览|业务 / 端 / 数据|fact_page_visit、company_view_raw、Redis 业务计数|ANL_B02；entity_detail_view、business_view_recorded<br>[EVT_08](./ipaklink-埋点数据后台接口文档.md#444-evt_08-entity_detail_view)、[EVT_09](./ipaklink-埋点数据后台接口文档.md#451-evt_09-business_view_recorded)|业务 PV 和实体详情曝光分别是浏览对账和 F01 的必要输入|详情业务对象可见才发曝光；两个有效业务浏览计两个业务 PV，普通页面 PV 不叠加|
|BIZ-03|收藏与取消事实|业务|user_actions、analytics_outbox|ANL_B05、EVT_10|用于收藏行为分析，本次 F01/F02 均依赖现有独立事件|仅实际关系变更才投递；重复幂等调用不新增事实；记录实际启用日期和历史数据覆盖|
|BIZ-04|联系、提交及成功询盘|业务 / 端 / 数据|inquiries、analytics_outbox、fact_inquiry_flow|ANL_B01、K02；contact_supplier_click、inquiry_submit、inquiry_submit_failed、inquiry_created<br>[EVT_11](./ipaklink-埋点数据后台接口文档.md#461-evt_11-contact_supplier_click)、[EVT_12](./ipaklink-埋点数据后台接口文档.md#462-evt_12-inquiry_submit)、[EVT_13](./ipaklink-埋点数据后台接口文档.md#463-evt_13-inquiry_submit_failed)、[EVT_14](./ipaklink-埋点数据后台接口文档.md#464-evt_14-inquiry_created)|缺少 PG 成功事实或关联键无法校验询盘总量与转化|同一请求重试只产生一个成功事实；无归因键仍计业务总量|
|BIZ-05|会话打开与消息持久化|业务 / 端 / 数据|messages、analytics_mongo_outbox、fact_dialog_reply|ANL_B03；chat_session_open、message_send_succeeded<br>[EVT_15](./ipaklink-埋点数据后台接口文档.md#471-evt_15-chat_session_open)、[EVT_16](./ipaklink-埋点数据后台接口文档.md#472-evt_16-message_send_succeeded)|真实会话与持久化消息是消息量及回复统计的必要输入|示例会话排除；发送重试不重计；询盘卡片与人工普通消息区分|
|BIZ-06|翻译执行与记录同步|端 / 业务 / 数据|translate_records、fact_translate_task|ANL_B04；translate_started、translate_completed、translate_sync_completed<br>[EVT_19](./ipaklink-埋点数据后台接口文档.md#481-evt_19-translate_started)、[EVT_20](./ipaklink-埋点数据后台接口文档.md#482-evt_20-translate_completed)、[EVT_21](./ipaklink-埋点数据后台接口文档.md#483-evt_21-translate_sync_completed)|需要区分翻译执行和记录同步，给出正确的成功率和耗时|本地成功与同步失败可以同时成立，统计分别入账；文本、对话、图片、文件可分组|
|BIZ-07|账号注册事实|业务|users、analytics_outbox|account_registered<br>[EVT_23](./ipaklink-埋点数据后台接口文档.md#491-evt_23-account_registered)|用于注册渠道分析，本次留存由首次有效活跃定义|注册事务成功后按用户去重，手机或邮箱渠道字段可在仓库核验|

### 2.3 接入与传输

|功能编号|功能|主责|数据落点|接口或事件|业务价值|验收条件|
|---|---|---|---|---|---|---|
|ING-01|认证与策略校验|采集|认证主体、事件注册表|ANL_01–03|错误身份会污染主体统计和数据访问边界|匿名凭据只能上报绑定安装/epoch，登录主体从认证上下文取得|
|ING-02|Redis 入口限流|采集 / 平台|Redis 限流命名空间|ANL_02、03、后台查询|采集入口无配额会影响写入容量和业务服务稳定性|限额超出时 429 和 Retry-After；Redis 故障时采集入口返回可重试 503|
|ING-03|批次校验与事件级回执|采集|analytics_ingest_receipt|ANL_03|没有逐条回执就无法区分应重试和应丢弃的数据|外层超过限额返回 413；同一合法批次可出现四种逐条回执|
|ING-04|Kafka 生产与持久确认|采集 / 平台|analytics.event|ANL_K01|Kafka 未确认便回成功会丢失客户端已清除的事件|Kafka 确认后才返回 accepted；ACK 未知重试同一 eventId|
|ING-05|业务可靠投递|业务 / 平台|PG 与 Mongo Outbox、inquiries.created|ANL_K01、K02|业务提交和异步发布之间的失败会造成成功事实缺失|进程在事务后退出仍可补发；位点只在下游持久确认后推进|
|ING-06|消费重试和死信|数据 / 平台|analytics.event.dlq.v1|ANL_K03|缺少失败隔离和连续位点会造成消费停滞或漏数|临时失败重试；永久失败经清洗入 DLQ；位点不跳过未持久化记录|
|ING-07|结构化文件日志|平台 / 采集|JSON Lines .log、analytics_admin_audit|所有服务|结构化 .log 是既定要求，也是联调与故障排查依据|访问、运行、错误、重放及审计均有 .log；可轮转续传；无原始请求体和正文|

### 2.4 数仓与质量

|功能编号|功能|主责|数据落点|接口或事件|业务价值|验收条件|
|---|---|---|---|---|---|---|
|DW-01|原始事实接收|数据|events_raw|ANL_K01|固定 Kafka 水位重建和排查须留安全原始事实|可按原 Kafka 分区和位点追踪；非法正文在落仓前拦截|
|DW-02|逻辑去重及冲突隔离|数据 / 采集|events_canonical、analytics_ingest_receipt|内部去重构建|重复消费和迟到补报会直接造成统计虚增|重试、回放和消费者重启后报表只计一次；冲突版本可追踪|
|DW-03|安全维度与主体|数据 / 业务|dim_entity_safe、fact_actor_active_day、首活索引|内部维度同步|跨端去重和历史维度决定活跃与分组的可信度|历史事件保留事件时快照，登录跨端去重，匿名独立|
|DW-04|事实和聚合|数据|fact_page_visit、fact_search、fact_inquiry_flow、fact_dialog_reply、fact_translate_task、agg_metric_day、agg_retention_day|内部聚合|本次页面、询盘、搜索、翻译和留存依赖派生事实|跨日 UV 重算，分位数可合并，观察未成熟返回 provisional|
|DW-05|快照发布及内部回滚|数据 / 平台|analytics_report_build、analytics_outbox|ANL_10、内部发布与回滚作业|逐表切换会造成看板同时读到不同版本|校验成功才切换整份清单；失败保留旧快照；可由受权值班人员按运行流程回切，记录审计|
|DW-06|业务对账与告警|数据 / 平台|agg_quality_minute、inquiries、messages、company_view_raw|ANL_17|缺少权威源对账时无法判断指标下降还是漏采|按同一业务日期及水位展示来源数、仓库数和差值|
|DW-07|回放与日期重算|数据 / 平台|analytics_repair_job、DLQ|ANL_21、22；内部执行器|本次发生迟到或漏采时必须能恢复统计|dryRun 不发布；重放保留原事件 ID；同幂等键只建一单；值班可创建并按 ID 查结果|
|DW-08|删除与留存|平台 / 数据|analytics_subject_erasure、TTL、抑制清单|内部清理作业|未有保留和抑制规则，重放可能恢复已清理数据|主体清理覆盖明细和派生快照；回放或备份恢复后不复活数据|

### 2.5 运营看板

|功能编号|功能|主责|数据落点|接口或事件|业务价值|验收条件|
|---|---|---|---|---|---|---|
|BI-01|经营总览|Web / 数据|指标卡、日趋势、同期对比|ANL_11|为本次经营指标提供统一入口和下钻起点|DAU/WAU/MAU、匿名 UV、业务 PV、询盘量及翻译成功率均可查询|
|BI-02|页面分析|Web / 数据|PV/UV 榜、闭合率、退出率、五桶停留图|ANL_12|页面可见停留、闭合率和访问量需要可用的查询界面|筛选到单页时五个桶齐全；时长分母可查看|
|BI-03|搜索分析|Web / 数据|搜索请求、零结果和打开率、收藏变化|ANL_13，feature=search|搜索入口效率是本次核心，收藏显示可独立延期|成功零结果和请求失败分开；展示搜索量、零结果率和打开率；收藏页见 BI-11|
|BI-04|询盘与搜索漏斗|Web / 数据|F01/F02 条形图、耗时、未归因原因|ANL_14|F01/F02 是本次询盘转化和搜索转化的验收页面|F01 以 24 小时、F02 以 15 分钟判定；阶段与归因分母可复核|
|BI-05|聊天与翻译|Web / 数据|发送量、24h 回复、语言对、翻译耗时和错误|ANL_13|本次需要区分沟通使用、翻译执行和记录同步质量|人工与系统消息分类正确；执行成功率与同步失败率分开|
|BI-06|活跃留存|Web / 数据|Cohort 热力矩阵与成熟提示|ANL_15|本次明确要验收 D1/D7/D30 留存口径和成熟状态|D1/D7/D30 可筛选；未成熟显示预计日期；历史未知 cohort 单列|
|BI-07|质量看板|Web / 数据|受理、拒绝、延迟、Lag、对账、作业状态|ANL_17、21、22|没有质量入口无法反馈采集缺失、对账差和作业结果|可定位来源、版本和异常日期；清楚显示过期或部分数据|
|BI-08|页面权限、筛选与快照轮询|Web / 平台|权限菜单、快照、全局筛选、通知和轮询|ANL_10|授权菜单和版本轮询保证看板可用，分钟级刷新无需推送|每 60 秒只查元数据；snapshotId 变化后刷新报表；同次下钻保持快照；旧响应不覆盖新筛选|
|BI-09|数据展示状态|Web / 数据|加载、空数据、权限不足、缺分母、低基数|ANL_11–15、17|真实零值、无分母及未成熟若混显会误读业务|无有效分母显示为空；低基数输出 suppressed；同一页卡片共用 snapshotId|
|BI-10|会话时间线|Web / 平台|受控元数据、页面停留、步骤终态|ANL_16|用于单会话排查，本次用质量与对账聚合定位问题|工单与权限有效后才可读取；分页稳定，内容无原文和译文|
|BI-11|收藏看板|Web / 数据|agg_metric_day；维度和真实添加/取消事件|ANL_13，feature=favorite|用于衡量收藏转化，依赖 BIZ-03 的事实覆盖|按添加、取消、人数展示，标记历史覆盖起点|
|BI-12|报表导出|Web / 数据|快照和查询定义；导出作业方案见 §4|ANL_24、25|便于运营分发和复盘，在线查询先满足核心分析|导出固定快照、使用同一数据范围及脱敏口径，审计可追踪|
|BI-13|列表卡片可见曝光与 CTR|端 / 数据 / Web|新版本事件；稳定 placement、target、impression 关联|ANL_03、23；EVT_25、26 契约评审|能度量首页推荐和搜索卡片曝光；本次可计算搜索打开率|明确可见阈值，虚拟列表滚动不重复触发，CTR 分母为有效卡片曝光|

### 2.6 权限与语料

|功能编号|功能|主责|数据落点|接口或事件|业务价值|验收条件|
|---|---|---|---|---|---|---|
|GOV-01|角色与数据范围|平台 / 业务|analytics_access_grant|首期后台接口|内部使用者必须按角色和数据范围查询|运营、分析、值班按权限校验数据范围；扩展角色随对应 Should 功能启用|
|GOV-02|受控操作审计|平台|analytics_admin_audit、审计 .log|ANL_21、内部回滚与清理|修复与回滚涉及受控数据和写操作，需要可追溯|记录操作者、工单、范围和结果；审计失败拒绝受控操作|
|GOV-03|聊天、翻译与分析隔离|业务 / 平台|messages、translate_records、ClickHouse|ANL_K01、K03|行为分析与业务正文隔离是本次上线条件|行为库、Topic、日志及死信中找不到正文和凭据|
|GOV-04|语料汇总与作业状态|业务 / Web|corpus_datasets、corpus_items、语料作业状态|ANL_18、19；corpus_asset_status_changed<br>[EVT_24](./ipaklink-埋点数据后台接口文档.md#492-evt_24-corpus_asset_status_changed)|用于语料运营，对应功能启用时按数据集范围开放|按数据集、语言对、状态可查，越权返回 403，汇总可追溯到受权作业|
|GOV-05|语料用途与撤回传播|业务 / 平台|corpus_datasets、corpus_items|内部治理作业|新增语料用途及撤回传播依赖治理规则；已有业务内容的权限及分析主体清理在 Must|启用来源须校验用途与授权；撤回后影响关联条目和统计|

### 2.7 后台管理

|功能编号|功能|主责|数据落点|接口或事件|业务价值|验收条件|
|---|---|---|---|---|---|---|
|ADM-01|事件字典、灰度规则编辑与发布|采集 / 平台|analytics_event_schema、analytics_collection_policy|ANL_26–31、43、45|本次发布和关停可由部署配置执行|有草稿、校验、生效版本和操作审计|
|ADM-02|修复历史、失败明细与后台回滚|平台 / Web|analytics_repair_job、analytics_report_snapshot|ANL_32–34、44；复用 ANL_21、22|本次用作业 ID 和受控运行流程处置|按状态和日期查作业，回滚后显示目标快照并审计|
|ADM-03|授权、撤销和审计查询|平台 / Web|analytics_access_grant、analytics_admin_audit|ANL_35–38|本次授权可经 SSO 和受控配置管理|权限范围可验证，撤销生效，审计记录可按工单查找|
|ADM-04|语料导入、审核、清理操作|业务 / 平台|corpus_datasets、corpus_items、corpus_jobs|ANL_39–41；ANL_18、19 读取|需与 GOV-04、05 对齐用途和删除语义|创建动作带用途与工单，进度可查，撤回后的分析数据可重算|
|ADM-05|质量异常消息通知|平台 / Web|质量事件、告警投递去重键与审计|ANL_42；内部投递任务|重要异常便于主动发现，本次值班使用质量页和现有监控|同一异常按窗口去重，授权渠道可接收并能打开对应质量范围|

## 3. 采集事件子清单

24 项已有事件随父功能统一交付。BI-13 新增 EVT_25/26，事件字段和可见阈值以本次 API 契约评审为准。

下表按接口文档第 4 章逐项拆解第 2 章的功能。事件编号可直接跳到完整字段表和传输示例。App 专用字段位于 `events[].properties`，服务端专用字段位于 `properties`。括号中的条件沿用对应契约。

### 3.1 会话与页面

|事件编号 / eventName|所属功能|生产端|专用字段与条件|业务入口 / 分析接口|看板或校验入口|验收条件|
|---|---|---|---|---|---|---|
|[EVT_01 / §4.3.1](./ipaklink-埋点数据后台接口文档.md#431-evt_01-app_session_start)<br>`app_session_start`|SDK-03|App|`launchSource`（string，是）|端侧生命周期<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_11、IPK_ANL_16|冷启动或切号新建 sessionId；同一会话只记一次启动，launchSource 与入口一致|
|[EVT_02 / §4.3.2](./ipaklink-埋点数据后台接口文档.md#432-evt_02-app_background)<br>`app_background`|SDK-03|App|`properties` 固定为空对象|端侧生命周期<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_16|active→inactive→background 只产生一条；再次回前台后切后台可产生新事件|
|[EVT_03 / §4.3.3](./ipaklink-埋点数据后台接口文档.md#433-evt_03-screen_view)<br>`screen_view`|SDK-04|App|`entrySource`（string，是）|端侧导航<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_12、IPK_ANL_16|首屏和真实切页各一个 pageViewId；重复回调与短后台返回不增加 PV|
|[EVT_04 / §4.3.4](./ipaklink-埋点数据后台接口文档.md#434-evt_04-screen_exit)<br>`screen_exit`|SDK-04|App|`visibleMs`（int，是）、`exitReason`（string，是）|端侧导航<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_12、IPK_ANL_16|前台 20 秒、后台 30 秒、再前台 10 秒的 visibleMs 约为 30000；异常退出无结算|

### 3.2 搜索与详情

|事件编号 / eventName|所属功能|生产端|专用字段与条件|业务入口 / 分析接口|看板或校验入口|验收条件|
|---|---|---|---|---|---|---|
|[EVT_05 / §4.4.1](./ipaklink-埋点数据后台接口文档.md#441-evt_05-search_submit)<br>`search_submit`|BIZ-01|App|`searchId`（UUID，是）、`scope`（string，是）、`queryLengthBucket`（string，是）、`filterCount`（int，是）|GET /search<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_13、IPK_ANL_14|新请求生成 searchId；翻页不新增提交；搜索原词和筛选原文不入仓|
|[EVT_06 / §4.4.2](./ipaklink-埋点数据后台接口文档.md#442-evt_06-search_result)<br>`search_result`|BIZ-01|App|`searchId`（UUID，是）、`scope`（string，是）、`outcome`（string，是）、`resultCount`（int，成功时必填）、`latencyMs`（int，是）、`errorCode`（string，失败时必填）|GET /search<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_13、IPK_ANL_14|成功零结果填写 0；失败带 errorCode 且无 resultCount；分页不覆盖首屏|
|[EVT_07 / §4.4.3](./ipaklink-埋点数据后台接口文档.md#443-evt_07-search_result_open)<br>`search_result_open`|BIZ-01|App|`searchId`（UUID，是）、`targetType`（string，是）、`targetId`（string，是）、`position`（int，是）|GET /search 返回结果；后续进入对应详情<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_13、IPK_ANL_14|序号从 1 开始并计入分页偏移；用户结果的目标 ID 入仓前假名化|
|[EVT_08 / §4.4.4](./ipaklink-埋点数据后台接口文档.md#444-evt_08-entity_detail_view)<br>`entity_detail_view`|BIZ-02|App|`targetType`（string，是）、`targetId`（string，是）、`companyId`（string，是）、`productId`（string，产品时必填）、`searchId`（UUID，否）、`entrySource`（string，是）|`GET /companies/{companyId}`、`GET /products/{productId}`；各自响应的实体 ID 映射到事件字段。<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_14|对象加载可见才触发；企业/产品目标与 ID 一致；搜索来源可通过 searchId 关联|

### 3.3 浏览与收藏

|事件编号 / eventName|所属功能|生产端|专用字段与条件|业务入口 / 分析接口|看板或校验入口|验收条件|
|---|---|---|---|---|---|---|
|[EVT_09 / §4.5.1](./ipaklink-埋点数据后台接口文档.md#451-evt_09-business_view_recorded)<br>`business_view_recorded`|BIZ-02|服务端|`viewRef`（string，是）、`targetType`（string，是）、`targetId`（string，是）|POST /views；IPK_ANL_B02<br>`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|IPK_ANL_11、IPK_ANL_17|MongoDB 持久化后出事件；同一 viewRef 去重，两次真实浏览保留两条|
|[EVT_10 / §4.5.2](./ipaklink-埋点数据后台接口文档.md#452-evt_10-favorite_changed)<br>`favorite_changed`|BIZ-03|服务端|`targetType`（string，是）、`targetId`（string，是）、`action`（string，是）|POST /favorites；POST /favorites/cancel；IPK_ANL_B05<br>`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|IPK_ANL_13|添加和取消分别得到 add、remove；重复幂等请求无新增事实|

### 3.4 询盘

|事件编号 / eventName|所属功能|生产端|专用字段与条件|业务入口 / 分析接口|看板或校验入口|验收条件|
|---|---|---|---|---|---|---|
|[EVT_11 / §4.6.1](./ipaklink-埋点数据后台接口文档.md#461-evt_11-contact_supplier_click)<br>`contact_supplier_click`|BIZ-04|App|`contactFlowId`（UUID，是）、`companyId`（string，是）、`productId`（string，否）|页面跳转 InquiryEdit，无独立写接口<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_14|联系点击生成 contactFlowId 并传入表单，企业及产品标识一致|
|[EVT_12 / §4.6.2](./ipaklink-埋点数据后台接口文档.md#462-evt_12-inquiry_submit)<br>`inquiry_submit`|BIZ-04|App|`contactFlowId`（UUID，否）、`clientRequestId`（string，是）、`companyId`（string，是）、`productId`（string，否）|POST /inquiries；IPK_ANL_B01<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_14|实际发出业务请求才上报；网络重试沿用 clientRequestId，计一次逻辑提交|
|[EVT_13 / §4.6.3](./ipaklink-埋点数据后台接口文档.md#463-evt_13-inquiry_submit_failed)<br>`inquiry_submit_failed`|BIZ-04|App|`clientRequestId`（string，是）、`attemptNo`（int，是）、`errorCode`（string，是）|POST /inquiries；IPK_ANL_B01<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_16（授权会话时间线）|网络尝试号递增；失败使用固定错误码；表单未通过校验时无本事件|
|[EVT_14 / §4.6.4](./ipaklink-埋点数据后台接口文档.md#464-evt_14-inquiry_created)<br>`inquiry_created`|BIZ-04|服务端|`inquiryRef`（string，是）、`companyId`（string，是）、`productId`（string，否）、`contactFlowId`（UUID，否）、`clientRequestId`（string，否）|POST /inquiries；inquiries.created（K02）；IPK_ANL_B01<br>`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|IPK_ANL_11、IPK_ANL_14、IPK_ANL_17|PG 已提交才投递；同一 inquiryId 只计一次；归因缺失仍计入成功总量|

### 3.5 消息与通知

|事件编号 / eventName|所属功能|生产端|专用字段与条件|业务入口 / 分析接口|看板或校验入口|验收条件|
|---|---|---|---|---|---|---|
|[EVT_15 / §4.7.1](./ipaklink-埋点数据后台接口文档.md#471-evt_15-chat_session_open)<br>`chat_session_open`|BIZ-05|App|`dialogRef`（string，是）、`entrySource`（string，是）|GET /chat/dialogs/{id}/messages 是相关读取接口；进入页触发分析<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_13|真实会话可见才触发；再次进入可记新事件；会话引用与发送事实一致|
|[EVT_16 / §4.7.2](./ipaklink-埋点数据后台接口文档.md#472-evt_16-message_send_succeeded)<br>`message_send_succeeded`|BIZ-05|服务端|`dialogRef`（string，是）、`messageRef`（string，是）、`messageType`（string，是）、`senderKind`（string，是）|POST /chat/dialogs/{id}/messages；IPK_ANL_B03<br>`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|IPK_ANL_13、IPK_ANL_17|消息首次持久化计一次；补发无重复；人工、系统、询盘卡片分类正确|
|[EVT_17 / §4.7.3](./ipaklink-埋点数据后台接口文档.md#473-evt_17-notification_open)<br>`notification_open`|SDK-07|App|`category`（string，是）|GET /notifications 为列表读取；有已读动作时沿用 POST /notifications/read<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|仓库验收，按事件入仓验收|实际点击通知才入仓，收到推送或已读回调不重复；按 category 抽验|
|[EVT_18 / §4.7.4](./ipaklink-埋点数据后台接口文档.md#474-evt_18-message_translation_toggle)<br>`message_translation_toggle`|SDK-08|App|`displayMode`（string，是）|端侧显示；如需同步译文仍走既有 POST /chat/messages/{id}/translations<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|仓库验收，按事件入仓验收|切换后的 displayMode 正确；重复渲染和同状态点击不入仓|

### 3.6 翻译

|事件编号 / eventName|所属功能|生产端|专用字段与条件|业务入口 / 分析接口|看板或校验入口|验收条件|
|---|---|---|---|---|---|---|
|[EVT_19 / §4.8.1](./ipaklink-埋点数据后台接口文档.md#481-evt_19-translate_started)<br>`translate_started`|BIZ-06|App|`taskId`（UUID，是）、`mode`（string，是）、`sourceLang`（string，是）、`targetLang`（string，是）|端侧翻译执行；记录同步独立<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_13|任务实际开始才上报；主动重试生成新 taskId；同事件补传保留 eventId|
|[EVT_20 / §4.8.2](./ipaklink-埋点数据后台接口文档.md#482-evt_20-translate_completed)<br>`translate_completed`|BIZ-06|App|`taskId`（UUID，是）、`mode`（string，是）、`sourceLang`（string，是）、`targetLang`（string，是）、`outcome`（string，是）、`latencyMs`（int，是）、`errorCode`（string，失败时必填）|端侧翻译执行；记录同步独立<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|IPK_ANL_13|taskId 与启动一致；首个有效终态计一次；失败带错误码，取消单列|
|[EVT_21 / §4.8.3](./ipaklink-埋点数据后台接口文档.md#483-evt_21-translate_sync_completed)<br>`translate_sync_completed`|BIZ-06|服务端|`recordRef`（string，是）、`mode`（string，是）、`outcome`（string，是）|POST /translate/records、/translate/dialog-sessions、/translate/image-records、/translate/file-jobs；IPK_ANL_B04<br>`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|IPK_ANL_13|四种业务记录同步按模式区分；记录 ID 和状态版本去重；与执行成败分开|
|[EVT_22 / §4.8.4](./ipaklink-埋点数据后台接口文档.md#484-evt_22-translate_copy)<br>`translate_copy`|SDK-08|App|`mode`（string，是）|端侧剪贴板操作<br>`POST /ipaklink/v1/analytics/events/report`（IPK_ANL_03），Collector 写 K01|仓库验收，按事件入仓验收|剪贴板写入成功才入仓，复制失败无事件；仅采集 mode|

### 3.7 账号与语料

|事件编号 / eventName|所属功能|生产端|专用字段与条件|业务入口 / 分析接口|看板或校验入口|验收条件|
|---|---|---|---|---|---|---|
|[EVT_23 / §4.9.1](./ipaklink-埋点数据后台接口文档.md#491-evt_23-account_registered)<br>`account_registered`|BIZ-07|服务端|`channel`（string，是）|POST /auth/register<br>`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|仓库验收，按事件入仓验收|用户首次注册事务提交才入仓；按权威用户键去重，原账号标识不入分析消息|
|[EVT_24 / §4.9.2](./ipaklink-埋点数据后台接口文档.md#492-evt_24-corpus_asset_status_changed)<br>`corpus_asset_status_changed`|GOV-04|服务端|`datasetCode`（string，是）、`languagePair`（string，是）、`status`（string，是）、`reasonCode`（string，否）|内部治理作业；IPK_ANL_18、19 为查询接口<br>`analytics.event`（IPK_ANL_K01），仅受信任的内部生产者|IPK_ANL_18（语料汇总）；IPK_ANL_19 查询作业状态|状态真实变更才入仓；按数据集、语言对及状态核验汇总；审查原文不进入事件|

## 4. 数据库与接口落点

下表为功能与数据库、接口的实施对应。数据设计可复用现有模型或随功能增补表、索引和关联键。

|功能|数据模型与复用|实现与验收|
|---|---|---|
|SDK-07、SDK-08、BIZ-03、BIZ-07|沿用事件注册表的 event_name、schema_version、source、property_schema、status 和安全 properties；保留可选事件的版本定义|启用采集并验证通知 category、译文 displayMode/mode、收藏 action、注册 channel；报表标明数据启用日期|
|BI-10 会话时间线|ClickHouse 保留 session_id、page_view_id、sequence 与发生时间；PG 现有授权的 scope、ticket_id、expires_at 可支持后续受控访问|启用 ANL_16、会话查询索引及审计；同快照分页稳定，越权返回 403|
|BI-11 收藏分析|保留 metric_code、metric_version、dim_set、dim_values 和 generation；收藏事件保留稳定目标 ID 及 action|新增聚合口径，ANL_13 按 feature=favorite 查询；收藏前的历史范围注明覆盖状态|
|BI-12 报表导出|复用 snapshot_id、metric_version、权限作用域和审计 request_id；查询参数保持可序列化|如需异步导出，再建导出作业表，字段包括 job_id、snapshot_id、requester_id、query_spec、status、artifact_ref、expires_at|
|BI-13 列表曝光与 CTR|复用事件信封的 page_view_id、session_id、schema_version 及事件白名单；保留已有稳定 targetType/targetId|评审新事件的 placementCode、listRequestId、impressionId、position、visibleMs 与去重窗；曝光分母定稿后再启用|
|ADM-01 采集治理|PG 配置保留 policy_version、status、effective_at，事件字典保留 schema_version/status|补草稿、校验、发布 API，审计记录关联 policy_version|
|ADM-02 修复与回滚后台|PG 保留 job_id、scope、status、created_at、finished_at、result，以及快照 ID、历史 manifest 和发布水位|增加列表及回滚操作，按数据范围校验；回滚操作写审计|
|ADM-03 权限与审计管理|复用 grant_id、principal_id、role_code、scope、expires_at、revoked_at 和 audit request_id|补授权/撤销与审计搜索；依查询负载再补索引|
|GOV-04、GOV-05、ADM-04 语料治理|统一 datasetCode、sourceRef、revision、policyRef 与 expiresAt/tombstoneAt 的命名和关系；沿用 MongoDB 的内容隔离|按用途建集合、作业表及索引，再启用写入、审核、清理与派生统计|
|ADM-05 质量异常通知|保留质量 bucket、fact_code、来源和快照 ID，以便后续生成稳定告警键|按窗口做投递去重；启用授权渠道；状态和操作落审计|

## 5. 交付验收

|验收面|统一交付判据|
|---|---|
|采集|24 个已有事件及卡片曝光与点击按已确认 Schema 启用；断网、限流、批次回执、死信和去重可验证|
|数据层|业务成功事实可对账；快照、重算、回放、清理、角色权限与审计可验收|
|全部看板|总览、页面、搜索、F01/F02、聊天与翻译、留存、质量、收藏、卡片 CTR、导出及受控路径可查询或操作|
|治理与管理|配置、修复和快照、授权和审计、语料、通知投递按功能编号验收|

F01 使用 24 小时归因窗口，F02 使用 15 分钟搜索窗口，停留时间累计有效前台可见片段。看板每 60 秒通过 HTTPS 查询 ANL_10，快照变化后刷新。依赖项可以按开发顺序实现，全部功能进入同一次交付验收。
