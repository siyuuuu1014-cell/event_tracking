# ipaklink 埋点数据后台数据字典

版本 1.0 · 数据库设计 · 基线《冻结功能清单 v4.1》

## 1. 范围

数据模型服务于 [冻结功能清单](./ipaklink-埋点数据后台功能清单.md) 的统一交付 53 项。数据字典定义字段、键、分期、关联、迁移和验收。端侧及业务接口仍以 [事件与接口契约](./ipaklink-埋点数据后台接口文档.md) 为准。既有业务表和集合只列分析所需的字段投影，源字段名与类型在业务沙箱核对后映射。本稿未执行迁移。所有表纳入本次交付，下文原有期次标记用于识别此前的扩展边界。HTTP 及 Mock 以 [本次 API 接口文档](./ipaklink-埋点数据后台API接口文档.md) 为准。

## 2. 字段表填写规则

- 字段表一律使用 `字段 | 类型 | 长度 | 主键 | 外键 | 唯一 | 非空 | 默认值 | 说明`。长度对固定类型表示字节，对 PostgreSQL `varchar` 和 `char` 表示字符上限或定长，`—` 表示类型无固定长度或不适用。ClickHouse 可空字段以 `Nullable(T)` 落库。
- 主键栏的「复合」表示属于同一个主键，唯一栏的「组合」表示整组键唯一。ClickHouse 键在说明中列为排序键或逻辑去重键，由数据生成及快照发布校验保证。排序主键本身不保证物理唯一。[ClickHouse 数据建模说明](https://clickhouse.com/blog/postgres-to-clickhouse-data-modeling-tips)
- PostgreSQL 外键栏标为 `FK` 的关系要在数据库中建立，复合外键整体声明。MongoDB 或跨库关系标为「逻辑」，由写入应用、必要时的同库事务和定期对账维护。物理外键默认使用 `ON DELETE RESTRICT`，删除业务内容通过受控清理作业传播。[PostgreSQL 约束](https://www.postgresql.org/docs/18/ddl-constraints.html) · [MongoDB 数据一致性](https://www.mongodb.com/docs/v8.0/data-modeling/data-consistency/)
- 默认值为 `—` 表示未声明业务默认值，必填字段由写方明确给值。ClickHouse 写入器按信封校验必填字段，避免引擎默认空字符串或零掩盖漏列。MongoDB 必填与默认值分别由集合验证器和业务写入器保证。安全属性只录注册字段。时间以 UTC 存储，业务日期按冻结设计所用的 Asia/Tashkent 计算。

## 3. 关系与完整性

|关系|基数|关联与处理|
|---|---|---|
|事件版本 → 受理记录|一对多|analytics_ingest_receipt(event_name,schema_version) 复合 FK 指向 analytics_event_schema；策略使用独立 policy_version FK。受理记录和外键依赖到期前保留父记录。|
|快照 ↔ 数据集构建|多对多|analytics_report_snapshot_build 将 snapshot_id 与 (dataset,date_key,generation) 连接。逻辑上一个构建可被多个快照复用，同一快照的同一数据集日期仅选择一代。|
|内部账号 ↔ 分析角色|多对多|analytics_access_grant 为关联表。role_code 指向 analytics_role，principal_id 指向 analytics_admin_account。可按角色、范围与工单授予多条记录。|
|快照 → 回放发布结果、导出、通知|一对多|published_snapshot_id、snapshot_id 物理 FK 指向快照。删除前检查合规保留、导出和审计引用。|
|语料数据集修订 → 条目和作业|一对多|corpus_items.datasetId、corpus_jobs.datasetId 手动引用 MongoDB corpus_datasets._id。写入校验父集合修订状态和权限。父集禁用后阻止新写入并扫描已存在的子项，清理按作业推进与复核。|
|业务主体或内容 → 分析事实|一对多|通过 actor_key、目标 ID、dialog_ref、record_ref 等安全标识跨库关联。业务库为权威源，行为仓库按水位补偿和对账。|
|会话 → 页面访问 → 事件|一对多|session_id 标识会话，page_view_id 标识访问。ClickHouse 按逻辑键关联。迟到或缺失结算走未闭合或修复流程，查询不推定退出时刻。|
|列表曝光 → 点击|一对多|下期 BI-13 用 impression_id 对应多次点击，CTR 分子以是否存在匹配点击去重。曝光字段和阈值随下期接口冻结。|

### 3.1 关键物理关联示例

```sql
ALTER TABLE analytics_ingest_receipt
  ADD CONSTRAINT fk_ingest_schema
  FOREIGN KEY (event_name, schema_version)
  REFERENCES analytics_event_schema (event_name, schema_version)
  ON DELETE RESTRICT;

CREATE TABLE analytics_report_snapshot_build (
  snapshot_id varchar(80) NOT NULL,
  dataset varchar(80) NOT NULL,
  date_key date NOT NULL,
  generation bigint NOT NULL CHECK (generation > 0),
  PRIMARY KEY (snapshot_id, dataset, date_key),
  FOREIGN KEY (snapshot_id)
    REFERENCES analytics_report_snapshot (snapshot_id) ON DELETE RESTRICT,
  FOREIGN KEY (dataset, date_key, generation)
    REFERENCES analytics_report_build (dataset, date_key, generation) ON DELETE RESTRICT
);
CREATE INDEX idx_snapshot_build_reverse
  ON analytics_report_snapshot_build (dataset, date_key, generation);
```

关联表设计取代数据库方案里以 JSON manifest 作为权威关系的方式。PG 发布事务写入或验证关联行、归档上一快照、激活新快照。对外 manifest 由关联行组装，接口结构保持兼容。本期按快照轮询更新。

## 4. PostgreSQL 事务、配置与治理表

### analytics_event_schema

**阶段与功能** 本期；SDK-06、ING-01、ADM-01。

主键 (event_name, schema_version)。启用后的版本只增不改；兼容规则按来源校验。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`event_name`|`varchar(80)`|80|复合|—|组合|是|—|稳定事件代码|
|`schema_version`|`integer`|4|复合|—|组合|是|—|正整数版本|
|`source`|`varchar(12)`|12|—|—|—|是|—|app 或 server|
|`property_schema`|`jsonb`|—|—|—|—|是|—|事件属性、类型、枚举和必填规则|
|`status`|`varchar(16)`|16|—|—|—|是|`draft`|draft、active、retired|
|`created_at`|`timestamptz`|8|—|—|—|是|`now()`|登记时间|
|`activated_at`|`timestamptz`|8|—|—|—|否|—|生效时间；历史回放可读取已退役版本|

### analytics_collection_policy

**阶段与功能** 本期；SDK-01、ADM-01。

主键 policy_version。生效版本通过同事务更新的发布记录或部分唯一索引保证单一 active。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`policy_version`|`integer`|4|是|—|是|是|—|配置版本，正整数|
|`rules`|`jsonb`|—|—|—|—|是|—|应用平台、版本范围、采集配额与事件开关的结构化配置|
|`status`|`varchar(16)`|16|—|—|—|是|`draft`|draft、active、retired|
|`effective_at`|`timestamptz`|8|—|—|—|否|—|正式生效时间|
|`created_at`|`timestamptz`|8|—|—|—|是|`now()`|创建时间|

### analytics_ingest_receipt

**阶段与功能** 本期；SDK-02、ING-03、ING-04、DW-02。

主键 event_id。索引 expires_at；同 ID 不同摘要拒绝；确认状态通过租约令牌条件更新。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`event_id`|`uuid`|16|是|—|是|是|—|客户端事件 ID|
|`event_name`|`varchar(80)`|80|—|FK 组 A → analytics_event_schema.event_name|—|是|—|事件注册表的事件名|
|`schema_version`|`integer`|4|—|FK 组 A → analytics_event_schema.schema_version|—|是|—|事件注册表的版本|
|`policy_version`|`integer`|4|—|FK → analytics_collection_policy.policy_version|—|是|—|服务端实际采用的配置版本|
|`producer_scope`|`varchar(128)`|128|—|—|—|是|—|租约及身份校验作用域|
|`subject_epoch`|`uuid`|16|—|—|—|是|—|安装身份阶段；切号后不能沿用|
|`payload_hash`|`char(64)`|64|—|—|—|是|—|规范负载 SHA-256|
|`canonical_time`|`timestamptz`|8|—|—|—|是|—|验证后的发生时间|
|`first_received_at`|`timestamptz`|8|—|—|—|是|`now()`|首次接收时刻|
|`state`|`varchar(16)`|16|—|—|—|是|`pending`|pending、published|
|`lease_token`|`uuid`|16|—|—|—|否|—|当前发布者的租约令牌|
|`lease_until`|`timestamptz`|8|—|—|—|否|—|租约到期|
|`published_at`|`timestamptz`|8|—|—|—|否|—|持久确认时刻|
|`expires_at`|`timestamptz`|8|—|—|—|是|—|清理时间，建议覆盖补报和重试窗口|

### analytics_outbox

**阶段与功能** 本期；ING-05。

主键 event_id；待投递部分索引 (available_at,event_id)。与权威业务写入同事务。目标 Topic 以冻结期实际启用的业务分析流为准。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`event_id`|`uuid`|16|是|—|是|是|—|业务事实稳定事件 ID|
|`aggregate_id`|`text`|—|—|—|—|是|—|安全对象引用|
|`topic`|`varchar(249)`|249|—|—|—|是|—|目标 Topic|
|`payload_safe`|`jsonb`|—|—|—|—|是|—|经过白名单投影的信封|
|`status`|`varchar(16)`|16|—|—|—|是|`pending`|pending、published、failed|
|`attempts`|`integer`|4|—|—|—|是|`0`|尝试次数|
|`available_at`|`timestamptz`|8|—|—|—|是|`now()`|下次领取时间|
|`lease_token`|`uuid`|16|—|—|—|否|—|发布租约令牌|
|`lease_until`|`timestamptz`|8|—|—|—|否|—|发布租约到期|
|`created_at`|`timestamptz`|8|—|—|—|是|`now()`|业务事务写入时间|
|`published_at`|`timestamptz`|8|—|—|—|否|—|持久确认时间|

### analytics_metric_def

**阶段与功能** 本期；BI-01–09、DW-04。

主键 (metric_code,metric_version)。查询模板固定并按权限与维度白名单运行。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`metric_code`|`varchar(80)`|80|复合|—|组合|是|—|指标代码|
|`metric_version`|`integer`|4|复合|—|组合|是|—|统计口径版本|
|`dimension_set`|`jsonb`|—|—|—|—|是|—|允许筛选和分组的维度|
|`query_template_key`|`varchar(80)`|80|—|—|—|是|—|固定 SQL 模板标识|
|`definition`|`jsonb`|—|—|—|—|是|—|分子、分母、时间口径、空值语义|
|`status`|`varchar(16)`|16|—|—|—|是|`active`|active、retired|

### analytics_report_build

**阶段与功能** 本期；DW-04、DW-05。

主键 (dataset,date_key,generation)，被发布清单明细引用。每个数据集使用声明的日期轴，例如业务日或 cohort 日。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`dataset`|`varchar(80)`|80|复合|—|组合|是|—|数据集代码|
|`date_key`|`date`|—|复合|—|组合|是|—|此构建覆盖的日期键|
|`generation`|`bigint`|8|复合|—|组合|是|—|构建代际，正整数|
|`source_offsets`|`jsonb`|—|—|—|—|是|—|各 Topic 分区的已固定消费水位|
|`checksum`|`char(64)`|64|—|—|—|否|—|校验通过后必填|
|`status`|`varchar(16)`|16|—|—|—|是|`building`|building、verified、failed|
|`reconciliation`|`jsonb`|—|—|—|—|否|—|构建验收的校验摘要|
|`built_at`|`timestamptz`|8|—|—|—|否|—|构建完成时间|

### analytics_report_snapshot

**阶段与功能** 本期；DW-05、BI-08；BI-12、ADM-02 复用。

主键 snapshot_id；对 status='published' 建常量部分唯一索引，保证只有一份当前快照。前一代变更为 archived 后仍可在到期前读取。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`snapshot_id`|`varchar(80)`|80|是|—|是|是|—|对外快照 ID|
|`metric_version`|`integer`|4|—|—|—|是|—|本次发布使用的统计版本|
|`generated_at`|`timestamptz`|8|—|—|—|是|`now()`|发布生成时间|
|`source_watermark`|`timestamptz`|8|—|—|—|是|—|对外数据水位|
|`status`|`varchar(16)`|16|—|—|—|是|`draft`|draft、published、archived、expired|
|`expires_at`|`timestamptz`|8|—|—|—|否|—|当前快照可空；归档后由清理策略设定|

### analytics_report_snapshot_build

**阶段与功能** 本期新增关联表；DW-05、BI-08。

多对多中间表。PK(snapshot_id,dataset,date_key) 保证同快照同数据集同日仅有一代。FK(snapshot_id) 及 FK(dataset,date_key,generation)，两者 ON DELETE RESTRICT。对后一组 FK 建索引。查询服务可从关系行组装 manifest，无需复制一份权威清单。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`snapshot_id`|`varchar(80)`|80|复合|FK → analytics_report_snapshot.snapshot_id|组合|是|—|所属快照|
|`dataset`|`varchar(80)`|80|复合|FK 组 B → analytics_report_build.dataset|组合|是|—|被引用的构建数据集|
|`date_key`|`date`|—|复合|FK 组 B → analytics_report_build.date_key|组合|是|—|被引用的构建日期|
|`generation`|`bigint`|8|—|FK 组 B → analytics_report_build.generation|—|是|—|被引用的构建代际|

### analytics_repair_job

**阶段与功能** 本期；DW-07、BI-07；ADM-02 复用。

主键 job_id，UQ(operator_id,idempotency_key)；按 (status,created_at) 查找。请求指纹变化返回 409。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`job_id`|`varchar(80)`|80|是|—|是|是|—|作业标识|
|`job_type`|`varchar(16)`|16|—|—|—|是|—|rebuild、replay|
|`operator_id`|`varchar(128)`|128|—|逻辑 → analytics_admin_account.account_id|组合|是|—|内部操作者账号 ID|
|`idempotency_key`|`varchar(128)`|128|—|—|组合|是|—|创建请求去重键|
|`request_hash`|`char(64)`|64|—|—|—|是|—|规范请求摘要|
|`scope`|`jsonb`|—|—|—|—|是|—|日期或 Topic 分区和位点范围|
|`ticket_id`|`varchar(80)`|80|—|—|—|是|—|授权工单|
|`dry_run`|`boolean`|1|—|—|—|是|`false`|试运行不发布|
|`status`|`varchar(16)`|16|—|—|—|是|`queued`|queued、running、succeeded、failed|
|`result`|`jsonb`|—|—|—|—|否|—|安全结果、计数或失败原因|
|`published_snapshot_id`|`varchar(80)`|80|—|FK → analytics_report_snapshot.snapshot_id|—|否|—|发布成功后的快照；试运行为空|
|`created_at`|`timestamptz`|8|—|—|—|是|`now()`|创建时间|
|`finished_at`|`timestamptz`|8|—|—|—|否|—|结束时间|

### analytics_admin_account

运营人员使用维护命令逐人开户，命令交互读取初始密码并生成 Argon2id 哈希。账号停用后保留历史审计引用。

| 字段 | 类型 | 长度 | 主键 | 外键 | 唯一 | 非空 | 默认值 | 说明 |
|---|---|---|---|---|---|---|---|---|
| `account_id` | `varchar(128)` | 128 | 是 | — | 是 | 是 | — | 后端生成的稳定账号 ID，用于授权和审计 |
| `username` | `varchar(64)` | 64 | 否 | — | 是 | 是 | — | 内部登录名，写入前转为小写 |
| `display_name` | `varchar(100)` | 100 | 否 | — | 否 | 是 | — | 员工显示名 |
| `password_hash` | `text` | — | 否 | — | 否 | 是 | — | 含随机盐和算法参数的 Argon2id 哈希 |
| `status` | `varchar(16)` | 16 | 否 | — | 否 | 是 | `active` | `active` 或 `disabled` |
| `created_at` | `timestamptz` | — | 否 | — | 否 | 是 | `now()` | 首次开户时间 |

后台登录会话存在 Redis，以随机会话令牌的摘要为键，值为 `account_id`，配套 TTL。登录限流计数也存 Redis。每次保护请求检查账号状态和有效授权。

### analytics_repair_failure

记录修复作业的安全失败码及可定位的 Kafka 位点。原消息内容不落此表。

| 字段 | 类型 | 长度 | 主键 | 外键 | 唯一 | 非空 | 默认值 | 说明 |
|---|---|---|---|---|---|---|---|---|
| `failure_id` | `bigserial` | 8 | 是 | — | 是 | 是 | 序列 | 失败明细标识与稳定分页顺序 |
| `job_id` | `varchar(80)` | 80 | 否 | → analytics_repair_job.job_id | 否 | 是 | — | 所属修复作业 |
| `topic` | `varchar(249)` | 249 | 否 | — | 否 | 否 | — | 对应 Kafka Topic |
| `partition` | `integer` | 4 | 否 | — | 否 | 否 | — | 对应 Kafka 分区 |
| `offset_value` | `varchar(20)` | 20 | 否 | — | 否 | 否 | — | 十进制 Kafka 位点 |
| `error_code` | `varchar(80)` | 80 | 否 | — | 否 | 是 | — | 安全错误码 |
| `occurred_at` | `timestamptz` | — | 否 | — | 否 | 是 | now() | 失败时间 |

### analytics_role

**阶段与功能** 本期新增角色字典；GOV-01、ADM-03。

主键 role_code。初期按部署配置初始化运营、分析及值班角色。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`role_code`|`varchar(40)`|40|是|—|是|是|—|角色代码|
|`name`|`varchar(80)`|80|—|—|—|是|—|显示名称|
|`status`|`varchar(16)`|16|—|—|—|是|`active`|active、disabled|

### analytics_access_grant

**阶段与功能** 本期；GOV-01；BI-10、ADM-03 复用。

内部账号与角色的多对多关联表。角色和账号均为物理 FK。增量迁移立即约束新授权。历史开发授权可保留，账号映射处理后可验证全部历史数据。索引(principal_id,role_code) 和 role_code；允许按不同范围或工单分别授予。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`grant_id`|`uuid`|16|是|—|是|是|—|授权记录 ID|
|`principal_id`|`varchar(128)`|128|—|→ analytics_admin_account.account_id|—|是|—|内部账号稳定 ID|
|`role_code`|`varchar(40)`|40|—|FK → analytics_role.role_code|—|是|—|角色代码|
|`scope`|`jsonb`|—|—|—|—|是|—|允许的数据集、区域或用途|
|`ticket_id`|`varchar(80)`|80|—|—|—|否|—|工单标识，敏感访问按策略必填|
|`expires_at`|`timestamptz`|8|—|—|—|否|—|到期时间|
|`revoked_at`|`timestamptz`|8|—|—|—|否|—|撤销生效时间|

### analytics_access_grant_request

按操作者与幂等键保存创建授权的请求摘要，与授权和审计同事务提交。

| 字段 | 类型 | 长度 | 主键 | 外键 | 唯一 | 非空 | 默认值 | 说明 |
|---|---|---|---|---|---|---|---|---|
| `operator_id` | `varchar(128)` | 128 | 联合 | → analytics_admin_account.account_id | 联合 | 是 | — | 发起授予的内部账号 |
| `idempotency_key` | `varchar(128)` | 128 | 联合 | — | 联合 | 是 | — | 请求头提供的幂等键 |
| `request_hash` | `char(64)` | 64 | 否 | — | 否 | 是 | — | 规范化请求 SHA-256 十六进制摘要 |
| `grant_id` | `uuid` | — | 否 | → analytics_access_grant.grant_id | 否 | 是 | — | 首次成功创建的授权 |

### analytics_admin_audit

**阶段与功能** 本期；GOV-02、ING-07；ADM-03 复用。

追加写。索引(operator_id,occurred_at)、(request_id)；下期按真实负载增补 ticket_id 索引。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`audit_id`|`uuid`|16|是|—|是|是|—|审计记录 ID|
|`operator_id`|`varchar(128)`|128|—|逻辑 → analytics_admin_account.account_id|—|是|—|后台操作者安全标识|
|`action`|`varchar(80)`|80|—|—|—|是|—|读取、配置、修复、回滚等动作|
|`ticket_id`|`varchar(80)`|80|—|—|—|否|—|操作关联工单|
|`scope_safe`|`jsonb`|—|—|—|—|是|—|脱敏后的访问范围|
|`result_code`|`varchar(80)`|80|—|—|—|是|—|结果码|
|`request_id`|`varchar(128)`|128|—|—|—|是|—|链路追踪编号，单请求可对应多条审计|
|`occurred_at`|`timestamptz`|8|—|—|—|是|`now()`|审计时间|

### analytics_subject_erasure

**阶段与功能** 本期；DW-08、GOV-03。

索引(actor_key,status)。写入抑制记录后再执行仓库删除和派生数据重算；恢复备份及回放必须读取。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`job_id`|`uuid`|16|是|—|是|是|—|清理作业 ID|
|`actor_key`|`varchar(128)`|128|—|—|—|是|—|分析主体假名|
|`status`|`varchar(16)`|16|—|—|—|是|`queued`|queued、running、succeeded、failed|
|`requested_at`|`timestamptz`|8|—|—|—|是|`now()`|发起时间|
|`finished_at`|`timestamptz`|8|—|—|—|否|—|完成时间|

### analytics_export_job

**阶段与功能** 下期；BI-12。

异步导出落表方案。本期复用快照键、口径版本、查询参数和审计关联。FK(snapshot_id) ON DELETE RESTRICT；索引(requester_id,created_at) 和 expires_at。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`job_id`|`varchar(80)`|80|是|—|是|是|—|导出 ID|
|`snapshot_id`|`varchar(80)`|80|—|FK → analytics_report_snapshot.snapshot_id|—|是|—|绑定已发布快照|
|`requester_id`|`varchar(128)`|128|—|逻辑 → analytics_admin_account.account_id|—|是|—|内部请求人账号 ID|
|`query_spec`|`jsonb`|—|—|—|—|是|—|固定时区、筛选、列和权限作用域|
|`status`|`varchar(16)`|16|—|—|—|是|`queued`|queued、running、succeeded、failed|
|`artifact_ref`|`text`|—|—|—|—|否|—|授权文件引用|
|`request_id`|`varchar(128)`|128|—|—|—|是|—|审计关联|
|`created_at`|`timestamptz`|8|—|—|—|是|`now()`|创建时间|
|`expires_at`|`timestamptz`|8|—|—|—|是|—|导出及文件访问截止|

### analytics_alert_delivery

**阶段与功能** 下期；ADM-05。

质量异常工单或通知投递防重表。PK(delivery_id)；UQ(alert_key, channel_code, destination_ref)，避免同一窗口重复发送。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`delivery_id`|`uuid`|16|是|—|是|是|—|投递标识|
|`alert_key`|`char(64)`|64|—|—|组合|是|—|规则、指标、维度和告警窗口的稳定摘要|
|`snapshot_id`|`varchar(80)`|80|—|FK → analytics_report_snapshot.snapshot_id|—|否|—|告警观测所用的快照|
|`channel_code`|`varchar(40)`|40|—|—|组合|是|—|授权通知渠道代号|
|`destination_ref`|`varchar(128)`|128|—|—|组合|是|—|授权接收对象或群组安全引用|
|`status`|`varchar(16)`|16|—|—|—|是|`pending`|pending、sent、failed|
|`attempts`|`integer`|4|—|—|—|是|`0`|发送尝试次数|
|`created_at`|`timestamptz`|8|—|—|—|是|`now()`|创建时间|
|`sent_at`|`timestamptz`|8|—|—|—|否|—|实际投递时间|

### analytics_quality_observation

采集端按请求写入处理计数，消费者按 Kafka 坐标记一次已确认死信。无平台和版本的观测只用于无维度筛选的质量统计。

| 字段 | 类型 | 长度 | 主键 | 外键 | 唯一 | 非空 | 默认值 | 说明 |
|---|---|---|---|---|---|---|---|---|
| observation_key | varchar(128) | 128 | 是 | — | 是 | 是 | — | 请求 ID 或死信的源坐标 |
| recorded_at | timestamptz | — | 否 | — | 否 | 是 | clock_timestamp() | 质量观测入库时刻 |
| platform | varchar(16) | 16 | 否 | — | 否 | 否 | — | 可验证的平台 |
| app_version | varchar(40) | 40 | 否 | — | 否 | 否 | — | 可验证的版本 |
| rejected | integer | 4 | 否 | — | 否 | 是 | 0 | 拒绝事件数 |
| duplicates | integer | 4 | 否 | — | 否 | 是 | 0 | 重复事件数 |
| rate_limited | integer | 4 | 否 | — | 否 | 是 | 0 | 限流请求数 |
| retry | integer | 4 | 否 | — | 否 | 是 | 0 | 返回重试状态的事件数 |
| dlq | integer | 4 | 否 | — | 否 | 是 | 0 | 已确认的死信数 |

### analytics_kafka_lag_sample

每个分区按采样时间保存消费组已提交位点与 Kafka 末端位点，积压为两者的差。

| 字段 | 类型 | 长度 | 主键 | 外键 | 唯一 | 非空 | 默认值 | 说明 |
|---|---|---|---|---|---|---|---|---|
| sampled_at | timestamptz | — | 联合 | — | 联合 | 是 | — | 一组分区共用的采样时刻 |
| topic | varchar(249) | 249 | 联合 | — | 联合 | 是 | — | Kafka Topic |
| partition | integer | 4 | 联合 | — | 联合 | 是 | — | Kafka 分区 |
| next_offset | bigint | 8 | 否 | — | 否 | 是 | — | 消费组的下一位点 |
| high_offset | bigint | 8 | 否 | — | 否 | 是 | — | 末端位点 |
| stalled | boolean | 1 | 否 | — | 否 | 否 | — | 有积压且一段时间未推进，历史不足为空 |

### 4.1 既有 PostgreSQL 业务表的分析投影

以下行是分析适配器的输入字段，类型描述投影结果。既有业务主键、列名和约束由各自业务迁移维护。源 ID 在可信边界内用于关联，发往分析流前按事件契约转换为安全引用。

#### inquiries 询盘投影

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`inquiry_id`|text|—|—|—|—|是|—|权威询盘键，映射 inquiryRef 并派生事件 ID|
|`buyer_id`|text|—|—|逻辑 → users 的业务主键|—|是|—|买家主体，出站前转为 actorKey|
|`company_id`|text|—|—|逻辑 → companies 的业务主键|—|是|—|目标供应商|
|`product_id`|text|—|—|逻辑 → products 的业务主键|—|否|—|相关产品，存在时校验归属企业|
|`contact_flow_id`|uuid|16|—|—|—|否|—|经校验的联系链路，历史或无归因记录可空|
|`client_request_id`|text|—|—|—|—|否|—|询盘幂等提交键，归因辅助信息|
|`submitted_at`|timestamptz|8|—|—|—|是|—|已提交的业务时间，取权威行而非客户端时间|

#### user_actions 收藏投影

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`relation_id`|text|—|—|—|—|是|—|稳定关系 ID，下期启用分析|
|`actor_id`|text|—|—|逻辑 → users 的业务主键|—|是|—|变更主体，出站前转换为 actorKey|
|`target_type`|text|—|—|—|—|是|—|企业或产品类型|
|`target_id`|text|—|—|逻辑 → 由 target_type 指定的业务实体|—|是|—|收藏目标|
|`state_version`|bigint|8|—|—|—|是|—|随有效变更推进，和 relation_id 共同生成稳定事件 ID|
|`action`|text|—|—|—|—|是|—|从状态变化派生 add 或 remove|
|`changed_at`|timestamptz|8|—|—|—|是|—|业务事务写入时间|

#### users 注册投影

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`user_id`|text|—|—|—|—|是|—|稳定业务主体，派生 actorKey 和事件 ID；下期启用注册分析|
|`channel`|text|—|—|—|—|是|—|phone 或 email，只传渠道类型|
|`registered_at`|timestamptz|8|—|—|—|是|—|注册事务首次成功时刻|

## 5. ClickHouse 分析表

### events_raw

**阶段与功能** 本期；DW-01、DW-02。

MergeTree，按接收月分区，ORDER BY (received_at,topic,kafka_partition,kafka_offset,event_id)。一行一次仓库投递，允许重复。事件发生日只用于分析，原始回放按源位点固定水位。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`event_id`|`UUID`|16|—|—|—|是|—|稳定事件 ID|
|`event_name`|`LowCardinality(String)`|—|—|—|—|是|—|事件注册代码|
|`schema_version`|`UInt16`|2|—|—|—|是|—|采集契约版本|
|`occurred_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|实际发生时刻|
|`received_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|首次进入接入层时刻|
|`business_date`|`Date`|—|—|—|—|是|—|由发生时间归一的业务日期|
|`source`|`LowCardinality(String)`|—|—|—|—|是|—|app 或 server|
|`producer`|`LowCardinality(String)`|—|—|—|—|是|—|可信生产服务|
|`actor_key`|`String`|—|—|—|—|否|—|分析主体，系统事实可空|
|`anon_key`|`String`|—|—|—|—|否|—|匿名主体假名|
|`identity_type`|`LowCardinality(String)`|—|—|—|—|是|—|anonymous、authenticated 或 system|
|`session_id`|`UUID`|16|—|—|—|否|—|App 事件必填，业务事实可空|
|`page_view_id`|`UUID`|16|—|—|—|否|—|访问实例，指定事件必填|
|`screen_name`|`String`|—|—|—|—|否|—|注册页面名|
|`sequence`|`UInt32`|4|—|—|—|否|—|会话内序号，App 事件必填|
|`platform`|`String`|—|—|—|—|否|—|平台|
|`app_version`|`String`|—|—|—|—|否|—|App 版本|
|`app_language`|`String`|—|—|—|—|否|—|界面语言|
|`region_id`|`UInt32`|4|—|—|—|否|—|地区字典 ID|
|`aggregate_ref`|`String`|—|—|—|—|否|—|业务事件安全分区引用，业务事件必填|
|`policy_version`|`UInt32`|4|—|—|—|否|—|应用策略版本|
|`request_id`|`String`|—|—|—|—|否|—|采集请求关联|
|`ingest_batch_id`|`UUID`|16|—|—|—|否|—|App 批次标识|
|`payload_hash`|`FixedString(64)`|64|—|—|—|是|—|已规范化安全负载的 SHA-256 摘要|
|`properties_json`|`String`|—|—|—|—|是|—|白名单业务属性，详见 §8|
|`topic`|`LowCardinality(String)`|—|—|—|—|是|—|原始 Kafka Topic|
|`kafka_partition`|`UInt32`|4|—|—|—|是|—|原始分区|
|`kafka_offset`|`UInt64`|8|—|—|—|是|—|原始位点|
|`broker_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|Broker 时间，诊断使用|
|`ingested_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|仓库写入时刻|
|`replay_run_id`|`String`|—|—|—|—|否|—|回放作业 ID，禁止改写原始事件身份|

### events_canonical

**阶段与功能** 本期；DW-02、SDK-06、BI-10。

ReplacingMergeTree，按 business_date 的月份分区，ORDER BY (business_date,event_id)，由 event_id 固定分片。逻辑唯一(event_id)，构建时先固定水位并显式去重。保留源坐标供定位；重试不生成新事件 ID。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`event_id`|`UUID`|16|—|—|—|是|—|稳定事件 ID|
|`event_name`|`LowCardinality(String)`|—|—|—|—|是|—|事件注册代码|
|`schema_version`|`UInt16`|2|—|—|—|是|—|采集契约版本|
|`occurred_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|实际发生时刻|
|`received_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|首次进入接入层时刻|
|`business_date`|`Date`|—|—|—|—|是|—|由发生时间归一的业务日期|
|`source`|`LowCardinality(String)`|—|—|—|—|是|—|app 或 server|
|`producer`|`LowCardinality(String)`|—|—|—|—|是|—|可信生产服务|
|`actor_key`|`String`|—|—|—|—|否|—|分析主体，系统事实可空|
|`anon_key`|`String`|—|—|—|—|否|—|匿名主体假名|
|`identity_type`|`LowCardinality(String)`|—|—|—|—|是|—|anonymous、authenticated 或 system|
|`session_id`|`UUID`|16|—|—|—|否|—|App 事件必填，业务事实可空|
|`page_view_id`|`UUID`|16|—|—|—|否|—|访问实例，指定事件必填|
|`screen_name`|`String`|—|—|—|—|否|—|注册页面名|
|`sequence`|`UInt32`|4|—|—|—|否|—|会话内序号，App 事件必填|
|`platform`|`String`|—|—|—|—|否|—|平台|
|`app_version`|`String`|—|—|—|—|否|—|App 版本|
|`app_language`|`String`|—|—|—|—|否|—|界面语言|
|`region_id`|`UInt32`|4|—|—|—|否|—|地区字典 ID|
|`aggregate_ref`|`String`|—|—|—|—|否|—|业务事件安全分区引用，业务事件必填|
|`policy_version`|`UInt32`|4|—|—|—|否|—|应用策略版本|
|`request_id`|`String`|—|—|—|—|否|—|采集请求关联|
|`ingest_batch_id`|`UUID`|16|—|—|—|否|—|App 批次标识|
|`payload_hash`|`FixedString(64)`|64|—|—|—|是|—|已规范化安全负载的 SHA-256 摘要|
|`properties_json`|`String`|—|—|—|—|是|—|白名单业务属性，详见 §8|
|`topic`|`LowCardinality(String)`|—|—|—|—|是|—|选中记录的 Kafka Topic|
|`kafka_partition`|`UInt32`|4|—|—|—|是|—|选中记录的原始分区|
|`kafka_offset`|`UInt64`|8|—|—|—|是|—|选中记录的原始位点|

### fact_page_visit

**阶段与功能** 本期；SDK-04、BI-02。

按业务日分区管理，逻辑键 (business_date,generation,page_view_id)；结算缺失时照计 PV，时长为 NULL。入口维度来自初次进入事件。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`page_view_id`|`UUID`|16|—|逻辑 → events_canonical.page_view_id|—|是|—|一次页面访问 ID|
|`session_id`|`UUID`|16|—|逻辑 → events_canonical.session_id|—|是|—|所属 App 会话|
|`actor_key`|`String`|—|—|—|—|是|—|访问者的安全分析键|
|`screen_name`|`LowCardinality(String)`|—|—|—|—|是|—|叶子路由|
|`entry_source`|`LowCardinality(String)`|—|—|—|—|是|—|进入来源|
|`entry_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|进入时间|
|`exit_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|结算时间|
|`visible_ms`|`UInt64`|8|—|—|—|否|—|累计前台可见时长|
|`exit_reason`|`String`|—|—|—|—|否|—|页面离开原因|
|`closure_status`|`LowCardinality(String)`|—|—|—|—|是|—|open、closed、unknown|
|`is_session_exit`|`Bool`|1|—|—|—|否|—|成熟会话中的最后访问；未成熟保持 NULL|
|`platform`|`LowCardinality(String)`|—|—|—|—|是|`unknown`|入口平台|
|`app_version`|`String`|—|—|—|—|是|`unknown`|入口版本|
|`region_id`|`UInt32`|4|—|—|—|是|`0`|归一地区，0 为 unknown|

### fact_actor_active_day

**阶段与功能** 本期；DW-03、BI-01、BI-06。

逻辑键 (business_date,generation,actor_key,platform,app_version,region_id)。按窗口查询时对 actor_key 重新去重。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`actor_key`|`String`|—|—|—|—|是|—|稳定分析主体|
|`identity_type`|`LowCardinality(String)`|—|—|—|—|是|—|anonymous 或 authenticated|
|`platform`|`LowCardinality(String)`|—|—|—|—|是|`unknown`|当日活动平台|
|`app_version`|`String`|—|—|—|—|是|`unknown`|当日活动版本|
|`region_id`|`UInt32`|4|—|—|—|是|`0`|当日活动地区，0 为 unknown|
|`first_activity_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|该组合当天首个合格活动|

### dim_actor_first_active

**阶段与功能** 本期；DW-03、BI-06。

按 generation 管理，同一版本逻辑唯一 actor_key。actor_key 清理时重建；历史截断主体只进入 unknown cohort。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|首活构建代际|
|`actor_key`|`String`|—|—|—|—|是|—|认证主体假名|
|`first_business_date`|`Date`|—|—|—|—|否|—|首活日，截断时可空|
|`first_event_id`|`UUID`|16|—|—|—|否|—|可验证首活事件|
|`platform`|`LowCardinality(String)`|—|—|—|—|是|`unknown`|首活平台|
|`app_version`|`String`|—|—|—|—|是|`unknown`|首活版本|
|`region_id`|`UInt32`|4|—|—|—|是|`0`|首活地区|
|`history_truncated`|`Bool`|1|—|—|—|是|`false`|是否缺少可证明的早期历史|

### dim_entity_safe

**阶段与功能** 本期；DW-03。

逻辑键 (target_type,target_id,effective_at)。消费前校验来源，按发生时刻匹配生效版本。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`target_type`|`LowCardinality(String)`|—|—|—|—|是|—|company 或 product|
|`target_id`|`String`|—|—|—|—|是|—|业务实体稳定 ID|
|`effective_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|版本生效时间|
|`valid_until`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|空表示当前有效|
|`region_id`|`UInt32`|4|—|—|—|是|`0`|未解析为 0|
|`category_code`|`String`|—|—|—|—|否|—|可公开业务类别|
|`industry_code`|`String`|—|—|—|—|否|—|可公开行业分类|

### fact_search

**阶段与功能** 本期；BIZ-01、BI-03、BI-04。

逻辑键 (business_date,generation,actor_key,search_id)。从同一原始事件水位构建，按同主体、同搜索 ID、同目标和 15 分钟窗口关联。写入 `fact_search`，快照数据集名称为 `searches`。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`actor_key`|`String`|—|—|—|—|是|—|主体分析键|
|`search_id`|`UUID`|16|—|—|—|是|—|逻辑搜索 ID|
|`scope`|`LowCardinality(String)`|—|—|—|—|是|—|搜索范围|
|`submitted_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|发起时间|
|`first_result_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|首屏终态|
|`outcome`|`String`|—|—|—|—|否|—|success 或 failed；未结束为空|
|`result_count`|`UInt32`|4|—|—|—|否|—|成功的首屏条数|
|`error_code`|`String`|—|—|—|—|否|—|安全固定错误码|
|`latency_ms`|`UInt64`|8|—|—|—|否|—|首屏耗时|
|`first_open_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|有效打开时间|
|`detail_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|匹配目标的详情曝光时间|
|`matched_target_type`|`String`|—|—|—|—|否|—|匹配企业/产品类型|
|`matched_target_id`|`String`|—|—|—|—|否|—|匹配目标 ID|
|`platform`|`LowCardinality(String)`|—|—|—|—|是|unknown|搜索发起时的平台|
|`app_version`|`String`|—|—|—|—|是|unknown|搜索发起时的 App 版本|
|`region_id`|`UInt32`|4|—|—|—|是|0|搜索发起时的地区|

### fact_inquiry_flow

**阶段与功能** 本期；BIZ-04、BI-04。

逻辑键 (business_date,generation,actor_key,contact_flow_id)。详情曝光分母可直接从规范事件计算；联系键只在联系动作产生，不能用本表反推所有详情曝光。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`actor_key`|`String`|—|—|—|—|是|—|已认证主体|
|`page_view_id`|`UUID`|16|—|逻辑 → fact_page_visit.page_view_id|—|是|—|归因详情访问|
|`contact_flow_id`|`UUID`|16|—|—|—|是|—|联系链路键|
|`company_id`|`String`|—|—|—|—|是|—|目标企业|
|`product_id`|`String`|—|—|—|—|否|—|可用时存商品 ID|
|`detail_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|该链路详情锚点|
|`contact_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|联系动作|
|`client_request_id`|`String`|—|—|—|—|否|—|匹配提交请求|
|`submit_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|实际提交时间|
|`inquiry_ref`|`String`|—|—|逻辑 → fact_inquiry_created.inquiry_ref|—|否|—|匹配权威询盘|
|`created_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|PG 提交时间|
|`attribution_status`|`LowCardinality(String)`|—|—|—|—|是|—|关联成功或对应失败状态|

### fact_inquiry_created

**阶段与功能** 本期；BI-01、DW-06。

逻辑键 (business_date,generation,inquiry_ref)。业务日期按 PG 提交时间，保留无归因事实。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`inquiry_ref`|`String`|—|—|—|—|是|—|权威询盘安全引用|
|`actor_key`|`String`|—|—|—|—|是|—|买家分析假名|
|`submitted_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|PG 权威提交时刻|
|`company_id`|`String`|—|—|—|—|是|—|目标企业|
|`product_id`|`String`|—|—|—|—|否|—|目标产品|
|`contact_flow_id`|`UUID`|16|—|—|—|否|—|可以匹配时回填|
|`attribution_reason`|`String`|—|—|—|—|否|—|未归因原因|

### fact_dialog_reply

**阶段与功能** 本期；BIZ-05、BI-05。

逻辑键 (business_date,generation,dialog_ref,anchor_message_ref)。仅普通人工消息作为锚点，同一对话匹配对侧人工回复。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`dialog_ref`|`String`|—|—|—|—|是|—|安全会话引用|
|`anchor_message_ref`|`String`|—|—|—|—|是|—|该轮首条人工消息引用|
|`sender_actor_key`|`String`|—|—|—|—|是|—|首发人工主体假名|
|`anchor_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|锚点时刻|
|`reply_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|对侧首次人工回复|
|`latency_ms`|`UInt64`|8|—|—|—|否|—|回复时差|
|`mature`|`Bool`|1|—|—|—|是|`false`|达到 24 小时观察窗口及水位|

### fact_translate_task

**阶段与功能** 本期；BIZ-06、BI-05。

逻辑键 (business_date,generation,actor_key,task_id)。状态取首个有效终态，同步状态从独立服务端事件计算。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`actor_key`|`String`|—|—|—|—|是|—|分析主体|
|`task_id`|`UUID`|16|—|—|—|是|—|一次任务执行键|
|`mode`|`LowCardinality(String)`|—|—|—|—|是|—|text、dialog、image、file|
|`source_lang`|`LowCardinality(String)`|—|—|—|—|是|`und`|源语言|
|`target_lang`|`LowCardinality(String)`|—|—|—|—|是|`und`|目标语言|
|`started_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|执行开始|
|`terminal_at`|`DateTime64(3, 'UTC')`|8|—|—|—|否|—|首个有效终态时刻|
|`outcome`|`String`|—|—|—|—|否|—|success、failed、cancelled|
|`latency_ms`|`UInt64`|8|—|—|—|否|—|执行耗时|
|`error_code`|`String`|—|—|—|—|否|—|失败时固定错误码|
|`platform`|`LowCardinality(String)`|—|—|—|是|unknown|任务开始时的平台|
|`app_version`|`String`|—|—|—|是|unknown|任务开始时的版本|
|`region_id`|`UInt32`|4|—|—|—|是|0|任务开始时的地区|

### agg_metric_day

**阶段与功能** 本期；DW-04、BI-01–05，BI-11、13 复用。

逻辑键 (business_date,generation,metric_code,metric_version,dim_set,dim_values)。按固定顺序和字典生成维度键。日指标数据集在快照中注册为 dailyMetrics。已有全域日活、匿名 UV 和翻译成功率分子分母。跨日 UV 和 P95 在事实或兼容查询层重算，各日比率按分子和分母合并。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`metric_code`|`LowCardinality(String)`|—|—|—|—|是|—|指标代码|
|`metric_version`|`UInt32`|4|—|—|—|是|—|统计口径版本|
|`dim_set`|`LowCardinality(String)`|—|—|—|—|是|—|白名单维度组合编号|
|`dim_values`|`String`|—|—|—|—|是|—|按固定顺序编码的维度值|
|`value_count`|`UInt64`|8|—|—|—|否|—|可加数量指标|
|`numerator`|`UInt64`|8|—|—|—|否|—|比率分子|
|`denominator`|`UInt64`|8|—|—|—|否|—|比率分母|
|`provisional`|`Bool`|1|—|—|—|是|`false`|临时统计状态|

### agg_retention_day

**阶段与功能** 本期；BI-06。

逻辑键 (cohort_date,generation,offset_day,cohort_dim)，按 cohort 月分区。保留首活维度，不按回访当日维度重分组。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|发布代际|
|`cohort_date`|`Date`|—|—|—|—|是|—|首活自然日|
|`offset_day`|`UInt8`|1|—|—|—|是|—|1、7、30|
|`cohort_dim`|`String`|—|—|—|—|是|—|首活维度组合|
|`cohort_size`|`UInt64`|8|—|—|—|是|—|首活用户数量|
|`retained_count`|`UInt64`|8|—|—|—|否|—|未成熟时为空|
|`mature`|`Bool`|1|—|—|—|是|`false`|区间和观察水位是否完整|
|`expected_mature_date`|`Date`|—|—|—|—|是|—|最早成熟业务日|

### agg_quality_minute

**阶段与功能** 本期；BI-07、DW-06，ADM-05 复用。

逻辑键 (bucket_at,generation,service,source,app_version,platform)。接收、拒绝和重复为桶计数；Kafka lag 为采样值，跨桶展示按最新或最大值。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|快照代际|
|`bucket_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|UTC 分钟桶起点|
|`service`|`LowCardinality(String)`|—|—|—|—|是|—|组件名|
|`source`|`LowCardinality(String)`|—|—|—|—|是|`unknown`|app、server 或 unknown|
|`app_version`|`String`|—|—|—|—|是|`unknown`|上报版本|
|`platform`|`LowCardinality(String)`|—|—|—|—|是|`unknown`|平台|
|`accepted`|`UInt64`|8|—|—|—|是|`0`|确认接收数|
|`rejected`|`UInt64`|8|—|—|—|是|`0`|拒绝数|
|`rate_limited`|`UInt64`|8|—|—|—|是|`0`|限流数|
|`duplicate`|`UInt64`|8|—|—|—|是|`0`|幂等重复数|
|`late`|`UInt64`|8|—|—|—|是|`0`|迟到数|
|`dlq`|`UInt64`|8|—|—|—|是|`0`|进入死信数|
|`lag_records`|`UInt64`|8|—|—|—|否|—|分区积压快照|
|`p95_ingest_delay_ms`|`UInt64`|8|—|—|—|否|—|入仓延迟 P95|

### fact_reconciliation_day

**阶段与功能** 本期；DW-06、BI-07，ADM-05 复用。

逻辑键 (business_date,generation,fact_code,source)。源与仓库使用相同观察范围和水位。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`fact_code`|`LowCardinality(String)`|—|—|—|—|是|—|询盘、业务浏览或发送等事实代码|
|`source`|`LowCardinality(String)`|—|—|—|—|是|—|PG 或 MongoDB 权威源|
|`source_count`|`UInt64`|8|—|—|—|是|—|权威源数量|
|`warehouse_count`|`UInt64`|8|—|—|—|是|—|仓库数量|
|`delta`|`Int64`|8|—|—|—|是|—|warehouse_count - source_count|
|`unattributed_count`|`UInt64`|8|—|—|—|否|—|仅适用有归因口径的事实|
|`source_watermark`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|共同观测水位|

### fact_card_impression

**阶段与功能** 下期；BI-13。

从卡片事件生成快照事实。逻辑键为 (business_date,generation,actor_key,impression_id)。达到可见标准才记一条，重复点击时同曝光仅计一个已点击曝光。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`generation`|`UInt64`|8|—|—|—|是|—|统计发布代际|
|`business_date`|`Date`|—|—|—|—|是|—|由该事实锚点归一的业务日|
|`impression_id`|`UUID`|16|—|—|—|是|—|稳定曝光实例|
|`actor_key`|`String`|—|—|—|—|是|—|分析主体|
|`page_view_id`|`UUID`|16|—|逻辑 → fact_page_visit.page_view_id|—|是|—|页面访问|
|`list_request_id`|`UUID`|16|—|—|—|是|—|本次列表数据批次|
|`placement_code`|`LowCardinality(String)`|—|—|—|—|是|—|首页或搜索等曝光位置|
|`target_type`|`LowCardinality(String)`|—|—|—|—|是|—|实体类型|
|`target_id`|`String`|—|—|—|—|是|—|实体 ID|
|`position`|`UInt32`|4|—|—|—|是|—|列表中的位置|
|`visible_ms`|`UInt64`|8|—|—|—|是|—|可见时长|
|`exposed_at`|`DateTime64(3, 'UTC')`|8|—|—|—|是|—|满足可见阈值的时刻|
|`first_click_at`|`Nullable(DateTime64(3, 'UTC'))`|8|—|—|—|否|—|能与该曝光关联的首个点击|
|`platform`|`LowCardinality(String)`|—|—|—|—|是|unknown|曝光时的平台|
|`app_version`|`String`|—|—|—|—|是|unknown|曝光时的 App 版本|
|`region_id`|`UInt32`|4|—|—|—|是|0|曝光时的地区和查询授权维度|

## 6. MongoDB 业务集合与语料

### 6.1 既有集合的分析投影视图

以下对象是数据契约投影，由业务适配器从既有集合读取并转换。它们不要求创建同名 MongoDB 集合或改造业务表。旧字段名和 BSON 类型以联调环境为准；下列类型适用于规范化投影。正文不供分析适配器读取。

#### messages 分析投影

**阶段与功能** 本期；BIZ-05、DW-06。

权威键为业务 messageId；逻辑关联 dialogId。分析侧生成独立假名后发 Kafka，不转发原值。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`messageId`|`string`|—|—|—|—|是|—|原业务消息稳定 ID，写分析前转 messageRef|
|`dialogId`|`string`|—|—|—|—|是|—|会话标识，写分析前转 dialogRef|
|`senderId`|`string`|—|—|—|—|否|—|人工发送主体由服务转为 actorKey；系统消息可空|
|`messageType`|`string`|—|—|—|—|是|—|映射为事件字典允许的类型|
|`senderKind`|`string`|—|—|—|—|是|—|human、system、inquiryCard，由可信来源归类|
|`createdAt`|`BSON Date`|8|—|—|—|是|—|首次持久化时间|

#### 公司与产品浏览明细分析投影

**阶段与功能** 本期；BIZ-02、DW-06。

来源为 company_view_raw。按稳定浏览操作键生成 viewRef，重试不新增事实。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`viewRef`|`string`|—|—|—|—|是|—|浏览业务记录唯一引用|
|`targetType`|`string`|—|—|—|—|是|—|company 或 product|
|`targetId`|`string`|—|—|—|—|是|—|业务实体 ID|
|`createdAt`|`BSON Date`|8|—|—|—|是|—|业务浏览发生时间|

#### 翻译记录同步分析投影

**阶段与功能** 本期；BIZ-06。

覆盖四类既有同步集合；由原始记录键和可持久状态版本生成事件，不传原文、译文和附件。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`recordRef`|`string`|—|—|—|—|是|—|稳定安全记录引用|
|`mode`|`string`|—|—|—|—|是|—|text、dialog、image、file|
|`outcome`|`string`|—|—|—|—|是|—|success 或可持久追踪的 failed|
|`stateVersion`|`BSON Long`|8|—|—|—|是|—|状态版本；重复写同状态不递增|
|`occurredAt`|`BSON Date`|8|—|—|—|是|—|权威同步状态发生时间|

### 6.2 实体集合

#### analytics_mongo_outbox

**阶段与功能** 本期；ING-05、BIZ-05、BIZ-06。

_id 可直接使用 UUID。创建候选业务事实与出站文档同事务；索引(status,availableAt)，按租约条件确认。payloadSafe 只含事件白名单字段。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`_id`|`UUID`|16|是|—|是|是|—|稳定业务 eventId|
|`topic`|`string`|—|—|—|—|是|—|目标 Topic|
|`aggregateRef`|`string`|—|—|—|—|是|—|安全对象引用|
|`payloadSafe`|`document`|—|—|—|—|是|—|安全事件信封|
|`status`|`string`|—|—|—|—|是|`pending`|pending、published、failed|
|`attempts`|`BSON Int32`|4|—|—|—|是|`0`|投递次数|
|`availableAt`|`BSON Date`|8|—|—|—|是|—|下一次可领取时间|
|`leaseToken`|`UUID`|16|—|—|—|否|—|租约令牌|
|`leaseUntil`|`BSON Date`|8|—|—|—|否|—|租约到期|
|`createdAt`|`BSON Date`|8|—|—|—|是|—|生成事实时间|
|`publishedAt`|`BSON Date`|8|—|—|—|否|—|持久确认时间|

#### corpus_datasets

**阶段与功能** 下期；GOV-04、GOV-05、ADM-04。

UQ(datasetCode,revision)。修改用途生成新修订；到期或撤回通过状态受控传播。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`_id`|`ObjectId`|12|是|—|是|是|—|数据集修订主键|
|`datasetCode`|`string`|—|—|—|组合|是|—|数据集代码|
|`revision`|`BSON Int32`|4|—|—|组合|是|—|数据集修订号|
|`purposeCode`|`string`|—|—|—|—|是|—|批准的用途类型|
|`allowedLanguages`|`array<string>`|—|—|—|—|是|—|注册允许语言集合|
|`policyRef`|`string`|—|—|—|—|是|—|处理依据或授权记录引用|
|`retentionDays`|`BSON Int32`|4|—|—|—|是|—|此用途的保留期限|
|`status`|`string`|—|—|—|—|是|`draft`|draft、active、retired|
|`createdAt`|`BSON Date`|8|—|—|—|是|—|创建时间|

#### corpus_items

**阶段与功能** 下期；GOV-04、GOV-05、ADM-04。

UQ(datasetId,sourceRef,revision)，索引(datasetId,reviewStatus,createdAt)、(sourceRef,tombstoneAt)。一数据集修订对应多条记录；来源撤回按 sourceRef 跨数据集查找。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`_id`|`ObjectId`|12|是|—|是|是|—|条目主键|
|`datasetId`|`ObjectId`|12|—|逻辑 → corpus_datasets._id|组合|是|—|所属数据集修订|
|`sourceRef`|`string`|—|—|—|组合|是|—|可信业务来源安全引用|
|`revision`|`BSON Int32`|4|—|—|组合|是|—|条目修订号|
|`sourceLang`|`string`|—|—|—|—|是|—|源语言|
|`targetLang`|`string`|—|—|—|—|是|—|目标语言|
|`text`|`string`|—|—|—|—|否|—|受控脱敏正文，与文件引用至少有一项|
|`fileRef`|`string`|—|—|—|—|否|—|受控文件引用|
|`reviewStatus`|`string`|—|—|—|—|是|`pending`|pending、approved、rejected、removed|
|`policyRef`|`string`|—|—|—|—|是|—|条目用途依据或授权引用|
|`createdAt`|`BSON Date`|8|—|—|—|是|—|创建时间|
|`expiresAt`|`BSON Date`|8|—|—|—|是|—|治理到期点，清理作业先传播再处理内容|
|`tombstoneAt`|`BSON Date`|8|—|—|—|否|—|撤回或删除生效点|

#### corpus_import_inputs

授权的导入输入登记，维护端创建，请求只携带引用。所属数据集、用途和批准工单均在导入时校验。

| 字段 | 类型 | 长度 | 主键 | 外键 | 唯一 | 非空 | 默认值 | 说明 |
|---|---|---|---|---|---|---|---|---|
| `_id` | string | — | 是 | — | 是 | 是 | — | 受控导入引用 |
| `datasetId` | ObjectId | 12 | 否 | 逻辑 → corpus_datasets._id | 否 | 是 | — | 目标数据集及版本 |
| `purposeCode` | string | — | 否 | — | 否 | 是 | — | 与数据集一致的授权用途 |
| `items` | array | ≤1000 项 | 否 | — | 否 | 是 | — | 受限正文及来源元数据 |
| `createdAt` | date | — | 否 | — | 否 | 是 | — | 登记时间 |

#### corpus_jobs

**阶段与功能** 下期；GOV-04、ADM-04。

索引(datasetId,createdAt)。作业创建时校验数据集级权限，任务只处理其绑定修订。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`_id`|`string`|—|是|—|是|是|—|对外 jobRef|
|`datasetId`|`ObjectId`|12|—|逻辑 → corpus_datasets._id|—|是|—|所属数据集修订|
|`type`|`string`|—|—|—|—|是|—|import、reviewSync、erase|
|`status`|`string`|—|—|—|—|是|`queued`|queued、running、succeeded、failed|
|`scannedCount`|`BSON Long`|8|—|—|—|是|`0`|扫描条数|
|`acceptedCount`|`BSON Long`|8|—|—|—|是|`0`|成功条数|
|`rejectedCount`|`BSON Long`|8|—|—|—|是|`0`|拒绝条数|
|`createdAt`|`BSON Date`|8|—|—|—|是|—|作业创建时间|
|`finishedAt`|`BSON Date`|8|—|—|—|否|—|作业完成时间|
|`errorCode`|`string`|—|—|—|—|否|—|固定失败码，不保存异常原文|

## 7. 端侧 SQLite 与 Redis

### sdk_event_queue

**阶段与功能** 本期；SDK-02、SDK-05。

SQLite PK(event_id)，索引(identity_epoch,queue_state,next_retry_ms)。保存白名单事件，不储存登录 token。重装创建新安装 ID；到期按现有 7 天窗口处理。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`event_id`|`TEXT`|—|是|—|是|是|—|UUID 文本|
|`install_id`|`TEXT`|—|—|—|—|是|—|随机安装 ID|
|`identity_epoch`|`TEXT`|—|—|—|—|是|—|队列所属身份阶段|
|`identity_type`|`TEXT`|—|—|—|—|是|—|anonymous 或 authenticated|
|`session_id`|`TEXT`|—|—|—|—|是|—|App 会话 UUID|
|`payload_json`|`TEXT`|—|—|—|—|是|—|待上报事件的白名单 JSON|
|`occurred_at_ms`|`INTEGER`|—|—|—|—|是|—|发生时间毫秒|
|`queue_state`|`TEXT`|—|—|—|—|是|`pending`|pending 或领取中状态|
|`attempts`|`INTEGER`|—|—|—|—|是|`0`|尝试次数|
|`next_retry_ms`|`INTEGER`|—|—|—|—|是|—|下次可发时间|
|`expires_at_ms`|`INTEGER`|—|—|—|—|是|—|队列清理截止|

### Redis 令牌桶哈希

**阶段与功能** 本期；ING-02。

键形态 analytics:rl:环境:入口:维度:安全标识。以原子脚本读取、补充、扣减和设置 TTL，多维度分别核验，键按业务与统计资源隔离。以下约束由脚本执行。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`redis_key`|`string`|—|是|—|是|是|—|按入口、IP 假名、安装、主体或查询操作者区分|
|`tokens`|`int64`|8|—|—|—|是|—|按固定精度整数保存剩余配额|
|`last_refill_ms`|`int64`|8|—|—|—|是|—|上次补充时间|
|`ttl_ms`|`int64`|8|—|—|—|是|—|键 TTL，至少覆盖两倍补满时间；TTL 为键元数据|

## 8. 事件属性数据字典

本节是 `events_raw.properties_json` 和 `events_canonical.properties_json` 内的注册字段，同时适用于 Kafka `properties`。App 发送时位于 `events[].properties`。属性是 JSON 成员，主键、外键及唯一键由所在事件或派生表维护。标为条件必填的字段在相应分支由事件模式校验。

### EVT_01 app_session_start

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`launchSource`|string|—|—|—|—|是|—|coldStart / foreground / deepLink / push / accountSwitch / unknown；取本次会话的建立原因|

### EVT_02 app_background

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`properties`|object|—|—|—|—|是|—|固定空对象 {}，无专用字段。|

### EVT_03 screen_view

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`entrySource`|string|—|—|—|—|是|—|tab / home / search / detail / message / push / deepLink / unknown|

### EVT_04 screen_exit

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`visibleMs`|integer（JSON）|—|—|—|—|是|—|0–28,800,000 毫秒，使用单调时钟累计前台可见时长|
|`exitReason`|string|—|—|—|—|是|—|navigate / sessionTimeout / accountSwitch / appClose|

### EVT_05 search_submit

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`searchId`|string（UUID）|36|—|—|—|是|—|每次新逻辑搜索由 App 生成|
|`scope`|string|—|—|—|—|是|—|all / company / product / user|
|`queryLengthBucket`|string|—|—|—|—|是|—|0 / 1-3 / 4-10 / 11-30 / 31+；端侧按去除首尾空白后的字符数分桶，不上传搜索词|
|`filterCount`|integer（JSON）|—|—|—|—|是|—|0–50，已生效的筛选项数，不上传筛选原文|

### EVT_06 search_result

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`searchId`|string（UUID）|36|—|—|—|是|—|沿用搜索提交 ID|
|`scope`|string|—|—|—|—|是|—|all / company / product / user，与提交一致|
|`outcome`|string|—|—|—|—|是|—|success / failed|
|`resultCount`|integer（JSON）|—|—|—|—|条件|—|成功时必填。0–1,000,000；当前搜索范围的首屏结果条数，失败时省略|
|`latencyMs`|integer（JSON）|—|—|—|—|是|—|请求发出到首屏处理结束的毫秒数，0–86,400,000|
|`errorCode`|string|—|—|—|—|条件|—|失败时必填。固定错误字典，成功时省略|

### EVT_07 search_result_open

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`searchId`|string（UUID）|36|—|—|—|是|—|结果所属搜索 ID|
|`targetType`|string|—|—|—|—|是|—|company / product / user|
|`targetId`|string|—|—|—|—|是|—|结果实体业务 ID；user ID 由网关假名化后进入 Kafka|
|`position`|integer（JSON）|—|—|—|—|是|—|卡片序号，从 1 开始，计入分页偏移|

### EVT_08 entity_detail_view

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`targetType`|string|—|—|—|—|是|—|company / product|
|`targetId`|string|—|—|—|—|是|—|企业用 companyId，产品用 productId|
|`companyId`|string|—|—|—|—|是|—|当前企业或商品归属企业 ID|
|`productId`|string|—|—|—|—|条件|—|产品时必填。当前产品 ID，企业详情省略|
|`searchId`|string（UUID）|36|—|—|—|否|—|从搜索结果进入且有有效链路时传|
|`entrySource`|string|—|—|—|—|是|—|tab / home / search / detail / message / push / deepLink / unknown|

### EVT_09 business_view_recorded

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`viewRef`|string|—|—|—|—|是|—|业务浏览记录稳定引用|
|`targetType`|string|—|—|—|—|是|—|company / product，按业务类别映射|
|`targetId`|string|—|—|—|—|是|—|浏览主体 ID|

### EVT_10 favorite_changed

下期启用，本期可登记停用模式。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`targetType`|string|—|—|—|—|是|—|company / product；取业务接口的 targetType|
|`targetId`|string|—|—|—|—|是|—|被收藏或取消的实体 ID|
|`action`|string|—|—|—|—|是|—|add / remove|

### EVT_11 contact_supplier_click

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`contactFlowId`|string（UUID）|36|—|—|—|是|—|点击时生成，并传给询盘表单|
|`companyId`|string|—|—|—|—|是|—|被联系企业 ID|
|`productId`|string|—|—|—|—|否|—|从商品相关入口进入且已选商品时传|

### EVT_12 inquiry_submit

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`contactFlowId`|string（UUID）|36|—|—|—|否|—|沿用联系入口，未建立归因链路时省略|
|`clientRequestId`|string|—|—|—|—|是|—|稳定的业务幂等键，和业务请求一致|
|`companyId`|string|—|—|—|—|是|—|询盘目标企业，取业务 targetCompanyId|
|`productId`|string|—|—|—|—|否|—|业务请求有产品 ID 时传|

### EVT_13 inquiry_submit_failed

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`clientRequestId`|string|—|—|—|—|是|—|沿用提交幂等键|
|`attemptNo`|integer（JSON）|—|—|—|—|是|—|从 1 开始的实际网络尝试序号|
|`errorCode`|string|—|—|—|—|是|—|固定错误字典，禁止错误原文|

### EVT_14 inquiry_created

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`inquiryRef`|string|—|—|—|—|是|—|已提交询盘稳定引用|
|`companyId`|string|—|—|—|—|是|—|PG 询盘目标企业 ID|
|`productId`|string|—|—|—|—|否|—|PG 询盘有关联产品时传|
|`contactFlowId`|string（UUID）|36|—|—|—|否|—|仅从已验证的业务关联读取|
|`clientRequestId`|string|—|—|—|—|否|—|已有业务请求幂等键时补充|

### EVT_15 chat_session_open

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`dialogRef`|string|—|—|—|—|是|—|App 传真实业务会话 ID，Collector 映射为 Kafka 安全假名|
|`entrySource`|string|—|—|—|—|是|—|tab / home / search / detail / message / push / deepLink / unknown|

### EVT_16 message_send_succeeded

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`dialogRef`|string|—|—|—|—|是|—|会话安全引用，与 chat_session_open 入仓引用一致|
|`messageRef`|string|—|—|—|—|是|—|已持久消息的安全引用|
|`messageType`|string|—|—|—|—|是|—|text / image / file / audio / card / other|
|`senderKind`|string|—|—|—|—|是|—|human / system / inquiryCard；回复指标仅使用 human|

### EVT_17 notification_open

下期启用，本期可登记停用模式。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`category`|string|—|—|—|—|是|—|comments / likes / follows / inquiries / system|

### EVT_18 message_translation_toggle

下期启用，本期可登记停用模式。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`displayMode`|string|—|—|—|—|是|—|original / translated，记录切换后的状态|

### EVT_19 translate_started

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`taskId`|string（UUID）|36|—|—|—|是|—|本次执行任务 ID|
|`mode`|string|—|—|—|—|是|—|text / dialog / image / file|
|`sourceLang`|string|—|—|—|—|是|—|业务字典中的源语言，未知 und|
|`targetLang`|string|—|—|—|—|是|—|业务字典中的目标语言，未知 und|

### EVT_20 translate_completed

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`taskId`|string（UUID）|36|—|—|—|是|—|沿用任务开始 ID|
|`mode`|string|—|—|—|—|是|—|text / dialog / image / file，与开始一致|
|`sourceLang`|string|—|—|—|—|是|—|任务的源语言；自动识别可填识别结果|
|`targetLang`|string|—|—|—|—|是|—|任务的目标语言|
|`outcome`|string|—|—|—|—|是|—|success / failed / cancelled|
|`latencyMs`|integer（JSON）|—|—|—|—|是|—|任务运行时长，0–86,400,000 毫秒|
|`errorCode`|string|—|—|—|—|条件|—|失败时必填。固定错误字典，成功或取消时省略|

### EVT_21 translate_sync_completed

本期启用。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`recordRef`|string|—|—|—|—|是|—|同步记录的稳定安全引用|
|`mode`|string|—|—|—|—|是|—|text / dialog / image / file，对应业务记录类型|
|`outcome`|string|—|—|—|—|是|—|success / failed；表达同步结果|

### EVT_22 translate_copy

下期启用，本期可登记停用模式。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`mode`|string|—|—|—|—|是|—|text / dialog / image / file|

### EVT_23 account_registered

下期启用，本期可登记停用模式。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`channel`|string|—|—|—|—|是|—|phone / email，取实际注册渠道|

### EVT_24 corpus_asset_status_changed

下期启用，本期可登记停用模式。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`datasetCode`|string|—|—|—|—|是|—|已注册的数据集代码|
|`languagePair`|string|—|—|—|—|是|—|源语言:目标语言，使用业务语言字典|
|`status`|string|—|—|—|—|是|—|pending / approved / rejected / removed|
|`reasonCode`|string|—|—|—|—|否|—|治理作业注册的固定原因码；禁止审核意见原文|


### EVT_25 card_impression

App 首次达到连续可见阈值时上报。页面事件信封中的 `screenName` 和 `pageViewId` 必填。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`impressionId`|UUID|16|—|逻辑 → fact_card_impression.impression_id|—|是|—|曝光实例，点击沿用曝光时的 ID|
|`listRequestId`|UUID|16|—|—|—|是|—|列表请求批次|
|`placementCode`|string|—|—|—|—|是|—|homeRecommendation 或 searchResults|
|`targetType`|string|—|—|—|—|是|—|company 或 product|
|`targetId`|string|80|—|—|—|是|—|公司或产品稳定业务 ID|
|`position`|int|32 位|—|—|—|是|—|从 1 开始，上限 2147483647|
|`visibleMs`|int|32 位|—|—|—|是|—|连续可见时长，范围为 1000–28800000 毫秒|

### EVT_26 card_impression_click

点击已记录的有效曝光时上报。各字段沿用对应曝光值，页面事件信封中的 `screenName` 和 `pageViewId` 必填。

|字段|类型|长度|主键|外键|唯一|非空|默认值|说明|
|---|---|---|---|---|---|---|---|---|
|`impressionId`|UUID|16|—|逻辑 → fact_card_impression.impression_id|—|是|—|曝光实例，点击沿用曝光时的 ID|
|`listRequestId`|UUID|16|—|—|—|是|—|列表请求批次|
|`placementCode`|string|—|—|—|—|是|—|homeRecommendation 或 searchResults|
|`targetType`|string|—|—|—|—|是|—|company 或 product|
|`targetId`|string|80|—|—|—|是|—|公司或产品稳定业务 ID|
|`position`|int|32 位|—|—|—|是|—|从 1 开始，上限 2147483647|

## 9. 实施顺序与验收

1. 本期先建 PG 事件注册、策略、受理、角色、发布与修复表，以及快照构建关联表、MongoDB Outbox 和 SQLite 队列。冻结清单指定的下期事件可预存模式定义，配置保持停用。Kafka 事件输入、DLQ、Redis 限流和 JSON Lines `.log` 作为存储外围接口。日志按既有设计使用 ts、level、service、requestId、traceId、action、resultCode 等安全字段，按日或容量轮转。
2. 建原始和规范事件层，再按快照构建各事实、聚合及对账表。验证相同 eventId 的重复投递、失败回放、固定水位和原子快照；超出观察期或未闭合的统计使用空值与状态。业务库源表不由分析服务写入。
3. 按各表标注的下期对象和冻结功能清单的字段预留启动下期。通知点击、译文深度、收藏、注册与受控会话沿用现有注册、事件表及权限表。导出、语料和告警按表上标识接入。卡片事实随页面事实构建并发布。采集策略管理、修复后台和审计检索复用本期控制表。
4. 迁移验收包括无父记录的物理 FK 写入拒绝、重复关联键拒绝、撤回时跨库逻辑引用传播、SQLite 切号隔离、同一 PostgreSQL 查询事务读取同一快照关联集合。真实部署前验证既有业务 ID/BSON 类型、ClickHouse 实际版本、保留期和索引负载。

