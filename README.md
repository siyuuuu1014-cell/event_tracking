# ipaklink Event Tracking

独立的埋点后台工程。ipaklink App 通过版本化采集接口接入。其业务服务和业务数据由原项目维护。

## 当前实现

- Python 3.12 + FastAPI，应用层、领域层、端口与存储适配器独立。
- `GET /health/live`、`GET /health/ready`、`GET /ipaklink/v1/analytics/config`。
- PostgreSQL 控制面与快照关系迁移，ClickHouse 事件原始层，Redis、Kafka 与 MongoDB 副本集的本地环境。
- ANL_01 读取实际 PostgreSQL 配置。开发种子策略关闭采集。登录态鉴权、匿名签发、事件批量写入、消费进程与报表尚待接入。带 Authorization 的 ANL_01 请求当前返回可重试 503。
- API 写入结构化 JSON Lines `.log`，请求以 `X-Request-Id` 追踪。

## Docker 启动

本地开发配置仅监听回环地址。数据库账号和无认证的中间件设置仅用于本地联调。

```powershell
docker compose up -d --build
docker compose ps -a
Invoke-RestMethod http://127.0.0.1:58000/health/live
Invoke-RestMethod http://127.0.0.1:58000/health/ready
Invoke-RestMethod 'http://127.0.0.1:58000/ipaklink/v1/analytics/config?platform=android&appVersion=1.2.0&installId=3030e190-a030-4bba-9cec-057d5f419f01'
```

OpenAPI 调试入口为 `http://127.0.0.1:58000/docs`。迁移、Kafka Topic 初始化和 MongoDB 副本集初始化是一次性服务，正常完成显示退出码 0。Docker 第一次创建 ClickHouse 数据卷时执行初始化 SQL。已有数据卷需要显式执行新增 ClickHouse 迁移。

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

