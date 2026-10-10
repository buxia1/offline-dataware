# 离线数仓骨架（offline-dataware）

一个小规模、纯离线、可完整跑通的数仓骨架。目的是**先把链路跑通**，业务逻辑后续补充。

全部组件跑在 **WSL2 + Docker** 里，一台 16GB 内存的笔记本即可。

---

## 架构

```
                    ┌──────────────────────────────────────────┐
                    │         DolphinScheduler (调度)           │
                    │   定时触发 · 依赖编排 · 失败重试 · 日志    │
                    └──────────────────────────────────────────┘
                                        │
                                        ▼
  订单链路（DS 工作流 offline_dataware）
  Python 脚本  ──►  Kafka  ──►  StarRocks Routine Load  ──►  StarRocks
  模拟订单        消息队列      ⭐ StarRocks 原生消费       ODS → DWD → DWS → ADS
                              （2026-10-09 起替代 Spark 摄入）

  订单事件 / 累积快照链路（DUPLICATE 事件流 → 累积快照事实表）
  Python 脚本  ──►  Kafka  ──►  StarRocks Routine Load  ──►  StarRocks
  模拟事件流       ods_order_event  ⭐ 原生消费（Exactly-Once）  ODS → DWD(累积快照 dwd_order_lifecycle)
  ⚠️ 与上面一条独立：不同 topic、不同表、不同 Routine Load 作业

  商品 / 维度链路（DS 工作流 dim_product_chain）
  Python 脚本  ──►  CSV  ──►  Stream Load  ──►  StarRocks
  模拟商品快照     文件同步     HTTP 导入       ODS → DIM(SCD1/SCD2) → DWD
  ⚠️ 生成器不在调度里（CSV 视为"上游同步"），调度从 Stream Load 开始
```

**订单的四层视角**（同一实体，四种粒度，不要互相 JOIN 出报表）：

| 表 | 粒度高 | 说明 |
|---|---|---|
| `ods.ods_order` | 消息 | 订单快照原始落地（**主键模型** → 重复消息自动折叠）|
| `dwd.dwd_order_detail` | 订单×天 | 按天 `INSERT OVERWRITE`，同一订单跨天可能多行 |
| `dwd.dwd_order_lifecycle` | **订单（一行到底）** | **累积快照**：五个里程碑列，每发生一个就回填一列；NULL = 还没发生（卡单就是永远 NULL）|
| `ods.ods_order_event` | 事件 | 事件流原始落地（**`DUPLICATE KEY`** → 重复消息**原样保留**，见下方"幂等性"）|

**两条链路的差异是刻意的**：

| | 订单链路 | 商品链路 |
|---|---|---|
| 数据形态 | **事件流**，一条一条持续产生 | **实体状态**，每天一份全量快照 |
| 传输方式 | Kafka + Routine Load | CSV 文件 + Stream Load |
| 同步节奏 | 按天增量 | 按天全量快照 |

真实业务里商品也是整表导出走 DataX，不走消息队列 —— **事件用流、实体用快照**是通用的分层原则。

| 组件 | 版本 | 职责 | 端口 |
|---|---|---|---|
| Kafka | 3.8.1 | 消息队列，数据入口 | 9092 |
| Spark | 3.5.1 | ⚠️ **当前不承担摄入**（2026-10-09 起 ODS 改由 Routine Load 常驻消费）；后续「湖仓一体」阶段用于直读文件做清洗 | — |
| StarRocks | 3.5.0 | 存储 + 计算 + 对外查询（allin1 单容器） | 9030 / 8030 / 8040 |
| MySQL | 8.0 | DolphinScheduler 的元数据库 | 13306 |
| DolphinScheduler | 3.2.0 | 工作流调度（standalone 模式） | 12345 |

---

## 目录结构

```
offline-dw/
├── docker-compose.yml              所有服务定义
├── README.md
├── data/dim/                        商品快照 CSV（生成物，不进版本库）
├── docs/
│   ├── PITFALLS.md                 踩坑记录（最有价值的部分）
│   └── dolphinscheduler-workflow.md  DS 工作流的节点配置
├── sql/                            各层建表与转换 SQL
│   ├── ods_order.sql
│   ├── ods_routine_load.sql                ⭐ 两条 Routine Load 作业（ODS 摄入，2026-10-09）
│   ├── dwd_order_detail.sql
│   ├── dwd_add_history_partitions.sql
│   ├── dws_user_order_day.sql
│   ├── ads_daily_sales.sql
│   │   ── 订单事件 / 累积快照链路 ──
│   ├── ods_order_event.sql                事件流落地层（DUPLICATE KEY，不分区）
│   ├── dwd_order_lifecycle.sql            累积快照事实表 + 期望视图
│   ├── dwd_order_lifecycle_load.sql       累积快照装载（${FROM_DT}，INSERT 即 UPSERT）
│   ├── dqc_order_chain.sql                订单链路 DQC（含 ⑤a/⑤b 防空上游假绿）
│   │   ── 商品 / 维度链路 ──
│   ├── ods_product.sql                    商品快照落地层（保留全部历史）
│   ├── dim_product.sql                    商品维度 SCD1（只有当前状态）
│   ├── dim_product_load.sql               SCD1 装载
│   ├── dim_product_scd2.sql               商品维度 SCD2 拉链表
│   ├── dim_product_scd2_load.sql          SCD2 装载（TRUNCATE + INSERT 合一）
│   ├── dwd_order_sku_detail.sql           订单 + 商品属性宽表
│   ├── dwd_order_sku_detail_add_partitions.sql  补历史分区
│   └── dwd_order_sku_detail_load.sql      物化装载（Shell 模板）
├── scripts/
│   ├── gen_mock_orders_snapshot.py  模拟订单**快照**生成器（--date，可重放，号段 +500）
│   ├── gen_mock_orders.py           模拟订单**事件流**生成器（--date，可重放，号段 +0）
│   ├── check_routine_load.sh        ⭐ Routine Load 健康检查（异常 exit 1，挂 DS 定时告警）
│   ├── dwd_overwrite.sh             DWD 按天覆盖（Shell，给 DS 用）
│   ├── dwd_order_lifecycle_load.sh  累积快照装载外壳（五道防线，默认增量、--full 全量重建）
│   ├── dqc_order_chain.sh           订单链路 DQC（可选业务日期参数）
│   ├── gen_mock_products.py         模拟商品快照生成器（支持 --date 造历史）
│   ├── load_product_to_ods.sh       商品 CSV → ODS（Stream Load）
│   └── dwd_sku_load.sh              商品宽表逐天物化
├── kafka/                          空目录（Kafka 数据不挂载）
├── starrocks/
│   ├── fe/{conf,log,meta}          meta 挂载用于持久化
│   └── be/{conf,log,storage}
├── mysql/{conf,data}
├── spark/{conf,jars}
└── ds/{bin,libs,logs}
```

**没有进版本库的**（见 `.gitignore`）：运行时数据（meta/storage/data/logs）和下载的二进制依赖（jar）。

---

## 环境要求

- Windows 11 + WSL2（Ubuntu 22.04）
- Docker Desktop，**WSL Integration 打开 Ubuntu-22.04**
- 物理内存 **16GB 以上**（WSL 分配 8GB）
- WSL 时区设为 `Asia/Shanghai`（**必须**，见 PITFALLS）

### `C:\Users\<你>\.wslconfig`

```ini
[wsl2]
memory=8GB
processors=6
swap=2GB
networkingMode=mirrored
dnsTunneling=true
firewall=true

[experimental]
autoMemoryReclaim=gradual
sparseVhd=true
```

`networkingMode=mirrored` 让 Windows 侧可以直接用 `localhost` 访问容器端口，省掉端口转发配置。

---

## 快速开始

### 1. 前置准备

```bash
# 时区（关键，否则数据日期会错一天）
sudo timedatectl set-timezone Asia/Shanghai

# StarRocks BE 需要的大内存映射数
sudo sysctl -w vm.max_map_count=2000000
echo "vm.max_map_count=2000000" | sudo tee -a /etc/sysctl.conf
```

### 2. 下载二进制依赖（这些不在版本库里）

```bash
# MySQL 驱动（DolphinScheduler 用）
mkdir -p ds/libs
curl -L -o ds/libs/mysql-connector-java-8.0.30.jar \
  https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar

# DS 建表 SQL
curl -L -o sql/dolphinscheduler_mysql.sql \
  https://raw.githubusercontent.com/apache/dolphinscheduler/3.2.0/dolphinscheduler-dao/src/main/resources/sql/dolphinscheduler_mysql.sql

# docker CLI（静态编译版，给 DS 容器跨容器调用 Spark 用）
mkdir -p ds/bin
curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz -o /tmp/docker.tgz
tar -xzf /tmp/docker.tgz -C /tmp docker/docker
mv /tmp/docker/docker ds/bin/docker && chmod +x ds/bin/docker
```

### 3. 启动基础设施

```bash
docker compose up -d kafka starrocks mysql
```

等约 60 秒让 StarRocks 起来，然后初始化 DS 的元数据库：

```bash
docker compose exec -T mysql mysql -uroot -proot123 dolphinscheduler < sql/dolphinscheduler_mysql.sql
docker compose up -d dolphinscheduler spark
```

### 4. 建表

```bash
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/ods_order.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dwd_order_detail.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dwd_add_history_partitions.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dws_user_order_day.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/ads_daily_sales.sql
```

### 5. 生成数据并跑通链路

```bash
pip3 install --user kafka-python

python3 scripts/gen_mock_orders.py 1000

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic ods_order \
  --partitions 3 --replication-factor 1
```

首次建立 ODS 摄入 —— **两条 Routine Load 作业**（2026-10-09 起，不再用 Spark 摄入）：

```sql
-- 见 sql/ods_routine_load.sql（含完整注释与运维命令）
CREATE ROUTINE LOAD ods.ods_order_load ON ods_order
COLUMNS(order_id, user_id, product_id, amount, order_time, status,
        dt = to_date(order_time))
PROPERTIES("format"="json",
           "jsonpaths"="[\"$.order_id\",\"$.user_id\",\"$.product_id\",\"$.amount\",\"$.order_time\",\"$.status\"]",
           "max_filter_ratio"="0")
FROM KAFKA("kafka_broker_list"="kafka:29092",
           "kafka_topic"="ods_order",
           "kafka_partitions"="0,1,2",
           "kafka_offsets"="OFFSET_BEGINNING");   -- ⚠️ 仅【首次空表初始化】才可以这么写
```

> ⚠️ 上面用 `OFFSET_BEGINNING` 是因为**首次初始化时表是空的**。
> 一旦表里已有数据，**必须换成精确位点**，否则 DUPLICATE KEY 表会重灌（见「幂等性 → ODS 摄入」）。

建完确认状态：

```bash
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW ROUTINE LOAD FROM ods\G"
```

<details>
<summary>历史做法（已废弃，保留供追溯）</summary>

首次跑 Spark 作业（要下载约 30MB 依赖）：

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.4.0 \
  /opt/offline-dw/scripts/ods_order_to_starrocks.py
```

</details>


**国内网络建议加镜像**：

```bash
  --repositories https://maven.aliyun.com/repository/public
```

### 5b. 商品链路（独立于订单链路）

商品/维度链路额外需要 5 张表：

```bash
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/ods_product.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dim_product.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dim_product_scd2.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dwd_order_sku_detail.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dwd_order_sku_detail_add_partitions.sql
```

完整跑法见「商品 / 维度链路」一节。

### 6. DolphinScheduler

浏览器打开 <http://localhost:12345/dolphinscheduler/ui>
账号 `admin` / 密码 `dolphinscheduler123`（**登录后立刻改**）

- 数据源中心 → 注册数据源：类型 `MySQL`，主机 `starrocks`，端口 `9030`，用户 `root`，密码留空
- 按 `docs/dolphinscheduler-workflow.md` 建工作流

---

## 数据分层

| 层 | 表模型 | 说明 |
|---|---|---|
| **ODS** | `DUPLICATE KEY` | 原始落地，纯追加，保留所有脏数据 |
| **DIM** | `PRIMARY KEY` | 维度表。SCD1 只留当前状态；SCD2 拉链表留全部版本 |
| **DWD** | `DUPLICATE KEY` + 按 `dt` 分区 | 清洗后的订单明细，一行一个订单 |
| **DWS** | `PRIMARY KEY(user_id, dt)` | 按用户按天汇总（原子指标） |
| **ADS** | `PRIMARY KEY(dt)` | 每日大盘（派生指标：客单价、支付率） |

**商品链路的分层落点**：

```
ods.ods_product            每天一份全量快照（DUPLICATE KEY，一行不覆盖）
        │
        ├──► dim.dim_product         SCD1：每商品 1 行，只有"现在"
        │
        └──► dim.dim_product_scd2    SCD2：每商品 N 行（N=变更次数+1），能回答"当时"
                    │
                    └──► dwd.dwd_order_sku_detail
                         订单 + 下单当天的商品属性（品类/品牌/单价）
```

**SCD1 和 SCD2 的区别**（同一份 ODS 原料，两种用法）：

| | `dim_product`（SCD1） | `dim_product_scd2`（SCD2） |
|---|---|---|
| 主键 | `product_id` | `(product_id, valid_from)` |
| 行数 | 每商品 1 行 | 每商品 N 行（N = 变更次数 + 1） |
| 时间列 | 无 | `valid_from` / `valid_to` / `is_current` |
| 能回答 | 商品**现在**是什么品类 | 商品**在 9-20 那天**是什么品类 |
| 用途 | 看当前状态 | 历史回溯（订单口径必须用这个） |

**为什么 SCD2 的 `valid_to` 用哨兵值 `9999-12-31` 而不是 `NULL`**：JOIN 条件要写 `dt BETWEEN valid_from AND valid_to`，`BETWEEN` 遇到 `NULL` 返回 `NULL`，当前版本就永远匹配不上。

**⚠️ 全局必须过的不变式**（改任何装载 SQL 之后都重跑一遍）：

```sql
-- ① 每个商品恰好一个当前版本，ratio 必须 = 1.00
SELECT count(*) AS rows_, count(DISTINCT product_id) AS pids,
       sum(is_current) AS cur,
       round(sum(is_current)/count(DISTINCT product_id), 2) AS ratio
FROM dim.dim_product_scd2;

-- ② 版本区间无重叠、无空洞 → broken_links 必须 = 0
--   ⚠️ 第二行 WHEN 不能省。DATE_ADD(DATE '9999-12-31', INTERVAL 1 DAY) 返回 NULL，
--   而 NULL <> next_from 是 NULL（不是 TRUE），CASE 会落到 ELSE 0 ——
--   于是"永久有效的版本后面又跟了一个版本"这种断裂会被静默放过。
SELECT sum(CASE WHEN next_from IS NULL THEN 0
                WHEN valid_to = DATE '9999-12-31' THEN 1
                WHEN DATE_ADD(valid_to, INTERVAL 1 DAY) <> next_from THEN 1
                ELSE 0 END) AS broken_links
FROM (SELECT product_id, valid_to,
             LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_from
      FROM dim.dim_product_scd2) t;

-- ③ is_current 与 valid_to 自洽 → 两个都必须是 0
SELECT sum(CASE WHEN is_current = 1 AND valid_to <> DATE '9999-12-31' THEN 1 ELSE 0 END) AS bad_current,
       sum(CASE WHEN is_current = 0 AND valid_to  = DATE '9999-12-31' THEN 1 ELSE 0 END) AS bad_closed
FROM dim.dim_product_scd2;

-- ④ 物化对账：行数和金额必须完全相等
SELECT (SELECT count(*) FROM dwd.dwd_order_detail)                AS order_rows,
       (SELECT count(*) FROM dwd.dwd_order_sku_detail)            AS sku_rows,
       (SELECT round(sum(amount),2) FROM dwd.dwd_order_detail)    AS order_total,
       (SELECT round(sum(amount),2) FROM dwd.dwd_order_sku_detail) AS sku_total;
```

**清洗规则（DWD）：**

| 规则 | 影响行数 |
|---|---|
| 过滤 `user_id IS NULL` | 约 5% |
| `amount` 取绝对值（负数修正） | 约 3% |
| 同一 `order_id` 只保留金额最大的一条 | 约 2% |

**去重排序键用 `ABS(amount)` 而不是 `amount`** —— 负数只是脏数据，金额的绝对值才是业务事实。

---

## 商品 / 维度链路

### 怎么跑

```bash
cd ~/offline-dw

# 1. 生成商品快照 CSV（--date 造历史快照）
python3 scripts/gen_mock_products.py --date 2026-09-20
python3 scripts/gen_mock_products.py --date 2026-09-21
python3 scripts/gen_mock_products.py --date 2026-09-26

# 2. 建表（只需一次）
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/ods_product.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dim_product.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dim_product_scd2.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dwd_order_sku_detail.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dwd_order_sku_detail_add_partitions.sql

# 3. CSV → ODS（Stream Load，snapshot_date 从文件名自动解析）
bash scripts/load_product_to_ods.sh data/dim/product_snapshot_20260920.csv
bash scripts/load_product_to_ods.sh data/dim/product_snapshot_20260921.csv
bash scripts/load_product_to_ods.sh data/dim/product_snapshot_20260926.csv

# 4. DIM 层：SCD1 快照 与 SCD2 拉链表
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dim_product_load.sql
docker compose exec -T starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dim_product_scd2_load.sql

# 5. DWD 宽表：订单 + 下单当天的商品属性（逐天 INSERT OVERWRITE）
bash scripts/dwd_sku_load.sh

# 6. 验证（4 条不变式见上一节）
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT count(*) AS rows_, count(DISTINCT product_id) AS pids, sum(is_current) AS cur,
       round(sum(is_current)/count(DISTINCT product_id),2) AS ratio
FROM dim.dim_product_scd2;"
```

### ⚠️ SCD2 装载必须是"清空 + 重建"一次执行

`sql/dim_product_scd2_load.sql` 里 **`TRUNCATE` 和 `INSERT` 写在同一个文件**，理由：

`is_current` 没有任何约束能保护它。主键 `(product_id, valid_from)` 只保证"版本不重复"，**保证不了"每个商品恰好一个 `is_current=1`"** —— 那是业务语义，只能靠装载 SQL 算对。

如果只跑 `INSERT`（不清空），重跑会叠加出**一个商品两个"当前版本"**，`ratio` 变成 `2.00`，而且**不报错**。

> **规则：需要"清空 + 重建"的操作必须放在同一个文件里，一次执行。**
> 一个"可以忘记执行就会弄坏数据"的文件拆分，是设计缺陷，不是使用者的错。

### 验证版本数是否合理（比 `ratio` 更强）

```sql
-- 版本数 = 该商品真正发生变更的次数 + 1，不是快照天数
SELECT product_id, count(*) AS versions
FROM dim.dim_product_scd2 GROUP BY product_id HAVING count(*) > 1 ORDER BY product_id;

-- 交叉验证：各快照之间真正有差异的商品数，应该和上面的商品集合一致
SELECT count(*) AS changed_products FROM ods.ods_product a JOIN ods.ods_product b
  ON a.product_id = b.product_id
 AND a.snapshot_date = '2026-09-20' AND b.snapshot_date = '2026-09-21'
WHERE a.category <> b.category OR a.price <> b.price OR a.status <> b.status;
```

**本项目实际结果**：09-26 那批和 09-21 **完全相同**（差异 0 行），所以每个商品只有 2 个版本（09-20、09-21）—— **快照相同不产生新版本，这是 SCD2 的正确行为**，不是漏数据。

### 为什么必须物化到 DWD

`dwd_order_sku_detail` 的 JOIN 条件是 `o.dt BETWEEN s.valid_from AND s.valid_to`，**这是范围 JOIN，哈希优化用不上**（哈希只能回答"相不相等"，回答不了"落不落在区间里"）。

实测对比：

| 写法 | 耗时 |
|---|---|
| 单表聚合（无 JOIN） | 0.37 秒 |
| 等值 JOIN（把 `BETWEEN` 去掉） | 0.37 秒 |
| **`BETWEEN` 范围 JOIN** | **1 分 41 秒** |

所以把 JOIN 的结果**物化**到 DWD，让这个代价**每天付一次**，而不是每次查询都付。

---

## 订单事件链路 / 累积快照事实表

**这是订单的第四条链路**，和 `offline_dataware`（订单快照）**独立**：不同 topic、不同表、不同 Spark 脚本。
订单快照回答"**这单现在是什么状态**"（按天覆盖）；事件流 + 累积快照回答"**这单走到哪一步了、卡在哪**"（一行到底、持续回填）。

**涉及的表**

| 表 | 模型 | 说明 |
|---|---|---|
| `ods.ods_order_event` | `DUPLICATE KEY` | 事件流落地（事件：`order`/`pay`/`ship`/`finish`/`cancel`）。行数 == `(order_id,event_type)` 去重对数（有防线⑤ 守着）|
| `ods.ods_kafka_offset` | `PRIMARY KEY(topic, partition_id)` | **消费位点表** —— 记住"读到哪了"，增量摄入靠它 |
| `dwd.dwd_order_lifecycle` | `PRIMARY KEY(order_id)` | **累积快照**：五个里程碑列 + 派生列。当前 **900 行**（09-20 ~ 09-28）|
| `dwd.v_order_lifecycle_expected` | 视图 | 由事件流推导"期望快照"，装载与对账的**单一真相源** |

**怎么跑**（逐天，三步）

```bash
# ① 发当天到期的事件（先 dry-run 看汇总）
python3 scripts/gen_mock_orders.py --date 2026-09-21 --dry-run
python3 scripts/gen_mock_orders.py --date 2026-09-21

# ② 摄入到 ODS —— 不需要手工跑！
#    Routine Load 是【常驻】作业，消息进 Kafka 后会自动落 ODS。
#    确认摄入是否生效：
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW ROUTINE LOAD FROM ods\G"
bash scripts/check_routine_load.sh;  echo "EXIT=$?"

# ③ 装载累积快照（默认增量；--full 是全量重建的恢复手段）
bash scripts/dwd_order_lifecycle_load.sh
```

**⚠️ 逐天回放必须按顺序**（生成器**有状态**）：它读 `ods_order_event` 判断"哪些里程碑还没发"，只发"到期日 **==** `--date`"的事件 —— **跳过某天就永远不补发**。脚本为此内置两类告警（`overdue` / `missing_prev`），命中会 `exit 1` 并打印明细。

**累积快照的价值**：`current_stage` + 各里程碑的 NULL 直接回答"卡在哪一步"：

```sql
-- 各类卡单：里程碑永远 NULL
SELECT current_stage, count(*) FROM dwd.dwd_order_lifecycle GROUP BY current_stage;

-- 某个订单的完整轨迹（一行看完下单→支付→发货→完成）
SELECT * FROM dwd.dwd_order_lifecycle WHERE order_id = 20260920000;
```

**生成器不在调度里**（和商品快照生成器同一个定位：模拟"上游业务系统"），见下文「调度」。

---

## 调度

**四个工作流**（2026-10-09 现状，以数据库为准）：

```
┌─ offline_dataware（订单快照链路，每天 02:00，exec_type=1 串行等待）────┐
│  dwd_delete → dws_agg → ads_metric → dqc_order_chain                 │
│    Shell        SQL        SQL          Shell                         │
│  ⚠️ dwd_delete 名字骗人，真身是 INSERT OVERWRITE ... PARTITION        │
│  ⚠️ ods_spark 节点已删除（2026-10-09 ODS 改 Routine Load 后不再需要） │
└───────────────────────────────────────────────────────────────────────┘
                               │ 今天成功
                               ▼
┌─ dim_product_chain（商品链路，每天 03:00，exec_type=2 串行丢弃）──────┐
│  wait_order_chain → truncate_and_load_ods → dim_product_load →       │
│     DEPENDENT            Shell                   Shell                │
│        → dim_product_scd2_load → dwd_sku_reload → dq_check            │
│                 Shell                 Shell          Shell            │
└──────────────────────────────────────────────────────────────────────┘

┌─ order_event_chain（订单事件 / 累积快照链路，每天 02:30，END 失败策略）┐
│  dwd_lifecycle_load                                                  │
│     Shell(dwd_order_lifecycle_load.sh)                               │
│  ⚠️ ods_event_spark 节点已删除（2026-10-09 同上）                    │
└──────────────────────────────────────────────────────────────────────┘

┌─ routine_load_health（摄入健康检查，每 30 分钟，END 失败策略）────────┐
│  check_routine_load                                                  │
│     Shell(check_routine_load.sh)                                     │
│  ⚠️ 为什么必须有：摄入改成常驻作业后，DS 里【再没有节点会变红】      │
│     作业挂了数据就静静不进来 → 用这个把"常驻作业死了"翻译成告警       │
└──────────────────────────────────────────────────────────────────────┘
```

> **ODS 摄入已不在 DS 的 DAG 里** —— 它由两条常驻 Routine Load 作业承担
> （`ods.ods_order_event_load` / `ods.ods_order_load`），见「幂等性」。


### 商品链路为什么「定时」和「依赖」两个都要

| | 作用 | 缺了会怎样 |
|---|---|---|
| **定时 03:00** | **触发**工作流 | 没有任何东西会启动它，`wait_order_chain` 永远不会被评估 |
| **`wait_order_chain`**（依赖节点）| **确认**订单链路今天已经成功 | 订单链路慢或失败时会读到过期的 `dwd_order_detail`，算出错的结果 |

依赖节点配置：类型「工作流」→ `offline_dataware` → 任务「**ALL**」→ 周期「今天」→ 失败策略「**等待**」。

### 工作流都必须用「串行丢弃」

`offline_dataware` 和 `dim_product_chain` 的执行策略都是 **`SERIAL_DISCARD`（串行丢弃）**，不是默认的「并行」。

**原因**：两者都含 `TRUNCATE`。如果允许同一工作流的两个实例并发运行，会出现：

```
实例A: TRUNCATE ods_product ✓
实例A: 导入 CSV-09-20 ✓
实例B: TRUNCATE ods_product      ← 把 A 刚导入的清掉了
        ↓
最终既丢数据又重复，而且不报错
```

**这是「静默损坏」类问题** —— 只有靠执行策略从源头禁止并发才能防住。

### 补数（回填历史）

**补数的完整链条**（因为商品链路读订单链路的 DWD）：

```
① 补订单链路 offline_dataware      → dwd_order_detail 多出几天
② 补 dwd_order_sku_detail 的分区    → 手工 ALTER（见「已知限制 9」）
③ 跑商品链路 dim_product_chain      → dwd_order_sku_detail 自动跟上
```

**第③ 步为什么能自动跟上**：`dwd_sku_load.sh` 里是 `SELECT DISTINCT dt FROM dwd_order_detail` —— **动态取天数**，不写死。

**实测的两个坑**：

| 坑 | 表现 | 正确做法 |
|---|---|---|
| **日期偏移** | 补数范围写 `09-22 ~ 09-26`，**实际处理的是 `09-21 ~ 09-25`** | `${system.biz.date}` = **调度日期 − 1 天**。验证方法：看 DS 日志里的 `D=20xxxxxx` |
| **实例被静默丢掉** | 5 个日期的实例**只跑了 1 个**，而且不报错 | 执行方式必须选「**串行执行**」—— 工作流执行策略是「串行丢弃」，并行补数会被丢掉 |

**先看日志验证日期映射，再决定补数范围** —— 别猜。

详细节点配置见 `docs/dolphinscheduler-workflow.md`。

---

## 幂等性

**整个链路可以反复重跑，结果不变。**

| 层 | 靠什么保证 |
|---|---|
| **ODS 摄入（两条链路）** | ⭐ **StarRocks Routine Load 原生 Exactly-Once** —— 位点由引擎维护、且与数据在同一事务里提交。作业配置见 `sql/ods_routine_load.sql` |
| ODS（订单） | **`PRIMARY KEY(order_id)` 表模型** —— 即使重复消费也自动折叠 |
| ODS（事件）⚠️ | **`DUPLICATE KEY` 表模型 —— 重复【不会折叠】**，所以幂等**必须**靠摄入层（现已由 Routine Load 保证） |
| ODS（商品） | 工作流开头 `TRUNCATE ods_product`，再全量重灌所有快照；Stream Load 标签**每次运行唯一** |
| DWD（订单） | `INSERT OVERWRITE ... PARTITION (p<日期>)`，原子覆盖当天分区 |
| DWD（累积快照） | `PRIMARY KEY(order_id)` + **`INSERT` 即 UPSERT**（只回填"那天及之后有事件"的订单）|
| DWD（商品宽表） | 同上，`dwd_sku_load.sh` 逐天 `INSERT OVERWRITE` |
| DIM（SCD1） | `PRIMARY KEY` 表模型，同键自动覆盖 |
| **DIM（SCD2）** | **`TRUNCATE` + `INSERT` 合一的装载 SQL**（主键挡不住版本漂移，见上文） |
| DWS / ADS | `PRIMARY KEY` 表模型，同键自动覆盖 |

**验证方法**：连续执行两次工作流，对比三层的行数和金额，必须完全一致。

> **⚠️ 事件流的幂等"不能靠表模型"**：`ods_order_event` 是 `DUPLICATE KEY`（重复原样保留）。
> 而 `MAX(CASE WHEN event_type='x' ...)` 这类聚合会**把重复折叠成同一个值 → 值级对账看不见重复**。
> 装载脚本因此专门加了「事件表行数 = `(order_id,event_type)` 去重对数」这道**行级**防线
> （见 PITFALLS §3.17），健康检查脚本也带同一条检查。

### ⭐ ODS 摄入：从「Spark 批 + 手工位点表」改为「Routine Load」（2026-10-09）

**改前**：Spark 批作业消费 Kafka → 写 ODS；因为要用批工具做流式的活，
不得不自己维护一张**位点表 `ods.ods_kafka_offset`**（记 `next_offset`）、
一个 199 行的 Python 摄入脚本、一个 130 行的 Shell 外壳（含 4 道防线）。

**为什么换**（都有实测依据）：

| 原因 | 依据 |
|---|---|
| 省掉固定开销 | 旧路径**空跑也要 13.7 秒**（读到 0 行照样花 —— 全是 JVM + Ivy + JDBC 开销）|
| 删掉三样组件 | 位点表 + Python 脚本 + Shell 外壳（含 4 道防线）全部退役 |
| **消掉重复实现的正确性风险** | 手工位点表 = 重新实现 StarRocks 自带的 Exactly-Once；10-05 静默清库事故就是这套手工逻辑的漏洞 |

**改后**：两条常驻作业（`sql/ods_routine_load.sql`）

| 作业 | topic | 目标表 |
|---|---|---|
| `ods.ods_order_event_load` | `ods_order_event` | `ods_order_event` |
| `ods.ods_order_load` | `ods_order` | `ods_order` |

**⚠️ 三个必须记住的写法**（实测/官方文档核实，写错会静默出错）：

| 点 | 说明 |
|---|---|
| `kafka_offsets` 是**逗号分隔**，且与 `kafka_partitions` **按顺序一一对应** | 建作业时用它精确续接，不重灌 |
| **绝不能写 `OFFSET_BEGINNING`** | 事件表是 `DUPLICATE KEY`，重灌会把 2379 行变成 **4758** 行（不折叠！）|
| `max_filter_ratio` 默认 `1`（=不生效），**必须显式设 `0`** | 否则坏数据被**静默过滤**；设 0 则一条坏数据就把作业暂停 |

**⚠️ 常驻作业的新风险：挂了没人知道。**
所以必须有 `scripts/check_routine_load.sh` + DS 工作流 `routine_load_health` 每 30 分钟检查一次，
异常 `exit 1` 交给告警组。这是「常驻作业」相对「每天跑一次的节点」**唯一新增的运维负担**。

**验证判据**（改造后已全部通过）：

| 项 | 期望 |
|---|---|
| 两条作业 `State` | `RUNNING`；`ErrorLogUrls` 空 |
| `ods_order` / `ods_order_event` / `dwd_order_lifecycle` | **800 / 2379 / 900**（与改造前一致）|
| 事件表不变式 | 行数 == `(order_id,event_type)` 去重对数 |
| 两条 DQC | `EXIT=0` |


### 更严格：用指纹验证

行数一样**不代表**数据一样 —— 品类改了、版本区间挪了，行数都可能纹丝不动。用 `sql/fingerprint_product_chain.sql` 把**每一行的每一列**都算进一个数字：

```bash
# 跑工作流【之前】记一次
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/fingerprint_product_chain.sql

# 执行工作流，然后用【同一条命令】再记一次
# 四个指纹必须一字不差
```

原理：`sum(crc32(一行所有列拼成的字符串))`。用 `sum` 而不是整表算 md5，是因为**数据库里行的物理顺序不保证**，而 `sum` 与顺序无关 —— 否则同样的数据会算出不同指纹，白查半天。

**它还能发现「非确定性」**：如果**没改任何代码**，两次指纹却不同 → 说明链路里藏了不确定性（SQL 里用了 `now()`、或 `ORDER BY` 有并列值导致每次取到不同的行）。**这类 bug 极难发现，指纹几乎是唯一能抓住它的手段。**

---

## 运维命令

```bash
# 各层数据量
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT (SELECT count(*) FROM ods.ods_order)        AS ods,
       (SELECT count(*) FROM dwd.dwd_order_detail) AS dwd,
       (SELECT count(*) FROM dws.dws_user_order_day) AS dws,
       (SELECT count(*) FROM ads.ads_daily_sales)  AS ads_days;"

# DWD 按日期分布
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT dt, count(*) AS cnt FROM dwd.dwd_order_detail GROUP BY dt ORDER BY dt;"

# 分区列表
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B -e "
SHOW PARTITIONS FROM dwd.dwd_order_detail;" | cut -f2 | sort
# SCD2 不变式：ratio 必须 = 1.00
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT count(*) AS rows_, sum(is_current) AS cur,
       round(sum(is_current)/count(DISTINCT product_id),2) AS ratio
FROM dim.dim_product_scd2;"

# 【一条命令跑完 5 项数据质量检查】全部通过 = 没有任何输出，退出码 0
bash scripts/dqc_dim_product.sh

# 订单链路 DQC
bash scripts/dqc_order_chain.sh;  echo "订单 EXIT=$?"

# 累积快照：装载 + 五道防线（默认增量、--full 全量重建）
bash scripts/dwd_order_lifecycle_load.sh

# 累积快照：阶段分布（卡单一眼可见）
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT current_stage, count(*) AS cnt FROM dwd.dwd_order_lifecycle
GROUP BY current_stage ORDER BY cnt DESC;"

# 事件流：按天分布 + 重复检查（行数必须 == 去重对数）
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT dt, count(*) AS rows_, count(DISTINCT concat(order_id,'-',event_type)) AS pairs
FROM ods.ods_order_event GROUP BY dt ORDER BY dt;"

# ⭐ Routine Load 状态（ODS 摄入，2026-10-09 起）
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW ROUTINE LOAD FROM ods\G"
# 关注：State=RUNNING / Progress / ErrorLogUrls 空

# ⭐ 摄入健康检查（异常 exit 1；--list 只列状态）
bash scripts/check_routine_load.sh;  echo "EXIT=$?"

# ⭐ Routine Load 运维
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
PAUSE  ROUTINE LOAD FOR ods.ods_order_event_load;"
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
RESUME ROUTINE LOAD FOR ods.ods_order_event_load;"

# 数据质量检查的【自检】：用内存里的假数据证明检查真的能发现问题
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dqc_dim_product_selftest.sql

# 【指纹】跑工作流前后各执行一次，输出必须一字不差
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/fingerprint_product_chain.sql

# 工作流的真实状态（权威来源，导出 JSON 不可信，见「重要提醒」）
docker compose exec -T mysql mysql -uroot -proot123 dolphinscheduler -e "
SELECT p.name, p.version, p.release_state AS def_online,
       p.execution_type AS exec_type, s.crontab, s.release_state AS sched_online
FROM t_ds_process_definition p
LEFT JOIN t_ds_schedules s ON s.process_definition_code = p.code
ORDER BY p.name;" 2>/dev/null
# def_online/sched_online: 1=上线 0=下线    exec_type: 0=并行 1=串行等待 2=串行丢弃

# 物化对账：两边行数与金额必须完全相等
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT (SELECT count(*) FROM dwd.dwd_order_detail)                 AS order_rows,
       (SELECT count(*) FROM dwd.dwd_order_sku_detail)             AS sku_rows,
       (SELECT round(sum(amount),2) FROM dwd.dwd_order_detail)     AS order_total,
       (SELECT round(sum(amount),2) FROM dwd.dwd_order_sku_detail) AS sku_total;"

# 业务报表：各品类每天销售额
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT dt, category, count(*) AS orders, round(sum(amount),2) AS sales
FROM dwd.dwd_order_sku_detail GROUP BY dt, category ORDER BY dt, sales DESC;"

# 容器状态与内存
docker compose ps
docker stats --no-stream

# 全部停止 / 重启
docker compose stop
docker compose restart dolphinscheduler
```

---

## 已知限制

1. ~~**ODS（订单）每次从 Kafka 全量重读**~~ —— ⭐ **已修（2026-10-09）**：改用 Routine Load，位点由引擎维护。
   历史痕迹：改前 topic `ods_order` 有 **1800 条消息**、表里只有 **800 行**，说明重复读过 2.25 倍，靠 `PRIMARY KEY` 折叠兜住。
2. ~~**ODS（事件）曾经不幂等**~~ —— ⭐ **已修**：原用位点表 `ods_kafka_offset` 增量，现已改为 Routine Load 的 Exactly-Once。
3. **⚠️ "先 TRUNCATE 再从 Kafka 全量重灌"不是长期方案** —— Kafka `log.retention.hours=168`（7 天），消息过期后清表就等于**清库**；而且 topic 被**部分**裁剪时，"topic 非空"的检查拦不住，会灌进残缺数据。Routine Load 按位点续接，**不再重读历史，retention 就无关了**。
4. **装载脚本的防线有"口径"** —— 局部范围的检查**证明不了全表**。事件表重复那次，三条防线（值级 `EXCEPT`、快照表行数、`DISTINCT order_id`）全部报绿，是因为它们都没问过"这张表自己的原始行数对不对"。见 PITFALLS §3.17。
5. **DWD 去重只在单天内生效** —— 同一 `order_id` 跨天出现会在两个分区各留一份。增量处理的固有边界。
6. **StarRocks 用的是 allin1 单容器**（FE + BE 合一），仅供开发验证，不能上生产。
7. **Kafka 数据未挂载**（放在容器内 `/tmp`），容器重建即丢失。ODS 层靠工作流重跑重建。
8. **Spark 依赖缓存也在容器内**（`/tmp/.ivy2`），容器重建要重新下载 30MB。
9. **SCD2 是全量重建**（`TRUNCATE` + 从 ODS 完整重推）—— 快照天数一多会变慢，增量维护尚未实现。
10. **商品快照的生成不在调度里** —— CSV 由 `gen_mock_products.py` 手工产出（视为"上游同步"）。调度只负责"CSV → 数仓"这一段，所以**快照不会自己每天长出来**。
11. **订单快照 / 事件流两个生成器也不在调度里** —— 它们模拟"上游业务系统"。**本项目数据手动添加、将来直接换外源数据集，所以有意不配 cron**。日常顺序：
    ```
    【手工】gen_mock_orders_snapshot.py --date <业务日期>   → ods_order
    【手工】gen_mock_orders.py          --date <业务日期>   → ods_order_event
          ↓
    【常驻】Routine Load 自动摄入两条链路的 Kafka 消息 → ODS   ← 2026-10-09 起，不再由 DS 触发
          ↓
    【DS 02:00】offline_dataware  : dwd_delete → dws_agg → ads_metric → dqc
    【DS 02:30】order_event_chain : dwd_lifecycle_load
    【DS 03:00】dim_product_chain : wait_order_chain → … → dwd_sku_reload → dq_check
    【每30分钟】routine_load_health: check_routine_load（摄入健康检查）
    ```
    ⚠️ **事件生成器有状态**（读 `ods_order_event` 判断该发什么），**必须逐天按顺序跑，跳过某天就永远不补发**。
12. **`dwd_order_sku_detail` 的范围 JOIN 每天付一次代价** —— 这是"物化换查询速度"的必然代价。
13. **补数要手工补分区** —— `dwd_order_sku_detail` 缺 `p20260915`~`p20260919` 等分区；动态分区**只创建"未来"，不创建历史**（`history_partition_num=0`）。补数进来的新日期，必须先照 `sql/dwd_order_sku_detail_add_partitions.sql` 手工 `ADD PARTITION`（且**必须先 `dynamic_partition.enable=false`**，理由见 PITFALLS §3.3）。**这是动态分区的固有行为、属运维常规动作**；想省掉它可改表达式分区，见「后续方向 → 可选架构改进」。
14. **`wait_order_chain` 依赖的是"今天"的实例** —— 跨天补数时，依赖检查会对不上，需要单独手工执行。
15. ⭐ **常驻 Routine Load 的新运维负担** —— 作业挂了**DS 里没有任何节点会变红**，数据静静不进来。靠 `routine_load_health` 每 30 分钟检查兜底。**这是"常驻作业"相对"每天跑一次的节点"唯一新增的成本。**
16. ⭐ **Routine Load 的 `max_filter_ratio` 已设为 `0`** —— 一条坏数据就把作业**暂停**（而不是静默过滤）。这是有意为之（宁停不脏），但意味着**暂停后需要人去 `RESUME`**。
17. **不要用裸的 `docker cp` 往项目里放文件，也不要直接 `chown` 改属主** —— 见 PITFALLS §6.4。
18. ⚠️ **StarRocks FE 的堆（8G）大于容器上限（3G）** —— `fe.conf` 里 `JAVA_OPTS` 写 `-Xmx8192m`，
    但 `docker-compose.yml` 是 `mem_limit: 3g`。当前数据小（`ods_order_event` 数据文件 42.4 KB、
    BE 常驻 RSS 462 MB）所以没事，**但放大数据量前必须修**，否则容易被 OOM kill。
19. ⚠️ **`apache/spark:3.5.1` 里没有任何湖仓连接器** —— iceberg/paimon/hudi/hadoop-aws/aws-java-sdk
    全都没有（252 个 jar 里只有 parquet*、Ivy 缓存 0 个）。要用 Iceberg 需自己挂 jar，
    见「下一阶段：湖仓一体」。
20. ⚠️ **Spark 容器以 uid 185 运行，而项目目录属主是 1000** —— 挂载虽是 `RW=true`，
    容器内**仍然写不进去**（`touch` 直接 `Permission denied`）。要让它落文件必须先
    `chown 185:185 <目录>` 或预建目录并放开权限。**这是"湖仓"落本地文件时的第一道坎。**
21. ⚠️ **Docker 默认 `bridge` 网络没有 DNS** —— 容器之间用容器名互相解析会失败（`getent hosts` 空）。
    表现极具迷惑性：S3A/HTTP 客户端会**无限重试**，看起来像"卡死"（实测挂了 15 分钟无任何输出）。
    需用**用户自定义网络**，或直接写 IP。
22. **MinIO 已改变分发方式（2026-10-09 实测）** —— `minio/minio` 已从 Docker Hub 下架，
    `dl.min.io` 上的 server 与 `mc` 二进制均返回 **410 Gone**，`quay.io`/`bitnami`/各加速站全不可用。
    免费路径只剩 `cgr.dev/chainguard/minio`。**故湖层第一阶段先用本地文件系统。**

## 后续方向

- [x] 维度建模：商品维度、缓慢变化维（SCD1 + SCD2 拉链表）
- [x] **把商品/维度链路接进 DolphinScheduler**（含跨工作流依赖 + 定时）
- [x] 数据质量检查节点（DQC）—— 6 项检查 + 自检
- [x] 用 DS **补数**回填历史数据（**实测两个坑**：`${system.biz.date}` = 调度日期 −1 天；执行方式必须选「串行执行」，否则被"串行丢弃"静默丢掉）
- [x] 补上 `ods_order` 里 09-22~09-25 那 4 天（`dwd_order_detail` 从 4 天 → **8 天 / 763 行**）
- [x] **DQC 加一条「重算对账」** —— `⑥ 重物化属性一致`：宽表里的商品属性必须等于 SCD2 对该日期算出的属性。盖住两个盲区：属性值不同、以及 **JOIN 不上的孤儿行**（范围 JOIN 不满足时那行会直接消失，计数纹丝不动）。写 `<=>` 而非 `<>`（NULL 安全），用 `LEFT JOIN` 而非 `NOT EXISTS`（StarRocks 不支持关联子查询里的非等值谓词）
- [x] 作业失败告警（邮件 / 钉钉）—— DS 里四个工作流都配了 `warning_type=2` + 告警组 `2`
- [x] SCD2 改增量维护，并与全量重建做等价性验证（`scripts/dim_product_scd2_incremental.sh`，指纹一字不差）
- [x] **累积快照事实表**（下单 → 支付 → 发货 → 完成）—— 表/视图/装载/五道防线，**09-20 ~ 09-28 共 900 行**，卡单三类可见
- [x] **订单事件链路的摄入改增量**（`ods_kafka_offset` 位点表）—— 幂等已实测（重跑 `读到 0 行`）。**（2026-10-09 该方案已被 Routine Load 取代）**
- [x] **把订单事件 / 累积快照链路接进 DS**（工作流三 `order_event_chain`：`dwd_lifecycle_load`，02:30，失败策略 `END`）
- [x] **累积快照逐天回放**（09-20 ~ 09-28，`dwd_order_lifecycle` **900 行**，卡单三类可见）
- [ ] ~~把 DWD 清洗逻辑搬到 Spark SQL（上规模后）~~ —— **已实测否决，不打算做**，理由见下方「为什么不做」
- [x] ⭐ **ODS 改用 StarRocks Routine Load（省掉 Spark 这一跳）**（2026-10-09 完成）
      —— 两条常驻作业 `ods_order_event_load` / `ods_order_load`，Exactly-Once；
      退役了位点表 + 199 行 Python + 130 行外壳；新增 `check_routine_load.sh` + DS 工作流 `routine_load_health`。
      详见「幂等性 → ODS 摄入」一节。
- [ ] ⭐ **下一阶段：湖仓一体** —— 见下方「下一阶段：湖仓一体」一节（含**分阶段推进清单**：第 0 步修地基 / 第 1 步湖层只读旁路 / 第 2 步 DWD 下沉 Spark / 第 3 步放量）
      —— 边界已定：**Iceberg + 本地文件系统（MinIO-ready）**；先 1000 单/天跑通、再上 1 万单/天 × 90 天
      —— **这一条正是上方那条被否决待办的"前提条件"**（原文：改数据落点后才该重新评估）
- [x] `docs/dimension-modeling.md`：维度建模 + SCD2 完整说明

**可选架构改进**（不是待办任务 —— 现状能正常工作，属于"想省掉人工操作"时才做）：

- [ ] `dwd_order_sku_detail` 改**表达式分区** —— 现在是动态分区（`dynamic_partition.history_partition_num=0`：**只建未来、不建历史**），所以补历史某天前需要手工 `ALTER TABLE ... ADD PARTITION`。**这是 StarRocks 动态分区的固有行为，属运维常规动作**（建表时也用 `sql/dwd_add_history_partitions.sql` 补过 `p20260915`~`p20260919`）；改成表达式分区可一劳永逸消掉它（PITFALLS §3.2 推荐过）

### 为什么不做「把 DWD 清洗逻辑搬到 Spark SQL」

结论：**在「数据主存是 StarRocks、不是文件」这个前提下，搬过去一定更慢。已实测，不做。**

**① 三批逻辑都已用 Spark SQL 重写并验证过等价**（订单明细清洗 / SCD2 时点关联 / 累积快照推导），
结果逐行逐列一致（763 / 763 / 900 行），**也就是说"能做"是已验证的** —— 否决的是"值得做"。

**② 慢的原因不是算不动，是搬运。** batch C（2379 事件）成本拆解：

| 阶段 | 耗时 | 占比 |
|---|---|---|
| SparkSession 启动 | 1.4 s | 17% |
| **读 ODS（JDBC 抽取）** | **3.5 s** | **42%** |
| 计算（聚合 + 派生） | 1.0 s | 12% |
| **写回 DWD（JDBC 写入）** | **2.5 s** | **30%** |

**真正算数据只占 12%，82% 花在启动和数据搬运上。**

**③ 规模变大也救不了这个架构。** 同一段 SCD2 关联（等值 + 日期区间）两引擎对比
（用隔离 benchmark 表，以 `ods_order` 为种子放大，未触碰真实表）：

| 规模 | 老逻辑（StarRocks SQL） | 新逻辑（Spark SQL） |
|---|---|---|
| 800 行 | 1256 ms | ≈ 11900 ms |
| 3815 行（5 倍）| 1632 ms | 12994 ms |

注意**斜率**：StarRocks 5 倍数据只多 30%（库内执行、亚线性）；
Spark 几乎不动（固定开销主导：JVM + 全量 JDBC 搬运）。

**④ 这条待办的前提是"湖仓"，本项目不是。** 「大数据用 Spark」的经验来自
**数据以 Parquet 存在 HDFS/S3** 的场景 —— 那时 Spark 能直读文件、零搬运。
而本项目的数据主存在 StarRocks 里，Spark 想算就必须 JDBC 抽出来、再写回去，
上面那 82% 就是"用 Spark"本身带来的成本。**只要不改数据落点，规模再大也消不掉它。**

> **什么情况下才该重新评估**：改架构 —— ODS 落 Parquet/对象存储，Spark 直读文件清洗、
> 结果再进 StarRocks 供查询（标准「湖仓 + 数仓」混合）。那时 42% 的读开销才真正消失。
> 这是另一件工程（要重设计 ODS 落点），不是"把清洗搬个家"。

### 为什么「ODS 改用 Routine Load」值得做（与上一条相反）

**结论：已实施（2026-10-09）。这是"换一种摄入方式"，不是"把清洗搬进 StarRocks"。**

> 具体配置、验收判据、运维与回滚见「幂等性 → ODS 摄入」一节。
> 下面保留**当初的决策依据**，便于将来追溯为什么这么改。

**① 当初这一跳的真实成本：13.7 秒，而它什么都没读到。**

实测 `bash scripts/ods_order_event_ingest.sh`（位点已在 2379，无新消息）：

```
退出码=0   墙钟=13670 ms
本次从 Kafka 读到 0 行，按 (order_id,event_type) 去重后 0 行
没有新消息，退出（未写入、未改位点）
```

**空跑也要 13.7 秒** —— 全是 Spark 作业的固定开销（JVM 启动 + Ivy 解析 + Kafka 连接）。
Routine Load 是 StarRocks 常驻消费，**没有这个固定开销**。

**② 它消掉了三样东西**（都是维护负担 + 故障面）：

| 改前的东西 | 改后 |
|---|---|
| `ods_kafka_offset` 位点表 | ✅ 已退役，StarRocks 自己管位点 |
| `ods_order_event_to_starrocks.py`（199 行）| ✅ 可由 `CREATE ROUTINE LOAD` 取代 |
| `ods_order_event_ingest.sh`（130 行外壳 + 四道防线）| ✅ 同上，Exactly-Once 由 StarRocks 保证 |

**③ 位点表这套手工幂等，本质是在重新实现 StarRocks 已有的能力。**
官方文档明确：Routine Load 支持 **Exactly-Once 语义，保证数据不丢不重**
（[使用 Routine Load 导入数据](https://docs.starrocks.io/zh/docs/loading/kafka/RoutineLoad/)），
并且每个导入任务是一个独立事务、通过 Stream Load 机制提交。

**④ 能力上够用 —— 当前"清洗"只是解析和类型转换。**
`ods_order_to_starrocks.py` / `ods_order_event_to_starrocks.py` 实际只做：
JSON 解析 → 类型转换（`order_time`/`event_time` 转 timestamp）→ 派生 `dt = to_date(event_time)`。
Routine Load 支持 JSON + **衍生列**（`COLUMNS` 里写函数），
`dt=to_date(event_time)` 这类派生在导入时就能完成
（[导入过程中实现数据转换](https://docs.starrocks.io/zh/docs/loading/Etl_in_loading/)）。

**⑤ 实施时踩到的三个点（都已写进 `sql/ods_routine_load.sql` 注释）**

- **`kafka_offsets` 必须显式给、且与 `kafka_partitions` 按顺序对应** ——
  不给默认是 `OFFSET_END`（会**跳过**未读消息）；给 `OFFSET_BEGINNING` 会**重灌**。
  `ods_order` 是 `PRIMARY KEY` → 重复折叠无害；`ods_order_event` 是 `DUPLICATE KEY` → **重复会真的多出行**。
- **`max_filter_ratio` 默认 `1`（不生效），必须显式设 `0`** —— 否则坏数据被静默过滤。
- **`SHOW ROUTINE LOAD` 的 `State` 在第 8 列**、状态含 `NEED_SCHEDULE` —— 写监控脚本时会踩。

**⑥ 常驻作业的新负担：挂了没人知道。**
DS 里不再有摄入节点会变红，所以必须补 `routine_load_health` 工作流（每 30 分钟）。
**这是这次改造唯一"变麻烦"的地方**，也是为什么不只是"删掉旧组件"那么简单。

> **注意措辞**：这一步 ≠「把清洗和 join 都搬进 StarRocks」。
> DWD 层（`dwd_overwrite.sh` / `dwd_sku_load.sh` / `dwd_order_lifecycle_load.sh`）**本来就在 StarRocks 里**，
> 不在 Spark 里。Routine Load 换掉的只是**摄入**这一跳，DWD 那三层逻辑一行都没改。

---

## 下一阶段：湖仓一体（方案已定，待实施）

> **状态**：方案已与用户确认边界，**尚未实施**。本节的实测数据均为 2026-10-09 在
> `D:\develop\workspace\deepseek_harness_temp` 用临时探针验证所得，**未改动本项目任何文件**。

### 确认的四项边界

| # | 决策 | 选择 |
|---|---|---|
| 1 | 湖表格式 | **Iceberg** |
| 2 | 文件落点 | **先用本地文件系统**（`MinIO-ready` 设计，将来切对象存储只改 warehouse 路径）|
| 3 | 数据量目标 | **先 1000 单/天 跑通，再上 1 万单/天 × 90 天**（≈270 万事件）|
| 4 | 推进方式 | **代码由用户自己写**，agent 只出方案与验收判据 |

### 目标架构

```
Kafka (ods_order_event / ods_order)
   │
   ├─► StarRocks Routine Load ──► StarRocks ODS（现状，保留不动）
   │                                    │
   │                                    ▼
   │                            DWD/DWS/DIM/ADS（服务层，保留）
   │
   └─► 【新增】Spark ──► Iceberg on 本地文件系统（湖层）
                              │
                              ▼
                   StarRocks External Catalog（查询湖层）
```

**要点**：StarRocks **不拆**，继续当服务层；湖仓是**叠加**不是替换。
**业务内容不变**：不新增指标/维度/ADS 表，现有 4 条链路行为不变。

**这正是上方「为什么不做把 DWD 清洗逻辑搬到 Spark SQL」里预留的那条路。**
当时结论是：只要数据主存在 StarRocks 里，Spark 想算就得 JDBC 抽出来再写回去，
82% 开销消不掉；**④ 已明确「改动数据落点后才该重新评估」** —— 本节就是那件事。

### 实测关键结论（本次验证）

**① StarRocks 侧零成本。** allin1 镜像的 BE **自带**湖格式 reader，且在 `be/lib/*-reader-lib`
下（**`fe/lib` 是空的，别误判为"不支持"**）：

| 格式 | 镜像内自带 | 建 external catalog 实测 |
|---|---|---|
| Iceberg | `iceberg-core-1.9.0.jar` 等 | ✅ `Type=Iceberg` |
| Paimon | `paimon-bundle-1.0.1.jar` | ✅ `Type=Paimon` |
| Hudi | `hudi-common-0.15.0.jar` | ✅ 语法通（报缺 `hive.metastore.uris`，非缺连接器）|

`iceberg.catalog.type` 的 `hadoop/hive/rest/glue/jdbc/custom` **六种全被接受**。
指向 `s3a://` 时**真的去连了 S3**（返回 `NoSuchBucket` 而非认证错误）——
说明 endpoint / 密钥 / `s3a` 方案 / `path-style` 全被接受。

**② Spark 侧是空白的。** `apache/spark:3.5.1` 里 iceberg/paimon/hudi/hadoop-aws/aws-java-sdk
**一个都没有**（252 个 jar 里只有 parquet*，Ivy 缓存 0 个）。但 Maven Central 可达，
`iceberg-spark-runtime-3.5_2.12-1.9.0.jar`（44 MB）实测 2.7 秒下完。

**需要新增的产物只有 3 个 jar + 1 个湖目录**：

| jar | 用途 |
|---|---|
| `iceberg-spark-runtime-3.5_2.12-1.9.0.jar` | Iceberg 表格式 |
| `hadoop-aws-3.3.4.jar` | **仅当**落对象存储（S3/MinIO）才需要 |
| `aws-java-sdk-bundle-1.12.262.jar` | 同上 |

**不需要 Hive Metastore** —— 用 Iceberg `hadoop` catalog（文件系统做 catalog）。

**③ 本地文件系统 + Iceberg 通路已端到端验证通过**（本次实测）：

- Spark（uid 185）写出 `file:///lake/warehouse` 下的 Iceberg 表：
  `metadata/*.metadata.json` + `metadata/*.avro` + `data/dt=*/**.parquet`（14 个文件，`ICEBERG_ROWS=3`）
- **StarRocks 容器读到了同一份文件**，`v1/v2.metadata.json` 内容完整（`format-version: 2`）
- 换成 MinIO **只需改 warehouse 路径 + 加 2 个 S3 jar**，Iceberg 代码一行不改

### ⚠️ 必须提前知道的坑（本次实测踩到）

| # | 坑 | 现象 | 解法 |
|---|---|---|---|
| 1 | **Spark 容器 uid 185 ≠ 项目目录属主 1000** | 挂载是 `RW=true` 却 `Permission denied`，Iceberg 报 `Mkdirs failed to create` | 预建湖目录并放开权限（需 `docker exec -u 0`；普通 `docker exec` 也是 uid 185，**改不动**）|
| 2 | **StarRocks FE 堆 8G vs 容器上限 3G** | `fe.conf` 写 `-Xmx8192m`，`mem_limit: 3g` | 现在数据小（`ods_order_event` 42.4 KB）撑着，**放量前必须修** |
| 3 | **MinIO 已改变分发方式** | `minio/minio` 从 Docker Hub 下架；`dl.min.io` 的 server/mc 二进制均 **410 Gone**；`quay.io`/`bitnami` 全不可用 | 免费路径只剩 `cgr.dev/chainguard/minio`（实测可拉取）。**故本阶段先用本地文件系统** |
| 4 | Chainguard MinIO 数据目录属主必须是 **uid 65532** | 否则后台扫描报 `Prefix access is denied: .minio.sys/buckets/.bloomcycle.bin`，表现为**桶建了却读不到** | `chown 65532:65532` |
| 5 | Chainguard MinIO 的 `GetObject` 返回 `AccessDenied` | `CreateBucket`/`ListBuckets`/`PutObject` 都成功、对象确实落盘（`xl.meta`），**但读不回来** | **未解决**，这也是暂缓对象存储的原因 |
| 6 | Docker 默认 `bridge` 网络**没有 DNS** | 容器间用容器名互相解析失败，S3A 会**无限重试**（实测挂死 15 分钟无输出） | 用**用户自定义网络**，或直接用 IP |
| 7 | 磁盘上看到的不是普通文件 | MinIO 把对象存成目录 + `xl.meta` | 别用 `cat` 判断对象是否存在 |
| 8 | **`fe.conf` 挂单文件 + `:ro` 会起不来** | `entrypoint.sh: line 34: .../fe.conf: Read-only file system` → 容器 `Exited (1)` | 挂**整个 `conf` 目录**（读写），不要挂单个文件 |
| 9 | **StarRocks 官方"自动算堆"会造成重复 `-Xmx`** | 设 `FE_ENABLE_AUTO_JVM_XMX_DETECT=true` 后，命令行变成 `-Xmx8192m -Xmx1075m` 两个（它只是**追加**，不替换 `fe.conf`）| 直接改 `fe.conf` 并挂载，别用这个开关 |
| 10 | **`wsl --shutdown` 后 StarRocks 容器 bind mount 可能失效** | 容器看不到宿主 `fe/meta`，会在临时层**重新初始化一个空 FE 元数据** → 表现为"数据库全没了"，但宿主数据完好 | 重建容器前先跑「金丝雀」确认挂载能解析（见下方一节） |
| 11 | **改 `.wslconfig` 后不 `wsl --shutdown` 不生效** | 只改文件，`free -h` 仍是旧值 | 改完必须 `wsl --shutdown` |
| 12 | **`docker compose down` / `--force-recreate kafka` 会丢光 topic** | Kafka 数据原在容器临时层；现已挂载到 `./kafka/logs`，但仍须遵守"先复制、再重建" | 见「Kafka 数据持久化」一节 |

### 分阶段推进清单

> 勾选即代表**验收判据全部满足**。原则：**每步可回滚、可对账、不推翻既有决定**。
> ⚠️ 湖目录与权限见「必须提前知道的坑」第 1 条 —— 这是第一步就会撞上的坎。

**第 0 步 · 修地基**（放量前必须做）—— ✅ **2026-10-10 已完成**（commit `4e60bb3`）

- [x] **修 `fe.conf` 堆与容器上限的矛盾** —— `-Xmx8192m` vs `mem_limit: 3g`（否则放量后被 OOM kill）
      —— 实测：FE 进程 `-Xmx1024m -Xms1024m`；重启后两条 Routine Load 仍 `RUNNING`、无 OOM
      —— **改法**：`fe.conf` 由"镜像自带"改为**目录挂载**，配置进项目、不再随容器重建丢失
- [x] **建湖目录并配好权限** —— Spark 以 uid 185 运行、项目目录属主是 1000，挂载 `RW=true` 也写不进
      —— 实测：`./lake` + `chmod 1777`，挂给 starrocks 与 spark 同一份；Spark 可写、StarRocks 可读
- [x] **评估 WSL 内存上限** —— 宿主 16 GB、Windows 已用 13.6 GB 时**不可上调**；清后台后调到 12 GB
      —— 实测：WSL 由 8 Gi → **11 Gi**，可用由 2.7 Gi → **8.4 Gi**
- [x] **挂载 Kafka 数据** —— 原放容器 `/tmp` 未挂载，容器重建即丢（实测 `ods_order` 已因 retention 在丢早期消息）
      —— 实测：先复制到宿主 `./kafka/logs` 再重建，offset `783/825/771`、`606/591/603` **完全保留**

**第 1 步 · 湖层只读旁路**（不动生产链路）

- [ ] **挂 Iceberg runtime jar** —— `iceberg-spark-runtime-3.5_2.12-1.9.0.jar`（44 MB；本地文件系统**不需要** S3 那两个 jar）
      —— 判据：`spark.sql("SHOW NAMESPACES IN lake")` 不报 `ClassNotFoundException`
- [ ] **重放历史 8 天灌湖** —— 用生成器（09-20 ~ 09-27），**只旁路、不碰现有链路**
      —— 判据：Iceberg 落出 `metadata/*.metadata.json` + `data/dt=*/**.parquet`
- [ ] **让 StarRocks 发现湖层表** —— 建 external catalog（`iceberg.catalog.type=hadoop`，**无需 Hive Metastore**）
      —— 判据：`SHOW DATABASES FROM <catalog>` 有库；`SELECT count(*)` 能跑通
- [ ] **行数与逐行对账** —— 湖层结果 == StarRocks 对应表
      —— 判据：行数一致；`dwd_equiv.sh` 逐行全列 diff + 逐列指纹一致
- [ ] **确认现有链路零影响**
      —— 判据：「运维命令」里各项检查全部仍然通过（800/2379/763/763/900/601/8 + 两条 DQC `EXIT=0`）

**第 2 步 · DWD 计算下沉到 Spark**（直读文件，这才消掉那 82% 的 JDBC 搬运）

- [ ] **订单明细清洗改写为 Spark SQL** —— 读湖层文件，不读 JDBC
      —— 判据：与 `dwd_order_detail` 逐行等价（763 行）+ 逐列指纹一致
- [ ] **SCD2 时点关联改写为 Spark SQL**
      —— 判据：与 `dwd_order_sku_detail` 逐行等价（763 行）+ 逐列指纹一致
- [ ] **累积快照推导改写为 Spark SQL**
      —— 判据：与 `dwd_order_lifecycle` 逐行等价（900 行）+ 阶段分布 `pay=279 ship=250 finish=180 order=111 cancel=80`
- [ ] **服务层查询不受影响** —— 必要时用 external catalog 视图过渡
      —— 判据：现有报表 SQL 结果不变
- [ ] **记录性能对比** —— 「文件直读」vs「JDBC 搬运」实测耗时
      —— 判据：给出两个引擎在同一数据量下的耗时表（这是"湖仓是否值得"的最终证据）

**第 3 步 · 放量**（先 1000 单/天跑通，再上 1 万单/天 × 90 天）

- [ ] **放量到 1000 单/天** —— 改 `NEW_ORDERS_PER_DAY` 与号段
      —— 判据：链路跑通、对账通过、**仍可重放**（同一天重跑逐字节相同）
- [ ] **逐天连续推进不漏天** —— 生成器只发"到期日 == `--date`"的事件，跳过某天就永远不补发
      —— 判据：自带的 `overdue` / `missing_prev` 两种漏发检查均为 0
- [ ] **重定分区与桶数** —— 现状 `dwd_order_detail` **28 个分区里 20 个是空的**，放量后更浪费
      —— 判据：无空分区堆积；补数不再需要手工 `ADD PARTITION`
- [ ] **放量到 1 万单/天 × 90 天**（≈270 万事件）
      —— 判据：峰值内存不越容器上限（尤其 StarRocks 3 GB）；作业不 OOM

### 待决问题

| # | 问题 | 建议 |
|---|---|---|
| 1 | MinIO 的 `GetObject AccessDenied` | 先用本地文件系统；要切对象存储时可换 **SeaweedFS**（实测可拉取、S3 兼容、活跃维护）|
| 2 | 双写一致性（Routine Load + 湖层）| 第 1 步**只重放历史**灌湖，暂不双写，避免两份摄入成本与语义分歧 |
| 3 | 湖层与 StarRocks 的共享目录 | 两者必须挂到同一路径；Spark 用 uid 185、StarRocks BE 用 root，权限需一次配好 |

### 高危操作：重建容器前的「金丝雀」检查

**背景**：2026-10-09 一次 `wsl --shutdown` 之后，StarRocks 容器的 bind mount 失效，
容器在自己的临时层**重新初始化了一个空 FE 元数据** —— 表现为 `SHOW DATABASES` 只剩 `sys`、
`ods/dwd/dws/dim/ads` 全部消失，看上去像"数据全丢了"（实际宿主数据完好）。

**结论：重建/重启 StarRocks 之前，先用一次性容器确认挂载能正确解析。**

```bash
HOST=/home/l/offline-dw/starrocks
docker rm -f sr_canary >/dev/null 2>&1
docker run -d --name sr_canary \
  -v "$HOST/fe/meta:/data/deploy/starrocks/fe/meta" \
  -v "$HOST/be/storage:/data/deploy/starrocks/be/storage" \
  starrocks/allin1-ubuntu:3.5.0 tail -f /dev/null >/dev/null 2>&1
sleep 3
docker exec sr_canary bash -c '
  grep clusterId /data/deploy/starrocks/fe/meta/image/VERSION   # 期望 938046516
  ls /data/deploy/starrocks/fe/meta/image/v2/ | wc -l           # 期望 2
  ls /data/deploy/starrocks/fe/meta/bdb | wc -l                 # 期望 33
  cat /data/deploy/starrocks/be/storage/cluster_id              # 期望 938046516-3.5.0'
docker rm -f sr_canary >/dev/null 2>&1
```

四项都符合期望 → 可以安全重建。**任何一项不符，先别动。**

### Kafka 数据持久化（2026-10-10 起）

数据目录已挂载到宿主 `./kafka/logs`（容器内仍是 `/tmp/kraft-combined-logs`）。
**首次挂载时必须"先复制、再重建"**，否则宿主空目录会盖住容器里的数据、重建后即丢失：

```bash
cd /home/l/offline-dw
docker compose stop kafka
mkdir -p kafka/logs
docker cp kafka:/tmp/kraft-combined-logs/. kafka/logs/   # 先搬家
docker compose up -d kafka                                # 再重建
```

**决定性的持久化验证**（重建后 offset 必须一字不差）：

```bash
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ods_order_event > /tmp/before.txt
cd /home/l/offline-dw && docker compose up -d --force-recreate kafka && sleep 25
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ods_order_event > /tmp/after.txt
diff /tmp/before.txt /tmp/after.txt && echo "✅ 持久化成功"
```

> ⚠️ 宿主 `kafka/logs` 的属主必须是 **uid 1000**（= 容器内 `appuser`，也 = 宿主用户 `l`）。
> 用 `sudo mkdir` 建成 root 会导致 Kafka 建不出 `.lock` 而**直接退出**。

### 开机后 Routine Load 被暂停（已知现象）

**每次开机，StarRocks 常先于 Kafka 就绪**，BE 连不上 Kafka 会把作业**暂停**：

```
ReasonOfStateChanged: errCode = 2, msg='... Connect to ipv4#172.18.0.3:29092 failed: Connection refused'
```

**这不是数据问题，也不是消息过期 —— 是连接失败。** `PAUSED` 不会自愈，需要手工恢复：

```bash
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "SHOW ROUTINE LOAD FROM ods\G" | grep -E "Name:|State:|ReasonOfStateChanged"
# errCode=2（数据源连不上）→ 安全恢复：
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "RESUME ROUTINE LOAD FOR ods.ods_order_event_load;"
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "RESUME ROUTINE LOAD FOR ods.ods_order_load;"
```

> ⚠️ **恢复后必须复核事件表不变式**（`DUPLICATE KEY` 表重复会真的多出行）：
> `SELECT count(*)`, `count(DISTINCT concat(order_id,'-',event_type))` 两者应**始终相等**（当前 2379）。
> ⚠️ 若暂停原因是 `max_filter_ratio=0` 撞到坏数据（**errCode 不同**），**绝不可自动 RESUME** —— 会无限撞错，这正是"宁停不脏"的设计意图。
> ⚠️ `RESUME` 后短暂出现 `NEED_SCHEDULE` 是**正常中间态**，几秒后自动转 `RUNNING`；健康检查已单独处理它，不会误报警。

---

## 重要提醒

**DolphinScheduler 的工作流定义只存在于 MySQL 里**，不是文件。

- 用「导出工作流」功能导出 JSON，放进仓库 `dolphin/` 目录
- 否则一旦 MySQL 数据卷损坏，所有工作流都要手工重建
- **文件名用工作流名，不要用 DS 自动生成的 `workflow_<时间戳>.json`**：

```
dolphin/
├── offline_dataware.json      ← 订单链路
└── dim_product_chain.json     ← 商品链路
```

**为什么要固定文件名**：时间戳命名每次导出都产生新文件，`dolphin/` 越堆越多、分不清哪份是当前的，而且 **git 里看不到"改了什么"**。固定文件名可以覆盖式更新，`git diff dolphin/dim_product_chain.json` 就能直接看出"这次给 DAG 加了哪个节点"。

### ⚠️ 导出的 JSON 有两个不可信之处

**① `schedule.releaseState` 永远是 `OFFLINE`**

实测：订单链路的定时**确实在上线运行**（每天 02:00 都在跑），但它导出的 JSON 里同样写着 `OFFLINE`。这是 **DS 3.2.0 导出功能的固有行为**，不是你的配置有问题。

> **含义：从 JSON 导入工作流后，定时默认是「下线」状态，必须手工点一次「上线」。** 否则你会以为恢复了，其实定时没生效。

**② 它不含节点脚本内容**

节点体里只有"去读 `sql/xxx.sql`"或"执行 `scripts/xxx.sh`"（见「为什么节点引用文件」）。**所以恢复时必须同时拿到仓库的 `sql/` 和 `scripts/`** —— 单靠一个 JSON 跑不起来。

**要确认工作流的真实状态，查数据库，不要看 JSON**（命令见「运维命令」）。
