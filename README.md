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
  Python 脚本  ──►  Kafka  ──►  Spark  ──►  StarRocks
  模拟订单        消息队列      批处理       ODS → DWD → DWS → ADS

  订单事件 / 累积快照链路（DUPLICATE 事件流 → 累积快照事实表）
  Python 脚本  ──►  Kafka  ──►  Spark  ──►  StarRocks
  模拟事件流       ods_order_event  增量摄入   ODS → DWD(累积快照 dwd_order_lifecycle)
  ⚠️ 与上面一条独立：不同 topic、不同表、不同 Spark 脚本

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
| 传输方式 | Kafka + Spark | CSV 文件 + Stream Load |
| 同步节奏 | 按天增量 | 按天全量快照 |

真实业务里商品也是整表导出走 DataX，不走消息队列 —— **事件用流、实体用快照**是通用的分层原则。

| 组件 | 版本 | 职责 | 端口 |
|---|---|---|---|
| Kafka | 3.8.1 | 消息队列，数据入口 | 9092 |
| Spark | 3.5.1 | 从 Kafka 读数据、解析 JSON、写入 StarRocks | — |
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
│   ├── ods_order_to_starrocks.py    Spark 作业：Kafka ods_order     → ODS
│   ├── ods_order_event_to_starrocks.py  Spark 作业：Kafka ods_order_event → ODS（增量，维护位点）
│   ├── ods_order_event_ingest.sh    事件流摄入外壳（防线 + 位点表替换）
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

首次跑 Spark 作业（要下载约 30MB 依赖）：

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.4.0 \
  /opt/offline-dw/scripts/ods_order_to_starrocks.py
```

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

# ② 摄入到 ODS
bash scripts/ods_order_event_ingest.sh

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

**两个工作流，商品链路依赖订单链路**：

```
┌─ offline_dataware（订单链路，每天 02:00，exec_type=1 串行等待）───────┐
│  ods_spark → dwd_delete → dws_agg → ads_metric → dqc_order_chain    │
│    Shell       Shell        SQL        SQL           Shell           │
│  ⚠️ dwd_delete 名字骗人，真身是 INSERT OVERWRITE ... PARTITION       │
│  ⚠️ truncate_ods 节点已删除（ODS 改主键模型后不再需要先清空）        │
└──────────────────────────────────────────────────────────────────────┘
                              │ 今天成功
                              ▼
┌─ dim_product_chain（商品链路，每天 03:00，exec_type=2 串行丢弃）──────┐
│  wait_order_chain → truncate_and_load_ods → dim_product_load →       │
│     DEPENDENT            Shell                   Shell                │
│        → dim_product_scd2_load → dwd_sku_reload → dq_check            │
│                 Shell                 Shell          Shell            │
└──────────────────────────────────────────────────────────────────────┘

┌─ （待建）订单事件 / 累积快照链路 ────────────────────────────────────┐
│  ods_event_spark → dwd_lifecycle_load                                │
│     Shell(Spark)      Shell(dwd_order_lifecycle_load.sh)             │
│  建议 02:30（订单链路之后）；触发侧生成器仍在调度外                   │
└──────────────────────────────────────────────────────────────────────┘
```

### 商品链路为什么「定时」和「依赖」两个都要

| | 作用 | 缺了会怎样 |
|---|---|---|
| **定时 03:00** | **触发**工作流 | 没有任何东西会启动它，`wait_order_chain` 永远不会被评估 |
| **`wait_order_chain`**（依赖节点）| **确认**订单链路今天已经成功 | 订单链路慢或失败时会读到过期的 `dwd_order_detail`，算出错的结果 |

依赖节点配置：类型「工作流」→ `offline_dataware` → 任务「**ALL**」→ 周期「今天」→ 失败策略「**等待**」。

### 两个工作流都必须用「串行丢弃」

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
| ODS（订单） | **`PRIMARY KEY(order_id)` 表模型** —— 重复消息自动折叠（原来的 `truncate_ods` 节点已因此删除）|
| ODS（事件）⚠️ | **消费位点表 `ods_kafka_offset` + 只读新消息（增量）** —— **不要用"清表 + 全量重灌"**，Kafka 有 retention，消息过期后清表 = 清库（见「已知限制」）|
| ODS（商品） | 工作流开头 `TRUNCATE ods_product`，再全量重灌所有快照；Stream Load 标签**每次运行唯一** |
| DWD（订单） | `INSERT OVERWRITE ... PARTITION (p<日期>)`，原子覆盖当天分区 |
| DWD（累积快照） | `PRIMARY KEY(order_id)` + **`INSERT` 即 UPSERT**（只回填"那天及之后有事件"的订单）|
| DWD（商品宽表） | 同上，`dwd_sku_load.sh` 逐天 `INSERT OVERWRITE` |
| DIM（SCD1） | `PRIMARY KEY` 表模型，同键自动覆盖 |
| **DIM（SCD2）** | **`TRUNCATE` + `INSERT` 合一的装载 SQL**（主键挡不住版本漂移，见上文） |
| DWS / ADS | `PRIMARY KEY` 表模型，同键自动覆盖 |

**验证方法**：连续执行两次工作流，对比三层的行数和金额，必须完全一致。

> **⚠️ 事件流的幂等是"不能靠表模型"的**：`ods_order_event` 是 `DUPLICATE KEY`（重复原样保留），
> 所以幂等必须在**摄入层**保证（位点表）。而 `MAX(CASE WHEN event_type='x' ...)` 这类聚合
> 会**把重复折叠成同一个值 → 值级对账看不见重复**。装载脚本因此专门加了
> 「事件表行数 = `(order_id,event_type)` 去重对数」这道**行级**防线（见 PITFALLS §3.17）。

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

# 事件流摄入（增量：只读位点之后的新消息；--reset 才是清表重灌）
bash scripts/ods_order_event_ingest.sh

# 数据质量检查的【自检】：用内存里的假数据证明检查真的能发现问题
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dqc_dim_product_selftest.sql

# 【指纹】跑工作流前后各执行一次，输出必须一字不差
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/fingerprint_product_chain.sql

# 两个工作流的真实状态（权威来源，导出 JSON 不可信，见「重要提醒」）
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

1. **ODS（订单）每次从 Kafka 全量重读**（`startingOffsets=earliest`），靠主键模型折叠重复所以**结果正确**，但数据量大了会变慢。**这是性能债，不是正确性债。**
2. **ODS（事件）曾经不幂等** —— 全量重读 + `append` 写 `DUPLICATE KEY` 表 → 每次重跑翻倍，且**值级对账看不出来**。**修法：位点表增量**（待实施，见「幂等性」）。
3. **⚠️ "先 TRUNCATE 再从 Kafka 全量重灌"不是长期方案** —— Kafka `log.retention.hours=168`（7 天），消息过期后清表就等于**清库**；而且 topic 被**部分**裁剪时，"topic 非空"的检查拦不住，会灌进残缺数据。它只能当**回补手段**，且必须配"进度不得超过 Kafka 现存最早 offset"的防线。
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
    【DS 02:00】offline_dataware    : ods_spark → dwd_delete → dws_agg → ads_metric → dqc
    【DS 02:30】order_event_chain   : ods_event_spark → dwd_lifecycle_load
    【DS 03:00】dim_product_chain   : wait_order_chain → … → dwd_sku_reload → dq_check
    ```
    ⚠️ **事件生成器有状态**（读 `ods_order_event` 判断该发什么），**必须逐天按顺序跑，跳过某天就永远不补发**。
    ⚠️ **不跑生成器时，DS 工作流仍会"成功"但什么都没做**（摄入节点打印 `没有新消息，退出`，退出码 0）—— 这是设计如此。判据：日志里有没有 `本次从 Kafka 读到 N 行`。
12. **`dwd_order_sku_detail` 的范围 JOIN 每天付一次代价** —— 这是"物化换查询速度"的必然代价。
13. **补数要手工补分区** —— `dwd_order_sku_detail` 缺 `p20260915`~`p20260919` 等分区；动态分区**只创建"未来"，不创建历史**（`history_partition_num=0`）。补数进来的新日期，必须先照 `sql/dwd_order_sku_detail_add_partitions.sql` 手工 `ADD PARTITION`（且**必须先 `dynamic_partition.enable=false`**，理由见 PITFALLS §3.3）。**这是动态分区的固有行为、属运维常规动作**；想省掉它可改表达式分区，见「后续方向 → 可选架构改进」。
14. **`wait_order_chain` 依赖的是"今天"的实例** —— 跨天补数时，依赖检查会对不上，需要单独手工执行。

## 后续方向

- [x] 维度建模：商品维度、缓慢变化维（SCD1 + SCD2 拉链表）
- [x] **把商品/维度链路接进 DolphinScheduler**（含跨工作流依赖 + 定时）
- [x] 数据质量检查节点（DQC）—— 6 项检查 + 自检
- [x] 用 DS **补数**回填历史数据（**实测两个坑**：`${system.biz.date}` = 调度日期 −1 天；执行方式必须选「串行执行」，否则被"串行丢弃"静默丢掉）
- [x] 补上 `ods_order` 里 09-22~09-25 那 4 天（DWD 从 4 天/416 行 → **8 天/942 行**）
- [x] **DQC 加一条「重算对账」** —— `⑥ 重物化属性一致`：宽表里的商品属性必须等于 SCD2 对该日期算出的属性。盖住两个盲区：属性值不同、以及 **JOIN 不上的孤儿行**（范围 JOIN 不满足时那行会直接消失，计数纹丝不动）。写 `<=>` 而非 `<>`（NULL 安全），用 `LEFT JOIN` 而非 `NOT EXISTS`（StarRocks 不支持关联子查询里的非等值谓词）
- [x] 作业失败告警（邮件 / 钉钉）—— DS 里两个工作流都配了 `warning_type=2` + 告警组
- [x] SCD2 改增量维护，并与全量重建做等价性验证（`scripts/dim_product_scd2_incremental.sh`，指纹一字不差）
- [x] **累积快照事实表**（下单 → 支付 → 发货 → 完成）—— 表/视图/装载/五道防线，**09-20 ~ 09-28 共 900 行**，卡单三类可见
- [x] **订单事件链路的摄入改增量**（`ods_kafka_offset` 位点表）—— 幂等已实测（重跑 `读到 0 行`）
- [x] **把订单事件 / 累积快照链路接进 DS**（工作流三 `order_event_chain`：`ods_event_spark` → `dwd_lifecycle_load`，02:30，失败策略 `END`）
- [x] **累积快照逐天回放**（09-20 ~ 09-28，`dwd_order_lifecycle` **900 行**，卡单三类可见）
- [ ] 把 DWD 清洗逻辑搬到 Spark SQL（上规模后）
- [ ] ODS 改用 StarRocks Routine Load（省掉 Spark 这一跳）
- [x] `docs/dimension-modeling.md`：维度建模 + SCD2 完整说明

**可选架构改进**（不是待办任务 —— 现状能正常工作，属于"想省掉人工操作"时才做）：

- [ ] `dwd_order_sku_detail` 改**表达式分区** —— 现在是动态分区（`dynamic_partition.history_partition_num=0`：**只建未来、不建历史**），所以补历史某天前需要手工 `ALTER TABLE ... ADD PARTITION`。**这是 StarRocks 动态分区的固有行为，属运维常规动作**（建表时也用 `sql/dwd_add_history_partitions.sql` 补过 `p20260915`~`p20260919`）；改成表达式分区可一劳永逸消掉它（PITFALLS §3.2 推荐过）

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
