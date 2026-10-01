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
  订单链路（已接入 DS 调度）
  Python 脚本  ──►  Kafka  ──►  Spark  ──►  StarRocks
  模拟订单        消息队列      批处理       ODS → DWD → DWS → ADS

  商品 / 维度链路（手工执行，尚未接调度）
  Python 脚本  ──►  CSV  ──►  Stream Load  ──►  StarRocks
  模拟商品快照     文件同步     HTTP 导入       ODS → DIM(SCD2) → DWD
```

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
│   ├── gen_mock_orders.py          模拟订单生成器
│   ├── ods_order_to_starrocks.py   Spark 作业：Kafka → ODS
│   ├── dwd_overwrite.sh            DWD 按天覆盖（Shell，给 DS 用）
│   ├── gen_mock_products.py        模拟商品快照生成器（支持 --date 造历史）
│   ├── load_product_to_ods.sh      商品 CSV → ODS（Stream Load）
│   └── dwd_sku_load.sh             商品宽表逐天物化
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
SELECT sum(CASE WHEN next_from IS NULL THEN 0
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

## 调度

工作流 `offline_dataware`，5 个节点串行：

```
truncate_ods → ods_spark → dwd_overwrite → dws_agg → ads_metric
    SQL          Shell          Shell          SQL        SQL
```

- 日期参数用内置的 `${system.biz.date}`（`yyyyMMdd`，即"昨天"）
- **DWD 用 Shell 任务而不是 SQL 任务**（原因见 PITFALLS：DS 的 SQL 任务参数是 JDBC 绑定，拼不了分区名）
- 定时：`0 0 2 * * ?`（每天凌晨 2 点）
- 回填历史数据用 DS 的**补数**功能

详细节点配置见 `docs/dolphinscheduler-workflow.md`。

---

## 幂等性

**整个链路可以反复重跑，结果不变。**

| 层 | 靠什么保证 |
|---|---|
| ODS（订单） | 工作流开头 `TRUNCATE`，从 Kafka 全量重建 |
| ODS（商品） | 用文件名解析出的 `label` 做 Stream Load 事务标签，**同一天重复导入会被拒绝** |
| DWD（订单） | `INSERT OVERWRITE ... PARTITION (p<日期>)`，原子覆盖当天分区 |
| DWD（商品宽表） | 同上，`dwd_sku_load.sh` 逐天 `INSERT OVERWRITE` |
| DIM（SCD1） | `PRIMARY KEY` 表模型，同键自动覆盖 |
| **DIM（SCD2）** | **`TRUNCATE` + `INSERT` 合一的装载 SQL**（主键挡不住版本漂移，见上文） |
| DWS / ADS | `PRIMARY KEY` 表模型，同键自动覆盖 |

**验证方法**：连续执行两次工作流，对比三层的行数和金额，必须完全一致。

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

1. **ODS 每次从 Kafka 全量重读**，Kafka 消息只增不减（受保留策略控制）。数据量大了会变慢。
2. **DWD 去重只在单天内生效** —— 同一 `order_id` 跨天出现会在两个分区各留一份。增量处理的固有边界。
3. **StarRocks 用的是 allin1 单容器**（FE + BE 合一），仅供开发验证，不能上生产。
4. **Kafka 数据未挂载**（放在容器内 `/tmp`），容器重建即丢失。ODS 层靠工作流重跑重建。
5. **Spark 依赖缓存也在容器内**（`/tmp/.ivy2`），容器重建要重新下载 30MB。
6. **SCD2 是全量重建**（`TRUNCATE` + 从 ODS 完整重推）—— 快照天数一多会变慢，增量维护尚未实现。
7. **商品链路未接入 DS 调度** —— 目前是手工执行 `bash scripts/*.sh`。
8. **`dwd_order_sku_detail` 的范围 JOIN 每天付一次代价** —— 这是"物化换查询速度"的必然代价。

## 后续方向

- [x] 维度建模：商品维度、缓慢变化维（SCD1 + SCD2 拉链表）
- [ ] **把商品/维度链路接进 DolphinScheduler**
- [ ] 数据质量检查节点（DQC）—— 把上面的 4 条不变式固化成 DS 节点
- [ ] SCD2 改增量维护，并与全量重建做等价性验证
- [ ] 累积快照事实表（下单 → 支付 → 发货 → 完成）
- [ ] 作业失败告警（邮件 / 钉钉）
- [ ] 用 DS 补数回填历史数据（**注意：DWD 那天的分区必须先存在**）
- [ ] 把 DWD 清洗逻辑搬到 Spark SQL（上规模后）
- [ ] ODS 改用 StarRocks Routine Load（省掉 Spark 这一跳）
- [ ] `docs/dimension-modeling.md`：维度建模 + SCD2 完整说明

---

## 重要提醒

**DolphinScheduler 的工作流定义只存在于 MySQL 里**，不是文件。

- 建议在 DS 里用「导出工作流」功能导出 JSON，放进仓库 `dolphin/` 目录
- 否则一旦 MySQL 数据卷损坏，所有工作流都要手工重建
