# DolphinScheduler 工作流配置

工作流定义**只存在于 DS 的 MySQL 里，不是文件**。本文是它的完整说明书，用于重建。

> **强烈建议**：在 DS 里用「导出工作流」导出 JSON，存进仓库 `dolphin/` 目录。否则 MySQL 数据卷一旦损坏，所有配置都要手工重建。

---

## 前置准备

### 1. 注册 StarRocks 数据源

**数据源中心 → 创建数据源**

| 字段 | 值 |
|---|---|
| 数据源 | **MySQL**（StarRocks 兼容 MySQL 协议） |
| 数据源名称 | `starrocks` |
| IP/主机名 | **`starrocks`**（Docker 服务名，不是 localhost） |
| 端口 | `9030` |
| 用户名 | `root` |
| 密码 | 留空 |
| 数据库名 | `dwd` |

### 2. 让 DS 容器能跨容器调用 Spark

DS 的 Shell 任务只在**自己容器内部**执行，看不见隔壁的 Spark 容器。解法是给 DS 容器挂上 Docker 的控制接口。

**`docker-compose.yml` 里给 `dolphinscheduler` 服务加两行挂载：**

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
  - ./ds/bin/docker:/usr/local/bin/docker:ro
```

**`ds/bin/docker` 是静态编译的 Docker CLI**（下载方法见 README）。

重建容器并验证：

```bash
docker compose up -d dolphinscheduler
sleep 40
docker compose exec dolphinscheduler docker ps
```

**能列出宿主机上的所有容器，才算打通。**

> **为什么重建 DS 容器是安全的**：工作流定义、调度配置全在 MySQL 里。这正是当初把默认的 H2 内存数据库换成 MySQL 的价值——用 H2 的话这次重建会丢光所有工作流。

---

## 工作流结构

工作流名 `offline_dataware`，5 个节点串行：

```
① truncate_ods ──► ② ods_spark ──► ③ dwd_overwrite ──► ④ dws_agg ──► ⑤ ads_metric
     SQL              Shell              Shell               SQL           SQL
   非查询                                非查询             非查询         非查询
```

**「SQL 类型」全部选「非查询」**——这些语句都不返回结果集。选「查询」DS 会一直等结果，最后超时。

---

## ① truncate_ods

| 字段 | 值 |
|---|---|
| 任务类型 | SQL |
| 数据源 | `starrocks` |
| SQL 类型 | 非查询 |

```sql
TRUNCATE TABLE ods.ods_order
```

**作用**：清空 ODS，让后面的 Spark 作业从 Kafka 全量重建。

`TRUNCATE` 只清数据，**保留分区结构**。

---

## ② ods_spark

| 字段 | 值 |
|---|---|
| 任务类型 | SHELL |

```bash
docker exec spark /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --repositories https://maven.aliyun.com/repository/public \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.4.0 \
  /opt/offline-dw/scripts/ods_order_to_starrocks.py
```

**作用**：读 Kafka 全量消息，解析 JSON，写入 `ods.ods_order`。

**注意**：`docker exec` 的执行者是 Docker 引擎，不是 DS 容器——所以能进到 `spark` 容器里，网络隔离不是障碍。

**失败时看日志**：`从 Kafka 读到的行数: N` 这行说明读到了多少。如果是 0，检查 Kafka 里有没有数据。

---

## ③ dwd_overwrite

| 字段 | 值 |
|---|---|
| 任务类型 | **SHELL**（不是 SQL，原因见下） |

```bash
D=${system.biz.date}
DF="${D:0:4}-${D:4:2}-${D:6:2}"
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
INSERT OVERWRITE dwd.dwd_order_detail PARTITION (p${D})
SELECT
    order_id, dt, user_id, product_id, amount, order_time, status
FROM (
    SELECT
        order_id,
        dt,
        user_id,
        product_id,
        ABS(amount) AS amount,
        order_time,
        status,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ABS(amount) DESC
        ) AS rn
    FROM ods.ods_order
    WHERE user_id IS NOT NULL
      AND dt = '${DF}'
) t
WHERE rn = 1;
"
```

### 为什么这一跳必须用 Shell 任务

**DS 的 SQL 任务把 `${param}` 编译成 JDBC 的 `?` 占位符**（不是文本替换），而 `?` 只能填"值"，不能填"名字"。

```sql
PARTITION (p${bizdate})   →   PARTITION (p?)   ← 语法错误，无解
```

**Shell 任务是纯文本替换**，所以：

| 变量 | 值 | 用途 |
|---|---|---|
| `$D` | `20260920` | **分区名** `p20260920`（标识符，不能加引号） |
| `$DF` | `2026-09-20` | **日期值** `'2026-09-20'`（字符串，要加引号） |

`${D:0:4}` 是 shell 的字符串切片（从第 0 位取 4 个字符）。**一个参数变成两种格式，SQL 任务做不到这件事。**

### 为什么用 INSERT OVERWRITE 而不是 DELETE + INSERT

**StarRocks 的 `DELETE ... WHERE` 只接受字面量**，不接受函数或占位符：

```sql
DELETE FROM t WHERE dt = STR_TO_DATE('...', '%Y%m%d')
-- Right expr of binary predicate should be value.
```

原因是 StarRocks 的删除**不当场执行**——它记录一条"删除谓词"到元数据，后续读取时过滤。**谓词要长期保存，所以条件必须能写死。**

**而且实测 `DELETE` + `INSERT` 会导致数据翻倍**（82 → 164），删除谓词在同一个会话里没有立即生效。

**`INSERT OVERWRITE` 是原子的，要么全换要么不变。**

### 日期参数

`${system.biz.date}` 是 DS 的内置参数，代表"业务日期"（昨天），格式 `yyyyMMdd`。

- **手动点「执行」** → 处理昨天
- **用「补数」功能** → 处理指定的历史日期

**去重只在当天范围内生效**——同一 `order_id` 跨天出现会在两个分区各留一份。这是增量处理的固有边界，不是 bug。

---

## ④ dws_agg

| 字段 | 值 |
|---|---|
| 任务类型 | SQL |
| 数据源 | `starrocks` |
| SQL 类型 | 非查询 |

```sql
INSERT INTO dws.dws_user_order_day
SELECT
    user_id,
    dt,
    COUNT(*) AS order_cnt,
    COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
    SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
    SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
FROM dwd.dwd_order_detail
WHERE dt = STR_TO_DATE('${system.biz.date}', '%Y%m%d')
GROUP BY user_id, dt;
```

**这里是 SQL 任务，可以用 `${system.biz.date}`**——因为它只出现在**值的位置**（引号里面），JDBC 的 `?` 能正常工作。

**`INSERT INTO` 就够了，不需要 `OVERWRITE`**：DWS 是 `PRIMARY KEY` 表，同键自动覆盖，天然幂等。

### 条件聚合的写法

| 列 | 读法 |
|---|---|
| `COUNT(CASE WHEN status='paid' THEN 1 END)` | paid 的行返回 1，其余返回 NULL；`COUNT` 不数 NULL → **paid 的条数** |
| `SUM(CASE WHEN status='paid' THEN amount ELSE 0 END)` | paid 的行取金额，其余取 0 → **paid 的金额合计** |

**`SUM` 要写 `ELSE 0`，`COUNT` 不用**：某用户当天一单都没付时，`SUM` 在全 NULL 情况下返回 `NULL`，报表会显示空白；有 `ELSE 0` 才返回正确的 `0`。

---

## ⑤ ads_metric

| 字段 | 值 |
|---|---|
| 任务类型 | SQL |
| 数据源 | `starrocks` |
| SQL 类型 | 非查询 |

```sql
INSERT INTO ads.ads_daily_sales
SELECT
    dt,
    SUM(order_cnt)                                  AS order_cnt,
    SUM(paid_cnt)                                   AS paid_cnt,
    SUM(paid_amount)                                AS paid_amount,
    SUM(refund_amount)                              AS refund_amount,
    SUM(paid_amount) - SUM(refund_amount)           AS net_amount,
    ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt), 0), 2)   AS avg_order_amount,
    ROUND(SUM(paid_cnt) / NULLIF(SUM(order_cnt), 0), 4)     AS pay_rate
FROM dws.dws_user_order_day
WHERE dt = STR_TO_DATE('${system.biz.date}', '%Y%m%d')
GROUP BY dt;
```

### 两个细节

**① 派生指标必须先各自 `SUM` 再运算**

```sql
SUM(paid_amount) - SUM(refund_amount)     ✓
SUM(paid_amount - refund_amount)          ✗ 换成比率时结果会完全不同
```

**原子指标（可加）和派生指标（不可加）要分清**：订单数、金额合计往上汇总永远安全；支付率、客单价必须在最终粒度上现算。

**② `NULLIF(x, 0)` 是除零保护**

`NULLIF(a, b)` = "a 等于 b 就返回 NULL，否则返回 a"。

当某个日期支付订单数是 0 时，分母变 `NULL`，结果是 `NULL` 而不是**报错**。

**这是给定时任务准备的**：手工跑时数据总有支付订单看不出问题；哪天上游异常、某天真的零支付，作业会半夜崩掉，你第二天早上才发现。

---

## 定时

**工作流定义 → 定时 → 新建**

| 字段 | 值 |
|---|---|
| 开始时间 | **改成今天**（默认是次日，不改会干等一天） |
| Cron | `0 0 2 * * ?` |
| 时区 | 确认 `Asia/Shanghai` |

```
0    0    2    *    *    ?    *
秒   分   时   天   月   周   年
```

**DS 的 cron 是 6~7 位**（最前面多个"秒"），不是 Linux 的 5 位。「周」通常写 `?` 而不是 `*`。

**两个「上线」缺一不可**：工作流定义要上线，定时任务也要上线。

---

## 验证幂等

**连续执行两次，三层数据必须完全一致。**

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT (SELECT count(*) FROM dwd.dwd_order_detail)   AS dwd,
       (SELECT count(*) FROM dws.dws_user_order_day) AS dws,
       (SELECT count(*) FROM ads.ads_daily_sales)    AS ads_days;"
```

| 层 | 靠什么保证幂等 |
|---|---|
| ODS | 开头 `TRUNCATE`，从 Kafka 全量重建 |
| DWD | `INSERT OVERWRITE ... PARTITION (p<日期>)` |
| DWS / ADS | `PRIMARY KEY` 表模型，同键覆盖 |

**还有一个更强的验证**：手工清空所有下游表 → 确认全是 0 → 点执行 → 看数据自己回来。

**"先破坏，再重建"比"对比结果"强得多**——它证明的不只是结果对，而是整个链路真的在跑。

---

## 补数（回填历史）

配了定时之后，手动执行只处理"昨天"。要回填历史某几天：

**工作流实例页面 → 找「补数」入口 → 选日期范围 → 执行**

DS 会为范围内每一天生成一次执行，并把那天的日期作为"业务日期"传给 `${system.biz.date}`。

> 补数有一个前提：**DWD 表里那一天的分区必须已经存在**。动态分区不会回溯创建历史分区，需要先手工 `ALTER TABLE ... ADD PARTITION`（见 `sql/dwd_add_history_partitions.sql`）。
