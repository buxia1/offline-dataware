# DolphinScheduler 工作流配置

工作流定义**只存在于 DS 的 MySQL 里，不是文件**。本文是它的完整说明书，用于重建。

> **强烈建议**：在 DS 里用「导出工作流」导出 JSON，存进仓库 `dolphin/` 目录。否则 MySQL 数据卷一旦损坏，所有配置都要手工重建。

**本项目有两个工作流**：

| 工作流 | 作用 | 定时 | 执行策略 |
|---|---|---|---|
| `offline_dataware` | 订单链路（Kafka → ODS → DWD → DWS → ADS）| 每天 02:00 | **串行丢弃** |
| `dim_product_chain` | 商品 / 维度链路（CSV → ODS → DIM → DWD）| 每天 03:00 | **串行丢弃** |

**`dim_product_chain` 依赖 `offline_dataware`**（因为商品宽表要读订单链路的 `dwd_order_detail`）—— 见文末「工作流二：dim_product_chain」。

> ⚠️ **导出的 JSON 有两个不可信之处**
> ① `schedule.releaseState` **永远是 `OFFLINE`**（实测：订单链路的定时明明在上线运行，导出里也写 OFFLINE）。所以**导入后定时默认下线，必须手工点一次「上线」**。
> ② JSON 里**不含节点脚本内容**，只有文件路径（`sql/xxx.sql`、`scripts/xxx.sh`）。恢复时必须同时拿到仓库的 `sql/` 和 `scripts/`。

---

# 工作流一：offline_dataware

订单链路（Kafka → ODS → DWD → DWS → ADS），5 个节点。

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
① truncate_ods ──► ② ods_spark ──► ③ dwd_delete ──► ④ dws_agg ──► ⑤ ads_metric
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

## ③ dwd_delete

> **关于这个节点的名字**：它叫 `dwd_delete`，但实际做的是 `INSERT OVERWRITE`，**不是 `DELETE`**。
> 名字是早期用 `DELETE + INSERT` 时留下的（那个写法实测会让数据翻倍，见下），后来改成 `INSERT OVERWRITE` 但没改名。
> **这只是历史遗留的命名，不影响功能。** 本文档以前误写成 `dwd_overwrite`，已按现网实际名称更正。

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

---
---

# 工作流二：dim_product_chain

商品 / 维度链路。**6 个节点，全部是 Shell 任务**（`wait_order_chain` 是 DEPENDENT）。

## 结构

```
wait_order_chain ──► truncate_and_load_ods ──► dim_product_load
   DEPENDENT               Shell                     Shell
        ──► dim_product_scd2_load ──► dwd_sku_reload ──► dq_check
                  Shell                     Shell           Shell
```

## 为什么全部用 Shell 任务

1. **`dim_product_scd2_load` 必须在一次执行里跑 `TRUNCATE` + `INSERT`**。SQL 节点能否跑多语句取决于数据源有没有开 `allowMultiQueries`（不确定）；**mysql CLI 原生支持多语句**，没有这个不确定性。
2. **节点体里一个 `${...}` 都没有**，所以 DS 不会做参数替换（`${D}`、`${DF}` 这些占位符都在 SQL 文件里）。

## 为什么节点引用文件而不是内联 SQL

节点体只有两类内容：

- `docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < /opt/offline-dw/sql/xxx.sql`
- `bash /opt/offline-dw/scripts/xxx.sh`

**代价**：导出的 JSON 不再自包含 —— 恢复时必须同时拿到仓库的 `sql/` 和 `scripts/`。

**收益**：同一段逻辑只存在一处，**改文件即生效**，不会再出现"节点里改了、文件里忘了改"的漂移（这个坑踩过一次：`sql/dwd_order_detail_insert.sql` 用的是早已废弃的 `${bizdate}`）。

> **所以 `docker-compose.yml` 里给 `dolphinscheduler` 容器挂了 `./sql` 和 `./scripts`（只读），给 `spark` 容器挂了 `./data`。**
> **副作用**：`sql/`、`scripts/` 里的文件名和路径**不能随便改** —— 一改名节点里写死的路径就失效。

## ① wait_order_chain

| 字段 | 值 |
|---|---|
| 任务类型 | **DEPENDENT**（在「逻辑节点」分组里，不是「通用组件」）|
| 依赖类型 | 工作流 |
| 项目名称 | 当前项目 |
| 工作流名称 | `offline_dataware` |
| 任务名称 | **`ALL`**（整个工作流，不是某个单节点）|
| 时间周期 | 日 / **今天** |
| 检查间隔 | 10 秒 |
| 依赖失败策略 | **等待** |
| 依赖失败等待时间 | 30~60 分（默认 1 分太紧）|
| 前置任务 | （空，它是第一个节点）|

**作用**：确认订单链路**今天已经成功**，然后才继续。

**为什么用「等待」而不是「失败」**：商品宽表要读 `dwd_order_detail`。订单链路今天慢了或还没跑完时，商品链路应该**等** —— 直接失败只会让你第二天早上看到一个无意义报错。

**⚠️ 这个节点只负责「检查」，不负责「触发」。** 它自己不会启动工作流，所以商品链路**必须另有定时**，否则这个节点永远不会被评估。这就是"定时 + 依赖"两个都要的原因。

## ② truncate_and_load_ods

| 字段 | 值 |
|---|---|
| 任务类型 | SHELL |
| 前置任务 | `wait_order_chain` |

```bash
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "TRUNCATE TABLE ods.ods_product"

docker exec -e SR_HOST=starrocks spark bash -c 'for f in /opt/offline-dw/data/dim/product_snapshot_*.csv; do bash /opt/offline-dw/scripts/load_product_to_ods.sh "$f" || exit 1; done'
```

**四个要点**：

1. **`TRUNCATE` 和导入必须在同一个节点里** —— `ods_product` 是 `DUPLICATE KEY`（只追加），不清空就会叠加出重复行。
2. **`-e SR_HOST=starrocks`** —— 脚本默认 `localhost`（宿主上用），但在容器里 `localhost` 是容器自己，必须传容器名。
3. **`\$f` 的反斜杠不能少** —— 外层是单引号，要让 `$f` 在 **spark 容器里**展开，不能被 DS 容器的 shell 提前吃掉。
4. **`|| exit 1`** —— 任何一个 CSV 失败就立刻中断，而不是继续灌下一个。

**为什么这条路要绕到 spark 容器**：DS 容器**没有 python3**，而 `load_product_to_ods.sh` 用 python3 解析 Stream Load 的返回体（`Status` / `NumberLoadedRows`）。spark 容器有 python3 + curl，且 `./data` 已挂进去。（**只看 `curl` 返回 0 是不够的** —— 必须看返回体里的 `Status` 字段，这是"导入成功但 0 行数据"那个坑的根源。）

## ③ dim_product_load（SCD1）

```bash
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < /opt/offline-dw/sql/dim_product_load.sql
```

`<` 重定向由 **DS 容器的 shell** 执行，读的是 DS 容器里的 `/opt/offline-dw/sql/dim_product_load.sql`。**这条命令能跑通，就证明 A1 那个挂载生效了。**

**幂等靠 `PRIMARY KEY(product_id)` 同键覆盖，不需要 `TRUNCATE`。**

## ④ dim_product_scd2_load（SCD2）

```bash
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < /opt/offline-dw/sql/dim_product_scd2_load.sql
```

**这个文件里同时有 `TRUNCATE` 和 `INSERT`**，靠 mysql CLI 的多语句能力一次执行完。理由见 README「SCD2 装载必须是"清空 + 重建"一次执行」。

**为什么 SCD2 必须清空而 SCD1 不用**：SCD2 的主键是 `(product_id, valid_from)`，而 `valid_from` 是**推导出来的**。快照集合一变，同一个商品推导出的 `valid_from` 可能不同 → 键不同 → 旧行留着 → **一个商品两个"当前版本"**。

> **主键里一旦包含"推导出来的列"，主键就保护不了幂等性了。**

## ⑤ dwd_sku_reload

```bash
bash /opt/offline-dw/scripts/dwd_sku_load.sh
```

**必须在 DS 容器里跑**（脚本内部要 `docker exec`，只有 DS 容器有 docker CLI）。

脚本做两件事（细节见脚本内注释）：

- **从 `SELECT DISTINCT dt FROM dwd_order_detail` 动态取天数** —— 不写死。写死的日期列表在新增一天订单后会**静默漏算**。
- 逐天 `INSERT OVERWRITE`，SQL 从 `sql/dwd_order_sku_detail_load.sql` 读（不内联）。

**为什么必须覆盖全部天**：SCD2 是全量重建的，重建一次所有天的匹配结果都可能变。

**已知限制**：`INSERT OVERWRITE ... PARTITION (p<日期>)` 要求分区已存在。如果哪天回填了新的一天订单而 `dwd_order_sku_detail` 没有那天的分区，这里会失败（**大声失败，好事**），需要先按 `sql/dwd_order_sku_detail_add_partitions.sql` 的写法补分区。

## ⑥ dq_check

```bash
bash /opt/offline-dw/scripts/dqc_dim_product.sh
```

**这个节点存在的意义就是「坏了让工作流失败」。** 不要加 `|| true`、不要吞退出码。

DS 判断节点成败**只看退出码，不看日志内容** —— 只打印不退出的话 DS 会以为成功，坏数据照样往下流。

它查 5 项（**全部通过 = 没有任何输出 + 退出码 0**）：

| # | 检查 | 抓什么 |
|---|---|---|
| ① | 每商品恰好一个当前版本 | 重跑叠加（曾出现 `ratio = 2.00`）|
| ② | 版本区间无断裂 | 重叠 / 空洞 / **哨兵值后面还有版本** |
| ③ | `is_current` 与 `valid_to` 自洽 | 是当前版本却没写 `9999-12-31` |
| ④ | 物化行数一致 | JOIN 写错导致丢行 / 多行 |
| ⑤ | 物化金额一致 | JOIN 一对多导致金额被放大 |

**⚠️ 检查② 里的哨兵值拦截不能省**：`DATE_ADD(DATE '9999-12-31', INTERVAL 1 DAY)` 返回 **NULL**，而 `NULL <> next_from` 是 **NULL**（不是 TRUE），`CASE` 会落到 `ELSE 0` —— 断裂被**静默放过**。这和 PITFALLS 里"`BETWEEN` 遇 `NULL` 返回 `NULL`"是同一个陷阱。

**自检**：`sql/dqc_dim_product_selftest.sql` 用内存里的假数据证明这 5 条规则真的能发现问题（**10 个用例**：① 3 个、② 4 个、③ 3 个，含上面那个哨兵值盲区）。**一个从来没失败过的检查，跟没有检查是一样的。**

**这条链路是"自愈"的** —— 每个节点都清空+重建，所以中间表弄不坏（手工改坏 SCD2，重跑时节点④ 会整个覆盖掉）。**因此 DQC 真正能抓的是两处它修不了的地方**：CSV 输入本身有问题、或装载 SQL 逻辑被改错。

## 定时与执行策略

| 项 | 值 |
|---|---|
| 定时 | `0 0 3 * * ? *`（每天 03:00，错开订单链路的 02:00）|
| 时区 | `Asia/Shanghai` |
| 开始时间 | **改成当天**（默认次日，不改会干等一天）|
| 执行策略 | **串行丢弃**（`SERIAL_DISCARD`）|

**执行策略为什么不能是「并行」**：节点② 含 `TRUNCATE`。允许并发实例时会出现"A 清空 → B 清空 → A 导入 → B 导入"的交错，**既丢数据又重复，而且不报错**。

> **`offline_dataware` 也必须改成「串行丢弃」** —— 它开头也 `TRUNCATE ods_order`，而且有 02:00 的定时，手工点击可能撞上。

**两个「上线」缺一不可**：工作流定义要上线，**定时任务也要上线**。

## 验证幂等

跑工作流**前后各执行一次**指纹，**四个数字必须一字不差**：

```bash
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/fingerprint_product_chain.sql
```

指纹 = `sum(crc32(一行所有列拼成的字符串))`。**行数一样不代表数据一样** —— 品类改了、版本区间挪了，行数可能纹丝不动。

用 `sum` 而不是整表 md5，是因为**数据库里行的物理顺序不保证**，而 `sum` 与顺序无关；否则同样的数据会算出不同指纹，白查半天。

**它还能发现"非确定性"**：没改任何代码两次指纹却不同 → 说明链路里藏了不确定性（SQL 里用了 `now()`、或 `ORDER BY` 有并列值导致每次取到不同的行）。
