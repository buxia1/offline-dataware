# DolphinScheduler 工作流三：订单事件 / 累积快照链路

> 本文是 `docs/dolphinscheduler-workflow.md` 的**续篇**，只讲新增的第三个工作流。
> 该文件里已有的通用知识（数据源注册、DS 容器跨容器调 Spark、cron 6~7 位、两个「上线」缺一不可、告警配置）**本文不重复**，请对照阅读。

**工作流名建议**：`order_event_chain`

**涉及的表与脚本**

| 对象 | 说明 |
|---|---|
| `ods.ods_order_event` | 事件流落地（`DUPLICATE KEY`，8 列）|
| `ods.ods_kafka_offset` | **消费位点表**（`PRIMARY KEY(topic, partition_id)`）|
| `dwd.dwd_order_lifecycle` | 累积快照事实表（`PRIMARY KEY(order_id)`）|
| `scripts/ods_order_event_ingest.sh` | 摄入外壳（增量 / `--reset`）|
| `scripts/ods_order_event_to_starrocks.py` | Spark 摄入作业（被上面的外壳调用）|
| `scripts/dwd_order_lifecycle_load.sh` | 累积快照装载 + 五道防线（默认增量 / `--full`）|

---

## 一、先说清楚：为什么生成器**不能**放进 DS

事件数据的产生者是 `scripts/gen_mock_orders.py`（模拟"上游业务系统"）。**它不能放进 DS**，实测原因：

```
$ docker exec dolphinscheduler bash -c 'which python3'
bash: line 1: python3: command not found
```

**DS 容器里根本没有 Python**。而生成器需要 Python + `kafka-python`。

**所以定位和商品链路一致**：

| 链路 | 生成/同步 | 数仓加工 |
|---|---|---|
| 商品 | `gen_mock_products.py` 产出 CSV（**调度外**）| DS 从 Stream Load 开始 |
| 订单快照 | `gen_mock_orders_snapshot.py`（**调度外**）| DS 从 `ods_spark` 开始 |
| **订单事件** | **`gen_mock_orders.py`（调度外）** | **DS 从 `ods_event_spark` 开始** ← 本文 |

---

## 二、工作流结构

**2 个节点，串行**：

```
① ods_event_spark ──► ② dwd_lifecycle_load
      Shell                    Shell
```

**两个都用 Shell 任务**，理由同 `dim_product_chain`：

1. 脚本都是"外壳 + 多语句"，Shell 里跑 `bash xxx.sh` 最直接；
2. 节点体里**一个 `${...}` 都没有** —— DS 不会做参数替换，避免 `${system.biz.date}` 被吃掉或语义错位。

---

## ① ods_event_spark

| 字段 | 值 |
|---|---|
| 任务类型 | **SHELL** |
| 前置任务 | （空，它是第一个节点）|
| worker 分组 | `default` |
| 失败重试次数 | 0 |
| 超时 | 0（不设）|

**脚本内容**：

```bash
bash /opt/offline-dw/scripts/ods_order_event_ingest.sh
```

**作用**：从 Kafka **增量**读新消息 → 写 `ods.ods_order_event` → 回写 `ods_kafka_offset` 位点。

### ⚠️ 为什么**不传日期参数**

这是增量方案带来的好处：**位点表自己知道读到哪了**，不需要 DS 告诉它"该处理哪天"。

对比一下另外两个节点（都依赖 `${system.biz.date}`）：

| 节点 | 需要日期吗 | 为什么 |
|---|---|---|
| `ods_spark`（订单快照）| ❌ 不需要 | 全量重读，主键表折叠重复 |
| `dwd_delete`（订单 DWD）| ✅ 需要 | `INSERT OVERWRITE ... PARTITION (p${D})` 必须点名分区 |
| **`ods_event_spark`（事件摄入）** | ❌ **不需要** | **位点表是状态** |

### 内置的四道防线（脚本自己会拦）

| # | 防线 | 触发时 |
|---|---|---|
| ① | topic 为空 → 拒绝执行 | 防止 `--reset` 把事件表清空 = 静默清库 |
| ② | 位点进度 < Kafka 现存最早 offset | **retention 已删掉我们没读的消息** —— 必须人工介入 |
| ③ | 事件表行数 ≠ 位点表之和 | 写入了但没记账（或反之）|
| ④ | 本次新增 ≠ topic 增量 | 丢消息或重复 |

**任何一道不过 → 退出码 1 → 工作流失败**（这正是我们要的）。

---

## ② dwd_lifecycle_load

| 字段 | 值 |
|---|---|
| 任务类型 | **SHELL** |
| 前置任务 | `ods_event_spark` |
| worker 分组 | `default` |
| 失败重试次数 | 0 |
| 超时 | 0（不设）|

**脚本内容**：

```bash
bash /opt/offline-dw/scripts/dwd_order_lifecycle_load.sh
```

**作用**：把事件流推导成累积快照，**增量回填**（`INSERT` 即 UPSERT）到 `dwd.dwd_order_lifecycle`。

### 为什么不传 `--full`、也不传日期

| 参数 | 默认行为 | 说明 |
|---|---|---|
| 不传 → `--inc` | 回填起点自动取 `ods.ods_order_event` 的 **`MAX(dt)`** | 按"数据自己的时间轴"推进，可重放 |
| `--full` | `TRUNCATE` + 从 1970 全量重推 | **只用于增量逻辑坏掉时的恢复**，不要日常跑 |

> **注意**：默认模式的起点是 `MAX(dt)`，**不是"今天"**。这正好是我们要的 —— 摄入刚写进去的批次，其日期就是事件表的最大日期。

### 五道防线（全是数据完整性检查）

| # | 防线 | 打印的样子 |
|---|---|---|
| ⓪ | 事件表**非空**（空表直接 `exit 1`）| `事件表总行数: 2052` |
| ① | 范围内没有订单 → 明确退出 | `范围内订单数: 225` |
| ② | 占位符 `${FROM_DT}` 替换校验 | 防止 `> NULL` 静默 no-op |
| ③ | **全表双向 `EXCEPT` 对账 = 0** | `双向对账差异行数: 0` |
| ④ | 不变式（行数=唯一订单、lag 自洽、stage 非空）| `order_time 为空: 0` 等 |
| ⑤ | **事件表行数 == `(order_id,event_type)` 去重对数** | `事件表行数/去重对数: 2052 / 2052` |

**防线⑤ 为什么必须有**：`ods_order_event` 是 `DUPLICATE KEY`，摄入层一旦不幂等就会翻倍；
而**防线③ 是值级对账，抓不到"完全相同的重复行"**（`EXCEPT` 会去重）。只有防线⑤ 是全表**行级**检查。
详见 `docs/PITFALLS.md` §3.17。

---

## 三、定时与执行策略

**工作流定义 → 定时 → 新建**

| 字段 | 值 |
|---|---|
| 开始时间 | **改成当天**（默认次日，不改会干等一天）|
| Cron | `0 30 2 * * ? *`（每天 **02:30**）|
| 时区 | 确认 `Asia/Shanghai` |
| 执行策略 | **串行丢弃**（`SERIAL_DISCARD`）|
| 失败策略 | **`STOP`（结束）** ← ⚠️ **不是 `CONTINUE`** |
| 告警 | 失败告警；告警组选**和另外两个工作流同一个** |

```
0    30   2    *    *    ?    *
秒   分   时   天   月   周   年
```

### ⚠️ 为什么失败策略必须是 `STOP`（这一点和现有两个工作流不同）

现有两个工作流用的都是 `failure_strategy = 1`（**`CONTINUE` 容错**）—— 失败会自动重试。

**但事件链路的防线是"数据完整性检查"，不是"临时故障"**：

| 防线失败的含义 | 该怎么做 |
|---|---|
| 防线② 位点进度 < Kafka 现存最早 | **消息已被 retention 删掉，数据已丢** —— 重试一百次也没用 |
| 防线③ 行数 ≠ 位点之和 | 摄入与记账不一致 —— **重试只会让状态更乱** |
| 防线⑤ 事件表有重复 | 数据已脏 —— **重试不会自动修复** |

**2026-10-05 事故的教训正在这里**：当时的 `CONTINUE` 策略把失败重试到"侥幸全绿"，
结果 `ods_order = 0 行`、DQC 假绿、没人发现。

> **规则：防线失败 → 必须中断。重试解决不了数据完整性问题。**

### 为什么是 02:30

| 工作流 | 定时 | 说明 |
|---|---|---|
| `offline_dataware` | 02:00 | 订单链路 |
| **`order_event_chain`** | **02:30** | **本文（错开半小时，避免抢 Spark 容器）** |
| `dim_product_chain` | 03:00 | 商品链路 |

---

## 四、触发侧：外部 cron（必须配，否则链路不会自己长数据）

DS 只负责"Kafka → 数仓"。**事件本身要有人发**：

```cron
# 每天 01:30 发"昨天"的事件（与 DS 的 system.biz.date = D-1 对齐）
30 1 * * * cd /home/l/offline-dw && /usr/bin/python3 scripts/gen_mock_orders.py --date "$(date -d 'yesterday' +\%F)" >> /tmp/gen_event.log 2>&1
```

### ⚠️ 日期对齐（这是最容易错的地方）

DS 的 `${system.biz.date}` = **调度日期 − 1 天**（PITFALLS §2 实测）。

```
第 D 天 01:30  【外部 cron】生成器 --date <D-1>    → Kafka 增加 D-1 那天的事件
第 D 天 02:30  【DS】ods_event_spark              → 增量摄入（只读新增的）
               【DS】dwd_lifecycle_load           → 增量回填 D-1 那天动过的订单
```

**为什么生成器日期要用 `D-1` 而不是 `D`**：因为 DS 在 D 天凌晨处理的是 D-1 那天的数据。生成器和 DS 必须对同一天。

**验证方法**：看生成器输出里的 `日期 2026-09-XX` 这一行，和 DS 日志里的 `D=202609XX` 应该是**同一天**。

> **`kafka-python` 只装在 `l` 用户下**（PITFALLS §6.4）。如果 cron 以 root 运行，会报 `ModuleNotFoundError: No module named 'kafka'`。
> **解法**：cron 里用 `sudo -u l`，或确认 cron 用户是 `l`。

---

## 五、节点依赖（可选）

如果你希望**更严格**地保证"订单链路的 DWD 已经就绪后才跑事件链"，可以加一个依赖节点：

| 字段 | 值 |
|---|---|
| 任务类型 | **DEPENDENT**（在「逻辑节点」分组）|
| 依赖类型 | 工作流 |
| 工作流名称 | `offline_dataware` |
| 任务名称 | **`ALL`** |
| 时间周期 | 日 / **今天** |
| 检查间隔 | 10 秒 |
| 依赖失败策略 | **等待** |
| 失败等待时间 | 30~60 分 |

**但大多数情况下不需要** —— 事件链和订单链**互相不读对方的表**（事件链只读 `ods_order_event`，订单链只读 `ods_order`）。加它是为了"数据齐全"的语义，不是硬依赖。

> ⚠️ 加了依赖节点后，**工作流会变成 3 个节点**，且它是 `ROOT`。

---

## 六、验证幂等（部署后必做）

**连续执行两次工作流，行数必须完全一致。**

```bash
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT (SELECT count(*) FROM ods.ods_order_event) AS event_rows,
       (SELECT count(DISTINCT concat(order_id,'-',event_type)) FROM ods.ods_order_event) AS event_pairs,
       (SELECT sum(next_offset) FROM ods.ods_kafka_offset) AS offset_sum,
       (SELECT count(*) FROM dwd.dwd_order_lifecycle) AS lifecycle;"
```

**四个数字的关系（都必须满足）**：

```
event_rows == event_pairs == offset_sum     ← 行数 = 去重对数 = 位点之和
lifecycle == 唯一订单数 == 视图行数
```

**期望的"跑第二遍"输出**：

```
位点表起始位置: {"ods_order_event": {"0": 199, "1": 189, "2": 224}}
本次从 Kafka 读到 0 行，按 (order_id,event_type) 去重后 0 行
没有新消息，退出（未写入、未改位点）
摄入前/后行数 : 2052 → 2052   （本次新增 0 行）
✅ 摄入完成（行数与位点一致，本次新增 0 行）
```

**"没有新消息 + 行数不变"就是增量生效的铁证。**

---

## 七、补数（回填历史）

事件链的补数和另外两个链路**不一样**，因为生成器**有状态**：

> 生成器读 `ods_order_event` 判断"哪些里程碑还没发"，**只发"到期日 == `--date`"的事件** ——
> **跳过某天就永远不补发**，而且会报 `overdue` / `missing_prev` 告警并 `exit 1`。

### 补历史的正确顺序

```bash
# ① 从最早缺失的那天开始，逐天"生成 → 摄入 → 装载"，不能跳
for d in 2026-09-23 2026-09-24 2026-09-25; do
    sudo -u l python3 scripts/gen_mock_orders.py --date "$d" --dry-run   # 先看：逾期/前置必须 0
    sudo -u l python3 scripts/gen_mock_orders.py --date "$d"             # 真发
    bash scripts/ods_order_event_ingest.sh                                # 增量摄入
    bash scripts/dwd_order_lifecycle_load.sh                              # 增量装载
done
```

**判断标准**：每天 dry-run 的 `逾期未发 ✅ 0 条` + `前置里程碑缺失 ✅ 0 条` + **退出码 0**。

### 如果 ODS 数据丢了（Kafka 还完整）

```bash
# 位点表 + 事件表一起清，下次从 earliest 重灌
bash scripts/ods_order_event_ingest.sh --reset
bash scripts/dwd_order_lifecycle_load.sh --full      # 快照也全量重推
```

**⚠️ 前提是 Kafka 里数据完整** —— 如果 Kafka 已经过期删除，`--reset` 会灌进残缺数据。
`--reset` 之前先确认：`kafka-get-offsets.sh` 的 topic 总量 == 期望总量。

---

## 八、上线检查清单

| # | 检查 | 命令 / 位置 |
|---|---|---|
| 1 | 位点表已建 | `DESC ods.ods_kafka_offset;` |
| 2 | 事件表行数 == 去重对数 == 位点之和 | 上面第六节的 SQL |
| 3 | 工作流**定义**已上线 | 工作流定义页 → 上线 |
| 4 | **定时任务**也已上线 | 定时管理页 → 上线（**两个上线缺一不可**）|
| 5 | 执行策略 = 串行丢弃 | 工作流定义 → 执行策略 |
| 6 | 失败策略 = `STOP`（**不是 `CONTINUE`**）| 定时 → 失败策略 |
| 7 | 告警组已选（和另外两个工作流同组）| 定时 → 告警 |
| 8 | 外部 cron 已配（生成器）| `crontab -l` |
| 9 | DS 容器跨容器调 Spark 仍然可用 | `docker exec dolphinscheduler docker exec spark echo SPARK_OK` |
| 10 | 手工跑一次，观察两节点全绿 | 工作流实例页 |

**验证 DS 容器能力（第 9 项）**：

```bash
docker exec dolphinscheduler bash -c 'docker exec spark echo SPARK_OK_FROM_DS'
# 期望输出：SPARK_OK_FROM_DS
```

> ⚠️ DS 容器里**没有 python3**（本文第一节）—— 所以**任何节点都不能引用 `python3`**。
> 摄入/装载都是 Shell 脚本调 `docker exec`，不受影响。

---

## 九、导出的 JSON

按项目惯例，建好后导出到 `dolphin/`：

```
dolphin/
├── offline_dataware.json       ← 工作流一
├── dim_product_chain.json      ← 工作流二
└── order_event_chain.json      ← 工作流三（新建）
```

**导出后记得手工检查**：`releaseState` 在 JSON 里永远写 `OFFLINE`（DS 3.2.0 的固有行为），**导入后必须手工点上线**。
