# DS 调度接线规格（湖仓分层新架构）

> 适用架构：2026-10-15 定稿 —— 湖侧 `lake.ods` / `lake.dim` / `lake.dwd` 是**唯一计算源**，
> 仓侧 StarRocks 只保留 `dws` / `ads`，由 `scripts/run_dws_ads_from_lake.sh` 从 `lake.dwd` 物化。
>
> 本文**只讲 DolphinScheduler 侧怎么接**。脚本自身行为见各脚本头部注释、
> `docs/lake-tier-runbook.md`（湖仓运行手册）与 `docs/PITFALLS.md`。
>
> 本文写于 2026-10-15，当天核过的事实：DS 容器 `apache/dolphinscheduler-standalone-server:3.2.0`
> （容器名 `dolphinscheduler`），4 个流程、4 条 schedule、以上命令均在本地环境实测通过。

> ✅ **2026-10-16 更新：本文已落地** —— 按本规格建成了工作流 **`offline_dataware_lake`**
> （v5，8 个节点，`release_state=1` 已上线），实例 346 端到端全绿（3 分 34 秒）。
> 实际节点名与命令见 `README.md` 的「⭐ 当前生产链路」一节（**以那里为准**）。
> 文中 §1 提到的两个坑（`run_dwd.sh` 的宿主路径、`run_dim.sh` 的 Windows 临时目录）
> **已由方案 C 消除**（改为常驻 `docker exec spark` + 容器内锁 `/tmp/.spark_job.lock`）；
> §7 的待拍板项 1、2 已定。

---

## 0. 目标链路

```
Kafka ──ODS 重放──> lake.ods ──run_dim.sh──> lake.dim
                                                  │
                                                  v
                          lake.dwd（明细 / SKU / 累积快照）
                                                  │  run_dws_ads_from_lake.sh（默认 auto 增量）
                                                  v
                     仓侧 dws.dws_user_order_day / ads.ads_daily_sales
                                                  │
                                                  v
                                         DQC（你负责）
```

每天要跑的 4 条命令见 §2；节点怎么串见 §3。

---

## 1. 接线前必须先解决的两件事

### 1.1 `scripts/lake/run_dwd.sh` 的宿主路径问题（必解决，否则 100% 跑不起来）

**事实**

- DS 容器（`dolphinscheduler`）里**只挂了 `scripts/` 和 `sql/`** —— 见 `docker-compose.yml:150-151`。
  容器内**没有** `/opt/offline-dw/lake`、没有 `/opt/offline-dw/spark`。
- `scripts/lake/run_dwd.sh:27-28` 用脚本位置反推项目根：
  `HERE=/opt/offline-dw/scripts/lake` → `PROJ=/opt/offline-dw`（**容器内路径**）。
- 然后 `scripts/lake/run_dwd.sh:72-77` 执行
  `docker run ... -v "$PROJ/lake:/opt/offline-dw/lake" -v "$PROJ/spark/jars:/opt/offline-dw/jars:ro" -v "$PROJ/sql:/opt/offline-dw/sql:ro"`。
- **`docker run -v` 的源路径是由宿主上的 docker daemon 解析的**，不是 DS 容器解析的。
  宿主上 `/opt/offline-dw` 不存在（实测 `ls: cannot access '/opt/offline-dw'`），
  于是 daemon 会**自动创建空目录**再挂进去 → Spark 的 warehouse 为空、`--jars` 为空 →
  作业失败；更坏的情况是它"成功"地写出一个新的空 warehouse。

> **同一脚本里的两种路径语义**（最容易踩的点）：
> `docker run -v <源>` 的源 = **宿主**路径；
> `docker cp <本地端>` 的本地端 = **执行 docker CLI 的那个容器内**路径。
> 所以 `scripts/lake/run_dwd.sh:93` 的 `docker cp "$HERE/lake_dwd_build.py"` 用容器内路径是**对的**。

**三选一解决**

| 方案 | 做法 | 改动量 | 备注 |
|---|---|---|---|
| **A（最小改动，推荐先做）** | 宿主机执行 `sudo mkdir -p /opt && sudo ln -s /home/l/offline-dw /opt/offline-dw` | 零脚本改动 | daemon 会把 `/opt/offline-dw/lake` 解析到 `/home/l/offline-dw/lake`。DS 容器内 `/opt/offline-dw/scripts` 仍走自己的 bind mount，不冲突 |
| **B（最显式）** | 给湖侧脚本加 `HOST_PROJ="${HOST_PROJ:-$PROJ}"`，所有 `-v` 改用 `$HOST_PROJ/...`；DS 任务里写 `HOST_PROJ=/home/l/offline-dw` | 改 `scripts/lake/run_dwd.sh`、`scripts/lake/run_dim.sh`（+ `run_ods` 类脚本） | 不依赖宿主 `/opt`，换机器只要改一个环境变量 |
| **C（最省资源）** | 把 `docker run --rm -d --name lake_dwd_tmp ...` 改成 `docker exec spark ...`（`spark` 容器常驻，且 compose 已给它挂好 `scripts`/`sql`/`lake`/`jars`） | 改脚本 | 不再每次起临时容器，但要重新验幂等与隔离 |

建议：先 A 把接线跑通，后面有空再做 B（B 会让脚本彻底摆脱"路径必须叫 /opt/offline-dw"的隐含假设）。

### 1.2 `scripts/lake/run_dim.sh` 依赖 Windows 临时目录（从 DS 跑**必失败**）

- `scripts/lake/run_dim.sh:12-13`：
  `PROJ="${PROJ:-/home/l/offline-dw}"`、`SCRATCH="${SCRATCH:-/mnt/d/develop/workspace/deepseek_harness_temp}"`。
- 三处用到 scratch，**坏只坏在第三处**：
  - `scripts/lake/run_dim.sh:27` `-v "$SCRATCH:/opt/offline-dw/scratch:ro"` → `-v` 源由**宿主** daemon 解析，`/mnt/d/...` 在宿主存在 → 能挂上；
  - `scripts/lake/run_dim.sh:39` `-f /opt/offline-dw/scratch/lake_dim.sql` → 走上面的挂载，能读；
  - `scripts/lake/run_dim.sh:48` `docker cp "$SCRATCH/lake_dim_build.py" "$CT:/tmp/o/build.py"` → **`docker cp` 的本地端在 DS 容器内解析**，
    DS 容器里没有 `/mnt/d/...` → `No such file or directory`；脚本是 `set -euo pipefail` → 节点直接红。
- 所以**从 DS 跑 `run_dim.sh` 会死在 `scripts/lake/run_dim.sh:48`**；从宿主手工跑则一切正常——这正是它一直没被发现的原因。
- 这两个文件**其实已经在仓库里**：`sql/lake/lake_dim.sql`、`scripts/lake/lake_dim_build.py`，scratch 只是历史遗留。

**建议**改成与 `run_dwd.sh` 同款自包含（注意它**现在根本没挂 `sql/`**，要补一个 `-v`）：

```bash
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ="$(cd "$HERE/../.." && pwd)"
docker run --rm -d --name "$CT" --network offline-dw_default --memory 4g \
  -v "$PROJ/lake:/opt/offline-dw/lake" \
  -v "$PROJ/spark/jars:/opt/offline-dw/jars:ro" \
  -v "$PROJ/sql:/opt/offline-dw/sql:ro"     # ← 用这个替掉原来的 -v "$SCRATCH:/opt/offline-dw/scratch:ro"
docker exec "$CT" bash -c "$SPARK_SQL -f /opt/offline-dw/sql/lake/lake_dim.sql"
docker cp "$HERE/lake_dim_build.py" "$CT:/tmp/o/build.py"
```

---

## 2. 每天要跑的命令（均已实测）

| # | 命令（DS 容器内路径） | 作用 | 幂等 |
|---|---|---|---|
| 1 | `bash /opt/offline-dw/scripts/lake/lake_ods_replay.sh --date YYYY-MM-DD` | Kafka → `lake.ods`（按天覆盖）| 是（重跑同一天结果不变）|
| 2 | `bash /opt/offline-dw/scripts/lake/run_dim.sh build` | `lake.ods.ods_product` → `lake.dim` | 是 |
| 3 | `bash /opt/offline-dw/scripts/lake/run_dwd.sh incremental all --lookback 3` | 湖侧 DWD 三张表按天增量 | 是（写前按范围 DELETE）|
| 4 | `bash /opt/offline-dw/scripts/run_dws_ads_from_lake.sh` | 仓侧 `dws`/`ads` 自动增量（水位自算）| 是 |
| 5 | 你的 DQC | — | — |

**两条必须注意**

- ★ 第 3 步**必须显式写 `--lookback 3`**：`scripts/lake/lake_dwd_build.py:475` 的 `--lookback` 默认是 **0**
  （只算水位那一天），而生成器的迟到事件会回拨 **3 天** → 不写会漏迟到数据。
  仓侧物化（第 4 步）默认 lookback 也是 3，两边要对齐。
- ★ 第 3、4 步**都不需要传日期参数**，区间自己算（水位 = 目标表 `MAX(dt)`）。
  这是新架构相对旧链路（`WHERE dt = STR_TO_DATE('${system.biz.date}')`）的主要收益：
  补数和迟到事件不再依赖调度参数，不会"参数拼错就少算一天"。

**第 1 步的疑点（要你确认）**：如果你现在的湖侧 ODS 是 Kafka routine load 直连 `lake.ods`，
而不是每天跑一次重放，那第 1 步要换掉（见 §7.4）。

---

## 3. 建议的流程与节点

### 3.1 做法：改造现有流程 `offline_dataware`（code `23147253500544`）

它现在是**旧架构**（以下为 2026-10-15 查库实测的节点图，来自 `t_ds_process_task_relation`）：

```
dwd_delete(SHELL, code 23175931853441)  →  dws_agg(SQL)  →  ads_metric(SQL)  →  dqc_order_chain(SHELL)
```

其中 `dwd_delete` 的脚本体是仓侧 `INSERT OVERWRITE dwd.dwd_order_detail PARTITION ...`，
前置的 `${D:0:4}-${D:4:2}-${D:6:2}` 日期转换就写在它里面（可以照抄到新节点）。

保留流程名与 code，把前三个节点换成湖侧链路：

| 节点名 | 类型 | 命令 | 前置节点 |
|---|---|---|---|
| `lake_ods_replay` | SHELL | `D="${system.biz.date}"; bash /opt/offline-dw/scripts/lake/lake_ods_replay.sh --date "${D:0:4}-${D:4:2}-${D:6:2}"` | 无（起始节点）|
| `lake_dim_build` | SHELL | `bash /opt/offline-dw/scripts/lake/run_dim.sh build` | `lake_ods_replay` |
| `lake_dwd_incremental` | SHELL | `bash /opt/offline-dw/scripts/lake/run_dwd.sh incremental all --lookback 3` | `lake_dim_build` |
| `wh_dws_ads_materialize` | SHELL | `bash /opt/offline-dw/scripts/run_dws_ads_from_lake.sh` | `lake_dwd_incremental` |
| `dqc_order_chain`（已有，保留）| SHELL | `bash /opt/offline-dw/scripts/dqc_order_chain.sh ${system.biz.date}` | `wh_dws_ads_materialize` |

如果采用了 §1.1 的**方案 B**，前 3 个节点的命令前面加 `HOST_PROJ=/home/l/offline-dw `。

### 3.2 依赖必须串行，不要并行

第 2、3 步都是 Spark 作业（`docker run` 起临时容器，内存 4g/5g，**算在宿主头上**，不受
DS 容器 `mem_limit: 2g` 约束），并行会互相抢内存；第 4 步依赖第 3 步刚写完的 `lake.dwd`。
所以整条链串成一条线，DS 里就是"每个节点只挂一个前置"。

时序参考（保留现有 cron 习惯）：`offline_dataware` 每天 `0 0 2 * * ? *`（02:00），
`order_event_chain` 02:30、`dim_product_chain` 03:00；`routine_load_health` 每 30 分钟。

### 3.3 重试 / 超时 / 失败策略建议

| 节点 | failRetryTimes | 超时(s) | 理由 |
|---|---|---|---|
| `lake_ods_replay` | 1 | 1800 | Kafka 读取偶发超时可重试；幂等 |
| `lake_dim_build` | 1 | 1800 | 幂等 |
| `lake_dwd_incremental` | 1 | 3600 | 首次或补历史区间会久 |
| `wh_dws_ads_materialize` | 1 | 1800 | 幂等（默认 auto 只算几天）|
| `dqc_order_chain` | 0 | 600 | 你负责 |

`failureStrategy` 用 `END`（本节点失败 → 后续节点不跑），别用 `CONTINUE`。

### 3.4 改造后全景

| 流程 | code | cron | 改后 |
|---|---|---|---|
| `offline_dataware` | 23147253500544 | 02:00 | **湖侧 ODS→DIM→DWD→仓侧物化 + 你的 DQC**（§3.1）|
| `routine_load_health` | 23307453803776 | 每 30 分 | 不动 |
| `order_event_chain` | 23289358842240 | 02:30 | 节点 `dwd_lifecycle_load` **删除**（湖侧 `run_dwd.sh` 已建 `lake.dwd.dwd_order_lifecycle`）；流程可下线 |
| `dim_product_chain` | 23220716579072 | 03:00 | 仓侧 ODS/DIM/SKU 节点删除或改为湖侧（见 §4）；`dq_check` 你定 |

> 实测现状（2026-10-15 查 `t_ds_process_definition` / `t_ds_schedules`）：
> 4 个流程 `release_state` **全是 0（未上线）**，4 条 schedule 的 `release_state` 也全是 0；
> crontab 依次为 `0 0 2 * * ? *`（offline_dataware）、`0 0/30 * * * ? *`（routine_load_health）、
> `0 30 2 * * ? *`（order_event_chain）、`0 0 3 * * ? *`（dim_product_chain）；
> `failure_strategy`：`offline_dataware`=1、`dim_product_chain`=1、其余=0
> （DS 里 0=END、1=CONTINUE，建议新链路全用 END）。
>
> 另：`t_ds_task_definition` 里有**两条同名 `dwd_delete`** —— SHELL 版 `23175931853441`（当前图里用的）
> 和一条历史 SQL 版 `23175818993024`（已不在任何流程图里）。改任务时按 **code** 认，别按名字找。

---

## 4. 旧节点处置建议

另两个流程的现有节点图（2026-10-15 查库实测）：

```
dim_product_chain:  wait_order_chain(DEPENDENT) → truncate_and_load_ods → dim_product_load
                    → dim_product_scd2_load → dwd_sku_reload → dq_check
order_event_chain:  dwd_lifecycle_load
routine_load_health: check_routine_load
```

| 流程 | 节点 | 处置 | 理由 |
|---|---|---|---|
| `offline_dataware` | `dwd_delete` | **删除** | 仓侧 dwd 由湖侧接管；仓侧 `dwd.dwd_order_detail` 已无消费者（`dws`/`ads` 改从 `lake.dwd` 物化）|
| | `dws_agg` / `ads_metric` | **删除** | 被 `wh_dws_ads_materialize` 替代。它们只有 `WHERE dt = 昨天`，补数、迟到、区间外漂移全漏 |
| | `dqc_order_chain` | 保留 | 你负责 |
| `dim_product_chain` | `truncate_and_load_ods` / `dim_product_load` / `dim_product_scd2_load` | **删除或改为湖侧** | 现在灌的是**仓侧** `ods.ods_product`、`dim.dim_product_scd2`；湖侧对应物是第 1 步（product 重放）+ 第 2 步 `run_dim.sh` |
| | `dwd_sku_reload` | **删除** | 仓侧 `dwd.dwd_order_sku_detail` 由 `run_dwd.sh` 在湖侧生成 |
| | `dq_check` | 保留 / 你定 | 你的 DQC |
| | `wait_order_chain` | 若 `offline_dataware` 仍跑则保留 | DEPENDENT：等 `offline_dataware` 当天实例成功 |
| `order_event_chain` | `dwd_lifecycle_load` | **删除** | 仓侧累积快照改由湖侧 `run_dwd.sh` 产出 |
| `routine_load_health` | `check_routine_load` | 保留 | Kafka routine load 健康检查，与湖仓边界无关 |

> ⚠️ **删节点 ≠ 删表**。我只盘点了 DS 里的消费者，没盘你手上的报表/看板/其他程序。
> 建议：先只改 DS（仓侧表原样留着，不 DROP），跑一两个周期确认没人读，再考虑清表。

---

## 5. 注意事项清单（踩坑速查）

1. DS 是 **standalone 单容器**，SHELL 任务就在该容器里执行；容器里只有 `scripts/` 和 `sql/`
   （`docker-compose.yml:150-151`），`lake/`、`data/`、`spark/` 都不在 → 见 §1.1。
2. **`docker run -v` 源路径 = 宿主路径；`docker cp` 本地端 = CLI 所在容器内路径。** 别混。
3. 任务命令一律写 `/opt/offline-dw/...`（容器内绝对路径）。DS SHELL 任务的当前目录**不是**仓库根。
4. **`--truncate` 绝不要进调度** —— 它会把两张仓表整表清空重灌。不带参数默认就是 auto 增量。
5. `scripts/lake/run_dwd.sh:30,33-35` 用固定容器名 `lake_dwd_tmp`，并在脚本里 `trap cleanup EXIT` + 进入时先 `docker rm -f`
   → 同一脚本**不能并发**，DS 上必须串行（两个实例会互相 `docker rm -f` 掉对方的容器）。
6. `docker run` 用 `--network offline-dw_default`（compose project 名派生）。**改了 compose 的
   project 名或目录名，要同步改脚本里的网络名**，否则容器起不来。
   已被证明可行的一点：现有节点 `ods_spark` 就是用 `docker exec spark ...` 跑的，
   说明 DS 容器里**有 docker CLI 且能连到宿主 daemon**（§1.1 的方案 C 因此成立）。
7. 三个湖侧脚本都 `set -euo pipefail`，失败会非 0 退出 → DS 能正确判红，不需要额外包装。
8. `scripts/lake/lake_ods_replay.sh:63` 会 `docker exec spark chown -R 185:185 ...`
   （Iceberg 写入用户）；`spark` 容器没起时这步会被 `|| true` 吞掉，但**后续 Spark 写入会因权限失败**
   → 链路开始前确认 `docker compose up -d spark`。
9. `lake.ods.ods_order_flat` 是 **Kafka 外表**，`REFRESH EXTERNAL TABLE` 会报错，属**正常**
   （物化脚本已把它列为非关键表，只 warn）。
10. 日期参数：DS 的 `${system.biz.date}` 是**昨天**（`yyyyMMdd`）；
    `lake_ods_replay.sh --date` 要 `yyyy-MM-dd`，用 `${D:0:4}-${D:4:2}-${D:6:2}` 转
    （现有 `dwd_delete` 节点就是这么写的，可照抄）。
11. 内存：`starrocks` 3g、`spark` 2g、`dolphinscheduler` 2g、`mysql` 600m、`kafka` heap 512m。
    Spark 临时容器（4g/5g）另算。宿主吃紧时先确认没有并行 Spark 作业。
12. 库里 4 条 `t_ds_schedules` 的 `release_state` **都是 0（未上线）**。要靠 cron 自动跑，
    记得在界面上把流程**上线**（`t_ds_process_definition.release_state` 也要对应）。
13. `dolphin/*.json` 是整理后的快照，若拿**旧** JSON 重新导入 DS，会把
    `dim_product_load` / `ods_spark` / `ods_event_spark` 的路径覆盖回扁平写法（坏引用）。
    导入后对一次这三条。

---

## 6. 接完怎么验（不依赖 DQC）

```bash
# 0) 前置：spark 容器在跑
docker exec spark true

# 1) 手动逐个跑 §2 的 4 条命令，每条都得 EXIT=0
# 2) 湖侧水位
docker exec spark /opt/spark/bin/spark-sql ... -e "SELECT count(*), max(dt) FROM lake.dwd.dwd_order_detail"
# 3) 仓侧物化自检（--dry-run 只读不写，会打印区间与湖侧现状）
bash /opt/offline-dw/scripts/run_dws_ads_from_lake.sh --dry-run
# 4) 幂等：默认增量连跑两遍，第二遍数字必须一字不差
```

当前基线（2026-10-15 实测，可用于对照）：

| 表 | 行数 | 口径 |
|---|---|---|
| `lake.dwd.dwd_order_detail` | 854,844 | `sum(amount)=140,891,053.50` |
| `lake.dwd.dwd_order_sku_detail` | 854,844 | 负值 0 |
| `lake.dwd.dwd_order_lifecycle` | 900,000 | `sum=144,975,064.50` |
| `dws.dws_user_order_day` | 18,000 | 90 天 × 200 用户 |
| `ads.ads_daily_sales` | 90 | 90 天 |

---

## 7. 待你拍板的点

1. **§1.1 选哪个方案？**（A 宿主软链 / B `HOST_PROJ` 改造 / C 改用 `docker exec spark`）
   选 B 或 C 要我改脚本的话说一声。
2. **§1.2 的 `run_dim.sh` 自包含改造**（去掉 Windows scratch 依赖）要不要我做？
3. **`dim_product_chain` 里的仓侧维度/ODS 节点**：保留（仓侧仍想留一份）还是下线？
4. **湖侧 ODS 的日常来源**是不是"每天重放昨天那一天"？如果现在是 Kafka routine load 直连
   `lake.ods`，§2 第 1 步要换。
