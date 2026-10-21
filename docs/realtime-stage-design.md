# 实时化（方向 5）设计草案 —— Kafka → Iceberg 5 分钟热层

> 状态：v0.1 草案（2026-10-21）｜方向 5「实时化」的设计与 PoC 清单，**待拍板**（见 §8）
> 范围：只解决「今天的数据 5 分钟内可查」，**不解决**秒级、CDC、流式 DWS 全链路。
> 所有「实测」标注均来自 2026-10-21 的隔离实验（warehouse/checkpoint 全在 `/tmp`，未写生产数据；记录见附录 A）。
> 代码与命令以本文 §5 为准，落地由用户执行；改完后请把实测结果补进附录 A。

---

## 0. 结论先行

| # | 结论 |
|---|---|
| **默认方案** | **A1：Spark Structured Streaming（5 分钟 micro-batch）→ Iceberg 独立 RT 表**，跑在新增的 `spark-rt` 容器（`mem_limit=1.2g`、`--driver-memory 512m`、`local[2]`）；checkpoint 落宿主 `/home/l/offline-dw/lake/checkpoint/`；**T+1 全量重放仍是唯一真源**，RT 表只做热层。 |
| **为什么不是 Flink** | 把 5 分钟压到 10 秒要多养一个 JM+TM 常驻（≥2.5g），而宿主只剩 ~1.4GB 空闲、StarRocks 已占 55~80%；延迟只降一档，代价是整机 OOM 级风险 + 一套全新引擎的版本矩阵。 |
| **为什么不是 Routine Load** | 它只写 StarRocks 内表 raw ODS，湖侧依旧没有实时数据，等于把「湖=唯一真源」退化成双源双写；DWS/ADS 仍要 T+1 重算，26 项湖↔仓对账会因仓侧多出的行每天变红。 |
| **最关键的三条硬约束** | ① **绝不能和 `lake_ods_replay` 写同一张物理表**（覆盖写与追加写并发会产生重复或 `CommitFailedException`）；② **首次启动必须 `startingOffsets=latest`**（否则把 312 万条历史重放进 RT 表）；③ **StarRocks 不会自动感知新快照**，可见性 = trigger + 显式 `REFRESH EXTERNAL TABLE`（实测 0.19s/次）。 |
| **PoC 一晚可完成** | 判据 7 条（§4.1），其中最关键 3 条：端到端 P95 ≤ 6 分钟、连跑 4 小时含一次 `kill -9` 重启不断流、对 T+1 的 11 节点链路零影响（仍 114~130s、DQC 6/6、对账 0 差异）。 |
| **回滚 ≤ 5 分钟** | `docker stop spark-rt` 即回到纯 T+1，**不必改任何已有脚本/工作流**（这正是选「独立 RT 表 + 独立容器」的直接收益）。 |
| **零内存备选（A0）** | 若不愿付这 1.2g：把 `lake_ods_replay.sh --date $(date +%F)` 每 5 分钟跑一次（幂等覆盖同一张表，**零新增内存/零新增容器**，实测单次 24.3s）。代价：写放大 288×、10× 量下单次涨到 >3 分钟、DS 每天多 288 次执行。见 **§2.4**（含骨架与补丁），**这是拍板点 1**。 |

---

## 1. 现状基线（实测，作为选型与容量推演的依据）

| 维度 | 实测值 | 备注 |
|---|---|---|
| 机器内存 | 15.9GB；WSL 11GiB，`available` 6GiB，swap 2GiB | 宿主空闲 ~1.4GB |
| 容器限额 | spark 2g、starrocks 3g、dolphinscheduler 2g、mysql 600m、kafka 无限制 | `limit` 之和 ≈ **18.6GB > 11GiB**，已严重超卖 |
| 当前占用（`docker stats`） | spark 0.25GiB、SR **1.67~2.39GiB（55~80%）**、DS 1.32GiB、mysql 0.52GiB、kafka 1.09GiB | SR 随缓存波动，用户侧已观察到 76% |
| 事件量 | ods_order_event 3,120,421 条 / 93 天（2026-09-17~12-18）= **33,553 条/天**；单日峰值 35,517 | Kafka latest 与 SR 双向核对 |
| 订单量 | ods_order 900,000 条 / 90 天 = 10,000 单/天 | 订单:事件 ≈ 1:3.47 |
| 峰值假设（10×） | 335,000 条/天 = **3.9 条/s**；5 分钟批 ≈ **1,170 行**；1 分钟批 ≈ 233 行 | 本设计按 10× 留余量 |
| 湖 | 159MB / 2,176 parquet / 283 `metadata.json`；单表最大 dwd_order_lifecycle 1,394 文件 61MB | `find` 统计 |
| Iceberg 表属性 | 仅 `write.parquet.compression-codec=zstd`，`format-version` 未显式设（实测落 v2） | `SHOW TBLPROPERTIES` |
| T+1 链路 | 11 节点，114~130s | DS v10 |
| StarRocks 版本/目录 | 3.5.0；catalog `lake`（hadoop 类型，warehouse `/opt/offline-dw/lake/warehouse`） | `SHOW CATALOGS` |

**这批数字决定了三件事**：单批数据极小（千行级）、所以瓶颈不在吞吐而在**文件/元数据数量**；内存是唯一硬约束；StarRocks 已无余量，实时化不能走「仓侧加常驻消费者」的路。

---

## 2. 选型对比

### 2.1 三案速览

| 维度 | **A. Spark Structured Streaming → Iceberg**（micro-batch） | **B. Flink 1.20 + Iceberg**（新容器） | **C. 回退 Routine Load → StarRocks 仓侧热层** |
|---|---|---|---|
| 内存占用（实测/估算） | **实测**：`driver 700m` 峰值 java RSS 677MiB、容器 ~1.0GiB；`512m` 可正常跑完 45 万行；`400m` 直接拒绝启动（硬下限 471,859,200B ≈ 450MiB）。建议独立容器 `mem_limit=1.2g` | 新常驻 **JM ~1g + TM ≥1.5g**（含 RocksDB/网络缓冲）= **≥2.5~3g**，还要新增镜像 ~1.5GB | **0 新增**：复用 SR 现有 3g。但 3 个消费 task 常驻，SR 现 55~80% 已无余量；`max_filter_ratio` 配错一条脏数据即暂停 |
| 延迟量级 | **5 分钟**（trigger 可调 1 分钟，文件数线性×5） | 秒级~10 秒 | 秒级（`maxBatchIntervalS` 实测 20s/10s 已定义） |
| 恰好一次 | **仅 at-least-once**（Iceberg 提交与 checkpoint 两阶段，非原子）；靠读侧去重 + T+1 全量校准收敛 | Flink checkpoint + Iceberg 两阶段提交，**可做到端到端 EO**（需开 checkpoint 并保证 sink 参与） | **原生 EO**（位点与数据同事务），但只到仓内表 |
| 与 T+1 共存/切换成本 | **低**：独立表 + 独立容器，T+1 一行不改 | 中：多一套引擎，Iceberg 表两边都写需版本对齐 | **最低**：`RESUME` 一条命令即恢复。但语义上是「第二份真源」 |
| StarRocks 侧可见性 | 需 `REFRESH EXTERNAL TABLE`（实测 0.19s/次）；不刷新依赖后台 10min 周期 | 同 A（都是 Iceberg 文件，SR 侧无差别） | **即时**（仓内表，写入即可查，无刷新概念） |
| 运维复杂度 | 低：同一镜像、同一 5 个 jar、同一套 DQC/对账 SQL 复用 | 高：新镜像、新版本矩阵、新 UI/日志/调参、Iceberg-Flink runtime 与湖侧 1.9 的兼容性需另行验证 | 低，但**不可逆**：SR 3.5 无 `DROP ROUTINE LOAD`，STOP 后定义永久留在 FE |
| 失败恢复 | checkpoint 续跑，重启即续（实测 checkpoint 仅 20KB/批）；崩溃窗口内可能重复 ≤1 批 | savepoint/checkpoint，语义更强但恢复更重 | 消费 task 自动重试；暂停后需人工 RESUME，位点可能回退重投 |
| 对 11 节点工作流影响 | **零**（不进 DS 工作流，独立常驻；只在 DS 加 1 个健康检查节点） | 零，但多一个「DS 外常驻」组件要盯 | 会改变「仓侧 ods 由湖派生」的假设 → `recon_lake_wh.sh` 26 项大面积变红 |

### 2.2 一句话评价

- **A**：延迟够用（5 分钟），代价最小（0 新引擎、0 新 jar、0 改现有脚本），唯一要新增的是 **1.2g 内存**和**一套小文件维护**。
- **B**：语义最强、延迟最低，但在这个 11GiB 单机上是「用 3g 换 4 分 50 秒」，投入产出比最差。
- **C**：最便宜的验证路径，但方向错——它不产生湖侧实时，把「湖=唯一真源」降级为双源。

### 2.3 推荐：A1 = A 的流式落地形态

> **Spark Structured Streaming（5min micro-batch）→ `lake.ods.ods_order_rt` / `lake.ods.ods_order_event_rt`（独立 RT 表）→ StarRocks 视图（可选）→ T+1 全量重放继续做唯一真源。**

四个理由，按权重排序：

1. **不破坏现有质量体系**：RT 表与主表 schema 完全一致、物理隔离，`dqc_lake_ods.sh`（6 项）、`recon_lake_wh.sh`（26 项）、`lake_maintain.py` 都是「按表名枚举」，新增两张表即可自动纳入，不改判据逻辑。
2. **回滚是 5 分钟的事**：`docker stop spark-rt` + `DROP VIEW`，T+1 链路一行代码都不动。
3. **复用度最高**：同一镜像（`apache/spark:3.5.1`）、同一 5 个 jar、同一 `_spark_lib.sh` 的提交约定、同一套 `TIMESTAMP_NTZ` 与 `Asia/Shanghai` 口径——所有已知坑（坑 30~34）都已经被踩平。
4. **延后不改变方向**：将来若真需要秒级，RT 表已经就位，换 Flink 只是换写入端，表和下游视图不用动。

### 2.4 零内存备选 A0：「5 分钟幂等覆盖」（对应 §8 拍板点 1，请务必看完再决定）

A1 的唯一代价是 **1.2g**。如果这 1.2g 挤不出来，有一个**几乎零成本**的替代：不发流式作业，而是让**已有的幂等重放脚本**每 5 分钟把「今天」重算一遍。

```
DS 工作流 rt_refresh_5m（cron */5 * * * *，1 节点）
  └─ bash scripts/lake/lake_ods_replay.sh --date $(date +%F)     # 只跑今天，dynamic 覆盖该 dt 分区
```

它之所以成立，是因为 `lake_ods_replay` 本来就是**按 dt 覆盖 + 幂等**（脚本头注释原文：「重跑同一天结果不变」）。

| 维度 | **A1 真流式（§2.3 推荐）** | **A0 幂等覆盖（零内存）** |
|---|---|---|
| 新增内存 | +1.2g 容器（实测 RSS ~1.0GiB） | **0**（复用 2g 的 `spark` 容器，一次性 driver，跑完释放） |
| 单次耗时 | 常驻，每批 commit 1~2s | **实测 24.3s**（`--date 2026-12-18 --dry-run`，含 JVM 启动 + 两个 topic 全量扫描）；带写约 27~30s |
| 每天提交数 | 2 表 × 288 = **576** | 2 表 × 288 = **576**（元数据增速与 A1 相同，治理沿用 §3.1） |
| 数据写放大 | 只写新行（约 2MB/天） | **每次重写整个当天分区**：当前 ≈0.3~0.6GB/天，10× 峰值 ≈3~6GB/天（磁盘 951G 可用，能扛；SSD 寿命需留意） |
| 恰好一次 | at-least-once，崩溃可能重复 ≤1 批（§3.2） | **天然不会重复**：覆盖写是原子替换，无 offsets、无 checkpoint、无状态 |
| 失败恢复 | checkpoint 续跑 + 健康检查 | 失败即本次没跑，**下一个 5 分钟自动重来**；没有「状态坏了」这种故障 |
| 与 T+1 的冲突 | 靠「独立 RT 表」隔离（§3.4） | **零数据冲突**（同一张表、不同 dt 分区，且覆盖写原子）；⚠️ 但 `lake_ods_replay.sh` **自己不拿锁**，A0 的 wrapper 必须显式 `source _spark_lib.sh; spark_acquire_lock`，否则每 5 分钟起的 1g JVM 会和 02:00 批撞 2g 上限 |
| DQC / 26 项对账 | 要新增 RT vs 主表的对比项 | **零改动**（还是那些表、那些口径） |
| 回滚 | `docker stop spark-rt` | **停掉 DS 工作流**（10s），数据留在原地，无残留表 |
| 天花板 | 延迟可压到 1 分钟；量级无关 | **10× 时会退化**：脚本恒 `startingOffsets=earliest`，单次成本 ∝ Kafka 保留全量（90 天 × 日量）；31M 事件时单次 >3 分钟，装不进 5 分钟窗口 |

**A0 的 10× 悬崖怎么破**（若选 A0 必须在灰度前做完）：把「读 Kafka 全量再按 dt 过滤」改成「读当天窗口」——每天 00:00 记一次各分区 `end offsets` 到 `lake/ops/rt_offsets/<date>.json`，提交时 `--starting-offsets-file` 传进去，单次成本从 O(90 天) 降到 O(当天)（≈2~5s）。这是 `lake_ods_replay.py` 的**增量参数**，不动现有默认行为。

**一句话决策规则**：

- 要「今天的数据 5 分钟内可查」+ 不想动内存 → **A0**（PoC 半小时，灰度 7 天，零风险）。
- 要「连续消费 / 1 分钟级 / 将来接 CDC」→ **A1**（本文档主体，已按 A1 写全）。
- 两者不互斥：**A0 先把口径和 DQC 跑通，A1 再换写入端**，RT 表与下游视图不用改。

---

## 3. 边界条件与硬约束（**本节最重要**）

### 3.1 小文件与 Iceberg 元数据膨胀

**实测基准**（`/tmp` 实验，8 次微批 / 40 万行 / 17 个 dt 分区）：

| 产物 | 8 批实测 | 折算每次提交 |
|---|---|---|
| parquet 数据文件 | 38 个 / 6.3MB（均值 **166KB/文件**） | 3~6 个 |
| `metadata.json` | 9 个（v1~v9） | +1 |
| `snap-*.avro` 快照 | 8 个 | +1 |
| manifest `*-m0.avro` | 8 个（每个 ~8KB） | +1~3 |
| metadata 目录总大小 | **328KB** | **≈41KB/提交** |
| checkpoint 目录 | **164KB**（offsets 68K + commits 68K） | **≈20KB/批** |

**按真实量推演**（33.5k 事件/天，10× 峰值 335k/天）：

| trigger | 提交/天 | parquet/天（1~2 个活跃分区） | 元数据/天 | 不清理时 1 年后元数据量 |
|---|---|---|---|---|
| 30 秒 | 2,880 | 2,880~5,760 | ~118MB | 43GB（不可接受） |
| **5 分钟（推荐）** | **288** | **288~576** | **~12MB** | **4.3GB** |
| 15 分钟 | 96 | 96~192 | ~4MB | 1.4GB |

> 结论：**元数据增长速度是数据本身的 2 倍**（12MB 元数据/天 vs 数据约 2MB/天）。不治理，30 天后单表 1 万个文件，`SELECT` 的 planning 与 StarRocks 的 manifest 解析都会明显变慢。

**对策（四条，缺一不可）**：

1. **建表就开元数据自清理**（Iceberg 1.9 默认 `delete-after-commit=false`、`previous-versions-max=100`，等于永不清理）：
   ```sql
   'write.metadata.delete-after-commit.enabled'='true',
   'write.metadata.previous-versions-max'='50',   -- 留 50 版，兼顾 SR 的 FE 缓存（见 3.3）
   ```
2. **trigger 固定 5 分钟**，不要用 30 秒/1 分钟去「换延迟」——延迟收益 <5 分钟量级，文件数是 5~10 倍。
3. **每日一次 compaction**（不是每次提交后做）：
   ```sql
   CALL lake.system.rewrite_data_files(
     table => 'ods.ods_order_event_rt',
     strategy => 'binpack',
     options => map('min-input-files','4','target-file-size-bytes','33554432','rewrite-all','false'));
   ```
4. **`lake_maintain` 扩一个阶段**（现有脚本只做 `expire_snapshots` + 盘点，**不做 rewrite**）：

   | 阶段 | 现状 | 改造后 |
   |---|---|---|
   | 盘点 | ✅ `.snapshots` / `.files` / `.manifests` | ✅ 增加 `files_per_active_partition`、`metadata_dir_bytes` 两个派生指标 |
   | `--rewrite` | ❌ 无 | **新增**，在 `--expire` **之前**执行；只对 `*_rt` 表按 `min-input-files=4` binpack |
   | `--expire` | ✅ 保留 5 个快照（周日） | RT 表用更短策略：`retain_last=10`、`older_than => TIMESTAMP '<shell 算出的 -3 天字面量>'`（**不要写 `now()-3d`，Spark SQL 解析不了，实测报 `mismatched input '(' expecting STRING`**） |
   | 验收判据 | 「快照数下降且行数不变」 | 追加：「文件数下降 **且** 行数不变 **且** 快照数不增」；不满足则只告警、不阻断主表维护 |

   调用点：`run_maintain.sh` 在 02:00 工作流里是第 9 个节点（`lake_maintain`，15 分钟超时），改造后加 `--rewrite` 参数，**不新增节点**（保持 11 节点不变）。

**不要做的事**：不要为了「小文件」把 `write.target-file-size-bytes` 调小——它只管单文件上限，不解决「每次提交写一批」的本质；也不要开 `write.distribution-mode=hash`，那会把每批再按分区拆成更多文件。

### 3.2 恰好一次（两阶段边界与崩溃窗口）

**结论：Spark Structured Streaming + Iceberg 是 at-least-once，不是 exactly-once。** 不要在文档里承诺 EO。

边界在哪：

```
[Spark 作业]  读取 offsets → 计算 → Iceberg 原子提交（生成新 snapshot）
                                        ↓  ← 崩溃发生在这里
              checkpoint 写 offsets/commits（原子 rename） → 下一批
```

- **原子单元不同**：Iceberg 的原子单元是「表（snapshot）」，Spark 的原子单元是「checkpoint 目录里的 batch 文件」，两者不在同一事务里。
- **崩溃窗口**：Iceberg 提交成功、checkpoint 未落盘 → 重启后**重放该批** → 重复行。
  - 窗口时长实测：单批 commit 1~2s + checkpoint 写 ~1s ≈ **2~3 秒**
  - 重复量 = 单批行数：5 分钟批 ≈ 120 行（10× 峰值 ≈ 1,170 行）
- **不会丢数据的理由**：Kafka `retention=720h`，offset 存在 checkpoint，重放窗口远小于 retention；真正会丢的只有「checkpoint 被手工删除且 topic 已过期」这种运维事故。

**接受 at-least-once 的兜底设计**（三层）：

| 层 | 机制 | 现状 |
|---|---|---|
| 读时去重 | `Window.partitionBy("dt","order_id","event_type").orderBy(event_time, order_time).rowNumber == 1` | ✅ `lake_ods_replay.py` 已有同款逻辑，DWS 侧可复用 |
| 每日校准 | T+1 全量重放重写 `dt <= 昨天` 的主表，RT 表只保留 `dt >= 今天-3` | 待建（RT 表按 dt 清理即可，见 3.4） |
| 对账闸门 | `recon_lake_wh.sh` 增加 2 项：RT 表按 dt 计数 vs 主表同 dt 计数 | 灰度期必做 |

**如果非要 EO**：只能用 `foreachBatch` + `MERGE INTO`（Iceberg 1.9 支持），但那是全表/分区级扫描重写，`driver 512m` 的容器跑不动——**列入「不做」**。

### 3.3 StarRocks 对外部表 Iceberg 的可见性

这是最容易被想当然的一点，分三层说：

| 层 | 事实 | 影响 |
|---|---|---|
| 表级刷新 | `REFRESH EXTERNAL TABLE lake.ods.ods_order_event_rt;` 实测 **0.19s**（含 mysql 客户端启动） | 可以按分钟级节拍刷新，无需担心开销 |
| 后台刷新 | `enable_background_refresh_connector_metadata=true`、`background_refresh_metadata_interval_millis=**600000**`（10min） | 不显式刷新时，最坏 **10 分钟**才自动感知新快照 |
| FE 内存缓存 | 坑 31：FE 按**路径**缓存元数据（`iceberg_metadata_memory_cache_expiration_seconds` 默认 86500s ≈ 24h），极端情况下 `REFRESH` 也清不掉，要 `sr_catalog_reset.sh` 重建 catalog | RT 表**频繁提交 + 元数据自清理**会放大这个风险 → 用 `previous-versions-max=50` 留足冗余 |

**仓库已有实证**（`sql/lake/lake_ods.sql` 文件头）：写第 4 行不刷新读到 3，`REFRESH` 后读到 4；`dqc_lake_ods.sh` 里为此循环刷新 3 次（坑 31）。

**因此本设计的可见性口径**：

```
端到端延迟 = trigger(5min) + commit(~2s) + REFRESH(<1s) ≈ 5.1 分钟（走显式刷新）
           = trigger(5min) + 后台刷新(≤10min) ≈ 最坏 15 分钟（不刷新）
```

**硬约束**：**不要每次 commit 后刷新**（288 次/天 × N 表 × 全 FE 元数据重载）。用 1 分钟节拍的小脚本刷新，或「查询前刷新」（复用 `dqc_lake_ods.sh` / `recon_lake_wh.sh` 里已有的刷新约定）。

**PoC 必测项**（我无法在不写生产数据的前提下测出，必须由实现者测）：写入后不刷新，按 0/1/2/5/10/15/20 分钟打点，记录「首次可查时间」，**用来确认 10 分钟后台刷新是否真的生效**。

### 3.4 与 `lake_ods_replay`（按 dt 覆盖写）的写冲突

**硬结论：RT 表必须独立，主表由 T+1 独占。**

如果强行同表（**不要这样做**），三种时序都会出问题：

| 时序 | 结果 |
|---|---|
| 流先提交，replay 后覆盖 | replay 的 `INSERT OVERWRITE ... dynamic` 只删「它读到的那个 dt 的文件」，会把流刚写的文件一起删掉；数据由 replay 自己从 Kafka 全量补回（不丢），但流的写入「落空」，且**流的 checkpoint 认为自己写成功了** |
| replay 先提交，流后提交 | 流的提交基于旧 base，hadoop catalog 无锁，靠 `version-hint` 检测冲突 → 冲突则重试（`commit.retry.num-retries=4`），重试后 **append 到新快照** → **重复行**；不冲突则直接追加 → 同样重复 |
| 真正并发 | 一方 `CommitFailedException`（hadoop catalog 的 `HadoopTableOperations.commit` 校验版本号）；Spark 重试 4 次仍失败 → **`lake_ods_replay` 节点失败 → 11 节点链断在第 1 环** |

**隔离设计的三个配套**：

1. **表名**：`lake.ods.ods_order_rt` / `lake.ods.ods_order_event_rt`，schema **从 `sql/lake/lake_ods.sql` 原样复制**（列名/类型/`TIMESTAMP_NTZ`/`PARTITIONED BY (dt)` 一字不差），保证将来 `UNION ALL` 或视图不报错。
2. **写权限**：约定「`*_rt` 表只允许流式作业写」，在 `README`/运维手册里写死；批量重放永远不碰 `_rt`。
3. **生命周期**：RT 表按 dt 滚动清理，只保留最近 7 天（迟到事件回拨 3 天，留一倍余量）。

   ⚠️ **实测纠正：Iceberg 1.9 + hadoop catalog 不支持分区级 `DROP PARTITION`**——`ALTER TABLE ... DROP PARTITION WHERE ...` 报 `[PARSE_SYNTAX_ERROR] near 'WHERE': missing '('`，`DROP PARTITION (dt = DATE '...')` 报 `[INVALID_PARTITION_OPERATION.PARTITION_MANAGEMENT_IS_UNSUPPORTED] ... does not support partition management`。正确姿势是**三步：逻辑删 → 重写压实 → 过期快照**（物理文件在 `rewrite` 时才真正回收）：

   ```bash
   # 在 spark 容器内执行；CUTOFF 由 shell 算成字面量（Spark SQL 吃 TIMESTAMP 字面量，不吃 now()-3d）
   CUTOFF=$(date -d '-7 days' '+%Y-%m-%d %H:%M:%S')
   docker exec spark /opt/spark/bin/spark-sql --master local[2] --driver-memory 700m \
     --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
     --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
     --conf spark.sql.catalog.lake.type=hadoop \
     --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse \
     -e "DELETE FROM lake.ods.ods_order_event_rt WHERE dt < DATE '$(date -d '-7 days' '+%F')';
         CALL lake.system.rewrite_data_files(table => 'ods.ods_order_event_rt', strategy => 'binpack',
              options => map('min-input-files','2','target-file-size-bytes','33554432'));
         CALL lake.system.expire_snapshots(table => 'ods.ods_order_event_rt',
              older_than => TIMESTAMP '${CUTOFF}', retain_last => 10);"
   ```

   | 步骤 | 作用 | 代价（实测） |
   |---|---|---|
   | `DELETE FROM ... WHERE dt < ...` | 分区级逻辑删，生成 delete file（v2 merge-on-read） | 秒级 |
   | `rewrite_data_files(...binpack)` | 重写数据文件并**丢掉落进删除谓词的行** → 物理回收 | 30 文件→12 文件、5.6MB，**6.5s** |
   | `expire_snapshots(older_than => TIMESTAMP '...')` | 删旧快照及其文件（否则文件永远留着） | 秒级 |
   | `rewrite_manifests` | 每 7 天一次，9 manifest → 1，**2.7s** | 可选 |

   这样 RT 表**永不增长**（稳态 <50MB / <300 文件）。注意 `DELETE` 后**必须**再 `rewrite`，否则数据文件还在，删了个寂寞。

### 3.5 checkpoint / 状态目录位置与磁盘占用

| 项 | 取值 | 说明 |
|---|---|---|
| 位置 | 宿主 `/home/l/offline-dw/lake/checkpoint/<job>/`（容器内 `/opt/offline-dw/lake/checkpoint/...`） | 与 warehouse 同级、同一 bind mount，容器重建不丢 |
| **不要**放容器 `/tmp` | 容器重建即丢 → 从 `latest` 重来，丢当天数据 | —— |
| 实测占用 | 8 批 164KB（offsets 68K、commits 68K）≈ **20KB/批** | 无聚合状态 → **无 state store / 无 RocksDB** |
| 稳态估算 | `minBatchesToRetain=100`（默认）→ 100 × 20KB ≈ **2MB/作业**；288 批/天也不会线性涨 | 磁盘可忽略 |
| 真正的磁盘成本 | 在 Iceberg 元数据（12MB/天，§3.1）与小文件 | 治理点在 3.1 |
| 禁止操作 | 手工 `rm -rf checkpoint` 而不先 `expire` 表 → 从 earliest 重放 312 万条并写重复 | 写进 runbook |

### 3.6 反压与 Spark 2g 内存的可行性推演

**数据量侧**：3.9 条/s（10× 峰值），5 分钟批 1,170 行。**实测吞吐 4.6~7.3 万行/秒**（读 + `from_json` + 写 Iceberg），即单批 1,170 行耗时 <0.1s，commit 1~2s。**计算侧毫无压力，永远触发不到反压**。

**内存侧（实测三个点）**：

| driver | 结果 | 峰值 |
|---|---|---|
| 400m | ❌ 直接拒绝启动：`[INVALID_DRIVER_MEMORY] System memory 419430400 must be at least 471859200` | 硬下限 **450MiB** |
| **512m** | ✅ 跑完 449,991 行 / 38 文件 | 容器 ~1.0GiB |
| 700m | ✅ 跑完 399,992 行 / 35 文件 | java RSS **677MiB**，容器 ~1.0GiB |

**能不能和批作业挤在同一个 2g 容器？不能。**
批链路峰值 <1.1GiB（坑 33 实测），流式 0.68GiB 堆 → 合计 1.78GiB，再加 metaspace/直接内存/页缓存必撞 2g；且 02:00 批量跑时正好是流式最活跃的时段，OOM 会同时打断两条链路。

**结论与代价**：

| 选项 | 代价 | 判定 |
|---|---|---|
| **A1 新增 `spark-rt` 容器 `mem_limit=1.2g`** | 机器 +1.2g（10.5%），实测容器 RSS ~1.0GiB，余量 200MB | ✅ **推荐** |
| A2 与批同容器、错峰 | 02:00 停流 2 分钟（数据由 T+1 兜底） | ⚠️ 可作为 PoC 首晚的省事做法，长期不接受 |
| A3 批作业跑时 `stop` 流 | 每天丢 2 分钟热层，且 DS 要加编排 | ❌ 把简单问题做复杂 |

> 注意：这 1.2g 必须**从别处挤**。当前 `limit` 之和 ≈18.6GB、WSL 只有 11GiB，已经超卖；调研期间（09:55）观察到 StarRocks 与 DolphinScheduler 容器被重启过一次（`OOMKilled=false`、`exit=0`、原因未查明），说明余量本就很薄。

**反压参数**（给死值，别调）：

```
--conf spark.sql.streaming.minBatchesToRetain=100
--conf spark.sql.shuffle.partitions=8        # 不是 32：流式每批才千行
--conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true
```
`maxOffsetsPerTrigger=20000`（≈10× 峰值下 17 个 5 分钟批的量），正常永远触发不到，纯粹防「某天 topic 被灌爆」。

### 3.7 其它硬约束（踩过的坑，直接抄）

| # | 约束 | 依据 |
|---|---|---|
| 1 | **bootstrap 必须是 `kafka:29092`** | 实测用 `kafka:9092` 会拿到 advertised `localhost:9092`，Spark 只在 WARN 里刷 `Connection to node 1 (localhost/127.0.0.1:9092) could not be established`，最终 **0 行、看起来像「没数据」** |
| 2 | 时间字段 `TIMESTAMP_NTZ` + `spark.sql.session.timeZone=Asia/Shanghai` | 否则 StarRocks 侧偏 -8h（`sql/lake/lake_ods.sql` 头号坑） |
| 3 | 写文件属主 `185:185` | spark 镜像默认 uid=185；warehouse 根是 `drwxrwxrwt`。新容器同镜像，无需 chown |
| 4 | 不能进 DS 工作流 | 11 节点串行、`timeout=240min`、`SERIAL_WAIT`；常驻作业会卡死整条链 |
| 5 | 容器 `restart: unless-stopped` | 否则宿主重启后热层静默消失；DS 不会因此变红（坑 15） |
| 6 | 不许手工删 checkpoint、不许对 `_rt` 表跑批 | 前者重放 312 万条，后者破坏 §3.4 隔离 |

---

## 4. 分层落地路线

### 4.1 阶段 1：PoC（一晚，4 小时运行）

> **若拍板选 A0（§2.4）**，PoC 缩短到 ~40 分钟，判据取下面 1/3/6/7 四条 + 第 8 条：
> 建 DS 工作流 `rt_refresh_5m`（cron `*/5 * * * *`，1 节点，`bash scripts/lake/lake_ods_replay.sh --date $(date +%F)`）→ 跑 2 小时 → 验「每 5 分钟内 `max(event_time)` 追平墙钟」「DQC 6/6 仍绿」「对账 26 项 0 差异」「02:00 批链路时长不变（±10s）」。
> 回滚：DS 工作流「下线」。

**动作**：建 RT 表 → 起 `spark-rt` → 跑 4 小时（含 10× 峰值压测：临时提高生成器速率，**只写 `/tmp` 不写 warehouse**）→ 中途 `kill -9` 一次 → 出报告。

**验收判据（全部满足才算过）**：

| # | 判据 | 测法 |
|---|---|---|
| 1 | 端到端 **P95 ≤ 6 分钟** | `max(event_time)` 在 Iceberg 侧 vs 当前墙钟，每分钟采样一次，算分布 |
| 2 | 连跑 4 小时**不崩溃**；`kill -9` 后 ≤3 分钟自动续跑，**重复 ≤1 批** | `docker inspect --format '{{.State.OOMKilled}}'`；对比重启前后 `count(*)` 增量 |
| 3 | 按 dt 计数：RT 表 vs 主表同 dt，差 ≤ **0.1%**（去重后） | §5.4 的 DQC SQL |
| 4 | 内存：容器 RSS **≤1.05GiB**，`OOMKilled=false` | `docker stats` 采样 + `docker inspect` |
| 5 | 文件数：4 小时 parquet ≤ **200/表**；跑一次 compaction 后单 dt = **1 文件**、行数不变 | `.files` 元数据表 |
| 6 | StarRocks 可见性：`REFRESH` 后 ≤6 分钟可查；不刷新记录首次可见分钟数 | §5.5 |
| 7 | **对 T+1 零影响**：11 节点仍 114~130s、DQC 6/6、对账 26 项 0 差异 | 次日 02:00 正常跑完 |

**回滚动作**：`docker stop spark-rt && docker rm spark-rt`（RT 表可留，不占地方；删表见 §4.4）。

### 4.2 阶段 2：灰度（7 天，与 T+1 并行）

- 流式持续跑，**不动任何现有脚本**；只在 DS 加 **1 个** 5 分钟健康检查节点（第 12 个节点，只读：checkpoint 最新 batch 时间、Kafka lag、RT 表 `max(dt)`）。
- 每天 03:00 追加一条对账（可挂进 `lake_ops_ledger.sh`，不新增节点）：`rt_vs_batch_diff`（按 dt 的行数差 + 抽样逐值比对）。
- 判据：7 天内 **`rt_vs_batch_diff` 的绝对值 ≤ 3 行/天**（迟到事件回拨 3 天导致的正常差异），且**无一天超 10 行**；健康检查节点 7 天全绿。
- 回滚：同上，`docker stop` 即退。

### 4.3 阶段 3：切换（T+1 降级为兜底）

- 对外口径改为「**T+1 全量 + 当天 5 分钟热层**」；StarRocks 建视图（`lake.dws.v_ods_order_event_hot`）供 Ad-hoc 查询，DWS/ADS 仍由 T+1 物化。
- 可选（**本期建议不做**）：把 `run_dws_ads_from_lake.sh` 的增量区间 `D2` 从「湖侧 dwd MAX(dt)」扩到「今天」，每 10 分钟跑一次 → 分钟级 DWS。代价是每 10 分钟起一个批作业，**2g 容器与流式容器抢内存**，必须先解决 §3.6 的内存预算。
- 判据：连续 14 天「热层可用率 ≥ 99%」，且 DQC/对账全绿。

### 4.4 回滚（目标 ≤5 分钟，实测可到 1 分钟）

```bash
#!/usr/bin/env bash
# scripts/rt/rollback_rt.sh（草案，实现者自行落地；本草案不写仓库）
set -euo pipefail
HOST_WAREHOUSE=/home/l/offline-dw/lake/warehouse

echo "[1/4] 停流式容器（2s）"
docker stop spark-rt || true

echo "[2/4] 摘掉热层视图（1s，视图不存在则跳过）"
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -e \
  "DROP VIEW IF EXISTS lake.dws.v_ods_order_event_hot" || true

echo "[3/4] 确认 T+1 主表完好（1min）"
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot --batch --raw --skip-column-names -e \
  "SELECT 'event', count(*), max(dt) FROM lake.ods.ods_order_event"

echo "[4/4] RT 表保留不删（再上线可续跑）；要彻底清掉执行："
cat <<'SQL'
  -- 不能 DROP PARTITION（Iceberg 1.9 + hadoop 不支持，见 §3.4）；清理顺序：
  DELETE FROM lake.ods.ods_order_event_rt;                 -- 逻辑清空
  CALL lake.system.rewrite_data_files(table => 'ods.ods_order_event_rt');   -- 物理回收
  CALL lake.system.expire_snapshots(table => 'ods.ods_order_event_rt',
       older_than => TIMESTAMP '<7 天前字面量>', retain_last => 1);
  -- 或彻底删表（不可逆，需先确认无人查询）
  -- DROP TABLE lake.ods.ods_order_event_rt PURGE;
SQL
```

**回滚时间分解**：`docker stop` 2s + 视图 1s + 校验 60s ≈ **65 秒**；**不需要**改 `lake_ods_replay`、`lake_dwd_incremental`、`lake_maintain`、DS 工作流——这是「独立 RT 表」换来的最大收益。

---

## 5. PoC 需要的代码/命令清单（骨架，不含完整实现）

### 5.1 加一个容器（`docker-compose.yml` 片段，**照抄 `spark` 服务的挂载**）

```yaml
  spark-rt:
    image: apache/spark:3.5.1
    container_name: spark-rt
    hostname: spark-rt
    restart: unless-stopped
    mem_limit: 1200m            # 实测容器 RSS ~1.0GiB，留 200MB
    environment:
      TZ: Asia/Shanghai
    entrypoint: ["bash","-lc","sleep infinity"]
    volumes:
      - ./scripts:/opt/offline-dw/scripts:ro
      - ./sql:/opt/offline-dw/sql:ro
      - ./lake:/opt/offline-dw/lake            # warehouse + checkpoint，同一挂载
      - ./spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar:/opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar
      - ./spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar:/opt/offline-dw/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar
      - ./spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar:/opt/offline-dw/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar
      - ./spark/jars/kafka-clients-3.4.1.jar:/opt/offline-dw/jars/kafka-clients-3.4.1.jar
      - ./spark/jars/commons-pool2-2.11.1.jar:/opt/offline-dw/jars/commons-pool2-2.11.1.jar
```
> ⚠️ 必须挂**单个 jar 文件**，不能挂 `./spark/jars` 整个目录——bind mount 会盖掉镜像自带 252 个 jar（README 已记录）。

### 5.2 建 RT 表（`sql/lake/lake_ods_rt.sql`，schema 从 `lake_ods.sql` 原样复制）

```sql
CREATE TABLE IF NOT EXISTS lake.ods.ods_order_event_rt (
    order_id     BIGINT        COMMENT "订单ID（湖侧允许 NULL）",
    user_id      BIGINT        COMMENT "用户ID（脏数据：可能为 NULL）",
    product_id   BIGINT        COMMENT "商品ID",
    amount       DECIMAL(10,2) COMMENT "金额（脏数据：可能为负）",
    order_time   TIMESTAMP_NTZ COMMENT "⚠️ 下单时间（东八区墙钟时间）—— 必须 NTZ",
    event_type   VARCHAR(16)   COMMENT "order / pay / ship / finish / cancel",
    event_time   TIMESTAMP_NTZ COMMENT "⚠️ 事件发生时间",
    dt           DATE          COMMENT "= to_date(event_time)"
)
USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES (
  'format-version'='2',
  'write.parquet.compression-codec'='zstd',
  -- 小文件/元数据治理（§3.1）
  'write.metadata.delete-after-commit.enabled'='true',
  'write.metadata.previous-versions-max'='50',
  'write.target-file-size-bytes'='134217728',
  'write.distribution-mode'='none',          -- 流式默认；开 hash 会把每批拆成更多文件
  'commit.retry.num-retries'='4'
);
-- ods_order_rt 同构，去掉 event_* 三列、加 status VARCHAR(32)
```

### 5.3 提交命令（可直接复制）

```bash
docker exec spark-rt /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --driver-memory 512m \
  --conf spark.sql.session.timeZone=Asia/Shanghai \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.streaming.minBatchesToRetain=100 \
  --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar,/opt/offline-dw/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/offline-dw/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/offline-dw/jars/kafka-clients-3.4.1.jar,/opt/offline-dw/jars/commons-pool2-2.11.1.jar \
  /opt/offline-dw/scripts/lake/rt_ingest.py \
    --topic ods_order_event \
    --table lake.ods.ods_order_event_rt \
    --checkpoint /opt/offline-dw/lake/checkpoint/ods_order_event_rt \
    --trigger "5 minutes"
```

### 5.4 骨架（`scripts/lake/rt_ingest.py`，~40 行能跑）

```python
spark = (SparkSession.builder
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "file:///opt/offline-dw/lake/warehouse")
    .getOrCreate())

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")     # ← 必须 29092
       .option("subscribe", args.topic)
       .option("startingOffsets", "latest")                  # ← 首次必须是 latest
       .option("maxOffsetsPerTrigger", "20000")              # 反压保护
       .option("failOnDataLoss", "false")
       .load())

df = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("j"))
      .select("j.*")
      .withColumn("order_time", F.to_timestamp("order_time"))   # 先 String 再 cast，别直接声明 Timestamp
      .withColumn("event_time", F.to_timestamp("event_time"))
      .withColumn("dt", F.to_date("event_time")))

(df.writeStream
   .format("iceberg")
   .outputMode("append")
   .option("checkpointLocation", args.checkpoint)
   .option("fanout-enabled", "false")
   .trigger(processingTime=args.trigger)
   .toTable(args.table))            # 等价于 query.start()，但表名写在元数据里更清晰
```

**关键点（每条都对应一个坑）**：

| 要点 | 原因 |
|---|---|
| `startingOffsets=latest` | 用 `earliest` 会把 312 万条历史重放进 RT 表（重复 + 数小时） |
| `.option("fanout-enabled","false")` | 多分区写入时避免每 task 打开过多文件句柄 |
| `toTable()` 而非 `start(path)` | 表名进 checkpoint 元数据，重启后不会因为路径漂移写错表 |
| `to_timestamp` 而不是 `CAST(... AS TIMESTAMP)` | 保持 `timestamp_ntz` 语义，StarRocks 侧不偏 8h |
| 不加 `.cache()` | 流式 DataFrame 不能 cache，且当前量无需 |

### 5.5 观测：延迟 / 文件数 / 内存 / StarRocks 可见性

> **注意**：裸 `spark-sql` 默认没有 Iceberg catalog，直接查 `lake.ods.*` 会 `TABLE_OR_VIEW_NOT_FOUND`。
> 下面所有命令都靠这个包装（写成 `scripts/rt/_rt_sql.sh` 或直接贴在终端里）：
> ```bash
> rt_sql() {   # 用法: rt_sql "SELECT ..."
>   docker exec spark /opt/spark/bin/spark-sql \
>     --master local[2] --driver-memory 700m \
>     --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
>     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
>     --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
>     --conf spark.sql.catalog.lake.type=hadoop \
>     --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse \
>     --conf spark.sql.session.timeZone=Asia/Shanghai \
>     -e "$1" 2>/dev/null | grep -v "^Time taken"
> }
> ```
> 实测要点：`--jars` + `spark.sql.extensions` **两个都不能少**，否则 `CALL ...` 直接 `[PARSE_SYNTAX_ERROR]`。

```bash
# 1) 当前热层数据水位（Iceberg 侧）—— 延迟指标就看这个
rt_sql "SELECT max(event_time) AS hw, max(dt) AS max_dt, count(*) AS c
        FROM lake.ods.ods_order_event_rt;"

# 2) 文件数与元数据体量（判断 §3.1 是否失控）
rt_sql "SELECT count(*) files, sum(file_size_in_bytes)/1024/1024 AS mb
        FROM lake.ods.ods_order_event_rt.files;"
docker exec spark du -sh /opt/offline-dw/lake/warehouse/ods/ods_order_event_rt/metadata

# 3) 快照与每次提交写了多少（每次提交应只有 1~6 个文件、百来行）
rt_sql "SELECT snapshot_id, committed_at,
               summary['added-data-files'] AS files, summary['added-records'] AS rows
        FROM lake.ods.ods_order_event_rt.snapshots ORDER BY committed_at DESC LIMIT 10;"

# 4) 内存/是否被 OOM
docker stats --no-stream spark-rt
docker inspect spark-rt --format 'restarts={{.RestartCount}} oom={{.State.OOMKilled}}'

# 5) Kafka 落后量（流式有 consumer group，可直接看 lag）
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group spark-rt-ods_order_event 2>/dev/null | awk 'NR==1||/ods_order/'

# 6) StarRocks 可见性打点（PoC 必做，见 §3.3）
#    写入后不刷新，按分钟打点，记录「首次可查到该条」的时刻
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -e "
  SET @t0=NOW();
  REFRESH EXTERNAL TABLE lake.ods.ods_order_event_rt;
  SELECT CONCAT('refresh_ok rows=', COUNT(*), ' max_event_time=', IFNULL(MAX(event_time),'NULL'))
  FROM lake.ods.ods_order_event_rt;"
```

**StarRocks 侧查询别写「未来日期」**：湖里已有 2026-10~12 月的未来分区（生成器一次性灌到 12-18），
`WHERE dt >= current_date()-3` 这类条件会把未来数据一起算进来，对账/告警一律显式写上界：
`WHERE dt BETWEEN date_sub(current_date(),3) AND current_date()`。

### 5.6 DQC：用 SQL 校验（3 条，加进 `sql/dqc/dqc_lake_ods.sql` 或独立 `dqc_lake_rt.sql`）

```sql
-- R1 水位：热层最大 dt 不得落后当天超过 1 天
SELECT IFNULL(MAX(dt), DATE '1970-01-01') >= date_sub(current_date(), 1) AS ok_wm,
       MAX(dt) AS max_dt
FROM lake.ods.ods_order_event_rt;

-- R2 当日行数落在合理区间（前 7 天均值 ×0.7 ~ ×1.5）
SELECT IFNULL(SUM(rows),0) BETWEEN 0.7*avg7 AND 1.5*avg7 AS ok_rows, IFNULL(SUM(rows),0) AS rows_today
FROM (
  SELECT dt, COUNT(*) AS rows FROM lake.ods.ods_order_event_rt
  WHERE dt = current_date() GROUP BY dt
) t, (
  SELECT AVG(c) AS avg7 FROM (
    SELECT dt, COUNT(*) c FROM lake.ods.ods_order_event_rt
    WHERE dt BETWEEN date_sub(current_date(),8) AND date_sub(current_date(),1)
    GROUP BY dt) x
) a;

-- R3 与 T+1 主表同 dt 差（灰度期核心判据）
SELECT r.dt, IFNULL(r.c,0) rt_rows, IFNULL(b.c,0) batch_rows, IFNULL(r.c,0)-IFNULL(b.c,0) diff
FROM (SELECT dt, COUNT(*) c FROM lake.ods.ods_order_event_rt WHERE dt >= date_sub(current_date(),3) GROUP BY dt) r
FULL OUTER JOIN (SELECT dt, COUNT(*) c FROM lake.ods.ods_order_event WHERE dt >= date_sub(current_date(),3) GROUP BY dt) b
  ON r.dt = b.dt
ORDER BY r.dt DESC;
```
> ⚠️ 标量子查询不匹配分区返回 `NULL` 不是 0（坑 34）——判据里一律 `IFNULL`。

### 5.7 每日 compaction + 清理（挂进 `lake_maintain`，见 §3.1 / §3.4）

```bash
# 关键：expire_snapshots 的 older_than 只吃 TIMESTAMP 字面量；DROP PARTITION 在 Iceberg 1.9+hadoop 不可用
CUTOFF=$(date -d '-3 days' '+%Y-%m-%d %H:%M:%S')
CUTDT=$(date -d '-7 days' '+%F')
docker exec spark /opt/spark/bin/spark-sql --master local[2] --driver-memory 700m \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse \
  -e "
  CALL lake.system.rewrite_data_files(table => 'ods.ods_order_event_rt', strategy => 'binpack',
       options => map('min-input-files','4','target-file-size-bytes','33554432','rewrite-all','false'));
  CALL lake.system.rewrite_manifests(table => 'ods.ods_order_event_rt');
  DELETE FROM lake.ods.ods_order_event_rt WHERE dt < DATE '${CUTDT}';
  CALL lake.system.rewrite_data_files(table => 'ods.ods_order_event_rt', strategy => 'binpack',
       options => map('min-input-files','2','target-file-size-bytes','33554432'));
  CALL lake.system.expire_snapshots(table => 'ods.ods_order_event_rt',
       older_than => TIMESTAMP '${CUTOFF}', retain_last => 10);"
```

**验收判据（`lake_maintain` 的 `--rewrite` 阶段照这个判）**：跑一次后 `files` 数下降、`count(*)` 不变、`metadata` 目录不增大；
不满足只告警不阻断主表维护（RT 表是热层，坏了不影响 T+1）。

---

### 5.8 若拍板选 A0：只需要三件东西

**① wrapper（新增 `scripts/rt/rt_refresh_5m.sh`）——必须自己拿锁**

```bash
#!/usr/bin/env bash
# 每 5 分钟把「今天」的 dt 分区重算覆盖一次（幂等；复用 lake_ods_replay.sh，不新增任何 Python）
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PROJ="$(cd "$HERE/../.." && pwd)"
SPARK_CT="${SPARK_CT:-spark}"
LOCK_DIR="${LOCK_DIR:-/tmp/.spark_job.lock}"        # 容器内路径
# shellcheck source=../lake/_spark_lib.sh
source "$PROJ/scripts/lake/_spark_lib.sh"
spark_ensure_running
spark_acquire_lock                                  # ★ 和批链路共用一把锁：不会和 02:00 的 11 节点撞 2g
DATE="$(TZ=Asia/Shanghai date +%F)"
DRIVER_MEM="${DRIVER_MEM:-700m}" SPARK_MASTER="${SPARK_MASTER:-local[2]}" \
  bash "$PROJ/scripts/lake/lake_ods_replay.sh" --date "$DATE"
```

**② `lake_ods_replay.sh` 的 3 行补丁**（现在 `--driver-memory 1g` 与 `local[4]` 写死，A0 要压到 700m/local[2]，否则 5 分钟一次会把 2g 容器顶满）

```diff
 SPARK_ARGS=(
   --master "${SPARK_MASTER:-local[4]}"
-  --driver-memory 1g
+  --driver-memory "${DRIVER_MEM:-1g}"
```
默认值不变 → **T+1 链路零影响**；同时和 `_spark_lib.sh` 的 `DRIVER_MEM` 约定对齐。

**③ DS 工作流 `rt_refresh_5m`（1 节点，cron `*/5 * * * *`）**

| 项 | 取值 |
|---|---|
| 节点类型 | SHELL（1 个） |
| 脚本 | `bash scripts/rt/rt_refresh_5m.sh` |
| 调度 | `*/5 * * * *`，`FAILURE_STRATEGY=CONTINUE`（一次失败不影响下一次） |
| 超时 | 4 分钟（跑满 4 分钟 = 已经在堆积，DS 会标红，正好当告警） |
| 放哪 | **独立工作流**，不要塞进 `offline_dataware_lake`（11 节点串行、`timeout=240` 分钟） |

**验收命令（每 5 分钟看一次，连看 2 小时）**

```bash
# 端到端延迟 = 墙钟 - 湖侧水位（应稳定落在 0~6 分钟）
rt_sql "SELECT max(event_time) AS hw, max(dt) AS max_dt, count(*) AS c FROM lake.ods.ods_order_event;"
date '+now=%F %T'

# 每次执行的耗时分布（DS 的库在 mysql 容器里；DS 容器自己没装 mysql 客户端）
docker exec mysql sh -lc 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "
  SELECT id, name, state, start_time, end_time,
         TIMESTAMPDIFF(SECOND, start_time, end_time) AS secs
  FROM dolphinscheduler.t_ds_process_instance
  WHERE name LIKE '\''rt_refresh_5m%'\'' ORDER BY id DESC LIMIT 20;"' 2>/dev/null

# 有没有把 T+1 拖慢（次日 02:00 对比 114~130s 基线）
# 有没有把 2g 容器顶爆
docker inspect spark --format 'oom={{.State.OOMKilled}} restarts={{.RestartCount}}'
```

**A0 的已知短板**（写进 runbook，别踩）：① 每次重写整个当天分区 → 写放大；② `startingOffsets=earliest` 使单次成本 ∝ Kafka 保留全量（10× 前按 §2.4 补 offset 窗口）；③ DS 每天多 288 条执行记录。

---

### 5.9 A0 落地验收记录（2026-10-21）

| 项 | 实测 |
|---|---|
| 代码 | `scripts/rt/rt_refresh_5m.sh`（新增 988B）+ `scripts/lake/lake_ods_replay.sh:50` 改为 `--driver-memory "${DRIVER_MEM:-1g}"`（默认值不变 → T+1 零影响） |
| DS 定义 | `rt_refresh_5m` id **57** / code **23440390609024** / version 1 / `release_state=1`；任务 id **41**（SHELL，`bash /opt/offline-dw/scripts/rt/rt_refresh_5m.sh`，超时 **4 分钟**，失败策略 CONTINUE） |
| 定时 | schedule id **10**，cron **`0 0/5 * * * ? *`**，`release_state=1`，Asia/Shanghai |
| 单次耗时 | 手动 **30.1s / 25.8s**；定时实例 412（13:35）= **25s**、411（13:30）= 51s（含串行等待） |
| 幂等性 | 连跑两次：总行数 **3,108,472** 不变、`dt=2026-10-21` **33,605** 不变、当日 `ods_order` **10,000** 不变 |
| 资源 | `OOMKilled=false`、`RestartCount=0`；锁正常释放 |
| 副作用 | `lake.dwd` 854,844 / 900,000、`dws` 18,000、`ads` 90 均不变（A0 只写湖 ODS 当日分区） |

**踩坑（2026-10-21 实证）**：手工跑到一半被中断会残留 `/tmp/.spark_job.lock`；共享库的残留判定是 **30 分钟**，
期间每 5 分钟的实例都在等锁 → DS 超时（4 分钟）标红；实例 410（13:25）就是这样等了 326s 才跑完并变绿。
**改进建议**：`_spark_lib.sh` 增加可配置等待上限（如 `LOCK_WAIT_SECONDS`），A0 设 0（忙则跳过）或 60s；
或把 A0 超时压到 2 分钟且等待上限 < 超时。

---

## 6. 本阶段明确不做

| 不做 | 原因 / 何时再做 |
|---|---|
| 真实数据源接入、CDC（Debezium/OGG） | 上游仍是模拟生成器；接入属于 C2，与实时化正交 |
| **流式 DWS/ADS 全链路**（分钟级指标） | 每 10 分钟起批作业与流式抢 2g 内存；等内存预算解决（阶段 3 可选项） |
| **Exactly-once 落到 StarRocks 内表** | 需要 `foreachBatch` + `MERGE`，512m driver 跑不动；仓侧仍走 T+1 幂等 upsert |
| 恢复 `ods_order_load` / `ods_order_event_load`（Routine Load） | 与「湖=唯一真源」冲突；仅作为 PoC 失败时的 10 分钟兜底，不纳入设计 |
| 秒级延迟（<10s）、窗口聚合、watermark 乱序处理 | 业务无需求；Flink 方案被否的根本原因 |
| 流式做 dwd 去重 / SCD2 / 用户维 | 去重逻辑复杂（跨天曾致湖少 209,772 行），留给 T+1 |
| 多副本 / HA / Kafka 扩分区 / 容器编排 | 单机 15.9GB，属 C3 生产形态议题 |
| 监控大屏、自动调参、告警平台 | 只加 1 个 DS 健康检查节点 + 3 个指标（水位、文件数、OOM） |

---

## 7. 风险清单

| # | 风险 | 触发条件 | 缓解动作 |
|---|---|---|---|
| 1 | **宿主/容器内存超卖导致进程被杀** | 新增 spark-rt 1.2g 后 WSL 可用 <0.5GiB；`docker inspect OOMKilled=true` 或容器被外部重启 | 上线前先做内存预算（见 §8 待拍板 1）；`mem_limit=1.2g` 硬顶；02:00 批处理时段监控 `free -g`；必要时把 DS 从 2g 降到 1.5g |
| 2 | **at-least-once 重复行污染热层** | 崩溃/重启后重放，`rt_vs_batch_diff` 出现 ≤1 批（~120 行）的增量 | 读侧 `row_number` 去重；每日 T+1 校准；DQC R3 阈值 0.1% 告警 |
| 3 | **StarRocks FE 元数据缓存过期（坑 31）** | 查 RT 表报 `[1064] ... File does not exist: .../snap-xxx.avro`，`REFRESH` 无效 | `previous-versions-max=50`；`expire` 保留 ≥3 天；兜底 `scripts/lake/sr_catalog_reset.sh`（实测 1 秒恢复） |
| 4 | **小文件/元数据膨胀** | 单表 >5,000 文件 或 metadata 目录 >200MB 或 SR 查询 p95 翻倍 | §3.1 四条治理；`lake_maintain --rewrite`；文件数告警阈值 5,000 |
| 5 | **常驻作业静默死亡**（坑 15：DS 不会变红） | checkpoint 最新 batch 时间 >15 分钟未更新 | `restart: unless-stopped`；DS 加 5 分钟健康检查节点；健康检查失败通知 |
| 6 | **首次启动 `startingOffsets` 配错** | 误用 `earliest` → 重放 312 万条，RT 表暴增 + 大量重复 | 提交模板固定 `latest`；上线前预检脚本校验 RT 表 `count(*)` 与 `max(dt)`；PoC 首晚先用 `/tmp` warehouse 验证 |
| 7 | **有人对 `_rt` 表跑批量重放** | 并发 `INSERT OVERWRITE` → `CommitFailedException` 或重复 | 运维手册写死「`*_rt` 只给流式写」；`lake_ods_replay.py` 加表名断言（拒绝 `*_rt`） |
| 8 | **Iceberg 版本升级导致 checkpoint 不兼容** | 升级 `iceberg-spark-runtime` 后重启报 `IllegalStateException: Cannot load table` | RT 作业与批作业用**同一份** jar；升级窗口内先停流式、清 RT 表、从 `latest` 重起 |

---

## 8. 需要拍板的三个点

**1. 形态：上 A1（真流式，+1.2g）还是 A0（5 分钟幂等覆盖，零内存）？**

| 你更在意 | 选 | 代价 |
|---|---|---|
| 内存一点都不能动、只要能查当天 | **A0** | 写放大 288×（0.3~0.6GB/天）、DS 每天多 288 次执行记录、10× 量前必须补 offset 窗口 |
| 要连续消费 / 延迟压到 1 分钟 / 以后接 CDC | **A1** | +1.2g 容器（实测 RSS ~1.0GiB）、要维护 RT 表与 checkpoint |
| 两者都不确定 | **先 A0 跑 1 晚**（半小时能搭完），用同一套 DQC 验收，再决定要不要升级 A1 | 一晚 |

**我的建议：A0 先上，A1 作为二期**。理由：A0 把「口径、DQC、可见性」三件事都用零成本验证掉，且它复用同一张表 → 不需要 RT 表、不需要对账新项、不需要 1.2g。A1 的唯一独占优势是「不受 Kafka 历史量影响 + 延迟可到 1 分钟」，这两条在 33.5k 条/天时都不是瓶颈。
> 如果选 A0，本文档 §3.2（EO）、§3.5（checkpoint）、§5.1~5.4（容器/RT 表/流式骨架）**全部不适用**，只看 §3.1、§3.3、§3.7 与 §5.5~5.7。

**2. 若选 A1：这 1.2g 从哪挤？**

选项：① DS `2g→1.5g`（实占 1.32g，余量变小，风险中等）；② SR `3g→2.5g`（已占 55~80%，风险高，**不建议**）；③ 不动任何限额、接受 WSL 继续超卖（当前 limit 之和 ≈18.6GB > 11GiB，最省事但最脆）。
**我的建议：①+③**，并且上线后第一周每天看一次 `docker inspect --format '{{.State.OOMKilled}}'`。
（依据：调研期间 09:55 观测到 SR 与 DS 被重启过——`OOMKilled=false`、`exit=0`、原因未查明——说明这台机器的余量本来就薄。）

**3. 热层的对外边界？**

(a) 只做内部对账与 Ad-hoc，对外口径仍写 T+1；**(b)** 建 StarRocks 视图对外，接受「刷新才可见、最坏 15 分钟」；(c) 连当天 DWS 一起做（需先解决内存预算）。
**我的建议：PoC 选 (a)，灰度转 (b)，(c) 单独立项**（每 10 分钟起批作业会和流式抢 2g）。

---

## 附录 A：本次只读实验记录（可复现，全部在 `/tmp`，未写生产数据）

| 实验 | 命令要点 | 结果 |
|---|---|---|
| 流式可行性 | `spark-submit --master local[2] --driver-memory 700m` 读 `kafka:29092` `/tmp` checkpoint，console sink，`maxOffsetsPerTrigger=50000` | 5 批 25 万行无异常；容器 RSS 峰值 **932MiB** |
| 写 Iceberg | 同上，`writeStream.format("iceberg").toTable("probe.ods.ods_order_event_rt")`，warehouse `file:///tmp/probe_wh4`，trigger 10s | 8 批 40 万行；**38 个 parquet / 6.3MB**；metadata **328KB**（9 json + 8 snap + 8 manifest）；checkpoint **164KB**；吞吐 4.6~7.3 万行/s |
| 内存下限 | `--driver-memory 400m` | ❌ `System memory 419430400 must be at least 471859200` |
| 内存下限 | `--driver-memory 512m` | ✅ 45 万行正常跑完 |
| 内存峰值 | 700m 采样 26 次 | java RSS 峰值 **677MiB**；容器 **998MiB/2GiB** |
| StarRocks 刷新 | `REFRESH EXTERNAL TABLE lake.ods.ods_order_event` ×3 | **0.19s / 次** |
| FE 刷新配置 | `SHOW ADMIN SHOW FRONTEND CONFIG`（节选） | `background_refresh_metadata_interval_millis=600000`；`iceberg_metadata_memory_cache_expiration_seconds=86500`（坑 31） |

| 单日重放耗时（A0 依据） | `bash scripts/lake/lake_ods_replay.sh --date 2026-12-18 --dry-run`（只读，不写） | **24.3s**（含 JVM 启动 + 两个 topic 全量扫描 + 去重计数） |
| Iceberg 维护语法 | `rewrite_data_files` / `rewrite_manifests` / `expire_snapshots` / `DROP PARTITION`（`spark-sql --jars iceberg + spark.sql.extensions`） | rewrite 30 文件→12、5.6MB、**6.5s**；manifests 9→1、**2.7s**；`expire_snapshots(now()-INTERVAL)` ❌、`'字符串'` ❌、`TIMESTAMP '...'` ✅；`DROP PARTITION` ❌（`PARTITION_MANAGEMENT_IS_UNSUPPORTED`） |
| Kafka 连接串 | `kafka:9092` vs `kafka:29092` | 9092 ❌（advertised `localhost`，0 行）；**29092 ✅** |

**已知环境异常（记录备查）**：调研期间 09:55 观察到 `starrocks`、`dolphinscheduler` 容器被重启（`RestartCount=0`、`OOMKilled=false`、`exit=0`，原因未查明）。此后 SR 内存占用 1.67GiB（55%），与用户所述 76% 的差异**可能**来自此次重启（也可能是 FE 缓存自然回收），故本文档统一按「SR 可能随时回到 76~80%」做保守估算。

---

## 附录 B：与现有资产的映射（改动清单）

| 现有资产 | 改不改 | 说明 |
|---|---|---|
| `scripts/lake/lake_ods_replay.py` | ❌ 不改 | 只加一条「拒绝写 `*_rt`」的断言（可选） |
| `scripts/lake/lake_maintain.py` | ✅ 小改 | 新增 `--rewrite` 阶段 + 2 个盘点指标（§3.1） |
| `docker-compose.yml` | ✅ 新增服务 | `spark-rt`（§5.1） |
| `sql/lake/lake_ods_rt.sql` | ✅ 新增 | RT 表 DDL（§5.2） |
| `scripts/lake/rt_ingest.py` / `rt_ingest.sh` | ✅ 新增 | 流式作业 + 提交封装 |
| `scripts/dqc_lake_ods.sh` / `recon_lake_wh.sh` | ❌ 不改判据 | 灰度期把 R3 对账挂进 `lake_ops_ledger.sh` |
| DS 工作流 `offline_dataware_lake`（v10，11 节点） | ⚠️ 仅灰度加 1 节点 | 健康检查节点；**不把流式作业放进工作流** |
