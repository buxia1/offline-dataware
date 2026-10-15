# 湖侧链路操作手册（交付后由你执行）

> 本文由 agent 交付、**由你执行**。所有命令都已在本机 2026-10-14 的状态下验证过可行，
> 但下面标注 ⚠️ 的 5 处是**已知缺陷**，直接照抄会出问题，必须先按「修复」改掉。
> 目标终局：**湖侧 = 唯一计算源，仓侧 dws/ads = 湖侧数据的物化视图**。

---

## 0. 当前状态盘点（2026-10-14 实测）

| 层 | 湖侧 | 仓侧 |
|---|---|---|
| ODS | `ods_order` 900,000 / `ods_order_event` 3,120,421 / `ods_product` 4,500 | 同 |
| DIM | `dim_product_scd2` 4,500 / `dim_product` 50 | 同 |
| DWD | `dwd_order_detail` 854,844 / `dwd_order_sku_detail` 854,844 / `dwd_order_lifecycle` 900,000 | 同 |
| DWS | `dws_user_order_day` 18,000 | 18,000 |
| ADS | `ads_daily_sales` 90 | 90 |

湖↔仓 10 张表逐项对账（行数 / distinct 主键 / 天数 / 金额合计）**全一致**。
跑全量湖侧 DWD 的命令是 `bash scripts/run_lake_dwd.sh build all`（约 2 分钟），
但**它现在的 `--mode append` 会重复追加**（见 ⚠️ 缺陷 1）。

---

## 1. ⚠️ 必须先修的 5 处缺陷（都在湖侧脚本里，还没修）

### 缺陷 1：`scripts/run_lake_dwd.sh` 用了 `append`，重跑会把三张表翻倍

第 56 行附近：

```bash
  /tmp/o/build.py --table "$TABLE" --mode append $DWD_FLAG $RANGE
```

**为什么错**：Iceberg 表没有 `TRUNCATE`，脚本作者（我）当时用 `append` + 手工 `DELETE` 绕过，
但 `run_lake_dwd.sh` 里**没有 DELETE 这一步** → 同一区间重跑 = 行数翻倍，且不报错。

**改法**（选一种，推荐 A）：

```bash
# A. 让 py 自己先删区间再写（推荐：幂等，可重复执行）
#    lake_dwd_build.py 里在写入前执行：
#      spark.sql(f"DELETE FROM lake.dwd.{table} WHERE dt >= '{d1}' AND dt <= '{d2}'")
#    注意 lifecycle 表没有 dt 列，要按 order_id 集合删：
#      spark.sql(f"DELETE FROM lake.dwd.dwd_order_lifecycle WHERE order_id IN (SELECT order_id FROM scope_ids)")
#    然后 --mode append 就是安全的
# B. 外壳脚本里先调一次 spark-sql 执行 DELETE，再 append
```

**验收判据**：连跑两遍 `run_lake_dwd.sh build all`，行数不变（854,844 / 854,844 / 900,000）。

### 缺陷 2：三个 `run_lake_*.sh` 把远端路径写死

```bash
SCRATCH="${SCRATCH:-/mnt/d/develop/workspace/deepseek_harness_temp}"
```

`/mnt/d/...` 是 WSL 里访问 Windows 盘的路径，**仓库的自包含性被破坏了**。
更糟的是 `run_lake_dws_ads.sh` 第 20 行还会 `cp` 覆盖仓库里的 `scripts/lake_dws_ads.sql`：

```bash
cp "$LOCAL/lake_dws_ads.sql" "$PROJ/scripts/lake_dws_ads.sql"
```

**改法**：

```bash
# 统一改为「从仓库自身取代码」，靠脚本位置推导 PROJ
PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# 挂载 $PROJ/scripts:/opt/offline-dw/app:ro，直接跑 /opt/offline-dw/app/lake_*_build.py
# 删掉 run_lake_dws_ads.sh 里那两行 cp
```

**验收判据**：把仓库整体 `cp -r` 到另一个目录，三个脚本仍能跑通。

### 缺陷 3：`dwd_order_sku_streamload.py` 的 SCD2 关联是 `inner`，会静默丢单

实测：仓侧 SKU 854,844 行，逐日行数与 `dwd_order_detail` 相等，但 `SUM(amount)` 比明细少
**8,476,266.40**（因为 25,534 行金额没取 `ABS`，已修）；修完之后**仍可能**有订单在 SCD2 里
找不到当天版本而被 `inner` 丢掉。

**改法**：改成 `left` 关联，并显式检查失配：

```python
joined = part.join(F.broadcast(scd2), cond, "left")
miss = joined.filter(F.col("valid_from").isNull()).count()
if miss: raise SystemExit(f"WARN: {miss} 单在 SCD2 里找不到当天版本")
```

**验收判据**：`miss == 0`；且 `count(DISTINCT order_id)` == 明细的去重订单数。

### 缺陷 4：生命周期作业的 `--dates` 增量语义**不完整**

`dwd_order_lifecycle_streamload.py` 的增量是「取出该天的 order_id → 只重算这些订单」。
**为什么不够**：`pay/ship/finish/cancel` 事件可能比 `order` 事件晚很多天（实测 `LATE_EVENT` 会把
`event_time` 回拨 3 天，`OOO_EVENT` 也会伪造更早时间戳）。只按「当天新出现的 order_id」重算，
会漏掉「订单已存在、但今天新收到里程碑事件」的情况。

**改法**：增量集合必须取「订单号出现在该区间**任一事件表分区**里」的并集：

```python
scope_ids = (ev.filter((F.col("dt") >= d1) & (F.col("dt") <= d2))
               .select("order_id").distinct())
# 顺带提醒：order_time 可能落在更早的分区，聚合时不能按 dt 过滤事件，
# 要拿 scope_ids 回全表重聚合：ev.join(scope_ids, "order_id").groupBy("order_id")
```

**验收判据**：拿 3 天做增量，结果与「全量重建」在这 3 天的订单上逐列一致。

### 缺陷 5：湖侧写入没有「写后自检」

`lake_dwd_build.py` / `lake_dws_ads_build.py` 写完只打印行数，不校验。
仓侧那几支 `*_streamload.py` 有逐行自检（TAB 列数、逗号检查）与导入后回读比对，湖侧应该同等对待。

**改法**：每次写完，回读「行数 + 金额合计 + 去重主键数」三者，与 Spark 侧期望值比对，不等则 `exit 2`。

---

## 2. 你要做的 4 步

### 步骤 1（基础）：把湖侧脚本参数化 + 修缺陷 1/2

改完后，任何目录下都能：

```bash
PROJ=/path/to/offline-dw
bash "$PROJ/scripts/run_lake_dwd.sh" build all
bash "$PROJ/scripts/run_lake_dwd.sh" build dwd_order_detail 2026-11-01 2026-11-03
bash "$PROJ/scripts/run_lake_dim.sh" build
bash "$PROJ/scripts/run_lake_dws_ads.sh" build
```

**验收**：同一命令连跑两遍，行数不变。

### 步骤 2：湖侧「按天增量」

**为什么**：现在全是全量（2 分钟能跑完，但它是**重算 90 天**；接调度后每天重算 90 天会越积越慢，
而且 Iceberg 每次全量重写会堆快照 —— 已经踩过 152 MB 的坑）。

**推荐方案（按天全量重建，不做 upsert）**：
每天只重建「最近 N 天」（N=3，覆盖迟到/乱序事件：生成器 `LATE_EVENT_RATE` 回拨 3 天）：

```bash
D1=$(date -d "-3 day" +%F); D2=$(date +%F)
bash scripts/run_lake_dwd.sh build dwd_order_detail   "$D1" "$D2"
bash scripts/run_lake_dwd.sh build dwd_order_sku_detail "$D1" "$D2"
bash scripts/run_lake_dwd.sh build dwd_order_lifecycle "$D1" "$D2"   # ⚠️ 缺陷 4 修完才准
bash scripts/run_lake_dws_ads.sh build "$D1" "$D2"                   # ⚠️ 需先给 py 加上区间参数
```

**为什么不推荐做真正的 upsert**：DWS 是 `(user_id, dt)` 主键、ADS 是 `dt` 主键、DWD 是
`(order_id)` / `(dt, order_id)`；Iceberg 的 `MERGE INTO` 能做，但代价是「迟到事件改动了 3 天前
某个已聚合的用户」时，要把受影响的 `(user_id, dt)` 重算 —— 逻辑复杂度远高于按天覆盖。
**按天全量重建 = 幂等 + 简单 + 对账容易**，这是本项目的正确取舍。

**验收**：增量跑完后，湖侧与仓侧对同一区间的结果一致。

### 步骤 3：仓侧 dws/ads 改成「湖侧物化」（你要的这一步）

**为什么必须物化、不能直连湖侧查询**：StarRocks 读 Iceberg external catalog 没有本地索引、
没有分区裁剪之外的优化，下游点查（按用户、按日期）会明显退化。所以保留仓侧表，
但**数据来源从仓侧 dwd 换成湖侧 dwd**。

**代码**（新增两个 SQL + 一个外壳，或用现有 `scripts/dws_ads_streamload.py` 改成读湖侧）：

```sql
-- sql/dws_from_lake.sql
-- 口径与 sql/dws_user_order_day_insert.sql 逐字一致，只把来源换成 lake.dwd.dwd_order_detail
INSERT OVERWRITE dws.dws_user_order_day
SELECT user_id, dt,
       COUNT(*)                                              AS order_cnt,
       COUNT(CASE WHEN status = 'paid'   THEN 1 END)         AS paid_cnt,
       SUM(CASE WHEN status = 'paid'   THEN amount ELSE 0 END) AS paid_amount,
       SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
FROM lake.dwd.dwd_order_detail            -- ★ 唯一改动：仓侧 ods/dwd → 湖侧 dwd
WHERE dt = STR_TO_DATE('${system.biz.date}', '%Y%m%d')
GROUP BY user_id, dt;
```

```sql
-- sql/ads_from_lake.sql
-- 口径与 sql/ads_daily_sales_insert.sql 逐字一致
INSERT OVERWRITE ads.ads_daily_sales
SELECT dt,
       SUM(order_cnt)                                    AS order_cnt,
       SUM(paid_cnt)                                     AS paid_cnt,
       SUM(paid_amount)                                  AS paid_amount,
       SUM(refund_amount)                                AS refund_amount,
       SUM(paid_amount) - SUM(refund_amount)             AS net_amount,
       ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt), 0), 2)   AS avg_order_amount,
       ROUND(SUM(paid_cnt)    / NULLIF(SUM(order_cnt), 0), 4)  AS pay_rate
FROM dws.dws_user_order_day
WHERE dt = STR_TO_DATE('${system.biz.date}', '%Y%m%d')
GROUP BY dt;
```

> ⚠️ **`INSERT OVERWRITE` 对 PRIMARY KEY 表**：`dws_user_order_day` 是 `PRIMARY KEY(user_id, dt)`、
> `ads_daily_sales` 是 `PRIMARY KEY(dt)`，**无分区**。StarRocks 的 PK 表**不支持** `INSERT OVERWRITE PARTITION`，
> 但支持**无分区表的整表 `INSERT OVERWRITE`**。你们现在的
> `sql/dws_user_order_day_insert.sql:21` 用的就是 `INSERT OVERWRITE`（按 `dt` 过滤），
> 所以**照原样能跑**；只有当你要改成「只覆盖某天」时才需要在外面包 `DELETE WHERE dt=...`。
> **建议**：先按天 `DELETE WHERE dt = '${bizdate}'` + `INSERT INTO`，语义最明确、不受版本差异影响。

**验收判据**（三条都要过）：
```sql
-- ① 仓侧 dws/ads 的天数、总量与湖侧一致
SELECT (SELECT count(DISTINCT dt) FROM dws.dws_user_order_day) dws_days,
       (SELECT count(DISTINCT dt) FROM ads.ads_daily_sales)   ads_days;
-- ② 汇总守恒：DWS 的 order_cnt 合计 == 湖侧 detail 行数
SELECT (SELECT sum(order_cnt) FROM dws.dws_user_order_day) dws_oc,
       (SELECT count(*) FROM lake.dwd.dwd_order_detail)     detail_rows;
-- ③ 逐日对账：任意一天两侧金额相等（替换日期）
SELECT (SELECT sum(paid_amount) FROM ads.ads_daily_sales WHERE dt='2026-11-15') wh,
       (SELECT sum(paid_amount) FROM ... 湖侧同口径 ...) lake;
```

### 步骤 4：调度 + 维护 + 对账 接进 DolphinScheduler

**为什么**：现在全是手工跑（`bash scripts/run_lake_*.sh`），没有增量、没有失败告警。
仓侧那套调度（串行丢弃、`ds/alerts/notify.sh` 告警）是现成的，**照抄结构即可**。

接线顺序（串行，别并行 —— 湖侧写同一批 Iceberg 快照会冲突）：

```
[湖侧增量 DWD] → [湖侧 DWS/ADS] → [仓侧 dws/ads 物化] → [对账] → [湖表维护]
```

- **对账节点**：把这次的一次性人工核对沉淀成脚本（我实测用过的口径）：
  ```sql
  -- 一行一表：湖侧 vs 仓侧逐项比
  SELECT 'detail' t, count(*) r, count(DISTINCT order_id) o, sum(amount) a FROM lake.dwd.dwd_order_detail
  UNION ALL SELECT 'detail_wh', count(*), count(DISTINCT order_id), sum(amount) FROM dwd.dwd_order_detail
  UNION ALL SELECT 'sku', count(*), count(DISTINCT order_id), sum(amount) FROM lake.dwd.dwd_order_sku_detail
  UNION ALL SELECT 'sku_wh', count(*), count(DISTINCT order_id), sum(amount) FROM dwd.dwd_order_sku_detail
  UNION ALL SELECT 'lifecycle', count(*), count(DISTINCT order_id), sum(amount) FROM lake.dwd.dwd_order_lifecycle
  UNION ALL SELECT 'lifecycle_wh', count(*), count(DISTINCT order_id), sum(amount) FROM dwd.dwd_order_lifecycle;
  -- 两边对应行的 r/o/a 三个数必须相等，否则告警
  ```
- **维护节点**：`python3 scripts/lake_maintain.py`（只读盘点）+ `--expire --keep N`（清快照）。
  实测已把快照从 181/184 清到各 5 个、磁盘 152 MB → 92 MB，行数不变。
- **⚠️ 别把 ODS 纳入「湖侧增量」**：`lake.ods.*` 是从 **Kafka 重放**来的，不是从仓侧 ODS 拷贝。
  如果你要「仓侧 ODS 也从湖侧来」，那是**另一件事**，得先决定 Routine Load 是否停 —— 
  停了就没有实时摄入了，湖侧重放是全量批处理，延时以小时计。

---

## 3. 终局验收清单（湖仓一体「完工」的判据）

- [ ] 湖侧脚本自包含（不依赖 `/mnt/d/...`），任意目录可跑
- [ ] 湖侧 DWD/DWS/ADS 有**按天增量**，重复执行幂等（连跑两遍行数不变）
- [ ] 湖侧每次写入后**自检**（行数 + 金额 + 去重主键）并失败即退出
- [ ] 仓侧 `dws`/`ads` 由**湖侧物化**（来源 = `lake.dwd.*`），逐日与湖侧相等
- [ ] DolphinScheduler 里湖侧链路**串行**接入 + 失败告警生效
- [ ] 湖表维护（`expire_snapshots` / `remove_orphan_files`）已进调度
- [ ] 对账作业可重复执行（不是一次性人工核对）
- [ ] 湖↔仓对账连续 N 天（建议 7 天）零差异后，才评估停更仓侧 ods/dim/dwd

> 最后一条是关键：**仓侧 ODS/DWD 停更 = Routine Load 停 = 失去实时摄入**。
> 湖侧目前只有离线重放能力，这个决定建议放到最后、并且先并行跑 1~2 个月看差异。

---

## 4. 交接给我的方式

每一步你改完、跑完，把**命令 + 输出尾巴**贴回来即可，我负责：
- 判断判据是否真的满足（尤其是「行数对了但口径错了」这类）
- 补分支场景（迟到事件、跨天订单、NULL user_id 45,156 单这类边界）
- 更新 README / PITFALLS 的对应章节

> 我承诺：从这条消息起，**不再直接执行**湖仓链路的写操作，只给代码与判据。
