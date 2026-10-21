# 湖侧链路操作手册（现状 / 验收脚本 / 调度现状）

> 本文对应仓库提交 `d92be7e` 之后的状态，**已于 2026-10-16 更新到「调度已接入并跑通」**。
> **五处缺陷已修复并实测过、湖侧按天增量已实现并实测过、仓侧 dws/ads 物化（含按天自动增量）
> 已实现并实测过；DolphinScheduler 接线与可重复执行的对账作业也已完成**（见 §2 步骤 4）。
> 已定论：**`remove_orphan_files` 不用做**（2026-10-18 `dry_run` 实测 8 张表全 0，见附录 C）；`user_id IS NULL` 的口径已于 2026-10-18 拍板（见 §2 步骤 2 末）。
> 失败告警已于 2026-10-16 配好（该定时 `warning_type=2` + 告警组 2）。
> 目标终局：**湖侧 = 唯一计算源，仓侧 dws/ads = 湖侧数据的物化视图。**

---

## 0. 现状与结论（2026-10-15 实测）

### 0.1 这一轮方案变化：湖仓边界反转

| 层 | 湖侧（唯一计算源） | 仓侧（物化 / 服务层） |
|---|---|---|
| ODS | `ods_order` 900,000 / `ods_order_event` 3,120,421 / `ods_product` 4,500 | 同 |
| DIM | `dim_product_scd2` 4,500 / `dim_product` 50 | 同 |
| DWD | `dwd_order_detail` 854,844 / `dwd_order_sku_detail` 854,844 / `dwd_order_lifecycle` 900,000 | 同 |
| DWS | — | `dws_user_order_day` 18,000（来源 = `lake.dwd.*`） |
| ADS | — | `ads_daily_sales` 90（来源 = 仓侧 dws） |

**明确对齐口径**（避免再出现文档与代码打架）：

- **ods / dim / dwd 在湖侧算**，结果同步/物化到仓侧同名表；
- **dws / ads 只在仓侧算，且来源是 `lake.dwd.*`** —— 湖侧**不建** dws/ads。

**为什么和最初的文档描述相反（这段是真实历史，不要当成写错）**：第一版方案里湖侧是「影子链路」，
是**从仓侧 ods 拷过去**、只做 DWD/DWS 校验用的；后来明确诉求变成「湖侧当唯一计算源，
dws/ads 下沉仓侧」，边界就反转了。**这是有意的架构转向**，文档原先那套描述已经作废。
教训：凡是要动湖仓边界的改动，必须同步改这条链的每一份说明，否则文档会和实现互相矛盾。

湖↔仓 10 张表逐项对账（行数 / distinct 主键 / 天数 / 金额合计）**全一致**
（口径命令见 §1.1，其中 `detail` / `sku` 的 `SUM(amount) = 140,891,053.50`、
`lifecycle = 144,975,064.50`；仓侧 dws = `18000|90|854844|119749476.90|14069110.40`、
ads = `90|90|854844|105680366.50`）。

跑全量湖侧 DWD：`bash scripts/lake/run_dwd.sh build all`（约 2 分钟，现在幂等；
`run_dwd.sh` 默认模式已修正，但 **defect #1 的 `append` 隐患仍建议你在自己仓库复现一遍 §1.1 的连跑两遍验证**）。

---

## 1. 必须先做的两件事

### 1.1 刷新湖侧外部表元数据（否则你之前看到的一切可能都是旧的）

**这是本轮最有价值的发现，也最隐蔽**：StarRocks 查 Iceberg 外部表用的是**缓存的元数据快照**。
湖侧被 Spark 写过之后（哪怕只是一次 rebuild、一次 append），StarRocks **不刷新就继续返回旧快照**。

实测复现（原始数字）：

```
Spark 侧删掉一条合成行后： total = 854844
StarRocks 不刷新读到的 ： 854845      ← 旧快照
REFRESH EXTERNAL TABLE 之后： 854844  ← 正确
```

**为什么它比想象中更严重**：

1. 容器重启后，StarRocks 重建元数据缓存；若你的水位逻辑依赖「仓侧 `MAX(dt)`」，
   **重启前后会得到不同水位** —— 修过的增量逻辑会被这个掩盖。
2. 物化脚本的第一步就是读 `lake.dwd` 的行数 / 水位，用旧快照 = **用错的区间做对称差自检，
   而且两边一起错、自检还全绿**。

**处置**：`scripts/run_dws_ads_from_lake.sh` 已内建 `refresh_lake()`：物化前先对 5 张湖侧外部表
（`lake.ods.ods_order_event` / `lake.dim.dim_product_scd2` / `lake.dwd.dwd_order_detail` /
`lake.dwd.dwd_order_sku_detail` / `lake.dwd.dwd_order_lifecycle`）执行 `REFRESH EXTERNAL TABLE`，
失败只告警（`ods_order_flat` 是 Kafka 外表，刷新报错属正常）。
**你自己写的任何读湖侧的下游任务，第一步也必须先 `REFRESH EXTERNAL TABLE`。**

> 这段防护已经**直接落在仓库脚本里**：`scripts/run_dws_ads_from_lake.sh` 的 `LAKE_TABLES`
> 数组是**数据驱动**的 —— 每张湖表的「是否关键 / 刷新失败是 `die` 还是只 `warn` / 为什么」
> 都写在数组元素里（`lake.dwd.dwd_order_detail` 是唯一 `critical=1` 的，拿不到新快照直接 `die`）。
> `scripts/recon_lake_wh.sh` 开头也对湖侧表做同样的刷新。

### 1.2 复核附录 A 的 5 处缺陷修复（判据都给好了）

附录 A 是**已修复**的 5 处缺陷（**不是待办**），每处都写了改法、为什么错、以及可执行的验收判据。
现在湖侧单日实测：`2026-09-20` 连跑两遍输出逐行相同、`EXIT=0`、
`before=854844 → after=854844`（未翻倍）、单日 9,475 行 / `sum=1544512.70`。
请在你自己的仓库里照附录 A 的判据复现一遍。

---

## 2. 你要做的 4 步

### 步骤 1（基础）：把湖侧脚本参数化 + 修缺陷 1/2 —— ✅ 已完成，仅需复核

现在任何目录下都能跑：

```bash
PROJ=/path/to/offline-dw
bash "$PROJ/scripts/lake/run_dwd.sh" build all
bash "$PROJ/scripts/lake/run_dwd.sh" build dwd_order_detail 2026-11-01 2026-11-03
bash "$PROJ/scripts/lake/run_dim.sh" build
# run_dws_ads.sh 未入库；湖侧 DWS/ADS 那套已删（见 cfab3e0），
# 现行方案：湖侧只到 DWD，仓侧用 scripts/run_dws_ads_from_lake.sh 物化 dws/ads
```

**验收**：同一命令连跑两遍，行数不变。

### 步骤 2：湖侧「按天增量」 —— ✅ 已完成，仅需按下列判据复核

**已实现**（`scripts/lake/lake_dwd_build.py` 336 → 502 行，`scripts/lake/run_dwd.sh` 88 → 110 行）。
三条关键设计（照抄时别改错，每条都有原因）：

1. **水位取「目标表自己的 `MAX(dt)`」，不是 ODS 的** —— 否则 ODS 某天被重放后水位会跳回过去，
   重复重算甚至回退。取目标表自己的水位才能保证 DWD 永远「落后于或等于 ODS」。
2. **水位的上下界**：`D1 = max(表 MAX(dt) + 1, ODS/dim 的 MIN(dt))`；`D2 = min(ODS 的 MAX(dt), 今天)`。
   表为空 → 回退全区间；`D1 > D2` → 直接 skip（不删不写，`exit 0`），所以倒挂不会误删。
3. **区间是「每张表独立解析」的**，在循环里逐表算，三张表水位不同也不会互相污染。
   删除沿用 `dwd_order_lifecycle` 的 scope 合并（`order_id` 集合），没有另起分支。

**新增的完整性 gate（写后自检）**：每日行数与「从 ODS 现算的期望值」比对 ——
`detail` / `sku` 用非空 `user_id` 的 `COUNT(*)`，`lifecycle` 用 `COUNT(DISTINCT order_id)`。
之所以要有它：以前的 `self_check()` 只跟**自己**对（`before` 与 `after` 互相对得上），
**挡不住「静默丢行」**；现在用 ODS 当独立基准。

**runner 会打印水位**，第二天该从哪天开始一目了然。

**已在沙盒副本验证**（没有碰你的正式数据）——三条：
① 单日 rebuild 后 `before=854844 → after=854844`（幂等）；
② `D1 > D2` 倒挂时 exit 0 且**没有执行任何 DELETE**；
③ 按天增量连跑两次，逐日 `(dt, cnt, sum(amount))` 完全一致。

**验收判据**（你自己跑）：

```bash
D1=$(date -d "-3 day" +%F); D2=$(date +%F)
bash scripts/lake/run_dwd.sh build dwd_order_detail     "$D1" "$D2"
bash scripts/lake/run_dwd.sh build dwd_order_sku_detail "$D1" "$D2"
bash scripts/lake/run_dwd.sh build dwd_order_lifecycle  "$D1" "$D2"
# 每条命令的 RESULT 行：before == after（未翻倍）；同区间连跑两遍，逐日数字不变
```

**`user_id IS NULL` 的口径（2026-10-18 已拍板：保留现状）**：清洗把 `user_id IS NULL` 的
**45,156 单**挡在明细层之外（`ods_order` 900,000 → `dwd_order_detail` 854,844，占 5.02%，
与 `NULL_USER_RATE=0.05` 吻合），而**快照层 `dwd_order_lifecycle` 与 `ods_order` 保留全量**
（实测 NULL 分别 **45,187 / 45,156**）。完整性 gate 只看非空 `user_id` 行 —— 与这个口径一致，
**不需要改**。下游 `dws`/`ads` 跟随明细层：「总订单数」取哪张表必须在报表里写明，
`dwd_order_detail` 是「有用户的订单」，全量订单看 `dwd_order_lifecycle`。
判据：`count(*) - count(user_id)` 明细 **0** / 快照 **45,187** / ODS **45,156**。

### 步骤 3：仓侧 dws/ads 改成「湖侧物化」 —— ✅ 已完成，默认已按天自动增量

**为什么必须物化、不能直连湖侧查询**：StarRocks 读 Iceberg external catalog 没有本地索引、
没有分区裁剪之外的优化，下游点查（按用户、按日期）会明显退化。所以保留仓侧表，
但**数据来源从仓侧 dwd 换成湖侧 dwd**。

**代码**：`scripts/run_dws_ads_from_lake.sh`（110 → **约 325 行**）+ 它内嵌的 SQL。
（当时那 30 条断言的测试脚本 `test_inc_dws_ads.sh` 是一次性工具、未入库；
现在仓库里常驻的回归自测是 `scripts/test_recon.sh`，专门覆盖对账脚本本身。）

**三种运行模式**（默认模式从 TRUNCATE 全量改成了 auto 增量 —— 日常调度无人值守不能再整表清空）：

| 模式 | 命令 | 用途 |
|---|---|---|
| 自动增量（默认） | `bash scripts/run_dws_ads_from_lake.sh` | 调度用。`D2 = 湖侧 MAX(dt)`；`D1 = 仓侧 MAX(dt) - (lookback-1)` |
| 指定区间 | `--range 2026-12-10 2026-12-18` | 补数 / 回刷 / 破坏后自愈 |
| 全量重灌 | `--truncate` | 首次建表、清探针残留行 |
| 只读预演 | `--dry-run` | 不写库，打印区间与预计影响 |

`--lookback N`（**默认 3**）是**回捞窗口**：湖侧迟到事件会改动最近的若干天，
每次物化都要把「仓侧水位往前 N 天」重算，否则那天永远错（湖仓两侧一起错，
逐日对称差自检**发现不了**）。

> **默认值为什么是 3（2026-10-15 从 2 改成 3）**：`lookback=2` 的实际覆盖面是 `[D2-1, D2]`，
> 而事件生成器 `LATE_EVENT_RATE` 会把 `event_time` 回拨 **3 天** —— 也就是
> 「最新日之前 2~3 天到货的事件」落在窗口外，**默认增量永远不重算它，而且自检全绿**。
> 改成 3 后覆盖面 = `[D2-2, D2]`，恰好覆盖生成器的最大迟到跨度。
> 复核实测：`--lookback 2` 时 12-16 的迟到事件不被重算、`dws_mismatch=0`、`EXIT=0`；
> `--lookback 3` 才把它并入。**所以 `lookback` 不是越小越省事，是越小越容易悄悄错。**

**几个必须知道的行为**：

- `refresh_lake()` 在解析区间**之前**执行（见 §1.1）；
- **`lake.dwd.dwd_order_detail` 刷新失败 = 直接 `die`**：水位、期望行数、逐行对称差全来自它，
  拿不到新快照时继续跑只会产出「看起来全绿」的错数据。其余表（含 Kafka 外表
  `lake.ods.ods_order_flat`）刷新失败只 `warn`；
- **`D1` 会钳到湖侧 `MIN(dt)`**，仓侧水位晚于湖侧最新时直接 skip 退出；
- **区间倒挂直接拒**：`D1 > D2` 时 `exit 1` 并打印原因，不写库；
- **`--truncate` 与 `--range` 互斥**：同时给会直接报错退出。
  （以前是「后写的那个静默吃掉前一个」—— 最坏情况是「以为只回刷 2 天」变成
  「两张仓表被 TRUNCATE 后整表重灌」；）
- **`--incremental` = `--auto`**（兼容旧写法）。它以前被写成 `MODE="range"` 却不塞 `POS`，
  在 `set -u` 下**必然崩**（`POS[0]: unbound variable`），而脚本的告警原文还建议你去用它；
- **覆盖 gate**：auto 模式要求「仓侧 `MAX(dt)` 追平 `D2`」且「仓侧 `MIN(dt) <= D1`」，
  否则报错退出 —— 这条 gate 专治「区间对但中间有洞、自检看不出来」；
- **C2 窗口外漂移探测（新增）**：所有对称差 gate 都只看 `[D1, D2]`，
  所以「自检全部通过」可能只是「窗口内没问题」。C2 会把审计区间扩到
  `[FULL_MIN, D1-1]`（本次不回刷的那段）单独数一遍，非 0 就显式告警并给出修复命令：
  报告里会写清「本次逐行对称差只校验了 `D1 ~ D2`」。
  auto 模式下 C2 **只告警不 fail**（调度不该天天红），但假绿被消掉了；
- **渲染产物用 `mktemp -d` + `trap` 清理**：以前是固定的 `/tmp/dws_ads_from_lake.*.sql`，
  跑完不删、两个并发实例会互相覆盖渲染 SQL 并执行到对方的内容；
- **`lake.dwd.*` 的 CSV 暂存在 `/tmp/o/…`**：`run_dwd.sh` 会先 `docker cp` 到容器 `/tmp/o/`。
  如果 `spark` 容器不可写（生产常见），你需要把 `/tmp/o` 换成挂载卷，或把构建脚本烘进镜像。

**验收判据**（三条都要过，实测原始输出见附录 B）：

```sql
-- ① 仓侧 dws/ads 的天数、总量与湖侧一致
SELECT (SELECT count(DISTINCT dt) FROM dws.dws_user_order_day) dws_days,
       (SELECT count(DISTINCT dt) FROM ads.ads_daily_sales)   ads_days;
-- ② 汇总守恒：DWS 的 order_cnt 合计 == 湖侧 detail 行数
SELECT (SELECT sum(order_cnt) FROM dws.dws_user_order_day) dws_oc,
       (SELECT count(*) FROM lake.dwd.dwd_order_detail)     detail_rows;
-- ③ 逐行对称差必须为 0（脚本自带）
--    dws_mismatch / ads_mismatch 都为 0；整表数字应为
--    dws:18000|90|854844|119749476.90|14069110.40
--    ads:90|90|854844|105680366.50
```

### 步骤 4：调度 + 维护 + 对账 + 台账 接进 DolphinScheduler —— ✅ 2026-10-16 已完成（v10，全部 11 个节点）

**已落地**：工作流 **`offline_dataware_lake`**（**v10**，`release_state=1`；v7 九节点 10-16 → v8 于 10-18 补第 10 个节点「巡检台账」→ **v10 于 10-19 补第 3 个节点「湖侧 ODS 闸门」**），定时 `0 0 2 * * ? *`
（每天 02:00，schedule id 7 **已上线**，失败告警 `warning_type=2` / 告警组 2），执行策略「串行等待」。
**端到端实测**：实例 346（8 节点）全绿 **3 分 34 秒**；实例 347（v7，9 节点，
2026-10-16 16:55:15 → 16:59:31）全绿 **4 分 16 秒**；**v8 实例 398 = 260 秒**（优化起点）。
**性能优化（2026-10-19）**：实例 **401 全绿 117 秒（1 分 57 秒）** —— replay 85→25、dim 39→15、dwd 59→29、maintain 40→13；三档改动 = 少起 Spark（W1/W2）+ 快照 expire 改周更（W3）+ `local[4]`/shuffle 32（W4）+ replay 加 `.cache()` 让 Kafka 只读一遍（W7）。数据零变化（对账 26 项 OK / 两个 DQC 通过 / 湖表行数不变）。剩下 ~45 秒是 5 次 Spark 启动的固定成本，不建议再压。**v10（11 节点，加第 3 个节点 `dqc_lake_ods`）实例 403 全绿 114 秒**。
节点名与命令见 `README.md` 的「⭐ 当前生产链路」。

接线顺序（串行，实测就是这个拓扑；别并行 —— 湖侧写同一批 Iceberg 快照会冲突）：

```
[lake_ods_replay] → [lake_ods_product] → [dqc_lake_ods] → [lake_dim_build]
                  → [lake_dwd_incremental] → [wh_dws_ads_materialize]
                  → { [dqc_order_chain], [dqc_dim_product], [recon_lake_wh] }
                  → [lake_maintain] → [lake_ops_ledger]
```

- **对账节点已沉淀成脚本**：`scripts/recon_lake_wh.sh`（**只读**；退出码 `0` 全绿 / `2` 对账不一致
  → DS 告警 / `1` 用法或环境错误）。四层校验，分层是有意的：
  **A 水位·新鲜度** → **B 湖侧闭合**（DWD 每天行数 vs 由 ODS **现算**的期望 —— **这是唯一能发现
  「湖仓两边一起错」的检查，对称差永远发现不了**）→ **C 湖↔仓逐行对称差** →
  **D 仓侧覆盖 / 硬基线**。参数：`--range D1 D2` / `--fast` / `--strict` / `--allow-lag N` / `--json`。
  自测：`bash scripts/test_recon.sh`（内存假数据，**9/9 通过**）；全区间实测 **3.1 秒**。
  注意：它比的是**仓侧 `dws`/`ads`**（仓侧 `ods`/`dwd`/`dim` 已不再被调度更新，拿它们比没有意义）。
- **维护节点：已进调度** ✅（v7 第 9 个节点，2026-10-16）—— wrapper `scripts/lake/run_maintain.sh`
  一条命令，**自动枚举 8 张湖表**
  （不写死表名，以后加表不用改脚本）：
  ```bash
  bash scripts/lake/run_maintain.sh --expire --keep 5     # wrapper：容器检查 + 串行锁 + 退出码透传
  bash scripts/lake/run_maintain.sh --auto               # DS 日常用法：平时只盘点，周日才 expire
  ```
  DS 维护节点（第 10 个）填 `bash /opt/offline-dw/scripts/lake/run_maintain.sh --auto`（2026-10-19 起；平时只盘点约 13 秒、周日才 expire 约 40 秒）。
  `--tables a,b` 可限定表；**不加 `--expire` 就是只读盘点**（零写入，15 秒）。
  **脚本自己验收**：快照数下降 且 行数不变，不达标退出码 `1`（可直接挂进 DS 当告警用）。
  实测 2026-10-16：8 张表快照 `46/18/12/11/9/7/2/2` → **全部 ≤5**，
  `lake/warehouse` **177 MB → 143 MB**、行数逐表不变、退出码 `0`；
  紧接着 `bash scripts/recon_lake_wh.sh` **26 项全绿 / 5.1 秒**。
  已挂进 DS 当第 9 个节点（前置 = 三个质检/对账节点全部结束），实例 347 里本节点 **43 秒**。
- **巡检台账：已进调度** ✅（v8 第 10 个节点，2026-10-18）—— `scripts/ops/ops_ledger.sh` 每天往
  `ds/logs/ops/ledger.csv` 追加一行：链路状态 / 三个闸门 state / 湖表行数 / 快照数 / Kafka 消息数 /
  仓库体积。**从 `lake_maintain` 与 `recon_lake_wh` 的日志解析，不重跑 Spark**（实测 3 秒）。
  它**只记录不判断**（不告警、不修复）；「连续 7 天零差异」就从这本 CSV 数
  （`awk -F, 'NR>1 && $11=="OK"'`，第 11 列 = `recon_result`）；2026-10-19 起扩到 **26 列**（末 5 列 `dq_ods_*` = 湖侧 ODS 闸门结果）。首次定时写入 2026-10-19 02:00。
- **失败告警：已配** ✅（2026-10-16）—— `t_ds_schedules` id 7 `warning_type=2` + 告警组 `2`，
  与四个旧流程一致；每天 02:00 失败会发通知。
- **⚠️ 别把 ODS 纳入「湖侧增量」**：`lake.ods.*` 是从 **Kafka 重放**来的，不是从仓侧 ODS 拷贝。
  如果你要「仓侧 ODS 也从湖侧来」，那是**另一件事**，得先决定 Routine Load 是否停 ——
  停了就没有实时摄入了，湖侧重放是全量批处理，延时以小时计。
  （⛔ 2026-10-19 已退役：`ods_order_load` / `ods_order_event_load` 两个作业都已 `PAUSE`，
   仓侧 `ods` 不再更新；湖侧重放读的是同一批 Kafka topic，与它们无关。）
- **⚠️ dim 表重建后必须清 StarRocks 元数据缓存**：`sql/lake/lake_dim.sql` 每天 `DROP`+`CREATE`，
  Iceberg 元数据从头编号、旧目录被 purge；而 FE 按**路径**缓存元数据（默认 ~24h），于是一段时间内
  `lake.dim.*` 的查询会命中**重建前的旧 snapshot** → DQC 报 `[1064] ... File does not exist: .../snap-xxxx.avro`。
  `REFRESH EXTERNAL TABLE` 清不掉这层缓存（2026-10-19 实测两次均失败），要把 catalog 重建一次：
  `scripts/lake/sr_catalog_reset.sh`（`DROP CATALOG IF EXISTS lake` + `CREATE EXTERNAL CATALOG lake`，
  只重建定义、不动湖数据，实测 1 秒恢复 4500/50 行）。已内置在 `run_dim.sh`（建表+灌数完成后自动跑，`dry` 不跑），
  DS 节点不用改命令；重置失败会直接让节点失败（不会带着坏缓存往下跑）。
- **Kafka 保留期 = 重放窗口上限，30 天**（2026-10-18 实测）：broker `log.retention.hours=720`，
  两个 topic 无覆盖配置；`lake_ods_replay.py` 是 `startingOffsets=earliest` **全量重读**，
  所以消息一过期就再也补不回来。**按生产时间算**：2026-10-14 一次性回填的 90 天数据
  会在 ≈2026-11-13 整体过期，此后 `--date` 只对保留期内的数据有效。
  查窗口：`docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic <topic> --time -2 | -1`
  （当前两个 topic 最早 offset 均为 `0`，即还没有任何消息被清）。

---

## 附录 A：五处缺陷的修复说明（已修，验收判据保留）

| # | 位置 | 原来错在哪 | 现在怎么修 | 验收 |
|---|---|---|---|---|
| 1 | `scripts/lake/run_dwd.sh`（`--mode append`） | 没有 DELETE 就打 `append`，同区间重跑**行数翻倍且不报错** | py 自己在写入前 `DELETE FROM lake.dwd.{table} WHERE dt BETWEEN d1 AND d2`；lifecycle 没有 `dt` 列，按 `order_id IN (SELECT order_id FROM scope_ids)` 删 | 连跑两遍 `run_dwd.sh build all`，854,844 / 854,844 / 900,000 不变。**实测**：`2026-09-20` 单日连跑两遍 `before=854844 → after=854844`（未翻倍） |
| 2 | 三个 `run_lake_*.sh` | `SCRATCH="${SCRATCH:-/mnt/d/...}"` 把 Windows 盘路径写死，仓库不自包含；其中一支还会 `cp` 覆盖仓库里的临时 SQL | 全部改成 `PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"`，挂载 `$PROJ/scripts:/opt/offline-dw/app:ro` 从仓库自身取代码 | 把仓库整体 `cp -r` 到另一目录，脚本仍能跑通 |
| 3 | `dwd_order_sku_streamload.py` | SCD2 关联用 `inner`，SCD2 里找不到当天版本的订单被**静默丢弃**（实测少了 8,476,266.40 金额，其中 25,534 行是漏了 `ABS`，已修） | 改 `left` 关联 + 显式统计失配并报错；金额取 `ABS` | `miss == 0`，且 `count(DISTINCT order_id)` == 明细去重订单数。**注意**：`user_id IS NULL` 的 45,156 行见 §2 步骤 2 末的口径问题 |
| 4 | `dwd_order_lifecycle_streamload.py` | 增量只取「当天新出现的 `order_id`」，漏掉「订单已存在但今天才收到里程碑事件」（`LATE_EVENT` 会把 `event_time` 回拨 3 天） | 增量集合取「出现在该区间任一事件表分区里的 `order_id`」并集，再拿 `scope_ids` 回全表重聚合（不能按 `dt` 过滤事件，`order_time` 可能落在更早分区） | 拿 3 天做增量，与全量重建在该 3 天的订单上逐列一致。**已实现**：三张表按天增量连跑两遍逐日一致 |
| 5 | `lake_dwd_build.py` | 写完只打印行数，不校验；但与 lake DWD 同级的仓侧 `*_streamload.py` 有逐行自检 | 加 `self_check()`：回读「行数 + 金额合计 + 去重主键数」与 Spark 期望值比对，不等 `exit 2` | 故意让某天少写一行，`self_check()` 必须报错退出 |

---

## 附录 B：本轮 30 条断言的实测记录（`PASS=30 FAIL=0`）

测试脚本：`test_inc_dws_ads.sh`（一次性工具，未入库）。全部代码改动落在
`scripts/run_dws_ads_from_lake.sh`（`1 file changed, 214 insertions(+), 101 deletions(-)`），
`bash -n` 通过。

1. **auto 增量连跑两遍幂等**：两次均 `exit 0`，自检通过
   （`覆盖 gate(auto)`：仓侧 dws `MAX(dt)` == `D2 = 2026-12-18`；
   覆盖 gate：`MIN(dt) = 2026-09-20 <= D1`），整表数字一字不差。解析区间 `2026-12-17 ~ 2026-12-18`。
2. **破坏 → `--range` 回刷自愈**：故意打乱 `2026-12-10` / `2026-12-11`，
   回刷后整表数字完全恢复、两日逐日恢复、自检通过。
3. **迟到事件 + lookback 语义**：往湖侧 INSERT 一条迟到订单
   （`order_id=2026121699999 / user_id=99999999 / dt=2026-12-16 / amount=123.45`）→
   立刻可检出漂移（对称差 1/2）→ `--lookback 2` 重算 `2026-12-17~18`，
   **没**重算 12-16，漂移仍在（证明回捞窗口是必需的）→ `--lookback 3` 重算到 12-16，
   漂移清成 0，与湖侧现算完全一致。
   清理：Iceberg 经 StarRocks 不支持 `DELETE`（报 1064），改用
   `run_dwd.sh build dwd_order_detail 2026-12-16 2026-12-16` 单日 rebuild
   （`before=854845 written=9517 after=854844`）+ `REFRESH EXTERNAL TABLE`，
   湖侧回到 854,844、仓侧回到基线、`user_id=99999999` 残留 0。
4. **防呆**：手工全区间回刷可执行（`exit=0`）、全区间回刷后整表硬基线通过、
   区间倒挂被拒（`exit=1`）且原因已打印。
5. **收尾复核**：dws 逐行对称差 = 0、ads 逐行对称差 = 0。
   最终整表：`dws:18000|90|854844|119749476.90|14069110.40`、`ads:90|90|854844|105680366.50`；
   `2026-12-16:9517:1337110.30, 12-17:9485:1324362.00, 12-18:9472:1348545.20`。

### 本轮踩到的坑（会再犯，记下来）

- **Iceberg 经 StarRocks 不支持 `DELETE`**（报 `1064`）。清理脏数据的正确姿势 =
  走湖侧单日 `run_dwd.sh build <table> <d1> <d2>` 重建，**再** `REFRESH EXTERNAL TABLE`。
- **StarRocks 外部表元数据是缓存的**：写完不刷新读到旧快照（§1.1 有原始数字）。
- **`git apply` 报 `unable to unlink … Permission denied`**：WSL 侧文件属主/权限不对，
  不是补丁坏了。先 `ls -l` 看属主，必要时 `sudo chown $(id -u):$(id -g)` 后再 apply。
- 容器里跑 Spark 写湖的 uid 与沙盒目录属主不一致时，Iceberg 写 manifest 会
  `Permission denied`；沙盒目录 `chmod 0777` 即可（生产环境目录属主本来就对，不受影响）。

---

## 附录 C：终局验收清单

- [x] 湖侧脚本自包含（不依赖 `/mnt/d/...`），任意目录可跑
- [x] 湖侧 DWD 有**按天增量**，重复执行幂等（连跑两遍行数不变）
- [x] 湖侧每次写入后**自检**（行数 + 金额 + 去重主键）并失败即退出
- [x] 仓侧 `dws`/`ads` 由**湖侧物化**（来源 = `lake.dwd.*`），逐日与湖侧相等
- [x] 仓侧物化**默认按天自动增量**（`--truncate` 才全量）
- [x] DolphinScheduler 里全链路**串行**接入 —— `offline_dataware_lake` **v10，11 节点**，
      流程与定时都已上线；实测实例 347 全绿 **4 分 16 秒**（v5/8 节点时实例 346 为 3 分 34 秒）；**优化后实例 401 = 117 秒**；**v10（11 节点）实例 403 = 114 秒**
- [x] 对账作业可重复执行（不是一次性人工核对）：`scripts/recon_lake_wh.sh`，执行前对湖侧表
      `REFRESH EXTERNAL TABLE` —— 自测 `scripts/test_recon.sh` **9/9 通过**，全区间实测 3.1 秒
- [x] **失败告警已配**（2026-10-16）：该定时 `warning_type=2` / `warning_group_id=2`
      （与四个旧流程同一个告警组）—— 判据：
      `SELECT warning_type, warning_group_id FROM t_ds_schedules WHERE id = 7`
- [x] 湖表维护**已进调度** ✅ 2026-10-16 —— DS 第 9 个节点 `lake_maintain`
      （`bash /opt/offline-dw/scripts/lake/run_maintain.sh --auto`，超时 15 min，
      前置 = `dqc_order_chain` / `dqc_dim_product` / `recon_lake_wh`；实例 347 里 **43 秒**、退出码 0）。
      脚本自验通过（8 张表全覆盖、快照 ≤ 5、行数不变、对账 26 项全绿）。
      `remove_orphan_files` **已定论：不用做**（2026-10-18）。8 张表逐个
      `CALL lake.system.remove_orphan_files(..., dry_run => true)` **全部返回 0 行** ——
      Iceberg 判「无主」看的是**有没有被任何 metadata JSON 引用**，历史 metadata 都还在
      （per-table 14~138 个），所以盘上比当前快照多出来的 1,787 文件 / ≈12 MB 是**时间旅行副本**、
      不是无主文件。真要回收得删历史 metadata（`write.metadata.previous-versions-max`），
      会缩短回看窗口，不划算。复查命令见 README「湖表快照清理」小节。
- [x] **巡检台账已进调度** ✅ 2026-10-18 —— `offline_dataware_lake` **v10** 第 11 个节点
      `lake_ops_ledger`（`bash /opt/offline-dw/scripts/ops/ops_ledger.sh ${system.biz.date}`，超时 5 min，
      前置 = `lake_maintain`）；实测 3 秒、回放实例 349 得一行、`notes` 为空。
      台账 = `ds/logs/ops/ledger.csv`；**首次真实写入 = 实例 398**（2026-10-19 09:59，`dt=2026-10-18`、`flow_state=7`、260 秒、`recon OK` / 26 项、`notes` 空）。
      「连续 7 天零差异」按 `dt` 数：从 `dt=2026-10-18` 这行起算，凑满 7 行最早要 10-25。
      ⚠️ 台账读的时候流程自己还在 `RUNNING`，所以 `flow_state` 是用其余 9 个节点推断的、`duration_s` 是「启动至今」（比最终值少约 5 秒）、`nodes_*` 不含台账自己。
- [x] **StarRocks 元数据缓存有解药** ✅ 2026-10-19 —— `scripts/lake/sr_catalog_reset.sh`
      （`DROP CATALOG` + `CREATE CATALOG`，实测 1 秒、`dim_product_scd2` 4500 行 / `dim_product` 50 行），
      已内置进 `run_dim.sh`（建表+灌数后自动跑）；原因是 dim 表每天 `DROP`+`CREATE` 会让 FE 缓存命中
      已 purge 的旧 snapshot，`REFRESH EXTERNAL TABLE` 对此无效（实例 379 失败即此）
- [x] 口径已拍板（2026-10-18）：**保留现状** —— 明细层过滤（854,844 = 900,000 − 45,156），
      快照层 / ODS 保留全量（NULL 45,187 / 45,156）；
      判据：`count(*) - count(user_id)` = **0 / 45,187 / 45,156**
- [ ] 湖↔仓对账**连续 7 天零差异**（日历时间，只能每天定时跑一次）
      —— 已记：2026-10-16 实例 347（手动）✅ 9/9 绿、4 分 16 秒；recon 26 项 OK
      —— 已记：2026-10-18 实例 349（手动，与 348 并发）✅ 9/9 绿；recon 26 项 OK
      —— 已记：2026-10-19 实例 400（W1~W3 后）✅ 10/10 绿、217 秒；实例 401（W4+W7 后）✅ 10/10 绿、117 秒；实例 403（v10，11 节点）✅ 11/11 绿、114 秒、湖侧 ODS 闸门 6/6 通过；recon 26 项 OK
      —— 已记：2026-10-20 实例 405（UI 补跑 129 秒）/ 406（OpenAPI 补跑 110 秒）✅ 11/11 绿；recon 26 项 OK（台账 dt=2026-10-20）
      —— 缺跑：2026-10-19 / 10-20 / 10-21 三天 02:00 均因 Docker Desktop 未启动而未触发（根因见 README 已知限制 23）；10-21 09:00 手工补齐
      —— 待记（2026-10-22 起）：2026-10-21 ~ 10-24 定时 02:00 每天一行；凑满 dt=10-18 ~ 10-24 共 7 天再勾掉本条

> 关于「停更仓侧 ods/dim/dwd」这一条：**2026-10-16 起仓侧 `ods`/`dim`/`dwd` 已不再被调度更新**
> （新链路只物化仓侧 `dws`/`ads`）；**2026-10-19 两个 Routine Load 也已 `PAUSE`**，
> 仓侧这一层自此完全冻结（最后构建 2026-10-14 08:51），只作对照与回滚。
> ⚠️ 代价：**失去实时摄入** —— 湖侧只有离线重放（`scripts/lake/lake_ods_replay.py` 直读 Kafka，
> 从 `earliest` 全量重读、按 dt 覆盖写），延时以小时计；Kafka 只保 30 天（`log.retention.hours=720`），
> 超出窗口的日期得重新灌。恢复办法：`RESUME ROUTINE LOAD FOR ods.ods_order_load;`
> （PAUSE 不丢偏移量），或按 `sql/ods/ods_routine_load.sql` 重建作业。

---

## 附录 D：怎么把结果交回来

每一步你改完、跑完，把**命令 + 输出尾巴**贴回来即可（§1.1 我已实测，可直接引用），我负责：

- 判断判据是否真的满足（尤其是「行数对了但口径错了」这类）
- 补分支场景（迟到事件、跨天订单、`user_id IS NULL` 的 45,156 单这类边界）
- 更新 README / PITFALLS 的对应章节

> **注（2026-10-16 起）**：早期「只给代码、不执行写操作」的约定已取消 —— 现在 agent 直接执行
> 并回报实测证据；但凡涉及**数据写入 / 重建 / 清理**的操作（`--truncate`、`build` 重刷、
> `--expire`），先说明影响面与回滚方式，再执行。
