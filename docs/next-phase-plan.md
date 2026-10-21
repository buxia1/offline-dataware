# 下一阶段计划（2026-10-20 起）

> **依据**：上个项目的结束状态（截图 m00001，commit `60ed6a9`）+ 2026-10-20 11:35~11:50（北京时间）对该项目的实测。
> 本文件是**计划**，不是改动记录；每条勾选都以验收判据实测通过为准。
> **项目真实位置**：WSL `Ubuntu-22.04:/home/l/offline-dw`（Windows 侧 `\\wsl.localhost\Ubuntu-22.04\home\l\offline-dw`）。容器挂载的就是这个目录，不是 Windows 下载区。

---

## 0. 现状快照（实测，2026-10-20 11:45 北京时间）

| 维度 | 状态 | 证据 |
|---|---|---|
| 仓库 | `60ed6a9` = `origin/main`（已 push）；工作树仅 `starrocks/fe/conf/fe.conf` 多一行重复 `priority_networks`（坑 30，无害） | `git status --short` |
| 生产流程 | `offline_dataware_lake` **v10 / 11 节点 / release_state=1** | `t_ds_process_definition id=44` |
| 定时 | `0 0 2 * * ? *` Asia/Shanghai，release=1，告警 warning_type=2 + warning_group_id=2；旧 4 条定时均 release=0 | `t_ds_schedules id=7` |
| 最近实例 | 404（10-20 09:50，v10，130s，11/11 绿）；403（10-19 17:19，v10，108s）；402（v9，10-19 17:15 失败） | `t_ds_process_instance` |
| **首次真实定时** | **仍未触发**：10-20 凌晨没有 02:00 实例，容器 09:49 才起来（FE 时钟是 UTC，RL PauseTime 01:49:34 UTC = 09:49 北京） | 实例查询 + `SELECT now()` |
| 台账 | `ds/logs/ops/ledger.csv` **1 表头 + 6 行数据 / 26 列**；dt 只有 3 个（10-17、10-18×4、10-19）；其中实例 400 那行 `notes=maintain-log-unparsed`（旧解析 bug，已修） | `cat` 实测 |
| 旧链路 | 两个 Routine Load 均 `PAUSED`（10-19 起）；仓侧 ods/dwd/dim 自 10-16 冻结 | `SHOW ROUTINE LOAD` |
| 湖 | `lake`（Iceberg）catalog：ods 3 / dim 2 / dwd 3 / dws 1 / ads 1；warehouse 151MB | `SHOW CATALOGS` + `du` |
| 仓 | dws 仅 1 张（18,000 行）、ads 仅 1 张（90 行） | `count(*)` |
| 数据量 | 90 万单 / 事件湖侧 3,111,841、仓侧 3,120,421（去重口径两侧均 2,911,488）/ dwd 85.5 万行；每天一跑 ~120~130s | 实例 404 + 表行数 |
| 资源 | WSL 11Gi（可用 6.4Gi）；容器 starrocks 2.28/3GiB（76%）、DS 1.40/2GiB（70%）、kafka 1.10GiB、spark 183MiB；WSL 盘 951G 空闲；Windows 宿主 Free 1.4GB/15.9GB | `free -h`、`docker stats` |
| 备份 | **完全没有**（DS 元库、StarRocks 数据卷、湖 warehouse 都无备份机制） | `docs/` 全文 grep 无备份方案 |
| 文字口径 | 多处陈旧：坑 2（FE 8G vs 3G，实际已改 `-Xmx1024m`）、README:16「v8 十节点」、README:91「version 7」、README:949「9 节点」、README:1398 台账计数命令（按行数、未去重）、README:1401「首个定时写入是 10-19 02:00」（未发生）、README:1713「v8 第 10 个节点」、runbook:339「v8，10 节点」 | 逐条核对 |

---

## 1. 本周必做收尾（10-20 ~ 10-25，不依赖任何新决策）

### W1 让首次真实定时跑起来（今晚 10-21 02:00）

- **前置检查（10-20 晚）**：`docker ps` 6 容器 Up；`t_ds_schedules id=7` release_state=1；WSL 不 shutdown、Windows 不休眠（已查 AC 睡眠=从不；10-19/10-20 凌晨机器是**关机**状态，根源是"人关机"，不是电源策略）。
- **验收（10-21 上午）**：出现 02:00 实例 state=7、耗时 ≈120s、11/11 绿；台账新增 `dt=2026-10-20, run_date=2026-10-21` 行，且 `flow_state=7 / nodes_ok=10 / nodes_fail=0 / dq_ods_state=7 / recon_result=OK / recon_checks=26 / notes 空`。
- **若缺跑**：当故障处理（记根因 + 手工补跑一次并在台账留痕），不能靠"下次会跑"。

### W2 旧链路正式退场（10-21）

- ⚠️ **实测纠正**：StarRocks 3.5 **没有** `DROP/DELETE ROUTINE LOAD`（`DROP ROUTINE LOAD FOR ...` → `ERROR 1064` syntax error；`information_schema.routine_loads` 也不存在 → `ERROR 5502`）。所以"STOP + DELETE"里的 DELETE 不可执行；**"定义留档"就是 `sql/ods/ods_routine_load.sql` 留在仓库**，STOP 后作业会以 `State=STOPPED` 永久留在 FE 里（不可恢复）。
- ✅ **已执行（2026-10-21 10:57）**：`STOP ROUTINE LOAD FOR ods.ods_order_load;` 与 `ods.ods_order_event_load;` 均成功 → `SHOW ALL ROUTINE LOAD FROM ods` 显示两条 `State=STOPPED`、`EndTime=2026-10-21 02:57:39`（UTC）。
- **踩坑**：STOP 后**普通 `SHOW ROUTINE LOAD FROM ods` 返回 `Empty set`**（已停止作业被过滤），必须 `SHOW ALL ROUTINE LOAD`；错误提示原文 `Please use another one: show all routine load job for job_name`。三条核对命令里的第 1 条需加 `ALL`。
- `sql/ods/ods_routine_load.sql` 末尾已追加"A 方案已执行"注记。
- **三条核对命令**：
  1. `docker exec starrocks mysql -h 127.0.0.1 -P9030 -uroot -e "SHOW ALL ROUTINE LOAD FROM ods\G" | grep -E 'Name:|State:|EndTime'` → 两个 `STOPPED`、`EndTime` 非空（普通 `SHOW` 看不到已 STOP 作业）。
  2. Kafka 位点不再前进：`kafka-get-offsets.sh` 记录此时位点，5 分钟后再跑一次，与作业 `Progress` 比对（STOPPED 后不会再提交）。
  3. 行数冻结：`SELECT count(*) FROM ods.ods_order;` = 900,000、`ods.ods_order_event` = 3,120,421，隔天不变；DS 里 `routine_load_health` 保持 release_state=0。
- 做完这一步，README「实时摄入 = 无」的结论才算真正落地（同步到 W4 文档）。

### W3 7 天零差异验收（10-25 那跑之后）

- **判据必须按 dt 去重**（今天实测：6 行数据但只有 3 个 dt；截图里的 `awk -F, 'NR>1 && $11=="OK"' | wc -l` 今天=6，10-21 多写一行就变 7 → 会**假通过**；README.md:1398 里写的正是这个按行数的命令）。
- **正确判据**（去重 + 连续 + 排除带 notes 的行）：
  ```bash
  docker exec dolphinscheduler awk -F, 'NR>1 && $11=="OK" && $26=="" && !seen[$1]++ {n++} END{print n" 天 recon=OK（去重）"}' \
    /opt/dolphinscheduler/logs/ops/ledger.csv
  ```
  结果需 **= 7**（dt 10-18..10-24 全覆盖），且这 7 个 dt 的每一行都满足 `recon_result=OK && recon_checks=26 && notes 为空 && flow_state=7 && nodes_fail=0`；再用 `cut -d, -f1 | sort -u` 核对日期连续性（不能缺中间某天）。
- 通过后 runbook 附录 C 最后一项 `[ ] 湖↔仓对账连续 7 天零差异` 才能勾掉 → **终局验收清单 100%**。

### W4 文档一致性修正（半小时，单独 1 个 commit）

> ✅ **2026-10-20 12:45 已执行** —— commit `ce686e6`（README.md 14 处 + runbook 1 处，工作树已干净；`fe.conf` 的重复 `priority_networks` 已 `git checkout --` 还原）。修的内容：节点数/版本号统一到 v10 十一节点、台账「连续 N 天」命令改为按 dt 去重（+ 排除 notes 非空行）、FE 堆 8G 遗留改为已修（坑 2 + 已知限制 #18 + 放量清单）、「首个定时写入 10-19 02:00」改为「10-19/10-20 均未触发，首跑待 10-21」、Kafka 未挂载改为已挂载、湖侧事件行数补 10-20 复核值。

- 坑 2（README:1208）与 README:1779 的"遗留"说明 → 改为"堆已修（10-10 commit `4e60bb3`，实测 `-Xmx1024m`）"
- README:91「version 7」→ `version 10`；README:16 ASCII 图 `v8 十节点` → `v10 十一节点`；README:949 `9 节点` → `11 节点`；README:1713「v8 第 10 个节点」→「v10 第 11 个节点」；runbook:339 `v8，10 节点` → `v10，11 节点`
- README:1398 的台账计数命令 → 换成 W3 的去重版（这是那条唯一的"假通过"入口）
- README:1401「首个定时写入是 2026-10-19 02:00」→ 改为"截至 10-20 仍未触发，首跑见 10-21 02:00"
- 台账 7 天判据（README / runbook 两处）换成 W3 的去重命令
- `git checkout -- starrocks/fe/conf/fe.conf` 清掉脏改动

---

## 2. 阶段 2：候选模块（原「方案表」未找到，以下按缺口优先级自拟）

> 我在仓库、工作区、git 历史（含已删除文件）、GitHub issue/wiki/projects、以及 Windows 侧上个项目的全部中间产物里都**没找到「阶段 2 方案表」**。下面是我按"现在最缺什么"排的包，编号和粒度你可以直接改；如果你手上有原表，把模块名发我，我按表对齐。

### 2.0 与 2026-10-19「五方向」清单的对应（2026-10-20 补）

> 你 10-19 上午定过 5 条后续方向（记录在会话里、当时没入库，2026-10-20 从会话记录找回）。它们与下面 A/B/C/D 包的对应关系：

| 10-19 方向 | 状态（2026-10-20） | 落在哪个包 |
|---|---|---|
| ① 性能与资源 | ✅ 已完成：260s→217s→117s，v10 十一节点 114s（W1~W4+W7，`cb120ec`/`36bfa34`）；余量问题归 A1 | 不再单列 |
| ② 质量闸门升级 | ✅ 主体完成：湖侧 ODS 闸门 6 项（v10 第 3 节点）、湖↔仓对账 26 项、台账 26 列、DQC 自检+告警；**尾巴**：ad-hoc 隔离、DQC 失败是否阻塞下游 未验收 | A3 少量 + 遗留项 |
| ③ **冷备与补数** | ✅ **规范/脚本/DS 编排/恢复演练就绪（2026-10-21）**：`docs/backup-restore-and-backfill.md` + `scripts/backup/*` + 两个 DS 工作流 JSON（每天 03:00 / 周日 03:30）；**待做** = S7 整机重装端到端（S1/S3-a/S4/S5/S6 已于 2026-10-21 实机演练通过，见手册 §10） | **A2 + A6** |
| ④ 对象存储 MinIO/S3 | ⬜ 未开工：仍是本地文件系统（MinIO-ready）；MinIO `GetObject AccessDenied` 未解，备选 SeaweedFS | 新增 **A8** |
| ⑤ 实时化 | ✅ **A0 已上线并验收（2026-10-21 13:25 起）**：DS 工作流 `rt_refresh_5m`（定义 id 57，cron `0 0/5 * * * ? *`），单次 25~30s、幂等、与 02:00 共用串行锁，只让湖 ODS 当日分区 5 分钟新鲜；落地记录见 `docs/realtime-stage-design.md` §5.9。**A1（独立 spark-rt 容器 +1.2g）留二期** | **C1** |

### 2.1 D1 真实数据接入（深化主线，2026-10-21 立项，进行中）

> 来源：10-21 你定的深化方向（更多技术栈 / 更复杂表关系 / 更多字段 / 真实数据来源），并明确"先做 D1、要更大的数据集、不要 Olist 那种同质化的"。

- **选型**：主 = dunnhumby《The Complete Journey》（8 表 / 约 4,000 万行 / 约 2.5GB / 营销触达 + 优惠券 + 曝光因果，同质化低）；
  规模校验 = NYC TLC 2022–2024（约 1.2~1.3 亿行 parquet）；备选 = Yelp 8.6GB（JSON/文本）、Stack Exchange（XML/强关系）。
- **边界**：`lake.retail.*` 与现有 mock 的 `lake.ods/dim/dwd/dws/ads` 隔离；retail 是**回填型**工作流（手动/数据更新时触发），
  不接日更调度、不写主台账（单独 `retail_ledger.csv`），保住 7 天对账判据不被污染。
- **交付**：`docs/d1-real-data-ingestion-plan.md`（选型对比 + 8 表 ODS/DIM/DWD/DWS/ADS 设计 + 7 节点 DAG +
  DQC 10 条 + 验收 7 条 + 内存耗时预算 + 风险 8 条 + 命令清单）。
- **状态**：文档已起草并入库；待你回家确认数据集与下载渠道后，我补精确 schema 与 D2 建模文档，你落地 `scripts/retail/*` 与 DS 工作流。
- **工作量**：3~5 天（每天 1~2 小时），Step 2（ODS schema）与 Step 4（事实粒度）最费时。

### 包 A｜生产化与容量（建议先做，1~2 周）

- **A1 容量与资源基线（1 天）**：现在可行但余量薄 —— WSL 可用 6.4Gi，starrocks 76%、DS 70%，容器 limit 之和 ≈19GB（严重超卖，靠空闲才没炸）。产出 `docs/capacity.md`：每天 1 万单的实测画像、10x（10 万单/天）需要的内存/磁盘/Kafka 分区、16GB 宿主下的上限结论。
- **A2 备份与恢复（规范/脚本/编排/演练全部完成 2026-10-21）**：DS 元库 + FE image + warehouse + data + Kafka 元数据，日报 7 天 / 周报 28 天，DS 两个工作流每天 03:00 / 周日 03:30 编排，校验 `verify_backup.sh --deep`（含 DS dump 导入 scratch 库）。**演练结论：S1 5 s、S3-a 30 s、S4 60 s、S5 2 min、S6 解包校验 2 s 全部实测通过**（手册 §10）；仅剩 S7 整机端到端桌面推演。
- **A3 监控告警扩展（2 天）**：现在只有"流程实例失败"告警。补三类：容器存活（6 个 up）、磁盘水位（>80%）、**今日台账缺行/失败**（防"凌晨关机缺跑"的唯一可靠手段）——`scripts/ops/ops_health.sh` 返回非 0 即告警；台账顺带加 3 列（host_free_gb / containers_ok / fe_heap_mb）。**验收 = 人为制造 3 种故障各触发一次告警**。
- **A4 10x 放量压测（3 天，旁路）**：100k 单/天 × 90 天 = 900 万单 / ~3100 万事件。风险点：Kafka 3 分区要加、Kafka 盘 710MB×10、Spark `local[4]` 2g 上限、allin1 3g。**验收 = 旁路 catalog 上跑通 10 天 100k/天**，记录各节点耗时与内存峰值，给 **go / no-go**（no-go 就扩宿主内存或转生产部署，见 C3）。
- **A5 表达式分区改造（1 天）**：README 唯一未做的"可选架构改进"——`dwd_order_sku_detail` 动态分区改表达式分区，消除"关开关→逐条 ADD→开开关"以及漏跑 `Insert has filtered data` 硬失败。**验收 = 补历史分区无需手工 ALTER，90 天行数/对账不变**。
- **A6 补数 / 回填框架（2 天）**：现在缺跑只能手工重放，且**手工重放会往台账多写一行**（10-18 的 4 行就是这么来的，直接破坏 7 天判据）。目标：DS 按 `dt` 区间参数化触发、湖侧分区幂等覆盖写、补跑不污染台账（或单独标记 `run_mode=backfill`）。**验收 = 故意漏跑一天，用一条命令补回且对账 26 项仍 OK、台账不多行**。
- **A7 上游生成器入调度（1 天）**：三个生成器（商品快照 / 订单快照 / 事件）现在手工跑、事件生成器有状态（跳天不补发），是"端到端无人值守"的硬缺口。目标：改成按 `dt` 参数化、可重复执行，接进 DS 前置节点或独立工作流。**验收 = 连续 2 天不碰键盘全链路自动跑通**。
- **A8 对象存储（MinIO/S3）选型与改造（2 天，方向 ④；可推迟）**：湖层现在落本地盘，换对象存储 = `warehouse` 改 S3 路径 + Spark 挂 `hadoop-aws`（坑表实测：`hadoop-aws-3.3.4.jar`）。已知卡点：Chainguard MinIO `GetObject` 返回 `AccessDenied`（未解，坑表第 5 条），备选 **SeaweedFS**（S3 兼容、可拉取）。**验收 = 旁路 warehouse 在对象存储上跑通一天重放 + 对账 26 项 OK，再决定是否迁移**。

### 包 B｜数据应用层（用户价值最高，A 之后 2 周）

现状：dws 1 张、ads 1 张，只能回答"每天卖了多少"。下面每张表都能独立验收。

- **B1 DWS 主题扩展（4 张，从 lake.dwd 物化）**
  - `dws_category_day`：品类/商品日销（GMV、单量、AOV、退款、动销 SKU 数）
  - `dws_order_funnel_day`：下单→支付→发货→完成漏斗与卡单率（用 dwd_order_lifecycle 的 current_stage）
  - `dws_user_rfm`：RFM 分层（周/月粒度）
  - `dws_retention_day`：次日 / 7 日留存（按用户首单日）
- **B2 ADS 报表（3~5 张 + 每张 1 条 DQC + recon 扩到 30+ 项）**：`ads_category_sales`、`ads_funnel_daily`、`ads_user_segment`、`ads_retention`；每张配行数 / 金额守恒 / 主键唯一断言，挂 `dqc_ads.sh` 或并入现有闸门。
- **B3 BI / 查询层（2 天）**：StarRocks MySQL 协议 9030 直连 Metabase/Superset；3 个看板（经营日报 / 品类 / 用户）。注意别往 11Gi 里再塞重容器，优先 Windows 侧或轻量版，且先过 A1 容量结论。
- **B4 维度与口径治理（2 天）**：`dim_date`（周/月/季/节假日，替代字符串 dt 分组）；统一"已支付金额/退款/净额"口径；`docs/metrics.md` 指标字典（每个指标写清分子分母与空值规则——套坑 34 的 `IFNULL/NULLIF`）。

### 包 C｜实时与生产形态（取决于目标，最后做）

- **C1 实时摄入 PoC**：Kafka → Structured Streaming/Flink 微批写 Iceberg（1~5 分钟延迟），补上已退役 Routine Load 的实时路径。
- **C2 真实数据源替换 mock**：文件/CDC 接入协议 + 迟到/重复事件处理（当前三个生成器手工跑、事件生成器有状态，见已知限制 #10/#11）。
- **C3 生产部署形态**：DS 独立 master/worker、Spark standalone 多 executor、StarRocks 3 节点——**16GB 单机不适合**，需先定硬件。

### 包 D｜工程化（可与 A/B 并行）

- **D1 CI（GitHub Actions）**：shellcheck + `python -m py_compile` + SQL 语法冒烟 + `test_recon.sh`。
- **D2 回归**：湖层幂等、DQC 自测、物化增量（现有 `scripts/test_recon.sh` 9/9 是基础）。
- **D3 版本/发布**：`docs/CHANGELOG.md`；DS 定义只存 MySQL（导出 JSON 永远 OFFLINE 且不含脚本），发布必须同时归档 `sql/` + `scripts/` + 定义导出。

---

## 3. 排期与里程碑（3 周 + 第 4 周可选）

| 时间 | 内容 | 里程碑 |
|---|---|---|
| 10-20 ~ 10-21 | W1~W4 | **M0** 首次自动跑绿；**M1** 旧链路 STOP 完成 |
| 10-22 ~ 10-25 | A1 容量基线（A2 已于 10-21 完成） | **M2 10-25** 7 天零差异 → 终局验收清单 100% |
| 10-26 ~ 11-01 | A3 监控告警、A5 表达式分区 | **M3 11-01** 生产化硬化：备份演练通过 + 3 类告警实测 + 分区改造零差异 |
| 11-02 ~ 11-07 | A4 10x 压测 | **M4 11-07** go / no-go 结论 |
| 11-08 ~ 11-14 | B1 前两张 + B3 看板雏形 + B4 dim_date | **M5 11-14** 应用层 v1（+2 张 dws、1 个看板） |
| 11-15 ~ 11-21 | B1 其余 + B2 + 指标字典 + D1 CI | **M6 11-21** 应用层完整（4 dws + 4 ads + 3 看板 + 30 项对账） |
| 之后 | C1 / C2 / C3 按决策 | — |

---

## 4. 主要风险（按发生概率 × 影响排序）

1. **凌晨机器关机导致定时不跑**（已连续 2 天）：靠 A3 的"台账缺行告警"兜底；或改常开机/云主机。
2. **7 天验收假通过**（dt 重复 + 数行判据）：W3 改去重判据，文档同步。
3. **内存余量薄**：往 11Gi 里再加容器前必须过 A1；starrocks 已 76%，再涨会 OOM（坑 18/33）。
4. **Kafka 只保 30 天**（720h）：补数超窗需重新生成数据（生成器有状态、跳天不补发）。
5. **allin1 单容器不能上生产**（已知限制 #6）：C3 之前不要对外承诺 SLA / 并发。
6. **湖表 DDL 会毒化 FE 元数据缓存**（坑 31）：新增湖表必须走 `sr_catalog_reset.sh` 路径（已内置于 run_dim.sh）。
7. **RL 退役后无实时摄入**：任何"分钟级"需求都要走 C1，不要临时 RESUME 旧 RL（会重现 10-05 类事故）。

---

## 5. 需要你拍板的三件事（拍完我直接开工）

1. **阶段 2 范围**：按我建议的"包 A → 包 B"走？还是你手上有原方案表（发我模块名 / W 编号，我按表对齐）。
2. **是否把 10x 放量作为下一个硬目标**：决定 A4 是否插队、是否申请更多 Windows 内存（宿主只剩 1.4GB，扩 WSL 需先腾地方）。
3. **是否需要分钟级实时**：需要就立项 C1；不需要就 10-21 删完旧 RL 后在 README 把"无实时摄入"写成结论。

---

## 附：今天就能跑的三条核对命令

```bash
# 1) 定时是否就绪（应 release_state=1 且 crontab=02:00、告警 group=2）
docker exec mysql mysql -uroot -proot123 dolphinscheduler -e \
 "SELECT id,release_state,crontab,warning_type,warning_group_id FROM t_ds_schedules WHERE id=7;"

# 2) 台账（当前 6 行 / 26 列 / 3 个 dt）
docker exec dolphinscheduler sh -lc 'tail -n +2 /opt/dolphinscheduler/logs/ops/ledger.csv | cut -d, -f1 | sort -u'

# 3) 旧 RL 状态（现在 PAUSED，10-21 后应变 STOPPED）
docker exec starrocks mysql -h 127.0.0.1 -P9030 -uroot -e "SHOW ROUTINE LOAD FROM ods\G" | grep -E 'Name:|State:'
```
