# 冷备与补数：场景矩阵、备份 SOP 与恢复演练

> 对应阶段 2 计划里的 **A2（备份 + 恢复演练）** 与 **A6（补数框架）**，也是你 10-19 定的「后续方向」第 3 条。
> 撰写：2026-10-20；适用版本：`offline_dataware_lake` **v10（十一节点）**，HEAD `59ca40a`。
> 代码与命令由你落地（`scripts/backup/`），本文只给规范、判据与操作顺序。

---

## 0. 一句话结论

**这套系统里只有 4 样东西"丢了就补不回来"：DS 元库、StarRocks FE 元数据镜像、Iceberg warehouse（湖数据本体）、以及 Kafka 里 30 天内的原始消息。** 其余（仓侧内部表、dws/ads、Kafka 之外的一切）都能从这 4 样重建。

所以备份清单只有 4 行 + 3 行"顺手的"：见 §2。**每周一次全量 ≈ 400 MB，每日一次增量 ≈ 15 MB，951 GB 空闲盘随便放。**

配套的一句话规矩：**恢复必须演练过**。没演练过的备份等于没有备份 —— §7 给了演练模板和验收判据，第一次演练建议在 10 月内做完。

---

## 1. 口径：RPO / RTO 先定下来

| 术语 | 含义 | 本项目取值 |
|---|---|---|
| **RPO**（最多丢多少数据） | 两次备份之间允许丢的时间窗 | DS 元库 ≤ 24h；FE 镜像 ≤ 24h；湖数据 ≤ 7 天（周备份 + 每次重大变更前手动）；Kafka ≤ 7 天（可选） |
| **RTO**（多久能恢复） | 从发现到恢复验收通过 | 见 §4 每个场景的估计值 |

**为什么湖数据允许 7 天**：湖表按 `dt` 分区、每天覆盖写，且 **`lake.ods.*` 里留着 90 天的全量数据**（它本身就是"30 天窗口外的原始数据存档"）。真正怕的是**整个 warehouse 目录丢失**（误删 / 磁盘坏），不是"某天算错"。算错重算，目录没了才需要冷备。

**当前状态（2026-10-20 实测）**：**零备份**。`backups/` 目录不存在，`docs/` 里也没有恢复手册 —— 本文就是补这个洞。

---

## 2. 资产清单：什么要备、怎么备

| # | 资产 | 宿主路径 | 体积（实测） | 丢了会怎样 | 能否重建 | 备份方式 | 频率 |
|---|---|---|---|---|---|---|---|
| 1 | **DS 元库** | 容器 `mysql` 的 `dolphinscheduler` 库（65 表 / 4.3 MB） | dump ≈ 5 MB，gz 后 ≈ 1 MB | 流程定义、定时、告警、实例历史全没；码头 JSON 导出**不含节点脚本、schedule.releaseState 恒为 OFFLINE**，只靠它恢复不了 | ❌ 不能 | `mysqldump --single-transaction`（逻辑） | 每日 |
| 2 | **StarRocks FE 元数据镜像** | `starrocks/fe/meta/image/` | 3.6 MB | catalog / 库表 / 分区 / 用户权限全没；internal 表数据还在 BE 但"没人认识它们" | ❌ 不能（重建=手工重放全部 DDL） | `ALTER SYSTEM CREATE IMAGE` + 打包 `meta/image` | 每日 |
| 3 | **Iceberg warehouse（湖数据）** | `lake/warehouse/`（10 张表，151 MB，其中 dwd 85 MB / ods 65 MB） | 151 MB（实测 tar.gz **122.5 MiB / 5918 条目 / 3.7 s**） | 90 天原始与加工数据全没 → 整仓只能等下一次上游重放（且 Kafka 过期后不可逆） | ⚠️ 只能从 Kafka 重放≤30 天 | `tar -czf`（目录 + metadata 一起，**必须同一时刻快照**） | 每周 + 变更前 |
| 4 | **Kafka 原始消息（30 天窗口）** | `kafka/logs/`（711 MB） | 711 MB（tar.gz ≈ 650 MB） | 30 天内任何 `lake.ods` 损坏都无法重放 | ❌ 不能 | `tar -czf`（**必须停 Kafka 或物理快照**，见 §6 注意） | 可选 / 每周 |
| 5 | 巡检台账 | `ds/logs/ops/ledger.csv` | 几 KB | 7 天零差异的验收证据没了 | ⚠️ 部分（可从实例日志重算） | 直接 `cp` | 每日 |
| 6 | 生成器输入（商品快照等） | `data/` | 368 KB | 当天商品维度缺输入 | ✅ 可重跑生成器 | `tar` | 每日 |
| 7 | 配置与代码 | 仓库 + `docker-compose.yml` + `starrocks/fe/conf/` + `spark/jars/` | 仓库 ~1.5 MB；jars 52 MB | 代码可 clone，**jars 与 fe.conf 是"可下载但要重下 52 MB 且版本要对"** | ✅ 可重现 | git（已 push）+ jars/conf 打包 | 变更时 |
| 8 | 仓侧内部表（ods/dwd/dim/dws/ads） | `starrocks/be/storage/` 580 MB | 580 MB | dws/ads 等查询直接不可用 | ✅ **从湖物化**（`run_dws_ads_from_lake.sh`，幂等） | **不备份**，靠重建 | — |

### 2.1 为什么 BE `storage/` 不备份（重要决策）

- 仓侧 `dws`/`ads` 是"湖侧数据的物化视图"（runbook 的终局定义），`run_dws_ads_from_lake.sh` 可幂等重建；
- 仓侧 `ods/dwd/dim` 自 2026-10-16 已冻结、不再更新，且新客官链路的数据真源在湖侧；
- 备 BE 的代价是 580 MB + **必须与 FE 镜像严格同一时刻**，否则 meta 与数据对不上（比不备更危险）。
- **决策**：BE 不备，改为"重建 + 验收"：恢复后跑 `run_dws_ads_from_lake.sh` + `recon_lake_wh.sh`（26 项）+ 两个 DQC，全绿即视为恢复成功。这个结论要在第一次演练里实测 RTO。

### 2.2 为什么 FE 只备 `meta/image` 而不备整个 `meta/`（313 MB）

- `meta/` 里 `image/` 是**一致快照**（3.6 MB；实体文件其实是 `meta/image/v2/image.NNNNN`，`image/` 下只有 `ROLE`/`VERSION`/`v2` 三个条目），`bdb/` 是运行中的 RocksDB 工作目录（313 MB，含 WAL/log），**热备 `bdb` 拿不到一致状态**，且恢复时必须停 FE 再放回，容易把一个能用的库搞坏；
- 正确姿势：`ALTER SYSTEM CREATE IMAGE`（强制落一个新 image）→ 打包 `meta/image` → 恢复时停 FE、**先把 `meta/bdb` 改名移走**、替换 `meta/image`、起 FE。
- ⚠️ **不移走 bdb 是最隐蔽的错**：bdb 是 image 之后的 redo journal（`CheckpointController.deleteOldJournals` 只清 image 之前的），不移走 FE 会把更新的 journal 重放回来，看起来"恢复没生效"。
- 代价：image 里的 `bdb` 需要回放到最新（丢掉"上次 image 之后"的增量，通常只有几分钟的 catalog 变更）—— 对每天 02:00 跑一次批量的系统可接受。
- ⚠️ **2026-10-21 演练实测（关键）**：只放 image、把 `bdb` 整体移走后，FE **拒绝启动**，日志原文：
  `MetaHelper.checkMetaDir(): image exists, but bdb dir is empty, set start_with_incomplete_meta to true if you want to forcefully recover from image data`。
  所以恢复流程必须**临时**在 `starrocks/fe/conf/fe.conf` 追加 `start_with_incomplete_meta = true`；恢复成功后再删掉该行重启一次（实测 12 s 正常起），否则以后每次启动都会跳过元数据一致性检查。`scripts/backup/restore_fe.sh` 已内置这两步。

### 2.3 被 `.gitignore` 排除、必须由备份兜住的对象（2026-10-20 实测核对）

| 对象 | 体积 | 为什么 git 兜不住 |
|---|---|---|
| `ds/libs/mysql-connector-java-8.0.30.jar` | 2.5 MB | gitignored；DS 连 StarRocks 必需 |
| `ds/bin/docker` | 40 MB | gitignored；DS 节点里 `docker exec` 用的静态 docker 客户端 |
| `spark/jars/*.jar` | 52 MB（iceberg-runtime 45 MB） | gitignored；离线环境重建要重新凑版本 |
| `starrocks/fe/conf/` | 24 KB / 5 文件 | 含 `fe.conf`（堆、priority_networks） |
| `data/` | 368 KB / 90 个 CSV | 商品快照输入，丢了当天维度跑不出 |
| `ds/alerts/notify.sh` | KB 级 | 告警通道 |
| `starrocks/be/storage/cluster_id` | 15 B | 金丝雀判据（`938046516-3.5.0`），root 权限，`docker exec cat` 取 |
| git 本体 | — | 由 `git bundle --all`（实测 520 KB）兜；`docker-compose.yml`/`.gitignore`/`README.md` 在 git 里 |

**还有两件不是文件、但备份也救不回来的状态**：
1. **DS 定时上线状态**：码头 JSON 导出后 `schedule.releaseState` 永远是 OFFLINE，恢复后必须在 UI 手工点上线一次（README 已记）；
2. **Kafka 消费者组位点**：在 `__consumer_offsets` topic 里，只有随 `kafka/logs` 一起备份才在。

---

## 3. 备份规范

### 3.1 目录布局

```
backups/
├── daily/2026-10-20/                    # 实测 332 KB / 78 s
│   ├── ds_dolphinscheduler.sql.gz       # DS 元库（65 表）
│   ├── fe_image.tar.gz                  # starrocks/fe/meta/image（198 KB）
│   ├── fe_image.txt                     # image id / sha256 / checksum / clusterId / journalId
│   ├── state.tar.gz                     # 巡检台账 + ds/alerts
│   ├── be_cluster_id.txt                # 金丝雀判据
│   ├── data.tar.gz                      # 生成器输入（90 个商品快照）
│   ├── git.txt                          # commit + status
│   └── manifest.sha256                  # 以上所有文件的 sha256
├── weekly/2026-10-25/                   # 实测 189 MB / 16 s
│   ├── lake_warehouse.tar.gz            # 湖数据本体（122.5 MiB / 5918 条目）
│   ├── conf_jars.tar.gz                 # fe/conf + spark/jars + compose + ds/libs + ds/bin/docker
│   ├── repo.bundle                      # git bundle --all（520 KB）
│   ├── kafka_topics.txt                 # topic/分区/offset 基线（get-offsets）
│   ├── kafka_logs.tar.gz                # 可选（--with-kafka，需停 Kafka）
│   └── manifest.sha256
└── drill/2026-10-26/                    # 恢复演练的恢复点（时间戳子目录）
```

### 3.2 命名与保留

- 目录名 `YYYY-MM-DD`（本地时区 Asia/Shanghai），文件名带类型前缀；
- 保留：**daily 保留 7 天、weekly 保留 4 周、drill 保留最近 1 次**；更早的用 `find backups -name '2026-*' -mtime +30 -delete` 之类清理（写进脚本，别手工）；
- **落盘位置**：主份 `backups/`（WSL 根盘，951 GB 空闲）；**副本**建议另存 `/mnt/d/offline-dw-backups/`（D: 281 GB 空闲）—— 跨盘副本才能挡"WSL 磁盘/虚拟机损坏"这一类事故。跨盘 copy 实测性能见任务回报（备份体积小，不是瓶颈）。

### 3.3 每次备份必须带 manifest

```bash
# 生成（在备份目录内）
sha256sum * > manifest.sha256

# 校验（任意时刻，先跑这个再谈恢复）
sha256sum -c manifest.sha256
```

**没有 manifest 的备份不要拿去恢复** —— 半截 tar / 被截断的 dump 是最常见的"备份事故"。

### 3.4 备份时间窗

- 每日 **03:00**（02:00 工作流通常 02:03 结束，避开 DDL/加载期）；
- 每周 **周日 03:30**（`lake_maintain` 周日才 expire 快照，等它跑完再打 warehouse 包）；
- **变更前手动**：改 DDL、改 compose、升级镜像、放量、`--truncate`、`--expire` 之前，先手动跑一次 daily + weekly。

---

## 4. 恢复场景矩阵（S1 ~ S7）

> 判据统一口径：`flow_state=7`、`recon 26 项 OK`、两个 DQC 通过、10 张湖表行数与基线一致。

| # | 场景 | 典型触发 | 影响面 | 恢复动作 | 预计 RTO | 数据损失 |
|---|---|---|---|---|---|---|
| **S1** | 仓侧 dws/ads 数据错/缺失 | 物化失败、口径改错 | 报表数字错 | 从湖侧重物化 + 对账（`bash scripts/run_dws_ads_from_lake.sh --date <dt>` 幂等，可回填） | **实测 5 s**（--truncate 全量重灌 + 自检） | 无（湖在） |
| **S2** | 湖侧 `dwd/ads/dws` 某天算错 | 逻辑 bug、脏数据已进湖 | 加工层错 | 修逻辑 → 单天重算（`run_dwd.sh incremental all --lookback N` 或按天重跑）→ 物化 → 对账 | 20~40 min | 无（lake.ods 在） |
| **S3** | 湖侧 `lake.ods` 某 dt 缺/坏 | replay 静默 0 条、重放写坏 | **真源缺失** | ① ≤30 天：Kafka 重放该天（`lake_ods_replay.sh --date`）→ 下游重算（**实测：重放 30 s、行数逐字不变、对账 23 项 OK**）；② >30 天：找 warehouse 备份恢复该表目录；③ 两者都无：数据永久丢失（这就是要备份的理由） | 30~60 min | ≤1 个 dt |
| **S4** | DS 元库损坏/误删 | 误 `DROP DATABASE`、磁盘坏 | 调度瘫痪 | 停 DS → `DROP/CREATE DATABASE` → 导入最近 dump → 起 DS → 核对流程/定时/告警 → **用带 `scheduleTime` 的 API 触发一次工作流**（漏这个参数报 50014） | **实测 60 s**（停→导 6 s→起 44 s） | 上次 dump 后新增的实例历史 |
| **S5** | FE 元数据损坏 | 升级失败、误删 `meta/` | 仓侧不可查 | 停 starrocks → `mv meta/bdb` 移走 → 还原 `meta/image` → **`fe.conf` 临时加 `start_with_incomplete_meta = true`**（不加 FE 拒启）→ 起（实测 16 s）→ 成功后删该行再重启 → 跑 `sr_catalog_reset.sh` | **实测 2 min 内** | 上次 image 后的 catalog 变更 |
| **S6** | Iceberg warehouse 损坏/误删 | `rm -rf` 误伤、磁盘坏 | **湖全没了** | 停 spark/starrocks 对湖的写入 → 还原 `lake_warehouse.tar.gz` → 10 表 `count(*)` 比对基线 → catalog reset → `recon_lake_wh.sh` → 当日重跑 | 实测：解包 1~2 s + 独立 catalog 校验 10 表一致；**真实 swap 到原路径未做**（演练用隔离目录，不碰生产） | 上次周备份之后的新增 |

> ⚠️ **S6 必须恢复到原路径**：Iceberg metadata 里记的是绝对路径（`file:///opt/offline-dw/lake/warehouse/...`），解到 `/tmp` 或别的目录再指 catalog 是无效演练（读到的是新目录里的旧 metadata，数据仍指向原文件）。正确做法是 §7.3 的 swap 流程。
| **S7** | 整机重装 / WSL 毁 | 系统事故 | 全停 | 按 §6.2 顺序恢复（compose → 容器 → 4 类数据 → 代码 → 全链路验收） | 2~4 h | 取决于备份 |

**S3 的数据损失是唯一"不可逆"的**：`lake.ods` = 从 Kafka 重放出来的，Kafka 30 天前的消息被 retention 删掉后，那天的原始事件就只剩湖里那一份。**所以 warehouse 周备份是这套系统的最后一道防线。**

---

## 5. 补数：什么能补、什么不能补

### 5.1 补数依赖矩阵

| 要补的东西 | 依赖 | 窗口内（≤30 天） | 窗口外 |
|---|---|---|---|
| `lake.ods.*` 某天 | Kafka 原始消息 | ✅ 重放（`lake_ods_replay.sh --date YYYY-MM-DD`） | ❌ Kafka 无；只能 warehouse 备份恢复 |
| `lake.dwd.*` 某天 | `lake.ods` 该天 | ✅ 重算 | ✅ 重算（lake.ods 数据仍在湖里） |
| `lake.ads/dws` 某天 | 湖表 | ✅ 重算 + 物化 | ✅ 同左 |
| 仓侧 `dws/ads` | 湖表 | ✅ 物化 | ✅ 同左 |
| 仓侧 `ods/dwd/dim` | 已冻结 | ⚠️ 仅兼容视图，不再维护 | — |

**结论：只要 `lake.ods` 在，30 天窗口外的"加工层补数"也能做；真正卡窗口的只有"湖侧 ODS 本身丢了"这一种。**

### 5.2 用 DS 补数 vs 手工补

- **DS 补数**：UI → 工作流实例 → 补数（选日期区间）。两个已实测的坑必须记住：
  1. `${system.biz.date}` = **调度日期 − 1 天**。要补"数据日期 2026-10-15"，补数时选 **10-16**；
  2. 执行方式必须选「**串行执行**」，否则被"串行丢弃"**静默丢掉**（实例不跑、不报错）。
- **手工补**（更可控，推荐）：按节点顺序跑脚本，`--date` 显式传：
  ```bash
  # 仅示意顺序，真实参数以 README:97-111 节点表为准
  bash scripts/lake/lake_ods_replay.sh --date 2026-10-15
  bash scripts/dqc_lake_ods.sh 20261015
  bash scripts/lake/run_dim.sh build
  bash scripts/lake/run_dwd.sh incremental all --lookback 3
  bash scripts/run_dws_ads_from_lake.sh --date 2026-10-15
  bash scripts/recon_lake_wh.sh
  ```

### 5.3 补数之后必须做的事

1. `dqc_lake_ods.sh` 对该天 → 通过（绝对断言：量级不断崖、主键唯一、枚举合法、7 天无缺口）；
2. `recon_lake_wh.sh` → 26 项 OK；
3. 台账 `lake_ops_ledger` 若有该天记录，**手工补一行**（或重跑该节点），并在 `notes` 写明"补数"；
4. 若补的是 `lake.ods`：**当天 dwd 必须重算**（DWD 只单天内去重，跨天重复会在两个分区各留一份 —— 坑 #5）。

---

## 6. 操作 SOP

### 6.1 编排：两个 DS 工作流（2026-10-20 起）

| 工作流 | 定时 | 节点 | 实测耗时 / 产出 |
|---|---|---|---|
| `offline_dataware_backup` | 每天 03:00 | `backup_daily` → `verify_daily --deep` | 80~100 s / 332 KB |
| `offline_dataware_backup_weekly` | 周日 03:30 | `backup_weekly` → `verify_weekly` | ~16 s / 189 MB |

- **运行位置**：DS standalone 容器（root）。节点命令：`export ROOT=/opt/offline-dw; bash /opt/offline-dw/scripts/backup/backup_daily.sh`。
- **前提**：`docker-compose.yml` 里 DS 必须有 `- ./:/opt/offline-dw` 挂载（`./scripts`、`./sql` 是更深的子挂载，仍为只读）。**重建 DS 容器前确认这条还在**，否则节点找不到 `lake/`、`backups/`、`ds/logs/`。
- **产物归属**：脚本末尾 `chown -R 1000:1000 "$OUT"`，宿主上还是 `l` 的文件；`/backups/` 已在 `.gitignore`。
- **为什么不用 cron**：WSL 的 cron 不随开机自启、需要宿主路径与权限、没有告警/历史/重试；DS 里跑顺带白拿告警组 2 和运行历史。cron 只作为"DS 长期不跑"时的兜底（可选）。
- **为什么"备份 DS 自己的元库"不算循环依赖**：DS 停着时元库不会变化，最后一份 dump 仍代表最新状态；需要保护的是"运行中产生的变更"，那时 DS 一定在跑。
- **导入方式**：项目管理 → 工作流定义 → **导入工作流** → 选 `dolphin/offline_dataware_backup.json` / `dolphin/offline_dataware_backup_weekly.json` → 导入后**手工点「上线」**（DS 导出/导入的 `releaseState` 恒为 OFFLINE）。
  ⚠️ 两个实测坑（2026-10-21 已踩）：① 导入后 DS 自动把工作流名改成 `<原名>_import_<时间戳>`，UI 里看着像没导对，改回原名即可；② 导入的**定时**也已建好但 `release_state=0`，要在「工作流定时」里单独上线（定义上线 ≠ 定时上线）。
  ✅ 2026-10-21 实测：定义 code `23438712655360`（定时 id 8，每天 03:00）/ `23438712670080`（定时 id 9，周日 03:30），均上线；试跑实例 408 = `backup_daily` 100 s + `verify_daily` 11 s 全绿，产物 336K / `verify --deep` PASS=8。

手工等价命令（在宿主或 DS 容器里都能跑）：

```bash
# 每日 03:00 —— 实测 332 KB / 78 s（其中 ~76 s 在等 FE checkpoint）
bash scripts/backup/backup_daily.sh
# 手工等价命令见附录 A。唯一容易踩的点：
#   ALTER SYSTEM CREATE IMAGE 是【异步】的，必须轮询 meta/image/v2 里出现新的 image.<journalId> 再打包；
#   按 meta/image/image.* 找会永远找不到（image/ 下只有 ROLE/VERSION/v2）
```

```bash
# 每周日 03:30（等 lake_maintain 的 expire 跑完）—— 实测 189 MB / 16 s
bash scripts/backup/backup_weekly.sh
# 需要 Kafka 原始消息时（会停 Kafka ~30s，保证 segment 一致，见 6.3）：
bash scripts/backup/backup_weekly.sh --with-kafka
```

### 6.2 恢复（整机顺序，S7）

1. 装 Docker + compose 插件，`git clone git@github.com:buxia1/offline-dataware.git`；
2. 补回被 gitignore 的运行时文件：`ds/libs/mysql-connector-java-8.0.30.jar`、`ds/bin/docker`、`spark/jars/*.jar`、`starrocks/fe/conf/`、`ds/alerts/`（从 **weekly** 的 `conf_jars.tar.gz`；`data/` 在 daily）；
3. `docker compose up -d` 起空容器（`mysql` 会建空库）；
4. **恢复 DS 元库**：停 DS → `zcat ds_dolphinscheduler.sql.gz | docker exec -i mysql mysql -uroot -proot123 dolphinscheduler` → 起 DS；
5. **恢复 FE 镜像**：停 starrocks → **`mv meta/bdb meta/bdb.bak.<ts>`**（关键，见 §2.2）→ 用 `fe_image.tar.gz` 覆盖 `meta/image` → 起 → 跑金丝雀（§7.4）；
6. **恢复湖数据**：停 spark **和 starrocks**（两者都读湖）→ `tar -xzf lake_warehouse.tar.gz -C /home/l/offline-dw` → 起；
7. 恢复 `data/`；（可选）恢复 `kafka/logs`；
8. `bash scripts/lake/sr_catalog_reset.sh`；
9. **全链路验收**：10 表行数对照基线 → `recon_lake_wh.sh` 26 项 OK → 两个 DQC 通过 → 手工触发一次 `offline_dataware_lake` 全绿。

### 6.3 三个必须注意的操作细节

1. **`mysqldump` 不要加 `--databases`**：dump 里会带 `USE dolphinscheduler;`，恢复时若误导入生产库会**覆盖线上元库**。恢复时显式指定目标库名。
2. **Kafka 目录不能在运行中直接 tar**：`kafka/logs` 里是活跃的 segment，运行中打包可能拿到"半截 segment + 不一致的 offset 索引"。要么 `docker compose stop kafka` 后打包（30 秒级），要么接受"恢复后需要消费者从头重放"的语义 —— 二选一，别假装它一致。
3. **FE 恢复后必须 reset external catalog**：`dim` 表每天 DROP+CREATE 会毒化 FE 的 Iceberg 元数据缓存（坑 #31），恢复后的 catalog 缓存更是全旧，不 reset 会出现"表在但查不到数据"。

---

## 7. 恢复演练：模板与验收判据

### 7.1 为什么必须演练

备份的失败模式都是"**看起来成功**"：dump 截断、tar 少了 data 目录、image 是旧的、恢复后 catalog 没 reset —— 全都要在**真恢复**时才暴露。演练一次 = 把 RTO 从"估计"变成"实测"。

### 7.2 演练记录模板

| 项 | 填写 |
|---|---|
| 演练日期 / 执行人 | 2026-10-__ / |
| 演练场景 | S4（DS 元库）· S6（warehouse）· S5（FE）—— 首次建议三个都做 |
| 使用的备份 | `backups/...`（写路径 + manifest 校验结果） |
| 开始/结束时间 | |
| 实测 RTO | |
| 恢复验证 | 表数 / 湖表行数 diff / recon 26 项 / DQC / 调度全绿 |
| 踩到的坑 | |
| 对备份脚本的修改 | |

### 7.3 首次演练（2026-10-21 已完成，记录见 §10）

S1、S3-a、S4、S5、S6 已在真实环境实机演练并通过；S7 整机重装为桌面推演（分步命令已分别由 D2/D3/D4/D5 验证）。**结论：备份可用，RTO 全部低于原估计。** 下次建议做 S7 端到端（换机或重建 WSL 时顺带补做）。

### 7.4 验收判据（恢复成功的定义）

| 判据 | 通过标准 |
|---|---|
| 湖表行数 | 10 张表 `count(*)` 与基线一致（基线见 §8） |
| 对账 | `recon_lake_wh.sh` 26/26 OK |
| 质量闸门 | `dqc_lake_ods`（6 项）+ `dqc_order_chain` + `dqc_dim_product` 全通过 |
| 调度 | `offline_dataware_lake` v10 手工触发 11/11 绿、耗时 ≈ 114 s |
| 台账 | 新写一行，`notes` 空、`recon_result=OK` |

### 7.5 校验各层能证明什么、不能证明什么

| 校验 | 能证明 | 不能证明 |
|---|---|---|
| `gzip -t` | 压缩流 CRC 完好、没截断 | 内容语义对不对 |
| `tar -tzf` | 条目存在、树结构对 | 文件内容与源一致 |
| `sha256sum -c manifest` | 备份未被篡改/截断（备份后立刻算的才有效） | 备份与生产"同一时刻一致" |
| DS dump 导入 scratch 库 | 65 表 / 5 流程定义 / 5 条定时（1 条上线）可查 | DS 调度真能跑 |
| FE image `crc32(image.<id>) == checksum.<id>` | 镜像自身完整（checksum 是 CRC32 十进制实测证实） | FE 真能起 |
| lake 解档后 md5 清单比对 | 与现场逐字节一致 | Spark 真能写 |
| 真停 FE → 换 image → 起 → `current_version()` + 10 表计数 | FE 真能用这份 image 起起来（2026-10-21 演练新增，第一次就因为缺 `start_with_incomplete_meta` 失败） | 恢复后新写入是否与 BE 一致 |
| Kafka 解档条目 / offset 基线 | 日志齐全 | broker 能起；热备不保证崩溃一致 |

**能恢复 ≠ 校验通过**：真正的"能恢复"只有 §7.3 演练能证明。

---

## 8. 恢复验收基线（2026-10-20 实测，恢复后逐项比对）

| 对象 | 基线 |
|---|---|
| DS 元库表数 | 65 |
| 流程定义 | `offline_dataware_lake` v10、release_state=1；另有 4 个历史流程 release_state=0 |
| 定时 | id=7，`0 0 2 * * ? *`，Asia/Shanghai，warning_type=2 / group=2 |
| 湖表（10 张，`lake` catalog 10-20 实测） | `ods.ods_order` 900,000 / `ods.ods_order_event` **3,111,841** / `ods.ods_product` 4,500 / `dim.dim_product` 50 / `dim.dim_product_scd2` 4,500 / `dwd.dwd_order_detail` 854,844 / `dwd.dwd_order_sku_detail` 854,844 / `dwd.dwd_order_lifecycle` 900,000 / `dws.dws_user_order_day` 18,000 / `ads.ads_daily_sales` 90 |
| 仓侧内部表 | `ods_order` 900,000 / `ods_order_event` 3,120,421 / `dwd_order_detail` 854,844 / `dwd_order_sku_detail` 854,844 / `dwd_order_lifecycle` 900,000 / `dim_product` 50 / `dim_product_scd2` 4,500 / `dws_user_order_day` 18,000 / `ads_daily_sales` 90 |
| Kafka | topics：`ods_order`、`ods_order_event`、`test`（各 3 分区）+ `__consumer_offsets`（50 分区）；offset 基线由周备的 `kafka_topics.txt` 记录（容器内日志目录 `/tmp/kraft-combined-logs`，保留 720h） |
| FE image | 最新 `image.NNNNN` 目录名 + 文件数（每日备份里记录） |

> 首次演练时把上表填成**机器可读的基线文件**（例如 `docs/baselines/lake_counts.txt`），恢复脚本自动 diff。

---

## 9. 已知限制与待办

| # | 限制 | 影响 | 处置 |
|---|---|---|---|
| 1 | **02:00 真实定时尚未触发过**（10-19/10-20/10-21 三次均未触发；根因：Docker Desktop `AutoStart=False`，WSL 08:58 才起） | 不知道自动定时是否稳 | 二选一：打开 Docker Desktop 开机自启；或建 01:50 的 Windows 计划任务拉起 Docker |
| 2 | ~~WSL 里没有常驻 cron~~ **已解决** | — | 备份已改成 DS 两个独立工作流（2026-10-20/21），cron 仅作兜底 |
| 3 | Kafka 备份需停 Kafka 或接受不一致 | 周备窗口 30 秒级停机 | 先只做"topic 元数据 + offset 快照"，Kafka 数据本身等放量后再议 |
| 4 | Iceberg 快照回滚（`rollback_to_snapshot`）未验证 | S3 的"窗口外恢复"少一条路 | 标记 ⏳ 待验证；当前一律用 warehouse tar 恢复 |
| 5 | ~~BE 重建 RTO 未实测~~ **已实测** | — | 仓侧 dws/ads 全量 `--truncate` 重建 **5 s**（见 §10 D5） |
| 6 | 备份未进 git（体积大） | 备份清单/基线文件要进 git，数据不进 | 只提交 `scripts/backup/`、基线文件、本文档 |
| 7 | FE image 生成要等 checkpoint 周期（实测 0.2 s 返回、最晚 72 s 落盘；FE 自己也约 2 分钟轮转一次） | 每日备份耗时 ≈80 s | 可接受（03:00 无批处理）；校验必须轮询 journalId，不能只看"号变了" |
| 8 | image 每轮转一次 `MetaCleaner` 立刻删旧 image | 打包瞬间撞车可能 tar 到正在删的文件 | 归档后立刻 `tar -tzf` 校验；失败重试一次 |
| 9 | 宿主/容器都没有 pigz/zstd/lz4，只有 gzip/bzip2/xz | 压缩只能用 `gzip -9` | 够用（Kafka 711 MB → 96 MB / 19~25 s） |
| 10 | `/mnt/d`（9p）写 tar 会丢 owner/mode（变 0777 root:root） | 副本目录权限不对，审计困难 | 主份放 ext4 `/home/l/offline-dw/backups`，`/mnt/d` 只当副本（实测跨盘只慢 13%） |
| 11 | README 金丝雀写 `bdb` 期望 33 项、`image/v2` 2 项 | 数字会随 journal 轮转变化（实测现 46 个 .jdb） | 金丝雀只判"非空且可读 + clusterId 对"，别比固定数字（README 已同步改） |

---

## 10. 首次恢复演练记录（2026-10-21，实机）

> 执行方式：真实生产容器上做，每步先取基线、失败自动回滚；S7 为桌面推演。基线 = §8 + 湖 10 表行数 + `recon 26 项`。

| 编号 | 场景 | 动作 | 实测结果 | 耗时 |
|---|---|---|---|---|
| D0 | 基线 | 采集版本/catalog/10 表行数/仓侧行数/DS 元数据/FE image | 全部留档，`recon 26/26 OK` | 20 s |
| D1 | S3-a（窗口内补数） | `lake_ods_replay.sh --date 2026-10-20` 重放 | 分区行数 `10000 / 33680` 逐字不变、对账 23 项 OK（幂等） | 30 s |
| D2 | S6（warehouse） | 周备 `lake_warehouse.tar.gz`（129 MB / 6230 条目）解到隔离目录，独立 catalog `lake_rst` 读 10 表 | 10 表行数与生产逐项一致；解包 2 s；**生产目录未被触碰** | ~40 s |
| D3 | S5（FE image） | 停 starrocks → `mv bdb`（46 个 .jdb / 315 MB）→ 还原 image → 起 | 首次失败（见下方坑 1）；加 `start_with_incomplete_meta` 后 **16 s 起**，版本 `3.5.0-10d7323`、`default_catalog+lake`、湖表/`dws`/`ads` 行数全一致；去掉开关再重启 12 s 仍正常；`recon 26/26 OK` | ~2 min |
| D4 | S4（DS 元库） | 现做 dump（92 K）→ 停 DS → `DROP/CREATE DATABASE` → 导入（6 s）→ 起 DS（44 s） | 7 定义 / 33 任务 / 7 定时（3 上线）/ 395 实例 / admin 逐项一致；再用 API 触发备份工作流 → **实例 409 全绿** | ~1 min |
| D5 | S1（仓侧重建） | `run_dws_ads_from_lake.sh --truncate` | dws 18000 行 / ads 90 行重建，`paid=119749476.90`、`net=105680366.50` 与重建前逐字一致；自检 + 对账 26 项 OK | **5 s** |
| D6 | S7（整机） | 桌面推演：按 §6.2 顺序逐条核对命令 | 各步已由 D2/D3/D4/D5 单独验证；缺"从零 compose + 数据卷"端到端 | — |

**踩到并已修回的坑：**

1. **FE 只放 image 会拒启**：`image exists, but bdb dir is empty, set start_with_incomplete_meta to true ...` → `restore_fe.sh` 已内置"加开关 → 恢复 → 删开关并重启"；
2. **`tar` 顶层是 `lake/warehouse/...`**：解到隔离目录必须 `--strip-components=1`，否则 catalog 指向空目录；
3. **Spark 读 Iceberg 必须显式写 catalog 类**：`--conf spark.sql.catalog.X=org.apache.iceberg.spark.SparkCatalog`（只给 `.type` 会 `Catalog 'X' plugin class not found`）；或 `--conf spark.sql.defaultCatalog=X` 后写两段式表名；
4. **DS API 手工触发必须带 `scheduleTime`**（漏了报 `MissingServletRequestParameterException` → 50014）；返回的 `data` 是定义 code，实例 id 要查 `t_ds_process_instance`；
5. **`/mnt/d` 镜像不能用 `cp -a`**（9p 刷 `Operation not permitted`）→ 周报镜像已改 `cp -r --no-preserve=ownership,timestamps`。

**结论**：S1 / S3-a / S4 / S5 / S6 的恢复动作全部实测可用；RTO 均低于原估计（S1 5 s、S4 60 s、S5 2 min）；唯一未实机的是 S7 整机重装。

## 附录 A：命令速查

```bash
# ── 每日 ──────────────────────────────────────────────
bash /home/l/offline-dw/scripts/backup/backup_daily.sh
# 手工等价：
# DS 元库（绝不要加 --databases）
docker exec mysql mysqldump -uroot -proot123 --single-transaction --quick \
  --routines --triggers --events --no-tablespaces dolphinscheduler | gzip -9 > ds.sql.gz
# FE 镜像（异步：ALTER 后轮询 v2 目录出现新 image.<id>）
docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -e "ALTER SYSTEM CREATE IMAGE;"
ls -1t /home/l/offline-dw/starrocks/fe/meta/image/v2 | head -3    # 取最新 image.NNNNN
tar -czf fe_image.tar.gz -C /home/l/offline-dw/starrocks/fe/meta image
# 台账 / 输入
cp /home/l/offline-dw/ds/logs/ops/ledger.csv .
tar -czf data.tar.gz -C /home/l/offline-dw data

# ── 每周 ──────────────────────────────────────────────
bash /home/l/offline-dw/scripts/backup/backup_weekly.sh                 # 湖 + jars/conf + git bundle + kafka 元数据
bash /home/l/offline-dw/scripts/backup/backup_weekly.sh --with-kafka    # 加 Kafka 原始消息（停 Kafka ~30s）

# ── 恢复 ──────────────────────────────────────────────
# DS 元库（目标库必须显式写，不能靠 dump 里的 USE）
zcat ds_dolphinscheduler.sql.gz | docker exec -i mysql mysql -uroot -proot123 dolphinscheduler
# FE 镜像（★ 先移走 bdb，否则 journal 重放，回滚"没生效"）
bash /home/l/offline-dw/scripts/backup/restore_fe.sh /path/to/fe_image.tar.gz --yes
# 手工等价：
docker compose -f /home/l/offline-dw/docker-compose.yml stop starrocks
mv /home/l/offline-dw/starrocks/fe/meta/bdb /home/l/offline-dw/starrocks/fe/meta/bdb.bak.$(date +%s)
rm -rf /home/l/offline-dw/starrocks/fe/meta/image/*
tar -xzf fe_image.tar.gz -C /home/l/offline-dw/starrocks/fe/meta       # 归档根是 image/
echo 'start_with_incomplete_meta = true' >> /home/l/offline-dw/starrocks/fe/conf/fe.conf   # ★ 实测必需（否则 FE 拒启）
docker compose -f /home/l/offline-dw/docker-compose.yml start starrocks
bash /home/l/offline-dw/scripts/lake/sr_catalog_reset.sh
# 湖数据
bash /home/l/offline-dw/scripts/backup/restore_lake.sh /path/to/lake_warehouse.tar.gz --yes

# ── 验收 ──────────────────────────────────────────────
docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -N -e "
  SELECT 'ods.ods_order', COUNT(*) FROM lake.ods.ods_order
  UNION ALL SELECT 'ods.ods_order_event', COUNT(*) FROM lake.ods.ods_order_event
  UNION ALL SELECT 'dwd.dwd_order_detail', COUNT(*) FROM lake.dwd.dwd_order_detail;"
bash /home/l/offline-dw/scripts/recon_lake_wh.sh
bash /home/l/offline-dw/scripts/dqc_lake_ods.sh $(date -d yesterday +%Y%m%d)
```

## 附录 B：与阶段 2 计划的对应关系

| 本文章节 | 计划项 | 状态 |
|---|---|---|
| §2 / §6.1 | A2 备份机制 | 本文档 = 规范；`scripts/backup/*` 待你落地 |
| §4 / §6.2 | A2 恢复演练 | 首次演练计划 §7.3；S4 已在验证 |
| §5 | A6 补数框架 | 依赖矩阵 + DS 补数两个坑 + 手工补命令 |
| §8 | A3 监控基线 | 基线文件化后可进台账 |

> 与「方向 3 冷备与补数」的关系：本文 = 该方向的**文档交付**；代码（`scripts/backup/backup_daily.sh`、`backup_weekly.sh`、`restore_*.sh`、`verify_backup.sh`、`scripts/backfill.sh`）由你按 §6/附录 A 落地，我已在对话里给出可抄的实现骨架。

---

## 附录 C：实测验证记录（2026-10-20，本轮真实环境）

| 项 | 结果 |
|---|---|
| 每日备份（不含 jars） | **332 KB / 78 s**（其中 ~76 s 等 FE checkpoint 生成新 image） |
| FE image 生成 | `ALTER SYSTEM CREATE IMAGE` 后等 checkpoint 周期（实测 60~120 s），落 `meta/image/v2/image.NNNNN`；单发 `image/` 只有 ROLE/VERSION/v2 会误判"未生成" |
| spark jars | 52 MB 源 / tar.gz **46 MB**（iceberg-runtime 占大头）——已改为跟**周备**走，不塞日报 |
| 每周备份 | lake tar **122.5 MiB / 3.7 s / 5918 条目 / 10 张表 version-hint**；整包 189 MB / 16 s（含 jars 46 MB + repo.bundle 520 KB） |
| 完整性校验（`--deep`） | manifest 全 PASS；DS dump 导入 scratch 库：**65 表 / 5 个流程定义 / 1 条在线定时**，校验+清理 12 s |
| DS dump | mysqldump 无 `--no-tablespaces` 会在部分 MySQL 8 环境报 PROCESS 权限错；本环境加不加都能跑，脚本统一加上 |
| 湖表基线 | 见 §8（`lake.ods.ods_order_event` = 3,111,841，注意与仓侧 3,120,421 不同口径） |
| Kafka offset 基线（`kafka-get-offsets.sh`） | `ods_order` 303765/302960/303275；`ods_order_event` 1039676/1040063/1040682；两个 consumer group LAG=0 |
| 解档一致性 | lake 解档 5248 文件，md5 清单与现场**逐字节一致**；两次独立 tar 的 sha256 相同 |
| 整套冷备（验证员交叉实测） | 6 个 artifact 共 **286 MB / 89.5 s**（含等 FE checkpoint 41.9 s），`sha256sum -c` 全通过 |
| DS dump 导入耗时 | 5.63 s（65 表）；跨容器可移植（同 dump 导入 mysql8 6.47 s 结果一致） |

**本轮最重要的两个新发现**：
1. **FE image 路径与命名**：`meta/image/v2/image.NNNNN` + `checksum.NNNNN`，若按 `meta/image/image.*` 找会永远找不到（我第一版脚本就踩了，日常备份会静默失败）；
2. **Iceberg 绝对路径**：备份/恢复演练必须恢复到原路径，否则演练无效（§4 S6、§7.3 已改）。
