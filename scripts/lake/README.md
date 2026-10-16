# 湖侧链路脚本（Iceberg）

**湖侧是计算源**：`lake.ods` → `lake.dim` → `lake.dwd`（到 DWD 为止）。
仓侧 StarRocks 只负责 `dws` / `ads`，用 `sql/dws_ads_from_lake.sql` 从 `lake.dwd.dwd_order_detail` 物化。

| 脚本 | 作用 |
|---|---|
| `lake_ods_replay.py` + `lake_ods_replay.sh` | **Kafka → lake.ods 按天重放**：`bash scripts/lake/lake_ods_replay.sh --date 2026-09-20 [--dry-run]`。⚠️ `--dry-run` 必须放在 `.py` 之后 |
| `lake_dim_build.py` + `run_dim.sh` | SCD2 与当前版维度：`bash scripts/lake/run_dim.sh ddl\|build\|dry` |
| `lake_dwd_build.py` + `run_dwd.sh` | 明细 / SKU / 累积快照三张 DWD：`bash scripts/lake/run_dwd.sh build [表名\|all] [D1 D2]` |
| `lake_maintain.py` + `run_maintain.sh` | 湖表维护：**自动枚举** `lake.ods/dim/dwd` 全部表（当前 8 张）；只读盘点；`--expire --keep N` 清历史快照；`--tables a,b` 限定表。清理后**自验「快照数下降 且 行数不变」**，不达标退出码 1 |

**DDL** 在 `sql/lake/`：`lake_ods.sql` / `lake_dim.sql` / `lake_dwd.sql`。

**湖表维护**（`run_maintain.sh` 是 wrapper，与 `run_dim.sh` 同一套约定；不加 `--expire` 就是零写入的
只读盘点，实测 15 秒）：

```bash
bash scripts/lake/run_maintain.sh --expire --keep 5     # 真清理，每表保留最近 5 个快照
bash scripts/lake/run_maintain.sh                       # 只读盘点
```

DS 维护节点填 `bash /opt/offline-dw/scripts/lake/run_maintain.sh --expire --keep 5`。
验收判据由脚本自己执行并打印：**快照数下降 且 行数不变**，任一项不达标退出码 `1`（直接让 DS 节点变红）。

⚠️ **ODS 的两条来源**：日常是 **Kafka 重放**（上表第一行）；`run_ods.sh` 是「仓侧 ODS → 湖侧 ODS」的整表重建，
只在 Kafka 消息已被 retention 清掉、需要从仓侧那份不丢的 ODS 重建湖侧时才用（需要先从 Windows 侧
把 `lake_ods_ingest.py` 取回仓库，见 `run_ods.sh` 顶部注释）。
