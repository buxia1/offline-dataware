# legacy_warehouse —— 「仓侧 ODS → DIM → DWD」那一套（已退役）

**退役日期：2026-10-16**。原因：计算层搬到湖侧（Spark + Iceberg）之后，「在 StarRocks 里做
维度 / 明细 / 汇总」这条链路被 `scripts/lake/` 取代，对应的工作流也全部下线
（`offline_dataware` / `dim_product_chain` / `order_event_chain` 均 `release_state=0`）。

保留它们的唯一理由是**口径对照**：`ABS(amount)`、SCD2 范围关联 `o.dt BETWEEN valid_from AND valid_to`、
按天 `INSERT OVERWRITE`、累积快照的五个里程碑回填，这些逻辑在
`scripts/lake/lake_dwd_build.py` / `scripts/lake/lake_dim_build.py` 里是**逐字照搬**的，
湖侧结果对不上时要回头读这几支。

## 文件与取代关系

| 退役脚本 | 原职责 | 现在由谁取代 |
|---|---|---|
| `load_product_to_ods.sh` | 商品快照 CSV → 仓侧 `ods.ods_product`（Stream Load） | `scripts/lake/run_ods_product.sh` → 湖侧 `lake.ods.ods_product` |
| `dim_product_scd2_load.sh` | 仓侧 SCD2 全量重建（TRUNCATE + 全量重推） | `scripts/lake/run_dim.sh build` → `lake.dim.*` |
| `dim_product_scd2_incremental.sh` | 仓侧 SCD2 增量装载（外壳替换 `${LAST}`） | 同上（湖侧每次 `DELETE` + `append`，天然幂等） |
| `dwd_sku_load.sh` | 仓侧 `dwd_order_sku_detail` 逐天物化（范围 JOIN） | `scripts/lake/lake_dwd_build.py --table dwd_order_sku_detail` |
| `dwd_order_lifecycle_load.sh` | 仓侧累积快照 `dwd_order_lifecycle`（增量 / `--full`） | `scripts/lake/lake_dwd_build.py --table dwd_order_lifecycle` |
| `dwd_overwrite.sh` | 仓侧 `dwd_order_detail` 按天 `INSERT OVERWRITE`（内联 SQL） | `scripts/lake/lake_dwd_build.py --table dwd_order_detail` |

## 没有一起搬过来的 SQL（仍在 `sql/` 下，还要读）

- `sql/{ods,dim,dwd}/*`：仓侧建表与装载 SQL（历史口径参考，`dwc` 与 `lake_dwd_build.py` 对照用）
- `sql/dws_ads/*_insert.sql`：**仓侧 DWS / ADS 的指标口径原文** —— 现在的主链路
  `sql/dws_ads_from_lake.sql` 的口径就是逐字照搬这两支，所以它们**仍然是活文档**

## 用法（如果还要考古）

这些脚本原本都是从**仓库根目录**跑的（内部用 `sql/...` 这种相对路径）：

```bash
cd /home/l/offline-dw
bash scripts/legacy_warehouse/dwd_order_lifecycle_load.sh --full
```

⚠️ 三点：

1. 它们全都假设**仓侧 `ods.*` 是最新的**。湖侧链路**不依赖**仓侧 ODS，所以从 2026-10-16 起
   这些脚本产出的数据就停在旧口径上了 —— **不要再拿它的结果与湖侧对账**（要用 `scripts/recon_lake_wh.sh`）。
2. 它们原先挂在 DS 工作流 `offline_dataware` / `dim_product_chain` / `order_event_chain` 上，
   节点里的脚本路径写的是 `scripts/xxx.sh`（**旧位置**）。那三个工作流已全部离线；如果哪天要
   重新上线，记得把节点路径改成 `scripts/legacy_warehouse/xxx.sh`。
3. `dwd_overwrite.sh` 与旧工作流里的 `dwd_delete` 节点**其实是两条独立实现**（节点里是内联 SQL）,
   别以为改这里会影响那个节点。
