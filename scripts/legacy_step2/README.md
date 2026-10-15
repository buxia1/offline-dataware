# ⚠️ 已废弃：第 2 步「Spark 算 DWD/DWS/ADS → Stream Load 写回仓侧」

这里的东西是 **2026-10-14 早些时候**的产物：当时用户选的是「方案 A —— DWD/DWS/ADS 计算下沉 Spark，
结果写回**仓侧**」，那时湖侧只有 ODS 两张表，所以脚本必须「读湖 → 写仓」。

**后经用户确认，架构改成**：

```
Kafka ─┬─ Routine Load ─→ 仓侧 ODS（实时摄入缓冲，保留）
       └─ 湖侧重放 ────→ lake.ods → lake.dim → lake.dwd     ← 计算源（湖侧到 DWD 为止）
                                              │
                                              └→ 仓侧 dws / ads（在 StarRocks 里物化）
```

因此这一套**已被 `scripts/lake/lake_dwd_build.py`（湖侧算 DWD）+ `scripts/lake/run_dwd.sh` 取代**。
保留在此**只为两件事**：

1. **Stream Load 写仓侧的完整口径与坑**（README 的「这几支作业共同的坑」19~28 条就是这里来的）：
   直连 BE 8040（不能打 FE 8030）、必须 `Expect: 100-continue`、`columns` 头用**逗号**分隔、
   `max_filter_ratio=0`、时间要显式格式化、驱动 jar 必须挂进 `/opt/spark/jars/`、
   label 复用会报 `Label Already Exists` 而客户端只看到 empty reply……
2. **「为什么不能用 JDBC 写仓」的实测证据**：JDBC 写 9,475 行 = **467 秒**（≈20 行/秒，90 万行要 13 小时）；
   Stream Load 写 854,844 行 = **34 秒**。这是「湖仓是否值得做」的关键论据。

## 文件清单

| 文件 | 当时干什么 | 现在该用什么 |
|---|---|---|
| `dwd_order_detail_streamload.py` | 湖 ODS → 清洗 → 写仓侧 `dwd_order_detail` | `scripts/lake/lake_dwd_build.py` |
| `dwd_order_sku_streamload.py` | 湖 ODS + JDBC 读仓侧 SCD2 → 写仓侧 sku | 同上（口径已挪进去）|
| `dwd_order_lifecycle_streamload.py` | 湖事件表 → 聚合 → 写仓侧 lifecycle | 同上 |
| `dws_ads_streamload.py` | 湖 ODS → 聚合 dws/ads → 写仓侧 | `sql/dws_ads_from_lake.sql`（StarRocks 侧物化）|
| `run_dwd_detail_full.sh` / `run_dwd_sku_full.sh` / `run_dws_full.sh` / `run_lifecycle.sh` | 各自的外壳（补分区 / DELETE / 起容器） | `scripts/lake/run_dwd.sh` 等 |
| `probe_lifecycle_streamload.py` / `probe_lifecycle_bisect.py` | **一次性探针**：排查 `F.format_number` 输出千分位逗号污染 TSV（按行数二分定位到具体行） | 已完成使命，留作方法参考 |
| `probe_kafka.py` | 早期 Kafka 连通性探针 | 已被 `scripts/check_routine_load.sh` 取代 |

**不要**在新链路里引用本目录的任何文件。
