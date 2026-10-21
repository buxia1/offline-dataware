# D1 真实数据接入方案（数据集选型 + 落地方案）

| 项 | 内容 |
|---|---|
| 文档状态 | 草案 v1，2026-10-21（待你回家确认数据集后定稿） |
| 分工 | 文档/设计/验收判据 = 我；下载、脚本落地、DS 接线命令 = 你 |
| 上游 | `docs/next-phase-plan.md` 阶段 2；方向 5 实时化（A0 已上线）之后的深化主线 |
| 目标 | 把「模拟 90 万单」的湖仓扩成「真实数据 + 复杂关系 + 多字段」的建模底座，为 D2 维度建模 v2 铺路 |

## 0. 结论先行

- **主数据集：dunnhumby《The Complete Journey》**（8 张表、约 4,000 万行、原始约 2.5GB、杂货零售 + 营销触达/优惠券/曝光）。
  理由：关系最复杂（多事实 + 桥表 + 因果曝光表）、字段最"业务"、国内数仓教程几乎没人用（同质化低）、单机 11G 内存完全跑得动。
- **规模校验集：NYC TLC 行程数据 2022–2024**（约 1.2~1.3 亿行、parquet 约 3~5GB），只用来练分区裁剪 / 压缩 / 大表 join / 物化重刷，不参与业务建模。
- **备选**：想练半结构化 JSON + 文本 → Yelp Open Dataset（约 8.6GB）；想练 XML + 强关系 → Stack Exchange 数据转储（先裁一个小站点）。
- **不选 Olist**：9.9 万单、约 100MB、全网教程泛滥；虽真实但"关系浅、字段少、无营销侧"，补不上你要的深度。

## 1. 为什么换数据集（现状痛点）

| 现状 | 问题 |
|---|---|
| 9 张 mock 表（90 万单/312 万事件） | 生成器有状态（README 已知限制 #10/#11），字段窄（订单/事件就那几列），关系是"一条流水线"，没有营销、没有多事实 |
| 只有 1 张 dws + 1 张 ads | 维度建模没展开：没有 dim_date、没有多粒度事实、没有桥表 |
| 现有链路是"按天增量" | 适合运维演练，但数据本身没有真实业务含义，无法支撑"复杂表关系"的建模练习 |

D1 的验收不是"把数据导进来"，而是：**同一个湖仓里并存两套数据集**——mock 保规模与实时演练，真实数据保建模深度。

## 2. 选型原则

1. **体量 ≥ 10× Olist**（Olist 约 100MB / 100 万行）；
2. **关系复杂**：≥ 5 张表，有事实/维度/桥表/多对多，最好有"曝光-转化"这类因果结构；
3. **字段多且真实**：不是 7 列流水，而是有商品层级、家庭画像、促销、地理等；
4. **同质化低**：不是人手一份的 Olist/Instacart/MovieLens；
5. **单机可跑**：原始数据 ≤ 10GB（受 11G 内存 + 2g Spark 容器约束，需可分块处理）；
6. **许可清晰**：个人学习/研究可用，且不进 git（`data/` 已在 `.gitignore`）；
7. **有真实时间列**：能按天/周分区，能练增量与重跑。

## 3. 候选对比

| 数据集 | 原始体积 | 规模 | 表数 / 关系 | 字段/嵌套 | 同质化 | 单机 | 获取 | 适合 |
|---|---|---|---|---|---|---|---|---|
| Olist 巴西电商 | ~100MB | 9.9 万单 / ~100 万行 | 9 表，星型 | 中 | **高** | 易 | Kaggle | 入门电商 |
| **dunnhumby Complete Journey** | **~2.5GB** | **~4,000 万行**（causal 3,676 万） | **8 表：交易/商品/家庭/活动/券/核销/曝光** | 中（~60 列） | **低** | 易 | dunnhumby 官网（注册）/ Kaggle 镜像 | **建模深度、营销归因** |
| Yelp Open Dataset | ~8.6GB | 700 万 review / 1,100 万行 | 5 表（user/business/review/tip/checkin），JSON 嵌套属性 | 高（嵌套 dict/list） | 中 | 中 | 官网表单下载 | 半结构化、地理、文本 |
| NYC TLC 行程 | 3~5GB parquet | **1.2~1.3 亿行** | 2 事实 + zone lookup（外部加天气/节假日维） | 低（~19 列） | 中 | 中 | 官方直连（免注册） | **规模、分区、压缩** |
| H&M 时尚推荐 | ~3GB CSV | 3,150 万交易 / 137 万客户 / 10.5 万商品 | 3 表 | 中（商品 25+ 属性） | 中 | 易 | Kaggle | 商品 SCD2、零售 |
| Amazon Reviews 2023 | 单品类 10~40GB | 单品类 1,000 万~4,000 万条 | 2 表（review + metadata） | 极高（品类树/价格） | 低 | **难（需裁剪）** | HuggingFace / Kaggle | 超大规模、文本 |
| Stack Exchange 转储 | 数十 GB | 亿级 | 8 表（posts/users/comments/votes/badges/links/tags） | 高（XML） | 低 | 难 | archive.org | 强关系 + XML，需先裁站点 |
| GitHub Archive | 单月 4~6GB | 百万/天 | 嵌套事件 JSON（actor/repo/org/payload） | 极高 | 中 | 中 | gharchive | 事件流、增量 |
| 天池淘宝 UserBehavior | ~3.4GB | 1 亿行 | **1 表**（user/item/category/behavior/ts） | 低 | 高 | 易 | 天池 | 纯规模压测 |

> 体积/行数为公开资料范围值；**导入前用 `wc -l` / `du -sh` 实测校准并写入清单**（见 §5.1），文档不把估算值当验收依据。

## 4. 推荐组合与理由

### 4.1 主：dunnhumby《The Complete Journey》

**关系结构（这正是 Olist 给不了的）：**

```
dim_household (801 户)          dim_product (92,353 SKU，含 department/commodity/brand)
      │                                │
      ├── fct_transaction (约 259.6 万行：household×basket×product×day，含金额/数量/券折扣)
      ├── fct_coupon_redemption (约 2,318 行：household×coupon×day)  ← 桥表
      ├── bridge_campaign_household (7,208 行：活动×家庭)
      │
dim_campaign (30 个活动) ── dim_coupon (116,204 张券，coupon_upc×product×campaign)
      │
      └── fct_causal_exposure (约 3,676 万行：product×store×week × display/mailer 曝光标志)
```

- **多事实**：交易、曝光、核销三张事实表，粒度不同（行级 / 周×店×品 / 券×户×日）；
- **桥表/多对多**：券 → 商品 → 活动 → 家庭；
- **营销归因**：曝光表 × 交易表可以做"触达组 vs 未触达组"的 lift 分析；
- **真实画像**：家庭 8 个社会人口属性（可做 SCD2 练习）、商品 4 级层级、102 周时间轴（可造 dim_date）；
- 体量：约 2.5GB / 约 4,000 万行，比 Olist 大 25 倍体积、40 倍行数，但仍在单机舒适区。

**代价（写清楚）**：数据是 2013–2014 年美国杂货零售，业务叙事要重新设计（不是"电商订单流水"），且**静态快照**——没有每日新增，DAG 是"回填型"而非"日更型"。

### 4.2 规模校验：NYC TLC（可选并行）

- 只做两件事：① 分区裁剪/谓词下推的效果对比；② 小文件合并（compaction）与物化重刷；
- 表：`lake.retail.tlc_trip`（按 pickup 月分区）+ `taxi_zone_lookup`（265 行维度）；
- 不并入业务 DWS/ADS，避免维护面膨胀。

### 4.3 备选切换条件

- 你更想要 **半结构化/文本/地理** → 换 Yelp（8.6GB，5 表，JSON 嵌套属性要用 Spark `from_json` 展开）；
- 你更想要 **XML + 强关系** → Stack Exchange，但先裁 `dba.stackexchange.com` 这类小站（约 1~2GB）再扩；
- 你更想要 **纯规模** → TLC 或天池 UserBehavior，直接当性能测试集。

## 5. 主数据集落地方案（dunnhumby）

### 5.1 Step 1：下载、校验、建清单（你执行）

```bash
# 方式一：dunnhumby 官网（免费注册后下载 8 个 CSV）
#   https://www.dunnhumby.com/source-files/  → "The Complete Journey"
# 方式二：Kaggle 镜像（需要 kaggle.json）
mkdir -p ~/offline-dw/data/raw/dunnhumby && cd ~/offline-dw/data/raw/dunnhumby
# kaggle datasets download -d frtgnn/dunnhumby-the-complete-journey -p . && unzip -o *.zip

# 校验 + 建清单（结果写入 _inventory.txt，作为后续对账基线）
for f in *.csv; do
  printf "%-28s rows=%-10s bytes=%-12s sha256=%s\n" "$f" \
    "$(tail -n +2 "$f" | wc -l)" "$(stat -c %s "$f")" "$(sha256sum "$f" | cut -c1-16)"
done | tee _inventory.txt
du -sh . && ls -lh
head -1 transaction_data.csv product.csv hh_demographic.csv causal_data.csv
```

> 落位：`data/raw/dunnhumby/`（`data/` 已 gitignore，不进仓库）。
> 注意：8 个文件是 CSV、逗号分隔；如遇编码/空值问题，在 Spark 侧显式 schema + `nullValue` 处理，不要靠推断。

### 5.2 Step 2：湖侧 ODS（原始层）

新建 Lake 库 `lake.retail`（与 mock 的 `lake.ods/dwd/...` 隔离），8 张 ODS 表：

| ODS 表 | 源文件 | 关键字段 | 分区 |
|---|---|---|---|
| `ods_transaction` | transaction_data.csv | household_key, basket_id, day, product_id, quantity, sales_value, retail_disc, coupon_disc, coupon_match_disc, store_id, trans_time, week_no | `week_no`（1..102） |
| `ods_product` | product.csv | product_id, manufacturer, department, brand, commodity_desc, sub_commodity_desc, curr_size_of_product | 无（快照） |
| `ods_household` | hh_demographic.csv | household_key, age_desc, marital_status_code, income_desc, homeowner_desc, hh_comp_desc, household_size_desc, kid_category_desc | 无 |
| `ods_campaign` | campaign_desc.csv | campaign, description, start_day, end_day | 无 |
| `ods_campaign_household` | campaign_table.csv | household_key, campaign, description | 无 |
| `ods_coupon` | coupon.csv | coupon_upc, product_id, campaign | 无 |
| `ods_coupon_redemption` | coupon_redempt.csv | household_key, day, coupon_upc, campaign | 无 |
| `ods_causal` | causal_data.csv | product_id, store_id, week_no, display, mailer（曝光标志；**字段以 `head -1` 实测为准**） | `week_no` |

写入方式：Spark 读 CSV（显式 schema）→ `INSERT OVERWRITE` 对应分区（幂等，可重跑）。

### 5.3 Step 3：DIM 层

| 维度 | 粒度 | 要点 |
|---|---|---|
| `dim_date` | day（711 天） | 从 day 1..711 生成：日期、周、月、季、年、是否促销周；**这是 mock 数据集缺的第一块** |
| `dim_household` | household_key | 8 个属性；SCD2 练习点（按属性变化打版本，数据本身只有一个快照，可人为切两版做机制验证） |
| `dim_product` | product_id | 4 级层级 department→commodity→sub_commodity→brand；`curr_size_of_product` 解析出数值+单位 |
| `dim_campaign` | campaign | 活动起止日 → 与 dim_date 关联 |
| `dim_coupon` | coupon_upc | 券×商品×活动 |
| `dim_store` | store_id | 从交易/曝光表 distinct 生成（数据无独立门店文件） |

### 5.4 Step 4：DWD 事实层（三种粒度）

| 事实表 | 粒度 | 度量 |
|---|---|---|
| `fct_transaction` | household × basket × product × day（行级） | quantity、sales_value、retail_disc、coupon_disc、coupon_match_disc、净额=销售额−折扣 |
| `fct_causal_exposure` | product × store × week | display_flag、mailer_flag（曝光强度） |
| `fct_coupon_redemption` | household × coupon × day | 核销次数 |
| `bridge_campaign_household` | campaign × household | 触达标记 |

> 建模考点：三张事实表粒度不同，DWS 聚合时**不能交叉 join 后重复计数**（典型的 fan-out 陷阱，D2 文档里会专门写口径）。

### 5.5 Step 5：DWS / ADS

| 层 | 表 | 指标 |
|---|---|---|
| DWS | `dws_household_week` | 消费额、订单数、件数、券核销数、核销率、客单价、活跃周标记（RFM 底座） |
| DWS | `dws_product_week` | 销量、销售额、购买家庭数、渗透率、促销销售占比（retail_disc>0） |
| DWS | `dws_campaign_week` | 触达家庭数、核销家庭数、核销率、触达组 vs 未触达组消费 lift |
| ADS | `ads_household_segment` | 家庭分群（RFM 八分群）及消费占比 |
| ADS | `ads_product_contribution` | 品类/品牌销售贡献 + 帕累托 |
| ADS | `ads_campaign_roi` | 活动 ROI（核销额 / 触达成本可以先用核销额近似） |
| ADS | `ads_coupon_funnel` | 发券→触达→核销漏斗 |

### 5.6 Step 6：调度（DS 工作流）

**性质：回填型（一次性 + 手动重跑），不是日更型。** 建议单独工作流，不与 `offline_dataware_lake` 混：

```
retail_backfill (手动触发 / 数据更新时触发)
  1 retail_ods_load     : bash scripts/retail/run_retail.sh ods     (60min 超时)
  2 retail_dqc_ods      : bash scripts/retail/dqc_retail.sh ods     (10min)
  3 retail_dim_build    : bash scripts/retail/run_retail.sh dim     (30min)
  4 retail_fct_build    : bash scripts/retail/run_retail.sh fct     (60min)
  5 retail_agg_build    : bash scripts/retail/run_retail.sh agg     (30min)
  6 retail_dqc_all      : bash scripts/retail/dqc_retail.sh all     (10min)
  7 retail_recon        : bash scripts/retail/recon_retail.sh       (10min，退出码 2=不一致)
```

- 复用现有 Spark 容器与串行锁 `/tmp/.spark_job.lock`（`_spark_lib.sh`），与 02:00 主链路互斥；
- 幂等：所有写入按分区 `INSERT OVERWRITE`（week_no / 快照），重跑不产生重复；
- 脚本全放 `scripts/retail/`，DS 里以 `bash /opt/offline-dw/scripts/retail/...` 调用（沿用现有挂载）。

### 5.7 Step 7：DQC 与对账（10 条必须）

| # | 检查 | 判据 |
|---|---|---|
| 1 | 8 张 ODS 表行数 = 源 CSV 行数 | Δ=0（以 `_inventory.txt` 为准） |
| 2 | transaction 行级唯一 | `(household_key,basket_id,day,product_id,trans_time)` 重复=0 |
| 3 | product_id 外键覆盖率 | 100%（交易/曝光/券都能在商品维找到） |
| 4 | household_key 外键覆盖率 | 100% |
| 5 | 金额合法 | `sales_value>=0`、`quantity>0`、净额=销售额−折扣（Δ<0.01） |
| 6 | 分区完整 | `week_no` 覆盖 1..102，无空洞 |
| 7 | 券核销外键 | coupon_redemption.coupon_upc ⊆ coupon.coupon_upc |
| 8 | 曝光表粒度唯一 | `(product_id,store_id,week_no)` 重复=0 |
| 9 | 金额对账 | 源 `sum(sales_value)` vs 湖 `sum(sales_value)`，Δ<0.01 |
| 10 | 汇总层可回算 | DWS 周汇总 = 从 DWD 重算（同一口径），Δ<0.01 |

### 5.8 验收判据（D1 完成的定义）

- [ ] `_inventory.txt` 存在，8 表行数与源一致；
- [ ] `lake.retail` 下 8 ODS + 6 DIM + 4 事实/桥 表全部建成，行数在上表预期量级；
- [ ] DQC 10 条全绿（`dqc_retail.sh all` 退出码 0）；
- [ ] 对账 Δ<0.01（金额、行数）；
- [ ] 4 张 ADS 能在 StarRocks 里查到（外部 catalog 直读 Iceberg）；
- [ ] 文档 `docs/dimension-modeling-v2.md` 起草（D2 产物，与本阶段同步开始）；
- [ ] 全流程重跑一次仍幂等（行数不变）。

### 5.9 内存 / 耗时预算

| 项 | 估算 | 说明 |
|---|---|---|
| 原始数据 | ~2.5GB / ~4,000 万行 | causal_data 约 3,676 万行 / 约 1.5GB，是最大单文件 |
| 湖侧 parquet | ~0.8~1.2GB | 压缩后 |
| Spark 配置 | 沿用 `local[4]` / driver 1g / `shuffle.partitions=32` | 不加大，避免 2g 容器 GC |
| 单次回填总耗时 | 约 5~10 分钟 | 按 week_no 分批处理 causal；transaction 一次读完 |
| 内存峰值 | < 1.5GB（driver+executor 同 JVM） | 分批 + 不 cache 大表 |

## 6. 与现有链路的集成

| 对象 | 处理 |
|---|---|
| 湖 catalog | 现有 `lake`（warehouse `file:///opt/offline-dw/lake/warehouse`）下新增 `retail` 库，不动 mock 的 `ods/dim/dwd/dws/ads` |
| StarRocks | 外部 catalog `lake` 自动可见 `lake.retail.*`；仓侧新增 `dws_retail` / `ads_retail` 两张物化（沿用 `run_dws_ads_from_lake.sh` 的模式，写一个 `run_retail_wh.sh`） |
| DS | 新工作流 `retail_backfill`，7 节点（§5.6），与主链路串行锁互斥 |
| 台账 | **不改 26 列主台账**；retail 工作流成功与否记自己的 `retail_ledger.csv`（避免污染 7 天对账验收） |
| 备份 | `backups/weekly` 的 lake tar 会自动包含 `retail`（同 warehouse 目录），无需改脚本；数据量 +1GB 可接受 |
| 文档 | 新增 `docs/d1-real-data-ingestion-plan.md`（本文）、`docs/dimension-modeling-v2.md`（D2 起草） |

## 7. 风险与对策

| # | 风险 | 对策 |
|---|---|---|
| 1 | dunnhumby 官网需注册、Kaggle 镜像未必同步 | 两个渠道都给；下载后立刻 `_inventory.txt` 固化基线；不把原始数据进 git |
| 2 | 数据 2013–2014 美国杂货，业务叙事与"电商"脱节 | 叙事改为"零售营销效果分析"（活动触达/券核销/家庭分层），D2 文档统一口径 |
| 3 | causal_data 3,676 万行，2g Spark 容器易 OOM | 按 week_no 分批写；不 cache；driver 1g、local[4]；必要时先落 parquet 再聚合 |
| 4 | 静态数据集没有日增量 | 明确定位为"回填型"工作流；不接主链条日调度；模拟数据继续负责日更演练 |
| 5 | 双数据集增加维护面 | 边界写死：mock=规模/运维/A0 实时；retail=建模深度；台账、备份、对账互不干扰 |
| 6 | 许可 | dunnhumby 数据仅个人学习/研究使用，不二次分发；README 标注来源；`data/` 已 gitignore |
| 7 | 8 表字段与文档估算有出入 | §5.1 先实测；以 `head -1` 与 `_inventory.txt` 为准，schema 在 Step 2 定稿 |
| 8 | 若改选 Yelp，JSON 嵌套展开工作量大 | 先只展开 review/business 核心字段，attributes 字典按需拍平，不追求一次到底 |

## 8. 双数据集边界（写进 README）

| 维度 | mock（现有） | retail（D1 新增） |
|---|---|---|
| 用途 | 规模、性能、实时（A0 5 分钟）、7 天对账、运维演练 | 真实关系、维度建模深度、营销归因 |
| 规模 | 90 万单 / 312 万事件 | 约 4,000 万行 / 8 表 |
| 更新 | 每天 02:00 自动（云侧 T+1）+ 每 5 分钟实时 ODS | 手动/数据更新时回填 |
| 湖库 | `lake.ods/dim/dwd/dws/ads` | `lake.retail.*` |
| 台账 | `ds/logs/ops/ledger.csv`（26 列，7 天对账用） | `ds/logs/ops/retail_ledger.csv` |
| 备份 | daily/weekly 已覆盖 | weekly lake tar 自动覆盖 |

## 9. 备选：规模数据集 TLC 轻量方案（可选）

```bash
# 官方直连（免注册）：每年 1 个文件，yellow/green 各 12 个月
mkdir -p ~/offline-dw/data/raw/tlc && cd ~/offline-dw/data/raw/tlc
for y in 2022 2023 2024; do for m in $(seq -w 1 12); do
  curl -fL -O "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${y}-${m}.parquet"
done; done
du -sh .   # 预期 2~4GB
```

- `lake.retail.tlc_trip`：按 `date_trunc('month', tpep_pickup_datetime)` 分区；
- `dim_taxi_zone`：官方 `taxi_zone_lookup.csv`（265 行）；
- 练习：`WHERE pickup_month='2024-06'` 的分区裁剪对比；小文件合并前后查询耗时；`INSERT OVERWRITE` 单月重刷。

## 10. 下一步（分工）

**我（文档侧，已完成/待办）：**
- [x] 本文档（选型 + 落地 + DQC + 验收 + 风险）
- [ ] 你确认数据集后：补 8 表精确 schema（按你 `head -1` 实测字段）、写 `docs/dimension-modeling-v2.md` 骨架（ER 图 + 指标字典）
- [ ] 给 DS 工作流导入 JSON + 验收命令

**你（代码/命令侧）：**
- [ ] 回家确认下载渠道，跑 §5.1 建 `_inventory.txt`
- [ ] 写 `scripts/retail/{ingest,run_retail,dqc_retail,recon_retail}.sh`（我可以先给骨架代码，你落地）
- [ ] DS 导入 `retail_backfill` 工作流并跑通一次
- [ ] 按 §5.8 打勾验收

**预计工作量**：3~5 天（每天 1~2 小时），其中 Step 2/4 最费时（schema 与粒度设计）。

## 附录 A：为什么这是"深化"而不是"再加一个玩具数据集"

1. **多事实一致性**：交易/曝光/核销三种粒度 → 逼你定义口径，而不是"一张宽表打天下"；
2. **桥表与多对多**：券-商品-活动-家庭 → 逼你处理 fan-out、去重、归因；
3. **真实维度层级**：商品 4 级 + 家庭画像 + 时间轴 → dim_date / SCD2 / 层级汇总都有落点；
4. **规模**：4,000 万行 + TLC 1.3 亿行 → 分区、压缩、物化重刷、查询裁剪全部可练；
5. **叙事**：零售营销效果分析 → 指标字典能写出"业务含义"，而不是"订单数/销售额"两列。

## 附录 B：后续 D2 预览（不在本阶段实施）

- `dim_date`、`dim_customer/household`、`dim_product`（SCD2）、`dim_geo`（若选 Yelp/TLC）；
- 4 粒度事实表 + 桥表 + 累积快照；
- 指标字典（原子指标/派生指标/口径/负责人）；
- ER 图 + 分层血缘（ODS→DIM→DWD→DWS→ADS）；
- 与 dbt 的衔接（D3 技术栈）预留：模型名、测试点、文档字段先按 dbt 习惯设计。
