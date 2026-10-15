#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
湖侧 DWD 层构建：lake.ods.* -> lake.dwd.*

三张表的口径**逐字照搬**仓侧实现（只把事实来源从仓侧换成湖侧 Iceberg）：

  dwd_order_detail      <- scripts/legacy_step2/dwd_order_detail_streamload.py
  dwd_order_sku_detail  <- scripts/legacy_step2/dwd_order_sku_streamload.py
  dwd_order_lifecycle   <- scripts/legacy_step2/dwd_order_lifecycle_streamload.py

用法：
  spark-submit build.py --table dwd_order_detail --mode rebuild
  spark-submit build.py --table all --mode append
  spark-submit build.py --table dwd_order_detail --mode append --start 2026-09-20 --end 2026-09-20
  spark-submit build.py --table all --mode dry            # 只算不写
  spark-submit build.py --table all --mode incremental    # ★ 按天增量：区间自己算（见下）
  spark-submit build.py --table all --mode incremental --lookback 3   # 回捞最近 3 天（迟到事件）

幂等约定（Iceberg 没有 TRUNCATE，一律「先 DELETE 本次范围、再 append」）：
  · detail / sku 带 dt：DELETE FROM <表> WHERE dt BETWEEN d1 AND d2
                        没有给区间时 = 全量重写（DELETE 整表）
  · lifecycle 无 dt   ：DELETE FROM <表> WHERE order_id IN (本次 scope 订单集合)
  · 写完后回读「行数 + SUM(amount) + COUNT(DISTINCT order_id)」与 Spark 侧期望比对，
    任一项不一致 -> 打印明确错误并 exit 2（由 run_dwd.sh 整体判失败）

所以 append / rebuild 两种 mode 的清理动作完全一致（都按上面规则删本次范围），
mode 只用来区分"算不算、写不写"，重复执行同一命令行数不会变化。

按天增量（--mode incremental）：区间 = [水位+1, ODS 最新完整日]，水位取【目标表自己的 MAX(dt)】：
  · detail / sku 带 dt      -> MAX(dt)
  · lifecycle 无 dt 列      -> MAX(dt) of lake.ods.ods_order_event
  · 空表                    -> MIN(dt) of ODS（第一次跑自动退化成"建表区间全量"）
  · 已知水位落后时，-1 天重算作为兜底（重算幂等，代价很小）
  · --lookback N            -> 水位再往前 N 天，用来回捞迟到事件
水位取"目标表自己"而不是 ODS，是为了保证 DWD ≤ ODS：若 ODS 某天刚被重放、
而 DWD 还没算，水位会停在 DWD 的历史位置，下次增量自然把那天补上。

只处理 ODS 已有的完整日：d1 > d2（没有新数据）时不算不写，打印 NOTHING TO DO 后正常退出——重复执行安全。

写入后除逐表自检外，还有一道**按天闭合检查**：detail 每天行数 vs ODS 当天
「user_id 非空」订单数、lifecycle vs 事件表当天 order_id 去重数。不一致的日期会被列出来；
默认只 WARN（--strict-closure 才 exit 2）——因为「ODS 当天分区被重放但还没重算」是正常瞬态。
"""
import argparse
import datetime
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

LAKE = "lake"
SRC_ORDER = "lake.ods.ods_order"
SRC_EVENT = "lake.ods.ods_order_event"
SRC_SCD2 = "lake.dim.dim_product_scd2"

T_DETAIL = "lake.dwd.dwd_order_detail"
T_SKU = "lake.dwd.dwd_order_sku_detail"
T_LIFE = "lake.dwd.dwd_order_lifecycle"

# 三张目标表的列顺序（与 sql/lake/lake_dwd.sql 的 CREATE TABLE 严格一致）
COLS_DETAIL = ["order_id", "dt", "user_id", "product_id", "amount", "order_time", "status"]
COLS_SKU = ["order_id", "dt", "user_id", "product_id", "amount", "order_time", "status",
            "category", "brand", "sku_price", "valid_from", "valid_to"]
COLS_LIFE = ["order_id", "user_id", "product_id", "amount", "order_time", "pay_time",
             "ship_time", "finish_time", "cancel_time", "current_stage", "pay_lag_hours",
             "ship_lag_days", "finish_lag_days", "last_event_time", "update_time"]

EPOCH = "1970-01-01 00:00:00"

# 自检失败统一退出码（写作模式用于告诉 runner「数不对，别当成功」）
EXIT_SELF_CHECK_FAILED = 2

# lifecycle 增量按订单集合删时用的 temp view 名
SCOPE_VIEW = "_lake_dwd_scope_order_ids"


def fail(msg):
    """自检/关联校验不通过：打印明确错误，并以 exit 2 结束整个 spark-submit。"""
    text = "!!! SELF-CHECK FAILED: %s" % msg
    print(text)
    sys.stdout.flush()
    sys.stderr.write(text + "\n")
    sys.stderr.flush()
    sys.exit(EXIT_SELF_CHECK_FAILED)


def build_spark():
    return (SparkSession.builder
            .appName("lake_dwd_build")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse",
                    "file:///opt/offline-dw/lake/warehouse")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())


def read_orders(spark, d1=None, d2=None):
    df = spark.read.format("iceberg").load(SRC_ORDER)
    if d1:
        df = df.filter((F.col("dt") >= F.lit(d1)) & (F.col("dt") <= F.lit(d2)))
    return df


def read_events(spark, d1=None, d2=None):
    df = spark.read.format("iceberg").load(SRC_EVENT)
    if d1:
        df = df.filter((F.col("dt") >= F.lit(d1)) & (F.col("dt") <= F.lit(d2)))
    return df


# --------------------------------------------------------------------------
# 1) dwd_order_detail
#    仓侧清洗：user_id IS NOT NULL / ABS(amount) / 按 (dt, order_id) 取 ABS(amount) 最大的一行
#    ★ 与仓侧脚本一致：先按天去重（湖侧 ODS 已按天去重，这里保留 ROW_NUMBER 以防重跑叠数据）
# --------------------------------------------------------------------------
def build_detail(spark, d1=None, d2=None):
    src = read_orders(spark, d1, d2)
    w = Window.partitionBy("dt", "order_id").orderBy(F.col("amount").desc())
    df = (src
          .filter(F.col("user_id").isNotNull())
          .withColumn("amount", F.abs(F.col("amount")))
          .withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .select(*[F.col(c) for c in COLS_DETAIL]))
    return df


# --------------------------------------------------------------------------
# 2) dwd_order_sku_detail
#    仓侧口径：o.dt BETWEEN s.valid_from AND s.valid_to（SCD2 时点关联）
#    ★ 用 LEFT JOIN 而不是 INNER：inner 会把「当天找不到商品版本」的订单静默丢掉，
#      这类失配必须显式暴露（run_one 里统计 valid_from IS NULL，>0 则告警 + exit 2）。
#    维度侧从仓侧 dim_product_scd2 换成湖侧（湖仓主存迁湖后的必然）
# --------------------------------------------------------------------------
def build_sku(spark, d1=None, d2=None):
    fact = build_detail(spark, d1, d2)
    # ★ 维度侧给 product_id 起别名：两张表都有 product_id，join 后再 select 会报
    #   [AMBIGUOUS_REFERENCE] Reference `product_id` is ambiguous
    dim = (spark.read.format("iceberg").load(SRC_SCD2)
           .select(F.col("product_id").alias("_dim_pid"), "valid_from", "valid_to",
                   "category", "brand", F.col("price").alias("sku_price")))
    joined = (fact.join(F.broadcast(dim),
                        (fact.product_id == dim._dim_pid) &
                        (fact.dt >= dim.valid_from) & (fact.dt <= dim.valid_to),
                        "left")
              .select(*[F.col(c) for c in COLS_SKU]))
    return joined


# --------------------------------------------------------------------------
# 3) dwd_order_lifecycle
#    仓侧视图 dwd.v_order_lifecycle_expected 的逐字移植：
#    五个里程碑 MAX(CASE WHEN event_type=... THEN event_time END)
#    ★ GREATEST 必须用 COALESCE 兜底（任一里程碑 NULL 会让整列退化）
#
#    增量语义（scope）：
#      scope = 该区间【任一事件分区】里出现过的 order_id 的并集
#              —— 不只是「该区间新出现的订单」：老订单当天收到新里程碑事件也要重算
#      聚合必须【回全表、不按 dt 过滤】：order_time（event_type='order'）可能落在
#      比事件当天更早的分区，若只聚合 [d1,d2] 的事件，老订单的 order_time 会被抹成 NULL。
# --------------------------------------------------------------------------
def build_lifecycle(spark, d1=None, d2=None):
    # 全量事件（不带 dt 过滤），供 scope 订单做完整历史重聚合
    ev_all = read_events(spark)

    scope = None
    if d1:
        # ★ scope 用区间事件表的 order_id 并集（distinct 后是全量里那一小撮订单）
        scope = (read_events(spark, d1, d2)
                 .select("order_id").distinct()
                 .cache())
        n_scope = scope.count()
        print("  SCOPE dt in [%s, %s] 订单数 = %d" % (d1, d2, n_scope))
        # 先 join scope 再 groupBy：等价于「全表 groupBy 后 semi-join scope」，
        # 但只把 scope 订单的完整历史喂给聚合，既不漏老订单的早分区 order_time，
        # 也不多算无关订单。注意 join 用的是全表事件，没有 dt 过滤。
        ev_all = ev_all.join(scope, on="order_id", how="inner")

    agg = (ev_all.groupBy("order_id").agg(
        F.max("user_id").alias("user_id"),
        F.max("product_id").alias("product_id"),
        F.max("amount").alias("amount"),
        F.max(F.when(F.col("event_type") == "order", F.col("event_time"))).alias("order_time"),
        F.max(F.when(F.col("event_type") == "pay", F.col("event_time"))).alias("pay_time"),
        F.max(F.when(F.col("event_type") == "ship", F.col("event_time"))).alias("ship_time"),
        F.max(F.when(F.col("event_type") == "finish", F.col("event_time"))).alias("finish_time"),
        F.max(F.when(F.col("event_type") == "cancel", F.col("event_time"))).alias("cancel_time"),
    ))

    ts = F.lit(EPOCH).cast("timestamp")
    last_ev = F.greatest(
        F.coalesce(F.col("order_time"), ts), F.coalesce(F.col("pay_time"), ts),
        F.coalesce(F.col("ship_time"), ts), F.coalesce(F.col("finish_time"), ts),
        F.coalesce(F.col("cancel_time"), ts))

    df = (agg
          .withColumn("last_event_time", last_ev)
          .withColumn("current_stage", F.when(F.col("cancel_time") == F.col("last_event_time"), "cancel")
                      .when(F.col("finish_time") == F.col("last_event_time"), "finish")
                      .when(F.col("ship_time") == F.col("last_event_time"), "ship")
                      .when(F.col("pay_time") == F.col("last_event_time"), "pay")
                      .otherwise("order"))
          # TIMESTAMPDIFF(HOUR, order_time, pay_time)：Spark 无该函数。
          # ★ TIMESTAMP_NTZ 不能直接 cast("long")（报 Cannot resolve CAST(pay_time AS BIGINT)），
          #   必须走 unix_timestamp（秒）；整除 3600 与 TIMESTAMPDIFF(HOUR) 同义（向下取整）
          .withColumn("pay_lag_hours",
                      F.when(F.col("pay_time").isNull() | F.col("order_time").isNull(), F.lit(None).cast("int"))
                       .otherwise((F.unix_timestamp("pay_time") - F.unix_timestamp("order_time")) / 3600)
                       .cast("int"))
          .withColumn("ship_lag_days", F.datediff("ship_time", "order_time"))
          .withColumn("finish_lag_days", F.datediff("finish_time", "order_time"))
          .withColumn("update_time", F.current_timestamp())
          .select(*[F.col(c) for c in COLS_LIFE]))
    return df, scope


BUILDERS = {
    "dwd_order_detail": (build_detail, T_DETAIL),
    "dwd_order_sku_detail": (build_sku, T_SKU),
    "dwd_order_lifecycle": (build_lifecycle, T_LIFE),
}

ORDER_OF_EXEC = ["dwd_order_detail", "dwd_order_sku_detail", "dwd_order_lifecycle"]

# 每张目标表对应的「期望行数」来源（按天闭合检查用）：
#   op="notnull" -> 当天 ODS 里该列非空的行数（detail：user_id IS NOT NULL）
#   op="distinct"-> 当天 ODS 里该列的去重数（lifecycle：事件表当天 order_id 去重数）
CLOSURE_SPEC = {
    "dwd_order_detail":     dict(src=SRC_ORDER, col="user_id",  op="notnull"),
    "dwd_order_sku_detail": dict(src=SRC_ORDER, col="user_id",  op="notnull"),
    "dwd_order_lifecycle":  dict(src=SRC_EVENT, col="order_id", op="distinct"),
}


def _d(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


def _s(x):
    return x.strftime("%Y-%m-%d")


def built_boundary(spark, name):
    """目标表自己的水位：已经算到哪一天了。

    lifecycle 没有 dt 列，用事件 ODS 的 MAX(dt) —— 代价是可能重算一天，但幂等（先 DELETE 再 append）。
    表不存在 / 为空 -> None（第一次跑，交由 resolve_range 退化到 ODS 的 MIN(dt)）。
    """
    try:
        if name == "dwd_order_lifecycle":
            val = spark.sql("SELECT max(dt) FROM %s" % SRC_EVENT).collect()[0][0]
        else:
            val = spark.sql("SELECT max(dt) FROM %s" % BUILDERS[name][1]).collect()[0][0]
    except Exception as exc:                                    # noqa: BLE001
        print("  [水位] %s 读不到 MAX(dt)（%s），按空表处理" % (name, type(exc).__name__))
        return None
    return _s(val) if val else None


def resolve_range(spark, name, incremental, explicit_start, explicit_end, lookback):
    """增量模式下的区间计算；返回 (d1, d2)。

    · rebuild / 显式给了区间 -> 原样返回
    · 空表                   -> [ods_min, ods_max]（等价于"把已有 ODS 全建一遍"）
    · 有水位                 -> [水位+1, ods_max]
    · --lookback N           -> d1 再往前 N 天
    """
    if not incremental or explicit_start:
        return explicit_start, explicit_end
    ods_min, ods_max = (spark.sql("SELECT min(dt), max(dt) FROM %s" % SRC_ORDER)
                        .collect()[0])
    if ods_max is None:
        return None, None
    ods_min, ods_max = _s(ods_min), _s(ods_max)

    b = built_boundary(spark, name)
    if b is None:
        d1 = ods_min
        print("  [区间] %s 目标表为空 -> 首次建表区间 [%s, %s]" % (name, d1, ods_max))
    else:
        d1 = _s(_d(b) + datetime.timedelta(days=1))
        print("  [区间] %s 水位=%s -> 从 %s 起（上限取 ODS 完整日 %s）" % (name, b, d1, ods_max))

    if lookback and lookback > 0:
        back = _s(max(_d(d1) - datetime.timedelta(days=lookback), _d(ods_min)))
        if back != d1:
            print("  [区间] --lookback %d 回捞 -> d1 从 %s 改为 %s" % (lookback, d1, back))
            d1 = back

    if _d(d1) > _d(ods_max):
        print("  [区间] %s 水位(%s) 已到 ODS 最新完整日(%s) -> NOTHING TO DO" % (name, b, ods_max))
        return None, None
    return d1, ods_max


def day_closure_check(spark, name, d1, d2, strict):
    """按天闭合检查：DWD 每天行数 vs 由 ODS 现算的期望行数。只在增量模式下跑。"""
    spec = CLOSURE_SPEC[name]
    if spec["op"] == "notnull":
        exp = (spark.sql("SELECT dt, sum(CASE WHEN %s IS NOT NULL THEN 1 ELSE 0 END) n "
                         "FROM %s WHERE dt >= '%s' AND dt <= '%s' GROUP BY dt"
                         % (spec["col"], spec["src"], d1, d2))
               .collect())
    else:
        exp = (spark.sql("SELECT dt, count(DISTINCT %s) n FROM %s "
                         "WHERE dt >= '%s' AND dt <= '%s' GROUP BY dt"
                         % (spec["col"], spec["src"], d1, d2))
               .collect())
    exp_map = {_s(r["dt"]): int(r["n"]) for r in exp}

    if name == "dwd_order_lifecycle":
        got = (spark.sql("SELECT e.dt dt, count(DISTINCT d.order_id) n "
                         "FROM %s e JOIN %s d ON d.order_id = e.order_id "
                         "WHERE e.dt >= '%s' AND e.dt <= '%s' GROUP BY e.dt"
                         % (spec["src"], BUILDERS[name][1], d1, d2))
               .collect())
    else:
        got = (spark.sql("SELECT dt, count(*) n FROM %s "
                         "WHERE dt >= '%s' AND dt <= '%s' GROUP BY dt"
                         % (BUILDERS[name][1], d1, d2))
               .collect())
    got_map = {_s(r["dt"]): int(r["n"]) for r in got}

    bad = []
    for day in sorted(set(exp_map) | set(got_map)):
        e, g = exp_map.get(day, 0), got_map.get(day, 0)
        if e != g:
            bad.append("%s DWD=%d 期望=%d 差=%+d" % (day, g, e, g - e))
    print("  CLOSURE %s 检查 %d 天 / 不一致 %d 天" % (name, len(exp_map), len(bad)))
    if bad:
        for line in bad[:10]:
            print("    !! %s" % line)
        if len(bad) > 10:
            print("    ...（共 %d 天，只列前 10）" % len(bad))
        msg = ("[%s] 按天闭合检查 %d 天不一致：DWD 与 ODS 现算值不符。"
               "常见原因：该天 ODS 分区刚被重放但 DWD 还没重算；"
               "修法 `--mode incremental --lookback N` 或显式 `--start/--end` 重算那几天"
               % (name, len(bad)))
        if strict:
            fail(msg)
        print("  !! WARN %s（默认不中断；--strict-closure 会 exit %d）"
              % (msg, EXIT_SELF_CHECK_FAILED))
    else:
        print("  CLOSURE %s OK" % name)
    return len(bad)



def table_stats(df):
    """行数 / 金额合计 / 去重主键数（自检口径）。"""
    r = (df.agg(
            F.count(F.lit(1)).alias("n"),
            F.coalesce(F.sum("amount"), F.lit(0).cast("decimal(38,2)")).alias("s"),
            F.countDistinct("order_id").alias("k"))
         .collect()[0])
    return int(r["n"]), r["s"], int(r["k"])


def self_check(spark, name, tgt, df, d1, d2, scope):
    """写入后回读落库数据，与 Spark 侧期望比对；不一致 -> exit 2。"""
    exp_n, exp_s, exp_k = table_stats(df)

    back = spark.read.format("iceberg").load(tgt)
    if name == "dwd_order_lifecycle":
        # lifecycle 本次只重写了 scope 内的订单 -> 只回读这部分（其余行不该被动过）
        if scope is not None:
            back = back.join(scope, on="order_id", how="inner")
    elif d1:
        # detail / sku 本次只重写了 dt 区间 -> 只回读该区间
        back = back.filter((F.col("dt") >= F.lit(d1)) & (F.col("dt") <= F.lit(d2)))

    got_n, got_s, got_k = table_stats(back)
    print("  SELF-CHECK %s 期望(n=%d, sum=%s, key=%d) 回读(n=%d, sum=%s, key=%d)"
          % (name, exp_n, exp_s, exp_k, got_n, got_s, got_k))

    bad = []
    if got_n != exp_n:
        bad.append("行数 回读=%d 期望=%d" % (got_n, exp_n))
    if got_k != exp_k:
        bad.append("去重主键数 回读=%d 期望=%d" % (got_k, exp_k))
    if got_s != exp_s:
        bad.append("金额合计 回读=%s 期望=%s" % (got_s, exp_s))
    if bad:
        fail("[%s] 写入后自检不一致：%s" % (name, "；".join(bad)))
    print("  SELF-CHECK %s OK" % name)


def run_one(spark, name, mode, d1, d2, incremental=False, lookback=0, strict_closure=False):
    fn, tgt = BUILDERS[name]

    # ★ 增量：区间自己算（水位 = 目标表自己的 MAX(dt)）；算不出新数据就整块跳过
    d1, d2 = resolve_range(spark, name, incremental, d1, d2, lookback)
    skipped = (d1 is None and d2 is None)
    if skipped:
        print("=" * 70)
        print("BUILD %s -> %s mode=%s SKIP（没有新数据）" % (name, tgt, mode))
        return 0

    print("=" * 70)
    print("BUILD %s -> %s mode=%s range=%s..%s" % (name, tgt, mode, d1, d2))
    before = spark.sql("SELECT count(*) FROM %s" % tgt).collect()[0][0]
    print("  当前行数 = %d" % before)

    built = fn(spark, d1, d2)
    if name == "dwd_order_lifecycle":
        df, scope = built
    else:
        df, scope = built, None
    df = df.cache()
    n = df.count()
    print("  算出 %d 行" % n)

    # ★ SKU 失配告警：left join 后 valid_from IS NULL = 当天没有命中的商品版本。
    #   以前 inner join 会把这批订单静默丢掉，现在必须报出来；写入模式直接 exit 2。
    if name == "dwd_order_sku_detail":
        n_miss = df.filter(F.col("valid_from").isNull()).count()
        print("  SKU 失配检查：left join 后 valid_from IS NULL = %d / %d 行" % (n_miss, n))
        if n_miss > 0:
            msg = ("SKU SCD2 关联失配 %d / %d 行（valid_from IS NULL）：这些订单在 dt "
                   "当天没有有效商品版本，left join 已保留但维度字段为 NULL" % (n_miss, n))
            print("  !! WARN %s" % msg)
            if mode != "dry":
                fail(msg)
            print("  [dry-run] 仅告警不中断（写入模式会 exit %d）" % EXIT_SELF_CHECK_FAILED)

    if n == 0:
        print("  !! 0 行，跳过（不做任何 DELETE）")
        return 0

    if mode == "dry":
        print("  [dry-run] 不写")
        df.show(3, truncate=False)
        return n

    # ---- 幂等清理：只删「本次要写的范围」，再 append ----
    if not d1:
        spark.sql("DELETE FROM %s" % tgt)
        print("  全量模式：已 DELETE 整表")
    elif name == "dwd_order_lifecycle":
        # lifecycle 没有 dt 列 -> 按订单集合删；scope 先落 temp view 才能进 SQL 子查询
        scope.createOrReplaceTempView(SCOPE_VIEW)
        spark.sql("DELETE FROM %s WHERE order_id IN (SELECT order_id FROM %s)" % (tgt, SCOPE_VIEW))
        print("  lifecycle 增量：已按 scope 订单集合 DELETE（temp view=%s）" % SCOPE_VIEW)
    else:
        spark.sql("DELETE FROM %s WHERE dt >= '%s' AND dt <= '%s'" % (tgt, d1, d2))
        print("  已 DELETE dt in [%s, %s] 的旧行" % (d1, d2))

    df.writeTo(tgt).append()

    # ---- 写入后自检 ----
    self_check(spark, name, tgt, df, d1, d2, scope)

    # ---- 按天闭合检查（只有增量/区间模式才有意义；全量模式自检已覆盖） ----
    if d1:
        day_closure_check(spark, name, d1, d2, strict_closure)

    after = spark.sql("SELECT count(*) FROM %s" % tgt).collect()[0][0]
    print("  RESULT %s before=%d written=%d after=%d" % (name, before, n, after))
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, choices=sorted(BUILDERS) + ["all"])
    ap.add_argument("--mode", default="rebuild",
                    choices=["rebuild", "append", "dry", "incremental"])
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    # ★ --lookback N：增量的 d1 再往前 N 天，用来回捞迟到事件（重复计算幂等，代价很小）
    ap.add_argument("--lookback", type=int, default=0)
    # ★ --strict-closure：按天闭合检查不一致时直接 exit 2（默认只 WARN）
    ap.add_argument("--strict-closure", action="store_true")
    args = ap.parse_args()

    # incremental 语义 = "区间自己算"，落盘清理动作与 append 相同（写前 DELETE 本次范围）
    incremental = (args.mode == "incremental")
    mode = "append" if incremental else args.mode

    d1, d2 = args.start, (args.end or args.start)
    print("MODE=%s%s" % (args.mode, "  lookback=%d" % args.lookback if args.lookback else ""))
    spark = build_spark()
    total = 0
    tables = ORDER_OF_EXEC if args.table == "all" else [args.table]
    for t in tables:
        total += run_one(spark, t, mode, d1, d2,
                         incremental=incremental, lookback=args.lookback,
                         strict_closure=args.strict_closure)
        print("")
    print("==========")
    print("DWD_TOTAL = %d" % total)
    spark.stop()
    print("EXIT=0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
