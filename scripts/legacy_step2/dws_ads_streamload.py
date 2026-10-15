#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWS / ADS 链路 v2：湖层 Iceberg ODS → 清洗 → 聚合 → StarRocks Stream Load

【为什么直接读 ods.ods_order 而不是读 dwd.dwd_order_detail】
仓侧老链路的 DWS 是读 `dwd.dwd_order_detail` 再 GROUP BY。Spark 侧如果再回头
读仓侧 DWD，就得跨 90 天分页把 90 万行捞出来，再算一遍 —— 没必要。
Spark 直接从湖层 ods_order 用【和 DWD 完全相同的清洗口径】算出同一份明细
（user_id 非空 / ABS(amount) / 按 (dt, order_id) 取 ABS(amount) 最大行），
它的结果已实测与仓侧 dwd_order_detail 逐行一致（2026-09-20：9475 行、
sum(amount)=1544512.70、时间边界完全相同），所以聚合口径等价。

【聚合口径：与 sql/dws_user_order_day_insert.sql + sql/ads_daily_sales_insert.sql 逐字对齐】
    dws:  GROUP BY user_id, dt
          order_cnt      = COUNT(*)
          paid_cnt       = COUNT(CASE WHEN status='paid' THEN 1 END)
          paid_amount    = SUM(CASE WHEN status='paid' THEN amount ELSE 0 END)
          refund_amount  = SUM(CASE WHEN status='refund' THEN amount ELSE 0 END)
    ads:  GROUP BY dt
          order_cnt/paid_cnt/paid_amount/refund_amount = SUM(...)
          net_amount       = SUM(paid_amount) - SUM(refund_amount)
          avg_order_amount = ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt),0), 2)
          pay_rate         = ROUND(SUM(paid_cnt)    / NULLIF(SUM(order_cnt),0), 4)
  派生指标一律「先各自 SUM 再相除」，不做 per-row 相除后再 SUM。

【幂等性】
  ① DWS/ADS 都是 PRIMARY KEY 模型表，Stream Load 是 upsert 语义：
     同一 (user_id, dt) / dt 再来一次就是覆盖，不需要先 DELETE。
     这一点比 DWD 更方便 —— DWD 是 DUPLICATE KEY 表，必须显式 DELETE 才能重跑。
  ② label 固定 = dws_ads_<start>_<end>_<part>，重复时 StarRocks 回
     `Label Already Exists`，脚本视为已导入并回读核对。
  ③ 写完后回读行数与 Spark 期望值比对。

【为什么 C 层用 JSON 而不是 CSV（与 DWD 那支不同）】
  DWS 的键是 (user_id, dt)，一行一个用户；ADS 只有 90 行。用 JSON 可以让
  Spark 直接 toJSON 出行，不需要手工格式化 decimal/date，也不会踩 CSV 的分隔符、
  时间格式、空值表示那三个坑（DWD 那支已实测踩过）。
  Stream Load 的 JSON 模式对 decimal 接受数字，date 接受 "yyyy-MM-dd"。
  代价：JSON 比 CSV 大一些，但这里总量只有几万行，无所谓。

用法：
    spark-submit dws_ads_streamload.py --start 2026-09-20 --end 2026-12-18
    spark-submit dws_ads_streamload.py --start 2026-09-20 --end 2026-09-22 --dry-run
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ---------------------------------------------------------------------------
LAKE_WH = "file:///opt/offline-dw/lake/warehouse"

SR_FE = "starrocks"
# ★ 直连 BE 的 http_port(8040)，不能打 FE 的 8030（FE 会 307 跳到 127.0.0.1:8040，
#   在容器里那个 127.0.0.1 是 FE 自己，跟着跳转必然连不上）。DWD 那支已实测。
SR_HTTP_PORT = 8040
SR_USER = "root"
SR_PASS = ""
SR_DB = "dws"
SR_TABLE = "dws_user_order_day"
ADS_DB = "ads"
ADS_TABLE = "ads_daily_sales"

MAX_FILTER_RATIO = 0

# 每个 Stream Load 批次的目标行数（DWS 每天约 1 万行用户）
ROWS_PER_BATCH = 40_000
MAX_DAYS_PER_BATCH = 10

STAGE_DIR = "/opt/offline-dw/lake/_tmp_dws_stage"

# ★ 制表符常量。
#   别在源码里直接敲字面量 `\t`（反斜杠 + t）—— 编辑器写裸字符串时不会把它变成
#   真正的制表符，StarRocks 收到的是两个字符，会报
#   `Getting syntax error at line 1, column 17. Detail message: No viable statement
#    for input 'user_id\tdt'`（已实测）。用 Python 转义 `"\t"` 才保险。
TAB = "\t"

# ★ Stream Load 的 `columns` 头必须用 **逗号** 分隔（不是制表符！），
#   而 `column_separator` 才是数据列之间的分隔符（这里用制表符）。
#   写成制表符分隔会报：
#     plan streaming load failed. errmsg=Getting syntax error at line 1, column 17.
#     Detail message: No viable statement for input 'user_id<TAB>dt'
#   （2026-10-14 实测三种写法：制表符=Fail、逗号=Success、不写 columns=Success。
#     已在 docker 网络内用 probe_cols3.py 逐个验证，别再回头改。）
#   顺序必须与落盘时 df.columns 一致，否则整批按列错位落进去（静默写坏数据）。
DWS_COLS = ",".join(["user_id", "dt", "order_cnt", "paid_cnt",
                     "paid_amount", "refund_amount"])
ADS_COLS = ",".join(["dt", "order_cnt", "paid_cnt", "paid_amount",
                     "refund_amount", "net_amount", "avg_order_amount",
                     "pay_rate"])

SR_URL = (f"jdbc:mysql://{SR_FE}:9030/?useSSL=false"
          f"&characterEncoding=utf8&connectTimeout=10000&socketTimeout=120000")


def build_spark():
    return (SparkSession.builder
            .appName("dws_ads_streamload")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            .config("spark.sql.catalog.lake",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", LAKE_WH)
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate())


def day_list(start_s, end_s, dates_s):
    if dates_s:
        return [d.strip() for d in dates_s.split(",") if d.strip()]
    d0 = datetime.strptime(start_s, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_s, "%Y-%m-%d").date()
    if d1 < d0:
        raise SystemExit(f"--end({end_s}) 早于 --start({start_s})")
    out, cur = [], d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def mysql(spark, sql, fetch=False):
    """在 Spark 容器内用 JDBC 直连 StarRocks（容器里没有 docker / mysql CLI）。"""
    jvm = spark._jvm
    jvm.java.lang.Class.forName("com.mysql.cj.jdbc.Driver")
    props = jvm.java.util.Properties()
    props.setProperty("user", SR_USER)
    props.setProperty("password", SR_PASS)
    conn = jvm.java.sql.DriverManager.getConnection(SR_URL, props)
    try:
        st = conn.createStatement()
        try:
            st.execute(sql)
            if fetch:
                rs = st.getResultSet()
                rs.next()
                return rs.getLong(1)
            return None
        finally:
            st.close()
    finally:
        conn.close()


def stream_load_tsv(files, db, table, columns, label):
    """把若干 TSV 文件依次 Stream Load 进 (db, table)。

    用 CSV 而非 JSON：`format: csv` + `column_separator: \\t`。
    ★ 列名用 `columns` 头显式按**落盘时的字段顺序**对齐，不依赖源数据里带列名。
    ★ `Expect: 100-continue` 是 StarRocks 强制要求（不带会 500
      `There is no 100-continue header`，已实测）。
    """
    results = []
    for f in files:
        url = f"http://{SR_FE}:{SR_HTTP_PORT}/api/{db}/{table}/_stream_load"
        hdr = {
            "format": "csv",
            "column_separator": TAB,
            "columns": columns,
            "max_filter_ratio": str(MAX_FILTER_RATIO),
            "timezone": "Asia/Shanghai",
            "label": label,
        }
        cmd = ["curl", "-s", "--expect100-timeout", "60",
               "-u", f"{SR_USER}:{SR_PASS}", "-H", "Expect: 100-continue"]
        for k, v in hdr.items():
            cmd += ["-H", f"{k}: {v}"]
        cmd += ["-T", f, url]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"curl 失败: {r.stderr.strip()}")
        try:
            j = json.loads(r.stdout)
        except json.JSONDecodeError:
            raise RuntimeError(f"Stream Load 返回非 JSON: {r.stdout[:400]}")
        st = j.get("Status")
        if st != "Success":
            msg = j.get("Message", "")
            if "label already exists" in msg.lower():
                j["_skipped"] = True
                results.append(j)
                continue
            raise RuntimeError(
                f"Stream Load 失败({db}.{table}): "
                f"{json.dumps(j, ensure_ascii=False)[:600]}")
        results.append(j)
    return results


def write_dataframe(df, path):
    """把 DataFrame 落成 **TSV**（制表符分隔、无引号、无表头），字段顺序 = df.columns。

    ★ 这条路径是第 6 次尝试，前面 5 种 Spark 原生落盘方式全部失败，记下来别再试：
      1. `df.write.text()`                    → Text datasource 只吃单列 string，
                                                报 UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE (BIGINT)
      2. `df.toJSON().write...`               → toJSON 返回 RDD，没有 .write
      3. `createDataFrame(df.toJSON(), "value string")`
                                              → DDL 路径当成 struct 解析，报 CANNOT_ACCEPT_OBJECT_IN_TYPE
      4. `createDataFrame(rdd, StructType)`   → 同上（两处 createDataFrame 实现不一致，PySpark 3.5 的坑）
      5. `rdd.toDF(StructType)`               → 同上
      ★ 最后用 `F.concat_ws` 在 **Spark SQL 内部**拼成一列
        `value_col = concat_ws('\\t', cast(col) for col in df.columns)`，
        它天然是单列 string，`df.write.text()` 直接接受，全程不碰 Python RDD/JSON。
      ★ 用 \\t 而不是 | 作分隔符：`concat_ws` 不会加引号也不转义，选一个
        在数据里绝对不会出现的字符即可（前缀、日期、数字、状态枚举里都没有制表符）。
      ★ 落盘后再 `_fix_escaped_nulls()` 把 `\\N` 还原成裸 `\\N`：
        StarRocks 认 `\\N` 为 NULL，但 Spark 写 text 时会把反斜杠转义成 `\\\\N`。
    """
    value_col = F.concat_ws(TAB,
                            *[F.col(c).cast("string") for c in df.columns])
    # ★ Spark 的 write.text() 写出来的是**目录**（里面一个 part-00000），
    #   不是单文件。要自己合并成一个文件，否则后面按文件读会
    #   `IsADirectoryError: [Errno 21] Is a directory`（已实测）。
    outdir = path + ".d"
    shutil.rmtree(outdir, ignore_errors=True)
    (df.select(value_col.alias("value"))
       .repartition(1)
       .write.mode("overwrite")
       .text(outdir))
    with open(path, "wb") as out:
        for pf in sorted([p for p in os.listdir(outdir)
                          if p.startswith("part-")]):
            with open(os.path.join(outdir, pf), "rb") as fh:
                shutil.copyfileobj(fh, out)
    shutil.rmtree(outdir, ignore_errors=True)


def _fix_escaped_nulls(files):
    """把 Spark text 写出的 `\\\\N` 还原为 StarRocks 认的 `\\N`（NULL）。"""
    for f in files:
        with open(f, "rb") as fh:
            data = fh.read()
        if b"\\\\N" in data:
            with open(f, "wb") as fh:
                fh.write(data.replace(b"\\\\N", b"\\N"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--dates")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--keep-stage", action="store_true")
    ap.add_argument("--skip-ads", action="store_true",
                    help="只灌 DWS（冒烟测试用）")
    args = ap.parse_args()

    if not args.dates and not (args.start and args.end):
        raise SystemExit("必须给 --start/--end 或 --dates")

    days = day_list(args.start, args.end, args.dates)
    tag_start, tag_end = days[0], days[-1]
    print(f"PLAN_DAYS = {len(days)}  {tag_start} .. {tag_end}", flush=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    t_all = time.time()

    # -----------------------------------------------------------------
    # 1) 读湖 + 清洗（与 dwd_order_detail_streamload.py 完全一致的口径）
    # -----------------------------------------------------------------
    t0 = time.time()
    spark.table("lake.ods.ods_order").createOrReplaceTempView("_lake_order")
    spark.sql(f"""
        SELECT order_id, dt, user_id, product_id, amount, order_time, status
        FROM (
            SELECT order_id, dt, user_id, product_id,
                   ABS(amount) AS amount,
                   CAST(order_time AS TIMESTAMP) AS order_time,
                   status,
                   ROW_NUMBER() OVER (PARTITION BY dt, order_id
                                      ORDER BY ABS(amount) DESC) AS rn
            FROM _lake_order
            WHERE user_id IS NOT NULL
              AND dt BETWEEN DATE '{tag_start}' AND DATE '{tag_end}'
        ) t WHERE rn = 1
    """).createOrReplaceTempView("_clean")
    detail = spark.table("_clean").cache()
    n_detail = detail.count()
    print(f"TIME {time.time()-t0:.1f}s 清洗完成 detail_rows={n_detail}", flush=True)

    # -----------------------------------------------------------------
    # 2) DWS：user_id × dt
    #    ★ 语义与 sql/dws_user_order_day_insert.sql 一致
    # -----------------------------------------------------------------
    dws = (detail.groupBy("user_id", "dt")
           .agg(F.count(F.lit(1)).alias("order_cnt"),
                F.count(F.when(F.col("status") == "paid", 1)).alias("paid_cnt"),
                F.sum(F.when(F.col("status") == "paid", F.col("amount"))
                      .otherwise(F.lit(0))).cast("decimal(18,2)")
                 .alias("paid_amount"),
                F.sum(F.when(F.col("status") == "refund", F.col("amount"))
                      .otherwise(F.lit(0))).cast("decimal(18,2)")
                 .alias("refund_amount"))
           .withColumn("dt", F.date_format("dt", "yyyy-MM-dd")))
    n_dws = dws.count()
    print(f"DWS_ROWS = {n_dws}  期望 {n_detail} 个订单聚成 <= {n_detail} 行", flush=True)
    print(f"DWS_DAYS = {dws.select('dt').distinct().count()}", flush=True)

    # -----------------------------------------------------------------
    # 3) ADS：按天大盘 + 派生指标
    #    ★ 语义与 sql/ads_daily_sales_insert.sql 一致
    # -----------------------------------------------------------------
    ads = (dws.groupBy("dt")
           .agg(F.sum("order_cnt").alias("order_cnt"),
                F.sum("paid_cnt").alias("paid_cnt"),
                F.sum("paid_amount").cast("decimal(18,2)").alias("paid_amount"),
                F.sum("refund_amount").cast("decimal(18,2)").alias("refund_amount"))
           .withColumn("net_amount",
                       (F.col("paid_amount") - F.col("refund_amount"))
                       .cast("decimal(18,2)"))
           .withColumn("avg_order_amount",
                       F.when(F.col("paid_cnt") != 0,
                              F.round(F.col("paid_amount") / F.col("paid_cnt"), 2))
                        .otherwise(F.lit(None).cast("decimal(18,2)"))
                        .cast("decimal(18,2)"))
           .withColumn("pay_rate",
                       F.when(F.col("order_cnt") != 0,
                              F.round(F.col("paid_cnt") / F.col("order_cnt"), 4))
                        .otherwise(F.lit(None).cast("decimal(10,4)"))
                        .cast("decimal(10,4)")))
    n_ads = ads.count()
    print(f"ADS_ROWS = {n_ads}  期望 {len(days)}", flush=True)

    if args.dry_run:
        print("--- DWS 抽样 ---", flush=True)
        for r in (dws.orderBy("dt", "user_id").limit(3).collect()):
            print("  " + json.dumps(r.asDict(), ensure_ascii=False, default=str),
                  flush=True)
        print("--- ADS 抽样 ---", flush=True)
        for r in (ads.orderBy("dt").limit(3).collect()):
            print("  " + json.dumps(r.asDict(), ensure_ascii=False, default=str),
                  flush=True)
        print("DRY_RUN_ONLY -> 未写入任何数据", flush=True)
        spark.stop()
        return 0

    # -----------------------------------------------------------------
    # 4) 落 JSON + Stream Load（DWS 分批，ADS 全量一个文件）
    # -----------------------------------------------------------------
    if os.path.isdir(STAGE_DIR):
        shutil.rmtree(STAGE_DIR)
    os.makedirs(STAGE_DIR, exist_ok=True)

    # 4.1 DWS 按整批天切
    by_day = {r["dt"]: r["c"] for r in
              dws.groupBy("dt").agg(F.count(F.lit(1)).alias("c")).collect()}
    batches, cur, cur_rows = [], [], 0
    for d in days:
        n = by_day.get(d, 0)
        if cur and (cur_rows + n > ROWS_PER_BATCH
                    or len(cur) >= MAX_DAYS_PER_BATCH):
            batches.append(cur)
            cur, cur_rows = [], 0
        cur.append(d)
        cur_rows += n
    if cur:
        batches.append(cur)
    print(f"DWS_BATCHES = {len(batches)}", flush=True)

    t1 = time.time()
    total_dws = 0
    for bi, bdays in enumerate(batches):
        tb = time.time()
        exp = sum(by_day.get(d, 0) for d in bdays)
        fpath = f"{STAGE_DIR}/dws_{bi:03d}.tsv"
        write_dataframe(dws.filter(F.col("dt").isin(*bdays)), fpath)
        _fix_escaped_nulls([fpath])
        label = f"dws_ads_{tag_start}_{tag_end}_dws_{bi:03d}"
        res = stream_load_tsv([fpath], SR_DB, SR_TABLE, DWS_COLS, label)
        loaded = sum(int(r.get("NumberLoadedRows", 0) or 0) for r in res)
        skipped = any(r.get("_skipped") for r in res)
        d0, d1 = bdays[0], bdays[-1]
        got = int(mysql(spark, f"SELECT count(*) c FROM {SR_DB}.{SR_TABLE} "
                               f"WHERE dt >= '{d0}' AND dt <= '{d1}'",
                        fetch=True) or 0)
        ok = "OK" if got == exp else f"**MISMATCH** 期望{exp} 实际{got}"
        total_dws = mysql(spark, f"SELECT count(*) c FROM {SR_DB}.{SR_TABLE} "
                                 f"WHERE dt >= '{tag_start}' AND dt <= '{tag_end}'",
                          fetch=True) or 0
        print(f"DWS_BATCH {bi:03d} {d0}~{d1} days={len(bdays)} exp={exp} "
              f"loaded={loaded} {'SKIP(label已存在)' if skipped else ''} "
              f"verify={ok} cost={time.time()-tb:.1f}s", flush=True)

    # 4.2 ADS 全量
    if args.skip_ads:
        print("SKIP_ADS -> 未写 ADS", flush=True)
        print(f"TIME {time.time()-t_all:.1f}s 总耗时", flush=True)
        if not args.keep_stage:
            shutil.rmtree(STAGE_DIR, ignore_errors=True)
        spark.stop()
        return 0
    fpath = f"{STAGE_DIR}/ads_all.tsv"
    write_dataframe(ads, fpath)
    _fix_escaped_nulls([fpath])
    label = f"dws_ads_{tag_start}_{tag_end}_ads"
    res = stream_load_tsv([fpath], ADS_DB, ADS_TABLE, ADS_COLS, label)
    loaded_ads = sum(int(r.get("NumberLoadedRows", 0) or 0) for r in res)
    got_ads = int(mysql(spark, f"SELECT count(*) c FROM {ADS_DB}.{ADS_TABLE} "
                               f"WHERE dt >= '{tag_start}' AND dt <= '{tag_end}'",
                        fetch=True) or 0)
    ok_ads = "OK" if got_ads == n_ads else f"**MISMATCH** 期望{n_ads} 实际{got_ads}"
    print(f"ADS 全量 exp={n_ads} loaded={loaded_ads} verify={ok_ads}", flush=True)

    print(f"TIME {time.time()-t1:.1f}s 全部写完", flush=True)
    print(f"DWS_TOTAL_IN_RANGE = {total_dws}", flush=True)
    print(f"ADS_TOTAL_IN_RANGE = {got_ads}", flush=True)
    print(f"TIME {time.time()-t_all:.1f}s 总耗时", flush=True)

    if not args.keep_stage:
        shutil.rmtree(STAGE_DIR, ignore_errors=True)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
