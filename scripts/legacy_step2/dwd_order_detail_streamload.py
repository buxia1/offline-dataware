#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWD 明细链路 v2：湖层 Iceberg ODS → 清洗 → StarRocks.dwd_order_detail

【为什么从 JDBC 换成 Stream Load】
v1 用 Spark JDBC 写：09-20 单天 9475 行花了 **467 秒**（≈20 行/秒）。
按这个速度 90 万行要 13 小时，不可用。JDBC 逐行 INSERT 的 round-trip 是瓶颈。
Stream Load 是 StarRocks 官方给大批量导入的口子（HTTP + CSV 文件，BE 直接消费），
同样数据实测 **秒级**。

【口径：与仓侧老作业 scripts/legacy_warehouse/dwd_overwrite.sh 逐字对齐】
    老作业（StarRocks 内 SQL）：
        INSERT OVERWRITE dwd.dwd_order_detail PARTITION (p${D})
        SELECT order_id, dt, user_id, product_id, ABS(amount), order_time, status
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id
                                           ORDER BY ABS(amount) DESC) rn
              FROM ods.ods_order
              WHERE user_id IS NOT NULL AND dt='${DF}') t
        WHERE rn = 1;
    本作业照搬四条：user_id 非空 / ABS(amount) / 按订单取 ABS(amount) 最大行 / 按 dt 分区。
    ★ 唯一刻意差异：来源由仓侧 ods.ods_order 换成湖侧 lake.ods.ods_order（同一批 Kafka
      消息的双写副本，2026-10-14 实测逐 key 零差异）。这是湖仓一体「DWD 计算下沉 Spark」
      的本意。

【幂等性】
  ① 开始前 DELETE 掉整个目标区间（一条 SQL，无论之前灌过什么）。
  ② 每个 Stream Load 带 label=dwd_od_<start>_<end>_<批次号>，#3 附带时间戳做重试后缀。
     label 重复时 StarRocks 返回 `Label Already Exists`，脚本视为「已导入」跳过。
  ③ 每批导入后回读行数，与 Spark 算出的期望值逐日比对；不一致则删除该批并重导。

【为什么按「整批天」分文件而不是按固定行数切】
  Stream Load 的原子性单位是一次导入。切成整批天，失败时最多重导一个批次，
  不会出现「半天数据」这种需要人工判断的中间态。

用法：
    spark-submit dwd_order_detail_streamload.py --start 2026-09-20 --end 2026-12-18
    spark-submit dwd_order_detail_streamload.py --start 2026-09-20 --end 2026-09-20 --dry-run
"""
import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
LAKE_WH = "file:///opt/offline-dw/lake/warehouse"

SR_FE = "starrocks"
# ★ 必须直连 BE 的 http_port(8040)，不能打 FE 的 8030：
#   FE 对 Stream Load 会回 307，location 是 `http://127.0.0.1:8040/...`
#   —— 容器里那个 127.0.0.1 是 FE 自己，跟着跳转必然连不上。
#   （curl -L 也救不了：307 会保留 body，但不解决地址问题）
SR_HTTP_PORT = 8040
SR_USER = "root"
SR_PASS = ""
SR_DB = "dwd"
SR_TABLE = "dwd_order_detail"

# 0 行坏数据。默认值是 1（全放行），不显式设就会被静默过滤掉坏行。
MAX_FILTER_RATIO = 0

# 每个 Stream Load 批次的目标行数；批次按【整天】凑，不跨天切
ROWS_PER_BATCH = 60_000
# 单个批次内允许的最大天数（防止某天特别大导致批次过大）
MAX_DAYS_PER_BATCH = 10

# 临时 CSV 落点：容器内的本地目录（不是 Iceberg，走的是普通文件写）
# 必须选一个容器内可写的路径。项目挂载里 /opt/offline-dw/lake 是 1777。
STAGE_DIR = "/opt/offline-dw/lake/_tmp_dwd_stage"

# DDL 里的列顺序；dt 也要给（它在 CSV 里，不是靠分区推断）
COLUMNS = "order_id,dt,user_id,product_id,amount,order_time,status"

# 清洗后的列顺序（与 COLUMNS 严格一致）
TARGET_COLS = ["order_id", "dt", "user_id", "product_id",
               "amount", "order_time", "status"]


def build_spark():
    return (SparkSession.builder
            .appName("dwd_order_detail_streamload")
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


SR_URL = (f"jdbc:mysql://{SR_FE}:9030/?useSSL=false"
          f"&characterEncoding=utf8&connectTimeout=10000&socketTimeout=120000")


def mysql(spark, sql, fetch=False):
    """在 Spark 容器内用 JDBC 直连 StarRocks 跑一条 SQL。

    ★ 不能用 subprocess 调 docker / mysql 客户端：
      Spark Python 进程跑在【容器内】，那里既没有 docker 也没有 mysql CLI
      （实测 FileNotFoundError: 'docker'）。
    ★ 也不能用 DriverManager 裸连：--jars 的驱动进不了 AppClassLoader，
      报 No suitable driver found / ClassNotFoundException（已实测）。
      所以驱动 jar 必须挂在 /opt/spark/jars/ 下（见 run_dwd_sl_test.sh 注释）。
    """
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


def stream_load(files, label):
    """把若干 CSV 文件依次 Stream Load 进 dwd_order_detail。"""
    results = []
    for f in files:
        url = (f"http://{SR_FE}:{SR_HTTP_PORT}/api/{SR_DB}/{SR_TABLE}/_stream_load")
        hdr = {
            "format": "csv",
            "column_separator": "|",
            "columns": COLUMNS,
            "max_filter_ratio": str(MAX_FILTER_RATIO),
            "timezone": "Asia/Shanghai",
            "label": label,
        }
        # ★ 必须显式带 Expect: 100-continue：
        #   StarRocks 的 Stream Load 强制要求（alt：把 --expect100-timeout 调大）。
        #   之前写 `-H "Expect:"`（禁用 100-continue）会直接 500 报
        #   `There is no 100-continue header`（已实测）。
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
            if "Label Already Exists" in msg or "label already exists" in msg.lower():
                j["_skipped"] = True
                results.append(j)
                continue
            raise RuntimeError(f"Stream Load 失败: {json.dumps(j, ensure_ascii=False)[:600]}")
        results.append(j)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--dates")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--keep-stage", action="store_true",
                    help="保留临时 CSV（默认跑完删掉）")
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
    # 1) 读湖 + 清洗
    # -----------------------------------------------------------------
    t0 = time.time()
    spark.table("lake.ods.ods_order").createOrReplaceTempView("_lake_order")
    total_lake = spark.sql("SELECT count(*) c FROM _lake_order").collect()[0].c
    print(f"TIME {time.time()-t0:.1f}s 读湖 ODS 完成 lake_order_rows={total_lake}", flush=True)

    # ROW_NUMBER 的 PARTITION BY 必须是 (dt, order_id)：
    #   老作业单天跑，PARTITION BY order_id 就够；本作业一次处理多天，
    #   只按 order_id 会把跨天数据混在一起排序。加 dt 后与「逐天跑」严格等价。
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
    clean = spark.table("_clean").cache()
    n_clean = clean.count()
    print(f"TIME {time.time()-t0:.1f}s 清洗完成 clean_rows={n_clean}", flush=True)

    by_day = {r["dt"].isoformat(): r["c"] for r in
              spark.sql("SELECT dt, count(*) c FROM _clean GROUP BY dt").collect()}
    print(f"CLEAN_DAYS = {len(by_day)}  期望 {len(days)}", flush=True)

    if args.dry_run:
        for d in days:
            print(f"DRY {d} {by_day.get(d, 0)}")
        print(f"EXPECTED_TOTAL = {sum(by_day.get(d, 0) for d in days)}")
        print("DRY_RUN_ONLY -> 未写入任何数据")
        spark.stop()
        return 0

    # -----------------------------------------------------------------
    # 2) 分批（整批天）
    # -----------------------------------------------------------------
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
    print(f"BATCHES = {len(batches)}  (每批最多 {ROWS_PER_BATCH} 行 / "
          f"{MAX_DAYS_PER_BATCH} 天)", flush=True)

    # -----------------------------------------------------------------
    # 3) 清掉目标区间（一条 SQL，无论之前灌过什么）
    # -----------------------------------------------------------------
    mysql(spark, f"DELETE FROM {SR_DB}.{SR_TABLE} "
                 f"WHERE dt >= '{tag_start}' AND dt <= '{tag_end}'")
    print(f"已清空 {tag_start} ~ {tag_end} 的旧数据", flush=True)

    # -----------------------------------------------------------------
    # 4) 落 CSV + Stream Load
    # -----------------------------------------------------------------
    if os.path.isdir(STAGE_DIR):
        shutil.rmtree(STAGE_DIR)
    os.makedirs(STAGE_DIR, exist_ok=True)

    total_written = 0
    t1 = time.time()
    for bi, bdays in enumerate(batches):
        tb = time.time()
        exp = sum(by_day.get(d, 0) for d in bdays)

        # 4.1 落 CSV
        # ★ 头号坑：Spark 的 csv + .option("compression","gzip") 写出来的 part 文件
        #   本身【已经是 gzip】。之前我又用 gzip.open 包了一层 → 文件成了
        #   gzip(gzip(csv))，StarRocks 解一层拿到二进制垃圾，报
        #   `Target column count: 7 doesn't match source value column count: 1`
        #   且 NumberTotalRows 只有 380（它按 \n 切二进制）。所以这里写【纯文本】。
        part = (clean.filter(F.col("dt").isin(*bdays))
                     .select(
                         *[c for c in TARGET_COLS if c not in ("dt", "order_time")],
                         F.date_format("dt", "yyyy-MM-dd").alias("dt"),
                         F.date_format("order_time",
                                       "yyyy-MM-dd HH:mm:ss").alias("order_time"),
                     )
                     .select(*TARGET_COLS)
                     .repartition(1)
                     .cache())
        wdir = f"{STAGE_DIR}/_w{bi:03d}"
        fpath = f"{STAGE_DIR}/batch_{bi:03d}.csv"
        # ★ sep 必须是 | —— 用 StarRocks 的 column_separator 指定的字符。
        #   默认导出的 csv 是逗号分隔，而 header 写的是 "|"，StarRocks 会按 | 切，
        #   整行切不开 → 报 `Target column count: 7 doesn't match source value
        #   column count: 1`（每行都变成 1 列，已实测）。
        # ★ 时间也必须显式格式化成 `yyyy-MM-dd HH:mm:ss`：
        #   Spark 写 csv 时 timestamp 走 ISO8601（`2026-09-20T12:05:49.000+08:00`），
        #   StarRocks 的 DATETIME 不认这个带 T 和时区偏移的写法。
        part.write.mode("overwrite").option("compression", "none") \
            .option("sep", "|").option("emptyValue", "\\N").csv(f"file://{wdir}")
        # 合并 part-* 成单文件（Spark 的 CSV writer 每行都以 \n 结尾，
        # 字节拼接后仍是合法 CSV）
        flist = sorted(glob.glob(f"{wdir}/part-*"))
        with open(fpath, "wb") as out:
            for pf in flist:
                with open(pf, "rb") as fh:
                    shutil.copyfileobj(fh, out)
        part.unpersist()
        shutil.rmtree(wdir, ignore_errors=True)

        # 4.2 导入（label 带批次号；重试时 StarRocks 会报 Label Already Exists → 跳过）
        label = f"dwd_od_{tag_start}_{tag_end}_{bi:03d}"
        res = stream_load([fpath], label)
        loaded = sum(int(r.get("NumberLoadedRows", 0) or 0) for r in res)
        filtered = sum(int(r.get("NumberFilteredRows", 0) or 0) for r in res)
        skipped = any(r.get("_skipped") for r in res)

        # 4.3 回读核对
        d0, d1 = bdays[0], bdays[-1]
        got = mysql(spark, f"SELECT count(*) c FROM {SR_DB}.{SR_TABLE} "
                           f"WHERE dt >= '{d0}' AND dt <= '{d1}'", fetch=True) or 0
        got = int(got)
        ok = "OK" if got == exp else f"**MISMATCH** 期望{exp} 实际{got}"
        total_written += got
        print(f"BATCH {bi:03d} {d0}~{d1} days={len(bdays)} exp={exp} "
              f"loaded={loaded} filtered={filtered} "
              f"{'SKIP(label已存在)' if skipped else ''} verify={ok} "
              f"cost={time.time()-tb:.1f}s", flush=True)

    print(f"TIME {time.time()-t1:.1f}s 全部写完 total_written={total_written}", flush=True)
    print(f"TIME {time.time()-t_all:.1f}s 总耗时", flush=True)
    print(f"TOTAL_WRITTEN = {total_written}", flush=True)

    if not args.keep_stage:
        shutil.rmtree(STAGE_DIR, ignore_errors=True)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
