#!/usr/bin/env bash
# =============================================================================
# 湖侧 Spark 作业公共库（方案 C：复用常驻容器 spark）
#
# 约定：调用方先定义 HERE / PROJ / SPARK_CT / LOCK_DIR，再 `source "$HERE/_spark_lib.sh"`。
# 提供：
#   spark_ensure_running   —— 常驻容器存活检查（不在就退出码 69）
#   spark_acquire_lock     —— 串行锁（宿主机同一时刻只允许一个 Spark 作业）
#   SPARK_SQL[] / SPARK_SUBMIT[] —— 容器内标准启动参数
#
# 环境变量：
#   SPARK_CT    常驻 Spark 容器名（默认 spark）
#   LOCK_DIR    串行锁目录（由调用方给默认值）
#   DRIVER_MEM  driver 堆内存（默认 1g；compose 里 spark 的 mem_limit 是硬上限）
#
# ★ 锁目录为什么放在 scripts/ 下，而不是 lake/ 下：
#   实测 DS 容器只挂了 ./scripts ./sql ./ds/{alerts,logs} + docker.sock，
#   【没有】挂 ./lake。所以：
#     宿主跑：$PROJ=/home/l/offline-dw → 锁在 /home/l/offline-dw/scripts/...
#     DS 里跑：$PROJ=/opt/offline-dw   → 锁在 /opt/offline-dw/scripts/...
#   两者指向宿主同一个 ./scripts 目录 → 锁真正共享（放 lake/ 下就会变成两把锁）。
# =============================================================================

spark_ensure_running() {
  if [ "$(docker inspect -f '{{.State.Running}}' "$SPARK_CT" 2>/dev/null || echo false)" != "true" ]; then
    echo "!!! 容器 $SPARK_CT 不在运行。先启动：docker compose up -d spark" >&2
    exit 69
  fi
}

# 串行锁：spark 容器只有一个（mem_limit 2~4g），并发跑两个作业会抢内存被 OOM kill。
# 用 mkdir 的原子性做锁，不依赖 flock 是否存在（DS 基础镜像里未必有）。
spark_acquire_lock() {
  local age
  mkdir -p "$(dirname "$LOCK_DIR")" 2>/dev/null || true
  while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    if [ -d "$LOCK_DIR" ]; then
      age=$(( $(date +%s) - $(stat -c %Y "$LOCK_DIR" 2>/dev/null || echo 0) ))
      if [ "$age" -gt 1800 ]; then
        echo "WARN: 锁 $LOCK_DIR 已存在 ${age}s（>30min），判定为残留（进程被 kill 没清锁），清除" >&2
        rm -rf "$LOCK_DIR"; continue
      fi
      echo "… 另一个 Spark 作业在跑（$LOCK_DIR，已 ${age}s），等待中"
    fi
    sleep 5
  done
  echo "已取得串行锁：$LOCK_DIR"
  trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT
}

# ---- 容器内标准参数 ----
# ★ 脚本 / SQL 一律用【容器内路径】：仓库的 scripts/ sql/ lake/ 都已挂进 spark 容器，
#   所以不再需要 docker cp，也不再受「宿主路径 vs 容器路径」困扰。
SPARK_SQL=(/opt/spark/bin/spark-sql --master local[2] --driver-memory "${DRIVER_MEM:-1g}" \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse)

SPARK_SUBMIT=(/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory "${DRIVER_MEM:-1g}" \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar)
