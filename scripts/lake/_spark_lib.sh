#!/usr/bin/env bash
# =============================================================================
# 湖侧 Spark 作业公共库（方案 C：复用常驻容器 spark）
#
# 约定：调用方先定义 HERE / PROJ / SPARK_CT / LOCK_DIR，再 `source "$HERE/_spark_lib.sh"`。
# 提供：
#   spark_ensure_running   —— 常驻容器存活检查（不在就退出码 69）
#   spark_acquire_lock     —— 串行锁（同一时刻只允许一个 Spark 作业）
#   SPARK_SQL[] / SPARK_SUBMIT[] —— 容器内标准启动参数
#
# 环境变量：
#   SPARK_CT    常驻 Spark 容器名（默认 spark）
#   LOCK_DIR    串行锁目录（默认 /tmp/.spark_job.lock，**容器内**路径）
#   DRIVER_MEM  driver 堆内存（默认 1g；compose 里 spark 的 mem_limit 是硬上限）
#
# ★ 锁为什么必须是「容器内」路径（2026-10-16 实测修正）：
#   宿主和 DS 都是 docker exec 进【同一个】spark 容器跑作业，
#   所以容器内的同一个目录天然就是两边的共享点。
#   旧写法把锁放在 $PROJ/scripts/.spark_job.lock：
#     · 宿主跑：$PROJ=/home/l/offline-dw → /home/l/offline-dw/scripts/...
#     · DS  跑：$PROJ=/opt/offline-dw   → /opt/offline-dw/scripts/...
#   这两个路径虽然指向宿主的同一个目录，但 DS 容器把 ./scripts 以 **:ro** 挂载
#   （docker inspect 显示 RW=false）→ mkdir 永远失败、[ -d ] 又恒为假 →
#   脚本变成「一个字都不打印」的死循环，一直卡到 DS 节点超时（实测 EXIT=124）。
#   现在锁落在容器内 /tmp/.spark_job.lock：不依赖任何 bind mount 的写权限，
#   宿主与 DS 共用同一把锁。
# =============================================================================

spark_ensure_running() {
  if [ "$(docker inspect -f '{{.State.Running}}' "$SPARK_CT" 2>/dev/null || echo false)" != "true" ]; then
    echo "!!! 容器 $SPARK_CT 不在运行。先启动：docker compose up -d spark" >&2
    exit 69
  fi
}

# 串行锁：spark 容器只有一个（mem_limit 2~4g），并发跑两个作业会抢内存被 OOM kill。
# 用 mkdir 的原子性做锁，不依赖 flock 是否存在（DS 基础镜像里未必有）。
# LOCK_DIR 必须是【容器内】路径，见文件头说明。
spark_acquire_lock() {
  local age
  # 先探测目标位置可不可写：不可写就直接失败，绝不静默死循环
  if ! docker exec "$SPARK_CT" sh -c ": > '$LOCK_DIR.probe' && rm -f '$LOCK_DIR.probe'" 2>/dev/null; then
    echo "!!! 锁目录不可写：$LOCK_DIR（容器 $SPARK_CT 内）" >&2
    exit 70
  fi
  while ! docker exec "$SPARK_CT" mkdir "$LOCK_DIR" 2>/dev/null; do
    if docker exec "$SPARK_CT" test -d "$LOCK_DIR" 2>/dev/null; then
      age=$(( $(date +%s) - $(docker exec "$SPARK_CT" stat -c %Y "$LOCK_DIR" 2>/dev/null || echo 0) ))
      if [ "$age" -gt 1800 ]; then
        echo "WARN: 锁 $LOCK_DIR 已存在 ${age}s（>30min），判定为残留（进程被 kill 没清锁），清除" >&2
        docker exec "$SPARK_CT" rm -rf "$LOCK_DIR"; continue
      fi
      echo "… 另一个 Spark 作业在跑（$LOCK_DIR，已 ${age}s），等待中"
    else
      echo "!!! 无法创建锁 $LOCK_DIR（容器 $SPARK_CT 内权限异常）" >&2
      exit 70
    fi
    sleep 5
  done
  echo "已取得串行锁（容器 $SPARK_CT）：$LOCK_DIR"
  trap 'docker exec "$SPARK_CT" rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT
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

