#!/bin/bash
# =============================================================================
# DWD 层按天覆盖作业
#
# 用途：把 ODS 中某一天的订单清洗后写入 DWD 的对应分区
# 运行环境：DolphinScheduler 的 Shell 任务节点
#
# 参数：${system.biz.date} —— DS 内置参数，业务日期，格式 yyyyMMdd
#       手动执行时是"昨天"；用补数功能可以指定历史日期
#
# 【为什么用 Shell 任务而不是 SQL 任务】
#   DS 的 SQL 任务把 ${param} 编译成 JDBC 的 ? 占位符（不是文本替换），
#   而 ? 只能填"值"，不能填"名字"（表名、列名、分区名）。
#   写成 PARTITION (p${bizdate}) 会变成 PARTITION (p?)，语法错误且无解。
#   Shell 任务是纯文本替换，所以能拼出分区名。
#
# 【为什么用 INSERT OVERWRITE 而不是 DELETE + INSERT】
#   1. StarRocks 的 DELETE ... WHERE 只接受字面量，不接受函数：
#      DELETE FROM t WHERE dt = STR_TO_DATE(...)  →  Right expr should be value
#   2. 实测 DELETE + INSERT 会导致数据翻倍（删除谓词在同一会话里没立即生效）
#   3. INSERT OVERWRITE 是原子的，要么全换要么不变
# =============================================================================

set -e

# ---- 一个参数，两种格式 ----
# ${system.biz.date} 由 DS 在运行前替换成 20260920 这样的字符串
D=${system.biz.date}

# shell 字符串切片：${var:起始位置:长度}
# ${D:0:4} = 2026    ${D:4:2} = 09    ${D:6:2} = 20
DF="${D:0:4}-${D:4:2}-${D:6:2}"

echo "业务日期(压缩格式): $D"
echo "业务日期(标准格式): $DF"

# ---- 执行覆盖 ----
# 注意两处 $D / $DF 的用法区别：
#   p${D}   分区名，是「标识符」，绝对不能加引号
#   '${DF}' 日期值，是「字符串字面量」，必须加引号
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
INSERT OVERWRITE dwd.dwd_order_detail PARTITION (p${D})
SELECT
    order_id, dt, user_id, product_id, amount, order_time, status
FROM (
    SELECT
        order_id,
        dt,
        user_id,
        product_id,
        ABS(amount) AS amount,
        order_time,
        status,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ABS(amount) DESC
        ) AS rn
    FROM ods.ods_order
    WHERE user_id IS NOT NULL
      AND dt = '${DF}'
) t
WHERE rn = 1;
"

echo "DWD 覆盖完成: $DF"

# ---- 验证（可选，调试时打开）----
# docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
# SELECT dt, count(*) AS cnt FROM dwd.dwd_order_detail GROUP BY dt ORDER BY dt;"
