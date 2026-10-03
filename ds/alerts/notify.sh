#!/bin/bash
# DolphinScheduler 告警脚本（Script 通道）
#
# 【DS 是怎么调用这个脚本的】
#   alert-server 的 ScriptSender 通过 /bin/sh 执行，参数是【命名选项】：
#     notify.sh -t "<标题>" -c "<内容>" [-p "<userParams>"]
#   ⚠️ 不是位置参数！用 $1/$2 读会拿到 "-t" / "-c" 这些选项字符串本身。
#   这是实测确认的（不是文档猜的）：
#       $ /opt/ds-alerts/notify.sh -t "标题" -c "内容"
#       → 用 $1/$2 的旧版把「标题: -t / 内容: 标题」写进了日志，整体错位。
#
# 【为什么用 getopts 而不是手工 while+case】
#   getopts 能正确处理"-t"后面跟的字符串里带空格、带引号的情况，
#   而且选项缺失/未知选项能提前发现，不用自己写一堆判断。
#
# 【为什么写文件而不是只 echo】
#   alert-server 的 stdout 混在 DS 日志里不好找；
#   写独立文件 + 时间戳，一眼看出"哪次告警、什么时候"。
#
# 【参数】
#   -t  标题（告警实例名 + 工作流信息）
#   -c  内容（失败的任务与错误摘要）
#   -p  userParams（告警实例上填的"自定义参数"，没用可以不给）

set -u

LOG=/tmp/ds-alerts.log
TITLE=""
CONTENT=""
USER_PARAMS=""

while getopts "t:c:p:" opt; do
    case "$opt" in
        t) TITLE="$OPTARG" ;;
        c) CONTENT="$OPTARG" ;;
        p) USER_PARAMS="$OPTARG" ;;
        *) ;;
    esac
done

{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== 告警 ====="
    echo "标题: $TITLE"
    echo "内容: $CONTENT"
    [ -n "$USER_PARAMS" ] && echo "自定义参数: $USER_PARAMS"
    echo
} >> "$LOG"

# 同时输出到 stdout，便于在 DS 日志里排查
echo "[ALERT] $TITLE"

# 必须 exit 0：非 0 会被 alert-server 记为"发送失败"，
# 于是 t_ds_alert 里那条告警会一直重试
exit 0
