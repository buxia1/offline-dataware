# 踩坑记录

搭建这个骨架时实际遇到的问题。**每条都带真实报错信息**，方便以后直接搜索。

按"再次遇到的可能性"排序，越靠前越容易再踩。

---

## 一、数据正确性（最隐蔽，最难查）

### 1.1 时区不统一导致日期错一天

**现象**

数据看起来完全正常，但 `dt` 和 `order_time` 对不上：

```
| dt         | order_time          |
| 2026-09-22 | 2026-09-23 04:51:06 |   ← 差一天
```

统计发现 **667 / 2000 行**有这个问题——**正好三分之一**。

**原因**

链路上有三处时区，默认值不一致：

| 环节 | 默认时区 | 后果 |
|---|---|---|
| Python 生成器（跑在 WSL） | **UTC** | 生成的时间字符串是 UTC |
| Spark 算 `dt = to_date(order_time)` | **UTC** | `dt` 是 UTC 日期，与 `order_time` 内部一致 |
| JDBC 写入，URL 里写了 `serverTimezone=Asia/Shanghai` | **+8** | Connector/J 把时间戳平移了 8 小时 |

结果：`order_time` 被 +8 平移，**但 `dt` 是 DATE 类型，不受时区转换影响**。

`order_time` 落在 UTC 16:00~23:59 的 8 小时区间的记录，平移后跨过午夜 → `dt` 差一天。**8/24 = 1/3，与实测完全吻合。**

**为什么难查**

- 不报错，不崩溃
- 前面几轮跑下来都"正常"，直到给 DWD 加分区才暴露（`dt` 是分区键，值错了写不进去）
- `count(*)`、金额合计这些总量指标全都是对的，只有日期维度错

**解法：三处统一成 `Asia/Shanghai`**

```bash
# 1. WSL
sudo timedatectl set-timezone Asia/Shanghai

# 2. spark 容器（docker-compose.yml）
environment:
  TZ: Asia/Shanghai

# 3. Spark session（scripts/ods_order_to_starrocks.py）
spark = (SparkSession.builder
    .appName("ods_order")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .getOrCreate())
```

**核心原则：`serverTimezone` 必须等于 JVM 的默认时区。**

**验证方法**

```sql
SELECT count(*) AS total,
       count(CASE WHEN dt <> DATE(order_time) THEN 1 END) AS mismatch
FROM ods.ods_order;
-- mismatch 必须是 0
```

---

### 1.2 bind mount 到不存在的路径不报错

**现象**

```yaml
volumes:
  - ./aaa:/完全/不存在的/路径
```

容器正常启动，看起来挂上了，**实际什么都没挂**——Docker 会创建那个空目录，两边都是空的。

最坑的是：**你以为数据持久化了，其实还在容器层里，容器一重建就全丢。**

**为什么踩到**

StarRocks allin1 镜像的真实路径不是 `/opt/starrocks/`，而是 **`/data/deploy/starrocks/`**。按常规猜的路径一个都不存在。

**解法：挂载后必须用探针文件验证**

```bash
# 在容器里创建文件
docker compose exec starrocks touch /data/deploy/starrocks/fe/log/probe.txt

# 在宿主机确认能看到
ls -l ./starrocks/fe/log/probe.txt

# 能看到 → 挂载真的生效了；看不到 → 挂了个空目录
```

**通用做法：任何 bind mount 配好后，都跑一次探针测试。**

**怎么找真实路径**

```bash
docker compose exec starrocks sh -c \
  'find / -xdev -maxdepth 6 \( -name fe.conf -o -name be.conf \) 2>/dev/null'
```

配置文件所在的目录就是安装根目录，数据目录在它旁边。

---

### 1.3 ODS 全量重读 Kafka 导致重复

**现象**

同一批 1000 条数据，DWD 里出现 2000 行。

**原因**

Spark 脚本用 `startingOffsets=earliest`，**每次执行都把 Kafka 里所有消息重读一遍**。Kafka 的消息不会因为被读走而消失（只受保留策略控制），所以跑两次就是两遍。

**⚠️ 本节原结论「这是设计使然，不是 bug —— 重复由 DWD 去重解决」是错的，2026-10-07 已实测推翻。**

**为什么错**：那个"设计"只对**主键模型**成立。

| 表 | 模型 | 重复消息的结局 |
|---|---|---|
| `ods_order` | `PRIMARY KEY(order_id)` | 自动折叠 ✅ |
| `ods_order_event` | `DUPLICATE KEY(order_id)` | **原样保留** ❌ |

而 `dwd_order_lifecycle` 是**累积快照**（PK 表），它按 `order_id` UPSERT —— **等于默认"每个订单的每个里程碑在事件表里只有一行"**。这个前提在 `DUPLICATE KEY` 下不成立。详见 §3.17。

**但做实验时会把数字搞乱**。判断方法：看重复行数是不是**正好是整数倍**（我们遇到过 116 和 66，正好是单批次 58 和 33 的两倍）。

**解法**

- 做对照实验前，先删 topic 重建 + `TRUNCATE` 下游表
- 生产环境改用**增量**（维护 offset，每次只读新消息），或换 StarRocks Routine Load
- ⚠️ **不要**用"先 `TRUNCATE` 再从 Kafka 全量重灌"当长期方案 —— Kafka 有 retention，消息一旦过期，清表就等于**清库**（2026-10-05 事故的根因）。它只能当**回补手段**，且必须配"进度不得超过可用范围"的防线

---

## 二、DolphinScheduler

### 2.1 SQL 任务的参数是 JDBC 绑定，不是文本替换

**现象**

```sql
INSERT OVERWRITE dwd.dwd_order_detail PARTITION (p${bizdate})
```

报错：

```
No viable statement for input 'PARTITION (p'20260920''
```

**原因**

DS 的 SQL 任务把 `${param}` 编译成 **JDBC 的 `?` 占位符**，不是字符串替换。日志里能看到铁证：

```
[INFO] prepare statement replace sql :
       DELETE FROM dwd.dwd_order_detail WHERE dt = STR_TO_DATE(?, '%Y%m%d')
       sql parameters : {1=Property{prop='bizdate', type=VARCHAR, value='20260920'}}
```

**`?` 只能填"值"，不能填"名字"**（表名、列名、分区名都是名字）。

数据库解析 SQL 骨架时 `?` 的值还没送到，**它连去哪张表找列都不知道，整句话没法解析**。

这与 SQL 子句的执行顺序（FROM → WHERE）无关——**解析和绑定都发生在执行之前**。

**解法：要拼 SQL 结构，必须用 Shell 任务**

| | SQL 任务 | Shell 任务 |
|---|---|---|
| 替换方式 | JDBC 参数绑定（`?`） | **纯文本替换** |
| 值位置 | ✓ | ✓ |
| 标识符位置 | ❌ | ✓ |
| 格式转换 | ❌ | ✓（shell 切片、`date` 命令） |

**规则：参数只当"值"用 → SQL 任务；要拼"结构" → Shell 任务。**

---

### 2.2 StarRocks 的 DELETE 只接受字面量

**现象**

```sql
DELETE FROM dwd.dwd_order_detail
WHERE dt = STR_TO_DATE('${bizdate}', '%Y%m%d')
```

报错：

```
Right expr of binary predicate should be value.
```

**原因**

StarRocks 的删除**不是当场删数据**，而是记录一条**"删除谓词"**（delete predicate）到元数据，后续读取时过滤、后台合并时才物理删除。

**谓词要长期保存，所以条件必须是能写死的字面量**，不能是表达式或占位符。

| 字面量 | 表达式 |
|---|---|
| `'2026-09-20'`、`123` | `STR_TO_DATE(...)`、`1+1`、`UPPER(name)` |

**同样的 `WHERE`，在 `SELECT` 里能用表达式，在 `DELETE` 里不行。**

**解法：别用 `DELETE`，改用 `INSERT OVERWRITE`**

`INSERT OVERWRITE` 是写操作，条件允许表达式，而且是**原子的**。

**另一个更重要的理由**：`DELETE` + `INSERT` 的两步方案本身就不安全——

- 依赖 DELETE 立即生效（实测**没生效，数据翻倍了**：82 → 164）
- 两步之间有空窗，查询会看到"数据不存在"
- 任一步失败会留下半完成状态

**`INSERT OVERWRITE` 要么全换要么不变，没有这些问题。**

---

### 2.3 cron 表达式是 6~7 位，不是 Linux 的 5 位

| | 字段数 | 例子 |
|---|---|---|
| Linux crontab | **5 位**：分 时 日 月 周 | `0 2 * * *` |
| **DolphinScheduler（Quartz）** | **6~7 位**：**秒** 分 时 日 月 周 [年] | `0 0 2 * * ?` |

**最前面多一个"秒"，而且"周"那一栏通常写 `?` 而不是 `*`**（日和周互斥，写 `*` 部分解析器会报冲突）。

直接从 Linux 抄过来的表达式**永远不触发，而且不报错**。

---

### 2.4 新建定时任务的开始时间默认是"次日"

**现象**

定时配好了、也上线了，**干等一整天都不触发**。

**原因**

新建定时时，开始时间默认填的是**次日 00:00:00**。即使上线，也要等到明天。

**解法**

编辑定时任务，把开始时间改成**今天**（或当前时间之前）。

---

### 2.5 两个「上线」缺一不可

- **工作流定义** 要上线
- **定时任务** 也要上线

少一个都不会自动执行。

---

### 2.6 删除节点后必须重连连线

在画布上删掉一个节点后，上下游的连线会断开，**必须手工把线接上**，否则 DAG 结构不完整，节点会被跳过。

---

### 2.7 告警：4 个连在一起的坑（配一次要踩全）

**背景：DS 的告警是"三层结构"，缺一层就静默失效。**

```
① 告警实例（通道：邮件/钉钉/Script…）
      ↑ 被引用
② 告警组（出事通知谁）
      ↑ 被引用
③ 定时任务上的【告警类型 + 告警组】   ← 缺这层 = 前面全白做，且不报错
```

以下 4 条**每条都会让告警静默失效**，而且报错信息完全指不到原因。

---

#### 坑 1：告警组**不在工作流编辑界面**，在【定时】里

**现象**

在工作流定义里翻遍也找不到「告警组」这个字段 —— 只有**手动点「运行」**时弹窗里才有。

**原因**

`t_ds_process_definition` 表**有** `warning_group_id` 列，但 **DS 3.2.0 的 UI 不暴露它**。

查前端 bundle 里的字段分布，`warningGroupId` 只出现在：

```
start-modal.js          ← 手动运行弹窗
timing-modal.js         ← 定时配置弹窗   ★ 永久生效的那个
dag-startup-param.js
use-start.js / use-modal.js
```

**永久生效的入口**：

```
项目管理 → 工作流定义 → 点该工作流的「定时」 → 编辑
  → 【告警类型】改成「失败」 → 才会出现【告警组】下拉
```

**两个入口的分工**（**不是二选一**）：

| 入口 | 生效范围 |
|---|---|
| **定时 → 告警组** | **自动调度**失败（每天 02:00/03:00 那种，人不在电脑前）|
| **运行弹窗 → 告警组** | **只对那一次手动执行**生效，不落库 |

**底层证据**：`ProcessAlertManager.sendAlertProcessInstance()` 里取的是

```java
processInstance.getWarningGroupId()    // ← 从【流程实例】取，不是从工作流定义
```

自动调度时，实例的告警组来自**定时的 `warning_group_id`**；手动执行时来自**运行弹窗选的值**。

---

#### 坑 2：`warningType = NONE` 时，告警组下拉**根本不显示**

**现象**

进了定时编辑页，**找不到「告警组」下拉** —— 以为这个版本没这功能。

**原因**

前端源码里的渲染条件：

```js
this.timingForm.warningType !== "NONE" && renderAlertGroupSelect()
```

**告警类型是 `NONE` 时，下拉框直接不渲染。** 所以必须**先**把告警类型改成「失败」或「全部」，下拉才会出现。

**顺序错了就会以为"没这个功能"。**

---

#### 坑 3：Script 插件用**命名参数**调用，不是位置参数 `$1`/`$2`

**现象**

Script 告警脚本按常规写成 `$1` = 标题、`$2` = 内容，结果收到的日志是：

```
标题: -t
内容: 告警标题
```

**参数整体错位。**

**原因**

`alert-server` 的 `ScriptSender` 是这样拼命令的（从字节码常量池里挖出来的）：

```
/bin/sh  <脚本路径>  -t  "<标题>"  -c  "<内容>"  [-p  "<userParams>"]
```

**是命名选项，不是位置参数。** 用 `$1`/`$2` 读，拿到的是 `-t`、`-c` 这些**选项字符串本身**。

**解法：用 `getopts` 解析**

```bash
while getopts "t:c:p:" opt; do
    case "$opt" in
        t) TITLE="$OPTARG" ;;
        c) CONTENT="$OPTARG" ;;
        p) USER_PARAMS="$OPTARG" ;;
    esac
done
```

**顺带两条**：

- 脚本**必须 `exit 0`** —— 返回非 0 会被 alert-server 记为"发送失败"，告警进重试队列
- 脚本**必须可执行**（`chmod +x`），而且 DS 容器里 `./scripts` 是**只读挂载**，告警脚本要放**新挂载目录**（本项目用 `./ds/alerts:/opt/ds-alerts`，**不带 `:ro`**）

---

#### 坑 4：「创建租户」≠ 能用，还要在 worker 容器里 `useradd`

**租户是什么**：任务在 worker 容器里以哪个 **Linux 用户**执行。DS 提交任务时会执行类似

```bash
sudo -u <租户名> bash -c "..."
```

**所以租户名必须在 worker 容器里是一个真实存在的用户。**

| 场景 | 要不要建租户 |
|---|---|
| 本项目（standalone，容器 root，`default` 已在用，工作流跑得通）| **不用建** |
| 多团队隔离权限/资源 | 建，每个团队一个 |
| 任务必须用特定系统用户（如 `hadoop`）| 建，并在容器里 `useradd` |

**踩了会怎样**：DS 里建好租户、填上名字，任务执行时报

```
sudo: unknown user: <租户名>
```

**注意**：**告警实例不需要租户** —— 所以告警的编辑弹窗里根本没有这个字段。看到「租户」只在**用户管理**和**工作流节点**上。

---

#### 怎么验证告警真的通了（别只看"配好了"）

**推荐做法：造一次真失败**，而不是依赖"测试发送"按钮（**DS 3.2.0 的 Script 插件弹窗里没有那个按钮** —— 前端只对手部分插件渲染）。

| # | 验证点 | 命令 / 位置 |
|---|---|---|
| 1 | 告警落库 | `SELECT id, alert_type, alert_status, title FROM t_ds_alert;` |
| 2 | **发给了哪个组** | 该表 `alert_group_id` 字段 |
| 3 | 通道发送结果 | `SELECT * FROM t_ds_alert_send_status;`（`send_status=1` + `send script alert msg success`）|
| 4 | 脚本真的收到 | 脚本自己写的日志文件 |

**实测的完整链路（2026-10-03，故意让 `truncate_ods` 报 SQL 语法错）**：

```
① 任务失败
② master 的 ProcessAlertManager.sendAlertProcessInstance()
     ├─ 拼标题 "start process failed"
     ├─ 拼内容 ProjectAlertContent JSON
     ├─ getWarningGroupId() → 2
     └─ AlertDao.addAlert() → 写 t_ds_alert
③ alert-server 轮询 t_ds_alert → 按 alert_group_id=2 顺外键查
     组2 → alert_instance_ids="1" → 实例1 → plugin_define_id=2 → "Script"
④ SPI 加载 dolphinscheduler-alert-script-3.2.0.jar
     → 执行 notify.sh -t "..." -c "..."
⑤ 脚本写日志 ✅
⑥ 回写 t_ds_alert_send_status（send_status=1）
```

**关键设计**：master 与 alert-server **不直接通信**，而是**用 `t_ds_alert` 表当队列**。好处是 master 写完立刻返回（发邮件超时也不影响工作流）、失败可重试、可多实例不重复发。代价是有**秒级延迟**（实测落库→脚本收到约 2 秒）。

---

## 三、StarRocks 表与分区

### 3.1 分区列必须是 key 列的一部分

**现象**

```sql
DUPLICATE KEY(order_id)
PARTITION BY RANGE(dt)
```

建表直接报错。

**解法**

```sql
DUPLICATE KEY(order_id, dt)   -- dt 进 key
```

**副作用**：key 列必须是表的**前几列**，所以 `dt` 要从最后一列挪到前面。**这会静默弄坏现有的 `INSERT INTO ... SELECT`**——那种写法是按位置对应的，不看列名。

**解法**：永远写显式列名。

```sql
INSERT INTO dwd.dwd_order_detail (order_id, dt, user_id, ...)
SELECT order_id, dt, user_id, ... FROM ...
```

**这是 SQL 的通用纪律：别依赖列的位置，永远写列名。**

---

### 3.2 动态分区**不回溯**创建历史分区

**现象**

```sql
PARTITION BY RANGE(dt) ()
PROPERTIES (
  "dynamic_partition.enable" = "true",
  "dynamic_partition.start" = "-30",   -- 以为会创建过去 30 天
  "dynamic_partition.end" = "3"
)
```

结果只创建了 **4 个分区**（今天 ~ 今天+3），历史一天都没有。

导入历史数据报错：

```
Error: The row is out of partition ranges. Please add a new partition.
```

**`dynamic_partition.start` 的作用是「保留」多少天历史（用于删除），不是「创建」。**

**解法：手工补历史分区（这叫"补数"）**

先关掉动态分区（否则不允许手工加）：

```sql
ALTER TABLE dwd.dwd_order_detail SET ("dynamic_partition.enable" = "false");

ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260920
  VALUES [('2026-09-20'), ('2026-09-21'));
-- ... 每天一条
```

**曾经的推荐是「表达式分区」，经实测已否决**（它确实能自动建分区，但会废掉逐天 `INSERT OVERWRITE ... PARTITION (pX)`）—— 详见 **§3.7**。

**这张表实际的解法：在物化脚本里自动补分区**（对比"有数据的天" vs "现有分区"，缺的自动 `ADD PARTITION`）—— 详见 **§3.8**。

---

### 3.3 动态分区表不允许手工 ADD/DROP PARTITION

**现象**

```
Cannot add/drop partition on a Dynamic Partition Table,
Use command ALTER TABLE tbl_name SET ("dynamic_partition.enable" = "false") firstly.
```

按报错提示先关掉即可：

```sql
ALTER TABLE dwd.dwd_order_detail SET ("dynamic_partition.enable" = "false");
```

**⚠️ 关掉之后必须自己记得开回来** —— 这个开关是**手工状态位，没有任何东西会自动恢复它**。忘了开，后果是：

- 将来**新的日期分区不再自动创建**（到了那天才发现，导入直接报错）
- 而且**不会报任何错、不会有任何警告** —— 表照样能查、旧数据照样在

**这条坑在同一个项目里踩过两次**：

| 位置 | 情况 |
|---|---|
| `sql/dwd_add_history_partitions.sql` | 原版结尾**漏了** `SET ... = 'true'`，跑一次就把开关永久留在 `false`（已补） |
| `scripts/dwd_sku_load.sh` | 用 `trap ... EXIT` 兜底：**无论成功、失败还是被 Ctrl-C，都保证恢复** |

**通用做法：任何"改状态 → 干活 → 改回来"的流程，都要用 `trap` 兜底，别指望自己记得。**

---

### 3.4 BE 自动探测内存不准，能拖垮整机

**现象**

```sql
SHOW PROC '/backends'\G
-- MemLimit: 6.282GB     ← 容器总共才 8GB！
```

BE 按"系统内存的 90%"自算上限，**在 cgroup 环境里探测不准**（StarRocks 官方 issue #43225、#29631 有记录）。一个失控查询就能把整台机器拖崩。

**解法：容器级硬限制**

```yaml
starrocks:
  mem_limit: 3g      # 加一行
```

改完 BE 的 `MemLimit` 降到 2.43GB。**这是硬保险，BE 再怎么涨也越不过 3GB。**

---

### 3.5 表模型选错会导致重跑数据翻倍

| 表模型 | 重复写入的行为 | 适用层 |
|---|---|---|
| `DUPLICATE KEY` | **追加**，数据翻倍 | ODS、DWD（事实明细） |
| `PRIMARY KEY` | **同键覆盖**，幂等 | DWS、ADS（汇总快照） |

给 `DUPLICATE KEY` 的表配定时任务，**每跑一次数据就多一份**。

---

### 3.6 `INSERT OVERWRITE` 撞上"分区必须先存在"

**现象**

```sql
INSERT OVERWRITE dwd.dwd_order_sku_detail PARTITION (p20260922)
SELECT ...;
```

报错：

```
Error: The row is out of partition ranges. Please add a new partition.
```

**很多引擎的 `INSERT OVERWRITE ... PARTITION (pX)` 会顺手把分区建出来，StarRocks 不会。** 它要求 `pX` **已经存在**，否则直接失败。

**为什么容易踩**

动态分区**只创建"未来"、不创建历史**（§3.2）。所以：

- 正常每天跑 → `p<今天>` 由动态分区提前建好了 → 一直正常
- 一旦**补数**回填历史某天 → 那天没有分区 → 失败

**实测**：`dwd_order_sku_detail` 缺 `p20260922` 分区时，这个失败是**大声的**（好事，不会静默产错数据），但足以让整个工作流中断。

**解法**：在物化脚本开头自动补分区（§3.8）。

**⚠️ 别顺便改用表达式分区** —— 它有更麻烦的后遗症（§3.7）。

---

### 3.7 表达式分区能自动建分区，但**废掉按分区名 `INSERT OVERWRITE`**

**背景**：既然动态分区不建历史分区（§3.2），很自然会想：**换成表达式分区是不是一劳永逸？**

**隔离实验**（独立测试库 `scratch_test`，做完即 `DROP`，未触碰真实表）：

| 测试 | 结果 |
|---|---|
| `PARTITION BY date_trunc('day', dt)` 建表 | ✅ 成功 |
| 插入两个**从未声明**的日期 | ✅ 分区 `p20260115` / `p20260320` **自动创建**，命名规则和动态分区一样 |
| **`INSERT OVERWRITE ... PARTITION (p20260115)`** | ❌ **报错**：`Currently, only List partitions are supported.` |
| 整表 `INSERT OVERWRITE`（不带 `PARTITION`） | ✅ 能，但**语义是替换整张表**（旧分区结构留着，数据清空） |

**结论：表达式分区确实能自动建分区，但代价是废掉「逐天 `INSERT OVERWRITE ... PARTITION (pX)`」—— 而那是逐天物化方案的核心。**

**它还会丢掉动态分区的自动清理**（`dynamic_partition.start = -30` 自动删旧分区），旧分区只增不减。

**这个行为很反直觉**：分区确实自己长出来了，但**逐分区写入的语法反而没了** —— 两者不能兼得。

**所以本项目的选择是"方案 A"**：保留动态分区（拿到自动清理）+ 在 `dwd_sku_load.sh` 里脚本级自动补分区（§3.8）。

---

### 3.8 脚本里自动补分区：`trap` + `grep -x` + 右开区间

**做法**：物化脚本开头对比「源表有数据的天」vs「目标表现有的分区」，缺的自动补。

```bash
EXISTING=$(docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B \
           -e "SHOW PARTITIONS FROM dwd.dwd_order_sku_detail" | cut -f2)

MISSING=""
for DF in $DAYS; do
    D=$(echo "$DF" | tr -d '-')
    echo "$EXISTING" | grep -qx "p${D}" || MISSING="$MISSING $D"
done
```

**三处必须做对，错一处就是静默故障：**

| # | 要点 | 错了会怎样 |
|---|---|---|
| ① | **`trap` 必须在"关开关"之前注册** | 中途失败 → `set -e` 直接退出 → 开关**永久留在 `false`**（§3.3），且不报错 |
| ② | **`grep` 必须带 `-x`**（整行匹配） | 少了它 `"p2026092"` 会**误匹配** `"p20260922"` → 漏建分区 |
| ③ | **`VALUES` 上界必须写"下一天"** | 写当天 = **空区间**；写后天 = **吞掉中间那天**（静默错误） |

```bash
# ① 先注册恢复动作，再动开关
trap restore_dynamic_partition EXIT

# ③ VALUES 左闭右开，上界 = 下一天
NEXT=$(date -d "$DF2 + 1 day" +%Y-%m-%d)
echo "ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p${D} VALUES [('${DF2}'), ('${NEXT}'));"
```

**只在真要补分区时才注册 `trap`** —— 让 99% 的正常运行**完全不碰**那个开关。

**实测验证**（2026-10-03）：

- 删掉 `p20260922` 后再跑脚本 → 分区被自动补回，`130` 行数据重新物化
- `p20260922` 的 `VersionCount = 2` → 铁证"先建分区、再 `INSERT OVERWRITE`"两步都在同一次运行里发生了
- 复查指纹 **一字未变**（`1959155153321`）→ 幂等
- 复查 `dynamic_partition.enable = true` → `trap` 生效
- 日志出现 `分区已齐全`（无缺失时的分支）与 `需要补的分区：` / `✅ 分区已补齐`（有缺失时的分支）

---

### 3.9 StarRocks 不支持"关联子查询里用非等值谓词"

**现象**

想找"在维表里找不到对应版本"的孤儿行，最容易写出的写法是：

```sql
SELECT count(*)
FROM dwd.dwd_order_sku_detail sku
WHERE NOT EXISTS (
    SELECT 1 FROM dim.dim_product_scd2 s
    WHERE s.product_id = sku.product_id
      AND sku.dt BETWEEN s.valid_from AND s.valid_to   -- ← 非等值，就是这里
);
```

报错：

```
ERROR 1064 (HY000): Getting analyzing error.
Detail message: Not support Non-EQ correlated predicate in correlated subquery.
```

**原因**

关联子查询里，`s.product_id = sku.product_id` 是**等值**谓词（StarRocks 能把它改写成 JOIN），但 `sku.dt BETWEEN ...` 是**非等值**谓词，**它不支持**。

**为什么容易踩**

「拉链表"当时口径"查询」的**标准写法**就是 `BETWEEN valid_from AND valid_to`（同一个文件里的 `dwd_order_sku_detail_load.sql` 就是这么 JOIN 的，完全合法）。但那个是**普通 JOIN**；**一旦挪进 `NOT EXISTS` / 关联子查询，就撞上这个限制**。

> **区别**：普通 `JOIN ... ON a = b AND c BETWEEN d AND e` ✅ 合法；`NOT EXISTS` 里同样的条件 ❌ 不合法。

**解法：改成 `LEFT JOIN ... IS NULL`**

```sql
SELECT count(*)
FROM (
    SELECT s.product_id AS matched
    FROM dwd.dwd_order_sku_detail sku
    LEFT JOIN dim.dim_product_scd2 s
      ON sku.product_id = s.product_id
     AND sku.dt BETWEEN s.valid_from AND s.valid_to
) t
WHERE matched IS NULL
```

**⚠️ 别被"JOIN 不上所以该用 `NOT EXISTS`"的语义直觉带偏** —— 语义上 `NOT EXISTS` 更贴切，但**引擎不支持**。`LEFT JOIN ... IS NULL` 是等价改写。

**真实影响**：DQC 检查⑥（重物化属性一致）里的孤儿行检测，最初就是被这个报错挡住的。

---

### 3.10 SQL 报"语法错误在某行"，真凶往往在**文件末尾**

**现象**

`sql/dqc_dim_product.sql` 里少了一个右括号（`AS BIGINT)` 应为 `AS BIGINT))`），报错却是：

```
ERROR 1064 (HY000) at line 20: Getting syntax error at line 102, column 0.
Detail message: No viable statement for input 'WITH checks AS ( ...
```

**报了 `line 102`，但真正的错误在文件末尾（line 119）。**

**原因**

`CAST(` 没闭合 → 这个 `SELECT` 没结束 → **CTE `checks` 没结束** → 后面的主查询

```sql
SELECT check_name, violations FROM checks WHERE violations <> 0;
```

被**吞进了 CTE 内部** → 整个 `WITH ... AS (` 结构崩塌 → 解析器在**中途**迷路，报错位置乱跳。

**怎么一眼看穿**

关键线索是报错里的 **`Unexpected input '<EOF>'`** —— 它说的是"**输入提前结束了**"，也就是**括号/引号没配平**。

用最简例子复现：

```sql
SELECT CAST( (SELECT 1) + (SELECT 2) AS BIGINT     -- 少一个 )
```
```
ERROR 1064: Unexpected input '<EOF>', the most similar input is {')'}
```

**报错直接点名缺 `)`。**

**排查纪律**

| ❌ 不要 | ✅ 要 |
|---|---|
| 盯着报错说的那一行看 | **先检查括号/引号是否配平**（尤其长嵌套 SQL）|
| 逐行删代码试 | 读报错末尾有没有 `Unexpected input '<EOF>'` |

> **通用规律：CTE / 子查询里的括号一旦缺失，报错位置会出现在任何地方，唯独不会指向真正缺括号的那一行。**

---

### 3.11 `UNION ALL` 的列名以**第一个 `SELECT`** 为准 —— 漏写别名报错完全指错方向

**现象**

`WITH checks AS (... UNION ALL ...)` 里有 5 个分支，**每一个都漏写了列别名**：

```sql
WITH checks AS (
    SELECT '① ...' AS check_name,
           CAST(... AS BIGINT)                    -- ← 少 AS violations
    UNION ALL
    SELECT '② ...' AS check_name,
           CAST(count(*) AS BIGINT)               -- ← 少 AS violations
    ...
)
SELECT check_name, violations FROM checks WHERE violations <> 0;
```

报错：

```
ERROR 1064 (HY000): Getting analyzing error.
Detail message: Column 'violations' cannot be resolved.
```

**报错只说"`violations` 解析不了"，完全不会提"你少写了列别名"，也不指向任何一行。**

**原因**

`UNION ALL` 的结果集**只以第一个 `SELECT` 的列名为准**，后面的分支不参与命名。

- 第一项漏别名 → 第二列**无名**
- 无名 → 整个 CTE 没有 `violations` 这一列
- → 最后 `SELECT violations` 失败

**为什么极难查（本次踩坑真实过程）**

| 假象 | 真相 |
|---|---|
| 单独跑某一项 → 正常 | 我的测试串里**恰好带了别名**，和文件里的不是同一段 SQL |
| 怀疑是中文注释 / BOM / 编码 | `cat -A`、`xxd`、去注释版全部排除 |
| 怀疑是 `NOT IN` 优化器问题 | 那只是叠加进来的**第二个**独立问题（§3.12）|

**真正的定位方法：把文件里每个分支结尾那一行 diff 出来。**

```bash
grep -n "AS BIGINT" sql/dqc_order_chain.sql
# 正确应为：  ... AS BIGINT) AS violations
# 全是：      ... AS BIGINT)          ← 一眼看出 5 个分支都缺
```

**通用纪律：写 `UNION ALL` 的 CTE，让每个分支都把两个列别名写全**（`check_name` 和 `violations`），不要只写第一个。

> **教训比坑本身更重要**：**"我的测试通过了"不等于"文件里的代码通过了"** —— 一定要拿**文件里的原文**去跑，而不是手打一份看起来一样的。

---

### 3.12 StarRocks 在小派生表上的优化器怪癖（自检里踩到）

自检用 `UNION ALL` 造**内存假表**（不落真实表）时，接连踩到 3 个和真实表上表现**不一样**的行为：

| # | 写法 | 现象 |
|---|---|---|
| 1 | `NOT IN (SELECT ... )`，派生表只有 **1 行** | `ERROR 1064: nest-loop join not support: NULL_AWARE_LEFT_ANTI_JOIN`（同样写法在真实表上完全正常）|
| 2 | `LEFT JOIN ... WHERE w.dt IS NULL` | **结果错误**：`LEFT JOIN` 输出明明是 2 行、`matched` 有 `NULL`，但 `COUNT(*)` 返回 **0** |
| 3 | `ABS(10.00 - 10.01) >= 0.01` 做小数差异用例 | 判定**不稳定**：单独跑返回 1，放进带 `sum()`/`CAST()` 的语句里返回 0（常量折叠踩到精度边界）|

**应对**

| 场景 | 做法 |
|---|---|
| 自检里判断"某天在目标表里一条都没有" | 用**标量子查询**：`WHERE (SELECT count(*) FROM w WHERE w.dt = d.dt) = 0`（§2 实测稳定）|
| 自检的数值用例 | **一律用整数计数**（`ABS(a.order_cnt - b.c)`），不要用小数差异 |
| 真实表上的金额比对 | **不受影响** —— 那是真的列，不是字面量。检查④ 照常用 `>= 0.01` |

**纪律：自检代码和被测代码要分开考虑。**

被测 SQL 跑在**真实表**上，可以用 `NOT IN`、小数容差；
自检 SQL 跑在**内存假表**上，要避开优化器在小数据上的退化路径。

> 这类"小数据上结果不同"的问题**只在自检里出现**，很容易被误判成"检查逻辑写错了"，从而改坏本来正确的检查。

---

### 3.13 工作目录里的临时 `.py` 文件会**静默遮蔽标准库**

**现象**

在项目/工作目录下放了一个临时脚本 `bisect.py`，之后**任何**在那目录里跑的 Python 程序都可能崩：

```
ImportError: cannot import name 'bisect' from 'bisect'
             (/mnt/d/develop/workspace/.../bisect.py)
```

真正诡异的是**报错的地方和那个文件毫无关系** —— 崩在 `random` → `tempfile` 的导入链上：

```
File ".../tempfile.py", line 184, in <module>
    from random import Random as _Random
File ".../random.py", line 56, in <module>
    from bisect import bisect as _bisect
ImportError: cannot import name 'bisect' from 'bisect'
```

**原因**

Python 的导入查找顺序里，**当前工作目录（`sys.path[0]`）排在最前面**。

所以 `bisect.py` 会**盖住**标准库的 `bisect` 模块 —— 而 `random`、`tempfile` 等一堆标准库模块**内部依赖 `bisect`**，于是它们全部炸掉。

**为什么难查**

- 报错说的是 `random` / `tempfile` / `apport`，**没有一行提到你放的那个文件**
- 那个文件本身可能只是几行无关的调试代码，看起来完全无害
- 只有仔细读 `from bisect import ... from (/path/bisect.py)` 那一行才会发现真凶

**危险文件名清单**（不要放在工作目录 / 项目根目录）

| 类别 | 例子 |
|---|---|
| 标准库模块 | `bisect.py`、`random.py`、`json.py`、`types.py`、`queue.py`、`select.py`、`code.py`、`io.py`、`string.py`、`copy.py` |
| 常用三方库 | `pandas.py`、`numpy.py`、`spark.py` |

**解法**

1. **临时脚本一律放 `/tmp`**，不要放在项目目录或工作目录里
2. 名字加前缀避免撞库：`tmp_bisect.py`、`probe_bisect.py`
3. 已经踩了：把文件删掉或改名，**不用改任何代码**

**验证命令**（看工作目录有没有遮蔽标准库的文件）

```bash
python3 -c "
import sys, os
stdlib = set(sys.stdlib_module_names) if hasattr(sys,'stdlib_module_names') else set()
hits = [f for f in os.listdir('.') if f.endswith('.py') and f[:-3] in stdlib]
print('遮蔽标准库的文件:', hits or '无')
"
```

> **通用规律：临时文件要么放 `/tmp`，要么加前缀。**
> 放在工作目录里，且名字正好撞上标准库 —— 会破坏**同目录下所有** Python 程序，而且报错完全不指向它。

---

### 3.14 StarRocks 的 `UPDATE` **不接受表别名**

**现象**

```sql
UPDATE dim.dim_product_scd2 AS s
SET s.is_current = 0
WHERE s.is_current = 1;
```

报错：

```
ERROR 1064 (HY000): Getting syntax error at line 1, column 24.
Detail message: Unexpected input 'AS', the most similar input is {'SET'}.
```

**换了写法也一样不行**（实测，StarRocks 3.5.0）：

| 写法 | 结果 |
|---|---|
| `UPDATE t AS s SET s.x = 1` | ❌ `Unexpected input 'AS'` |
| `UPDATE t s SET s.x = 1`（裸别名）| ❌ `Unexpected input 's'` |
| `UPDATE t JOIN other ON ... SET t.x = 1` | ❌ `Unexpected input 's'` |
| **`UPDATE t SET x = 1 WHERE t.y = ...`**（全表名）| ✅ **唯一可行** |

**解法：所有列引用都写全表名，不用别名**

```sql
UPDATE dim.dim_product_scd2
SET is_current = 0,
    valid_to = ( ... WHERE m.product_id = dim.dim_product_scd2.product_id ... )
WHERE is_current = 1;
```

**和 MySQL 的差异**：MySQL 里 `UPDATE t AS s` 是合法且常用的。**从 MySQL 迁移过来的 SQL 会在这里直接报错**（好在这个错很响，不会静默）。

**✅ 好消息：`UPDATE` 的其他能力都正常**

| 能力 | 支持 |
|---|---|
| `SET` 里放**不关联**的标量子查询 | ✅ |
| `SET` 里放**关联**子查询（用全表名）| ✅ |
| `WHERE EXISTS (...)` 关联子查询 | ✅ |
| `SET` 里 `DATE_SUB((子查询), INTERVAL 1 DAY)` | ✅ |
| 派生表里带窗口函数，外层再关联 | ✅ |
| `MERGE INTO` | ❌ **不支持**（`Unexpected input 'MERGE'`）|

**所以"先 UPDATE 关闭旧版本、再 INSERT 新版本"这套 SCD2 增量维护，不需要 `MERGE` 就能做。**

---

### 3.15 CTE 的作用域**只覆盖紧随其后的那一条语句**

**现象**

多语句脚本里，`WITH` 定义的 CTE 被后面的独立语句引用：

```sql
WITH marked AS (SELECT ...)
UPDATE ... WHERE ... EXISTS (SELECT 1 FROM marked ...);   -- ✅ 这条能用 marked

INSERT INTO ... SELECT ... FROM marked;                    -- ❌ 这条用不了
```

第二条报错可能是**误导性的**：

```
ERROR 1046 (3D000): No database selected
```

**报"没选数据库"，是因为 StarRocks 把 `marked` 当成了「表名」** —— CTE 已经不在作用域里了。

**原因**

`WITH ... AS ( ... )` 是**绑定到紧随其后的那一条语句**的，**不是会话级变量**。

**解法：每一条需要它的语句，都把自己的 CTE 重新写一遍**

```sql
WITH m AS (SELECT ...) INSERT ... SELECT ... FROM m;    -- 第1条：自带 CTE
-- 中间不能插别的
UPDATE ... SET x = (SELECT ... FROM (SELECT ...) m ...); -- 第2条：把推导重写一遍
```

**⚠️ 这也意味着：`WITH ... AS (...)` 后面跟多条语句时，只有第一条能用到 CTE。** 别指望"定义一次、多处复用"。

**代价**：SCD2 增量维护里，"哪些是新变更"这段推导要在 `UPDATE` 和 `INSERT` 里**各写一遍**（实测确实要这样，没有 workaround）。

**如果实在想避免重复**：把推导结果先落到一张**临时表**，两条语句都读它（本项目未采用，因为要管临时表的清理）。

---

### 3.16 `prev_date IS NULL` 在「全量重建」与「增量维护」里**语义不同**（静默多版本）

**背景**

SCD2 判断"是否产生新版本"用同一段模式：

```sql
LAG(snapshot_date) OVER (PARTITION BY product_id ORDER BY snapshot_date) AS prev_date
...
WHERE prev_date IS NULL                          -- 「第一行」
   OR NOT (category <=> prev_category AND ...)   -- 「和上一行不同」
```

**同一段 SQL，在全量和增量里含义完全不同：**

| 场景 | `LAG` 的窗口范围 | `prev_date IS NULL` 意味着 |
|---|---|---|
| **全量重建**（从 ODS 整表推）| 全部历史快照 | 这个商品**首次出现** → **该建版本** ✅ |
| **增量维护**（只处理新快照）| **只在新快照集合内** | **新集合的第一条** → **可能等于现有当前版本** → **不该建版本** ❌ |

**踩坑现象（实测）**

商品1 的历史：`09-20 家电 → 09-21 图书 → 09-26 图书 → 09-27 服饰`

- 全量重建：**3 个版本**（09-20 / 09-21 / 09-27）—— 09-26 和上一版本同为"图书"，**不建**
- 增量维护（错误版）：**4 个版本** —— 因为 09-26 是新集合的第一条，`prev_date IS NULL` **无条件选中了它**

```
增量多出：09-26 图书  ← 和当前版本完全相同的"冗余版本"
```

**这个 bug 的特征：不报错、行数变多、`ratio` 仍然是 1**（每个商品仍只有一个当前版本），
**只有做等价性验证（增量 vs 全量 指纹对比）才能发现。**

**解法：第一条和后续行，用不同的比较对象**

```sql
LEFT JOIN dim.dim_product_scd2 s                    -- 取现有当前版本
       ON s.product_id = lg.product_id AND s.is_current = 1
WHERE ( lg.prev_date IS NULL                                            -- 新集合第一条
        AND NOT (lg.category <=> s.category AND ...) )                  -- → 比【SCD2 当前版本】
   OR ( lg.prev_date IS NOT NULL                                        -- 后续行
        AND NOT (lg.category <=> lg.prev_category AND ...) )            -- → 比【上一条快照】
```

**判据**：
- **第一条新快照**：和**现有当前版本**比，不同才建版本
- **后续新快照**：和**上一条新快照**比，不同才建版本

**⚠️ 顺带一个 StarRocks 限制**：上面那个"取现有当前版本"的关联，**`EXISTS` 里放非等值谓词会被拒绝**（§3.9）：

```
ERROR 1064: Not support Non-EQ correlated predicate in correlated subquery
```

但**这里用的是等值关联**（`s.product_id = lg.product_id`），所以 `LEFT JOIN` 完全可行 —— 这也正是**为什么必须用 `LEFT JOIN ... IS NULL` 而不是 `NOT EXISTS`** 的又一个实例。

**等价性验证的做法**（这才是唯一能发现这类 bug 的办法）

```bash
# 1. 克隆一份数据，或记下当前指纹
# 2. 跑增量 → 记指纹
# 3. 跑全量重建 → 记指纹
# 4. 两个指纹必须【一字不差】
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/fingerprint_product_chain.sql
```

**实测结果**（本项目，起点 09-21 → 处理 09-26 + 09-27）：

| 路径 | 行数 | 当前版本数 | 指纹 |
|---|---|---|---|
| 全量重建 | 150 | 50 | `350365544376` |
| 增量（修正版）| 150 | 50 | `350365544376` ✅ |

**逐行 `EXCEPT` 差异 = 0 / 0**（双向都无差异）。

> **通用规律：凡是"把全量逻辑改成增量"的重构，都必须做等价性验证。**
> **光看行数不够** —— 本次是行数变了（150 → 200）才暴露；
> 但也可能行数一样、只是值不同（那样就得靠指纹或逐列对比）。
> 见本项目 `sql/fingerprint_product_chain.sql` 的设计理由："行数一样不代表数据一样"。

---

### 3.17 累积快照的三大前提：值级对账抓不到"重复行"

**背景**

`dwd.dwd_order_lifecycle`（累积快照）按 `order_id` UPSERT，**隐含假设事件表里每个 `(order_id, event_type)` 只有一行**。
2026-10-07 实测：`ods.ods_order_event` 有 **991 行 / 612 去重对** —— 379 组重复，而装载脚本**三条防线全部报绿**。

**为什么三条防线都瞎了**

| 防线 | 它比的是什么 | 为什么绕过重复 |
|---|---|---|
| ③ 双向 `EXCEPT` 对账 | **值级**去重后的差异 | 两份**完全相同**的副本，`EXCEPT` 去重后差异 = 0 |
| ④ `行数 = 唯一订单数` | **快照表** | 快照是投影，重复已被 `MAX` 折叠，主键表不会有重复行 |
| ① `范围内订单数 = count(DISTINCT order_id)` | **订单个数** | 只数订单，对"行数重复"天然不敏感 |

**实测证据**（两张表只差一份完全相同的副本）：

```
a 表 1 行 vs b 表 2 行（b 多一份完全相同的副本）
双向 EXCEPT 差异行数：0        ← 值级对账看不见重复
```

**必须补的防线**：**"行级"不变式**，而且口径要对：

```bash
# ✅ 正确：按 (order_id, event_type) 去重对数比
EVT_ROWS=$(... "SELECT count(*) FROM ods.ods_order_event")
EVT_PAIRS=$(... "SELECT count(DISTINCT concat(order_id,'-',event_type)) FROM ods.ods_order_event")
# ❌ 错误：一个订单本来就有多个事件，count(DISTINCT order_id) 永远不相等 → 假红
```

**更深的教训（三条，都超出"重复"本身）**

1. **防线有"口径"** —— 局部口径的防线**证明不了全表**。本项目那次绿勾的构成是
   `范围内订单数 123`（局部）/ `实际行数 200`（全表）—— 两个数字本来就不该相等，**却因为都是"看起来合理"的数字而没人追问**。
   > **自检打印的每个数字，都要问一句"这是哪个口径的"。**
2. **"重复"有两种，危害等级差一个数量级**：
   - **完全相同的副本** → 值不可见，只是浪费（本例：`event_time` 等所有列都相同）
   - **内容有差异的副本** → `MAX(CASE WHEN ...)` 会**静默挑一个**，结果错但不报错
   （实测：两份副本 `event_time` 不同时，视图取到的是 `2026-09-25`，而不是正确值）
   > **重复拖得越久，越可能从"无害副本"漂成"有害差异"。**
3. **局部范围检查 + 全表口径打印 = 假绿的标准配方**。§8 的"先破坏再重建"在这里同样适用：
   要证明防线有效，**先造一份脏数据，看它报不报红**。（本次修复的验证就是这么做的 —— 修完对着现有的 991 行脏表跑，它从"报绿"变成"报红"，这就是防线生效的铁证。）


## 四、Spark 与依赖

### 4.1 Ivy 缓存目录不可写

**现象**

```
Exception in thread "main" java.io.FileNotFoundException:
/home/spark/.ivy2/cache/resolved-org.apache.spark-spark-submit-parent-xxx.xml
```

**原因**

Spark 官方镜像以 `spark` 用户运行，HOME 是 `/home/spark`，**该用户没权限创建 `.ivy2` 目录**。

**解法**：把缓存指到全局可写的 `/tmp`

```bash
--conf spark.jars.ivy=/tmp/.ivy2
```

**代价**：容器重建（`docker compose up -d spark`）缓存就没了，下次要重下 30MB。

---

### 4.2 国内访问 Maven Central 会断流

**现象**

```
[FAILED] com.mysql#mysql-connector-j;8.4.0!mysql-connector-j.jar:
Downloaded file size (2211840) doesn't match expected Content Length (2533399)

Server access error ... (javax.net.ssl.SSLHandshakeException: Remote host terminated the handshake)
```

**解法**

```bash
# 1. 先清掉失败的缓存（必须，否则 Ivy 认为这个坐标已处理过）
docker compose exec spark bash -c \
  'rm -rf /tmp/.ivy2/cache/com.mysql; rm -f /tmp/.ivy2/jars/com.mysql*'

# 2. 用阿里云镜像重跑
--repositories https://maven.aliyun.com/repository/public
```

**更稳的方案**：`curl` 手动下载（自带重试）后挂载进容器

```bash
curl -L --retry 5 --retry-delay 3 -o spark/jars/mysql-connector-j-8.4.0.jar \
  https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar
```

```yaml
volumes:
  - ./spark/jars/mysql-connector-j-8.4.0.jar:/opt/spark/jars/mysql-connector-j-8.4.0.jar:ro
```

**挂单个文件是安全的**——不会遮蔽 `/opt/spark/jars` 目录里自带的几百个 jar。**挂整个目录才会。**

---

### 4.3 Spark 的 JDBC `overwrite` 模式在 StarRocks 上**不可用**（实测）

**背景**：ODS 摄入不幂等（§1.3 / §3.17）时，第一个想到的修法是"让 Spark 用 `overwrite` 覆盖"。
**2026-10-07 实测：这条路在 StarRocks 上走不通，而且失败方式很危险。**

**实测结果（隔离探针，只碰临时表）**

| 写法 | 结果 |
|---|---|
| `mode("overwrite")` + `option("truncate","true")` | `java.sql.SQLSyntaxErrorException`：`Getting syntax error at line 1, column 76. Detail message: Unexpected input ',', the most similar input is {'('}` |
| `mode("overwrite")`（不带 truncate） | **先把已存在的表连结构一起 DROP 掉**，然后 `CREATE TABLE` 报同样的语法错 |

**最危险的细节**：`overwrite` **先把表删了，再建表失败** —— 于是**表直接消失**。探针实测：
```
--- 表还在吗（1=在）---
0
```
> 如果这个模式用在 `ods_order_event` 上，一次失败就会**把整张表连同数据删掉**。

**原因**

Spark 的 JDBC 方言生成的是 **MySQL 方言 DDL**（`truncate=true` 那条更是 MySQL 多表 `TRUNCATE TABLE a, b` 的语法），StarRocks 解析不了。

**结论：StarRocks + Spark JDBC 只有 `append` 可用。**

**正确解法**（不要绕 `overwrite`）：

```bash
# 用 StarRocks 原生语句显式清空，再用 append 全量写入
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "TRUNCATE TABLE ods.ods_order_event"
docker exec spark /opt/spark/bin/spark-submit ... scripts/ods_order_event_to_starrocks.py
```

但注意：**"先清再灌"本身又引入了"Kafka 过期 → 清库"的风险**（§1.3）。
所以它只能当**回补手段**，日常必须走**增量**（维护 offset，不重读历史）。

**通用规律**
> **换数据库引擎时，"ORM/框架的方言支持"是要单独验证的一项。**
> 不要假设 `mode("overwrite")` / `truncate=true` 这类参数"只是写法差异" ——
> 它可能生成目标库根本不认的 SQL，**而且在失败前已经造成了破坏**。
> 验证方式：**拿一张一次性临时表做探针**，别直接在生产表上试。

---

### 4.4 Spark 写 StarRocks：DataFrame 里**不能有目标表没有的列**（实测）

**现象（2026-10-07，卡了整整一轮排查）**

增量摄入需要把 Kafka 的 `partition` / `offset` 元数据列带出来算消费位点。
于是代码写成"把元数据列和业务列放在同一张 DataFrame 里"，结果写库直接报：

```
AnalysisException: Column k_part not found in schema Some(StructType(
  order_id, user_id, product_id, amount, order_time, event_type, event_time, dt))
```

**误导之处**：报错说"`k_part` 不在 schema 里"，但 `rows.schema` **明明有它**：
```
INSTR-A rows.schema = [order_id, ..., dt, k_part, k_off]     ← 有
（紧接着 write 就失败）
```
看起来像"列在中途丢了"，实际上是**写入校验拒绝多出来的列**，报错把"表的 schema"和"DataFrame 的 schema"说反了。

**隔离实验（三列简单 DataFrame，与 Kafka 无关，一次性定性）**

| 实验 | DataFrame 的列 | 结果 |
|---|---|---|
| A | 8 列，与目标表**完全一致** | ✅ 成功 |
| B | 7 列，目标表列名的**子集** | ✅ 成功 |
| C | 8 列 **+ `k_part`**（多一列） | ❌ `Column k_part not found in schema` |
| D | `c1,c2,c3`（完全无关的列名） | ❌ `Column c1 not found in schema` |

**结论：JDBC 写入要求 DataFrame 的列名是目标表列名的子集（不能多、名字要对）。**

**也更省事的定位方式**：既然连 `c1` 都报错，就说明**报错与业务无关**，是写入路径的通用约束 ——
一测就知道，不用翻源码。

**解法：把"写库的 DataFrame"和"算位点的 DataFrame"分开**

```python
raw = spark.read.format("kafka")...

# ① 写库用：只保留业务列（并显式声明列序）
biz = (raw.select(from_json(col("value").cast("string"), schema).alias("j"))
       .select([col("j." + c) for c in JSON_COLS])      # ← dt 是派生列，不在 JSON 里！
       .withColumn("order_time", col("order_time").cast("timestamp"))
       .withColumn("event_time", col("event_time").cast("timestamp"))
       .withColumn("dt", to_date(col("event_time")))
       .select(BIZ_COLS))
biz.write.format("jdbc").option("dbtable", "ods_order_event") \
   .option("columns", ",".join(BIZ_COLS)) ...          # ← 显式列映射更稳

# ② 算位点用：只保留 partition/offset，绝不写 ODS
offsets_df = raw.select(col("partition").alias("k_part"), col("offset").alias("k_off"))
```

**连带踩到的第二个坑**：`dt` 是**派生列**（`to_date(event_time)`），**不在 Kafka 的 JSON 结构体里**。
把它和 JSON 字段一起 `select(col("j." + c))` 会报：

```
AnalysisException: [FIELD_NOT_FOUND] No such struct field `dt` in `order_id`, ..., `event_time`
```
→ **先 select JSON 字段、withColumn 派生、最后再 select 一次**。

**通用规律**
> **"报错说列不在 schema 里，但 `df.schema` 明明有它"→ 别怀疑 DataFrame，怀疑写入路径的约束。**
> 定位手法：**用一个三列表做隔离实验**。如果连无关列名都报同样的错，那就与你的数据无关，
> 是写入路径的通用规则（本例：不允许额外列）。这比读框架源码快得多。

---

## 五、Kafka

### 5.1 advertised listener 配错会静默超时

**现象**

客户端能连上 `kafka:29092`，然后卡住超时。**用 `nc`、`telnet`、`ping` 测试全都是通的。**

**原因**

Kafka 客户端连接分两个阶段：

1. 连 bootstrap server
2. 发 `METADATA` 请求，broker 返回一份**自己写的地址表**
3. 客户端**断开**，按表里的地址重新连接

第 3 步用的是 `ADVERTISED_LISTENERS` 里配的地址，**不是客户端连进来的地址**。

只配 `localhost:9092` 的话，Spark 容器拿到的地址就是 `localhost:9092`——**那是容器自己**。

**为什么难查**：TCP 层成功、元数据请求成功，只有第三步失败。所以所有连通性测试都显示"正常"。

**解法：双 listener**

```yaml
KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
```

| listener | 给谁用 |
|---|---|
| `PLAINTEXT://kafka:29092` | **容器之间**（Spark 等） |
| `PLAINTEXT_HOST://localhost:9092` | WSL 主机 / Windows 侧 |

**规则：客户端从哪条 listener 进来，broker 就报那条 listener 对应的 advertised 地址。**

**真正的验证方法**（需要一个 Kafka 客户端从别的容器发起）：

```bash
docker network ls | grep offline
docker run --rm --network offline-dw_default apache/kafka:3.8.1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

---

### 5.2 单节点必须显式设置单副本

**现象**

Kafka 启动后卡在创建内部 topic。

**原因**

Kafka 的 `__consumer_offsets` 等内部 topic **默认要 3 副本**，单节点凑不出来，一直重试超时。

**解法**

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

**StarRocks 也一样**：单 BE 建表必须写 `"replication_num" = "1"`，否则报
`Failed to find enough host with storage medium and tag`。

---

## 六、Docker 与 WSL

### 6.1 项目代码必须放在 WSL 文件系统里

**不要放在 `/mnt/d/` 或 `/mnt/e/`。**

跨文件系统读写在 WSL2 里慢 **5~10 倍**，Kafka 和 Spark 会被拖死。

放 `~/`（`/home/<user>/`）下面。

### 6.2 WSL 内存配置在 `.wslconfig`，不在单个发行版里

```ini
[wsl2]
memory=8GB
```

这个设置**对所有发行版生效**（Ubuntu + docker-desktop 共享同一个虚拟机），改了要 `wsl --shutdown` 重进。

### 6.3 Docker 磁盘镜像会疯涨，要提前挪到非系统盘

`docker_data.vhdx` 存放所有镜像、容器、数据卷，**只涨不缩**。

这套栈跑起来 30~60GB 是常态。Docker Desktop → Settings → **Resources → Advanced → Disk image location** 可以改位置（**不在 WSL Integration 那一页**）。

---

### 6.4 `wsl.exe` 默认用户是 `root` —— 依赖和属主都可能对不上

**现象（2026-10-07，同一个坑一天踩两次）**

```bash
wsl.exe -e bash -lc "cd /home/l/offline-dw && python3 scripts/gen_mock_orders.py --date 2026-09-22"
# ModuleNotFoundError: No module named 'kafka'
```

但**用户在自己终端跑同一条命令是成功的**。

**原因**：`wsl.exe` 进来的用户是 `root`，而 `kafka-python` 装在 **`l` 的 user site-packages**：

```
root 下 python3 → /usr/bin/python3        → 没有 kafka
l    下 python3 → /home/l/.local/lib/python3.10/site-packages/kafka  → 有
```

**排查方法**

```bash
whoami                                   # → root（大概率）
sudo -u l bash -lc 'python3 -c "import kafka; print(kafka.__file__)"'
```

**连带后果：临时文件属主**

以 root 跑脚本会在 `/tmp` 留下 root 属主的文件。**下次脚本降权到 `l` 跑时，重定向写入会失败**：

```
line 42: /tmp/dry_2026-09-22.txt: Permission denied
```

而重定向失败会让**整条命令的退出码变成 1**，**被误判成"业务逻辑报错"**（本次就误判成"跳天告警"）。

**解法**

```bash
# ① 整脚本降权重入（推荐，属主也一起解决）
if [ "$(whoami)" != "l" ]; then
    exec sudo -u l bash "$0" "$@"
fi

# ② 临时目录用 l 可写的位置，别用 /tmp
WORK=/home/l/replay_work

# ③ 清掉历史遗留的 root 属主文件
rm -f /tmp/dry_*.txt /tmp/send_*.txt ...
```

**通用规律**
> **"我这边能跑"和"agent 那边能跑"不是同一个环境。**
> 只要命令里有 `python`（或其他对 user site-packages 敏感的解释器），**先确认 `whoami`**。
> 而 `Permission denied` 出现在**重定向**上时，别去读业务日志 —— 先看文件属主。

---

### 6.5 用 `docker cp` 调试会往项目目录里留文件

**现象**：容器里没有编辑器，调试脚本的常见做法是

```bash
docker cp /mnt/d/.../probe.py spark:/opt/offline-dw/scripts/_probe.py
```

但 `/opt/offline-dw/scripts` 是 **bind mount 到宿主项目目录**的 —— 于是这个临时探针文件
**真的出现在 `/home/l/offline-dw/scripts/` 里，而且属主是 `root`**。

**后果**：`git status` 变脏；如果忘了删，会被误提交。

**解法**

```bash
# 拷进容器调试完，务必删除（两边都要，因为透传）
docker exec spark rm -f /opt/offline-dw/scripts/_probe.py
sudo -u l rm -f /home/l/offline-dw/scripts/_probe.py
cd /home/l/offline-dw && git status --short      # 必须回到只剩自己的改动
```

**更好的做法**：临时脚本放**项目外**再挂载/拷入，或者直接用 `spark-submit /mnt/d/...` 路径（
宿主 `/mnt/d` 在容器里不可见时才需要 `docker cp`）。

**通用规律**
> **`docker cp` 到 bind mount 的目标 = 往宿主机写文件。**
> 调试完**一定要 `git status --short` 确认项目目录只剩预期的改动**。

---

### 6.6 推送 GitHub 失败：先分清"网络错"还是"凭证错"（2026-10-07 实测）

**现象（同一晚四种报错轮流出现，很容易误判成仓库坏了）**

```
gnutls_handshake() failed: The TLS connection was non-properly terminated
Error in the HTTP2 framing layer
GnuTLS recv error (-110): The TLS connection was non-properly terminated
Failed to connect to github.com port 443 after 134883 ms: Connection timed out
```

**这些都是网络/协议层错误，和仓库、权限、提交无关** —— 本地提交一个字节都不会丢。

**⚠️ 本项目的网络环境（写在 `.wslconfig` 里的 mirrored 模式 + 代理）**

```
http_proxy / https_proxy = http://127.0.0.1:7892     ← 代理是间歇性的
```

实测代理 **第 1 次请求必秒断**（`unexpected eof`，0.1 秒），之后才正常 —— 所以"多试一次"常常就好了。

**诊断方法：用 push 端点直接看**（这一步能一刀切开"网络错"和"凭证错"）

```bash
# push 端点（不要用首页！首页 200 不代表 push 能过）
URL=https://github.com/<user>/<repo>.git/info/refs?service=git-receive-pack
timeout 12 curl -sS -o /dev/null -w "%{http_code}|%{time_total}s\n" "$URL"
```

| 返回 | 含义 |
|---|---|
| **401** | ✅ **网络通、端点可达，只是没认证** —— 问题在凭证，不在网络 |
| `SSL unexpected eof` / 000 | 代理这一跳断了 → 重试或绕开代理 |
| 超时 | 网络确实不通 |

同时对照测一次 **绕开代理**：

```bash
timeout 12 env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  curl -sS -o /dev/null -w "%{http_code}\n" "$URL"
```

**解法（按有效性排序，2026-10-07 实测）**

| # | 做法 | 结果 |
|---|---|---|
| 1 | `git -c http.version=HTTP/1.1 push origin main` | ✅ **本次靠它成功**（HTTP/2 framing 错误的标准解法）|
| 2 | 绕开代理：`env -u http_proxy -u https_proxy ... git push` | 视当时网络，本次直连一度也超时 |
| 3 | 纯重试（代理间歇性）| 有时可行 |
| 4 | 换 SSH：`ssh.github.com:443` | 彻底绕开 HTTP 代理与凭证 |

**⚠️ 两个坑**

1. **`git push --dry-run` 也需要认证** —— 公开仓库的 dry-run 一样会失败：
   ```
   fatal: could not read Username for 'https://github.com': terminal prompts disabled
   ```
   这是"禁用交互提示"的结果，**不是网络错**，别拿它当网络测试。
2. **本机凭证助手是 Windows 的 GCM**（`git-credential-manager.exe`），**在 WSL 里可能弹不出窗口而挂住**。
   挂住的解法：
   ```bash
   git config --global credential.helper store   # 换文件存储，输一次 PAT
   ```
   （PAT = GitHub Personal Access Token，勾 `repo` 权限；**不是账号密码**）

**永久配置（可选）**

```bash
# 给 GitHub 单独禁用代理（若直连更稳）
git config --global http.https://github.com/.proxy ""
# 固定用 HTTP/1.1
git config --global http.version HTTP/1.1
```

**通用规律**
> **"push 失败"要先分清是网络层还是凭证层。**
> 判据很简单：**用 `curl` 打 push 端点，401 就说明网络没问题，别再去折腾网络**。
> 而 `gnutls_handshake` / `HTTP2 framing` / `recv error -110` 全是一个家族：**传输层被打断**，
> 优先试 **HTTP/1.1** 和**换一条链路**（绕代理 / SSH）。

---

## 七、shell 引号与模板占位符

### 7.1 双引号里套双引号会被 bash 吃掉

**现象**

```bash
docker compose exec starrocks mysql -P9030 -h127.0.0.1 -uroot \
  -e "ALTER TABLE tbl SET ("dynamic_partition.enable" = "false");"
```

bash 遇到第一个内层 `"` 就认为外层字符串结束了，实际传给 mysql 的是：

```
ALTER TABLE tbl SET (dynamic_partition.enable = false);
```

引号没了 → 语法错误。

**三种解法**

| 解法 | 写法 |
|---|---|
| 内层改单引号 | `-e "ALTER TABLE t SET ('k' = 'v');"` |
| 转义 | `-e "ALTER TABLE t SET (\"k\" = \"v\");"` |
| **写进 .sql 文件** ← 推荐 | `mysql ... < xxx.sql` |

**一旦 SQL 里开始出现引号，就该用文件而不是 `-e "..."`。**

---

### 7.2 模板占位符没被替换 → SQL 变成 `> NULL` → **静默 no-op**

**现象**

SQL 文件里用占位符做参数，直接执行它：

```bash
docker exec -i starrocks mysql ... < sql/dim_product_scd2_incremental.sql
```

**没有任何报错，退出码 0，但表一行都没变。**

**原因**

SQL 里写的是：

```sql
WHERE l.snapshot_date > '${LAST}'
```

`${LAST}` 是**项目自定义的占位符**，不是 shell 变量 —— `mysql < 文件` **不会替换它**。于是实际执行的是字面量 `'${LAST}'`。

关键一步：

```
CAST('${LAST}' AS DATE)  →  NULL
```

于是条件变成：

```sql
snapshot_date > NULL     →  恒为 NULL（不是 TRUE）
```

**`WHERE` 全都过滤掉 → 0 行命中 → UPDATE 不动、INSERT 不插 → 表纹丝不动。**

**实测证据**

| 条件 | 命中行数 |
|---|---|
| `snapshot_date > '${LAST}'`（未替换）| **0** |
| `snapshot_date > '2026-09-27'`（正确替换）| **50** |
| `CAST('${LAST}' AS DATE)` | **`NULL`** |

**为什么难发现**

- **不报错**（`> NULL` 是合法表达式，只是永远不成立）
- **退出码 0**（SQL 执行成功了，只是没匹配到行）
- **"跑完了"的观感和"成功"完全一样** —— 直到你发现表没变，开始怀疑是不是逻辑写错了

> **这是 `NULL` 比较陷阱的第 N 个变体。** 同类：`BETWEEN` 遇 `NULL`（§3.7）、`DATE_ADD(哨兵值)` 返回 `NULL` 导致检查静默放过（§2.7 / DQC ②）、`NULL <> x` 不是 `TRUE`。
> **共同点：`NULL` 参与的判断既不报错、也不为真，而是"消失"。**

**本项目里正确套路的范例**：`scripts/dwd_sku_load.sh`

```bash
sed -e "s/\${D}/${D}/g" -e "s/\${DF}/${DF}/g" "$SQL_FILE" \
  | docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot
```

**解法：占位符必须有"外壳脚本"做替换，而且替换后要自检**

```bash
TMP_SQL=$(mktemp /tmp/xxx.XXXXXX.sql)
sed "s/\${LAST}/${LAST}/g" "$SQL_FILE" > "$TMP_SQL"

# ★ 关键：替换完立刻检查占位符是否真的消失了
if grep -q '\${LAST}' "$TMP_SQL"; then
    echo "❌ 占位符 \${LAST} 未被替换 —— 拒绝执行（否则会静默 no-op）"
    exit 1
fi
```

**三道防线（配这种"模板 + 外壳"脚本时都该有）**

| # | 防线 | 防什么 |
|---|---|---|
| ① | **没有新数据就提前退出并打印** | 防"成功但没做事"，让人分不清"没数据"和"脚本坏了" |
| ② | **替换后校验占位符已消失** | 防本节这个坑 —— **静默 no-op** |
| ③ | **跑完做不变式校验**（如区间无断裂、每商品一个当前版本）| 防逻辑写对但结果坏 |

**通用规律**

> **任何"SQL 模板 + 外壳脚本替换"的设计，都必须有防线②。**
> 否则**替换规则一失效（变量名改了、sed 写错、路径变了），整个脚本就退化成"什么都不做但报成功"** ——
> 而这比报错危险得多，因为**报错会让人去修，静默成功不会**。

---

## 八、一条通用的排查思路

**要证明"A 导致了 B"，光看 B 的样子不够——先让 B 消失，再看 A 能不能把它变回来。**

验证调度是否真的生效：

```bash
# 1. 手工清空所有下游表
# 2. 确认全是 0
# 3. 点执行
# 4. 数据自己回来了 → 只可能是调度器干的
```

**"先破坏，再重建"比"对比结果"强得多。**

**更省事的做法：在设计工作流时就把"清空"节点放进去。** 它既解决了幂等问题，又让整个流程变成自证的——不用事后想办法证明。
