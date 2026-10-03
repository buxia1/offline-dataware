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

**这是设计使然，不是 bug** —— ODS 是追加层，重复由 DWD 去重解决。

**但做实验时会把数字搞乱**。判断方法：看重复行数是不是**正好是整数倍**（我们遇到过 116 和 66，正好是单批次 58 和 33 的两倍）。

**解法**

- 做对照实验前，先删 topic 重建 + `TRUNCATE` 下游表
- 生产环境改用增量（`startingOffsets=latest` + 维护 offset），或换 StarRocks Routine Load

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

## 七、shell 引号

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
