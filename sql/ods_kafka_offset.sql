CREATE TABLE IF NOT EXISTS ods.ods_kafka_offset (
    topic         VARCHAR(64) NOT NULL COMMENT "topic 名",
    partition_id  INT         NOT NULL COMMENT "分区号",
    next_offset   BIGINT      NOT NULL COMMENT "下一条要读的 offset（已消费 + 1）",
    update_time   DATETIME             COMMENT "更新时间"
) ENGINE = OLAP
PRIMARY KEY(topic, partition_id)
DISTRIBUTED BY HASH(topic) BUCKETS 1
PROPERTIES ("replication_num" = "1");
