# Test Runbook

从零重测清单，所有命令在集群 hadoop1 执行。前置：4 个 fat jar 已上传到 `/lib/`。

## 速查

| 作业 | 消费组 | 读 → 写 |
|---|---|---|
| CDC | 无（读 binlog） | MySQL → `ods-mes-report` + `ods-mes-order` |
| DWD | `mes-production-monitor-dwd` | ods → `dwd-mes-report` + CK `dwd_mes_report` |
| DWS | `mes-production-monitor-dws` | `dwd-mes-report` → CK `dws_report_1min` + `dws_order_achievement` |
| 告警 | `mes-production-monitor-alert` | `dwd-mes-report` → CK `ads_alert` + `mes-alert` |

连通性检查：先确认 MySQL、ClickHouse、Kafka 能连通。

> 只有 3 个 consumer group（DWD/DWS/告警），CDC 读 binlog 无 group，靠清 checkpoint 重置。

## 1. 清空 MySQL 表（含维表）

```bash
# 一次清空 10 库所有表（含维表；用 information_schema 自动发现表，不区分新旧库）
mysql -hhadoop1 -uroot -p'TMcode@0204' -N <<'SQL' | mysql -hhadoop1 -uroot -p'TMcode@0204'
SELECT CONCAT('TRUNCATE TABLE `', table_schema, '`.`', table_name, '`;')
FROM information_schema.tables
WHERE table_schema REGEXP '^mes_(old|new)_';
SQL
```

## 2. 重建 ClickHouse

```bash
# 01-ddl.sql 先上传到 hadoop1:/lib/：scp docker/clickhouse/init/01-ddl.sql hadoop1:/lib/
# 先删库，再重放 DDL 重建（DDL 是 CREATE ... IF NOT EXISTS，必须先 DROP 才会重建）
clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' \
  -q "DROP DATABASE IF EXISTS mes_production_monitor;"
  
clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' \
  --multiquery < /lib/01-ddl.sql
```

## 3. 重置 Kafka

```bash
# ① 删 consumer group（作业已停才能删）
kafka-consumer-groups.sh --bootstrap-server hadoop1:9092 --group mes-production-monitor-dwd --delete
kafka-consumer-groups.sh --bootstrap-server hadoop1:9092 --group mes-production-monitor-dws --delete
kafka-consumer-groups.sh --bootstrap-server hadoop1:9092 --group mes-production-monitor-alert --delete

# ② 删 + 重建 topic（不存在的 --delete 报错忽略）
kafka-topics.sh --bootstrap-server hadoop1:9092,hadoop2:9092,hadoop3:9092 --delete --topic ods-mes-report
kafka-topics.sh --bootstrap-server hadoop1:9092,hadoop2:9092,hadoop3:9092 --delete --topic ods-mes-order
kafka-topics.sh --bootstrap-server hadoop1:9092,hadoop2:9092,hadoop3:9092 --delete --topic dwd-mes-report
kafka-topics.sh --bootstrap-server hadoop1:9092,hadoop2:9092,hadoop3:9092 --delete --topic mes-alert

kafka-topics.sh --bootstrap-server hadoop1:9092,hadoop2:9092,hadoop3:9092 --create --topic ods-mes-report  --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server hadoop1:9092,hadoop2:9092,hadoop3:9092 --create --topic ods-mes-order   --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server hadoop1:9092,hadoop2:9092,hadoop3:9092 --create --topic dwd-mes-report  --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server hadoop1:9092,hadoop2:9092,hadoop3:9092 --create --topic mes-alert       --partitions 3 --replication-factor 3
```

> 删 topic 后重建仍报「已存在」= broker 关了 `delete.topic.enable`，去 `server.properties` 打开。

## 4. 清 checkpoint

```bash
hdfs dfs -rm -r /checkpoint/cdc /checkpoint/dwd /checkpoint/dws /checkpoint/alert
```

## 5. 灌数（backfill）

```bash
java -jar /lib/mes-production-monitor-datagen-1.0-SNAPSHOT.jar --mode backfill --backfill.days 7
```

> backfill 已含灌维度表 + 订单 + 报工，不用单独 `--mode dim`。

## 6. CDC → 验证

```bash
flink run-application -t yarn-application \
  -Dyarn.application.name=mes-cdc \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=1024m \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -c com.mes.production.monitor.cdc.CdcToKafkaJob \
  /lib/mes-production-monitor-cdc-1.0-SNAPSHOT.jar \
  --job.parallelism 2
```

等 RUNNING 后验证 ODS：

```bash
kafka-console-consumer.sh --bootstrap-server hadoop1:9092 \
  --topic ods-mes-report --from-beginning \
  --isolation-level read_committed --property print.key=true \
  --property key.separator=' | '
```

✅ `db_name` 覆盖 10 库、`op` 有 c/u/d、`record_id` 非空。

## 7. DWD → 验证

```bash
flink run-application -t yarn-application \
  -Dyarn.application.name=mes-dwd \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=1024m \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -c com.mes.production.monitor.dwd.DwdWideTableJob \
  /lib/mes-production-monitor-dwd-1.0-SNAPSHOT.jar
```

```bash
clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' -q "
SELECT outputtype, count() AS total,
       countIf(productionorder = '') AS order_miss,
       countIf(partcode = '') AS part_miss,
       countIf(linecode = '') AS line_miss,
       countIf(workshop = '') AS workshop_miss
FROM mes_production_monitor.v_dwd_mes_report
GROUP BY outputtype;"
```

✅ 参考基线（本次实测，重测应一致）：
- 总笔数 = 33674（BARCODE 14580 + MANUAL 19094）
- `order_miss` = 0（订单都能关联上）
- `part_miss` = 44（MANUAL）、`workshop_miss` = 25（BARCODE）/ 44（MANUAL）——悬空外键造成的少量 miss，接近 0 即可
- `line_miss` = 14580 = BARCODE 全部，属正常（扫码无 workcenterid）

## 8. DWS → 验证

```bash
flink run-application -t yarn-application \
  -Dyarn.application.name=mes-dws \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=2048m \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -c com.mes.production.monitor.dws.DwsMetricsJob \
  /lib/mes-production-monitor-dws-1.0-SNAPSHOT.jar \
  --job.parallelism 1
```

```bash
clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' \
  -q "SELECT sum(output_quantity) FROM mes_production_monitor.dws_report_1min;"
clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' \
  -q "SELECT sum(lotquantity) FROM mes_production_monitor.v_dwd_mes_report;"
```

✅ 参考基线（本次实测，重测应一致）：`sum(output_quantity)` = 568334，`sum(lotquantity)` = 568287，差 47（≈0.008%，在 0.5% 对账阈值内）。

> 差 47 不是迟到数据丢失：DWS 里超出 `allowedLateness` 的迟到增量会走 `sideOutputLateData` 旁路，仍重插回 `dws_report_1min`（SummingMergeTree 自动求和），不丢数。且方向是 DWS **多** 47（不是少），属水印/窗口边界上的极小误差。

## 9. 告警 → 验证

```bash
flink run-application -t yarn-application \
  -Dyarn.application.name=mes-alert \
  -Djobmanager.memory.process.size=1024m \
  -Dtaskmanager.memory.process.size=1024m \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -c com.mes.production.monitor.alert.AlertJob \
  /lib/mes-production-monitor-alert-1.0-SNAPSHOT.jar
```

```bash
yarn application -list    # 4 个应 RUNNING
```

① 超报 → `OVER_REPORT`：

```bash
java -jar /lib/mes-production-monitor-datagen-1.0-SNAPSHOT.jar --mode anomaly --anomaly.type over-report
clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' -q "
SELECT alert_type, level, company, productionorder, order_quantity, report_quantity, achieve_rate
FROM mes_production_monitor.v_ads_alert WHERE alert_type='OVER_REPORT' ORDER BY alert_time DESC LIMIT 10;"
```

② 滞后 → `LAG`：

```bash
java -jar /lib/mes-production-monitor-datagen-1.0-SNAPSHOT.jar --mode anomaly --anomaly.type lag
clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' -q "
SELECT alert_type, level, company, productionorder, order_quantity, report_quantity, achieve_rate
FROM mes_production_monitor.v_ads_alert WHERE alert_type='LAG' ORDER BY alert_time DESC LIMIT 10;"
```

③ 漏报 → `MISSING_REPORT`（约 2 分钟触发 + 3 分钟恢复）：

```bash
# 终端 1：持续造数，保持所有线活跃
java -jar /lib/mes-production-monitor-datagen-1.0-SNAPSHOT.jar --mode streaming
# 终端 2：断流注入
java -jar /lib/mes-production-monitor-datagen-1.0-SNAPSHOT.jar --mode anomaly --anomaly.type line-idle
# 验证
clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' -q "
SELECT alert_type, level, company, linecode, alert_time, recover_time
FROM mes_production_monitor.v_ads_alert WHERE alert_type='MISSING_REPORT' ORDER BY alert_time DESC LIMIT 10;"
```

> `level=NONE` 是「恢复/关闭」记录，不是告警级别：`LAG` 和 `MISSING_REPORT` 可恢复（达成率追上 / 产线重新报工），恢复时用同一 `alertId` 输出 `level=NONE` + `recoverTime`，CK ReplacingMergeTree 覆盖原记录（不是新增一条）。`OVER_REPORT` 不可恢复、不写 NONE。只想看「告警中」就过滤 `level != 'NONE'`。

## 10. 对账（可选）

一次性：把两个对账 SQL 上传到 hadoop1（只在 Windows 本机执行一次）：

```bash
scp tools/reconcile/mysql_baseline.sql hadoop1:/lib/
scp tools/reconcile/clickhouse_result.sql hadoop1:/lib/
```

然后在 hadoop1 直接执行（时间范围自动取，覆盖 backfill 的 7 天）：

```bash
FROM=$(date -d '7 days ago' '+%Y-%m-%d 08:30:00')
TO=$(date -d '+1 day' '+%Y-%m-%d 08:30:00')

# ① MySQL 基线（真值，逐库循环 10 库）
for company in zh zz hf wh cq; do
  for prefix in mes_old mes_new; do
    sed "s/{{DB}}/${prefix}_${company}/g; s/{{FROM}}/$FROM/g; s/{{TO}}/$TO/g" \
      /lib/mysql_baseline.sql | mysql -hhadoop1 -uroot -p'TMcode@0204'
  done
done

# ② ClickHouse 结果（Flink 结果）
sed "s/{{FROM}}/$FROM/g; s/{{TO}}/$TO/g" /lib/clickhouse_result.sql \
  | clickhouse-client --host 127.0.0.1 --port 9001 -u default --password 'TMClickhouse0204' --multiquery
```

**验证（比对 `report_qty`，两边应一致）**：

**① 总量对账**

```sql
-- CK 总量（参考值 ≈ 568287）
SELECT sum(lotquantity) FROM mes_production_monitor.v_dwd_mes_report;

-- MySQL 总量（粘到 mysql，跨 10 库 lot + 5 旧库扫码）
SELECT
  (SELECT SUM(l.finishedquantity) FROM mes_old_zh.lot l JOIN mes_old_zh.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_new_zh.lot l JOIN mes_new_zh.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_old_zz.lot l JOIN mes_old_zz.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_new_zz.lot l JOIN mes_new_zz.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_old_hf.lot l JOIN mes_old_hf.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_new_hf.lot l JOIN mes_new_hf.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_old_wh.lot l JOIN mes_old_wh.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_new_wh.lot l JOIN mes_new_wh.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_old_cq.lot l JOIN mes_old_cq.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_new_cq.lot l JOIN mes_new_cq.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(b.quantity) FROM mes_old_zh.barcodeautomatic b JOIN mes_old_zh.productionorder p ON p.productionorderid=b.productionorderid WHERE b.quantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(b.quantity) FROM mes_old_zz.barcodeautomatic b JOIN mes_old_zz.productionorder p ON p.productionorderid=b.productionorderid WHERE b.quantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(b.quantity) FROM mes_old_hf.barcodeautomatic b JOIN mes_old_hf.productionorder p ON p.productionorderid=b.productionorderid WHERE b.quantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(b.quantity) FROM mes_old_wh.barcodeautomatic b JOIN mes_old_wh.productionorder p ON p.productionorderid=b.productionorderid WHERE b.quantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(b.quantity) FROM mes_old_cq.barcodeautomatic b JOIN mes_old_cq.productionorder p ON p.productionorderid=b.productionorderid WHERE b.quantity>0 AND p.productionorderno NOT LIKE 'FG%')
AS mysql_total;
```

**② 按公司抽验**（以 zh 为例，zz/hf/wh/cq 同理）

```sql
-- CK：zh 基地
SELECT sum(lotquantity) FROM mes_production_monitor.v_dwd_mes_report WHERE company='zh';

-- MySQL：zh = mes_old_zh + mes_new_zh
SELECT
  (SELECT SUM(l.finishedquantity) FROM mes_old_zh.lot l JOIN mes_old_zh.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(l.finishedquantity) FROM mes_new_zh.lot l JOIN mes_new_zh.productionorder p ON p.productionorderid=l.productionorderid WHERE l.finishedquantity>0 AND p.productionorderno NOT LIKE 'FG%')
+ (SELECT SUM(b.quantity) FROM mes_old_zh.barcodeautomatic b JOIN mes_old_zh.productionorder p ON p.productionorderid=b.productionorderid WHERE b.quantity>0 AND p.productionorderno NOT LIKE 'FG%')
AS zh_total;
```

✅ 两边 `report_qty` 一致（差异 < 0.5%）即通过。只比 `report_qty`（**不比 `record_cnt`**，扫码通道笔数口径不同：基线按 LPN 分组、CK 是明细粒度）；三粒度（基地×班次、车间×产线、订单级）差异率 < 0.5%。

## 11. 停掉所有作业

```bash
yarn application -list
yarn application -kill <appId>    # 逐个 kill
flink list -a                     # 确认无 RUNNING
```

---

## 常见坑

- 第 3/4 个作业卡 ACCEPTED → YARN AM 上限（`maximum-am-resource-percent`，已改 0.5）。
- DWS OOM → `--job.parallelism 1`（batch 已缩到 100）；仍 OOM 把 TM 提到 2048m。
- 告警 OOM → 单独把告警 TM 提到 1536m。
- CK 查出来偏大 → 走 `v_` 视图（FINAL 去重）。
