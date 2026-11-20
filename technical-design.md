# MES 实时生产监控预警平台 —— 技术设计

> 用 Flink CDC / Flink / Kafka / ClickHouse 把「30 分钟批生产报表」改造成秒级实时链路。

## 1. 项目概述

**业务背景**：一家制造业企业有 5 个生产基地（珠海/郑州/合肥/武汉/重庆），每个基地跑着新旧两代 MES，报工数据散落在 10 个 MySQL 库中。报工有两类通道：工人在 MES 上手工录入完成量（`lot` 表），旧 MES 另有条码过线扫码通道（`barcodeautomatic` 表，条码过线自动累加数量）。

企业原有的「五地生产报表」靠定时任务每 30 分钟跑一次 SQL 汇总，存在三个痛点：

- **延迟高**：报工最多 30 分钟才反映到报表，超报 / 滞后 / 漏报无法及时发现，往往下班对账才发现。
- **口径分散**：新旧 MES 两套通道口径不一致（手动按 `finishedquantity`、扫码按 LPN 分组求和），靠人工在报表 SQL 里 UNION + 过滤维护，易出错。
- **查询慢**：报表直查 MySQL，无法支撑大屏秒级下钻。

**目标**：把「30 分钟批报表」改造成 **Flink CDC → Kafka → Flink → ClickHouse** 的秒级链路——报工秒级可查、实时算产量与达成率、异常三分类告警 + 分级升级。

**价值**：监控时效 **30 分钟 → 秒级**；异常发现从「人看报表」变为「系统主动告警」；10 库 2 套口径统一成一张实时宽表。

## 2. 业务与数据

### 2.1 基地拓扑（10 库同实例）

| 基地 | 旧 MES 库 | 新 MES 库 | company |
|---|---|---|---|
| 珠海 | mes_old_zh | mes_new_zh | zh |
| 郑州 | mes_old_zz | mes_new_zz | zz |
| 合肥 | mes_old_hf | mes_new_hf | hf |
| 武汉 | mes_old_wh | mes_new_wh | wh |
| 重庆 | mes_old_cq | mes_new_cq | cq |

10 库放同一 MySQL 实例：CDC 用 `database-name = 'mes_(old|new)_.*'` 正则一次匹配全部库（避免 10 个 binlog client）。`company` / `datasource` 由 CDC 元数据列 `database_name` 解析（前缀 → datasource，后缀 → company）。

### 2.2 报工通道

| 通道 | 表 | 说明 |
|---|---|---|
| 手动报工 | `lot`（新旧都有） | `finishedquantity` 完成量 |
| 过线扫码 | `barcodeautomatic`（仅旧库） | `quantity` 累加 |

关键：**旧 MES 两套通道，新 MES 只有手动**，两套最终 UNION 成同一张明细。

### 2.3 关键源字段

- `lot`：lotid(PK) / productionorderid / finishedquantity / createddate(事件时间)
- `barcodeautomatic`：barcode(PK) / lpnbarcode / productionorderid / quantity / scantime(事件时间)
- `productionorder`：productionorderid(PK) / productionorderno / orderquantity / createddate / duedate
- 维表：user / workcenter(→worksite) / worksite / part

### 2.4 原报表口径（对账基线，必须严格一致）

1. 班次日界 **08:30**（`DATE(DATE_SUB(time, INTERVAL 510 MINUTE))`）。
2. 过滤 `finishedquantity > 0`、订单号 `NOT LIKE 'FG%'`。
3. 扫码按 `(lpnbarcode, productionorderid, modelencode, model, users)` 分组 `SUM(quantity)`、`MAX(scantime)`。
4. 两通道都用 `productionorderid` 关联订单表取 `productionorderno`（DWD 同时保留 ID 与编号）。

## 3. 总体架构

分层：**ODS(Kafka) → DWD(宽表) → DWS(聚合) → ADS(告警)**。

| 作业 | 实现 | 输入 → 输出 |
|---|---|---|
| 作业1 CDC | Flink SQL（3 个 MySQL CDC source） | MySQL binlog → `ods-mes-report` + `ods-mes-order` |
| 作业2 DWD | DataStream（广播维表 join） | ods → `dwd-mes-report` + CK `dwd_mes_report` |
| 作业3 DWS | DataStream（窗口 + 有状态） | `dwd-mes-report` → CK `dws_report_1min` + `dws_order_achievement` |
| 作业4 告警 | DataStream（KeyedProcessFunction + Timer） | `dwd-mes-report` → `mes-alert` + CK `ads_alert` + 钉钉 |

## 4. 技术选型

| 组件 | 版本 |
|---|---|
| Flink | 1.18.1 |
| Flink CDC MySQL | 3.2.1 |
| Kafka | 3.6.1 |
| ClickHouse | 24.3（clickhouse-jdbc 0.8.6） |
| MySQL | 8.0 |
| Java | 8 |
| Hadoop | 3.3.6（YARN + HDFS） |

Flink 作业是纯 Java 应用（非 Spring Boot）。

## 5. 数据模型

### 5.1 Kafka Topic

| Topic | 分区键 | 分区 |
|---|---|---|
| `ods-mes-report` | `company:datasource:productionorderid` | 6 |
| `ods-mes-order` | `company:datasource:productionorderid` | 3 |
| `dwd-mes-report` | `company:datasource:productionorder` | 6 |
| `mes-alert` | `company:linecode:productionorder` | 3 |

### 5.2 DWD 宽表关键字段

- `record_id` = lotid / barcode（业务唯一键）；`version` = CDC `ts_ms`。
- 保留 `productionorderid`（达成率需先按订单聚合）+ `productionorder`（订单编号）。
- `shift_date` = 08:30 日界**显式算好写进 DWD**（不依赖窗口 offset，见 6.3）。

### 5.3 ClickHouse 表（DDL 见 `docker/clickhouse/init/01-ddl.sql`）

| 表 | 引擎 | 去重/排序键 | 要点 |
|---|---|---|---|
| `dwd_mes_report` | ReplacingMergeTree(version) | `(company, datasource, shift_date, outputtype, record_id)` | 键不放 `productionorder`（可变维度）；`is_deleted` 墓碑 |
| `dws_report_1min` | SummingMergeTree((output_quantity, record_cnt)) | `(company, ts_minute, workshop, linecode, partcode)` | 带符号增量累加，迟到直接 INSERT 补数 |
| `dws_order_achievement` | ReplacingMergeTree(version) | `(company, datasource, shift_date, productionorder)` | 加 `datasource` 消歧同名订单 |
| `ads_alert` | ReplacingMergeTree(version) | `(company, alert_type, alert_id)` | 升级只改 level+version，覆盖不新增 |

所有表配 `v_` 视图（`SELECT ... FINAL`），下游/对账一律走视图。ReplacingMergeTree 只在后台 merge 去重，查询不加 FINAL 会看到重复行、结果偏大。

### 5.4 字段映射

| 宽表字段 | 手动 lot | 扫码 barcodeautomatic |
|---|---|---|
| productionorderid | lot.productionorderid | 直接 |
| productionorder | 经 productionorderid join | 同 |
| lotquantity | finishedquantity | SUM(quantity)（LPN 分组） |
| outputdatetime | createddate | MAX(scantime) |
| linecode/linename | workcenter | 空（扫码无 workcenterid） |

## 6. Flink 作业设计

### 6.1 作业1 CDC（ODS）

- 3 个 source（正则匹配库名，非每库一个）：`lot` / `barcodeautomatic` / `productionorder`。
- 报工写 Kafka 用 **`kafka` + `debezium-json`**（保留完整 changelog 含 `-U` 旧值）。不能用普通 `json` 只取新值——报工 UPDATE 修正会重复累加，只有对账才发现。
- `server-id` 必须配互不重叠区间（每个并行子任务占一个）；`scan.startup.mode=initial`。

### 6.2 作业2 DWD

- DataStream，维表用 Broadcast State（数据小）。
- **订单维是 SCD**：流采集（CDC → `ods-mes-order`）+ 维表消费（广播按 `productionorderid` join）；`open()` 里 JDBC 全量 bootstrap 兜底，解决广播流晚于主流的竞态（维度全空）。
- 扫码**不聚合**（明细原样下发，聚合边界交给 DWS 窗口显式定义）。
- 过滤 `lotquantity>0`、`NOT LIKE 'FG%'`；维度 miss 加 `dim_miss_count` Counter（静默失败必须监控）。

### 6.3 作业3 DWS

- **分钟产量**：`ChangelogNormalizer` 把明细归一化成带符号增量（修正/删除产负数回撤）→ 1min 窗口 → SummingMergeTree；迟到走 side output 补数。
- **达成率**：按 `(company+datasource+shift_date+productionorder)` 累计，**维度聚合交给 CK 查询侧现算**（不做流上二级聚合，避免 retract 复杂度）。
- **8:30 班次日**：显式算 `shift_date`，不依赖窗口 offset（Flink 窗口基于 UTC epoch，08:30 的 offset 是 0.5h 不是 8.5h，算错会整体偏移 8 小时）。
- 分钟 + 订单两粒度同一作业多路 sink，不拆两个作业（避免重复消费、口径漂移）。

### 6.4 作业4 告警

- **订单 keyBy**：超报（累计报工 > 订单量，**不可自动恢复**）+ 滞后（达成率 < 计划进度 − 阈值，可恢复）。
- **产线 keyBy**：漏报（产线连续无报工）用**处理时间 Timer**——事件时间 Timer 在「无数据 → watermark 不推进」时永不触发。
- 分级升级 P3→P2→P1（升级前先删旧 Timer）。
- 状态两层清理：业务主动 clear + `StateTtlConfig` 兜底（RocksDB + 增量 checkpoint）。
- 钉钉限流 + 聚合发送（同一产线/类型合并）。

## 7. 指标与告警规则

| 指标 | 定义 |
|---|---|
| 实时产量 | SUM(lotquantity) |
| 达成率 | 先订单聚合报工量，再维度聚合（避免 orderquantity 重复累加） |
| 计划进度 | 已用时间 / 生产周期（线性爬坡） |

| 告警 | 触发 | 默认阈值 |
|---|---|---|
| 超报 OVER_REPORT | 累计报工 > 订单量 | 1.0 |
| 滞后 LAG | 达成率 < 计划进度 − 阈值 | 0.2 |
| 漏报 MISSING_REPORT | 产线连续无报工 | 60 分钟 |

阈值全在 `alert.properties` 配置，改配置不改代码。

## 8. 正确性设计

| 问题 | 方案 |
|---|---|
| 报工 UPDATE 修正 | debezium-json 保留 changelog + CK ReplacingMergeTree 取最新 |
| 扫码天然多条 | DWD 不聚合，DWS 窗口定义边界 |
| 达成率重复计数 | 先订单聚合再维度聚合 |
| 乱序/迟到 | watermark 60s + side output 补数 |
| 端到端一致性 | Kafka exactly-once（2PC）；CK 幂等去重最终一致 |

**对账**：原报表口径查 MySQL 出基线，查 CK 出结果，`(company, shift_date)`、`(company, workshop, linecode)`、订单级三粒度比对 `report_qty`，差异 < 0.5%（`tools/reconcile/*.sql`）。CK 必须走 `v_` 视图。

## 9. 性能与容错

- checkpoint 60s，RocksDB + 增量 checkpoint。
- 分钟聚合支持**加盐两阶段聚合**（`agg.salt.buckets`，默认 1 关闭）。
- CK 写入自定义 `ClickHouseBatchSink`：条数 + 时间双触发攒批，checkpoint 时强制 flush。
- 可观测 metrics：`dirty_record_count` / `filtered_record_count` / `dim_miss_count` / `late_record_count` / `alert_fired_count` / `state_size_mb`。

## 10. 已知坑（详见 TROUBLESHOOTING.md）

1. Flink / CDC / Kafka connector 版本必须严格对齐。
2. MySQL 前置：binlog ROW、server-id 唯一、账号 REPLICATION 权限。
3. changelog 写 append-only sink 报错；绕开报错只取新值 = 修正重复累加。
4. 维表广播竞态（open() JDBC bootstrap 兜底）。
5. 漏报 Timer 用事件时间永不触发。
6. 状态无限膨胀（订单键无界，两层清理）。
7. CK Too many parts（高频小批量 INSERT，需攒批）。
8. ReplacingMergeTree 查询不加 FINAL 偏大。
9. 班次日界 offset 算错 8 小时。
10. POJO 不满足规范静默退化 Kryo（用 `disableGenericTypes()` 暴露）。
11. checkpoint 路径 local 用 `file://`、YARN 用 `hdfs://`。

## 11. 里程碑

M0 最小垂直切片 → M1 造数（backfill + streaming + 异常注入）→ M2 CDC → M3 DWD → M4 DWS → **M4.5 对账** → M5 告警 → M6 大屏。

> M0（单库单表跑通，先清完环境/版本坑）和 M4.5（对账证明「算对了」）是两个不能跳的关卡。
