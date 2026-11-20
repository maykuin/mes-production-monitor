# mes-production-monitor · MES 五地生产报工实时监控预警平台

**模拟企业级 MES 场景**：5 个生产基地 × 新旧两代 MES = 10 个 MySQL 库，报工分「手动录入 + 过线扫码」两套通道。用 **Flink CDC → Kafka → Flink → ClickHouse** 把原有「30 分钟批报表」改造成秒级实时链路：报工明细秒级可查、产量/达成率实时计算、超报/滞后/漏报三分类告警 + 分级升级。

- 技术设计：[technical-design.md](./technical-design.md)
- 测试手册：[test-runbook.md](./test-runbook.md)
- 踩坑记录：[TROUBLESHOOTING.md](./TROUBLESHOOTING.md)

## 技术栈

| 组件 | 版本 |
|---|---|
| Flink | 1.18.1 |
| Flink CDC MySQL | 3.2.1 |
| Kafka | 3.6.1（3 节点） |
| ClickHouse | 24.3 |
| MySQL | 8.0（10 库同实例） |
| Java | 8 |

## 工程结构

| 模块 | 职责 |
|---|---|
| common | POJO / 枚举 / 配置加载 / 班次工具 / CK 攒批 Sink |
| datagen | 造数：`backfill` / `streaming` / 异常注入 |
| cdc | 作业1：MySQL CDC → `ods-mes-report` + `ods-mes-order` |
| dwd | 作业2：维表广播 join → `dwd-mes-report` + CK |
| dws | 作业3：分钟聚合 + 达成率 → CK |
| alert | 作业4：超报/滞后/漏报 → `mes-alert` + CK + 钉钉 |
| docker | 本地环境（MySQL / Kafka / ClickHouse） |
| tools/reconcile | 对账脚本 |

## 快速开始

```bash
cd docker && docker compose up -d      # 本地起 MySQL + Kafka + ClickHouse
mvn -q clean install -DskipTests        # 编译
```

本地 IDEA 依次运行 4 个作业主类（配置见各模块 `*.properties`），造数用 `datagen`。

集群（YARN）提交、逐层验证、对账的完整步骤见 [test-runbook.md](./test-runbook.md)。
