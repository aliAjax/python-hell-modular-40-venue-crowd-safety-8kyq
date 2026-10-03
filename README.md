# 大型场馆人群安全与现场指挥

只使用Python标准库和SQLite的模块化服务，默认端口`8340`。支持场馆区域、入场口、容量、通道、安保岗位、医疗点、事件、限流、开放通道、疏散和医疗任务、人员到位、区域恢复、延迟重复事件和容量冲突。

入场、区域和事件已接成**额度账**：区域余量、场馆总容量、进场口放行上限三道额度在放行时同一事务一起扣，哪一道不够就整笔拒绝。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期（启动时执行额度回填迁移）。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：容量计算、事件优先级、状态机、团队冲突约束和三类额度上限计算。
- `src/repository.py`：SQLite持久化、乐观锁、幂等、额度账户/流水/放行批次和审计查询。
- `src/service.py`：用例编排、权限校验、版本控制、原子放行、额度重算、迁移回填与对账。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8340
```

## 核心对象

`venue`为场馆，`zone`为区域，`gate`为入场口，`post`为安保岗位，`medical_point`为医疗点，`incident`为事件，`task`为现场任务。

## 额度账

每个区域、场馆、进场口各有一个额度账户（`quota_accounts`，上限`limit_amount`+已用`used_amount`），每次变动记一条流水（`quota_entries`），每批放行记一条批次（`admission_batches`，即幂等键）。

- **三道额度一起扣**：`zone.admit`在同一事务里扣区域余量、场馆总容量、进场口放行上限；任一道剩余不足就整笔拒绝，一道都不扣，区域占用不变。
- **上限来源**：区域按状态取`capacity`/`admit_limit`/0，再减去未结案事件的处置预留（按严重度`medium`5%、`high`10%、`critical`20%区域容量）；场馆限流用`capacity_limit`，否则用`total_capacity`或各区域容量之和；进场口限流用`flow_limit`，开放不限量，关闭为0。
- **并发**：两个操作员同时提交同一区域放行，事务串行化后后到的按最新余量校验，不够就整笔拒绝；写冲突自动重试。
- **状态联动**：区域限流/疏散/恢复/关闭、场馆限流/关闭/重开、进场口限流/恢复/重开、事件升降级/结案/重开，都会在同一事务里重算对应额度上限，还没放行的部分随上限下调释放（流水记`recalc`）；进场口重新开放时本窗口已用量清零（流水记`reset`）。
- **幂等与回滚**：放行可带`idempotency_key`（或`Idempotency-Key`请求头），中途失败时扣掉的额度和放行记录一起回滚，用同一键重试不会记两遍。
- **迁移回填**：旧数据升级时`migrate_quotas()`按现有占用回填初始额度（流水记`backfill`），可重复执行；`verify_quota_ledger()`校验区域账户已用=区域占用、场馆账户已用=各区域占用之和。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/quotas`：额度账户列表
- `GET /api/ledger`，可用`?account_id=`、`?batch_id=`过滤：额度流水
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建，对`admit`等动作同样生效（同一批次不会记两遍）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

容量和调度规则为可运行的简化模型，不接入闸机、视频分析、室内定位、消防联动或真实应急指挥系统。事件处置预留比例为固定档位，进场口放行量按开放窗口累计，不模拟按时间窗自动重置。
