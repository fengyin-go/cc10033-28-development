# 维度建模开发中心

面向数据仓库建设场景，提供维度建模、事实表设计、缓慢变化维和汇总层构建的建模平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 维度定义 | `dim_definition` | 维度表 | 维度编码、维度名称、主键定义 |
| 维度属性 | `dim_attr` | 属性字段 | 属性名称、所属维度、数据类型 |
| 事实设计 | `fact_design` | 事实表 | 事实名称、业务过程、粒度声明 |
| 度量配置 | `measure_config` | 度量字段 | 度量名称、所属事实、度量类型 |
| SCD处理 | `scd_handler` | SCD策略 | 策略名称、目标维度、SCD类型 |
| 代理键 | `surrogate_key` | 键值定义 | 键值名称、目标维度、生成策略 |
| 层级构建 | `hierarchy_build` | 层级关系 | 层级名称、所属维度、父子字段 |
| 星型查询 | `star_query` | 查询定义 | 查询名称、事实表、关联维度 |
| 汇总构建 | `agg_summary` | 汇总表 | 汇总名称、源事实表、汇总维度 |
| 桥接关联 | `dim_bridge` | 桥接表 | 桥接名称、多对多维度、关联事实 |
| ETL映射 | `etl_mapping` | 映射规则 | 规则名称、源表、目标表 |
| 快照计划 | `snapshot_plan` | 快照策略 | 策略名称、目标表、快照周期 |
| 模型血缘 | `model_lineage` | 依赖关系 | 关系ID、上游模型、下游模型 |
| 一致性维度 | `conform_dim` | 标准维度 | 标准名称、维度主题、标准键值 |
| DataVault | `data_vault` | Vault模型 | 模型名称、Hub实体、Link关系 |
| 多维立方 | `cube_builder` | Cube定义 | Cube名称、源事实表、维度组合 |
| 变更日志 | `slow_change_log` | 维度变更 | 变更ID、目标维度、变更字段 |
| 日期维度 | `date_dim_gen` | 日期记录 | 日期键、完整日期、年份 |
| 模型校验 | `model_validate` | 校验规则 | 规则名称、目标模型、校验类型 |
| 部署流水线 | `deploy_pipeline` | 部署任务 | 任务名称、目标模型、部署环境 |

## DataVault 批次清单

`data_vault` 的「执行加载」是批量装载入口：每次调用都会在 `vault_batch`
落一个批次头、在 `vault_batch_row` 落逐行清单，记录源行标识、业务键、
Hub/Link/Satellite 哈希键、输入判定和原始来源数据。

- `app/services/vault_manifest.py`：逐行判定 `新增` / `重复提交` / `Satellite更正` /
  `迟到更正` / `拒绝`。重复提交不新增 Hub、Link 或卫星版本；卫星属性真实变化时
  保留旧值和新值，并按源时间区分迟到更正。
- `app/services/vault_lineage.py`：消费批次清单，为已确认的 Hub/Link 建立实际装载
  来源关系，为卫星更正记录来源源行和批次；重复提交与拒绝行不建关系。
- `app/routers/vault.py`：`POST /api/data_vault/batches`（批量上报）、
  `GET /api/data_vault/batches/{批次号}`（批次逐行明细）、
  `POST /api/model_lineage/vault-relations/consume`（血缘消费）。

批量装载有三种等价入口：

```bash
# 1) 专用装载接口：按模型名称或 entry_id 提交一批源记录
curl -X POST http://127.0.0.1:8000/api/data_vault/batches \
  -H 'Content-Type: application/json' \
  -d '{"model_name":"CustomerHub","batch_id":"VB-001","records":[
        {"源行标识":"R1","customer_id":"K1","customer_level":"普通"},
        {"源行标识":"R2","customer_id":"K1","customer_level":"普通"},
        {"源行标识":"R3","customer_id":"K1","customer_level":"黄金"}]}'

# 2) 经典动作接口：records 可放在顶层或 context 里
curl -X POST http://127.0.0.1:8000/api/data_vault/3/actions \
  -H 'Content-Type: application/json' \
  -d '{"action":"执行加载","batch_id":"VB-002","records":[{"源行标识":"R1","customer_id":"K1"}]}'

# 3) 回查批次与触发血缘消费
curl http://127.0.0.1:8000/api/data_vault/batches/VB-001
curl -X POST http://127.0.0.1:8000/api/model_lineage/vault-relations/consume
```

同一批里可以同时出现原行、完全重复行和改值行，清单会分别标成
`新增`、`重复提交`、`Satellite更正`；与历史版本一致但源时间更早的行标成
`迟到更正` 并保留为历史版本。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
