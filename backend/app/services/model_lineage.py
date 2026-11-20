"""模型血缘服务：追踪数仓模型间的上下游依赖关系，支持影响分析和断链修复。

增强能力：
- 消费 validate.result 通道，基于校验结果自动检测断链
- 消费 validate.diff_confirmed 通道更新影响分析
- 影响级别计算：根据关联类型和下游模型数量
- 断链修复：自动尝试重建维度关联关系
- 血缘图构建：基于维度-事实关联矩阵生成DAG
- 消费 Vault 批次清单：把实际装载的 Hub/Link/Satellite 来源关系写入血缘，
  卫星更正可回溯到具体源行和批次
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services import vault_lineage
from app.services.base import BaseModuleService
from app.store import store


class ModelLineageService(BaseModuleService):
    MODULE = "model_lineage"
    ENTITY = "依赖关系"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["关系ID", "上游模型", "下游模型", "依赖字段", "关联类型", "影响级别", "更新状态"]
    REQUIRED_FIELDS = ["关系ID", "上游模型", "下游模型"]
    STATUS_ORDER = ["正常", "变更中", "断链", "已下线"]
    ACTION_RULES = {"分析影响": "变更中", "修复断链": "断链"}
    NEGATIVE_ACTIONS = []

    ASSOCIATION_TYPES = {"维度关联", "聚合", "引用", "拷贝", "桥接"}

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建血缘关系：自动判断关联类型并基于校验结果检测健康状态。"""
        upstream = str(values.get("上游模型", "")).strip()
        downstream = str(values.get("下游模型", "")).strip()

        # 自动推断关联类型
        assoc_type = str(values.get("关联类型", "")).strip()
        if assoc_type not in self.ASSOCIATION_TYPES:
            assoc_type = _infer_association_type(upstream, downstream)
            values["关联类型"] = assoc_type

        # 自动计算影响级别
        impact_level = _calculate_impact_level(upstream, downstream)
        values["影响级别"] = impact_level

        # 消费校验结果通道，评估血缘健康
        validation_msgs = store.channel.consume("validate.result", "model_lineage")
        for msg in validation_msgs:
            target = msg["payload"].get("target_model", "")
            if target in (upstream, downstream) and not msg["payload"].get("passed"):
                values["更新状态"] = "变更中"

        # 消费差异确认通道
        diff_msgs = store.channel.consume("validate.diff_confirmed", "model_lineage")
        for msg in diff_msgs:
            if msg["payload"].get("target_model") in (upstream, downstream):
                values["_diff_acknowledged"] = True

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("lineage.created", {
            "lineage_id": entry["关系ID"],
            "upstream": entry["上游模型"],
            "downstream": entry["下游模型"],
            "assoc_type": entry["关联类型"],
            "impact_level": entry["影响级别"],
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "model_lineage", "lineage_relation",
            f"lineage_{entry['id']}",
            {"upstream": entry["上游模型"], "downstream": entry["下游模型"], "type": entry["关联类型"]},
            source_modules=["dim_definition", "fact_design", "model_lineage"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        if action == "分析影响":
            # 执行影响分析
            impact_analysis = _analyze_impact(entry)

            # 消费所有未处理的 Vault 批次清单，把实际装载来源关系落到血缘
            vault_result = vault_lineage.consume_vault_batches()
            if vault_result["created_relations"]:
                impact_analysis["vault_relations_created"] = vault_result["created_relations"]

            store.channel.publish("lineage.impact_analysis", {
                "lineage_id": entry["关系ID"],
                "upstream": entry["上游模型"],
                "downstream": entry["下游模型"],
                "impact": impact_analysis,
                "entry_id": entry_id,
            })

            # 消费最近的校验结果
            recent_validations = store.channel.consume("validate.result", "model_lineage")
            for v in recent_validations:
                if v["payload"]["target_model"] in (entry["上游模型"], entry["下游模型"]):
                    entry["更新状态"] = "正常" if v["payload"]["passed"] else "断链"
                    break

        elif action == "修复断链":
            # 尝试修复断链
            repair_result = _repair_broken_lineage(entry)
            if repair_result["success"]:
                entry["status"] = "正常"
                entry["pending"] = False
                store.channel.publish("lineage.repaired", {
                    "lineage_id": entry["关系ID"],
                    "upstream": entry["上游模型"],
                    "downstream": entry["下游模型"],
                    "action": repair_result["action"],
                    "entry_id": entry_id,
                })
            else:
                entry["pending"] = True
                entry["abnormal"] = True

        return entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 血缘拓扑（DAG节点和边）
        nodes: set[str] = set()
        edges: list[dict[str, str]] = []
        for row in store.rows(self.MODULE):
            nodes.add(row.get("上游模型", ""))
            nodes.add(row.get("下游模型", ""))
            edges.append({"from": row.get("上游模型", ""), "to": row.get("下游模型", ""),
                         "type": row.get("关联类型", ""), "status": row.get("status", "")})
        base["dag"] = {"nodes": sorted(n for n in nodes if n), "edges": edges,
                       "node_count": len(nodes), "edge_count": len(edges)}

        # 断链统计
        broken = sum(1 for row in store.rows(self.MODULE) if row.get("status") == "断链")
        base["broken_lineages"] = broken
        base["consumed_validations"] = len(store.channel.consume("validate.result", "model_lineage"))
        # Vault 批次清单产生的实际装载来源关系
        base["vault_lineage"] = vault_lineage.vault_lineage_snapshot()
        return base


def _infer_association_type(upstream: str, downstream: str) -> str:
    """根据模型命名推断关联类型。"""
    if upstream.startswith("dim_") and downstream.startswith("dwd_"):
        return "维度关联"
    elif upstream.startswith("dwd_") and downstream.startswith("dws_"):
        return "聚合"
    elif upstream.startswith("dws_") and downstream.startswith("ads_"):
        return "聚合"
    return "引用"


def _calculate_impact_level(upstream: str, downstream: str) -> str:
    """计算影响级别。"""
    if upstream.startswith("dim_") and downstream.startswith("dim_"):
        return "低"
    elif upstream.startswith("dim_") and downstream.startswith("dwd_"):
        return "高"
    elif upstream.startswith("dwd_") and downstream.startswith("dws_"):
        return "高"
    elif "维度关联" in _infer_association_type(upstream, downstream):
        return "高"
    return "中"


def _analyze_impact(entry: dict[str, Any]) -> dict[str, Any]:
    """分析变更影响范围。"""
    downstream_model = entry.get("下游模型", "")
    affected = []
    for row in store.rows("model_lineage"):
        if row.get("上游模型") == downstream_model and row.get("关系ID") != entry["关系ID"]:
            affected.append({"model": row.get("下游模型"), "type": row.get("关联类型"),
                           "level": row.get("影响级别")})
    return {"changed_model": entry.get("上游模型"), "affected_downstream": affected,
            "total_affected": len(affected), "analyzed_at": datetime.now(timezone.utc).isoformat()}


def _repair_broken_lineage(entry: dict[str, Any]) -> dict[str, Any]:
    """尝试修复断链血缘关系。"""
    upstream = entry.get("上游模型", "")
    downstream = entry.get("下游模型", "")

    # 维度关联断链：检查维度定义是否已恢复
    if entry.get("关联类型") == "维度关联":
        for d in store.rows("dim_definition"):
            dim_table = f"dim_{d.get('维度编码', '').lower().replace('dim_', '')}"
            if dim_table == upstream or d.get("维度编码", "") in upstream:
                if d.get("status") in ("已发布", "已建模"):
                    return {"success": True, "action": f"重新关联维度{d.get('维度名称')}"}

    # 聚合断链：检查上游事实表是否可用
    if entry.get("关联类型") == "聚合":
        for f in store.rows("fact_design"):
            if f.get("事实名称", "") in upstream:
                if f.get("status") in ("已发布", "已设计"):
                    return {"success": True, "action": f"重连事实表{f.get('事实名称')}"}

    return {"success": False, "action": "", "reason": "上游模型不可用或未恢复"}