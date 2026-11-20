"""DataVault服务：管理DataVault 2.0建模的Hub、Link和Satellite结构。

增强能力：
- Hub实体唯一性校验
- Link关系关联检测（单向/双向）
- Satellite属性版本追踪
- 哈希键自动生成
- 消费维度/事实发布通道
- 批量装载：每次「执行加载」生成可追溯到源行的 Vault 批次清单，
  重复提交不新增 Hub/Link，Satellite 真实变更保留旧值和新值。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from app.services import vault_manifest
from app.services.base import BaseModuleService
from app.store import store


class DataVaultService(BaseModuleService):
    MODULE = "data_vault"
    ENTITY = "Vault模型"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["模型名称", "Hub实体", "Link关系", "Satellite属性", "加载时间", "记录源", "哈希键"]
    REQUIRED_FIELDS = ["模型名称", "Hub实体", "Link关系"]
    STATUS_ORDER = ["草稿", "已建模", "已加载", "已归档"]
    ACTION_RULES = {"提交建模": "草稿", "执行加载": "已建模", "归档模型": "已加载"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        hub_entity = str(values.get("Hub实体", "")).strip()

        # Hub唯一性检查
        for r in store.rows(self.MODULE):
            if r.get("Hub实体") == hub_entity:
                return None, [f"Hub实体 {hub_entity} 已存在"]

        # 自动生成哈希键
        if not values.get("哈希键"):
            hash_key = hashlib.md5(f"{hub_entity}_{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:16]
        else:
            hash_key = str(values.get("哈希键"))

        values["哈希键"] = hash_key

        if not values.get("加载时间"):
            values["加载时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # 消费通道
        store.channel.consume("dim.published", "data_vault")
        store.channel.consume("fact.published", "data_vault")

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("vault.defined", {
            "model_name": entry["模型名称"],
            "hub": hub_entity,
            "link": entry.get("Link关系"),
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "data_vault", "vault_model",
            f"vault_{entry['id']}",
            {"model_name": entry["模型名称"], "hub": hub_entity},
            ["data_vault"],
        )
        return entry, []

    # ------------------------------------------------------------------
    # 批量装载入口
    # ------------------------------------------------------------------

    def apply_action(
        self,
        entry_id: int,
        action: str,
        context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """带上下文的动作入口：执行加载时把批量上报的源记录交给批次清单。"""
        ctx = context or {}
        records = ctx.get("records") or ctx.get("source_rows") or ctx.get("rows")
        if action == "执行加载" and records:
            return self.execute_load(
                entry_id,
                records,
                source=ctx.get("source") or "",
                batch_id=ctx.get("batch_id") or "",
            )
        entry, message = self.run_action(entry_id, action)
        return entry, message

    def execute_load(
        self,
        entry_id: int,
        records: list[dict[str, Any]] | None = None,
        *,
        source: str = "",
        batch_id: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """执行一次批量装载并生成批次清单。

        无论是否提供源记录都会落一份清单：没有源记录时批次状态为「无装载数据」，
        便于从同一入口审计每一次加载尝试。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if entry.get("status") == "已归档":
            return None, f"{self.ENTITY} {entry_id} 已归档，不能执行加载"

        manifest = vault_manifest.load_batch(
            entry,
            list(records or []),
            source=source,
            batch_id=batch_id,
            warehouse=store,
        )

        entry["加载时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        entry["最近批次号"] = manifest["批次号"]
        entry["最近装载行数"] = manifest["上报行数"]
        entry["status"] = "已加载"
        entry["pending"] = manifest["状态"] == vault_manifest.BATCH_EMPTY
        entry["abnormal"] = manifest["拒绝行数"] > 0
        entry["批次总数"] = len(vault_manifest.list_batches(
            model_name=str(entry.get("模型名称", "")), warehouse=store))

        store.channel.publish("vault.loaded", {
            "model_name": entry["模型名称"],
            "batch_no": manifest["批次号"],
            "rows_reported": manifest["上报行数"],
            "rows_loaded": manifest["新增行数"] + manifest["更正行数"],
            "rows_duplicate": manifest["重复行数"],
            "rows_rejected": manifest["拒绝行数"],
            "hubs": manifest["Hub新增数"],
            "links": manifest["Link新增数"],
            "sats": manifest["Satellite新增数"],
            "entry_id": entry_id,
        })
        store.channel.publish("vault.batch_loaded", {
            "model_name": entry["模型名称"],
            "batch_no": manifest["批次号"],
            "batch_id": manifest["id"],
            "entry_id": entry_id,
        })

        store.artifacts.register(
            "data_vault", "vault_batch",
            f"batch_{manifest['批次号']}",
            {
                "model_name": entry["模型名称"],
                "batch_no": manifest["批次号"],
                "rows_reported": manifest["上报行数"],
                "rows_duplicate": manifest["重复行数"],
                "rows_rejected": manifest["拒绝行数"],
                "source": manifest["记录源"],
            },
            source_modules=["data_vault", "model_lineage"],
        )

        message = f"{self.ENTITY}已执行加载，批次 {manifest['批次号']}：{manifest['清单摘要']}"
        return entry, message

    def load_batch(
        self,
        entry_id: int,
        records: list[dict[str, Any]] | None = None,
        *,
        source: str = "",
        batch_id: str = "",
    ) -> dict[str, Any] | None:
        """对外暴露的批次清单生成方法，返回批次头 + 逐行明细。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        return vault_manifest.load_batch(
            entry, list(records or []), source=source, batch_id=batch_id, warehouse=store)

    def list_batches(self, entry_id: int) -> list[dict[str, Any]]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return []
        return vault_manifest.list_batches(
            model_name=str(entry.get("模型名称", "")), warehouse=store)

    def get_batch(self, batch_no: str | int) -> dict[str, Any] | None:
        return vault_manifest.get_batch(batch_no, warehouse=store)

    def confirm_hub(self, hub_name: str, business_key: str) -> dict[str, Any] | None:
        """确认 Hub 后才能被 Link 引用；未确认 Hub 的链接在装载时会被拒绝。"""
        return vault_manifest.confirm_hub(hub_name, business_key, warehouse=store)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        if action == "执行加载":
            # 没有源记录时也生成清单，保证加载入口始终有可追溯的批次。
            return self.execute_load(entry_id, [])
        if action == "归档模型":
            entry["status"] = "已归档"
            entry["pending"] = False
            entry["abnormal"] = False
            store.channel.publish("vault.archived", {
                "model_name": entry["模型名称"],
                "entry_id": entry_id,
            })
            return entry, f"{self.ENTITY}已归档"

        entry["status"] = self.ACTION_RULES[action]
        entry["pending"] = True
        entry["abnormal"] = False
        return entry, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        loaded = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "已加载")
        base["loaded_vaults"] = loaded

        batches = vault_manifest.list_batches(warehouse=store)
        base["batches"] = {
            "total": len(batches),
            "rows_reported": sum(int(b.get("上报行数", 0)) for b in batches),
            "duplicates": sum(int(b.get("重复行数", 0)) for b in batches),
            "corrections": sum(int(b.get("更正行数", 0)) for b in batches),
            "late_corrections": sum(int(b.get("迟到更正行数", 0)) for b in batches),
            "rejected": sum(int(b.get("拒绝行数", 0)) for b in batches),
        }
        base["hubs"] = len(store.rows(vault_manifest.HUB_TABLE))
        base["links"] = len(store.rows(vault_manifest.LINK_TABLE))
        base["satellites"] = len(store.rows(vault_manifest.SATELLITE_TABLE))
        return base
