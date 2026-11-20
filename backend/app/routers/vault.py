"""DataVault 批次清单接口：批量装载入口、批次回查、血缘消费。

批次清单由「执行加载」动作产生；这里的接口只负责装载与回查，不重复判定业务语义。
为便于调用，同时提供两套路径：
- /api/data_vault/batches ...（挂在模块名下，与前端工作台一致）
- /api/vault/...（领域内的简写）
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import vault_lineage, vault_manifest
from app.services import service_for
from app.store import store

router = APIRouter(prefix="/api", tags=["Vault批次清单"])


class BatchLoadPayload(BaseModel):
    """批量上报载荷：可用模型名称或 Vault 模型 id 指定目标。"""

    model_name: str | None = None
    entry_id: int | None = None
    source: str | None = None
    batch_id: str | None = None
    records: list[dict[str, Any]] = Field(default_factory=list)

    def rows(self) -> list[dict[str, Any]]:
        return list(self.records or [])


def _data_vault_service():
    service = service_for("data_vault")
    if service is None:  # pragma: no cover - 注册表固定包含 data_vault
        raise HTTPException(status_code=500, detail="DataVault 服务未注册")
    return service


def _resolve_entry(payload: BatchLoadPayload) -> dict[str, Any]:
    service = _data_vault_service()
    if payload.entry_id is not None:
        entry = service.get_entry(payload.entry_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"Vault模型 {payload.entry_id} 不存在")
        return entry
    if payload.model_name:
        for row in store.rows("data_vault"):
            if str(row.get("模型名称", "")) == payload.model_name:
                return row
        raise HTTPException(status_code=404, detail=f"Vault模型 {payload.model_name} 不存在")
    raise HTTPException(status_code=422, detail="需要提供 entry_id 或 model_name")


def _load(payload: BatchLoadPayload) -> dict[str, Any]:
    entry = _resolve_entry(payload)
    service = _data_vault_service()
    entry, _ = service.execute_load(
        entry["id"], payload.rows(),
        source=payload.source or "", batch_id=payload.batch_id or "",
    )
    batch_no = entry.get("最近批次号") if entry else None
    batch = vault_manifest.get_batch(batch_no) if batch_no else None
    if batch is None:  # pragma: no cover - execute_load 一定落批次
        raise HTTPException(status_code=500, detail="批次清单生成失败")
    return batch


# --------------------------------------------------------------------------
# 批量装载入口
# --------------------------------------------------------------------------


@router.post("/data_vault/batches")
def load_batch(payload: BatchLoadPayload) -> dict[str, Any]:
    """提交一批源记录，返回本次生成的批次清单（含逐行明细）。"""
    return _load(payload)


@router.post("/vault/batches")
def load_batch_alias(payload: BatchLoadPayload) -> dict[str, Any]:
    return _load(payload)


# --------------------------------------------------------------------------
# 批次回查
# --------------------------------------------------------------------------


@router.get("/data_vault/batches")
def list_batches(model_name: str | None = Query(default=None)) -> dict[str, Any]:
    rows = vault_manifest.list_batches(model_name=model_name)
    return {"items": rows, "total": len(rows)}


@router.get("/data_vault/batches/{batch_no}")
def get_batch(batch_no: str) -> dict[str, Any]:
    batch = vault_manifest.get_batch(batch_no)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_no} 不存在")
    return batch


@router.get("/data_vault/{entry_id}/batches")
def list_model_batches(entry_id: int) -> dict[str, Any]:
    service = _data_vault_service()
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Vault模型 {entry_id} 不存在")
    rows = service.list_batches(entry_id)
    return {"items": rows, "total": len(rows)}


@router.get("/vault/batches")
def list_batches_alias(model_name: str | None = Query(default=None)) -> dict[str, Any]:
    return list_batches(model_name)


@router.get("/vault/batches/{batch_no}")
def get_batch_alias(batch_no: str) -> dict[str, Any]:
    return get_batch(batch_no)


# --------------------------------------------------------------------------
# 模型血缘消费
# --------------------------------------------------------------------------


def _consume() -> dict[str, Any]:
    result = vault_lineage.consume_vault_batches()
    result.pop("relations", None)
    return {**result, "snapshot": vault_lineage.vault_lineage_snapshot()}


@router.post("/model_lineage/vault-relations/consume")
def consume_model_lineage() -> dict[str, Any]:
    """让模型血缘消费尚未处理的批次清单，建立实际装载来源关系。"""
    return _consume()


@router.get("/model_lineage/vault-relations")
def model_lineage_relations() -> dict[str, Any]:
    """查看批次清单产生的实际装载来源关系汇总。"""
    return vault_lineage.vault_lineage_snapshot()


@router.post("/vault/lineage/consume")
def consume_lineage() -> dict[str, Any]:
    return _consume()


@router.get("/vault/lineage")
def lineage_snapshot() -> dict[str, Any]:
    return vault_lineage.vault_lineage_snapshot()
