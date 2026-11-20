"""Vault 批次清单 → 模型血缘：把实际装载的来源关系落到血缘表。

一批源记录被 DataVault 装载后，血缘必须回答三个问题：
1. 这条记录实际来自哪个源系统、哪个批次、哪个源行；
2. 这次装载锚定到哪个已确认的 Hub / Link；
3. Satellite 更正来自哪一条源行、哪一个批次，改动了哪些字段。

因此这里只消费批次清单，不重新判断业务语义：
- 只为「已确认」的 Hub / Link 建立关系，未确认或拒绝的对象不进入血缘；
- 重复提交不产生新的装载关系，避免血缘被重放请求放大；
- Satellite 更正会单独建一条关系，携带新旧值、源行标识和批次号。
"""
from __future__ import annotations

from typing import Any

from app.services import vault_manifest
from app.store import store

VAULT_LOAD = "Vault装载"
VAULT_LINK = "Vault关联"
VAULT_SATELLITE = "Vault卫星装载"
VAULT_CORRECTION = "Vault卫星更正"

LINEAGE_MODULE = "model_lineage"


def _db(warehouse: Any = None) -> Any:
    return warehouse if warehouse is not None else store


def _relation_id(batch_no: str, *parts: Any) -> str:
    suffix = "-".join(str(part) for part in parts if str(part))
    return f"VLIN-{batch_no}-{suffix}".upper()


def _exists(db: Any, relation_id: str) -> bool:
    return any(row.get("关系ID") == relation_id for row in db.rows(LINEAGE_MODULE))


def _insert_relation(db: Any, row: dict[str, Any]) -> dict[str, Any]:
    existing = next((item for item in db.rows(LINEAGE_MODULE) if item.get("关系ID") == row["关系ID"]), None)
    if existing is not None:
        return existing
    row.setdefault("id", max((int(item.get("id", 0)) for item in db.rows(LINEAGE_MODULE)), default=0) + 1)
    row.setdefault("pending", False)
    row.setdefault("abnormal", False)
    db.insert(LINEAGE_MODULE, row)
    return row


def _hub_is_confirmed(db: Any, hub_hash: str) -> dict[str, Any] | None:
    for hub in db.rows(vault_manifest.HUB_TABLE):
        if hub.get("Hub哈希键") == hub_hash:
            if hub.get("确认状态") == vault_manifest.HUB_CONFIRMED:
                return hub
            return None
    return None


def _link_is_confirmed(db: Any, link_hash: str) -> dict[str, Any] | None:
    for link in db.rows(vault_manifest.LINK_TABLE):
        if link.get("Link哈希键") == link_hash:
            if link.get("确认状态") != vault_manifest.HUB_CONFIRMED:
                return None
            # Link 两端的 Hub 也必须已确认，否则链接本身不成立。
            if _hub_is_confirmed(db, str(link.get("主Hub哈希键", ""))) is None:
                return None
            if _hub_is_confirmed(db, str(link.get("关联Hub哈希键", ""))) is None:
                return None
            return link
    return None


def _satellite_row(db: Any, satellite_hash: str, batch_no: str, source_row_id: str) -> dict[str, Any] | None:
    for satellite in db.rows(vault_manifest.SATELLITE_TABLE):
        if satellite.get("Satellite哈希键") != satellite_hash:
            continue
        if satellite.get("批次号") != batch_no:
            continue
        if satellite.get("源行标识") != source_row_id:
            continue
        return satellite
    return None


def _base_relation(batch: dict[str, Any], detail: dict[str, Any], *, assoc_type: str,
                   relation_id: str, dependency: str, impact: str) -> dict[str, Any]:
    source = str(batch.get("记录源", ""))
    upstream = f"src:{source}" if source else "src:未知源"
    return {
        "关系ID": relation_id,
        "上游模型": upstream,
        "下游模型": str(batch.get("模型名称", "")),
        "依赖字段": dependency,
        "关联类型": assoc_type,
        "影响级别": impact,
        "更新状态": "同步",
        "status": "正常",
        "pending": False,
        "abnormal": False,
        "批次号": batch.get("批次号", ""),
        "源行标识": detail.get("源行标识", ""),
        "输入类型": detail.get("输入类型", ""),
        "清单行ID": detail.get("id"),
        "Hub实体": detail.get("Hub实体", ""),
        "Hub哈希键": detail.get("Hub哈希键", ""),
        "Link关系": detail.get("Link关系", ""),
        "Link哈希键": detail.get("Link哈希键", ""),
        "Satellite属性": detail.get("Satellite属性", ""),
        "Satellite哈希键": detail.get("Satellite哈希键", ""),
        "Satellite版本": detail.get("Satellite版本", 0),
        "变更字段": detail.get("变更字段", ""),
        "旧值": detail.get("旧值", ""),
        "新值": detail.get("新值", ""),
        "是否迟到": detail.get("是否迟到", ""),
    }


def build_lineage_for_batch(batch: dict[str, Any], *, warehouse: Any = None) -> list[dict[str, Any]]:
    """按批次清单建立实际装载来源关系。

    返回本次新建的血缘关系；重复调用不会重复建关系。
    """
    db = _db(warehouse)
    batch_no = str(batch.get("批次号", ""))
    details = batch.get("清单")
    if details is None:
        details = vault_manifest.batch_details(batch_no, warehouse=db)

    created: list[dict[str, Any]] = []

    for detail in details:
        input_type = detail.get("输入类型", "")
        if input_type == vault_manifest.LOAD_REJECTED:
            # 拒绝行不产生装载关系；拒绝原因留在批次清单里。
            detail["血缘状态"] = "未建关系：装载被拒绝"
            continue
        if input_type == vault_manifest.LOAD_DUPLICATE:
            # 重复提交没有新增装载，不应放大血缘。
            detail["血缘状态"] = "未建关系：重复提交"
            continue

        hub_hash = str(detail.get("Hub哈希键", ""))
        hub = _hub_is_confirmed(db, hub_hash) if hub_hash else None
        if hub is None:
            detail["血缘状态"] = "未建关系：Hub未确认"
            continue

        seq = detail.get("源行序号", 0)
        hub_relation_id = _relation_id(batch_no, "HUB", seq)
        if not _exists(db, hub_relation_id):
            relation = _base_relation(
                batch, detail, assoc_type=VAULT_LOAD, relation_id=hub_relation_id,
                dependency=hub_hash, impact="高",
            )
            relation["装载对象"] = f"Hub:{hub.get('Hub实体')}"
            relation["对象名称"] = hub.get("Hub实体", "")
            relation["对象哈希键"] = hub_hash
            _insert_relation(db, relation)
            created.append(relation)
        hub["血缘关系ID"] = hub_relation_id
        detail["Hub血缘关系ID"] = hub_relation_id

        link_hash = str(detail.get("Link哈希键", ""))
        link_state = detail.get("Link装载状态", "")
        if link_hash and link_state in (vault_manifest.LINK_CREATED, vault_manifest.LINK_EXISTING):
            link = _link_is_confirmed(db, link_hash)
            if link is None:
                detail["血缘状态"] = "未建关系：Link未确认"
            else:
                link_relation_id = _relation_id(batch_no, "LINK", seq)
                if not _exists(db, link_relation_id):
                    relation = _base_relation(
                        batch, detail, assoc_type=VAULT_LINK, relation_id=link_relation_id,
                        dependency=link_hash, impact="高",
                    )
                    relation["装载对象"] = f"Link:{link.get('Link关系')}"
                    relation["对象名称"] = link.get("Link关系", "")
                    relation["对象哈希键"] = link_hash
                    relation["关联Hub实体"] = link.get("关联Hub实体", "")
                    relation["关联Hub哈希键"] = link.get("关联Hub哈希键", "")
                    _insert_relation(db, relation)
                    created.append(relation)
                link["血缘关系ID"] = link_relation_id
                detail["Link血缘关系ID"] = link_relation_id

        satellite_hash = str(detail.get("Satellite哈希键", ""))
        if not satellite_hash:
            continue

        satellite = _satellite_row(
            db, satellite_hash, batch_no, str(detail.get("源行标识", "")))
        # 关系ID只按清单行定位，不随卫星版本后来失效而变化，保证重复消费幂等。
        sat_relation_id = _relation_id(batch_no, "SAT", seq)

        if not _exists(db, sat_relation_id):
            is_correction = input_type in vault_manifest.CORRECTION_TYPES
            relation = _base_relation(
                batch, detail,
                assoc_type=VAULT_CORRECTION if is_correction else VAULT_SATELLITE,
                relation_id=sat_relation_id,
                dependency=satellite_hash,
                impact="高" if is_correction else "中",
            )
            relation["装载对象"] = f"Satellite:{detail.get('Satellite属性', '')}"
            relation["对象名称"] = detail.get("Satellite属性", "")
            relation["对象哈希键"] = satellite_hash
            relation["版本状态"] = satellite.get("状态", "") if satellite else ""
            relation["版本动作"] = satellite.get("版本动作", "") if satellite else ""
            relation["更新状态"] = "卫星更正" if is_correction else "同步"
            if is_correction:
                relation["更正来源"] = (
                    f"批次 {batch_no} / 源行 {detail.get('源行标识', '')}"
                )
            _insert_relation(db, relation)
            created.append(relation)

        if satellite is not None:
            satellite["血缘关系ID"] = sat_relation_id
        detail["Satellite血缘关系ID"] = sat_relation_id
        if not str(detail.get("血缘状态", "")).startswith("未建关系"):
            detail["血缘状态"] = "已建关系"

    batch["血缘已生成"] = "是"
    return created


def consume_vault_batches(*, warehouse: Any = None) -> dict[str, Any]:
    """消费所有尚未生成血缘的批次清单。

    既消费通道消息，也扫描批次表，保证先装载后启动血缘服务时不会漏掉历史批次。
    """
    db = _db(warehouse)
    channel = getattr(db, "channel", None) or store.channel
    notified = channel.consume("vault.batch_loaded", "model_lineage")

    pending: list[dict[str, Any]] = []
    seen: set[str] = set()
    for batch in db.rows(vault_manifest.BATCH_TABLE):
        batch_no = str(batch.get("批次号", ""))
        if batch.get("血缘已生成") == "是" or batch_no in seen:
            continue
        pending.append(batch)
        seen.add(batch_no)

    created: list[dict[str, Any]] = []
    for batch in pending:
        created.extend(build_lineage_for_batch(batch, warehouse=db))

    return {
        "consumed_batches": len(pending),
        "notified_batches": len(notified),
        "created_relations": len(created),
        "relations": created,
    }


def vault_lineage_snapshot(*, warehouse: Any = None) -> dict[str, Any]:
    """按装载对象汇总批次清单产生的血缘，便于核对实际装载来源。"""
    db = _db(warehouse)
    rows = [
        row for row in db.rows(LINEAGE_MODULE)
        if str(row.get("关系ID", "")).startswith("VLIN-")
    ]
    by_object: dict[str, int] = {}
    by_type: dict[str, int] = {}
    corrections = 0
    for row in rows:
        by_object[str(row.get("装载对象", "")).split(":")[0]] = (
            by_object.get(str(row.get("装载对象", "")).split(":")[0], 0) + 1
        )
        by_type[row.get("关联类型", "")] = by_type.get(row.get("关联类型", ""), 0) + 1
        if row.get("关联类型") == VAULT_CORRECTION:
            corrections += 1
    return {
        "total": len(rows),
        "by_object": {key: value for key, value in by_object.items() if key},
        "by_type": by_type,
        "corrections": corrections,
        "batches": len({row.get("批次号") for row in rows if row.get("批次号")}),
    }
