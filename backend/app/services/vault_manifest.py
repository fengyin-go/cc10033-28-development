"""Vault 批次清单引擎：把一次批量上报拆解成可追溯到源行的 Hub/Link/Satellite 装载明细。

设计目标：
- 每个批次生成一份清单，逐条记录源行、业务键、Hub/Link/Satellite 哈希键和原始来源数据；
- 同一来源重复提交时按哈希键去重，不新增重复 Hub/Link，也不产生重复 Satellite 版本；
- Satellite 属性真正变化时保留旧值和新值，不能把真实变更误判为重复输入；
- Link 只允许指向已确认的 Hub，未确认的 Hub 会让链接被拒绝并留下原因；
- 批次清单落在内存仓库的 vault_batch / vault_batch_row 表中，供模型血缘消费。

批次清单不是日志副本：每一条清单行都保留了源行标识、输入判定依据和本次产生的
Hub/Link/Satellite 键，因此从下游血缘关系可以反向追到具体源行和批次。
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from app.store import store

BATCH_TABLE = "vault_batch"
BATCH_ROW_TABLE = "vault_batch_row"
HUB_TABLE = "vault_hub"
LINK_TABLE = "vault_link"
SATELLITE_TABLE = "vault_satellite"

# 清单行的输入类型
LOAD_NEW = "新增"
LOAD_DUPLICATE = "重复提交"
LOAD_CORRECTION = "Satellite更正"
LOAD_LATE_CORRECTION = "迟到更正"
LOAD_REJECTED = "拒绝"
CORRECTION_TYPES = {LOAD_CORRECTION, LOAD_LATE_CORRECTION}

# Hub / Link 装载状态
HUB_CREATED = "新增"
HUB_EXISTING = "已存在"
HUB_REJECTED = "拒绝"
LINK_CREATED = "已关联"
LINK_EXISTING = "已存在"
LINK_NOT_CONFIGURED = "未配置"
LINK_REJECTED = "拒绝"

# 批次状态
BATCH_DONE = "已完成"
BATCH_PARTIAL = "部分拒绝"
BATCH_REJECTED = "全部拒绝"
BATCH_EMPTY = "无装载数据"

HUB_CONFIRMED = "已确认"
HUB_UNCONFIRMED = "未确认"
SATELLITE_CURRENT = "当前"
SATELLITE_HISTORY = "历史"

_IGNORED_HASH_TOKENS = {
    "md5", "sha1", "sha256", "hash", "concat", "upper", "lower", "trim",
    "coalesce", "nvl", "cast", "isnull", "record", "nullif", "ifnull",
}

_SOURCE_ROW_ID_KEYS = ("源行标识", "源行号", "source_row_id", "sourceRowId", "row_id", "rowId")
_BUSINESS_KEY_KEYS = ("业务键", "business_key", "businessKey", "bk")
_SATELLITE_DIRECT_KEYS = ("卫星属性", "satellite_attributes", "satelliteAttributes")
_SOURCE_TIME_KEYS = ("源时间", "source_time", "sourceTime", "event_time", "eventTime", "变更时间", "更新时间")
_LINKED_HUB_KEYS = ("关联Hub", "关联HUB", "linked_hub", "linkedHub")
_LINKED_BUSINESS_KEY_KEYS = ("关联业务键", "linked_business_key", "linkedBusinessKey")
_LINKED_HASH_KEYS = ("关联Hub哈希键", "linked_hub_hash", "linkedHubHash")
_LATE_FLAG_KEYS = ("是否迟到", "is_late", "isLate")
_HUB_CONFIRM_KEYS = ("Hub确认状态", "hub_confirm_status", "hubConfirmStatus")

# 源行里不参与 Satellite 属性的元数据字段
_META_FIELDS = {
    *_SOURCE_ROW_ID_KEYS, *_BUSINESS_KEY_KEYS, *_SATELLITE_DIRECT_KEYS,
    *_SOURCE_TIME_KEYS, *_LINKED_HUB_KEYS, *_LINKED_BUSINESS_KEY_KEYS,
    *_LINKED_HASH_KEYS, *_LATE_FLAG_KEYS, *_HUB_CONFIRM_KEYS,
    "记录源", "来源", "source", "批次号", "批次", "batch_id", "batchId",
    "模型名称", "备注", "remark", "操作类型", "action", "变更原因",
    "哈希键", "hash_key", "hashKey", "输入类型", "是否重复",
}


def _db(warehouse: Any = None) -> Any:
    return warehouse if warehouse is not None else store


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _canonical(value: Any) -> Any:
    """把任意值转换成稳定可序列化结构，保证同一行重复提交得到同一哈希。"""
    if isinstance(value, dict):
        return {str(key): _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value).strip()


def _stable_json(value: Any) -> str:
    return json.dumps(_canonical(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash_key(*parts: Any) -> str:
    """生成 Data Vault 风格哈希键：同一模型、同一业务键/属性组合结果稳定。"""
    return hashlib.md5(_stable_json(list(parts)).encode("utf-8")).hexdigest().upper()


def _pick(row: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in row and _text(row.get(key)):
            return row.get(key)
    return None


def _business_key_fields(expression: str) -> list[str]:
    """从 MD5(order_id)、MD5(warehouse_id.sku_id) 之类的表达式里拆出业务键字段。"""
    expression = _text(expression)
    if not expression:
        return []
    match = re.search(r"\(([^)]*)\)", expression)
    body = match.group(1) if match else expression
    tokens = re.split(r"[,，.。+;|/\s]+", body)
    fields: list[str] = []
    for token in tokens:
        cleaned = re.sub(r"[^0-9A-Za-z_一-龥]", "", token)
        if not cleaned or cleaned.lower() in _IGNORED_HASH_TOKENS:
            continue
        if cleaned not in fields:
            fields.append(cleaned)
    return fields


def _business_key_value(row: dict[str, Any], model: dict[str, Any]) -> tuple[str, str]:
    """返回 (业务键值, 业务键字段说明)。优先取显式业务键，其次按模型哈希表达式拼装。"""
    explicit = _pick(row, _BUSINESS_KEY_KEYS)
    if explicit is not None:
        return _text(explicit), "业务键"

    fields = _business_key_fields(str(model.get("哈希键", "")))
    values = []
    used = []
    for field in fields:
        candidate = _pick(row, (field,))
        if candidate is not None:
            values.append(_text(candidate))
            used.append(field)
    if values:
        return "|".join(values), "+".join(used)
    return "", ""


def _source_row_id(row: dict[str, Any], ordinal: int) -> str:
    explicit = _pick(row, _SOURCE_ROW_ID_KEYS)
    if explicit is not None:
        return _text(explicit)
    return f"ROW-{ordinal:04d}"


def _source_time(row: dict[str, Any]) -> str:
    return _text(_pick(row, _SOURCE_TIME_KEYS))


def _is_late_flag(row: dict[str, Any]) -> bool:
    value = _pick(row, _LATE_FLAG_KEYS)
    return _text(value).lower() in {"1", "true", "yes", "y", "是", "迟到"}


def _says_unconfirmed(row: dict[str, Any]) -> bool:
    value = _pick(row, _HUB_CONFIRM_KEYS)
    return _text(value) in {"未确认", "否", "N", "n", "false", "False"}


def _parse_time(value: str) -> datetime | None:
    value = _text(value)
    if not value:
        return None
    normalized = value.replace("T", " ").replace("Z", "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(normalized[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _is_older(candidate: str, current: str) -> bool:
    left, right = _parse_time(candidate), _parse_time(current)
    if left is None or right is None:
        return False
    return left < right


def _is_newer(candidate: str, current: str) -> bool:
    left, right = _parse_time(candidate), _parse_time(current)
    if left is None or right is None:
        return False
    return left > right


def _satellite_attributes(row: dict[str, Any], ignored: Iterable[str] = ()) -> dict[str, Any]:
    explicit = _pick(row, _SATELLITE_DIRECT_KEYS)
    if isinstance(explicit, dict):
        return {str(key): _canonical(value) for key, value in explicit.items()}
    if isinstance(explicit, str):
        try:
            parsed = json.loads(explicit)
            if isinstance(parsed, dict):
                return {str(key): _canonical(value) for key, value in parsed.items()}
        except (TypeError, ValueError):
            pass
    skip = set(_META_FIELDS) | set(ignored)
    return {
        str(key): _canonical(value)
        for key, value in row.items()
        if str(key) not in skip
        and not str(key).startswith("_")
        and not isinstance(value, (dict, list))
    }


def _diff_attributes(old: dict[str, Any], new: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    old_changed: dict[str, Any] = {}
    new_changed: dict[str, Any] = {}
    fields: list[str] = []
    for key in new:
        if _stable_json(old.get(key)) != _stable_json(new.get(key)):
            fields.append(key)
            old_changed[key] = old.get(key, "")
            new_changed[key] = new.get(key, "")
    for key in old:
        if key not in new:
            fields.append(key)
            old_changed[key] = old.get(key, "")
            new_changed[key] = ""
    return old_changed, new_changed, fields


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


# ---------------------------------------------------------------------------
# Hub / Link 登记与查询
# ---------------------------------------------------------------------------


def register_hub(
    hub_name: str,
    business_key: str,
    *,
    source: str = "",
    confirmed: bool = True,
    model_name: str = "",
    warehouse: Any = None,
) -> dict[str, Any]:
    """登记一个 Hub，可显式登记为未确认，用于验证 Link 不得指向未确认 Hub。"""
    db = _db(warehouse)
    hub_name = _text(hub_name)
    business_key = _text(business_key)
    hub_hash = _hash_key("HUB", hub_name, business_key)
    for existing in db.rows(HUB_TABLE):
        if existing.get("Hub实体") == hub_name and existing.get("业务键") == business_key:
            if confirmed and existing.get("确认状态") != HUB_CONFIRMED:
                existing["确认状态"] = HUB_CONFIRMED
                existing["status"] = HUB_CONFIRMED
                existing["pending"] = False
                existing["确认时间"] = existing.get("确认时间") or _now()
            return existing
    row = {
        "id": _next_id(db.rows(HUB_TABLE)),
        "Hub实体": hub_name,
        "业务键": business_key,
        "Hub哈希键": hub_hash,
        "记录源": _text(source),
        "来源模型": model_name,
        "确认状态": HUB_CONFIRMED if confirmed else HUB_UNCONFIRMED,
        "确认时间": _now() if confirmed else "",
        "首次批次号": "",
        "首次源行标识": "",
        "首次加载时间": _now(),
        "血缘关系ID": "",
        "status": HUB_CONFIRMED if confirmed else HUB_UNCONFIRMED,
        "pending": not confirmed,
        "abnormal": False,
    }
    db.insert(HUB_TABLE, row)
    return row


def confirm_hub(hub_name: str, business_key: str, *, warehouse: Any = None) -> dict[str, Any] | None:
    """确认一个已登记的 Hub；确认后 Link 才能引用它。"""
    db = _db(warehouse)
    for row in db.rows(HUB_TABLE):
        if row.get("Hub实体") == _text(hub_name) and row.get("业务键") == _text(business_key):
            row["确认状态"] = HUB_CONFIRMED
            row["status"] = HUB_CONFIRMED
            row["pending"] = False
            row["确认时间"] = _now()
            return row
    return None


def _find_hub(db: Any, hub_name: str, business_key: str) -> dict[str, Any] | None:
    for row in db.rows(HUB_TABLE):
        if row.get("Hub实体") == hub_name and row.get("业务键") == business_key:
            return row
    return None


def _find_hub_by_hash(db: Any, hub_hash: str) -> dict[str, Any] | None:
    for row in db.rows(HUB_TABLE):
        if row.get("Hub哈希键") == hub_hash:
            return row
    return None


def _load_hub(
    db: Any,
    model: dict[str, Any],
    *,
    business_key: str,
    source: str,
    row_id: str,
    batch_id: str,
    confirmed: bool,
) -> tuple[dict[str, Any] | None, str]:
    hub_name = _text(model.get("Hub实体"))
    if not hub_name:
        return None, HUB_REJECTED
    existing = _find_hub(db, hub_name, business_key)
    if existing is not None:
        # 未确认的 Hub 只能通过 confirm_hub 显式确认，不能被后续默认行悄悄提升状态，
        # 否则 Link 可能绕过「不得指向未确认 Hub」的约束。
        existing["最后批次号"] = batch_id
        existing["最后源行标识"] = row_id
        existing["最后加载时间"] = _now()
        existing["装载次数"] = int(existing.get("装载次数", 1)) + 1
        return existing, HUB_EXISTING
    hub_hash = _hash_key("HUB", hub_name, business_key)
    row = {
        "id": _next_id(db.rows(HUB_TABLE)),
        "Hub实体": hub_name,
        "业务键": business_key,
        "Hub哈希键": hub_hash,
        "记录源": source,
        "来源模型": model.get("模型名称", ""),
        "确认状态": HUB_CONFIRMED if confirmed else HUB_UNCONFIRMED,
        "确认时间": _now() if confirmed else "",
        "首次批次号": batch_id,
        "首次源行标识": row_id,
        "首次加载时间": _now(),
        "最后批次号": batch_id,
        "最后源行标识": row_id,
        "最后加载时间": _now(),
        "装载次数": 1,
        "血缘关系ID": "",
        "status": HUB_CONFIRMED if confirmed else HUB_UNCONFIRMED,
        "pending": not confirmed,
        "abnormal": False,
    }
    db.insert(HUB_TABLE, row)
    return row, HUB_CREATED


def _load_link(
    db: Any,
    model: dict[str, Any],
    *,
    row: dict[str, Any],
    primary_hub: dict[str, Any],
    source: str,
    row_id: str,
    batch_id: str,
) -> tuple[dict[str, Any] | None, str, str]:
    link_name = _text(model.get("Link关系"))
    if not link_name:
        return None, LINK_NOT_CONFIGURED, "模型未定义 Link 关系"

    linked_hub_name = _text(_pick(row, _LINKED_HUB_KEYS))
    linked_business_key = _text(_pick(row, _LINKED_BUSINESS_KEY_KEYS))
    linked_hash = _text(_pick(row, _LINKED_HASH_KEYS))

    if not linked_hub_name and not linked_business_key and not linked_hash:
        # 源行没有提供关联对象时退化为自关联：Link 仍然只锚定本行已确认的 Hub。
        if primary_hub.get("确认状态") != HUB_CONFIRMED:
            return None, LINK_REJECTED, (
                f"Link 不得指向未确认的 Hub：主 Hub {primary_hub.get('Hub实体')}"
                f"({primary_hub.get('业务键')}) 当前状态为{primary_hub.get('确认状态')}"
            )
        link_hash = _hash_key("LINK", link_name, primary_hub.get("Hub哈希键"))
        for existing in db.rows(LINK_TABLE):
            if existing.get("Link关系") == link_name and existing.get("Link哈希键") == link_hash:
                return existing, LINK_EXISTING, ""
        link_row = {
            "id": _next_id(db.rows(LINK_TABLE)),
            "Link关系": link_name,
            "Link哈希键": link_hash,
            "主Hub实体": primary_hub.get("Hub实体"),
            "主Hub哈希键": primary_hub.get("Hub哈希键"),
            "关联Hub实体": primary_hub.get("Hub实体"),
            "关联Hub哈希键": primary_hub.get("Hub哈希键"),
            "记录源": source,
            "来源模型": model.get("模型名称", ""),
            "确认状态": HUB_CONFIRMED,
            "首次批次号": batch_id,
            "首次源行标识": row_id,
            "首次加载时间": _now(),
            "血缘关系ID": "",
            "status": HUB_CONFIRMED,
            "pending": False,
            "abnormal": False,
        }
        db.insert(LINK_TABLE, link_row)
        return link_row, LINK_CREATED, ""

    referenced: dict[str, Any] | None = None
    if linked_hash:
        referenced = _find_hub_by_hash(db, linked_hash)
    if referenced is None and linked_business_key:
        if linked_hub_name:
            referenced = _find_hub(db, linked_hub_name, linked_business_key)
        else:
            matches = [h for h in db.rows(HUB_TABLE) if h.get("业务键") == linked_business_key]
            referenced = matches[0] if len(matches) == 1 else None

    if referenced is None:
        return None, LINK_REJECTED, (
            f"Link 不得指向未确认的 Hub：关联 Hub {linked_hub_name or '?'}"
            f" 业务键 {linked_business_key or linked_hash or '?'} 尚未登记"
        )
    if referenced.get("确认状态") != HUB_CONFIRMED:
        return None, LINK_REJECTED, (
            f"Link 不得指向未确认的 Hub：{referenced.get('Hub实体')}"
            f"({referenced.get('业务键')}) 当前状态为{referenced.get('确认状态')}"
        )
    if primary_hub.get("确认状态") != HUB_CONFIRMED:
        return None, LINK_REJECTED, (
            f"Link 不得指向未确认的 Hub：主 Hub {primary_hub.get('Hub实体')}"
            f"({primary_hub.get('业务键')}) 当前状态为{primary_hub.get('确认状态')}"
        )

    link_hash = _hash_key("LINK", link_name, primary_hub.get("Hub哈希键"), referenced.get("Hub哈希键"))
    for existing in db.rows(LINK_TABLE):
        if existing.get("Link关系") == link_name and existing.get("Link哈希键") == link_hash:
            return existing, LINK_EXISTING, ""

    link_row = {
        "id": _next_id(db.rows(LINK_TABLE)),
        "Link关系": link_name,
        "Link哈希键": link_hash,
        "主Hub实体": primary_hub.get("Hub实体"),
        "主Hub哈希键": primary_hub.get("Hub哈希键"),
        "关联Hub实体": referenced.get("Hub实体"),
        "关联Hub哈希键": referenced.get("Hub哈希键"),
        "记录源": source,
        "来源模型": model.get("模型名称", ""),
        "确认状态": HUB_CONFIRMED,
        "首次批次号": batch_id,
        "首次源行标识": row_id,
        "首次加载时间": _now(),
        "血缘关系ID": "",
        "status": HUB_CONFIRMED,
        "pending": False,
        "abnormal": False,
    }
    db.insert(LINK_TABLE, link_row)
    return link_row, LINK_CREATED, ""


# ---------------------------------------------------------------------------
# Satellite 版本装载
# ---------------------------------------------------------------------------


def _satellite_versions(db: Any, satellite_name: str, hub_hash: str) -> list[dict[str, Any]]:
    versions = [
        row for row in db.rows(SATELLITE_TABLE)
        if row.get("Satellite属性") == satellite_name and row.get("Hub哈希键") == hub_hash
    ]
    return sorted(versions, key=lambda item: int(item.get("版本", 0)))


def _load_satellite(
    db: Any,
    model: dict[str, Any],
    *,
    hub: dict[str, Any],
    attributes: dict[str, Any],
    source: str,
    row_id: str,
    batch_id: str,
    source_time: str,
    late_flag: bool,
) -> tuple[dict[str, Any] | None, str, dict[str, Any], dict[str, Any], list[str], str]:
    """返回 (卫星版本行, 输入类型, 旧值, 新值, 变更字段, 判定依据)。"""
    satellite_name = _text(model.get("Satellite属性"))
    if not satellite_name:
        return None, LOAD_REJECTED, {}, {}, [], "模型未定义 Satellite 属性"
    if not attributes:
        return None, LOAD_REJECTED, {}, {}, [], "源行未提供卫星属性"

    hub_hash = str(hub.get("Hub哈希键", ""))
    satellite_hash = _hash_key("SAT", satellite_name, hub_hash, attributes)
    versions = _satellite_versions(db, satellite_name, hub_hash)
    current = next((item for item in reversed(versions) if item.get("状态") == SATELLITE_CURRENT), None)
    if current is None and versions:
        current = versions[-1]

    # 历史上出现过完全相同属性组合时，只有源时间没有更新才算重复输入；
    # 源时间更新说明是真实的回退/再变更，必须按新版本装载，不能误判为重复。
    seen = next(
        (item for item in reversed(versions) if item.get("Satellite哈希键") == satellite_hash),
        None,
    )
    if seen is not None and not _is_newer(source_time, str(seen.get("源时间", ""))):
        return seen, LOAD_DUPLICATE, {}, {}, [], (
            f"重复提交：卫星属性哈希与已装载版本 v{seen.get('版本')} 一致，源时间未更新，不新增版本"
        )

    if current is None:
        version = 1
        is_late = False
        old_values, new_values, changed = {}, dict(attributes), list(attributes)
        basis = "首次装载卫星属性"
    else:
        version = max(int(item.get("版本", 0)) for item in versions) + 1
        old_values, new_values, changed = _diff_attributes(
            json.loads(current.get("属性值") or "{}"), attributes)
        is_late = late_flag or _is_older(source_time, str(current.get("源时间", "")))
        basis = "卫星属性发生变化"
        if is_late:
            basis = "卫星属性迟到更正：源时间早于当前版本，保留旧值但不覆盖当前值"

    satellite_row = {
        "id": _next_id(db.rows(SATELLITE_TABLE)),
        "Satellite属性": satellite_name,
        "Hub实体": hub.get("Hub实体"),
        "业务键": hub.get("业务键"),
        "Hub哈希键": hub_hash,
        "Satellite哈希键": satellite_hash,
        "版本": version,
        "属性值": _stable_json(attributes),
        "旧值": _stable_json(old_values),
        "新值": _stable_json(new_values),
        "变更字段": ",".join(changed),
        "记录源": source,
        "来源模型": model.get("模型名称", ""),
        "源行标识": row_id,
        "批次号": batch_id,
        "源时间": source_time,
        "加载时间": _now(),
        "是否迟到": "是" if is_late else "否",
        "是否更正": "否" if current is None else "是",
        "版本动作": "新增：建立首个卫星版本",
        "状态": SATELLITE_CURRENT,
        "血缘关系ID": "",
        "status": SATELLITE_CURRENT,
        "pending": False,
        "abnormal": False,
    }

    if current is None:
        input_type = LOAD_NEW
    elif is_late:
        # 迟到更正作为历史版本落库，保留旧值和新值，但不覆盖已经更新的当前值。
        input_type = LOAD_LATE_CORRECTION
        satellite_row["状态"] = SATELLITE_HISTORY
        satellite_row["status"] = SATELLITE_HISTORY
        satellite_row["版本动作"] = "迟到更正：保留为历史版本，不覆盖当前值"
    else:
        input_type = LOAD_CORRECTION
        current["状态"] = SATELLITE_HISTORY
        current["status"] = SATELLITE_HISTORY
        current["失效批次号"] = batch_id
        satellite_row["版本动作"] = "更正：旧版本失效，新版本生效"

    db.insert(SATELLITE_TABLE, satellite_row)
    return satellite_row, input_type, old_values, new_values, changed, basis


# ---------------------------------------------------------------------------
# 批次装载主流程
# ---------------------------------------------------------------------------


def _resolve_model(model_name: str, db: Any) -> dict[str, Any] | None:
    for row in db.rows("data_vault"):
        if _text(row.get("模型名称")) == _text(model_name):
            return row
    return None


def _batch_no() -> str:
    return f"VB-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"


def _is_unconfirmed_row(row: dict[str, Any], model: dict[str, Any]) -> bool:
    """Hub 允许显式标记为未确认，用于验证 Link 不得指向未确认 Hub。"""
    if _says_unconfirmed(row):
        return True
    status = _text(model.get("Hub确认状态") or model.get("确认状态"))
    return status == HUB_UNCONFIRMED


def load_batch(
    model_or_name: dict[str, Any] | str,
    source_rows: list[dict[str, Any]],
    *,
    source: str = "",
    batch_id: str = "",
    warehouse: Any = None,
) -> dict[str, Any]:
    """把一批源记录装载进 Vault，并生成可追溯的批次清单。

    Args:
        model_or_name: Vault 模型行（data_vault 记录）或模型名称。
        source_rows: 源记录列表，逐条装载、逐条判定。
        source: 记录源；为空时取模型的「记录源」。
        batch_id: 外部指定的批次号；为空时自动生成。

    Returns:
        批次清单字典，包含批次头、逐行明细和汇总计数。
    """
    db = _db(warehouse)
    if isinstance(model_or_name, str):
        model = _resolve_model(model_or_name, db)
    else:
        model = model_or_name
    if model is None:
        raise ValueError(f"未找到Vault模型：{model_or_name}")

    if isinstance(source_rows, dict):
        # 容忍 {"records": [...]}/{"rows": [...]} 包装形式。
        source_rows = (
            source_rows.get("records")
            or source_rows.get("rows")
            or source_rows.get("source_rows")
            or []
        )
    source_rows = list(source_rows or [])

    batch_no = batch_id or _batch_no()
    source = _text(source) or _text(model.get("记录源"))
    model_name = _text(model.get("模型名称"))
    created_at = _now()

    batch = {
        "id": _next_id(db.rows(BATCH_TABLE)),
        "批次号": batch_no,
        "模型名称": model_name,
        "Hub实体": _text(model.get("Hub实体")),
        "Link关系": _text(model.get("Link关系")),
        "Satellite属性": _text(model.get("Satellite属性")),
        "记录源": source,
        "上报行数": len(source_rows),
        "新增行数": 0,
        "重复行数": 0,
        "更正行数": 0,
        "迟到更正行数": 0,
        "拒绝行数": 0,
        "Hub新增数": 0,
        "Link新增数": 0,
        "Satellite新增数": 0,
        "状态": BATCH_EMPTY,
        "清单摘要": "",
        "装载时间": created_at,
        "status": BATCH_EMPTY,
        "pending": False,
        "abnormal": False,
    }
    # 先登记批次头，清单行再引用它的 id。
    db.insert(BATCH_TABLE, batch)

    detail_rows: list[dict[str, Any]] = []
    business_key_field_cache = _business_key_fields(str(model.get("哈希键", "")))

    for ordinal, row in enumerate(source_rows, start=1):
        if not isinstance(row, dict):
            row = {"值": row}
        row_id = _source_row_id(row, ordinal)
        business_key, business_key_desc = _business_key_value(row, model)
        source_time = _source_time(row)
        confirmed = not _is_unconfirmed_row(row, model)
        ignored = business_key_field_cache + [f for f in _business_key_fields_from_desc(business_key_desc)]
        attributes = _satellite_attributes(row, ignored=ignored)

        detail: dict[str, Any] = {
            "id": _next_id(db.rows(BATCH_ROW_TABLE)),
            "批次号": batch_no,
            "批次ID": batch["id"],
            "模型名称": model_name,
            "记录源": source,
            "源行标识": row_id,
            "源行序号": ordinal,
            "业务键": business_key,
            "业务键字段": business_key_desc,
            "输入类型": LOAD_REJECTED,
            "判定依据": "",
            "Hub实体": batch["Hub实体"],
            "Hub哈希键": "",
            "Hub装载状态": HUB_REJECTED,
            "Link关系": batch["Link关系"],
            "Link哈希键": "",
            "Link装载状态": LINK_NOT_CONFIGURED,
            "关联Hub实体": "",
            "关联Hub哈希键": "",
            "Satellite属性": batch["Satellite属性"],
            "Satellite哈希键": "",
            "Satellite版本": 0,
            "旧值": "",
            "新值": "",
            "变更字段": "",
            "源时间": source_time,
            "是否迟到": "是" if _is_late_flag(row) else "否",
            "拒绝原因": "",
            "来源数据": _stable_json(row),
            "装载时间": _now(),
            "status": LOAD_REJECTED,
            "pending": False,
            "abnormal": False,
        }

        if not business_key:
            detail["判定依据"] = "缺少业务键"
            detail["拒绝原因"] = f"源行未提供业务键，模型哈希键为 {model.get('哈希键', '') or '空'}"
            _append_detail(db, detail)
            detail_rows.append(detail)
            continue

        hub, hub_state = _load_hub(
            db, model, business_key=business_key, source=source,
            row_id=row_id, batch_id=batch_no, confirmed=confirmed,
        )
        if hub is None:
            detail["判定依据"] = "Hub实体未定义"
            detail["拒绝原因"] = "Vault模型缺少 Hub 实体定义"
            _append_detail(db, detail)
            detail_rows.append(detail)
            continue

        detail["Hub哈希键"] = hub.get("Hub哈希键", "")
        detail["Hub装载状态"] = hub_state
        if hub_state == HUB_CREATED:
            batch["Hub新增数"] += 1

        # Link 只在模型显式提供关联 Hub 时处理；不得指向未确认 Hub。
        link, link_state, link_reason = _load_link(
            db, model, row=row, primary_hub=hub, source=source,
            row_id=row_id, batch_id=batch_no,
        )
        if link is not None:
            detail["Link哈希键"] = link.get("Link哈希键", "")
            detail["关联Hub实体"] = link.get("关联Hub实体", "")
            detail["关联Hub哈希键"] = link.get("关联Hub哈希键", "")
            detail["Link装载状态"] = link_state
            if link_state == LINK_CREATED:
                batch["Link新增数"] += 1
            hub["血缘关系ID"] = hub.get("血缘关系ID") or ""
            link["血缘关系ID"] = link.get("血缘关系ID") or ""
        else:
            detail["Link装载状态"] = link_state
            if link_state == LINK_REJECTED:
                detail["判定依据"] = "Link 关联被拒绝"
                detail["拒绝原因"] = link_reason
                _append_detail(db, detail)
                detail_rows.append(detail)
                continue

        satellite, input_type, old_values, new_values, changed, basis = _load_satellite(
            db, model, hub=hub, attributes=attributes, source=source,
            row_id=row_id, batch_id=batch_no, source_time=source_time,
            late_flag=_is_late_flag(row),
        )
        detail["判定依据"] = basis
        if satellite is None and input_type == LOAD_REJECTED:
            detail["拒绝原因"] = basis
            _append_detail(db, detail)
            detail_rows.append(detail)
            continue

        if satellite is not None:
            detail["Satellite哈希键"] = satellite.get("Satellite哈希键", "")
            detail["Satellite版本"] = satellite.get("版本", 0)
            detail["旧值"] = _stable_json(old_values)
            detail["新值"] = _stable_json(new_values)
            detail["变更字段"] = ",".join(changed)
            detail["是否迟到"] = satellite.get("是否迟到", detail["是否迟到"])
            if input_type == LOAD_NEW and satellite.get("版本") == 1:
                batch["Satellite新增数"] += 1

        if input_type in CORRECTION_TYPES:
            # 更正行在清单里要能区分于重复提交，也要能从血缘回到此源行。
            detail["输入类型"] = input_type
            detail["判定依据"] = (
                f"{basis}；旧值 {_stable_json(old_values)}；新值 {_stable_json(new_values)}"
            )
            batch["更正行数"] += 1
            if input_type == LOAD_LATE_CORRECTION:
                batch["迟到更正行数"] += 1
        elif input_type == LOAD_DUPLICATE:
            detail["输入类型"] = LOAD_DUPLICATE
            detail["判定依据"] = "重复提交：业务键与卫星属性哈希均与已装载版本一致，不新增 Hub/Link/Satellite"
            batch["重复行数"] += 1
        else:
            detail["输入类型"] = LOAD_NEW
            batch["新增行数"] += 1

        detail["status"] = detail["输入类型"]
        _append_detail(db, detail)
        detail_rows.append(detail)

    rejected = sum(1 for row in detail_rows if row["输入类型"] == LOAD_REJECTED)
    batch["拒绝行数"] = rejected
    if not detail_rows:
        batch["状态"] = BATCH_EMPTY
    elif rejected == len(detail_rows):
        batch["状态"] = BATCH_REJECTED
    elif rejected:
        batch["状态"] = BATCH_PARTIAL
    else:
        batch["状态"] = BATCH_DONE
    batch["status"] = batch["状态"]
    batch["abnormal"] = rejected > 0
    batch["pending"] = False
    batch["清单摘要"] = (
        f"上报 {batch['上报行数']} 行：新增 {batch['新增行数']}、重复提交 {batch['重复行数']}、"
        f"更正 {batch['更正行数']}（迟到 {batch['迟到更正行数']}）、拒绝 {batch['拒绝行数']}；"
        f"Hub 新增 {batch['Hub新增数']}、Link 新增 {batch['Link新增数']}、"
        f"Satellite 新增 {batch['Satellite新增数']}"
    )

    result = dict(batch)
    result["清单"] = [dict(row) for row in detail_rows]
    return result


def _business_key_fields_from_desc(desc: str) -> list[str]:
    return [item for item in _text(desc).split("+") if item]


def _append_detail(db: Any, detail: dict[str, Any]) -> None:
    """登记清单行；同时把批次行挂到 data_vault 模块可见的明细表。"""
    db.insert(BATCH_ROW_TABLE, detail)


def list_batches(*, model_name: str | None = None, warehouse: Any = None) -> list[dict[str, Any]]:
    db = _db(warehouse)
    rows = list(db.rows(BATCH_TABLE))
    if model_name:
        rows = [row for row in rows if row.get("模型名称") == _text(model_name)]
    return sorted(rows, key=lambda row: int(row.get("id", 0)))


def get_batch(batch_no_or_id: str | int, *, warehouse: Any = None) -> dict[str, Any] | None:
    """按批次号或 id 取批次清单，附带逐行明细。"""
    db = _db(warehouse)
    batch = None
    for row in db.rows(BATCH_TABLE):
        if _text(row.get("批次号")) == _text(batch_no_or_id) or int(row.get("id", 0)) == _int(batch_no_or_id):
            batch = row
            break
    if batch is None:
        return None
    result = dict(batch)
    result["清单"] = [
        dict(row) for row in db.rows(BATCH_ROW_TABLE)
        if row.get("批次号") == batch.get("批次号")
    ]
    return result


def batch_details(batch_no: str, *, warehouse: Any = None) -> list[dict[str, Any]]:
    db = _db(warehouse)
    return [
        dict(row) for row in db.rows(BATCH_ROW_TABLE)
        if row.get("批次号") == _text(batch_no)
    ]


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1
