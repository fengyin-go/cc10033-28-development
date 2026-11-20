"""DataVault 批次清单测试：逐条装载判定、重复提交去重、卫星更正与血缘追溯。"""
from __future__ import annotations

from typing import Any

import pytest

from app.services import vault_lineage, vault_manifest
from app.services.data_vault import DataVaultService
from app.store import Store
from app.seed import SEED_ROWS


@pytest.fixture
def warehouse() -> Store:
    return Store()


@pytest.fixture
def model(warehouse: Store) -> dict[str, Any]:
    row = {
        "id": 900, "模型名称": "订单批次模型", "Hub实体": "HUB_ORDER",
        "Link关系": "LNK_ORDER_CUSTOMER", "Satellite属性": "SAT_ORDER",
        "记录源": "pos_system", "哈希键": "MD5(order_id)",
        "status": "已建模", "pending": True, "abnormal": False,
    }
    warehouse.rows("data_vault").append(row)
    return row


@pytest.fixture
def customer_hub(warehouse: Store) -> dict[str, Any]:
    return vault_manifest.register_hub(
        "HUB_CUSTOMER", "C001", source="crm", warehouse=warehouse)


def _row(row_id: str, order_id: str, amount: str, status: str,
         source_time: str, hub: str = "C001", **extra: Any) -> dict[str, Any]:
    record = {
        "源行标识": row_id, "order_id": order_id,
        "amount": amount, "order_status": status, "源时间": source_time,
        "关联Hub": "HUB_CUSTOMER", "关联业务键": hub,
    }
    record.update(extra)
    return record


class TestBatchManifest:
    def test_each_source_row_recorded_with_keys_and_source_payload(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        rows = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00")]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)

        assert batch["上报行数"] == 1
        assert len(batch["清单"]) == 1
        detail = batch["清单"][0]
        assert detail["源行标识"] == "SRC-1"
        assert detail["业务键"] == "O1001"
        assert detail["Hub哈希键"]
        assert detail["Link哈希键"]
        assert detail["Satellite哈希键"]
        assert detail["来源数据"].startswith("{")
        assert detail["输入类型"] == vault_manifest.LOAD_NEW

        # 清单行持久化后可按批次号回查
        stored = vault_manifest.batch_details(batch["批次号"], warehouse=warehouse)
        assert len(stored) == 1
        assert stored[0]["来源数据"] == detail["来源数据"]

    def test_duplicate_submission_does_not_create_hub_or_link(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        rows = [
            _row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00"),
            _row("SRC-2", "O1001", "100", "NEW", "2026-03-01 10:00:00"),
        ]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        types = [detail["输入类型"] for detail in batch["清单"]]
        assert types == [vault_manifest.LOAD_NEW, vault_manifest.LOAD_DUPLICATE]

        hubs = [h for h in warehouse.rows(vault_manifest.HUB_TABLE) if h["Hub实体"] == "HUB_ORDER"]
        assert len(hubs) == 1
        assert len(warehouse.rows(vault_manifest.LINK_TABLE)) == 1
        assert len(warehouse.rows(vault_manifest.SATELLITE_TABLE)) == 1
        assert batch["Hub新增数"] == 1
        assert batch["Link新增数"] == 1

    def test_replaying_same_batch_is_all_duplicate(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        rows = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00")]
        vault_manifest.load_batch(model, rows, warehouse=warehouse)
        replay = vault_manifest.load_batch(model, rows, warehouse=warehouse)

        assert replay["新增行数"] == 0
        assert replay["重复行数"] == 1
        assert replay["Hub新增数"] == 0
        assert replay["Link新增数"] == 0
        assert len(warehouse.rows(vault_manifest.HUB_TABLE)) == 2  # HUB_CUSTOMER + HUB_ORDER
        assert len(warehouse.rows(vault_manifest.LINK_TABLE)) == 1
        assert len(warehouse.rows(vault_manifest.SATELLITE_TABLE)) == 1

    def test_satellite_change_keeps_old_and_new_values(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        first = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00")]
        second = [_row("SRC-3", "O1001", "150", "PAID", "2026-03-02 09:00:00")]
        vault_manifest.load_batch(model, first, warehouse=warehouse)
        batch = vault_manifest.load_batch(model, second, warehouse=warehouse)

        detail = batch["清单"][0]
        assert detail["输入类型"] == vault_manifest.LOAD_CORRECTION
        assert '"amount":"100"' in detail["旧值"]
        assert '"amount":"150"' in detail["新值"]
        assert set(detail["变更字段"].split(",")) == {"amount", "order_status"}

        versions = sorted(
            warehouse.rows(vault_manifest.SATELLITE_TABLE), key=lambda r: r["版本"])
        assert [v["版本"] for v in versions] == [1, 2]
        assert versions[0]["状态"] == vault_manifest.SATELLITE_HISTORY
        assert versions[1]["状态"] == vault_manifest.SATELLITE_CURRENT
        assert versions[1]["旧值"] == detail["旧值"]
        assert versions[1]["新值"] == detail["新值"]

    def test_duplicate_after_correction_is_not_treated_as_correction(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        original = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00")]
        correction = [_row("SRC-3", "O1001", "150", "PAID", "2026-03-02 09:00:00")]
        vault_manifest.load_batch(model, original, warehouse=warehouse)
        vault_manifest.load_batch(model, correction, warehouse=warehouse)

        # 原始行原样再次到达：与已装载历史版本完全一致，判为重复提交。
        replay_original = vault_manifest.load_batch(model, original, warehouse=warehouse)
        assert replay_original["清单"][0]["输入类型"] == vault_manifest.LOAD_DUPLICATE

        # 更正行重复到达：与当前版本属性完全一致，同样判为重复提交。
        replay_correction = vault_manifest.load_batch(model, correction, warehouse=warehouse)
        assert replay_correction["清单"][0]["输入类型"] == vault_manifest.LOAD_DUPLICATE
        assert len(warehouse.rows(vault_manifest.SATELLITE_TABLE)) == 2

    def test_late_row_with_new_values_is_correction_not_duplicate(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        current = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00")]
        latest = [_row("SRC-3", "O1001", "150", "PAID", "2026-03-03 09:00:00")]
        vault_manifest.load_batch(model, current, warehouse=warehouse)
        vault_manifest.load_batch(model, latest, warehouse=warehouse)

        # 源时间早于当前版本但属性确实不同：是迟到的卫星属性更正，不能当重复输入丢弃。
        late = [_row("SRC-4", "O1001", "120", "SHIPPED", "2026-03-02 08:00:00")]
        batch = vault_manifest.load_batch(model, late, warehouse=warehouse)
        detail = batch["清单"][0]
        assert detail["输入类型"] == vault_manifest.LOAD_LATE_CORRECTION
        assert detail["是否迟到"] == "是"
        assert '"amount":"120"' in detail["新值"]
        assert '"amount":"150"' in detail["旧值"]

        versions = sorted(warehouse.rows(vault_manifest.SATELLITE_TABLE), key=lambda r: r["版本"])
        assert len(versions) == 3
        # 迟到版本落为历史，当前值仍是 SRC-3 的数据。
        assert versions[1]["状态"] == vault_manifest.SATELLITE_CURRENT
        assert versions[2]["状态"] == vault_manifest.SATELLITE_HISTORY

    def test_link_to_unconfirmed_hub_is_rejected(
            self, warehouse: Store, model: dict[str, Any]) -> None:
        vault_manifest.register_hub("HUB_CUSTOMER", "C999", confirmed=False, warehouse=warehouse)
        rows = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00", hub="C999")]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)

        detail = batch["清单"][0]
        assert detail["输入类型"] == vault_manifest.LOAD_REJECTED
        assert "未确认" in detail["拒绝原因"]
        assert warehouse.rows(vault_manifest.LINK_TABLE) == []
        assert batch["Link新增数"] == 0

    def test_confirming_hub_allows_previous_rejected_link(
            self, warehouse: Store, model: dict[str, Any]) -> None:
        vault_manifest.register_hub("HUB_CUSTOMER", "C999", confirmed=False, warehouse=warehouse)
        rows = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00", hub="C999")]
        rejected = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        assert rejected["清单"][0]["输入类型"] == vault_manifest.LOAD_REJECTED

        vault_manifest.confirm_hub("HUB_CUSTOMER", "C999", warehouse=warehouse)
        accepted = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        assert accepted["清单"][0]["输入类型"] == vault_manifest.LOAD_NEW
        assert accepted["Link新增数"] == 1
        assert len(warehouse.rows(vault_manifest.LINK_TABLE)) == 1

    def test_missing_business_key_is_rejected(
            self, warehouse: Store, model: dict[str, Any]) -> None:
        rows = [{"源行标识": "SRC-X", "amount": "10"}]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        assert batch["清单"][0]["输入类型"] == vault_manifest.LOAD_REJECTED
        assert batch["清单"][0]["拒绝原因"]
        assert batch["状态"] == vault_manifest.BATCH_REJECTED


class TestLoadEntryPoint:
    def test_execute_load_writes_manifest_batch(self, warehouse: Store, monkeypatch) -> None:
        monkeypatch.setattr("app.services.data_vault.store", warehouse)
        service = DataVaultService()
        entry, _ = service.create_entry({
            "模型名称": "服务层模型", "Hub实体": "HUB_SVC",
            "Link关系": "LNK_SVC", "Satellite属性": "SAT_SVC",
            "记录源": "pos_system", "哈希键": "MD5(order_id)",
        })
        assert entry is not None

        loaded, message = service.execute_load(entry["id"], [
            {"源行标识": "S-1", "order_id": "O9", "amount": "5"},
            {"源行标识": "S-2", "order_id": "O9", "amount": "5"},
        ])
        assert loaded is not None
        assert "批次" in message
        assert len(vault_manifest.list_batches(warehouse=warehouse)) == 1

    def test_action_without_records_still_records_batch(self, warehouse: Store, monkeypatch) -> None:
        monkeypatch.setattr("app.services.data_vault.store", warehouse)
        service = DataVaultService()
        entry, _ = service.create_entry({
            "模型名称": "空载模型", "Hub实体": "HUB_EMPTY", "Link关系": "LNK_EMPTY",
        })
        assert entry is not None
        loaded, _ = service.execute_load(entry["id"], [])
        assert loaded is not None
        batch = vault_manifest.get_batch(loaded["最近批次号"], warehouse=warehouse)
        assert batch is not None
        assert batch["状态"] == vault_manifest.BATCH_EMPTY


class TestVaultLineage:
    def test_lineage_records_actual_load_source(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        rows = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00")]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        created = vault_lineage.build_lineage_for_batch(batch, warehouse=warehouse)

        relations = [r for r in created if r["关联类型"].startswith("Vault")]
        assert relations
        assert all(r["批次号"] == batch["批次号"] for r in relations)
        assert all(r["源行标识"] == "SRC-1" for r in relations)
        assert all(r["上游模型"] == "src:pos_system" for r in relations)

        objects = {r["装载对象"] for r in relations}
        assert "Hub:HUB_ORDER" in objects
        assert "Link:LNK_ORDER_CUSTOMER" in objects
        assert "Satellite:SAT_ORDER" in objects

    def test_lineage_is_idempotent(self, warehouse: Store, model: dict[str, Any],
                                   customer_hub: dict[str, Any]) -> None:
        rows = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00")]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        first = vault_lineage.build_lineage_for_batch(batch, warehouse=warehouse)
        second = vault_lineage.consume_vault_batches(warehouse=warehouse)
        assert first
        assert second["created_relations"] == 0

    def test_correction_lineage_keeps_source_row_and_batch(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        first = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00")]
        second = [_row("SRC-3", "O1001", "150", "PAID", "2026-03-02 09:00:00")]
        vault_manifest.load_batch(model, first, warehouse=warehouse)
        batch = vault_manifest.load_batch(model, second, warehouse=warehouse)
        created = vault_lineage.build_lineage_for_batch(batch, warehouse=warehouse)

        corrections = [r for r in created if r["关联类型"] == "Vault卫星更正"]
        assert len(corrections) == 1
        correction = corrections[0]
        assert correction["源行标识"] == "SRC-3"
        assert correction["批次号"] == batch["批次号"]
        assert "SRC-3" in correction["更正来源"]
        assert '"amount":"150"' in correction["新值"]
        assert '"amount":"100"' in correction["旧值"]

    def test_duplicate_and_rejected_rows_do_not_create_lineage(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        vault_manifest.register_hub("HUB_CUSTOMER", "C999", confirmed=False, warehouse=warehouse)
        rows = [
            _row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00"),
            _row("SRC-2", "O1001", "100", "NEW", "2026-03-01 10:00:00"),
            _row("SRC-3", "O1002", "50", "NEW", "2026-03-01 11:00:00", hub="C999"),
        ]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        created = vault_lineage.build_lineage_for_batch(batch, warehouse=warehouse)

        source_rows = {r["源行标识"] for r in created}
        assert "SRC-1" in source_rows
        assert "SRC-2" not in source_rows
        assert "SRC-3" not in source_rows

    def test_link_lineage_requires_confirmed_hubs(
            self, warehouse: Store, model: dict[str, Any]) -> None:
        vault_manifest.register_hub("HUB_CUSTOMER", "C999", confirmed=False, warehouse=warehouse)
        rows = [_row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00", hub="C999")]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        created = vault_lineage.build_lineage_for_batch(batch, warehouse=warehouse)
        assert created == []
        assert all("未建关系" in d["血缘状态"] for d in batch["清单"])


class TestMixedBatch:
    def test_original_duplicate_correction_in_one_batch(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        rows = [
            _row("SRC-1", "O1001", "100", "NEW", "2026-03-01 10:00:00"),
            _row("SRC-2", "O1001", "100", "NEW", "2026-03-01 10:00:00"),
            _row("SRC-3", "O1001", "150", "PAID", "2026-03-02 09:00:00"),
        ]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)
        assert [d["输入类型"] for d in batch["清单"]] == [
            vault_manifest.LOAD_NEW,
            vault_manifest.LOAD_DUPLICATE,
            vault_manifest.LOAD_CORRECTION,
        ]
        assert batch["新增行数"] == 1
        assert batch["重复行数"] == 1
        assert batch["更正行数"] == 1
        assert batch["Hub新增数"] == 1
        assert batch["Link新增数"] == 1

        created = vault_lineage.build_lineage_for_batch(batch, warehouse=warehouse)
        loaded_rows = {r["源行标识"] for r in created}
        assert loaded_rows == {"SRC-1", "SRC-3"}
        # SRC-1 与 SRC-3 各自建立 Hub 关系，但 Hub 只有一条。
        hub_relations = [r for r in created if r["关联类型"] == "Vault装载" and r["对象名称"] == "HUB_ORDER"]
        assert len(hub_relations) == 2
        order_hubs = [h for h in warehouse.rows(vault_manifest.HUB_TABLE) if h["Hub实体"] == "HUB_ORDER"]
        assert len(order_hubs) == 1


class TestLineageServiceConsumesBatches:
    """模型血缘通过服务入口消费批次清单，而不是只读通道。"""

    def test_analyze_impact_consumes_pending_batches(
            self, warehouse: Store, monkeypatch) -> None:
        from app.services import model_lineage

        monkeypatch.setattr("app.services.data_vault.store", warehouse)
        monkeypatch.setattr("app.services.model_lineage.store", warehouse)
        monkeypatch.setattr("app.services.vault_lineage.store", warehouse)

        service = DataVaultService()
        entry, _ = service.create_entry({
            "模型名称": "血缘消费模型", "Hub实体": "HUB_LIN", "Link关系": "LNK_LIN",
            "Satellite属性": "SAT_LIN", "记录源": "lin_src", "哈希键": "MD5(order_id)",
        })
        service.execute_load(entry["id"], [
            {"源行标识": "L-1", "order_id": "O-L", "amount": "1"},
            {"源行标识": "L-2", "order_id": "O-L", "amount": "2"},
        ], batch_id="VB-LIN-1")

        lineage_service = model_lineage.ModelLineageService()
        relation, _ = lineage_service.create_entry({
            "关系ID": "LIN-VAULT-1", "上游模型": "src:lin_src",
            "下游模型": "血缘消费模型", "关联类型": "引用",
        })
        assert relation is not None
        lineage_service.run_action(relation["id"], "分析影响")

        rows = [
            row for row in warehouse.rows("model_lineage")
            if str(row.get("关系ID", "")).startswith("VLIN-VB-LIN-1")
        ]
        assert rows
        assert {row["源行标识"] for row in rows} == {"L-1", "L-2"}
        snapshot = lineage_service.domain_snapshot()
        assert snapshot["vault_lineage"]["total"] == len(rows)
        assert snapshot["vault_lineage"]["corrections"] == 1


class TestLinkGuardForUnconfirmedPrimaryHub:
    def test_unconfirmed_primary_hub_never_produces_link(
            self, warehouse: Store, model: dict[str, Any], customer_hub: dict[str, Any]) -> None:
        rows = [_row("SRC-U", "O2001", "10", "NEW", "2026-03-01 10:00:00",
                     **{"Hub确认状态": "未确认"})]
        batch = vault_manifest.load_batch(model, rows, warehouse=warehouse)

        detail = batch["清单"][0]
        assert detail["输入类型"] == vault_manifest.LOAD_REJECTED
        assert detail["Link装载状态"] == vault_manifest.LINK_REJECTED
        assert "未确认" in detail["拒绝原因"]
        assert warehouse.rows(vault_manifest.LINK_TABLE) == []

        created = vault_lineage.build_lineage_for_batch(batch, warehouse=warehouse)
        assert created == []


# ---------------------------------------------------------------------------
# HTTP 层：批量提交 → 批次清单 → 血缘消费
# ---------------------------------------------------------------------------


import asyncio
import json

from app.main import app


def _request(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    payload = json.dumps(body).encode() if body is not None else b""

    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": method, "scheme": "http", "path": path, "raw_path": path.encode(),
        "query_string": b"", "headers": [(b"content-type", b"application/json")],
        "client": ("127.0.0.1", 1234), "server": ("127.0.0.1", 8000),
    }
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        nonlocal payload
        if payload:
            data, payload = payload, b""
            return {"type": "http.request", "body": data, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    asyncio.run(app(scope, receive, send))
    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return status, json.loads(raw) if raw else None


class TestVaultBatchApi:
    def _create_model(self, monkeypatch) -> dict[str, Any]:
        from app.store import store as global_store
        monkeypatch.setattr("app.services.data_vault.store", global_store)
        service = DataVaultService()
        entry, _ = service.create_entry({
            "模型名称": "API批次模型", "Hub实体": "HUB_API", "Link关系": "LNK_API",
            "Satellite属性": "SAT_API", "记录源": "api_src", "哈希键": "MD5(order_id)",
        })
        assert entry is not None
        return entry

    def test_batch_submission_records_each_input_type(self, monkeypatch) -> None:
        from app.store import store as global_store
        self._create_model(monkeypatch)
        status, batch = _request("POST", "/api/data_vault/batches", {
            "model_name": "API批次模型",
            "batch_id": "VB-API-TEST",
            "records": [
                {"源行标识": "P-1", "order_id": "O-API", "amount": "10"},
                {"源行标识": "P-2", "order_id": "O-API", "amount": "10"},
                {"源行标识": "P-3", "order_id": "O-API", "amount": "30"},
            ],
        })
        assert status == 200
        assert [row["输入类型"] for row in batch["清单"]] == ["新增", "重复提交", "Satellite更正"]

        status, detail = _request("GET", "/api/data_vault/batches/VB-API-TEST")
        assert status == 200
        assert [row["源行标识"] for row in detail["清单"]] == ["P-1", "P-2", "P-3"]
        assert detail["清单"][2]["旧值"] == '{"amount":"10"}'
        assert detail["清单"][2]["新值"] == '{"amount":"30"}'

        status, result = _request("POST", "/api/model_lineage/vault-relations/consume")
        assert status == 200
        assert result["consumed_batches"] >= 1
        assert result["snapshot"]["total"] >= 3
        assert result["snapshot"]["corrections"] >= 1

        # 清空 API 测试模型，避免影响其它测试。
        global_store.rows("data_vault")[:] = [
            row for row in global_store.rows("data_vault")
            if row.get("模型名称") != "API批次模型"
        ]

    def test_action_endpoint_accepts_top_level_records(self, monkeypatch) -> None:
        from app.store import store as global_store
        entry = self._create_model(monkeypatch)
        status, payload = _request("POST", f"/api/data_vault/{entry['id']}/actions", {
            "action": "执行加载",
            "batch_id": "VB-API-ACTION",
            "records": [
                {"源行标识": "Q-1", "order_id": "O-Q", "amount": "7"},
                {"源行标识": "Q-2", "order_id": "O-Q", "amount": "7"},
            ],
        })
        assert status == 200
        assert payload["ok"] is True
        assert "VB-API-ACTION" in payload["message"]

        status, batch = _request("GET", "/api/data_vault/batches/VB-API-ACTION")
        assert status == 200
        assert batch["重复行数"] == 1
        assert len(batch["清单"]) == 2

        global_store.rows("data_vault")[:] = [
            row for row in global_store.rows("data_vault")
            if row.get("模型名称") != "API批次模型"
        ]
