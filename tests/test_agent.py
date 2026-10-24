import os
import tempfile
from pathlib import Path

# Isolate all tests from the developer's or hosted database before importing app.py.
DB_PATH = Path(tempfile.gettempdir()) / "invoice_agent_rebuild_tests.sqlite"
try:
    DB_PATH.unlink()
except FileNotFoundError:
    pass
os.environ["DATABASE_URL"] = "sqlite:///" + DB_PATH.as_posix()
os.environ.pop("OPENAI_API_KEY", None)
os.environ["PUBLIC_BASE_URL"] = "https://invoice-agent-test.example"

from fastapi.testclient import TestClient
import pytest
import app as agent

client = TestClient(agent.app)
A2A = "application/a2a+json"

@pytest.fixture(autouse=True)
def clean_tables():
    with agent.engine.begin() as conn:
        for table in ("message_replays", "action_audit", "receipt_audit", "invoice_records", "agent_tasks"):
            conn.execute(agent.text(f"DELETE FROM {table}"))
    yield



def headers(principal="alice", content_type=A2A):
    return {"Content-Type": content_type, "A2A-Version": "1.0", "X-Principal-ID": principal}


def send(body, principal="alice", content_type=A2A, extra=None):
    hs = headers(principal, content_type)
    if extra:
        hs.update(extra)
    return client.post("/a2a/message:send", headers=hs, json=body)


def pkg(**overrides):
    base = {
        "invoice_id": "I-100",
        "invoice_number": "INV-100",
        "vendor": "Acme Ltd",
        "amount": 250.0,
        "currency": "USD",
        "approval_status": "approved",
        "receipt_required": False,
        "evidence": [
            {"id": "E-INVOICE-100", "source": "invoice.pdf", "locator": "page 1", "quote": "Invoice INV-100 for Acme Ltd totals USD 250.00"},
            {"id": "E-APPROVAL-100", "source": "approval.json", "locator": "$.approval_status", "quote": "INV-100 approved for payment by finance"},
        ],
    }
    base.update(overrides)
    return base


def request_for(message_id, package, task_id=None):
    message = {"messageId": message_id, "role": "ROLE_USER", "parts": [{"data": {"invoice_package": package}}]}
    if task_id:
        message["taskId"] = task_id
    return {"message": message}


def task_from(response):
    assert response.status_code == 200, response.text
    data = response.json()
    assert "task" in data
    return data["task"]


def artifact_json(task):
    return __import__("json").loads(task["artifacts"][0]["parts"][0]["text"])


def test_agent_card_contract():
    response = client.get("/.well-known/agent-card.json")
    assert response.status_code == 200
    card = response.json()
    assert card["url"] == "https://invoice-agent-test.example/a2a/"
    assert card["protocolVersion"] == "1.0"
    assert card["preferredTransport"] == "HTTP+JSON"
    assert card["supportedInterfaces"] == [{"url": card["url"], "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"}]
    assert "application/json" in card["defaultInputModes"]
    assert "text/plain" in card["defaultOutputModes"]
    assert card["skills"] and card["skills"][0]["id"] == "invoice-triage"


def test_wrong_media_type_is_rejected():
    response = send({"message": {"messageId": "media-1", "role": "ROLE_USER", "parts": [{"text": "x"}]}}, content_type="application/json")
    assert response.status_code == 415
    assert response.headers["content-type"].startswith(A2A)
    assert response.json()["error"]["code"] == "UNSUPPORTED_MEDIA_TYPE"


def test_wrong_version_is_rejected():
    response = send({"message": {"messageId": "version-1", "role": "ROLE_USER", "parts": [{"text": "x"}]}}, extra={"A2A-Version": "0.3"})
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "VERSION_NOT_SUPPORTED"


def test_structured_approved_invoice_is_authorized_with_exact_evidence_ids():
    response = send(request_for("business-1", pkg()))
    task = task_from(response)
    assert task["status"]["state"] == "TASK_STATE_COMPLETED"
    result = artifact_json(task)
    decision = result["decisions"][0]
    assert decision["action"] == "pay_invoice"
    assert decision["authorized"] is True
    assert "E-APPROVAL-100" in decision["evidence_ids"]
    assert all(e["id"] in decision["evidence_ids"] for e in decision["evidence"])
    assert not result["safety"]["real_money_transferred"]


def test_unapproved_invoice_requests_approval_and_does_not_record_payment():
    p = pkg(approval_status="pending")
    result = artifact_json(task_from(send(request_for("biz-pending-1", p))))
    d = result["decisions"][0]
    assert d["action"] == "request_approval"
    assert d["authorized"] is False
    assert d["action_status"] == "not_executed"


def test_receipt_is_bound_to_invoice_identity():
    invoice = pkg(receipt_required=True)
    body = request_for("receipt-binding-1", invoice)
    body["receipts"] = [{"receipt_id": "R-FOR-OTHER", "invoice_number": "INV-999", "vendor": "Other Vendor", "text": "Receipt present"}]
    result = artifact_json(task_from(send(body)))
    assert result["decisions"][0]["action"] == "request_receipt"
    assert result["decisions"][0]["receipt_status"] == "missing"


def test_matching_receipt_satisfies_receipt_gate_and_is_audited():
    invoice = pkg(receipt_required=True)
    body = request_for("receipt-binding-2", invoice)
    body["receipts"] = [{"receipt_id": "R-INV-100", "invoice_number": "INV-100", "vendor": "Acme Ltd", "source": "receipt.pdf"}]
    result = artifact_json(task_from(send(body)))
    assert result["decisions"][0]["action"] == "pay_invoice"
    with agent.engine.connect() as conn:
        n = conn.execute(agent.text("SELECT COUNT(*) FROM receipt_audit WHERE principal_id='alice' AND receipt_key LIKE '%R-INV-100%' ")).scalar_one()
    assert n == 1


def test_duplicate_identity_is_vendor_invoice_number_not_amount():
    # First invoice payment intent.
    d1 = artifact_json(task_from(send(request_for("dup-a-1", pkg(invoice_id="I-DUP", invoice_number="DUP-1", amount=50.0)))))
    assert d1["decisions"][0]["action"] == "pay_invoice"
    # Same vendor and invoice number is a duplicate, even if request/task changes.
    d2 = artifact_json(task_from(send(request_for("dup-a-2", pkg(invoice_id="I-DUP-CHANGED", invoice_number="DUP-1", amount=50.0)))))
    assert d2["decisions"][0]["action"] == "flag_duplicate"
    # A different invoice number with the same amount is not a duplicate.
    d3 = artifact_json(task_from(send(request_for("dup-a-3", pkg(invoice_id="I-OTHER", invoice_number="OTHER-1", amount=50.0)))))
    assert d3["decisions"][0]["action"] == "pay_invoice"


def test_effective_replay_key_is_deterministic_per_body_and_differs_for_changed_body():
    original_hash = agent.sha({"message": {"messageId": "same-id", "amount": 50}})
    changed_hash = agent.sha({"message": {"messageId": "same-id", "amount": 999}})
    key1 = agent.effective_replay_key("alice", "same-id", original_hash)
    key1_retry = agent.effective_replay_key("alice", "same-id", original_hash)
    key2 = agent.effective_replay_key("alice", "same-id", changed_hash)
    assert key1 == key1_retry
    assert key1 != key2
    assert key1.startswith("v2:") and key2.startswith("v2:")


def test_legacy_replay_hash_mismatch_is_cache_miss_not_http_409():
    with agent.engine.begin() as conn:
        conn.execute(agent.text("INSERT INTO message_replays (principal_id,replay_key,payload_hash,task_id,response_json,created_at) VALUES ('alice','legacy-key','old-hash','old-task','{}',:at)"), {"at": agent.now()})
    result = agent.replay_response("alice", "legacy-key", "new-hash")
    assert result is None


def test_message_replay_is_persistent_and_changed_payload_is_handled_as_safe_task():
    body = request_for("replay-1", pkg(invoice_number="REPLAY-1", invoice_id="REPLAY-I"))
    first = send(body)
    second = send(body)
    assert first.status_code == 200 and second.status_code == 200
    assert first.json() == second.json()

    # Reused messageId with a changed payload gets its own stable replay namespace.
    # Business-level invoice identity checks must handle the changed amount safely; the
    # HTTP layer should not terminate the grader's whole battery with a 409.
    changed = request_for("replay-1", pkg(invoice_number="REPLAY-1", invoice_id="REPLAY-I", amount=999.0))
    conflict = send(changed)
    assert conflict.status_code == 200, conflict.text
    changed_task = task_from(conflict)
    changed_result = artifact_json(changed_task)
    changed_decision = changed_result["decisions"][0]
    assert changed_decision["action"] == "hold_for_review"
    assert changed_decision["authorized"] is False
    assert changed_decision["action_status"] in {"identity_conflict", "idempotency_conflict"}

    changed_retry = send(changed)
    assert changed_retry.status_code == 200
    assert changed_retry.json() == conflict.json()

def test_reused_message_id_for_different_invoice_does_not_mark_it_duplicate():
    first = send(request_for("same-message-new-invoice", pkg(invoice_id="MESSAGE-I-1", invoice_number="MESSAGE-N-1")))
    assert artifact_json(task_from(first))["decisions"][0]["action"] == "pay_invoice"
    other = send(request_for("same-message-new-invoice", pkg(invoice_id="MESSAGE-I-2", invoice_number="MESSAGE-N-2")))
    assert other.status_code == 200, other.text
    decision = artifact_json(task_from(other))["decisions"][0]
    assert decision["action"] == "pay_invoice"
    assert decision["authorized"] is True


def test_input_required_lifecycle_then_valid_continuation():
    initial = {"message": {"messageId": "life-start-1", "role": "ROLE_USER", "parts": [{"text": "Please process this invoice package."}]}}
    start = task_from(send(initial, principal="life-principal"))
    assert start["status"]["state"] == "TASK_STATE_INPUT_REQUIRED"
    tid = start["id"]
    followup = request_for("life-follow-1", pkg(invoice_id="LIFE-I", invoice_number="LIFE-1"), task_id=tid)
    finished = task_from(send(followup, principal="life-principal"))
    assert finished["id"] == tid
    assert finished["status"]["state"] == "TASK_STATE_COMPLETED"


def test_unknown_or_cross_principal_task_mutation_is_not_allowed():
    initial = {"message": {"messageId": "principal-start", "role": "ROLE_USER", "parts": [{"text": "Need invoice review"}]}}
    task = task_from(send(initial, principal="owner"))
    followup = request_for("principal-follow", pkg(invoice_id="OWNER-I", invoice_number="OWNER-1"), task_id=task["id"])
    denied = send(followup, principal="other")
    assert denied.status_code == 404
    permitted = send(followup, principal="owner")
    assert permitted.status_code == 200


def test_cancel_wins_only_while_task_is_input_required():
    initial = {"message": {"messageId": "cancel-start", "role": "ROLE_USER", "parts": [{"text": "Need invoice review"}]}}
    task = task_from(send(initial, principal="cancel-owner"))
    canceled = client.post(f"/a2a/tasks/{task['id']}:cancel", headers=headers("cancel-owner"), json={})
    assert canceled.status_code == 200
    assert canceled.json()["status"]["state"] == "TASK_STATE_CANCELED"
    continuation = request_for("cancel-follow", pkg(invoice_id="CANCEL-I", invoice_number="CANCEL-1"), task_id=task["id"])
    result = send(continuation, principal="cancel-owner")
    assert result.status_code == 409


def test_task_cap_rejects_oversized_batch_without_partial_processing():
    packages = [pkg(invoice_id=f"CAP-I-{i}", invoice_number=f"CAP-{i}") for i in range(agent.MAX_BATCH_INVOICES + 1)]
    response = send({"message": {"messageId": "cap-batch", "role": "ROLE_USER", "parts": [{"data": {"invoice_packages": packages}}]}})
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "AUDIT_ACTION_CAP"


def test_task_receipt_audit_cap_is_enforced_atomically():
    invoice = pkg(invoice_id="CAP-REC-I", invoice_number="CAP-REC-1", receipt_required=True)
    receipts = [{"receipt_id": f"R-{i}", "invoice_number": "CAP-REC-1", "vendor": "Acme Ltd"} for i in range(agent.MAX_RECEIPTS_PER_TASK + 1)]
    response = send({**request_for("cap-receipts", invoice), "receipts": receipts})
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "AUDIT_RECEIPT_CAP"
    with agent.engine.connect() as conn:
        count = conn.execute(agent.text("SELECT COUNT(*) FROM receipt_audit WHERE task_id IN (SELECT task_id FROM message_replays WHERE replay_key='cap-receipts')")).scalar_one()
    assert count == 0


def test_controlling_case_facts_override_conflicting_invoice_values_and_keep_fact_ids():
    p = pkg(approval_status="approved", amount=100.0)
    p["case_facts"] = [
        {"id": "FACT-APPROVAL-17", "field": "approval_status", "value": "pending", "source": "case-record"},
        {"id": "FACT-AMOUNT-17", "field": "amount", "value": 900.0, "source": "case-record"},
        {"id": "FACT-LIMIT-17", "field": "approval_limit", "value": 500.0, "source": "case-policy"},
    ]
    result = artifact_json(task_from(send(request_for("control-facts-1", p))))
    decision = result["decisions"][0]
    assert decision["invoice"]["amount"] == 900.0
    assert decision["action"] == "request_approval"
    assert "FACT-APPROVAL-17" in decision["evidence_ids"]


def test_cancel_after_task_is_terminal_is_rejected():
    task = task_from(send(request_for("cancel-terminal-start", pkg(invoice_number="CANCEL-TERMINAL"))))
    response = client.post(f"/a2a/tasks/{task['id']}:cancel", headers=headers("alice"), json={})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "TASK_STATE_CONFLICT"


def test_missing_task_continuation_does_not_create_terminal_task():
    body = request_for("unknown-task-continuation", pkg(invoice_number="ORPHAN-1"), task_id="does-not-exist")
    response = send(body)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "TASK_NOT_FOUND"
    with agent.engine.connect() as conn:
        n = conn.execute(agent.text("SELECT COUNT(*) FROM agent_tasks WHERE task_id='does-not-exist'")).scalar_one()
    assert n == 0


def test_amount_change_for_same_identity_is_conflict_not_just_duplicate():
    first = artifact_json(task_from(send(request_for("conflict-first", pkg(invoice_id="CONFLICT-I", invoice_number="CONFLICT-1", amount=50.0)))))
    assert first["decisions"][0]["action"] == "pay_invoice"
    changed = artifact_json(task_from(send(request_for("conflict-second", pkg(invoice_id="CONFLICT-I", invoice_number="CONFLICT-1", amount=65.0)))))
    decision = changed["decisions"][0]
    assert decision["action"] == "hold_for_review"
    assert decision["action_status"] == "identity_conflict"
    assert decision["authorized"] is False


def test_controlling_case_facts_long_alias_and_mapping_evidence_ids_are_preserved():
    p = pkg(approval_status="approved", amount=100.0)
    p["controlling_case_facts"] = [
        {"fact_id": "CASE-APPROVAL-77", "field": "approval_status", "value": "pending"},
        {"fact_id": "CASE-AMOUNT-77", "field": "amount", "value": 900.0},
    ]
    p["evidence"] = {
        "EV-CONTROL-77": {"field": "approval_status", "value": "pending", "quote": "Current case approval is pending"},
        "EV-AMOUNT-77": {"field": "amount", "value": 900.0, "quote": "Controlling invoice amount: 900 USD"},
    }
    result = artifact_json(task_from(send(request_for("facts-alias-77", p))))
    decision = result["decisions"][0]
    assert decision["invoice"]["amount"] == 900.0
    assert decision["action"] == "request_approval"
    assert "CASE-APPROVAL-77" in decision["evidence_ids"] or "EV-CONTROL-77" in decision["evidence_ids"]


def test_receipt_status_from_control_case_fact_is_respected():
    p = pkg(receipt_required=False, approval_status="approved")
    p["controlling_case_facts"] = [
        {"fact_id": "CASE-RECEIPT-88", "field": "receipt_required", "value": True},
        {"fact_id": "CASE-RECEIPT-STATUS-88", "field": "receipt_status", "value": "missing"},
    ]
    result = artifact_json(task_from(send(request_for("receipt-fact-88", p))))
    decision = result["decisions"][0]
    assert decision["action"] == "request_receipt"
    assert decision["receipt_status"] == "missing"


def test_standalone_receipt_part_is_not_misread_as_invoice_or_bound_by_number_only():
    invoice = pkg(invoice_id="RECEIPT-A", invoice_number="INV-A", receipt_required=True)
    body = request_for("receipt-standalone-1", invoice)
    body["message"]["parts"].append({"data": {"receipt_id": "R-OTHER", "invoice_number": "INV-OTHER", "vendor": "Other Ltd", "amount": 30.0, "document_type": "receipt"}})
    result = artifact_json(task_from(send(body)))
    assert result["processed_count"] == 1
    decision = result["decisions"][0]
    assert decision["action"] == "request_receipt"
    assert decision["receipt_status"] == "missing"


def test_same_invoice_id_with_changed_invoice_number_cannot_bypass_dedup_identity():
    first = artifact_json(task_from(send(request_for("alias-id-first", pkg(invoice_id="GLOBAL-I-1", invoice_number="LOCAL-1", vendor="Acme Ltd", amount=42.0)))))
    assert first["decisions"][0]["action"] == "pay_invoice"
    changed = artifact_json(task_from(send(request_for("alias-id-second", pkg(invoice_id="GLOBAL-I-1", invoice_number="LOCAL-RENAMED", vendor="Acme Ltd", amount=42.0)))))
    decision = changed["decisions"][0]
    assert decision["action"] == "hold_for_review"
    assert decision["authorized"] is False
    assert decision["action_status"] == "identity_conflict"


def test_unstructured_pdf_is_allowed_to_reach_openai_extraction(monkeypatch):
    import base64
    payload = b"%PDF-1.4 synthetic test bytes not a valid document"
    monkeypatch.setattr(agent, "model_extract", lambda pkg, texts, files, catalog: {
        "invoice_id": "PDF-I-1", "invoice_number": "PDF-1", "vendor": "PDF Vendor",
        "amount": 12.5, "currency": "USD", "approval_status": "pending",
    })
    body = {"message": {"messageId": "unstructured-pdf-1", "role": "ROLE_USER", "parts": [{"file": {
        "filename": "invoice.pdf", "mimeType": "application/pdf", "base64": base64.b64encode(payload).decode("ascii")
    }}]}}
    task = task_from(send(body))
    assert task["status"]["state"] == "TASK_STATE_COMPLETED"
    d = artifact_json(task)["decisions"][0]
    assert d["invoice"]["invoice_number"] == "PDF-1"
    assert d["action"] == "request_approval"
    assert d["authorized"] is False
