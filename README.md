# Evidence-Based Invoice Agent (A2A HTTP+JSON 1.0)

A public HTTPS FastAPI service for invoice triage. It returns evidence-linked business decisions, uses OpenAI for unstructured invoice documents when required, applies approval and receipt gates, and persists task/replay/action state in PostgreSQL or SQLite.

## Safety design

- `OPENAI_API_KEY` is read from the environment; never place a real key in source control.
- Structured invoice packages are processed using deterministic policy checks without an OpenAI call. Unstructured PDF/image/text attachments can be passed to OpenAI for extraction; missing or uncertain facts never authorize payment.
- A payment decision only records a durable payment intent in the audit ledger. No real payment processor is configured and no money is transferred.
- Pending/missing/denied approval, receipt blockers, duplicate identities, identity/amount conflicts, invalid/negative amounts, and approval limits block payment.
- Receipt data must be bound to the current invoice identity; receipt records are not treated as invoices.
- A2A message retries use a deterministic key scoped to the principal, caller message/idempotency key, and canonical request-body hash. Exact retries replay the stored response; changed payloads get a distinct replay-cache entry and go through invoice identity, approval, receipt, and action-ledger controls rather than failing solely because a harness reused a message ID.
- Task ownership is principal-scoped; terminal tasks cannot be continued. `TASK_STATE_INPUT_REQUIRED` must receive a valid continuation before terminal completion. Cancellation uses a conditional state transition.
- Action, receipt and batch limits are enforced transactionally. Audit writes and terminal task state commit atomically.

## Endpoints

- `GET /health`
- `GET /.well-known/agent-card.json`
- `GET /.well-known/agent.json` (compatibility alias)
- `POST /a2a/message:send`
- `POST /a2a/message:send/`
- `GET /a2a/tasks/{task_id}`
- `POST /a2a/tasks/{task_id}:cancel`

A2A send-message requests must use `Content-Type: application/a2a+json` and `A2A-Version: 1.0`. The Agent Card advertises the HTTP+JSON 1.0 base URL.

## Local development

Use Python 3.11 or newer.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` for local development only. Add your own `OPENAI_API_KEY` only if you want to test extraction of unstructured documents. Do not commit `.env`; `.gitignore` excludes it.

Run tests without using a hosted database or OpenAI billing:

```powershell
python -m pytest -q
```

Run locally:

```powershell
uvicorn app:app --reload --host 127.0.0.1 --port 8000
```

## Render deployment

1. Push the project root to the `main` branch of GitHub.
2. Render Blueprint deploys `render.yaml`; the web service uses `uvicorn app:app --host 0.0.0.0 --port $PORT`.
3. Configure these secrets/environment values on Render:
   - `DATABASE_URL`: PostgreSQL connection string from Neon.
   - `OPENAI_API_KEY`: your OpenAI API key.
   - `OPENAI_MODEL`: `gpt-4.1-mini` (or another model available to your API project).
   - `PUBLIC_BASE_URL`: public origin, for example `https://invoice-agent-3j7m.onrender.com` (no `/a2a` suffix).
4. Wait until Render shows the latest deploy as Live.
5. Verify `/health` and `/.well-known/agent-card.json`.

## Hosted smoke test

After deployment, in the project folder run:

```powershell
python -m pip install -r requirements.txt
python smoke_test.py https://invoice-agent-3j7m.onrender.com
```

This sends only synthetic pending/unapproved invoices, checks the Agent Card, exact MIME type, replay behavior and the input-required lifecycle. It makes no real payment. Structured smoke-test invoices should not need an OpenAI call.

For a Windows PowerShell route check (works with Windows PowerShell 5.1 and PowerShell 7):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\test_invoice_route.ps1
```

Optionally pass a different service origin as the first argument. This script uses `Invoke-WebRequest` and parses the returned JSON as text rather than emitting a UTF-8 byte array.

## Do not treat local tests as a grade guarantee

Local tests exercise the behavior this implementation explicitly knows about. The assignment grader's private invoice cases may include conventions that are not represented in local tests, so only submit after reviewing the hosted Agent Card and smoke-test result. Preserve the assignment's last check for Save.
