# HG Demo Agent — BioT connectivity agent (Hologic presale demo)

Python device agent simulating a Hologic Dimensions mammography workstation
(`DIM-4521`) connected to BioT Demo2. Built for the Hologic NextGen Connectivity
presale demo (meeting week of 2026-08-03). Companion portal:
[hologic-nexgen-connectivity](https://github.com/dannyadler/hologic-nexgen-connectivity)
(Lovable, BioT REST).

Runs on macOS and Windows 11 (ARM64, verified live under UTM on Apple Silicon).

## What it implements (all live against BioT 2.4.1)

| Capability | Hologic questionnaire | Mechanism |
|---|---|---|
| Persistent outbound-only connectivity + reachability | Q1 | MQTT/mTLS (per-device X.509), clientId `dev_DIM-4521`, `_status._connection` on platform |
| Status telemetry | Q17/Q18 | `<clientId>/from-device/status`, `{metadata:{timestamp_ms}, data:{STATUS attrs}}` every 10s |
| Offline store-and-forward, chronological replay | Q31 | SQLite queue, drains on reconnect, no silent loss, queue depth reported |
| OTA with Hologic approval gate | Q7/Q10 | `configuration` named shadow delta → simulated install → reported state + status |
| Device REST API access | — | MQTT token flow: publish empty to `<clientId>/from-device/token`, JWT arrives on `to-device/token` |
| Error events + event-triggered log retrieval | Q11 | `e` key → hg_device_event entity + gzipped log capture → File API upload → hg_log_bundle entity |
| Persistent device state | — | SW version + exam counter survive restarts (SQLite kv) |

## Run

```sh
python3 -m venv .venv && source .venv/bin/activate   # Windows: python -m venv .venv && .venv\Scripts\activate
pip install paho-mqtt
python agent.py
```

Keys: `x` exam, `e` error event + log bundle, `q` quit.
Self-check: `python test_queue.py`. Set `"debugShadow": true` in config.json to
log raw shadow traffic.

## Files

- `agent.py` — everything (single file by design for the demo)
- `config.json` — device identity, endpoints, org and template IDs (no secrets)
- `certs/` — NOT in git. Per-device X.509 cert from BioT (portal: device → generate certificate). Place `certificate.pem`, `private_key.pem`, `ca.pem` here.
- `test_queue.py` — offline-queue self-check

## Platform contracts (live-verified, saves R&D the digging)

- Status attrs come back NESTED under `_status` on GET /device/v2/devices/{id}; config attrs under `_configuration`.
- Device create API needs `_id` + `_templateId` (flat); wrong body shape returns a misleading 403 ACCESS_DENIED.
- Config shadow (`$aws/things/<clientId>/shadow/name/configuration`) delivers reference attributes as `{id}` ONLY — no display name. The portal therefore also writes `hgm_targetSwName` (plain Label) so the device knows what to install.
- Report config changes back to `.../configuration/update` as `{"state":{"reported":{...}}}` or the delta refires forever.
- Generic-entity create requires `_templateId`, `_name`, `_ownerOrganization:{id}` (org-scoped) — V1 API on Demo2. Works with a DEVICE-type token (verified live).
- File upload: POST `/file/v1/files/upload` `{name, mimeType}` → `{id, signedUrl}` → PUT bytes to signedUrl → attach `{id}` to the entity's FILE attribute (verified live with a device token).
- Don't call `wait_for_publish()` inside paho callbacks (deadlocks the network loop).
- Cross-platform: use `shutil.disk_usage` not `os.statvfs` (Unix-only).
- One clientId per running agent — two agents (e.g. Mac + VM) with `dev_DIM-4521` fight over the MQTT connection. Retire one before starting the other.

## Hardening backlog for R&D (deliberate demo shortcuts)

1. OTA is simulated (`run_ota` sleeps): implement real package download via the File API, signature verification against a Hologic-pinned key, A/B install with rollback (Hologic Q7/Q32).
2. Log capture is generated content: collect real Hologic-defined paths/artifacts per product line (their Q11/Q14 model: Hologic defines the manifest, agent executes).
3. Windows service packaging + MSI installer with pre-install network validation (their Q2/Q6). Currently a console script.
4. Config: only `hgm_targetSwVersion/Name` + `hgm_logLevel` handled; generalize to a config-schema-driven handler.
5. Token handling: fresh token per batch (per BioT docs); add retry/backoff and 403-expiry handling.
6. Queue: add size cap + overflow alert (their Q31 "no silent loss" clause), currently unbounded.
7. Status interval, backoff, and queue limits should come from remote configuration, not config.json.
8. Certificate rotation + TPM-backed key storage where available (their Q33).

## Demo environment

BioT Demo2 (dev): API `https://api.dev.demo2.biot-med.com`, org "MGH - Mass
General Hospital". Data model, template IDs, and deployment notes:
`Hologic_Demo_Implementation_Instructions.md` in the project Drive folder
(CRM/Hologic/Presale Planning/POC/Demo).
