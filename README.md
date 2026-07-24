# HG Demo Agent — BioT connectivity agent (Hologic presale demo)

Python device agent simulating a Hologic Dimensions mammography workstation
(`DIM-4521`) connected to BioT Demo2. Built for the Hologic NextGen Connectivity
presale demo (meeting week of 2026-08-03). Companion portal:
[hologic-nexgen-connectivity](https://github.com/dannyadler/hologic-nexgen-connectivity)
(Lovable, BioT REST).

## What it implements (all live against BioT 2.4.1)

| Capability | Hologic questionnaire | Mechanism |
|---|---|---|
| Simplified install & onboarding (non-IT operator) | Q2 | `agent_gui.py` first-run Activation wizard: site/serial/enrollment code -> Activate, credentials staged, connects and registers |
| Pre-install network validation | Q6 | `preflight_checks()` (in wizard + `agent.py --preflight`): DNS, port 8883, API TLS handshake / interception detection |
| Persistent outbound-only connectivity + reachability | Q1 | MQTT/mTLS (per-device X.509), clientId `dev_DIM-4521`, `_status._connection` on platform |
| Status telemetry | Q17/Q18 | `<clientId>/from-device/status`, `{metadata:{timestamp_ms}, data:{STATUS attrs}}` every 10s |
| Offline store-and-forward, chronological replay | Q31 | SQLite queue, drains on reconnect, no silent loss, queue depth reported |
| OTA with Hologic approval gate | Q7/Q10 | `configuration` named shadow delta → simulated install → reported state + status |
| Rollback + automatic disaster recovery | Q32 | tracks last known-good; post-install validation; a package failing validation auto-rolls-back and is not retried. Remote rollback = deploy a prior approved version |
| Device REST API access | — | MQTT token flow: publish empty to `<clientId>/from-device/token`, JWT arrives on `to-device/token` |
| Error events + event-triggered log retrieval | Q11 | `e` key → hg_device_event entity + gzipped log capture → File API upload → hg_log_bundle entity |
| Persistent device state | — | SW version + exam counter survive restarts (SQLite kv) |

## Run

```sh
python3 -m venv .venv && source .venv/bin/activate
pip install paho-mqtt certifi
python3 agent_gui.py            # GUI: first launch shows the Activation wizard (Q2)
python3 agent_gui.py --reenroll # replay the wizard for a demo
python3 agent.py                # headless console (keys: x exam, e error, q quit)
python3 agent.py --preflight    # standalone pre-install network validator (Q6)
python3 agent.py --silent       # unattended enrollment: validate + run headless (Q2)
```

The GUI first-run **Activation wizard** (Q2, no IT specialist): operator enters
site, serial, enrollment code -> Activate. It runs real pre-flight checks (DNS,
port 8883, API TLS/interception -> Q6), verifies credentials, connects over
MQTT/mTLS, and confirms the device is registered in the fleet, then opens the
console. Enrollment is remembered in the SQLite kv (`enrolled`); `--reenroll`
clears it to re-run the wizard.

**Rollback demo (Q32):** from an installed baseline, deploy a good update (e.g.
`AWS-1.13.0`) so the prior version becomes last known-good; then deploy a version
named with a `-bad` suffix (e.g. `AWS-1.14.0-bad`) or listed in config
`knownBadVersions`. Post-install validation fails and the agent automatically
rolls back to the last known-good and refuses to retry the bad package. Remote
rollback is just deploying a prior approved version from the portal.

Self-checks: `python3 test_queue.py` (offline queue), `python3 test_rollback.py`
(validation + rollback). Set `"debugShadow": true` in config.json to log raw
shadow traffic.

## Package as a double-click exe (Windows, no Python on the target)

Run once on a Windows device:

```
build.bat
```

Produces `dist\HGDeviceConsole.exe` (operator GUI: onboarding wizard + console)
and `dist\HGDeviceAgent.exe` (headless; `--preflight`, `--silent`). Ship the exe
next to `config.json` and the `certs\` folder — when frozen the agent looks for
both beside the executable, not inside the bundle. Unattended fleet rollout:
`HGDeviceAgent.exe --silent` validates the network, marks enrolled, and runs
headless (SCCM/Intune/GPO friendly).

## Files

- `agent.py` — agent core (single file by design for the demo)
- `agent_gui.py` — tkinter GUI: onboarding wizard + device console
- `build.bat` — PyInstaller build of the two Windows executables
- `config.json` — device identity, endpoints, org and template IDs (no secrets). Optional `knownBadVersions` list forces post-install validation to fail (Q32 rollback demo).
- `certs/` — NOT in git. Per-device X.509 cert from BioT (portal: device → generate certificate). Place `certificate.pem`, `private_key.pem`, `ca.pem` here.
- `test_queue.py` — offline-queue self-check
- `test_rollback.py` — post-install validation + rollback self-check (Q32)

## Platform contracts (live-verified, save your R&D the digging)

- Status attrs come back NESTED under `_status` on GET /device/v2/devices/{id}; config attrs under `_configuration`.
- Device create API needs `_id` + `_templateId` (flat); wrong body shape returns a misleading 403 ACCESS_DENIED.
- Config shadow (`$aws/things/<clientId>/shadow/name/configuration`) delivers reference attributes as `{id}` ONLY — no display name. The portal therefore also writes `hgm_targetSwName` (plain Label) so the device knows what to install.
- Report config changes back to `.../configuration/update` as `{"state":{"reported":{...}}}` or the delta refires forever.
- Generic-entity create requires `_templateId`, `_name`, `_ownerOrganization:{id}` (org-scoped) — V1 API on Demo2.
- File upload: POST `/file/v1/files/upload` `{name, mimeType}` → `{id, signedUrl}` → PUT bytes to signedUrl → attach `{id}` to the entity's FILE attribute.
- Don't call `wait_for_publish()` inside paho callbacks (deadlocks the network loop).

## Hardening backlog for R&D (deliberate demo shortcuts)

1. OTA + rollback are simulated (`_apply_package` sleeps; `_validate_install` fails on a `-bad` version name or config `knownBadVersions`). Implement real package download via the File API, signature verification against a Hologic-pinned key, and a true A/B partition install so the rollback restores an image rather than re-fetching (Hologic Q7/Q32).
2. Log capture is generated content: collect real Hologic-defined paths/artifacts per product line (their Q11/Q14 model: Hologic defines the manifest, agent executes).
3. Onboarding wizard + pre-flight validation are built (Q2/Q6); credential provisioning is pre-staged. Remaining: real fleet-provisioning cert issuance triggered by the enrollment code, code-signed exe/MSI, and Windows service registration. (PyInstaller build in `build.bat`.)
4. Config: only `hgm_targetSwVersion/Name` + `hgm_logLevel` handled; generalize to a config-schema-driven handler.
5. Token handling: fresh token per batch (per BioT docs); add retry/backoff and 403-expiry handling.
6. Queue: add size cap + overflow alert (their Q31 "no silent loss" clause), currently unbounded.
7. Status interval, backoff, and queue limits should come from remote configuration, not config.json.
8. Certificate rotation + TPM-backed key storage where available (their Q33).

## Demo environment

BioT Demo2 (dev): API `https://api.dev.demo2.biot-med.com`, org "MGH - Mass
General Hospital". Data model and template IDs:
`Hologic_Demo_Implementation_Instructions.md` in the project folder.
