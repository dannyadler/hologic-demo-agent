#!/usr/bin/env python3
"""HG demo device agent — BioT connectivity agent for the Hologic demo.

Simulates a Hologic Dimensions mammography workstation (DIM-4521) connected
to BioT Demo2 over MQTT/mTLS, following the same flow as a real device:

  - persistent outbound-only MQTT connection (TLS 1.2+, per-device X.509 cert)
  - STATUS telemetry to <clientId>/from-device/status
  - offline store-and-forward queue (SQLite), chronological replay on reconnect
  - remote configuration via the BioT `configuration` named shadow:
    OTA target version (hgm_targetSwVersion/hgm_targetSwName) and log level
  - device REST API access via the MQTT token flow (<clientId>/from-device/token)
  - error events: creates an hg_device_event entity, captures + gzips device
    logs, uploads them via the File API, and registers an hg_log_bundle
  - config backup capture + shadow-triggered restore (hg_config_backup)
  - persistent device state (installed SW version, exam counter) across restarts

Run:  python3 agent.py            (config.json in the same folder)
Keys: e=error event (+ log bundle), x=exam, b=config backup, q=quit
"""
import gzip
import json
import os
import shutil
import socket
import sqlite3
import ssl
import sys
import threading
import time
import random
import urllib.request
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

# When frozen by PyInstaller, __file__ points into the temp extraction dir, so
# config.json and certs/ must be found next to the .exe instead. Keeping them
# external (not bundled) is also correct: certs are per-device and secret.
if getattr(sys, "frozen", False):
    HERE = os.path.dirname(sys.executable)
else:
    HERE = os.path.dirname(os.path.abspath(__file__))
CFG = json.load(open(os.path.join(HERE, "config.json")))

AGENT_VERSION = "1.6.0"
DEFAULT_SW_VERSION = "AWS-1.11.2"  # factory-installed device software version
DEBUG_SHADOW = CFG.get("debugShadow", False)

# HTTPS REST verification: freshly installed Windows Python has no CA bundle, so
# urllib can't verify the public cert of the API endpoint. Prefer certifi's
# bundle; fall back to the platform default (macOS ships one).
try:
    import certifi
    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except Exception:
    _SSL_CTX = ssl.create_default_context()


# ---------------------------------------------------------------- store ----
class Store:
    """SQLite: offline queue + persistent key/value device state."""

    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS q (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " topic TEXT, payload TEXT, ts INTEGER)"
        )
        self.db.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
        self.db.commit()
        self.lock = threading.Lock()

    def put(self, topic, payload, ts):
        with self.lock:
            self.db.execute("INSERT INTO q (topic, payload, ts) VALUES (?,?,?)", (topic, payload, ts))
            self.db.commit()

    def depth(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM q").fetchone()[0]

    def drain(self, publish_fn):
        while True:
            with self.lock:
                row = self.db.execute("SELECT id, topic, payload FROM q ORDER BY id LIMIT 1").fetchone()
            if row is None:
                return 0
            mid, topic, payload = row
            if not publish_fn(topic, payload):
                return self.depth()
            with self.lock:
                self.db.execute("DELETE FROM q WHERE id=?", (mid,))
                self.db.commit()

    def get(self, k, default=None):
        with self.lock:
            row = self.db.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
        return row[0] if row else default

    def set(self, k, v):
        with self.lock:
            self.db.execute("INSERT INTO kv (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))
            self.db.commit()


OfflineQueue = Store  # test_queue.py compatibility


# ---------------------------------------------------------------- agent ----
class Agent:
    def __init__(self):
        self.client_id = CFG["connectionClientId"]
        self.device_id = CFG["deviceId"]
        self.api = CFG.get("apiBase", "https://api.dev.demo2.biot-med.com")
        self.org_id = CFG["ownerOrganizationId"]
        self.tpl = CFG["templates"]  # {deviceEvent, logBundle, configBackup}

        self.status_topic = f"{self.client_id}/from-device/status"
        shadow = f"$aws/things/{self.client_id}/shadow/name/configuration"
        self.cfg_delta_topic = f"{shadow}/update/delta"
        self.cfg_get_accepted = f"{shadow}/get/accepted"
        self.cfg_get_topic = f"{shadow}/get"
        self.cfg_update_topic = f"{shadow}/update"
        self.token_sub_topic = f"{self.client_id}/to-device/token"
        self.token_pub_topic = f"{self.client_id}/from-device/token"

        self.store = Store(os.path.join(HERE, "offline_queue.db"))
        self.queue = self.store
        self.connected = False
        self.sw_version = self.store.get("sw_version", DEFAULT_SW_VERSION)
        self.exam_count = int(self.store.get("exam_count", 128))
        self.log_level = self.store.get("log_level", "info")
        self.last_error = ""
        self.updating = False
        self.stop = False
        self.require_manual_ota = False   # GUI sets True: operator must click Install (Q7)
        self.pending_ota = None           # (version_name, reported) awaiting operator approval
        self.prev_sw_version = self.store.get("prev_sw_version", "")  # last known-good (Q32)
        self.failed_versions = set()      # versions that failed post-install validation (Q32)
        self.last_update_status = self.store.get("last_update_status", "")
        self.restore_attr = CFG.get("restoreAttr", "hgm_restoreBackupId")  # Q14 restore trigger
        self.last_restore_id = self.store.get("last_restore_id", "")
        self.restoring = False
        self._token = None
        self._token_evt = threading.Event()

        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id, protocol=mqtt.MQTTv311)
        c.tls_set(
            ca_certs=os.path.join(HERE, "certs", "ca.pem"),
            certfile=os.path.join(HERE, "certs", "certificate.pem"),
            keyfile=os.path.join(HERE, "certs", "private_key.pem"),
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        c.reconnect_delay_set(min_delay=CFG.get("reconnectMinSec", 1), max_delay=CFG.get("reconnectMaxSec", 30))
        c.on_connect = self.on_connect
        c.on_disconnect = self.on_disconnect
        c.on_message = self.on_message
        self.mqtt = c

    # -- callbacks
    def on_connect(self, client, userdata, flags, rc, props=None):
        self.connected = True
        log(f"MQTT connected as {self.client_id}")
        client.subscribe([(self.cfg_delta_topic, 1), (self.cfg_get_accepted, 1), (self.token_sub_topic, 1)])
        client.publish(self.cfg_get_topic, "{}", qos=1)  # fetch config missed while offline
        threading.Thread(target=self.drain_queue, daemon=True).start()

    def on_disconnect(self, client, userdata, flags, rc, props=None):
        self.connected = False
        log(f"MQTT disconnected (rc={rc}) — queuing locally, auto-reconnect with backoff")

    def on_message(self, client, userdata, msg):
        raw = msg.payload.decode(errors="replace")
        if DEBUG_SHADOW:
            log(f"SHADOW MSG on {msg.topic}: {raw[:400]}")
        try:
            body = json.loads(raw or "{}")
        except Exception:
            return
        if msg.topic == self.token_sub_topic:
            self._token = (body.get("data") or {}).get("accessJwt", {}).get("token")
            if self._token:
                self._token_evt.set()
            return
        if msg.topic == self.cfg_delta_topic:
            state = body.get("state") or {}
        elif msg.topic == self.cfg_get_accepted:
            state = (body.get("state") or {}).get("delta") or {}
        else:
            return
        if state:
            threading.Thread(target=self.apply_config, args=(state,), daemon=True).start()

    # -- device REST API access (docs: device-api-access)
    def get_api_token(self, timeout=10):
        """Fresh JWT per batch of API calls, per BioT recommendation."""
        self._token_evt.clear()
        self._token = None
        self.mqtt.publish(self.token_pub_topic, "", qos=1)
        if not self._token_evt.wait(timeout):
            raise RuntimeError("device API token not received within timeout")
        return self._token

    def api_request(self, method, path, token, body=None, data=None, content_type="application/json"):
        url = path if path.startswith("http") else f"{self.api}{path}"
        payload = data if data is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(url, data=payload, method=method)
        if not path.startswith("http"):
            req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", content_type)
        with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as res:
            text = res.read().decode()
            return json.loads(text) if text else {}

    # -- error event + log bundle (W4)
    def handle_error_event(self):
        code = f"E-{random.randint(1000,9999)}"
        self.last_error = code
        log(f"error event raised: {code}")
        self.send_status()
        threading.Thread(target=self._report_event_and_logs, args=(code,), daemon=True).start()

    def _report_event_and_logs(self, code):
        try:
            log("EVENT: requesting device API token over MQTT...")
            token = self.get_api_token()
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            # 1. device event entity
            self.api_request("POST", "/generic-entity/v1/generic-entities", token, body={
                "_templateId": self.tpl["deviceEvent"],
                "_name": f"evt-{self.device_id}-{int(time.time())}",
                "_ownerOrganization": {"id": self.org_id},
                "hg_eventSeverity": "error",
                "hg_errorCode": code,
                "hg_eventTime": now,
                "hg_eventDetails": f"Device-reported error {code}: detector subsystem fault during exam preparation.",
                "hg_eventDevice": {"id": self.device_id},
            })
            log(f"EVENT: hg_device_event created ({code})")
            # 2. capture + compress logs (simulated content, real gzip)
            log("EVENT: capturing device logs (event-triggered retrieval)...")
            log_bytes = gzip.compress(fake_logs(self.device_id, code, self.sw_version).encode())
            fname = f"{self.device_id}-logs-{int(time.time())}.log.gz"
            # 3. create file + upload to signed URL
            f = self.api_request("POST", "/file/v1/files/upload", token,
                                 body={"name": fname, "mimeType": "application/gzip"})
            self.api_request("PUT", f["signedUrl"], token, data=log_bytes, content_type="application/gzip")
            log(f"EVENT: log bundle uploaded ({fname}, {len(log_bytes)} bytes compressed)")
            # 4. register the log bundle entity, attached to this device
            self.api_request("POST", "/generic-entity/v1/generic-entities", token, body={
                "_templateId": self.tpl["logBundle"],
                "_name": fname,
                "_ownerOrganization": {"id": self.org_id},
                "hg_capturedAt": now,
                "hg_triggerType": "event",
                "hg_bundleStatus": "complete",
                "hg_logFile": {"id": f["id"]},
                "hg_bundleDevice": {"id": self.device_id},
            })
            log("EVENT: hg_log_bundle registered — visible in the portal Log Bundles tab")
        except Exception as e:
            log(f"EVENT: FAILED — {e}")

    # -- configuration backup + restore (Q14)
    def _collect_config(self):
        """Snapshot the device's Hologic-defined configuration set.
        ponytail: a real agent reads a Hologic-defined manifest of files,
        registry keys, and calibration data; here we snapshot the agent state
        plus a representative device config block."""
        return {
            "deviceId": self.device_id,
            "capturedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "agentVersion": AGENT_VERSION,
            "installedSwVersion": self.sw_version,
            "logLevel": self.log_level,
            "examCountTotal": self.exam_count,
            "deviceConfig": {
                "acquisition": {"kv": 28, "mas": 80, "gridMode": "auto", "detectorGain": 1.02},
                "network": {"dhcp": True, "ntp": "pool.hologic.local"},
                "dicom": {"aeTitle": f"HG_{self.device_id.replace('-', '')}", "port": 104},
                "calibration": {"lastFlatField": "2026-07-20", "detectorTempSetpointC": 31.5},
            },
        }

    def capture_config_backup(self, trigger="manual"):
        threading.Thread(target=self._do_config_backup, args=(trigger,), daemon=True).start()

    def _do_config_backup(self, trigger):
        try:
            if not self.tpl.get("configBackup"):
                log("BACKUP: no configBackup template configured — skipping")
                return
            log(f"BACKUP: capturing device configuration ({trigger})...")
            token = self.get_api_token()
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            blob = gzip.compress(json.dumps(self._collect_config(), indent=2).encode())
            fname = f"{self.device_id}-config-{int(time.time())}.json.gz"
            version = f"cfg-{self.sw_version}-{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}"
            f = self.api_request("POST", "/file/v1/files/upload", token,
                                 body={"name": fname, "mimeType": "application/gzip"})
            self.api_request("PUT", f["signedUrl"], token, data=blob, content_type="application/gzip")
            log(f"BACKUP: snapshot uploaded ({fname}, {len(blob)} bytes)")
            self.api_request("POST", "/generic-entity/v1/generic-entities", token, body={
                "_templateId": self.tpl["configBackup"],
                "_name": fname,
                "_ownerOrganization": {"id": self.org_id},
                "hg_cfgCapturedAt": now,
                "hg_cfgTrigger": trigger,
                "hg_cfgStatus": "complete",
                "hg_cfgVersion": version,
                "hg_cfgFile": {"id": f["id"]},
                "hg_cfgDevice": {"id": self.device_id},
            })
            log(f"BACKUP: hg_config_backup registered ({version}) — visible in the portal Backups tab")
        except Exception as e:
            log(f"BACKUP: FAILED — {e}")

    def _do_restore(self, backup_id):
        self.restoring = True
        try:
            log(f"RESTORE: fetching config backup {backup_id}...")
            token = self.get_api_token()
            ent = self.api_request("GET", f"/generic-entity/v1/generic-entities/{backup_id}", token)
            fileref = ent.get("hg_cfgFile") or {}
            file_id = fileref.get("id") if isinstance(fileref, dict) else fileref
            if not file_id:
                raise RuntimeError("backup has no config file attached")
            dl = self.api_request("GET", f"/file/v1/files/{file_id}/download", token)
            with urllib.request.urlopen(dl["signedUrl"], timeout=30, context=_SSL_CTX) as r:
                cfg = json.loads(gzip.decompress(r.read()))
            sections = list((cfg.get("deviceConfig") or {}).keys())
            log(f"RESTORE: applying snapshot from {cfg.get('capturedAt')} — sections: {', '.join(sections)}")
            if cfg.get("logLevel"):
                self.log_level = cfg["logLevel"]
                self.store.set("log_level", self.log_level)
            self.last_restore_id = backup_id
            self.store.set("last_restore_id", backup_id)
            log("RESTORE: complete — device configuration restored to the selected snapshot")
        except Exception as e:
            log(f"RESTORE: FAILED — {e}")
        finally:
            self.restoring = False
            self.report_config({self.restore_attr: backup_id})  # clear the shadow delta
            self.send_status()

    # -- remote configuration
    def apply_config(self, state):
        if "hgm_logLevel" in state and state["hgm_logLevel"]:
            self.log_level = state["hgm_logLevel"]
            self.store.set("log_level", self.log_level)
            log(f"CONFIG: log level -> {self.log_level}")
            self.report_config({"hgm_logLevel": self.log_level})
        rid = state.get(self.restore_attr)
        if rid and rid != self.last_restore_id and not self.restoring:
            log(f"RESTORE: restore request received for backup {rid}")
            threading.Thread(target=self._do_restore, args=(rid,), daemon=True).start()
        target = state.get("hgm_targetSwVersion")
        name = state.get("hgm_targetSwName")
        if not name and isinstance(target, dict):
            name = target.get("name")
        if target or name:
            reported = {}
            if target:
                reported["hgm_targetSwVersion"] = target
            if name:
                reported["hgm_targetSwName"] = name
            if name and name in self.failed_versions:
                # A version that failed post-install validation is not retried; the
                # device stays on the last known-good until a new target is approved.
                log(f"OTA: {name} previously failed validation — not reinstalling; awaiting a new approved target")
                self.report_config(reported)
                return
            if name and name != self.sw_version and not self.updating:
                if self.require_manual_ota:
                    self.pending_ota = (name, reported)
                    log(f"OTA: approved update {name} available — awaiting operator install")
                else:
                    self.run_ota(name, reported)
            elif name and name == self.sw_version:
                log(f"CONFIG: target SW {name} already installed")
                self.report_config(reported)
            elif not name:
                log("CONFIG: target SW reference received without a version name — waiting for hgm_targetSwName")
                self.report_config(reported)

    def _apply_package(self, version_name, label="update"):
        """Simulated package apply: download, verify, install.
        ponytail: sleep-based simulation; a real device streams the package
        via the BioT file API and verifies its signature."""
        for step, secs in [(f"{label}: downloading package", 3),
                           ("verifying signature and integrity", 2),
                           ("installing (no reboot required)", 3)]:
            log(f"OTA: {step}...")
            time.sleep(secs)
        self.sw_version = version_name
        self.store.set("sw_version", version_name)

    def _validate_install(self, version_name):
        """Post-install health check (Q6 post-install / Q32 auto-rollback trigger).
        ponytail: a real device runs a self-test suite; here a package whose
        version name ends in '-bad' (or is listed in config knownBadVersions)
        deterministically fails, so the automatic-rollback path is demoable."""
        log("OTA: running post-install validation...")
        time.sleep(1)
        bad = version_name.lower().endswith("-bad") or version_name in CFG.get("knownBadVersions", [])
        return not bad

    def run_ota(self, version_name, reported):
        """Install an approved package, validate it, and auto-roll-back on failure.
        Preserves the prior version as last known-good (Q32)."""
        self.updating = True
        old = self.sw_version
        log(f"OTA: update available -> {version_name} (Hologic-approved package)")
        self._apply_package(version_name)
        if self._validate_install(version_name):
            self.prev_sw_version = old
            self.store.set("prev_sw_version", old)
            self.last_update_status = "ok"
            self.store.set("last_update_status", "ok")
            self.last_error = ""
            self.updating = False
            log(f"OTA: SUCCESS — {old} -> {version_name} (post-install validation passed)")
            self.report_config(reported)
            self.send_status()
        else:
            # Automated rollback to the last known-good state (Q32 sub-clause 3).
            log(f"OTA: post-install validation FAILED for {version_name} — starting automatic rollback")
            self.failed_versions.add(version_name)
            self._apply_package(old, label="rollback to last known-good")
            self.last_update_status = "rolled_back"
            self.store.set("last_update_status", "rolled_back")
            self.last_error = f"UPDATE_ROLLBACK {version_name}->{old}"
            self.updating = False
            log(f"OTA: ROLLED BACK to last known-good {old} — device operational, update rejected")
            # Report the processed target so the shadow delta clears; the device
            # will not retry this version (see failed_versions guard).
            self.report_config(reported)
            self.send_status()

    def approve_pending_ota(self):
        """Operator clicked Install in the GUI (Q7: end user approves before install)."""
        if not self.pending_ota or self.updating:
            return
        name, reported = self.pending_ota
        self.pending_ota = None
        threading.Thread(target=self.run_ota, args=(name, reported), daemon=True).start()

    def perform_exam(self):
        self.exam_count += 1
        self.store.set("exam_count", self.exam_count)
        log(f"exam performed, total={self.exam_count}")
        self.send_status()

    def report_config(self, reported):
        self.mqtt.publish(self.cfg_update_topic, json.dumps({"state": {"reported": reported}}), qos=1)

    # -- publishing
    def publish_raw(self, topic, payload):
        if not self.connected:
            return False
        info = self.mqtt.publish(topic, payload, qos=1)
        try:
            info.wait_for_publish(timeout=5)
        except Exception:
            info = None
        ok = bool(info) and info.is_published()
        if not ok:
            self.connected = False
            log("publish timed out — treating link as offline")
        return ok

    def drain_queue(self):
        if self.store.depth() == 0:
            return
        n = self.store.drain(self.publish_raw)
        if n == 0:
            log("offline queue drained (chronological order)")
        else:
            log(f"queue drain interrupted, {n} left")

    def send_status(self):
        if self.connected and self.store.depth() > 0:
            self.drain_queue()
        data = {
            "hgm_agentVersion": AGENT_VERSION,
            "hgm_swVersion": self.sw_version,
            "hgm_examCountTotal": self.exam_count,
            "hgm_diskFreeGb": round(disk_free_gb(), 1),
            "hgm_detectorTempC": round(random.gauss(31.5, 0.4), 2),
            "hgm_queueDepth": self.store.depth(),
        }
        if self.last_error:
            data["hgm_lastErrorCode"] = self.last_error
        ts = int(time.time() * 1000)
        payload = json.dumps({"metadata": {"timestamp": ts}, "data": data})
        if self.publish_raw(self.status_topic, payload):
            log(f"status sent (sw={self.sw_version}, exams={self.exam_count}, queue={data['hgm_queueDepth']})")
        else:
            self.store.put(self.status_topic, payload, ts)
            log(f"OFFLINE — status queued (depth={self.store.depth()})")

    # -- demo triggers (stdin)
    def stdin_loop(self):
        if not sys.stdin:  # windowed exe has no console/stdin
            return
        log("keys: e=error event (+ log bundle), x=exam performed, b=config backup, q=quit")
        for line in sys.stdin:
            k = line.strip().lower()
            if k == "e":
                self.handle_error_event()
            elif k == "x":
                self.perform_exam()
            elif k == "b":
                self.capture_config_backup()
            elif k == "q":
                self.stop = True
                return

    def run(self):
        log(f"HG demo agent v{AGENT_VERSION} — device {self.device_id} (installed SW {self.sw_version})")
        threading.Thread(target=self.stdin_loop, daemon=True).start()
        self.mqtt.connect_async(CFG["iotEndpoint"], 8883, keepalive=60)
        self.mqtt.loop_start()
        interval = CFG.get("statusIntervalSec", 10)
        while not self.stop:
            self.send_status()
            for _ in range(interval * 10):
                if self.stop:
                    break
                time.sleep(0.1)
        self.mqtt.loop_stop()
        self.mqtt.disconnect()
        log("agent stopped")


def fake_logs(device_id, code, sw):
    """Simulated device log content for the demo bundle."""
    lines = [f"# {device_id} diagnostic log capture — sw {sw}"]
    t0 = time.time() - 600
    for i in range(200):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t0 + i * 3))
        lvl = random.choices(["INFO", "DEBUG", "WARN"], [6, 3, 1])[0]
        lines.append(f"{ts} {lvl} acq.detector temp={round(random.gauss(31.5,0.4),2)}C gain=ok frame={1000+i}")
    lines.append(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} ERROR {code} detector subsystem fault during exam preparation")
    return "\n".join(lines)


def disk_free_gb():
    return shutil.disk_usage(HERE).free / 2**30  # cross-platform (statvfs is Unix-only)


# ---------------------------------------------------- pre-install checks ----
# Real network pre-flight for the onboarding wizard and the standalone
# validator (`python agent.py --preflight`). Covers Hologic Q6: endpoint
# reachability, DNS, port, and TLS-interception detection before install.
def _host_of(url):
    u = urlparse(url if "://" in url else "https://" + url)
    return u.hostname


def _tcp_check(name, host, port):
    try:
        with socket.create_connection((host, port), timeout=8):
            return {"name": name, "ok": True, "detail": f"{host}:{port} reachable"}
    except Exception as e:
        return {"name": name, "ok": False, "detail": f"{host}:{port} unreachable — {e}"}


def _tls_check(name, host, port=443):
    # A verified handshake failing on an otherwise-reachable host is the classic
    # signal of a corporate TLS-inspection proxy re-signing with a private CA.
    try:
        with socket.create_connection((host, port), timeout=8) as sock:
            with _SSL_CTX.wrap_socket(sock, server_hostname=host) as s:
                issuer = dict(x[0] for x in s.getpeercert().get("issuer", ())).get(
                    "organizationName", "unknown")
                return {"name": name, "ok": True, "detail": f"TLS verified, issuer: {issuer}"}
    except ssl.SSLCertVerificationError:
        return {"name": name, "ok": False,
                "detail": "certificate not trusted — likely TLS interception/proxy; allowlist the endpoint"}
    except Exception as e:
        return {"name": name, "ok": False, "detail": str(e)}


def _dns_check(name, host):
    try:
        return {"name": name, "ok": True, "detail": f"{host} -> {socket.gethostbyname(host)}"}
    except Exception as e:
        return {"name": name, "ok": False, "detail": f"{host}: {e}"}


def preflight_checks(cfg=CFG):
    """Return [{name, ok, detail}] for the device's connectivity prerequisites."""
    iot = cfg["iotEndpoint"]
    api_host = _host_of(cfg.get("apiBase", ""))
    return [
        _dns_check("IoT endpoint DNS resolves", iot),
        _dns_check("API endpoint DNS resolves", api_host),
        _tcp_check("MQTT/TLS port open (8883)", iot, 8883),
        _tls_check("API TLS handshake (443)", api_host),
    ]


LOG_SINKS = []  # optional callables(str): UIs subscribe here; console print always happens


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    try:
        print(line, flush=True)  # windowed exe may have no stdout
    except Exception:
        pass
    for sink in list(LOG_SINKS):
        try:
            sink(line)
        except Exception:
            pass


def _run_preflight_cli():
    all_ok = True
    print("Pre-install network validation:")
    for r in preflight_checks():
        print(f"  [{'PASS' if r['ok'] else 'FAIL'}] {r['name']} — {r['detail']}")
        all_ok &= r["ok"]
    return all_ok


if __name__ == "__main__":
    if "--preflight" in sys.argv:
        sys.exit(0 if _run_preflight_cli() else 1)
    if "--silent" in sys.argv:
        # Unattended enrollment for fleet rollout (SCCM/Intune/GPO): validate the
        # network, mark enrolled, then run headless. Non-zero exit on failure.
        if not _run_preflight_cli():
            print("Silent enrollment aborted: network prerequisites not met.")
            sys.exit(1)
        a = Agent()
        a.store.set("enrolled", "1")
        print(f"Silent enrollment OK — {a.device_id} running headless.")
        a.run()
        sys.exit(0)
    Agent().run()
