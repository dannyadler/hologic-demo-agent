#!/usr/bin/env python3
"""Offline self-check for the Q14 config-backup snapshot logic (no network).
Run: python3 test_backup.py
"""
import gzip
import json
from agent import Agent

a = Agent()
snap = a._collect_config()

assert snap["deviceId"] == a.device_id
assert "deviceConfig" in snap and snap["deviceConfig"], "snapshot must carry a device config block"
assert snap["installedSwVersion"] == a.sw_version

# gzip round-trip is byte-safe (same path the agent uploads/restores through)
blob = gzip.compress(json.dumps(snap, indent=2).encode())
restored = json.loads(gzip.decompress(blob))
assert restored == snap, "gzip round-trip must preserve the snapshot exactly"

print("config-backup self-check passed")
