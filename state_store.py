#!/usr/bin/env python3
import json
import logging
import threading
import os
import redis

log = logging.getLogger("neveroff.state_store")

REDIS_URL = os.getenv("REDIS_URL")
STATE_KEY = "neveroff:state"

REDIS_AVAILABLE = False
r = None

if REDIS_URL:
    try:
        r = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            ssl_cert_reqs=None  
        )
        r.ping()
        REDIS_AVAILABLE = True
        log.info("Redis connected successfully.")
    except Exception as e:
        log.warning("Redis unavailable at startup. Running without persistence: %s", e)
        REDIS_AVAILABLE = False
else:
    log.warning("REDIS_URL not set. Running without persistence.")

class StateStore:
    def __init__(self, path=None):
        self._lock = threading.Lock()
        self._data = {}
        self._load()

    def _load(self):
        if not REDIS_AVAILABLE:
            return
        try:
            raw = r.get(STATE_KEY)
            if raw:
                self._data = json.loads(raw)
        except Exception as e:
            log.warning("Redis load failed (non-fatal): %s", e)

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)

    def update(self, d: dict):
        if not isinstance(d, dict):
            return
        with self._lock:
            self._data.update(d)
            self._write()

    def _write(self):
        if not REDIS_AVAILABLE:
            return
        try:
            r.set(STATE_KEY, json.dumps(self._data))
        except Exception as e:
            log.warning("Redis save failed (non-fatal): %s", e)
