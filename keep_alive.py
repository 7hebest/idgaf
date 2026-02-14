from flask import Flask, Response
from threading import Thread
import time
import os
import json
import logging
import redis

log = logging.getLogger("neveroff.keep_alive")

if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(h)

log.setLevel(logging.INFO)

app = Flask("neveroff-health")

REDIS_URL = os.getenv("REDIS_URL")
STATE_KEY = "neveroff:state"
HEALTH_ACTIVITY_TIMEOUT = int(os.getenv("HEALTH_ACTIVITY_TIMEOUT", "180"))

REDIS_AVAILABLE = False
r = None

if REDIS_URL:
    try:
        r = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_timeout=3,
            socket_connect_timeout=3,
            retry_on_timeout=True,
            ssl_cert_reqs=None
        )
        r.ping()
        REDIS_AVAILABLE = True
        log.info("Health: Redis connected.")
    except Exception as e:
        log.warning("Health: Redis unavailable: %s", e)
        REDIS_AVAILABLE = False
else:
    log.warning("Health: REDIS_URL not set.")

def _read_state():
    if not REDIS_AVAILABLE:
        return {}
    try:
        raw = r.get(STATE_KEY)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return {}

@app.route("/")
def index():
    return "neveroff alive", 200

@app.route("/health")
def health():
    state = _read_state()
    last_activity = state.get("last_activity_timestamp", 0)

    try:
        last_activity = float(last_activity)
    except:
        last_activity = 0

    if last_activity <= 0:
        return Response("STARTING", status=200)

    age = time.time() - last_activity

    if age > HEALTH_ACTIVITY_TIMEOUT:
        return Response("UNHEALTHY", status=500)

    return Response("OK", status=200)

def run(port: int):
    app.run(host="0.0.0.0", port=int(port), debug=False, use_reloader=False)

def keep_alive(port: int = 8080):
    thread = Thread(target=run, args=(int(port),), daemon=True)
    thread.start()
    log.info("keep_alive started on port %d", int(port))
