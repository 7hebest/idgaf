#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import threading
import traceback
import logging
import random
import socket
from typing import Optional

import websocket

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from state_store import StateStore

OP_DISPATCH = 0
OP_HEARTBEAT = 1
OP_IDENTIFY = 2
OP_PRESENCE_UPDATE = 3
OP_RECONNECT = 7
OP_INVALID_SESSION = 9
OP_HELLO = 10
OP_HEARTBEAT_ACK = 11
ACTIVITY_TYPE_CUSTOM = 4

def _parse_bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _parse_int_env(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        v2 = os.getenv("PORT_HTTP")
        if v2:
            try:
                return int(v2)
            except Exception:
                return default
        return default
    try:
        return int(str(v).strip())
    except Exception:
        v2 = os.getenv("PORT_HTTP")
        try:
            if v2:
                return int(v2)
        except Exception:
            pass
        return default


def _parse_float_env(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _parse_str_env(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    if s == "": 
        return default
    return s

STATUS = _parse_str_env("status", "online")
CUSTOM_STATUS = _parse_str_env("custom_status", "")
EMOJI_NAME = _parse_str_env("emoji_name", "")
EMOJI_ID = _parse_str_env("emoji_id", None)
EMOJI_ANIMATED = _parse_bool_env("emoji_animated", False)

TOKEN = _parse_str_env("token", "")

GATEWAY_URL = _parse_str_env("gateway_url", "wss://gateway.discord.gg/?v=10&encoding=json")

PERSIST_PATH = "/tmp/neveroff_state.json"

HEARTBEAT_TIMEOUT_MULTIPLIER = _parse_float_env("HEARTBEAT_TIMEOUT_MULTIPLIER", 0.0)

RECONNECT_BASE_BACKOFF = _parse_float_env("RECONNECT_BASE_BACKOFF", 1.0)
RECONNECT_MAX_BACKOFF = int(_parse_float_env("RECONNECT_MAX_BACKOFF", 60.0))
RECONNECT_JITTER = _parse_bool_env("RECONNECT_JITTER", True)
RECV_TIMEOUT = _parse_float_env("RECV_TIMEOUT", 15.0)
SEND_TIMEOUT = _parse_float_env("SEND_TIMEOUT", 5.0)

HEALTH_ACTIVITY_TIMEOUT = int(_parse_int_env("HEALTH_ACTIVITY_TIMEOUT", 180))   
HEALTH_WATCH_INTERVAL = int(_parse_int_env("HEALTH_WATCH_INTERVAL", 10))  

PRESENCE_REFRESH_INTERVAL = int(_parse_int_env("PRESENCE_REFRESH_INTERVAL", 3600))  

RESUME_WAIT_TIMEOUT = float(_parse_float_env("RESUME_WAIT_TIMEOUT", 10.0))

IDENTIFY_WINDOW_SECONDS = int(_parse_int_env("IDENTIFY_WINDOW_SECONDS", 24 * 3600))
IDENTIFY_WINDOW_LIMIT = int(_parse_int_env("IDENTIFY_WINDOW_LIMIT", 1000))

INTENTS = _parse_int_env("INTENTS", 0)

LOG_LEVEL_RAW = os.getenv("LOG_LEVEL", "INFO")
if LOG_LEVEL_RAW is None:
    LOG_LEVEL_RAW = "INFO"
LOG_LEVEL = str(LOG_LEVEL_RAW).strip().upper()
VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
if LOG_LEVEL not in VALID_LEVELS:
    LOG_LEVEL = "INFO"

PORT = _parse_int_env("PORT", 8080)

IDENTITY_OS = _parse_str_env("IDENTITY_OS", "Windows")
IDENTITY_BROWSER = _parse_str_env("IDENTITY_BROWSER", "Discord Desktop")
IDENTITY_DEVICE = _parse_str_env("IDENTITY_DEVICE", "Windows PC")

IDENTITY_PROPS = {
    "$os": IDENTITY_OS,
    "$browser": IDENTITY_BROWSER,
    "$device": IDENTITY_DEVICE,
}

if not TOKEN:
    print("[ERROR] Missing environment variable: token. Please add your Discord token.")
    sys.exit(1)

class MaskingFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%'):
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self._token = TOKEN or ""
        self._masked = None
        if self._token:
            prefix = self._token[:6]
            self._masked = prefix + "...[masked]"
        else:
            self._masked = "[no-token]"

    def format(self, record):
        try:
            s = super().format(record)
        except Exception:
            s = record.getMessage()
        if self._token:
            s = s.replace(self._token, self._masked)
        return s

logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

for h in list(logger.handlers):
    logger.removeHandler(h)

stream_h = logging.StreamHandler(sys.stdout)
stream_h.setFormatter(MaskingFormatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(stream_h)

log = logging.getLogger("neveroff")

log.propagate = True

state = StateStore(PERSIST_PATH)
session_id: Optional[str] = state.get("session_id")
sequence = state.get("sequence")

_identify_window_start = state.get("identify_window_start") or 0
_identify_count = state.get("identify_count") or 0

ws = None
ws_lock = threading.Lock()
should_stop = threading.Event()

last_ack_timestamp = state.get("last_ack_timestamp") or time.time()

last_activity_timestamp = state.get("last_activity_timestamp") or time.time()

USERNAME = state.get("username") or "unknown"
DISCRIMINATOR = state.get("discriminator") or "0000"
USERID = state.get("user_id") or "unknown"

_last_identify_time = state.get("_last_identify_time") or 0.0
_identify_failures = state.get("_identify_failures") or 0
_IDENTIFY_MIN_INTERVAL = float(_parse_float_env("IDENTIFY_MIN_INTERVAL", 5.0))  
_IDENTIFY_MAX_FAILURES = int(_parse_int_env("IDENTIFY_MAX_FAILURES", 5))

_ready_event = threading.Event()

ACTIVITY_ZOMBIE_TIMEOUT = int(_parse_int_env("ACTIVITY_ZOMBIE_TIMEOUT", 120))  
ACTIVITY_WATCH_INTERVAL = int(_parse_int_env("ACTIVITY_WATCH_INTERVAL", 5))

def safe_save_state():
    try:
        state.update({
            "session_id": session_id,
            "sequence": sequence,
            "last_ack_timestamp": last_ack_timestamp,
            "identify_window_start": _identify_window_start,
            "identify_count": _identify_count,
            "username": USERNAME,
            "discriminator": DISCRIMINATOR,
            "user_id": USERID,
            "_last_identify_time": _last_identify_time,
            "_identify_failures": _identify_failures,
            "last_activity_timestamp": last_activity_timestamp,
        })
    except Exception as e:
        log.warning("Failed to persist state: %s", e)


def build_presence_payload(
    status=STATUS,
    custom=CUSTOM_STATUS,
    emoji_name=EMOJI_NAME,
    emoji_id=EMOJI_ID,
    emoji_animated=EMOJI_ANIMATED,
):
    activity = {
        "type": ACTIVITY_TYPE_CUSTOM,
        "state": custom,
        "name": "Custom Status",
        "id": "custom",
    }
    if emoji_name:
        emoji_obj = {"name": emoji_name}
        if emoji_id:
            emoji_obj["id"] = emoji_id
            emoji_obj["animated"] = bool(emoji_animated)
        activity["emoji"] = emoji_obj
    payload = {
        "op": OP_PRESENCE_UPDATE,
        "d": {
            "since": 0,
            "activities": [activity],
            "status": status,
            "afk": False,
        },
    }
    return payload


def _record_activity():
    global last_activity_timestamp
    last_activity_timestamp = time.time()
    safe_save_state()


def send_json(sock, obj) -> bool:
    try:
        with ws_lock:
            sock.settimeout(SEND_TIMEOUT)
            sock.send(json.dumps(obj))
        return True
    except Exception as e:
        log.debug("send_json failed: %s", e)
        return False


def handle_dispatch(data):
    global sequence, session_id, last_ack_timestamp, USERNAME, DISCRIMINATOR, USERID
    if "s" in data and data["s"] is not None:
        sequence = data["s"]
    t = data.get("t")
    d = data.get("d")
    _record_activity()
    if t == "READY":
        session_id = d.get("session_id", session_id)
        user = d.get("user", {}) or {}
        USERNAME = user.get("username", USERNAME)
        DISCRIMINATOR = user.get("discriminator", DISCRIMINATOR)
        USERID = user.get("id", USERID)
        log.info(
            "READY as %s#%s (%s) | Session: %s",
            USERNAME,
            DISCRIMINATOR,
            USERID,
            (session_id[:8] + "...") if session_id else "none",
        )
        safe_save_state()
        _ready_event.set()
    elif t == "RESUMED":
        log.info("Session resumed successfully.")
        safe_save_state()
        _ready_event.set()
    else:
        log.debug("Dispatch event: %s", t)


def heartbeat_loop(sock, interval_ms, stop_event: threading.Event):
    """
    Heartbeat thread:
      - sends OP 1 regularly
      - optionally enforces a missed-ACK timeout if HEARTBEAT_TIMEOUT_MULTIPLIER > 0
    """
    global sequence, last_ack_timestamp
    interval = interval_ms / 1000.0
    while not stop_event.is_set():
        if stop_event.wait(interval):
            return
        try:
            now = time.time()

            if HEARTBEAT_TIMEOUT_MULTIPLIER > 0:
                multiplier = HEARTBEAT_TIMEOUT_MULTIPLIER
                if multiplier < 1.0:
                    multiplier = 2.0
                if (now - (last_ack_timestamp or 0.0)) > (interval * multiplier):
                    log.warning(
                        "Missed heartbeat ACK for %.1fs (>%.1fs). Closing socket to force reconnect.",
                        now - (last_ack_timestamp or 0.0),
                        interval * multiplier,
                    )
                    try:
                        sock.close()
                    finally:
                        return

            hb_payload = {"op": OP_HEARTBEAT, "d": sequence}
            if not send_json(sock, hb_payload):
                log.warning("Failed to send heartbeat; exiting heartbeat loop.")
                return
        except Exception:
            log.exception("Exception in heartbeat loop - exiting")
            return


def _consume_identify_window():
    """
    Ensure identify window state is correct. Reset if older than window.
    """
    global _identify_window_start, _identify_count
    now = int(time.time())
    if _identify_window_start <= 0 or (now - _identify_window_start) > IDENTIFY_WINDOW_SECONDS:
        _identify_window_start = now
        _identify_count = 0
        safe_save_state()


def _increment_identify_counter():
    global _identify_count, _identify_window_start
    _consume_identify_window()
    _identify_count += 1
    safe_save_state()


def _send_identify_or_resume(sock):
    """
    Send RESUME if we have a session_id and sequence, else IDENTIFY.
    Guard IDENTIFY frequency to avoid hitting IDENTIFY limits.
    Return tuple (mode_sent, success_bool) where mode_sent in ("resume","identify","none")
    """
    global _last_identify_time, _identify_failures, session_id, sequence

    now = time.time()

    if session_id and sequence is not None:
        resume_payload = {
            "op": 6,
            "d": {
                "token": TOKEN,
                "session_id": session_id,
                "seq": sequence,
            },
        }
        log.info("Attempting RESUME (session present).")
        ok = send_json(sock, resume_payload)
        if ok:
            log.debug("RESUME sent.")
        else:
            log.warning("RESUME send failed.")
        return "resume", ok

    _consume_identify_window()
    if _identify_count >= IDENTIFY_WINDOW_LIMIT:

        remaining = IDENTIFY_WINDOW_SECONDS - (int(time.time()) - _identify_window_start)
        if remaining < 0:
            remaining = 0
        log.error("IDENTIFY rolling limit reached (%d/%d). Sleeping %ds to avoid ban.",
                  _identify_count, IDENTIFY_WINDOW_LIMIT, remaining)
        time.sleep(remaining + 2)
        _consume_identify_window()

    if now - _last_identify_time < _IDENTIFY_MIN_INTERVAL:
        wait = _IDENTIFY_MIN_INTERVAL - (now - _last_identify_time)
        log.info("IDENTIFY rate guard active, sleeping %.2fs before IDENTIFY.", wait)
        time.sleep(wait)

    identify_payload = {
        "op": OP_IDENTIFY,
        "d": {
            "token": TOKEN,
            "properties": IDENTITY_PROPS,
            "presence": {"status": STATUS, "afk": False},
            "compress": False,
            "intents": INTENTS,
        },
    }
    log.info("Sending IDENTIFY (new session).")
    ok = send_json(sock, identify_payload)
    if ok:
        _last_identify_time = time.time()
        _identify_failures = 0
        _increment_identify_counter()
    else:
        _identify_failures += 1
        log.warning("IDENTIFY send failed (failure count=%d).", _identify_failures)
    if _identify_failures >= _IDENTIFY_MAX_FAILURES:
        sleep = min(60 * (_identify_failures - _IDENTIFY_MAX_FAILURES + 1), 600)
        log.warning("Too many IDENTIFY failures; sleeping %ds to avoid rate limits.", sleep)
        time.sleep(sleep)
    safe_save_state()
    return "identify", ok


def _health_watchdog_thread():
    """
    Watch last_ack_timestamp / last_activity_timestamp and exit process if unhealthy.
    This forces the platform (Render) to perform a restart.
    """
    while not should_stop.is_set():
        try:
            now = time.time()
            la = last_activity_timestamp or 0

            if (now - la) > HEALTH_ACTIVITY_TIMEOUT:
                log.error("Health watchdog: no activity for %.1fs (> %ds). Exiting to trigger restart.",
                          now - la, HEALTH_ACTIVITY_TIMEOUT)

                safe_save_state()
                os._exit(1)
        except Exception:
            log.exception("Health watchdog exception")

        time.sleep(HEALTH_WATCH_INTERVAL)


def _presence_refresh_thread():
    """
    Periodically resend presence to keep it fresh on Discord side.
    """
    while not should_stop.is_set():
        try:
            time.sleep(PRESENCE_REFRESH_INTERVAL)
            with ws_lock:
                if ws:
                    try:
                        pres = build_presence_payload()
                        send_json(ws, pres)
                        log.debug("Periodic presence refresh sent.")
                    except Exception:
                        log.debug("Periodic presence refresh failed.")
        except Exception:
            log.exception("Presence refresh thread exception")



def _activity_watchdog_thread():
    """
    Detect session zombie: no dispatch events received for some time even if heartbeat ACKs succeed.
    If suspected, force close to trigger resume/identify and recovery.
    """
    while not should_stop.is_set():
        try:
            time.sleep(ACTIVITY_WATCH_INTERVAL)
            now = time.time()
            la = last_activity_timestamp or 0
            if (now - la) > ACTIVITY_ZOMBIE_TIMEOUT:
                log.warning("Activity watchdog: no dispatchs for %.1fs (> %ds). Forcing socket close for recovery.",
                            now - la, ACTIVITY_ZOMBIE_TIMEOUT)

                try:
                    with ws_lock:
                        if ws:
                            ws.close()
                except Exception:
                    pass
        except Exception:
            log.exception("Activity watchdog exception")


def open_gateway_and_run():
    global ws, session_id, sequence, last_ack_timestamp, _ready_event
    backoff = RECONNECT_BASE_BACKOFF
    max_backoff = RECONNECT_MAX_BACKOFF
    RESET_SESSION_CODES = {4004, 4010, 4011, 4012, 4013, 4014}

    while not should_stop.is_set():
        hb_thread = None
        hb_stop = None
        sock = None
        _ready_event.clear()
        try:
            jitter = random.uniform(0.0, backoff * 0.3) if RECONNECT_JITTER else 0.0
            delay = backoff + jitter
            if backoff > RECONNECT_BASE_BACKOFF:
                log.info("Reconnecting in %.2fs (backoff)", delay)
                time.sleep(delay)

            log.info("Attempting connect to Gateway (backoff base %.1fs)...", backoff)
            try:

                sock = websocket.create_connection(GATEWAY_URL, timeout=10)
            except socket.gaierror as e:
                log.warning("DNS resolution error when connecting to gateway: %s", e)
                raise
            ws = sock
            sock.settimeout(RECV_TIMEOUT)

            hello_raw = sock.recv()
            hello = json.loads(hello_raw)
            if hello.get("op") != OP_HELLO or "d" not in hello:
                try:
                    sock.close()
                except Exception:
                    pass
                raise RuntimeError("Unexpected HELLO payload.")
            heartbeat_interval_ms = hello["d"]["heartbeat_interval"]

            last_ack_timestamp = time.time()
            _record_activity()
            safe_save_state()

            hb_stop = threading.Event()
            hb_thread = threading.Thread(
                target=heartbeat_loop,
                args=(sock, heartbeat_interval_ms, hb_stop),
                daemon=True,
            )
            hb_thread.start()

            mode, ok = _send_identify_or_resume(sock)

            waited_ok = False
            if ok:
                try:
                    waited_ok = _ready_event.wait(timeout=RESUME_WAIT_TIMEOUT)
                except Exception:
                    waited_ok = False

            if not waited_ok:

                if mode == "resume":
                    log.warning("RESUME did not produce READY/RESUMED within %.1fs - falling back to IDENTIFY.", RESUME_WAIT_TIMEOUT)

                    session_id = None
                    sequence = None
                    safe_save_state()

                    time.sleep(random.uniform(0.5, 2.0))

                    mode2, ok2 = _send_identify_or_resume(sock)
                    if ok2:
                        try:
                            waited_ok = _ready_event.wait(timeout=RESUME_WAIT_TIMEOUT)
                        except Exception:
                            waited_ok = False
                else:
                    log.warning("IDENTIFY/RESUME did not complete within timeout (%.1fs).", RESUME_WAIT_TIMEOUT)

            time.sleep(0.5)

            try:
                pres = build_presence_payload()
                send_json(sock, pres)
            except Exception:
                log.debug("Failed to send initial presence payload.")

            backoff = RECONNECT_BASE_BACKOFF

            while not should_stop.is_set():
                try:
                    raw = sock.recv()
                    if not raw:
                        raise websocket.WebSocketConnectionClosedException("Received empty data.")
                    data = json.loads(raw)
                    op = data.get("op")

                    if op == OP_DISPATCH:
                        handle_dispatch(data)
                    elif op == OP_RECONNECT:
                        log.info("Gateway requested reconnect (OP 7).")
                        try:
                            sock.close()
                        except Exception:
                            pass
                        break
                    elif op == OP_INVALID_SESSION:
                        resumable = data.get("d", False)
                        log.warning("Invalid session (OP 9). resumable=%s", resumable)
                        if not resumable:
                            session_id = None
                            sequence = None
                            safe_save_state()

                        time.sleep(random.uniform(1.0, 3.0))
                        break
                    elif op == OP_HEARTBEAT_ACK:
                        last_ack_timestamp = time.time()
                        _record_activity()
                        safe_save_state()
                    else:
                        log.debug("Received op %s", op)

                    close_code = getattr(sock, "close_code", None)
                    if close_code:
                        if close_code in RESET_SESSION_CODES:
                            session_id = None
                            sequence = None
                            log.error("Gateway sent fatal close code %s: resetting session.", close_code)
                            safe_save_state()
                            try:
                                sock.close()
                            except Exception:
                                pass
                            break

                except websocket.WebSocketTimeoutException:

                    continue
                except (websocket.WebSocketConnectionClosedException, ConnectionResetError) as e:
                    log.warning("WebSocket closed during recv: %s", e)
                    break
                except Exception as e:
                    log.exception("Unhandled exception in gateway loop: %s", e)
                    break

        except Exception as exc:
            close_code = getattr(ws, "close_code", None) if ws else None
            try:
                if hb_thread and hb_stop:
                    hb_stop.set()
                    hb_thread.join(timeout=2)
            except Exception:
                pass
            try:
                if ws:
                    with ws_lock:
                        try:
                            ws.close()
                        except Exception:
                            pass
                        ws = None
            except Exception:
                pass

            if isinstance(exc, websocket.WebSocketException) and close_code:
                if close_code in RESET_SESSION_CODES:
                    session_id = None
                    sequence = None
                    log.error("Fatal gateway close code %s: resetting session.", close_code)
                else:
                    log.warning("Gateway closed with code %s.", close_code)

            err_text = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            log.warning("Gateway error: %s. Will reconnect after backoff.", err_text)

            if backoff <= 0:
                backoff = RECONNECT_BASE_BACKOFF
            else:
                backoff = min(max_backoff, backoff * 2)

            sleep_time = backoff + random.uniform(0.5, min(5.0, backoff * 0.5))
            log.info("Sleeping %.2fs before next reconnect attempt.", sleep_time)
            time.sleep(sleep_time)
            continue
        finally:

            try:
                if 'hb_stop' in locals() and hb_stop:
                    hb_stop.set()
                if 'hb_thread' in locals() and hb_thread and hb_thread.is_alive():
                    hb_thread.join(timeout=2)
            except Exception:
                pass

            try:
                _ready_event.clear()
            except Exception:
                pass


def main():
    log.info(
        "Starting neveroff. Identity (placeholders until READY): %s / %s / %s",
        IDENTITY_OS,
        IDENTITY_BROWSER,
        IDENTITY_DEVICE,
    )
    log.info(
        "Config: STATUS=%s CUSTOM='%s' PERSIST=%s GATEWAY=%s",
        STATUS,
        CUSTOM_STATUS,
        PERSIST_PATH,
        GATEWAY_URL,
    )

    try:
        from keep_alive import keep_alive  
        try:
            keep_alive(PORT)
            log.info("External keep_alive started.")
        except Exception:
            log.exception("External keep_alive failed to start.")
    except Exception:
        log.debug("No external keep_alive module available; continuing.")

    hw = threading.Thread(target=_health_watchdog_thread, daemon=True)
    hw.start()

    pw = threading.Thread(target=_presence_refresh_thread, daemon=True)
    pw.start()

    aw = threading.Thread(target=_activity_watchdog_thread, daemon=True)
    aw.start()

    startup_delay = random.uniform(5.0, 25.0)
    log.info("Startup jitter: sleeping %.1fs", startup_delay)
    time.sleep(startup_delay)

    gw_thread = threading.Thread(target=open_gateway_and_run, daemon=True)
    gw_thread.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        should_stop.set()
        log.info("Shutting down by user request...")
        gw_thread.join(timeout=5)
        safe_save_state()
        sys.exit(0)


if __name__ == "__main__":
    main()
