#!/usr/bin/env python3
"""Loopback-only, height-bounded Ord reader and opt-in tuning watchdog.

Python 3.9+, standard library only. This module never opens a ledger or wallet.
"""
import argparse
import contextlib
import http.client
import http.server
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

READER_UNIT = "ord20-validation-reader.service"
MAX_BODY = 64 * 1024 * 1024
CONFIG_KEYS = {"schema", "upstream", "height_limit", "requests_per_second",
               "listen_port", "max_response_bytes"}


class PolicyError(Exception):
    """A controlled diagnostic that contains no configuration or response data."""


def finite_number(value, minimum, maximum):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise PolicyError("invalid numeric setting")
    if not minimum <= value <= maximum:
        raise PolicyError("numeric setting outside permitted range")
    return value


def integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise PolicyError("invalid integer setting")
    return value


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise PolicyError("duplicate JSON key")
        value[key] = item
    return value


def decode(raw):
    return json.loads(raw, object_pairs_hook=unique_object)


def read_json(path, maximum=65536):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise PolicyError("expected a regular configuration or status file")
    with path.open("rb") as source:
        raw = source.read(maximum + 1)
    if len(raw) > maximum:
        raise PolicyError("configuration or status file exceeds bound")
    return decode(raw)


def loopback_url(value, permitted_path=""):
    if not isinstance(value, str):
        raise PolicyError("upstream must be a loopback HTTP URL")
    try:
        url = urllib.parse.urlsplit(value)
        address = ipaddress.ip_address(url.hostname or "")
        port = url.port or 80
    except ValueError:
        raise PolicyError("upstream must use a literal loopback address") from None
    if (url.scheme != "http" or not address.is_loopback or url.username is not None
            or url.password is not None or url.query or url.fragment
            or url.path not in (permitted_path, permitted_path + "/")):
        raise PolicyError("upstream URL is outside the loopback policy")
    integer(port, 1, 65535)
    host = "[{}]".format(address) if address.version == 6 else str(address)
    return "http://{}:{}{}".format(host, port, permitted_path)


def validate_config(value):
    if not isinstance(value, dict) or set(value) - CONFIG_KEYS:
        raise PolicyError("unknown configuration field")
    if type(value.get("schema", 1)) is not int or value.get("schema", 1) != 1:
        raise PolicyError("unsupported configuration schema")
    result = {
        "schema": 1,
        "upstream": loopback_url(value.get("upstream")),
        "height_limit": integer(value.get("height_limit"), 0, 2147483647),
        "requests_per_second": finite_number(value.get("requests_per_second", 10), 0.1, 100),
        "listen_port": integer(value.get("listen_port", 18080), 1024, 65535),
        "max_response_bytes": integer(value.get("max_response_bytes", MAX_BODY), 1, MAX_BODY),
    }
    if urllib.parse.urlsplit(result["upstream"]).port == result["listen_port"]:
        raise PolicyError("reader cannot use its own listening port as upstream")
    return result


def read_config(path):
    return validate_config(read_json(path))


def atomic_json(path, value, mode=0o640, preserve=False, durable=True):
    """Replace a local control/status file; preserve control-file access modes."""
    path = Path(path)
    if path.is_symlink():
        raise PolicyError("refusing a symlink destination")
    original = path.stat() if preserve else None
    if original and not stat.S_ISREG(original.st_mode):
        raise PolicyError("control file must be regular")
    descriptor, name = tempfile.mkstemp(prefix=".ord20-validation-", dir=str(path.parent))
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
            output.write(json.dumps(value, indent=2, allow_nan=False) + "\n")
            output.flush()
            if durable:
                os.fsync(output.fileno())
            if original and hasattr(os, "fchown"):
                current = os.fstat(output.fileno())
                if (current.st_uid, current.st_gid) != (original.st_uid, original.st_gid):
                    os.fchown(output.fileno(), original.st_uid, original.st_gid)
            if hasattr(os, "fchmod"):
                os.fchmod(output.fileno(), stat.S_IMODE(original.st_mode) if original else mode)
        if original:
            current = path.stat()
            if (current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size) != (
                    original.st_dev, original.st_ino, original.st_mtime_ns, original.st_size):
                raise PolicyError("control file changed concurrently")
        os.replace(str(temporary), str(path))
    finally:
        if temporary.exists():
            temporary.unlink()


@contextlib.contextmanager
def owned_lock(path):
    """All config writers must use this lock; stale locks require operator review."""
    path = Path(path)
    try:
        descriptor = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise PolicyError("configuration or instance is already locked") from None
    identity = os.fstat(descriptor)
    try:
        os.write(descriptor, str(os.getpid()).encode("ascii"))
        yield
    finally:
        os.close(descriptor)
        if path.exists() and not path.is_symlink():
            current = path.stat()
            if (current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino):
                path.unlink()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def opener():
    # Environment proxies and upstream redirects must not escape loopback.
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


class Pacer:
    def __init__(self, clock=time.monotonic, sleep=time.sleep):
        self.clock, self.sleep, self.last = clock, sleep, -math.inf

    def wait(self, rate):
        delay = self.last + 1.0 / rate - self.clock()
        if delay > 0:
            self.sleep(delay)
        self.last = self.clock()


def allowed(path, ceiling):
    if path in ("/status", "/blockheight"):
        return True
    match = re.fullmatch(r"/(?:block|blockhash)/(0|[1-9][0-9]*)", path)
    if match:
        return int(match.group(1)) <= ceiling
    return re.fullmatch(r"/output/[0-9a-f]{64}:[0-9]{1,10}", path) is not None


def healthy_status(value, minimum):
    if (not isinstance(value, dict) or value.get("chain") != "mainnet"
            or value.get("unrecoverably_reorged") is not False
            or type(value.get("height")) is not int or value["height"] < minimum
            or type(value.get("inscriptions")) is not int or value["inscriptions"] <= 0
            or value.get("inscription_index") is False):
        raise PolicyError("upstream is not a healthy mainnet inscription index")


def bounded_view(path, raw, config):
    ceiling = config["height_limit"]
    if path == "/blockheight":
        text = raw.strip()
        if not re.fullmatch(rb"[0-9]+", text):
            raise PolicyError("invalid upstream height")
        return str(min(int(text), ceiling)).encode("ascii")
    if path == "/status":
        value = decode(raw)
        healthy_status(value, ceiling)
        value.update(upstream_height=value["height"], height=ceiling,
                     replay_height_ceiling=ceiling, replay_source="shared_ord_read_only",
                     independent_ord_rebuild=False)
        return json.dumps(value, allow_nan=False).encode("utf-8")
    if path.startswith("/block/"):
        value = decode(raw)
        if type(value.get("height")) is not int or value["height"] != int(path.rsplit("/", 1)[1]):
            raise PolicyError("block height identity mismatch")
    if path.startswith("/blockhash/") and not re.fullmatch(rb"[0-9a-f]{64}", raw.strip()):
        raise PolicyError("invalid upstream block hash")
    return raw


class Reader:
    def __init__(self, config_path, state_dir, resource_guard=None):
        self.config_path, self.state_dir = Path(config_path), Path(state_dir)
        self.config = read_config(self.config_path)
        self.resource_guard = Path(resource_guard) if resource_guard else None
        self.client, self.pacer = opener(), Pacer()
        self.stats = {"run_id": uuid.uuid4().hex, "requests": 0, "upstream_requests": 0,
                      "blocked": 0, "errors": 0, "transport_retries": 0,
                      "error_classes": {}, "max_upstream_concurrency": 1,
                      "requests_per_second": self.config["requests_per_second"]}

    def settings(self):
        value = read_config(self.config_path)
        if (value["upstream"] != self.config["upstream"]
                or value["listen_port"] != self.config["listen_port"]
                or value["height_limit"] < self.config["height_limit"]):
            raise PolicyError("upstream/listener changed or height ceiling rewound")
        if self.resource_guard and read_json(self.resource_guard).get("stop_reason"):
            raise PolicyError("resource guard stopped validation")
        self.config = value
        self.stats["requests_per_second"] = value["requests_per_second"]
        return value

    def fetch(self, path, config):
        request = urllib.request.Request(config["upstream"] + path,
                                         headers={"Accept": "application/json"})
        for attempt in range(3):
            self.pacer.wait(config["requests_per_second"])
            self.stats["upstream_requests"] += 1
            try:
                with self.client.open(request, timeout=15 if path.startswith("/block/") else 2) as response:
                    raw = response.read(config["max_response_bytes"] + 1)
                if len(raw) > config["max_response_bytes"]:
                    raise PolicyError("upstream response exceeds memory bound")
                return bounded_view(path, raw, config)
            except urllib.error.HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504) or attempt == 2:
                    raise
            except (OSError, urllib.error.URLError, http.client.IncompleteRead):
                if attempt == 2:
                    raise
            self.stats["transport_retries"] += 1
            time.sleep(0.5 * (attempt + 1))
        raise PolicyError("bounded upstream attempts exhausted")

    def save_stats(self):
        # Telemetry needs atomic visibility, not a disk flush for every GET.
        atomic_json(self.state_dir / "reader-stats.json", self.stats, durable=False)


def handler_for(reader):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, status, body):
            self.send_response(status)
            content_type = "text/plain" if self.path == "/blockheight" or self.path.startswith("/blockhash/") else "application/json"
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            reader.stats["requests"] += 1
            try:
                config = reader.settings()
                if not allowed(self.path, config["height_limit"]):
                    reader.stats["blocked"] += 1
                    self.respond(403, b'{"error":"outside bounded read view"}')
                else:
                    self.respond(200, reader.fetch(self.path, config))
            except Exception as error:
                reader.stats["errors"] += 1
                kind = type(error).__name__
                reader.stats["error_classes"][kind] = reader.stats["error_classes"].get(kind, 0) + 1
                with contextlib.suppress(OSError):
                    self.respond(503, b'{"error":"bounded reader unavailable"}')
            finally:
                reader.save_stats()

        def do_POST(self):
            self.respond(405, b'{"error":"read-only reader"}')

        do_PUT = do_DELETE = do_PATCH = do_POST
    return Handler


class SerialServer(http.server.HTTPServer):
    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(5)
        return connection, address


def cpu_percent(raw):
    if raw == "infinity":
        return math.inf
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(us|ms|s)", raw)
    if not match:
        raise PolicyError("unrecognized validation-reader CPU quota")
    return float(match[1]) * {"us": 0.0001, "ms": 0.1, "s": 100}[match[2]]


def systemd_cpu(value=None):
    state = subprocess.run(["systemctl", "is-active", "--quiet", READER_UNIT],
                           capture_output=True, timeout=5)
    if state.returncode:
        raise PolicyError("validation reader unit is inactive; no service will be started")
    if value is None:
        result = subprocess.run(["systemctl", "show", READER_UNIT, "-p", "CPUQuotaPerSecUSec", "--value"],
                                capture_output=True, text=True, timeout=5)
        if result.returncode:
            raise PolicyError("cannot inspect validation reader CPU quota")
        return cpu_percent(result.stdout.strip())
    quota = "" if math.isinf(value) else "{}%".format(value)
    result = subprocess.run(["systemctl", "set-property", "--runtime", READER_UNIT, "CPUQuota=" + quota],
                            capture_output=True, timeout=5)
    if result.returncode:
        raise PolicyError("cannot adjust validation reader CPU quota")


def change_config(path, rate=None, height=None, cpu=None, apply=False, lower_only=False):
    path = Path(path)
    proposed = read_config(path)
    if rate is not None:
        finite_number(rate, 0.1, 100)
        proposed["requests_per_second"] = min(rate, proposed["requests_per_second"]) if lower_only else rate
    if height is not None:
        integer(height, proposed["height_limit"], 2147483647)
        proposed["height_limit"] = height
    if cpu is not None:
        finite_number(cpu, 1, 1000)
    if not apply:
        return {"applied": False, "requests_per_second": proposed["requests_per_second"],
                "height_limit": proposed["height_limit"], "cpu_quota_percent": cpu}
    with owned_lock(path.with_name(path.name + ".lock")):
        # Reload inside the lock so a simultaneous height advance is preserved.
        current = read_config(path)
        proposed = dict(current)
        if rate is not None:
            proposed["requests_per_second"] = min(rate, current["requests_per_second"]) if lower_only else rate
        if height is not None:
            integer(height, current["height_limit"], 2147483647)
            proposed["height_limit"] = height
        previous_cpu = systemd_cpu() if cpu is not None else None
        desired_cpu = min(cpu, previous_cpu) if cpu is not None and lower_only else cpu
        if desired_cpu is not None:
            systemd_cpu(desired_cpu)
        try:
            atomic_json(path, validate_config(proposed), preserve=True)
        except Exception:
            if previous_cpu is not None:
                systemd_cpu(previous_cpu)
            raise
    return {"applied": True, "requests_per_second": proposed["requests_per_second"],
            "height_limit": proposed["height_limit"], "cpu_quota_percent": desired_cpu}


def probe(config, reader_state, initial_stats, max_latency, tip_url=None, max_lag=6):
    result = {"healthy": False, "reasons": [], "index_lag_checked": bool(tip_url)}
    started = time.monotonic()
    try:
        with opener().open(config["upstream"] + "/status", timeout=2) as response:
            raw = response.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise PolicyError("health response exceeds bound")
        status = decode(raw)
        healthy_status(status, config["height_limit"])
        result["status_seconds"] = round(time.monotonic() - started, 6)
        if result["status_seconds"] > max_latency:
            result["reasons"].append("source_slow")
        if tip_url:
            with opener().open(loopback_url(tip_url, "/blockheight"), timeout=2) as response:
                tip = response.read(65).strip()
            if not re.fullmatch(rb"[0-9]{1,10}", tip):
                raise PolicyError("invalid reference height")
            result["lag_blocks"] = max(0, int(tip) - status["height"])
            if result["lag_blocks"] > max_lag:
                result["reasons"].append("source_lag")
        counts = read_json(reader_state)
        if counts.get("run_id") != initial_stats.get("run_id"):
            result["reasons"].append("reader_restarted")
        for field in ("errors", "transport_retries"):
            if type(counts.get(field)) is not int or counts[field] != initial_stats.get(field):
                result["reasons"].append("reader_fetch_errors")
        result["healthy"] = not result["reasons"]
    except Exception:
        result["reasons"].append("source_or_reader_status_unavailable")
    return result


def watch(args):
    initial = read_json(args.reader_state)
    if (not isinstance(initial.get("run_id"), str)
            or any(type(initial.get(k)) is not int or initial[k] < 0 for k in ("errors", "transport_retries"))):
        raise PolicyError("invalid reader counters")
    failures = 0
    while True:
        config = read_config(args.config)
        result = probe(config, args.reader_state, initial, args.max_latency,
                       args.tip_height_url, args.max_lag)
        failures = 0 if result["healthy"] else failures + 1
        result.update(consecutive_bad_samples=failures, rollback_triggered=False)
        if failures >= args.bad_samples:
            result["rollback"] = change_config(args.config, rate=args.fallback_rps,
                                                cpu=args.fallback_cpu_percent,
                                                apply=args.apply, lower_only=True)
            result["rollback_triggered"] = args.apply
        print(json.dumps(result), flush=True)
        if args.once or failures >= args.bad_samples:
            return 0 if result["healthy"] or result["rollback_triggered"] else 2
        time.sleep(args.poll_seconds)


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("check-config", "serve", "set-rate", "set-height", "watch"):
        command = commands.add_parser(name)
        command.add_argument("--config", required=True, type=Path)
        if name == "serve":
            command.add_argument("--state-dir", required=True, type=Path)
            command.add_argument("--resource-guard", type=Path)
        if name in ("set-rate", "set-height"):
            command.add_argument("--apply", action="store_true")
        if name == "set-rate":
            command.add_argument("--requests-per-second", type=float, required=True)
            command.add_argument("--cpu-quota-percent", type=float)
        if name == "set-height":
            command.add_argument("--height-limit", type=int, required=True)
        if name == "watch":
            command.add_argument("--reader-state", type=Path, required=True)
            command.add_argument("--fallback-rps", type=float, default=10)
            command.add_argument("--fallback-cpu-percent", type=float)
            command.add_argument("--max-latency", type=float, default=0.25)
            command.add_argument("--max-lag", type=int, default=6)
            command.add_argument("--tip-height-url")
            command.add_argument("--bad-samples", type=int, default=3)
            command.add_argument("--poll-seconds", type=float, default=10)
            command.add_argument("--once", action="store_true")
            command.add_argument("--apply", action="store_true")
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        config = read_config(args.config)
        if args.command == "check-config":
            print(json.dumps({"valid": True, "requests_per_second": config["requests_per_second"],
                              "height_limit": config["height_limit"]}))
        elif args.command == "serve":
            args.state_dir.mkdir(mode=0o750, parents=True, exist_ok=True)
            if args.state_dir.is_symlink():
                raise PolicyError("state directory must not be a symlink")
            previous = signal.getsignal(signal.SIGTERM)
            def terminate(signum, frame):
                raise SystemExit(0)
            signal.signal(signal.SIGTERM, terminate)
            try:
                with owned_lock(args.state_dir / "reader.lock"):
                    reader = Reader(args.config, args.state_dir, args.resource_guard)
                    reader.save_stats()
                    with SerialServer(("127.0.0.1", config["listen_port"]), handler_for(reader)) as server:
                        server.serve_forever()
            finally:
                signal.signal(signal.SIGTERM, previous)
        elif args.command == "set-rate":
            print(json.dumps(change_config(args.config, rate=args.requests_per_second,
                                           cpu=args.cpu_quota_percent, apply=args.apply)))
        elif args.command == "set-height":
            print(json.dumps(change_config(args.config, height=args.height_limit, apply=args.apply)))
        else:
            finite_number(args.fallback_rps, 0.1, 100)
            if args.fallback_cpu_percent is not None:
                finite_number(args.fallback_cpu_percent, 1, 1000)
            finite_number(args.max_latency, 0.01, 10)
            finite_number(args.poll_seconds, 1, 300)
            integer(args.bad_samples, 1, 100)
            integer(args.max_lag, 0, 1000)
            if args.tip_height_url:
                loopback_url(args.tip_height_url, "/blockheight")
            return watch(args)
        return 0
    except (PolicyError, OSError, ValueError, subprocess.SubprocessError) as error:
        # Never echo URLs, credentials, configuration, response bodies or traces.
        print(json.dumps({"ok": False, "error_kind": type(error).__name__}), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
