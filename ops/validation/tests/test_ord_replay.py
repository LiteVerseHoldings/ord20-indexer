import argparse
import concurrent.futures
import contextlib
import http.server
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
import urllib.error
import urllib.request

TOOL = Path(__file__).resolve().parents[1] / "ord_replay.py"
spec = importlib.util.spec_from_file_location("ord_replay_under_test", TOOL)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def configuration(upstream="http://127.0.0.1:18081", **updates):
    value = dict(schema=1, upstream=upstream, height_limit=100,
                 requests_per_second=10, listen_port=18080)
    value.update(updates)
    return value


def status(height=200, **updates):
    value = dict(chain="mainnet", height=height, inscriptions=1,
                 inscription_index=True, unrecoverably_reorged=False)
    value.update(updates)
    return value


class Origin(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        owner = self.server
        with owner.guard:
            owner.requests.append(self.path)
            owner.active += 1
            owner.maximum_active = max(owner.maximum_active, owner.active)
        try:
            if owner.delay:
                time.sleep(owner.delay)
            code, headers = 200, {}
            if self.path in owner.responses:
                code, body, headers = owner.responses[self.path]
            elif self.path == "/status":
                body = json.dumps(status()).encode()
            elif self.path == "/blockheight":
                body = b"200"
            elif self.path.startswith("/blockhash/"):
                body = b"a" * 64
            elif self.path.startswith("/block/"):
                body = json.dumps(dict(height=int(self.path.rsplit("/", 1)[1]),
                                       hash="a" * 64, transactions=[])).encode()
            else:
                code, body = 404, b"not found"
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)
        finally:
            with owner.guard:
                owner.active -= 1


@contextlib.contextmanager
def origin(delay=0):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Origin)
    server.responses, server.requests = {}, []
    server.guard = threading.Lock()
    server.active = server.maximum_active = 0
    server.delay = delay
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, "http://127.0.0.1:{}".format(server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@contextlib.contextmanager
def relay(upstream, **updates):
    with tempfile.TemporaryDirectory() as directory:
        directory = Path(directory)
        config = directory / "reader.json"
        config.write_text(json.dumps(configuration(upstream, **updates)))
        reader = m.Reader(config, directory)
        reader.save_stats()
        server = m.SerialServer(("127.0.0.1", 0), m.handler_for(reader))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield reader, config, "http://127.0.0.1:{}".format(server.server_port)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


def request(url, method="GET"):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


class ConfigurationTests(unittest.TestCase):
    def test_conservative_default_and_no_required_dependencies(self):
        value = m.validate_config(dict(upstream="http://127.0.0.1:8080", height_limit=100))
        self.assertEqual(value["requests_per_second"], 10)
        self.assertEqual(value["listen_port"], 18080)
        self.assertEqual(value["max_response_bytes"], 64 * 1024 * 1024)

    def test_invalid_rates_are_rejected(self):
        for rate in (0, -1, True, "30", math.nan, math.inf, 100.1):
            with self.subTest(rate=rate), self.assertRaises(m.PolicyError):
                m.validate_config(configuration(requests_per_second=rate))

    def test_invalid_height_and_unknown_fields_are_rejected(self):
        for updates in ({"height_limit": True}, {"height_limit": -1},
                        {"height_limit": 2147483648}, {"schema": True},
                        {"schema": 2}, {"secret": "synthetic"}, {"listen_port": 80}):
            with self.subTest(updates=updates), self.assertRaises(m.PolicyError):
                m.validate_config(configuration(**updates))

    def test_external_authenticated_and_ambiguous_upstreams_are_rejected(self):
        urls = ("http://example.invalid", "https://127.0.0.1:8080", "http://localhost:8080",
                "http://user:password@127.0.0.1:8080", "http://127.0.0.1:8080/wallet",
                "http://127.0.0.1:8080?key=synthetic", "http://127.0.0.1:8080/#x",
                "file:///synthetic", "http://127.0.0.1:18080")
        for upstream in urls:
            with self.subTest(upstream=upstream), self.assertRaises(m.PolicyError):
                m.validate_config(configuration(upstream))
        self.assertEqual(m.loopback_url("http://[::1]:8080"), "http://[::1]:8080")

    def test_duplicate_config_keys_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text('{"height_limit":100,"height_limit":200}')
            with self.assertRaises(m.PolicyError):
                m.read_config(path)

    def test_cli_inspection_and_tuning_default_to_no_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(configuration()))
            before = path.read_bytes()
            with patch.object(m, "systemd_cpu") as cpu, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(m.main(["check-config", "--config", str(path)]), 0)
                self.assertEqual(m.main(["set-rate", "--config", str(path),
                                         "--requests-per-second", "30", "--cpu-quota-percent", "100"]), 0)
                self.assertEqual(m.main(["set-height", "--config", str(path), "--height-limit", "200"]), 0)
            cpu.assert_not_called()
            self.assertEqual(path.read_bytes(), before)

    def test_cli_does_not_echo_secret_input_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(configuration("http://user:DO_NOT_PRINT@127.0.0.1:8080")))
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(m.main(["check-config", "--config", str(path)]), 2)
            self.assertNotIn("DO_NOT_PRINT", output.getvalue())


class PacingTests(unittest.TestCase):
    def test_dynamic_rates_and_each_attempt_obey_spacing(self):
        clock = [0.0]
        sleeps = []
        def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds
        pace = m.Pacer(lambda: clock[0], sleep)
        pace.wait(10)
        self.assertEqual(sleeps, [])
        pace.wait(10)
        self.assertAlmostEqual(clock[0], 0.1)
        pace.wait(20)
        self.assertAlmostEqual(clock[0], 0.15)
        pace.wait(30)
        self.assertAlmostEqual(clock[0], 0.15 + 1 / 30)
        clock[0] += 1
        pace.wait(10)
        self.assertEqual(len(sleeps), 3)

    def test_transient_retries_are_paced_and_payload_stays_identical(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(configuration(requests_per_second=30)))
            reader = m.Reader(path, Path(directory))
            raw = b'{"height":100,"transactions":[]}'
            response = Mock()
            response.__enter__ = Mock(return_value=response)
            response.__exit__ = Mock(return_value=False)
            response.read.return_value = raw
            error = urllib.error.HTTPError("http://127.0.0.1", 503, "synthetic", {}, None)
            reader.client = Mock()
            reader.client.open.side_effect = [error, response]
            reader.pacer = Mock()
            with patch.object(m.time, "sleep"):
                self.assertEqual(reader.fetch("/block/100", reader.config), raw)
            self.assertEqual(reader.pacer.wait.call_args_list, [unittest.mock.call(30), unittest.mock.call(30)])
            self.assertEqual(reader.stats["transport_retries"], 1)

    def test_transport_retries_are_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(configuration()))
            reader = m.Reader(path, Path(directory))
            reader.client, reader.pacer = Mock(), Mock()
            error = urllib.error.HTTPError("http://127.0.0.1", 503, "synthetic", {}, None)
            reader.client.open.side_effect = error
            with patch.object(m.time, "sleep"), self.assertRaises(urllib.error.HTTPError):
                reader.fetch("/block/100", reader.config)
            self.assertEqual(reader.client.open.call_count, 3)
            self.assertEqual(reader.pacer.wait.call_count, 3)


class ReaderTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "POSIX SIGTERM cleanup")
    def test_cli_graceful_stop_releases_own_lock_and_allows_restart(self):
        with origin() as (_, upstream), tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            config = directory / "reader.json"
            config.write_text(json.dumps(configuration(upstream, listen_port=port)))
            state = directory / "state"
            for _ in range(2):
                process = subprocess.Popen([sys.executable, "-I", "-B", str(TOOL), "serve",
                                            "--config", str(config), "--state-dir", str(state)],
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 5
                    while True:
                        try:
                            if request("http://127.0.0.1:{}/status".format(port))[0] == 200:
                                break
                        except OSError:
                            pass
                        if time.monotonic() >= deadline or process.poll() is not None:
                            self.fail("synthetic reader did not start")
                        time.sleep(0.02)
                    process.terminate()
                    output, errors = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 0, (output, errors))
                    self.assertFalse((state / "reader.lock").exists())
                    self.assertTrue((state / "reader-stats.json").exists())
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate(timeout=5)

    def test_boundary_and_wallet_paths_never_reach_upstream(self):
        with origin() as (source, upstream), relay(upstream) as (_, _, url):
            paths = ("/block/101", "/blockhash/101", "/wallet", "/rpc", "/block/01",
                     "/block/%31", "/block/1?x=1", "/output/../wallet")
            for path in paths:
                self.assertEqual(request(url + path)[0], 403)
            self.assertEqual(source.requests, [])

    def test_write_methods_never_reach_upstream(self):
        with origin() as (source, upstream), relay(upstream) as (_, _, url):
            for method in ("POST", "PUT", "PATCH", "DELETE"):
                self.assertEqual(request(url + "/status", method)[0], 405)
            self.assertEqual(source.requests, [])

    def test_height_bounded_status_explicitly_discloses_shared_source(self):
        with origin() as (_, upstream), relay(upstream) as (_, _, url):
            self.assertEqual(request(url + "/blockheight"), (200, b"100"))
            code, raw = request(url + "/status")
            value = json.loads(raw)
            self.assertEqual(code, 200)
            self.assertEqual(value["height"], 100)
            self.assertEqual(value["upstream_height"], 200)
            self.assertFalse(value["independent_ord_rebuild"])

    def test_historical_block_and_hash_bytes_are_unchanged(self):
        with origin() as (source, upstream), relay(upstream) as (_, _, url):
            raw = b'{ "height":100, "transactions":[], "extra":"preserved" }'
            source.responses["/block/100"] = (200, raw, {})
            self.assertEqual(request(url + "/block/100"), (200, raw))
            self.assertEqual(request(url + "/blockhash/100"), (200, b"a" * 64))

    def test_wrong_height_invalid_json_and_missing_block_fail_without_retry(self):
        with origin() as (source, upstream), relay(upstream) as (_, _, url):
            for code, raw in ((200, b'{"height":101}'), (200, b'not JSON'), (404, b'missing')):
                source.requests.clear()
                source.responses["/block/100"] = (code, raw, {})
                self.assertEqual(request(url + "/block/100")[0], 503)
                self.assertEqual(source.requests, ["/block/100"])

    def test_bad_status_never_looks_healthy(self):
        with origin() as (source, upstream), relay(upstream) as (_, _, url):
            for values in (status(chain="testnet"), status(unrecoverably_reorged=True),
                           status(height=99), status(inscriptions=0), status(inscription_index=False)):
                source.responses["/status"] = (200, json.dumps(values).encode(), {})
                self.assertEqual(request(url + "/status")[0], 503)

    def test_response_memory_bound(self):
        with origin() as (source, upstream), relay(upstream, max_response_bytes=32) as (_, _, url):
            source.responses["/block/100"] = (200, b"x" * 64, {})
            self.assertEqual(request(url + "/block/100")[0], 503)
            self.assertEqual(len(source.requests), 1)

    def test_redirects_are_not_followed(self):
        with origin() as (destination, destination_url), origin() as (source, upstream):
            source.responses["/block/100"] = (302, b"", {"Location": destination_url + "/status"})
            with relay(upstream) as (_, _, url):
                self.assertEqual(request(url + "/block/100")[0], 503)
            self.assertEqual(destination.requests, [])

    def test_environment_proxy_is_ignored(self):
        with origin() as (_, upstream):
            with patch.dict(os.environ, {"http_proxy": "http://127.0.0.1:9", "HTTP_PROXY": "http://127.0.0.1:9", "no_proxy": "", "NO_PROXY": ""}):
                with relay(upstream) as (reader, _, _):
                    self.assertEqual(reader.fetch("/blockheight", reader.config), b"100")

    def test_parallel_clients_still_produce_one_upstream_request_at_a_time(self):
        with origin(delay=0.03) as (source, upstream), relay(upstream, requests_per_second=100) as (_, _, url):
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                responses = list(pool.map(request, [url + "/block/100"] * 4))
            self.assertTrue(all(code == 200 for code, _ in responses))
            self.assertEqual(source.maximum_active, 1)

    def test_rate_reload_and_height_advance_preserve_bound(self):
        with origin() as (_, upstream), relay(upstream) as (reader, config, url):
            m.change_config(config, rate=30, height=150, apply=True)
            self.assertEqual(request(url + "/block/150")[0], 200)
            self.assertEqual(reader.stats["requests_per_second"], 30)
            self.assertEqual(request(url + "/block/151")[0], 403)

    def test_manual_height_rewind_or_upstream_change_fails_closed(self):
        with origin() as (source, upstream), relay(upstream) as (_, config, url):
            config.write_text(json.dumps(configuration(upstream, height_limit=99)))
            self.assertEqual(request(url + "/block/99")[0], 503)
            config.write_text(json.dumps(configuration("http://127.0.0.1:9")))
            self.assertEqual(request(url + "/status")[0], 503)
            self.assertEqual(source.requests, [])

    def test_resource_guard_stops_reads_before_upstream(self):
        with origin() as (source, upstream), relay(upstream) as (reader, config, url):
            guard = config.with_name("guard.json")
            guard.write_text('{"stop_reason":"synthetic resource guard"}')
            reader.resource_guard = guard
            self.assertEqual(request(url + "/status")[0], 503)
            self.assertEqual(source.requests, [])


class UpdateAndRollbackTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "reader.json"
        self.path.write_text(json.dumps(configuration(height_limit=200, requests_per_second=30)))

    def test_rollback_only_lowers_rate_and_keeps_current_height(self):
        m.change_config(self.path, height=250, apply=True)
        m.change_config(self.path, rate=10, apply=True, lower_only=True)
        self.assertEqual(m.read_config(self.path)["height_limit"], 250)
        self.assertEqual(m.read_config(self.path)["requests_per_second"], 10)
        m.change_config(self.path, rate=20, apply=True, lower_only=True)
        self.assertEqual(m.read_config(self.path)["requests_per_second"], 10)

    def test_height_update_cannot_rewind(self):
        before = self.path.read_bytes()
        with self.assertRaises(m.PolicyError):
            m.change_config(self.path, height=199, apply=True)
        self.assertEqual(self.path.read_bytes(), before)

    def test_config_lock_prevents_concurrent_update(self):
        before = self.path.read_bytes()
        with m.owned_lock(self.path.with_name(self.path.name + ".lock")):
            with self.assertRaises(m.PolicyError):
                m.change_config(self.path, rate=10, apply=True)
        self.assertEqual(self.path.read_bytes(), before)

    @unittest.skipUnless(os.name == "posix", "Windows prevents replacing this open lock")
    def test_foreign_replacement_lock_is_not_removed(self):
        lock = self.path.with_name("synthetic.lock")
        with m.owned_lock(lock):
            lock.rename(lock.with_name("original.lock"))
            lock.write_text("foreign")
        self.assertEqual(lock.read_text(), "foreign")

    @unittest.skipUnless(os.name == "posix", "POSIX ownership and modes")
    def test_atomic_config_update_preserves_owner_and_mode(self):
        self.path.chmod(0o640)
        before = self.path.stat()
        m.change_config(self.path, rate=20, apply=True)
        after = self.path.stat()
        self.assertEqual((after.st_uid, after.st_gid, after.st_mode & 0o777),
                         (before.st_uid, before.st_gid, 0o640))

    def test_cpu_failure_does_not_change_config(self):
        before = self.path.read_bytes()
        with patch.object(m, "systemd_cpu", side_effect=[50, m.PolicyError("synthetic failure")]):
            with self.assertRaises(m.PolicyError):
                m.change_config(self.path, rate=20, cpu=100, apply=True)
        self.assertEqual(self.path.read_bytes(), before)

    def test_failed_config_write_restores_previous_cpu_quota(self):
        before = self.path.read_bytes()
        with patch.object(m, "systemd_cpu", side_effect=[50, None, None]) as cpu:
            with patch.object(m, "atomic_json", side_effect=OSError("synthetic")):
                with self.assertRaises(OSError):
                    m.change_config(self.path, rate=20, cpu=100, apply=True)
        self.assertEqual(cpu.call_args_list, [unittest.mock.call(), unittest.mock.call(100), unittest.mock.call(50)])
        self.assertEqual(self.path.read_bytes(), before)

    def test_cpu_rollback_never_increases_existing_quota(self):
        with patch.object(m, "systemd_cpu", side_effect=[25, None]) as cpu:
            m.change_config(self.path, rate=10, cpu=50, apply=True, lower_only=True)
        self.assertEqual(cpu.call_args_list, [unittest.mock.call(), unittest.mock.call(25)])

    def test_cpu_actions_are_runtime_only_and_scoped_to_reader(self):
        response = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        with patch.object(m.subprocess, "run", return_value=response) as run:
            m.systemd_cpu(50)
        self.assertEqual(run.call_args_list[1].args[0],
                         ["systemctl", "set-property", "--runtime", "ord20-validation-reader.service", "CPUQuota=50%"])
        with patch.object(m.subprocess, "run", return_value=response) as run:
            m.systemd_cpu(math.inf)
        self.assertEqual(run.call_args_list[1].args[0][-1], "CPUQuota=")

    def test_inactive_unit_is_never_started(self):
        response = subprocess.CompletedProcess([], 3, stdout="", stderr="")
        with patch.object(m.subprocess, "run", return_value=response) as run:
            with self.assertRaises(m.PolicyError):
                m.systemd_cpu(50)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(m.cpu_percent("500ms"), 50)
        self.assertEqual(m.cpu_percent("1s"), 100)
        self.assertTrue(math.isinf(m.cpu_percent("infinity")))


class WatchdogTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.config = Path(self.directory.name) / "reader.json"
        self.stats = self.config.with_name("stats.json")
        self.config.write_text(json.dumps(configuration(height_limit=100, requests_per_second=30)))
        self.initial = dict(run_id="synthetic-run", errors=0, transport_retries=0)
        self.stats.write_text(json.dumps(self.initial))

    def arguments(self, apply=False, once=True, bad_samples=1):
        return argparse.Namespace(config=self.config, reader_state=self.stats, fallback_rps=10,
                                  fallback_cpu_percent=50, max_latency=0.25, tip_height_url=None,
                                  max_lag=6, bad_samples=bad_samples, poll_seconds=1,
                                  once=once, apply=apply)

    def test_probe_checks_health_errors_restarts_and_optional_lag(self):
        with origin() as (source, upstream), origin() as (tip_source, tip_upstream):
            config = m.validate_config(configuration(upstream))
            self.assertTrue(m.probe(config, self.stats, self.initial, 1)["healthy"])
            tip_source.responses["/blockheight"] = (200, b"208", {})
            result = m.probe(config, self.stats, self.initial, 1, tip_upstream + "/blockheight", 6)
            self.assertIn("source_lag", result["reasons"])
            self.stats.write_text(json.dumps(dict(self.initial, errors=1)))
            self.assertIn("reader_fetch_errors", m.probe(config, self.stats, self.initial, 1)["reasons"])
            self.stats.write_text(json.dumps(dict(self.initial, run_id="replacement")))
            self.assertIn("reader_restarted", m.probe(config, self.stats, self.initial, 1)["reasons"])
            source.responses["/status"] = (200, json.dumps(status(unrecoverably_reorged=True)).encode(), {})
            self.assertFalse(m.probe(config, self.stats, self.initial, 1)["healthy"])

    def test_watch_is_read_only_by_default_even_when_unhealthy(self):
        before = self.config.read_bytes()
        with patch.object(m, "probe", return_value=dict(healthy=False, reasons=["source_slow"])):
            with patch.object(m, "systemd_cpu") as cpu, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(m.watch(self.arguments()), 2)
        cpu.assert_not_called()
        self.assertEqual(self.config.read_bytes(), before)

    def test_apply_rolls_back_rate_and_cpu_without_rewinding_height(self):
        with patch.object(m, "probe", return_value=dict(healthy=False, reasons=["source_slow"])):
            with patch.object(m, "systemd_cpu", side_effect=[100, None]) as cpu:
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(m.watch(self.arguments(apply=True)), 0)
        self.assertEqual(m.read_config(self.config)["requests_per_second"], 10)
        self.assertEqual(m.read_config(self.config)["height_limit"], 100)
        self.assertEqual(cpu.call_args_list[-1], unittest.mock.call(50))

    def test_consecutive_failures_reset_and_preserve_a_newer_height(self):
        count = [0]
        def probe(*args):
            count[0] += 1
            if count[0] == 2:
                m.change_config(self.config, height=150, apply=True)
            return dict(healthy=count[0] == 2, reasons=[])
        with patch.object(m, "probe", side_effect=probe), patch.object(m.time, "sleep"):
            with patch.object(m, "systemd_cpu", side_effect=[100, None]), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(m.watch(self.arguments(apply=True, once=False, bad_samples=2)), 0)
        self.assertEqual(count[0], 4)
        self.assertEqual(m.read_config(self.config)["height_limit"], 150)
        self.assertEqual(m.read_config(self.config)["requests_per_second"], 10)


if __name__ == "__main__":
    unittest.main()
