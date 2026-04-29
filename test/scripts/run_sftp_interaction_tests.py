#!/usr/bin/env python3
import argparse
import hashlib
import logging
import os
import queue
import re
import signal
import shutil
import socket
import subprocess
import tempfile
import threading
import textwrap
import time
import unittest
from pathlib import Path

import paramiko

from sftp_test_server_lib import (
    SFTPServerConfig,
    SFTPTestServer,
    load_or_create_host_key,
    write_known_host,
    write_known_hosts,
)

PARSED_ARGS: argparse.Namespace | None = None
logging.getLogger("paramiko").setLevel(logging.CRITICAL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SFTP interaction tests")
    parser.add_argument(
        "--duckdb-bin",
        default="./build/release/duckdb",
        help="DuckDB binary to use (default: ./build/release/duckdb)",
    )
    return parser.parse_args()


def normalize_host_binary_path(project_root: Path, path_text: str) -> str:
    if os.name == "nt":
        match = re.match(r"^/([a-zA-Z])/(.*)$", path_text)
        if match:
            drive = match.group(1).upper()
            rest = match.group(2).replace("/", "\\")
            path_text = f"{drive}:\\{rest}"

    path = Path(path_text)
    resolved = path.resolve() if path.is_absolute() else (project_root / path_text).resolve()
    if os.name == "nt" and not resolved.exists() and resolved.suffix.lower() != ".exe":
        exe_path = resolved.with_suffix(".exe")
        if exe_path.exists():
            resolved = exe_path
    return str(resolved)


class DuckDBResult:
    def __init__(self, completed: subprocess.CompletedProcess[str]):
        self.returncode = completed.returncode
        self.stdout = completed.stdout
        self.stderr = completed.stderr
        self.output = (completed.stdout or "") + (completed.stderr or "")


class SFTPInteractionTests(unittest.TestCase):
    SUBPROCESS_TIMEOUT_SECONDS = 20
    INTERRUPT_AFTER_SECONDS = 1.0
    INTERRUPT_FINISH_TIMEOUT_SECONDS = 5.0
    TEST_HANG_DELAY_MS = 10_000

    @classmethod
    def setUpClass(cls) -> None:
        if PARSED_ARGS is None:
            raise RuntimeError("Arguments must be parsed before running tests")
        args = PARSED_ARGS
        cls.project_root = Path(__file__).resolve().parents[2]
        cls.duckdb_bin = normalize_host_binary_path(cls.project_root, args.duckdb_bin)
        cls.data_dir = str((cls.project_root / "test/data").resolve())
        cls.tempdir = Path(tempfile.mkdtemp(prefix="h5db_sftp_interaction_"))
        cls.mutable_root = cls.tempdir / "mutable_root"
        cls.mutable_root.mkdir()
        cls.mutable_file = cls.mutable_root / "mutable.h5"

        cls.rsa_host_key = load_or_create_host_key(cls.tempdir / "rsa_hostkey", "rsa")
        cls.ecdsa_host_key = load_or_create_host_key(cls.tempdir / "ecdsa_hostkey", "ecdsa")

        cls.client_key_path = cls.tempdir / "client_rsa"
        client_key = paramiko.RSAKey.generate(2048)
        client_key.write_private_key_file(str(cls.client_key_path))
        cls.encrypted_client_key_path = cls.tempdir / "client_rsa_encrypted"
        client_key.write_private_key_file(str(cls.encrypted_client_key_path), password="secretpass")
        cls.client_public_blob = client_key.asbytes()
        cls.wrong_client_key_path = cls.tempdir / "wrong_client_rsa"
        wrong_client_key = paramiko.RSAKey.generate(2048)
        wrong_client_key.write_private_key_file(str(cls.wrong_client_key_path))

        cls.password_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(root_dir=cls.data_dir, username="h5db", password="h5db", host_keys=[cls.rsa_host_key]),
        )
        cls.password_server.start()

        cls.delayed_auth_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(
                root_dir=cls.data_dir,
                username="h5db",
                password="h5db",
                auth_delay_ms=300,
                host_keys=[cls.rsa_host_key],
            ),
        )
        cls.delayed_auth_server.start()

        cls.hanging_auth_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(
                root_dir=cls.data_dir,
                username="h5db",
                password="h5db",
                auth_delay_ms=cls.TEST_HANG_DELAY_MS,
                host_keys=[cls.rsa_host_key],
            ),
        )
        cls.hanging_auth_server.start()

        cls.key_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(
                root_dir=cls.data_dir,
                username="h5db",
                password=None,
                allowed_public_key_blobs={cls.client_public_blob},
                host_keys=[cls.rsa_host_key],
            ),
        )
        cls.key_server.start()

        cls.multi_key_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(
                root_dir=cls.data_dir,
                username="h5db",
                password="h5db",
                host_keys=[cls.rsa_host_key, cls.ecdsa_host_key],
            ),
        )
        cls.multi_key_server.start()

        cls.flaky_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(
                root_dir=cls.data_dir,
                username="h5db",
                password="h5db",
                host_keys=[cls.rsa_host_key],
                disconnect_after_read_calls=3,
            ),
        )
        cls.flaky_server.start()

        cls.hanging_cleanup_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(
                root_dir=cls.data_dir,
                username="h5db",
                password="h5db",
                handle_close_delay_ms=cls.TEST_HANG_DELAY_MS,
                host_keys=[cls.rsa_host_key],
            ),
        )
        cls.hanging_cleanup_server.start()

        cls.password_known_hosts = cls.tempdir / "password_known_hosts"
        cls.delayed_auth_known_hosts = cls.tempdir / "delayed_auth_known_hosts"
        cls.hanging_auth_known_hosts = cls.tempdir / "hanging_auth_known_hosts"
        cls.key_known_hosts = cls.tempdir / "key_known_hosts"
        cls.ipv6_known_hosts = cls.tempdir / "ipv6_known_hosts"
        cls.multi_key_rsa_known_hosts = cls.tempdir / "multi_key_rsa_known_hosts"
        cls.multi_key_all_known_hosts = cls.tempdir / "multi_key_all_known_hosts"
        cls.flaky_known_hosts = cls.tempdir / "flaky_known_hosts"
        cls.hanging_cleanup_known_hosts = cls.tempdir / "hanging_cleanup_known_hosts"
        cls.disconnect_on_stat_known_hosts = cls.tempdir / "disconnect_on_stat_known_hosts"
        cls.password_host_key_fingerprint = hashlib.sha1(cls.rsa_host_key.asbytes()).hexdigest()

        write_known_host(cls.password_known_hosts, "127.0.0.1", cls.password_server.port, cls.rsa_host_key)
        write_known_host(cls.delayed_auth_known_hosts, "127.0.0.1", cls.delayed_auth_server.port, cls.rsa_host_key)
        write_known_host(cls.hanging_auth_known_hosts, "127.0.0.1", cls.hanging_auth_server.port, cls.rsa_host_key)
        write_known_host(cls.key_known_hosts, "127.0.0.1", cls.key_server.port, cls.rsa_host_key)
        write_known_host(cls.multi_key_rsa_known_hosts, "127.0.0.1", cls.multi_key_server.port, cls.rsa_host_key)
        write_known_hosts(
            cls.multi_key_all_known_hosts,
            "127.0.0.1",
            cls.multi_key_server.port,
            [cls.rsa_host_key, cls.ecdsa_host_key],
        )
        write_known_host(cls.flaky_known_hosts, "127.0.0.1", cls.flaky_server.port, cls.rsa_host_key)
        write_known_host(
            cls.hanging_cleanup_known_hosts,
            "127.0.0.1",
            cls.hanging_cleanup_server.port,
            cls.rsa_host_key,
        )

        cls.ipv6_server = None
        if socket.has_ipv6:
            try:
                cls.ipv6_server = SFTPTestServer(
                    "::1",
                    0,
                    SFTPServerConfig(
                        root_dir=cls.data_dir,
                        username="h5db",
                        password="h5db",
                        host_keys=[cls.rsa_host_key],
                    ),
                )
                cls.ipv6_server.start()
                write_known_host(cls.ipv6_known_hosts, "::1", cls.ipv6_server.port, cls.rsa_host_key)
            except OSError:
                cls.ipv6_server = None

        cls.disconnect_on_stat_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(
                root_dir=cls.data_dir,
                username="h5db",
                password="h5db",
                host_keys=[cls.rsa_host_key],
                disconnect_on_stat=True,
            ),
        )
        cls.disconnect_on_stat_server.start()
        write_known_host(
            cls.disconnect_on_stat_known_hosts,
            "127.0.0.1",
            cls.disconnect_on_stat_server.port,
            cls.rsa_host_key,
        )

        cls.mutable_known_hosts = cls.tempdir / "mutable_known_hosts"
        cls.mutable_server = SFTPTestServer(
            "127.0.0.1",
            0,
            SFTPServerConfig(
                root_dir=str(cls.mutable_root),
                username="h5db",
                password="h5db",
                host_keys=[cls.rsa_host_key],
            ),
        )
        cls.mutable_server.start()
        write_known_host(
            cls.mutable_known_hosts,
            "127.0.0.1",
            cls.mutable_server.port,
            cls.rsa_host_key,
        )

        cls.symlink_known_hosts = cls.tempdir / "symlink_known_hosts"
        cls.symlink_server = None
        try:
            cls.symlink_root = cls.tempdir / "symlink_root"
            real_dir = cls.symlink_root / "real"
            real_dir.mkdir(parents=True)
            shutil.copyfile(Path(cls.data_dir) / "glob" / "glob_same_1.h5", real_dir / "nested.h5")
            shutil.copyfile(Path(cls.data_dir) / "glob" / "glob_same_2.h5", cls.symlink_root / "root_file.h5")
            os.symlink("real/nested.h5", cls.symlink_root / "link_file.h5")
            os.symlink("real", cls.symlink_root / "link_dir", target_is_directory=True)
            cls.symlink_server = SFTPTestServer(
                "127.0.0.1",
                0,
                SFTPServerConfig(
                    root_dir=str(cls.symlink_root),
                    username="h5db",
                    password="h5db",
                    host_keys=[cls.rsa_host_key],
                ),
            )
            cls.symlink_server.start()
            write_known_host(
                cls.symlink_known_hosts,
                "127.0.0.1",
                cls.symlink_server.port,
                cls.rsa_host_key,
            )
        except (OSError, NotImplementedError):
            cls.symlink_server = None

    @classmethod
    def tearDownClass(cls) -> None:
        for server in (
            cls.password_server,
            cls.delayed_auth_server,
            cls.hanging_auth_server,
            cls.key_server,
            cls.multi_key_server,
            cls.flaky_server,
            cls.hanging_cleanup_server,
            cls.ipv6_server,
            cls.disconnect_on_stat_server,
            cls.mutable_server,
            cls.symlink_server,
        ):
            if server is None:
                continue
            server.stop()
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def setUp(self) -> None:
        for server in (
            self.password_server,
            self.delayed_auth_server,
            self.hanging_auth_server,
            self.key_server,
            self.multi_key_server,
            self.flaky_server,
            self.hanging_cleanup_server,
            self.ipv6_server,
            self.disconnect_on_stat_server,
            self.mutable_server,
            self.symlink_server,
        ):
            if server is None:
                continue
            server.telemetry.reset()

    def run_sql(self, sql: str, env: dict[str, str] | None = None) -> DuckDBResult:
        try:
            completed = subprocess.run(
                [self.duckdb_bin, "-csv", "-noheader", "-unsigned", "-c", sql],
                cwd=self.project_root,
                env=env,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=self.SUBPROCESS_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as ex:
            output = ((ex.stdout or "") + (ex.stderr or "")).strip()
            raise AssertionError(
                f"DuckDB subprocess timed out after {self.SUBPROCESS_TIMEOUT_SECONDS}s.\n"
                f"SQL:\n{sql}\n"
                f"Partial output:\n{output}"
            ) from ex
        return DuckDBResult(completed)

    def send_process_interrupt(self, process: subprocess.Popen[str]) -> None:
        if os.name == "nt":
            ctrl_break = getattr(signal, "CTRL_BREAK_EVENT", None)
            try:
                process.send_signal(ctrl_break if ctrl_break is not None else signal.SIGINT)
            except OSError:
                pass
            return
        try:
            os.killpg(process.pid, signal.SIGINT)
        except ProcessLookupError:
            pass

    def kill_process(self, process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        if os.name == "nt":
            try:
                process.kill()
            except OSError:
                pass
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            try:
                process.kill()
            except OSError:
                pass

    def run_sql_with_interrupt(
        self, sql: str, before_interrupt=None, env: dict[str, str] | None = None
    ) -> DuckDBResult:
        popen_kwargs = {
            "cwd": self.project_root,
            "env": env,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
        }
        if os.name == "nt":
            popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            popen_kwargs["start_new_session"] = True

        process = subprocess.Popen(
            [self.duckdb_bin, "-csv", "-noheader", "-unsigned", "-c", sql],
            **popen_kwargs,
        )
        output_before_interrupt_error = None
        try:
            try:
                stdout, stderr = process.communicate(timeout=self.INTERRUPT_AFTER_SECONDS)
            except subprocess.TimeoutExpired:
                if before_interrupt is not None:
                    output_before_interrupt_error = before_interrupt()
                self.send_process_interrupt(process)
                try:
                    stdout, stderr = process.communicate(timeout=self.INTERRUPT_FINISH_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired as ex:
                    self.kill_process(process)
                    stdout, stderr = process.communicate()
                    raise AssertionError(
                        "DuckDB subprocess did not exit within "
                        f"{self.INTERRUPT_FINISH_TIMEOUT_SECONDS:.1f}s after interrupt.\n"
                        f"SQL:\n{sql}\n"
                        f"Partial output:\n{((stdout or '') + (stderr or '')).strip()}"
                    ) from ex
            else:
                raise AssertionError(
                    "DuckDB subprocess finished before the interrupt was sent.\n"
                    f"SQL:\n{sql}\n"
                    f"Output:\n{((stdout or '') + (stderr or '')).strip()}"
                )
        except Exception:
            self.kill_process(process)
            raise

        completed = subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)
        if output_before_interrupt_error is not None:
            raise AssertionError(
                output_before_interrupt_error
                + "\nOutput:\n"
                + ((completed.stdout or "") + (completed.stderr or "")).strip()
            )
        return DuckDBResult(completed)

    def assertOutputContains(self, result: DuckDBResult, needle: str) -> None:
        self.assertIn(needle, result.output, msg=result.output)

    def numeric_stdout_lines(self, result: DuckDBResult) -> list[str]:
        return [line.strip() for line in result.stdout.splitlines() if line.strip().isdigit()]

    def query_stdout_lines(self, result: DuckDBResult) -> list[str]:
        lines = []
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if stripped and stripped.lower() != "true":
                lines.append(stripped)
        return lines

    def assertNumericOutput(self, result: DuckDBResult, expected: list[str]) -> None:
        self.assertEqual(self.numeric_stdout_lines(result), expected, msg=result.output)

    def assertCsvOutput(self, result: DuckDBResult, expected: list[str]) -> None:
        self.assertEqual(self.query_stdout_lines(result), expected, msg=result.output)

    def assertGracefulSftpCleanup(
        self,
        server: SFTPTestServer,
        expected_connections: int,
        require_explicit_handle_closes: bool = False,
    ) -> None:
        deadline = time.monotonic() + 2.0
        last_state = ""
        while time.monotonic() < deadline:
            connections, _ = server.telemetry.snapshot()
            channel_eofs, channel_closes, transport_disconnects = server.telemetry.graceful_cleanup_snapshot()
            handle_open_calls, handle_close_calls, handle_close_requests, current_open_handles, max_open_handles = (
                server.telemetry.handle_snapshot()
            )
            last_state = (
                f"connections={len(connections)}, "
                f"per_connection_cleanup="
                f"{[(r.channel_eof_messages, r.channel_close_messages, r.transport_disconnect_messages) for r in connections]}, "
                f"channel_eofs={channel_eofs}, channel_closes={channel_closes}, "
                f"transport_disconnects={transport_disconnects}, "
                f"handle_open_calls={handle_open_calls}, handle_close_calls={handle_close_calls}, "
                f"handle_close_requests={handle_close_requests}, current_open_handles={current_open_handles}, "
                f"max_open_handles={max_open_handles}"
            )
            connections_clean = (
                len(connections) == expected_connections
                and all(record.channel_eof_messages > 0 for record in connections)
                and all(record.channel_close_messages > 0 for record in connections)
                and all(record.transport_disconnect_messages > 0 for record in connections)
                and channel_eofs >= expected_connections
                and channel_closes >= expected_connections
                and transport_disconnects >= expected_connections
            )
            handles_clean = not require_explicit_handle_closes or (
                handle_open_calls > 0
                and handle_open_calls == handle_close_requests
                and handle_open_calls == handle_close_calls
                and current_open_handles == 0
            )
            if connections_clean and handles_clean:
                return
            time.sleep(0.02)
        self.fail(f"SFTP server did not observe graceful cleanup: {last_state}")

    def start_ssh_agent(self) -> dict[str, str]:
        if os.name == "nt":
            self.skipTest("ssh-agent interaction test is currently Unix-only")
        if shutil.which("ssh-agent") is None or shutil.which("ssh-add") is None:
            self.skipTest("ssh-agent and ssh-add are required for SSH agent interaction tests")

        completed = subprocess.run(
            ["ssh-agent", "-s"],
            cwd=self.project_root,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=self.SUBPROCESS_TIMEOUT_SECONDS,
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(f"ssh-agent failed:\n{completed.stdout}{completed.stderr}")

        output = completed.stdout + completed.stderr
        sock_match = re.search(r"SSH_AUTH_SOCK=([^;]+);", output)
        pid_match = re.search(r"SSH_AGENT_PID=([0-9]+);", output)
        if not sock_match or not pid_match:
            raise AssertionError(f"Unable to parse ssh-agent environment:\n{output}")

        agent_env = os.environ.copy()
        agent_env["SSH_AUTH_SOCK"] = sock_match.group(1)
        agent_env["SSH_AGENT_PID"] = pid_match.group(1)
        return agent_env

    def stop_ssh_agent(self, agent_env: dict[str, str]) -> None:
        subprocess.run(
            ["ssh-agent", "-k"],
            cwd=self.project_root,
            env=agent_env,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=self.SUBPROCESS_TIMEOUT_SECONDS,
            check=False,
        )

    def add_ssh_agent_identity(self, agent_env: dict[str, str], key_path: Path) -> None:
        completed = subprocess.run(
            ["ssh-add", str(key_path)],
            cwd=self.project_root,
            env=agent_env,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=self.SUBPROCESS_TIMEOUT_SECONDS,
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(f"ssh-add failed:\n{completed.stdout}{completed.stderr}")

    def clear_ssh_agent_identities(self, agent_env: dict[str, str]) -> None:
        completed = subprocess.run(
            ["ssh-add", "-D"],
            cwd=self.project_root,
            env=agent_env,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=self.SUBPROCESS_TIMEOUT_SECONDS,
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(f"ssh-add -D failed:\n{completed.stdout}{completed.stderr}")

    def replace_mutable_fixture(self, fixture_name: str, mtime_seconds: int) -> None:
        temp_path = self.mutable_root / "mutable.tmp"
        shutil.copyfile(self.project_root / "test/data" / fixture_name, temp_path)
        os.utime(temp_path, (mtime_seconds, mtime_seconds))
        os.replace(temp_path, self.mutable_file)

    def test_wrong_password_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET bad_password (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'wrong-password',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.password_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "SSH password authentication failed for 'h5db'")

    def test_wrong_username_password_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET bad_password_username (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'wrong-user',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.password_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "SSH password authentication failed for 'wrong-user'")

    def test_delayed_password_auth_uses_interruptible_remote_wait(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET delayed_password (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.delayed_auth_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.delayed_auth_known_hosts}',
                PORT {self.delayed_auth_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.delayed_auth_server.port}/simple.h5', '/');
            """
        ).strip()
        start = time.monotonic()
        result = self.run_sql(sql)
        elapsed = time.monotonic() - start
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertNumericOutput(result, ["5"])
        self.assertGreaterEqual(
            elapsed,
            0.2,
            msg=f"delayed auth query returned before the server-side auth delay was exercised: elapsed={elapsed:.3f}s",
        )
        self.assertLess(
            elapsed,
            5.0,
            msg=f"delayed auth query did not return promptly after the auth delay: elapsed={elapsed:.3f}s",
        )
        connections, _ = self.delayed_auth_server.telemetry.snapshot()
        self.assertTrue(any(record.auth_method == "password" for record in connections))
        self.assertGracefulSftpCleanup(self.delayed_auth_server, expected_connections=1)

    def test_hanging_password_auth_remains_interruptible(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET hanging_password (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.hanging_auth_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.hanging_auth_known_hosts}',
                PORT {self.hanging_auth_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.hanging_auth_server.port}/simple.h5', '/');
            """
        ).strip()

        def auth_wait_started() -> str | None:
            connections, _ = self.hanging_auth_server.telemetry.snapshot()
            if not any(record.password_attempts > 0 for record in connections):
                return "Hanging-auth server did not receive a password auth request before the interrupt was sent."
            return None

        result = self.run_sql_with_interrupt(sql, before_interrupt=auth_wait_started)
        self.assertNotIn("5", result.stdout)

    def test_hanging_sftp_cleanup_remains_interruptible(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET hanging_cleanup (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.hanging_cleanup_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.hanging_cleanup_known_hosts}',
                PORT {self.hanging_cleanup_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.hanging_cleanup_server.port}/simple.h5', '/');
            """
        ).strip()

        def cleanup_wait_started() -> str | None:
            _, _, handle_close_requests, _, _ = self.hanging_cleanup_server.telemetry.handle_snapshot()
            if handle_close_requests == 0:
                return "Hanging-cleanup server did not observe an SFTP handle-close request before the interrupt."
            return None

        start = time.monotonic()
        self.run_sql_with_interrupt(sql, before_interrupt=cleanup_wait_started)
        elapsed = time.monotonic() - start
        self.assertLess(
            elapsed,
            self.INTERRUPT_AFTER_SECONDS + 3.0,
            msg=f"hanging cleanup did not return promptly after interrupt: elapsed={elapsed:.3f}s",
        )
        _, _, handle_close_requests, _, _ = self.hanging_cleanup_server.telemetry.handle_snapshot()
        self.assertGreater(handle_close_requests, 0)

    def test_public_key_auth_success(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET key_auth (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'h5db',
                KEY_PATH '{self.client_key_path}',
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertNumericOutput(result, ["5"])
        connections, _ = self.key_server.telemetry.snapshot()
        self.assertTrue(any(record.auth_method == "publickey" for record in connections))
        self.assertGracefulSftpCleanup(self.key_server, expected_connections=1)

    def test_encrypted_private_key_auth_success(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET encrypted_key_auth (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'h5db',
                KEY_PATH '{self.encrypted_client_key_path}',
                KEY_PASSPHRASE 'secretpass',
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertNumericOutput(result, ["5"])
        connections, _ = self.key_server.telemetry.snapshot()
        self.assertTrue(any(record.auth_method == "publickey" for record in connections))

    def test_ssh_agent_auth_success(self) -> None:
        agent_env = self.start_ssh_agent()
        try:
            self.add_ssh_agent_identity(agent_env, self.client_key_path)
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET agent_auth (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                    USERNAME 'h5db',
                    USE_AGENT true,
                    KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                    PORT {self.key_server.port}
                );
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
                """
            ).strip()
            result = self.run_sql(sql, env=agent_env)
            self.assertEqual(result.returncode, 0, msg=result.output)
            self.assertNumericOutput(result, ["5"])
            connections, _ = self.key_server.telemetry.snapshot()
            self.assertTrue(any(record.auth_method == "publickey" for record in connections))
        finally:
            self.stop_ssh_agent(agent_env)

    def test_missing_ssh_agent_error(self) -> None:
        if os.name == "nt":
            self.skipTest("SSH_AUTH_SOCK-specific agent error test is currently Unix-only")
        env = os.environ.copy()
        env.pop("SSH_AUTH_SOCK", None)
        env.pop("SSH_AGENT_PID", None)
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET missing_agent_auth (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'h5db',
                USE_AGENT true,
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql, env=env)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "SSH agent authentication setup failed for 'h5db'")

    def test_ssh_agent_with_no_identities_error(self) -> None:
        agent_env = self.start_ssh_agent()
        try:
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET empty_agent_auth (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                    USERNAME 'h5db',
                    USE_AGENT true,
                    KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                    PORT {self.key_server.port}
                );
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
                """
            ).strip()
            result = self.run_sql(sql, env=agent_env)
            self.assertNotEqual(result.returncode, 0, msg=result.output)
            self.assertOutputContains(result, "SSH agent authentication failed for 'h5db': agent has no identities")
        finally:
            self.stop_ssh_agent(agent_env)

    def test_ssh_agent_can_skip_rejected_identity_and_use_later_one(self) -> None:
        agent_env = self.start_ssh_agent()
        try:
            self.add_ssh_agent_identity(agent_env, self.wrong_client_key_path)
            self.add_ssh_agent_identity(agent_env, self.client_key_path)
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET agent_auth_two_identities (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                    USERNAME 'h5db',
                    USE_AGENT true,
                    KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                    PORT {self.key_server.port}
                );
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
                """
            ).strip()
            result = self.run_sql(sql, env=agent_env)
            self.assertEqual(result.returncode, 0, msg=result.output)
            self.assertNumericOutput(result, ["5"])
            connections, _ = self.key_server.telemetry.snapshot()
            self.assertTrue(any(record.auth_method == "publickey" for record in connections))
            self.assertTrue(any(record.publickey_attempts >= 2 for record in connections))
        finally:
            self.clear_ssh_agent_identities(agent_env)
            self.stop_ssh_agent(agent_env)

    def test_ssh_agent_all_loaded_identities_can_fail(self) -> None:
        agent_env = self.start_ssh_agent()
        try:
            self.add_ssh_agent_identity(agent_env, self.wrong_client_key_path)
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET wrong_agent_auth (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                    USERNAME 'h5db',
                    USE_AGENT true,
                    KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                    PORT {self.key_server.port}
                );
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
                """
            ).strip()
            result = self.run_sql(sql, env=agent_env)
            self.assertNotEqual(result.returncode, 0, msg=result.output)
            self.assertOutputContains(result, "SSH agent authentication failed for 'h5db':")
        finally:
            self.clear_ssh_agent_identities(agent_env)
            self.stop_ssh_agent(agent_env)

    def test_wrong_key_passphrase_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET wrong_key_passphrase (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'h5db',
                KEY_PATH '{self.encrypted_client_key_path}',
                KEY_PASSPHRASE 'wrongpass',
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "SSH public key authentication failed for 'h5db'")

    def test_missing_key_file_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET missing_key_file (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'h5db',
                KEY_PATH '{self.tempdir / "does_not_exist"}',
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "SSH public key authentication failed for 'h5db'")

    def test_wrong_public_key_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET wrong_key_auth (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'h5db',
                KEY_PATH '{self.wrong_client_key_path}',
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "SSH public key authentication failed for 'h5db'")

    def test_wrong_username_public_key_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET wrong_key_username (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'wrong-user',
                KEY_PATH '{self.client_key_path}',
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "SSH public key authentication failed for 'wrong-user'")

    def test_missing_known_hosts_file_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET missing_known_hosts (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.tempdir / "missing_known_hosts"}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.password_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "Failed to read known_hosts file")

    def test_host_key_fingerprint_mismatch_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET wrong_fingerprint (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                HOST_KEY_FINGERPRINT '0000000000000000000000000000000000000000',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.password_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(
            result,
            f"SSH host key fingerprint mismatch for '127.0.0.1:{self.password_server.port}'",
        )

    def test_host_key_fingerprint_only_success(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET fingerprint_only (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                HOST_KEY_FINGERPRINT '{self.password_host_key_fingerprint}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.password_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertNumericOutput(result, ["5"])

    def test_url_username_mismatch_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET mismatch_username (
                TYPE sftp,
                SCOPE 'sftp://',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://other-user@127.0.0.1:{self.password_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, "does not match secret username 'h5db'")

    def test_url_port_mismatch_error(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET mismatch_port (
                TYPE sftp,
                SCOPE 'sftp://',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.password_server.port + 1}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertOutputContains(result, f"does not match secret port '{self.password_server.port}'")

    def test_host_key_algorithms_select_requested_host_key(self) -> None:
        ecdsa_sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET host_key_algos_ecdsa (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.multi_key_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.multi_key_all_known_hosts}',
                PORT {self.multi_key_server.port},
                HOST_KEY_ALGORITHMS 'ecdsa-sha2-nistp256'
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(ecdsa_sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        connections, _ = self.multi_key_server.telemetry.snapshot()
        negotiated = [record.negotiated_host_key for record in connections if record.negotiated_host_key]
        self.assertEqual(negotiated, ["ecdsa-sha2-nistp256"], msg=str(negotiated))

        self.multi_key_server.telemetry.reset()
        rsa_sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET host_key_algos_rsa (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.multi_key_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.multi_key_all_known_hosts}',
                PORT {self.multi_key_server.port},
                HOST_KEY_ALGORITHMS 'ssh-rsa'
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(rsa_sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        connections, _ = self.multi_key_server.telemetry.snapshot()
        negotiated = [record.negotiated_host_key for record in connections if record.negotiated_host_key]
        self.assertEqual(negotiated, ["ssh-rsa"], msg=str(negotiated))

    def test_host_key_algorithms_multi_entry_order_is_respected(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET host_key_algos_multi (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.multi_key_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.multi_key_all_known_hosts}',
                PORT {self.multi_key_server.port},
                HOST_KEY_ALGORITHMS 'ssh-rsa,ecdsa-sha2-nistp256'
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        connections, _ = self.multi_key_server.telemetry.snapshot()
        negotiated = [record.negotiated_host_key for record in connections if record.negotiated_host_key]
        self.assertEqual(negotiated, ["ssh-rsa"], msg=str(negotiated))

    def test_invalid_host_key_algorithms_fall_back(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET bogus_host_key_algos (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.multi_key_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.multi_key_rsa_known_hosts}',
                PORT {self.multi_key_server.port},
                HOST_KEY_ALGORITHMS 'bogus'
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertNumericOutput(result, ["5"])
        connections, _ = self.multi_key_server.telemetry.snapshot()
        negotiated = [record.negotiated_host_key for record in connections if record.negotiated_host_key]
        self.assertIn("ssh-rsa", negotiated, msg=str(negotiated))

    def test_repeated_metadata_query_uses_external_cache(self) -> None:
        url = f"sftp://127.0.0.1:{self.password_server.port}/simple.h5"
        baseline_sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET metadata_cache_baseline (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_tree('{url}');
            """
        ).strip()
        baseline_result = self.run_sql(baseline_sql)
        self.assertEqual(baseline_result.returncode, 0, msg=baseline_result.output)
        baseline_numeric_lines = [
            line.strip() for line in baseline_result.stdout.splitlines() if line.strip().isdigit()
        ]
        self.assertEqual(baseline_numeric_lines, ["10"], msg=baseline_result.output)
        _, baseline_read_calls = self.password_server.telemetry.snapshot()
        self.assertGreater(baseline_read_calls, 0)

        self.password_server.telemetry.reset()

        repeated_sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET metadata_cache_repeated (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_tree('{url}');
            SELECT COALESCE(SUM(nr_bytes), 0)
            FROM duckdb_external_file_cache()
            WHERE path = '{url}';
            SELECT COUNT(*) FROM h5_tree('{url}');
            """
        ).strip()
        result = self.run_sql(repeated_sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        numeric_lines = [line.strip() for line in result.stdout.splitlines() if line.strip().isdigit()]
        self.assertEqual(len(numeric_lines), 3, msg=result.output)
        self.assertEqual(numeric_lines[0], "10", msg=result.output)
        self.assertEqual(numeric_lines[2], "10", msg=result.output)
        self.assertGreater(int(numeric_lines[1]), 0, msg=result.output)
        _, repeated_read_calls = self.password_server.telemetry.snapshot()
        self.assertEqual(repeated_read_calls, baseline_read_calls)

    def test_missing_mtime_metadata_query_uses_external_cache(self) -> None:
        url = f"sftp://127.0.0.1:{self.password_server.port}/simple.h5"
        self.password_server.config.stat_omit_mtime = True
        try:
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                PRAGMA enable_external_file_cache=true;
                SET validate_external_file_cache='VALIDATE_REMOTE';
                CREATE OR REPLACE TEMPORARY SECRET metadata_cache_missing_mtime (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                    USERNAME 'h5db',
                    PASSWORD 'h5db',
                    KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                    PORT {self.password_server.port}
                );
                SELECT COUNT(*) FROM h5_tree('{url}');
                SELECT COALESCE(SUM(nr_bytes), 0)
                FROM duckdb_external_file_cache()
                WHERE path = '{url}';
                """
            ).strip()
            result = self.run_sql(sql)
            self.assertEqual(result.returncode, 0, msg=result.output)
            numeric_lines = [line.strip() for line in result.stdout.splitlines() if line.strip().isdigit()]
            self.assertEqual(len(numeric_lines), 2, msg=result.output)
            self.assertEqual(numeric_lines[0], "10", msg=result.output)
            self.assertGreater(int(numeric_lines[1]), 0, msg=result.output)
        finally:
            self.password_server.config.stat_omit_mtime = False

    def test_no_validation_warm_cache_avoids_second_sftp_connection(self) -> None:
        url = f"sftp://127.0.0.1:{self.password_server.port}/simple.h5"
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            PRAGMA enable_external_file_cache=true;
            SET validate_external_file_cache='NO_VALIDATION';
            CREATE OR REPLACE TEMPORARY SECRET metadata_cache_no_validation (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_tree('{url}');
            SELECT COUNT(*) FROM h5_tree('{url}');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        numeric_lines = [line.strip() for line in result.stdout.splitlines() if line.strip().isdigit()]
        self.assertEqual(numeric_lines, ["10", "10"], msg=result.output)
        connections, read_calls = self.password_server.telemetry.snapshot()
        self.assertEqual(
            len(connections), 1, msg=str([(record.auth_method, record.read_calls) for record in connections])
        )
        self.assertGreater(read_calls, 0)

    def test_single_query_reuses_sftp_connection_across_h5_tree_and_h5_ls(self) -> None:
        url = f"sftp://127.0.0.1:{self.password_server.port}/simple.h5"
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET query_local_reuse_same_file (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT
                (SELECT COUNT(*) FROM h5_tree('{url}')) AS tree_cnt,
                (SELECT COUNT(*) FROM h5_ls('{url}', '/')) AS ls_cnt;
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertCsvOutput(result, ["10,5"])
        connections, read_calls = self.password_server.telemetry.snapshot()
        self.assertEqual(
            len(connections), 1, msg=str([(record.auth_method, record.read_calls) for record in connections])
        )
        self.assertGreater(read_calls, 0)

    def test_single_query_reuses_sftp_connection_across_glob_listing_and_reads(self) -> None:
        pattern = f"sftp://127.0.0.1:{self.password_server.port}/glob/glob_same_*.h5"
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET query_local_glob_reuse (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT
                (SELECT COUNT(*) FROM h5_tree('{pattern}')) AS tree_cnt,
                (SELECT COUNT(*) FROM h5_ls('{pattern}', '/')) AS ls_cnt,
                (SELECT COUNT(*) FROM h5_read('{pattern}', '/values')) AS read_cnt;
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertCsvOutput(result, ["12,8,5"])
        connections, read_calls = self.password_server.telemetry.snapshot()
        self.assertEqual(
            len(connections), 1, msg=str([(record.auth_method, record.read_calls) for record in connections])
        )
        self.assertGreater(read_calls, 0)

    def test_sftp_glob_crawl_handles_missing_readdir_permissions(self) -> None:
        self.password_server.config.list_folder_omit_permissions = True
        try:
            pattern = f"sftp://127.0.0.1:{self.password_server.port}/glob/**/glob_same_*.h5"
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET readdir_permissions_fallback (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                    USERNAME 'h5db',
                    PASSWORD 'h5db',
                    KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                    PORT {self.password_server.port}
                );
                SELECT COUNT(*) FROM h5_read('{pattern}', '/values');
                """
            ).strip()
            result = self.run_sql(sql)
            self.assertEqual(result.returncode, 0, msg=result.output)
            self.assertEqual(result.stdout.strip().splitlines()[-1], "6", msg=result.output)
        finally:
            self.password_server.config.list_folder_omit_permissions = False

    def test_sftp_glob_literal_directory_component_handles_missing_stat_permissions(self) -> None:
        self.password_server.config.stat_omit_permissions = True
        try:
            pattern = f"sftp://127.0.0.1:{self.password_server.port}/glob/nested/glob_same_*.h5"
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET stat_permissions_fallback (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                    USERNAME 'h5db',
                    PASSWORD 'h5db',
                    KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                    PORT {self.password_server.port}
                );
                SELECT COUNT(*) FROM h5_read('{pattern}', '/values');
                """
            ).strip()
            result = self.run_sql(sql)
            self.assertEqual(result.returncode, 0, msg=result.output)
            self.assertEqual(result.stdout.strip().splitlines()[-1], "1", msg=result.output)
        finally:
            self.password_server.config.stat_omit_permissions = False

    def test_sftp_ipv6_exact_path_and_remote_glob_route_through_sftp_backend(self) -> None:
        if self.ipv6_server is None:
            self.skipTest("IPv6 SFTP test fixture not available on this platform")

        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET ipv6_sftp (
                TYPE sftp,
                SCOPE 'sftp://[::1]:{self.ipv6_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.ipv6_known_hosts}',
                PORT {self.ipv6_server.port}
            );
            SELECT
                (SELECT COUNT(*) FROM h5_tree('sftp://[::1]:{self.ipv6_server.port}/simple.h5')),
                (SELECT COUNT(*) FROM h5_read('sftp://[::1]:{self.ipv6_server.port}/glob/glob_same_*.h5', '/values'));
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "10,5", msg=result.output)

    def test_sftp_glob_matches_duckdb_symlink_semantics(self) -> None:
        if self.symlink_server is None:
            self.skipTest("symlink test fixture not available on this platform")

        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET sftp_symlink_glob (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.symlink_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.symlink_known_hosts}',
                PORT {self.symlink_server.port}
            );
            SELECT
                (SELECT COUNT(*) FROM h5_read('sftp://127.0.0.1:{self.symlink_server.port}/*.h5', '/values')),
                (SELECT COUNT(*) FROM h5_read('sftp://127.0.0.1:{self.symlink_server.port}/*/nested.h5', '/values')),
                (SELECT COUNT(*) FROM h5_read('sftp://127.0.0.1:{self.symlink_server.port}/**/nested.h5', '/values')),
                (SELECT COUNT(*) FROM h5_read('sftp://127.0.0.1:{self.symlink_server.port}/**/*.h5', '/values'));
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertEqual(result.stdout.strip().splitlines()[-1], "5,6,3,8", msg=result.output)

    def test_single_query_reuses_public_key_sftp_connection(self) -> None:
        url = f"sftp://127.0.0.1:{self.key_server.port}/simple.h5"
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET query_local_reuse_public_key (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'h5db',
                KEY_PATH '{self.client_key_path}',
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT
                (SELECT COUNT(*) FROM h5_tree('{url}')) AS tree_cnt,
                (SELECT COUNT(*) FROM h5_ls('{url}', '/')) AS ls_cnt;
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertCsvOutput(result, ["10,5"])
        connections, read_calls = self.key_server.telemetry.snapshot()
        self.assertEqual(
            len(connections), 1, msg=str([(record.auth_method, record.read_calls) for record in connections])
        )
        self.assertEqual(connections[0].auth_method, "publickey", msg=str(connections[0]))
        self.assertGreater(read_calls, 0)

    def test_single_query_reuses_sftp_connection_across_different_files(self) -> None:
        simple_url = f"sftp://127.0.0.1:{self.password_server.port}/simple.h5"
        with_attrs_url = f"sftp://127.0.0.1:{self.password_server.port}/with_attrs.h5"
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET query_local_reuse_different_files (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT
                (SELECT COUNT(*) FROM h5_tree('{simple_url}')) AS tree_cnt,
                (SELECT int32_attr FROM h5_attributes('{with_attrs_url}', '/dataset_with_attrs')) AS attr_value;
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertCsvOutput(result, ["10,123456"])
        connections, read_calls = self.password_server.telemetry.snapshot()
        self.assertEqual(
            len(connections), 1, msg=str([(record.auth_method, record.read_calls) for record in connections])
        )
        self.assertGreater(read_calls, 0)

    def test_single_query_with_different_sftp_configs_opens_two_connections(self) -> None:
        simple_url = f"sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5"
        with_attrs_url = f"sftp://127.0.0.1:{self.multi_key_server.port}/with_attrs.h5"
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET file_specific_rsa (
                TYPE sftp,
                SCOPE '{simple_url}',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.multi_key_all_known_hosts}',
                PORT {self.multi_key_server.port},
                HOST_KEY_ALGORITHMS 'ssh-rsa'
            );
            CREATE OR REPLACE TEMPORARY SECRET file_specific_ecdsa (
                TYPE sftp,
                SCOPE '{with_attrs_url}',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.multi_key_all_known_hosts}',
                PORT {self.multi_key_server.port},
                HOST_KEY_ALGORITHMS 'ecdsa-sha2-nistp256'
            );
            SELECT
                (SELECT COUNT(*) FROM h5_tree('{simple_url}')) AS tree_cnt,
                (SELECT int32_attr FROM h5_attributes('{with_attrs_url}', '/dataset_with_attrs')) AS attr_value;
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertCsvOutput(result, ["10,123456"])
        connections, read_calls = self.multi_key_server.telemetry.snapshot()
        self.assertEqual(
            len(connections), 2, msg=str([(record.negotiated_host_key, record.read_calls) for record in connections])
        )
        negotiated = sorted(record.negotiated_host_key for record in connections if record.negotiated_host_key)
        self.assertEqual(negotiated, ["ecdsa-sha2-nistp256", "ssh-rsa"], msg=str(negotiated))
        self.assertGreater(read_calls, 0)
        self.assertGracefulSftpCleanup(self.multi_key_server, expected_connections=2)

    def test_single_query_with_fifty_h5_calls_reuses_one_sftp_connection(self) -> None:
        simple_url = f"sftp://127.0.0.1:{self.password_server.port}/simple.h5"
        with_attrs_url = f"sftp://127.0.0.1:{self.password_server.port}/with_attrs.h5"
        query_specs = [
            ("(SELECT COUNT(*) FROM h5_tree('{simple_url}'))", 10),
            ("(SELECT COUNT(*) FROM h5_ls('{simple_url}', '/'))", 5),
            ("(SELECT COUNT(*) FROM h5_read('{simple_url}', '/integers'))", 10),
            ("(SELECT COUNT(*) FROM h5_attributes('{with_attrs_url}', '/dataset_with_attrs'))", 1),
        ]
        subqueries: list[str] = []
        expected_total = 0
        for idx in range(50):
            sql_template, expected_value = query_specs[idx % len(query_specs)]
            subqueries.append(sql_template.format(simple_url=simple_url, with_attrs_url=with_attrs_url))
            expected_total += expected_value

        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET query_local_reuse_fifty_calls (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT {' + '.join(subqueries)} AS total_calls_result;
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertNumericOutput(result, [str(expected_total)])
        connections, read_calls = self.password_server.telemetry.snapshot()
        self.assertEqual(
            len(connections), 1, msg=str([(record.auth_method, record.read_calls) for record in connections])
        )
        self.assertGreater(read_calls, 0)
        handle_open_calls, handle_close_calls, handle_close_requests, current_open_handles, max_open_handles = (
            self.password_server.telemetry.handle_snapshot()
        )
        self.assertGreater(handle_open_calls, 0)
        self.assertEqual(handle_open_calls, handle_close_requests)
        self.assertEqual(handle_open_calls, handle_close_calls)
        self.assertEqual(current_open_handles, 0)
        self.assertGreater(max_open_handles, 0)
        self.assertGracefulSftpCleanup(
            self.password_server,
            expected_connections=1,
            require_explicit_handle_closes=True,
        )

    def test_glob_directory_handles_are_closed_gracefully(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET password_auth (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.password_server.port}/glob/glob_with_attrs_*.h5');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        numeric_lines = [line.strip() for line in result.stdout.splitlines() if line.strip().isdigit()]
        self.assertEqual(len(numeric_lines), 1, msg=result.stdout)
        self.assertGreater(int(numeric_lines[0]), 0)
        self.assertGracefulSftpCleanup(self.password_server, expected_connections=1)

        deadline = time.monotonic() + 2.0
        last_state = ""
        while time.monotonic() < deadline:
            handle_open_calls, handle_close_calls, handle_close_requests, current_open_handles, max_open_handles = (
                self.password_server.telemetry.handle_snapshot()
            )
            last_state = (
                f"handle_open_calls={handle_open_calls}, handle_close_calls={handle_close_calls}, "
                f"handle_close_requests={handle_close_requests}, current_open_handles={current_open_handles}, "
                f"max_open_handles={max_open_handles}"
            )
            if (
                handle_open_calls > 0
                and handle_open_calls == handle_close_calls
                and handle_close_requests > handle_open_calls
                and current_open_handles == 0
            ):
                return
            time.sleep(0.02)
        self.fail(f"SFTP server did not observe closed glob directory handles: {last_state}")

    def test_sftp_status_failure_during_file_stat_closes_handle_gracefully(self) -> None:
        self.password_server.config.fail_stat_calls = 1
        try:
            process = subprocess.Popen(
                [self.duckdb_bin, "-csv", "-noheader", "-unsigned"],
                cwd=self.project_root,
                text=True,
                encoding="utf-8",
                errors="replace",
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.addCleanup(lambda: self.kill_process(process))
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET stat_status_failure (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                    USERNAME 'h5db',
                    PASSWORD 'h5db',
                    KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                    PORT {self.password_server.port}
                );
                SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.password_server.port}/simple.h5');
                SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.password_server.port}/simple.h5');
                .quit
                """
            ).strip()
            stdout, stderr = process.communicate(sql + "\n", timeout=self.SUBPROCESS_TIMEOUT_SECONDS)
            output = (stdout or "") + (stderr or "")
            self.assertIn("Failed to stat SFTP file", output, msg=output)
            numeric_lines = [line.strip() for line in stdout.splitlines() if line.strip().isdigit()]
            self.assertEqual(numeric_lines, ["10"], msg=output)
            handle_open_calls, handle_close_calls, handle_close_requests, current_open_handles, _ = (
                self.password_server.telemetry.handle_snapshot()
            )
            self.assertGreaterEqual(handle_open_calls, 2)
            self.assertEqual(handle_open_calls, handle_close_requests)
            self.assertEqual(handle_open_calls, handle_close_calls)
            self.assertEqual(current_open_handles, 0)
        finally:
            self.password_server.config.fail_stat_calls = 0

    def test_sftp_status_failure_during_glob_stat_closes_directory_handle(self) -> None:
        self.password_server.config.list_folder_omit_permissions = True
        self.password_server.config.fail_stat_calls = 1
        try:
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET glob_stat_status_failure (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                    USERNAME 'h5db',
                    PASSWORD 'h5db',
                    KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                    PORT {self.password_server.port}
                );
                SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.password_server.port}/glo*/glob_same_*.h5');
                """
            ).strip()
            result = self.run_sql(sql)
            self.assertNotEqual(result.returncode, 0, msg=result.output)
            self.assertIn("Failed to stat SFTP path", result.output, msg=result.output)
            handle_open_calls, _, handle_close_requests, current_open_handles, _ = (
                self.password_server.telemetry.handle_snapshot()
            )
            self.assertEqual(handle_open_calls, 0)
            self.assertGreater(handle_close_requests, 0)
            self.assertEqual(current_open_handles, 0)
        finally:
            self.password_server.config.list_folder_omit_permissions = False
            self.password_server.config.fail_stat_calls = 0

    def test_single_h5_read_list_can_span_two_sftp_remotes(self) -> None:
        password_url = f"sftp://127.0.0.1:{self.password_server.port}/simple.h5"
        key_url = f"sftp://127.0.0.1:{self.key_server.port}/simple.h5"
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET password_remote_list_read (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            CREATE OR REPLACE TEMPORARY SECRET key_remote_list_read (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.key_server.port}/',
                USERNAME 'h5db',
                KEY_PATH '{self.client_key_path}',
                KNOWN_HOSTS_PATH '{self.key_known_hosts}',
                PORT {self.key_server.port}
            );
            SELECT COUNT(*), SUM(integers)
            FROM h5_read(['{password_url}', '{key_url}'], '/integers');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertCsvOutput(result, ["20,90"])

        password_connections, password_read_calls = self.password_server.telemetry.snapshot()
        self.assertEqual(
            len(password_connections),
            1,
            msg=str([(record.auth_method, record.read_calls) for record in password_connections]),
        )
        self.assertEqual(password_connections[0].auth_method, "password", msg=str(password_connections[0]))
        self.assertGreater(password_read_calls, 0)

        key_connections, key_read_calls = self.key_server.telemetry.snapshot()
        self.assertEqual(
            len(key_connections),
            1,
            msg=str([(record.auth_method, record.read_calls) for record in key_connections]),
        )
        self.assertEqual(key_connections[0].auth_method, "publickey", msg=str(key_connections[0]))
        self.assertGreater(key_read_calls, 0)

    def test_repeated_large_scan_still_reads_uncached_suffix(self) -> None:
        url = f"sftp://127.0.0.1:{self.password_server.port}/large/large_simple.h5"
        baseline_sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET large_scan_cache_budget_baseline (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT SUM(array_2d[1]) FROM h5_read('{url}', '/array_2d');
            """
        ).strip()
        baseline_result = self.run_sql(baseline_sql)
        self.assertEqual(baseline_result.returncode, 0, msg=baseline_result.output)
        baseline_numeric_lines = [
            line.strip() for line in baseline_result.stdout.splitlines() if line.strip().isdigit()
        ]
        self.assertEqual(baseline_numeric_lines, ["199999980000000"], msg=baseline_result.output)
        _, baseline_read_calls = self.password_server.telemetry.snapshot()
        self.assertGreater(baseline_read_calls, 0)

        self.password_server.telemetry.reset()

        repeated_sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET large_scan_cache_budget_repeated (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                PORT {self.password_server.port}
            );
            SELECT SUM(array_2d[1]) FROM h5_read('{url}', '/array_2d');
            SELECT SUM(array_2d[1]) FROM h5_read('{url}', '/array_2d');
            """
        ).strip()
        result = self.run_sql(repeated_sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        numeric_lines = [line.strip() for line in result.stdout.splitlines() if line.strip().isdigit()]
        self.assertEqual(numeric_lines, ["199999980000000", "199999980000000"], msg=result.output)
        _, repeated_read_calls = self.password_server.telemetry.snapshot()
        self.assertGreater(repeated_read_calls, baseline_read_calls)

    def test_mtime_change_invalidates_external_cache(self) -> None:
        now = int(time.time())
        self.replace_mutable_fixture("simple.h5", now - 120)

        process = subprocess.Popen(
            [self.duckdb_bin, "-csv", "-noheader", "-unsigned"],
            cwd=self.project_root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        def cleanup_process() -> None:
            if process.poll() is None:
                process.kill()
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None and not stream.closed:
                    stream.close()

        self.addCleanup(cleanup_process)
        self.assertIsNotNone(process.stdin)
        self.assertIsNotNone(process.stdout)
        self.assertIsNotNone(process.stderr)

        stdout_queue: queue.Queue[str | None] = queue.Queue()
        stderr_queue: queue.Queue[str | None] = queue.Queue()
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []

        def read_stream(stream, output_queue: queue.Queue[str | None]) -> None:
            try:
                for line in stream:
                    output_queue.put(line)
            finally:
                output_queue.put(None)

        stdout_thread = threading.Thread(target=read_stream, args=(process.stdout, stdout_queue), daemon=True)
        stderr_thread = threading.Thread(target=read_stream, args=(process.stderr, stderr_queue), daemon=True)
        stdout_thread.start()
        stderr_thread.start()

        def drain_queue(output_queue: queue.Queue[str | None], lines: list[str], timeout: float = 0.0) -> bool:
            deadline = time.monotonic() + timeout
            while True:
                try:
                    if timeout <= 0:
                        line = output_queue.get_nowait()
                    else:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return False
                        line = output_queue.get(timeout=remaining)
                except queue.Empty:
                    return False
                if line is None:
                    return True
                lines.append(line)

        def read_stdout_line(timeout_seconds: float) -> str:
            deadline = time.monotonic() + timeout_seconds
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.kill_process(process)
                    drain_queue(stdout_queue, stdout_lines, 1.0)
                    drain_queue(stderr_queue, stderr_lines, 1.0)
                    self.fail(
                        "Timed out waiting for DuckDB output.\n"
                        f"stdout:\n{''.join(stdout_lines)}\n"
                        f"stderr:\n{''.join(stderr_lines)}"
                    )
                try:
                    line = stdout_queue.get(timeout=remaining)
                except queue.Empty:
                    continue
                if line is None:
                    drain_queue(stderr_queue, stderr_lines)
                    self.fail(
                        "DuckDB process exited before producing the expected query result.\n"
                        f"stdout:\n{''.join(stdout_lines)}\n"
                        f"stderr:\n{''.join(stderr_lines)}"
                    )
                stdout_lines.append(line)
                return line

        def read_next_numeric_line() -> str:
            while True:
                line = read_stdout_line(self.SUBPROCESS_TIMEOUT_SECONDS)
                stripped = line.strip()
                if stripped.isdigit():
                    return stripped

        setup_sql = textwrap.dedent(
            f"""
            LOAD h5db;
            PRAGMA enable_external_file_cache=true;
            SET validate_external_file_cache='VALIDATE_REMOTE';
            CREATE OR REPLACE TEMPORARY SECRET mutable_mtime (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.mutable_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.mutable_known_hosts}',
                PORT {self.mutable_server.port}
            );
            SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.mutable_server.port}/mutable.h5');
            """
        ).strip()
        process.stdin.write(setup_sql + "\n")
        process.stdin.flush()

        first_line = read_next_numeric_line()
        self.assertEqual(first_line, "10")
        _, first_read_calls = self.mutable_server.telemetry.snapshot()
        self.assertGreater(first_read_calls, 0)
        self.mutable_server.telemetry.reset()

        self.replace_mutable_fixture("with_attrs.h5", now - 60)

        process.stdin.write(
            textwrap.dedent(
                f"""
                SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.mutable_server.port}/mutable.h5');
                .quit
                """
            )
        )
        process.stdin.flush()
        process.stdin.close()

        try:
            process.wait(timeout=self.SUBPROCESS_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as ex:
            self.kill_process(process)
            process.wait(timeout=1.0)
            drain_queue(stdout_queue, stdout_lines, 1.0)
            drain_queue(stderr_queue, stderr_lines, 1.0)
            raise AssertionError(
                "DuckDB subprocess timed out after mutable cache invalidation query.\n"
                f"stdout:\n{''.join(stdout_lines)}\n"
                f"stderr:\n{''.join(stderr_lines)}"
            ) from ex

        drain_queue(stdout_queue, stdout_lines, 1.0)
        drain_queue(stderr_queue, stderr_lines, 1.0)
        stdout = "".join(stdout_lines)
        stderr = "".join(stderr_lines)
        self.assertEqual(process.returncode, 0, msg=stdout + stderr)
        numeric_lines = [line.strip() for line in stdout.splitlines() if line.strip().isdigit()]
        self.assertEqual(numeric_lines, ["10", "3"], msg=stdout)
        _, second_read_calls = self.mutable_server.telemetry.snapshot()
        self.assertGreater(second_read_calls, 0)

    def test_server_disconnect_mid_query(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET flaky (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.flaky_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.flaky_known_hosts}',
                PORT {self.flaky_server.port}
            );
            SELECT SUM(regular) FROM h5_read(
                'sftp://127.0.0.1:{self.flaky_server.port}/large/large_pushdown_test.h5',
                '/regular'
            );
            """
        ).strip()
        start = time.monotonic()
        result = self.run_sql(sql)
        elapsed = time.monotonic() - start
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertTrue(
            "Failed to read SFTP data" in result.output or "Unexpected EOF while reading SFTP file" in result.output,
            msg=result.output,
        )
        self.assertLess(
            elapsed,
            5.0,
            msg=f"dead connection cleanup did not return promptly: elapsed={elapsed:.3f}s",
        )
        self.assertEqual(self.flaky_server.telemetry.graceful_cleanup_snapshot(), (0, 0, 0))
        _, _, handle_close_requests, _, _ = self.flaky_server.telemetry.handle_snapshot()
        self.assertEqual(handle_close_requests, 0)

    def test_sftp_early_eof_before_advertised_size_is_file_error(self) -> None:
        self.password_server.config.read_eof_after_bytes = 1
        try:
            sql = textwrap.dedent(
                f"""
                LOAD h5db;
                CREATE OR REPLACE TEMPORARY SECRET early_eof (
                    TYPE sftp,
                    SCOPE 'sftp://127.0.0.1:{self.password_server.port}/',
                    USERNAME 'h5db',
                    PASSWORD 'h5db',
                    KNOWN_HOSTS_PATH '{self.password_known_hosts}',
                    PORT {self.password_server.port}
                );
                SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.password_server.port}/simple.h5');
                """
            ).strip()
            result = self.run_sql(sql)
            self.assertNotEqual(result.returncode, 0, msg=result.output)
            self.assertIn("Unexpected EOF while reading SFTP file", result.output, msg=result.output)
            self.assertGracefulSftpCleanup(
                self.password_server,
                expected_connections=1,
                require_explicit_handle_closes=True,
            )
        finally:
            self.password_server.config.read_eof_after_bytes = None

    def test_server_disconnect_during_stat(self) -> None:
        sql = textwrap.dedent(
            f"""
            LOAD h5db;
            CREATE OR REPLACE TEMPORARY SECRET stat_disconnect (
                TYPE sftp,
                SCOPE 'sftp://127.0.0.1:{self.disconnect_on_stat_server.port}/',
                USERNAME 'h5db',
                PASSWORD 'h5db',
                KNOWN_HOSTS_PATH '{self.disconnect_on_stat_known_hosts}',
                PORT {self.disconnect_on_stat_server.port}
            );
            SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.disconnect_on_stat_server.port}/simple.h5', '/');
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertTrue(
            "Failed to stat SFTP file" in result.output or "Unable to init SFTP subsystem" in result.output,
            msg=result.output,
        )


def main() -> None:
    global PARSED_ARGS
    PARSED_ARGS = parse_args()
    unittest.main(argv=["run_sftp_interaction_tests.py"])


if __name__ == "__main__":
    main()
