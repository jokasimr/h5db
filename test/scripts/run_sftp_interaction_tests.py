#!/usr/bin/env python3
import argparse
import logging
import os
import shutil
import subprocess
import tempfile
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


class DuckDBResult:
    def __init__(self, completed: subprocess.CompletedProcess[str]):
        self.returncode = completed.returncode
        self.stdout = completed.stdout
        self.stderr = completed.stderr
        self.output = (completed.stdout or "") + (completed.stderr or "")


class SFTPInteractionTests(unittest.TestCase):
    SUBPROCESS_TIMEOUT_SECONDS = 20

    @classmethod
    def setUpClass(cls) -> None:
        if PARSED_ARGS is None:
            raise RuntimeError("Arguments must be parsed before running tests")
        args = PARSED_ARGS
        cls.project_root = Path(__file__).resolve().parents[2]
        cls.duckdb_bin = (
            str((cls.project_root / args.duckdb_bin).resolve())
            if not Path(args.duckdb_bin).is_absolute()
            else args.duckdb_bin
        )
        cls.data_dir = str((cls.project_root / "test/data").resolve())
        cls.tempdir = Path(tempfile.mkdtemp(prefix="h5db_sftp_interaction_", dir="/tmp"))
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

        cls.password_known_hosts = cls.tempdir / "password_known_hosts"
        cls.key_known_hosts = cls.tempdir / "key_known_hosts"
        cls.multi_key_rsa_known_hosts = cls.tempdir / "multi_key_rsa_known_hosts"
        cls.multi_key_all_known_hosts = cls.tempdir / "multi_key_all_known_hosts"
        cls.flaky_known_hosts = cls.tempdir / "flaky_known_hosts"
        cls.disconnect_on_stat_known_hosts = cls.tempdir / "disconnect_on_stat_known_hosts"

        write_known_host(cls.password_known_hosts, "127.0.0.1", cls.password_server.port, cls.rsa_host_key)
        write_known_host(cls.key_known_hosts, "127.0.0.1", cls.key_server.port, cls.rsa_host_key)
        write_known_host(cls.multi_key_rsa_known_hosts, "127.0.0.1", cls.multi_key_server.port, cls.rsa_host_key)
        write_known_hosts(
            cls.multi_key_all_known_hosts,
            "127.0.0.1",
            cls.multi_key_server.port,
            [cls.rsa_host_key, cls.ecdsa_host_key],
        )
        write_known_host(cls.flaky_known_hosts, "127.0.0.1", cls.flaky_server.port, cls.rsa_host_key)

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

    @classmethod
    def tearDownClass(cls) -> None:
        for server in (
            cls.password_server,
            cls.key_server,
            cls.multi_key_server,
            cls.flaky_server,
            cls.disconnect_on_stat_server,
            cls.mutable_server,
        ):
            server.stop()
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def setUp(self) -> None:
        for server in (
            self.password_server,
            self.key_server,
            self.multi_key_server,
            self.flaky_server,
            self.disconnect_on_stat_server,
            self.mutable_server,
        ):
            server.telemetry.reset()

    def run_sql(self, sql: str) -> DuckDBResult:
        try:
            completed = subprocess.run(
                [self.duckdb_bin, "-unsigned", "-c", sql],
                cwd=self.project_root,
                text=True,
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

    def assertOutputContains(self, result: DuckDBResult, needle: str) -> None:
        self.assertIn(needle, result.output, msg=result.output)

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
            COPY (
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertIn("5", result.stdout)
        connections, _ = self.key_server.telemetry.snapshot()
        self.assertTrue(any(record.auth_method == "publickey" for record in connections))

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
            COPY (
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.key_server.port}/simple.h5', '/')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertIn("5", result.stdout)
        connections, _ = self.key_server.telemetry.snapshot()
        self.assertTrue(any(record.auth_method == "publickey" for record in connections))

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
            COPY (
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5', '/')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
            COPY (
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5', '/')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
            COPY (
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5', '/')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
            COPY (
                SELECT COUNT(*) FROM h5_ls('sftp://127.0.0.1:{self.multi_key_server.port}/simple.h5', '/')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
            """
        ).strip()
        result = self.run_sql(sql)
        self.assertEqual(result.returncode, 0, msg=result.output)
        self.assertIn("5", result.stdout)
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
            COPY (
                SELECT COUNT(*) FROM h5_tree('{url}')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
            COPY (
                SELECT COUNT(*) FROM h5_tree('{url}')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
            COPY (
                SELECT COALESCE(SUM(nr_bytes), 0)
                FROM duckdb_external_file_cache()
                WHERE path = '{url}'
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
            COPY (
                SELECT COUNT(*) FROM h5_tree('{url}')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
            COPY (
                SELECT COUNT(*) FROM h5_tree('{url}')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
            COPY (
                SELECT COUNT(*) FROM h5_tree('{url}')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
            COPY (
                SELECT SUM(array_2d[1]) FROM h5_read('{url}', '/array_2d')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
            COPY (
                SELECT SUM(array_2d[1]) FROM h5_read('{url}', '/array_2d')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
            COPY (
                SELECT SUM(array_2d[1]) FROM h5_read('{url}', '/array_2d')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
            [self.duckdb_bin, "-unsigned"],
            cwd=self.project_root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
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

        def read_next_numeric_line() -> str:
            while True:
                line = process.stdout.readline()
                self.assertNotEqual(line, "", "DuckDB process exited before producing the expected query result")
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
            COPY (
                SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.mutable_server.port}/mutable.h5')
            ) TO STDOUT (FORMAT CSV, HEADER FALSE);
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
                COPY (
                    SELECT COUNT(*) FROM h5_tree('sftp://127.0.0.1:{self.mutable_server.port}/mutable.h5')
                ) TO STDOUT (FORMAT CSV, HEADER FALSE);
                .quit
                """
            )
        )
        process.stdin.flush()

        stdout, stderr = process.communicate(timeout=self.SUBPROCESS_TIMEOUT_SECONDS)
        self.assertEqual(process.returncode, 0, msg=(stdout or "") + (stderr or ""))
        numeric_lines = [line.strip() for line in stdout.splitlines() if line.strip().isdigit()]
        self.assertEqual(numeric_lines[0], "3", msg=stdout)
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
        result = self.run_sql(sql)
        self.assertNotEqual(result.returncode, 0, msg=result.output)
        self.assertTrue(
            "Failed to read SFTP data" in result.output or "Unexpected EOF while reading SFTP file" in result.output,
            msg=result.output,
        )

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
