#!/usr/bin/env python3
import argparse
import signal
import threading
from pathlib import Path

from sftp_test_server_lib import SFTPServerConfig, SFTPTestServer, load_or_create_host_key, write_known_host


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a local rooted SFTP test server")
    p.add_argument("--host", default="127.0.0.1", help="Listen host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=2222, help="Listen port (default: 2222)")
    p.add_argument("--directory", required=True, help="Root directory exposed by the SFTP server")
    p.add_argument("--username", default="h5db", help="Accepted username")
    p.add_argument("--password", default="h5db", help="Accepted password")
    p.add_argument("--host-key-file", default="", help="Optional host private key path")
    p.add_argument("--known-hosts-file", default="", help="Optional path where a matching known_hosts entry is written")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    root_dir = Path(args.directory).resolve()
    if not root_dir.is_dir():
        raise SystemExit(f"Directory does not exist: {root_dir}")

    host_key = load_or_create_host_key(args.host_key_file or Path("/tmp") / "h5db_sftp_hostkey", "rsa")
    if args.known_hosts_file:
        write_known_host(args.known_hosts_file, args.host, args.port, host_key)

    config = SFTPServerConfig(
        root_dir=str(root_dir), username=args.username, password=args.password, host_keys=[host_key]
    )
    server = SFTPTestServer(args.host, args.port, config)
    server.start()
    shutdown_requested = threading.Event()

    def handle_signal(signum, frame):
        shutdown_requested.set()
        server.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    print(f"Serving SFTP on {args.host}:{server.port} from {root_dir}", flush=True)

    try:
        while not shutdown_requested.is_set():
            signal.pause()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
