#!/usr/bin/env python3
import argparse
import os
import posixpath
import signal
import socket
import threading
import time
from pathlib import Path

import paramiko


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a local rooted SFTP test server with artificial read delay")
    p.add_argument("--host", default="127.0.0.1", help="Listen host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=2222, help="Listen port (default: 2222)")
    p.add_argument("--directory", required=True, help="Root directory exposed by the SFTP server")
    p.add_argument("--username", default="h5db", help="Accepted username")
    p.add_argument("--password", default="h5db", help="Accepted password")
    p.add_argument("--host-key-file", default="", help="Optional host private key path")
    p.add_argument("--known-hosts-file", default="", help="Optional path where a matching known_hosts entry is written")
    p.add_argument("--read-delay-ms", type=int, default=0, help="Sleep this many milliseconds before every file read")
    p.add_argument("--stat-delay-ms", type=int, default=0, help="Sleep this many milliseconds before stat/lstat")
    return p.parse_args()


class PasswordOnlyServer(paramiko.ServerInterface):
    def __init__(self, username: str, password: str):
        super().__init__()
        self.username = username
        self.password = password

    def check_auth_password(self, username: str, password: str):
        if username == self.username and password == self.password:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str):
        return "password"

    def check_channel_request(self, kind: str, chanid: int):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class SlowSFTPHandle(paramiko.SFTPHandle):
    def __init__(self, flags=0, read_delay_ms: int = 0):
        super().__init__(flags)
        self.read_delay_ms = read_delay_ms

    def read(self, offset, length):
        if self.read_delay_ms > 0:
            time.sleep(self.read_delay_ms / 1000.0)
        readfile = getattr(self, "readfile", None)
        if readfile is None:
            return paramiko.SFTP_FAILURE
        try:
            readfile.seek(offset)
            return readfile.read(length)
        except OSError as ex:
            return paramiko.SFTPServer.convert_errno(ex.errno)

    def stat(self):
        try:
            return paramiko.SFTPAttributes.from_stat(os.fstat(self.readfile.fileno()))
        except OSError as ex:
            return paramiko.SFTPServer.convert_errno(ex.errno)


class SlowRootedSFTPServer(paramiko.SFTPServerInterface):
    def __init__(self, server, root_dir: str, read_delay_ms: int, stat_delay_ms: int, *args, **kwargs):
        super().__init__(server, *args, **kwargs)
        self.root_dir = os.path.realpath(root_dir)
        self.read_delay_ms = read_delay_ms
        self.stat_delay_ms = stat_delay_ms

    def _resolve(self, path: str) -> str:
        normalized = posixpath.normpath(path)
        if not normalized.startswith("/"):
            normalized = "/" + normalized
        candidate = os.path.realpath(os.path.join(self.root_dir, normalized.lstrip("/")))
        if candidate != self.root_dir and not candidate.startswith(self.root_dir + os.sep):
            raise PermissionError("Path escapes server root")
        return candidate

    def _convert_error(self, ex: OSError):
        errno = ex.errno if ex.errno is not None else 1
        return paramiko.SFTPServer.convert_errno(errno)

    def list_folder(self, path: str):
        try:
            local_path = self._resolve(path)
            entries = []
            for name in sorted(os.listdir(local_path)):
                full_path = os.path.join(local_path, name)
                attrs = paramiko.SFTPAttributes.from_stat(os.lstat(full_path), filename=name)
                entries.append(attrs)
            return entries
        except OSError as ex:
            return self._convert_error(ex)

    def stat(self, path: str):
        try:
            if self.stat_delay_ms > 0:
                time.sleep(self.stat_delay_ms / 1000.0)
            return paramiko.SFTPAttributes.from_stat(os.stat(self._resolve(path)))
        except OSError as ex:
            return self._convert_error(ex)

    def lstat(self, path: str):
        try:
            if self.stat_delay_ms > 0:
                time.sleep(self.stat_delay_ms / 1000.0)
            return paramiko.SFTPAttributes.from_stat(os.lstat(self._resolve(path)))
        except OSError as ex:
            return self._convert_error(ex)

    def realpath(self, path: str):
        try:
            local_path = self._resolve(path)
            relative = os.path.relpath(local_path, self.root_dir)
            if relative == ".":
                return "/"
            return "/" + relative.replace(os.sep, "/")
        except OSError:
            return path

    def open(self, path: str, flags: int, attr):
        access_mode = flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)
        if access_mode != os.O_RDONLY or flags & (os.O_CREAT | os.O_TRUNC | os.O_APPEND | os.O_EXCL):
            return paramiko.SFTP_PERMISSION_DENIED

        try:
            local_path = self._resolve(path)
            handle = SlowSFTPHandle(flags, self.read_delay_ms)
            handle.readfile = open(local_path, "rb")
            return handle
        except OSError as ex:
            return self._convert_error(ex)


def load_or_create_host_key(path: str) -> paramiko.PKey:
    if path:
        key_path = Path(path)
        if key_path.exists():
            return paramiko.RSAKey.from_private_key_file(str(key_path))
        key = paramiko.RSAKey.generate(2048)
        key.write_private_key_file(str(key_path))
        return key
    return paramiko.RSAKey.generate(2048)


def write_known_hosts(path: str, host: str, port: int, host_key: paramiko.PKey) -> None:
    if not path:
        return
    known_host = f"[{host}]:{port}" if port != 22 else host
    line = f"{known_host} {host_key.get_name()} {host_key.get_base64()}\n"
    Path(path).write_text(line, encoding="utf-8")


def serve_client(
    client,
    host_key,
    root_dir: str,
    username: str,
    password: str,
    stop_event: threading.Event,
    read_delay_ms: int,
    stat_delay_ms: int,
) -> None:
    transport = None
    try:
        transport = paramiko.Transport(client)
        transport.add_server_key(host_key)
        transport.set_subsystem_handler(
            "sftp",
            paramiko.SFTPServer,
            SlowRootedSFTPServer,
            root_dir=root_dir,
            read_delay_ms=read_delay_ms,
            stat_delay_ms=stat_delay_ms,
        )
        server = PasswordOnlyServer(username, password)
        transport.start_server(server=server)
        while transport.is_active() and not stop_event.is_set():
            time.sleep(0.05)
    finally:
        if transport is not None:
            transport.close()
        client.close()


def main() -> None:
    args = parse_args()
    root_dir = os.path.realpath(args.directory)
    if not os.path.isdir(root_dir):
        raise SystemExit(f"Directory does not exist: {root_dir}")

    host_key = load_or_create_host_key(args.host_key_file)
    write_known_hosts(args.known_hosts_file, args.host, args.port, host_key)

    stop_event = threading.Event()

    def handle_signal(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((args.host, args.port))
        server_sock.listen(100)
        server_sock.settimeout(0.2)
        print(f"Serving slow SFTP on {args.host}:{args.port} from {root_dir}", flush=True)

        threads = []
        while not stop_event.is_set():
            try:
                client, _ = server_sock.accept()
            except TimeoutError:
                continue
            except OSError:
                if stop_event.is_set():
                    break
                raise

            thread = threading.Thread(
                target=serve_client,
                args=(
                    client,
                    host_key,
                    root_dir,
                    args.username,
                    args.password,
                    stop_event,
                    args.read_delay_ms,
                    args.stat_delay_ms,
                ),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join(timeout=1.0)


if __name__ == "__main__":
    main()
