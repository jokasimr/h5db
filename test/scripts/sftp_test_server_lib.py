#!/usr/bin/env python3
import os
import posixpath
import socket
import stat
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

import paramiko


def _host_key_name(key: paramiko.PKey) -> str:
    name = key.get_name()
    if name in ("rsa-sha2-256", "rsa-sha2-512"):
        return "ssh-rsa"
    return name


def write_known_host(path: str | Path, host: str, port: int, host_key: paramiko.PKey) -> None:
    known_host = f"[{host}]:{port}" if port != 22 else host
    line = f"{known_host} {_host_key_name(host_key)} {host_key.get_base64()}\n"
    Path(path).write_text(line, encoding="utf-8")


def load_or_create_host_key(path: str | Path, key_type: str = "rsa") -> paramiko.PKey:
    key_path = Path(path)
    if key_path.exists():
        if key_type == "rsa":
            return paramiko.RSAKey.from_private_key_file(str(key_path))
        if key_type == "ecdsa":
            return paramiko.ECDSAKey.from_private_key_file(str(key_path))
        raise ValueError(f"Unsupported host key type: {key_type}")
    if key_type == "rsa":
        key = paramiko.RSAKey.generate(2048)
    elif key_type == "ecdsa":
        key = paramiko.ECDSAKey.generate(bits=256)
    else:
        raise ValueError(f"Unsupported host key type: {key_type}")
    key.write_private_key_file(str(key_path))
    return key


@dataclass
class SFTPServerConfig:
    root_dir: str
    username: str = "h5db"
    password: Optional[str] = "h5db"
    allowed_public_key_blobs: set[bytes] = field(default_factory=set)
    read_delay_ms: int = 0
    stat_delay_ms: int = 0
    disconnect_on_stat: bool = False
    disconnect_after_read_calls: Optional[int] = None
    host_keys: list[paramiko.PKey] = field(default_factory=list)


@dataclass
class ConnectionRecord:
    auth_method: Optional[str] = None
    negotiated_host_key: Optional[str] = None
    read_calls: int = 0


class ServerTelemetry:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.connections: list[ConnectionRecord] = []
        self.total_read_calls = 0

    def add_connection(self) -> ConnectionRecord:
        with self.lock:
            record = ConnectionRecord()
            self.connections.append(record)
            return record

    def record_read_call(self, record: ConnectionRecord) -> int:
        with self.lock:
            self.total_read_calls += 1
            record.read_calls += 1
            return self.total_read_calls

    def snapshot(self) -> tuple[list[ConnectionRecord], int]:
        with self.lock:
            copied = [ConnectionRecord(r.auth_method, r.negotiated_host_key, r.read_calls) for r in self.connections]
            return copied, self.total_read_calls

    def reset(self) -> None:
        with self.lock:
            self.connections.clear()
            self.total_read_calls = 0


class AuthServer(paramiko.ServerInterface):
    def __init__(self, config: SFTPServerConfig, record: ConnectionRecord):
        super().__init__()
        self.config = config
        self.record = record

    def check_auth_password(self, username: str, password: str):
        if self.config.password is not None and username == self.config.username and password == self.config.password:
            self.record.auth_method = "password"
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def check_auth_publickey(self, username: str, key: paramiko.PKey):
        if username == self.config.username and key.asbytes() in self.config.allowed_public_key_blobs:
            self.record.auth_method = "publickey"
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str):
        auths = []
        if self.config.password is not None:
            auths.append("password")
        if self.config.allowed_public_key_blobs:
            auths.append("publickey")
        return ",".join(auths)

    def check_channel_request(self, kind: str, chanid: int):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class RootedSFTPHandle(paramiko.SFTPHandle):
    def __init__(
        self,
        flags: int,
        config: SFTPServerConfig,
        telemetry: ServerTelemetry,
        transport: paramiko.Transport,
        record: ConnectionRecord,
    ):
        super().__init__(flags)
        self.config = config
        self.telemetry = telemetry
        self.transport = transport
        self.record = record

    def stat(self):
        if self.config.stat_delay_ms > 0:
            time.sleep(self.config.stat_delay_ms / 1000.0)
        if self.config.disconnect_on_stat:
            self.transport.close()
            return paramiko.SFTP_FAILURE
        try:
            return paramiko.SFTPAttributes.from_stat(os.fstat(self.readfile.fileno()))
        except OSError as ex:
            return paramiko.SFTPServer.convert_errno(ex.errno)

    def read(self, offset, length):
        if self.config.read_delay_ms > 0:
            time.sleep(self.config.read_delay_ms / 1000.0)
        read_call_count = self.telemetry.record_read_call(self.record)
        if self.config.disconnect_after_read_calls and read_call_count > self.config.disconnect_after_read_calls:
            self.transport.close()
            return paramiko.SFTP_FAILURE
        return super().read(offset, length)


class RootedSFTPServer(paramiko.SFTPServerInterface):
    def __init__(
        self,
        server,
        config: SFTPServerConfig,
        telemetry: ServerTelemetry,
        transport: paramiko.Transport,
        record: ConnectionRecord,
        *args,
        **kwargs,
    ):
        super().__init__(server, *args, **kwargs)
        self.config = config
        self.telemetry = telemetry
        self.transport = transport
        self.record = record
        self.root_dir = os.path.realpath(config.root_dir)

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
        if self.config.stat_delay_ms > 0:
            time.sleep(self.config.stat_delay_ms / 1000.0)
        if self.config.disconnect_on_stat:
            self.transport.close()
            return paramiko.SFTP_FAILURE
        try:
            return paramiko.SFTPAttributes.from_stat(os.stat(self._resolve(path)))
        except OSError as ex:
            return self._convert_error(ex)

    def lstat(self, path: str):
        try:
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
            handle = RootedSFTPHandle(flags, self.config, self.telemetry, self.transport, self.record)
            handle.readfile = open(local_path, "rb")
            return handle
        except OSError as ex:
            return self._convert_error(ex)


class SFTPTestServer:
    def __init__(self, host: str, port: int, config: SFTPServerConfig):
        self.host = host
        self.port = port
        self.config = config
        self.telemetry = ServerTelemetry()
        self.stop_event = threading.Event()
        self.server_sock: Optional[socket.socket] = None
        self.accept_thread: Optional[threading.Thread] = None
        self.client_threads: list[threading.Thread] = []

    def start(self) -> None:
        if not self.config.host_keys:
            raise ValueError("At least one host key must be configured")
        root_dir = os.path.realpath(self.config.root_dir)
        if not os.path.isdir(root_dir):
            raise ValueError(f"Directory does not exist: {root_dir}")
        self.config.root_dir = root_dir

        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(100)
        self.server_sock.settimeout(0.2)
        self.port = self.server_sock.getsockname()[1]
        self.accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.accept_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.server_sock is not None:
            try:
                self.server_sock.close()
            except OSError:
                pass
            self.server_sock = None
        if self.accept_thread is not None:
            self.accept_thread.join(timeout=1.0)
            self.accept_thread = None
        for thread in self.client_threads:
            thread.join(timeout=1.0)
        self.client_threads.clear()

    def _accept_loop(self) -> None:
        assert self.server_sock is not None
        while not self.stop_event.is_set():
            try:
                client, _ = self.server_sock.accept()
            except TimeoutError:
                continue
            except OSError:
                if self.stop_event.is_set():
                    break
                raise

            thread = threading.Thread(target=self._serve_client, args=(client,), daemon=True)
            thread.start()
            self.client_threads.append(thread)

    def _serve_client(self, client: socket.socket) -> None:
        transport = None
        try:
            transport = paramiko.Transport(client)
            for host_key in self.config.host_keys:
                transport.add_server_key(host_key)
            record = self.telemetry.add_connection()
            transport.set_subsystem_handler(
                "sftp",
                paramiko.SFTPServer,
                RootedSFTPServer,
                config=self.config,
                telemetry=self.telemetry,
                transport=transport,
                record=record,
            )
            server = AuthServer(self.config, record)
            transport.start_server(server=server)
            while transport.is_active() and not self.stop_event.is_set():
                if record.negotiated_host_key is None:
                    active_key = transport.get_server_key()
                    if active_key is not None:
                        record.negotiated_host_key = _host_key_name(active_key)
                time.sleep(0.05)
        except paramiko.SSHException as ex:
            message = str(ex)
            if (
                isinstance(ex, paramiko.ssh_exception.IncompatiblePeer)
                or "Error reading SSH protocol banner" in message
            ):
                pass
            else:
                raise
        except (EOFError, OSError):
            pass
        finally:
            if transport is not None:
                transport.close()
            client.close()


def write_known_hosts(path: str | Path, host: str, port: int, host_keys: Iterable[paramiko.PKey]) -> None:
    known_host = f"[{host}]:{port}" if port != 22 else host
    lines = [f"{known_host} {_host_key_name(key)} {key.get_base64()}\n" for key in host_keys]
    Path(path).write_text("".join(lines), encoding="utf-8")
