#!/usr/bin/env python3
"""Minimal static file server with single-range GET/HEAD support."""

from __future__ import annotations

import argparse
import base64
import os
import re
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)$")
FLAKY_COUNTS: dict[str, int] = {}
FLAKY_LOCK = threading.Lock()


class RangeRequestHandler(SimpleHTTPRequestHandler):
    range_start: int | None = None
    range_end: int | None = None
    truncate_bytes: int | None = None
    corrupt_shift: int = 0

    def send_head(self):
        self.range_start = None
        self.range_end = None

        request_target = self.path
        raw_path = urllib.parse.urlsplit(request_target).path
        control = self._parse_control_path(raw_path)
        self.truncate_bytes = control["truncate_bytes"]
        self.corrupt_shift = control["corrupt_shift"]

        if control["slow_ms"] > 0:
            time.sleep(control["slow_ms"] / 1000.0)

        if control["redirect_code"] is not None and control["redirect_target"] is not None:
            self.send_response(control["redirect_code"])
            self.send_header("Location", control["redirect_target"])
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None

        if control["flaky_failures"] > 0 and control["flaky_status"] is not None:
            # Key flaky counters by the full request target so tests can namespace
            # independent failure scenarios with query strings on the same file path.
            flaky_key = f"{self.command}:{request_target}"
            with FLAKY_LOCK:
                count = FLAKY_COUNTS.get(flaky_key, 0)
                FLAKY_COUNTS[flaky_key] = count + 1
                should_fail = count < control["flaky_failures"]
            if should_fail:
                self.send_error(control["flaky_status"])
                return None

        if control["drop_failures"] > 0:
            drop_key = f"{self.command}:{request_target}"
            with FLAKY_LOCK:
                count = FLAKY_COUNTS.get(drop_key, 0)
                FLAKY_COUNTS[drop_key] = count + 1
                should_drop = count < control["drop_failures"]
            if should_drop:
                try:
                    self.connection.shutdown(2)
                except OSError:
                    pass
                self.connection.close()
                return None

        if control["status_code"] is not None:
            self.send_error(control["status_code"])
            return None

        if control["requires_auth"] and not self._is_authorized():
            self.send_response(HTTPStatus.UNAUTHORIZED)
            self.send_header("WWW-Authenticate", 'Basic realm="h5db-test"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return None

        rewritten_path = control["rewritten_path"]
        original = self.path
        self.path = rewritten_path
        path = self.translate_path(self.path)
        self.path = original
        if os.path.isdir(path):
            # Directory handling should also use the rewritten path.
            self.path = rewritten_path
            result = super().send_head()
            self.path = original
            return result

        try:
            file_obj = open(path, "rb")
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None

        try:
            size = os.fstat(file_obj.fileno()).st_size
            start, end = self._parse_range(self.headers.get("Range"), size)
            if start is None:
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-type", self.guess_type(path))
                self.send_header("Content-Length", str(size))
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Last-Modified", self.date_time_string(os.path.getmtime(path)))
                self.end_headers()
                return file_obj

            self.range_start = start
            self.range_end = end
            self.send_response(HTTPStatus.PARTIAL_CONTENT)
            self.send_header("Content-type", self.guess_type(path))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Last-Modified", self.date_time_string(os.path.getmtime(path)))
            self.end_headers()
            file_obj.seek(start)
            return file_obj
        except Exception:
            file_obj.close()
            raise

    def _parse_control_path(self, raw_path: str) -> dict:
        rewritten = raw_path
        requires_auth = False
        status_code: int | None = None
        slow_ms = 0
        redirect_code: int | None = None
        redirect_target: str | None = None
        flaky_failures = 0
        flaky_status: int | None = None
        drop_failures = 0
        truncate_bytes: int | None = None
        corrupt_shift = 0

        if rewritten.startswith("/auth/"):
            requires_auth = True
            rewritten = "/" + rewritten[len("/auth/") :]

        if rewritten.startswith("/redirect/"):
            parts = rewritten.split("/", 3)
            if len(parts) >= 4 and parts[2].isdigit():
                redirect_code = int(parts[2])
                redirect_target = "/" + parts[3]
                rewritten = "/" + parts[3]

        if rewritten.startswith("/flaky/"):
            parts = rewritten.split("/", 4)
            if len(parts) >= 5 and parts[2].isdigit() and parts[3].isdigit():
                flaky_failures = int(parts[2])
                flaky_status = int(parts[3])
                rewritten = "/" + parts[4]

        if rewritten.startswith("/dropflaky/"):
            parts = rewritten.split("/", 3)
            if len(parts) >= 4 and parts[2].isdigit():
                drop_failures = int(parts[2])
                rewritten = "/" + parts[3]

        if rewritten.startswith("/status/"):
            parts = rewritten.split("/", 3)
            if len(parts) >= 4 and parts[2].isdigit():
                status_code = int(parts[2])
                rewritten = "/" + parts[3]

        if rewritten.startswith("/slow/"):
            parts = rewritten.split("/", 3)
            if len(parts) >= 4 and parts[2].isdigit():
                slow_ms = int(parts[2])
                rewritten = "/" + parts[3]

        if rewritten.startswith("/truncate/"):
            parts = rewritten.split("/", 3)
            if len(parts) >= 4 and parts[2].isdigit():
                truncate_bytes = int(parts[2])
                rewritten = "/" + parts[3]

        if rewritten.startswith("/corrupt/"):
            rewritten = "/" + rewritten[len("/corrupt/") :]
            corrupt_shift = 1

        if not rewritten.startswith("/"):
            rewritten = "/" + rewritten

        return {
            "rewritten_path": rewritten,
            "requires_auth": requires_auth,
            "status_code": status_code,
            "slow_ms": slow_ms,
            "redirect_code": redirect_code,
            "redirect_target": redirect_target,
            "flaky_failures": flaky_failures,
            "flaky_status": flaky_status,
            "drop_failures": drop_failures,
            "truncate_bytes": truncate_bytes,
            "corrupt_shift": corrupt_shift,
        }

    def _is_authorized(self) -> bool:
        header = self.headers.get("Authorization")
        if not header:
            return False
        expected = "Basic " + base64.b64encode(b"h5db:duckdb").decode("ascii")
        return header.strip() == expected

    def copyfile(self, source, outputfile):
        if self.corrupt_shift > 0:
            source.seek(self.corrupt_shift, os.SEEK_CUR)

        if self.range_start is None or self.range_end is None:
            if self.truncate_bytes is None:
                return super().copyfile(source, outputfile)
            remaining = self.truncate_bytes
            bufsize = 64 * 1024
            while remaining > 0:
                chunk = source.read(min(bufsize, remaining))
                if not chunk:
                    break
                outputfile.write(chunk)
                remaining -= len(chunk)
            return None

        remaining = self.range_end - self.range_start + 1
        if self.truncate_bytes is not None:
            remaining = min(remaining, self.truncate_bytes)
        bufsize = 64 * 1024
        while remaining > 0:
            chunk = source.read(min(bufsize, remaining))
            if not chunk:
                break
            outputfile.write(chunk)
            remaining -= len(chunk)

    def _parse_range(self, header: str | None, size: int) -> tuple[int | None, int | None]:
        if not header:
            return None, None

        match = RANGE_RE.fullmatch(header.strip())
        if not match:
            return None, None

        start_s, end_s = match.groups()
        if start_s == "" and end_s == "":
            return None, None

        if start_s == "":
            length = int(end_s)
            if length <= 0:
                return None, None
            start = max(0, size - length)
            end = size - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else size - 1
            if start >= size:
                self.send_error(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE, "Invalid range")
                return None, None
            end = min(end, size - 1)
            if end < start:
                return None, None

        return start, end


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--directory", required=True)
    args = parser.parse_args()

    handler = lambda *hargs, **hkwargs: RangeRequestHandler(*hargs, directory=args.directory, **hkwargs)
    with ThreadingHTTPServer(("127.0.0.1", args.port), handler) as httpd:
        httpd.serve_forever()


if __name__ == "__main__":
    main()
