"""束流站剂量账本 HTTP 服务（标准库 ThreadingHTTPServer，零依赖）。"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from ledger import (
    ConflictError,
    DoseLedger,
    FrameFormatError,
    PoisonedError,
)

DATA_DIR = os.environ.get("DATA_DIR", "/data")
MAX_BODY = 2 * 1024 * 1024  # 2 MiB，远大于 32 条整数记录的合法请求

ledger = DoseLedger(DATA_DIR)


class _Server(ThreadingHTTPServer):
    # 40+ 并发 POST 时不能让 listen backlog（默认 5）丢掉连接。
    request_queue_size = 128
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    server_version = "DoseLedger/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:  # 安静一些
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    # -- 工具 --------------------------------------------------------------

    def _json(self, status: int, obj: dict) -> None:
        body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass

    # -- 路由 --------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        parts = urlsplit(self.path)
        if parts.path == "/healthz":
            if ledger.poisoned:
                self._json(503, {"status": "poisoned", "reason": ledger.poison_reason})
            else:
                self._json(
                    200,
                    {"status": "ok", "next_seq": ledger.snapshot_len() + 1},
                )
            return
        if parts.path == "/api/records":
            self._handle_records(parts.query)
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        parts = urlsplit(self.path)
        if parts.path == "/api/batches":
            self._handle_batch()
            return
        self._json(404, {"error": "not found"})

    # -- 业务 --------------------------------------------------------------

    def _handle_batch(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BODY:
            self._json(400, {"error": "invalid Content-Length"})
            return
        raw = self.rfile.read(length)
        try:
            req = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._json(400, {"error": "body must be a UTF-8 JSON object"})
            return
        if not isinstance(req, dict):
            self._json(400, {"error": "body must be a JSON object"})
            return
        seen_seq = req.get("seen_seq")
        records = req.get("records")
        if not isinstance(seen_seq, int) or isinstance(seen_seq, bool) or seen_seq < 1:
            self._json(400, {"error": "seen_seq must be a positive integer"})
            return
        if (
            not isinstance(records, list)
            or not (1 <= len(records) <= 32)
            or any(not isinstance(v, int) or isinstance(v, bool) for v in records)
        ):
            self._json(400, {"error": "records must be a list of 1..32 integers"})
            return
        try:
            next_seq = ledger.append_batch(seen_seq, records)
        except ConflictError as exc:
            self._json(
                409,
                {
                    "error": "conflict: stale seen_seq",
                    "current_next_seq": ledger.next_seq(),
                    "detail": str(exc),
                },
            )
            return
        except PoisonedError as exc:
            self._json(503, {"error": "ledger poisoned", "detail": str(exc)})
            return
        except FrameFormatError as exc:
            self._json(400, {"error": str(exc)})
            return
        self._json(
            200,
            {
                "status": "committed",
                "first_seq": seen_seq,
                "count": len(records),
                "next_seq": next_seq,
            },
        )

    def _handle_records(self, query: str) -> None:
        qs = parse_qs(query)
        try:
            cursor = int(qs.get("cursor", ["1"])[0])
            limit = int(qs.get("limit", ["100"])[0])
        except ValueError:
            self._json(400, {"error": "cursor and limit must be integers"})
            return
        try:
            items, next_cursor = ledger.read_records(cursor, limit)
        except PoisonedError as exc:
            self._json(503, {"error": "ledger poisoned", "detail": str(exc)})
            return
        except FrameFormatError as exc:
            self._json(400, {"error": str(exc)})
            return
        self._json(
            200,
            {
                "records": [{"seq": s, "dose": d} for s, d in items],
                "next_cursor": next_cursor,
            },
        )


def main() -> int:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    server = _Server((host, port), Handler)

    def _shutdown(signum, frame):  # noqa: ANN001
        # 让 serve_forever 尽快退出；shutdown() 必须由另一线程调用，
        # 否则与 serve_forever 在同一线程中自死锁。
        # 正在进行中的帧写入由 fsync + 启动恢复保证：要么完整，要么重启时截尾。
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    if ledger.poisoned:
        sys.stderr.write(
            "ledger started in POISONED state: %s\n" % ledger.poison_reason
        )
    else:
        sys.stderr.write("ledger healthy; next seq = %d\n" % ledger.next_seq())
    sys.stderr.write("listening on %s:%d\n" % (host, port))

    server.serve_forever()
    server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
