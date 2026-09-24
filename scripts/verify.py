#!/usr/bin/env python3
"""verify：一次性校验入口。

依次执行：
  1) 代码测试（unittest 全量）
  2) 构建检查（compileall + 全部模块导入）
  3) HTTP 冒烟 + 三个关键不变量的端到端验证（真实启动 HTTP 服务）：
     a. 截尾恢复：WAL 末端半帧在重启后被截除，序号连续重建；
     b. 损坏隔离：完整帧损坏 -> poisoned（503），禁止追加/读取且标记持久；
     c. 并发抢占：同一 seen_seq 的并发 POST 恰有一个 200，其余 409，
        磁盘无多余字节、序号连续。

任一环节失败即以非零退出码退出。
"""

from __future__ import annotations

import compileall
import json
import os
import py_compile
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_DIR = os.path.join(REPO_ROOT, "app")
sys.path.insert(0, APP_DIR)

from ledger import MAGIC, encode_frame  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    results.append((PASS if ok else FAIL, name, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail and not ok else ""))
    return ok


def wait_health(port: int, timeout: float = 10.0) -> dict:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/healthz", timeout=1
            ) as r:
                return json.loads(r.read())
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last_err = exc
            time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy: {last_err}")


def http(method: str, port: int, path: str, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self, data_dir: str, port: int):
        self.data_dir = data_dir
        self.port = port
        self.proc: subprocess.Popen | None = None
        self.log = os.path.join(data_dir, "server.log")

    def start(self) -> None:
        env = dict(os.environ, DATA_DIR=self.data_dir, PORT=str(self.port))
        logf = open(self.log, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, os.path.join(APP_DIR, "server.py")],
            cwd=APP_DIR,
            env=env,
            stdout=logf,
            stderr=subprocess.STDOUT,
        )

    def stop(self) -> None:
        assert self.proc is not None
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self.proc = None

    def dump_log(self) -> str:
        try:
            return open(self.log, encoding="utf-8").read()
        except OSError:
            return "(no log)"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        if self.proc is not None:
            self.stop()


# ---------------------------------------------------------------------------
# 1) 单元测试
# ---------------------------------------------------------------------------

def run_unit_tests() -> bool:
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(REPO_ROOT, "tests"), pattern="test_*.py")
    import io
    import contextlib

    buf = io.StringIO()
    runner = unittest.TextTestRunner(stream=buf, verbosity=2)
    result = runner.run(suite)
    ok = result.wasSuccessful()
    if not ok:
        print(buf.getvalue())
    check("unit tests (frame/recovery/poison/concurrency)", ok,
          f"{result.testsRun} run, "
          f"{len(result.failures) + len(result.errors)} failed")
    return ok


# ---------------------------------------------------------------------------
# 2) 构建检查（Python: 全量编译 + 导入）
# ---------------------------------------------------------------------------

def run_build_check() -> bool:
    ok = True
    for rel in ("app/ledger.py", "app/server.py", "scripts/verify.py"):
        path = os.path.join(REPO_ROOT, rel)
        try:
            py_compile.compile(path, doraise=True)
        except py_compile.PyCompileError as exc:
            check(f"compile {rel}", False, str(exc))
            ok = False
    compiled = compileall.compile_dir(
        APP_DIR, quiet=1, maxlevels=10, force=True
    )
    check("compileall app/", ok and compiled)

    # 导入检查（语法之外的 NameError 级问题）；server 导入时会初始化 DATA_DIR。
    try:
        subprocess.run(
            [sys.executable, "-c", "import ledger, server"],
            cwd=APP_DIR,
            check=True,
            capture_output=True,
            text=True,
            env=dict(os.environ, DATA_DIR=tempfile.mkdtemp(prefix="verify-import-")),
        )
        check("import modules", True)
    except subprocess.CalledProcessError as exc:
        check("import modules", False, exc.stderr)
        ok = False
    return ok and compiled


# ---------------------------------------------------------------------------
# 3a) HTTP 冒烟
# ---------------------------------------------------------------------------

def run_http_smoke(port: int) -> bool:
    ok = True
    st, body = http("POST", port, "/api/batches",
                    {"seen_seq": 1, "records": [11, 22, 33]})
    ok &= check("smoke: first batch 200", st == 200 and body["next_seq"] == 4,
                f"got {st} {body}")

    st, body = http("POST", port, "/api/batches",
                    {"seen_seq": 4, "records": list(range(100))})
    ok &= check("smoke: 96 records rejected (max 32)", st == 400, f"got {st}")

    st, body = http("POST", port, "/api/batches",
                    {"seen_seq": 4, "records": []})
    ok &= check("smoke: empty batch rejected", st == 400, f"got {st}")

    st, body = http("POST", port, "/api/batches",
                    {"seen_seq": 4, "records": ["x"]})
    ok &= check("smoke: non-integer record rejected", st == 400, f"got {st}")

    st, body = http("POST", port, "/api/batches",
                    {"seen_seq": 99, "records": [1]})
    ok &= check("smoke: stale seen_seq -> 409", st == 409, f"got {st} {body}")

    st, body = http("POST", port, "/api/batches",
                    {"seen_seq": 4, "records": [44, 55]})
    ok &= check("smoke: second batch committed", st == 200, f"got {st}")

    st, body = http("GET", port, "/api/records?cursor=2&limit=2")
    ok &= check(
        "smoke: cursor read continuous",
        st == 200
        and body["records"] == [{"seq": 2, "dose": 22}, {"seq": 3, "dose": 33}]
        and body["next_cursor"] == 4,
        f"got {st} {body}",
    )
    st, body = http("GET", port, "/api/records?cursor=6&limit=10")
    ok &= check(
        "smoke: tail cursor returns empty page",
        st == 200 and body["records"] == [] and body["next_cursor"] == 6,
        f"got {st} {body}",
    )
    st, body = http("GET", port, "/api/records?cursor=99&limit=10")
    ok &= check("smoke: gap cursor rejected", st == 400, f"got {st}")
    return ok


# ---------------------------------------------------------------------------
# 3b) 截尾恢复
# ---------------------------------------------------------------------------

def run_truncation_recovery() -> bool:
    data_dir = tempfile.mkdtemp(prefix="verify-trunc-")
    port = free_port()
    ok = True
    try:
        srv = Server(data_dir, port)
        srv.start()
        try:
            wait_health(port)
            st, _ = http("POST", port, "/api/batches",
                         {"seen_seq": 1, "records": [1, 2, 3]})
            ok &= check("trunc: seed batch A", st == 200, f"got {st}")
            st, _ = http("POST", port, "/api/batches",
                         {"seen_seq": 4, "records": [4]})
            ok &= check("trunc: seed batch B", st == 200, f"got {st}")
        finally:
            srv.stop()

        wal = os.path.join(data_dir, "wal.bin")
        good_size = os.path.getsize(wal)

        # 模拟进程在写入途中被 SIGKILL：物理文件末端每次只会出现一个不完整帧。
        # 依次覆盖三种残缺形态（帧头截断 / 载荷截断 / 尾标截断），每次重启后
        # 都必须仅截除末端残帧并恢复到同一 good_size，绝不进入 poisoned。
        partial_tails = [
            MAGIC[:4],                                          # 帧头截断
            MAGIC + (99).to_bytes(8, "big") + b'{"rec',         # 载荷截断、无尾标
            encode_frame(5, [5, 6])[:-7],                       # 尾标截断
        ]
        for i, tail in enumerate(partial_tails):
            with open(wal, "ab") as f:
                f.write(tail)
            assert os.path.getsize(wal) > good_size

            srv = Server(data_dir, port)
            srv.start()
            try:
                try:
                    health = wait_health(port)
                except Exception:  # noqa: BLE001
                    print(srv.dump_log())
                    raise
                ok &= check(
                    f"trunc: healthy after restart with partial tail #{i + 1}",
                    health.get("status") == "ok"
                    and health.get("next_seq") == 5,
                    str(health),
                )
                ok &= check(
                    f"trunc: incomplete tail #{i + 1} physically removed",
                    os.path.getsize(wal) == good_size,
                    f"size {os.path.getsize(wal)} != {good_size}",
                )
            finally:
                srv.stop()

        srv = Server(data_dir, port)
        srv.start()
        try:
            wait_health(port)
            st, body = http("GET", port, "/api/records?cursor=1&limit=100")
            doses = [r["dose"] for r in body["records"]]
            seqs = [r["seq"] for r in body["records"]]
            ok &= check(
                "trunc: seqs rebuilt continuously from WAL",
                st == 200 and doses == [1, 2, 3, 4] and seqs == [1, 2, 3, 4],
                f"got {st} {body}",
            )
            # 半批记录没有暴露，且没有占用序号：下一批仍从 5 开始。
            st, body = http("POST", port, "/api/batches",
                            {"seen_seq": 5, "records": [5, 6]})
            ok &= check(
                "trunc: next append reuses uncommitted seq 5",
                st == 200 and body["first_seq"] == 5 and body["next_seq"] == 7,
                f"got {st} {body}",
            )
        finally:
            srv.stop()
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
    return ok


# ---------------------------------------------------------------------------
# 3c) 损坏隔离（poisoned）
# ---------------------------------------------------------------------------

def run_corruption_isolation() -> bool:
    data_dir = tempfile.mkdtemp(prefix="verify-poison-")
    port = free_port()
    ok = True
    try:
        srv = Server(data_dir, port)
        srv.start()
        try:
            wait_health(port)
            http("POST", port, "/api/batches",
                 {"seen_seq": 1, "records": [1, 2]})
            http("POST", port, "/api/batches",
                 {"seen_seq": 3, "records": [3, 4]})
        finally:
            srv.stop()

        wal = os.path.join(data_dir, "wal.bin")
        data = bytearray(open(wal, "rb").read())
        # 翻转第一帧（中部）载荷字节 -> 完整帧校验失败。
        data[8 + 8 + 1] ^= 0xFF
        with open(wal, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        srv = Server(data_dir, port)
        srv.start()
        try:
            # 等待进程起来并轮询 healthz（poisoned 时返回 503，连接是通的）。
            deadline = time.time() + 10
            status = body = None
            while time.time() < deadline:
                try:
                    status, body = http("GET", port, "/healthz")
                    break
                except (urllib.error.URLError, ConnectionError, OSError):
                    time.sleep(0.1)
            ok &= check(
                "poison: healthz reports 503",
                status == 503 and body and body.get("status") == "poisoned",
                f"got {status} {body}",
            )
            st, body = http("POST", port, "/api/batches",
                            {"seen_seq": 5, "records": [9]})
            ok &= check("poison: append forbidden (503)",
                        st == 503, f"got {st}")
            st, body = http("GET", port, "/api/records?cursor=1&limit=10")
            ok &= check(
                "poison: reads forbidden, no misleading records (503)",
                st == 503 and body.get("records") is None,
                f"got {st} {body}",
            )
            ok &= check(
                "poison: marker persisted for future restarts",
                os.path.exists(os.path.join(data_dir, "wal.poison")),
            )
            ok &= check(
                "poison: corrupt WAL preserved untouched",
                open(wal, "rb").read() == bytes(data),
            )
        finally:
            srv.stop()

        # 再重启一次：仅凭 poison 标记仍保持隔离。
        srv = Server(data_dir, port)
        srv.start()
        try:
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                try:
                    status, _ = http("GET", port, "/healthz")
                    break
                except (urllib.error.URLError, ConnectionError, OSError):
                    time.sleep(0.1)
            ok &= check("poison: still 503 after another restart",
                        status == 503, f"got {status}")
        finally:
            srv.stop()
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
    return ok


# ---------------------------------------------------------------------------
# 3d) 并发抢占
# ---------------------------------------------------------------------------

def run_concurrent_preemption() -> bool:
    data_dir = tempfile.mkdtemp(prefix="verify-conc-")
    port = free_port()
    ok = True
    try:
        with Server(data_dir, port) as srv:
            wait_health(port)
            n = 40
            outcomes: list[tuple[int, dict]] = []
            barrier = threading.Barrier(n)
            lock = threading.Lock()

            def worker(i: int) -> None:
                barrier.wait()  # 尽量同时发出
                st, body = http(
                    "POST", port, "/api/batches",
                    {"seen_seq": 1, "records": [1000 + i]},
                )
                with lock:
                    outcomes.append((st, body))

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            winners = [o for o in outcomes if o[0] == 200]
            conflicts = [o for o in outcomes if o[0] == 409]
            ok &= check(
                "concurrency: exactly one winner among same seen_seq",
                len(winners) == 1 and len(conflicts) == n - 1,
                f"winners={len(winners)} conflicts={len(conflicts)} "
                f"other={[o[0] for o in outcomes if o[0] not in (200, 409)]}",
            )

            st, body = http("GET", port, "/api/records?cursor=1&limit=100")
            ok &= check(
                "concurrency: exactly one record, seq 1 continuous",
                st == 200 and len(body["records"]) == 1
                and body["records"][0]["seq"] == 1
                and 1000 <= body["records"][0]["dose"] < 1000 + n,
                f"got {st} {body}",
            )

            # 失败者不占序号：正确 seen_seq=2 立刻成功。
            st, body = http("POST", port, "/api/batches",
                            {"seen_seq": 2, "records": [2]})
            ok &= check("concurrency: next seq 2 free for honest client",
                        st == 200 and body["next_seq"] == 3, f"got {st} {body}")

            st, body = http("GET", port, "/api/records?cursor=1&limit=100")
            ok &= check(
                "concurrency: final sequence continuous 1..2",
                [r["seq"] for r in body["records"]] == [1, 2],
                str(body),
            )
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
    return ok


def main() -> int:
    print("== 1/4 unit tests ==")
    a = run_unit_tests()
    print("\n== 2/4 build checks ==")
    b = run_build_check()
    print("\n== 3/4 HTTP smoke ==")
    smoke_dir = tempfile.mkdtemp(prefix="verify-smoke-")
    smoke_port = free_port()
    try:
        with Server(smoke_dir, smoke_port) as srv:
            try:
                wait_health(smoke_port)
                c = run_http_smoke(smoke_port)
            except Exception as exc:  # noqa: BLE001
                print(srv.dump_log())
                check("http smoke", False, repr(exc))
                c = False
    finally:
        shutil.rmtree(smoke_dir, ignore_errors=True)

    print("\n== 4/4 durability invariants ==")
    d = run_truncation_recovery()
    e = run_corruption_isolation()
    f = run_concurrent_preemption()

    print("\n================ SUMMARY ================")
    failed = [r for r in results if r[0] == FAIL]
    for status, name, detail in results:
        print(f"  [{status}] {name}" + (f" -- {detail}" if detail else ""))
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 0 if (a and b and c and d and e and f and not failed) else 1


if __name__ == "__main__":
    raise SystemExit(main())
