"""束流站剂量账本：WAL 帧编解码、恢复（截尾 / poisoned）、并发安全追加。

零第三方依赖，仅用 Python 标准库。帧格式见仓库 README。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import List, Optional, Tuple

MAGIC = b"BLWALFR1"
MAGIC_LEN = 8
LENGTH_LEN = 8
CHECKSUM_LEN = 32
HEADER_LEN = MAGIC_LEN + LENGTH_LEN
MAX_RECORDS_PER_BATCH = 32
MAX_PAYLOAD = 1024 * 1024  # 1 MiB 硬上限
POISON_MARKER = "wal.poison"


class PoisonedError(RuntimeError):
    """账本处于 poisoned 状态（中部结构损坏 / 完整帧校验失败）。"""


class FrameFormatError(ValueError):
    """帧结构、长度或载荷不合法。"""


# ---------------------------------------------------------------------------
# 纯函数：帧编解码（便于单测直接覆盖）
# ---------------------------------------------------------------------------

def canonical_payload(first_seq: int, records: List[int]) -> bytes:
    """构造规范载荷：键排序、无空白、UTF-8，且不允许 ASCII 转义非 ASCII 字符。

    规范形式同时用于写入与恢复期比对，防止“同一 JSON 两种字节”被伪造。
    """
    if not isinstance(first_seq, int) or isinstance(first_seq, bool):
        raise FrameFormatError("seq must be an integer")
    if not (1 <= len(records) <= MAX_RECORDS_PER_BATCH):
        raise FrameFormatError("batch must contain 1..32 records")
    for v in records:
        if not isinstance(v, int) or isinstance(v, bool):
            raise FrameFormatError("dose records must be integers")
    obj = {"records": list(records), "seq": first_seq}
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def encode_frame(first_seq: int, records: List[int]) -> bytes:
    payload = canonical_payload(first_seq, records)
    length = len(payload)
    if length > MAX_PAYLOAD:
        raise FrameFormatError("payload too large")
    length_bytes = length.to_bytes(LENGTH_LEN, "big", signed=False)
    digest = hashlib.sha256(MAGIC + length_bytes + payload).digest()
    return MAGIC + length_bytes + payload + digest


def _parse_payload(payload: bytes) -> Tuple[int, List[int]]:
    try:
        obj = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrameFormatError(f"payload is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(obj, dict) or set(obj.keys()) != {"records", "seq"}:
        raise FrameFormatError("payload must be exactly {records, seq}")
    seq = obj["seq"]
    records = obj["records"]
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        raise FrameFormatError("payload seq must be a positive integer")
    if not isinstance(records, list) or not (
        1 <= len(records) <= MAX_RECORDS_PER_BATCH
    ):
        raise FrameFormatError("payload records must be a list of 1..32 items")
    for v in records:
        if not isinstance(v, int) or isinstance(v, bool):
            raise FrameFormatError("payload records must be integers")
    # 重新规范化比对：任何等价但非规范的字节都视为结构损坏。
    if canonical_payload(seq, records) != payload:
        raise FrameFormatError("payload is not in canonical form")
    return seq, records


# ---------------------------------------------------------------------------
# 账本
# ---------------------------------------------------------------------------

class DoseLedger:
    """基于单个 WAL 文件的剂量账本。

    线程安全：一把可重入锁串行化追加与状态变更；读取记录走同一把锁以避免
    与 poisoned 转换竞争。磁盘持久化依赖 page cache 刷盘（fsync）。
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.wal_path = os.path.join(data_dir, "wal.bin")
        self.poison_path = os.path.join(data_dir, POISON_MARKER)
        self._lock = threading.RLock()
        self._records: List[int] = []  # 下标 i 对应序号 i + 1
        self._poisoned = False
        self._poison_reason: Optional[str] = None
        self._recover()

    # -- 状态访问 ----------------------------------------------------------

    @property
    def poisoned(self) -> bool:
        return self._poisoned

    @property
    def poison_reason(self) -> Optional[str]:
        return self._poison_reason

    def next_seq(self) -> int:
        with self._lock:
            self._raise_if_poisoned()
            return len(self._records) + 1

    def snapshot_len(self) -> int:
        with self._lock:
            return len(self._records)

    # -- 恢复 --------------------------------------------------------------

    def _write_poison_marker(self, reason: str) -> None:
        # 原子落盘标记，保证重启后 poisoned 状态不丢失。
        tmp = self.poison_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(reason + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.poison_path)
            dirfd = os.open(self.data_dir, os.O_RDONLY)
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)
        except OSError:
            # 即使标记写失败，进程内仍保持 poisoned；WAL 本身未被改动。
            pass

    def _poison(self, reason: str) -> None:
        self._poisoned = True
        self._poison_reason = reason
        self._write_poison_marker(reason)

    def _raise_if_poisoned(self) -> None:
        if self._poisoned:
            raise PoisonedError(self._poison_reason or "ledger is poisoned")

    def _recover(self) -> None:
        """启动时扫描 WAL：末端不完整帧截尾；其余任何损坏一律 poisoned。"""
        if os.path.exists(self.poison_path):
            try:
                with open(self.poison_path, "r", encoding="utf-8") as f:
                    reason = f.read().strip()
            except OSError:
                reason = "poison marker present"
            self._poisoned = True
            self._poison_reason = reason or "poison marker present"
            return

        if not os.path.exists(self.wal_path):
            self._records = []
            return

        with open(self.wal_path, "rb") as f:
            data = f.read()

        pos = 0
        expected_seq = 1
        total = len(data)
        truncate_at: Optional[int] = None

        while pos < total:
            frame_start = pos
            # 帧头不完整：只允许发生在物理文件末端 -> 截尾。
            if total - pos < HEADER_LEN:
                truncate_at = frame_start
                break

            magic = data[pos : pos + MAGIC_LEN]
            if magic != MAGIC:
                self._poison(
                    f"bad magic at offset {frame_start}: {magic!r} (mid-file corruption)"
                )
                return

            length = int.from_bytes(
                data[pos + MAGIC_LEN : pos + HEADER_LEN], "big", signed=False
            )
            if length == 0 or length > MAX_PAYLOAD:
                self._poison(
                    f"illegal payload length {length} at offset {frame_start}"
                )
                return

            frame_end = pos + HEADER_LEN + length + CHECKSUM_LEN
            if frame_end > total:
                # 载荷或尾标被截断，且该帧位于物理文件末端 -> 截尾。
                truncate_at = frame_start
                break

            payload = data[pos + HEADER_LEN : pos + HEADER_LEN + length]
            stored_digest = data[
                pos + HEADER_LEN + length : pos + HEADER_LEN + length + CHECKSUM_LEN
            ]
            length_bytes = length.to_bytes(LENGTH_LEN, "big", signed=False)
            calc_digest = hashlib.sha256(
                MAGIC + length_bytes + payload
            ).digest()
            if stored_digest != calc_digest:
                self._poison(
                    f"checksum mismatch on complete frame at offset {frame_start}"
                )
                return

            try:
                seq, records = _parse_payload(payload)
            except FrameFormatError as exc:
                self._poison(f"invalid frame payload at offset {frame_start}: {exc}")
                return

            if seq != expected_seq:
                self._poison(
                    f"sequence gap at offset {frame_start}: expected {expected_seq}, "
                    f"frame starts at {seq}"
                )
                return

            self._records.extend(records)
            expected_seq += len(records)
            pos = frame_end

        if truncate_at is not None:
            # 唯一允许的修改：截掉物理文件末端的不完整帧（长度为 0 时删文件）。
            fd = os.open(self.wal_path, os.O_WRONLY)
            try:
                os.ftruncate(fd, truncate_at)
                os.fsync(fd)
            finally:
                os.close(fd)
            dirfd = os.open(self.data_dir, os.O_RDONLY)
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)

    # -- 追加 --------------------------------------------------------------

    def append_batch(self, seen_seq: int, records: List[int]) -> int:
        """乐观并发追加。

        seen_seq 必须等于当前下一序号；否则抛 ConflictError，不写字节、
        不占序号。完整帧 fsync 成功后才更新内存并返回新的下一序号。
        """
        with self._lock:
            self._raise_if_poisoned()
            if not isinstance(seen_seq, int) or isinstance(seen_seq, bool):
                raise FrameFormatError("seen_seq must be an integer")
            if not isinstance(records, list) or not (
                1 <= len(records) <= MAX_RECORDS_PER_BATCH
            ):
                raise FrameFormatError("records must be a list of 1..32 integers")
            for v in records:
                if not isinstance(v, int) or isinstance(v, bool):
                    raise FrameFormatError("records must be integers")

            current_next = len(self._records) + 1
            if seen_seq != current_next:
                raise ConflictError(
                    f"seen_seq {seen_seq} is stale; current next seq is {current_next}"
                )

            frame = encode_frame(current_next, records)

            # 先在内存之外完成一次完整的 write + fsync，成功后才提交序号。
            pre_existed = os.path.exists(self.wal_path)
            fd = os.open(self.wal_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            start_offset = os.fstat(fd).st_size
            try:
                view = memoryview(frame)
                written = 0
                while written < len(view):
                    n = os.write(fd, view[written:])
                    if n <= 0:
                        raise OSError("short write to WAL")
                    written += n
                os.fsync(fd)
            except BaseException:
                # 活动进程内写失败：把可能残留的半帧截回写入前偏移，保证
                # 失败批次不留下任何字节；连截回都失败才进入 poisoned。
                # （进程被 SIGKILL 的情形不写标记，由下次启动截尾恢复。）
                recover_failed = False
                try:
                    os.ftruncate(fd, start_offset)
                    os.fsync(fd)
                except OSError:
                    recover_failed = True
                os.close(fd)
                if recover_failed:
                    self._poison(
                        "partial frame could not be rolled back after write failure"
                    )
                raise
            else:
                os.close(fd)
                if not pre_existed:
                    dirfd = os.open(self.data_dir, os.O_RDONLY)
                    try:
                        os.fsync(dirfd)
                    finally:
                        os.close(dirfd)

            self._records.extend(records)
            return len(self._records) + 1

    # -- 读取 --------------------------------------------------------------

    def read_records(self, cursor: int, limit: int) -> Tuple[List[Tuple[int, int]], int]:
        """返回 ([(seq, dose), ...], next_cursor)。cursor 为首条期望序号（>=1）。"""
        with self._lock:
            self._raise_if_poisoned()
            if not isinstance(cursor, int) or isinstance(cursor, bool) or cursor < 1:
                raise FrameFormatError("cursor must be a positive integer")
            if not isinstance(limit, int) or isinstance(limit, bool) or not (
                1 <= limit <= 1000
            ):
                raise FrameFormatError("limit must be an integer in 1..1000")
            n = len(self._records)
            start = cursor - 1
            if start > n:
                # 不允许跳空读取，避免调用方据此误以为中间存在连续记录。
                raise FrameFormatError(
                    f"cursor {cursor} is beyond the ledger end {n + 1}"
                )
            end = min(start + limit, n)
            items = [
                (start + i + 1, self._records[start + i])
                for i in range(end - start)
            ]
            return items, end + 1


class ConflictError(RuntimeError):
    """seen_seq 与账本当前下一序号不一致（乐观抢占失败）。"""
