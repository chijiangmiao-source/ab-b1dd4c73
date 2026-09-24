"""DoseLedger 单元测试：帧格式、截尾恢复、poisoned 隔离、并发抢占。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app"))

from ledger import (
    CHECKSUM_LEN,
    HEADER_LEN,
    MAGIC,
    ConflictError,
    DoseLedger,
    FrameFormatError,
    PoisonedError,
    encode_frame,
)


class LedgerTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="ledger-test-")
        self.wal = os.path.join(self.dir, "wal.bin")

    def tearDown(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)

    def open_wal(self, mode: str):
        return open(os.path.join(self.dir, "wal.bin"), mode)

    def read_wal(self) -> bytes:
        with self.open_wal("rb") as f:
            return f.read()

    def write_wal(self, data: bytes) -> None:
        with self.open_wal("wb") as f:
            f.write(data)


class FrameEncodingTests(LedgerTestBase):
    def test_frame_roundtrip_structure(self) -> None:
        frame = encode_frame(1, [10, -20, 30])
        self.assertEqual(frame[:8], MAGIC)
        length = int.from_bytes(frame[8:16], "big")
        payload = frame[16 : 16 + length]
        # 规范载荷：键排序、无空白。
        self.assertEqual(payload, b'{"records":[10,-20,30],"seq":1}')
        digest = frame[16 + length :]
        self.assertEqual(len(digest), CHECKSUM_LEN)
        self.assertEqual(
            digest, hashlib.sha256(MAGIC + frame[8:16] + payload).digest()
        )

    def test_validation_rules(self) -> None:
        for bad in ([], [1] * 33):
            with self.assertRaises(FrameFormatError):
                encode_frame(1, bad)
        with self.assertRaises(FrameFormatError):
            encode_frame(1, [True])  # type: ignore[list-item]
        with self.assertRaises(FrameFormatError):
            encode_frame(True, [1])  # type: ignore[arg-type]


class AppendAndReadTests(LedgerTestBase):
    def test_continuous_seqs_and_cursor_reads(self) -> None:
        led = DoseLedger(self.dir)
        self.assertEqual(led.next_seq(), 1)
        self.assertEqual(led.append_batch(1, [5, 6, 7]), 4)
        self.assertEqual(led.append_batch(4, [8]), 5)
        self.assertEqual(led.append_batch(5, list(range(9, 41))), 37)  # 32 条

        led2 = DoseLedger(self.dir)  # 重启重建
        self.assertEqual(led2.next_seq(), 37)
        items, nxt = led2.read_records(1, 1000)
        self.assertEqual([d for _, d in items], list(range(5, 41)))
        self.assertEqual([s for s, _ in items], list(range(1, 37)))
        self.assertEqual(nxt, 37)

        page, nxt = led2.read_records(2, 3)
        self.assertEqual(page, [(2, 6), (3, 7), (4, 8)])
        self.assertEqual(nxt, 5)

        # 尾游标：暂无可读记录时返回空段而非报错。
        empty, nxt2 = led2.read_records(37, 10)
        self.assertEqual(empty, [])
        self.assertEqual(nxt2, 37)

    def test_cursor_beyond_end_rejected(self) -> None:
        led = DoseLedger(self.dir)
        led.append_batch(1, [1])
        with self.assertRaises(FrameFormatError):
            led.read_records(3, 10)

    def test_conflict_leaves_no_bytes_and_no_seq(self) -> None:
        led = DoseLedger(self.dir)
        led.append_batch(1, [1, 2])
        size_after_first = os.path.getsize(self.wal)
        with self.assertRaises(ConflictError):
            led.append_batch(1, [9])  # 过期所见序号
        with self.assertRaises(ConflictError):
            led.append_batch(5, [9])  # 跳号
        # 失败者不留下任何字节。
        self.assertEqual(os.path.getsize(self.wal), size_after_first)
        self.assertEqual(led.next_seq(), 3)
        # 账本仍可继续正常追加。
        self.assertEqual(led.append_batch(3, [3]), 4)

    def test_concurrent_same_seen_seq_exactly_one_wins(self) -> None:
        led = DoseLedger(self.dir)
        n = 24
        results = []  # (winner_index?)
        lock = threading.Lock()

        def worker(i: int) -> None:
            try:
                led.append_batch(1, [100 + i])
                with lock:
                    results.append(("ok", i))
            except ConflictError:
                with lock:
                    results.append(("conflict", i))
            except Exception as exc:  # noqa: BLE001
                with lock:
                    results.append(("error", repr(exc)))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        wins = [r for r in results if r[0] == "ok"]
        self.assertEqual(len(wins), 1, results)
        self.assertEqual(len([r for r in results if r[0] == "conflict"]), n - 1)

        # 磁盘上恰有一帧，序号连续；重启后一致。
        led2 = DoseLedger(self.dir)
        items, _ = led2.read_records(1, 100)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0][0], 1)
        self.assertEqual(led2.next_seq(), 2)


class RecoveryTests(LedgerTestBase):
    def _seed(self, batches: list[list[int]]) -> None:
        led = DoseLedger(self.dir)
        seq = 1
        for recs in batches:
            seq = led.append_batch(seq, recs)

    def test_truncated_header_at_tail_is_trimmed(self) -> None:
        self._seed([[1, 2], [3]])
        good_size = os.path.getsize(self.wal)
        with self.open_wal("ab") as f:
            f.write(MAGIC[:3])  # 连帧头都不完整的残帧
        led = DoseLedger(self.dir)  # 重启
        self.assertFalse(led.poisoned)
        self.assertEqual(led.next_seq(), 4)
        self.assertEqual(os.path.getsize(self.wal), good_size)

    def test_truncated_payload_at_tail_is_trimmed(self) -> None:
        self._seed([[1, 2], [3]])
        good_size = os.path.getsize(self.wal)
        with self.open_wal("ab") as f:
            f.write(MAGIC)
            f.write((5000).to_bytes(8, "big"))
            f.write(b'{"records":[9]')  # 载荷半截，无尾标
        led = DoseLedger(self.dir)
        self.assertFalse(led.poisoned)
        self.assertEqual(os.path.getsize(self.wal), good_size)
        self.assertEqual(led.next_seq(), 4)
        # 截尾后可以继续追加，序号连续。
        self.assertEqual(led.append_batch(4, [4]), 5)

    def test_truncated_checksum_at_tail_is_trimmed(self) -> None:
        self._seed([[1]])
        good_size = os.path.getsize(self.wal)
        frame = encode_frame(2, [2])
        with self.open_wal("ab") as f:
            f.write(frame[:-10])  # 尾标被截断
        led = DoseLedger(self.dir)
        self.assertFalse(led.poisoned)
        self.assertEqual(os.path.getsize(self.wal), good_size)

    def test_complete_frame_checksum_failure_poisons(self) -> None:
        self._seed([[1, 2], [3, 4]])
        data = bytearray(self.read_wal())
        # 翻转第一帧载荷中的一个字节（中部完整帧损坏）。
        data[HEADER_LEN + 2] ^= 0xFF
        self.write_wal(data)
        led = DoseLedger(self.dir)
        self.assertTrue(led.poisoned)
        self.assertIn("checksum mismatch", led.poison_reason or "")
        with self.assertRaises(PoisonedError):
            led.append_batch(5, [5])
        with self.assertRaises(PoisonedError):
            led.read_records(1, 10)
        # 毒化标记落盘，重启仍 poisoned。
        self.assertTrue(os.path.exists(os.path.join(self.dir, "wal.poison")))
        led2 = DoseLedger(self.dir)
        self.assertTrue(led2.poisoned)
        with self.assertRaises(PoisonedError):
            led2.append_batch(5, [5])
        # 损坏的 WAL 原样保留。
        self.assertEqual(self.read_wal(), bytes(data))

    def test_corrupted_last_complete_frame_also_poisons(self) -> None:
        # 末帧虽在文件末端，但边界完整、校验失败 -> 不得截尾，必须 poisoned。
        self._seed([[1, 2]])
        data = bytearray(self.read_wal())
        frame_len = len(encode_frame(1, [1, 2]))
        # 翻转末帧载荷一字节（末帧即第一帧）。
        idx = HEADER_LEN + 1
        data[idx] ^= 0x01
        self.write_wal(data)
        led = DoseLedger(self.dir)
        self.assertTrue(led.poisoned)
        # 损坏字节未被截断掩盖。
        self.assertEqual(os.path.getsize(self.wal), frame_len)

    def test_bad_magic_mid_file_poisons(self) -> None:
        self._seed([[1], [2]])
        data = bytearray(self.read_wal())
        first = encode_frame(1, [1])
        data[len(first)] = ord("X")  # 第二帧 magic 损坏
        self.write_wal(data)
        led = DoseLedger(self.dir)
        self.assertTrue(led.poisoned)
        self.assertIn("bad magic", led.poison_reason or "")
        with self.assertRaises(PoisonedError):
            led.read_records(1, 10)

    def test_sequence_gap_poisons(self) -> None:
        good = encode_frame(1, [1]) + encode_frame(2, [2])
        # 伪造一个校验自洽但序号跳跃的帧接在后面。
        forged = encode_frame(9, [9])
        with self.open_wal("wb") as f:
            f.write(good + forged)
        led = DoseLedger(self.dir)
        self.assertTrue(led.poisoned)
        self.assertIn("sequence gap", led.poison_reason or "")
        # 不得返回可能误导的记录：全部读取被拒。
        with self.assertRaises(PoisonedError):
            led.read_records(1, 10)

    def test_noncanonical_payload_poisons(self) -> None:
        # 手工构造字节等价但非规范的 JSON，并重算尾标 -> 结构损坏。
        payload = json.dumps({"seq": 1, "records": [1]}, indent=1).encode()
        frame = (
            MAGIC
            + len(payload).to_bytes(8, "big")
            + payload
            + hashlib.sha256(MAGIC + len(payload).to_bytes(8, "big") + payload).digest()
        )
        with self.open_wal("wb") as f:
            f.write(frame)
        led = DoseLedger(self.dir)
        self.assertTrue(led.poisoned)

    def test_empty_file_is_fresh_ledger(self) -> None:
        self.write_wal(b"")
        led = DoseLedger(self.dir)
        self.assertFalse(led.poisoned)
        self.assertEqual(led.next_seq(), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
