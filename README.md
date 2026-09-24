# 束流站剂量账本 (Beam Dose Ledger)

成批落盘、WAL 帧校验、崩溃截尾恢复、损坏隔离 (poisoned) 与乐观并发抢占服务。

## 帧格式（每个批次一帧，独立自描述）

```
+----------------+------------------+--------------------+------------------+
| magic  (8B)    | length (8B, BE)  | payload (length B) | sha256 (32B)     |
| "BLWALFR1"     | 载荷字节长度      | 规范 JSON (UTF-8)   | 尾标             |
+----------------+------------------+--------------------+------------------+
```

- 尾标 `sha256 = SHA-256(magic || length || payload)`。
- 规范载荷：键排序、无空白、UTF-8 的 JSON：`{"records":[...],"seq":N}`，
  `seq` 为本批第一条记录的序号（即调用方“所见下一序号”），全库从 1 起连续。
- 长度字段为 64 位大端整数；单帧载荷上限 1 MiB（1–32 条整数远小于此）。

## 恢复规则（启动时顺序扫描 WAL）

1. **仅**当帧位于物理文件末端且字节不完整（帧头或载荷/尾标被截断）时，才允许
   `ftruncate` 截掉该不完整尾帧，并持久化截断结果。
2. 边界完整的帧 SHA-256 校验失败、magic 非法、长度非法、载荷非规范/语义非法，
   或帧间序号不连续（中部结构损坏）→ 账本进入 **poisoned**：
   - 原子写入 `wal.poison` 标记（含原因与偏移），重启后仍保持 poisoned；
   - 禁止一切追加与读取（`/healthz` 及业务接口均返回 503），不返回任何
     可能误导的记录；损坏的 WAL 原样保留以便取证。

## HTTP API

- `POST /api/batches`，请求 `{"seen_seq": N, "records": [整数, ...]}`（1–32 条）。
  帧 `write + fsync` 完整持久化后才返回 200；`seen_seq` 不等于当前下一序号时返回
  409，不写入任何字节、不占用序号。
- `GET /api/records?cursor=N&limit=L`：按游标（首条期望序号，从 1 起）返回连续
  记录 `{"records":[{"seq":..,"dose":..}], "next_cursor":..}`。
- `GET /healthz`：健康检查，poisoned 时为 503。

## Docker Compose

```bash
APP_PORT=18080 docker compose up --build            # 可配置宿主端口
docker compose run --build verify                   # 一次性校验：单元/构建/恢复/并发/冒烟
# 或： docker compose up --build --abort-on-container-exit --exit-code-from verify
```

`verify` 服务以自身退出码证明：截尾恢复、损坏隔离、并发抢占、HTTP 冒烟全部通过。
