#!/usr/bin/env python3
"""容器健康检查：/healthz 返回 200 视为健康；503（poisoned）视为不健康。"""

import json
import os
import sys
import urllib.request

port = os.environ.get("PORT", "8080")
try:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as r:
        if r.status == 200:
            sys.exit(0)
        sys.exit(1)
except Exception:
    sys.exit(1)
