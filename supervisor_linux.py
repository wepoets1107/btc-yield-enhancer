#!/usr/bin/env python3
"""
BTC 收益增强策略 - Linux 侧自愈守护 (supervisor_linux)

职责：
- 每 CHECK_INTERVAL 秒检查 http://127.0.0.1:5050/btc-enhancer/api/status
- 引擎未初始化(503)或 status != running → 自动 POST /api/start 拉起（幂等安全）
- 存在 stop.flag 标记文件时跳过自愈（供人工停机逃生通道）
- 自身日志写入 logs/supervisor_linux.log（滚动, 2MB×3）

背景：
systemd 仅启动 app.py（Flask 外壳），不会自动 POST /api/start 初始化交易引擎。
服务重启后 engine=None，导致 /api/status 长期 503、策略空转。
本守护模拟 Windows supervisor.py 的语义，在 Linux 上补齐自愈能力。

部署：注册为 systemd service（见 /etc/systemd/system/btc-yield-enhancer-supervisor.service）。
"""

import os
import json
import time
import logging
import logging.handlers
import urllib.request
import urllib.error

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
HOST, PORT = "127.0.0.1", 5050
CHECK_INTERVAL = 15
STATUS_URL = f"http://{HOST}:{PORT}/btc-enhancer/api/status"
START_URL = f"http://{HOST}:{PORT}/btc-enhancer/api/start"
STOP_FLAG = os.path.join(WORK_DIR, "stop.flag")


def _setup_logging():
    log_dir = os.path.join(WORK_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "supervisor_linux.log"),
        maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] supervisor_linux: %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers):
        root.addHandler(fh)


def get_status():
    """返回 status dict；连不上返回 None；503(引擎未初始化)也解析 body 返回。"""
    try:
        with urllib.request.urlopen(STATUS_URL, timeout=4) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8"))
        except (ValueError, OSError):
            return None
    except (urllib.error.URLError, OSError, ValueError):
        return None


def post_start():
    try:
        req = urllib.request.Request(START_URL, method="POST", data=b"")
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status
    except (urllib.error.URLError, OSError):
        return None


def main():
    _setup_logging()
    logging.info("=== Linux supervisor 启动, 监控 %s:%d ===", HOST, PORT)
    while True:
        try:
            if os.path.exists(STOP_FLAG):
                time.sleep(CHECK_INTERVAL)
                continue
            st = get_status()
            if st is None:
                logging.warning("/api/status 无响应(服务可能未起), 下一轮再查")
            elif st.get("status") != "running":
                logging.info("引擎 status=%s, 触发 POST /api/start", st.get("status"))
                code = post_start()
                logging.info("POST /api/start -> %s", code)
        except Exception as e:
            logging.error("supervisor 主循环异常: %s", e)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
