#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略守护脚本（Layer 2）：拉起 BTC/ETH 两个实例并持续做"业务级"健康检查。

为什么需要它：
  系统服务 / 进程管理器（systemd、Windows 服务等）只在"进程退出"时才重拉。
  但僵尸是"进程活着、内部死了"（engine 未启 / 循环线程死 / 不交易），
  系统服务看不见，不会动它。本脚本探 /api/status，发现业务级死亡就杀掉重拉。

健康检查规则（GET /api/status）：
  HTTP 200 且 status in (running, ready) 且 trading_enabled=True 且 errors 为空 → 健康
  其余（含 503 engine 未启、status=error、trading=False、有 errors）→ 不健康

防误杀 / 防抖动：
  - 子进程启动后给 GRACE 宽限期，期间不判死刑（让自动启动完成 / 测试网重试）
  - 进程真的退出 → 立即重拉（不计入 MAX_FAILS）
  - 进程活着但不健康 → 连续 MAX_FAILS 次才重拉

用法：
  python run_all.py                       # 前台运行，Ctrl+C 退出（先终止子进程）
  STRAT_PYTHON=/path/python.exe run_all.py  # 指定子进程解释器

以后加新币：往 INSTANCES 里加一项即可。
"""
import os
import sys
import time
import json
import signal
import subprocess
import urllib.request
import urllib.error
import logging

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "supervisor.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("supervisor")

# 子进程解释器：优先环境变量，否则回退到已知可用的系统 Python（含 requests/websockets/flask）
PYTHON = os.environ.get(
    "STRAT_PYTHON",
    r"C:/Users/张无忌/AppData/Local/Programs/Python/Python314/python.exe",
)

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# 实例配置（以后加新币，往这里加一项即可）
INSTANCES = [
    {"name": "btc", "symbol": "BTC_USDC", "port": 5050, "trade_size": 100},
    {"name": "eth", "symbol": "ETH_USDC", "port": 5051, "trade_size": 100},
]

CHECK_INTERVAL = 60   # 健康检查间隔（秒）
GRACE = 60            # 子进程启动后的宽限期（秒），期间不判死刑
MAX_FAILS = 3         # 连续不健康次数达到才重拉


def _launch(inst):
    """启动一个子进程，返回 Popen。stdout/stderr 追加写入 logs/<name>.log。"""
    log_path = os.path.join(LOG_DIR, f"{inst['name']}.log")
    logf = open(log_path, "a", encoding="utf-8")
    cmd = [
        PYTHON, "app.py",
        "--symbol", inst["symbol"],
        "--port", str(inst["port"]),
        "--trade-size", str(inst["trade_size"]),
    ]
    logger.info("启动 %s: %s", inst["name"], " ".join(cmd))
    p = subprocess.Popen(
        cmd,
        cwd=PROJECT_DIR,
        stdout=logf,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
    )
    p._logf = logf
    return p


def _is_healthy(inst, proc):
    """探 /api/status，返回 (healthy: bool, detail: str)。"""
    if proc.poll() is not None:
        return False, f"进程已退出 code={proc.returncode}"
    url = f"http://127.0.0.1:{inst['port']}/api/status"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            if resp.status != 200:
                return False, f"HTTP {resp.status}"
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return False, f"请求失败: {e}"
    status = data.get("status")
    trading = data.get("trading_enabled")
    errors = data.get("errors") or []
    if status in ("running", "ready") and trading is True and not errors:
        return True, f"status={status} trading={trading}"
    return False, f"status={status} trading={trading} errors={errors}"


def main():
    procs = {}
    fails = {}
    boot_time = {}
    stop = {"flag": False}

    def _close_log(p):
        if p and getattr(p, "_logf", None):
            try:
                p._logf.close()
            except Exception:
                pass

    def shutdown(signum, frame):
        logger.info("收到退出信号，终止子进程...")
        stop["flag"] = True
        for inst in INSTANCES:
            p = procs.get(inst["name"])
            if p and p.poll() is None:
                p.terminate()
        time.sleep(2)
        for inst in INSTANCES:
            p = procs.get(inst["name"])
            if p and p.poll() is None:
                p.kill()
        for inst in INSTANCES:
            _close_log(procs.get(inst["name"]))
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # 首次启动所有实例
    for inst in INSTANCES:
        procs[inst["name"]] = _launch(inst)
        fails[inst["name"]] = 0
        boot_time[inst["name"]] = time.time()

    logger.info("守护脚本已启动，监控 %d 个实例（Ctrl+C 退出）", len(INSTANCES))

    while not stop["flag"]:
        time.sleep(CHECK_INTERVAL)
        for inst in INSTANCES:
            p = procs.get(inst["name"])

            # 情况 1：未启动或进程已死 → 立即重拉（不计入 MAX_FAILS）
            if p is None or p.poll() is not None:
                if p is not None and p.poll() is not None:
                    logger.error("%s 进程已退出(code=%s)，立即重拉",
                                 inst["name"], p.returncode)
                _close_log(p)
                procs[inst["name"]] = _launch(inst)
                fails[inst["name"]] = 0
                boot_time[inst["name"]] = time.time()
                continue

            # 情况 2：进程活着，探业务健康
            healthy, detail = _is_healthy(inst, p)
            if healthy:
                if fails[inst["name"]]:
                    logger.info("%s 恢复健康 (%s)", inst["name"], detail)
                fails[inst["name"]] = 0
                continue

            in_grace = (time.time() - boot_time[inst["name"]]) < GRACE
            fails[inst["name"]] += 1
            fc = fails[inst["name"]]

            if in_grace:
                logger.warning("%s 启动宽限期内不健康: %s", inst["name"], detail)
                continue

            if fc >= MAX_FAILS:
                logger.error("%s 连续 %d 次不健康，重拉: %s",
                             inst["name"], fc, detail)
                if p.poll() is None:
                    p.terminate()
                time.sleep(2)
                if p.poll() is None:
                    p.kill()
                _close_log(p)
                procs[inst["name"]] = _launch(inst)
                fails[inst["name"]] = 0
                boot_time[inst["name"]] = time.time()
            else:
                logger.warning("%s 不健康(%d/%d): %s",
                               inst["name"], fc, MAX_FAILS, detail)


if __name__ == "__main__":
    main()
