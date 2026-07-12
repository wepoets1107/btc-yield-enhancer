"""
BTC 收益增强策略 - Flask Web 应用

- REST API：策略启停、连接测试
- WebSocket /ws：实时推送策略状态
- 前端仪表盘 HTML
"""

import os
import json
import logging
import threading
import time as pytime

from flask import Flask, jsonify, request, make_response
from flask_sock import Sock

from strategy_engine import StrategyEngine
from deribit_api import DeribitClient

logger = logging.getLogger(__name__)

ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")


def _load_env_file():
    """从 .env 文件加载环境变量（在读取 os.environ 之前调用）"""
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())


_load_env_file()


def _save_env(client_id, client_secret):
    """将 Deribit 凭证写回 .env 文件，保证重启后不丢失"""
    try:
        lines = []
        if os.path.exists(ENV_FILE):
            with open(ENV_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
        found_id = found_secret = False
        for i, line in enumerate(lines):
            if line.startswith("DERIBIT_ID="):
                lines[i] = f"DERIBIT_ID={client_id}\n"
                found_id = True
            elif line.startswith("DERIBIT_SECRET="):
                lines[i] = f"DERIBIT_SECRET={client_secret}\n"
                found_secret = True
        if not found_id:
            lines.append(f"DERIBIT_ID={client_id}\n")
        if not found_secret:
            lines.append(f"DERIBIT_SECRET={client_secret}\n")
        with open(ENV_FILE, "w", encoding="utf-8") as f:
            f.writelines(lines)
        logger.info(".env file updated")
    except Exception as e:
        logger.error("Failed to save .env: %s", e)

# ---------------------------------------------------------------------------
# API 密钥从环境变量读取（不写入代码明文）
# ---------------------------------------------------------------------------
DERIBIT_CLIENT_ID = os.environ.get("DERIBIT_ID", "")
DERIBIT_CLIENT_SECRET = os.environ.get("DERIBIT_SECRET", "")
USE_TESTNET = os.environ.get("DERIBIT_TESTNET", "1") == "1"

if not DERIBIT_CLIENT_ID or not DERIBIT_CLIENT_SECRET:
    raise RuntimeError(
        "请设置环境变量 DERIBIT_ID 和 DERIBIT_SECRET\n"
        "Linux/Mac: export DERIBIT_ID=xxx && export DERIBIT_SECRET=xxx\n"
        "Windows:   set DERIBIT_ID=xxx && set DERIBIT_SECRET=xxx\n"
        "或在项目根目录创建 .env 文件"
    )

# ---------------------------------------------------------------------------
# Flask + WebSocket
# ---------------------------------------------------------------------------
app = Flask(__name__)
sock = Sock(app)

engine: StrategyEngine = None
engine_lock = threading.Lock()
ws_clients = set()         # 已连接的 WebSocket 客户端
ws_clients_lock = threading.Lock()

# ---------------------------------------------------------------------------
# WebSocket 广播
# ---------------------------------------------------------------------------

def broadcast_state(state: dict):
    """向所有连接的 WebSocket 客户端推送状态"""
    payload = json.dumps(state, ensure_ascii=False, default=str)
    # 复制客户端列表后立即解锁，避免发送时阻塞其他操作
    with ws_clients_lock:
        clients = list(ws_clients)
    dead = []
    for client in clients:
        try:
            client.send(payload)
        except Exception:
            dead.append(client)
    if dead:
        with ws_clients_lock:
            for c in dead:
                ws_clients.discard(c)


@sock.route("/ws")
def ws_handler(ws):
    """WebSocket 连接处理"""
    with ws_clients_lock:
        ws_clients.add(ws)
    logger.info("WebSocket client connected (%d total)", len(ws_clients))

    # 首次连接立即发送当前状态
    if engine:
        try:
            state = engine.get_state()
            ws.send(json.dumps(state, ensure_ascii=False, default=str))
        except Exception:
            pass

    # 保持连接，等待服务端推送
    try:
        while True:
            msg = ws.receive()
            if msg is None:
                break
    except Exception:
        pass
    finally:
        with ws_clients_lock:
            ws_clients.discard(ws)
        logger.info("WebSocket client disconnected (%d left)", len(ws_clients))


# ---------------------------------------------------------------------------
# 策略状态变更回调（每个策略循环结束时触发）
# ---------------------------------------------------------------------------

def on_state_update(state: dict):
    """策略引擎状态变更时，广播给所有 WebSocket 客户端"""
    broadcast_state(state)


# ---------------------------------------------------------------------------
# 页面路由
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """仪表盘主页"""
    html_path = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")
    with open(html_path, "r", encoding="utf-8") as f:
        content = f.read()
    resp = make_response(content)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------

@app.route("/api/status")
def api_status():
    if engine is None:
        return jsonify({"error": "Strategy engine not initialized"}), 503
    with engine_lock:
        state = engine.get_state()
    return jsonify(state)


@app.route("/api/init", methods=["POST"])
def api_init():
    """初始化引擎（连接+拉数据）。如果引擎已在运行，保持不动。"""
    global engine
    with engine_lock:
        if engine and engine._running:
            # 引擎已经在跑（不论是否交易中）→ 保持现状，不碰
            return jsonify({"success": True, "message": "Engine already running", "status": engine.status})
        # 没有引擎 → 新建并初始化（就绪状态，不交易）
        body = request.get_json(silent=True) or {}
        use_testnet = body.get("testnet", USE_TESTNET)
        engine = StrategyEngine(
            DERIBIT_CLIENT_ID, DERIBIT_CLIENT_SECRET,
            testnet=use_testnet,
            state_callback=on_state_update,
        )
        if not engine.initialize():
            return jsonify({"success": False, "message": "Initialization failed"}), 500
    # 等数据就绪再返回
    import time as pytime
    for _ in range(20):
        if engine.status == "ready":
            break
        pytime.sleep(0.5)
    return jsonify({"success": True, "message": "Engine initialized", "status": engine.status})


@app.route("/api/start", methods=["POST"])
def api_start():
    """启动交易"""
    global engine
    with engine_lock:
        # 如果引擎不存在或已停止，先初始化
        if engine is None or not engine._running:
            if engine:
                old_anchor = engine.anchor_price
            else:
                old_anchor = None
            engine = StrategyEngine(
                DERIBIT_CLIENT_ID, DERIBIT_CLIENT_SECRET,
                testnet=USE_TESTNET,
                state_callback=on_state_update,
            )
            if old_anchor and old_anchor > 0:
                engine.anchor_price = old_anchor
            if not engine.initialize():
                return jsonify({"success": False, "message": "Initialization failed"}), 500
        if engine._trading_enabled:
            return jsonify({"success": False, "message": "Already trading"})
        success = engine.start()
    return jsonify({
        "success": success,
        "message": "Trading started" if success else "Failed to start trading",
    })


@app.route("/api/stop", methods=["POST"])
def api_stop():
    if engine is None:
        return jsonify({"error": "No strategy running"}), 400
    engine.stop()
    return jsonify({"success": True, "message": "Strategy stopping"})


@app.route("/api/credentials", methods=["GET", "POST"])
def api_credentials():
    """GET: 返回当前 API 凭证（ID 脱敏）；POST: 更新凭证并重建连接"""
    global DERIBIT_CLIENT_ID, DERIBIT_CLIENT_SECRET, engine

    if request.method == "GET":
        masked = DERIBIT_CLIENT_ID[:4] + "****" if len(DERIBIT_CLIENT_ID) > 4 else "****"
        return jsonify({
            "client_id_masked": masked,
            "testnet": USE_TESTNET,
        })

    # POST — 更新凭证，停旧引擎，重建连接
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No data"}), 400

    new_id = data.get("client_id", "").strip()
    new_secret = data.get("client_secret", "").strip()

    if not new_id or not new_secret:
        return jsonify({"success": False, "message": "ID 和 Secret 不能为空"}), 400

    with engine_lock:
        if engine:
            engine.stop()
            engine = None

        DERIBIT_CLIENT_ID = new_id
        DERIBIT_CLIENT_SECRET = new_secret

        # 写回 .env 文件，保证重启后不丢失
        _save_env(new_id, new_secret)

        logger.info("API credentials updated, reinitializing...")
        engine = StrategyEngine(
            DERIBIT_CLIENT_ID, DERIBIT_CLIENT_SECRET,
            testnet=USE_TESTNET,
            state_callback=on_state_update,
        )
        if not engine.initialize():
            return jsonify({"success": False, "message": "新凭证连接失败"}), 500

    return jsonify({"success": True, "message": "已切换凭证并重新连接"})


@app.route("/api/params", methods=["GET", "POST"])
def api_params():
    """GET: 返回当前可编辑参数列表  POST: 运行时修改参数"""
    global engine

    if request.method == "GET":
        if engine is None:
            return jsonify({"editable": False, "message": "Engine not initialized"}), 503
        with engine_lock:
            cfg = engine.cfg
            return jsonify({
                "editable": True,
                "anchor_price": engine.anchor_price,
                "trade_size_usdc": cfg["trade_size_usdc"],
                "rv_min": cfg["rv_min"],
                "rv_max": cfg["rv_max"],
                "rv_update_interval_minutes": cfg.get("rv_update_interval_minutes", 60),
                "poll_interval": cfg["poll_interval"],
                "cooldown_seconds": cfg.get("cooldown_seconds", 60),
                "min_poll_balance_usdc": cfg["min_poll_balance_usdc"],
            })

    # POST — 修改参数
    if engine is None:
        return jsonify({"error": "Engine not initialized"}), 503
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No data"}), 400

    changed = []
    with engine_lock:
        # 整数型参数（带合理性校验）
        param_ranges_int = {
            "poll_interval": (5, 300),
            "cooldown_seconds": (10, 600),
            "rv_update_interval_minutes": (5, 1440),
        }
        for key, (lo, hi) in param_ranges_int.items():
            if key in data:
                val = int(data[key])
                if lo <= val <= hi:
                    engine.cfg[key] = val
                    changed.append(f"{key}={val}")

        # 浮点型参数（带合理性校验）
        param_ranges_float = {
            "trade_size_usdc": (10, 10000),
            "rv_min": (0.0001, 0.05),
            "rv_max": (0.001, 0.1),
            "min_poll_balance_usdc": (10, 10000),
        }
        for key, (lo, hi) in param_ranges_float.items():
            if key in data:
                val = float(data[key])
                if lo <= val <= hi:
                    engine.cfg[key] = val
                    changed.append(f"{key}={val}")

        # 锚点（特殊处理：需要重算阈值）
        if "anchor_price" in data:
            val = float(data["anchor_price"])
            if val > 0:
                engine.anchor_price = val
                engine._recalc_thresholds()
                changed.append(f"anchor=${val:.2f}")

    if changed:
        logger.info("Params updated: %s", ", ".join(changed))
        broadcast_state(engine.get_state())
    return jsonify({"success": True, "changed": changed})


@app.route("/api/kline")
def api_kline():
    """拉主网 BTC_USDC 现货 K 线（公共 API，无需鉴权，不受 testnet 开关影响）"""
    try:
        import requests as _requests
        end = int(pytime.time() * 1000)
        start = end - 7 * 86400 * 1000
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "public/get_tradingview_chart_data",
            "params": {
                "instrument_name": "BTC_USDC",
                "start_timestamp": start,
                "end_timestamp": end,
                "resolution": "5",
            },
        }
        resp = _requests.post(
            "https://www.deribit.com/api/v2/", json=payload, timeout=15
        )
        data = resp.json()
        return jsonify(data.get("result") or {"error": "no data"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/test-connection")
def api_test_connection():
    results = {}
    for label, testnet in [("mainnet", False), ("testnet", True)]:
        client = DeribitClient(DERIBIT_CLIENT_ID, DERIBIT_CLIENT_SECRET, testnet=testnet)
        info = client.check_connection()
        if info["connected"]:
            price = client.get_index_price("btc_usdc")
            info["btc_index_price"] = price
            try:
                usdc = client.get_account_summary(currency="USDC")
                if usdc:
                    info["usdc_balance"] = usdc.get("balance", 0)
            except Exception:
                pass
            try:
                btc = client.get_account_summary(currency="BTC")
                if btc:
                    info["btc_balance"] = btc.get("balance", 0)
            except Exception:
                pass
        results[label] = info
    return jsonify(results)


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    global engine
    if request.method == "GET":
        if engine is None:
            return jsonify({"error": "Engine not initialized"}), 503
        return jsonify(engine.cfg)
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No data"}), 400
    if engine and engine._running:
        return jsonify({"success": False, "message": "Cannot modify config while running"}), 400
    with engine_lock:
        if engine is None:
            use_testnet = data.get("testnet", USE_TESTNET)
            engine = StrategyEngine(DERIBIT_CLIENT_ID, DERIBIT_CLIENT_SECRET, testnet=use_testnet, state_callback=on_state_update)
        for key, val in data.items():
            if key in engine.cfg:
                engine.cfg[key] = val
    return jsonify({"success": True, "message": "Config updated"})


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("  BTC 收益增强策略 - Dashboard + WebSocket")
    print("  http://localhost:5050")
    print("=" * 60)
    app.run(host="127.0.0.1", port=5050, debug=False)
