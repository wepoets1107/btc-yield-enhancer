"""
BTC 收益增强策略引擎 v2

核心逻辑（产品定义）：
1. 启动时扫描 USDC + BTC 余额，记录初始总值
2. 以当前指数价作为价格锚（不额外买入 BTC）
3. 计算 30 天日化 RV，限幅 1%~5%，每天 16:00 BJT 后更新
4. 价格涨过锚 × (1 + RV) → 卖出 200U 等值 BTC
   价格跌破锚 × (1 - RV) → 买入 200U 等值 BTC
5. 成交后更新锚为成交均价
6. 资金不足时暂停对应方向，恢复后自动恢复
7. 买卖方向独立，一方不足不影响另一方
"""

from __future__ import annotations

import time
import math
import json
import os
import logging
import threading
from datetime import datetime, timezone, timedelta
from statistics import stdev
from typing import Optional

from deribit_api import DeribitClient
from deribit_ws import DeribitWSClient

logger = logging.getLogger(__name__)

BJT = timezone(timedelta(hours=8))
STATE_FILE = os.path.join(os.path.dirname(__file__), "state.json")

# 从环境变量覆盖默认配置（由 app.py 的 --symbol/--trade-size 设置）
_ENV_INSTRUMENT = os.environ.get("STRAT_INSTRUMENT", "BTC_USDC")
_ENV_INDEX = os.environ.get("STRAT_INDEX", "btc_usdc")
_ENV_TRADE_SIZE = float(os.environ.get("STRAT_TRADE_SIZE", "100"))

DEFAULT_CONFIG = {
    "trade_size_usdc": _ENV_TRADE_SIZE,
    "rv_min": 0.005,
    "rv_max": 0.05,
    "rv_update_interval_minutes": 15,
    "poll_interval": 30,
    "cooldown_seconds": 60,
    "instrument_name": _ENV_INSTRUMENT,
    "index_name": _ENV_INDEX,
    "min_poll_balance_usdc": 200,
}

# 按标的隔离 state 文件，支持 ETH/BTC 各自独立
_STATE_SUFFIX = _ENV_INSTRUMENT.lower().replace("_", "")
STATE_FILE = os.path.join(os.path.dirname(__file__), f"state_{_STATE_SUFFIX}.json")


class StrategyEngine:
    """策略引擎 - 在后台线程运行"""

    def __init__(self, client_id, client_secret, config=None, testnet=False, state_callback=None):
        self.api = DeribitClient(client_id, client_secret, testnet=testnet)
        self.testnet = testnet
        self.cfg = {**DEFAULT_CONFIG, **(config or {})}

        # 从 instrument_name 推导现货币种（BTC_USDC → BTC, ETH_USDC → ETH）
        self._spot_currency = self.cfg["instrument_name"].split("_")[0]
        self._spot_currency_lower = self._spot_currency.lower()
        # 从 state.json 恢复运行时修改的配置（只恢复用户可调的键，不覆盖新默认值）
        _saved = self._load_state()
        if _saved and isinstance(_saved.get("config"), dict):
            for _k in ["rv_min", "rv_max", "trade_size_usdc", "cooldown_seconds"]:
                if _k in _saved["config"]:
                    self.cfg[_k] = _saved["config"][_k]
        self._state_callback = state_callback  # 状态变更回调（用于 WebSocket 推送）

        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

        # 合约规格
        self.contract_size = 0.0001
        self.min_trade_amount = 0.0001
        self.tick_size = 1.0

        # 策略状态
        self.status = "stopped"
        self.initial_total_usdc = 0.0     # 启动时总资产 USDC 价值
        self.initial_usdc = 0.0
        self.initial_btc = 0.0
        self.usdc_balance = 0.0
        self.btc_balance = 0.0
        self.btc_value_usdc = 0.0
        self.total_value_usdc = 0.0
        self.btc_index_price = 0.0
        self.anchor_price = 0.0
        self.daily_rv = self.cfg["rv_min"]
        self.upper_threshold = 0.0
        self.lower_threshold = 0.0
        self.rv_updated_today = False
        self.last_rv_update: Optional[str] = None
        self.usdc_insufficient = False
        self.btc_insufficient = False
        self.api_connected = False
        self.trades: list[dict] = []
        self.errors: list[dict] = []
        self.last_update: Optional[str] = None
        self.start_time: Optional[str] = None
        self.total_pnl = 0.0
        self.total_trades = 0
        self.btc_cost_basis = 0.0
        self._cooldown_until = 0.0        # 防频繁交易的冷却时间（秒时间戳）
        self._cooldown_seconds = 180       # 冷静期：每次成交后暂停 3 分钟
        self._trading_enabled = False      # 交易开关：就绪后默认不交易，用户点击"启动"才开
        self.open_orders: list[dict] = []  # 当前挂单列表
        self._our_buy_id: Optional[str] = None   # 我们挂的买入单 ID
        self._our_sell_id: Optional[str] = None  # 我们挂的卖出单 ID

        # WebSocket 客户端（实时数据源）
        self._ws: Optional[DeribitWSClient] = None
        self._ws_enabled = False
        self._last_ws_index_update = 0.0  # 最新一次从 WS 拿到指数价的时间戳
        self._last_ws_check_ts = 0.0     # 最后一次检查 WS 连接的时间戳

        logger.info("StrategyEngine v2 created")

    # ------------------------------------------------------------------
    # 状态持久化
    # ------------------------------------------------------------------

    STATE_BACKUP_DAYS = 7
    _state_backup_dir = os.path.join(os.path.dirname(STATE_FILE), "state_backups")

    def _save_state(self):
        """保存锚点、初始值、交易记录、运行时配置到文件，重启时恢复

        安全策略：
        1. 先写临时文件再 rename（原子写入，防止写一半崩溃）
        2. 保存后 git commit 到仓库（历史版本可回滚）
        3. 保留 7 天滚动备份
        """
        try:
            data = {
                "anchor_price": self.anchor_price,
                "initial_usdc": self.initial_usdc,
                "initial_btc": self.initial_btc,
                "initial_total_usdc": self.initial_total_usdc,
                "trades": self.trades[-200:],  # 保留最近 200 笔
                "total_trades": self.total_trades,
                "was_trading": self._trading_enabled,  # 重启后自动恢复交易
                "config": self.cfg,  # 运行时配置（含 API 修改的下限/上限等）
                "updated_at": datetime.now(BJT).isoformat(),
            }
            # 原子写入
            import tempfile
            tmp = tempfile.NamedTemporaryFile(
                mode="w", dir=os.path.dirname(STATE_FILE),
                delete=False, suffix=".tmp",
            )
            try:
                json.dump(data, tmp, default=str)
                tmp.flush()
                os.fsync(tmp.fileno())
                tmp.close()
                os.replace(tmp.name, STATE_FILE)
            except Exception:
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass
                raise

            # git commit（记录变更历史）
            self._git_save_state()

            # 7 天滚动备份
            self._rotate_backup(data)

        except Exception as e:
            logger.warning("Failed to save state: %s", e)

    def _git_save_state(self):
        """git add + commit state.json（保留完整变更历史）"""
        if not os.path.exists(os.path.join(os.path.dirname(STATE_FILE), ".git")):
            return
        try:
            import subprocess
            git_dir = os.path.dirname(STATE_FILE)
            state_rel = os.path.basename(STATE_FILE)
            # add
            subprocess.run(
                ["git", "add", state_rel],
                cwd=git_dir, capture_output=True, timeout=10,
            )
            # commit（允许空提交不做）
            subprocess.run(
                ["git", "commit", "-m",
                 f"state: anchor={self.anchor_price} trades={self.total_trades}"],
                cwd=git_dir, capture_output=True, timeout=10,
            )
        except Exception as e:
            logger.warning("git save state error: %s", e)

    def _rotate_backup(self, data):
        """保留 7 天的 state 滚动备份（按日期）"""
        try:
            import shutil
            backup_dir = self._state_backup_dir
            os.makedirs(backup_dir, exist_ok=True)
            today = datetime.now(BJT).strftime("%Y%m%d")
            backup_path = os.path.join(backup_dir, f"state.{today}.json")
            if not os.path.exists(backup_path):
                with open(backup_path, "w") as f:
                    json.dump(data, f, default=str)
            # 清理超过 7 天的备份
            cutoff = (datetime.now(BJT) - timedelta(days=7)).strftime("%Y%m%d")
            for fname in os.listdir(backup_dir):
                if fname.startswith("state.") and fname.endswith(".json"):
                    date_part = fname.split(".")[1]
                    if date_part < cutoff:
                        try:
                            os.remove(os.path.join(backup_dir, fname))
                        except Exception:
                            pass
        except Exception as e:
            logger.warning("state backup error: %s", e)

    @staticmethod
    def _load_state():
        """从文件恢复锚点和初始值"""
        try:
            if not os.path.exists(STATE_FILE):
                return None
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
            if data.get("anchor_price", 0) > 0:
                return data
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    def _round_amount(self, amount_btc):
        if self.contract_size <= 0:
            return round(amount_btc, 6)
        units = max(1, round(amount_btc / self.contract_size)) if amount_btc > 0 else 0
        raw = units * self.contract_size
        cs_str = f"{self.contract_size:.10f}".rstrip("0").rstrip(".")
        decimals = max(0, len(cs_str.split(".")[1]) if "." in cs_str else 0)
        return round(raw, decimals)

    def _round_price(self, price: float) -> float:
        """按 tick_size 取整价格（BTC=1, ETH=0.1）"""
        if self.tick_size <= 0:
            return round(price, 1)
        decimals = max(0, round(-math.log10(self.tick_size)))
        return round(round(price / self.tick_size) * self.tick_size, decimals)

    def _fetch_instrument_info(self):
        try:
            instruments = self.api.get_instruments(currency=self._spot_currency, kind="spot")
            for inst in instruments:
                if inst["instrument_name"] == self.cfg["instrument_name"]:
                    self.contract_size = float(inst.get("contract_size", 0.0001))
                    self.min_trade_amount = float(inst.get("min_trade_amount", 0.0001))
                    self.tick_size = float(inst.get("tick_size", 1.0))
                    logger.info("Instrument: contract=%.4f min_trade=%.4f",
                                self.contract_size, self.min_trade_amount)
                    return True
        except Exception as e:
            logger.error("Fetch instrument error: %s", e)
        return False

    # ------------------------------------------------------------------
    # 公共控制
    # ------------------------------------------------------------------

    def initialize(self):
        """初始化（连接 + 拉数据 + 设锚）— 不启动交易循环"""
        if self._running:
            return False
        # 只取消当前策略标的的订单（不碰合约、期权、其他币种）
        try:
            self.api.cancel_all_by_instrument(self.cfg["instrument_name"])
        except Exception:
            pass
        # 启动 WebSocket 客户端（后台线程）
        try:
            self._ws = DeribitWSClient(
                self.api.client_id, self.api.client_secret,
                testnet=self.testnet,
                callback=self._on_ws_message,
                channels=[
                    f"user.portfolio.{self._spot_currency_lower}",
                    "user.portfolio.usdc",
                    f"ticker.{self.cfg['instrument_name']}.index",
                ],
            )
            self._ws.start()
            self._ws_enabled = True
            logger.info("WS client started")
        except Exception as e:
            logger.warning("WS client start failed (will use REST only): %s", e)
            self._ws_enabled = False
        self._running = True
        self._trading_enabled = False
        self._our_buy_id = None
        self._our_sell_id = None
        self._thread = threading.Thread(target=self._data_loop, daemon=True)
        self._thread.start()
        return True

    def start(self):
        """启动交易（需先 initialize）"""
        if not self._running:
            return False
        if self._trading_enabled:
            return False
        self._trading_enabled = True
        self._save_state()  # 立即保存 was_trading=true，重启后可恢复
        self._log_info("=== 交易已启动 ===")
        # 状态由 _data_loop 在下一轮自动切换为 running
        self._notify_state()
        return True

    def stop(self):
        """停止一切：WS 断开 + 取消挂单 + 保存交易记录"""
        # 停 WS
        if self._ws:
            try:
                self._ws.stop()
            except Exception:
                pass
            self._ws = None
            self._ws_enabled = False
        # 先取消我们的挂单
        self._cancel_our_orders()
        self._save_state()
        self._running = False
        self._trading_enabled = False
        self._set_status("stopped")
        self._notify_state()
        return True

    def _cancel_our_orders(self):
        """取消我们挂出的 maker 单"""
        for oid in [self._our_buy_id, self._our_sell_id]:
            if oid:
                try:
                    self.api.cancel_order(oid)
                    self._log_info("Cancelled order %s on stop", oid)
                except Exception as e:
                    self._log_info("Cancel %s failed: %s", oid, e)
        self._our_buy_id = None
        self._our_sell_id = None

    def get_state(self):
        with self._lock:
            # FIFO 计算交易盈亏
            tp = self._calc_trading_pnl()
            return {
                "status": self.status,
                "initial_usdc": self.initial_usdc,
                "initial_btc": self.initial_btc,
                "initial_total_usdc": self.initial_total_usdc,
                "usdc_balance": self.usdc_balance,
                "btc_balance": self.btc_balance,
                "btc_value_usdc": self.btc_value_usdc,
                "total_value_usdc": self.total_value_usdc,
                "btc_index_price": self.btc_index_price,
                "anchor_price": self.anchor_price,
                "daily_rv": self.daily_rv,
                "upper_threshold": self.upper_threshold,
                "lower_threshold": self.lower_threshold,
                "rv_updated_today": self.rv_updated_today,
                "last_rv_update": self.last_rv_update,
                "usdc_insufficient": self.usdc_insufficient,
                "btc_insufficient": self.btc_insufficient,
                "api_connected": self.api_connected,
                "trading_enabled": self._trading_enabled,
                "trades": list(self.trades[-50:]),
                "errors": list(self.errors[-20:]),
                "open_orders": self.open_orders,
                "last_update": self.last_update,
                "start_time": self.start_time,
                "total_pnl": self.total_value_usdc - self.initial_total_usdc,
                "trading_pnl": tp,
                "total_trades": self.total_trades,
                "btc_cost_basis": self.btc_cost_basis,
                "config": {
                    "trade_size_usdc": self.cfg["trade_size_usdc"],
                    "rv_min": self.cfg["rv_min"],
                    "rv_max": self.cfg["rv_max"],
                    "rv_update_interval_minutes": self.cfg.get("rv_update_interval_minutes", 60),
                    "poll_interval": self.cfg["poll_interval"],
                    "cooldown_seconds": self.cfg.get("cooldown_seconds", 60),
                    "min_poll_balance_usdc": self.cfg["min_poll_balance_usdc"],
                    "instrument_name": self.cfg["instrument_name"],
                    "testnet": self.testnet,
                },
            }

    def _calc_trading_pnl(self):
        """FIFO 配对买卖，计算已实现的交易盈亏"""
        buy_q = [t["price"] for t in self.trades if t["side"] == "buy"]
        sell_q = [t for t in self.trades if t["side"] == "sell"]
        btc_left = [t["amount_btc"] for t in self.trades if t["side"] == "buy"]
        pnl = 0.0
        bi = 0
        for s in sell_q:
            amt_s = s["amount_btc"]
            while amt_s > 0.000001 and bi < len(btc_left):
                pair = min(btc_left[bi], amt_s)
                cost = pair * buy_q[bi]
                revenue = pair * s["price"]
                pnl += revenue - cost
                btc_left[bi] -= pair
                amt_s -= pair
                if btc_left[bi] < 0.000001:
                    bi += 1
        return round(pnl, 2)

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    def _data_loop(self):
        """数据同步 + 可选交易循环
        第一阶段：初始化连接、余额、锚点、RV → 进入"就绪"状态
        第二阶段：用户点"启动"后 → 开启交易信号检查
        """
        logger.info("=== Data loop started ===")
        self.start_time = datetime.now(BJT).isoformat()

        try:
            self._set_status("initializing")
            if not self._init_strategy():
                self._set_status("error")
                self._add_error("Strategy initialization failed")
                self._running = False
                return

            self._set_status("ready")
            self._log_info("就绪 — 数据同步中，等待启动交易")
            self._notify_state()

            while self._running:
                try:
                    # 动态更新状态：交易开关决定 status
                    target_status = "running" if self._trading_enabled else "ready"
                    if self.status != target_status:
                        logger.info("STATUS_CHANGE: %s -> %s", self.status, target_status)
                        self._set_status(target_status)

                    self._update_index_price()
                    self._fetch_balances()
                    self._check_rv_update()
                    self._check_funds()
                    self._fetch_open_orders()

                    # 只有用户点了"启动"才管理挂单
                    if self._trading_enabled:
                        self._manage_maker_orders()

                    self.last_update = datetime.now(BJT).isoformat()
                    self._notify_state()
                except Exception as e:
                    logger.error("Loop error: %s", e, exc_info=True)
                    self._add_error(f"Loop: {e}")
                time.sleep(self.cfg["poll_interval"])

        except Exception as e:
            logger.error("Fatal: %s", e, exc_info=True)
            self._add_error(f"Fatal: {e}")
            self._set_status("error")

        logger.info("=== Data loop ended ===")

    def _notify_state(self):
        """通知前端状态更新（WebSocket 回调）"""
        if self._state_callback:
            try:
                self._state_callback(self.get_state())
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 初始化（智能检测已有持仓）
    # ------------------------------------------------------------------

    def _init_strategy(self):
        """初始化：连接 → 查余额 → 判断是否有 BTC → 设锚"""
        logger.info("Initializing...")

        # 1. 连接
        conn = self.api.check_connection()
        self.api_connected = conn.get("connected", False)
        if not self.api_connected:
            logger.error("Cannot connect: %s", conn.get("auth_error", "unknown"))
            return False
        self._fetch_instrument_info()

        # 2. 查余额（仅用于校验，不做初始记录——初始值应该从 state.json 恢复）
        bal = self._fetch_balances()
        if bal is None:
            return False

        # 3. 获取指数价
        price = self.api.get_index_price(self.cfg["index_name"])
        if not price or price <= 0:
            logger.error("Cannot get index price")
            return False
        self.btc_index_price = price

        # 4. 锚点 + 初始值：优先恢复上次保存的值，首次部署才 snapshot
        saved = self._load_state()
        if saved and abs(saved["anchor_price"] - price) / price < 0.10:
            self.anchor_price = saved["anchor_price"]
            self.initial_usdc = saved.get("initial_usdc", bal["usdc_balance"])
            self.initial_btc = saved.get("initial_btc", bal["btc_balance"])
            self.initial_total_usdc = saved.get("initial_total_usdc",
                bal["usdc_balance"] + bal["btc_balance"] * price)
            # 恢复历史交易记录
            old_trades = saved.get("trades", [])
            if old_trades:
                self.trades = old_trades
                self.total_trades = saved.get("total_trades", len(old_trades))
                logger.info("Restored %d trades from saved state", len(old_trades))
            logger.info("Anchor restored from saved state: %.2f (current price: %.2f)",
                        self.anchor_price, price)
            # 重启后自动恢复交易状态
            if saved.get("was_trading"):
                self._trading_enabled = True
                logger.info("Trading auto-resumed from saved state")
        else:
            # 首次部署或 state 坏了：用当前 balance snapshot 作为初始值
            if self.initial_total_usdc == 0:
                self.initial_usdc = bal["usdc_balance"]
                self.initial_btc = bal["btc_balance"]
                self.initial_total_usdc = self.initial_usdc + self.initial_btc * price
                logger.info("Initial snapshot: USDC=%.2f BTC=%.6f total=$%.2f",
                            self.initial_usdc, self.initial_btc, self.initial_total_usdc)
            self.anchor_price = price
            logger.info("Anchor set to index price: %.2f", self.anchor_price)

        # 5. 计算 RV + 阈值
        self._update_rv()
        self._recalc_thresholds()
        self._fetch_balances()

        logger.info("Strategy initialized: anchor=%.2f rv=%.2f%% upper=%.2f lower=%.2f",
                    self.anchor_price, self.daily_rv * 100,
                    self.upper_threshold, self.lower_threshold)
        return True

    # ------------------------------------------------------------------
    # 轮询更新
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # RV 计算
    # ------------------------------------------------------------------

    def _check_rv_update(self):
        """每小时更新一次 RV"""
        if not self.last_rv_update:
            self._update_rv()
            return
        # 解析上次更新时间
        try:
            last = datetime.fromisoformat(self.last_rv_update)
        except Exception:
            self._update_rv()
            return
        elapsed = (datetime.now(BJT) - last).total_seconds()
        interval_sec = self.cfg.get("rv_update_interval_minutes", 60) * 60
        if elapsed >= interval_sec:
            self._update_rv()

    def _update_rv(self):
        rv = self._calculate_daily_rv()
        if rv is not None:
            old = self.daily_rv
            self.daily_rv = rv
            self.last_rv_update = datetime.now(BJT).isoformat()
            self.rv_updated_today = True
            self._recalc_thresholds()
            logger.info("RV: %.2f%% → %.2f%%", old * 100, rv * 100)

    def _calculate_daily_rv(self):
        """用主网 BTC_USDC 现货 5 分钟 K 线，取 12 根(1小时窗口)的 RMS × √24 作为日化 RV"""
        end = int(time.time() * 1000)
        start = end - 3 * 3600 * 1000  # 拉3小时确保有12根
        data = self._fetch_public_kline("BTC_USDC", start, end, "5")
        if not data or not data.get("close") or not data.get("open"):
            return self._fallback_rv()

        opens = [o for o in data["open"] if o and o > 0]
        closes = [c for c in data["close"] if c and c > 0]
        min_len = min(len(opens), len(closes))
        if min_len < 12:
            return self._fallback_rv()

        opens = opens[-12:]
        closes = closes[-12:]

        sq_sum = 0.0
        n = 0
        for i in range(len(opens)):
            if opens[i] > 0:
                r = (closes[i] - opens[i]) / opens[i]
                sq_sum += r * r
                n += 1

        if n < 12:
            return self._fallback_rv()

        rv = math.sqrt(sq_sum / n)
        rv_daily = rv * math.sqrt(24)  # 小时 RMS → 日化 RV
        return max(self.cfg["rv_min"], min(self.cfg["rv_max"], rv_daily))

    def _fallback_rv(self):
        return self.cfg["rv_min"]

    @staticmethod
    def _fetch_public_kline(instrument, start_ms, end_ms, resolution):
        """通过主网公共 API 获取 K 线数据（无需鉴权，不受 testnet 影响）"""
        try:
            import requests
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "public/get_tradingview_chart_data",
                "params": {
                    "instrument_name": instrument,
                    "start_timestamp": int(start_ms),
                    "end_timestamp": int(end_ms),
                    "resolution": resolution,
                },
            }
            resp = requests.post(
                "https://www.deribit.com/api/v2/", json=payload, timeout=15
            )
            data = resp.json()
            return data.get("result")
        except Exception as e:
            logger.warning("Fetch public kline failed: %s", e)
            return None

    # ------------------------------------------------------------------
    # WebSocket 回调（由 WS 线程调用）
    # ------------------------------------------------------------------

    def _on_ws_message(self, msg: dict):
        """WS 推送实时更新余额/指数价缓存"""
        try:
            channel = msg.get("channel", "")
            data = msg.get("data", {})
            if channel == f"user.portfolio.{self._spot_currency_lower}":
                bal = data.get("balance", 0)
                if bal is not None and float(bal) >= 0:
                    old = self.btc_balance
                    self.btc_balance = float(bal)
                    if abs(self.btc_balance - old) > 1e-6:
                        logger.info("WS[%s]: %.6f -> %.6f", self._spot_currency_lower, old, self.btc_balance)
                    if self.btc_index_price > 0:
                        self._recalc_values()
            elif channel == "user.portfolio.usdc":
                bal = data.get("balance", 0)
                if bal is not None and float(bal) >= 0:
                    old = self.usdc_balance
                    self.usdc_balance = float(bal)
                    if abs(self.usdc_balance - old) > 1e-6:
                        logger.info("WS[usdc]: %.2f -> %.2f", old, self.usdc_balance)
                    if self.btc_index_price > 0:
                        self.btc_value_usdc = self.btc_balance * self.btc_index_price
                        self.total_value_usdc = self.usdc_balance + self.btc_value_usdc
            elif "index" in channel:
                idx = data.get("index_price") or data.get("idx")
                if idx is not None and float(idx) > 0:
                    old = self.btc_index_price
                    self.btc_index_price = float(idx)
                    if abs(self.btc_index_price - old) > 0.1:
                        logger.info("WS[index]: %.2f -> %.2f", old, self.btc_index_price)
                    self._last_ws_index_update = time.time()
                    self._recalc_values()
                    self.api_connected = True
            # WS 有推送说明连接正常
            self.api_connected = True
            if channel not in ("heartbeat",):
                self._last_ws_check_ts = time.time()
        except Exception as e:
            logger.warning("WS callback error: %s", e)

    def _recalc_values(self):
        """根据当前余额和指数价重算 USDC 价值"""
        self.btc_value_usdc = self.btc_balance * self.btc_index_price
        self.total_value_usdc = self.usdc_balance + self.btc_value_usdc

    # ------------------------------------------------------------------
    # 价格与余额（优先 WS 缓存，WS 不可用时 fallback REST）
    # ------------------------------------------------------------------

    _INDEX_REST_INTERVAL = 15  # 指数价 REST 备用拉取间隔（秒）
    _last_index_rest_ts = 0.0

    def _update_index_price(self):
        """获取指数价：优先 WS 实时数据，WS 过期时 fallback REST"""
        now = time.time()
        # WS 有数据且在 30 秒内更新过，直接用
        if self._ws_enabled and self.btc_index_price > 0 and (now - self._last_ws_index_update) < 30:
            return
        # REST fallback（限制频率）
        if now - self._last_index_rest_ts < self._INDEX_REST_INTERVAL:
            return
        self._last_index_rest_ts = now
        try:
            price = self.api.get_index_price(self.cfg["index_name"])
        except Exception:
            price = None
        if price and price > 0:
            self.btc_index_price = price
            self._recalc_values()
            self.api_connected = True
        elif not self._ws_enabled or not self._ws or not self._ws.connected:
            self.api_connected = False

    def _fetch_balances(self):
        """获取余额：WS 在线时不调 REST（WS 实时推送已在 _on_ws_message 更新）；
        WS 不可用时 fallback REST。
        """
        if self._ws_enabled and self._ws and self._ws.connected and self._ws.authenticated:
            # WS 在线，不调 REST，余额已由 _on_ws_message 实时更新
            self.api_connected = True
            return {"usdc_balance": self.usdc_balance, "btc_balance": self.btc_balance}
        # WS 不可用，REST fallback
        try:
            usdc = self.api.get_account_summary(currency="USDC")
            if usdc:
                self.usdc_balance = float(usdc.get("balance", 0))
            spot_bal = self.api.get_account_summary(currency=self._spot_currency)
            if spot_bal:
                self.btc_balance = float(spot_bal.get("balance", 0))
            return {"usdc_balance": self.usdc_balance, "btc_balance": self.btc_balance}
        except Exception as e:
            logger.error("Fetch balances (REST fallback): %s", e)
            return None

    def _fetch_open_orders(self):
        """获取当前所有挂单（含部分成交）"""
        try:
            orders = self.api.get_open_orders(self.cfg["instrument_name"])
            parsed = []
            instrument = self.cfg["instrument_name"]
            for o in orders:
                # 过滤：只保留我们策略标的的挂单，防止其他币种混入
                if o.get("instrument_name", "") != instrument:
                    self._log_info("Ignored non-%s order: %s %s @ %s",
                                   instrument, o.get("instrument_name"),
                                   o.get("direction"), o.get("price"))
                    continue
                filled = float(o.get("filled_amount", 0) or 0)
                amount = float(o.get("amount", 0) or 0)
                parsed.append({
                    "order_id": o.get("order_id", ""),
                    "side": o.get("direction", ""),
                    "price": float(o.get("price", 0) or 0),
                    "amount": amount,
                    "filled": filled,
                    "remaining": amount - filled,
                    "state": o.get("order_state", ""),
                    "label": o.get("label", ""),
                    "time": o.get("creation_timestamp", ""),
                })
            self.open_orders = parsed
        except Exception as e:
            logger.error("Fetch open orders: %s", e)

    # ------------------------------------------------------------------
    # 资金检查（买卖方向独立）
    # ------------------------------------------------------------------

    def _check_funds(self):
        """资金保护：USDC 或 BTC 价值低于 $200 时暂停对应方向"""
        threshold = self.cfg["min_poll_balance_usdc"]  # $200
        price = self.btc_index_price

        # USDC 检查
        if self.usdc_balance < threshold:
            if not self.usdc_insufficient:
                self.usdc_insufficient = True
                self._log_info("USDC insufficient (%.2f < %.2f), buy paused",
                               self.usdc_balance, threshold)
        else:
            if self.usdc_insufficient:
                self.usdc_insufficient = False
                self._log_info("USDC restored (%.2f), buy resumed", self.usdc_balance)

        # BTC 检查（按市价折算 USDC）
        btc_value = self.btc_balance * price if price > 0 else 0
        if btc_value < threshold:
            if not self.btc_insufficient:
                self.btc_insufficient = True
                self._log_info("BTC insufficient ($%.2f < %.2f), sell paused",
                               btc_value, threshold)
        else:
            if self.btc_insufficient:
                self.btc_insufficient = False
                self._log_info("BTC restored ($%.2f), sell resumed", btc_value)

    # ------------------------------------------------------------------
    # 主动挂单管理 — 提前在阈值位置挂 maker 单，避免行情波动来不及成交
    # ------------------------------------------------------------------

    def _recalc_thresholds(self):
        rv = self.daily_rv
        self.upper_threshold = self._round_price(self.anchor_price * (1 + rv))
        self.lower_threshold = self._round_price(self.anchor_price * (1 - rv))

    def _manage_maker_orders(self):
        """每轮循环维护一对 maker 限价单：
        买入单 @ 下阈值，卖出单 @ 上阈值。
        成交后自动更新锚点并重挂新单。
        """
        if not self._trading_enabled:
            return
        anchor = self.anchor_price
        if anchor <= 0 or self.daily_rv <= 0:
            return

        buy_price = self._round_price(self.lower_threshold)
        sell_price = self._round_price(self.upper_threshold)
        trade_size = self.cfg["trade_size_usdc"]

        # 获取当前所有挂单的 ID 集合，以及按价格索引
        current_ids = {o["order_id"] for o in self.open_orders}
        orders_by_price = {}
        for o in self.open_orders:
            orders_by_price.setdefault(o["side"], {})[round(o["price"])] = o["order_id"]

        # --- 防重复兜底：交易所已有同价位的挂单，但我们没追踪 → 认领回来 ---
        for side, our_attr, target_price in [
            ("buy", "_our_buy_id", buy_price),
            ("sell", "_our_sell_id", sell_price),
        ]:
            our_id = getattr(self, our_attr)
            if not our_id:
                existing = orders_by_price.get(side, {}).get(target_price)
                if existing and existing in current_ids:
                    setattr(self, our_attr, existing)
                    self._log_info("Reclaimed %s order %s at price %d", side, existing, target_price)

        # --- 检测成交：订单消失后查 Deribit 订单状态判断是否真成交 ---
        # Bugfix v1.9: 不再依赖余额变化（期权估值会污染 BTC balance），
        # 改为直接查询 get_order_state 的 order_state 字段。
        for side_key, our_id_attr in [("sell", "_our_sell_id"), ("buy", "_our_buy_id")]:
            our_id = getattr(self, our_id_attr)
            if our_id and our_id not in current_ids:
                # 查 Deribit 订单状态确认是否成交
                is_filled = False
                try:
                    order_result = self.api.get_order_state(our_id)
                    if order_result["success"]:
                        state = (order_result["result"] or {}).get("order_state", "")
                        if state == "filled":
                            is_filled = True
                        elif state == "open":
                            # 交易所说还在，但 get_open_orders 没返回——可能是 API 延迟，跳过本轮
                            self._log_info("%s order %s missing from open list but state=open, skipping", side_key, our_id)
                            continue
                        elif state == "cancelled":
                            # 明确取消了
                            pass
                    # 如果 success=False 或 result 为空 → 订单已不存在，视为取消
                except Exception as e:
                    self._log_info("%s order %s get_order_state failed: %s", side_key, our_id, e)
                    # API 失败时回退：不处理，留到下一轮再说
                    continue

                if not is_filled:
                    self._log_info("%s order %s was cancelled/removed (not filled)", side_key, our_id)
                    setattr(self, our_id_attr, None)
                    continue

                self._log_info("%s maker order %s was filled!", side_key, our_id)
                setattr(self, our_id_attr, None)
                # 触发冷静期
                self._cooldown_until = time.time() + self._cooldown_seconds
                self._log_info("Cooldown activated: %ds", self._cooldown_seconds)
                # 从交易所拉实际成交价（比阈值价更准确）
                fill_price = sell_price if side_key == "sell" else buy_price  # 默认值
                trade_amount = self.cfg["trade_size_usdc"] / fill_price
                try:
                    order_result = self.api.get_order_state(our_id)
                    if order_result["success"]:
                        parsed = self.api.parse_order_result(order_result["result"] or {})
                        if parsed["average_price"] > 0:
                            fill_price = parsed["average_price"]
                            trade_amount = parsed["filled_amount"]
                            self._log_info("Actual fill: %.6f BTC @ %.2f", trade_amount, fill_price)
                except Exception:
                    pass
                self.anchor_price = fill_price
                # 成交后立刻重算 RV，新挂单直接用最新波动率
                self._update_rv()
                self._recalc_thresholds()
                self._fetch_balances()
                # 记录成交
                with self._lock:
                    self.trades.append({
                        "id": f"{'B' if side_key == 'buy' else 'S'}{int(time.time())}",
                        "time": datetime.now(BJT).isoformat(),
                        "side": side_key,
                        "amount_btc": round(trade_amount, 6),
                        "price": fill_price,
                        "total_usdc": round(trade_amount * fill_price, 2),
                        "order_id": our_id,
                        "status": "filled",
                        "label": "maker",
                    })
                    self.total_trades += 1
                self._save_state()
                # 取消对侧挂单（价位已经变了）
                other_id = self._our_buy_id if side_key == "sell" else self._our_sell_id
                if other_id:
                    self.api.cancel_order(other_id)
                    setattr(self, "_our_buy_id" if side_key == "sell" else "_our_sell_id", None)
                # --- 方案A（后继）：成交后检查新锚点是否偏离当前指数价，偏离则继续追 ---
                idx = self.btc_index_price
                if idx > 0:
                    deviation = abs(idx / self.anchor_price - 1)
                    if deviation > self.daily_rv:
                        old_anchor = self.anchor_price
                        self.anchor_price = idx
                        self._update_rv()
                        self._recalc_thresholds()
                        self._log_info("方案A: Anchor追 %.2f -> %.2f (deviation %.4f%%), RV=%.2f%%",
                                       old_anchor, idx, deviation * 100, self.daily_rv * 100)
                        self._cooldown_until = time.time() + self._cooldown_seconds
                        self._log_info("方案A: Cooldown %ds", self._cooldown_seconds)
                        # 对侧挂单已在成交处理中取消，此处无需重复 cancel
                        buy_price = round(self.lower_threshold)
                        sell_price = round(self.upper_threshold)

        # --- 取消价位不对的挂单 ---
        for o in self.open_orders:
            target = buy_price if o["side"] == "buy" else sell_price
            if o["order_id"] == self._our_buy_id or o["order_id"] == self._our_sell_id:
                if abs(o["price"] - target) > 0.5:
                    self.api.cancel_order(o["order_id"])
                    if o["order_id"] == self._our_buy_id:
                        self._our_buy_id = None
                    else:
                        self._our_sell_id = None
                    self._log_info("Cancelled stale %s order at %.2f (target %.2f)",
                                   o["side"], o["price"], target)

        # --- 冷静期：成交后 3 分钟内不挂新单（但成交检测照常进行）---
        if time.time() < self._cooldown_until:
            # 每 5 轮（约 2.5 分钟）才打印一次冷静期提示，防刷屏
            if not hasattr(self, '_cooldown_log_counter'):
                self._cooldown_log_counter = 0
            self._cooldown_log_counter += 1
            if self._cooldown_log_counter % 5 == 1:
                self._log_info("Cooldown active, skipping new orders (%ds left)",
                               int(self._cooldown_until - time.time()))
            return

        # --- 计算下单量 ---
        def calc_amount(price):
            if price <= 0:
                return 0
            amt = self._round_amount(trade_size / price)
            return amt if amt >= self.min_trade_amount else 0

        # --- 挂买入单（防重复：检查交易所是否已有同价位挂单）---
        buy_amount = calc_amount(buy_price)
        buy_exists = any(
            o["side"] == "buy" and abs(o["price"] - buy_price) < 0.5
            for o in self.open_orders
        )
        if not self._our_buy_id and not buy_exists and buy_amount > 0 and not self.usdc_insufficient:
            if buy_amount * buy_price <= self.usdc_balance:
                self._log_info("Placing buy maker @ %.2f for %.6f BTC", buy_price, buy_amount)
                result = self.api.buy(
                    self.cfg["instrument_name"], amount=buy_amount,
                    order_type="limit", price=buy_price,
                    label="maker_buy", post_only=True,
                )
                if result["success"]:
                    parsed = self.api.parse_order_result(result["result"] or {})
                    self._our_buy_id = parsed["order_id"]
                    self._log_info("Buy maker placed: ID %s", self._our_buy_id)
                else:
                    self._log_info("Buy maker FAILED: %s", result.get("error"))
            else:
                self._log_info("Buy skipped: USDC insufficient (need %.2f have %.2f)",
                               buy_amount * buy_price, self.usdc_balance)
        elif self._our_buy_id:
            # 防刷屏：不成交时每 10 轮才打一次常规消息
            if not hasattr(self, '_buy_skip_counter'):
                self._buy_skip_counter = 0
            self._buy_skip_counter += 1
            if self._buy_skip_counter % 10 == 1:
                self._log_info("Buy already active: %s", self._our_buy_id)
        else:
            if not hasattr(self, '_buy_skip_counter'):
                self._buy_skip_counter = 0
            self._buy_skip_counter += 1
            if self._buy_skip_counter % 10 == 1:
                self._log_info("Buy skipped: amount=%.6f usdc_insuff=%s", buy_amount, self.usdc_insufficient)

        # --- 挂卖出单（防重复：检查交易所是否已有同价位挂单）---
        sell_amount = calc_amount(sell_price)
        sell_exists = any(
            o["side"] == "sell" and abs(o["price"] - sell_price) < 0.5
            for o in self.open_orders
        )
        if not self._our_sell_id and not sell_exists and sell_amount > 0 and not self.btc_insufficient:
            if sell_amount <= self.btc_balance:
                self._log_info("Placing sell maker @ %.2f for %.6f BTC", sell_price, sell_amount)
                result = self.api.sell(
                    self.cfg["instrument_name"], amount=sell_amount,
                    order_type="limit", price=sell_price,
                    label="maker_sell", post_only=True,
                )
                if result["success"]:
                    parsed = self.api.parse_order_result(result["result"] or {})
                    self._our_sell_id = parsed["order_id"]
                    self._log_info("Sell maker placed: ID %s", self._our_sell_id)
                else:
                    self._log_info("Sell maker FAILED: %s", result.get("error"))
            else:
                self._log_info("Sell skipped: BTC insufficient (need %.6f have %.6f)",
                               sell_amount, self.btc_balance)
        elif self._our_sell_id:
            if not hasattr(self, '_sell_skip_counter'):
                self._sell_skip_counter = 0
            self._sell_skip_counter += 1
            if self._sell_skip_counter % 10 == 1:
                self._log_info("Sell already active: %s", self._our_sell_id)
        else:
            if not hasattr(self, '_sell_skip_counter'):
                self._sell_skip_counter = 0
            self._sell_skip_counter += 1
            if self._sell_skip_counter % 10 == 1:
                self._log_info("Sell skipped: amount=%.6f btc_insuff=%s", sell_amount, self.btc_insufficient)

    # ------------------------------------------------------------------
    # 交易执行
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    def _set_status(self, s):
        with self._lock:
            self.status = s

    def _add_error(self, msg):
        with self._lock:
            self.errors.append({"time": datetime.now(BJT).isoformat(), "msg": msg})
            if len(self.errors) > 100:
                self.errors = self.errors[-100:]

    def _log_info(self, fmt, *args):
        logger.info(fmt, *args)
