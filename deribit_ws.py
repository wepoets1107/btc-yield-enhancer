"""
Deribit WebSocket 客户端（独立模块，不依赖 strategy_engine）

功能：
- 异步 WebSocket 连接 + 认证（client_credentials）
- 订阅：user.portfolio.BTC / user.portfolio.USDC / ticker.BTC_USDC.index
- 实时缓存余额、指数价（线程安全）
- 自动心跳 + 断线重连
- 可选回调分发（供 engine 或外部使用）

使用方式（独立运行）：
    python deribit_ws.py --client-id xxx --client-secret yyy

集成引用：
    from deribit_ws import DeribitWSClient
    ws = DeribitWSClient(client_id, client_secret)
    ws.set_callback(lambda msg: print(msg))
    ws.start()          # 后台线程启动
    # 随时读取缓存
    price = ws.cached_index_price
    btc = ws.cached_btc_balance
    usdc = ws.cached_usdc_balance
    ws.stop()
"""

from __future__ import annotations

import asyncio
import hmac
import hashlib
import json
import logging
import os
import threading
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# 安全浮点数转换
def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        v = float(value)
        return default if v != v else v  # NaN check
    except (TypeError, ValueError):
        return default


class DeribitWSClient:
    """独立 WebSocket 客户端"""

    WS_MAINNET = "wss://www.deribit.com/ws/api/v2"
    WS_TESTNET = "wss://test.deribit.com/ws/api/v2"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        testnet: bool = False,
        callback: Optional[Callable[[dict], None]] = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.testnet = testnet
        self.ws_url = self.WS_TESTNET if testnet else self.WS_MAINNET
        self._user_callback = callback

        # 缓存（线程安全）
        self._cache_lock = threading.Lock()
        self._cached_usdc_balance = 0.0
        self._cached_btc_balance = 0.0
        self._cached_index_price = 0.0

        # WS 状态
        self.connected = False
        self.authenticated = False
        self.error: Optional[str] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # 重连配置
        self._reconnect_delay = 5.0    # 初始重连间隔（秒）
        self._max_reconnect_delay = 60.0

        # 订阅频道
        self._channels = [
            "user.portfolio.btc",
            "user.portfolio.usdc",
            "ticker.BTC_USDC.index",
        ]

    # ------------------------------------------------------------------
    # 缓存属性（线程安全）
    # ------------------------------------------------------------------

    @property
    def cached_usdc_balance(self) -> float:
        with self._cache_lock:
            return self._cached_usdc_balance

    @cached_usdc_balance.setter
    def cached_usdc_balance(self, v: float):
        with self._cache_lock:
            self._cached_usdc_balance = v

    @property
    def cached_btc_balance(self) -> float:
        with self._cache_lock:
            return self._cached_btc_balance

    @cached_btc_balance.setter
    def cached_btc_balance(self, v: float):
        with self._cache_lock:
            self._cached_btc_balance = v

    @property
    def cached_index_price(self) -> float:
        with self._cache_lock:
            return self._cached_index_price

    @cached_index_price.setter
    def cached_index_price(self, v: float):
        with self._cache_lock:
            self._cached_index_price = v

    # ------------------------------------------------------------------
    # 控制
    # ------------------------------------------------------------------

    def set_callback(self, callback: Optional[Callable[[dict], None]]):
        """设置/更新消息回调"""
        self._user_callback = callback

    def start(self):
        """后台线程启动 WS 连接（非阻塞）"""
        if self._running:
            logger.warning("WS already running")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("WS client starting...")

    def stop(self):
        """停止 WS 连接"""
        self._running = False
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        logger.info("WS client stopping...")

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # 内部：asyncio 循环
    # ------------------------------------------------------------------

    def _run_loop(self):
        """在新的事件循环中运行 WS（线程入口）"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._ws_main())
        except Exception as e:
            logger.error("WS loop fatal: %s", e)
        finally:
            try:
                self._loop.close()
            except Exception:
                pass
            self.connected = False
            self.authenticated = False

    async def _ws_main(self):
        """主协程：连接 → 认证 → 订阅 → 消息循环 + 自动重连"""
        import websockets

        while self._running:
            try:
                logger.info("WS connecting to %s...", self.ws_url)
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self.connected = True
                    self._reconnect_delay = 5.0  # 成功连接，重置重连间隔
                    logger.info("WS connected")
                    await self._ws_authenticate(ws)
                    await self._ws_subscribe(ws)
                    await self._ws_message_loop(ws)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                self.authenticated = False
                self.error = str(e)
                logger.error("WS error: %s, reconnecting in %.0fs...", e, self._reconnect_delay)
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(self._reconnect_delay * 1.5, self._max_reconnect_delay)
            finally:
                self.connected = False

    # ------------------------------------------------------------------
    # 认证
    # ------------------------------------------------------------------

    async def _ws_authenticate(self, ws):
        """client_credentials 认证"""
        msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        }
        await ws.send(json.dumps(msg))
        response = await asyncio.wait_for(ws.recv(), timeout=15)
        data = json.loads(response)
        if "error" in data and data.get("error") is not None:
            err = data["error"]
            err_msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            self.error = f"Auth failed: {err_msg}"
            self.authenticated = False
            raise ConnectionError(self.error)
        self.authenticated = True
        logger.info("WS authenticated")

    # ------------------------------------------------------------------
    # 订阅
    # ------------------------------------------------------------------

    async def _ws_subscribe(self, ws):
        """订阅频道"""
        msg = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "private/subscribe",
            "params": {
                "channels": self._channels,
            },
        }
        await ws.send(json.dumps(msg))
        response = await asyncio.wait_for(ws.recv(), timeout=15)
        data = json.loads(response)
        if "result" in data:
            logger.info("WS subscribed: %s", data["result"])
        else:
            logger.warning("WS subscribe response: %s", data)

    # ------------------------------------------------------------------
    # 消息循环
    # ------------------------------------------------------------------

    async def _ws_message_loop(self, ws):
        """持续接收消息，分发到缓存 + 回调"""
        import websockets

        sub_messages = 0
        error_count = 0
        while self._running:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
                sub_messages += 1
                error_count = 0
                self._handle_message(raw)
            except asyncio.TimeoutError:
                # 超时正常（Deribit 心跳间隔较长）
                if sub_messages == 0 and self._running:
                    # 连接上来后30秒没有任何消息，发一次 ping 测试
                    try:
                        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 999, "method": "public/test", "params": {}}))
                    except Exception:
                        pass
                continue
            except websockets.exceptions.ConnectionClosed:
                logger.info("WS connection closed, reconnecting...")
                break
            except Exception as e:
                error_count += 1
                logger.warning("WS recv error (%d): %s", error_count, e)
                if error_count > 5:
                    logger.error("Too many WS errors, reconnecting...")
                    break
                continue

    def _handle_message(self, raw: str):
        """解析并分发 WS 消息"""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        try:
            # 推送通知：{ params: { channel, data } }
            if "params" in msg:
                params = msg.get("params", {})
                channel = params.get("channel", "")
                data = params.get("data", {})

                if channel == "heartbeat":
                    return

                # 更新缓存
                self._update_cache(channel, data)

                # 回调分发
                if self._user_callback:
                    try:
                        self._user_callback({"channel": channel, "data": data})
                    except Exception as e:
                        logger.error("WS callback error: %s", e)
                return

            # JSON-RPC 响应
            if "id" in msg:
                self._handle_rpc_response(msg)
        except Exception as e:
            logger.warning("WS handle_message error: %s", e)

    def _update_cache(self, channel: str, data: dict):
        if channel == "user.portfolio.btc":
            bal = _f(data.get("balance"))
            if bal >= 0:
                self.cached_btc_balance = bal
        elif channel == "user.portfolio.usdc":
            bal = _f(data.get("balance"))
            if bal >= 0:
                self.cached_usdc_balance = bal
        elif "index" in channel:
            # Deribit index ticker 可能用 index_price 或 idx 字段
            idx = _f(data.get("index_price")) or _f(data.get("idx"))
            if idx > 0:
                self.cached_index_price = idx

    def _handle_rpc_response(self, msg: dict):
        error = msg.get("error")
        if error is not None:
            err_msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            logger.warning("WS RPC error: %s", err_msg)
            self.error = err_msg

    # ------------------------------------------------------------------
    # 状态快照
    # ------------------------------------------------------------------

    def get_snapshot(self) -> dict:
        """获取当前缓存快照"""
        return {
            "connected": self.connected,
            "authenticated": self.authenticated,
            "error": self.error,
            "cached_usdc_balance": self.cached_usdc_balance,
            "cached_btc_balance": self.cached_btc_balance,
            "cached_index_price": self.cached_index_price,
        }


# ======================================================================
# CLI 独立运行测试
# ======================================================================

def _load_env():
    """载入 .env 文件到环境变量（仅 CLI 模式）"""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def _main():
    _load_env()
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Deribit WebSocket 客户端测试")
    parser.add_argument("--client-id", default=os.environ.get("DERIBIT_ID", ""))
    parser.add_argument("--client-secret", default=os.environ.get("DERIBIT_SECRET", ""))
    parser.add_argument("--testnet", action="store_true", default=os.environ.get("DERIBIT_TESTNET", "0") == "1")

    args = parser.parse_args()

    if not args.client_id or not args.client_secret:
        print("请设置 DERIBIT_ID 和 DERIBIT_SECRET 环境变量或通过参数传入")
        return

    def on_message(msg):
        print(f"[WS] {msg['channel']}: {msg['data']}")

    client = DeribitWSClient(
        args.client_id, args.client_secret,
        testnet=args.testnet,
        callback=on_message,
    )
    client.start()

    print("WS 客户端已启动，实时推送数据如下：")
    print("按 Ctrl+C 退出")
    print()

    try:
        while True:
            time.sleep(5)
            snap = client.get_snapshot()
            print(f"  connected={snap['connected']} auth={snap['authenticated']} "
                  f"btc={snap['cached_btc_balance']:.6f} usdc={snap['cached_usdc_balance']:.2f} "
                  f"index={snap['cached_index_price']:.2f}")
    except KeyboardInterrupt:
        print("\n正在停止...")
    finally:
        client.stop()


if __name__ == "__main__":
    _main()
