"""
Deribit API 客户端
封装 JSON-RPC 接口：认证、余额、指数价格、K线数据、现货买卖

参考：
- icefire-options-workbench (REST GET 公共数据)
- f405-options-bot (JSON-RPC 下单)
- DDH workbench (WebSocket 实时交易)
- binghuodao-options-notes (公共行情)
"""

from __future__ import annotations

import time
import json
import logging
import threading
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

# 安全浮点数转换（兼容 None / 字符串 / 各种边界）
def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        v = float(value)
        if v != v:  # NaN
            return default
        return v
    except (TypeError, ValueError):
        return default


class DeribitClient:
    """Deribit JSON-RPC API 客户端（同步、线程安全）"""

    MAINNET = "https://www.deribit.com/api/v2/"
    TESTNET = "https://test.deribit.com/api/v2/"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        testnet: bool = False,
    ) -> None:
        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret are required")

        self.client_id = client_id
        self.client_secret = client_secret
        self.testnet = testnet
        self.base_url = self.TESTNET if testnet else self.MAINNET

        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._lock = threading.RLock()
        self._request_id = 0
        self._last_auth_error: Optional[str] = None

    # ------------------------------------------------------------------
    # 底层 JSON-RPC 请求
    # ------------------------------------------------------------------

    def _call(
        self,
        method: str,
        params: Optional[dict] = None,
        need_auth: bool = False,
    ) -> dict:
        """发送 JSON-RPC POST 请求，统一返回 {success, result/error}"""
        if params is None:
            params = {}

        self._request_id += 1
        req_id = self._request_id

        payload = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params,
        }

        headers = {}
        if need_auth or method.startswith("private"):
            token = self._get_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"

        try:
            resp = requests.post(
                self.base_url,
                json=payload,
                headers=headers,
                timeout=20,
            )
            if resp.status_code != 200:
                logger.error(
                    "Deribit HTTP %d [%s]: %s",
                    resp.status_code, method, resp.text[:500],
                )
            data = resp.json()

            # JSON-RPC error（Deribit 正常响应但业务错误）
            if "error" in data and data["error"] is not None:
                err = data["error"]
                if isinstance(err, dict):
                    logger.error(
                        "Deribit API error [%s]: code=%s msg=%s",
                        method, err.get("code"), err.get("message"),
                    )
                else:
                    logger.error("Deribit API error [%s]: %s", method, err)
                return {"success": False, "error": err}

            return {"success": True, "result": data.get("result")}

        except requests.exceptions.RequestException as e:
            logger.error("Deribit request failed [%s]: %s", method, e)
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # 认证（带自动续期 + 线程安全）
    # ------------------------------------------------------------------

    def _authenticate(self) -> bool:
        """获取新的 access_token（client_credentials 模式）"""
        result = self._call("public/auth", {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        })
        if result["success"]:
            r = result["result"]
            self._token = r["access_token"]
            expires_in = _f(r.get("expires_in"), 3600)
            # 提前 120 秒续期，避免临界区过期
            self._token_expiry = time.time() + expires_in - 120
            self._last_auth_error = None
            logger.info(
                "Deribit authenticated (%s), token expires in %d s",
                "testnet" if self.testnet else "mainnet",
                expires_in,
            )
            return True

        err = result.get("error", "unknown_error")
        if isinstance(err, dict):
            err_msg = err.get("message", str(err))
        else:
            err_msg = str(err)
        self._last_auth_error = err_msg
        logger.error(
            "Deribit auth failed (%s): %s",
            "testnet" if self.testnet else "mainnet",
            err_msg,
        )
        return False

    def _get_token(self) -> Optional[str]:
        """获取有效 token，过期自动续期（double-checked locking）"""
        if self._token is None or time.time() >= self._token_expiry:
            with self._lock:
                if self._token is None or time.time() >= self._token_expiry:
                    if not self._authenticate():
                        return None
        return self._token

    def check_connection(self) -> dict:
        """检查 API 连接，返回详细状态字典"""
        token = self._get_token()
        result: dict = {
            "connected": False,
            "testnet": self.testnet,
            "auth_error": self._last_auth_error,
        }
        if not token:
            return result
        r = self._call("public/get_time")
        if r["success"]:
            result["connected"] = True
        else:
            result["error"] = str(r.get("error", "unknown"))
        return result

    # ------------------------------------------------------------------
    # 公共接口（无需认证）
    # ------------------------------------------------------------------

    def get_index_price(self, index_name: str = "btc_usdc") -> Optional[float]:
        """获取指数价格"""
        result = self._call("public/get_index_price", {
            "index_name": index_name,
        })
        if result["success"]:
            return _f(result["result"].get("index_price"))
        return None

    def get_instruments(
        self, currency: str = "BTC", kind: str = "spot"
    ) -> list[dict]:
        """获取交易品种列表"""
        result = self._call("public/get_instruments", {
            "currency": currency,
            "kind": kind,
            "expired": False,
        })
        if result["success"]:
            return result["result"]
        return []

    def get_ticker(self, instrument_name: str) -> Optional[dict]:
        """获取单个品种的完整 ticker（含 Greeks）"""
        result = self._call("public/ticker", {
            "instrument_name": instrument_name,
        })
        if result["success"]:
            return result["result"]
        return None

    def get_tradingview_chart_data(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str = "1D",
    ) -> Optional[dict]:
        """获取 K 线数据（波动率计算用）"""
        result = self._call("public/get_tradingview_chart_data", {
            "instrument_name": instrument_name,
            "start_timestamp": int(start_timestamp),
            "end_timestamp": int(end_timestamp),
            "resolution": resolution,
        })
        if result["success"]:
            return result["result"]
        return None

    def get_book_summary_by_currency(
        self, currency: str = "BTC", kind: str = "option"
    ) -> list[dict]:
        """获取期权/期货盘口摘要"""
        result = self._call("public/get_book_summary_by_currency", {
            "currency": currency,
            "kind": kind,
        })
        if result["success"]:
            return result["result"]
        return []

    # ------------------------------------------------------------------
    # 私有接口（账户 & 交易）
    # ------------------------------------------------------------------

    def get_account_summary(
        self, currency: str = "USDC", extended: bool = True
    ) -> Optional[dict]:
        """获取账户摘要（余额、保证金等）"""
        result = self._call("private/get_account_summary", {
            "currency": currency,
            "extended": extended,
        }, need_auth=True)
        if result["success"]:
            return result["result"]
        return None

    def get_positions(
        self, currency: str = "BTC", kind: str = "any"
    ) -> list[dict]:
        """获取持仓"""
        result = self._call("private/get_positions", {
            "currency": currency,
            "kind": kind,
        }, need_auth=True)
        if result["success"]:
            return result["result"]
        return []

    def buy(
        self,
        instrument_name: str,
        amount: float,
        order_type: str = "market",
        label: Optional[str] = None,
        price: Optional[float] = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> dict:
        """买入

        Args:
            instrument_name: 交易对名称 (e.g. BTC_USDC)
            amount: 数量（BTC 或合约张数）
            order_type: market / limit / stop_limit / stop_market
            label: 订单标签（便于识别）
            price: 限价单价格
            reduce_only: 仅减仓
            post_only: 仅挂单（不吃单）
        """
        params = {
            "instrument_name": instrument_name,
            "amount": amount,
            "type": order_type,
        }
        if label:
            params["label"] = label
        if price and order_type in ("limit", "stop_limit"):
            params["price"] = price
        if reduce_only:
            params["reduce_only"] = True
        if post_only:
            params["post_only"] = True
        return self._call("private/buy", params, need_auth=True)

    def sell(
        self,
        instrument_name: str,
        amount: float,
        order_type: str = "market",
        label: Optional[str] = None,
        price: Optional[float] = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> dict:
        """卖出"""
        params = {
            "instrument_name": instrument_name,
            "amount": amount,
            "type": order_type,
        }
        if label:
            params["label"] = label
        if price and order_type in ("limit", "stop_limit"):
            params["price"] = price
        if reduce_only:
            params["reduce_only"] = True
        if post_only:
            params["post_only"] = True
        return self._call("private/sell", params, need_auth=True)

    def get_order_state(self, order_id: str) -> dict:
        """查询订单状态"""
        return self._call("private/get_order_state", {
            "order_id": order_id,
        }, need_auth=True)

    def cancel_order(self, order_id: str) -> dict:
        """取消单笔订单"""
        return self._call("private/cancel", {
            "order_id": order_id,
        }, need_auth=True)

    def cancel_all(self, instrument_name: str = "") -> dict:
        """取消指定币种的所有订单（不传参则取消全部）"""
        params = {}
        if instrument_name:
            params["instrument_name"] = instrument_name
        return self._call("private/cancel_all", params, need_auth=True)

    def cancel_all_by_instrument(self, instrument_name: str) -> dict:
        """只取消指定 instrument 的订单（不碰其他币种/期权/期货）"""
        return self._call("private/cancel_all_by_instrument", {
            "instrument_name": instrument_name,
        }, need_auth=True)

    def get_open_orders(
        self, instrument_name: Optional[str] = None
    ) -> list[dict]:
        """获取未成交订单"""
        params = {}
        if instrument_name:
            params["instrument_name"] = instrument_name
        result = self._call("private/get_open_orders", params, need_auth=True)
        if result["success"]:
            return result["result"]
        return []

    def get_order_book(
        self, instrument_name: str, depth: int = 5
    ) -> Optional[dict]:
        """获取订单薄"""
        result = self._call("public/get_order_book", {
            "instrument_name": instrument_name,
            "depth": depth,
        })
        if result["success"]:
            return result["result"]
        return None

    # ------------------------------------------------------------------
    # 辅助：解析订单响应中的成交明细
    # ------------------------------------------------------------------

    @staticmethod
    def parse_order_result(result_data: dict) -> dict:
        """从 buy/sell 响应中提取可读的订单摘要"""
        order = result_data.get("order", result_data)
        trades = result_data.get("trades", [])

        filled_amount = _f(order.get("filled_amount"))
        avg_price = _f(order.get("average_price"))
        order_id = order.get("order_id", "")
        state = order.get("order_state", "")

        # 成交详情
        fills = []
        for t in trades:
            fills.append({
                "trade_id": t.get("trade_id"),
                "amount": _f(t.get("amount")),
                "price": _f(t.get("price")),
                "fee": _f(t.get("fee")),
                "fee_currency": t.get("fee_currency"),
                "timestamp": t.get("timestamp"),
            })

        return {
            "order_id": order_id,
            "state": state,
            "filled_amount": filled_amount,
            "average_price": avg_price,
            "total_cost": round(filled_amount * avg_price, 2),
            "label": order.get("label", ""),
            "direction": order.get("direction", ""),
            "instrument_name": order.get("instrument_name", ""),
            "fills": fills,
            "raw_order": order,
        }
