# ⚡ BTC Yield Enhancer / BTC 收益增强策略

> A Deribit spot maker grid strategy that automatically trades BTC/USDC around an anchor price, profiting from market volatility.
>
> 一个在 Deribit 上运行 BTC/USDC 现货 maker 网格的策略，围绕价格锚点自动低买高卖，从市场波动中获利。

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Flask](https://img.shields.io/badge/Flask-3.0+-green)

---

## 📖 Table of Contents / 目录

- [How It Works / 原理](#how-it-works--原理)
- [Getting Started / 快速开始](#getting-started--快速开始)
- [Web Dashboard / 仪表盘](#web-dashboard--仪表盘)
- [Architecture / 架构](#architecture--架构)
- [Configuration / 配置](#configuration--配置)
- [Maintenance / 维护](#maintenance--维护)
- [FAQ / 常见问题](#faq--常见问题)
- [License / 许可](#license--许可)

---

## How It Works / 原理

### Core Logic / 核心逻辑

The strategy maintains a **maker grid** on Deribit's BTC/USDC spot market:

1. **Anchor Price** – On startup, the current index price is recorded as the anchor.
2. **Daily RV (Realized Volatility)** – Calculated from 12 × 5-minute candles (1-hour window), RMS scaled by √24 to daily. Clamped between 0.5%–5.0%. Updated after each trade and every 15 minutes as a fallback.
3. **Price Channel** – A symmetrical channel around the anchor:
   - **Sell threshold** = anchor × (1 + RV)
   - **Buy threshold** = anchor × (1 − RV)
4. **Maker Orders** – A limit buy at the lower threshold and a limit sell at the upper threshold are placed as post-only maker orders.
5. **Fill → Update** – When either order fills:
   - Anchor updates to the fill price.
   - RV recalculates from live market data.
   - A **cool-down period** (3 min) prevents rapid re-entry.
   - **Plan A**: if the fill price deviates from the current index price by more than RV, the anchor chases the index price and re-enters.
6. **Independent Directional Protection** – When USDC balance drops below the threshold ($200), buying pauses. When BTC value drops below $200, selling pauses. Each recovers automatically.

策略在 Deribit BTC/USDC 现货市场运行一个 **maker 网格**：

1. **价格锚点** – 启动时以当前指数价为锚点
2. **日化 RV（已实现波动率）** – 取 12 根 5 分钟 K 线的 RMS 乘以 √24，限幅 0.5%–5.0%，成交后实时更新 + 15 分钟兜底更新
3. **价格通道** – 锚点的对称通道：
   - 卖出阈值 = 锚点 × (1 + RV)
   - 买入阈值 = 锚点 × (1 − RV)
4. **Maker 挂单** – 在上下阈值各挂一个 post-only 限价单
5. **成交 → 更新** – 任一方向成交后：
   - 锚点更新为成交价
   - 用最新市场数据重算 RV
   - **冷静期** 3 分钟，防止频繁入场
   - **方案A**：若成交价偏离当前指数价超过 RV，锚点追价并重新入场
6. **方向独立保护** – USDC 不足 $200 暂停买入，BTC 不足 $200 暂停卖出，恢复后自动恢复

### Example / 示例

```
Anchor: $65,000
RV: 2.0%
───────────────
Sell @ $66,300 ← maker sell placed here
         ↑
    index price
         ↓
Buy  @ $63,700 ← maker buy placed here

...BTC drops to $63,700 → buy fills, anchor → $63,700, RV recalculated
...BTC rises to $66,300 → sell fills, anchor → $66,300, RV recalculated
```

---

## Getting Started / 快速开始

### Prerequisites / 前置条件

- **Python 3.10+**
- **A Deribit account** with API credentials (mainnet or testnet)
  - [Deribit Testnet](https://test.deribit.com/) (recommended for first try)
  - [Deribit Mainnet](https://www.deribit.com/)
- API Key permissions required: `Trade`, `Read`

需要：Python 3.10+、Deribit 账户和 API 密钥（建议先从 Testnet 开始），API 密钥需要 `Trade` + `Read` 权限。

### Installation / 安装

```bash
# 1. Clone the repo
git clone https://github.com/wepoets1107/btc-yield-enhancer.git
cd btc-yield-enhancer

# 2. Create virtual environment (optional but recommended)
python -m venv venv
# Linux/macOS:
source venv/bin/activate
# Windows:
# .\venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create .env file from example
cp .env.example .env
```

### Configuration / 配置

Edit the `.env` file with your Deribit API credentials:

```bash
# .env — never commit this file!
DERIBIT_ID=your_client_id_here
DERIBIT_SECRET=your_client_secret_here
DERIBIT_TESTNET=1   # 1 = testnet, 0 = mainnet
```

> ⚠️ **Security**: `.env` is in `.gitignore` — your credentials will never be committed. The dashboard provides a UI to update credentials at runtime (and they get saved back to `.env`).

> ⚠️ **安全**：`.env` 已在 `.gitignore` 中，凭证不会提交到 Git。仪表盘提供运行时修改凭证的界面，修改后自动写回 `.env`。

### Run / 运行

```bash
python app.py
```

Then open your browser to: **http://localhost:5050**

启动后浏览器打开 **http://localhost:5050** 即可看到仪表盘。

---

## Web Dashboard / 仪表盘

The dashboard runs a real-time web UI at port 5050:

| Feature | Description |
|---|---|
| **Live price chart** | BTC K-line with anchor/sell/buy threshold lines |
| **Balance panel** | USDC, BTC, total asset value |
| **Parameter panel** | Editable anchor, trade size, RV limits, poll interval, cooldown |
| **Real-time stats** | BTC index price, deviation from anchor, trade count, P&L |
| **Order book** | Current open orders |
| **Trade history** | Last 50 trades |
| **API credentials** | Update ID/Secret/testnet at runtime |
| **WebSocket push** | All data updates in real-time |

操作流程：
1. 打开 http://localhost:5050
2. 如果 .env 没有凭证，在页面填写 API ID/Secret 并保存
3. 点击 **🔌 测试连接** 确认连接成功
4. 点击 **▶ 启动** → 策略初始化（连接、余额、锚点、RV）→ 状态变为"就绪"
5. 再次点击 **▶ 启动** → 交易开启，maker 挂单开始工作
6. 点击 **⏹ 停止** → 取消所有挂单，停止

**Trading flow:**
1. Click **▶ Start** → engine initializes (connect, fetch balances, set anchor) → status "ready"
2. Click **▶ Start** again → trading begins, maker orders appear on the book
3. Click **⏹ Stop** → cancels all open orders, stops the engine

**Parameter editing:** Edit values in the input fields, click **💾 保存参数** to apply in real-time without restart.

---

## Architecture / 架构

```
btc-yield-enhancer/
├── app.py                  # Flask web server + REST API + WebSocket
├── strategy_engine.py      # Core strategy logic (maker grid)
├── deribit_api.py          # Deribit JSON-RPC client (auth, trade, data)
├── requirements.txt        # Python dependencies
├── .env                    # API credentials (gitignored)
├── .env.example            # Template for .env (can be committed)
├── state.json              # Runtime state persistence (gitignored)
├── start.bat               # Windows startup script
├── stop.bat                # Windows stop script
├── static/
│   ├── dashboard.html      # Main dashboard HTML (Vue-free, vanilla)
│   ├── app.js              # Dashboard logic + WebSocket client
│   ├── lightweight-charts.standalone.production.js  # Charting lib
│   └── vue.min.js          # (unused, kept for compatibility)
└── venv/                   # Virtual environment (gitignored)
```

### Data Flow / 数据流

```
Deribit Exchange
      ↕ (JSON-RPC)
deribit_api.py
      ↕
strategy_engine.py (background thread: poll every 30s)
      ↕ (state callback)
app.py → WebSocket broadcast → dashboard.html (real-time UI)
```

### API Endpoints / 接口

| Method | Path | Description |
|---|---|---|
| GET | `/btc-enhancer/` | Dashboard page |
| GET | `/btc-enhancer/api/status` | Current strategy state (JSON) |
| POST | `/btc-enhancer/api/init` | Initialize engine |
| POST | `/btc-enhancer/api/start` | Start trading |
| POST | `/btc-enhancer/api/stop` | Stop & cancel orders |
| GET/POST | `/btc-enhancer/api/params` | Read/update runtime params |
| GET/POST | `/btc-enhancer/api/credentials` | Read/update API credentials |
| GET | `/btc-enhancer/api/kline` | Public BTC K-line (unauthenticated) |
| GET | `/btc-enhancer/api/test-connection` | Test both mainnet & testnet |
| WS | `/btc-enhancer/ws` | Real-time state push |

---

## Configuration / 配置

Detailed parameter reference / 详细参数说明：

| Param | Default | Range | Description |
|---|---|---|---|
| `trade_size_usdc` | 100 | 10–10,000 | Single leg trade size in USDC |
| `rv_min` | 0.5% | 0.01%–5% | Minimum daily RV (lower bound) |
| `rv_max` | 5% | 0.1%–10% | Maximum daily RV (upper bound) |
| `rv_update_interval_minutes` | 15 | 5–1440 | Fallback RV update interval |
| `poll_interval` | 30s | 5–300s | Balance/price polling interval |
| `cooldown_seconds` | 180 | 10–600 | Cool-down after each fill |
| `min_poll_balance_usdc` | $200 | $10–$10,000 | Balance threshold for directional pause |

---

## Maintenance / 维护

### State Persistence / 状态持久化

The strategy saves its state to `state.json` on every trade and on stop. On restart, it:
- Restores the anchor price (if within 10% of current index price)
- Restores historical trades and total trade count
- Auto-resumes trading if it was running before the restart

策略每次成交和停止时保存 state.json。重启时自动恢复锚点（偏差 10% 内）、历史成交记录，若之前交易已启动则自动恢复交易。

### Updating / 升级

```bash
cd btc-yield-enhancer
git pull
# Check for dependency changes
pip install -r requirements.txt --upgrade
# Restart the app
```

---

## FAQ / 常见问题

**Q: Does this hold BTC overnight?**
A: Yes. The strategy holds a BTC position between trades. It doesn't hedge — it's a directional maker grid that profits from volatility.

**Q: What's the expected return?**
A: Variable. With RV at 2% and 100 USDC trade size, each grid capture yields ~2 USDC per round trip (before fees). Deribit spot fees are 0.075%/0.07% (maker/taker) — maker-only orders minimize cost.

**Q: What if the market gaps through my order?**
A: The order is post-only, so it won't be taken at a worse price. If the price passes through but your order doesn't fill (due to moving too fast), the next poll cycle detects the gap and triggers Plan A — chasing the index price.

**Q: Can I run on testnet first?**
A: Absolutely recommended. Set `DERIBIT_TESTNET=1` in `.env`, fund your testnet wallet from [Deribit Testnet Faucet](https://test.deribit.com/faucet).

**Q: Does this affect other positions (futures, options)?**
A: No. The strategy only touches BTC/USDC spot orders. It cancels by instrument name, not by currency.

---

## License / 许可

MIT License. See [LICENSE](LICENSE).

---

*Built for the community by [冰火岛](https://binghuodao.club). Use at your own risk — always test on testnet first.*
*由冰火岛社区开发维护。请自行承担交易风险，务必先在 Testnet 测试。*
