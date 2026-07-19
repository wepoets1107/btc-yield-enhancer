/* BTC 收益增强策略 · 仪表盘 — WebSocket 版 */
(function () {
  "use strict";

  var state = { status: "stopped", trading: false, alive: false };
  var chart = null,
    candleSeries = null,
    anchorLine = null,
    upperLine = null,
    lowerLine = null;
  var klineCache = null,
    klineReady = false;
  var ws = null,
    wsReconnectTimer = null;

  function $(id) { return document.getElementById(id); }

  function setVal(el, text, cls) {
    if (typeof el === "string") el = $(el);
    if (!el) return;
    el.textContent = text;
    el.className = "val" + (cls ? " " + cls : "");
  }

  function fmtUsdc(v) { return v != null ? "$" + Number(v).toFixed(2) : "--"; }
  function fmtBtc(v) { return v != null ? Number(v).toFixed(6) + " BTC" : "--"; }
  function fmtPct(v) { return v != null ? Number(v * 100).toFixed(2) + "%" : "--"; }
  function fmtAnchor(v) { return v != null && v > 0 ? "$" + Number(v).toFixed(2) : "--"; }
  function clsVal(n) { if (!n || n === 0) return ""; return n > 0 ? "up" : "down"; }

  function setDot(el, cls) { var d = $(el); if (d) d.className = "dot " + cls; }

  // ---- K 线图 ----
  function createChart() {
    if (chart) return;
    var el = $("priceChart");
    if (!el || typeof LightweightCharts === "undefined") return;

    chart = LightweightCharts.createChart(el, {
      layout: { background: { type: "solid", color: "transparent" }, textColor: "#8b949e", fontSize: 10 },
      grid: { vertLines: { color: "rgba(48,54,61,0.3)" }, horzLines: { color: "rgba(48,54,61,0.3)" } },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: "rgba(48,54,61,0.5)", scaleMargins: { top: 0.05, bottom: 0.2 } },
      timeScale: { borderColor: "rgba(48,54,61,0.5)", timeVisible: true, secondsVisible: false },
      handleScroll: false, handleScale: false,
      width: el.clientWidth, height: 280,
    });

    candleSeries = chart.addCandlestickSeries({
      upColor: "#26a69a", downColor: "#ef5350",
      borderDownColor: "#ef5350", borderUpColor: "#26a69a",
      wickDownColor: "#ef5350", wickUpColor: "#26a69a",
      priceLineVisible: false, lastValueVisible: false,
    });

    anchorLine = chart.addLineSeries({
      color: "#ffd700", lineWidth: 2, priceLineVisible: false,
      lineStyle: LightweightCharts.LineStyle.Dashed,
    });
    upperLine = chart.addLineSeries({
      color: "#3fb950", lineWidth: 1, priceLineVisible: false,
      lineStyle: LightweightCharts.LineStyle.Dotted,
    });
    lowerLine = chart.addLineSeries({
      color: "#f85149", lineWidth: 1, priceLineVisible: false,
      lineStyle: LightweightCharts.LineStyle.Dotted,
    });

    $("chartCard").style.display = "";
    fetchKline();
    // 每 5 分钟刷新一次 K 线
    setInterval(fetchKline, 5 * 60 * 1000);

    var ro = new ResizeObserver(function () {
      if (chart) chart.applyOptions({ width: el.clientWidth });
    });
    ro.observe(el);
  }

  async function fetchKline() {
    try {
      var resp = await fetch("/api/kline");
      var data = await resp.json();
      if (!data || data.error || !data.ticks || !data.ticks.length) return;
      var candles = [];
      for (var i = 0; i < data.ticks.length; i++) {
        var t = Math.floor(data.ticks[i] / 1000);
        var o = data.open[i], h = data.high[i], l = data.low[i], c = data.close[i];
        if (o && h && l && c) candles.push({ time: t, open: o, high: h, low: l, close: c });
      }
      if (candles.length > 0) { candleSeries.setData(candles); klineCache = candles; klineReady = true; }
    } catch (e) { /* kline deferred */ }
  }

  function drawOverlay(data) {
    if (!candleSeries || !klineReady) return;
    var anchor = data.anchor_price, upper = data.upper_threshold, lower = data.lower_threshold;
    if (anchor && anchor > 0 && klineCache && klineCache.length > 0) {
      var pts = klineCache.map(function (p) { return { time: p.time, value: anchor }; });
      anchorLine.setData(pts);
      if (upper && upper > 0) upperLine.setData(klineCache.map(function (p) { return { time: p.time, value: upper }; }));
      if (lower && lower > 0 && lower !== upper) lowerLine.setData(klineCache.map(function (p) { return { time: p.time, value: lower }; }));
    }
  }

  // ---- UI 更新 ----
  function updateUI(data) {
    if (!data || data.error) { setStatus("stopped"); setDot("dotApi", "red"); $("sApi").textContent = "断开"; return; }

    // 动态显示标的名称（从 config 读取，如 BTC_USDC → BTC）
    var instr = data.config && data.config.instrument_name;
    if (instr) $("symbolTitle").textContent = instr.replace("_USDC", "");

    state.status = data.status || "stopped";
    // trading = 真正在交易；alive = 引擎活着（含就绪/初始化/运行）
    state.trading = data.status === "running";
    state.alive = state.trading || data.status === "ready" || data.status === "initializing";

    setStatus(data.status);
    $("lastUpdate").textContent = data.last_update ? data.last_update.split(".")[0].replace("T", " ") : "等待数据...";

    var extra = "BTC/USDC · WS";
    if (data.config && data.config.testnet) extra += " · TESTNET";
    $("topbarExtra").textContent = extra;

    setVal("vUsdc", fmtUsdc(data.usdc_balance));
    setVal("vBtc", fmtBtc(data.btc_balance));
    setVal("vBtcValue", fmtUsdc(data.btc_value_usdc), "highlight");
    setVal("vTotal", fmtUsdc(data.total_value_usdc));

    setVal("vRv", fmtPct(data.daily_rv), "highlight");

    // 锚点输入框（用户未编辑时才更新）
    var inpAnchor = $("inpAnchor");
    if (document.activeElement !== inpAnchor) {
      inpAnchor.value = data.anchor_price != null && data.anchor_price > 0 ? data.anchor_price.toFixed(2) : "";
    }

    setVal("vUpper", fmtAnchor(data.upper_threshold), "up");
    setVal("vLower", fmtAnchor(data.lower_threshold), "down");

    // 交易额输入框
    var inpTradeSize = $("inpTradeSize");
    if (document.activeElement !== inpTradeSize && data.config) {
      inpTradeSize.value = data.config.trade_size_usdc;
    }

    // 策略配置输入框（用户未编辑时才更新）
    var configInputs = ["inpRvMin", "inpRvMax", "inpRvInterval", "inpPoll", "inpCooldown", "inpBalanceThreshold"];
    var configKeys   = ["rv_min",    "rv_max",    "rv_update_interval_minutes", "poll_interval", "cooldown_seconds", "min_poll_balance_usdc"];
    if (data.config) {
      for (var i = 0; i < configInputs.length; i++) {
        var inp = $(configInputs[i]);
        if (inp && document.activeElement !== inp) {
          var val = data.config[configKeys[i]];
          // rv_min / rv_max 显示为百分比
          if (configKeys[i] === "rv_min" || configKeys[i] === "rv_max") val = (val * 100).toFixed(2);
          inp.value = val != null ? val : "";
        }
      }
    }

    setVal("vPrice", fmtAnchor(data.btc_index_price), "highlight");

    var dev = "--";
    if (data.anchor_price > 0 && data.btc_index_price) {
      var d = (data.btc_index_price / data.anchor_price - 1) * 100;
      dev = (d >= 0 ? "+" : "") + d.toFixed(2) + "%";
    }
    setVal("vDeviation", dev, clsVal(parseFloat(dev.replace("%", "").replace("+", ""))));

    setVal("vTrades", data.total_trades != null ? data.total_trades : "0");

    var tp = "--", tpCls = "";
    if (data.initial_total_usdc && data.total_value_usdc) {
      var diff = data.total_value_usdc - data.initial_total_usdc;
      var pct = (diff / data.initial_total_usdc) * 100;
      tp = (diff >= 0 ? "+" : "") + diff.toFixed(2) + " (" + (pct >= 0 ? "+" : "") + pct.toFixed(2) + "%)";
      tpCls = clsVal(diff);
    }
    setVal("vTotalPnl", tp, tpCls);

    // 交易盈亏
    var tPnl = data.trading_pnl != null ? data.trading_pnl : 0;
    var tPnlStr = (tPnl >= 0 ? "+" : "") + tPnl.toFixed(2);
    setVal("vTradePnl", tPnlStr, clsVal(tPnl));

    setVal("vInitTotal", fmtUsdc(data.initial_total_usdc));
    setVal("vUsdcStatus", data.usdc_insufficient ? "⚠ 不足" : "正常", data.usdc_insufficient ? "warn" : "");
    setVal("vBtcStatus", data.btc_insufficient ? "⚠ 不足" : "正常", data.btc_insufficient ? "warn" : "");
    setVal("vRvStatus", data.rv_updated_today ? "是 ✅" : "否 ⏳", data.rv_updated_today ? "up" : "warn");

    setDot("dotApi", data.api_connected ? "green" : "red");
    $("sApi").textContent = data.api_connected ? "已连接" : "断开";
    setDot("dotEngine", state.status === "running" ? "green" : state.status === "error" ? "red" : state.status === "ready" ? "yellow" : "gray");
    $("sEngine").textContent = statusLabel(state.status);
    $("sStart").textContent = data.start_time ? data.start_time.split(".")[0].replace("T", " ") : "--";
    setDot("dotRv", data.rv_updated_today ? "green" : "yellow");
    $("sRv").textContent = data.last_rv_update ? data.last_rv_update.split(".")[0].replace("T", " ") : "--";

    // 交易开关标签
    var tl = $("tradingLabel");
    if (tl) {
      tl.textContent = data.trading_enabled ? "🔴 交易中" : "🟡 未交易";
      tl.style.color = data.trading_enabled ? "var(--up)" : "var(--warn)";
    }

    updateTrades(data.trades || []);
    updateErrors(data.errors || []);
    updateOrders(data.open_orders || []);
    drawOverlay(data);

    // 按钮状态逻辑
    $("btnStart").disabled = state.trading || state.status === "initializing" || state.status === "error";
    $("btnStop").disabled = !state.trading;  // 只有交易中才显示停止可用
  }

  function setStatus(s) {
    var badge = $("statusBadge");
    badge.textContent = statusLabel(s);
    badge.className = "badge " + (s || "stopped");
  }

  function statusLabel(s) { return ({ running: "运行中", ready: "就绪", paused: "已暂停", stopped: "已停止", error: "错误", initializing: "初始化中" })[s] || s || "已停止"; }

  function updateTrades(trades) {
    var body = $("tradeBody"), noData = $("noTrades");
    if (!trades || !trades.length) { body.innerHTML = ""; noData.style.display = ""; return; }
    noData.style.display = "none";
    body.innerHTML = trades.map(function (t) {
      return "<tr><td>" + (t.time || "") + '</td><td><span class="tag ' + t.side + '">' + (t.side === "buy" ? "买入" : "卖出") + "</span></td><td>" + (t.amount_btc != null ? t.amount_btc.toFixed(6) : "") + "</td><td>$" + (t.price != null ? t.price.toFixed(2) : "") + "</td><td>$" + (t.total_usdc != null ? t.total_usdc.toFixed(2) : "") + "</td><td>" + (t.status || "") + "</td></tr>";
    }).join("");
  }

  function updateOrders(orders) {
    var body = $("orderBody"), noData = $("noOrders"), count = $("orderCount");
    if (!orders || !orders.length) { body.innerHTML = ""; noData.style.display = ""; if (count) count.textContent = ""; return; }
    noData.style.display = "none";
    if (count) count.textContent = "(" + orders.length + " 笔)";
    body.innerHTML = orders.map(function (o) {
      var t = o.time ? new Date(o.time).toLocaleString("zh-CN", {hour12:false}) : "";
      var sideTag = '<span class="tag ' + o.side + '">' + (o.side === "buy" ? "买入" : "卖出") + "</span>";
      var stateLabel = ({open:"挂单中",filled:"已成交",cancelled:"已取消",rejected:"已拒绝"})[o.state] || o.state;
      return "<tr><td>" + t + "</td><td>" + sideTag + "</td><td>" + o.amount.toFixed(6) + "</td><td>" + o.filled.toFixed(6) + "</td><td>" + o.remaining.toFixed(6) + "</td><td>$" + (o.price || 0).toFixed(2) + "</td><td>" + stateLabel + "</td></tr>";
    }).join("");
  }

  function updateErrors(errors) {
    var el = $("errorList");
    el.innerHTML = errors && errors.length ? errors.map(function (e) { return '<div class="err-item">⚠ ' + (e.msg || "") + "</div>"; }).join("") : "";
  }

  // ---- WebSocket ----
  function connectWS() {
    if (ws && ws.readyState === WebSocket.OPEN) return;
    var protocol = location.protocol === "https:" ? "wss:" : "ws:";
    var url = protocol + "//" + location.host + "/ws";
    ws = new WebSocket(url);

    ws.onopen = function () {
      setDot("dotApi", "green");
      $("sApi").textContent = "WS 已连接";
      if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }
    };

    ws.onmessage = function (e) {
      try { var data = JSON.parse(e.data); updateUI(data); } catch (err) { /* ignore */ }
    };

    ws.onclose = function () {
      setDot("dotApi", "yellow");
      $("sApi").textContent = "WS 断开，重连中...";
      wsReconnectTimer = setTimeout(connectWS, 3000);
    };

    ws.onerror = function () {
      ws.close();
    };
  }

  // ---- HTTP API（启停/测试） ----
  async function startStrategy() {
    $("btnStart").disabled = true;
    try { await (await fetch("/api/start", { method: "POST" })).json(); } catch (e) {}
    $("btnStart").disabled = false;
  }

  async function stopStrategy() {
    $("btnStop").disabled = true;
    try { await (await fetch("/api/stop", { method: "POST" })).json(); } catch (e) {}
    $("btnStop").disabled = false;
  }

  async function testConnection() {
    try {
      var d = await (await fetch("/api/test-connection")).json();
      var m = "";
      if (d.mainnet && d.testnet) {
        m += "主网: " + (d.mainnet.connected ? "✅" : "❌ " + (d.mainnet.auth_error || "断开"));
        m += "\n测试网: " + (d.testnet.connected ? "✅" : "❌ " + (d.testnet.auth_error || "断开"));
        if (d.testnet.connected) m += "\nBTC $" + (d.testnet.btc_index_price || "N/A") + " · USDC " + (d.testnet.usdc_balance || "N/A");
      } else { m = d.connected ? "✅ 已连接" : "❌ " + (d.auth_error || "失败"); }
      alert(m);
    } catch (e) { alert("❌ " + e.message); }
  }

  // ---- 启动 ----
  async function init() {
    createChart();

    // 自动初始化引擎（只同步数据，不启动交易）
    try {
      var resp = await fetch("/api/init", { method: "POST" });
      var result = await resp.json();
      console.log("Engine init:", result.message);
    } catch (e) {
      console.log("Engine not ready yet:", e.message);
    }

    connectWS();

    $("btnStart").addEventListener("click", startStrategy);
    $("btnStop").addEventListener("click", stopStrategy);
    $("btnTest").addEventListener("click", testConnection);

    // 加载当前 API 凭证（脱敏显示）
    try {
      var credResp = await fetch("/api/credentials");
      var creds = await credResp.json();
      $("inpApiId").value = creds.client_id_masked || "";
      $("inpTestnet").value = creds.testnet ? "1" : "0";
    } catch (e) { /* ignore */ }

    // 保存 API 凭证
    $("btnSaveCreds").addEventListener("click", async function () {
      var apiId = $("inpApiId").value.trim();
      var apiSecret = $("inpApiSecret").value.trim();
      if (!apiId || !apiSecret) { alert("请输入完整的 Client ID 和 Secret"); return; }
      if (!confirm("⚠ 修改凭证将断开当前连接并重新初始化，确定继续？")) return;
      try {
        var resp = await fetch("/api/credentials", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ client_id: apiId, client_secret: apiSecret }),
        });
        var result = await resp.json();
        if (result.success) {
          alert("✅ " + result.message);
          $("inpApiSecret").value = "";  // 清空密码框
          // 重新获取脱敏后的凭证
          var r = await (await fetch("/api/credentials")).json();
          $("inpApiId").value = r.client_id_masked || "";
        } else {
          alert("❌ " + (result.message || "保存失败"));
        }
      } catch (e) { alert("❌ " + e.message); }
    });

    // 保存参数 — 发送所有输入框的当前值
    $("btnSaveParams").addEventListener("click", async function () {
      var body = {};
      
      var anchor = parseFloat($("inpAnchor").value);
      if (anchor > 0) body.anchor_price = anchor;
      
      var tradeSize = parseFloat($("inpTradeSize").value);
      if (tradeSize > 0) body.trade_size_usdc = tradeSize;

      // rv_min / rv_max 前端显示为百分比，传回时除以 100
      var rvMin = parseFloat($("inpRvMin").value);
      if (rvMin > 0) body.rv_min = rvMin / 100;
      var rvMax = parseFloat($("inpRvMax").value);
      if (rvMax > 0) body.rv_max = rvMax / 100;

      var rvInterval = parseInt($("inpRvInterval").value);
      if (rvInterval > 0) body.rv_update_interval_minutes = rvInterval;
      var poll = parseInt($("inpPoll").value);
      if (poll > 0) body.poll_interval = poll;
      var cooldown = parseInt($("inpCooldown").value);
      if (cooldown > 0) body.cooldown_seconds = cooldown;
      var balThreshold = parseFloat($("inpBalanceThreshold").value);
      if (balThreshold > 0) body.min_poll_balance_usdc = balThreshold;

      if (Object.keys(body).length === 0) { alert("请输入有效参数"); return; }
      try {
        var resp = await fetch("/api/params", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        var result = await resp.json();
        if (result.success) {
          alert("✅ 已保存: " + (result.changed.join(", ") || "无变更"));
        } else {
          alert("❌ 保存失败: " + (result.message || "unknown"));
        }
      } catch (e) { alert("❌ " + e.message); }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else { init(); }
})();
