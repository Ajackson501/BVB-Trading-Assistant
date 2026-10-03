const express = require("express");
const WebSocket = require("ws");
const path = require("path");
const crypto = require("crypto");
const fs = require("fs");

// V3.2.7 experimental, informational only. No orders or strategy changes.
function analyzeChopRisk(candles) {
  const bars = Array.isArray(candles) ? candles.slice(-9) : [];
  if (bars.length < 9 || bars.some((b, i) =>
    ![b.open,b.high,b.low,b.close].every(Number.isFinite) ||
    b.high < Math.max(b.open,b.close) || b.low > Math.min(b.open,b.close) ||
    !Number.isFinite(Date.parse(b.time)) ||
    (i > 0 && Date.parse(b.time) - Date.parse(bars[i-1].time) !== 120000))) {
    return { state: 'WAIT', reason: 'Building recent candle history.' };
  }
  function measure(window) {
    let travel = 0, overlaps = 0, flips = 0, wickBars = 0;
    const ranges = window.map(b => b.high-b.low);
    for (let i=0;i<window.length;i++) {
      const b=window[i], range=ranges[i];
      if (range > 0 && (b.high-Math.max(b.open,b.close))/range >= 0.2 &&
          (Math.min(b.open,b.close)-b.low)/range >= 0.2) wickBars++;
      if (!i) continue;
      const p=window[i-1];
      travel += Math.abs(b.close-p.close);
      const smaller=Math.min(range,ranges[i-1]);
      if (smaller > 0 && Math.max(0,Math.min(b.high,p.high)-Math.max(b.low,p.low))/smaller >= 0.5) overlaps++;
      if ((b.close-b.open)*(p.close-p.open) < 0) flips++;
    }
    const meanRange=ranges.reduce((a,b)=>a+b,0)/window.length;
    const efficiency=travel > 0 ? Math.abs(window.at(-1).close-window[0].close)/travel : 0;
    const lowProgress=efficiency <= 0.3;
    const overlap=overlaps >= 4;
    const alternating=flips >= 3;
    const twoSided=wickBars >= 4;
    const first=window.slice(0,4).reduce((s,b)=>s+b.close,0)/4;
    const last=window.slice(-4).reduce((s,b)=>s+b.close,0)/4;
    const flat=meanRange > 0 && Math.abs(last-first) <= 0.5*meanRange;
    const score=[overlap,alternating,twoSided,flat].filter(Boolean).length;
    return { strong:meanRange > 0 && lowProgress && overlap && score >= 3,
      caution:meanRange > 0 && lowProgress && overlap && score >= 2 };
  }
  const current=measure(bars.slice(-8)), previous=measure(bars.slice(0,8));
  return { state: current.strong && previous.strong ? 'CHOP' :
      current.caution || (previous.strong && current.strong) ? 'CAUTION' : 'CLEAR',
    analyzedCandle:bars.at(-1).time };
}

const app = express();
app.use(express.json());

// Only expose the two dashboard image assets.
// Do NOT expose the whole application directory with express.static(__dirname),
// because that can make source/config files directly downloadable.
app.get("/BEARS.jpeg", (req, res) => res.sendFile(path.join(__dirname, "BEARS.jpeg")));
app.get("/BULLS.jpeg", (req, res) => res.sendFile(path.join(__dirname, "BULLS.jpeg")));

const PORT = process.env.PORT || 10000;

const ALPACA_API_KEY = process.env.ALPACA_API_KEY;
const ALPACA_SECRET_KEY = process.env.ALPACA_SECRET_KEY;

// Separate credentials for the protected BVB event feed.
// These are NOT Alpaca credentials. Store them only in Render environment variables.
const BVB_EVENT_TOKEN = process.env.BVB_EVENT_TOKEN || "";
const BVB_EVENT_USER = process.env.BVB_EVENT_USER || "";
const BVB_EVENT_PASSWORD = process.env.BVB_EVENT_PASSWORD || "";

function safeEqualText(a, b) {
  const left = Buffer.from(String(a || ""));
  const right = Buffer.from(String(b || ""));
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

function authorizeBVBEvents(req, res, next) {
  // Fail closed if protection has not been configured.
  if (!BVB_EVENT_TOKEN && !(BVB_EVENT_USER && BVB_EVENT_PASSWORD)) {
    return res.status(503).json({
      error: "BVB event feed protection is not configured"
    });
  }

  const auth = String(req.get("authorization") || "");

  // AI/API access: Authorization: Bearer <BVB_EVENT_TOKEN>
  if (BVB_EVENT_TOKEN && auth.startsWith("Bearer ")) {
    const supplied = auth.slice(7).trim();
    if (safeEqualText(supplied, BVB_EVENT_TOKEN)) return next();
  }

  // Owner/browser access: HTTP Basic authentication.
  if (BVB_EVENT_USER && BVB_EVENT_PASSWORD && auth.startsWith("Basic ")) {
    try {
      const decoded = Buffer.from(auth.slice(6), "base64").toString("utf8");
      const separator = decoded.indexOf(":");
      const user = separator >= 0 ? decoded.slice(0, separator) : "";
      const password = separator >= 0 ? decoded.slice(separator + 1) : "";
      if (safeEqualText(user, BVB_EVENT_USER) && safeEqualText(password, BVB_EVENT_PASSWORD)) {
        return next();
      }
    } catch (_) {}
  }

  res.set("WWW-Authenticate", 'Basic realm="BVB Events"');
  return res.status(401).json({ error: "Unauthorized" });
}

console.log("API key loaded:", Boolean(ALPACA_API_KEY));
console.log("Secret key loaded:", Boolean(ALPACA_SECRET_KEY));


// ==================================================
// BVB LIVE MARKET STATE
// ==================================================

let latestGOOGLTrade = null;
let alpacaStreamStatus = "connecting";

let developingCandle = null;

let completedCandles = [];
let riderCandles5m = [];
let riderCandles15m = [];
const developingRiderCandles = { 5: null, 15: null };

// Trend Event History
const MAX_TREND_EVENTS = 100;
let trendEventHistory = [];
let lastTrendEventKey = null;

const MAX_COMPLETED_CANDLES = 300;

let historySeeded = false;

// Daily higher-timeframe context. These bars are kept separate from
// the 2-minute execution engine so Daily Bias can guide without vetoing
// Oliver/BVB intraday signals.
let dailyCandles = [];
let dailyHistorySeeded = false;


// ==================================================
// INDEPENDENT PAPER-TRADING STUDY — OLIVER + AGENT C + RIDER
// ==================================================
// Uses the same GOOGL trade stream already received by BVB. Rider also loads
// completed 5-minute and 15-minute history once at startup and aggregates both
// timeframes from that same stream; no additional WebSocket is opened.
// Dashboard UI is intentionally unchanged.
//
// Persistence: set PAPER_JOURNAL_FILE to a path on a Render Persistent Disk
// (example: /var/data/bvb-paper-study.json) for deploy-safe persistence.
// Without a persistent disk, the default local file survives ordinary process
// restarts only while the instance filesystem remains available.

const PAPER_JOURNAL_FILE =
  process.env.PAPER_JOURNAL_FILE || path.join(__dirname, "bvb-paper-study.json");

const PAPER_STUDY_VERSION = "Oliver-v1.2_vs_AgentC-v1_vs_Rider-v1";

function newPaperAgentState() {
  return { position: null, trades: [], decisions: [], lastProcessedCandle: null, lastEvaluation: null };
}

let paperStudy = {
  version: PAPER_STUDY_VERSION,
  symbol: "GOOGL",
  timeframe: "2Min",
  updatedAt: null,
  agents: {
    Oliver: newPaperAgentState(),
    AgentC: newPaperAgentState(),
    Rider: newPaperAgentState()
  }
};

function loadPaperStudy() {
  try {
    if (!fs.existsSync(PAPER_JOURNAL_FILE)) return;
    const parsed = JSON.parse(fs.readFileSync(PAPER_JOURNAL_FILE, "utf8"));
    if (parsed?.agents?.Oliver && parsed?.agents?.AgentC) {
      paperStudy = parsed;
      paperStudy.version = PAPER_STUDY_VERSION;
      // Migrate earlier Oliver/Agent C journals without discarding their data.
      if (!paperStudy.agents.Rider) paperStudy.agents.Rider = newPaperAgentState();
      for (const name of ["Oliver", "AgentC", "Rider"]) {
        const agent = paperStudy.agents[name];
        if (!Array.isArray(agent.trades)) agent.trades = [];
        if (!Array.isArray(agent.decisions)) agent.decisions = [];
        if (!("lastProcessedCandle" in agent)) agent.lastProcessedCandle = null;
        if (!("lastEvaluation" in agent)) agent.lastEvaluation = null;
      }
      console.log(`Paper study loaded from ${PAPER_JOURNAL_FILE}`);
    }
  } catch (error) {
    console.error("Unable to load paper study journal:", error.message);
  }
}

function savePaperStudy() {
  try {
    paperStudy.updatedAt = new Date().toISOString();
    const directory = path.dirname(PAPER_JOURNAL_FILE);
    fs.mkdirSync(directory, { recursive: true });
    const temp = `${PAPER_JOURNAL_FILE}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(paperStudy, null, 2));
    fs.renameSync(temp, PAPER_JOURNAL_FILE);
  } catch (error) {
    console.error("Unable to save paper study journal:", error.message);
  }
}

function studyDateKey(time) {
  return newYorkDateKey(time);
}

function regularSessionForCandle(candle) {
  return getMarketSession(new Date(candle.time)).regularHours;
}

function recordPaperDecision(agentName, decision) {
  const agent = paperStudy.agents[agentName];
  if (!agent) return;
  agent.decisions.push(decision);
  if (agent.decisions.length > 2000) agent.decisions = agent.decisions.slice(-2000);
}

function openPaperPosition(agentName, direction, candle, reason, metadata = {}) {
  const agent = paperStudy.agents[agentName];
  if (!agent || agent.position) return false;
  agent.position = {
    direction,
    entryTime: candle.time,
    entryPrice: Number(candle.close),
    entryReason: reason,
    entryMetadata: metadata,
    bestPrice: Number(candle.close),
    worstPrice: Number(candle.close),
    barsHeld: 0
  };
  recordPaperDecision(agentName, {
    time: candle.time, type: "ENTRY", direction,
    price: Number(candle.close), reason
  });
  savePaperStudy();
  console.log(`${agentName} PAPER ENTRY ${direction} @ ${candle.close} — ${reason}`);
  return true;
}

function updatePaperExcursion(position, candle) {
  position.barsHeld += 1;
  if (position.direction === "CALL") {
    position.bestPrice = Math.max(position.bestPrice, Number(candle.high));
    position.worstPrice = Math.min(position.worstPrice, Number(candle.low));
  } else {
    position.bestPrice = Math.min(position.bestPrice, Number(candle.low));
    position.worstPrice = Math.max(position.worstPrice, Number(candle.high));
  }
}

function closePaperPosition(agentName, candle, reason, metadata = {}) {
  const agent = paperStudy.agents[agentName];
  if (!agent?.position) return false;
  const p = agent.position;
  const exitPrice = Number(candle.close);
  const signedMove = p.direction === "CALL"
    ? exitPrice - p.entryPrice
    : p.entryPrice - exitPrice;
  const favorableMove = p.direction === "CALL"
    ? p.bestPrice - p.entryPrice
    : p.entryPrice - p.bestPrice;
  const adverseMove = p.direction === "CALL"
    ? p.worstPrice - p.entryPrice
    : p.entryPrice - p.worstPrice;
  const trade = {
    agent: agentName,
    direction: p.direction,
    entryTime: p.entryTime,
    entryPrice: p.entryPrice,
    entryReason: p.entryReason,
    exitTime: candle.time,
    exitPrice,
    exitReason: reason,
    barsHeld: p.barsHeld,
    underlyingMove: Number(signedMove.toFixed(4)),
    maxFavorableMove: Number(favorableMove.toFixed(4)),
    maxAdverseMove: Number(adverseMove.toFixed(4)),
    result: signedMove > 0 ? "FAVORABLE" : signedMove < 0 ? "UNFAVORABLE" : "FLAT",
    metadata
  };
  agent.trades.push(trade);
  agent.position = null;
  recordPaperDecision(agentName, {
    time: candle.time, type: "EXIT", direction: trade.direction,
    price: exitPrice, reason, underlyingMove: trade.underlyingMove
  });
  savePaperStudy();
  console.log(`${agentName} PAPER EXIT ${trade.direction} @ ${exitPrice} — ${reason}`);
  return true;
}

function analyzeAgentCPaper(candles) {
  const ha = buildHeikinAshi(candles);
  if (ha.length < 4) return { signal: "WAIT", reason: "Need more HA candles." };
  const current = ha[ha.length - 1];
  const previous = ha[ha.length - 2];
  const run = getHARun(ha);

  // Agent C V1 study rules use only established HA concepts:
  // consecutive same-color control + no opposite wick = conviction.
  // Doji remains OBSERVATIONAL ONLY and never creates an entry or exit.
  const bullishConviction =
    current.color === "GREEN" && previous.color === "GREEN" &&
    run.color === "GREEN" && run.count >= 2 && current.noLowerWick;
  const bearishConviction =
    current.color === "RED" && previous.color === "RED" &&
    run.color === "RED" && run.count >= 2 && current.noUpperWick;

  return {
    signal: bullishConviction ? "CALL" : bearishConviction ? "PUT" : "WAIT",
    reason: bullishConviction
      ? "Consecutive green HA run with no lower wick (buyers in control)."
      : bearishConviction
      ? "Consecutive red HA run with no upper wick (sellers in control)."
      : "No Agent C conviction entry.",
    haColor: current.color,
    haDoji: current.isDoji,
    haRunColor: run.color,
    haRunCandles: run.count,
    noLowerWick: current.noLowerWick,
    noUpperWick: current.noUpperWick
  };
}

function analyzeRiderPaper(candles2m, candles5m, candles15m) {
  if (candles15m.length < 200 || candles5m.length < 20 || candles2m.length < 8) {
    return { signal: "WAIT", reason: "Waiting for completed 15-minute, 5-minute, and 2-minute history." };
  }
  const c2 = candles2m[candles2m.length - 1];
  const p2 = candles2m[candles2m.length - 2];
  const c5 = candles5m[candles5m.length - 1];
  const c15 = candles15m[candles15m.length - 1];
  const sma8_5 = calculateSMA(candles5m, 8);
  const sma20_5 = calculateSMA(candles5m, 20);
  const prev8_5 = calculatePreviousSMA(candles5m, 8);
  const sma20_15 = calculateSMA(candles15m, 20);
  const prev20_15 = calculatePreviousSMA(candles15m, 20);
  const sma200_15 = calculateSMA(candles15m, 200);
  const bias = Number(c15.close) > sma200_15 ? "BULLISH" : Number(c15.close) < sma200_15 ? "BEARISH" : "NEUTRAL";
  const context = Number(c15.close) > sma20_15 && sma20_15 >= prev20_15
    ? "BULLISH" : Number(c15.close) < sma20_15 && sma20_15 <= prev20_15 ? "BEARISH" : "NEUTRAL";
  const shortTrend = Number(c5.close) > sma20_5 && sma8_5 > sma20_5 && sma8_5 >= prev8_5
    ? "BULLISH" : Number(c5.close) < sma20_5 && sma8_5 < sma20_5 && sma8_5 <= prev8_5 ? "BEARISH" : "NEUTRAL";
  const prior2 = candles2m.slice(-4, -1);
  const priorHigh = Math.max(...prior2.map(c => Number(c.high)));
  const priorLow = Math.min(...prior2.map(c => Number(c.low)));
  const currentRange = Number(c2.high) - Number(c2.low);
  const compressed = prior2.every(c => Number(c.high) - Number(c.low) <= currentRange * 0.8);
  const recent5 = candles5m.slice(-5, -1);
  const bullishPullback = recent5.some((c, i) => i > 0 && Number(c.close) < Number(recent5[i - 1].close));
  const bearishPullback = recent5.some((c, i) => i > 0 && Number(c.close) > Number(recent5[i - 1].close));
  const bullishTakeover = Number(c2.close) > Number(c2.open) && Number(c2.close) > Number(p2.high);
  const bearishTakeover = Number(c2.close) < Number(c2.open) && Number(c2.close) < Number(p2.low);
  const higherLow = Number(c2.low) > Math.min(...candles2m.slice(-7, -3).map(c => Number(c.low)));
  const lowerHigh = Number(c2.high) < Math.max(...candles2m.slice(-7, -3).map(c => Number(c.high)));
  const call = bias === "BULLISH" && context === "BULLISH" && shortTrend === "BULLISH" &&
    bullishPullback && higherLow && Number(c2.close) > priorHigh && (bullishTakeover || compressed);
  const put = bias === "BEARISH" && context === "BEARISH" && shortTrend === "BEARISH" &&
    bearishPullback && lowerHigh && Number(c2.close) < priorLow && (bearishTakeover || compressed);
  return {
    signal: call ? "CALL" : put ? "PUT" : "WAIT",
    reason: call ? "15-minute bullish bias and context align with the 5-minute trend; a 2-minute higher low held and price broke the recent swing after a pullback." :
      put ? "15-minute bearish bias and context align with the 5-minute trend; a 2-minute lower high held and price broke the recent swing after a bounce." :
      "Rider is waiting for aligned 15-minute bias/context, 5-minute trend/location, and a confirmed 2-minute trigger.",
    bias, context, shortTrend,
    sma8_5: Number(sma8_5.toFixed(4)), sma20_5: Number(sma20_5.toFixed(4)), sma200_15: Number(sma200_15.toFixed(4)),
    pullback: call ? bullishPullback : put ? bearishPullback : false,
    trigger: call || put ? (compressed ? "COMPRESSION_BREAK" : "TAKEOVER_BREAK") : "NONE",
    timeframeNote: "Rider evaluates real completed 15-minute context, 5-minute trend/location, and 2-minute execution candles built from the existing live trade stream."
  };
}

function runRiderPaperTrader(candles) {
  const candle = candles[candles.length - 1];
  if (!candle || !regularSessionForCandle(candle)) return;
  const agent = paperStudy.agents.Rider;
  const analysis = analyzeRiderPaper(candles, riderCandles5m, riderCandles15m);
  agent.lastProcessedCandle = candle.time;
  agent.lastEvaluation = { time: candle.time, signal: analysis.signal, reason: analysis.reason };

  if (agent.position) {
    updatePaperExcursion(agent.position, candle);
    const p = agent.position;
    const invalidated = p.direction === "CALL"
      ? Number(candle.close) < Number(p.entryMetadata?.structureLow) || analysis.bias !== "BULLISH" || analysis.shortTrend === "BEARISH"
      : Number(candle.close) > Number(p.entryMetadata?.structureHigh) || analysis.bias !== "BEARISH" || analysis.shortTrend === "BULLISH";
    const opposite = (p.direction === "CALL" && analysis.signal === "PUT") ||
      (p.direction === "PUT" && analysis.signal === "CALL");
    if (invalidated) {
      closePaperPosition("Rider", candle, "Rider's 2-minute swing structure broke on a candle close.", { analysis });
    } else if (opposite) {
      closePaperPosition("Rider", candle, "Opposing Rider setup confirmed.", { analysis });
    }
    return;
  }

  if (analysis.signal === "CALL" || analysis.signal === "PUT") {
    const recent = candles.slice(-8, -1);
    openPaperPosition("Rider", analysis.signal, candle, analysis.reason, {
      ...analysis,
      structureLow: Math.min(...recent.map(c => Number(c.low))),
      structureHigh: Math.max(...recent.map(c => Number(c.high)))
    });
  }
}

function runOliverPaperTrader(candles) {
  if (candles.length < 200) return;
  const candle = candles[candles.length - 1];
  if (!regularSessionForCandle(candle)) return;
  const agent = paperStudy.agents.Oliver;
  const analysis = analyzeOliver(candles);
  agent.lastEvaluation = {
    time: candle.time,
    signal: agent.position ? `HOLD_${agent.position.direction}` :
      analysis.action === "CALL_SETUP" ? "CALL" : analysis.action === "PUT_SETUP" ? "PUT" : "WAIT",
    reason: agent.position ? "Managing an open paper position." : analysis.reason || "No Oliver entry setup."
  };

  if (agent.position) {
    updatePaperExcursion(agent.position, candle);
    const p = agent.position;
    const invalidated = Number.isFinite(Number(p.entryMetadata?.invalidation)) &&
      (p.direction === "CALL"
        ? Number(candle.low) <= Number(p.entryMetadata.invalidation)
        : Number(candle.high) >= Number(p.entryMetadata.invalidation));
    const oppositeSetup =
      (p.direction === "CALL" && analysis.action === "PUT_SETUP") ||
      (p.direction === "PUT" && analysis.action === "CALL_SETUP");
    const regimeLost =
      (p.direction === "CALL" && analysis.regime !== "BULLISH_REGIME") ||
      (p.direction === "PUT" && analysis.regime !== "BEARISH_REGIME");

    if (invalidated) return closePaperPosition("Oliver", candle, "Oliver invalidation level reached.", { analysis });
    if (oppositeSetup) return closePaperPosition("Oliver", candle, "Opposite Oliver setup confirmed.", { analysis });
    if (regimeLost) return closePaperPosition("Oliver", candle, "Oliver 200 SMA regime authorization lost.", { analysis });
    return;
  }

  if (analysis.action === "CALL_SETUP") {
    openPaperPosition("Oliver", "CALL", candle, analysis.reason, {
      entryEvent: analysis.entryEvent,
      trigger: analysis.trigger,
      invalidation: analysis.invalidation,
      regime: analysis.regime
    });
  } else if (analysis.action === "PUT_SETUP") {
    openPaperPosition("Oliver", "PUT", candle, analysis.reason, {
      entryEvent: analysis.entryEvent,
      trigger: analysis.trigger,
      invalidation: analysis.invalidation,
      regime: analysis.regime
    });
  }
}

function runAgentCPaperTrader(candles) {
  if (candles.length < 4) return;
  const candle = candles[candles.length - 1];
  if (!regularSessionForCandle(candle)) return;
  const agent = paperStudy.agents.AgentC;
  const analysis = analyzeAgentCPaper(candles);
  agent.lastEvaluation = {
    time: candle.time,
    signal: agent.position ? `HOLD_${agent.position.direction}` : analysis.signal,
    reason: agent.position ? "Managing an open paper position." : analysis.reason
  };

  if (agent.position) {
    updatePaperExcursion(agent.position, candle);
    // Dojis are deliberately not exits. Agent C exits only when the
    // established opposite directional HA control is confirmed.
    const opposite =
      (agent.position.direction === "CALL" && analysis.signal === "PUT") ||
      (agent.position.direction === "PUT" && analysis.signal === "CALL");
    if (opposite) {
      const oldDirection = agent.position.direction;
      closePaperPosition("AgentC", candle, "Opposite Agent C HA conviction confirmed.", { analysis });
      // Reversal can become the next independent paper position on the same
      // completed candle because the opposite conviction itself is the signal.
      openPaperPosition("AgentC", analysis.signal, candle, analysis.reason, analysis);
      return;
    }
    return;
  }

  if (analysis.signal === "CALL" || analysis.signal === "PUT") {
    openPaperPosition("AgentC", analysis.signal, candle, analysis.reason, analysis);
  }
}

function runIndependentPaperStudy(candles) {
  if (!candles.length) return;
  try {
    const candle = candles[candles.length - 1];
    if (!regularSessionForCandle(candle)) return;
    runOliverPaperTrader(candles);
    runAgentCPaperTrader(candles);
    runRiderPaperTrader(candles);
    for (const name of ["Oliver", "AgentC", "Rider"]) {
      const agent = paperStudy.agents[name];
      agent.lastProcessedCandle = candle.time;
      const evaluatedSignal = agent.lastEvaluation?.signal || "WAIT";
      agent.lastEvaluation = { ...agent.lastEvaluation, evaluatedSignal,
        signal: agent.position ? `HOLD_${agent.position.direction}` : "WAIT",
        positionState: agent.position?.direction || "FLAT" };
    }
    savePaperStudy();
    for (const name of ["Oliver", "AgentC", "Rider"]) {
      const agent = paperStudy.agents[name];
      console.log(`${name} PAPER CHECK ${candle.time} — ${agent.lastEvaluation?.signal || "WAIT"}${agent.position ? `; POSITION ${agent.position.direction} @ ${agent.position.entryPrice}` : "; FLAT"}`);
    }
  } catch (error) {
    console.error("Independent paper-study error:", error);
  }
}

function closePaperPositionsAtSessionEnd() {
  const session = getMarketSession(new Date());
  if (session.regularHours || completedCandles.length === 0) return;
  const last = completedCandles[completedCandles.length - 1];
  // Only close positions whose entry day is today ET, and only after 4 PM ET.
  const nowET = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", hour: "2-digit", minute: "2-digit", hour12: false
  }).format(new Date());
  const [h, m] = nowET.split(":").map(Number);
  if ((h * 60 + m) < 960) return;
  for (const name of ["Oliver", "AgentC", "Rider"]) {
    const pos = paperStudy.agents[name].position;
    if (pos && studyDateKey(pos.entryTime) === studyDateKey(new Date())) {
      closePaperPosition(name, last, "Regular market session ended.");
    }
  }
}

function paperDailySummary(dateKey = studyDateKey(new Date())) {
  const summarize = (name) => {
    const agent = paperStudy.agents[name];
    const trades = agent.trades.filter(t => studyDateKey(t.entryTime) === dateKey);
    const favorable = trades.filter(t => t.result === "FAVORABLE").length;
    const unfavorable = trades.filter(t => t.result === "UNFAVORABLE").length;
    const flat = trades.filter(t => t.result === "FLAT").length;
    const totalUnderlyingMove = trades.reduce((sum, t) => sum + Number(t.underlyingMove || 0), 0);
    return {
      trades: trades.length, favorable, unfavorable, flat,
      totalUnderlyingMove: Number(totalUnderlyingMove.toFixed(4)),
      openPosition: agent.position,
      tradeLog: trades
    };
  };
  return {
    date: dateKey,
    symbol: "GOOGL",
    timeframe: "2Min",
    note: "Results measure GOOGL underlying movement from paper entry to paper exit; they are not option-contract P/L.",
    Oliver: summarize("Oliver"),
    AgentC: summarize("AgentC"),
    Rider: summarize("Rider"),
    agentHealth: Object.fromEntries(["Oliver", "AgentC", "Rider"].map(name => [name, {
      lastProcessedCandle: paperStudy.agents[name].lastProcessedCandle,
      lastEvaluation: paperStudy.agents[name].lastEvaluation,
      activePosition: paperStudy.agents[name].position
    }]))
  };
}

loadPaperStudy();
setInterval(closePaperPositionsAtSessionEnd, 60 * 1000);

// Trend Hold is an observational state, never an order or position tracker.
let trendHold = { regime: "WAIT", stage: "WAIT", direction: "NONE", counterBars: 0, analyzedCandle: null, reason: "Waiting for completed candles." };

function updateTrendHold(candles, battle) {
  const last = candles[candles.length - 1];
  if (!last || trendHold.analyzedCandle === last.time) return trendHold;
  if (candles.length < 21) {
    trendHold = { regime: "WAIT", stage: "WAIT", direction: "NONE", counterBars: 0,
      analyzedCandle: last.time, reason: "Building enough completed candles to evaluate trend health." };
    return trendHold;
  }

  const close = Number(last.close);
  const sma8 = calculateSMA(candles, 8);
  const sma20 = calculateSMA(candles, 20);
  const prev8 = calculatePreviousSMA(candles, 8);
  const prev20 = calculatePreviousSMA(candles, 20);
  const sma200 = candles.length >= 200 ? calculateSMA(candles, 200) : null;
  const bullCore = close > sma8 && sma8 > sma20 && sma8 >= prev8 && sma20 >= prev20;
  const bearCore = close < sma8 && sma8 < sma20 && sma8 <= prev8 && sma20 <= prev20;
  const candidate = bullCore ? "BULL" : bearCore ? "BEAR" : "NONE";
  const old = trendHold;

  let direction = old.direction;
  if (direction === "NONE" && candidate !== "NONE") direction = candidate;

  const againstControl = direction === "BULL" ? battle.control === "BEARS" : direction === "BEAR" ? battle.control === "BULLS" : false;
  const againstHA = direction === "BULL" ? battle.haColor === "RED" : direction === "BEAR" ? battle.haColor === "GREEN" : false;
  const lost8 = direction === "BULL" ? close < sma8 : direction === "BEAR" ? close > sma8 : false;
  const lost20 = direction === "BULL" ? close < sma20 : direction === "BEAR" ? close > sma20 : false;
  const oppositeCore = direction === "BULL" ? bearCore : direction === "BEAR" ? bullCore : false;
  const warningEvidence = [againstControl, againstHA, lost8, battle.changeWatch === "WATCH" || battle.changeWatch === "WARNING"].filter(Boolean).length;
  const brokenEvidence = [oppositeCore, lost20, againstControl, battle.changeWatch === "WARNING"].filter(Boolean).length;

  let counterBars = (direction !== "NONE" && brokenEvidence >= 2) ? old.counterBars + 1 : 0;
  let stage = "WAIT";
  let reason = "Waiting for a clear two-minute directional trend.";

  if (direction !== "NONE") {
    if (counterBars >= 2 || (oppositeCore && lost20 && againstControl)) {
      stage = "REGIME_BROKEN";
      reason = "Two-minute trend structure has broken; opposing evidence persisted with opposing control.";
      direction = "NONE";
      counterBars = 0;
    } else if (brokenEvidence >= 2 || warningEvidence >= 2) {
      stage = "WARNING";
      reason = "Two-minute trend is weakening; opposing structure/control evidence is developing.";
    } else {
      stage = "HOLD";
      // Keep indicator calculations private in user-facing Trend Hold language.
      reason = "Two-minute trend remains intact.";
    }
  }

  trendHold = { regime: direction === "NONE" ? "NEUTRAL" : `${direction}_TREND`, stage, direction,
    counterBars, analyzedCandle: last.time, reason,
    sma8: Number(sma8.toFixed(4)), sma20: Number(sma20.toFixed(4)),
    sma200: Number.isFinite(sma200) ? Number(sma200.toFixed(4)) : null };
  return trendHold;
}

// ==================================================
// WEBSOCKET STATE
// ==================================================

let alpacaWS = null;
let reconnectTimer = null;

const NORMAL_RECONNECT_DELAY = 5000;

const CONNECTION_LIMIT_RETRY_DELAY = 15000;


// ==================================================
// NORMALIZE ALPACA BAR
// ==================================================

function normalizeBar(bar) {

  return {

    time: bar.t,

    open: Number(bar.o),

    high: Number(bar.h),

    low: Number(bar.l),

    close: Number(bar.c),

    volume: Number(bar.v) || 0

  };

}


// ==================================================
// ADD COMPLETED CANDLE
// ==================================================

function addCompletedCandle(candle) {

  if (!candle || !candle.time) {
    return;
  }


  const normalized = {

    time: candle.time,

    open: Number(candle.open),

    high: Number(candle.high),

    low: Number(candle.low),

    close: Number(candle.close),

    volume: Number(candle.volume) || 0

  };


  const existingIndex =
    completedCandles.findIndex(
      (bar) =>
        bar.time === normalized.time
    );


  if (existingIndex !== -1) {

    completedCandles[existingIndex] =
      normalized;

  } else {

    completedCandles.push(
      normalized
    );

  }


  completedCandles.sort(
    (a, b) =>
      new Date(a.time) -
      new Date(b.time)
  );


  if (
    completedCandles.length >
    MAX_COMPLETED_CANDLES
  ) {

    completedCandles =
      completedCandles.slice(
        -MAX_COMPLETED_CANDLES
      );

  }
// Run Trend Battle analysis whenever a new 2-minute candle completes
const trendAnalysis = analyzeTrendBattle(completedCandles);

if (trendAnalysis) {
  updateTrendHold(completedCandles, trendAnalysis);
  recordTrendEvent(trendAnalysis);
}

// Feed the same completed candle to both independent paper traders.
// This performs local calculations only and does not make another Alpaca request.
runIndependentPaperStudy(completedCandles);
}


// ==================================================
// SEED HISTORY FROM ALPACA
// ==================================================

async function seedHistoricalCandles() {

  if (
    !ALPACA_API_KEY ||
    !ALPACA_SECRET_KEY
  ) {

    console.error(
      "Cannot seed candle history: Alpaca credentials missing."
    );

    return;
  }


  try {

    console.log(
      "Seeding 2-minute candle history from Alpaca..."
    );


    // Pull several trading days so Oliver has enough
    // completed 2-minute candles for the 200 SMA.

    const end = new Date();

    const start = new Date();

    start.setUTCDate(
      start.getUTCDate() - 7
    );


    const params =
      new URLSearchParams({

        timeframe: "2Min",

        start:
          start.toISOString(),

        end:
          end.toISOString(),

        limit: "1000",

        feed: "iex",

        adjustment: "raw"

      });


    const url =
      `https://data.alpaca.markets/v2/stocks/GOOGL/bars?${params.toString()}`;


    const response =
      await fetch(
        url,
        {

          headers: {

            "APCA-API-KEY-ID":
              ALPACA_API_KEY,

            "APCA-API-SECRET-KEY":
              ALPACA_SECRET_KEY

          }

        }
      );


    const data =
      await response.json();


    if (!response.ok) {

      console.error(
        "Historical seed failed:",
        response.status,
        data
      );

      return;
    }


    const bars =
      data.bars || [];


    completedCandles =
      bars
        .map(normalizeBar)
        .sort(
          (a, b) =>
            new Date(a.time) -
            new Date(b.time)
        )
        .slice(
          -MAX_COMPLETED_CANDLES
        );


    historySeeded = true;

    // The historical bars are context; establish a state from the latest
    // completed bar once daily history is available, without emitting events.
    trendHold.analyzedCandle = null;
    updateTrendHold(completedCandles, analyzeTrendBattle(completedCandles));


    console.log(
      `Historical seed complete: ${completedCandles.length} candles loaded.`
    );


  } catch (error) {

    console.error(
      "Historical seed error:",
      error
    );

  }

}

async function seedRiderBars(timeframeMinutes, targetName) {
  if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY) return;
  try {
    const end = new Date();
    const start = new Date();
    start.setUTCDate(start.getUTCDate() - 30);
    const params = new URLSearchParams({
      timeframe: `${timeframeMinutes}Min`, start: start.toISOString(), end: end.toISOString(),
      limit: "1000", feed: "iex", adjustment: "raw"
    });
    const response = await fetch(`https://data.alpaca.markets/v2/stocks/GOOGL/bars?${params.toString()}`, {
      headers: { "APCA-API-KEY-ID": ALPACA_API_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY }
    });
    const data = await response.json();
    if (!response.ok) {
      console.error(`Rider ${timeframeMinutes}-minute history seed failed:`, response.status, data);
      return;
    }
    const nowMs = Date.now();
    const bars = (data.bars || []).map(normalizeBar)
      .filter(bar => new Date(bar.time).getTime() + timeframeMinutes * 60 * 1000 <= nowMs)
      .sort((a, b) => new Date(a.time) - new Date(b.time));
    if (targetName === "5m") riderCandles5m = bars.slice(-1000);
    else riderCandles15m = bars.slice(-1000);
    console.log(`Rider ${timeframeMinutes}-minute history seed complete: ${bars.length} bars loaded.`);
  } catch (error) {
    console.error(`Rider ${timeframeMinutes}-minute history seed error:`, error.message);
  }
}

function addCompletedRiderCandle(timeframeMinutes, candle) {
  const target = timeframeMinutes === 5 ? riderCandles5m : riderCandles15m;
  const normalized = {
    time: candle.time, open: Number(candle.open), high: Number(candle.high),
    low: Number(candle.low), close: Number(candle.close), volume: Number(candle.volume) || 0
  };
  const index = target.findIndex(bar => bar.time === normalized.time);
  if (index >= 0) target[index] = normalized;
  else target.push(normalized);
  target.sort((a, b) => new Date(a.time) - new Date(b.time));
  if (target.length > 1000) target.splice(0, target.length - 1000);
}


// ==================================================
// SEED COMPLETED DAILY HISTORY FROM ALPACA
// ==================================================

function newYorkDateKey(dateInput) {
  const date = dateInput instanceof Date ? dateInput : new Date(dateInput);
  if (Number.isNaN(date.getTime())) return null;

  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(date);

  const get = (type) => parts.find((part) => part.type === type)?.value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

async function seedDailyCandles() {
  if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY) {
    console.error("Cannot seed daily history: Alpaca credentials missing.");
    return;
  }

  try {
    console.log("Seeding five completed daily GOOGL candles from Alpaca...");

    const end = new Date();
    const start = new Date();
    start.setUTCDate(start.getUTCDate() - 30);

    const params = new URLSearchParams({
      timeframe: "1Day",
      start: start.toISOString(),
      end: end.toISOString(),
      limit: "1000",
      feed: "iex",
      adjustment: "raw"
    });

    const response = await fetch(
      `https://data.alpaca.markets/v2/stocks/GOOGL/bars?${params.toString()}`,
      {
        headers: {
          "APCA-API-KEY-ID": ALPACA_API_KEY,
          "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
        }
      }
    );

    const data = await response.json();

    if (!response.ok) {
      console.error("Daily seed failed:", response.status, data);
      return;
    }

    const todayET = newYorkDateKey(new Date());

    // Daily Bias deliberately uses completed daily bars only. The current
    // trading day's still-forming daily candle is excluded.
    dailyCandles = (data.bars || [])
      .map(normalizeBar)
      .filter((bar) => newYorkDateKey(bar.time) !== todayET)
      .sort((a, b) => new Date(a.time) - new Date(b.time))
      .slice(-5);

    dailyHistorySeeded = dailyCandles.length >= 5;
    trendHold.analyzedCandle = null;
    updateTrendHold(completedCandles, analyzeTrendBattle(completedCandles));

    console.log(
      `Daily seed complete: ${dailyCandles.length} completed daily candles loaded.`
    );
  } catch (error) {
    console.error("Daily seed error:", error);
  }
}

// ==================================================
// BUILD LIVE 2-MINUTE CANDLE
// ==================================================

function updateDevelopingRiderCandles(trade) {
  const price = Number(trade.price);
  const size = Number(trade.size) || 0;
  const tradeTime = new Date(trade.time);
  if (!Number.isFinite(price) || Number.isNaN(tradeTime.getTime())) return;
  for (const minutes of [5, 15]) {
    const duration = minutes * 60 * 1000;
    const bucketStart = Math.floor(tradeTime.getTime() / duration) * duration;
    const bucketTime = new Date(bucketStart).toISOString();
    let candle = developingRiderCandles[minutes];
    if (!candle || candle.time !== bucketTime) {
      if (candle) addCompletedRiderCandle(minutes, candle);
      developingRiderCandles[minutes] = {
        time: bucketTime, open: price, high: price, low: price, close: price, volume: size
      };
      continue;
    }
    candle.high = Math.max(candle.high, price);
    candle.low = Math.min(candle.low, price);
    candle.close = price;
    candle.volume += size;
  }
}

function updateDevelopingCandle(trade) {

  const price =
    Number(trade.price);

  const size =
    Number(trade.size) || 0;

  const tradeTime =
    new Date(trade.time);


  if (
    !Number.isFinite(price) ||
    Number.isNaN(
      tradeTime.getTime()
    )
  ) {

    return;

  }


  const bucket =
    new Date(tradeTime);


  bucket.setUTCSeconds(
    0,
    0
  );


  bucket.setUTCMinutes(
    Math.floor(
      bucket.getUTCMinutes() / 2
    ) * 2
  );


  const bucketTime =
    bucket.toISOString();


  // ------------------------------------------------
  // NEW 2-MINUTE PERIOD
  // ------------------------------------------------

  if (
    !developingCandle ||
    developingCandle.time !==
      bucketTime
  ) {


    if (developingCandle) {

      addCompletedCandle(
        developingCandle
      );


      console.log(
        "Completed 2-minute candle:",
        JSON.stringify(
          developingCandle
        )
      );

    }


    developingCandle = {

      time: bucketTime,

      open: price,

      high: price,

      low: price,

      close: price,

      volume: size

    };


    return;
  }


  // ------------------------------------------------
  // UPDATE CURRENT CANDLE
  // ------------------------------------------------

  developingCandle.high =
    Math.max(
      developingCandle.high,
      price
    );


  developingCandle.low =
    Math.min(
      developingCandle.low,
      price
    );


  developingCandle.close =
    price;


  developingCandle.volume +=
    size;

}


// ==================================================
// RECONNECT SCHEDULER
// ==================================================

function scheduleReconnect(
  delay = NORMAL_RECONNECT_DELAY
) {

  if (reconnectTimer) {
    return;
  }


  alpacaStreamStatus =
    "reconnecting";


  console.log(
    `Alpaca reconnect scheduled in ${delay / 1000} seconds.`
  );


  reconnectTimer =
    setTimeout(
      () => {

        reconnectTimer = null;

        connectAlpacaStream();

      },
      delay
    );

}


// ==================================================
// ALPACA LIVE WEBSOCKET
// ==================================================

function connectAlpacaStream() {

  if (
    !ALPACA_API_KEY ||
    !ALPACA_SECRET_KEY
  ) {

    alpacaStreamStatus =
      "credentials_missing";


    console.error(
      "Alpaca credentials are not configured."
    );


    return;
  }


  // Never intentionally open two sockets
  // inside the same Node process.

  if (
    alpacaWS &&
    (
      alpacaWS.readyState ===
        WebSocket.OPEN ||

      alpacaWS.readyState ===
        WebSocket.CONNECTING
    )
  ) {

    console.log(
      "Alpaca WebSocket already active. Skipping duplicate connection."
    );

    return;
  }


  alpacaStreamStatus =
    "connecting";


  console.log(
    "Opening Alpaca WebSocket..."
  );


  const ws =
    new WebSocket(
      "wss://stream.data.alpaca.markets/v2/iex"
    );


  alpacaWS = ws;


  let connectionLimitDetected =
    false;


  // ------------------------------------------------
  // SOCKET OPENED
  // ------------------------------------------------

  ws.on(
    "open",
    () => {

      console.log(
        "Alpaca WebSocket connected"
      );


      ws.send(
        JSON.stringify({

          action: "auth",

          key:
            ALPACA_API_KEY,

          secret:
            ALPACA_SECRET_KEY

        })
      );

    }
  );


  // ------------------------------------------------
  // RECEIVE ALPACA DATA
  // ------------------------------------------------

  ws.on(
    "message",
    (data) => {

      let messages;


      try {

        messages =
          JSON.parse(
            data.toString()
          );

      } catch (error) {

        console.error(
          "Unable to parse Alpaca message:",
          error.message
        );

        return;
      }


      if (!Array.isArray(messages)) {

        messages = [messages];

      }


      for (
        const message of messages
      ) {


        if (message.T !== "t") {

          console.log(
            "ALPACA MESSAGE:",
            JSON.stringify(
              message
            )
          );

        }


        // ------------------------------------------
        // AUTHENTICATED
        // ------------------------------------------

        if (
          message.T ===
            "success" &&

          message.msg ===
            "authenticated"
        ) {

          alpacaStreamStatus =
            "connected";


          ws.send(
            JSON.stringify({

              action:
                "subscribe",

              trades:
                ["GOOGL"]

            })
          );


          console.log(
            "Subscribed to live GOOGL trades"
          );


          continue;
        }


        // ------------------------------------------
        // ALPACA ERROR
        // ------------------------------------------

        if (
          message.T ===
            "error"
        ) {

          console.error(
            `Alpaca stream error ${
              message.code || ""
            }: ${
              message.msg ||
              "unknown error"
            }`
          );


          const isConnectionLimit =
            Number(
              message.code
            ) === 406 ||

            String(
              message.msg || ""
            )
              .toLowerCase()
              .includes(
                "connection limit"
              );


          if (isConnectionLimit) {

            connectionLimitDetected =
              true;


            alpacaStreamStatus =
              "waiting_for_connection_slot";


            console.log(
              "Alpaca connection slot is busy."
            );


            console.log(
              "This may be the previous Render instance shutting down."
            );


            console.log(
              "Will retry automatically in 15 seconds."
            );


            try {

              ws.close();

            } catch (_) {}


            continue;
          }


          alpacaStreamStatus =
            "error";


          continue;
        }


        // ------------------------------------------
        // LIVE GOOGL TRADE
        // ------------------------------------------

        if (
          message.T === "t" &&
          message.S === "GOOGL"
        ) {

          latestGOOGLTrade = {

            price:
              Number(
                message.p
              ),

            size:
              Number(
                message.s
              ) || 0,

            time:
              message.t

          };


          updateDevelopingRiderCandles(latestGOOGLTrade);
          updateDevelopingCandle(
            latestGOOGLTrade
          );

        }

      }

    }
  );


  // ------------------------------------------------
  // SOCKET ERROR
  // ------------------------------------------------

  ws.on(
    "error",
    (error) => {

      console.error(
        "Alpaca WebSocket error:",
        error.message
      );

    }
  );


  // ------------------------------------------------
  // SOCKET CLOSED
  // ------------------------------------------------

  ws.on(
    "close",
    () => {

      console.log(
        "Alpaca WebSocket disconnected"
      );


      if (alpacaWS === ws) {

        alpacaWS = null;

      }


      if (connectionLimitDetected) {

        scheduleReconnect(
          CONNECTION_LIMIT_RETRY_DELAY
        );

        return;
      }


      alpacaStreamStatus =
        "disconnected";


      scheduleReconnect(
        NORMAL_RECONNECT_DELAY
      );

    }
  );

}


// ==================================================
// GRACEFUL SHUTDOWN
// ==================================================

function shutdown() {

  console.log(
    "Server shutting down. Closing Alpaca WebSocket."
  );


  if (reconnectTimer) {

    clearTimeout(
      reconnectTimer
    );

    reconnectTimer = null;

  }


  if (alpacaWS) {

    try {

      alpacaWS.close();

    } catch (_) {}

  }


  setTimeout(
    () => process.exit(0),
    500
  );

}


process.on(
  "SIGTERM",
  shutdown
);


process.on(
  "SIGINT",
  shutdown
);

// ==================================================
// OLIVER ENGINE V1.1
// ==================================================
//
// Adds earlier trend-entry recognition:
//
// 1. Takeover near 8/20 SMA
// 2. Pullback/compression -> expansion
// 3. Reversal cluster -> directional break
//
// Uses COMPLETED 2-minute candles only.
// ==================================================


// --------------------------------------------------
// SIMPLE MOVING AVERAGE
// --------------------------------------------------

function calculateSMA(candles, period) {

  if (
    !Array.isArray(candles) ||
    candles.length < period
  ) {
    return null;
  }

  const selected =
    candles.slice(-period);

  const total =
    selected.reduce(
      (sum, candle) =>
        sum + Number(candle.close),
      0
    );

  return total / period;
}


// --------------------------------------------------
// PREVIOUS SMA
// --------------------------------------------------

function calculatePreviousSMA(
  candles,
  period
) {

  if (
    !Array.isArray(candles) ||
    candles.length < period + 1
  ) {
    return null;
  }

  const selected =
    candles.slice(
      -(period + 1),
      -1
    );

  const total =
    selected.reduce(
      (sum, candle) =>
        sum + Number(candle.close),
      0
    );

  return total / period;
}


// --------------------------------------------------
// CANDLE HELPERS
// --------------------------------------------------

function candleDirection(candle) {

  if (!candle) {
    return "UNKNOWN";
  }

  const open =
    Number(candle.open);

  const close =
    Number(candle.close);

  if (close > open) {
    return "GREEN";
  }

  if (close < open) {
    return "RED";
  }

  return "DOJI";
}


function candleBody(candle) {

  if (!candle) {
    return 0;
  }

  return Math.abs(
    Number(candle.close) -
    Number(candle.open)
  );
}


function candleRange(candle) {

  if (!candle) {
    return 0;
  }

  return Math.max(
    0,
    Number(candle.high) -
    Number(candle.low)
  );
}


function averageBody(candles) {

  if (
    !Array.isArray(candles) ||
    candles.length === 0
  ) {
    return 0;
  }

  const total =
    candles.reduce(
      (sum, candle) =>
        sum + candleBody(candle),
      0
    );

  return total / candles.length;
}


// --------------------------------------------------
// TAKEOVER CANDLES
// --------------------------------------------------

function isBullishTakeover(
  previous,
  current
) {

  if (!previous || !current) {
    return false;
  }

  if (
    candleDirection(previous) !== "RED" ||
    candleDirection(current) !== "GREEN"
  ) {
    return false;
  }

  return (
    Number(current.close) >
      Number(previous.open) &&

    Number(current.high) >=
      Number(previous.high)
  );
}


function isBearishTakeover(
  previous,
  current
) {

  if (!previous || !current) {
    return false;
  }

  if (
    candleDirection(previous) !== "GREEN" ||
    candleDirection(current) !== "RED"
  ) {
    return false;
  }

  return (
    Number(current.close) <
      Number(previous.open) &&

    Number(current.low) <=
      Number(previous.low)
  );
}


// --------------------------------------------------
// SHORT-TERM STRUCTURE
// --------------------------------------------------

function detectStructure(candles) {

  if (
    !Array.isArray(candles) ||
    candles.length < 4
  ) {
    return "INSUFFICIENT_DATA";
  }

  const recent =
    candles.slice(-4);

  const a = recent[0];
  const b = recent[1];
  const c = recent[2];
  const d = recent[3];

  const bullish =
    Number(c.high) >
      Number(a.high) &&
    Number(d.low) >
      Number(b.low);

  const bearish =
    Number(c.low) <
      Number(a.low) &&
    Number(d.high) <
      Number(b.high);

  if (bullish) {
    return "HH_HL";
  }

  if (bearish) {
    return "LH_LL";
  }

  return "MIXED";
}


// --------------------------------------------------
// EXPANSION CANDLE
// --------------------------------------------------

function detectExpansion(candles) {

  if (
    !Array.isArray(candles) ||
    candles.length < 3
  ) {
    return null;
  }

  const previous2 =
    candles[candles.length - 3];

  const previous1 =
    candles[candles.length - 2];

  const current =
    candles[candles.length - 1];

  const priorAverage =
    (
      candleBody(previous2) +
      candleBody(previous1)
    ) / 2;

  if (priorAverage <= 0) {
    return null;
  }

  if (
    candleBody(current) >=
    priorAverage * 1.5
  ) {
    return candleDirection(current);
  }

  return null;
}


// ==================================================
// V1.1 PATTERN #1
// TAKEOVER NEAR 8/20 SMA
// ==================================================

function detectTakeoverNearSMA(
  candles,
  sma8,
  sma20
) {

  if (
    !Array.isArray(candles) ||
    candles.length < 2 ||
    !Number.isFinite(sma8) ||
    !Number.isFinite(sma20)
  ) {
    return null;
  }

  const previous =
    candles[candles.length - 2];

  const current =
    candles[candles.length - 1];

  const price =
    Number(current.close);

  const tolerance =
    Math.max(
      price * 0.0025,
      0.20
    );

  const near8 =
    Math.min(
      Math.abs(Number(current.low) - sma8),
      Math.abs(Number(current.high) - sma8),
      Math.abs(price - sma8)
    ) <= tolerance;

  const near20 =
    Math.min(
      Math.abs(Number(current.low) - sma20),
      Math.abs(Number(current.high) - sma20),
      Math.abs(price - sma20)
    ) <= tolerance;

  if (!near8 && !near20) {
    return null;
  }

  if (
    isBullishTakeover(
      previous,
      current
    )
  ) {

    return {
      direction: "BULLISH",
      near:
        near20
          ? "20_SMA"
          : "8_SMA"
    };
  }

  if (
    isBearishTakeover(
      previous,
      current
    )
  ) {

    return {
      direction: "BEARISH",
      near:
        near20
          ? "20_SMA"
          : "8_SMA"
    };
  }

  return null;
}


// ==================================================
// V1.1 PATTERN #2
// COMPRESSION -> EXPANSION
// ==================================================

function detectCompressionExpansion(
  candles
) {

  if (
    !Array.isArray(candles) ||
    candles.length < 6
  ) {
    return null;
  }

  const current =
    candles[candles.length - 1];

  const compression =
    candles.slice(-3, -1);

  const baseline =
    candles.slice(-6, -3);

  const baselineBody =
    averageBody(baseline);

  const compressionBody =
    averageBody(compression);

  const currentBody =
    candleBody(current);

  if (
    baselineBody <= 0 ||
    compressionBody <= 0
  ) {
    return null;
  }

  const compressed =
    compressionBody <=
    baselineBody * 0.75;

  const expanded =
    currentBody >=
    compressionBody * 1.5;

  if (
    !compressed ||
    !expanded
  ) {
    return null;
  }

  const direction =
    candleDirection(current);

  if (
    direction !== "GREEN" &&
    direction !== "RED"
  ) {
    return null;
  }

  return direction === "GREEN"
    ? "BULLISH"
    : "BEARISH";
}


// ==================================================
// V1.1 PATTERN #3
// REVERSAL CLUSTER -> BREAK
// ==================================================

function detectReversalBreak(
  candles
) {

  if (
    !Array.isArray(candles) ||
    candles.length < 5
  ) {
    return null;
  }

  const a =
    candles[candles.length - 5];

  const b =
    candles[candles.length - 4];

  const c =
    candles[candles.length - 3];

  const d =
    candles[candles.length - 2];

  const current =
    candles[candles.length - 1];


  // Bullish:
  // higher low develops,
  // two green candles,
  // current candle breaks prior high.

  const bullish =
    Number(c.low) >
      Number(a.low) &&

    candleDirection(d) ===
      "GREEN" &&

    candleDirection(current) ===
      "GREEN" &&

    Number(current.high) >
      Number(d.high);


  if (bullish) {

    return {
      direction:
        "BULLISH",

      breakLevel:
        Number(d.high)
    };
  }


  // Bearish:
  // lower high develops,
  // two red candles,
  // current candle breaks prior low.

  const bearish =
    Number(c.high) <
      Number(a.high) &&

    candleDirection(d) ===
      "RED" &&

    candleDirection(current) ===
      "RED" &&

    Number(current.low) <
      Number(d.low);


  if (bearish) {

    return {
      direction:
        "BEARISH",

      breakLevel:
        Number(d.low)
    };
  }


  return null;
}


// ==================================================
// OLIVER ANALYSIS V1.2
// Restores full 200 SMA regime authorization
// ==================================================
function analyzeOliver(candles) {
  if (
    !Array.isArray(candles) ||
    candles.length < 200
  ) {
    return {
      action: "WAIT",
      reason:
        "Need at least 200 completed candles for Oliver's 200 SMA.",
      sma200Status: "BUILDING",
      completedCandles:
        Array.isArray(candles)
          ? candles.length
          : 0,
      candlesNeeded:
        Math.max(
          0,
          200 -
            (Array.isArray(candles)
              ? candles.length
              : 0)
        )
    };
  }

  const current =
    candles[candles.length - 1];

  const previous =
    candles[candles.length - 2];

  const price =
    Number(current.close);

  // ------------------------------------------------
  // MOVING AVERAGES
  // ------------------------------------------------
  const sma8 =
    calculateSMA(candles, 8);

  const sma20 =
    calculateSMA(candles, 20);

  const sma200 =
    calculateSMA(candles, 200);

  const previousSMA8 =
    calculatePreviousSMA(
      candles,
      8
    );

  const previousSMA20 =
    calculatePreviousSMA(
      candles,
      20
    );

  // 201 candles are required to determine
  // the direction/slope of the 200 SMA.
  const previousSMA200 =
    candles.length >= 201
      ? calculatePreviousSMA(
          candles,
          200
        )
      : null;

  const directionOf =
    (
      currentValue,
      previousValue
    ) => {
      if (
        !Number.isFinite(
          currentValue
        ) ||
        !Number.isFinite(
          previousValue
        )
      ) {
        return "UNAVAILABLE";
      }

      if (
        currentValue >
        previousValue
      ) {
        return "RISING";
      }

      if (
        currentValue <
        previousValue
      ) {
        return "FALLING";
      }

      return "FLAT";
    };

  const sma8Direction =
    directionOf(
      sma8,
      previousSMA8
    );

  const sma20Direction =
    directionOf(
      sma20,
      previousSMA20
    );

  const sma200Direction =
    directionOf(
      sma200,
      previousSMA200
    );

  // ------------------------------------------------
  // SHORT-TERM 8/20 STATE
  // ------------------------------------------------
  let state = "MIXED";

  if (
    price > sma8 &&
    sma8 > sma20 &&
    sma8Direction === "RISING" &&
    sma20Direction === "RISING"
  ) {
    state = "BULLISH";
  }

  if (
    price < sma8 &&
    sma8 < sma20 &&
    sma8Direction === "FALLING" &&
    sma20Direction === "FALLING"
  ) {
    state = "BEARISH";
  }

  // ------------------------------------------------
  // 200 SMA POSITION
  // ------------------------------------------------
  let sma200Context =
    "AT_200";

  if (price > sma200) {
    sma200Context =
      "ABOVE_200";
  } else if (price < sma200) {
    sma200Context =
      "BELOW_200";
  }

  // ------------------------------------------------
  // OLIVER 200 SMA REGIME
  //
  // Above + rising = bullish regime
  // Below + falling = bearish regime
  // Everything else = transition/neutral
  // ------------------------------------------------
  let regime =
    "TRANSITION";

  if (
    price > sma200 &&
    sma200Direction === "RISING"
  ) {
    regime =
      "BULLISH_REGIME";
  }

  if (
    price < sma200 &&
    sma200Direction === "FALLING"
  ) {
    regime =
      "BEARISH_REGIME";
  }

  if (
    sma200Direction ===
      "UNAVAILABLE"
  ) {
    regime =
      "REGIME_BUILDING";
  }

  // ------------------------------------------------
  // 200 ALIGNMENT
  // ------------------------------------------------
  let sma200Alignment =
    "NEUTRAL";

  if (
    state === "BULLISH" &&
    regime === "BULLISH_REGIME"
  ) {
    sma200Alignment =
      "ALIGNED";
  }

  if (
    state === "BEARISH" &&
    regime === "BEARISH_REGIME"
  ) {
    sma200Alignment =
      "ALIGNED";
  }

  if (
    state === "BULLISH" &&
    regime === "BEARISH_REGIME"
  ) {
    sma200Alignment =
      "COUNTER_TREND";
  }

  if (
    state === "BEARISH" &&
    regime === "BULLISH_REGIME"
  ) {
    sma200Alignment =
      "COUNTER_TREND";
  }

  // ------------------------------------------------
  // STANDARD OLIVER CONDITIONS
  // ------------------------------------------------
  const structure =
    detectStructure(candles);

  const bullishTakeover =
    isBullishTakeover(
      previous,
      current
    );

  const bearishTakeover =
    isBearishTakeover(
      previous,
      current
    );

  const expansion =
    detectExpansion(candles);

  // ------------------------------------------------
  // EXPERIMENTAL ENTRY RECOGNITION
  // ------------------------------------------------
  const takeoverNearSMA =
    detectTakeoverNearSMA(
      candles,
      sma8,
      sma20
    );

  const compressionExpansion =
    detectCompressionExpansion(
      candles
    );

  const reversalBreak =
    detectReversalBreak(
      candles
    );

  // ------------------------------------------------
  // LOCATION
  // ------------------------------------------------
  const distanceFrom8 =
    Math.abs(
      price - sma8
    );

  const distanceFrom20 =
    Math.abs(
      price - sma20
    );

  const distanceFrom200 =
    Math.abs(
      price - sma200
    );

  let nearestSMA =
    "8_SMA";

  let nearestDistance =
    distanceFrom8;

  if (
    distanceFrom20 <
    nearestDistance
  ) {
    nearestSMA =
      "20_SMA";
    nearestDistance =
      distanceFrom20;
  }

  if (
    distanceFrom200 <
    nearestDistance
  ) {
    nearestSMA =
      "200_SMA";
  }

  // ------------------------------------------------
  // EVIDENCE CHECKS
  // ------------------------------------------------
  let bullishChecks = 0;
  let bearishChecks = 0;

  if (state === "BULLISH") {
    bullishChecks++;
  }

  if (state === "BEARISH") {
    bearishChecks++;
  }

  if (structure === "HH_HL") {
    bullishChecks++;
  }

  if (structure === "LH_LL") {
    bearishChecks++;
  }

  if (bullishTakeover) {
    bullishChecks++;
  }

  if (bearishTakeover) {
    bearishChecks++;
  }

  if (expansion === "GREEN") {
    bullishChecks++;
  }

  if (expansion === "RED") {
    bearishChecks++;
  }

  // 200 regime is fundamental,
  // not merely price above/below the line.
  if (
    regime === "BULLISH_REGIME"
  ) {
    bullishChecks++;
  }

  if (
    regime === "BEARISH_REGIME"
  ) {
    bearishChecks++;
  }

  // Experimental early-entry patterns.
  if (
    takeoverNearSMA?.direction ===
      "BULLISH"
  ) {
    bullishChecks += 2;
  }

  if (
    takeoverNearSMA?.direction ===
      "BEARISH"
  ) {
    bearishChecks += 2;
  }

  if (
    compressionExpansion ===
      "BULLISH"
  ) {
    bullishChecks += 2;
  }

  if (
    compressionExpansion ===
      "BEARISH"
  ) {
    bearishChecks += 2;
  }

  if (
    reversalBreak?.direction ===
      "BULLISH"
  ) {
    bullishChecks += 2;
  }

  if (
    reversalBreak?.direction ===
      "BEARISH"
  ) {
    bearishChecks += 2;
  }

  // ------------------------------------------------
  // ENTRY EVENT
  // ------------------------------------------------
  let entryEvent =
    "NONE";

  if (takeoverNearSMA) {
    entryEvent =
      `${takeoverNearSMA.direction}_TAKEOVER_NEAR_${takeoverNearSMA.near}`;
  }

  if (compressionExpansion) {
    entryEvent =
      `${compressionExpansion}_COMPRESSION_EXPANSION`;
  }

  if (reversalBreak) {
    entryEvent =
      `${reversalBreak.direction}_REVERSAL_BREAK`;
  }

  const bullishEntryEvent =
    takeoverNearSMA?.direction ===
      "BULLISH" ||
    compressionExpansion ===
      "BULLISH" ||
    reversalBreak?.direction ===
      "BULLISH";

  const bearishEntryEvent =
    takeoverNearSMA?.direction ===
      "BEARISH" ||
    compressionExpansion ===
      "BEARISH" ||
    reversalBreak?.direction ===
      "BEARISH";

  // ------------------------------------------------
  // REGIME AUTHORIZATION
  // ------------------------------------------------
  const bullishAuthorized =
    regime === "BULLISH_REGIME";

  const bearishAuthorized =
    regime === "BEARISH_REGIME";

  // ------------------------------------------------
  // ACTION
  // ------------------------------------------------
  let action = "WAIT";

  let reason =
    "No confirmed Oliver entry event.";

  if (
    bullishEntryEvent &&
    bullishChecks >= 3 &&
    bullishChecks >
      bearishChecks
  ) {
    if (bullishAuthorized) {
      action =
        "CALL_SETUP";

      reason =
        "Bullish Oliver event confirmed inside an authorized bullish 200 SMA regime.";
    } else {
      action =
        "ARMED";

      reason =
        "Bullish event detected, but the 200 SMA regime does not yet authorize the CALL setup.";
    }
  }

  if (
    bearishEntryEvent &&
    bearishChecks >= 3 &&
    bearishChecks >
      bullishChecks
  ) {
    if (bearishAuthorized) {
      action =
        "PUT_SETUP";

      reason =
        "Bearish Oliver event confirmed inside an authorized bearish 200 SMA regime.";
    } else {
      action =
        "ARMED";

      reason =
        "Bearish event detected, but the 200 SMA regime does not yet authorize the PUT setup.";
    };
  }

  // ------------------------------------------------
  // TRIGGER / INVALIDATION
  // ------------------------------------------------
  let trigger = null;
  let invalidation = null;

  if (
    action === "CALL_SETUP"
  ) {
    trigger =
      Number(current.high);

    invalidation =
      Number(current.low);
  }

  if (
    action === "PUT_SETUP"
  ) {
    trigger =
      Number(current.low);

    invalidation =
      Number(current.high);
  }

  // ------------------------------------------------
  // SETUP QUALITY
  // ------------------------------------------------
  let setupQuality = "C";

  if (
    action === "CALL_SETUP" ||
    action === "PUT_SETUP"
  ) {
    setupQuality =
      "A";
  } else if (
    action === "ARMED"
  ) {
    setupQuality =
      "B";
  }

  // ------------------------------------------------
  // RESULT
  // ------------------------------------------------
  return {
    action,
    reason,

    setupQuality,

    state,
    regime,

    bullishAuthorized,
    bearishAuthorized,

    price:
      Number(
        price.toFixed(4)
      ),

    sma8:
      Number(
        sma8.toFixed(4)
      ),

    sma20:
      Number(
        sma20.toFixed(4)
      ),

    sma200:
      Number(
        sma200.toFixed(4)
      ),

    sma200Status:
      candles.length >= 201
        ? "LIVE_WITH_SLOPE"
        : "LIVE_WAITING_FOR_SLOPE",

    sma8Direction,
    sma20Direction,
    sma200Direction,

    sma200Context,
    sma200Alignment,

    structure,
    nearestSMA,

    bullishTakeover,
    bearishTakeover,
    expansion,

    takeoverNearSMA,
    compressionExpansion,
    reversalBreak,

    entryEvent,

    bullishChecks,
    bearishChecks,

    trigger,
    invalidation,

    analyzedCandle:
      current.time
  };
}

// ==================================================
// BVB V2 — TUG-OF-WAR TREND ENGINE
// ==================================================
//
// Purpose:
// Detect:
// 1. Who controls the trend — Bulls / Bears / Neutral
// 2. How strong that control is
// 3. Whether a trend is early, active, weakening, or extended
// 4. Heikin-Ashi trend behavior
// 5. Potential direction-change / exhaustion warnings
//
// IMPORTANT:
// - Uses completed candles only.
// - HA is used for trend interpretation.
// - Real OHLC candles remain the source for executable
//   entry / invalidation prices.
// - Oliver V1.1 remains intact.
// ==================================================


// ==================================================
// MARKET SESSION AWARENESS
// ==================================================
//
// Uses America/New_York because U.S. equity
// regular market hours are 9:30 AM - 4:00 PM ET.
//
// This is display/signal context.
// It does NOT change the underlying candle data.
// ==================================================

function getMarketSession(dateInput = new Date()) {

  const date =
    dateInput instanceof Date
      ? dateInput
      : new Date(dateInput);

  if (
    Number.isNaN(
      date.getTime()
    )
  ) {

    return {
      session: "UNKNOWN",
      regularHours: false
    };

  }


  const parts =
    new Intl.DateTimeFormat(
      "en-US",
      {
        timeZone:
          "America/New_York",

        weekday:
          "short",

        hour:
          "2-digit",

        minute:
          "2-digit",

        hour12:
          false
      }
    )
      .formatToParts(date);


  const getPart =
    (type) =>
      parts.find(
        (part) =>
          part.type === type
      )?.value;


  const weekday =
    getPart("weekday");

  const hour =
    Number(
      getPart("hour")
    );

  const minute =
    Number(
      getPart("minute")
    );


  const minutes =
    hour * 60 +
    minute;


  const isWeekend =
    weekday === "Sat" ||
    weekday === "Sun";


  if (isWeekend) {

    return {
      session: "CLOSED",
      regularHours: false
    };

  }


  // Premarket:
  // 4:00 AM - 9:29 AM ET

  if (
    minutes >= 240 &&
    minutes < 570
  ) {

    return {
      session: "PREMARKET",
      regularHours: false
    };

  }


  // Regular:
  // 9:30 AM - 3:59 PM ET

  if (
    minutes >= 570 &&
    minutes < 960
  ) {

    return {
      session: "REGULAR",
      regularHours: true
    };

  }


  // After hours:
  // 4:00 PM - 8:00 PM ET

  if (
    minutes >= 960 &&
    minutes < 1200
  ) {

    return {
      session: "AFTER_HOURS",
      regularHours: false
    };

  }


  return {
    session: "CLOSED",
    regularHours: false
  };

}

// --------------------------------------------------
// BUILD HEIKIN-ASHI FROM COMPLETED REAL CANDLES
// --------------------------------------------------

function buildHeikinAshi(candles) {

  if (
    !Array.isArray(candles) ||
    candles.length === 0
  ) {
    return [];
  }

  let previousHAOpen = null;
  let previousHAClose = null;

  return candles.map(
    (candle, index) => {

      const open =
        Number(candle.open);

      const high =
        Number(candle.high);

      const low =
        Number(candle.low);

      const close =
        Number(candle.close);


      const haClose =
        (
          open +
          high +
          low +
          close
        ) / 4;


      let haOpen;


      if (index === 0) {

        haOpen =
          (
            open +
            close
          ) / 2;

      } else {

        haOpen =
          (
            previousHAOpen +
            previousHAClose
          ) / 2;

      }


      const haHigh =
        Math.max(
          high,
          haOpen,
          haClose
        );


      const haLow =
        Math.min(
          low,
          haOpen,
          haClose
        );


      const body =
        Math.abs(
          haClose -
          haOpen
        );


      const range =
        Math.max(
          haHigh -
          haLow,
          0
        );


      const upperWick =
        Math.max(
          haHigh -
          Math.max(
            haOpen,
            haClose
          ),
          0
        );


      const lowerWick =
        Math.max(
          Math.min(
            haOpen,
            haClose
          ) -
          haLow,
          0
        );


      const color =
        haClose > haOpen
          ? "GREEN"
          : haClose < haOpen
          ? "RED"
          : "DOJI";


      // A relative doji test is better than requiring
      // the open and close to be exactly equal.

      const isDoji =
        range > 0 &&
        body / range <= 0.25;


      // "No wick" needs a tolerance because market
      // prices rarely produce perfect mathematical zero.

      const wickTolerance =
        Math.max(
          range * 0.08,
          0.01
        );


      const noLowerWick =
        lowerWick <=
        wickTolerance;


      const noUpperWick =
        upperWick <=
        wickTolerance;


      previousHAOpen =
        haOpen;

      previousHAClose =
        haClose;


      return {

        time:
          candle.time,

        open:
          haOpen,

        high:
          haHigh,

        low:
          haLow,

        close:
          haClose,

        color,

        body,

        range,

        upperWick,

        lowerWick,

        isDoji,

        noLowerWick,

        noUpperWick

      };

    }
  );

}


// --------------------------------------------------
// CURRENT HA RUN
// --------------------------------------------------

function getHARun(haCandles) {
  if (!Array.isArray(haCandles) || haCandles.length === 0) {
    return {
      color: "NONE",
      count: 0,
      currentColor: "NONE",
      doji: false
    };
  }

  const last = haCandles[haCandles.length - 1];

  // If current candle is directional, count the current run normally.
  if (last.color === "GREEN" || last.color === "RED") {
    let count = 0;

    for (let i = haCandles.length - 1; i >= 0; i--) {
      if (haCandles[i].color !== last.color) {
        break;
      }

      count++;
    }

    return {
      color: last.color,
      count,
      currentColor: last.color,
      doji: false
    };
  }

  // Current candle is a DOJI.
  // Preserve the directional run immediately preceding it.
  let i = haCandles.length - 2;

  while (i >= 0 && haCandles[i].color === "DOJI") {
    i--;
  }

  if (i < 0) {
    return {
      color: "NONE",
      count: 0,
      currentColor: "DOJI",
      doji: true
    };
  }

  const previousColor = haCandles[i].color;

  if (previousColor !== "GREEN" && previousColor !== "RED") {
    return {
      color: "NONE",
      count: 0,
      currentColor: "DOJI",
      doji: true
    };
  }

  let count = 0;

  while (i >= 0 && haCandles[i].color === previousColor) {
    count++;
    i--;
  }

  return {
    color: previousColor,
    count,
    currentColor: "DOJI",
    doji: true
  };
}


// --------------------------------------------------
// HA BODY TREND
// --------------------------------------------------

function getHABodyMomentum(
  haCandles
) {

  if (
    !Array.isArray(haCandles) ||
    haCandles.length < 3
  ) {
    return "UNKNOWN";
  }


  const a =
    haCandles[
      haCandles.length - 3
    ].body;


  const b =
    haCandles[
      haCandles.length - 2
    ].body;


  const c =
    haCandles[
      haCandles.length - 1
    ].body;


  if (
    c > b &&
    b >= a
  ) {
    return "EXPANDING";
  }


  if (
    c < b &&
    b <= a
  ) {
    return "SHRINKING";
  }


  return "MIXED";
}


// --------------------------------------------------
// DAILY BIAS — HIGHER-TIMEFRAME CONTEXT ONLY
// --------------------------------------------------

function analyzeDailyBias(candles) {
  if (!Array.isArray(candles) || candles.length < 5) {
    return {
      bias: "BUILDING", arrow: "…", confirmed: false,
      confirmation: "WAITING_FOR_5_DAILY_BARS",
      reason: `Need ${Math.max(0, 5 - (candles?.length || 0))} more completed daily candles.`,
      completedCandles: Array.isArray(candles) ? candles.length : 0
    };
  }

  // 5-DAY / 1-WEEK BIAS: context only. Exactly the five most recent
  // completed 1-day candles determine the working higher-timeframe view.
  const week = candles.slice(-5);
  const first = week[0];
  const current = week[week.length - 1];
  const firstClose = Number(first.close);
  const price = Number(current.close);
  const net = price - firstClose;
  const netPct = firstClose ? (net / firstClose) * 100 : 0;
  let upDays = 0, downDays = 0;
  for (let i = 1; i < week.length; i++) {
    const d = Number(week[i].close) - Number(week[i - 1].close);
    if (d > 0) upDays++;
    else if (d < 0) downDays++;
  }
  const ha = buildHeikinAshi(week);
  const currentHA = ha[ha.length - 1];
  const haRun = getHARun(ha);
  const structure = detectStructure(week);

  let bias = "TRANSITION", arrow = "↔";
  if (net > 0 && upDays >= downDays) { bias = "BULLISH"; arrow = "↑"; }
  else if (net < 0 && downDays >= upDays) { bias = "BEARISH"; arrow = "↓"; }

  const bullishHA = currentHA?.color === "GREEN" && !currentHA?.isDoji;
  const bearishHA = currentHA?.color === "RED" && !currentHA?.isDoji;
  const confirmed = (bias === "BULLISH" && bullishHA) || (bias === "BEARISH" && bearishHA);
  const confirmation = confirmed ? "HA_CONFIRMED" : bias === "TRANSITION" ? "MIXED" : "HA_NOT_CONFIRMED";
  const evidence = [
    `5-day net ${net >= 0 ? "+" : ""}${net.toFixed(2)} (${netPct >= 0 ? "+" : ""}${netPct.toFixed(2)}%)`,
    `${upDays} up day(s) / ${downDays} down day(s)`,
    `Latest daily HA ${currentHA?.color || "UNKNOWN"}`
  ];
  if (structure === "HH_HL") evidence.push("5-day HH/HL structure");
  if (structure === "LH_LL") evidence.push("5-day LH/LL structure");

  return {
    bias, arrow, confirmed, confirmation,
    reason: bias === "TRANSITION"
      ? "Five-day daily-candle view is mixed/transitioning; context only."
      : `${bias} five-day daily-candle context${confirmed ? "; latest daily Heikin-Ashi confirms." : "; latest daily Heikin-Ashi has not confirmed."}`,
    price: Number(price.toFixed(4)), firstClose: Number(firstClose.toFixed(4)),
    net: Number(net.toFixed(4)), netPct: Number(netPct.toFixed(3)),
    structure, haColor: currentHA?.color || "UNKNOWN", haDoji: Boolean(currentHA?.isDoji),
    haRunColor: haRun.color, haRunCandles: haRun.count, evidence,
    analyzedCandle: current.time, completedCandles: week.length
  };
}

function buildMarketReadV2(battle, dailyBias) {
  const bias = dailyBias?.bias || "BUILDING";
  const control = battle?.control || "NEUTRAL";
  const phase = battle?.phase || "WAIT";
  const action = battle?.action || "WAIT";
  const changeWatch = battle?.changeWatch || "OFF";

  let alignment = "NEUTRAL / TRANSITION";

  if (bias === "BULLISH" && control === "BULLS") alignment = "WITH 5-DAY BIAS";
  if (bias === "BEARISH" && control === "BEARS") alignment = "WITH 5-DAY BIAS";
  if (bias === "BULLISH" && control === "BEARS") alignment = "COUNTER 5-DAY BIAS";
  if (bias === "BEARISH" && control === "BULLS") alignment = "COUNTER 5-DAY BIAS";

  let headline = "WAIT — battle is not directional enough yet.";

  if (changeWatch === "WARNING") {
    headline = `${control} control may be changing. Review your position and wait for confirmation.`;
  } else if (changeWatch === "WATCH") {
    headline = `${control} still control the 2-minute trend, but the move is weakening.`;
  } else if (action === "CALL_ENTRY_READY") {
    headline = alignment === "WITH 5-DAY BIAS"
      ? "Potential CALL trend entry. The short-term move agrees with the 5-day context. Check the trigger and invalidation."
      : "Potential CALL trend entry. The 5-day context disagrees or is unconfirmed. Check the trigger and invalidation.";
  } else if (action === "PUT_ENTRY_READY") {
    headline = alignment === "WITH 5-DAY BIAS"
      ? "Potential PUT trend entry. The short-term move agrees with the 5-day context. Check the trigger and invalidation."
      : "Potential PUT trend entry. The 5-day context disagrees or is unconfirmed. Check the trigger and invalidation.";
  } else if (control === "BULLS") {
    headline = alignment === "WITH 5-DAY BIAS"
      ? `Bulls control the 2-minute trend and are moving with the ${bias.toLowerCase()} 5-day context.`
      : alignment === "COUNTER 5-DAY BIAS"
      ? `Bulls control the 2-minute trend, but the move is against the ${bias.toLowerCase()} 5-day context.`
      : "Bulls control the 2-minute trend while the 5-day context is still developing.";
  } else if (control === "BEARS") {
    headline = alignment === "WITH 5-DAY BIAS"
      ? `Bears control the 2-minute trend and are moving with the ${bias.toLowerCase()} 5-day context.`
      : alignment === "COUNTER 5-DAY BIAS"
      ? `Bears control the 2-minute trend, but the move is against the ${bias.toLowerCase()} 5-day context.`
      : "Bears control the 2-minute trend while the 5-day context is still developing.";
  }

  return {
    headline,
    alignment,
    phase,
    weekConfirmation: dailyBias?.confirmation || "WAITING",
    note: "Decision sequence: setup → trigger → trend health → deterioration → invalidation. Pressure alone is not an entry."
  };
}

// Translate engine states into an unambiguous dashboard action hierarchy.
// This changes display text only; the analyzer's action codes remain intact.
function getDashboardSignal(battle, regularHours) {
  const action = battle?.action || "WAIT";
  const crossed = battle?.entryMarker?.crossed || "NONE";
  const trigger = Number(battle?.entryPrice);
  const invalidation = Number(battle?.invalidation);
  const priceText = Number.isFinite(trigger) && trigger > 0
    ? `Trigger $${trigger.toFixed(2)}` : "Check the entry trigger";
  const riskText = Number.isFinite(invalidation) && invalidation > 0
    ? ` · invalidation $${invalidation.toFixed(2)}` : "";

  if (!regularHours) return {
    title: "MARKET CLOSED · NO LIVE ACTION",
    detail: "Last completed-candle analysis is informational."
  };
  if (battle?.changeWatch === "WARNING") return {
    title: "REVERSAL WARNING · WAIT",
    detail: "Existing trend is at risk; wait for a fresh confirmed setup."
  };
  if (battle?.changeWatch === "WATCH") return {
    title: "TREND WEAKENING · WATCH",
    detail: "Exhaustion is developing; wait for a fresh confirmed setup."
  };
  if (action === "CALL_ENTRY_READY" || action === "PUT_ENTRY_READY") return {
    title: `POTENTIAL ${action.startsWith("CALL") ? "CALL" : "PUT"} TREND ENTRY`,
    detail: `${priceText}${riskText} · verify trigger before acting.`
  };
  if (battle?.control === "BULLS" || battle?.control === "BEARS") return {
    title: `${battle.control} CONTROL · NO NEW SETUP`,
    detail: "Trend observation only. The app does not know whether you hold a position."
  };
  return { title: "WAIT · NO SETUP", detail: "Monitoring completed 2-minute candles." };
}

// --------------------------------------------------
// TREND BATTLE ANALYSIS
// --------------------------------------------------

function analyzeTrendBattle(
  candles
) {

  if (
    !Array.isArray(candles) ||
    candles.length < 21
  ) {

    return {

      control:
        "NEUTRAL",

      pressure:
        "WAITING",

      ropePosition:
        0,

      phase:
        "WAIT",

      action:
        "WAIT",

      reason:
        "Need at least 21 completed candles."

    };

  }


  // Oliver remains one of the underlying engines.

  const oliver =
    analyzeOliver(candles);


  const current =
    candles[
      candles.length - 1
    ];


  const price =
    Number(
      current.close
    );
const marketSession =
  getMarketSession(new Date());

  const haCandles =
    buildHeikinAshi(
      candles
    );


  const currentHA =
    haCandles[
      haCandles.length - 1
    ];


  const previousHA =
    haCandles.length >= 2
      ? haCandles[
          haCandles.length - 2
        ]
      : null;


  const haRun =
    getHARun(
      haCandles
    );


  const haBodyMomentum =
    getHABodyMomentum(
      haCandles
    );


  // ------------------------------------------------
  // PRESSURE SCORE
  //
  // Negative = Bears
  // Positive = Bulls
  //
  // This is NOT probability.
  // It drives the tug-of-war visualization.
  // ------------------------------------------------

  let score = 0;

  const bullReasons = [];
  const bearReasons = [];


  // ------------------------------------------------
  // OLIVER STATE
  // ------------------------------------------------

  if (
    oliver.state ===
    "BULLISH"
  ) {

    score += 18;

    bullReasons.push(
      "Oliver bullish state"
    );
  }


  if (
    oliver.state ===
    "BEARISH"
  ) {

    score -= 18;

    bearReasons.push(
      "Oliver bearish state"
    );
  }


  // ------------------------------------------------
  // STRUCTURE
  // ------------------------------------------------

  if (
    oliver.structure ===
    "HH_HL"
  ) {

    score += 14;

    bullReasons.push(
      "HH/HL structure"
    );
  }


  if (
    oliver.structure ===
    "LH_LL"
  ) {

    score -= 14;

    bearReasons.push(
      "LH/LL structure"
    );
  }


  // ------------------------------------------------
  // TAKEOVER
  // ------------------------------------------------

  if (
    oliver.bullishTakeover
  ) {

    score += 12;

    bullReasons.push(
      "Bullish takeover"
    );
  }


  if (
    oliver.bearishTakeover
  ) {

    score -= 12;

    bearReasons.push(
      "Bearish takeover"
    );
  }


  // ------------------------------------------------
  // OLIVER ENTRY EVENTS
  // ------------------------------------------------

  if (
    typeof oliver.entryEvent ===
      "string" &&
    oliver.entryEvent.startsWith(
      "BULLISH"
    )
  ) {

    score += 16;

    bullReasons.push(
      "Bullish Oliver entry event"
    );
  }


  if (
    typeof oliver.entryEvent ===
      "string" &&
    oliver.entryEvent.startsWith(
      "BEARISH"
    )
  ) {

    score -= 16;

    bearReasons.push(
      "Bearish Oliver entry event"
    );
  }


  // ------------------------------------------------
  // HEIKIN-ASHI CONTROL
  // ------------------------------------------------

  if (
    currentHA.color ===
    "GREEN"
  ) {

    score += 10;

    bullReasons.push(
      "Green HA control"
    );
  }


  if (
    currentHA.color ===
    "RED"
  ) {

    score -= 10;

    bearReasons.push(
      "Red HA control"
    );
  }


  // ------------------------------------------------
  // CONSECUTIVE HA RUN
  // ------------------------------------------------

  if (
    haRun.color ===
    "GREEN"
  ) {

    const runBonus =
      Math.min(
        haRun.count * 4,
        20
      );


    score +=
      runBonus;


    if (
      haRun.count >= 2
    ) {

      bullReasons.push(
        `${haRun.count} green HA candles`
      );

    }

  }


  if (
    haRun.color ===
    "RED"
  ) {

    const runBonus =
      Math.min(
        haRun.count * 4,
        20
      );


    score -=
      runBonus;


    if (
      haRun.count >= 2
    ) {

      bearReasons.push(
        `${haRun.count} red HA candles`
      );

    }

  }


  // ------------------------------------------------
  // NO OPPOSITE WICK = CONVICTION
  // ------------------------------------------------

  if (
    currentHA.color ===
      "GREEN" &&
    currentHA.noLowerWick
  ) {

    score += 10;

    bullReasons.push(
      "Green HA has no lower wick"
    );
  }


  if (
    currentHA.color ===
      "RED" &&
    currentHA.noUpperWick
  ) {

    score -= 10;

    bearReasons.push(
      "Red HA has no upper wick"
    );
  }


  // ------------------------------------------------
  // EXPANSION
  // ------------------------------------------------

  if (
    oliver.expansion ===
    "GREEN"
  ) {

    score += 8;

    bullReasons.push(
      "Bullish expansion"
    );
  }


  if (
    oliver.expansion ===
    "RED"
  ) {

    score -= 8;

    bearReasons.push(
      "Bearish expansion"
    );
  }


  // ------------------------------------------------
  // 200 SMA CONTEXT
  //
  // Deliberately lighter weighting.
  // We want context without allowing the 200 SMA
  // to automatically veto an intraday trend.
  // ------------------------------------------------

  if (
    oliver.sma200Context ===
    "ABOVE_200"
  ) {

    score += 6;

    bullReasons.push(
      "Price above 200 SMA"
    );
  }


  if (
    oliver.sma200Context ===
    "BELOW_200"
  ) {

    score -= 6;

    bearReasons.push(
      "Price below 200 SMA"
    );
  }


  // ------------------------------------------------
  // CAP SCORE
  // ------------------------------------------------

  score =
    Math.max(
      -100,
      Math.min(
        100,
        score
      )
    );


  // ------------------------------------------------
  // CONTROL
  // ------------------------------------------------

  let control =
    "NEUTRAL";


  if (
    score >= 15
  ) {
    control =
      "BULLS";
  }


  if (
    score <= -15
  ) {
    control =
      "BEARS";
  }


  // ------------------------------------------------
  // PRESSURE LABEL
  // ------------------------------------------------

  const absoluteScore =
    Math.abs(score);


  let pressure =
    "BALANCED";


  if (
    absoluteScore >= 75
  ) {

    pressure =
      "DOMINANT";

  } else if (
    absoluteScore >= 55
  ) {

    pressure =
      "STRONG";

  } else if (
    absoluteScore >= 35
  ) {

    pressure =
      "CONTROL";

  } else if (
    absoluteScore >= 15
  ) {

    pressure =
    "EARLY";

  }


  // ------------------------------------------------
  // HA CONTROL
  // ------------------------------------------------

  let haControl =
    "INDECISION";


  if (
    currentHA.color ===
      "GREEN"
  ) {

    haControl =
      currentHA.noLowerWick
        ? "BUYERS_STRONG"
        : "BUYERS";

  }


  if (
    currentHA.color ===
      "RED"
  ) {

    haControl =
      currentHA.noUpperWick
        ? "SELLERS_STRONG"
        : "SELLERS";

  }


  if (
    currentHA.isDoji
  ) {

    haControl =
      "INDECISION";

  }


  // ------------------------------------------------
  // EXHAUSTION EVIDENCE
  // ------------------------------------------------

  let bullExhaustion = 0;
  let bearExhaustion = 0;

  const exhaustionReasons = [];


  // Bulls currently control:
  // look for evidence that bullish control is fading.

  if (
    control === "BULLS"
  ) {

    if (
      currentHA.isDoji
    ) {

      bullExhaustion++;

      exhaustionReasons.push(
        "HA indecision"
      );

    }


    if (
      currentHA.color ===
        "GREEN" &&
      !currentHA.noLowerWick
    ) {

      bullExhaustion++;

      exhaustionReasons.push(
        "Lower HA wick developing"
      );

    }


    if (
      haBodyMomentum ===
        "SHRINKING"
    ) {

      bullExhaustion++;

      exhaustionReasons.push(
        "HA bodies shrinking"
      );

    }


    if (
      previousHA &&
      previousHA.color ===
        "GREEN" &&
      currentHA.color ===
        "RED"
    ) {

      bullExhaustion += 2;

      exhaustionReasons.push(
        "HA changed red"
      );

    }


    if (
      Number.isFinite(
        Number(oliver.sma8)
      ) &&
      price <
        Number(oliver.sma8)
    ) {

      bullExhaustion++;

      exhaustionReasons.push(
        "Price below 8 SMA"
      );

    }


    if (
      oliver.bearishTakeover
    ) {

      bullExhaustion += 2;

      exhaustionReasons.push(
        "Bearish takeover"
      );

    }

  }


  // Bears currently control:
  // look for evidence that bearish control is fading.

  if (
    control === "BEARS"
  ) {

    if (
      currentHA.isDoji
    ) {

      bearExhaustion++;

      exhaustionReasons.push(
        "HA indecision"
      );

    }


    if (
      currentHA.color ===
        "RED" &&
      !currentHA.noUpperWick
    ) {

      bearExhaustion++;

      exhaustionReasons.push(
        "Upper HA wick developing"
      );

    }


    if (
      haBodyMomentum ===
        "SHRINKING"
    ) {

      bearExhaustion++;

      exhaustionReasons.push(
        "HA bodies shrinking"
      );

    }


    if (
      previousHA &&
      previousHA.color ===
        "RED" &&
      currentHA.color ===
        "GREEN"
    ) {

      bearExhaustion += 2;

      exhaustionReasons.push(
        "HA changed green"
      );

    }


    if (
      Number.isFinite(
        Number(oliver.sma8)
      ) &&
      price >
        Number(oliver.sma8)
    ) {

      bearExhaustion++;

      exhaustionReasons.push(
        "Price above 8 SMA"
      );

    }


    if (
      oliver.bullishTakeover
    ) {

      bearExhaustion += 2;

      exhaustionReasons.push(
        "Bullish takeover"
      );

    }

  }


  const exhaustionScore =
    control === "BULLS"
      ? bullExhaustion
      : control === "BEARS"
      ? bearExhaustion
      : 0;


  // ------------------------------------------------
  // CHANGE WATCH
  // ------------------------------------------------

  let changeWatch =
    "OFF";


  if (
    exhaustionScore >= 2
  ) {

    changeWatch =
      "WATCH";

  }


  if (
    exhaustionScore >= 4
  ) {

    changeWatch =
      "WARNING";

  }


  // ------------------------------------------------
  // TREND PHASE
  // ------------------------------------------------

  let phase =
    "NEUTRAL";


  if (
    control !== "NEUTRAL" &&
    absoluteScore >= 15 &&
    absoluteScore < 35
  ) {

    phase =
      "EARLY";

  }


  if (
    control !== "NEUTRAL" &&
    absoluteScore >= 35 &&
    absoluteScore < 55
  ) {

    phase =
      "DEVELOPING";

  }


  if (
    control !== "NEUTRAL" &&
    absoluteScore >= 55
  ) {

    phase =
      "ACTIVE";

  }


  if (
    control !== "NEUTRAL" &&
    haRun.count >= 6 &&
    absoluteScore >= 55
  ) {

    phase =
      "EXTENDED";

  }


  if (
    changeWatch ===
      "WATCH"
  ) {

    phase =
      "WEAKENING";

  }


  if (
    changeWatch ===
      "WARNING"
  ) {

    phase =
      "REVERSAL_WATCH";

  }


  // ------------------------------------------------
  // ENTRY INFORMATION
  //
  // Actual entry prices always come from Oliver's
  // REAL candle trigger, never HA synthetic prices.
  // ------------------------------------------------

  let entryReady = false;

  let entryDirection =
    null;

  let entryPrice =
    null;

  let invalidation =
    null;


  if (
    oliver.action ===
    "CALL_SETUP"
  ) {

    entryReady = true;

    entryDirection =
      "CALL";

    entryPrice =
      oliver.trigger;

    invalidation =
      oliver.invalidation;

  }


  if (
    oliver.action ===
    "PUT_SETUP"
  ) {

    entryReady = true;

    entryDirection =
      "PUT";

    entryPrice =
      oliver.trigger;

    invalidation =
      oliver.invalidation;

  }


  // ------------------------------------------------
  // ACTION
  // ------------------------------------------------

  let action =
    "WAIT";


  if (
    control === "BULLS"
  ) {

    if (
      changeWatch ===
      "WARNING"
    ) {

      action =
        "BULL_TREND_EXIT_WARNING";

    } else if (
      changeWatch ===
      "WATCH"
    ) {

      action =
        "HOLD_BULL_WATCH";

    } else if (
      entryReady &&
      entryDirection ===
        "CALL"
    ) {

      action =
        "CALL_ENTRY_READY";

    } else {

      action =
        "HOLD_BULL_TREND";

    }

  }


  if (
    control === "BEARS"
  ) {

    if (
      changeWatch ===
      "WARNING"
    ) {

      action =
        "BEAR_TREND_EXIT_WARNING";

    } else if (
      changeWatch ===
      "WATCH"
    ) {

      action =
        "HOLD_BEAR_WATCH";

    } else if (
      entryReady &&
      entryDirection ===
        "PUT"
    ) {

      action =
        "PUT_ENTRY_READY";

    } else {

      action =
        "HOLD_BEAR_TREND";

    }

  }


  // ------------------------------------------------
  // 200 SMA DISPLAY CONTEXT
  // ------------------------------------------------

  let sma200Context =
    "UNAVAILABLE";


  if (
    oliver.sma200Context ===
      "ABOVE_200"
  ) {

    sma200Context =
      control === "BULLS"
        ? "WITH_TREND"
        : "COUNTER_TREND";

  }


  if (
    oliver.sma200Context ===
      "BELOW_200"
  ) {

    sma200Context =
      control === "BEARS"
        ? "WITH_TREND"
        : "COUNTER_TREND";

  }


  // ------------------------------------------------
  // RESULT
  // ------------------------------------------------

  return {

    control,

    pressure,

    // -100 = maximum Bear pull
    // 0 = center
    // +100 = maximum Bull pull

    ropePosition:
      score,

    phase,

    action,
    entryMarker: {
  bull: 35,
  bear: -35,
  crossed:
    score >= 35
      ? "BULL_ENTRY"
      : score <= -35
        ? "BEAR_ENTRY"
        : "NONE"
},

    price:
      Number(
        price.toFixed(4)
      ),

    haControl,

    haColor:
      currentHA.color,

    haRunColor:
      haRun.color,

    haRunCandles:
      haRun.count,

    haBodyMomentum,

    haDoji:
      currentHA.isDoji,

    haNoLowerWick:
      currentHA.noLowerWick,

    haNoUpperWick:
      currentHA.noUpperWick,

    changeWatch,

    exhaustionScore,

    exhaustionReasons,

    entryReady,

    entryDirection,

    entryPrice,

    invalidation,

    entryEvent:
      oliver.entryEvent ||
      "NONE",

    sma8:
      oliver.sma8,

    sma20:
      oliver.sma20,

    sma200:
      oliver.sma200,

    sma200Context,

    structure:
      oliver.structure,

    bullEvidence:
      bullReasons,

    bearEvidence:
      bearReasons,

    marketSession:
  getMarketSession(new Date()),

analyzedCandle:
  current.time
};

}
// ===============================================
// TREND EVENT RECORDER
// ===============================================

function recordTrendEvent(analysis) {
  if (!analysis) return null;

  const price = Number(analysis.price);
  if (!Number.isFinite(price)) return null;

  // Track only state changes that are useful to an AI/agent.
  // Price itself and HA run length are intentionally excluded from
  // the signature so normal market movement does not create spam.
  const snapshot = {
    control: analysis.control || "NEUTRAL",
    pressure: analysis.pressure || "UNKNOWN",
    phase: analysis.phase || "UNKNOWN",
    action: analysis.action || "WAIT",
    haControl: analysis.haControl || "INDECISION",
    haRunDirection: analysis.haRunColor || "NONE",
    entryReady: Boolean(analysis.entryReady),
    entryDirection: analysis.entryDirection || "NONE",
    changeWatch: analysis.changeWatch || "OFF",
    sma200Context: analysis.sma200Context || "UNAVAILABLE"
  };

  const eventKey = JSON.stringify(snapshot);

  // Ignore repeats where none of the meaningful AI-facing state changed.
  if (eventKey === lastTrendEventKey) {
    return null;
  }

  const previous =
    trendEventHistory.length > 0
      ? trendEventHistory[trendEventHistory.length - 1]
      : null;

  const previousState = previous?.state || null;
  const changed = [];

  if (!previousState) {
    changed.push("INITIAL_STATE");
  } else {
    for (const key of Object.keys(snapshot)) {
      if (previousState[key] !== snapshot[key]) {
        changed.push(key);
      }
    }
  }

  const controlFlip =
    Boolean(previousState) &&
    previousState.control !== snapshot.control &&
    (previousState.control === "BULLS" || previousState.control === "BEARS") &&
    (snapshot.control === "BULLS" || snapshot.control === "BEARS");

  if (controlFlip) {
    changed.push("CONTROL_FLIP");
  }

  const reversalWarning =
    snapshot.changeWatch === "WARNING" ||
    snapshot.phase === "REVERSAL_WATCH" ||
    String(snapshot.action).includes("EXIT_WARNING");

  // Compact packet intended for the future ChatGPT/agent integration.
  const aiPacket = {
    symbol: "GOOGL",
    timeframe: "2Min",
    time: analysis.analyzedCandle || new Date().toISOString(),
    price: Number(price.toFixed(4)),
    changed,
    control: snapshot.control,
    pressure: snapshot.pressure,
    phase: snapshot.phase,
    action: snapshot.action,
    ha: {
      control: snapshot.haControl,
      run: snapshot.haRunDirection,
      candles: analysis.haRunCandles ?? 0,
      doji: Boolean(analysis.haDoji)
    },
    entry: {
      ready: snapshot.entryReady,
      direction: snapshot.entryDirection,
      price: analysis.entryPrice ?? null,
      invalidation: analysis.invalidation ?? null,
      event: analysis.entryEvent || "NONE"
    },
    reversal: {
      watch: snapshot.changeWatch,
      warning: reversalWarning,
      exhaustionScore: analysis.exhaustionScore ?? 0,
      reasons: analysis.exhaustionReasons || []
    },
    sma: {
      sma8: analysis.sma8 ?? null,
      sma20: analysis.sma20 ?? null,
      sma200: analysis.sma200 ?? null,
      sma200Context: snapshot.sma200Context
    },
    structure: analysis.structure || "UNKNOWN",
    controlFlip,
    evidence: {
      bull: analysis.bullEvidence || [],
      bear: analysis.bearEvidence || []
    }
  };

  const event = {
    id: `${aiPacket.time}|${trendEventHistory.length + 1}`,
    ...aiPacket,
    state: snapshot
  };

  trendEventHistory.push(event);

  if (trendEventHistory.length > MAX_TREND_EVENTS) {
    trendEventHistory.shift();
  }

  lastTrendEventKey = eventKey;

  console.log(
    `BVB AI EVENT: ${snapshot.control} | ${snapshot.pressure} | ${snapshot.phase} | ${snapshot.action} | GOOGL $${price}`
  );

  return event;
}


// ==================================================
// BVB AI EVENT FEED
// ==================================================
//
// Returns meaningful completed-candle state changes only.
// Optional query: ?limit=25 (1-100)
// The newest event is also exposed separately for easy polling.
// ==================================================

app.get(
  "/bvb-events",
  authorizeBVBEvents,
  (req, res) => {

    const requestedLimit = Number.parseInt(req.query.limit, 10);

    const limit = Number.isFinite(requestedLimit)
      ? Math.max(1, Math.min(MAX_TREND_EVENTS, requestedLimit))
      : 25;

    const events = trendEventHistory.slice(-limit);

    res.json({
      symbol: "GOOGL",
      timeframe: "2Min",
      streamStatus: alpacaStreamStatus,
      historySeeded,
      eventCount: trendEventHistory.length,
      returned: events.length,
      latestEvent: events.length ? events[events.length - 1] : null,
      events
    });
  }
);


// ==================================================
// START MARKET DATA SYSTEM
// ==================================================

seedHistoricalCandles();
seedDailyCandles();
seedRiderBars(5, "5m");
seedRiderBars(15, "15m");

connectAlpacaStream();


// ==================================================
// DAILY BIAS ENDPOINT
// ==================================================

app.get(
  "/daily-bias",
  (req, res) => {
    const dailyBias = analyzeDailyBias(dailyCandles);
    const battle = analyzeTrendBattle(completedCandles);

    res.json({
      symbol: "GOOGL",
      timeframe: "1Day",
      dailyHistorySeeded,
      completedDailyCandleCount: dailyCandles.length,
      bias: dailyBias,
      intradayAlignment: buildAIRead(battle, dailyBias),
      trendHold
    });
  }
);

// ==================================================
// LIVE GOOGL ENDPOINT
// ==================================================

app.get(
  "/googl-live",
  (req, res) => {

    res.set("Cache-Control", "no-store");
    if (req.query.dashboard === "1") {
      return res.json({ latestTrade: latestGOOGLTrade, developing2MinCandle: developingCandle,
        streamStatus: alpacaStreamStatus, dashboard: buildDashboardView() });
    }

    res.json({

      symbol:
        "GOOGL",

      timeframe:
        "2Min",

      streamStatus:
        alpacaStreamStatus,

      historySeeded:
        historySeeded,

      trendHold,

      completedCandleCount:
        completedCandles.length,

      latestTrade:
        latestGOOGLTrade,

      developing2MinCandle:
        developingCandle,

      completedCandles:
        completedCandles

    });

  }
);


// ==================================================
// COMPLETED CANDLE HISTORY
// ==================================================

app.get(
  "/googl-history",
  (req, res) => {

    res.json({

      symbol:
        "GOOGL",

      timeframe:
        "2Min",

      historySeeded:
        historySeeded,

      count:
        completedCandles.length,

      candles:
        completedCandles

    });

  }
);


// ==================================================
// ROOT STATUS
// ==================================================

app.get(
  "/",
  (req, res) => {

    res.json({

      status:
        "online",

      service:
        "BVB Trading Assistant",

      alpacaConfigured:
        Boolean(
          ALPACA_API_KEY &&
          ALPACA_SECRET_KEY
        ),

      streamStatus:
        alpacaStreamStatus,

      historySeeded:
        historySeeded,

      completedCandleCount:
        completedCandles.length

    });

  }
);


// ==================================================
// HEALTH CHECK
// ==================================================

app.get(
  "/health",
  (req, res) => {

    res.json({

      status:
        "healthy"

    });

  }
);


// ==================================================
// NORMAL 2-MINUTE GOOGL BARS
// ==================================================

app.get(
  "/googl",
  async (req, res) => {

    try {

      if (
        !ALPACA_API_KEY ||
        !ALPACA_SECRET_KEY
      ) {

        return res
          .status(500)
          .json({

            error:
              "Alpaca API credentials are not configured"

          });

      }


      const url =
        "https://data.alpaca.markets/v2/stocks/GOOGL/bars?timeframe=2Min&limit=10&feed=iex";


      const response =
        await fetch(
          url,
          {

            headers: {

              "APCA-API-KEY-ID":
                ALPACA_API_KEY,

              "APCA-API-SECRET-KEY":
                ALPACA_SECRET_KEY

            }

          }
        );


      const text =
        await response.text();


      console.log(
        "Alpaca status:",
        response.status
      );


      if (!response.ok) {

        return res
          .status(
            response.status
          )
          .send(text);

      }


      res
        .type(
          "application/json"
        )
        .send(text);


    } catch (error) {

      console.error(
        "Alpaca error:",
        error
      );


      res
        .status(500)
        .json({

          error:
            "Unable to retrieve GOOGL data"

        });

    }

  }
);


// ==================================================
// HEIKIN-ASHI 2-MINUTE DATA
// ==================================================

app.get(
  "/googl-ha",
  async (req, res) => {

    try {

      if (
        !ALPACA_API_KEY ||
        !ALPACA_SECRET_KEY
      ) {

        return res
          .status(500)
          .json({

            error:
              "Alpaca API credentials are not configured"

          });

      }


      const url =
        "https://data.alpaca.markets/v2/stocks/GOOGL/bars?timeframe=2Min&limit=200&feed=iex";


      const response =
        await fetch(
          url,
          {

            headers: {

              "APCA-API-KEY-ID":
                ALPACA_API_KEY,

              "APCA-API-SECRET-KEY":
                ALPACA_SECRET_KEY

            }

          }
        );


      const data =
        await response.json();


      if (!response.ok) {

        return res
          .status(
            response.status
          )
          .json(data);

      }


      const bars =
        data.bars || [];


      let previousHAOpen =
        null;

      let previousHAClose =
        null;


      const heikinAshi =
        bars.map(
          (bar, index) => {

            const haClose =
              (
                bar.o +
                bar.h +
                bar.l +
                bar.c
              ) / 4;


            let haOpen;


            if (
              index === 0
            ) {

              haOpen =
                (
                  bar.o +
                  bar.c
                ) / 2;

            } else {

              haOpen =
                (
                  previousHAOpen +
                  previousHAClose
                ) / 2;

            }


            const haHigh =
              Math.max(
                bar.h,
                haOpen,
                haClose
              );


            const haLow =
              Math.min(
                bar.l,
                haOpen,
                haClose
              );


            previousHAOpen =
              haOpen;

            previousHAClose =
              haClose;


            return {

              time:
                bar.t,

              open:
                Number(
                  haOpen.toFixed(4)
                ),

              high:
                Number(
                  haHigh.toFixed(4)
                ),

              low:
                Number(
                  haLow.toFixed(4)
                ),

              close:
                Number(
                  haClose.toFixed(4)
                ),

              color:
                haClose >= haOpen
                  ? "GREEN"
                  : "RED"

            };

          }
        );


      res.json({

        symbol:
          "GOOGL",

        timeframe:
          "2Min",

        candles:
          heikinAshi

      });


    } catch (error) {

      console.error(
        "Heikin-Ashi error:",
        error
      );


      res
        .status(500)
        .json({

          error:
            "Unable to calculate Heikin-Ashi data"

        });

    }

  }
);


// ==================================================
// OLIVER LIVE ANALYSIS ENDPOINT
// ==================================================

app.get(
  "/oliver",
  (req, res) => {

    try {

      // Oliver uses COMPLETED candles only.
      // The developing candle is deliberately excluded
      // so an unfinished 2-minute candle cannot create
      // a false confirmed setup.

      const analysis =
        analyzeOliver(
          completedCandles
        );


      res.json({

        symbol:
          "GOOGL",

        timeframe:
          "2Min",

        agent:
          "Oliver",

        engine:
          "Oliver Engine V1",

        streamStatus:
          alpacaStreamStatus,

        historySeeded:
          historySeeded,

        completedCandleCount:
          completedCandles.length,

        latestTrade:
          latestGOOGLTrade,

        analysis:
          analysis

      });


    } catch (error) {

      console.error(
        "Oliver analysis error:",
        error
      );


      res
        .status(500)
        .json({

          error:
            "Unable to run Oliver analysis",

          message:
            error.message

        });

    }

  }
);


// ==================================================
// V3.2.8 display layer. Agent analyzers and paper execution rules are unchanged.
let cDisplayCache = { key: null, value: null };
function agentCDisplayContext(candles) {
  const key = JSON.stringify(candles);
  if (key === cDisplayCache.key) return cDisplayCache.value;
  const ha = buildHeikinAshi(candles);
  const analysis = analyzeAgentCPaper(candles);
  const day = candles.length ? newYorkDateKey(candles.at(-1).time) : null;
  let direction = "WAIT", previousDirection = "WAIT", confirmedAt = null;
  // Replay the unchanged analyzer, not an approximation of its conviction rules.
  // Only current regular-session confirmations establish the control outline.
  for (let i = 3; i < candles.length; i++) {
    if (newYorkDateKey(candles[i].time) !== day || !regularSessionForCandle(candles[i])) continue;
    const signal = analyzeAgentCPaper(candles.slice(0, i + 1)).signal;
    if ((signal === "CALL" || signal === "PUT") && signal !== direction) {
      previousDirection = direction;
      direction = signal;
      confirmedAt = candles[i].time;
    }
  }
  const strength = candleStrength(ha, direction, previousDirection, confirmedAt);
  const value = { direction, previousDirection, confirmedAt, analysis, strength };
  cDisplayCache = { key, value };
  return value;
}

function candleStrength(ha, direction, previousDirection, confirmedAt) {
  const last = ha.at(-1), previous = ha.at(-2);
  const bodies = ha.slice(-9, -1).map(c => c.body).filter(Number.isFinite).sort((a,b) => a-b);
  const median = bodies.length ? (bodies[Math.floor((bodies.length - 1)/2)] + bodies[Math.floor(bodies.length/2)])/2 : 0;
  // Experimental display scale only: 3 = roughly typical; 5 = >=1.75x reference.
  // A one-cent floor and a doji override prevent tiny bodies scoring as strong.
  const ratio = last ? last.body / Math.max(0.01, median) : 0;
  const score = !last || last.isDoji || last.body <= 0.01 || bodies.length < 3 ? 1 :
    ratio >= 1.75 ? 5 : ratio >= 1.25 ? 4 : ratio >= 0.75 ? 3 : ratio >= 0.30 ? 2 : 1;
  const bull = last?.color === "GREEN" ? score : 1;
  const bear = last?.color === "RED" ? score : 1;
  const opposition = direction === "CALL" ? last?.color === "RED" : direction === "PUT" ? last?.color === "GREEN" : false;
  const shrinking = !!last && !!previous && last.body < previous.body * 0.75;
  let status = "Waiting for completed candles";
  if (last) {
    if (last.isDoji || score === 1) status = "Small candle — little conviction";
    else if (opposition) status = (last.color === "GREEN" ? "Buyers" : "Sellers") + " pushing back — reversal not confirmed";
    else if (confirmedAt === last.time && previousDirection !== "WAIT") status = direction === "CALL" ? "Buyers take control" : "Sellers take control";
    else if (direction !== "WAIT" && shrinking) status = direction === "CALL" ? "Buying pressure easing" : "Selling pressure easing";
    else if (direction !== "WAIT") status = direction === "CALL" ? "Buyers in control" : "Sellers in control";
    else status = last.color === "GREEN" ? "Buyers pushing — waiting for confirmation" : "Sellers pushing — waiting for confirmation";
  }
  return { bull, bear, status, opposition, shrinking, color: last?.color || "DOJI", doji: !!last?.isDoji,
    time: last?.time || null, referenceBodies: bodies.length };
}

let lastKnownChopState = "WAIT";
function displayChopRisk(candles) {
  const result = analyzeChopRisk(candles);
  if (["CLEAR", "CAUTION", "CHOP"].includes(result.state)) lastKnownChopState = result.state;
  // Missing candles cannot silently clear a previously visible chop warning.
  return { ...result, state: result.state === "WAIT" ? lastKnownChopState : result.state,
    pending: result.state === "WAIT" };
}

function buildDashboardView(now = Date.now()) {
  const session = getMarketSession(new Date(now));
  const c = agentCDisplayContext(completedCandles);
  const battle = analyzeTrendBattle(completedCandles);
  const daily = analyzeDailyBias(dailyCandles);
  const chop = displayChopRisk(completedCandles);
  const candleTime = completedCandles.at(-1)?.time || null;
  const candleAge = now - Date.parse(candleTime);
  const priceAge = now - Date.parse(latestGOOGLTrade?.time);
  const fresh = alpacaStreamStatus === "connected" && Number(latestGOOGLTrade?.price) > 0 &&
    Number.isFinite(priceAge) && priceAge >= -10000 && priceAge < 30000 &&
    Number.isFinite(candleAge) && candleAge >= 120000 && candleAge < 360000;
  // A missing/invalid candle window cannot clear a warning or enable a pulse.
  const chopActive = chop.state === "CHOP" || chop.state === "CAUTION";
  const warningPending = chop.pending || chop.state === "WAIT";
  const legacyWarning = battle.changeWatch === "WARNING" || trendHold.stage === "REGIME_BROKEN";
  const legacyCaution = battle.changeWatch === "WATCH" || trendHold.stage === "WARNING";
  const weak = c.strength.opposition || c.strength.doji || c.strength.shrinking || legacyWarning || legacyCaution;
  const extended = battle.phase === "EXTENDED";
  const pulseDirection = session.regularHours && fresh && !chopActive && !warningPending && !weak && !extended
    ? c.analysis.signal : "WAIT";
  const direction = c.direction;
  const side = direction === "CALL" ? "Buyers" : direction === "PUT" ? "Sellers" : "Neither side";
  let action = { title:"WAIT — No clear entry", detail:"Waiting for stronger directional candles." };
  if (!session.regularHours) action = { title:"MARKET CLOSED", detail:"Last completed-candle readings are for reference." };
  else if (!fresh) action = { title:"WAIT — Data not current", detail:"Entry cues are paused until fresh prices and candles return." };
  else if (chopActive) action = { title:"WAIT — Choppy conditions", detail:"Avoid new entries until price movement becomes clearer." };
  else if (warningPending) action = { title:"WAIT — Checking conditions", detail:"Building an uninterrupted set of recent candles." };
  else if (legacyWarning) action = { title:"WAIT — Direction may change", detail:"The move is under pressure. Wait for a clearer new entry." };
  else if (weak) action = { title:"WAIT — Pressure is changing", detail:"The move is weakening or facing opposition. Wait for clearer conditions." };
  else if (extended) action = { title:"WAIT — Move is stretched", detail:"A fresh entry may be poorly timed. Wait for a better opportunity." };
  else if (pulseDirection !== "WAIT") action = { title:`POTENTIAL ${pulseDirection} ENTRY`, detail:"Directional candles support this side. A favorable cue is not a guarantee." };
  const supportWarning = legacyWarning ? "Direction-change warning: the move may be breaking down." :
    legacyCaution ? "Weakening warning: the move is losing support." :
    c.strength.opposition ? "The opposing side is pushing back; a reversal is not yet confirmed." :
    c.strength.doji ? "The latest candle shows indecision." :
    c.strength.shrinking ? "The latest candle is smaller; pressure is easing." : "";
  const warning = [
    !session.regularHours ? "Market closed — live entry cues are paused." : !fresh ? "Fresh data unavailable — entry cues are paused." : "",
    chopActive ? "No clear direction — wait for conditions to improve before a new entry." : warningPending ? "Recent candle history is incomplete — wait." : "",
    supportWarning,
    extended ? "The move is stretched; be cautious with a new entry." : "",
    "Check Entry Tracker for your marked trade. Buttons only mark an entry; they do not place orders."
  ].filter(Boolean).join(" ");
  const marketRead = {
    headline: chopActive ? "Price is moving back and forth without clear direction." :
      direction === "WAIT" ? "Neither side has confirmed control." : `${side} have confirmed control. ${c.strength.status}.`,
    context: daily.bias === "BULLISH" ? "5-day view: broader conditions favor buyers." :
      daily.bias === "BEARISH" ? "5-day view: broader conditions favor sellers." : "5-day view: broader conditions are mixed or still developing.",
    note: action.title + ". " + action.detail
  };
  return {
    version:"3.2.8", regularHours:session.regularHours, session:session.session, fresh,
    candleTime, quoteTime:latestGOOGLTrade?.time || null, direction, entrySignal:c.analysis.signal,
    pulseDirection, chopState:chop.state, chopActive, warningPending, strength:c.strength,
    candleControl: c.strength.doji ? "NEUTRAL" : c.strength.color === "GREEN" ? "BULLS" : c.strength.color === "RED" ? "BEARS" : "NEUTRAL",
    action, marketRead, warning,
    hold: { direction:direction === "CALL" ? "BULL" : direction === "PUT" ? "BEAR" : "NONE",
      stage:direction === "WAIT" ? "WAIT" : weak ? "WARNING" : "HOLD",
      headline:direction === "WAIT" ? "WAITING FOR DIRECTION" : `${side.toUpperCase()} CONFIRMED${weak ? " · UNDER PRESSURE" : ""}`,
      reason:chopActive ? "The last confirmed side is highlighted, but current conditions are choppy. Wait before a new entry." :
        supportWarning || "The confirmed direction remains in place until opposing control is confirmed." },
    // Preserve pre-existing risk observations separately from C's control state.
    riskStage:trendHold.stage, changeWatch:battle.changeWatch || "OFF",
    haRunColor:c.analysis.haRunColor || "NONE", haRunCandles:c.analysis.haRunCandles || 0
  };
}

// OLIVER LIVE DASHBOARD
// ==================================================

app.get(
  "/oliver-dashboard",
  (req, res) => {

    const analysis =
      analyzeOliver(
        completedCandles
      );


    const action =
      analysis.action ||
      "WAIT";


    let actionClass =
      "wait";


    let actionText =
      "WAIT";


    if (
      action ===
        "CALL_SETUP"
    ) {

      actionClass =
        "call";

      actionText =
        "CALL SETUP";

    }


    if (
      action ===
        "PUT_SETUP"
    ) {

      actionClass =
        "put";

      actionText =
        "PUT SETUP";

    }


    const arrow =
      (direction) => {

        if (
          direction ===
            "RISING"
        ) {

          return "↑";

        }


        if (
          direction ===
            "FALLING"
        ) {

          return "↓";

        }


        if (
          direction ===
            "FLAT"
        ) {

          return "→";

        }


        return "";

      };


    const money =
      (value) => {

        if (
          value === null ||
          value === undefined ||
          !Number.isFinite(
            Number(value)
          )
        ) {

          return "—";

        }


        return (
          "$" +
          Number(value)
            .toFixed(2)
        );

      };


const battle = analyzeTrendBattle(completedCandles);
const dailyBias = analyzeDailyBias(dailyCandles);
const view = buildDashboardView();
const aiRead = view.marketRead;
const hold = view.hold;
const ropePercent = Math.max(36, Math.min(64, 50 + Number(battle.ropePosition || 0) * 0.28));
const battleControl = view.candleControl;
const battlePressure = battle.pressure || "WAITING";
const dashboardStrength = view.strength.status;
const tugIntensity = battlePressure === "DOMINANT" || battlePressure === "STRONG" ? "tug-confirmed" :
  battlePressure === "CONTROL" ? "tug-building" : battlePressure === "EARLY" ? "tug-early" : "tug-waiting";
const battlePhase = battle.phase || "WAIT";
const battleAction = view.entrySignal === "WAIT" ? "WAIT" : view.entrySignal + "_ENTRY_READY";
const marketSession = view.session;
const regularHours = view.regularHours;
const dashboardSignal = view.action;
const fiveBoxStrength = view.strength;
const strengthBoxes = (side, count) =>
  Array.from({ length: 5 }, (_, i) =>
    `<span class="strengthBox ${side} ${i < count ? "on" : ""}"></span>`
  ).join("");

// V2.3 dashboard display data: side-specific HA scoreboards + live 2-minute clock.
const dashboardHA = buildHeikinAshi(completedCandles);
const dashboardHARun = getHARun(dashboardHA);
const bearHARun = dashboardHARun.color === "RED" ? dashboardHARun.count : 0;
const bullHARun = dashboardHARun.color === "GREEN" ? dashboardHARun.count : 0;
const candleClockData = {
  time: developingCandle?.time || null,
  open: Number(developingCandle?.open),
  close: Number(developingCandle?.close)
};


const html = `
<!DOCTYPE html>
<html>
<head>

<meta
  name="viewport"
  content="width=device-width, initial-scale=1.0"
/>

<title>Tug of War — V3.2.8 Test</title>

<style>

.entryTracker { margin: 18px 0; padding: 16px; border: 1px solid #d7b356;
  border-radius: 12px; background: #1b2230; color: #fff; text-align: center; }
.entryTrackerTitle { font-weight: 800; letter-spacing: .06em; color: #ffdc79; }
#entryTrackerState { font-size: 1.15rem; font-weight: 800; margin: 8px 0; }
#entryTrackerDetail { line-height: 1.4; margin: 8px 0; }
#entryTrackerButton { background: #f4c95d; color: #15191e; font-size: 1rem;
  font-weight: 800; border: 0; border-radius: 8px; padding: 12px 18px;
  min-height: 44px; cursor: pointer; }
#entryTrackerButton:disabled { opacity: .5; cursor: not-allowed; }
.entryTrackerFoot { font-size: .8rem; opacity: .8; margin-top: 8px; }

* {
  box-sizing: border-box;
}

html, body {
  margin: 0;
  width: 100%;
  min-height: 100%;
  background: #080c12;
  color: #ffffff;
  font-family:
    -apple-system,
    BlinkMacSystemFont,
    "Segoe UI",
    sans-serif;
}

body {
  padding: 18px;
}

.dashboard {
  width: 100%;
  max-width: 1100px;
  margin: auto;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 14px;
}

.title {
  font-size: 14px;
  letter-spacing: 2px;
  color: #8995a7;
}

.price {
  font-size: 28px;
  font-weight: 800;
}

.session {
  font-size: 12px;
  color: #9ba7b8;
  text-align: right;
}

/* -------------------------
   BATTLE STATUS
------------------------- */

.status {
  text-align: center;
  margin-bottom: 14px;
}

.control {
  font-size: clamp(26px, 5vw, 48px);
  font-weight: 900;
}

.pressure {
  margin-top: 4px;
  color: #aeb8c6;
  font-size: 15px;
}

/* -------------------------
   TUG OF WAR
------------------------- */

.arena {
  background: #111720;
  border: 1px solid #27303d;
  border-radius: 20px;
  padding: 20px;
}

.teams {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: 900;
  font-size: clamp(18px, 3vw, 28px);
}

.bears {
  color: #ff5b67;
}

.bulls {
  color: #55e69a;
}

.ropeArea {
  position: relative;
  height: clamp(145px, 21vw, 230px);
  margin-top: 5px;
}

.rope {
  position: absolute;
  left: 40%;
  right: 40%;
  top: 50%;
  height: 9px;
  transform: translateY(-50%);
  z-index: 2;
  border-radius: 10px;
  background:
    repeating-linear-gradient(
      45deg,
      #9b7653,
      #9b7653 8px,
      #c69a6b 8px,
      #c69a6b 16px
    );
}

/* CENTER */

.centerLine {
  position: absolute;
  left: 50%;
  top: calc(50% - 32px);
  height: 64px;
  width: 2px;
  background: #7c8796;
  z-index: 7;
}

.centerLabel {
  position: absolute;
  left: 50%;
  top: 0;
  transform: translateX(-50%);
  color: #7f8a99;
  font-size: 11px;
  z-index: 7;
}

/* ENTRY MARKERS */

.bearEntry {
  position: absolute;
  left: 32.5%;
  top: calc(50% - 30px);
  height: 60px;
  width: 2px;
  background: #ff5b67;
  z-index: 7;
}

.bullEntry {
  position: absolute;
  left: 67.5%;
  top: calc(50% - 30px);
  height: 60px;
  width: 2px;
  background: #55e69a;
  z-index: 7;
}

.entryText {
  position: absolute;
  top: calc(50% + 35px);
  transform: translateX(-50%);
  padding: 5px 9px;
  border-radius: 7px;
  background: rgba(10, 15, 23, 0.94);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.45);
  font-size: 11px;
  font-weight: 800;
  letter-spacing: 0.02em;
  white-space: nowrap;
  z-index: 7;
}

.bearText {
  left: 32.5%;
  color: #ff8790;
  border: 1px solid rgba(255, 91, 103, 0.8);
}

.bullText {
  left: 67.5%;
  color: #80ffc0;
  border: 1px solid rgba(85, 230, 154, 0.8);
}

/* KNOT */

.knot {
  position: absolute;
  left: ${ropePercent}%;
  top: calc(50% - 19px);

  width: 38px;
  height: 38px;

  transform: translateX(-50%);

  border-radius: 50%;

  background: #f3c969;
  border: 5px solid #ffffff;

  box-shadow:
    0 0 12px rgba(255,255,255,.35);
  z-index: 8;

  transition:
    left 0.8s ease;
}

/* -------------------------
   ACTION
------------------------- */

.actionBox {
  margin-top: 14px;
  text-align: center;

  padding: 13px;

  border-radius: 12px;

  background: #171e28;
  border: 1px solid #2c3644;
}

.action {
  font-size: clamp(20px, 4vw, 32px);
  font-weight: 900;
}

.phase {
  margin-top: 4px;
  font-size: 13px;
  color: #9ca7b6;
}

/* -------------------------
   INFO CARDS
------------------------- */

.cards {
  display: grid;
  grid-template-columns:
    repeat(4, 1fr);

  gap: 10px;
  margin-top: 12px;
}

.card {
  background: #111720;
  border: 1px solid #27303d;
  border-radius: 12px;
  padding: 11px;
}

.label {
  font-size: 10px;
  letter-spacing: 1px;
  color: #7f8a99;
}

.value {
  margin-top: 4px;
  font-size: 15px;
  font-weight: 800;
}

/* -------------------------
   DAILY BIAS + AI READ
------------------------- */

.dailyBiasLabel {
  font-size: 9px;
  letter-spacing: 1.4px;
  color: #7f8a99;
}

.dailyBiasValue {
  margin-top: 2px;
  font-size: 15px;
  font-weight: 900;
}

.biasBull { color: #55e69a; }
.biasBear { color: #ff5b67; }
.biasNeutral { color: #f3c969; }

.dailyConfirm,
.marketLine {
  margin-top: 2px;
  font-size: 10px;
  color: #9ba7b8;
}

.aiReadBox {
  margin-top: 12px;
  padding: 13px 14px;
  border-radius: 12px;
  background: #111720;
  border: 1px solid #27303d;
}

.aiReadTitle {
  font-size: 10px;
  letter-spacing: 1.4px;
  color: #7f8a99;
}

.aiReadHeadline {
  margin-top: 5px;
  font-size: 15px;
  font-weight: 800;
  line-height: 1.3;
}

.aiReadMeta {
  margin-top: 6px;
  font-size: 11px;
  font-weight: 700;
  color: #aeb8c6;
}

.aiReadNote {
  margin-top: 4px;
  font-size: 10px;
  color: #7f8a99;
}

/* -------------------------
   WARNING
------------------------- */

.warning {
  margin-top: 12px;
  text-align: center;

  padding: 10px;

  border-radius: 10px;

  background: #171e28;

  font-size: 13px;
}

/* -------------------------
   MOBILE / PORTRAIT
------------------------- */

@media (max-width: 700px) {

  body {
    padding: 10px;
  }

  .arena {
    padding: 14px 10px;
  }

  .cards {
    grid-template-columns:
      repeat(2, 1fr);
  }

  .ropeArea {
    height: 145px;
  }
}

/* -------------------------
   LANDSCAPE
------------------------- */

@media (orientation: landscape)
and (max-height: 700px) {

  body {
    padding: 8px 16px;
  }

  .header {
    margin-bottom: 5px;
  }

  .status {
    margin-bottom: 5px;
  }

  .arena {
    padding: 10px 18px;
  }

  .ropeArea {
    height: 145px;
  }

  .actionBox {
    margin-top: 6px;
    padding: 7px;
  }

  .cards {
    margin-top: 6px;
  }

  .warning {
    margin-top: 6px;
    padding: 6px;
  }
}

/* =========================================
   ANIMATED BULL vs BEAR TUG-OF-WAR
   ========================================= */

.tug-character {
  position: absolute;
  top: 50%;
  width: 44%;
  z-index: 3;
  filter: drop-shadow(0 8px 10px rgba(0,0,0,.45));
}

.tug-bear {
  left: 0;
  transform: translateY(-45%);
}

.tug-bull {
  right: 0;
  transform: translateY(-43%);
}

.tug-character img {
  display: block;
  width: 100%;
  height: auto;
}

/* The painted rope fades into the drawn center segment. */
.tug-bear img {
  mask-image: linear-gradient(to right, #000 0%, #000 86%, transparent 100%);
  animation: bearPull 1.4s ease-in-out infinite;
}
.tug-bull img {
  mask-image: linear-gradient(to left, #000 0%, #000 86%, transparent 100%);
  animation: bullPull 1.4s ease-in-out infinite;
}
/* Market closed: keep both characters completely still. */
.ropeArea.market-closed .tug-character img {
  animation: none !important;
  transform: none !important;
}
/* Tug-of-war intensity */

.tug-waiting .tug-character img {
  animation-duration: 2.4s;
  opacity: 0.75;
}

.tug-early .tug-character img {
  animation-duration: 1.8s;
  opacity: 0.9;
}

.tug-building .tug-character img {
  animation-duration: 1.1s;
  opacity: 1;
}

.tug-confirmed .tug-character img {
  animation-duration: 0.65s;
  opacity: 1;
}

/* Rope reacts to battle intensity */

.tug-waiting .rope {
  opacity: 0.65;
}

.tug-early .rope {
  opacity: 0.8;
}

.tug-building .rope {
  opacity: 0.95;
}

.tug-confirmed .rope {
  opacity: 1;
  filter: brightness(1.18);
}
@keyframes bearPull {
  0%, 100% {
    transform: translateX(0) rotate(0deg);
  }
  50% {
    transform: translateX(-7px) rotate(-2deg);
  }
}

@keyframes bullPull {
  0%, 100% {
    transform: translateX(0) rotate(0deg);
  }
  50% {
    transform: translateX(7px) rotate(2deg);
  }
}

/* Keep the full readout visible in one responsive dashboard. */
body { padding: clamp(8px, 1.3vw, 16px); }
.dashboard {
  max-width: 1450px;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  grid-template-areas:
    "header header"
    "status status"
    "arena arena"
    "action tracker"
    "cards cards"
    "hold analysis"
    "warning warning";
  gap: clamp(5px, .8vw, 10px);
  align-content: start;
}
.dashboard > * { min-width: 0; margin: 0; }
.header { grid-area: header; }
.status { grid-area: status; }
.arena { grid-area: arena; overflow: hidden; padding: 9px 15px; }
.actionBox { grid-area: action; }
.cards { grid-area: cards; margin: 0; }
.entryTracker { grid-area: tracker; }
.holdBox { grid-area: hold; }
.analysisBox { grid-area: analysis; }
.warning { grid-area: warning; }
.ropeArea { height: clamp(115px, 17vh, 175px); }
.tug-character { width: min(38%, 40vh); }
.rope { left: min(35%, 36vh); right: min(35%, 36vh); }
.control { font-size: clamp(25px, 3.2vw, 39px); line-height: 1.05; }
.pressure { margin-top: 1px; }
.actionBox, .entryTracker, .aiReadBox { padding: 9px 12px; }
.action { font-size: clamp(17px, 2.2vw, 27px); line-height: 1.15; }
.phase { font-size: 12px; line-height: 1.25; }
.entryTracker { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 3px 10px; align-items: center; text-align: left; }
.entryTrackerTitle, #entryTrackerState, #entryTrackerDetail, .entryTrackerFoot { grid-column: 1; margin: 0; }
.entryTrackerTitle { font-size: 11px; }
#entryTrackerState { font-size: 14px; line-height: 1.2; }
#entryTrackerDetail { font-size: 11px; line-height: 1.25; overflow-wrap: anywhere; }
.entryTrackerFoot { font-size: 10px; line-height: 1.2; }
#entryTrackerButton { grid-column: 2; grid-row: 1 / span 4; font-size: 12px; padding: 9px 12px; }
.card { padding: 7px 9px; }
.value { font-size: 13px; overflow-wrap: anywhere; }
.aiReadHeadline { font-size: 13px; line-height: 1.2; }
.aiReadMeta, .aiReadNote { margin-top: 3px; line-height: 1.2; }
.warning { padding: 6px 10px; font-size: 11px; }

@media (max-width: 700px) and (orientation: portrait) {
  body { padding: 7px; }
  .dashboard {
    grid-template-columns: minmax(0, 1fr);
    grid-template-areas:
      "header" "status" "arena" "action" "tracker"
      "cards" "hold" "analysis" "warning";
    gap: 5px;
  }
  .header { gap: 8px; }
  .title { font-size: 10px; letter-spacing: 1px; }
  .price { font-size: 22px; }
  .session { font-size: 10px; }
  .status { margin: 0; }
  .control { font-size: clamp(22px, 7vw, 31px); }
  .pressure { font-size: 12px; }
  .arena { padding: 7px 9px; border-radius: 12px; }
  .teams { font-size: 15px; }
  .ropeArea { height: clamp(105px, 17vh, 135px); }
  .tug-character { width: min(43%, 40vh); }
  .rope { left: min(40%, 36vh); right: min(40%, 36vh); }
  .entryText { font-size: 8px; padding: 4px 5px; }
  .knot { width: 28px; height: 28px; top: calc(50% - 14px); border-width: 4px; }
  .actionBox { padding: 7px; }
  .action { font-size: 17px; }
  .phase { font-size: 10px; }
  .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 4px; }
  .card { padding: 5px 8px; }
  .label { font-size: 9px; }
  .value { font-size: 12px; margin-top: 1px; }
  .entryTracker { padding: 7px 9px; gap: 2px 7px; }
  #entryTrackerState { font-size: 12px; }
  #entryTrackerButton { font-size: 11px; min-height: 42px; padding: 6px 8px; }
  .aiReadBox { padding: 7px 9px; }
  .aiReadHeadline { margin-top: 2px; font-size: 12px; }
  .aiReadMeta, .aiReadNote { font-size: 10px; }
}

@media (orientation: landscape) and (max-height: 600px) {
  body { padding: 5px 9px; }
  .dashboard {
    grid-template-areas:
      "header header"
      "status action"
      "arena tracker"
      "cards cards"
      "hold analysis"
      "warning warning";
    gap: 4px 7px;
  }
  .header { margin: 0; }
  .title { font-size: 10px; }
  .price { font-size: 22px; }
  .status { margin: 0; text-align: left; }
  .control { font-size: 22px; }
  .pressure { font-size: 11px; }
  .arena { padding: 5px 9px; border-radius: 10px; }
  .teams { font-size: 14px; }
  .ropeArea { height: clamp(100px, 27vh, 145px); }
  .tug-character { width: min(43%, 40vh); }
  .rope { left: min(40%, 36vh); right: min(40%, 36vh); }
  .entryText { font-size: 8px; padding: 3px 4px; }
  .knot { width: 26px; height: 26px; top: calc(50% - 13px); border-width: 4px; }
  .actionBox { padding: 6px; }
  .action { font-size: 16px; }
  .phase { font-size: 10px; }
  .entryTracker { padding: 6px 8px; }
  .entryTrackerFoot { font-size: 9px; }
  .cards { grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 5px; }
  .card { padding: 4px 7px; }
  .label { font-size: 8px; }
  .value { font-size: 11px; margin-top: 1px; }
  .aiReadBox { padding: 5px 8px; }
  .aiReadTitle { font-size: 9px; }
  .aiReadHeadline { margin-top: 2px; font-size: 11px; }
  .aiReadMeta, .aiReadNote { font-size: 9px; margin-top: 2px; }
  .warning { padding: 4px; font-size: 10px; }
}


.candleMiniCard .value { display:flex; align-items:center; justify-content:space-between; gap:8px; }
#candleCountdown { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:15px; }
#candleMove { font-size:10px; color:#f3c969; }
#candleMove.up { color:#55e69a; }
#candleMove.down { color:#ff5b67; }

/* V2.5: reserve a slim strip immediately ABOVE the character artwork.
   BEARS/BULLS labels and their HA scoreboards sit outside the images but flush to their top edge. */
.arena { position: relative; padding-top: 50px !important; }
.arena .ropeArea { margin-top: 0; }

/* =========================================
   V2.3 COCKPIT DASHBOARD UPGRADE
   Central round entry control + contoured readout panels
   ========================================= */
.teamOverlay { position:absolute; top:12px; z-index:11; font-size:clamp(15px,2.1vw,24px); font-weight:1000; letter-spacing:1px; text-shadow:0 2px 5px #000,0 0 10px #000; }
.teamOverlay.bearOverlay { left:2%; color:#ff6570; }
.teamOverlay.bullOverlay { right:2%; color:#62efa5; }
.haSideScore {
  position:absolute; top:4px; z-index:12; min-width:92px; padding:4px 9px 5px;
  border:2px solid currentColor; border-radius:7px; background:#080d13; text-align:center;
  box-shadow:inset 0 0 10px rgba(255,255,255,.04),0 3px 10px rgba(0,0,0,.45);
}
.haSideScore.bearScore { left:17%; color:#ff5b67; }
.haSideScore.bullScore { right:17%; color:#55e69a; }
.haSideScore .scoreTeam { font-size:9px; font-weight:900; letter-spacing:1.2px; }
.haSideScore .scoreNumber { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:23px; line-height:1; font-weight:1000; text-shadow:0 0 8px currentColor; }
.haSideScore .scoreLabel { font-size:7px; letter-spacing:1.3px; opacity:.9; margin-top:2px; }
.arena .teams { display:none; }


/* V3.1 — responsive five-box strength strip under the animation. */
.strengthMeter {
  grid-area:strength; grid-column:1 / -1; display:grid; grid-template-columns:minmax(0,1fr) auto minmax(0,1fr);
  align-items:center; gap:10px; min-height:34px; padding:5px 12px; margin:0;
  background:linear-gradient(180deg,#0d141d,#090e14); border:1px solid #293442; border-radius:10px;
}
.strengthSide { display:flex; align-items:center; gap:7px; min-width:0; }
.strengthBear { justify-content:flex-end; }
.strengthBull { justify-content:flex-start; }
.strengthName { font-size:10px; font-weight:1000; letter-spacing:1px; }
.strengthBear .strengthName,.strengthBear .strengthCount { color:#ff6570; }
.strengthBull .strengthName,.strengthBull .strengthCount { color:#62efa5; }
.strengthCount { font:900 10px ui-monospace,SFMono-Regular,Menlo,monospace; min-width:23px; }
.strengthBoxes { display:flex; gap:3px; }
.strengthBox { width:clamp(12px,2.1vw,24px); height:10px; border-radius:2px; border:1px solid #394553; background:#151d27; opacity:.45; }
.strengthBox.bear.on { background:#ff5b67; border-color:#ff7b84; opacity:1; box-shadow:0 0 6px rgba(255,91,103,.45); }
.strengthBox.bull.on { background:#55e69a; border-color:#75efad; opacity:1; box-shadow:0 0 6px rgba(85,230,154,.42); }
.strengthCaption { color:#9aa7b7; font-size:8px; font-weight:900; letter-spacing:1.2px; white-space:nowrap; }
@media (max-width:700px) {
  .strengthMeter { gap:5px; padding:4px 6px; min-height:30px; }
  .strengthSide { gap:4px; }
  .strengthBox { width:clamp(9px,3vw,15px); height:8px; gap:2px; }
  .strengthName { font-size:8px; }
  .strengthCount { font-size:8px; min-width:18px; }
  .strengthCaption { font-size:7px; letter-spacing:.7px; }
}
@media (max-width:430px) and (orientation:portrait) {
  .strengthCaption { display:none; }
  .strengthMeter { grid-template-columns:1fr 1fr; }
  .strengthBox { width:10px; }
}
@media (max-height:500px) and (orientation:landscape) {
  .strengthMeter { min-height:25px; padding:2px 8px; }
  .strengthBox { height:7px; }
}

.cockpit {
  grid-area: cockpit; position:relative; display:grid;
  grid-template-columns:minmax(0,1fr) clamp(132px,17vw,188px) minmax(0,1fr);
  grid-template-areas:"action center hold" "analysis center tracker";
  gap:8px 0; align-items:stretch; min-height:205px; margin-top:0;
}
.cockpit .actionBox { grid-area:action; }
.cockpit .holdBox { grid-area:hold; }
.cockpit .analysisBox { grid-area:analysis; }
.cockpit .entryTracker { grid-area:tracker; }
.cockpit .actionBox,.cockpit .holdBox,.cockpit .analysisBox,.cockpit .entryTracker {
  margin:0; min-width:0; display:flex; flex-direction:column; justify-content:center;
  background:linear-gradient(180deg,#151d28,#0e141d); border:1px solid #303b49;
}
.cockpit .actionBox,.cockpit .analysisBox { border-radius:16px 0 0 16px; padding-right:34px; }
.cockpit .holdBox,.cockpit .entryTracker { border-radius:0 16px 16px 0; padding-left:34px; }
.cockpitCenter {
  grid-area:center; z-index:5; align-self:center; justify-self:center; width:clamp(132px,17vw,188px);
  aspect-ratio:1; border-radius:50%; padding:10px; background:radial-gradient(circle at 50% 40%,#2a3441 0 44%,#10161e 45% 63%,#596473 64% 67%,#090d12 68%);
  box-shadow:0 0 0 5px #080c12,0 0 0 7px #27313e,0 12px 28px rgba(0,0,0,.6);
  display:flex; align-items:center; justify-content:center;
}
#entryTrackerButton {
  width:100%; height:100%; min-height:0; padding:15px; border-radius:50%; border:3px solid #ffe49b;
  background:radial-gradient(circle at 50% 35%,#ffe17e,#d8a72d 70%,#9a6f11); color:#11161d;
  box-shadow:inset 0 3px 7px rgba(255,255,255,.5),inset 0 -6px 12px rgba(79,49,0,.35),0 0 20px rgba(244,201,93,.25);
  font-size:clamp(12px,1.45vw,18px); line-height:1.15; font-weight:1000; letter-spacing:.04em; cursor:pointer;
}
#entryTrackerButton:disabled { opacity:.52; filter:grayscale(.35); cursor:not-allowed; }
.entryTracker { display:flex !important; text-align:left; }
.entryTrackerTitle,#entryTrackerState,#entryTrackerDetail,.entryTrackerFoot { margin:0; }
.entryTrackerTitle { color:#ffdc79; }
#entryTrackerState { margin-top:4px; }
#entryTrackerDetail { margin-top:3px; }
.entryTrackerFoot { margin-top:4px; }
.dashboard { grid-template-areas:"header header" "status status" "arena arena" "strength strength" "cockpit cockpit" "cards cards" "warning warning"; }
.actionBox,.entryTracker,.holdBox,.analysisBox { grid-area:unset; }

@media (max-width:700px) and (orientation:portrait) {
  .dashboard { grid-template-areas:"header" "status" "arena" "strength" "cockpit" "cards" "warning"; }
  .cockpit { grid-template-columns:minmax(0,1fr) 116px minmax(0,1fr); grid-template-areas:"action center hold" "analysis center tracker"; min-height:170px; gap:5px 0; }
  .cockpitCenter { width:116px; padding:8px; }
  .cockpit .actionBox,.cockpit .analysisBox { padding:7px 25px 7px 7px; }
  .cockpit .holdBox,.cockpit .entryTracker { padding:7px 7px 7px 25px; }
  #entryTrackerButton { font-size:11px; padding:8px; }
  .entryTrackerFoot { display:none; }
  #entryTrackerDetail { font-size:9px; }
  #entryTrackerState { font-size:10px; }
  .aiReadHeadline { font-size:10px; }
  .aiReadMeta,.aiReadNote { font-size:8px; }
}
@media (orientation:landscape) and (max-height:600px) {
  .dashboard { grid-template-areas:"header header" "status status" "arena arena" "strength strength" "cockpit cockpit" "cards cards" "warning warning"; }
  .cockpit { min-height:150px; grid-template-columns:minmax(0,1fr) 126px minmax(0,1fr); gap:4px 0; }
  .cockpitCenter { width:126px; padding:8px; }
  .cockpit .actionBox,.cockpit .analysisBox { padding:5px 26px 5px 7px; }
  .cockpit .holdBox,.cockpit .entryTracker { padding:5px 7px 5px 26px; }
  #entryTrackerButton { font-size:11px; padding:8px; }
  .entryTrackerFoot { display:none; }
}

/* V2.7: keep the battle marker between the illustrated hands. */
.ropeArea .knot { z-index: 9; }
.bearEntry,.bearText { left:36%; }
.bullEntry,.bullText { left:64%; }
.rope { left:28%; right:28%; }

/* A separate stock-move rope avoids mixing a personal entry with battle pressure. */
.entryGauge {
  grid-area:gauge; min-width:0; padding:9px 18px 7px;
  background:#111720; border:1px solid #303b49; border-radius:14px;
}
.gaugeHeader,.gaugeEnds { display:flex; justify-content:space-between; align-items:center; gap:8px; font-size:11px; color:#b7c2d0; }
.gaugeHeader span:first-child { font-weight:900; letter-spacing:.08em; color:#f3d783; }
.gaugeEnds span:first-child { color:#ff8590; }
.gaugeEnds span:last-child { color:#80ffc0; }
#gaugeMove { font-weight:800; color:#f5f7fa; text-align:center; }
.gaugeTrack { position:relative; height:60px; margin:3px 0; }
.gaugeRope { position:absolute; left:3%; right:3%; top:26px; height:10px; border-radius:9px;
  background:repeating-linear-gradient(45deg,#856143,#856143 7px,#c39764 7px,#c39764 14px);
  box-shadow:0 1px 8px #000; }
.gaugeGiveback { position:absolute; top:26px; height:10px; background:rgba(255,103,119,.65);
  box-shadow:0 0 9px rgba(255,103,119,.6); z-index:2; border-radius:8px; }
.gaugeBest { position:absolute; top:23px; width:15px; height:15px; transform:translateX(-50%) rotate(45deg);
  background:#c39cff; border:2px solid #eee0ff; box-shadow:0 0 11px #af80ff; z-index:5; }
.gaugeEntry,.gaugeCurrent { position:absolute; transform:translateX(-50%); font-size:10px; font-weight:900; white-space:nowrap; }
.gaugeEntry { left:50%; top:0; color:#f6cd72; }
.gaugeEntry::after { content:""; display:block; width:8px; height:20px; margin:2px auto 0; border-radius:5px; background:#f6cd72; }
.gaugeCurrent { top:43px; color:#73c9ff; transition:left .6s ease; }
.gaugeCurrent::before { content:""; position:absolute; left:50%; top:-22px; transform:translateX(-50%); width:8px; height:20px; border-radius:5px; background:#55b7ff; box-shadow:0 0 10px #42aaff; }
.gaugeProgress { margin-top:3px; min-height:16px; text-align:center; font-size:11px; font-weight:800; color:#d8bbff; }
.gaugeProgress.hasGiveback { color:#ff9ba5; }
.dashboard { grid-template-areas:"header header" "status status" "arena arena" "strength strength" "cockpit cockpit" "cards cards" "warning warning"; }
.cards { grid-template-columns:repeat(2,minmax(0,1fr)); }
.cockpit { grid-template-columns:minmax(0,1fr) clamp(205px,22vw,275px) minmax(0,1fr); }
.cockpitCenter { width:clamp(205px,22vw,275px); height:118px; aspect-ratio:auto; border-radius:65px; gap:9px; padding:12px;
  background:linear-gradient(180deg,#1b2733,#0d151d); }
.directionButton { width:50%; height:100%; min-height:62px; border-radius:50%; font-weight:1000; font-size:clamp(17px,2vw,23px); color:white; cursor:pointer; }
.callButton { border:2px solid #8ffac1; background:radial-gradient(circle at 45% 35%,#3bdc8e,#087246); }
.putButton { border:2px solid #ff9ca5; background:radial-gradient(circle at 45% 35%,#ff747e,#9d1725); }
.directionButton:disabled { opacity:.45; filter:grayscale(.5); cursor:not-allowed; }
/* V3.2.5 visual setup cues — display only; underlying trading logic is unchanged. */
.directionButton.setupCue { position:relative; opacity:1; filter:none; }
.callButton.setupCue {
  animation:bvbCallSetupPulse 1.65s ease-in-out infinite;
  box-shadow:0 0 0 3px rgba(104,255,183,.22),0 0 24px rgba(66,255,164,.82);
}
.putButton.setupCue {
  animation:bvbPutSetupPulse 1.65s ease-in-out infinite;
  box-shadow:0 0 0 3px rgba(255,122,137,.22),0 0 24px rgba(255,78,98,.82);
}
.directionButton.setupCue::after {
  content:"SETUP"; position:absolute; left:50%; bottom:-18px; transform:translateX(-50%);
  font-size:9px; letter-spacing:.12em; font-weight:1000; white-space:nowrap;
  color:#f5f7fa; text-shadow:0 1px 5px #000;
}
@keyframes bvbCallSetupPulse {
  0%,100% { transform:scale(1); box-shadow:0 0 0 2px rgba(104,255,183,.18),0 0 14px rgba(66,255,164,.52); }
  50% { transform:scale(1.045); box-shadow:0 0 0 5px rgba(104,255,183,.28),0 0 34px rgba(66,255,164,.95); }
}
@keyframes bvbPutSetupPulse {
  0%,100% { transform:scale(1); box-shadow:0 0 0 2px rgba(255,122,137,.18),0 0 14px rgba(255,78,98,.52); }
  50% { transform:scale(1.045); box-shadow:0 0 0 5px rgba(255,122,137,.28),0 0 34px rgba(255,78,98,.95); }
}
@media (prefers-reduced-motion:reduce) { .directionButton.setupCue { animation:none; } }

#endTrackingButton { border:1px solid #e4c36f; background:#263340; color:#f6d57a; border-radius:12px; padding:12px; font-weight:900; cursor:pointer; }
.cockpitCenter.trackingActive { flex-direction:column; height:104px; border-radius:18px; box-shadow:0 0 0 2px #27313e; }
#activeEntryLabel { color:#f5d884; font-weight:900; font-size:12px; letter-spacing:.04em; text-align:center; }
[hidden] { display:none !important; }
@media (max-width:700px) and (orientation:portrait) {
  .dashboard { grid-template-areas:"header" "status" "arena" "strength" "cockpit" "cards" "warning"; }
  .entryGauge { padding:7px 9px; }
  .gaugeHeader,.gaugeEnds { font-size:9px; }
  .cockpit { grid-template-columns:minmax(0,1fr) 124px minmax(0,1fr); }
  .cockpitCenter { width:124px; height:74px; gap:3px; padding:7px; }
  .directionButton { min-height:52px; font-size:12px; }
  .rope { left:31%; right:31%; }
}
@media (orientation:landscape) and (max-height:600px) {
  .dashboard { grid-template-areas:"header header" "status status" "arena arena" "strength strength" "cockpit cockpit" "cards cards" "warning warning"; }
  .entryGauge { padding:4px 10px; }
  .gaugeTrack { height:49px; }
  .gaugeRope { top:22px; }
  .gaugeGiveback { top:22px; }
  .gaugeBest { top:19px; }
  .gaugeEntry::after { height:16px; }
  .gaugeCurrent { top:34px; }
  .gaugeCurrent::before { top:-18px; height:16px; }
  .cockpit { grid-template-columns:minmax(0,1fr) 175px minmax(0,1fr); }
  .cockpitCenter { width:175px; height:85px; gap:5px; padding:8px; }
  .directionButton { min-height:60px; font-size:14px; }
  .rope { left:31%; right:31%; }
}


/* V2 decision-cockpit priorities */
.pressureSupport { opacity:.72; transform:scale(.96); }
body.trade-active .analysisBox, body.trade-active .entryTracker {
  border-color:#ffdc79 !important; box-shadow:0 0 0 1px rgba(255,220,121,.18),0 8px 22px rgba(0,0,0,.24);
}
body.trade-active .actionBox { opacity:1; }
body.trade-active .pressureSupport { opacity:.48; }
body.trade-active .entryGauge { box-shadow:0 0 0 1px rgba(255,220,121,.22); }


/* V3 live-test tuning: the main rope becomes the position tracker after entry. */
.bearEntry,.bullEntry,.entryText { display:none !important; }
.positionBest,.positionCurrent { position:absolute; z-index:12; transform:translateX(-50%); transition:left .55s ease; }
.positionBest { top:43%; width:16px; height:16px; background:#c39cff; border:2px solid #f0e5ff; transform:translate(-50%,-50%) rotate(45deg); box-shadow:0 0 12px #af80ff; }
.positionBest span { position:absolute; transform:rotate(-45deg); left:-9px; top:18px; color:#d8bbff; font-size:9px; font-weight:1000; }
.positionCurrent { top:43%; width:12px; height:28px; border-radius:8px; background:#55b7ff; box-shadow:0 0 11px #42aaff; transform:translate(-50%,-50%); }
.positionCurrent span { display:none; }
.positionGiveback { position:absolute; z-index:7; top:calc(43% - 5px); height:10px; background:rgba(255,103,119,.60); box-shadow:0 0 8px rgba(255,103,119,.5); border-radius:7px; }
.positionSideLabel { position:absolute; z-index:11; top:57%; font-size:9px; font-weight:900; letter-spacing:.04em; }
.positionLeft { left:31%; color:#ff8f9a; }
.positionRight { right:31%; color:#80ffc0; }
.positionStats { position:absolute; z-index:14; left:50%; bottom:0; transform:translateX(-50%); width:min(92%,720px); display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; text-align:center; text-shadow:0 1px 3px #000; }
.positionMetric { min-width:0; padding:3px 7px 2px; border-radius:8px; background:rgba(7,12,18,.72); border:1px solid rgba(255,255,255,.08); }
.positionMetricLabel { display:block; color:#9aa7b7; font-size:clamp(7px,.75vw,9px); font-weight:900; letter-spacing:.12em; line-height:1; }
.positionMetricValue { display:block; margin-top:2px; color:#f5f7fa; font:1000 clamp(14px,1.7vw,22px)/1 ui-monospace,SFMono-Regular,Menlo,monospace; white-space:nowrap; }
.positionMetric.giveback .positionMetricValue { color:#ff9ba5; }
@media (max-width:700px) and (orientation:portrait) {
  .positionStats { width:94%; gap:4px; }
  .positionMetric { padding:2px 3px; }
  .positionMetricValue { font-size:clamp(12px,3.3vw,17px); }
}
@media (orientation:landscape) and (max-height:600px) {
  .positionStats { bottom:-1px; gap:5px; }
  .positionMetricValue { font-size:14px; }
}
body.trade-active .centerLabel { color:#f6cd72; font-weight:1000; }
body.trade-active .centerLine { background:#f6cd72; box-shadow:0 0 8px rgba(246,205,114,.55); }
body.trade-active .knot { display:none; }
body.trade-active .pressureSupport { display:none; }

[hidden] { display:none !important; }
.strengthSide { padding:4px; border:1px solid transparent; border-radius:6px; }
.strengthBear.confirmedSide { border-color:#ff6570; background:#ff657012; }
.strengthBull.confirmedSide { border-color:#62efa5; background:#62efa512; }
.strengthCaption { white-space:normal; text-align:center; }
.cockpit .actionBox,.cockpit .aiReadBox,.cockpit .entryTracker { min-width:0; overflow-wrap:anywhere; }
.dataStatus { grid-column:1 / -1; text-align:center; font-size:10px; color:#aab8ca; padding:3px; }
.chopWarning { margin-top:6px; padding:6px 8px; border:1px solid #f5bf45; border-radius:8px; background:#382b13; color:#ffe5a1; font-size:12px; line-height:1.35; overflow-wrap:anywhere; }
.chopBasis { display:block; margin-top:3px; font-size:10px; opacity:.85; }
</style>
</head>

<body>

<div class="dashboard">

  <div class="header">

    <div>
      <div class="title">
        TUG OF WAR — LIVE TREND BATTLE
      </div>

      <div class="price" id="liveHeaderPrice">
        GOOGL $${Number(latestGOOGLTrade?.price || battle.price || 0).toFixed(2)}
      </div>
    </div>

    <div class="session">
      <div class="dailyBiasLabel">5-DAY MARKET BIAS · 1-DAY CANDLES</div>
      <div class="dailyBiasValue ${dailyBias.bias === "BULLISH" ? "biasBull" : dailyBias.bias === "BEARISH" ? "biasBear" : "biasNeutral"}">
        ${dailyBias.bias === "BULLISH" ? "🟢" : dailyBias.bias === "BEARISH" ? "🔴" : dailyBias.bias === "TRANSITION" ? "🟡" : "⚪"}
        ${dailyBias.bias} ${dailyBias.arrow || ""}
      </div>
      <div class="dailyConfirm">
        ${dailyBias.confirmed ? "TREND CONFIRMED" : dailyBias.bias === "BUILDING" ? "BUILDING" : "TREND NOT CONFIRMED"}
      </div>
      <div class="dailyConfirm">${dailyBias.bias === "BULLISH" ? "Broader market conditions favor buyers." : dailyBias.bias === "BEARISH" ? "Broader market conditions favor sellers." : dailyBias.bias === "TRANSITION" ? "Broader market conditions are mixed." : "Broader market conditions are still developing."}</div>
      <div class="marketLine">${marketSession} · ${regularHours ? "LIVE MARKET" : "MARKET CLOSED"} · <span id="marketClock">--:-- CT</span></div>
    </div>

  </div>


  <div class="status">

    <div class="control" id="controlHeadline">
      ${
        battleControl === "BULLS"
          ? "BULLS WINNING LATEST CANDLE"
          : battleControl === "BEARS"
          ? "BEARS WINNING LATEST CANDLE"
          : "⚖️ BATTLE NEUTRAL"
      }
    </div>

    <div class="pressure" id="pressureSummary">${dashboardStrength}</div>

  </div>


  <div class="arena">

    <div class="teamOverlay bearOverlay">BEARS</div>
    <div class="haSideScore bearScore"><div class="scoreTeam">BEARS</div><div class="scoreNumber">${String(bearHARun).padStart(2, "0")}</div><div class="scoreLabel">CONTROL</div></div>
    <div class="haSideScore bullScore"><div class="scoreTeam">BULLS</div><div class="scoreNumber">${String(bullHARun).padStart(2, "0")}</div><div class="scoreLabel">CONTROL</div></div>
    <div class="teamOverlay bullOverlay">BULLS</div>

    <div class="teams">

      <div class="bears">
         BEARS
      </div>

      <div class="bulls">
         BULLS 
      </div>

    </div>


    <div class="ropeArea ${tugIntensity} ${regularHours ? "market-open" : "market-closed"}">
    <div class="tug-character tug-bear"><img src="/BEARS.jpeg" alt="Bear pulling the rope"></div>
    <div class="tug-character tug-bull"><img src="/BULLS.jpeg" alt="Bull pulling the rope"></div>
    
      <div class="centerLabel" id="mainCenterLabel">NEUTRAL</div>
      <div class="rope"></div>
      <div class="centerLine"></div>
      <div class="knot" id="battleKnot"></div>
      <div class="positionBest" id="positionBest" hidden><span>BEST</span></div>
      <div class="positionCurrent" id="positionCurrent" hidden><span>CURRENT</span></div>
      <div class="positionGiveback" id="positionGiveback" hidden></div>
      <div class="positionSideLabel positionLeft" id="positionLeftLabel" hidden></div>
      <div class="positionSideLabel positionRight" id="positionRightLabel" hidden></div>
      <div class="positionStats" id="positionStats" hidden></div>

    </div>

  </div>

  <div class="strengthMeter" aria-label="Latest completed candle strength; outline marks last confirmed direction">
    <div id="bearStrength" class="strengthSide strengthBear ${view.direction === "PUT" ? "confirmedSide" : ""}"><span class="strengthName">BEARS</span><div class="strengthBoxes">${strengthBoxes("bear", fiveBoxStrength.bear)}</div><span class="strengthCount">${fiveBoxStrength.bear}/5</span></div>
    <div class="strengthCaption">CANDLE STRENGTH<br><span id="confirmedDirection">${view.direction === "CALL" ? "BUYERS" : view.direction === "PUT" ? "SELLERS" : "NO SIDE"} CONFIRMED</span></div>
    <div id="bullStrength" class="strengthSide strengthBull ${view.direction === "CALL" ? "confirmedSide" : ""}"><span class="strengthCount">${fiveBoxStrength.bull}/5</span><div class="strengthBoxes">${strengthBoxes("bull", fiveBoxStrength.bull)}</div><span class="strengthName">BULLS</span></div>
  </div>


  <div class="cockpit">
    <div class="actionBox">
      <div class="action" id="entryAction">${dashboardSignal.title}</div>
      <div class="phase" id="entryActionDetail">${dashboardSignal.detail}</div>
      <div class="chopWarning" id="chopWarning" role="status" ${view.chopActive ? "" : "hidden"}>WAIT — No clear direction. Price is moving back and forth. Wait for conditions to improve before a new entry. Existing trade warnings still apply.</div>
    </div>

    <div class="aiReadBox holdBox">
      <div class="aiReadTitle">TREND STATUS</div>
      <div class="aiReadHeadline" id="trendHeadline">${hold.headline}</div>
      <div class="aiReadNote" id="trendNote">${hold.reason}</div>
    </div>

    <div class="aiReadBox analysisBox">
      <div class="aiReadTitle">MARKET READ</div>
      <div class="aiReadHeadline" id="marketReadHeadline">${aiRead.headline}</div>
      <div class="aiReadMeta" id="marketReadMeta">${aiRead.context}</div>
      <div class="aiReadNote" id="marketReadNote">${aiRead.note}</div>
    </div>

    <div class="entryTracker" aria-live="polite">
      <div class="entryTrackerTitle">MY ENTRY TRACKER · GOOGL</div>
      <div id="entryTrackerState">Checking live price and direction…</div>
      <div id="entryTrackerDetail"></div>
      <div class="entryTrackerFoot">GOOGL marker only. Tracks the stock move from your marked entry; no option P&amp;L or orders.</div>
    </div>

    <div class="cockpitCenter" id="entryControls">
      <button id="markCallButton" class="directionButton callButton" type="button" disabled>CALL</button>
      <button id="markPutButton" class="directionButton putButton" type="button" disabled>PUT</button>
      <div id="activeEntryLabel" hidden></div>
      <button id="endTrackingButton" type="button" hidden>END TRACKING</button>
    </div>
  </div>

  <div class="cards">

    <div class="card contextSupport">
      <div class="label">5-DAY MARKET BIAS · 1-DAY CANDLES</div>
      <div class="value">${dailyBias.bias} ${dailyBias.arrow || ""}</div>
    </div>


    <div class="card candleMiniCard">
      <div class="label">2-MIN CANDLE · FORMING</div>
      <div class="value"><span id="candleCountdown">--:--</span> <span id="candleMove">WAITING</span></div>
    </div>

  </div>

  <div class="warning" id="warningSummary">${view.warning}</div>
  <div class="dataStatus" id="dataStatus">V3.2.8 TEST · Waiting for a fresh price</div>

</div>



<script>
const candleClockData = ${JSON.stringify(candleClockData)};
const candleCountdown = document.getElementById("candleCountdown");
const candleMove = document.getElementById("candleMove");
function refreshCandleClock() {
  const start = Date.parse(candleClockData.time);
  if (!Number.isFinite(start) || !Number.isFinite(candleClockData.open) || !Number.isFinite(candleClockData.close)) {
    candleCountdown.textContent = "--:--"; candleMove.textContent = "WAITING"; candleMove.className = ""; return;
  }
  const secondsLeft = Math.max(0, Math.ceil((start + 120000 - Date.now()) / 1000));
  candleCountdown.textContent = String(Math.floor(secondsLeft/60)).padStart(2,"0") + ":" + String(secondsLeft%60).padStart(2,"0");
  const move = candleClockData.close - candleClockData.open;
  candleMove.textContent = (move > 0 ? "+" : move < 0 ? "−" : "") + "$" + Math.abs(move).toFixed(2);
  candleMove.className = move > 0 ? "up" : move < 0 ? "down" : "";
}
refreshCandleClock(); setInterval(refreshCandleClock,250);
function refreshMarketClock() {
  document.getElementById("marketClock").textContent =
    new Intl.DateTimeFormat("en-US", { timeZone:"America/Chicago", hour:"numeric", minute:"2-digit", second:"2-digit" }).format(new Date()) + " CT";
}
refreshMarketClock(); setInterval(refreshMarketClock,1000);

// Browser-only entry marker. No orders and no brokerage access.
const trackerKey = "bvb-googl-manual-entry-v1";
let view = ${JSON.stringify(view)};
let quote = ${JSON.stringify(latestGOOGLTrade || null)};
let streamStatus = ${JSON.stringify(alpacaStreamStatus)};
let requestHealthy = true, requestInFlight = false;
let trackedEntry = null;
try {
  const saved = JSON.parse(localStorage.getItem(trackerKey) || "null");
  if (saved && ["CALL","PUT"].includes(saved.direction) && Number(saved.price) > 0 &&
      Number.isFinite(Number(saved.price)) && Number.isFinite(Date.parse(saved.time))) {
    trackedEntry = { ...saved, bestObservedMove: Number.isFinite(Number(saved.bestObservedMove)) ? Math.max(0, Number(saved.bestObservedMove)) : 0 };
  }
} catch (_) {}
const el = id => document.getElementById(id);
const callButton = el("markCallButton"), putButton = el("markPutButton"), endButton = el("endTrackingButton");
const cents = value => Math.round(Number(value)*100);
const dollars = value => "$" + Number(value).toFixed(2);
const moveText = value => (value < 0 ? "−" : "+") + dollars(Math.abs(value));
const timeText = time => Number.isFinite(Date.parse(time)) ? new Intl.DateTimeFormat("en-US", {
  timeZone:"America/Chicago", hour:"numeric", minute:"2-digit", second:"2-digit"
}).format(new Date(time)) + " CT" : "not available";
function freshNow() {
  const age = Date.now() - Date.parse(quote?.time);
  const candleAge = Date.now() - Date.parse(view.candleTime);
  return requestHealthy && streamStatus === "connected" && Number(quote?.price) > 0 &&
    Number.isFinite(age) && age >= -10000 && age < 30000 &&
    Number.isFinite(candleAge) && candleAge >= 120000 && candleAge < 360000;
}
function canMarkNow() { return view.regularHours && freshNow(); }
function setText(id, text) { el(id).textContent = text; }
function renderMarket() {
  const fresh = freshNow();
  if (Number(quote?.price) > 0) setText("liveHeaderPrice", "GOOGL " + dollars(quote.price));
  setText("dataStatus", "V3.2.8 TEST · " + (fresh ? "Price updated " : "LAST KNOWN PRICE · ") + timeText(quote?.time) +
    " · Confirmed candle ended " + timeText(Number.isFinite(Date.parse(view.candleTime)) ? new Date(Date.parse(view.candleTime) + 120000).toISOString() : null));
  setText("controlHeadline", view.candleControl === "BULLS" ? "BUYERS LEAD THE LAST COMPLETED CANDLE" :
    view.candleControl === "BEARS" ? "SELLERS LEAD THE LAST COMPLETED CANDLE" : "LAST COMPLETED CANDLE SHOWS INDECISION");
  setText("pressureSummary", view.strength.status);
  for (const side of ["bear","bull"]) {
    const box = el(side + "Strength"), count = view.strength[side];
    box.classList.toggle("confirmedSide", view.direction === (side === "bull" ? "CALL" : "PUT"));
    box.querySelectorAll(".strengthBox").forEach((b,i) => b.classList.toggle("on", i<count));
    box.querySelector(".strengthCount").textContent = count + "/5";
    box.setAttribute("aria-label", (side === "bull" ? "Buyers" : "Sellers") + " candle strength " + count + " of 5" +
      (view.direction === (side === "bull" ? "CALL" : "PUT") ? "; last confirmed direction" : ""));
  }
  setText("confirmedDirection", view.direction === "CALL" ? "BUYERS CONFIRMED" : view.direction === "PUT" ? "SELLERS CONFIRMED" : "DIRECTION UNCONFIRMED");
  setText("entryAction", view.regularHours && !fresh ? "WAIT — Data not current" : view.action.title);
  setText("entryActionDetail", view.regularHours && !fresh ? "Entry cues are paused until fresh prices and candles return." : view.action.detail);
  el("chopWarning").hidden = !view.chopActive;
  setText("trendHeadline", view.hold.headline);
  setText("trendNote", view.hold.reason);
  // Market Read has no dependency on trackedEntry, entry price or giveback.
  setText("marketReadHeadline", view.marketRead.headline);
  setText("marketReadMeta", view.marketRead.context);
  setText("marketReadNote", view.regularHours && !fresh ? "WAIT — Fresh data unavailable. No new entry cue." : view.marketRead.note);
  setText("warningSummary", (view.regularHours && !fresh ? "Fresh data unavailable — entry cues are paused. " : "") + view.warning);
  for (const side of ["bear","bull"]) {
    const count = view.haRunColor === (side === "bull" ? "GREEN" : "RED") ? view.haRunCandles : 0;
    document.querySelector("." + side + "Score .scoreNumber").textContent = String(count).padStart(2,"0");
  }
  const eligible = !trackedEntry && canMarkNow() && !view.chopActive && !view.warningPending;
  for (const [button, side] of [[callButton,"CALL"],[putButton,"PUT"]]) {
    const pulse = eligible && view.pulseDirection === side;
    button.classList.toggle("setupCue", pulse);
    button.setAttribute("aria-label", side + (pulse ? " — favorable entry conditions" : " — mark your entry"));
    button.disabled = !canMarkNow();
  }
}
function positionRead(move, best, giveback) {
  if (!view.regularHours || !freshNow()) return "WAIT — Fresh trading data unavailable";
  const same = view.direction === trackedEntry.direction;
  const oppositeConfirmed = view.direction !== "WAIT" && !same;
  const oppositeCandle = view.candleControl === (trackedEntry.direction === "CALL" ? "BEARS" : "BULLS");
  const evidence = [oppositeCandle, view.riskStage === "WARNING" || view.riskStage === "REGIME_BROKEN",
    oppositeConfirmed, view.changeWatch === "WARNING", oppositeConfirmed && view.entrySignal === view.direction].filter(Boolean).length;
  const ratio = best > 0 ? giveback/best : 0;
  if (oppositeConfirmed || view.riskStage === "REGIME_BROKEN" || (best >= 0.10 && ratio >= 0.90 && evidence >= 2))
    return "EXIT WARNING — Review your trade";
  if (best >= 0.10 && ratio >= 0.75 && evidence >= 2) return "REVERSAL RISK — Protect your gains";
  if (best >= 0.10 && ratio >= 0.55 && evidence >= 1) return "GAINS PULLING BACK — Watch closely";
  if (evidence >= 2 || (best >= 0.10 && ratio >= 0.35) || view.hold.stage === "WARNING")
    return (move < 0 ? "PRICE AGAINST YOUR ENTRY" : move === 0 ? "AT YOUR ENTRY" : "MOVE FAVORS YOUR TRADE") + " — Trend weakening";
  if (move < 0) return "PRICE AGAINST YOUR ENTRY" + (same ? " — Confirmed direction still supports your trade" : " — Watch closely");
  if (same) return giveback > 0 ? "MOVE FAVORS YOUR TRADE — Gains pulling back" : move === 0 ? "AT YOUR ENTRY — Direction supports your trade" : "MOVE FAVORS YOUR TRADE — Direction supports your trade";
  return "TRACKING — Waiting for clear direction";
}
function renderTracker() {
  const active = !!trackedEntry;
  document.body.classList.toggle("trade-active", active);
  document.body.classList.toggle("setup-mode", !active);
  el("entryControls").classList.toggle("trackingActive", active);
  callButton.hidden = active; putButton.hidden = active; endButton.hidden = !active;
  el("activeEntryLabel").hidden = !active;
  for (const id of ["positionCurrent","positionBest","positionGiveback","positionStats","positionLeftLabel","positionRightLabel"]) el(id).hidden = true;
  if (!active) {
    setText("entryTrackerState", canMarkNow() ? "Ready to mark your CALL or PUT" : "WAIT — Fresh trading data unavailable");
    setText("entryTrackerDetail", "After placing your trade, press its button to track GOOGL from that point. A steady button remains available during chop.");
    setText("mainCenterLabel", "NEUTRAL");
    return;
  }
  const entry = cents(trackedEntry.price);
  const fresh = view.regularHours && freshNow();
  const rawMove = fresh ? (cents(quote.price) - entry)/100 : null;
  const move = rawMove === null ? null : rawMove * (trackedEntry.direction === "CALL" ? 1 : -1);
  const priorBest = Math.max(0, cents(trackedEntry.bestObservedMove || 0))/100;
  const best = move === null ? priorBest : Math.max(priorBest, move);
  const giveback = move === null || best <= 0 ? 0 : Math.max(0, cents(best) - cents(move))/100;
  if (fresh && best > priorBest) {
    trackedEntry.bestObservedMove = best;
    try { localStorage.setItem(trackerKey, JSON.stringify(trackedEntry)); } catch (_) {}
  }
  setText("entryTrackerState", trackedEntry.direction + " · " + positionRead(move,best,giveback));
  // Dollar giveback is primary; percentages are suppressed when a small best move distorts them.
  const percent = best >= 0.50 ? " (" + Math.round(giveback/best*100) + "% of best move)" : "";
  setText("entryTrackerDetail", "Entry " + dollars(entry/100) + (move === null ? " · Waiting for a fresh price" :
    " · Move " + moveText(move) + " · Best +" + dollars(best) + " · Given back " + dollars(giveback) + percent) +
    (view.chopActive ? " · Choppy conditions: wait before a new entry; continue monitoring this trade." : ""));
  setText("activeEntryLabel", trackedEntry.direction + " ENTRY ACTIVE");
  setText("mainCenterLabel", "ENTRY " + dollars(entry/100));
  setText("positionLeftLabel", trackedEntry.direction === "PUT" ? "FAVORABLE" : "ADVERSE");
  setText("positionRightLabel", trackedEntry.direction === "CALL" ? "FAVORABLE" : "ADVERSE");
  el("positionLeftLabel").hidden = false; el("positionRightLabel").hidden = false;
  el("positionStats").hidden = false;
  if (move === null) { setText("positionStats", "Waiting for a fresh GOOGL price; your entry and best move are saved."); return; }
  const currentPercent = Math.max(31,Math.min(69,50+rawMove*9.5));
  el("positionCurrent").style.left = currentPercent + "%"; el("positionCurrent").hidden = false;
  if (best > 0) {
    const bestPercent = Math.max(31,Math.min(69,50+best*(trackedEntry.direction === "CALL" ? 1 : -1)*9.5));
    el("positionBest").style.left = bestPercent+"%"; el("positionBest").hidden = false;
    if (giveback > 0) {
      el("positionGiveback").style.left = Math.min(currentPercent,bestPercent)+"%";
      el("positionGiveback").style.width = Math.abs(currentPercent-bestPercent)+"%";
      el("positionGiveback").hidden = false;
    }
  }
  el("positionStats").innerHTML =
    '<div class="positionMetric"><span class="positionMetricLabel">MOVE</span><span class="positionMetricValue">'+moveText(move)+'</span></div>' +
    '<div class="positionMetric"><span class="positionMetricLabel">BEST</span><span class="positionMetricValue">+'+dollars(best)+'</span></div>' +
    '<div class="positionMetric giveback"><span class="positionMetricLabel">GIVEN BACK</span><span class="positionMetricValue">'+dollars(giveback)+'</span></div>';
}
function renderAll() { renderMarket(); renderTracker(); }
function markEntry(direction) {
  if (trackedEntry || !canMarkNow()) return;
  const next = { symbol:"GOOGL", direction, price:cents(quote.price)/100, time:new Date().toISOString(), sourceTime:quote.time, bestObservedMove:0 };
  try { localStorage.setItem(trackerKey, JSON.stringify(next)); trackedEntry = next; renderAll(); }
  catch (_) { setText("entryTrackerState", "Could not save your entry in this browser."); }
}
callButton.addEventListener("click", () => markEntry("CALL"));
putButton.addEventListener("click", () => markEntry("PUT"));
endButton.addEventListener("click", () => {
  if (!confirm("End tracking this entry? This does not close your trade.")) return;
  try { localStorage.removeItem(trackerKey); trackedEntry = null; renderAll(); }
  catch (_) { setText("entryTrackerState", "Could not clear the saved entry. Please try again."); }
});
async function syncLivePriceDisplay() {
  renderAll(); // Expire stale cues even while a request is pending.
  if (requestInFlight) return;
  requestInFlight = true;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 3000);
  try {
    const response = await fetch("/googl-live?dashboard=1", { cache:"no-store", signal:controller.signal });
    if (!response.ok) throw new Error("Live data unavailable");
    const live = await response.json();
    if (!live.dashboard || !live.latestTrade) throw new Error("Incomplete live data");
    view = live.dashboard; quote = live.latestTrade; streamStatus = live.streamStatus;
    requestHealthy = true;
    if (live.developing2MinCandle) {
      candleClockData.time = live.developing2MinCandle.time;
      candleClockData.open = Number(live.developing2MinCandle.open);
      candleClockData.close = Number(live.developing2MinCandle.close);
      refreshCandleClock();
    }
  } catch (_) { requestHealthy = false; }
  finally { clearTimeout(timeout); requestInFlight = false; renderAll(); }
}
renderAll();
syncLivePriceDisplay();
setInterval(syncLivePriceDisplay, 1000);
setTimeout(() => window.location.reload(), 10000);

</script>

</body>
</html>
`;


    res
      .type("html")
      .send(html);

  }
);



// ==================================================
// PAPER STUDY JOURNAL / DAILY REPORT
// Protected by the same BVB event-feed credentials.
// ==================================================

app.get("/paper-study", authorizeBVBEvents, (req, res) => {
  res.json({
    version: paperStudy.version,
    symbol: paperStudy.symbol,
    timeframe: paperStudy.timeframe,
    strategyNotes: {
      Rider: "Separate experimental paper study: 15-minute bias/context, 5-minute trend/location, and 2-minute pullback confirmation. Higher timeframes are seeded once from Alpaca and then aggregated from the existing live GOOGL trade stream.",
      RiderHistory: { bars5m: riderCandles5m.length, bars15m: riderCandles15m.length, required15mBars: 200 }
    },
    updatedAt: paperStudy.updatedAt,
    persistenceFile: PAPER_JOURNAL_FILE,
    agents: paperStudy.agents
  });
});

app.get("/paper-study/daily", authorizeBVBEvents, (req, res) => {
  const requested = String(req.query.date || "").trim();
  const dateKey = /^\d{4}-\d{2}-\d{2}$/.test(requested)
    ? requested
    : studyDateKey(new Date());
  res.json({
    ...paperDailySummary(dateKey),
    updatedAt: paperStudy.updatedAt,
    version: paperStudy.version
  });
});

// ==================================================
// START SERVER
// ==================================================

app.listen(
  PORT,
  () => {

    console.log(
      `BVB Trading Assistant running on port ${PORT}`
    );

  }
);
