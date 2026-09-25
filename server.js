const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());
app.use(express.static(__dirname));

const PORT = process.env.PORT || 10000;

const ALPACA_API_KEY = process.env.ALPACA_API_KEY;
const ALPACA_SECRET_KEY = process.env.ALPACA_SECRET_KEY;

console.log("API key loaded:", Boolean(ALPACA_API_KEY));
console.log("Secret key loaded:", Boolean(ALPACA_SECRET_KEY));


// ==================================================
// BVB LIVE MARKET STATE
// ==================================================

let latestGOOGLTrade = null;
let alpacaStreamStatus = "connecting";

let developingCandle = null;

let completedCandles = [];

// Trend Event History
const MAX_TREND_EVENTS = 100;
let trendEventHistory = [];
let lastTrendEventKey = null;

const MAX_COMPLETED_CANDLES = 300;

let historySeeded = false;


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
  recordTrendEvent(trendAnalysis);
}
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


// ==================================================
// BUILD LIVE 2-MINUTE CANDLE
// ==================================================

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
// OLIVER ANALYSIS V1.1
// ==================================================

function analyzeOliver(candles) {

  if (
    !Array.isArray(candles) ||
    candles.length < 21
  ) {

    return {

      action: "WAIT",

      reason:
        "Need at least 21 completed candles for Oliver analysis."

    };
  }


  const current =
    candles[candles.length - 1];

  const previous =
    candles[candles.length - 2];


  const price =
    Number(current.close);


  const sma8 =
    calculateSMA(
      candles,
      8
    );


  const sma20 =
    calculateSMA(
      candles,
      20
    );


  const sma200 =
    calculateSMA(
      candles,
      200
    );


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


  const previousSMA200 =
    calculatePreviousSMA(
      candles,
      200
    );


  const directionOf =
    (currentValue, previousValue) => {

      if (
        !Number.isFinite(currentValue) ||
        !Number.isFinite(previousValue)
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
  // STATE
  // ------------------------------------------------

  let state =
    "MIXED";


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
  // 200 SMA CONTEXT
  // ------------------------------------------------

  let sma200Context =
    "UNAVAILABLE";


  if (
    Number.isFinite(sma200)
  ) {

    if (price > sma200) {
      sma200Context =
        "ABOVE_200";
    }

    else if (
      price < sma200
    ) {
      sma200Context =
        "BELOW_200";
    }

    else {
      sma200Context =
        "AT_200";
    }
  }


  let sma200Alignment =
    "NEUTRAL";


  if (
    state === "BULLISH" &&
    sma200Context ===
      "ABOVE_200"
  ) {
    sma200Alignment =
      "ALIGNED";
  }


  if (
    state === "BEARISH" &&
    sma200Context ===
      "BELOW_200"
  ) {
    sma200Alignment =
      "ALIGNED";
  }


  if (
    state === "BULLISH" &&
    sma200Context ===
      "BELOW_200"
  ) {
    sma200Alignment =
      "COUNTER_TREND";
  }


  if (
    state === "BEARISH" &&
    sma200Context ===
      "ABOVE_200"
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
  // V1.1 ENTRY PATTERNS
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


  const nearestSMA =
    distanceFrom8 <
    distanceFrom20
      ? "8_SMA"
      : "20_SMA";


  // ------------------------------------------------
  // CHECKS
  // ------------------------------------------------

  let bullishChecks = 0;
  let bearishChecks = 0;


  if (
    state === "BULLISH"
  ) {
    bullishChecks++;
  }


  if (
    state === "BEARISH"
  ) {
    bearishChecks++;
  }


  if (
    structure === "HH_HL"
  ) {
    bullishChecks++;
  }


  if (
    structure === "LH_LL"
  ) {
    bearishChecks++;
  }


  if (bullishTakeover) {
    bullishChecks++;
  }


  if (bearishTakeover) {
    bearishChecks++;
  }


  if (
    expansion === "GREEN"
  ) {
    bullishChecks++;
  }


  if (
    expansion === "RED"
  ) {
    bearishChecks++;
  }


  if (
    sma200Alignment ===
      "ALIGNED" &&
    state === "BULLISH"
  ) {
    bullishChecks++;
  }


  if (
    sma200Alignment ===
      "ALIGNED" &&
    state === "BEARISH"
  ) {
    bearishChecks++;
  }


  // V1.1 patterns carry additional
  // confirmation because they are actual
  // entry-recognition events.

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


  if (
    takeoverNearSMA
  ) {

    entryEvent =
      `${takeoverNearSMA.direction}_TAKEOVER_NEAR_${takeoverNearSMA.near}`;
  }


  if (
    compressionExpansion
  ) {

    entryEvent =
      `${compressionExpansion}_COMPRESSION_EXPANSION`;
  }


  if (
    reversalBreak
  ) {

    entryEvent =
      `${reversalBreak.direction}_REVERSAL_BREAK`;
  }


  // ------------------------------------------------
  // ACTION
  //
  // V1.1 requires an entry event rather than
  // producing a setup from generic checks alone.
  // ------------------------------------------------

  let action =
    "WAIT";


  let reason =
    "No confirmed Oliver entry event.";


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


  if (
    bullishEntryEvent &&
    bullishChecks >= 3 &&
    bullishChecks >
      bearishChecks
  ) {

    action =
      "CALL_SETUP";

    reason =
      "Bullish Oliver entry event confirmed with supporting conditions.";
  }


  if (
    bearishEntryEvent &&
    bearishChecks >= 3 &&
    bearishChecks >
      bullishChecks
  ) {

    action =
      "PUT_SETUP";

    reason =
      "Bearish Oliver entry event confirmed with supporting conditions.";
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
  // RESULT
  // ------------------------------------------------

  return {

    action,

    reason,

    state,

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
      Number.isFinite(sma200)
        ? Number(
            sma200.toFixed(4)
          )
        : null,

    sma200Status:
      Number.isFinite(sma200)
        ? "LIVE"
        : "UNAVAILABLE",

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

  const control = analysis.control || "NEUTRAL";
  const action = analysis.action || "WAIT";
  const price = Number(analysis.price);

  if (!Number.isFinite(price)) return null;

  // A new event is created when the meaningful
  // trend state/action combination changes.
  const eventKey = `${control}|${action}`;

  if (eventKey === lastTrendEventKey) {
    return null;
  }

  const event = {
    time: analysis.analyzedCandle || new Date().toISOString(),
    price,
    control,
    pressure: analysis.pressure || "UNKNOWN",
    ropePosition: analysis.ropePosition ?? 0,
    phase: analysis.phase || "UNKNOWN",
    action,

    entryReady: analysis.entryReady ?? false,
    entryDirection: analysis.entryDirection || "NONE",
    entryPrice: analysis.entryPrice ?? null,
    invalidation: analysis.invalidation ?? null,
    entryEvent: analysis.entryEvent || "NONE",

    haColor: analysis.haColor || "UNKNOWN",
    haRunColor: analysis.haRunColor || "UNKNOWN",
    haRunCandles: analysis.haRunCandles ?? 0,
    haDoji: analysis.haDoji ?? false,

    sma8: analysis.sma8 ?? null,
    sma20: analysis.sma20 ?? null,
    sma200: analysis.sma200 ?? null,
    sma200Context: analysis.sma200Context || "UNAVAILABLE",

    structure: analysis.structure || "UNKNOWN",

    bullEvidence: analysis.bullEvidence || [],
    bearEvidence: analysis.bearEvidence || []
  };

  trendEventHistory.push(event);

  if (trendEventHistory.length > MAX_TREND_EVENTS) {
    trendEventHistory.shift();
  }

  lastTrendEventKey = eventKey;

  console.log(
    `TREND EVENT: ${control} | ${action} | GOOGL $${price}`
  );

  return event;
}


// ==================================================
// START MARKET DATA SYSTEM
// ==================================================

seedHistoricalCandles();

connectAlpacaStream();


// ==================================================
// LIVE GOOGL ENDPOINT
// ==================================================

app.get(
  "/googl-live",
  (req, res) => {

    res.json({

      symbol:
        "GOOGL",

      timeframe:
        "2Min",

      streamStatus:
        alpacaStreamStatus,

      historySeeded:
        historySeeded,

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

const ropePosition = Number(battle.ropePosition || 0);
const ropePercent = Math.max(0, Math.min(100, 50 + ropePosition / 2));
const tugShift = ropePosition * 0.7;

const battleControl = battle.control || "NEUTRAL";
const battlePressure = battle.pressure || "WAITING";

const tugIntensity =
  battlePressure === "CONFIRMED" ? "tug-confirmed" :
  battlePressure === "BUILDING" ? "tug-building" :
  battlePressure === "EARLY" ? "tug-early" :
  "tug-waiting";

const battlePhase = battle.phase || "WAIT";
const battleAction = battle.action || "WAIT";   

const entryCrossed = 
  battle.entryMarker?.crossed || "NONE";

const marketSession =
  battle.marketSession?.session || "UNKNOWN";

const regularHours =
  battle.marketSession?.regularHours ?? false;

const html = `
<!DOCTYPE html>
<html>
<head>

<meta
  name="viewport"
  content="width=device-width, initial-scale=1.0"
/>

<title>BVB V2 — Trend Battle</title>

<style>

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
  height: 110px;
  margin-top: 5px;
}

.rope {
  position: absolute;
  left: 5%;
  right: 5%;
  top: 52px;
  height: 10px;
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
  top: 22px;
  height: 70px;
  width: 2px;
  background: #7c8796;
}

.centerLabel {
  position: absolute;
  left: 50%;
  top: 0;
  transform: translateX(-50%);
  color: #7f8a99;
  font-size: 11px;
}

/* ENTRY MARKERS */

.bearEntry {
  position: absolute;
  left: 32.5%;
  top: 28px;
  height: 60px;
  width: 2px;
  background: #ff5b67;
}

.bullEntry {
  position: absolute;
  left: 67.5%;
  top: 28px;
  height: 60px;
  width: 2px;
  background: #55e69a;
}

.entryText {
  position: absolute;
  top: 90px;
  transform: translateX(-50%);
  font-size: 10px;
  white-space: nowrap;
}

.bearText {
  left: 32.5%;
  color: #ff7b84;
}

.bullText {
  left: 67.5%;
  color: #6bf0a9;
}

/* KNOT */

.knot {
  position: absolute;
  left: ${ropePercent}%;
  top: 38px;

  width: 38px;
  height: 38px;

  transform: translateX(-50%);

  border-radius: 50%;

  background: #f3c969;
  border: 5px solid #ffffff;

  box-shadow:
    0 0 12px rgba(255,255,255,.35);

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
    height: 105px;
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
    height: 100px;
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
  bottom: 28px;
  width: 150px;
  height: auto;
  z-index: 5;
  filter: drop-shadow(0 8px 10px rgba(0,0,0,.45));
}

.tug-bear {
  left: 20px;
  animation: bearPull 1.4s ease-in-out infinite;
}

.tug-bull {
  right: 20px;
  animation: bullPull 1.4s ease-in-out infinite;
}
/* Tug-of-war intensity */

.tug-waiting .tug-bear,
.tug-waiting .tug-bull {
  animation-duration: 2.4s;
  opacity: 0.75;
}

.tug-early .tug-bear,
.tug-early .tug-bull {
  animation-duration: 1.8s;
  opacity: 0.9;
}

.tug-building .tug-bear,
.tug-building .tug-bull {
  animation-duration: 1.1s;
  opacity: 1;
}

.tug-confirmed .tug-bear,
.tug-confirmed .tug-bull {
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

</style>
</head>

<body>

<div class="dashboard">

  <div class="header">

    <div>
      <div class="title">
        BVB V2 — LIVE TREND BATTLE
      </div>

      <div class="price">
        GOOGL $${Number(battle.price || 0).toFixed(2)}
      </div>
    </div>

    <div class="session">
      ${marketSession}<br>
      ${regularHours ? "LIVE MARKET" : "MARKET CLOSED"}
    </div>

  </div>


  <div class="status">

    <div class="control">
      ${
        battleControl === "BULLS"
          ? "BULLS IN CONTROL"
          : battleControl === "BEARS"
          ? "BEARS IN CONTROL"
          : "⚖️ BATTLE NEUTRAL"
      }
    </div>

    <div class="pressure">
      ${battlePressure}
    </div>

  </div>


  <div class="arena">

    <div class="teams">

      <div class="bears">
         BEARS
      </div>

      <div class="bulls">
         BULLS 
      </div>

    </div>


    <div class="ropeArea ${tugIntensity}" style="transform: translateX(${tugShift}px);">
    <img src="/BEARS.PNG" class="tug-character tug-bear" alt="Bear">

    <img src="/BULLS.PNG" class="tug-character tug-bull" alt="Bull">
    
      <div class="centerLabel">
        NEUTRAL
      </div>

      <div class="rope"></div>

      <div class="centerLine"></div>

      <div class="bearEntry"></div>

      <div class="bullEntry"></div>

      <div class="entryText bearText">
        PUT ENTRY ZONE
      </div>

      <div class="entryText bullText">
        CALL ENTRY ZONE
      </div>

      <div class="knot"></div>

    </div>

  </div>


  <div class="actionBox">

    <div class="action">
      ${
        entryCrossed === "BULL_ENTRY"
          ? "🔔 CALL ENTRY"
          : entryCrossed === "BEAR_ENTRY"
          ? "🔔 PUT ENTRY"
          : battleAction
      }
    </div>

    <div class="phase">
      ${battlePhase}
    </div>

  </div>


  <div class="cards">

    <div class="card">
      <div class="label">
        CONTROL
      </div>
      <div class="value">
        ${battleControl}
      </div>
    </div>


    <div class="card">
      <div class="label">
        PRESSURE
      </div>
      <div class="value">
        ${battlePressure}
      </div>
    </div>


    <div class="card">
      <div class="label">
        HEIKIN-ASHI
      </div>
      <div class="value">
        ${battle.haControl || "WAIT"}
      </div>
    </div>


    <div class="card">
      <div class="label">
        HA RUN
      </div>
      <div class="value">
        ${battle.haRunColor || "NONE"}
        ${battle.haRunLength || 0}
      </div>
    </div>

  </div>


  <div class="warning">

    ${
      !regularHours

        ? "🌙 Market closed — analysis is informational until regular trading resumes."

        : battlePhase === "WARNING"

        ? "🔔 Direction-change conditions developing."

        : entryCrossed !== "NONE"

        ? "🎯 Tug-of-war entry threshold crossed."

        : "Monitoring the battle for a change in control."
    }

  </div>

</div>


<script>

setTimeout(
  () => {
    window.location.reload();
  },
  10000
);

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
