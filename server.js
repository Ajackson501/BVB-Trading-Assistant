const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

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


    const html = `
<!DOCTYPE html>

<html>

<head>

<meta
  name="viewport"
  content="width=device-width, initial-scale=1.0"
/>

<title>
Agent Oliver — GOOGL
</title>

<style>

* {
  box-sizing: border-box;
}

body {

  margin: 0;

  padding: 24px;

  background: #0b0e13;

  color: #ffffff;

  font-family:
    -apple-system,
    BlinkMacSystemFont,
    "Segoe UI",
    sans-serif;

}

.container {

  max-width: 760px;

  margin: 0 auto;

}

.header {

  margin-bottom: 22px;

}

.agent {

  font-size: 14px;

  letter-spacing: 2px;

  color: #8d98a8;

}

.symbol {

  font-size: 36px;

  font-weight: 800;

  margin-top: 5px;

}

.price {

  font-size: 22px;

  color: #c9d1d9;

  margin-top: 4px;

}

.status {

  margin-top: 18px;

  padding: 22px;

  border-radius: 16px;

  text-align: center;

  font-size: 34px;

  font-weight: 900;

}

.status.call {

  background: #123d2b;

  border: 2px solid #2ecc71;

  color: #62e69a;

}

.status.put {

  background: #421d24;

  border: 2px solid #ff5364;

  color: #ff7583;

}

.status.wait {

  background: #2b3038;

  border: 2px solid #7d8795;

  color: #d1d5db;

}

.reason {

  margin-top: 10px;

  text-align: center;

  color: #aeb7c2;

  font-size: 15px;

}

.grid {

  display: grid;

  grid-template-columns:
    repeat(2, 1fr);

  gap: 14px;

  margin-top: 22px;

}

.card {

  background: #151a22;

  border: 1px solid #252c37;

  border-radius: 14px;

  padding: 18px;

}

.label {

  color: #8792a2;

  font-size: 12px;

  text-transform: uppercase;

  letter-spacing: 1px;

}

.value {

  margin-top: 7px;

  font-size: 22px;

  font-weight: 700;

}

.trade {

  margin-top: 22px;

  background: #151a22;

  border: 1px solid #252c37;

  border-radius: 14px;

  padding: 20px;

}

.tradeRow {

  display: flex;

  justify-content: space-between;

  align-items: center;

  padding: 12px 0;

  border-bottom:
    1px solid #252c37;

}

.tradeRow:last-child {

  border-bottom: none;

}

.tradeLabel {

  color: #8792a2;

}

.tradeValue {

  font-size: 21px;

  font-weight: 800;

  text-align: right;

}

.footer {

  margin-top: 20px;

  color: #657080;

  font-size: 12px;

  text-align: center;

}

@media (
  max-width: 600px
) {

  body {

    padding: 15px;

  }

  .grid {

    grid-template-columns:
      1fr;

  }

}

</style>

</head>


<body>

<div class="container">


<div class="header">

  <div class="agent">
    AGENT OLIVER — LIVE ANALYSIS
  </div>

  <div class="symbol">
    GOOGL
  </div>

  <div class="price">
    ${money(analysis.price)}
  </div>

</div>


<div class="status ${actionClass}">

  ${actionText}

</div>


<div class="reason">

  ${analysis.reason || ""}

</div>


<div class="grid">


<div class="card">

  <div class="label">
    8 SMA
  </div>

  <div class="value">
    ${money(analysis.sma8)}
    ${arrow(
      analysis.sma8Direction
    )}
  </div>

</div>


<div class="card">

  <div class="label">
    20 SMA
  </div>

  <div class="value">
    ${money(analysis.sma20)}
    ${arrow(
      analysis.sma20Direction
    )}
  </div>

</div>


<div class="card">

  <div class="label">
    200 SMA
  </div>

  <div class="value">
    ${money(analysis.sma200)}
    ${arrow(
      analysis.sma200Direction
    )}
  </div>

</div>


<div class="card">

  <div class="label">
    200 SMA Context
  </div>

  <div class="value">
    ${analysis.sma200Context || "—"}
  </div>

</div>


<div class="card">

  <div class="label">
    Market State
  </div>

  <div class="value">
    ${analysis.state || "—"}
  </div>

</div>


<div class="card">

  <div class="label">
    Structure
  </div>

  <div class="value">
    ${analysis.structure || "—"}
  </div>

</div>


<div class="card">

  <div class="label">
    Nearest Average
  </div>

  <div class="value">
    ${analysis.nearestSMA || "—"}
  </div>

</div>


<div class="card">

  <div class="label">
    Expansion
  </div>

  <div class="value">
    ${analysis.expansion || "NONE"}
  </div>

</div>


<div class="card">

  <div class="label">
    Bullish Checks
  </div>

  <div class="value">
    ${analysis.bullishChecks ?? 0}
  </div>

</div>


<div class="card">

  <div class="label">
    Bearish Checks
  </div>

  <div class="value">
    ${analysis.bearishChecks ?? 0}
  </div>

</div>


</div>


<div class="trade">


<div class="tradeRow">

  <div class="tradeLabel">
    Trigger
  </div>

  <div class="tradeValue">
    ${money(
      analysis.trigger
    )}
  </div>

</div>


<div class="tradeRow">

  <div class="tradeLabel">
    Invalidation
  </div>

  <div class="tradeValue">
    ${money(
      analysis.invalidation
    )}
  </div>

</div>


<div class="tradeRow">

  <div class="tradeLabel">
    Bullish Takeover
  </div>

  <div class="tradeValue">
    ${
      analysis.bullishTakeover
        ? "YES"
        : "NO"
    }
  </div>

</div>


<div class="tradeRow">

  <div class="tradeLabel">
    Bearish Takeover
  </div>

  <div class="tradeValue">
    ${
      analysis.bearishTakeover
        ? "YES"
        : "NO"
    }
  </div>

</div>


<div class="tradeRow">

  <div class="tradeLabel">
    200 SMA
  </div>

  <div class="tradeValue">

    ${money(
      analysis.sma200
    )}

    ${arrow(
      analysis.sma200Direction
    )}

  </div>

</div>


<div class="tradeRow">

  <div class="tradeLabel">
    200 Alignment
  </div>

  <div class="tradeValue">
    ${
      analysis.sma200Alignment ||
      "NEUTRAL"
    }
  </div>

</div>


<div class="tradeRow">

  <div class="tradeLabel">
    200 Status
  </div>

  <div class="tradeValue">
    ${
      analysis.sma200Status ||
      "INSUFFICIENT_DATA"
    }
  </div>

</div>


</div>


<div class="footer">

  2-minute GOOGL candles •

  ${
    completedCandles.length
  } completed candles •

  ${
    alpacaStreamStatus
  }

  <br><br>

  Last analyzed candle:

  ${
    analysis.analyzedCandle ||
    "waiting"
  }

</div>


</div>


<script>

// Refresh Oliver's analysis every 10 seconds.

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
