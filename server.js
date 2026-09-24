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

// Persistent trend lock: established control survives ordinary pullbacks.
let trendLock = {
  direction: "NEUTRAL",
  oppositeConfirmations: 0,
  lastProcessedCandle: null
};


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
      "Alpaca credentials missing."
    );

    return;
  }


  if (
    alpacaWS &&
    (
      alpacaWS.readyState ===
        WebSocket.OPEN ||
      alpacaWS.readyState ===
        WebSocket.CONNECTING
    )
  ) {

    return;
  }


  alpacaStreamStatus =
    "connecting";


  console.log(
    "Connecting to Alpaca IEX websocket..."
  );


  alpacaWS =
    new WebSocket(
      "wss://stream.data.alpaca.markets/v2/iex"
    );


  alpacaWS.on(
    "open",
    () => {

      alpacaStreamStatus =
        "connected";

      console.log(
        "Connected to Alpaca websocket."
      );

    }
  );


  alpacaWS.on(
    "message",
    (rawData) => {

      let messages;


      try {

        messages =
          JSON.parse(
            rawData.toString()
          );

      } catch (error) {

        console.error(
          "Unable to parse Alpaca websocket message:",
          error
        );

        return;
      }


      if (!Array.isArray(messages)) {

        messages = [messages];

      }


      for (
        const message of messages
      ) {

        // ------------------------------------------
        // AUTHENTICATION PROMPT
        // ------------------------------------------

        if (
          message.T ===
            "success" &&
          message.msg ===
            "connected"
        ) {

          alpacaWS.send(
            JSON.stringify({

              action: "auth",

              key:
                ALPACA_API_KEY,

              secret:
                ALPACA_SECRET_KEY

            })
          );


          continue;
        }


        // ------------------------------------------
        // AUTHENTICATION SUCCESS
        // ------------------------------------------

        if (
          message.T ===
            "success" &&
          message.msg ===
            "authenticated"
        ) {

          console.log(
            "Alpaca websocket authenticated."
          );


          alpacaWS.send(
            JSON.stringify({

              action:
                "subscribe",

              trades: [
                "GOOGL"
              ]

            })
          );


          continue;
        }


        // ------------------------------------------
        // LIVE TRADE
        // ------------------------------------------

        if (
          message.T === "t" &&
          message.S === "GOOGL"
        ) {

          const trade = {

            symbol:
              message.S,

            price:
              Number(message.p),

            size:
              Number(message.s) || 0,

            time:
              message.t

          };


          latestGOOGLTrade =
            trade;


          updateDevelopingCandle(
            trade
          );


          continue;
        }


        // ------------------------------------------
        // CONNECTION LIMIT / ERRORS
        // ------------------------------------------

        if (
          message.T === "error"
        ) {

          console.error(
            "Alpaca websocket error:",
            message
          );


          const errorText =
            String(
              message.msg || ""
            ).toLowerCase();


          if (
            message.code === 406 ||
            errorText.includes(
              "connection limit"
            )
          ) {

            alpacaStreamStatus =
              "waiting_for_connection_slot";


            try {

              alpacaWS.close();

            } catch (_) {}


            scheduleReconnect(
              CONNECTION_LIMIT_RETRY_DELAY
            );

          } else {

            alpacaStreamStatus =
              "error";

          }


          continue;
        }


        // Useful while diagnosing subscription state.
        if (
          message.T !==
            "subscription"
        ) {

          console.log(
            "Alpaca message:",
            message
          );

        }

      }

    }
  );


  alpacaWS.on(
    "close",
    (code, reason) => {

      console.log(
        "Alpaca websocket closed:",
        code,
        reason
          ? reason.toString()
          : ""
      );


      alpacaWS = null;


      if (
        alpacaStreamStatus !==
          "waiting_for_connection_slot"
      ) {

        alpacaStreamStatus =
          "disconnected";


        scheduleReconnect(
          NORMAL_RECONNECT_DELAY
        );

      }

    }
  );


  alpacaWS.on(
    "error",
    (error) => {

      console.error(
        "Alpaca websocket transport error:",
        error.message
      );

    }
  );

}


// ==================================================
// BASIC CANDLE HELPERS
// ==================================================

function candleDirection(candle) {

  if (!candle) {
    return "neutral";
  }


  if (
    candle.close >
    candle.open
  ) {

    return "green";

  }


  if (
    candle.close <
    candle.open
  ) {

    return "red";

  }


  return "neutral";

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


// ==================================================
// SIMPLE MOVING AVERAGE
// ==================================================

function calculateSMA(
  candles,
  period,
  endIndex =
    candles.length - 1
) {

  if (
    !Array.isArray(candles) ||
    period <= 0 ||
    endIndex < period - 1
  ) {

    return null;

  }


  let total = 0;


  for (
    let i =
      endIndex - period + 1;
    i <= endIndex;
    i++
  ) {

    total +=
      Number(
        candles[i].close
      );

  }


  return total / period;

}
// ==================================================
// PREVIOUS SMA
// ==================================================

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


// ==================================================
// CANDLE RANGE / AVERAGE BODY
// ==================================================

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


// ==================================================
// TAKEOVER CANDLES
// ==================================================

function isBullishTakeover(
  previous,
  current
) {

  if (!previous || !current) {
    return false;
  }

  if (
    candleDirection(previous) !== "red" ||
    candleDirection(current) !== "green"
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
    candleDirection(previous) !== "green" ||
    candleDirection(current) !== "red"
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


// ==================================================
// SHORT-TERM STRUCTURE
// ==================================================

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


// ==================================================
// EXPANSION CANDLE
// ==================================================

function detectExpansion(candles) {

  if (
    !Array.isArray(candles) ||
    candles.length < 3
  ) {
    return null;
  }

  const current =
    candles[candles.length - 1];

  const priorTwo =
    candles.slice(-3, -1);

  const baseline =
    averageBody(priorTwo);

  const currentBody =
    candleBody(current);

  if (
    baseline <= 0 ||
    currentBody <
      baseline * 1.5
  ) {
    return null;
  }

  return {
    direction:
      candleDirection(current),
    body: currentBody,
    baseline
  };
}


// ==================================================
// PULLBACK / COMPRESSION -> EXPANSION
// ==================================================

function detectCompressionExpansion(
  candles
) {

  if (
    !Array.isArray(candles) ||
    candles.length < 4
  ) {
    return null;
  }

  const recent =
    candles.slice(-4);

  const first =
    recent[0];

  const middle1 =
    recent[1];

  const middle2 =
    recent[2];

  const current =
    recent[3];

  const firstBody =
    candleBody(first);

  const middleAverage =
    averageBody([
      middle1,
      middle2
    ]);

  const currentBody =
    candleBody(current);

  if (
    firstBody <= 0 ||
    middleAverage <= 0
  ) {
    return null;
  }

  const compressed =
    middleAverage <
      firstBody * 0.75;

  const expanding =
    currentBody >=
      middleAverage * 1.5;

  if (
    !compressed ||
    !expanding
  ) {
    return null;
  }

  return {
    direction:
      candleDirection(current),
    compressed: true,
    expansion: true,
    currentBody,
    middleAverage
  };
}


// ==================================================
// REVERSAL CLUSTER + BREAK
// ==================================================

function detectReversalCluster(
  candles
) {

  if (
    !Array.isArray(candles) ||
    candles.length < 5
  ) {
    return null;
  }

  const recent =
    candles.slice(-5);

  const a = recent[0];
  const b = recent[1];
  const c = recent[2];
  const d = recent[3];
  const e = recent[4];

  const bullishCluster =
    Number(c.low) >=
      Number(a.low) &&
    Number(d.close) >
      Number(d.open) &&
    Number(e.close) >
      Number(e.open) &&
    Number(e.high) >
      Number(d.high);

  const bearishCluster =
    Number(c.high) <=
      Number(a.high) &&
    Number(d.close) <
      Number(d.open) &&
    Number(e.close) <
      Number(e.open) &&
    Number(e.low) <
      Number(d.low);

  if (bullishCluster) {

    return {
      direction: "green",
      pattern:
        "HIGHER_LOW_DIRECTIONAL_BREAK"
    };

  }

  if (bearishCluster) {

    return {
      direction: "red",
      pattern:
        "LOWER_HIGH_DIRECTIONAL_BREAK"
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

    state =
      "BULLISH";
  }


  if (
    price < sma8 &&
    sma8 < sma20 &&
    sma8Direction === "FALLING" &&
    sma20Direction === "FALLING"
  ) {

    state =
      "BEARISH";
  }


  // ------------------------------------------------
  // 200 SMA CONTEXT
  // ------------------------------------------------

  let sma200Context =
    "UNAVAILABLE";


  if (
    Number.isFinite(sma200)
  ) {

    if (
      price > sma200
    ) {

      sma200Context =
        "ABOVE_200";

    } else if (
      price < sma200
    ) {

      sma200Context =
        "BELOW_200";

    } else {

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
  // ENTRY PATTERN INFORMATION
  // ------------------------------------------------

  const compressionExpansion =
    detectCompressionExpansion(
      candles
    );


  const reversalCluster =
    detectReversalCluster(
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


  if (
    bullishTakeover
  ) {

    bullishChecks++;
  }


  if (
    bearishTakeover
  ) {

    bearishChecks++;
  }


  if (
    expansion?.direction ===
      "green"
  ) {

    bullishChecks++;
  }


  if (
    expansion?.direction ===
      "red"
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


  if (
    compressionExpansion?.direction ===
      "green"
  ) {

    bullishChecks += 2;
  }


  if (
    compressionExpansion?.direction ===
      "red"
  ) {

    bearishChecks += 2;
  }


  if (
    reversalCluster?.direction ===
      "green"
  ) {

    bullishChecks += 2;
  }


  if (
    reversalCluster?.direction ===
      "red"
  ) {

    bearishChecks += 2;
  } 
  // ------------------------------------------------
  // ACTION / ENTRY DECISION
  // ------------------------------------------------

  let action =
    "WAIT";

  let reason =
    "No confirmed Oliver entry event.";

  let entryEvent =
    "NONE";


  const bullishEntryEvent =
    bullishTakeover ||
    compressionExpansion?.direction === "green" ||
    reversalCluster?.direction === "green";


  const bearishEntryEvent =
    bearishTakeover ||
    compressionExpansion?.direction === "red" ||
    reversalCluster?.direction === "red";


  if (
    bullishEntryEvent &&
    bullishChecks >= 3 &&
    bullishChecks > bearishChecks
  ) {

    action =
      "CALL_SETUP";

    entryEvent =
      "BULLISH";

    reason =
      "Bullish Oliver entry event confirmed with supporting conditions.";
  }


  if (
    bearishEntryEvent &&
    bearishChecks >= 3 &&
    bearishChecks > bullishChecks
  ) {

    action =
      "PUT_SETUP";

    entryEvent =
      "BEARISH";

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
  // RETURN OLIVER ANALYSIS
  // ------------------------------------------------

  return {

    action,

    reason,

    entryEvent,

    price,

    state,

    structure,

    nearestSMA,

    sma8,

    sma20,

    sma200,

    sma8Direction,

    sma20Direction,

    sma200Direction,

    sma200Context,

    sma200Alignment,

    bullishChecks,

    bearishChecks,

    bullishTakeover,

    bearishTakeover,

    expansion:
      expansion?.direction ||
      "NONE",

    compressionExpansion:
      compressionExpansion
        ? compressionExpansion.direction
        : "NONE",

    reversalCluster:
      reversalCluster
        ? reversalCluster.direction
        : "NONE",

    trigger,

    invalidation,

    candleTime:
      current.time

  };

}


// ==================================================
// HEIKIN-ASHI CONVERSION
// ==================================================

function buildHeikinAshi(
  candles
) {

  if (
    !Array.isArray(candles) ||
    candles.length === 0
  ) {

    return [];
  }


  const result = [];

  let previousHAOpen = null;
  let previousHAClose = null;


  for (
    let i = 0;
    i < candles.length;
    i++
  ) {

    const candle =
      candles[i];


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


    if (
      i === 0 ||
      previousHAOpen === null ||
      previousHAClose === null
    ) {

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


    result.push({

      time:
        candle.time,

      open:
        haOpen,

      high:
        haHigh,

      low:
        haLow,

      close:
        haClose

    });


    previousHAOpen =
      haOpen;

    previousHAClose =
      haClose;
  }


  return result;
}


// ==================================================
// HEIKIN-ASHI CANDLE CLASSIFICATION
// ==================================================

function classifyHeikinAshi(
  candle
) {

  if (!candle) {

    return {
      direction: "NEUTRAL",
      type: "UNKNOWN"
    };
  }


  const open =
    Number(candle.open);

  const close =
    Number(candle.close);

  const high =
    Number(candle.high);

  const low =
    Number(candle.low);


  const body =
    Math.abs(
      close - open
    );


  const range =
    Math.max(
      high - low,
      0.000001
    );


  const upperWick =
    high -
    Math.max(
      open,
      close
    );


  const lowerWick =
    Math.min(
      open,
      close
    ) -
    low;


  const bodyRatio =
    body / range;


  // Small body with wicks on both sides:
  // indecision / possible transition.

  if (
    bodyRatio <= 0.30 &&
    upperWick > 0 &&
    lowerWick > 0
  ) {

    return {

      direction:
        close > open
          ? "GREEN"
          : close < open
            ? "RED"
            : "NEUTRAL",

      type:
        "INDECISION",

      bodyRatio,

      upperWick,

      lowerWick
    };
  }


  // Green HA candle with essentially
  // no lower wick = buyer conviction.

  if (
    close > open &&
    lowerWick <=
      range * 0.05
  ) {

    return {

      direction:
        "GREEN",

      type:
        "BUYERS_STRONG",

      bodyRatio,

      upperWick,

      lowerWick
    };
  }


  // Red HA candle with essentially
  // no upper wick = seller conviction.

  if (
    close < open &&
    upperWick <=
      range * 0.05
  ) {

    return {

      direction:
        "RED",

      type:
        "SELLERS_STRONG",

      bodyRatio,

      upperWick,

      lowerWick
    };
  }


  return {

    direction:
      close > open
        ? "GREEN"
        : close < open
          ? "RED"
          : "NEUTRAL",

    type:
      "STANDARD",

    bodyRatio,

    upperWick,

    lowerWick
  };

} 
// ==================================================
// HEIKIN-ASHI RUN ANALYSIS
// ==================================================

function analyzeHARun(
  haCandles
) {

  if (
    !Array.isArray(haCandles) ||
    haCandles.length === 0
  ) {

    return {
      direction: "NONE",
      count: 0,
      strength: "NONE"
    };
  }


  const last =
    classifyHeikinAshi(
      haCandles[
        haCandles.length - 1
      ]
    );


  if (
    last.direction !== "GREEN" &&
    last.direction !== "RED"
  ) {

    return {
      direction: "NONE",
      count: 0,
      strength: "NONE"
    };
  }


  let count = 0;


  for (
    let i =
      haCandles.length - 1;
    i >= 0;
    i--
  ) {

    const classification =
      classifyHeikinAshi(
        haCandles[i]
      );


    if (
      classification.direction !==
      last.direction
    ) {
      break;
    }


    count++;
  }


  let strength =
    "EARLY";


  if (
    count >= 3
  ) {

    strength =
      "ESTABLISHED";
  }


  if (
    count >= 5
  ) {

    strength =
      "EXTENDED";
  }


  return {

    direction:
      last.direction,

    count,

    strength,

    currentType:
      last.type

  };

}


// ==================================================
// TREND LOCK
// Keeps an established trend alive through ordinary
// pullbacks instead of flipping on every pressure move.
// ==================================================

function applyTrendLock(
  rawDirection,
  strength,
  candleTime
) {

  const normalizedDirection =
    rawDirection === "BULL"
      ? "BULL"
      : rawDirection === "BEAR"
        ? "BEAR"
        : "NEUTRAL";


  // Only update confirmation counters once
  // per completed 2-minute candle.

  const isNewCandle =
    candleTime &&
    candleTime !==
      trendLock.lastProcessedCandle;


  if (isNewCandle) {

    trendLock.lastProcessedCandle =
      candleTime;
  }


  // No established trend yet.
  // Require meaningful directional evidence
  // before creating the first lock.

  if (
    trendLock.direction ===
      "NEUTRAL"
  ) {

    if (
      normalizedDirection === "BULL" &&
      (
        strength === "STRONG" ||
        strength === "DOMINANT"
      )
    ) {

      trendLock.direction =
        "BULL";

      trendLock.oppositeConfirmations =
        0;

    } else if (
      normalizedDirection === "BEAR" &&
      (
        strength === "STRONG" ||
        strength === "DOMINANT"
      )
    ) {

      trendLock.direction =
        "BEAR";

      trendLock.oppositeConfirmations =
        0;
    }


    return {

      establishedTrend:
        trendLock.direction,

      management:
        trendLock.direction ===
          "BULL"
          ? "HOLD_CALL_TREND"
          : trendLock.direction ===
              "BEAR"
            ? "HOLD_PUT_TREND"
            : "WAIT",

      reversalWatch:
        false,

      oppositeConfirmations:
        trendLock.oppositeConfirmations

    };
  }


  // ----------------------------------------------
  // EXISTING BULL TREND
  // ----------------------------------------------

  if (
    trendLock.direction === "BULL"
  ) {

    if (
      normalizedDirection === "BULL"
    ) {

      if (isNewCandle) {

        trendLock.oppositeConfirmations =
          0;
      }


      return {

        establishedTrend:
          "BULL",

        management:
          "HOLD_CALL_TREND",

        reversalWatch:
          false,

        oppositeConfirmations:
          trendLock.oppositeConfirmations

      };
    }


    if (
      normalizedDirection === "BEAR"
    ) {

      if (
        isNewCandle &&
        (
          strength === "STRONG" ||
          strength === "DOMINANT"
        )
      ) {

        trendLock.oppositeConfirmations++;
      }


      if (
        trendLock.oppositeConfirmations >= 2
      ) {

        trendLock.direction =
          "BEAR";

        trendLock.oppositeConfirmations =
          0;


        return {

          establishedTrend:
            "BEAR",

          management:
            "REVERSAL_CONFIRMED_TO_PUT",

          reversalWatch:
            false,

          oppositeConfirmations:
            0

        };
      }


      return {

        establishedTrend:
          "BULL",

        management:
          "HOLD_CALL_REVERSAL_WATCH",

        reversalWatch:
          true,

        oppositeConfirmations:
          trendLock.oppositeConfirmations

      };
    }


    return {

      establishedTrend:
        "BULL",

      management:
        "HOLD_CALL_PULLBACK",

      reversalWatch:
        false,

      oppositeConfirmations:
        trendLock.oppositeConfirmations

    };
  }


  // ----------------------------------------------
  // EXISTING BEAR TREND
  // ----------------------------------------------

  if (
    trendLock.direction === "BEAR"
  ) {

    if (
      normalizedDirection === "BEAR"
    ) {

      if (isNewCandle) {

        trendLock.oppositeConfirmations =
          0;
      }


      return {

        establishedTrend:
          "BEAR",

        management:
          "HOLD_PUT_TREND",

        reversalWatch:
          false,

        oppositeConfirmations:
          trendLock.oppositeConfirmations

      };
    }


    if (
      normalizedDirection === "BULL"
    ) {

      if (
        isNewCandle &&
        (
          strength === "STRONG" ||
          strength === "DOMINANT"
        )
      ) {

        trendLock.oppositeConfirmations++;
      }


      if (
        trendLock.oppositeConfirmations >= 2
      ) {

        trendLock.direction =
          "BULL";

        trendLock.oppositeConfirmations =
          0;


        return {

          establishedTrend:
            "BULL",

          management:
            "REVERSAL_CONFIRMED_TO_CALL",

          reversalWatch:
            false,

          oppositeConfirmations:
            0

        };
      }


      return {

        establishedTrend:
          "BEAR",

        management:
          "HOLD_PUT_REVERSAL_WATCH",

        reversalWatch:
          true,

        oppositeConfirmations:
          trendLock.oppositeConfirmations

      };
    }


    return {

      establishedTrend:
        "BEAR",

      management:
        "HOLD_PUT_PULLBACK",

      reversalWatch:
        false,

      oppositeConfirmations:
        trendLock.oppositeConfirmations

    };
  }


  return {

    establishedTrend:
      "NEUTRAL",

    management:
      "WAIT",

    reversalWatch:
      false,

    oppositeConfirmations:
      0

  };

}
// ==================================================
// TREND BATTLE ENGINE
// ==================================================

function analyzeTrendBattle(
  candles
) {

  if (
    !Array.isArray(candles) ||
    candles.length < 21
  ) {

    return {

      battleState:
        "INSUFFICIENT_DATA",

      pressureDirection:
        "NEUTRAL",

      strength:
        "NONE",

      establishedTrend:
        trendLock.direction,

      management:
        "WAIT",

      reversalWatch:
        false

    };
  }


  const current =
    candles[
      candles.length - 1
    ];


  const oliver =
    analyzeOliver(
      candles
    );


  const haCandles =
    buildHeikinAshi(
      candles
    );


  const haRun =
    analyzeHARun(
      haCandles
    );


  const currentHA =
    classifyHeikinAshi(
      haCandles[
        haCandles.length - 1
      ]
    );


  // ------------------------------------------------
  // PRESSURE SCORE
  // ------------------------------------------------

  let bullScore = 0;
  let bearScore = 0;


  // Oliver market state

  if (
    oliver.state ===
      "BULLISH"
  ) {

    bullScore += 2;
  }


  if (
    oliver.state ===
      "BEARISH"
  ) {

    bearScore += 2;
  }


  // Market structure

  if (
    oliver.structure ===
      "HH_HL"
  ) {

    bullScore += 2;
  }


  if (
    oliver.structure ===
      "LH_LL"
  ) {

    bearScore += 2;
  }


  // Takeover candles

  if (
    oliver.bullishTakeover
  ) {

    bullScore += 2;
  }


  if (
    oliver.bearishTakeover
  ) {

    bearScore += 2;
  }


  // Expansion

  if (
    oliver.expansion ===
      "green"
  ) {

    bullScore += 2;
  }


  if (
    oliver.expansion ===
      "red"
  ) {

    bearScore += 2;
  }


  // Heikin-Ashi current candle

  if (
    currentHA.direction ===
      "GREEN"
  ) {

    bullScore++;
  }


  if (
    currentHA.direction ===
      "RED"
  ) {

    bearScore++;
  }


  if (
    currentHA.type ===
      "BUYERS_STRONG"
  ) {

    bullScore += 2;
  }


  if (
    currentHA.type ===
      "SELLERS_STRONG"
  ) {

    bearScore += 2;
  }


  // Heikin-Ashi consecutive run

  if (
    haRun.direction ===
      "GREEN"
  ) {

    bullScore +=
      Math.min(
        haRun.count,
        3
      );
  }


  if (
    haRun.direction ===
      "RED"
  ) {

    bearScore +=
      Math.min(
        haRun.count,
        3
      );
  }


  // Oliver entry-recognition patterns

  if (
    oliver.compressionExpansion ===
      "green"
  ) {

    bullScore += 2;
  }


  if (
    oliver.compressionExpansion ===
      "red"
  ) {

    bearScore += 2;
  }


  if (
    oliver.reversalCluster ===
      "green"
  ) {

    bullScore += 2;
  }


  if (
    oliver.reversalCluster ===
      "red"
  ) {

    bearScore += 2;
  }


  // 200 SMA alignment is supporting context,
  // not an entry by itself.

  if (
    oliver.sma200Alignment ===
      "ALIGNED" &&
    oliver.state ===
      "BULLISH"
  ) {

    bullScore++;
  }


  if (
    oliver.sma200Alignment ===
      "ALIGNED" &&
    oliver.state ===
      "BEARISH"
  ) {

    bearScore++;
  }


  // ------------------------------------------------
  // RAW PRESSURE DIRECTION
  // ------------------------------------------------

  const scoreDifference =
    bullScore - bearScore;


  let pressureDirection =
    "NEUTRAL";


  if (
    scoreDifference >= 2
  ) {

    pressureDirection =
      "BULL";
  }


  if (
    scoreDifference <= -2
  ) {

    pressureDirection =
      "BEAR";
  }


  // ------------------------------------------------
  // PRESSURE STRENGTH
  // ------------------------------------------------

  const winningScore =
    Math.max(
      bullScore,
      bearScore
    );


  const absoluteDifference =
    Math.abs(
      scoreDifference
    );


  let strength =
    "NEUTRAL";


  if (
    pressureDirection !==
      "NEUTRAL"
  ) {

    strength =
      "EARLY";
  }


  if (
    pressureDirection !==
      "NEUTRAL" &&
    winningScore >= 6 &&
    absoluteDifference >= 4
  ) {

    strength =
      "STRONG";
  }


  if (
    pressureDirection !==
      "NEUTRAL" &&
    winningScore >= 9 &&
    absoluteDifference >= 6
  ) {

    strength =
      "DOMINANT";
  }


  // ------------------------------------------------
  // CURRENT BATTLE STATE
  // ------------------------------------------------

  let battleState =
    "BATTLE_NEUTRAL";


  if (
    pressureDirection ===
      "BULL"
  ) {

    battleState =
      "BULLS_IN_CONTROL";
  }


  if (
    pressureDirection ===
      "BEAR"
  ) {

    battleState =
      "BEARS_IN_CONTROL";
  }


  // ------------------------------------------------
  // APPLY PERSISTENT TREND MEMORY
  // ------------------------------------------------

  const lockedTrend =
    applyTrendLock(
      pressureDirection,
      strength,
      current.time
    );


  // ------------------------------------------------
  // ENTRY STATUS
  //
  // Important:
  // ENTRY describes a developing opportunity.
  // MANAGEMENT describes what to do with an
  // already-established trend.
  // ------------------------------------------------

  let entryStatus =
    "WAIT";


  if (
    pressureDirection === "BULL" &&
    (
      strength === "STRONG" ||
      strength === "DOMINANT"
    )
  ) {

    entryStatus =
      "CALL_ENTRY_ACTIVE";
  }


  if (
    pressureDirection === "BEAR" &&
    (
      strength === "STRONG" ||
      strength === "DOMINANT"
    )
  ) {

    entryStatus =
      "PUT_ENTRY_ACTIVE";
  }


  if (
    pressureDirection === "BULL" &&
    strength === "EARLY"
  ) {

    entryStatus =
      "CALL_ENTRY_DEVELOPING";
  }


  if (
    pressureDirection === "BEAR" &&
    strength === "EARLY"
  ) {

    entryStatus =
      "PUT_ENTRY_DEVELOPING";
  }


  // If current pressure is opposite the locked
  // trend, it is a warning first — not an
  // automatic opposite entry.

  if (
    lockedTrend.establishedTrend ===
      "BULL" &&
    pressureDirection ===
      "BEAR"
  ) {

    entryStatus =
      "BEAR_REVERSAL_WATCH";
  }


  if (
    lockedTrend.establishedTrend ===
      "BEAR" &&
    pressureDirection ===
      "BULL"
  ) {

    entryStatus =
      "BULL_REVERSAL_WATCH";
  }


  // ------------------------------------------------
  // RETURN TREND BATTLE
  // ------------------------------------------------

  return {

    time:
      current.time,

    price:
      Number(
        current.close
      ),

    battleState,

    pressureDirection,

    strength,

    bullScore,

    bearScore,

    scoreDifference,

    entryStatus,

    establishedTrend:
      lockedTrend.establishedTrend,

    management:
      lockedTrend.management,

    reversalWatch:
      lockedTrend.reversalWatch,

    oppositeConfirmations:
      lockedTrend.oppositeConfirmations,

    haDirection:
      currentHA.direction,

    haType:
      currentHA.type,

    haRunDirection:
      haRun.direction,

    haRunCount:
      haRun.count,

    haRunStrength:
      haRun.strength,

    oliver

  };

}
// ==================================================
// TREND EVENT RECORDER
// ==================================================

function recordTrendEvent(
  analysis
) {

  if (
    !analysis ||
    !analysis.time
  ) {
    return;
  }


  const eventKey = [
    analysis.time,
    analysis.battleState,
    analysis.strength,
    analysis.entryStatus,
    analysis.establishedTrend,
    analysis.management
  ].join("|");


  // Avoid recording the exact same event twice.

  if (
    eventKey ===
      lastTrendEventKey
  ) {
    return;
  }


  lastTrendEventKey =
    eventKey;


  const event = {

    time:
      analysis.time,

    price:
      analysis.price,

    battleState:
      analysis.battleState,

    pressureDirection:
      analysis.pressureDirection,

    strength:
      analysis.strength,

    bullScore:
      analysis.bullScore,

    bearScore:
      analysis.bearScore,

    scoreDifference:
      analysis.scoreDifference,

    entryStatus:
      analysis.entryStatus,

    establishedTrend:
      analysis.establishedTrend,

    management:
      analysis.management,

    reversalWatch:
      analysis.reversalWatch,

    oppositeConfirmations:
      analysis.oppositeConfirmations,

    haDirection:
      analysis.haDirection,

    haType:
      analysis.haType,

    haRunDirection:
      analysis.haRunDirection,

    haRunCount:
      analysis.haRunCount,

    haRunStrength:
      analysis.haRunStrength,

    oliverState:
      analysis.oliver?.state ||
      "UNAVAILABLE",

    oliverStructure:
      analysis.oliver?.structure ||
      "UNAVAILABLE",

    sma8:
      analysis.oliver?.sma8 ??
      null,

    sma20:
      analysis.oliver?.sma20 ??
      null,

    sma200:
      analysis.oliver?.sma200 ??
      null,

    bullishChecks:
      analysis.oliver?.bullishChecks ??
      0,

    bearishChecks:
      analysis.oliver?.bearishChecks ??
      0

  };


  trendEventHistory.push(
    event
  );


  if (
    trendEventHistory.length >
    MAX_TREND_EVENTS
  ) {

    trendEventHistory =
      trendEventHistory.slice(
        -MAX_TREND_EVENTS
      );
  }


  console.log(
    "Trend event:",
    JSON.stringify(event)
  );

}


// ==================================================
// CURRENT LIVE ANALYSIS
// ==================================================

function getCurrentAnalysis() {

  if (
    !Array.isArray(
      completedCandles
    ) ||
    completedCandles.length <
      21
  ) {

    return {

      battleState:
        "INSUFFICIENT_DATA",

      pressureDirection:
        "NEUTRAL",

      strength:
        "NONE",

      entryStatus:
        "WAIT",

      establishedTrend:
        trendLock.direction,

      management:
        "WAIT",

      reversalWatch:
        false,

      bullScore: 0,

      bearScore: 0

    };
  }


  return analyzeTrendBattle(
    completedCandles
  );

}


// ==================================================
// MARKET HOURS / CENTRAL TIME
// ==================================================

// U.S. equity regular session:
// 9:30 AM - 4:00 PM Eastern
// 8:30 AM - 3:00 PM Central.
//
// We calculate using America/Chicago so DST is
// handled automatically.

function getCentralTimeParts(
  date = new Date()
) {

  const formatter =
    new Intl.DateTimeFormat(
      "en-US",
      {

        timeZone:
          "America/Chicago",

        year:
          "numeric",

        month:
          "2-digit",

        day:
          "2-digit",

        weekday:
          "short",

        hour:
          "2-digit",

        minute:
          "2-digit",

        second:
          "2-digit",

        hour12:
          false

      }
    );


  const parts =
    formatter.formatToParts(
      date
    );


  const values = {};


  for (
    const part of parts
  ) {

    if (
      part.type !==
        "literal"
    ) {

      values[
        part.type
      ] = part.value;
    }
  }


  return {

    year:
      Number(values.year),

    month:
      Number(values.month),

    day:
      Number(values.day),

    weekday:
      values.weekday,

    hour:
      Number(values.hour),

    minute:
      Number(values.minute),

    second:
      Number(values.second)

  };

}


// ==================================================
// U.S. MARKET HOLIDAYS
// ==================================================

function nthWeekdayOfMonth(
  year,
  month,
  weekday,
  occurrence
) {

  const first =
    new Date(
      Date.UTC(
        year,
        month - 1,
        1
      )
    );


  const offset =
    (
      weekday -
      first.getUTCDay() +
      7
    ) % 7;


  return (
    1 +
    offset +
    (
      occurrence - 1
    ) * 7
  );

}


function lastWeekdayOfMonth(
  year,
  month,
  weekday
) {

  const last =
    new Date(
      Date.UTC(
        year,
        month,
        0
      )
    );


  const offset =
    (
      last.getUTCDay() -
      weekday +
      7
    ) % 7;


  return (
    last.getUTCDate() -
    offset
  );

}


function observedHoliday(
  year,
  month,
  day
) {

  const date =
    new Date(
      Date.UTC(
        year,
        month - 1,
        day
      )
    );


  const weekday =
    date.getUTCDay();


  if (
    weekday === 6
  ) {

    date.setUTCDate(
      date.getUTCDate() - 1
    );
  }


  if (
    weekday === 0
  ) {

    date.setUTCDate(
      date.getUTCDate() + 1
    );
  }
  // ==================================================
// EASTER / GOOD FRIDAY
// ==================================================

function getEasterSunday(
  year
) {

  const a =
    year % 19;

  const b =
    Math.floor(
      year / 100
    );

  const c =
    year % 100;

  const d =
    Math.floor(
      b / 4
    );

  const e =
    b % 4;

  const f =
    Math.floor(
      (b + 8) / 25
    );

  const g =
    Math.floor(
      (b - f + 1) / 3
    );

  const h =
    (
      19 * a +
      b -
      d -
      g +
      15
    ) % 30;

  const i =
    Math.floor(
      c / 4
    );

  const k =
    c % 4;

  const l =
    (
      32 +
      2 * e +
      2 * i -
      h -
      k
    ) % 7;

  const m =
    Math.floor(
      (
        a +
        11 * h +
        22 * l
      ) / 451
    );

  const month =
    Math.floor(
      (
        h +
        l -
        7 * m +
        114
      ) / 31
    );

  const day =
    (
      (
        h +
        l -
        7 * m +
        114
      ) % 31
    ) + 1;


  return new Date(
    Date.UTC(
      year,
      month - 1,
      day
    )
  );

}


function dateKeyFromUTC(
  date
) {

  return [
    date.getUTCFullYear(),

    String(
      date.getUTCMonth() + 1
    ).padStart(
      2,
      "0"
    ),

    String(
      date.getUTCDate()
    ).padStart(
      2,
      "0"
    )

  ].join("-");
}


// ==================================================
// NYSE FULL-DAY HOLIDAYS
// ==================================================

function getMarketHolidays(
  year
) {

  const holidays =
    new Set();


  // New Year's Day

  holidays.add(
    observedHoliday(
      year,
      1,
      1
    )
  );


  // Martin Luther King Jr. Day
  // Third Monday in January

  const mlkDay =
    nthWeekdayOfMonth(
      year,
      1,
      1,
      3
    );


  holidays.add(
    `${year}-01-${String(
      mlkDay
    ).padStart(2, "0")}`
  );


  // Presidents Day
  // Third Monday in February

  const presidentsDay =
    nthWeekdayOfMonth(
      year,
      2,
      1,
      3
    );


  holidays.add(
    `${year}-02-${String(
      presidentsDay
    ).padStart(2, "0")}`
  );


  // Good Friday

  const easter =
    getEasterSunday(
      year
    );


  const goodFriday =
    new Date(
      easter
    );


  goodFriday.setUTCDate(
    goodFriday.getUTCDate() - 2
  );


  holidays.add(
    dateKeyFromUTC(
      goodFriday
    )
  );


  // Memorial Day
  // Last Monday in May

  const memorialDay =
    lastWeekdayOfMonth(
      year,
      5,
      1
    );


  holidays.add(
    `${year}-05-${String(
      memorialDay
    ).padStart(2, "0")}`
  );


  // Juneteenth

  holidays.add(
    observedHoliday(
      year,
      6,
      19
    )
  );


  // Independence Day

  holidays.add(
    observedHoliday(
      year,
      7,
      4
    )
  );


  // Labor Day
  // First Monday in September

  const laborDay =
    nthWeekdayOfMonth(
      year,
      9,
      1,
      1
    );


  holidays.add(
    `${year}-09-${String(
      laborDay
    ).padStart(2, "0")}`
  );


  // Thanksgiving
  // Fourth Thursday in November

  const thanksgiving =
    nthWeekdayOfMonth(
      year,
      11,
      4,
      4
    );


  holidays.add(
    `${year}-11-${String(
      thanksgiving
    ).padStart(2, "0")}`
  );


  // Christmas Day

  holidays.add(
    observedHoliday(
      year,
      12,
      25
    )
  );


  return holidays;
}


// ==================================================
// MARKET CLOCK
// ==================================================

function getMarketClock() {

  const now =
    new Date();


  const central =
    getCentralTimeParts(
      now
    );


  const dateKey =
    [
      central.year,

      String(
        central.month
      ).padStart(
        2,
        "0"
      ),

      String(
        central.day
      ).padStart(
        2,
        "0"
      )

    ].join("-");


  const holidays =
    getMarketHolidays(
      central.year
    );


  const isWeekend =
    central.weekday === "Sat" ||
    central.weekday === "Sun";


  const isHoliday =
    holidays.has(
      dateKey
    );


  const minutesNow =
    central.hour * 60 +
    central.minute;


  const regularOpen =
    8 * 60 + 30;


  const regularClose =
    15 * 60;


  const isRegularSession =
    !isWeekend &&
    !isHoliday &&
    minutesNow >=
      regularOpen &&
    minutesNow <
      regularClose;


  const displayTime =
    new Intl.DateTimeFormat(
      "en-US",
      {

        timeZone:
          "America/Chicago",

        hour:
          "numeric",

        minute:
          "2-digit",

        second:
          "2-digit",

        hour12:
          true

      }
    ).format(now);


  const displayDate =
    new Intl.DateTimeFormat(
      "en-US",
      {

        timeZone:
          "America/Chicago",

        weekday:
          "short",

        month:
          "short",

        day:
          "numeric"

      }
    ).format(now);


  let reason =
    "REGULAR_SESSION";


  if (isWeekend) {

    reason =
      "WEEKEND";

  } else if (isHoliday) {

    reason =
      "MARKET_HOLIDAY";

  } else if (
    minutesNow <
      regularOpen
  ) {

    reason =
      "PRE_MARKET";

  } else if (
    minutesNow >=
      regularClose
  ) {

    reason =
      "AFTER_HOURS";
  }


  return {

    status:
      isRegularSession
        ? "OPEN"
        : "CLOSED",

    isOpen:
      isRegularSession,

    reason,

    timeZone:
      "America/Chicago",

    time:
      displayTime,

    date:
      displayDate,

    dateKey

  };

}
  // ==================================================
// API - HEALTH
// ==================================================

app.get(
  "/",
  (req, res) => {

    res.redirect(
      "/googl-live"
    );

  }
);


// ==================================================
// API - LIVE STATE
// ==================================================

app.get(
  "/api/googl-live",
  (req, res) => {

    const analysis =
      getCurrentAnalysis();


    const marketClock =
      getMarketClock();


    res.json({

      symbol:
        "GOOGL",

      streamStatus:
        alpacaStreamStatus,

      historySeeded,

      completedCandleCount:
        completedCandles.length,

      latestTrade:
        latestGOOGLTrade,

      developingCandle,

      market:
        marketClock,

      analysis,

      trendLock: {

        direction:
          trendLock.direction,

        oppositeConfirmations:
          trendLock.oppositeConfirmations,

        lastProcessedCandle:
          trendLock.lastProcessedCandle

      },

      updatedAt:
        new Date().toISOString()

    });

  }
);


// ==================================================
// API - COMPLETED CANDLES
// ==================================================

app.get(
  "/api/googl-candles",
  (req, res) => {

    res.json({

      symbol:
        "GOOGL",

      count:
        completedCandles.length,

      candles:
        completedCandles

    });

  }
);


// ==================================================
// API - TREND EVENTS
// ==================================================

app.get(
  "/api/trend-events",
  (req, res) => {

    res.json({

      symbol:
        "GOOGL",

      count:
        trendEventHistory.length,

      events:
        trendEventHistory

    });

  }
);


// ==================================================
// API - OLIVER
// ==================================================

app.get(
  "/api/oliver",
  (req, res) => {

    const analysis =
      getCurrentAnalysis();


    res.json({

      symbol:
        "GOOGL",

      oliver:
        analysis.oliver ||
        null,

      establishedTrend:
        analysis.establishedTrend,

      currentPressure:
        analysis.pressureDirection,

      management:
        analysis.management,

      entryStatus:
        analysis.entryStatus

    });

  }
);


// ==================================================
// DASHBOARD
// ==================================================

app.get(
  "/googl-live",
  (req, res) => {

    res.type("html");


    res.send(`
<!DOCTYPE html>

<html>

<head>

<meta
  name="viewport"
  content="width=device-width, initial-scale=1"
/>

<title>
BVB GOOGL Live
</title>


<style>

* {
  box-sizing:
    border-box;
}


body {

  margin: 0;

  padding:
    18px;

  background:
    #0b0f14;

  color:
    #f5f7fa;

  font-family:
    -apple-system,
    BlinkMacSystemFont,
    "Segoe UI",
    sans-serif;

}


.container {

  max-width:
    900px;

  margin:
    0 auto;

}


.header {

  display:
    flex;

  justify-content:
    space-between;

  align-items:
    flex-start;

  gap:
    16px;

  flex-wrap:
    wrap;

  margin-bottom:
    18px;

}


.title {

  font-size:
    25px;

  font-weight:
    800;

}


.subtitle {

  color:
    #9aa4b2;

  margin-top:
    4px;

}


.market-box {

  text-align:
    right;

  padding:
    10px 14px;

  border:
    1px solid #29313c;

  border-radius:
    12px;

  background:
    #121820;

  min-width:
    180px;

}


#clock {

  font-size:
    21px;

  font-weight:
    800;

}


#clockDate {

  color:
    #9aa4b2;

  font-size:
    13px;

  margin-top:
    2px;

}


#marketStatus {

  margin-top:
    7px;

  font-weight:
    800;

}


.market-open {

  color:
    #4ade80;

}


.market-closed {

  color:
    #f87171;

}


.card {

  background:
    #121820;

  border:
    1px solid #29313c;

  border-radius:
    14px;

  padding:
    16px;

  margin-bottom:
    14px;

}


.card-title {

  color:
    #9aa4b2;

  font-size:
    12px;

  font-weight:
    800;

  letter-spacing:
    0.7px;

  text-transform:
    uppercase;

  margin-bottom:
    8px;

}


.big {

  font-size:
    28px;

  font-weight:
    850;

}


.medium {

  font-size:
    19px;

  font-weight:
    750;

}


.grid {

  display:
    grid;

  grid-template-columns:
    repeat(
      2,
      minmax(0, 1fr)
    );

  gap:
    12px;

}


.metric {

  background:
    #0d131a;

  border-radius:
    10px;

  padding:
    11px;

}


.metric-label {

  color:
    #8994a3;

  font-size:
    11px;

  font-weight:
    750;

  text-transform:
    uppercase;

}


.metric-value {

  margin-top:
    5px;

  font-size:
    17px;

  font-weight:
    800;

  overflow-wrap:
    anywhere;

}


.bull {

  color:
    #4ade80;

}


.bear {

  color:
    #f87171;

}


.neutral {

  color:
    #facc15;

}


.warning {

  color:
    #fb923c;

}


.status-line {

  margin-top:
    8px;

  color:
    #aeb7c4;

  line-height:
    1.4;

}


#management {

  margin-top:
    8px;

  font-size:
    20px;

  font-weight:
    850;

}


.small {

  font-size:
    12px;

  color:
    #8994a3;

}


@media (
  max-width: 620px
) {

  body {

    padding:
      10px;

  }


  .grid {

    grid-template-columns:
      1fr;

  }


  .market-box {

    text-align:
      left;

    width:
      100%;

  }

}

</style>

</head>


<body>

<div class="container">


  <div class="header">

    <div>

      <div class="title">
        BVB LIVE TREND BATTLE
      </div>

      <div class="subtitle">
        GOOGL · 2-Minute Live Monitor
      </div>

    </div>


    <div class="market-box">

      <div id="clock">
        --:--:--
      </div>

      <div id="clockDate">
        Central Time
      </div>

      <div
        id="marketStatus"
        class="market-closed"
      >
        MARKET --
      </div>

    </div>

  </div>


  <div class="card">

    <div class="card-title">
      GOOGL
    </div>

    <div
      id="price"
      class="big"
    >
      $---.--
    </div>

    <div
      id="streamStatus"
      class="small"
    >
      Stream: --
    </div>

  </div>


  <div class="card">

    <div class="card-title">
      Established Trend
    </div>

    <div
      id="establishedTrend"
      class="big neutral"
    >
      NEUTRAL
    </div>

    <div
      id="management"
      class="neutral"
    >
      WAIT
    </div>

    <div
      id="reversalMessage"
      class="status-line"
    >
      No reversal warning.
    </div>

  </div>


  <div class="grid">

    <div class="card">

      <div class="card-title">
        Current Pressure
      </div>

      <div
        id="pressure"
        class="medium neutral"
      >
        NEUTRAL
      </div>

    </div>


    <div class="card">

      <div class="card-title">
        Entry Status
      </div>

      <div
        id="entryStatus"
        class="medium neutral"
      >
        WAIT
      </div>

    </div>

  </div>
    <div class="grid">

    <div class="card">

      <div class="card-title">
        Battle State
      </div>

      <div
        id="battleState"
        class="medium neutral"
      >
        BATTLE_NEUTRAL
      </div>

    </div>


    <div class="card">

      <div class="card-title">
        Pressure Strength
      </div>

      <div
        id="strength"
        class="medium neutral"
      >
        NEUTRAL
      </div>

    </div>

  </div>


  <div class="grid">

    <div class="card">

      <div class="card-title">
        Bull Score
      </div>

      <div
        id="bullScore"
        class="big bull"
      >
        0
      </div>

    </div>


    <div class="card">

      <div class="card-title">
        Bear Score
      </div>

      <div
        id="bearScore"
        class="big bear"
      >
        0
      </div>

    </div>

  </div>


  <div class="card">

    <div class="card-title">
      Heikin-Ashi
    </div>


    <div class="grid">

      <div class="metric">

        <div class="metric-label">
          Current HA
        </div>

        <div
          id="haCurrent"
          class="metric-value"
        >
          --
        </div>

      </div>


      <div class="metric">

        <div class="metric-label">
          HA Run
        </div>

        <div
          id="haRun"
          class="metric-value"
        >
          --
        </div>

      </div>

    </div>

  </div>


  <div class="card">

    <div class="card-title">
      Agent Oliver
    </div>


    <div class="grid">

      <div class="metric">

        <div class="metric-label">
          Market State
        </div>

        <div
          id="oliverState"
          class="metric-value"
        >
          --
        </div>

      </div>


      <div class="metric">

        <div class="metric-label">
          Structure
        </div>

        <div
          id="structure"
          class="metric-value"
        >
          --
        </div>

      </div>


      <div class="metric">

        <div class="metric-label">
          8 SMA
        </div>

        <div
          id="sma8"
          class="metric-value"
        >
          --
        </div>

      </div>


      <div class="metric">

        <div class="metric-label">
          20 SMA
        </div>

        <div
          id="sma20"
          class="metric-value"
        >
          --
        </div>

      </div>


      <div class="metric">

        <div class="metric-label">
          200 SMA
        </div>

        <div
          id="sma200"
          class="metric-value"
        >
          --
        </div>

      </div>


      <div class="metric">

        <div class="metric-label">
          200 SMA Context
        </div>

        <div
          id="sma200Context"
          class="metric-value"
        >
          --
        </div>

      </div>

    </div>

  </div>


  <div class="card">

    <div class="card-title">
      Trend Memory
    </div>


    <div class="grid">

      <div class="metric">

        <div class="metric-label">
          Opposite Confirmations
        </div>

        <div
          id="oppositeConfirmations"
          class="metric-value"
        >
          0 / 2
        </div>

      </div>


      <div class="metric">

        <div class="metric-label">
          Completed Candles
        </div>

        <div
          id="completedCandles"
          class="metric-value"
        >
          0
        </div>

      </div>

    </div>

  </div>


</div>


<script>

// ==================================================
// DASHBOARD HELPERS
// ==================================================

function money(
  value
) {

  const number =
    Number(value);


  if (
    !Number.isFinite(number)
  ) {

    return "--";
  }


  return (
    "$" +
    number.toFixed(2)
  );

}


function setDirectionClass(
  element,
  direction
) {

  if (!element) {
    return;
  }


  element.classList.remove(
    "bull",
    "bear",
    "neutral",
    "warning"
  );


  if (
    direction === "BULL" ||
    direction === "BULLISH" ||
    direction === "GREEN"
  ) {

    element.classList.add(
      "bull"
    );

    return;
  }


  if (
    direction === "BEAR" ||
    direction === "BEARISH" ||
    direction === "RED"
  ) {

    element.classList.add(
      "bear"
    );

    return;
  }


  element.classList.add(
    "neutral"
  );

}


// ==================================================
// CLIENT-SIDE CLOCK
// Updates every second without waiting for API refresh.
// ==================================================

function updateLocalClock() {

  const now =
    new Date();


  const time =
    new Intl.DateTimeFormat(
      "en-US",
      {

        timeZone:
          "America/Chicago",

        hour:
          "numeric",

        minute:
          "2-digit",

        second:
          "2-digit",

        hour12:
          true

      }
    ).format(now);


  const date =
    new Intl.DateTimeFormat(
      "en-US",
      {

        timeZone:
          "America/Chicago",

        weekday:
          "short",

        month:
          "short",

        day:
          "numeric"

      }
    ).format(now);


  document.getElementById(
    "clock"
  ).textContent =
    time;


  document.getElementById(
    "clockDate"
  ).textContent =
    date + " · CT";

}


updateLocalClock();


setInterval(
  updateLocalClock,
  1000
);


// ==================================================
// LIVE DASHBOARD REFRESH
// ==================================================

async function refreshDashboard() {

  try {

    const response =
      await fetch(
        "/api/googl-live",
        {
          cache:
            "no-store"
        }
      );


    if (!response.ok) {

      throw new Error(
        "Live API returned " +
        response.status
      );
    }


    const data =
      await response.json();


    const analysis =
      data.analysis || {};


    const oliver =
      analysis.oliver || {};


    const market =
      data.market || {};


    // ----------------------------------------------
    // PRICE
    // ----------------------------------------------

    const livePrice =
      data.latestTrade?.price ??
      data.developingCandle?.close ??
      analysis.price;


    document.getElementById(
      "price"
    ).textContent =
      money(
        livePrice
      );


    document.getElementById(
      "streamStatus"
    ).textContent =
      "Stream: " +
      (
        data.streamStatus ||
        "--"
      );


    // ----------------------------------------------
    // MARKET STATUS
    // ----------------------------------------------

    const marketElement =
      document.getElementById(
        "marketStatus"
      );


    marketElement.textContent =
      "MARKET " +
      (
        market.status ||
        "--"
      );


    marketElement.classList.remove(
      "market-open",
      "market-closed"
    );


    marketElement.classList.add(
      market.isOpen
        ? "market-open"
        : "market-closed"
    );


    // ----------------------------------------------
    // ESTABLISHED TREND
    // ----------------------------------------------

    const establishedTrend =
      analysis.establishedTrend ||
      "NEUTRAL";


    const trendElement =
      document.getElementById(
        "establishedTrend"
      );


    trendElement.textContent =
      establishedTrend;


    setDirectionClass(
      trendElement,
      establishedTrend
    );


    // ----------------------------------------------
    // MANAGEMENT
    // ----------------------------------------------

    const management =
      analysis.management ||
      "WAIT";


    const managementElement =
      document.getElementById(
        "management"
      );


    managementElement.textContent =
      management;


    managementElement.classList.remove(
      "bull",
      "bear",
      "neutral",
      "warning"
    );


    if (
      management.includes(
        "CALL"
      )
    ) {

      managementElement.classList.add(
        "bull"
      );

    } else if (
      management.includes(
        "PUT"
      )
    ) {

      managementElement.classList.add(
        "bear"
      );

    } else {

      managementElement.classList.add(
        "neutral"
      );
    }


    // ----------------------------------------------
    // REVERSAL WATCH
    // ----------------------------------------------

    const reversalElement =
      document.getElementById(
        "reversalMessage"
      );


    if (
      analysis.reversalWatch
    ) {

      reversalElement.textContent =
        "REVERSAL WATCH — opposite pressure is developing. Trend has NOT flipped yet.";

      reversalElement.classList.add(
        "warning"
      );

    } else {

      reversalElement.textContent =
        "No confirmed reversal. Follow established trend management.";

      reversalElement.classList.remove(
        "warning"
      );
    } 


  return [
    date.getUTCFullYear(),
    String(
      date.getUTCMonth() + 1
    ).padStart(2, "0"),
    String(
      date.getUTCDate()
    ).padStart(2, "0")
  ].join("-");

} 
    // ----------------------------------------------
    // CURRENT PRESSURE
    // ----------------------------------------------

    const pressure =
      analysis.pressureDirection ||
      "NEUTRAL";


    const pressureElement =
      document.getElementById(
        "pressure"
      );


    pressureElement.textContent =
      pressure;


    setDirectionClass(
      pressureElement,
      pressure
    );


    // ----------------------------------------------
    // ENTRY STATUS
    // ----------------------------------------------

    const entryStatus =
      analysis.entryStatus ||
      "WAIT";


    const entryElement =
      document.getElementById(
        "entryStatus"
      );


    entryElement.textContent =
      entryStatus;


    if (
      entryStatus.includes(
        "CALL"
      ) ||
      entryStatus.includes(
        "BULL"
      )
    ) {

      setDirectionClass(
        entryElement,
        "BULL"
      );

    } else if (
      entryStatus.includes(
        "PUT"
      ) ||
      entryStatus.includes(
        "BEAR"
      )
    ) {

      setDirectionClass(
        entryElement,
        "BEAR"
      );

    } else {

      setDirectionClass(
        entryElement,
        "NEUTRAL"
      );
    }


    // ----------------------------------------------
    // BATTLE STATE
    // ----------------------------------------------

    const battleState =
      analysis.battleState ||
      "BATTLE_NEUTRAL";


    const battleElement =
      document.getElementById(
        "battleState"
      );


    battleElement.textContent =
      battleState;


    if (
      battleState.includes(
        "BULL"
      )
    ) {

      setDirectionClass(
        battleElement,
        "BULL"
      );

    } else if (
      battleState.includes(
        "BEAR"
      )
    ) {

      setDirectionClass(
        battleElement,
        "BEAR"
      );

    } else {

      setDirectionClass(
        battleElement,
        "NEUTRAL"
      );
    }


    // ----------------------------------------------
    // STRENGTH
    // ----------------------------------------------

    const strengthElement =
      document.getElementById(
        "strength"
      );


    strengthElement.textContent =
      analysis.strength ||
      "NEUTRAL";


    setDirectionClass(
      strengthElement,
      pressure
    );


    // ----------------------------------------------
    // SCORES
    // ----------------------------------------------

    document.getElementById(
      "bullScore"
    ).textContent =
      analysis.bullScore ??
      0;


    document.getElementById(
      "bearScore"
    ).textContent =
      analysis.bearScore ??
      0;


    // ----------------------------------------------
    // HEIKIN-ASHI
    // ----------------------------------------------

    const haCurrent =
      document.getElementById(
        "haCurrent"
      );


    haCurrent.textContent =
      (
        analysis.haDirection ||
        "--"
      ) +
      " · " +
      (
        analysis.haType ||
        "--"
      );


    setDirectionClass(
      haCurrent,
      analysis.haDirection
    );


    const haRun =
      document.getElementById(
        "haRun"
      );


    haRun.textContent =
      (
        analysis.haRunDirection ||
        "--"
      ) +
      " · " +
      (
        analysis.haRunCount ??
        0
      ) +
      " candles · " +
      (
        analysis.haRunStrength ||
        "--"
      );


    setDirectionClass(
      haRun,
      analysis.haRunDirection
    );


    // ----------------------------------------------
    // OLIVER
    // ----------------------------------------------

    const oliverState =
      document.getElementById(
        "oliverState"
      );


    oliverState.textContent =
      oliver.state ||
      "--";


    setDirectionClass(
      oliverState,
      oliver.state
    );


    document.getElementById(
      "structure"
    ).textContent =
      oliver.structure ||
      "--";


    document.getElementById(
      "sma8"
    ).textContent =
      money(
        oliver.sma8
      );


    document.getElementById(
      "sma20"
    ).textContent =
      money(
        oliver.sma20
      );


    document.getElementById(
      "sma200"
    ).textContent =
      money(
        oliver.sma200
      );


    document.getElementById(
      "sma200Context"
    ).textContent =
      oliver.sma200Context ||
      "--";


    // ----------------------------------------------
    // TREND MEMORY
    // ----------------------------------------------

    document.getElementById(
      "oppositeConfirmations"
    ).textContent =
      (
        analysis.oppositeConfirmations ??
        0
      ) +
      " / 2";


    document.getElementById(
      "completedCandles"
    ).textContent =
      data.completedCandleCount ??
      0;


  } catch (error) {

    console.error(
      "Dashboard refresh error:",
      error
    );


    document.getElementById(
      "streamStatus"
    ).textContent =
      "Dashboard connection error";

  }

}


// Initial load

refreshDashboard();


// Refresh market/trading information
// every 2 seconds. The clock itself updates
// every second independently.

setInterval(
  refreshDashboard,
  2000
);

</script>

</body>

</html>
    `);

  }
);


// ==================================================
// STARTUP
// ==================================================

async function startServer() {

  app.listen(
    PORT,
    () => {

      console.log(
        "BVB GOOGL scanner listening on port " +
        PORT
      );

    }
  );


  await seedHistoricalCandles();


  // Run an initial analysis after history
  // has been loaded.

  if (
    completedCandles.length >=
      21
  ) {

    const initialAnalysis =
      analyzeTrendBattle(
        completedCandles
      );


    recordTrendEvent(
      initialAnalysis
    );
  }


  connectAlpacaStream();

}


// ==================================================
// GRACEFUL SHUTDOWN
// ==================================================

function shutdown() {

  console.log(
    "Shutting down BVB scanner..."
  );


  if (
    reconnectTimer
  ) {

    clearTimeout(
      reconnectTimer
    );

    reconnectTimer =
      null;
  }


  if (
    alpacaWS
  ) {

    try {

      alpacaWS.close();

    } catch (_) {}

  }


  process.exit(0);

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
// START
// ==================================================

startServer();
