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

const MAX_COMPLETED_CANDLES = 200;

let historySeeded = false;


// ==================================================
// WEBSOCKET STATE
// ==================================================

let alpacaWS = null;
let reconnectTimer = null;

// Normal disconnect retry
const NORMAL_RECONNECT_DELAY = 5000;

// Longer retry for Alpaca 406.
// This gives the previous Render instance time to shut down.
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
// OLIVER ENGINE V1
// ==================================================
//
// Initial live-analysis engine.
//
// Uses:
// - 8 SMA
// - 20 SMA
// - price location relative to the averages
// - SMA alignment
// - SMA direction
// - short-term HH/HL and LH/LL structure
// - takeover candles
// - pullback -> expansion behavior
//
// 200 SMA intentionally remains separate until
// reliable historical/reference data is supplied.
//
// Output is analytical only.
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
// Used to determine whether an SMA is rising/falling.
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
// CANDLE DIRECTION
// --------------------------------------------------

function candleDirection(candle) {

  if (!candle) {
    return "UNKNOWN";
  }

  if (
    Number(candle.close) >
    Number(candle.open)
  ) {
    return "GREEN";
  }

  if (
    Number(candle.close) <
    Number(candle.open)
  ) {
    return "RED";
  }

  return "DOJI";
}


// --------------------------------------------------
// CANDLE BODY SIZE
// --------------------------------------------------

function candleBody(candle) {

  if (!candle) {
    return 0;
  }

  return Math.abs(
    Number(candle.close) -
    Number(candle.open)
  );
}


// --------------------------------------------------
// BULLISH TAKEOVER
//
// Current green candle overtakes the body/range
// of the previous red candle.
// --------------------------------------------------

function isBullishTakeover(
  previous,
  current
) {

  if (
    !previous ||
    !current
  ) {
    return false;
  }

  const previousDirection =
    candleDirection(previous);

  const currentDirection =
    candleDirection(current);

  if (
    previousDirection !== "RED" ||
    currentDirection !== "GREEN"
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


// --------------------------------------------------
// BEARISH TAKEOVER
// --------------------------------------------------

function isBearishTakeover(
  previous,
  current
) {

  if (
    !previous ||
    !current
  ) {
    return false;
  }

  const previousDirection =
    candleDirection(previous);

  const currentDirection =
    candleDirection(current);

  if (
    previousDirection !== "GREEN" ||
    currentDirection !== "RED"
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
// BASIC SHORT-TERM MARKET STRUCTURE
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
// DETECT EXPANSION CANDLE
//
// Looks for a current candle whose body is
// meaningfully larger than the prior two candles.
// --------------------------------------------------

function detectExpansion(
  candles
) {

  if (
    !Array.isArray(candles) ||
    candles.length < 3
  ) {
    return null;
  }

  const previous2 =
    candles[
      candles.length - 3
    ];

  const previous1 =
    candles[
      candles.length - 2
    ];

  const current =
    candles[
      candles.length - 1
    ];


  const priorAverage =
    (
      candleBody(previous2) +
      candleBody(previous1)
    ) / 2;


  const currentBody =
    candleBody(current);


  if (
    priorAverage <= 0
  ) {
    return null;
  }


  if (
    currentBody >=
    priorAverage * 1.5
  ) {

    return candleDirection(
      current
    );

  }


  return null;
}


// --------------------------------------------------
// OLIVER ANALYSIS
// --------------------------------------------------

function analyzeOliver(
  candles
) {

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
    candles[
      candles.length - 1
    ];

  const previous =
    candles[
      candles.length - 2
    ];


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


  const sma8Direction =
    sma8 > previousSMA8
      ? "RISING"
      : sma8 < previousSMA8
      ? "FALLING"
      : "FLAT";


  const sma20Direction =
    sma20 > previousSMA20
      ? "RISING"
      : sma20 < previousSMA20
      ? "FALLING"
      : "FLAT";


  const structure =
    detectStructure(
      candles
    );


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
    detectExpansion(
      candles
    );


  const price =
    Number(
      current.close
    );


  // -----------------------------------------------
  // STATE
  // -----------------------------------------------

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


  // -----------------------------------------------
  // LOCATION
  // -----------------------------------------------

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


  // -----------------------------------------------
  // SIGNAL SCORING
  //
  // This is NOT a probability.
  // It simply records how many Oliver conditions
  // currently agree.
  // -----------------------------------------------

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
    expansion === "GREEN"
  ) {
    bullishChecks++;
  }


  if (
    expansion === "RED"
  ) {
    bearishChecks++;
  }


  // -----------------------------------------------
  // ACTION
  // -----------------------------------------------

  let action =
    "WAIT";


  let reason =
    "No sufficiently aligned Oliver setup.";


  if (
    bullishChecks >= 3 &&
    bullishChecks >
      bearishChecks
  ) {

    action =
      "CALL_SETUP";


    reason =
      "Bullish Oliver conditions are aligned.";

  }


  if (
    bearishChecks >= 3 &&
    bearishChecks >
      bullishChecks
  ) {

    action =
      "PUT_SETUP";


    reason =
      "Bearish Oliver conditions are aligned.";

  }


  // -----------------------------------------------
  // TRIGGER / INVALIDATION
  // -----------------------------------------------

  let trigger =
    null;


  let invalidation =
    null;


  if (
    action ===
    "CALL_SETUP"
  ) {

    trigger =
      Number(
        current.high
      );


    invalidation =
      Number(
        current.low
      );

  }


  if (
    action ===
    "PUT_SETUP"
  ) {

    trigger =
      Number(
        current.low
      );


    invalidation =
      Number(
        current.high
      );

  }


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
      null,

    sma200Status:
      "NOT_CONFIGURED",

    sma8Direction,

    sma20Direction,

    structure,

    nearestSMA,

    bullishTakeover,

    bearishTakeover,

    expansion,

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


            if (index === 0) {

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
