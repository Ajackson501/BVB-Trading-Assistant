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

// Rolling history of completed 2-minute candles.
let completedCandles = [];

const MAX_COMPLETED_CANDLES = 200;

let historySeeded = false;


// ==================================================
// WEBSOCKET STATE
// ==================================================

let alpacaWS = null;
let reconnectTimer = null;
let reconnectEnabled = true;


// ==================================================
// BASIC HELPERS
// ==================================================

function round(value, decimals = 4) {

  if (!Number.isFinite(value)) {
    return null;
  }

  return Number(
    value.toFixed(decimals)
  );
}


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
// SMA CALCULATION
// ==================================================

function calculateSMA(
  candles,
  period
) {

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


// ==================================================
// SMA VALUE AT A PARTICULAR POINT IN HISTORY
// ==================================================

function calculateSMAAtIndex(
  candles,
  period,
  index
) {

  if (
    index < period - 1
  ) {

    return null;
  }


  const start =
    index - period + 1;


  const selected =
    candles.slice(
      start,
      index + 1
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
// OLIVER 8 / 20 SMA ANALYSIS
// ==================================================

function buildOliverAnalysis() {

  if (
    completedCandles.length < 20
  ) {

    return {

      ready: false,

      reason:
        "At least 20 completed candles are required.",

      candleCount:
        completedCandles.length

    };
  }


  const lastIndex =
    completedCandles.length - 1;


  const previousIndex =
    lastIndex - 1;


  const latestCandle =
    completedCandles[lastIndex];


  const previousCandle =
    completedCandles[previousIndex];


  const sma8 =
    calculateSMA(
      completedCandles,
      8
    );


  const sma20 =
    calculateSMA(
      completedCandles,
      20
    );


  const previousSMA8 =
    calculateSMAAtIndex(
      completedCandles,
      8,
      previousIndex
    );


  const previousSMA20 =
    calculateSMAAtIndex(
      completedCandles,
      20,
      previousIndex
    );


  // -----------------------------------------------
  // SMA DIRECTION
  // -----------------------------------------------

  let sma8Direction =
    "FLAT";


  if (
    sma8 > previousSMA8
  ) {

    sma8Direction =
      "RISING";

  } else if (
    sma8 < previousSMA8
  ) {

    sma8Direction =
      "FALLING";

  }


  let sma20Direction =
    "FLAT";


  if (
    sma20 > previousSMA20
  ) {

    sma20Direction =
      "RISING";

  } else if (
    sma20 < previousSMA20
  ) {

    sma20Direction =
      "FALLING";

  }


  // -----------------------------------------------
  // PRICE LOCATION
  // -----------------------------------------------

  let priceLocation =
    "BETWEEN_8_AND_20";


  if (
    latestCandle.close >
      Math.max(
        sma8,
        sma20
      )
  ) {

    priceLocation =
      "ABOVE_8_AND_20";

  } else if (
    latestCandle.close <
      Math.min(
        sma8,
        sma20
      )
  ) {

    priceLocation =
      "BELOW_8_AND_20";

  }


  // -----------------------------------------------
  // BASIC TREND STATE
  // -----------------------------------------------

  let trendState =
    "MIXED";


  if (
    latestCandle.close > sma8 &&
    sma8 > sma20 &&
    sma8Direction === "RISING" &&
    sma20Direction === "RISING"
  ) {

    trendState =
      "BULLISH";

  }


  if (
    latestCandle.close < sma8 &&
    sma8 < sma20 &&
    sma8Direction === "FALLING" &&
    sma20Direction === "FALLING"
  ) {

    trendState =
      "BEARISH";

  }


  // -----------------------------------------------
  // CANDLE DIRECTION
  // -----------------------------------------------

  const latestBullish =
    latestCandle.close >
    latestCandle.open;


  const latestBearish =
    latestCandle.close <
    latestCandle.open;


  const previousBullish =
    previousCandle.close >
    previousCandle.open;


  const previousBearish =
    previousCandle.close <
    previousCandle.open;


  // -----------------------------------------------
  // OLIVER TAKEOVER CANDLE
  //
  // Bullish:
  // Current green candle takes out previous red body.
  //
  // Bearish:
  // Current red candle takes out previous green body.
  // -----------------------------------------------

  const bullishTakeover =
    previousBearish &&
    latestBullish &&
    latestCandle.close >
      previousCandle.open;


  const bearishTakeover =
    previousBullish &&
    latestBearish &&
    latestCandle.close <
      previousCandle.open;


  // -----------------------------------------------
  // PROXIMITY TO 8 / 20 SMA AREA
  //
  // We don't want to pretend that every takeover
  // candle matters.
  //
  // This identifies whether the latest candle
  // interacted with the 8 or 20 SMA area.
  // -----------------------------------------------

  const touched8 =
    latestCandle.low <= sma8 &&
    latestCandle.high >= sma8;


  const touched20 =
    latestCandle.low <= sma20 &&
    latestCandle.high >= sma20;


  const nearOliverSMAZone =
    touched8 ||
    touched20;


  // -----------------------------------------------
  // INITIAL OLIVER EVENT
  // -----------------------------------------------

  let event =
    "NONE";


  if (
    bullishTakeover &&
    nearOliverSMAZone
  ) {

    event =
      "BULLISH_TAKEOVER_NEAR_SMA";

  }


  if (
    bearishTakeover &&
    nearOliverSMAZone
  ) {

    event =
      "BEARISH_TAKEOVER_NEAR_SMA";

  }


  // -----------------------------------------------
  // INITIAL BIAS
  //
  // IMPORTANT:
  // This is NOT an automatic trade signal.
  // -----------------------------------------------

  let bias =
    "WAIT";


  if (
    trendState === "BULLISH"
  ) {

    bias =
      "CALL_BIAS";

  }


  if (
    trendState === "BEARISH"
  ) {

    bias =
      "PUT_BIAS";

  }


  return {

    ready: true,

    symbol:
      "GOOGL",

    timeframe:
      "2Min",

    candleTime:
      latestCandle.time,

    candleCount:
      completedCandles.length,

    latestCandle,

    sma: {

      sma8:
        round(sma8),

      sma20:
        round(sma20),

      sma8Direction,

      sma20Direction,

      spread:
        round(
          sma8 - sma20
        )

    },

    state: {

      trend:
        trendState,

      priceLocation,

      bias

    },

    event: {

      type:
        event,

      bullishTakeover,

      bearishTakeover,

      touched8SMA:
        touched8,

      touched20SMA:
        touched20,

      nearOliverSMAZone

    },

    note:
      "Observation only. Bias is not an automatic trade entry."

  };
}


// ==================================================
// ADD COMPLETED CANDLE TO ROLLING BUFFER
// ==================================================

function addCompletedCandle(candle) {

  if (
    !candle ||
    !candle.time
  ) {

    return;
  }


  const normalized = {

    time:
      candle.time,

    open:
      Number(candle.open),

    high:
      Number(candle.high),

    low:
      Number(candle.low),

    close:
      Number(candle.close),

    volume:
      Number(candle.volume) || 0

  };


  const existingIndex =
    completedCandles.findIndex(
      (bar) =>
        bar.time ===
        normalized.time
    );


  if (
    existingIndex !== -1
  ) {

    completedCandles[
      existingIndex
    ] = normalized;

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


    if (
      !response.ok
    ) {

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


  // -----------------------------------------------
  // NEW 2-MINUTE PERIOD
  // -----------------------------------------------

  if (
    !developingCandle ||
    developingCandle.time !==
      bucketTime
  ) {


    if (
      developingCandle
    ) {

      addCompletedCandle(
        developingCandle
      );


      console.log(
        "Completed 2-minute candle:",
        JSON.stringify(
          developingCandle
        )
      );


      // Print Oliver's current interpretation
      // whenever a candle closes.
      const analysis =
        buildOliverAnalysis();


      if (
        analysis.ready
      ) {

        console.log(
          "OLIVER:",
          JSON.stringify({
            time:
              analysis.candleTime,

            trend:
              analysis.state.trend,

            bias:
              analysis.state.bias,

            sma8:
              analysis.sma.sma8,

            sma20:
              analysis.sma.sma20,

            event:
              analysis.event.type
          })
        );

      }

    }


    developingCandle = {

      time:
        bucketTime,

      open:
        price,

      high:
        price,

      low:
        price,

      close:
        price,

      volume:
        size

    };


    return;
  }


  // -----------------------------------------------
  // UPDATE CURRENT CANDLE
  // -----------------------------------------------

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
// WEBSOCKET RECONNECT CONTROL
// ==================================================

function scheduleReconnect() {

  if (
    !reconnectEnabled ||
    reconnectTimer
  ) {

    return;
  }


  alpacaStreamStatus =
    "reconnecting";


  reconnectTimer =
    setTimeout(
      () => {

        reconnectTimer =
          null;

        connectAlpacaStream();

      },
      5000
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


  const ws =
    new WebSocket(
      "wss://stream.data.alpaca.markets/v2/iex"
    );


  alpacaWS =
    ws;


  // ------------------------------------------------
  // OPEN
  // ------------------------------------------------

  ws.on(
    "open",
    () => {

      console.log(
        "Alpaca WebSocket connected"
      );


      ws.send(
        JSON.stringify({

          action:
            "auth",

          key:
            ALPACA_API_KEY,

          secret:
            ALPACA_SECRET_KEY

        })
      );

    }
  );


  // ------------------------------------------------
  // MESSAGE
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


      if (
        !Array.isArray(
          messages
        )
      ) {

        messages =
          [messages];

      }


      for (
        const message of messages
      ) {


        if (
          message.T !== "t"
        ) {

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
        // ERROR
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


          if (
            Number(
              message.code
            ) === 406 ||

            String(
              message.msg || ""
            )
              .toLowerCase()
              .includes(
                "connection limit"
              )
          ) {

            reconnectEnabled =
              false;


            alpacaStreamStatus =
              "connection_limit_exceeded";


            console.error(
              "Alpaca connection limit exceeded. Automatic reconnect disabled."
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
        // GOOGL TRADE
        // ------------------------------------------

        if (
          message.T ===
            "t" &&

          message.S ===
            "GOOGL"
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
  // ERROR
  // ------------------------------------------------

  ws.on(
    "error",
    (error) => {

      console.error(
        "Alpaca WebSocket error:",
        error.message
      );


      if (
        alpacaStreamStatus !==
        "connection_limit_exceeded"
      ) {

        alpacaStreamStatus =
          "error";

      }

    }
  );


  // ------------------------------------------------
  // CLOSE
  // ------------------------------------------------

  ws.on(
    "close",
    () => {

      console.log(
        "Alpaca WebSocket disconnected"
      );


      if (
        alpacaWS === ws
      ) {

        alpacaWS =
          null;

      }


      if (
        !reconnectEnabled
      ) {

        console.log(
          "Automatic Alpaca reconnect is disabled."
        );

        return;
      }


      alpacaStreamStatus =
        "disconnected";


      scheduleReconnect();

    }
  );
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

      historySeeded,

      completedCandleCount:
        completedCandles.length,

      latestTrade:
        latestGOOGLTrade,

      developing2MinCandle:
        developingCandle,

      completedCandles

    });

  }
);


// ==================================================
// OLIVER ENDPOINT
// ==================================================

app.get(
  "/googl-oliver",
  (req, res) => {

    res.json(
      buildOliverAnalysis()
    );

  }
);


// ==================================================
// COMPLETED CANDLE HISTORY ENDPOINT
// ==================================================

app.get(
  "/googl-history",
  (req, res) => {

    res.json({

      symbol:
        "GOOGL",

      timeframe:
        "2Min",

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

      historySeeded,

      completedCandleCount:
        completedCandles.length,

      oliverReady:
        completedCandles.length >= 20

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


      if (
        !response.ok
      ) {

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


      if (
        !response.ok
      ) {

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
                round(
                  haOpen
                ),

              high:
                round(
                  haHigh
                ),

              low:
                round(
                  haLow
                ),

              close:
                round(
                  haClose
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
