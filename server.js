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
// ADD COMPLETED CANDLE TO ROLLING BUFFER
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


  // If the candle already exists, replace it instead
  // of creating a duplicate.
  const existingIndex =
    completedCandles.findIndex(
      (bar) => bar.time === normalized.time
    );


  if (existingIndex !== -1) {

    completedCandles[existingIndex] =
      normalized;

  } else {

    completedCandles.push(
      normalized
    );

  }


  // Keep candles in chronological order.
  completedCandles.sort(
    (a, b) =>
      new Date(a.time) -
      new Date(b.time)
  );


  // Keep only the most recent 200.
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


  // Create the correct 2-minute time bucket.
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


    // The previous developing candle has now closed.
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


    // Start the new candle.
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


  // -----------------------------------------------
  // UPDATE CURRENT 2-MINUTE CANDLE
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

        reconnectTimer = null;

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


  // Prevent this Render process from opening another
  // socket when one is already active.
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


  alpacaWS = ws;


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


      if (
        !Array.isArray(messages)
      ) {

        messages = [messages];

      }


      for (
        const message of messages
      ) {


        // Avoid printing every trade to Render logs.
        // Status/error messages are still logged.
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
        // AUTHENTICATION SUCCESS
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


          // 406 = another WebSocket connection
          // is consuming the account allowance.
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
        // LIVE GOOGL TRADE
        // ------------------------------------------

        if (
          message.T === "t" &&
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
  // SOCKET ERROR
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
  // SOCKET CLOSED
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

        alpacaWS = null;

      }


      // Do not reconnect after Alpaca 406.
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

// Seed the rolling buffer first.
seedHistoricalCandles();

// Start live WebSocket.
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
