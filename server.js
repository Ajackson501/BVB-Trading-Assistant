const express = require("express");
const WebSocket = require("ws");
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const ALPACA_API_KEY = process.env.ALPACA_API_KEY;
const ALPACA_SECRET_KEY = process.env.ALPACA_SECRET_KEY;
console.log("API key loaded:", Boolean(ALPACA_API_KEY));
console.log("Secret key loaded:", Boolean(ALPACA_SECRET_KEY));
let latestGOOGLTrade = null;
let alpacaStreamStatus = "connecting";

function connectAlpacaStream() {
  const ws = new WebSocket("wss://stream.data.alpaca.markets/v2/iex");

  ws.on("open", () => {
    console.log("Alpaca WebSocket connected");

    ws.send(JSON.stringify({
      action: "auth",
      key: ALPACA_API_KEY,
      secret: ALPACA_SECRET_KEY
    }));
  });

  ws.on("message", (data) => {
    const messages = JSON.parse(data.toString());

    for (const message of messages) {
      if (message.T === "success" && message.msg === "authenticated") {
        alpacaStreamStatus = "connected";

        ws.send(JSON.stringify({
          action: "subscribe",
          trades: ["GOOGL"]
        }));

        console.log("Subscribed to live GOOGL trades");
      }

      if (message.T === "t" && message.S === "GOOGL") {
        latestGOOGLTrade = {
          price: message.p,
          size: message.s,
          time: message.t
        };
      }
    }
  });

  ws.on("error", (error) => {
    console.error("Alpaca WebSocket error:", error.message);
    alpacaStreamStatus = "error";
  });

  ws.on("close", () => {
    console.log("Alpaca WebSocket disconnected");
    alpacaStreamStatus = "disconnected";

    setTimeout(connectAlpacaStream, 5000);
  });
}

connectAlpacaStream();
app.get("/googl-live", (req, res) => {
  res.json({
    symbol: "GOOGL",
    streamStatus: alpacaStreamStatus,
    latestTrade: latestGOOGLTrade
  });
});
app.get("/", (req, res) => {
  res.json({
    status: "online",
    service: "BVB Trading Assistant",
    alpacaConfigured: Boolean(ALPACA_API_KEY && ALPACA_SECRET_KEY)
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "healthy" });
});

app.get("/googl", async (req, res) => {
  try {
    if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY) {
      return res.status(500).json({
        error: "Alpaca API credentials are not configured"
      });
    }

    const url =
      "https://data.alpaca.markets/v2/stocks/GOOGL/bars?timeframe=2Min&limit=10&feed=iex";

    const response = await fetch(url, {
      headers: {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
      }
    });

const text = await response.text();

console.log("Alpaca status:", response.status);
console.log("Alpaca response:", text);

if (!response.ok) {
  return res.status(response.status).send(text);
}

res.type("application/json").send(text);

  } catch (error) {
    console.error("Alpaca error:", error);
    res.status(500).json({
      error: "Unable to retrieve GOOGL data"
    });
  }
});
app.get("/googl-ha", async (req, res) => {
  try {
    const url =
      "https://data.alpaca.markets/v2/stocks/GOOGL/bars?timeframe=2Min&limit=200&feed=iex";

    const response = await fetch(url, {
      headers: {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
      }
    });

    const data = await response.json();

    if (!response.ok) {
      return res.status(response.status).json(data);
    }

    const bars = data.bars || [];

    let previousHAOpen = null;
    let previousHAClose = null;

    const heikinAshi = bars.map((bar, index) => {
      const haClose = (bar.o + bar.h + bar.l + bar.c) / 4;

      let haOpen;

      if (index === 0) {
        haOpen = (bar.o + bar.c) / 2;
      } else {
        haOpen = (previousHAOpen + previousHAClose) / 2;
      }

      const haHigh = Math.max(bar.h, haOpen, haClose);
      const haLow = Math.min(bar.l, haOpen, haClose);

      previousHAOpen = haOpen;
      previousHAClose = haClose;

      return {
        time: bar.t,
        open: Number(haOpen.toFixed(4)),
        high: Number(haHigh.toFixed(4)),
        low: Number(haLow.toFixed(4)),
        close: Number(haClose.toFixed(4)),
        color: haClose >= haOpen ? "GREEN" : "RED"
      };
    });

    res.json({
      symbol: "GOOGL",
      timeframe: "2Min",
      candles: heikinAshi
    });

  } catch (error) {
    console.error("Heikin-Ashi error:", error);

    res.status(500).json({
      error: "Unable to calculate Heikin-Ashi data"
    });
  }
});
app.listen(PORT, "0.0.0.0", () => {
  console.log(`BVB Trading Assistant running on port ${PORT}`);
});
