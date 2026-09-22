const express = require("express");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

const ALPACA_API_KEY = process.env.ALPACA_API_KEY;
const ALPACA_SECRET_KEY = process.env.ALPACA_SECRET_KEY;
console.log("API key loaded:", Boolean(ALPACA_API_KEY));
console.log("Secret key loaded:", Boolean(ALPACA_SECRET_KEY));

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
      "https://data.alpaca.markets/v2/stocks/GOOGL/bars?timeframe=1Min&limit=10&feed=iex";

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

app.listen(PORT, "0.0.0.0", () => {
  console.log(`BVB Trading Assistant running on port ${PORT}`);
});
