const express = require("express");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get("/", (req, res) => {
  res.json({
    status: "online",
    service: "BVB Trading Assistant"
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "healthy" });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`BVB Trading Assistant running on port ${PORT}`);
});
