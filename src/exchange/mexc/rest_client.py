import hashlib
import hmac
import logging
import math
import time
import urllib.parse

import aiohttp

BASE_URL = "https://api.mexc.com"
logger = logging.getLogger(__name__)


def _round_step(qty: float, step: float) -> float:
    """Round qty down to the nearest step increment."""
    if step <= 0:
        return qty
    precision = max(0, -int(math.floor(math.log10(step))))
    return round(math.floor(qty / step) * step, precision)


class MexcRestClient:
    def __init__(self, api_key: str, api_secret: str):
        self._key = api_key
        self._secret = api_secret
        self._session: aiohttp.ClientSession | None = None
        self._step_sizes: dict[str, float] = {}

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(
            headers={"X-MEXC-APIKEY": self._key, "Content-Type": "application/json"},
        )

    async def stop(self) -> None:
        if self._session:
            await self._session.close()

    def _sign(self, params: dict) -> str:
        params["timestamp"] = int(time.time() * 1000)
        query = urllib.parse.urlencode(params)
        sig = hmac.new(self._secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return query + "&signature=" + sig

    async def load_lot_sizes(self, symbols: list[str]) -> None:
        """Fetch and cache lot size step for each symbol."""
        sym_set = set(symbols)
        async with self._session.get(f"{BASE_URL}/api/v3/exchangeInfo") as r:
            data = await r.json()
        for s in data.get("symbols", []):
            if s["symbol"] not in sym_set:
                continue
            for f in s.get("filters", []):
                if f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
                    self._step_sizes[s["symbol"]] = step if step > 0 else 1e-8
                    break
        logger.info(f"Loaded lot sizes for {len(self._step_sizes)} symbols")

    async def get_balances(self) -> dict[str, float]:
        """Returns {asset: free_balance} for non-zero balances."""
        signed = self._sign({})
        async with self._session.get(f"{BASE_URL}/api/v3/account?{signed}") as r:
            data = await r.json()
        if "balances" not in data:
            raise RuntimeError(f"get_balances error: {data}")
        return {b["asset"]: float(b["free"]) for b in data["balances"] if float(b["free"]) > 0}

    async def market_buy_quote(self, symbol: str, quote_qty: float) -> dict:
        """BUY: spend quote_qty of quote currency, receive base.
        Returns order dict with executedQty (base received)."""
        params = {
            "symbol": symbol,
            "side": "BUY",
            "type": "MARKET",
            "quoteOrderQty": f"{quote_qty:.8f}",
        }
        signed = self._sign(params)
        async with self._session.post(f"{BASE_URL}/api/v3/order?{signed}") as r:
            data = await r.json()
        if "orderId" not in data:
            raise RuntimeError(f"market_buy_quote {symbol} failed: {data}")
        logger.debug(f"BUY {symbol} quoteQty={quote_qty:.4f} → executedQty={data.get('executedQty')}")
        return data

    async def ping_ms(self) -> float:
        """Measure REST round-trip to MEXC."""
        t0 = time.time()
        async with self._session.get(f"{BASE_URL}/api/v3/ping") as r:
            await r.read()
        return (time.time() - t0) * 1000

    async def market_sell(self, symbol: str, base_qty: float) -> dict:
        """SELL: sell base_qty of base currency, receive quote.
        Returns order dict with cummulativeQuoteQty (quote received)."""
        step = self._step_sizes.get(symbol, 1e-8)
        base_qty = _round_step(base_qty, step)
        if base_qty <= 0:
            raise RuntimeError(f"market_sell {symbol}: qty rounded to zero (step={step})")
        params = {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "quantity": f"{base_qty:.8f}",
        }
        signed = self._sign(params)
        async with self._session.post(f"{BASE_URL}/api/v3/order?{signed}") as r:
            data = await r.json()
        if "orderId" not in data:
            raise RuntimeError(f"market_sell {symbol} failed: {data}")
        logger.debug(f"SELL {symbol} qty={base_qty:.8f} → quoteQty={data.get('cummulativeQuoteQty')}")
        return data
