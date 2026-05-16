import asyncio
import logging
import time

from src.arbitrage.models import Opportunity, Triangle
from src.exchange.mexc.rest_client import MexcRestClient

logger = logging.getLogger(__name__)

_KNOWN_QUOTES = ["USDT", "USDC", "EUR", "USD1", "USDE", "BTC", "ETH", "BNB"]


def _parse_pair(symbol: str) -> tuple[str, str]:
    for q in sorted(_KNOWN_QUOTES, key=len, reverse=True):
        if symbol.endswith(q):
            return symbol[: -len(q)], q
    raise ValueError(f"Cannot parse pair symbol: {symbol}")


def _start_currency(triangle: Triangle) -> str:
    """Currency we need in our wallet to start this triangle."""
    sym, direction = triangle.pairs[0], triangle.directions[0]
    base, quote = _parse_pair(sym)
    # direction=True (BUY): spend quote → start currency is quote
    # direction=False (SELL): spend base → start currency is base
    return quote if direction else base


class ExecutionEngine:
    def __init__(
        self,
        client: MexcRestClient,
        trade_amount: float,
        min_profit_pct: float,
        cooldown_sec: float = 5.0,
    ):
        self._client = client
        self._trade_amount = trade_amount
        self._min_profit_pct = min_profit_pct
        self._cooldown = cooldown_sec
        self._lock = asyncio.Lock()
        self._last_exec = 0.0
        self.total_profit = 0.0
        self.cycles = 0

    async def on_opportunity(self, opp: Opportunity) -> None:
        if opp.profit_pct < self._min_profit_pct:
            return
        if self._lock.locked():
            return
        if time.time() - self._last_exec < self._cooldown:
            return

        async with self._lock:
            self._last_exec = time.time()
            await self._execute(opp)

    async def _execute(self, opp: Opportunity) -> None:
        triangle = opp.triangle
        start_curr = _start_currency(triangle)

        try:
            balances = await self._client.get_balances()
        except Exception as e:
            logger.error(f"Balance fetch failed: {e}")
            return

        available = balances.get(start_curr, 0.0)
        amount = min(self._trade_amount, available * 0.99)

        if amount < 1.0:
            logger.warning(
                f"Skip {triangle.pairs}: insufficient {start_curr} "
                f"(have {available:.4f}, need ≥1.0)"
            )
            return

        logger.info(
            f">>> EXEC {triangle.pairs}  expected={opp.profit_pct:.4f}%  "
            f"{start_curr}={amount:.4f}"
        )

        current = amount

        for i, (sym, direction) in enumerate(zip(triangle.pairs, triangle.directions)):
            try:
                if direction:
                    # BUY: spend current (quote), receive base
                    result = await self._client.market_buy_quote(sym, current)
                    current = float(result.get("executedQty", 0))
                else:
                    # SELL: spend current (base), receive quote
                    result = await self._client.market_sell(sym, current)
                    current = float(result.get("cummulativeQuoteQty", 0))

                if current <= 0:
                    raise RuntimeError(f"Leg {i+1} returned zero amount")

                logger.info(f"  Leg {i+1}/{len(triangle.pairs)} {sym} → {current:.6f}")

            except Exception as e:
                logger.error(f"  Leg {i+1} {sym} FAILED: {e}")
                if i > 0:
                    logger.warning(
                        f"  PARTIAL EXECUTION after {i} leg(s) — "
                        f"check balance manually: {start_curr}"
                    )
                return

        profit = current - amount
        pct = profit / amount * 100
        self.total_profit += profit
        self.cycles += 1

        logger.info(
            f"<<< DONE cycle #{self.cycles}  "
            f"in={amount:.4f} out={current:.4f} {start_curr}  "
            f"profit={profit:+.4f} ({pct:+.4f}%)  "
            f"cumulative={self.total_profit:+.4f}"
        )
