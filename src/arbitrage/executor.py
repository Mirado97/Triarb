import asyncio
import logging
import time
from typing import Awaitable, Callable

from src.arbitrage.models import Opportunity, Triangle
from src.exchange.mexc.rest_client import MexcRestClient

logger = logging.getLogger(__name__)

_KNOWN_QUOTES = ["USDT", "USDC", "EUR", "USD1", "USDE", "BTC", "ETH", "BNB"]
Broadcast = Callable[[dict], Awaitable[None]]


def _parse_pair(symbol: str) -> tuple[str, str]:
    for q in sorted(_KNOWN_QUOTES, key=len, reverse=True):
        if symbol.endswith(q):
            return symbol[: -len(q)], q
    raise ValueError(f"Cannot parse pair symbol: {symbol}")


def _start_currency(triangle: Triangle) -> str:
    sym, direction = triangle.pairs[0], triangle.directions[0]
    base, quote = _parse_pair(sym)
    return quote if direction else base


def _currency_after_leg(triangle: Triangle, leg_idx: int) -> str:
    """Currency we hold after completing leg at leg_idx (0-based)."""
    sym, direction = triangle.pairs[leg_idx], triangle.directions[leg_idx]
    base, quote = _parse_pair(sym)
    return base if direction else quote


class ExecutionEngine:
    def __init__(
        self,
        client: MexcRestClient,
        trade_amount: float,
        min_profit_pct: float,
        cooldown_sec: float = 5.0,
        broadcast: Broadcast | None = None,
    ):
        self._client = client
        self._trade_amount = trade_amount
        self._min_profit_pct = min_profit_pct
        self._cooldown = cooldown_sec
        self._lock = asyncio.Lock()
        self._last_exec = 0.0
        self._broadcast = broadcast
        self._start_ts = time.time()
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

    async def _emit(self, msg: dict) -> None:
        if self._broadcast:
            try:
                await self._broadcast(msg)
            except Exception:
                pass

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
                f"Skip {triangle.pairs}: low {start_curr} "
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
                    result = await self._client.market_buy_quote(sym, current)
                    current = float(result.get("executedQty", 0))
                else:
                    result = await self._client.market_sell(sym, current)
                    current = float(result.get("cummulativeQuoteQty", 0))

                if current <= 0:
                    raise RuntimeError("Returned zero amount")

                logger.info(f"  Leg {i+1}/{len(triangle.pairs)} {sym} → {current:.6f}")

            except Exception as e:
                logger.error(f"  Leg {i+1} {sym} FAILED: {e}")
                if i > 0:
                    stuck_curr = _currency_after_leg(triangle, i - 1)
                    logger.warning(
                        f"  STUCK: {current:.6f} {stuck_curr} after {i} leg(s)"
                    )
                    await self._emit({
                        "type": "stuck",
                        "data": {
                            "pairs": list(triangle.pairs),
                            "completed_legs": i,
                            "stuck_currency": stuck_curr,
                            "stuck_amount": current,
                            "ts": int(time.time()),
                            "reason": str(e),
                        },
                    })
                return

        profit = current - amount
        pct = profit / amount * 100
        self.total_profit += profit
        self.cycles += 1

        logger.info(
            f"<<< DONE #{self.cycles}  in={amount:.4f} out={current:.4f} "
            f"{start_curr}  profit={profit:+.4f} ({pct:+.4f}%)  "
            f"total={self.total_profit:+.4f}"
        )

        await self._emit({
            "type": "trade",
            "data": {
                "id": self.cycles,
                "pairs": list(triangle.pairs),
                "profit_pct": pct,
                "profit_abs": profit,
                "start_amount": amount,
                "start_currency": start_curr,
                "ts": int(time.time()),
            },
        })
        await self._emit({
            "type": "stats",
            "data": {
                "cycles": self.cycles,
                "total_profit": self.total_profit,
                "start_ts": self._start_ts,
            },
        })
