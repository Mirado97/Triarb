import logging

from src.arbitrage.models import Opportunity, Triangle
from src.exchange.mexc.ws_client import OrderBook

logger = logging.getLogger(__name__)

MIN_PROFIT_PCT = 0.02


def _find_leg(pair_map: dict, from_curr: str, to_curr: str) -> tuple[str, bool] | tuple[None, None]:
    """Находит пару и направление для конвертации from_curr → to_curr.

    buy=True:  платим quote (from_curr), получаем base (to_curr) → amount / ask
    buy=False: платим base (from_curr), получаем quote (to_curr) → amount * bid
    """
    if (to_curr, from_curr) in pair_map:   # base=to, quote=from → BUY
        return pair_map[(to_curr, from_curr)], True
    if (from_curr, to_curr) in pair_map:   # base=from, quote=to → SELL
        return pair_map[(from_curr, to_curr)], False
    return None, None


def build_triangles(symbols: set[str]) -> list[Triangle]:
    known_quotes = {"USDT", "BTC", "ETH", "BNB"}

    pair_map: dict[tuple[str, str], str] = {}
    for sym in symbols:
        for q in sorted(known_quotes, key=len, reverse=True):
            if sym.endswith(q):
                base = sym[: -len(q)]
                if base:
                    pair_map[(base, q)] = sym
                break

    currencies = {c for pair in pair_map for c in pair}
    seen: set[frozenset] = set()
    triangles: list[Triangle] = []

    for a in currencies:
        for b in currencies:
            if b == a:
                continue
            for c in currencies:
                if c in (a, b):
                    continue

                s1, d1 = _find_leg(pair_map, a, b)
                s2, d2 = _find_leg(pair_map, b, c)
                s3, d3 = _find_leg(pair_map, c, a)

                if s1 is None or s2 is None or s3 is None:
                    continue

                # Дедупликация с учётом направления: (пара, buy?) → один ключ на traversal
                key = frozenset(((s1, d1), (s2, d2), (s3, d3)))
                if key in seen:
                    continue
                seen.add(key)

                triangles.append(Triangle(
                    pairs=(s1, s2, s3),
                    directions=(d1, d2, d3),
                ))

    logger.info(f"Built {len(triangles)} triangles from {len(symbols)} symbols")
    return triangles


class ArbitrageEngine:
    def __init__(self, triangles: list[Triangle], on_opportunity=None):
        self._triangles = triangles
        self._books: dict[str, OrderBook] = {}
        self._on_opportunity = on_opportunity

    async def on_book_update(self, book: OrderBook) -> None:
        self._books[book.symbol] = book
        await self._check_all()

    async def _check_all(self) -> None:
        for t in self._triangles:
            if not all(p in self._books for p in t.pairs):
                continue
            opp = self._calc(t)
            if opp and opp.profit_pct > MIN_PROFIT_PCT:
                logger.info(f"OPPORTUNITY: {t.pairs}  profit={opp.profit_pct:.4f}%")
                if self._on_opportunity:
                    await self._on_opportunity(opp)

    def _calc(self, t: Triangle) -> Opportunity | None:
        amount = 1.0
        bids, asks = [], []

        for sym, buy in zip(t.pairs, t.directions):
            book = self._books[sym]
            bids.append(book.bid)
            asks.append(book.ask)

            if book.bid == 0 or book.ask == 0:
                return None

            if buy:
                amount = amount / book.ask   # платим quote, получаем base
            else:
                amount = amount * book.bid   # платим base, получаем quote

        return Opportunity(
            triangle=t,
            profit_pct=(amount - 1.0) * 100,
            bids=tuple(bids),
            asks=tuple(asks),
        )
