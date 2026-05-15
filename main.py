import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src" / "proto"))

import asyncio
import logging

from src.arbitrage.engine import ArbitrageEngine, build_triangles
from src.arbitrage.models import Opportunity
from src.exchange.mexc.ws_client import MEXCWebSocket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# Пары для треугольников через BTC, ETH, USDT
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "ETHBTC",
    "BNBUSDT", "BNBBTC",  "BNBETH",
    "SOLUSDT", "SOLBTC",  "SOLETH",
    "XRPUSDT", "XRPBTC",  "XRPETH",
]


async def on_opportunity(opp: Opportunity) -> None:
    pairs = " → ".join(opp.triangle.pairs)
    print(f"[ARB] {pairs}  profit={opp.profit_pct:+.4f}%")


async def main() -> None:
    triangles = build_triangles(set(SYMBOLS))

    engine = ArbitrageEngine(triangles, on_opportunity=on_opportunity)
    ws = MEXCWebSocket(on_book_update=engine.on_book_update)
    ws.set_symbols(SYMBOLS)

    await ws.start()


if __name__ == "__main__":
    asyncio.run(main())
