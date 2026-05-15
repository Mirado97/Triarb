import sys
from pathlib import Path

# protoc генерирует bare imports — нужно добавить src/proto в sys.path
sys.path.insert(0, str(Path(__file__).parent / "src" / "proto"))

import asyncio
import logging

from src.exchange.mexc.ws_client import MEXCWebSocket, OrderBook

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# Три пары для теста треугольника USDT → BTC → ETH → USDT
TEST_SYMBOLS = ["BTCUSDT", "ETHUSDT", "ETHBTC"]


async def on_update(book: OrderBook) -> None:
    print(
        f"{book.symbol:10s}  bid={book.bid:<12}  ask={book.ask:<12}  "
        f"latency={book.latency_ms:.1f}ms"
    )


async def main() -> None:
    client = MEXCWebSocket(on_update)
    client.set_symbols(TEST_SYMBOLS)
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())
