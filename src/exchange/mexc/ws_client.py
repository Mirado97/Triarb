import asyncio
import json
import logging
import time
from dataclasses import dataclass

import aiohttp
import orjson

logger = logging.getLogger(__name__)


@dataclass
class OrderBook:
    symbol: str
    bid: float
    ask: float
    bid_qty: float
    ask_qty: float
    exchange_ts: float  # Unix seconds from exchange
    receive_ts: float   # Unix seconds when received locally

    @property
    def latency_ms(self) -> float:
        return (self.receive_ts - self.exchange_ts) * 1000


class MEXCWebSocket:
    WS_URL = "wss://wbs.mexc.com/ws"
    BATCH_SIZE = 10
    RECONNECT_DELAY = 5
    PING_INTERVAL = 20  # секунды между application-level PING

    def __init__(self, on_book_update):
        self._on_book_update = on_book_update
        self._topics: list[str] = []
        self._ws = None

    def set_symbols(self, symbols: list[str]) -> None:
        self._topics = [f"spot@public.bookTicker.v3.api@{s}" for s in symbols]

    async def start(self) -> None:
        while True:
            try:
                await self._run()
            except Exception as e:
                logger.error(f"WS error: {e}, reconnecting in {self.RECONNECT_DELAY}s")
                await asyncio.sleep(self.RECONNECT_DELAY)

    async def _run(self) -> None:
        # heartbeat=None — отключаем aiohttp-level ping, используем MEXC application-level PING
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.WS_URL, heartbeat=None) as ws:
                self._ws = ws
                logger.info("WS connected to MEXC")
                await self._subscribe_all(ws)
                ping_task = asyncio.create_task(self._ping_loop(ws))
                try:
                    await self._listen(ws)
                finally:
                    ping_task.cancel()
        logger.warning("WS connection closed")

    async def _ping_loop(self, ws) -> None:
        """Держим соединение живым application-level PING каждые 20 секунд."""
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            try:
                await ws.send_str('{"method":"PING"}')
                logger.debug("Sent PING")
            except Exception:
                break

    async def _subscribe_all(self, ws) -> None:
        for i in range(0, len(self._topics), self.BATCH_SIZE):
            batch = self._topics[i : i + self.BATCH_SIZE]
            await ws.send_str(json.dumps({"method": "SUBSCRIPTION", "params": batch}))
            logger.info(f"Subscribed batch {i // self.BATCH_SIZE + 1}: {batch}")
            await asyncio.sleep(0.1)

    async def _listen(self, ws) -> None:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle(msg.data)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await self._handle(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    async def _handle(self, raw) -> None:
        receive_ts = time.time()
        try:
            data = orjson.loads(raw)
        except Exception:
            return

        # Логируем всё кроме book-апдейтов (для диагностики)
        if "d" not in data:
            logger.info(f"MSG: {raw[:300]}")

        if data.get("method") == "PING":
            await self._ws.send_str('{"method":"PONG"}')
            return

        d = data.get("d")
        if not d or "a" not in d or "b" not in d:
            return

        try:
            book = OrderBook(
                symbol=d["s"],
                bid=float(d["b"]),
                ask=float(d["a"]),
                bid_qty=float(d["B"]),
                ask_qty=float(d["A"]),
                exchange_ts=data["t"] / 1000,
                receive_ts=receive_ts,
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"Parse error: {e}, raw: {raw[:200]}")
            return

        await self._on_book_update(book)
