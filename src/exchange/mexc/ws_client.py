import asyncio
import json
import logging
import time
from dataclasses import dataclass

import aiohttp

from src.proto.PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper

logger = logging.getLogger(__name__)

_WS_HEADERS = {
    "Origin": "https://www.mexc.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    ),
}


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
    WS_URL = "wss://wbs-api.mexc.com/ws"
    BATCH_SIZE = 10
    RECONNECT_DELAY = 5
    PING_INTERVAL = 20

    def __init__(self, on_book_update):
        self._on_book_update = on_book_update
        self._topics: list[str] = []
        self._ws = None

    def set_symbols(self, symbols: list[str]) -> None:
        # @10ms = 100 обновлений/сек — оптимально для арбитража
        self._topics = [f"spot@public.aggre.bookTicker.v3.api.pb@10ms@{s}" for s in symbols]

    async def start(self) -> None:
        while True:
            try:
                await self._run()
            except Exception as e:
                logger.error(f"WS error: {e}, reconnecting in {self.RECONNECT_DELAY}s")
                await asyncio.sleep(self.RECONNECT_DELAY)

    async def _run(self) -> None:
        async with aiohttp.ClientSession(headers=_WS_HEADERS) as session:
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
            if msg.type == aiohttp.WSMsgType.BINARY:
                await self._handle_binary(msg.data)
            elif msg.type == aiohttp.WSMsgType.TEXT:
                logger.debug(f"TEXT: {msg.data[:200]}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    async def _handle_binary(self, raw: bytes) -> None:
        receive_ts = time.time()
        try:
            wrapper = PushDataV3ApiWrapper.FromString(raw)
        except Exception as e:
            logger.warning(f"Protobuf parse error: {e}")
            return

        field = wrapper.WhichOneof("body")
        if field != "publicAggreBookTicker":
            return

        t = wrapper.publicAggreBookTicker
        try:
            book = OrderBook(
                symbol=wrapper.symbol,
                bid=float(t.bidPrice),
                ask=float(t.askPrice),
                bid_qty=float(t.bidQuantity),
                ask_qty=float(t.askQuantity),
                exchange_ts=wrapper.sendTime / 1000,
                receive_ts=receive_ts,
            )
        except (ValueError, AttributeError) as e:
            logger.warning(f"Field error: {e}")
            return

        await self._on_book_update(book)
