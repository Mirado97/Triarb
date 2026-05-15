"""Тест разных топиков и заголовков."""
import asyncio
import json
import aiohttp

HEADERS = {
    "Origin": "https://www.mexc.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
}

TESTS = [
    ("wss://wbs-api.mexc.com/ws", "spot@public.aggre.bookTicker.v3.api.pb@BTCUSDT"),
    ("wss://wbs-api.mexc.com/ws", "spot@public.bookTicker.v3.api.pb@BTCUSDT"),
    ("wss://wbs-api.mexc.com/ws", "spot@public.miniTicker.v3.api.pb@BTCUSDT"),
    ("wss://wbs-api.mexc.com/ws", "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT"),
    ("wss://wbs.mexc.com/ws",     "spot@public.aggre.bookTicker.v3.api.pb@BTCUSDT"),
]

async def test(url, topic):
    label = f"{url.split('/')[2]} | {topic}"
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.ws_connect(url, heartbeat=None) as ws:
                await ws.send_str(json.dumps({"method": "SUBSCRIPTION", "params": [topic]}))
                msg = await asyncio.wait_for(ws.receive(), timeout=6)
                print(f"[{msg.type.name}] {label}")
                if msg.type == aiohttp.WSMsgType.TEXT:
                    print(f"  TEXT: {msg.data[:200]}")
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    print(f"  BINARY: {len(msg.data)} bytes  <-- ДАННЫЕ ПРИШЛИ!")
    except asyncio.TimeoutError:
        print(f"[TIMEOUT] {label}")
    except Exception as e:
        print(f"[ERROR]   {label}: {e}")
    await asyncio.sleep(1)

async def main():
    for url, topic in TESTS:
        await test(url, topic)

asyncio.run(main())
