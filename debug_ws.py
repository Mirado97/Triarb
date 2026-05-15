"""
Тест: кандидаты с @Xms, ждём бинарные данные.
aggre.deals@100ms — принята. Ищем bookTicker/depth аналог.
"""
import asyncio
import json
import aiohttp

HEADERS = {
    "Origin": "https://www.mexc.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

CANDIDATES = [
    "spot@public.aggre.bookTicker.v3.api.pb@100ms@BTCUSDT",  # с @100ms
    "spot@public.aggre.bookTicker.v3.api.pb@10ms@BTCUSDT",   # с @10ms (быстрее)
    "spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT",        # depth @100ms
    "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT",        # deals (уже работал) — ждём binary
]

async def test(topic):
    url = "wss://wbs-api.mexc.com/ws"
    print(f"\nTopic: {topic}")
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.ws_connect(url, heartbeat=None) as ws:
                await ws.send_str(json.dumps({"method": "SUBSCRIPTION", "params": [topic]}))
                # Получаем до 3 сообщений за 8 секунд
                for _ in range(3):
                    msg = await asyncio.wait_for(ws.receive(), timeout=8)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        print(f"  TEXT: {msg.data[:150]}")
                        if "Blocked" in msg.data:
                            break
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        print(f"  BINARY: {len(msg.data)} bytes  <-- РАБОТАЕТ!")
                        break
    except asyncio.TimeoutError:
        print("  TIMEOUT (нет данных)")
    except Exception as e:
        print(f"  ERROR: {e}")

async def main():
    for topic in CANDIDATES:
        await test(topic)

asyncio.run(main())
