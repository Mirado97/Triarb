import json
import logging
import pathlib

from aiohttp import web

logger = logging.getLogger(__name__)
HTML_PATH = pathlib.Path(__file__).parent / "index.html"


class UIServer:
    def __init__(self, port: int = 8765):
        self._port = port
        self._clients: set[web.WebSocketResponse] = set()
        self._app = web.Application()
        self._app.router.add_get("/", self._index)
        self._app.router.add_get("/ws", self._ws_handler)
        self.paused = False

    async def start(self) -> None:
        runner = web.AppRunner(self._app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._port)
        await site.start()
        logger.info(f"UI: http://localhost:{self._port}")

    async def broadcast(self, msg: dict) -> None:
        if not self._clients:
            return
        data = json.dumps(msg)
        dead = set()
        for ws in self._clients:
            try:
                await ws.send_str(data)
            except Exception:
                dead.add(ws)
        self._clients -= dead

    async def _index(self, _: web.Request) -> web.Response:
        return web.Response(
            text=HTML_PATH.read_text(encoding="utf-8"),
            content_type="text/html",
        )

    async def _ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._clients.add(ws)
        logger.debug("UI client connected")

        # Send current pause state to new client
        await ws.send_str(json.dumps({"type": "paused", "data": self.paused}))

        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        cmd = json.loads(msg.data)
                        if cmd.get("cmd") == "toggle_pause":
                            self.paused = not self.paused
                            state = "PAUSED" if self.paused else "RESUMED"
                            logger.info(f"Bot {state} via UI")
                            await self.broadcast({"type": "paused", "data": self.paused})
                    except Exception:
                        pass
        finally:
            self._clients.discard(ws)
        return ws
