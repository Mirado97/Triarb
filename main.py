import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src" / "proto"))

import asyncio
import logging
import os

from dotenv import load_dotenv

from src.arbitrage.engine import ArbitrageEngine, build_triangles
from src.arbitrage.executor import ExecutionEngine
from src.exchange.mexc.rest_client import MexcRestClient
from src.exchange.mexc.ws_client import MEXCWebSocket
from src.ui.ws_server import UIServer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

ZERO_FEE_SYMBOLS = [
    "0GUSDC", "1INCHUSDC", "AAVEUSDC", "ACHUSDC", "ACTUSDC", "ADAEUR", "ADAUSDC",
    "AEVOUSDC", "AIXBTUSDC", "ALGOUSDC", "ALLOUSDC", "ANIMEUSDC", "APEUSDC", "API3USDC",
    "APTUSDC", "ARBUSDC", "ARKMUSDC", "ARUSDC", "ASTERUSDC", "ATOMUSDC", "ATUSDC",
    "AUSDC", "AVAXEUR", "AVAXUSDC", "AVNTUSDC", "AXSUSDC", "AZEROUSDC", "AZTECUSDC",
    "BABYDOGEUSDC", "BABYUSDC", "BANANAS31USDC", "BANANAUSDC", "BANKUSD1", "BANKUSDC",
    "BARDUSDC", "BBUSDC", "BCHUSDC", "BEAMXUSDC", "BERAUSDC", "BIGTIMEUSDC", "BIOUSDC",
    "BIRBUSDC", "BLURUSDC", "BMTUSDC", "BNBUSDC", "BOBUSDC", "BOMEUSDC", "BONKUSDC",
    "BRLUSDT", "BTCEUR", "BTCUSD1", "BTCUSDC", "BTCUSDE", "CAKEUSDC", "CAWUSDC",
    "CCUSDC", "CETUSUSDC", "CFXUSDC", "CGPTUSDC", "CHZUSDC", "CKBUSDC", "COMPUSDC",
    "COOKIEUSDC", "COTIUSDC", "CRVUSDC", "CUSDC", "CVXUSDC", "CYBERUSDC", "CYSUSDC",
    "DASHUSDC", "DOGEEUR", "DOGEUSDC", "DOGEUSDE", "DOGSUSDC", "DOLOUSDC", "DOTUSDC",
    "DSYNCUSDC", "DYDXUSDC", "EDENUSDC", "EGLDUSDC", "EIGENUSDC", "ENAUSDC", "ENAUSDE",
    "ENJUSDC", "ENSUSDC", "EPICUSDC", "ERAUSDC", "ESPUSDC", "ETCUSDC", "ETHEUR",
    "ETHFIUSD1", "ETHFIUSDC", "ETHFIUSDE", "ETHUSD1", "ETHUSDC", "ETHUSDE", "EURUSDT",
    "FARTCOINUSDC", "FETUSDC", "FFUSDC", "FHEUSDC", "FILUSDC", "FLOKIUSDC", "FLUXUSDC",
    "FORMUSDC", "FTTUSDC", "FTUSDC", "FUNTOKENUSDC", "GALAUSDC", "GIGGLEUSDC", "GMXUSDC",
    "GRIFFAINUSDC", "GRTUSDC", "GUNUSDC", "HBARUSDC", "HEMIUSDC", "HIVEUSDC", "HMSTRUSDC",
    "HOLOUSDC", "HOMEUSDC", "HUMAUSDC", "HUSDC", "HYPERUSDC", "HYPEUSDC", "ICPUSDC",
    "IDEXUSDC", "ILVUSDC", "IMXUSDC", "INITUSDC", "INJUSDC", "INXUSDC", "IOTAUSDC",
    "IOUSDC", "IPUSDC", "IRYSUSDC", "JASMYUSDC", "JELLYJELLYUSDC", "JTOUSDC", "JUPUSDC",
    "KAIAUSDC", "KAITOUSDC", "KASEUR", "KASUSD1", "KASUSDC", "KASUSDE", "KERNELUSDC",
    "KILOUSDC", "KITEUSDC", "KMNOUSDC", "LAUSD1", "LAUSDC", "LDOUSDC", "LIGHTUSDC",
    "LINEAUSDC", "LINGOUSDC", "LINKEUR", "LINKUSDC", "LITUSDC", "LPTUSDC", "LTCEUR",
    "LTCUSDC", "LUNCUSDC", "MAGICUSDC", "MANTAUSDC", "MASKUSDC", "MAVUSDC", "MELANIAEUR",
    "MELANIAUSD1", "MELANIAUSDC", "MEMEUSDC", "METUSDC", "MINAUSDC", "MIRAUSDC", "MMTUSDC",
    "MNTUSDC", "MONUSDC", "MORPHOUSDC", "MOVEUSDC", "MUBARAKUSDC", "MXEUR", "MXUSDC",
    "MYXUSDC", "NAKAUSDC", "NEARUSDC", "NEOUSDC", "NEWTUSDC", "NIGHTUSDC", "NILUSDC",
    "NMRUSDC", "NOTUSDC", "NPCUSDC", "NXPCUSDC", "OMIUSDC", "ONDOUSDC", "OPENUSDC",
    "OPUSDC", "ORCAUSDC", "ORDIUSDC", "PARTIUSDC", "PEAQUSDC", "PENDLEUSDC", "PENGUUSDC",
    "PEOPLEUSDC", "PEPEEUR", "PEPEUSDC", "PEPEUSDE", "PHAUSDC", "PIEUR", "PIPPINUSDC",
    "PIUSD1", "PIUSDC", "PIUSDE", "PIXELUSDC", "PLUMEUSDC", "PNUTUSDC", "POLUSDC",
    "POPCATUSDC", "PROVEUSDC", "PUMPUSDC", "PYTHUSDC", "QNTUSDC", "QTUMUSDC", "QUBICUSDC",
    "RAREUSDC", "RAYUSDC", "RBNTUSDC", "REDUSDC", "RENDERUSDC", "REZUSDC", "RIOEUR",
    "RIOUSDC", "RNBWUSDC", "ROSEUSDC", "RSRUSDC", "RUNEUSDC", "RVNUSDC", "SAGAUSDC",
    "SAHARAUSD1", "SAHARAUSDC", "SANDUSDC", "SAPIENUSDC", "SEIUSDC", "SENTUSDC", "SHIBEUR",
    "SHIBUSDC", "SIGNUSDC", "SKLUSDC", "SKYUSDC", "SNXUSDC", "SOLEUR", "SOLUSD1",
    "SOLUSDC", "SOLUSDE", "SOMIUSDC", "SPKUSDC", "SPXUSDC", "STABLEUSDC", "STOUSDC",
    "STRKUSDC", "STXUSDC", "SUIEUR", "SUIUSDC", "SUIUSDE", "SUPRAUSDC", "SUSDC",
    "SUSHIUSDC", "SYRUPUSDC", "TAGUSD1", "TAOEUR", "TAOUSDC", "THEUSDC", "TIAUSDC",
    "TLMUSDC", "TNSRUSDC", "TONEUR", "TONUSDC", "TOWNSUSDC", "TREEUSDC", "TRUMPEUR",
    "TRUMPUSD1", "TRUMPUSDC", "TRXUSDC", "TRXUSDE", "TSTUSDC", "TURBOUSDC", "TWTUSDC",
    "ULTIMAEUR", "ULTIMAUSDC", "ULTIMAUSDE", "UMAUSDC", "UNIUSDC", "USD1USDT", "USDCEUR",
    "USDCUSDT", "USDEUSDT", "USELESSUSDC", "USUALUSDC", "VANRYUSDC", "VELODROMEUSDC",
    "VETUSDC", "VIRTUALUSDC", "WALUSDC", "WAVESUSDC", "WBTCUSDC", "WCTUSDC", "WETUSDC",
    "WEXOUSDC", "WIFEUR", "WIFUSDC", "WLDUSDC", "WLFIUSD1", "WLFIUSDC", "WUSDC",
    "XAIUSDC", "XCNUSDC", "XDCUSDC", "XLMUSDC", "XMRUSDC", "XPLUSDC", "XRPEUR",
    "XRPUSD1", "XRPUSDC", "XRPUSDE", "XRPUSDT", "XTZUSDC", "XVGUSDC", "YBUSDC",
    "YGGUSDC", "ZBCNUSDC", "ZBTUSDC", "ZECUSDC", "ZENUSDC", "ZKPUSDC", "ZKUSDC",
    "ZROUSDC",
]


async def latency_loop(ui: UIServer) -> None:
    import time as _time
    import urllib.request as _urllib

    def _ping() -> float:
        start = _time.monotonic()
        _urllib.urlopen("https://api.mexc.com/api/v3/ping", timeout=5)
        return round((_time.monotonic() - start) * 1000, 2)

    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(10)
        lat = None
        try:
            lat = await loop.run_in_executor(None, _ping)
        except Exception:
            pass
        if lat is not None:
            await ui.broadcast({
                "type": "latency",
                "data": {"ts": int(_time.time()), "rest_ms": lat},
            })


async def balance_loop(client: MexcRestClient, ui: UIServer) -> None:
    while True:
        try:
            balances = await client.get_balances()
            await ui.broadcast({"type": "balance", "data": balances})
        except Exception as e:
            logging.getLogger(__name__).error(f"Balance poll: {e}")
        await asyncio.sleep(30)


async def main() -> None:
    log = logging.getLogger(__name__)

    api_key    = os.environ.get("MEXC_API_KEY", "")
    api_secret = os.environ.get("MEXC_API_SECRET", "")
    trade_amount   = float(os.environ.get("TRADE_AMOUNT", "50"))
    min_profit_pct = float(os.environ.get("MIN_PROFIT_PCT", "0.03"))
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"
    ui_port = int(os.environ.get("UI_PORT", "8765"))

    triangles = build_triangles(set(ZERO_FEE_SYMBOLS))
    used = sorted({p for t in triangles for p in t.pairs})
    log.info(f"Triangles: {len(triangles)}, pairs: {len(used)}")
    log.info(f"Trade: ${trade_amount}  MinProfit: {min_profit_pct}%  DryRun: {dry_run}")

    ui = UIServer(port=ui_port)
    await ui.start()

    client: MexcRestClient | None = None
    executor: ExecutionEngine | None = None

    if api_key and not dry_run:
        client = MexcRestClient(api_key, api_secret)
        await client.start()
        await client.load_lot_sizes(used)
        executor = ExecutionEngine(
            client=client,
            trade_amount=trade_amount,
            min_profit_pct=min_profit_pct,
            broadcast=ui.broadcast,
            is_paused=lambda: ui.paused,
        )
        on_opportunity = executor.on_opportunity
        log.info("Trading mode: LIVE")
    else:
        if not api_key:
            log.warning("No API keys — monitor only")
        else:
            log.info("DRY_RUN=true — monitor only")

        async def on_opportunity(opp):
            pairs = " → ".join(opp.triangle.pairs)
            log.info(f"[SIGNAL] {pairs}  profit={opp.profit_pct:+.4f}%")

    ws_client = MEXCWebSocket(on_book_update=ArbitrageEngine(
        triangles, on_opportunity=on_opportunity
    ).on_book_update)
    ws_client.set_symbols(used)

    tasks = [
        asyncio.create_task(ws_client.start()),
        asyncio.create_task(latency_loop(ui)),
    ]
    if client:
        tasks.append(asyncio.create_task(balance_loop(client, ui)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
