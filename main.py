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

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# Все пары с 0% комиссией (мейкер и тейкер) на MEXC спот
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


async def main() -> None:
    log = logging.getLogger(__name__)

    api_key = os.environ.get("MEXC_API_KEY", "")
    api_secret = os.environ.get("MEXC_API_SECRET", "")
    trade_amount = float(os.environ.get("TRADE_AMOUNT", "50"))
    min_profit_pct = float(os.environ.get("MIN_PROFIT_PCT", "0.03"))
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"

    triangles = build_triangles(set(ZERO_FEE_SYMBOLS))
    used = sorted({p for t in triangles for p in t.pairs})
    log.info(f"Triangles: {len(triangles)}, subscribing to {len(used)} pairs")
    log.info(f"Trade amount: ${trade_amount}  Min profit: {min_profit_pct}%  Dry run: {dry_run}")

    if dry_run or not api_key:
        if not api_key:
            log.warning("No API keys — running in monitor-only mode")

        async def on_opportunity(opp):
            pairs = " → ".join(opp.triangle.pairs)
            log.info(f"[SIGNAL] {pairs}  profit={opp.profit_pct:+.4f}%")

    else:
        client = MexcRestClient(api_key, api_secret)
        await client.start()
        await client.load_lot_sizes(used)

        executor = ExecutionEngine(
            client=client,
            trade_amount=trade_amount,
            min_profit_pct=min_profit_pct,
        )
        on_opportunity = executor.on_opportunity

    engine = ArbitrageEngine(triangles, on_opportunity=on_opportunity)
    ws = MEXCWebSocket(on_book_update=engine.on_book_update)
    ws.set_symbols(used)

    await ws.start()


if __name__ == "__main__":
    asyncio.run(main())
