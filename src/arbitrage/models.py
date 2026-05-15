from dataclasses import dataclass, field
from time import time


@dataclass
class Triangle:
    """
    Треугольник: три пары для цикла base → quote1 → quote2 → base.
    Пример: USDT→BTC→ETH→USDT через пары BTCUSDT, ETHBTC, ETHUSDT.
    """
    pairs: tuple[str, str, str]        # ("BTCUSDT", "ETHBTC", "ETHUSDT")
    # Направление каждой ноги: True = buy (берём base, платим quote), False = sell
    directions: tuple[bool, bool, bool]


@dataclass
class Opportunity:
    triangle: Triangle
    profit_pct: float      # (result - 1.0) * 100
    bids: tuple[float, float, float]
    asks: tuple[float, float, float]
    ts: float = field(default_factory=time)   # Unix seconds когда найдено
