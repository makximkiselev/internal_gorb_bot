# handlers/pricing/rules.py
from math import ceil
from typing import Dict, Union

Number = Union[int, float]

def retail_margin_percent(cost: Number) -> float:
    """Возвращает наценку (в долях), исходя из закупочной цены (min_cost)."""
    c = float(cost)
    if c <= 5000:
        return 0.30
    if 5001 <= c <= 10000:
        return 0.20
    if 10001 <= c <= 20000:
        return 0.15
    if 20001 <= c <= 30000:
        return 0.15
    if 30001 <= c <= 40000:
        return 0.10
    if 40001 <= c <= 50000:
        return 0.05
    if 50001 <= c <= 60000:
        return 0.08
    if 60001 <= c <= 70000:
        return 0.06
    if 70001 <= c <= 80000:
        return 0.05
    if 80001 <= c <= 90000:
        return 0.05
    if 90001 <= c <= 100000:
        return 0.04
    if 100001 <= c <= 120000:
        return 0.04
    # >= 120001
    return 0.04

def price_min(min_cost: Number) -> int:
    """Минимальная цена (как есть)."""
    return int(round(float(min_cost)))

def price_opt(min_cost: Number) -> int:
    """Цена для опта: +500 к минимальной."""
    return price_min(min_cost) + 500

def price_retail(min_cost: Number) -> int:
    """Цена для розничного канала: min + наценка по сетке (округление вверх)."""
    c = float(min_cost)
    margin = retail_margin_percent(c)
    return int(ceil(c * (1.0 + margin)))

def compute_price_variants(min_cost: Number) -> Dict[str, int]:
    """Удобная обёртка: все три ценника сразу."""
    m = price_min(min_cost)
    return {
        "min": m,
        "opt": price_opt(m),
        "retail": price_retail(m),
    }
