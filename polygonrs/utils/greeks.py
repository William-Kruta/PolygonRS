from __future__ import annotations

import datetime
import logging

logger = logging.getLogger(__name__)

from py_vollib.black_scholes_merton import black_scholes_merton as bsm_price
from py_vollib.black_scholes_merton.implied_volatility import implied_volatility as bsm_iv
from py_vollib.black_scholes_merton.greeks.analytical import (
    delta as bsm_delta,
    gamma as bsm_gamma,
    theta as bsm_theta,
    vega as bsm_vega,
    rho as bsm_rho,
)

# Default annualised risk-free rate. Override per-call or set module-level.
DEFAULT_RISK_FREE_RATE: float = 0.045


def _dte_to_years(dte: int) -> float:
    return dte / 365.0


def calc_iv(
    option_price: float,
    underlying_price: float,
    strike: float,
    dte: int,
    flag: str,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    dividend_yield: float = 0.0,
) -> float | None:
    """
    Back out implied volatility from a market price using BSM.

    flag: 'c' for call, 'p' for put
    Returns None if the solver fails (e.g. price outside no-arbitrage bounds).
    """
    t = _dte_to_years(dte)
    if t <= 0:
        return None
    try:
        return bsm_iv(
            option_price, underlying_price, strike, t,
            risk_free_rate, dividend_yield, flag,
        )
    except Exception as e:
        logger.debug("IV solve failed (S=%.2f K=%.2f dte=%d flag=%s): %s", underlying_price, strike, dte, flag, e)
        return None


def calc_greeks(
    underlying_price: float,
    strike: float,
    dte: int,
    sigma: float,
    flag: str,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    dividend_yield: float = 0.0,
) -> dict[str, float | None]:
    """
    Compute BSM greeks given a known IV (sigma).

    flag: 'c' for call, 'p' for put
    theta is returned as a daily value (divided by 365).
    """
    t = _dte_to_years(dte)
    if t <= 0:
        return {k: None for k in ("delta", "gamma", "theta", "vega", "rho")}

    args = (flag, underlying_price, strike, t, risk_free_rate, sigma, dividend_yield)
    try:
        return {
            "delta": bsm_delta(*args),
            "gamma": bsm_gamma(*args),
            "theta": bsm_theta(*args) / 365.0,
            "vega":  bsm_vega(*args) / 100.0,
            "rho":   bsm_rho(*args) / 100.0,
        }
    except Exception as e:
        logger.debug("Greeks calculation failed (S=%.2f K=%.2f dte=%d): %s", underlying_price, strike, dte, e)
        return {k: None for k in ("delta", "gamma", "theta", "vega", "rho")}


def calc_greeks_from_price(
    option_price: float,
    underlying_price: float,
    strike: float,
    dte: int,
    flag: str,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    dividend_yield: float = 0.0,
) -> dict[str, float | None]:
    """
    Convenience wrapper: back out IV from market price then compute all greeks.
    Returns a dict with 'iv' plus all greek keys. Any field is None on failure.
    """
    iv = calc_iv(
        option_price, underlying_price, strike, dte, flag,
        risk_free_rate, dividend_yield,
    )
    if iv is None:
        return {k: None for k in ("iv", "delta", "gamma", "theta", "vega", "rho")}

    greeks = calc_greeks(
        underlying_price, strike, dte, iv, flag,
        risk_free_rate, dividend_yield,
    )
    return {"iv": iv, **greeks}
