"""
Black-Scholes options pricing and Greeks.

Uses only stdlib math — no scipy dependency.
"""

from math import log, sqrt, exp, erf, pi


def _norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return exp(-0.5 * x * x) / sqrt(2.0 * pi)


def black_scholes_call(
    S: float, K: float, T: float, r: float, sigma: float
) -> float:
    """
    Black-Scholes call option price.

    Parameters
    ----------
    S : spot price
    K : strike price
    T : time to expiry in years
    r : annualized risk-free rate (e.g. 0.05 for 5%)
    sigma : annualized volatility (e.g. 0.40 for 40%)
    """
    if T <= 0:
        return max(S - K, 0.0)
    if sigma <= 0:
        return max(S - K * exp(-r * T), 0.0)

    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)

    return S * _norm_cdf(d1) - K * exp(-r * T) * _norm_cdf(d2)


def call_delta(
    S: float, K: float, T: float, r: float, sigma: float
) -> float:
    """Delta of a call option (dC/dS)."""
    if T <= 0 or sigma <= 0:
        return 1.0 if S > K else 0.0

    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    return _norm_cdf(d1)


def call_gamma(
    S: float, K: float, T: float, r: float, sigma: float
) -> float:
    """Gamma of a call option (d²C/dS²)."""
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0

    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    return _norm_pdf(d1) / (S * sigma * sqrt(T))


def call_theta(
    S: float, K: float, T: float, r: float, sigma: float
) -> float:
    """Theta of a call option (dC/dT) per day."""
    if T <= 0 or sigma <= 0:
        return 0.0

    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)

    term1 = -(S * _norm_pdf(d1) * sigma) / (2.0 * sqrt(T))
    term2 = -r * K * exp(-r * T) * _norm_cdf(d2)
    return (term1 + term2) / 365.0


def historical_volatility(prices: list[float], annualize: bool = True) -> float:
    """
    Compute historical volatility from a series of daily closing prices.

    Returns annualized volatility by default.
    """
    if len(prices) < 2:
        return 0.4  # fallback

    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            returns.append(log(prices[i] / prices[i - 1]))

    if len(returns) < 2:
        return 0.4

    mean_r = sum(returns) / len(returns)
    var = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
    daily_vol = sqrt(var)

    if annualize:
        return daily_vol * sqrt(252)
    return daily_vol
