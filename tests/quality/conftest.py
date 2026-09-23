from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from market_ops.quality.checks import OHLCV_COLUMNS


def frame(session: date, tickers: list[str], **overrides) -> pd.DataFrame:
    """A realistic raw-zone frame: every OHLCV column, ``date`` as datetime.date.

    ``overrides`` replaces whole columns, e.g. ``close=[1.0, None]``.
    """
    n = len(tickers)
    data = {
        "date": [session] * n,
        "ticker": tickers,
        "open": [100.0] * n,
        "high": [101.0] * n,
        "low": [99.0] * n,
        "close": [100.5] * n,
        "volume": [93171.830207] * n,  # Polygon volume is fractional; kept as float
        "vwap": [100.2] * n,
        "trade_count": [1234.0] * n,
    }
    data.update(overrides)
    return pd.DataFrame(data, columns=OHLCV_COLUMNS)


@pytest.fixture
def make_frame():
    return frame
