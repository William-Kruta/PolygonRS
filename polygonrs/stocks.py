from __future__ import annotations

import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)

import polars as pl
import duckdb
from polygon import StocksClient

from .data.db import get_coverage, insert_candles, query_candles, upsert_coverage
from .options import _call_with_retry, _to_dt


def _flatten_snapshot(r: dict) -> dict:
    day = r.get("day") or {}
    prev_day = r.get("prevDay") or {}
    last_trade = r.get("lastTrade") or {}
    last_quote = r.get("lastQuote") or {}
    min_bar = r.get("min") or {}
    return {
        "ticker": r.get("ticker"),
        "day_open": day.get("o"),
        "day_high": day.get("h"),
        "day_low": day.get("l"),
        "day_close": day.get("c"),
        "day_volume": day.get("v"),
        "day_vwap": day.get("vw"),
        "prev_close": prev_day.get("c"),
        "prev_volume": prev_day.get("v"),
        "last_trade_price": last_trade.get("p"),
        "last_trade_size": last_trade.get("s"),
        "last_trade_timestamp": last_trade.get("t"),
        "bid": last_quote.get("p"),
        "ask": last_quote.get("P"),
        "bid_size": last_quote.get("s"),
        "ask_size": last_quote.get("S"),
        "min_close": min_bar.get("c"),
        "min_volume": min_bar.get("v"),
        "todays_change": r.get("todaysChange"),
        "todays_change_pct": r.get("todaysChangePerc"),
        "updated": r.get("updated"),
    }


def _empty_snapshot_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "ticker": pl.String,
            "day_open": pl.Float64,
            "day_high": pl.Float64,
            "day_low": pl.Float64,
            "day_close": pl.Float64,
            "day_volume": pl.Float64,
            "day_vwap": pl.Float64,
            "prev_close": pl.Float64,
            "prev_volume": pl.Float64,
            "last_trade_price": pl.Float64,
            "last_trade_size": pl.Float64,
            "last_trade_timestamp": pl.Int64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "bid_size": pl.Float64,
            "ask_size": pl.Float64,
            "min_close": pl.Float64,
            "min_volume": pl.Float64,
            "todays_change": pl.Float64,
            "todays_change_pct": pl.Float64,
            "updated": pl.Int64,
        }
    )


def _parse_bars_response(
    response: dict, symbol: str, multiplier: int, timespan: str
) -> pl.DataFrame:
    results = response.get("results") or []
    if not results:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime("ms", "UTC"),
                "symbol": pl.String,
                "interval": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "vwap": pl.Float64,
                "transactions": pl.Int64,
            }
        )
    ticker = response.get("ticker", symbol)
    return (
        pl.DataFrame(results)
        .rename(
            {
                "v": "volume",
                "vw": "vwap",
                "o": "open",
                "c": "close",
                "h": "high",
                "l": "low",
                "t": "timestamp",
                "n": "transactions",
            }
        )
        .with_columns(
            pl.col("timestamp").cast(pl.Datetime("ms", "UTC")).alias("timestamp"),
            pl.lit(ticker).alias("symbol"),
            pl.lit(f"{multiplier}{timespan}").alias("interval"),
        )
        .select(
            [
                "timestamp",
                "symbol",
                "interval",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vwap",
                "transactions",
            ]
        )
    )


class Stocks:
    """Wrapper around polygon.io StocksClient."""

    def __init__(self, api_key: str, use_async: bool = False):
        self.client = StocksClient(api_key, use_async=use_async)

    # ------------------------------------------------------------------
    # OHLCV data
    # ------------------------------------------------------------------

    def get_aggregate_bars(
        self,
        symbol: str,
        from_date: datetime.date | datetime.datetime | str,
        to_date: datetime.date | datetime.datetime | str,
        multiplier: int = 1,
        timespan: str = "day",
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 5000,
        full_range: bool = False,
    ):
        return _call_with_retry(
            self.client.get_aggregate_bars,
            symbol,
            from_date,
            to_date,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
            multiplier=multiplier,
            timespan=timespan,
            full_range=full_range,
        )

    def get_aggregate_bars_df(
        self,
        symbol: str,
        from_date: datetime.date | datetime.datetime | str,
        to_date: datetime.date | datetime.datetime | str,
        multiplier: int = 1,
        timespan: str = "day",
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 5000,
        full_range: bool = False,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> pl.DataFrame:
        """
        Fetch OHLCV candles as a Polars DataFrame.

        Pass conn to enable local DuckDB caching — same gap-filling logic as Options.
        """
        if conn is not None:
            return self._get_aggregate_bars_cached(
                symbol, from_date, to_date, multiplier, timespan, conn
            )

        response = self.get_aggregate_bars(
            symbol,
            from_date,
            to_date,
            multiplier=multiplier,
            timespan=timespan,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
            full_range=full_range,
        )
        return _parse_bars_response(response, symbol, multiplier, timespan)

    def _fetch_and_store(
        self,
        symbol: str,
        from_dt: datetime.datetime,
        to_dt: datetime.datetime,
        multiplier: int,
        timespan: str,
        conn: duckdb.DuckDBPyConnection,
    ):
        logger.debug(
            "API fetch %s [%s → %s] (%s%s)",
            symbol,
            from_dt.date(),
            to_dt.date(),
            multiplier,
            timespan,
        )
        response = self.get_aggregate_bars(
            symbol, from_dt, to_dt, multiplier=multiplier, timespan=timespan
        )
        df = _parse_bars_response(response, symbol, multiplier, timespan)
        if not df.is_empty():
            logger.debug("Storing %d candles for %s", len(df), symbol)
            insert_candles(df, conn)
        else:
            logger.debug("No candles returned for %s in range", symbol)
        upsert_coverage(conn, symbol, f"{multiplier}{timespan}", from_dt, to_dt)

    def _get_aggregate_bars_cached(
        self,
        symbol: str,
        from_date,
        to_date,
        multiplier: int,
        timespan: str,
        conn: duckdb.DuckDBPyConnection,
    ) -> pl.DataFrame:
        interval = f"{multiplier}{timespan}"
        from_dt = _to_dt(from_date)
        to_dt = _to_dt(to_date)

        coverage = get_coverage(conn, symbol, interval)

        if coverage is None:
            logger.info("No coverage for %s — fetching full range", symbol)
            self._fetch_and_store(symbol, from_dt, to_dt, multiplier, timespan, conn)
        else:
            checked_from = _to_dt(coverage["checked_from"])
            checked_to = _to_dt(coverage["checked_to"])

            if from_dt < checked_from:
                logger.info(
                    "Backward gap for %s: fetching %s → %s",
                    symbol,
                    from_dt.date(),
                    checked_from.date(),
                )
                self._fetch_and_store(
                    symbol, from_dt, checked_from, multiplier, timespan, conn
                )

            if to_dt > checked_to:
                logger.info(
                    "Forward gap for %s: fetching %s → %s",
                    symbol,
                    checked_to.date(),
                    to_dt.date(),
                )
                self._fetch_and_store(
                    symbol, checked_to, to_dt, multiplier, timespan, conn
                )

            if from_dt >= checked_from and to_dt <= checked_to:
                logger.debug("Cache hit for %s — no API call needed", symbol)

        return query_candles(conn, symbol, interval, from_dt, to_dt)

    def get_grouped_daily_bars(
        self,
        date: datetime.date | datetime.datetime | str,
        adjusted: bool = True,
    ):
        """Fetch OHLCV bars for all tickers on a given date."""
        return _call_with_retry(
            self.client.get_grouped_daily_bars,
            date,
            adjusted=adjusted,
        )

    def get_daily_open_close(
        self,
        symbol: str,
        date: datetime.date | datetime.datetime | str,
        adjusted: bool = True,
    ) -> dict:
        return _call_with_retry(
            self.client.get_daily_open_close,
            symbol,
            date,
            adjusted=adjusted,
        )

    def get_previous_close(self, ticker: str, adjusted: bool = True) -> dict:
        return _call_with_retry(
            self.client.get_previous_close,
            ticker,
            adjusted=adjusted,
        )

    # ------------------------------------------------------------------
    # Trades & quotes
    # ------------------------------------------------------------------

    def get_trades(
        self,
        ticker: str,
        timestamp: Optional[str] = None,
        sort: str = "timestamp",
        order: str = "asc",
        limit: int = 5000,
        all_pages: bool = False,
    ):
        return _call_with_retry(
            self.client.get_trades,
            ticker,
            timestamp=timestamp,
            sort=sort,
            order=order,
            limit=limit,
            all_pages=all_pages,
        )

    def get_quotes(
        self,
        ticker: str,
        timestamp: Optional[str] = None,
        sort: str = "timestamp",
        order: str = "asc",
        limit: int = 5000,
        all_pages: bool = False,
    ):
        return _call_with_retry(
            self.client.get_quotes,
            ticker,
            timestamp=timestamp,
            sort=sort,
            order=order,
            limit=limit,
            all_pages=all_pages,
        )

    def get_last_trade(self, ticker: str) -> dict:
        return _call_with_retry(self.client.get_last_trade, ticker)

    def get_last_quote(self, ticker: str) -> dict:
        return _call_with_retry(self.client.get_last_quote, ticker)

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------

    def get_snapshot(self, ticker: str) -> dict:
        return _call_with_retry(self.client.get_snapshot, ticker)

    def get_snapshots(self, tickers: list[str]) -> pl.DataFrame:
        """
        Fetch snapshots for multiple tickers, returning a Polars DataFrame.
        Batches into groups of 250 (polygon's per-request max).
        Requires a paid polygon plan.
        """
        BATCH_SIZE = 250
        raw: list[dict] = []

        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i : i + BATCH_SIZE]
            result = _call_with_retry(
                self.client.get_snapshots,
                ",".join(batch),
            )
            if isinstance(result, list):
                raw.extend(result)
            elif isinstance(result, dict):
                raw.extend(result.get("tickers", []))

        if not raw:
            logger.warning("No snapshot data returned for tickers: %s", tickers)
            return _empty_snapshot_df()

        rows = [_flatten_snapshot(r) for r in raw]
        return pl.DataFrame(rows, infer_schema_length=len(rows))

    def get_gainers_losers(self, direction: str = "gainers") -> pl.DataFrame:
        """
        direction: "gainers" or "losers"
        Returns a DataFrame of top market movers for the current session.
        Requires a paid polygon plan.
        """
        result = _call_with_retry(self.client.get_gainers_losers, direction)
        tickers = result if isinstance(result, list) else result.get("tickers", [])
        if not tickers:
            logger.warning("No %s returned", direction)
            return _empty_snapshot_df()
        rows = [_flatten_snapshot(r) for r in tickers]
        return pl.DataFrame(rows, infer_schema_length=len(rows))

    # ------------------------------------------------------------------
    # Technical indicators
    # ------------------------------------------------------------------

    def get_sma(
        self,
        symbol: str,
        timespan: str = "day",
        window_size: int = 50,
        series_type: str = "close",
        adjusted: bool = True,
        order: str = "desc",
        limit: int = 5000,
    ):
        return _call_with_retry(
            self.client.get_sma,
            symbol,
            timespan=timespan,
            adjusted=adjusted,
            window_size=window_size,
            series_type=series_type,
            order=order,
            limit=limit,
        )

    def get_ema(
        self,
        symbol: str,
        timespan: str = "day",
        window_size: int = 50,
        series_type: str = "close",
        adjusted: bool = True,
        order: str = "desc",
        limit: int = 5000,
    ):
        return _call_with_retry(
            self.client.get_ema,
            symbol,
            timespan=timespan,
            adjusted=adjusted,
            window_size=window_size,
            series_type=series_type,
            order=order,
            limit=limit,
        )

    def get_rsi(
        self,
        symbol: str,
        timespan: str = "day",
        window_size: int = 14,
        series_type: str = "close",
        adjusted: bool = True,
        order: str = "desc",
        limit: int = 5000,
    ):
        return _call_with_retry(
            self.client.get_rsi,
            symbol,
            timespan=timespan,
            adjusted=adjusted,
            window_size=window_size,
            series_type=series_type,
            order=order,
            limit=limit,
        )

    def get_macd(
        self,
        symbol: str,
        timespan: str = "day",
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        series_type: str = "close",
        adjusted: bool = True,
        order: str = "desc",
        limit: int = 5000,
    ):
        return _call_with_retry(
            self.client.get_macd,
            symbol,
            timespan=timespan,
            adjusted=adjusted,
            short_window_size=short_window,
            long_window_size=long_window,
            signal_window_size=signal_window,
            series_type=series_type,
            order=order,
            limit=limit,
        )
