from __future__ import annotations

import datetime
from typing import Optional

import polars as pl
import duckdb
import polygon
from polygon.options.options import (
    OptionsClient,
    build_option_symbol,
    convert_option_symbol_formats,
    detect_option_symbol_format,
    parse_option_symbol,
)

from .data.db import get_coverage, insert_candles, query_candles, upsert_coverage


def _to_dt(d) -> datetime.datetime:
    if isinstance(d, datetime.datetime):
        return d if d.tzinfo else d.replace(tzinfo=datetime.timezone.utc)
    if isinstance(d, datetime.date):
        return datetime.datetime(d.year, d.month, d.day, tzinfo=datetime.timezone.utc)
    if isinstance(d, str):
        dt = datetime.datetime.fromisoformat(d)
        return dt if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc)
    raise TypeError(f"Cannot convert {type(d)} to datetime")


class Options:
    """Wrapper around polygon.io OptionsClient."""

    def __init__(self, api_key: str, use_async: bool = False):
        self.client = OptionsClient(api_key, use_async=use_async)

    # ------------------------------------------------------------------
    # Symbol helpers
    # ------------------------------------------------------------------

    def build_symbol(
        self,
        underlying_symbol: str,
        expiry: datetime.date | datetime.datetime | str,
        call_or_put: str,
        strike_price: float | int,
        fmt: str = "polygon",
        prefix_o: bool = False,
    ) -> str:
        return build_option_symbol(
            underlying_symbol,
            expiry,
            call_or_put,
            strike_price,
            _format=fmt,
            prefix_o=prefix_o,
        )

    def parse_symbol(
        self,
        option_symbol: str,
        fmt: str = "polygon",
        output_format: str = "object",
    ):
        return parse_option_symbol(option_symbol, _format=fmt, output_format=output_format)

    def convert_symbol(self, option_symbol: str, from_format: str, to_format: str) -> str:
        return convert_option_symbol_formats(option_symbol, from_format, to_format)

    def detect_symbol_format(self, option_symbol: str) -> str | bool | list:
        return detect_option_symbol_format(option_symbol)

    # ------------------------------------------------------------------
    # Price / OHLC data
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
        return self.client.get_aggregate_bars(
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

        If conn is provided, the local DB is used as a cache — only date ranges
        not yet stored will trigger an API call. Coverage cases:
          - No coverage:           full API call [from, to]
          - Backward gap:          API call [from, checked_from]
          - Forward gap:           API call [checked_to, to]
          - Fully within coverage: DB-only, no API call
        """
        if conn is not None:
            return self._get_aggregate_bars_cached(
                symbol, from_date, to_date, multiplier, timespan, conn
            )

        response = self.get_aggregate_bars(
            symbol, from_date, to_date,
            multiplier=multiplier, timespan=timespan,
            adjusted=adjusted, sort=sort, limit=limit,
            full_range=full_range,
        )
        return self._parse_bars_response(response, symbol, multiplier, timespan)

    def _parse_bars_response(
        self, response: dict, symbol: str, multiplier: int, timespan: str
    ) -> pl.DataFrame:
        results = response.get("results") or []
        if not results:
            return pl.DataFrame(schema={
                "timestamp": pl.Datetime("ms", "UTC"), "symbol": pl.String,
                "interval": pl.String, "open": pl.Float64, "high": pl.Float64,
                "low": pl.Float64, "close": pl.Float64, "volume": pl.Int64,
                "vwap": pl.Float64, "transactions": pl.Int64,
            })
        ticker = response.get("ticker", symbol)
        return (
            pl.DataFrame(results)
            .rename({"v": "volume", "vw": "vwap", "o": "open", "c": "close",
                     "h": "high", "l": "low", "t": "timestamp", "n": "transactions"})
            .with_columns(
                pl.col("timestamp").cast(pl.Datetime("ms", "UTC")).alias("timestamp"),
                pl.lit(ticker).alias("symbol"),
                pl.lit(f"{multiplier}{timespan}").alias("interval"),
            )
            .select(["timestamp", "symbol", "interval", "open", "high", "low",
                     "close", "volume", "vwap", "transactions"])
        )

    def _fetch_and_store(
        self,
        symbol: str,
        from_dt: datetime.datetime,
        to_dt: datetime.datetime,
        multiplier: int,
        timespan: str,
        conn: duckdb.DuckDBPyConnection,
    ):
        response = self.get_aggregate_bars(symbol, from_dt, to_dt,
                                           multiplier=multiplier, timespan=timespan)
        df = self._parse_bars_response(response, symbol, multiplier, timespan)
        if not df.is_empty():
            insert_candles(df, conn)
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
            self._fetch_and_store(symbol, from_dt, to_dt, multiplier, timespan, conn)
        else:
            checked_from = _to_dt(coverage["checked_from"])
            checked_to   = _to_dt(coverage["checked_to"])

            if from_dt < checked_from:
                self._fetch_and_store(symbol, from_dt, checked_from, multiplier, timespan, conn)

            if to_dt > checked_to:
                self._fetch_and_store(symbol, checked_to, to_dt, multiplier, timespan, conn)

        return query_candles(conn, symbol, interval, from_dt, to_dt)

    def get_daily_open_close(
        self,
        symbol: str,
        date: datetime.date | datetime.datetime | str,
        adjusted: bool = True,
    ) -> dict:
        return self.client.get_daily_open_close(symbol, date, adjusted=adjusted)

    def get_previous_close(self, ticker: str, adjusted: bool = True) -> dict:
        return self.client.get_previous_close(ticker, adjusted=adjusted)

    # ------------------------------------------------------------------
    # Trades & quotes
    # ------------------------------------------------------------------

    def get_trades(
        self,
        option_symbol: str,
        timestamp: Optional[str] = None,
        sort: str = "timestamp",
        order: str = "asc",
        limit: int = 5000,
        all_pages: bool = False,
    ):
        return self.client.get_trades(
            option_symbol,
            timestamp=timestamp,
            sort=sort,
            order=order,
            limit=limit,
            all_pages=all_pages,
        )

    def get_quotes(
        self,
        option_symbol: str,
        timestamp: Optional[str] = None,
        sort: str = "timestamp",
        order: str = "asc",
        limit: int = 5000,
        all_pages: bool = False,
    ):
        return self.client.get_quotes(
            option_symbol,
            timestamp=timestamp,
            sort=sort,
            order=order,
            limit=limit,
            all_pages=all_pages,
        )

    def get_last_trade(self, ticker: str) -> dict:
        return self.client.get_last_trade(ticker)

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def get_snapshot(
        self,
        underlying_symbol: str,
        option_symbol: str,
        all_pages: bool = False,
    ):
        return self.client.get_snapshot(
            underlying_symbol,
            option_symbol,
            all_pages=all_pages,
        )

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
        return self.client.get_sma(
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
        return self.client.get_ema(
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
        return self.client.get_rsi(
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
        return self.client.get_macd(
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
