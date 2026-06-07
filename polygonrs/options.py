from __future__ import annotations

import datetime
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

import polars as pl
import duckdb
import polygon
import requests
from polygon.options.options import (
    OptionsClient,
    build_option_symbol,
    convert_option_symbol_formats,
    detect_option_symbol_format,
    parse_option_symbol,
)
from polygon import ReferenceClient

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


def _flatten_snapshot(r: dict, today: datetime.date) -> dict:
    d = r.get("details", {})
    g = r.get("greeks", {})
    day = r.get("day", {})
    q = r.get("last_quote", {})
    ua = r.get("underlying_asset", {})
    expiry_str = d.get("expiration_date")
    dte = (datetime.date.fromisoformat(expiry_str) - today).days if expiry_str else None
    return {
        "ticker": d.get("ticker"),
        "underlying_ticker": ua.get("ticker"),
        "contract_type": d.get("contract_type"),
        "exercise_style": d.get("exercise_style"),
        "expiration_date": expiry_str,
        "strike_price": (
            float(d["strike_price"]) if d.get("strike_price") is not None else None
        ),
        "shares_per_contract": d.get("shares_per_contract"),
        "dte": dte,
        "implied_volatility": r.get("implied_volatility"),
        "open_interest": r.get("open_interest"),
        "delta": g.get("delta"),
        "gamma": g.get("gamma"),
        "theta": g.get("theta"),
        "vega": g.get("vega"),
        "bid": q.get("bid"),
        "ask": q.get("ask"),
        "underlying_price": ua.get("price"),
        "change_to_break_even": ua.get("change_to_break_even"),
        "day_open": day.get("open"),
        "day_high": day.get("high"),
        "day_low": day.get("low"),
        "day_close": day.get("close"),
        "day_volume": day.get("volume"),
        "day_vwap": day.get("vwap"),
        "day_change_pct": day.get("change_percent"),
    }


def _empty_snapshot_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "ticker": pl.String,
            "underlying_ticker": pl.String,
            "contract_type": pl.String,
            "exercise_style": pl.String,
            "expiration_date": pl.String,
            "strike_price": pl.Float64,
            "shares_per_contract": pl.Int64,
            "dte": pl.Int64,
            "implied_volatility": pl.Float64,
            "open_interest": pl.Int64,
            "delta": pl.Float64,
            "gamma": pl.Float64,
            "theta": pl.Float64,
            "vega": pl.Float64,
            "bid": pl.Float64,
            "ask": pl.Float64,
            "underlying_price": pl.Float64,
            "change_to_break_even": pl.Float64,
            "day_open": pl.Float64,
            "day_high": pl.Float64,
            "day_low": pl.Float64,
            "day_close": pl.Float64,
            "day_volume": pl.Int64,
            "day_vwap": pl.Float64,
            "day_change_pct": pl.Float64,
        }
    )


def _is_rate_limit(response) -> bool:
    if isinstance(response, dict) and response.get("status") == "ERROR":
        return "exceeded" in response.get("error", "").lower()
    if isinstance(response, list) and response and isinstance(response[0], dict):
        first = response[0]
        if first.get("status") == "ERROR":
            return "exceeded" in first.get("error", "").lower()
    return False


def _call_with_retry(fn, *args, retries: int = 3, wait: float = 60.0, **kwargs):
    for attempt in range(retries):
        try:
            result = fn(*args, **kwargs)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < retries - 1:
                logger.warning(
                    "Network error (%s) — retrying in 5s... (%d/%d)",
                    type(e).__name__,
                    attempt + 1,
                    retries - 1,
                )
                time.sleep(5)
                continue
            raise

        if not _is_rate_limit(result):
            return result
        if attempt < retries - 1:
            logger.warning(
                "Rate limit hit — waiting %.0fs then retrying... (%d/%d)",
                wait,
                attempt + 1,
                retries - 1,
            )
            time.sleep(wait)
    raise RuntimeError("Rate limit exceeded after max retries.")


class Options:
    """Wrapper around polygon.io OptionsClient."""

    def __init__(self, api_key: str, use_async: bool = False):
        self.client = OptionsClient(api_key, use_async=use_async)
        self.ref = ReferenceClient(api_key, use_async=use_async)

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
        return parse_option_symbol(
            option_symbol, _format=fmt, output_format=output_format
        )

    def convert_symbol(
        self, option_symbol: str, from_format: str, to_format: str
    ) -> str:
        return convert_option_symbol_formats(option_symbol, from_format, to_format)

    def detect_symbol_format(self, option_symbol: str) -> str | bool | list:
        return detect_option_symbol_format(option_symbol)

    # ------------------------------------------------------------------
    # Contracts
    # ------------------------------------------------------------------
    def get_contracts(
        self,
        symbol: str,
        all_pages: bool = True,
        max_dte: int = None,
    ) -> pl.DataFrame:
        logger.debug("Fetching contracts for %s (max_dte=%s)", symbol, max_dte)
        today = datetime.date.today()
        expiry_lte = (
            (today + datetime.timedelta(days=max_dte)).isoformat()
            if max_dte is not None
            else None
        )
        contracts = _call_with_retry(
            self.ref.get_option_contracts,
            underlying_ticker=symbol,
            expiration_date_lte=expiry_lte,
            all_pages=all_pages,
        )

        # Non-rate-limit API error
        if isinstance(contracts, dict) and contracts.get("status") == "ERROR":
            raise RuntimeError(f"[{symbol}] API error: {contracts.get('error')}")

        # Filter out any error dicts mixed into a list response
        contracts = [c for c in contracts if "expiration_date" in c]
        if not contracts:
            logger.warning("No contracts returned for %s", symbol)
            return pl.DataFrame(
                schema={
                    "ticker": pl.String,
                    "underlying_ticker": pl.String,
                    "contract_type": pl.String,
                    "expiration_date": pl.String,
                    "strike_price": pl.Float64,
                    "dte": pl.Int64,
                }
            )

        df = pl.DataFrame(contracts).with_columns(
            (pl.col("expiration_date").str.to_date() - pl.lit(today))
            .dt.total_days()
            .alias("dte")
        )
        if max_dte is not None:
            df = df.filter(pl.col("dte") <= max_dte)

        logger.info("Got %d contracts for %s", len(df), symbol)
        return df

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
        return self._parse_bars_response(response, symbol, multiplier, timespan)

    def _parse_bars_response(
        self, response: dict, symbol: str, multiplier: int, timespan: str
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
        df = self._parse_bars_response(response, symbol, multiplier, timespan)
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

    # ------------------------------------------------------------------
    # Bulk snapshot
    # ------------------------------------------------------------------

    def get_bulk_snapshot(
        self,
        symbol: str,
        max_dte: int = None,
        contract_type: str = None,
        moneyness: str = None,
        strike_gte: float = None,
        strike_lte: float = None,
    ) -> pl.DataFrame:
        """
        Fetch all option contracts for an underlying in one call per page,
        including greeks, IV, open interest, and day OHLCV.
        Uses /v3/snapshot/options/{underlying} directly.

        contract_type: "call", "put", "both", or None (same as "both")
        moneyness:     "itm", "otm", "both", or None — filters by abs(delta) > 0.5
        """
        if contract_type == "both":
            contract_type = None
        if moneyness == "both":
            moneyness = None

        today = datetime.date.today()
        params: dict = {"limit": 250, "apiKey": self.client.KEY}
        if max_dte is not None:
            params["expiration_date.lte"] = (
                today + datetime.timedelta(days=max_dte)
            ).isoformat()
        if contract_type is not None:
            params["contract_type"] = contract_type
        if strike_gte is not None:
            params["strike_price.gte"] = strike_gte
        if strike_lte is not None:
            params["strike_price.lte"] = strike_lte

        url = f"https://api.polygon.io/v3/snapshot/options/{symbol}"
        raw: list[dict] = []

        while url:
            logger.debug("Snapshot page: %s", url)
            data = _call_with_retry(
                lambda u, p: requests.get(u, params=p, timeout=30).json(),
                url,
                params,
            )
            if data.get("status") == "ERROR":
                raise RuntimeError(f"[{symbol}] snapshot error: {data.get('error')}")
            raw.extend(data.get("results", []))
            url = data.get("next_url")
            params = {
                "apiKey": self.client.KEY
            }  # next_url already carries other params

        if not raw:
            logger.warning("No snapshot results for %s", symbol)
            return _empty_snapshot_df()

        rows = [_flatten_snapshot(r, today) for r in raw]
        df = pl.DataFrame(rows, infer_schema_length=len(rows))

        if moneyness == "itm":
            df = df.filter(pl.col("delta").abs() > 0.5)
        elif moneyness == "otm":
            df = df.filter(pl.col("delta").abs() < 0.5)

        logger.info(
            "Snapshot: %d contracts for %s (moneyness=%s)",
            len(df),
            symbol,
            moneyness or "both",
        )
        return df

    # ------------------------------------------------------------------
    # Screener
    # ------------------------------------------------------------------
    def screen_options(
        self,
        symbols: list[str],
        max_dte: int = None,
        contract_type: str = None,
        moneyness: str = None,
        side: str = None,
        delta: tuple[float | None, float | None] | None = None,
    ) -> pl.DataFrame:
        logger.info("Screening %d symbols: %s", len(symbols), symbols)
        frames = []
        for s in symbols:
            try:
                frames.append(
                    self.get_bulk_snapshot(
                        s,
                        max_dte=max_dte,
                        contract_type=contract_type,
                        moneyness=moneyness,
                    )
                )
            except Exception as e:
                logger.error("Failed to fetch snapshot for %s: %s", s, e)

        if not frames:
            return _empty_snapshot_df()

        target_schema = _empty_snapshot_df().schema
        normalized = []
        for frame in frames:
            if frame.is_empty():
                continue
            try:
                normalized.append(
                    frame.cast(
                        {
                            col: dtype
                            for col, dtype in target_schema.items()
                            if col in frame.columns
                        }
                    )
                )
            except Exception as e:
                logger.warning(
                    "Schema normalization failed for a frame, skipping: %s", e
                )

        if not normalized:
            return _empty_snapshot_df()

        df = pl.concat(normalized, how="diagonal")

        if side == "short":
            # Collateral = strike × 100 (cash-secured puts / calls at strike obligation).
            # Covered calls and naked strategies require broker-specific margin inputs
            # not available from snapshot data — expand here for spreads/condors etc.
            df = df.with_columns(
                [
                    (1 - pl.col("delta").abs()).alias("pop"),
                    (
                        (pl.col("bid") / (pl.col("strike_price") * 100))
                        * (365 / pl.col("dte"))
                    ).alias("annual_yield"),
                ]
            ).with_columns(
                [
                    # Expected yield: probability-weighted return
                    (pl.col("annual_yield") * pl.col("pop")).alias("expected_yield"),
                    # Yield per unit of assignment risk: higher = better risk-adjusted
                    (pl.col("annual_yield") / pl.col("delta").abs()).alias("yield_per_risk"),
                ]
            ).sort("expected_yield", descending=True)
        elif side == "long":
            # breakeven_move_pct: how far the underlying must move (as a fraction)
            # for the position to break even at expiry. Polygon provides change_to_break_even
            # in dollar terms; dividing by underlying_price gives the relative move needed.
            df = df.with_columns(
                [
                    pl.col("delta").abs().alias("pop"),
                    (
                        pl.col("change_to_break_even").abs()
                        / pl.col("underlying_price")
                    ).alias("breakeven_move_pct"),
                ]
            )
        elif side is not None:
            raise ValueError(f"side must be 'long' or 'short', got {side!r}")

        if delta is not None:
            d_min, d_max = delta
            abs_delta = pl.col("delta").abs()
            mask = pl.lit(True)
            if d_min is not None:
                mask = mask & (abs_delta >= d_min)
            if d_max is not None:
                mask = mask & (abs_delta <= d_max)
            df = df.filter(mask)

        logger.info("screen_options complete — %d total contracts", len(df))
        return df
