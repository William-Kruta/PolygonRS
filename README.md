# PolygonRS

A Python wrapper around the [Polygon.io](https://polygon.io) API with local DuckDB caching. Supports options chains, equities snapshots, OHLCV bars, greeks, and screening.

## Installation

```bash
pip install polygonrs
```

## Setup

Pass your Polygon.io API key directly when instantiating a client:

```python
from polygonrs import Options, Stocks, open_db

op = Options("your_polygon_api_key")
st = Stocks("your_polygon_api_key")
conn = open_db()  # optional — enables local caching
```

If you prefer to load keys from a `.env` file, install `python-dotenv` separately and call `load_dotenv()` before reading `os.getenv("API_KEY")` in your own script. The library itself does not depend on `python-dotenv`.

---

## Options

```python
from polygonrs import Options, open_db

op = Options(api_key)
conn = open_db()  # optional, enables caching
```

### Symbol Helpers

| Method | Description |
|---|---|
| `build_symbol(underlying, expiry, call_or_put, strike, prefix_o=False)` | Build an option symbol string |
| `parse_symbol(option_symbol)` | Parse a symbol into its components |
| `convert_symbol(option_symbol, from_format, to_format)` | Convert between symbol formats |
| `detect_symbol_format(option_symbol)` | Detect the format of a symbol string |

Supported formats: `polygon`, `tda`, `tos`, `ibkr`, `tradier`, `trade_station`

```python
symbol = op.build_symbol("KTOS", dt.date(2026, 5, 29), "put", 55, prefix_o=True)
# "O:KTOS260529P00055000"
```

### Contracts

```python
df = op.get_contracts("AAPL", max_dte=30)
```

Returns a Polars DataFrame with columns including `ticker`, `underlying_ticker`, `contract_type`, `expiration_date`, `strike_price`, `dte`.

### OHLCV / Aggregate Bars

```python
# Raw API call
df = op.get_aggregate_bars_df(symbol, from_date, to_date)

# With caching — only fetches missing date ranges from the API
df = op.get_aggregate_bars_df(symbol, from_date, to_date, conn=conn)
```

Returns a Polars DataFrame with columns: `timestamp, symbol, interval, open, high, low, close, volume, vwap, transactions`

| Parameter | Default | Description |
|---|---|---|
| `multiplier` | `1` | Timespan multiplier |
| `timespan` | `"day"` | `"minute"`, `"hour"`, `"day"`, `"week"`, `"month"` |
| `adjusted` | `True` | Adjust for splits |
| `conn` | `None` | DuckDB connection — enables local caching |

### Daily / Previous Close

```python
op.get_daily_open_close(symbol, date)   # OHLC for a specific date
op.get_previous_close(symbol)           # Previous trading day's OHLC
```

### Trades & Quotes

```python
op.get_trades(option_symbol)            # Trade tick data
op.get_quotes(option_symbol)            # NBBO quote tick data
op.get_last_trade(option_symbol)        # Most recent trade
```

### Snapshot

```python
op.get_snapshot(underlying_symbol, option_symbol)
# e.g. op.get_snapshot("KTOS", "O:KTOS260529P00055000")
```

### Bulk Snapshot

Fetches all contracts for an underlying in one paginated call, returning greeks, IV, open interest, and day OHLCV:

```python
df = op.get_bulk_snapshot(
    "AAPL",
    max_dte=30,
    contract_type="put",   # "call", "put", or None
    moneyness="otm",       # "itm", "otm", or None
    strike_gte=150.0,
    strike_lte=200.0,
)
```

### Screener

Screen multiple tickers at once. Adds side-specific analytics columns when `side` is set:

```python
df = op.screen_options(
    ["AAPL", "MSFT", "NVDA"],
    max_dte=30,
    contract_type="put",
    moneyness="otm",
    side="short",          # "short" or "long"
    delta=(0.15, 0.30),    # abs(delta) range filter
)
```

**`side="short"` adds:** `pop`, `annual_yield`, `expected_yield`, `yield_per_risk` — sorted by `expected_yield` descending.

**`side="long"` adds:** `pop` (= abs(delta)), `breakeven_move_pct`.

### Technical Indicators

```python
op.get_sma(symbol, timespan="day", window_size=50)
op.get_ema(symbol, timespan="day", window_size=50)
op.get_rsi(symbol, timespan="day", window_size=14)
op.get_macd(symbol, timespan="day", short_window=12, long_window=26, signal_window=9)
```

---

## Stocks

```python
from polygonrs import Stocks, open_db

st = Stocks(api_key)
conn = open_db()
```

### OHLCV / Aggregate Bars

Same interface as Options — caching works identically:

```python
df = st.get_aggregate_bars_df("AAPL", from_date, to_date)
df = st.get_aggregate_bars_df("AAPL", from_date, to_date, conn=conn)
```

### Grouped Daily Bars

```python
df = st.get_grouped_daily_bars("2026-06-05")  # all tickers for a given date
```

### Daily / Previous Close

```python
st.get_daily_open_close("AAPL", "2026-06-05")
st.get_previous_close("AAPL")
```

### Trades & Quotes

```python
st.get_trades("AAPL")
st.get_quotes("AAPL")
st.get_last_trade("AAPL")
st.get_last_quote("AAPL")
```

### Snapshots

```python
st.get_snapshot("AAPL")                           # single ticker
st.get_snapshots(["AAPL", "MSFT", "NVDA"])        # batched, returns Polars DataFrame
st.get_gainers_losers(direction="gainers")         # top market movers
```

`get_snapshots` batches in groups of 250 (Polygon's per-request max). Requires a paid Polygon plan.

### Technical Indicators

```python
st.get_sma("AAPL", timespan="day", window_size=50)
st.get_ema("AAPL", timespan="day", window_size=50)
st.get_rsi("AAPL", timespan="day", window_size=14)
st.get_macd("AAPL", timespan="day", short_window=12, long_window=26, signal_window=9)
```

---

## Greeks Calculator

Local Black-Scholes-Merton calculations via `py-vollib` — no API call required:

```python
from polygonrs.utils.greeks import calc_iv, calc_greeks, calc_greeks_from_price

# Back out IV from a market price
iv = calc_iv(
    option_price=3.50,
    underlying_price=150.0,
    strike=145.0,
    dte=21,
    flag="p",              # 'c' for call, 'p' for put
)

# Compute greeks given a known IV
greeks = calc_greeks(
    underlying_price=150.0,
    strike=145.0,
    dte=21,
    sigma=0.30,
    flag="p",
)
# {"delta": ..., "gamma": ..., "theta": ..., "vega": ..., "rho": ...}

# Convenience: IV + all greeks in one call
result = calc_greeks_from_price(
    option_price=3.50,
    underlying_price=150.0,
    strike=145.0,
    dte=21,
    flag="p",
)
# {"iv": ..., "delta": ..., "gamma": ..., "theta": ..., "vega": ..., "rho": ...}
```

`theta` is returned as a daily value. `vega` and `rho` are per 1% move. Any field is `None` if the solver fails (e.g. price outside no-arbitrage bounds).

Override the default risk-free rate (4.5%):

```python
result = calc_greeks_from_price(..., risk_free_rate=0.05, dividend_yield=0.01)
```

---

## Caching

When a DuckDB connection is passed to `get_aggregate_bars_df`, the library caches results locally and only calls the API for date ranges not yet stored.

```python
conn = open_db()  # default: ~/.local/share/polygonrs/polygon_rs.db

# First call — full API request, result stored in DB
df = op.get_aggregate_bars_df(symbol, "2026-01-01", "2026-04-20", conn=conn)

# Second call — only fetches the new forward range from the API
df = op.get_aggregate_bars_df(symbol, "2026-01-01", "2026-05-30", conn=conn)

# Third call — entirely within cached range, no API call made
df = op.get_aggregate_bars_df(symbol, "2026-02-01", "2026-03-01", conn=conn)
```

Both `Options` and `Stocks` share the same `candles` table in the DB — symbol names are unique across the two asset classes so there is no collision.

### DB location

Override the default path via environment variable or config file:

```bash
POLYGON_RS_DB=/path/to/custom.db
```

Or set `"database"` in `~/.local/share/polygonrs/config.json`.
