# PolygonRS

A Python wrapper around the [Polygon.io](https://polygon.io) API with local DuckDB caching.

## Installation

```bash
uv add polygonrs
```

## Setup

Create a `.env` file in your project root:

```
API_KEY=your_polygon_api_key
```

---

## Options

```python
from polygonrs.options import Options
from polygonrs.data.db import _init_tables

op = Options(api_key)
conn = _init_tables()  # optional, enables caching
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

### Technical Indicators

```python
op.get_sma(symbol, timespan="day", window_size=50)
op.get_ema(symbol, timespan="day", window_size=50)
op.get_rsi(symbol, timespan="day", window_size=14)
op.get_macd(symbol, timespan="day", short_window=12, long_window=26, signal_window=9)
```

---

## Caching

When a DuckDB connection is passed to `get_aggregate_bars_df`, the library caches results locally and only calls the API for date ranges not yet stored.

```python
conn = _init_tables()  # default: ~/.local/share/polygonrs/polygon_rs.db

# First call — full API request, result stored in DB
df = op.get_aggregate_bars_df(symbol, "2026-01-01", "2026-04-20", conn=conn)

# Second call — only fetches the new forward range from the API
df = op.get_aggregate_bars_df(symbol, "2026-01-01", "2026-05-30", conn=conn)

# Third call — entirely within cached range, no API call made
df = op.get_aggregate_bars_df(symbol, "2026-02-01", "2026-03-01", conn=conn)
```

### DB location

Override the default path via environment variable or config file:

```bash
POLYGON_RS_DB=/path/to/custom.db
```

Or set `"database"` in `~/.local/share/polygonrs/config.json`.
