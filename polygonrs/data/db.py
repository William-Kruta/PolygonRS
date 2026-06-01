import datetime
import polars as pl
import duckdb
from .config import get_db_path


def _init_tables(db_path: str = None) -> duckdb.DuckDBPyConnection:
    if db_path is None:
        db_path = str(get_db_path())
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS option_candles (
            timestamp       TIMESTAMPTZ NOT NULL,
            symbol          VARCHAR     NOT NULL,
            interval        VARCHAR     NOT NULL,
            open            DOUBLE,
            high            DOUBLE,
            low             DOUBLE,
            close           DOUBLE,
            volume          BIGINT,
            vwap            DOUBLE,
            transactions    BIGINT,
            PRIMARY KEY (timestamp, symbol, interval)
        );

        CREATE TABLE IF NOT EXISTS option_snapshots (
            collected_at        TIMESTAMPTZ NOT NULL,
            symbol              VARCHAR     NOT NULL,
            underlying_symbol   VARCHAR     NOT NULL,
            expiry              DATE        NOT NULL,
            strike              DOUBLE      NOT NULL,
            option_type         VARCHAR     NOT NULL,
            bid                 DOUBLE,
            ask                 DOUBLE,
            last_price          DOUBLE,
            volume              BIGINT,
            open_interest       BIGINT,
            implied_volatility  DOUBLE,
            delta               DOUBLE,
            gamma               DOUBLE,
            theta               DOUBLE,
            vega                DOUBLE,
            in_the_money        BOOLEAN,
            PRIMARY KEY (collected_at, symbol)
        );

        CREATE TABLE IF NOT EXISTS symbol_coverage (
            symbol          VARCHAR     NOT NULL,
            interval        VARCHAR     NOT NULL,
            checked_from    TIMESTAMPTZ NOT NULL,
            checked_to      TIMESTAMPTZ NOT NULL,
            min_date        TIMESTAMPTZ,
            max_date        TIMESTAMPTZ,
            updated_at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (symbol, interval)
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_option_candles_symbol_interval
            ON option_candles (symbol, interval);
        CREATE INDEX IF NOT EXISTS idx_option_snapshots_underlying
            ON option_snapshots (underlying_symbol, collected_at);
        CREATE INDEX IF NOT EXISTS idx_symbol_coverage_symbol
            ON symbol_coverage (symbol);
        """
    )
    return conn


# ------------------------------------------------------------------
# Coverage helpers
# ------------------------------------------------------------------

def get_coverage(conn: duckdb.DuckDBPyConnection, symbol: str, interval: str) -> dict | None:
    row = conn.execute(
        """
        SELECT checked_from, checked_to, min_date, max_date
        FROM symbol_coverage
        WHERE symbol = ? AND interval = ?
        """,
        [symbol, interval],
    ).fetchone()
    if row is None:
        return None
    return {
        "checked_from": row[0],
        "checked_to":   row[1],
        "min_date":     row[2],
        "max_date":     row[3],
    }


def upsert_coverage(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    interval: str,
    checked_from: datetime.datetime,
    checked_to: datetime.datetime,
):
    row = conn.execute(
        """
        SELECT MIN(timestamp), MAX(timestamp)
        FROM option_candles
        WHERE symbol = ? AND interval = ?
        """,
        [symbol, interval],
    ).fetchone()
    min_date, max_date = row

    conn.execute(
        """
        INSERT INTO symbol_coverage (symbol, interval, checked_from, checked_to, min_date, max_date, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (symbol, interval) DO UPDATE SET
            checked_from = LEAST(excluded.checked_from, symbol_coverage.checked_from),
            checked_to   = GREATEST(excluded.checked_to, symbol_coverage.checked_to),
            min_date     = excluded.min_date,
            max_date     = excluded.max_date,
            updated_at   = excluded.updated_at
        """,
        [symbol, interval, checked_from, checked_to, min_date, max_date],
    )


# ------------------------------------------------------------------
# Query helpers
# ------------------------------------------------------------------

def query_candles(
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    interval: str,
    from_date: datetime.datetime,
    to_date: datetime.datetime,
) -> pl.DataFrame:
    return conn.execute(
        """
        SELECT timestamp, symbol, interval, open, high, low, close, volume, vwap, transactions
        FROM option_candles
        WHERE symbol = ? AND interval = ?
          AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
        """,
        [symbol, interval, from_date, to_date],
    ).pl()


# ------------------------------------------------------------------
# Insert helpers
# ------------------------------------------------------------------

def insert_data(
    df: pl.DataFrame,
    db_cols: list,
    table_name: str,
    conn: duckdb.DuckDBPyConnection,
    pk_cols: list = None,
):
    if df.is_empty():
        return
    final_cols = [c for c in db_cols if c in df.columns]
    df = df.select(final_cols)
    col_names = ", ".join(final_cols)

    if pk_cols:
        pk_cols_in_df = [c for c in pk_cols if c in df.columns]
        if pk_cols_in_df:
            df = df.unique(subset=pk_cols_in_df, keep="first")
        pk_where = " AND ".join([f"existing.{c} = df.{c}" for c in pk_cols])
        conn.execute(
            f"""
            INSERT INTO {table_name} ({col_names})
            SELECT {col_names} FROM df
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} existing
                WHERE {pk_where}
            )
            """
        )
    else:
        conn.execute(
            f"INSERT INTO {table_name} ({col_names}) SELECT {col_names} FROM df"
        )


CANDLE_COLS = [
    "timestamp", "symbol", "interval",
    "open", "high", "low", "close",
    "volume", "vwap", "transactions",
]

SNAPSHOT_COLS = [
    "collected_at", "symbol", "underlying_symbol", "expiry", "strike",
    "option_type", "bid", "ask", "last_price", "volume", "open_interest",
    "implied_volatility", "delta", "gamma", "theta", "vega", "in_the_money",
]


def insert_candles(df: pl.DataFrame, conn: duckdb.DuckDBPyConnection):
    insert_data(df, CANDLE_COLS, "option_candles", conn, pk_cols=["timestamp", "symbol", "interval"])


def insert_snapshots(df: pl.DataFrame, conn: duckdb.DuckDBPyConnection):
    insert_data(df, SNAPSHOT_COLS, "option_snapshots", conn, pk_cols=["collected_at", "symbol"])
