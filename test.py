import os
import datetime as dt
from dotenv import load_dotenv

from polygonrs.options import Options
from polygonrs.data.db import _init_tables

load_dotenv()

api_key = os.getenv("API_KEY")

op = Options(api_key)
conn = _init_tables()

symbol = op.build_symbol("KTOS", dt.date(2026, 5, 29), "put", 55, prefix_o=True)

# First call — hits API for full range, stores in DB + coverage
df = op.get_aggregate_bars_df(symbol, dt.date(2026, 1, 1), dt.date(2026, 4, 20), conn=conn)
print("Call 1:", df.shape)

# Second call — DB has up to 4/20, API called only for 4/20 → 5/30
df = op.get_aggregate_bars_df(symbol, dt.date(2026, 1, 1), dt.date(2026, 5, 30), conn=conn)
print("Call 2 (full range):")
print(df)

# Third call — fully within coverage, no API call
df = op.get_aggregate_bars_df(symbol, dt.date(2026, 4, 17), dt.date(2026, 5, 29), conn=conn)
print("Call 3 (cached):")
print(df)
