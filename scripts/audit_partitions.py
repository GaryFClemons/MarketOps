import hashlib
from pathlib import Path
import pandas as pd

for part in sorted(Path("data/raw/ohlcv").glob("dt=*")):
    expected = part.name.removeprefix("dt=")
    f = part / "ohlcv.parquet"
    if not f.exists():
        print(f"{part.name}: NO FILE")
        continue
    dates = sorted(pd.read_parquet(f, columns=["date"])["date"].astype(str).unique())
    digest = hashlib.sha256(f.read_bytes()).hexdigest()[:12]
    print(f"{part.name}: {'ok' if dates == [expected] else f'MISMATCH {dates}'}  {digest}")