from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from binance import AsyncClient
from pydantic import BaseModel, Field

from shannon_cli.strategies.interfaces import SignalClient
from shannon_cli.utils import AsyncTTL

KLINES_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

# Used for local development & test
# CLI creates a new dir if not running from current file
# Fix: use current __file__ to navigate backwards to the root of this directory
# to fix into the "prices" folder.
PRICES_DIR = Path(__file__).parent.parent.parent / Path("prices")
if PRICES_DIR.exists():
    mtime = PRICES_DIR.stat().st_mtime
    now = datetime.now()
    print(now.timestamp() - mtime)  # in seconds
    if int(now.timestamp() - mtime) > (60 * 60 * 3):  # 3 hours
        for sub in PRICES_DIR.iterdir():
            sub.unlink()
PRICES_DIR.mkdir(exist_ok=True)


class KLineInterval(str, Enum):
    KLINE_INTERVAL_1MINUTE = "1m"
    KLINE_INTERVAL_3MINUTE = "3m"
    KLINE_INTERVAL_5MINUTE = "5m"
    KLINE_INTERVAL_15MINUTE = "15m"
    KLINE_INTERVAL_30MINUTE = "30m"
    KLINE_INTERVAL_1HOUR = "1h"
    KLINE_INTERVAL_2HOUR = "2h"
    KLINE_INTERVAL_4HOUR = "4h"
    KLINE_INTERVAL_6HOUR = "6h"
    KLINE_INTERVAL_8HOUR = "8h"
    KLINE_INTERVAL_12HOUR = "12h"
    KLINE_INTERVAL_1DAY = "1d"
    KLINE_INTERVAL_3DAY = "3d"
    KLINE_INTERVAL_1WEEK = "1w"
    KLINE_INTERVAL_1MONTH = "1M"


class SymbolInfo(BaseModel):
    quote_asset: str = Field(..., alias="quoteAsset")
    base_asset: str = Field(..., alias="baseAsset")
    symbol: str
    status: str


class MarketInfo(BaseModel):
    symbols: List[SymbolInfo]

    def pairs(self, quote_asset: str) -> Dict[str, SymbolInfo]:
        return {s.symbol: s for s in self.symbols if s.quote_asset == quote_asset}


class BinanceSignalClient(SignalClient):
    def __init__(
        self,
        kline_interval: KLineInterval = KLineInterval.KLINE_INTERVAL_1DAY,
        cache_file_results_seconds: int | None = 60 * 30,  # 30 minutes
        time_ago: str = "1 year ago UTC",
        kline_limit: int = 100,
    ) -> None:
        self.cache_results_seconds = cache_file_results_seconds
        self.time_ago = time_ago  # at least 100 klines
        self.interval = kline_interval.value
        self.limit = kline_limit

    async def __aenter__(self):
        self.client = await AsyncClient.create()
        return self

    async def __aexit__(self, *args):
        await self.client.close_connection()

    def _parse_trades(self, trades) -> pd.DataFrame:
        df = pd.DataFrame.from_records(
            trades,
            columns=KLINES_COLUMNS,
        )
        df["date"] = pd.to_datetime(df["date"], unit="ms", utc=False)
        return df.drop(["ignore"], axis=1).reset_index()

    async def _extract(self, trading_pair: str, feather_path: Path):
        # https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
        self.log(f"Getting {trading_pair} pair, interval: {self.interval}")
        trades = await self.client.get_klines(
            interval=self.interval,
            symbol=trading_pair,
            limit=self.limit,
        )
        df = self._parse_trades(trades)
        if self.cache_results_seconds is not None:
            df.to_feather(feather_path.as_posix())
        return df

    async def _extract_historical(self, trading_pair: str, feather_path: Path):
        # Examples
        # (client.KLINE_INTERVAL_3MINUTE, "6 months ago UTC"),
        # (client.KLINE_INTERVAL_5MINUTE, "20 minutes ago UTC"),
        self.log(
            f"Downloading {trading_pair}, interval: {self.interval} - {self.time_ago}"
        )
        trades = await self.client.get_historical_klines(
            trading_pair,
            self.interval,
            self.time_ago,
        )
        df = self._parse_trades(trades)
        df.to_feather(feather_path.as_posix())
        return df

    def _check_file_cache(self, feather_path: Path) -> Optional[pd.DataFrame]:
        if self.cache_results_seconds is not None and feather_path.exists():
            mtime = feather_path.stat().st_mtime
            now = datetime.now()
            if int(now.timestamp() - mtime) < self.cache_results_seconds:
                return pd.read_feather(feather_path)

    @AsyncTTL(time_to_live=60 * 6, maxsize=256)
    async def get_pairs(self, quote_asset: str = "USDT") -> Dict[str, SymbolInfo]:
        # TODO: Check for more useful in the response
        market_info = await self.client.get_exchange_info()
        return MarketInfo(**market_info).pairs(quote_asset)

    @AsyncTTL(time_to_live=60 * 6, maxsize=256)
    async def get_historic_data(self, trading_pair: str) -> pd.DataFrame:
        today = datetime.today().date()
        feather_path = f"{trading_pair}_{today}_{self.interval}.feather".lower()
        prices_feather = Path(PRICES_DIR, feather_path)
        if isinstance(df := self._check_file_cache(prices_feather), pd.DataFrame):
            return df
        return await self._extract_historical(trading_pair, prices_feather)

    # Second layer of caching
    @AsyncTTL(time_to_live=60, maxsize=256, disable_logs=True)
    async def get_current_data(self, trading_pair: str) -> pd.DataFrame:
        feather_path = f"{trading_pair}_{self.interval}.feather".lower()
        prices_feather = Path(PRICES_DIR, feather_path)
        # Third layer of caching
        if isinstance(df := self._check_file_cache(prices_feather), pd.DataFrame):
            self.log(f"Using file cache for {trading_pair}")
            return df
        return await self._extract(trading_pair, prices_feather)
