from asyncio import Semaphore, TaskGroup, as_completed
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_ta as ta

from shannon_cli.crypto_providers.binance import BinanceSignalClient
from shannon_cli.strategies.interfaces import (
    Pair,
    PairSignal,
    PairSignalMetadata,
    SignalStrategy,
)
from shannon_cli.utils import TaskCache

# Capture concurrent requests in a single cache for all strategies
global_task_cache = TaskCache(ttl_seconds=30)


class DCAStrategy(SignalStrategy):
    def __init__(
        self,
        *,
        max_parallel_requests: int = 5,
        client: BinanceSignalClient,
    ) -> None:
        self.throttle = Semaphore(max_parallel_requests)
        self.current_date_str = str(datetime.now())
        self.client = client

    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Define the DCA strategy in this method"""
        df["close"] = df["close"].astype(float)
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)

        # RSI
        df["rsi"] = ta.rsi(df["close"], length=12)

        # Heikin Ashi
        # df.ta.ha(append=True)

        # Moving Averages
        df["sma_50"] = df["close"].rolling(window=50).mean()
        df["sma_200"] = df["close"].rolling(window=200).mean()

        # # MACD
        # ta.macd(df["close"])

        # Volume (you'll need to ensure your dataframe has a 'volume' column)
        df["volume_rolling_avg"] = df["volume"].rolling(window=20).mean()

        # Percentage from 50-day Moving Average
        df["pct_from_sma_50"] = (df["close"] - df["sma_50"]) / df["sma_50"] * 100

        return df.replace(0, np.nan).dropna(axis=0, how="all")

    async def _check_signal(self, trading_pair: Pair) -> PairSignal | None:
        # HERE: Bring db layer to DCAStrategy
        # Check if the pair is in the database and if values have changed

        # Here we check the buy using the injected patterns/indicators
        async with self.throttle:
            pair = trading_pair.value
            df = await self.client.get_current_data(pair)

            df = self._add_indicators(df)
            if df.empty:
                return None

            df.set_index("date", inplace=True)
            df_row_count = len(df.index)

            current_volume = float(df.iloc[-1]["volume"])
            current_price = df.iloc[-1]

            # Calculate days since highest and lowest price
            days_since_highest_price = len(df) - df.index.get_loc(df["close"].idxmax())
            days_since_lowest_price = len(df) - df.index.get_loc(df["close"].idxmin())

            # NOTE: Improve strategy with percentage difference of the lows & highs
            # to reduce false positives
            rsi = current_price.rsi.item() if current_price.rsi else -1

            current_close_price = df.iloc[-1]["close"]
            min_close_price = df["close"].min()
            max_close_price = df["close"].max()

            # Calculate percentage difference from high/low
            pct_diff_from_high = (
                (current_close_price - max_close_price) / max_close_price
            ) * 100
            pct_diff_from_low = (
                (current_close_price - min_close_price) / min_close_price
            ) * 100

            # Calculate percentage difference from sma50
            pct_diff_from_sma50 = (
                (current_close_price - current_price.sma_50.item())
                / current_price.sma_50.item()
            ) * 100

            # Current volume compared to the rolling average volume
            volume_rolling_avg = current_price["volume_rolling_avg"].item()
            volume_above_avg = True if current_volume > volume_rolling_avg else False

            # Expand the metadata information:
            metadata = PairSignalMetadata(
                **{
                    "data": df[["close", "close_time"]].to_dict(orient="records"),
                    "days_since_highest_price": days_since_highest_price,
                    "days_since_lowest_price": days_since_lowest_price,
                    "pct_diff_from_sma50": pct_diff_from_sma50,
                    "pct_diff_from_high": pct_diff_from_high,
                    "pct_diff_from_low": pct_diff_from_low,
                    "volume_above_avg": volume_above_avg,
                    "lowest_price": min_close_price,
                    "highest_price": max_close_price,
                    "date": self.current_date_str,
                    "days_count": df_row_count,
                }
            )

            # Check if current price is the lowest price up until today
            if current_close_price <= min_close_price or pct_diff_from_low < 3:
                self.log(pair, f"{min_close_price=} {current_close_price=}")
                return PairSignal(
                    close_price=current_price.close.item(),
                    metadata=metadata,
                    should_buy=True,
                    pair=pair,
                    rsi=rsi,
                )

            # Check if current price is the highest price up until today
            if current_close_price >= max_close_price or pct_diff_from_high > -3:
                self.log(pair, f"{max_close_price=} {current_close_price=}")
                return PairSignal(
                    close_price=current_price.close.item(),
                    metadata=metadata,
                    should_sell=True,
                    pair=pair,
                    rsi=rsi,
                )

    async def check_signals(self, pairs: list[Pair]) -> list[PairSignal]:
        async with TaskGroup() as tg:
            tasks = []
            for pair in pairs:
                task = tg.create_task(self._check_signal(pair))
                tasks.append(task)

            signals = []
            for signal_coro in as_completed(tasks):
                if pair_signal := await signal_coro:
                    signals.append(pair_signal)

            return signals
