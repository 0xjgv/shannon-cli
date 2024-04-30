from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager
from typing import Iterable

import pandas as pd
from pydantic import BaseModel, Field

from shannon_cli.config import Pair


class PairSignalData(BaseModel):
    close_time: int = Field(..., alias="close_time")
    close_price: float = Field(..., alias="close")


class PairSignalMetadata(BaseModel):
    days_since_highest_price: int
    days_since_lowest_price: int
    pct_diff_from_sma50: float
    data: list[PairSignalData]
    pct_diff_from_high: float
    pct_diff_from_low: float
    volume_above_avg: float
    highest_price: float
    lowest_price: float
    days_count: int
    date: str


class PairSignal(BaseModel):
    should_sell: bool | None = None
    should_buy: bool | None = None
    metadata: PairSignalMetadata
    close_price: float
    rsi: float
    pair: str

    def __str__(self) -> str:
        """Format the PairSignal message"""
        pct_diff = round(self.metadata.pct_diff_from_low, 2)
        action_direction = "buy" if self.should_buy else ""
        if self.should_sell:
            pct_diff = round(self.metadata.pct_diff_from_high, 2)
            action_direction = "sell"

        mvrv_z_score = round(self.metadata.mvrv_z_score, 2)
        close_price = self.close_price
        rsi = round(self.rsi, 2)

        return f"[{self.pair} {action_direction}] {close_price=} {rsi=} {mvrv_z_score=} {pct_diff=}"


class SignalStrategy(ABC):
    def log(self, *args, **kwargs):
        print(f"[{self.__class__.__name__} >]", *args, **kwargs)

    @abstractmethod
    async def check_signals(self, pairs: list[Pair] | None = None) -> Iterable: ...


class SignalClient(AbstractAsyncContextManager):
    def log(self, *args, **kwargs):
        print(f"[> {self.__class__.__name__}]", *args, **kwargs)

    @abstractmethod
    async def get_historic_data(self, trading_pair: str) -> pd.DataFrame: ...

    @abstractmethod
    async def get_current_data(self, trading_pair: str) -> pd.DataFrame: ...

    @abstractmethod
    async def get_pairs(self) -> Iterable[str]: ...
