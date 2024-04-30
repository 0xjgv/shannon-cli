import argparse
import asyncio

from shannon_cli.config import Pair
from shannon_cli.crypto_providers.binance import BinanceSignalClient
from shannon_cli.strategies.dca_strategy import DCAStrategy

PAIRS = list(Pair)


async def get_dca_signals():
    print("Checking signals...")

    # Check signals for all pairs
    async with BinanceSignalClient(
        cache_file_results_seconds=60,  # 1 minute
        kline_limit=360,  # 1 year
    ) as binance_client:
        dca_strategy = DCAStrategy(client=binance_client)
        if pairs := await dca_strategy.check_signals(PAIRS):
            print("\nFound signals:")
            for pair in pairs:
                print(pair)
        else:
            print("\nNo signals found.")
        print("Done")


def main():
    parser = argparse.ArgumentParser(description="Find signals for DCA strategy.")
    parser.add_argument("action", choices=["dca_signals"])
    args = parser.parse_args()

    if args.action == "dca_signals":
        asyncio.run(get_dca_signals())


if __name__ == "__main__":
    main()
