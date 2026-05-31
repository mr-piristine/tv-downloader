import polars as pl
from ..models import HistoryResultSet

class PolarsAdapter:
    @staticmethod
    def to_dataframe(result_set: HistoryResultSet) -> pl.DataFrame:
        """Transforms a HistoryResultSet container into a fast Polars DataFrame 
        with metadata injected as columns.
        """
        if not result_set.candles:
            return pl.DataFrame()

        # 1. Structure the foundational time-series vectors
        data = {
            "datetime": [c.timestamp for c in result_set.candles],
            "open": [c.open for c in result_set.candles],
            "high": [c.high for c in result_set.candles],
            "low": [c.low for c in result_set.candles],
            "close": [c.close for c in result_set.candles],
            "volume": [c.volume for c in result_set.candles],
        }

        # 2. Convert to Polars native memory representation
        df = pl.DataFrame(data)

        # 3. Inject execution metadata columns using Polars expression literals
        df = df.with_columns([
            pl.lit(result_set.symbol).alias("symbol"),
            pl.lit(result_set.venue).alias("venue"),
            pl.lit(result_set.time_period).alias("time_period"),
            pl.lit(result_set.source).alias("source")
        ])

        return df

    @staticmethod
    def print_dataframe(result_set: HistoryResultSet) -> None:
        """Helper method that instantly prints the HistoryResultSet in Polars format."""
        df = PolarsAdapter.to_dataframe(result_set)
        if df.is_empty():
            print("Empty DataFrame (No candles collected).")
        else:
            print("\n--- High-Performance Polars DataFrame View ---")
            print(df)
            print("-----------------------------------------------\n") 