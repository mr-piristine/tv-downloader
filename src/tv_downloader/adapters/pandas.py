import pandas as pd
from typing import List
from ..models import Candle

class PandasAdapter:
    @staticmethod
    def to_dataframe(candles: List[Candle]) -> pd.DataFrame:
        """Transforms object schemas to a structured Pandas DataFrame."""
        if not candles:
            return pd.DataFrame()
            
        data = [{
            "datetime": c.timestamp,
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close,
            "volume": c.volume
        } for c in candles]
        
        df = pd.DataFrame(data)
        df.set_index("datetime", inplace=True)
        return df
    


