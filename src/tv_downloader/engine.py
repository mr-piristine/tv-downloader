import websocket
import ssl
import time
from datetime import datetime
from typing import List, Optional, Union
from .models import Asset, Interval, Candle, HistoryResultSet
from .session import TVSession
from .protocol import TVProtocol

class HistoricalDownloader:
    WS_URL = "wss://data.tradingview.com/socket.io/websocket"

    def __init__(self, session: Optional[TVSession] = None):
        self.session = session if session else TVSession()

    def fetch(self, asset: Asset, interval: Union[Interval, str], n_bars: int = 100) -> HistoryResultSet:
        """Connects, streams and maps TradingView's history payloads."""
        # Cleanly resolve strings to your custom Enum types instantly
        if isinstance(interval, str):
            interval = Interval.from_str(interval)
            
        # ... rest of your engine websocket code remains exactly identical ...

        # def fetch(self, asset: Asset, interval: Interval, n_bars: int = 100) -> List[Candle]:
        # """Connects, streams and maps TradingView's split timeline history payloads."""
        ws = websocket.WebSocket(sslopt={"cert_reqs": ssl.CERT_NONE})
        
        headers = {
            "User-Agent": self.session.headers["User-Agent"],
            "Origin": "https://www.tradingview.com"
        }

        cookie_str = "; ".join([f"{k}={v}" for k, v in self.session.cookies.items()]) if self.session.cookies else None
        ws.connect(self.WS_URL, header=[f"{k}: {v}" for k, v in headers.items()], cookie=cookie_str)
        ws.settimeout(2.0)

        chart_session = TVSession.generate_random_string()
        symbol_id = "sds_sym_1"
        series_id = "sds_ser_1"
        candles: List[Candle] = []

        try:
            # Read socket welcome signature
            _ = ws.recv()
            
            # Send initialization protocol
            ws.send(TVProtocol.create_auth_packet())
            ws.send(TVProtocol.create_session_packet(chart_session))
            ws.send(TVProtocol.create_resolve_packet(chart_session, symbol_id, asset.tv_ticker))
            ws.send(TVProtocol.create_series_packet(chart_session, symbol_id, series_id, interval.value, n_bars))

            start_time = time.time()
            data_received = False
            
            while not data_received:
                # 12-second max execution limit to prevent infinite hangs
                if time.time() - start_time > 12:
                    break
                
                try:
                    raw_packet = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                if "~h~" in raw_packet:
                    ws.send(raw_packet)
                    continue

                messages = TVProtocol.decode(raw_packet)

                for msg in messages:
                    if not isinstance(msg, dict):
                        continue
                    
                    method = msg.get("m")
                    params = msg.get("p", [])
                    
                    if not params or not isinstance(params, list):
                        continue

                    # Deep scan the payload for TradingView's explicit OHLCV 'v' blocks
                    # This bypasses channel structural discrepancies entirely.
                    for param in params:
                        if isinstance(param, dict):
                            # Look inside standard series wrappers or raw root structures
                            target_dict = param.get(series_id, param) if series_id in param else param
                            
                            if isinstance(target_dict, dict) and "s" in target_dict:
                                points = target_dict["s"]
                                if isinstance(points, list):
                                    for point in points:
                                        if isinstance(point, dict) and "v" in point:
                                            v = point["v"]
                                            if isinstance(v, list) and len(v) >= 6:
                                                try:
                                                    candles.append(Candle(
                                                        timestamp=datetime.fromtimestamp(int(v[0])),
                                                        open=float(v[1]),
                                                        high=float(v[2]),
                                                        low=float(v[3]),
                                                        close=float(v[4]),
                                                        volume=float(v[5])
                                                    ))
                                                except (ValueError, TypeError):
                                                    continue

                    # Break as soon as TradingView broadcasts completion status and data is stored
                    if method == "series_completed" or (len(candles) >= n_bars and n_bars > 0):
                        if candles:
                            data_received = True
                            break
        finally:
            ws.close()

        # # Deduplicate identical data points safely
        # seen = set()
        # unique_candles = []
        # for c in candles:
        #     if c.timestamp not in seen:
        #         seen.add(c.timestamp)
        #         unique_candles.append(c)

        # unique_candles.sort(key=lambda x: x.timestamp)
        # print(f"✅ Successfully collected {len(unique_candles)} candles.")
        # return unique_candles

        # Deduplicate and sort candles safely
        seen = set()
        unique_candles = []
        for c in candles:
            if c.timestamp not in seen:
                seen.add(c.timestamp)
                unique_candles.append(c)

        unique_candles.sort(key=lambda x: x.timestamp)
        print(f"✅ Successfully collected {len(unique_candles)} candles.")
        
        # Return the comprehensive structured result set instead of just a raw list
        return HistoryResultSet(
            asset=asset,
            interval=interval,
            candles=unique_candles
        )