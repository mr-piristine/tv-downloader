import json
import re
from typing import List, Dict, Any

class TVProtocol:
    @staticmethod
    def encode(message: Dict[str, Any]) -> str:
        payload = json.dumps(message, separators=(',', ':'))
        return f"~m~{len(payload)}~m~{payload}"

    @staticmethod
    def decode(raw_data: str) -> List[Dict[str, Any]]:
        parsed_messages = []
        packets = re.split(r"~m~\d+~m~", raw_data)
        for packet in packets:
            if not packet.strip():
                continue
            try:
                parsed_messages.append(json.loads(packet))
            except json.JSONDecodeError:
                continue
        return parsed_messages

    @staticmethod
    def create_session_packet(session_id: str, prefix: str = "cs_") -> str:
        return TVProtocol.encode({
            "m": "chart_create_session",
            "p": [f"{prefix}{session_id}", ""]
        })

    @staticmethod
    def create_auth_packet(token: str = "unauthorized_user_token") -> str:
        """TradingView requires an explicit authorization state broadcast."""
        return TVProtocol.encode({
            "m": "set_auth_token",
            "p": [token]
        })

    @staticmethod
    def create_resolve_packet(session_id: str, symbol_id: str, asset_ticker: str, prefix: str = "cs_") -> str:
        return TVProtocol.encode({
            "m": "resolve_symbol",
            "p": [f"{prefix}{session_id}", symbol_id, f"={json.dumps({'symbol': asset_ticker, 'adjustment': 'splits'})}"]
        })

    @staticmethod
    def create_series_packet(session_id: str, symbol_id: str, series_id: str, interval: str, bars: int, prefix: str = "cs_") -> str:
        return TVProtocol.encode({
            "m": "create_series",
            "p": [f"{prefix}{session_id}", series_id, "s1", symbol_id, interval, bars]
        })