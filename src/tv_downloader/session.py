import random
import string
import requests
from typing import Optional

class TVSession:
    BASE_URL = "https://www.tradingview.com"

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        self.username = username
        self.password = password
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Origin": "https://www.tradingview.com"
        }
        self.cookies = {}
        self._initialize_session()

    def _initialize_session(self):
        """Pre-warms the session with baseline tracking cookies and handles authentication."""
        session = requests.Session()
        session.headers.update(self.headers)
        
        # Hit the home page first to grab mandatory cross-site tracking cookies
        try:
            session.get(self.BASE_URL, timeout=10)
        except Exception:
            pass # Fallback in case of slow routing
        
        if self.username and self.password:
            login_url = f"{self.BASE_URL}/accounts/signin/"
            payload = {"username": self.username, "password": self.password}
            response = session.post(login_url, data=payload, timeout=10)
            if response.status_code != 200:
                raise PermissionError("Failed to authenticate with TradingView credentials.")
            
        self.cookies = session.cookies.get_dict()

    @staticmethod
    def generate_random_string(length: int = 12) -> str:
        return "".join(random.choice(string.ascii_lowercase) for _ in range(length))