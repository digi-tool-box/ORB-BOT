import asyncio
import sys
from html import escape

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

def send_telegram(message, token=None, chat_id=None):
    """Send a Telegram message. Safe to call - won't crash if token/chat_id missing."""
    if not token or not chat_id:
        return False
    if not HAS_REQUESTS:
        print("⚠️ Telegram: 'requests' module not installed. pip install requests")
        sys.stdout.flush()
        return False
    try:
        url = TELEGRAM_API.format(token=token)
        payload = {"chat_id": chat_id, "text": escape(message), "parse_mode": "HTML"}
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ Telegram send failed: {r.text[:100]}")
            sys.stdout.flush()
            return False
        return True
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")
        sys.stdout.flush()
        return False
