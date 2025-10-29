import os
import requests
import threading
from faker import Faker

fake = Faker()

url = os.getenv("SUBMIT_URL", "https://workshop.grizzlyfrog.com/api/submit")
name = os.getenv("NAME", fake.name())


def post_async(payload: dict):
    """Send POST in a background thread (fire and forget)."""

    def _run():
        try:
            print(f"🧩 Submit event: {payload}")
            requests.post(url, json={**payload, "name": name}, timeout=2)
        except Exception:
            pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()


def post_sync(payload: dict):
    """Send POST synchronously."""
    print(f"🧩 Submit event: {payload}")
    try:
        requests.post(url, json={**payload, "name": name}, timeout=5)
    except Exception:
        pass
