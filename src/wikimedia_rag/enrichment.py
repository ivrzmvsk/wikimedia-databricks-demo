"""Sequential driver HTTP client with a global request budget (including retries)."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import time
import math
from urllib.parse import urljoin, urlparse

import requests

from wikimedia_rag.corpus import parse_summary, summary_url


class PageError(RuntimeError):
    pass


class BudgetExhausted(PageError):
    pass


class SummaryClient:
    def __init__(self, contact, max_attempts=3, retries=2, timeout=10, delay=1,
                 max_backoff=60, session=None, sleep=time.sleep, clock=time.monotonic):
        if not contact.strip() or any(c in contact for c in "\r\n"):
            raise ValueError("A valid operator contact is required")
        if not 1 <= max_attempts <= 100 or not 0 <= retries <= 3 or not 0 < timeout <= 60 or not 0.5 <= delay <= 60 or not 0 < max_backoff <= 300:
            raise ValueError("Invalid bounded HTTP settings")
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": f"WikimediaAssistant/1.0 ({contact})",
                                     "Accept": "application/json"})
        self.max_attempts, self.retries, self.timeout = max_attempts, retries, timeout
        self.delay, self.max_backoff = delay, max_backoff
        self.sleep, self.clock, self.next_request = sleep, clock, 0
        self.attempts = 0

    def fetch(self, title, fetched_at):
        retry, redirects, url = 0, 0, summary_url(title)
        while retry <= self.retries:
            if self.attempts >= self.max_attempts:
                raise BudgetExhausted("Total HTTP attempt budget exhausted")
            self.sleep(max(0, self.next_request - self.clock()))
            self.attempts += 1
            self.next_request = self.clock() + self.delay
            try:
                # Follow redirects explicitly so every request consumes the global budget.
                response = self.session.get(url, timeout=self.timeout, allow_redirects=False)
            except requests.RequestException as exc:
                if retry == self.retries:
                    raise PageError("HTTP transport failure") from exc
                self.next_request = max(self.next_request, self.clock() + 2 ** retry)
                retry += 1
                continue
            if response.status_code in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                target = urlparse(urljoin(url, location or ""))
                if (not location or target.scheme != "https" or target.hostname != "en.wikipedia.org"
                        or not target.path.startswith("/api/rest_v1/page/summary/")
                        or target.username or target.password or target.port not in (None, 443)
                        or redirects >= 3):
                    raise PageError("Unsafe, missing or excessive HTTP redirect")
                url = target.geturl()
                redirects += 1
                continue
            if response.status_code == 429 or 500 <= response.status_code < 600:
                wait = 2 ** retry
                header = response.headers.get("Retry-After")
                if header:
                    try:
                        wait = float(header)
                    except ValueError:
                        try:
                            wait = (parsedate_to_datetime(header) - datetime.now(timezone.utc)).total_seconds()
                        except (TypeError, ValueError):
                            raise PageError("Invalid Retry-After")
                if not math.isfinite(wait) or wait > self.max_backoff:
                    raise PageError("Retry-After exceeds bounded backoff; retry in a later job")
                if retry == self.retries:
                    raise PageError(f"HTTP {response.status_code}: retries exhausted")
                self.next_request = max(self.next_request, self.clock() + max(0, wait))
                retry += 1
                continue
            if response.status_code != 200:
                raise PageError(f"HTTP {response.status_code}")
            try:
                return parse_summary(response.json(), fetched_at)
            except (ValueError, TypeError, AttributeError) as exc:
                raise PageError(f"Unusable summary: {exc}") from exc
        raise PageError("Retries exhausted")
