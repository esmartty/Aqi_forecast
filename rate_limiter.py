from collections import deque
from threading import Lock
from parser import parse_numeric
import time


class RateLimiter:
    """
    Header-aware rate limiter for OpenAQ API calls.

    Priority:
    1. OpenAQ rate-limit headers (source of truth).
    2. Local deque fallback if headers are unavailable.
    """

    def __init__(self, max_per_minute=60, max_per_hour=2000):
        self.max_per_minute = max_per_minute
        self.max_per_hour = max_per_hour

        # fallback (legacy safety net)
        self.calls = deque()

        self.lock = Lock()

        # server-driven state
        self.remaining = None
        self.reset_at = None

    # ---------------------------
    # headers sync
    # ---------------------------

    def observe_headers(self, headers):
        if not headers:
            return

        remaining = parse_numeric(
            headers.get("x-ratelimit-remaining"),
            cast_type=int,
        )

        reset_seconds = parse_numeric(
            headers.get("x-ratelimit-reset"),
            cast_type=int,
        )

        with self.lock:
            if remaining is not None:
                self.remaining = remaining

            if reset_seconds is not None:
                self.reset_at = time.monotonic() + max(reset_seconds, 0)

    # ---------------------------
    # fallback limiter (deque)
    # ---------------------------

    def _purge(self, now):
        cutoff = now - 3600
        while self.calls and self.calls[0] <= cutoff:
            self.calls.popleft()

    def _fallback_wait(self, now):
        self._purge(now)

        minute_count = sum(
            1 for t in self.calls if t > now - 60
        )

        if (
            len(self.calls) < self.max_per_hour
            and minute_count < self.max_per_minute
        ):
            self.calls.append(now)
            return

        next_allowed = now

        if len(self.calls) >= self.max_per_hour:
            next_allowed = max(next_allowed, self.calls[0] + 3600)

        if minute_count >= self.max_per_minute:
            oldest = min(t for t in self.calls if t > now - 60)
            next_allowed = max(next_allowed, oldest + 60)

        time.sleep(max(next_allowed - now, 0))

    # ---------------------------
    # main logic
    # ---------------------------

    def wait(self):
        while True:
            now = time.monotonic()

            # -----------------------
            # 1. SERVER-DRIVEN MODE
            # -----------------------
            with self.lock:
                if self.remaining is not None:
                    if self.remaining > 0:
                        self.remaining -= 1  # reserve slot
                        return

                    # no remaining -> wait for reset
                    if self.reset_at is not None:
                        wait_time = self.reset_at - now
                        if wait_time > 0:
                            time.sleep(wait_time)
                        continue

            # -----------------------
            # 2. FALLBACK MODE
            # -----------------------
            with self.lock:
                self._fallback_wait(now)
                return