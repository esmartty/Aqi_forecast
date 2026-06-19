import time

from unittest.mock import patch
from rate_limiter import RateLimiter


def test_observe_headers_updates_state():
    limiter = RateLimiter()

    before = time.monotonic()

    limiter.observe_headers(
        {
            "x-ratelimit-remaining": "9",
            "x-ratelimit-reset": "60",
        }
    )

    assert limiter.remaining == 9
    assert limiter.reset_at is not None
    assert limiter.reset_at >= before + 59


def test_wait_decrements_remaining():
    #test self.remaining -= 1

    limiter = RateLimiter()

    limiter.observe_headers(
        {
            "x-ratelimit-remaining": "2",
            "x-ratelimit-reset": "60",
        }
    )

    limiter.wait()

    assert limiter.remaining == 1

    limiter.wait()

    assert limiter.remaining == 0


@patch("rate_limiter.time.sleep")
def test_wait_sleeps_until_reset(mock_sleep):
    limiter = RateLimiter()

    limiter.remaining = 0
    limiter.reset_at = time.monotonic() + 10

    def side_effect(seconds):
        limiter.remaining = 1

    mock_sleep.side_effect = side_effect

    limiter.wait()

    mock_sleep.assert_called_once()

    sleep_seconds = mock_sleep.call_args[0][0]

    assert sleep_seconds > 0


def test_wait_clears_stale_server_state_after_reset():
    limiter = RateLimiter()

    limiter.remaining = 0
    limiter.reset_at = time.monotonic() - 1

    with patch("rate_limiter.RateLimiter._fallback_wait") as mock_fallback:
        limiter.wait()

    assert limiter.remaining is None
    assert limiter.reset_at is None
    mock_fallback.assert_called_once()


def test_fallback_mode_used_when_no_headers():
    limiter = RateLimiter(max_per_minute=100)

    assert limiter.remaining is None

    limiter.wait()

    assert len(limiter.calls) == 1


def test_fallback_allows_multiple_requests():
    limiter = RateLimiter(max_per_minute=100)

    limiter.wait()
    limiter.wait()

    assert len(limiter.calls) == 2


def test_fallback_respects_max_per_minute():
    limiter = RateLimiter(max_per_minute=2)

    limiter.wait()
    limiter.wait()
    limiter.wait()

    assert len(limiter.calls) <= 2


def test_fallback_purges_old_requests():
    limiter = RateLimiter(max_per_minute=100)

    now = time.monotonic()

    limiter.calls.append(now - 4000)  # older than 1 hour
    limiter.calls.append(now)

    limiter._purge(now)

    assert len(limiter.calls) == 1


def test_fallback_blocks_when_limit_reached():
    limiter = RateLimiter(max_per_minute=1)

    limiter.wait()

    with patch("rate_limiter.time.sleep") as mock_sleep:
        limiter.wait()
        mock_sleep.assert_called()