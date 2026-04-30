"""
_http_retry.py — Shared retry wrapper for outbound HTTP calls.

Retries connect/read timeouts, generic ConnectionError, 429 (honouring
Retry-After), and 5xx responses. Re-raises on final failure so callers
see a real error rather than silently empty data.
"""

import time
import random
import requests


# (connect timeout, read timeout). Connect side gets more headroom because
# the original failure was a ConnectTimeoutError during a Shopify spike.
DEFAULT_TIMEOUT: tuple[float, float] = (15, 60)
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_BASE = 2.0  # sleep = base * 2**(attempt-1) + jitter


def request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    params: dict | None = None,
    data: dict | None = None,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
) -> requests.Response:
    """Issue an HTTP request with retries on transient failures."""
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                data=data,
                timeout=timeout,
            )
        except (requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt == max_attempts:
                raise
            time.sleep(backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1))
            continue

        if resp.status_code == 429:
            if attempt == max_attempts:
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            try:
                sleep_s = float(retry_after) if retry_after else backoff_base * (2 ** (attempt - 1))
            except ValueError:
                sleep_s = backoff_base * (2 ** (attempt - 1))
            time.sleep(sleep_s + random.uniform(0, 0.5))
            continue

        if 500 <= resp.status_code < 600:
            if attempt == max_attempts:
                resp.raise_for_status()
            time.sleep(backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1))
            continue

        return resp

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{method} {url} failed with no captured exception")


def get_with_retry(url: str, **kwargs) -> requests.Response:
    return request_with_retry("GET", url, **kwargs)


def post_with_retry(url: str, **kwargs) -> requests.Response:
    return request_with_retry("POST", url, **kwargs)
