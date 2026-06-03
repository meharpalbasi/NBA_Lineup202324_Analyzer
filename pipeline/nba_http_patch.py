"""Make nba_api defeat stats.nba.com's Akamai TLS fingerprinting.

Since ~Feb 2026, stats.nba.com (Akamai Bot Manager) drops requests whose TLS/JA3
fingerprint doesn't look like a real browser — i.e. anything using plain
``requests``/urllib3. The connection completes the TLS handshake and then the
HTTP response is silently withheld (read timeout), regardless of headers or IP.

The fix is to issue requests with a browser TLS fingerprint. ``curl_cffi`` does
exactly that. nba_api's ``library/http.py`` does ``import requests`` and then
``requests.get(url, params, headers, proxies, timeout)``; we swap that module's
``requests`` reference for a thin shim backed by ``curl_cffi`` with
``impersonate="chrome"``. curl_cffi's Response is API-compatible (``.text``,
``.status_code``, ``.url``), so nothing else changes.

Importing this module applies the patch. Import it once before any nba_api call.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("pipeline.nba_http_patch")

IMPERSONATE = "chrome"
PATCHED = False

try:
    from curl_cffi import requests as _cffi
    import nba_api.library.http as _nba_http

    class _CurlCffiRequests:
        """Minimal ``requests``-compatible shim that impersonates Chrome's TLS."""

        @staticmethod
        def get(*args, **kwargs):
            kwargs.setdefault("impersonate", IMPERSONATE)
            return _cffi.get(*args, **kwargs)

    # nba_api references the module global ``requests`` at call time, so
    # replacing it here transparently routes every stats request through curl_cffi.
    _nba_http.requests = _CurlCffiRequests()
    PATCHED = True
    logger.info("nba_api HTTP patched to use curl_cffi (impersonate=%s)", IMPERSONATE)
except Exception as exc:  # pragma: no cover - defensive
    logger.warning(
        "curl_cffi patch NOT applied (%s) — nba_api will use plain requests and "
        "will likely time out against stats.nba.com's Akamai protection.",
        exc,
    )
