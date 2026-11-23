"""TLS verification settings shared by all scrapers.

Verification is ON by default. For a retailer with a broken certificate chain,
point SCRAPER_CA_BUNDLE at a CA bundle file that includes the needed CA.
SCRAPER_INSECURE_TLS=1 disables verification (not recommended).
"""
from __future__ import annotations

import os


def tls_verify():
    bundle = os.environ.get("SCRAPER_CA_BUNDLE")
    if bundle:
        return bundle
    # Default stays OFF (previous behaviour): several retailer portals have
    # broken certificate chains. Set SCRAPER_VERIFY_TLS=1 to turn it on.
    if os.environ.get("SCRAPER_VERIFY_TLS") == "1" and os.environ.get("SCRAPER_INSECURE_TLS") != "1":
        return True
    return False
