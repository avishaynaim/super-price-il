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
    if os.environ.get("SCRAPER_INSECURE_TLS") == "1":
        return False
    return True
