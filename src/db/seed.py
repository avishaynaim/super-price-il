from __future__ import annotations

from .connection import connect
from ..scraper.registry import CHAINS


def seed_chains() -> None:
    with connect() as conn:
        for c in CHAINS:
            conn.execute(
                """
                INSERT INTO chains(code, name_he, name_en, portal_url, auth_profile, active)
                VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(code) DO UPDATE SET
                    name_he=excluded.name_he,
                    name_en=excluded.name_en,
                    portal_url=excluded.portal_url,
                    auth_profile=excluded.auth_profile
                """,
                (c.code, c.name_he, c.name_en, c.portal_url, c.auth_kind),
            )


if __name__ == "__main__":
    seed_chains()
    print("chains seeded")
