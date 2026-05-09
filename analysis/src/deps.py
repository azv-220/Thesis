"""Optional dependency checks with clear failure messages."""

from __future__ import annotations


def require_duckdb():
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing duckdb. Install project dependencies with "
            "`python -m pip install -r requirements.txt`."
        ) from exc
    return duckdb
