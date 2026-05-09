#!/usr/bin/env python3
"""Command runner for the thesis data pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from analysis.src.config import load_config
from analysis.src.pipeline import (
    build_curated_tables,
    build_manifest,
    build_raw_parquet,
    copy_event_inputs,
    validate_outputs,
)


COMMANDS = {
    "copy-events": copy_event_inputs,
    "manifest": build_manifest,
    "raw-parquet": build_raw_parquet,
    "curated": build_curated_tables,
    "validate": validate_outputs,
}


def run_all(config_path: Path) -> None:
    config = load_config(config_path)
    copy_event_inputs(config)
    build_manifest(config)
    build_raw_parquet(config)
    build_curated_tables(config)
    validate_outputs(config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run thesis data pipeline tasks.")
    parser.add_argument(
        "command",
        choices=sorted([*COMMANDS.keys(), "all"]),
        help="Pipeline command to run.",
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "config" / "config_user.yaml"),
        help="Path to YAML config file.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    try:
        if args.command == "all":
            run_all(config_path)
            return

        config = load_config(config_path)
        COMMANDS[args.command](config)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
