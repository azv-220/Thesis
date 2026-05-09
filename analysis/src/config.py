"""Configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]


class DependencyError(RuntimeError):
    pass


@dataclass(frozen=True)
class PipelineConfig:
    repo_root: Path
    raw_csv_dir: Path
    event_input_dir: Path
    build_dir: Path
    donations_csv: str
    fundraisers_csv: str
    usb_event_source_dir: Path

    @property
    def donations_csv_path(self) -> Path:
        return self.raw_csv_dir / self.donations_csv

    @property
    def fundraisers_csv_path(self) -> Path:
        return self.raw_csv_dir / self.fundraisers_csv

    @property
    def raw_parquet_dir(self) -> Path:
        return self.build_dir / "raw_parquet"

    @property
    def curated_dir(self) -> Path:
        return self.build_dir / "curated"

    @property
    def logs_dir(self) -> Path:
        return self.build_dir / "logs"


def require_yaml() -> Any | None:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        return None
    return yaml


def parse_simple_yaml(text: str) -> dict[str, Any]:
    """Parse the limited two-level YAML shape used by config_template.yaml.

    This keeps basic commands usable before project dependencies are installed.
    Install PyYAML for normal use.
    """
    data: dict[str, Any] = {}
    current: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if not line.startswith(" ") and line.endswith(":"):
            current = line[:-1].strip()
            data[current] = {}
            continue
        if current and line.startswith("  ") and ":" in line:
            key, value = line.strip().split(":", 1)
            value = value.strip().strip('"').strip("'")
            data[current][key.strip()] = value
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            data[key.strip()] = value.strip().strip('"').strip("'")
            current = None
            continue
        raise ValueError(f"Unsupported config line: {raw_line}")
    return data


def resolve_path(repo_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return repo_root / path


def load_config(path: Path) -> PipelineConfig:
    yaml = require_yaml()
    config_path = path.expanduser()
    if not config_path.is_absolute():
        config_path = REPO_ROOT / config_path
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. "
            "Copy config/config_template.yaml to config/config_user.yaml."
        )

    text = config_path.read_text()
    yaml = require_yaml()
    if yaml is None:
        data = parse_simple_yaml(text)
    else:
        data = yaml.safe_load(text) or {}
    paths = data.get("paths", {})
    raw_files = data.get("raw_files", {})

    return PipelineConfig(
        repo_root=REPO_ROOT,
        raw_csv_dir=resolve_path(REPO_ROOT, paths["raw_csv_dir"]),
        event_input_dir=resolve_path(REPO_ROOT, paths["event_input_dir"]),
        build_dir=resolve_path(REPO_ROOT, paths["build_dir"]),
        donations_csv=raw_files["donations_csv"],
        fundraisers_csv=raw_files["fundraisers_csv"],
        usb_event_source_dir=resolve_path(REPO_ROOT, data["usb_event_source_dir"]),
    )
