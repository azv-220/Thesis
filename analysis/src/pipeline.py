"""Data pipeline tasks for raw and curated Parquet outputs."""

from __future__ import annotations

import csv
import json
import re
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from analysis.src.config import PipelineConfig
from analysis.src.deps import require_duckdb


DONATION_COLUMNS = [
    "__index_0",
    "donation_id",
    "amount",
    "is_offline",
    "is_anonymous",
    "created_at",
    "name",
    "profile_url",
    "verified",
    "currencycode",
    "fund_id",
    "checkout_id",
    "date_created",
]

FUNDRAISER_COLUMNS = [
    "__index_0",
    "fund_id",
    "title",
    "tag",
    "description",
    "date_created",
    "organizer_name",
    "organizer_city",
    "N_donors",
    "progress",
    "goal",
    "url",
    "city",
    "state",
    "length",
]

EVENT_FILE_RE = re.compile(r"^all_.*fund_ids(?:\.csv)?$")
INTEGER_RE = re.compile(r"^\d+$")
WHOLE_FLOAT_RE = re.compile(r"^(\d+)\.0+$")


def ensure_dirs(config: PipelineConfig) -> None:
    for path in [
        config.event_input_dir,
        config.raw_parquet_dir,
        config.curated_dir,
        config.logs_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def json_default(value):
    if isinstance(value, Path):
        return str(value)
    return value


def write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_default) + "\n")


def remove_existing(path: Path) -> None:
    if path.exists():
        path.unlink()


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def quote(path: Path) -> str:
    return str(path).replace("'", "''")


def duck_columns(columns: Iterable[str]) -> str:
    return "{" + ", ".join(f"'{col}': 'VARCHAR'" for col in columns) + "}"


def read_csv_expr(path: Path, columns: list[str]) -> str:
    return (
        f"read_csv('{quote(path)}', "
        "header=true, "
        f"columns={duck_columns(columns)}, "
        "strict_mode=false, "
        "ignore_errors=false, "
        "parallel=false)"
    )


def normalize_string_sql(column: str) -> str:
    return (
        f"CASE WHEN {column} IS NULL OR {column} = '' OR lower({column}) = 'nan' "
        f"THEN NULL ELSE {column} END"
    )


def normalize_id_sql(column: str) -> str:
    normalized = normalize_string_sql(column)
    return (
        f"CASE "
        f"WHEN {normalized} IS NULL THEN NULL "
        f"WHEN regexp_matches({normalized}, '^[0-9]+$') THEN {normalized} "
        f"WHEN regexp_matches({normalized}, '^[0-9]+\\.0+$') "
        f"THEN regexp_extract({normalized}, '^([0-9]+)\\.0+$', 1) "
        f"ELSE NULL END"
    )


def bool_sql(column: str) -> str:
    return (
        f"CASE lower({column}) "
        "WHEN 'true' THEN TRUE "
        "WHEN 'false' THEN FALSE "
        "ELSE NULL END"
    )


def date_from_prefix_sql(column: str) -> str:
    return f"try_cast(substr({column}, 1, 10) AS DATE)"


def timestamp_sql(column: str) -> str:
    return (
        f"COALESCE("
        f"try_strptime({column}, '%Y-%m-%dT%H:%M:%S%z'), "
        f"try_strptime({column}, '%Y-%m-%d %H:%M:%S%z')"
        f")"
    )


def copy_event_inputs(config: PipelineConfig) -> None:
    ensure_dirs(config)
    if not config.usb_event_source_dir.exists():
        raise FileNotFoundError(f"USB event source not found: {config.usb_event_source_dir}")

    copied = []
    for source in sorted(config.usb_event_source_dir.iterdir()):
        if source.is_file() and EVENT_FILE_RE.match(source.name):
            target = config.event_input_dir / source.name
            shutil.copy2(source, target)
            copied.append({"source": source, "target": target, "size_bytes": target.stat().st_size})

    write_json(config.logs_dir / "copy_events.json", {"copied_at": now_utc(), "files": copied})
    print(f"Copied {len(copied)} event fund-id files to {config.event_input_dir}")


def build_manifest(config: PipelineConfig) -> None:
    ensure_dirs(config)
    inputs = [config.donations_csv_path, config.fundraisers_csv_path]
    for path in inputs:
        if not path.exists():
            raise FileNotFoundError(f"Raw input not found: {path}")

    manifest = {"created_at": now_utc(), "files": []}
    for path in inputs:
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle)
            header = next(reader)
        manifest["files"].append(
            {
                "path": path,
                "name": path.name,
                "size_bytes": path.stat().st_size,
                "modified_time": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
                "header": ["__index_0" if col == "" else col for col in header],
            }
        )

    event_files = sorted(p for p in config.event_input_dir.glob("all_*fund_ids*") if p.is_file())
    manifest["event_files"] = [
        {"path": path, "name": path.name, "size_bytes": path.stat().st_size} for path in event_files
    ]
    write_json(config.logs_dir / "input_manifest.json", manifest)
    print(f"Wrote manifest to {config.logs_dir / 'input_manifest.json'}")


def build_raw_parquet(config: PipelineConfig) -> None:
    ensure_dirs(config)
    duckdb = require_duckdb()
    con = duckdb.connect()

    raw_outputs = {
        "donations": (config.donations_csv_path, DONATION_COLUMNS, config.raw_parquet_dir / "donations.parquet"),
        "fundraisers": (
            config.fundraisers_csv_path,
            FUNDRAISER_COLUMNS,
            config.raw_parquet_dir / "fundraisers.parquet",
        ),
    }
    log = {"created_at": now_utc(), "outputs": []}
    for table_name, (source, columns, target) in raw_outputs.items():
        if not source.exists():
            raise FileNotFoundError(f"Raw input not found: {source}")
        target.parent.mkdir(parents=True, exist_ok=True)
        remove_existing(target)
        expr = read_csv_expr(source, columns)
        con.execute(
            f"""
            COPY (
                SELECT * FROM {expr}
            )
            TO '{quote(target)}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{quote(target)}')").fetchone()[0]
        log["outputs"].append({"table": table_name, "source": source, "target": target, "rows": rows})

    event_raw_dir = config.raw_parquet_dir / "event_fund_ids"
    event_raw_dir.mkdir(parents=True, exist_ok=True)
    for source in sorted(config.event_input_dir.glob("all_*fund_ids*")):
        target = event_raw_dir / f"{source.name}.parquet"
        remove_existing(target)
        rows = parse_event_file(source)
        temp_csv = config.build_dir / "tmp" / f"{source.name}.csv"
        temp_csv.parent.mkdir(parents=True, exist_ok=True)
        with temp_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["source_file", "fund_id"])
            for fund_id in rows:
                writer.writerow([source.name, fund_id])
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_csv('{quote(temp_csv)}', header=true, all_varchar=true)
            )
            TO '{quote(target)}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
        log["outputs"].append({"table": "event_fund_ids_raw", "source": source, "target": target, "rows": len(rows)})

    write_json(config.logs_dir / "raw_parquet.json", log)
    print(f"Wrote raw Parquet outputs to {config.raw_parquet_dir}")


def parse_event_id(file_name: str) -> str:
    name = Path(file_name).name
    if name.endswith(".csv"):
        name = name[:-4]
    if name.startswith("all_"):
        name = name[4:]
    if name.endswith("_fund_ids"):
        name = name[: -len("_fund_ids")]
    return name


def parse_event_file(path: Path) -> list[str]:
    fund_ids = []
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        for row_number, row in enumerate(reader, start=1):
            cells = [cell.strip() for cell in row if cell.strip()]
            if not cells:
                continue
            candidate = cells[-1]
            if row_number == 1 and candidate in {"0", "fund_id", "fund_ids"}:
                continue
            if INTEGER_RE.match(candidate):
                fund_ids.append(candidate)
                continue
            match = WHOLE_FLOAT_RE.match(candidate)
            if match:
                fund_ids.append(match.group(1))
    return fund_ids


def build_event_membership_csv(config: PipelineConfig) -> Path:
    records: dict[tuple[str, str], set[str]] = defaultdict(set)
    for path in sorted(config.event_input_dir.glob("all_*fund_ids*")):
        event_id = parse_event_id(path.name)
        for fund_id in parse_event_file(path):
            records[(event_id, fund_id)].add(path.name)

    target = config.build_dir / "tmp" / "event_membership.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["event_id", "event_name", "fund_id", "source_files"])
        for (event_id, fund_id), sources in sorted(records.items()):
            writer.writerow([event_id, event_id.replace("_", " "), fund_id, ";".join(sorted(sources))])
    return target


def build_curated_tables(config: PipelineConfig) -> None:
    ensure_dirs(config)
    duckdb = require_duckdb()
    con = duckdb.connect()

    donations_raw = config.raw_parquet_dir / "donations.parquet"
    fundraisers_raw = config.raw_parquet_dir / "fundraisers.parquet"
    if not donations_raw.exists() or not fundraisers_raw.exists():
        raise FileNotFoundError("Raw Parquet outputs are missing. Run `raw-parquet` first.")

    fact_target = config.curated_dir / "fact_donations.parquet"
    dim_target = config.curated_dir / "dim_fundraisers.parquet"
    text_target = config.curated_dir / "fundraiser_text.parquet"
    event_target = config.curated_dir / "event_membership.parquet"
    for target in [fact_target, dim_target, text_target, event_target]:
        remove_existing(target)

    created_at_expr = timestamp_sql("created_at")
    con.execute(
        f"""
        COPY (
            SELECT
                {normalize_string_sql('donation_id')} AS donation_id,
                {normalize_id_sql('fund_id')} AS fund_id,
                try_cast(nullif(amount, '') AS DOUBLE) AS amount,
                {created_at_expr} AS created_at,
                cast({created_at_expr} AS DATE) AS donation_date,
                {date_from_prefix_sql('date_created')} AS fundraiser_date_created,
                {bool_sql('is_offline')} AS is_offline,
                {bool_sql('is_anonymous')} AS is_anonymous,
                {bool_sql('verified')} AS verified,
                {normalize_string_sql('currencycode')} AS currencycode,
                {normalize_string_sql('profile_url')} AS profile_url,
                {normalize_string_sql('name')} AS name,
                {normalize_string_sql('checkout_id')} AS checkout_id
            FROM read_parquet('{quote(donations_raw)}')
        )
        TO '{quote(fact_target)}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    fundraiser_date_expr = date_from_prefix_sql("date_created")
    con.execute(
        f"""
        COPY (
            SELECT
                fund_id,
                date_created,
                n_donors,
                progress,
                goal,
                tag,
                organizer_city,
                city,
                state
            FROM (
                SELECT
                    {normalize_id_sql('fund_id')} AS fund_id,
                    {fundraiser_date_expr} AS date_created,
                    try_cast(nullif(N_donors, '') AS BIGINT) AS n_donors,
                    try_cast(nullif(progress, '') AS DOUBLE) AS progress,
                    try_cast(nullif(goal, '') AS DOUBLE) AS goal,
                    {normalize_string_sql('tag')} AS tag,
                    {normalize_string_sql('organizer_city')} AS organizer_city,
                    {normalize_string_sql('city')} AS city,
                    {normalize_string_sql('state')} AS state,
                    row_number() OVER (
                        PARTITION BY {normalize_id_sql('fund_id')}
                        ORDER BY try_cast(__index_0 AS BIGINT) NULLS LAST
                    ) AS rn
                FROM read_parquet('{quote(fundraisers_raw)}')
            )
            WHERE rn = 1 AND fund_id IS NOT NULL
        )
        TO '{quote(dim_target)}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT
                fund_id,
                title,
                description,
                length,
                url,
                organizer_name
            FROM (
                SELECT
                    {normalize_id_sql('fund_id')} AS fund_id,
                    {normalize_string_sql('title')} AS title,
                    {normalize_string_sql('description')} AS description,
                    try_cast(nullif(length, '') AS BIGINT) AS length,
                    {normalize_string_sql('url')} AS url,
                    {normalize_string_sql('organizer_name')} AS organizer_name,
                    row_number() OVER (
                        PARTITION BY {normalize_id_sql('fund_id')}
                        ORDER BY try_cast(__index_0 AS BIGINT) NULLS LAST
                    ) AS rn
                FROM read_parquet('{quote(fundraisers_raw)}')
            )
            WHERE rn = 1 AND fund_id IS NOT NULL
        )
        TO '{quote(text_target)}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    event_csv = build_event_membership_csv(config)
    con.execute(
        f"""
        COPY (
            SELECT * FROM read_csv('{quote(event_csv)}', header=true, all_varchar=true)
        )
        TO '{quote(event_target)}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    outputs = {}
    for name, path in {
        "fact_donations": fact_target,
        "dim_fundraisers": dim_target,
        "fundraiser_text": text_target,
        "event_membership": event_target,
    }.items():
        outputs[name] = {
            "path": path,
            "rows": con.execute(f"SELECT count(*) FROM read_parquet('{quote(path)}')").fetchone()[0],
        }
    write_json(config.logs_dir / "curated.json", {"created_at": now_utc(), "outputs": outputs})
    print(f"Wrote curated Parquet outputs to {config.curated_dir}")


def validate_outputs(config: PipelineConfig) -> None:
    ensure_dirs(config)
    duckdb = require_duckdb()
    con = duckdb.connect()
    paths = {
        "fact_donations": config.curated_dir / "fact_donations.parquet",
        "dim_fundraisers": config.curated_dir / "dim_fundraisers.parquet",
        "fundraiser_text": config.curated_dir / "fundraiser_text.parquet",
        "event_membership": config.curated_dir / "event_membership.parquet",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Curated outputs missing: {missing}")

    checks = {
        "fact_donations_rows": f"SELECT count(*) FROM read_parquet('{quote(paths['fact_donations'])}')",
        "dim_fundraisers_rows": f"SELECT count(*) FROM read_parquet('{quote(paths['dim_fundraisers'])}')",
        "fundraiser_text_rows": f"SELECT count(*) FROM read_parquet('{quote(paths['fundraiser_text'])}')",
        "event_membership_rows": f"SELECT count(*) FROM read_parquet('{quote(paths['event_membership'])}')",
        "duplicate_event_memberships": (
            f"SELECT count(*) FROM ("
            f"SELECT event_id, fund_id, count(*) AS n "
            f"FROM read_parquet('{quote(paths['event_membership'])}') "
            f"GROUP BY event_id, fund_id HAVING n > 1)"
        ),
        "donations_missing_fund_id": (
            f"SELECT count(*) FROM read_parquet('{quote(paths['fact_donations'])}') WHERE fund_id IS NULL"
        ),
        "donations_missing_amount": (
            f"SELECT count(*) FROM read_parquet('{quote(paths['fact_donations'])}') WHERE amount IS NULL"
        ),
        "donations_missing_created_at": (
            f"SELECT count(*) FROM read_parquet('{quote(paths['fact_donations'])}') WHERE created_at IS NULL"
        ),
        "fundraisers_missing_fund_id": (
            f"SELECT count(*) FROM read_parquet('{quote(paths['dim_fundraisers'])}') WHERE fund_id IS NULL"
        ),
        "fundraiser_ids_with_duplicates": (
            f"SELECT count(*) FROM ("
            f"SELECT fund_id, count(*) AS n "
            f"FROM read_parquet('{quote(paths['dim_fundraisers'])}') "
            f"GROUP BY fund_id HAVING n > 1)"
        ),
        "donation_funds_without_fundraiser": (
            f"SELECT count(*) FROM ("
            f"SELECT DISTINCT fund_id FROM read_parquet('{quote(paths['fact_donations'])}') WHERE fund_id IS NOT NULL"
            f") d LEFT JOIN ("
            f"SELECT DISTINCT fund_id FROM read_parquet('{quote(paths['dim_fundraisers'])}') WHERE fund_id IS NOT NULL"
            f") f USING (fund_id) WHERE f.fund_id IS NULL"
        ),
    }
    report = {"created_at": now_utc(), "checks": {}}
    for name, sql in checks.items():
        report["checks"][name] = con.execute(sql).fetchone()[0]
    write_json(config.logs_dir / "validation_report.json", report)
    print(f"Wrote validation report to {config.logs_dir / 'validation_report.json'}")
