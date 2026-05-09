from pathlib import Path

from analysis.src.pipeline import parse_event_file, parse_event_id


def test_parse_event_id_csv():
    assert parse_event_id("all_Hurricane_Dorian_fund_ids.csv") == "Hurricane_Dorian"


def test_parse_event_id_no_extension():
    assert parse_event_id("all_Harvey_fund_ids") == "Harvey"


def test_parse_event_file_uses_numeric_last_column(tmp_path: Path):
    path = tmp_path / "all_trial_fund_ids.csv"
    path.write_text(",0\n0,123\n1,456\nbad,not_id\n")
    assert parse_event_file(path) == ["123", "456"]
