from pathlib import Path

def test_analysis_sql_files_exist():
    root = Path(__file__).resolve().parents[1]
    qdir = root / "sql" / "analysis"
    files = list(qdir.glob("*.sql"))
    assert len(files) >= 3
