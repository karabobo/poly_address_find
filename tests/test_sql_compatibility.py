import ast
import re
from pathlib import Path

from pm_robot.storage.db import connect, run_migrations


SQL_MARKERS = ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "ALTER ", "VALUES ")
NUMERIC_SEPARATOR = re.compile(r"\d_\d")


def test_sql_literals_avoid_numeric_separators_for_older_sqlite_versions():
    incompatible = []
    for path in Path("src").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            upper_value = node.value.upper()
            if any(marker in upper_value for marker in SQL_MARKERS) and NUMERIC_SEPARATOR.search(node.value):
                incompatible.append(f"{path}:{node.lineno}")

    assert incompatible == []


def test_final_research_schema_supersedes_missing_historical_marker(tmp_path):
    conn = connect(tmp_path / "research.sqlite")
    try:
        run_migrations(conn)
        conn.execute("DELETE FROM schema_migrations WHERE version = 50")
        conn.commit()

        applied = run_migrations(conn)

        assert applied == [50]
        assert conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version = 50"
        ).fetchone() is not None
        assert conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'wallet_activity_watermarks'"
        ).fetchone() is None
    finally:
        conn.close()
