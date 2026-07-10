import ast
import re
from pathlib import Path


SQL_MARKERS = ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "ALTER ", "VALUES ")
NUMERIC_SEPARATOR = re.compile(r"\d_\d")


def test_sql_literals_avoid_numeric_separators_for_older_sqlite_versions():
    incompatible = []
    for root in (Path("src"), Path("tests")):
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                    continue
                upper_value = node.value.upper()
                if any(marker in upper_value for marker in SQL_MARKERS) and NUMERIC_SEPARATOR.search(node.value):
                    incompatible.append(f"{path}:{node.lineno}")

    assert incompatible == []
