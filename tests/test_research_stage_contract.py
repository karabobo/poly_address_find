from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src" / "pm_robot"
RUNTIME_PATHS = (
    SRC_ROOT,
    REPO_ROOT / "deploy",
    REPO_ROOT / "scripts",
)
FORBIDDEN_RUNTIME_TERMS = (
    "candidate_stage",
    "leader_scores",
    "review_events",
    "copyability",
    "copy_graph",
    "copy-backtest",
    "paper_candidate",
    "paper_approved",
    "paper-run",
    "paper-settle",
    "live_eligible",
    "publish-leaders",
    "maker_fraction",
)


def _runtime_files() -> list[Path]:
    paths: list[Path] = []
    for root in RUNTIME_PATHS:
        for path in root.rglob("*"):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            if root == SRC_ROOT and "migrations" in path.parts:
                continue
            if path.suffix in {".py", ".sh", ".yml", ".service", ".timer"}:
                paths.append(path)
    return sorted(paths)


def test_runtime_has_no_retired_execution_or_legacy_stage_contracts():
    violations: list[str] = []
    for path in _runtime_files():
        text = path.read_text(encoding="utf-8").lower()
        for term in FORBIDDEN_RUNTIME_TERMS:
            if term in text:
                violations.append(f"{path.relative_to(REPO_ROOT)}: {term}")

    assert violations == [], "\n".join(violations)


def test_retired_ad_hoc_scoring_scripts_are_absent():
    assert not (REPO_ROOT / "scripts" / "build_candidate_review_queue.py").exists()
    assert not (REPO_ROOT / "scripts" / "rank_wallets.py").exists()
