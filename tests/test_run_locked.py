import os
import shutil
import subprocess
from pathlib import Path

import pytest


def test_lock_timeout_is_visible_failure(tmp_path: Path):
    if shutil.which("flock") is None:
        pytest.skip("flock command is required for run_locked integration test")
    lock = tmp_path / "writer.lock"
    script = Path("deploy/scripts/run_locked.sh").resolve()
    holder = subprocess.Popen(
        ["flock", str(lock), "sh", "-c", "printf 'ready\\n'; sleep 5"],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        env = os.environ.copy()
        env.update(
            {
                "PM_ROBOT_LOCK": str(lock),
                "PM_ROBOT_LOCK_WAIT": "0",
                "PM_ROBOT_TASK_NAME": "test-task",
                "PM_ROBOT_LOCK_TIMEOUT_EXIT": "75",
            }
        )
        result = subprocess.run(
            [str(script), "true"],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 75
        assert "lock_skipped: task=test-task" in result.stderr
    finally:
        holder.terminate()
        holder.wait(timeout=5)
