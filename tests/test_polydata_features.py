from pathlib import Path

from pm_robot.research.polydata_features import extract_polydata


def test_extract_polydata_sample_json():
    candidates, features = extract_polydata(Path("tests/fixtures/polydata_sample_traders.json"))
    assert len(candidates) == 5
    assert len(features) == 5
    by_address = {f.address: f for f in features}
    kch = by_address["0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee"]
    assert kch.event_win_rate is not None
    assert kch.avg_dca_entries is not None
    assert kch.hygiene_status == "clean"
