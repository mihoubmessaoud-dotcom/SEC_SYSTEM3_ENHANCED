from modules.data_repository import DataRepository, DuplicateWriteError


def test_set_and_get_raw_success():
    repo = DataRepository()
    repo.set_raw("revenue_2025", 416_161)
    got = repo.get_raw("revenue_2025")
    assert got["value"] == 416_161
    assert got["reason"] == "RAW_SOURCE_DATA"


def test_get_raw_missing_returns_no_source_data():
    repo = DataRepository()
    got = repo.get_raw("missing")
    assert got["value"] is None
    assert got["reason"] == "NO_SOURCE_DATA"


def test_set_clean_and_get_clean_with_reason():
    repo = DataRepository()
    repo.set_clean("gross_margin_2025", 0.462, reason="computed_from_revenue_minus_cogs")
    got = repo.get_clean("gross_margin_2025")
    assert got["value"] == 0.462
    assert got["reason"] == "computed_from_revenue_minus_cogs"


def test_get_clean_missing_returns_no_source_data():
    repo = DataRepository()
    got = repo.get_clean("not_found")
    assert got["value"] is None
    assert got["reason"] == "NO_SOURCE_DATA"


def test_duplicate_raw_write_rejected():
    repo = DataRepository()
    repo.set_raw("market_cap_2025", 300_000)
    try:
        repo.set_raw("market_cap_2025", 301_000)
        assert False, "Expected duplicate write rejection"
    except DuplicateWriteError as exc:
        assert "DUPLICATE_WRITE" in str(exc)


def test_clean_overwrite_rejected():
    repo = DataRepository()
    repo.set_clean("net_margin_2025", 0.241, reason="calculated")
    try:
        repo.set_clean("net_margin_2025", 0.250, reason="recalculated")
        assert False, "Expected clean overwrite rejection"
    except DuplicateWriteError as exc:
        assert "DUPLICATE_WRITE" in str(exc)


def test_clean_requires_reason():
    repo = DataRepository()
    try:
        repo.set_clean("roic_2025", 0.18, reason="")
        assert False, "Expected reason validation error"
    except ValueError as exc:
        assert "reason must be a non-empty string" in str(exc)


def test_audit_log_records_every_write():
    repo = DataRepository()
    repo.set_raw("eps_2025", 6.84)
    repo.set_clean("pe_ratio_2025", 22.9, reason="market_price_over_eps")
    logs = repo.audit_log
    assert len(logs) == 2
    assert logs[0]["action"] == "set_raw"
    assert logs[1]["action"] == "set_clean"
    assert logs[1]["reason"] == "market_price_over_eps"


def test_read_only_snapshots():
    repo = DataRepository()
    repo.set_raw("price_2025", 69.9)
    snapshot = repo.raw_data
    try:
        snapshot["price_2025"] = 70.0
        assert False, "Snapshot should be read-only"
    except TypeError:
        pass
