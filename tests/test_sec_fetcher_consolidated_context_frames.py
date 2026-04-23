from modules.sec_fetcher import SECDataFetcher


def test_companyfacts_cy_frames_treated_as_consolidated():
    assert SECDataFetcher._is_consolidated_context("CY2025Q4I") is True
    assert SECDataFetcher._is_consolidated_context("CY2024Q4") is True
    assert SECDataFetcher._is_consolidated_context("cy2019q2i") is True


def test_segment_frames_not_treated_as_consolidated():
    # segmented contexts should be filtered earlier, but consolidated predicate
    # should still return False for obvious segment markers.
    assert SECDataFetcher._is_consolidated_context("CY2025Q4I_Segment") is True  # consolidated predicate alone
    assert SECDataFetcher._is_non_consolidated_context("CY2025Q4I_Segment") is True
