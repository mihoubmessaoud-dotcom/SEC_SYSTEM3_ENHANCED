from layers.sec_layer import SECLayer


def test_sec_layer_keeps_annual_q4_frames_and_drops_quarterly_fp():
    layer = SECLayer(user_agent="test-agent")
    concepts = ["us-gaap:TotalDebt"]
    payload = {
        "facts": {
            "us-gaap": {
                "TotalDebt": {
                    "units": {
                        "USD": [
                            {
                                "val": 100,
                                "end": "2020-01-31",
                                "fy": 2020,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2020-03-01",
                                "frame": "CY2019Q4I",
                            },
                            {
                                "val": 999,
                                "end": "2020-04-30",
                                "fy": 2020,
                                "fp": "Q1",
                                "form": "10-Q",
                                "filed": "2020-05-10",
                                "frame": "CY2020Q1I",
                            },
                        ]
                    }
                }
            }
        }
    }

    periods = layer._extract_periods(payload, concepts, start_year=2020, end_year=2020)
    facts = periods["2020"]["facts"]
    assert "us-gaap:TotalDebt" in facts
    assert facts["us-gaap:TotalDebt"]["value"] == 100


def test_sec_layer_uses_fy_to_bucket_fiscal_year():
    layer = SECLayer(user_agent="test-agent")
    concepts = ["us-gaap:TotalDebt"]
    payload = {
        "facts": {
            "us-gaap": {
                "TotalDebt": {
                    "units": {
                        "USD": [
                            # Fiscal 2022 ended in Jan 2023: should be bucketed into 2022, not 2023.
                            {
                                "val": 123,
                                "end": "2023-01-29",
                                "fy": 2022,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2023-03-01",
                                "frame": "CY2022Q4I",
                            }
                        ]
                    }
                }
            }
        }
    }
    periods = layer._extract_periods(payload, concepts, start_year=2022, end_year=2022)
    assert "us-gaap:TotalDebt" in periods["2022"]["facts"]
    assert periods["2022"]["facts"]["us-gaap:TotalDebt"]["value"] == 123
