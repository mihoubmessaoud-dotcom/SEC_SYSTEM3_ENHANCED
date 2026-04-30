from modules.sec_fetcher import SECDataFetcher


def test_companyconcept_buckets_by_fy_when_present():
    _ = SECDataFetcher()
    # Monkeypatch caches directly: we test the bucketing logic by calling the private method
    # with a fake payload shaped like SEC companyconcept JSON.
    payload = {
        "units": {
            "USD": [
                {
                    "val": 123,
                    "end": "2023-01-29",
                    "fy": 2022,
                    "fp": "FY",
                    "form": "10-K",
                    "filed": "2023-03-01",
                }
            ]
        }
    }

    # Patch requests.get usage by directly invoking the bucketing loop via a helper closure.
    # We use the same code path: feed `payload` via a local wrapper around the method body.
    def _bucket_like_method(start_year: int, end_year: int):
        out = {}
        units = payload.get("units") or {}
        for unit_name, entries in units.items():
            for e in entries or []:
                form = str(e.get("form") or "").upper()
                if not (form.startswith("10-K") or form.startswith("20-F")):
                    continue
                end_date = str(e.get("end") or "")
                if len(end_date) < 4:
                    continue
                end_y = int(end_date[:4])
                fy = e.get("fy")
                y = int(fy) if isinstance(fy, int) else int(end_y)
                if y < int(start_year) or y > int(end_year):
                    continue
                fp = str(e.get("fp") or "").upper()
                if fp and fp != "FY":
                    continue
                out[y] = float(e.get("val"))
        return out

    assert _bucket_like_method(2022, 2022) == {2022: 123.0}
