class DataAnchorValidator:
    TOLERANCE = 0.001

    def validate_year(self, ticker: str, year: int, balance: dict, audit: object) -> dict:
        missing = [
            a
            for a in ["total_assets", "total_liabilities", "total_equity"]
            if not balance.get(a)
        ]

        if missing:
            if audit is not None:
                audit.flag(year, "ANCHOR_MISSING", "HIGH", f"مفقود: {missing}")
            return {
                "status": "ANCHOR_MISSING",
                "missing": missing,
                "allow_computation": False,
            }

        A = float(balance["total_assets"])
        L = float(balance["total_liabilities"])
        E = float(balance["total_equity"])

        if L == 0 and E > 0:
            L = A - E
            if audit is not None:
                audit.correction(year, "total_liabilities", 0, L, "computed_as_A_minus_E")

        delta = abs(A - L - E)
        delta_pct = delta / A if A > 0 else 999

        if delta_pct > self.TOLERANCE:
            severity = "CRITICAL" if delta_pct > 0.01 else "HIGH"
            if audit is not None:
                audit.flag(
                    year,
                    "BALANCE_IDENTITY_FAIL",
                    severity,
                    f"delta={delta:.0f} ({delta_pct:.3%})",
                )
            return {
                "status": "BALANCE_FAIL",
                "delta_pct": delta_pct,
                "allow_computation": delta_pct < 0.05,
            }

        return {"status": "PASS", "allow_computation": True}

    def validate_all_years(self, ticker: str, years_balance: dict, audit: object) -> dict:
        results = {
            year: self.validate_year(ticker, year, bal, audit)
            for year, bal in (years_balance or {}).items()
        }
        valid = [y for y, r in results.items() if r["allow_computation"]]
        blocked = [y for y, r in results.items() if not r["allow_computation"]]
        return {
            "valid_years": sorted(valid),
            "blocked_years": sorted(blocked),
            "details": results,
        }
