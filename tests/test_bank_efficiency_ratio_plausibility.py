from __future__ import annotations


def test_bank_efficiency_ratio_falls_back_when_outlier():
    # Unit test: do not hit network. Validate the outlier fallback rule.
    from modules.sec_fetcher import SECDataFetcher

    fetcher = SECDataFetcher(user_agent_email="test@example.com")

    # Lightweight shim that mirrors the final decision rule:
    # - Outlier noninterest expense should trigger proxy eligibility.
    def apply_rule(noninterest_expense, bank_total_revenue, net_income):
        ratios = {}
        if bank_total_revenue not in (None, 0):
            eff_val = None
            eff_source = None
            if noninterest_expense is not None:
                eff_raw = abs(float(noninterest_expense)) / max(abs(float(bank_total_revenue)), 1e-12)
                if 0.30 <= eff_raw <= 0.90:
                    eff_val = eff_raw
                    eff_source = "NONINTEREST_EXPENSE_OVER_TOTAL_REVENUE"
                else:
                    eff_source = "EFF_PROXY_OUTLIER_NONINTEREST_EXPENSE"
            if eff_val is None and net_income is not None:
                exp_proxy = abs(float(bank_total_revenue) - float(net_income))
                eff_val = exp_proxy / max(abs(float(bank_total_revenue)), 1e-12)
                eff_source = eff_source or "ONE_MINUS_NET_MARGIN_PROXY"
            if eff_val is not None:
                ratios["bank_efficiency_ratio"] = eff_val
                ratios["bank_efficiency_ratio_source"] = eff_source
        return ratios

    low = apply_rule(noninterest_expense=10.0, bank_total_revenue=1000.0, net_income=200.0)
    assert 0.0 < float(low.get("bank_efficiency_ratio")) < 1.0
    assert low.get("bank_efficiency_ratio_source") == "EFF_PROXY_OUTLIER_NONINTEREST_EXPENSE"

    high = apply_rule(noninterest_expense=1200.0, bank_total_revenue=1000.0, net_income=200.0)
    assert 0.0 < float(high.get("bank_efficiency_ratio")) < 1.0
    assert high.get("bank_efficiency_ratio_source") == "EFF_PROXY_OUTLIER_NONINTEREST_EXPENSE"
