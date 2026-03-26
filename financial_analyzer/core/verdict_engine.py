VERDICT_CONTEXT = {
    "hardware_platform": {"roic_override_threshold": 0.40, "ignore_low_current_ratio": True, "ignore_low_fcf_yield": True},
    "consumer_staples": {"roic_override_threshold": 0.09, "ignore_high_debt": True, "mandatory_missing_tolerance": 12},
    "commercial_bank": {"fcf_blocked_in_verdict": True, "working_capital_blocked": True, "fraud_flag_tolerance": 10},
    "investment_bank": {"fcf_blocked_in_verdict": True, "working_capital_blocked": True, "failure_prob_cap": 0.15},
    "insurance_life": {"combined_ratio_blocked": True, "high_fcf_yield_positive": True, "use_roe_when_roic_missing": True},
    "insurance_broker": {"negative_pb_context": "buyback", "failure_prob_cap": 0.12, "combined_ratio_blocked": True},
    "insurance_pc": {"combined_ratio_is_primary": True},
    "integrated_oil": {"cyclical_margin_context": True},
    "ev_automaker": {"startup_loss_tolerance": True, "p_e_context": "growth_phase"},
}


class VerdictEngine:
    def compute_verdict(self, ticker: str, year: int, ratios: dict, strategic: dict, sub_sector: str, audit: object) -> dict:
        ctx = VERDICT_CONTEXT.get(sub_sector, {})
        signals_pos = []
        signals_neg = []
        score = 70

        roic = ratios.get("roic", {})
        roic_val = roic.get("value") if isinstance(roic, dict) else getattr(roic, "value", roic)
        if roic_val is not None:
            override = ctx.get("roic_override_threshold")
            if override and roic_val > override:
                score += 15
                signals_pos.append(f"ROIC={roic_val:.1%} ممتاز")
            elif roic_val > 0.10:
                score += 8
                signals_pos.append(f"ROIC={roic_val:.1%} جيد")
            elif roic_val < 0:
                score -= 10
                signals_neg.append("ROIC سالب")

        if not ctx.get("fcf_blocked_in_verdict"):
            fcf = ratios.get("fcf_yield", {})
            fcf_val = fcf.get("value") if isinstance(fcf, dict) else getattr(fcf, "value", fcf)
            if fcf_val is not None:
                if fcf_val > 0.03:
                    score += 5
                    signals_pos.append(f"FCF Yield={fcf_val:.1%}")
                elif fcf_val < -0.05:
                    score -= 8
                    signals_neg.append("FCF سالب")

        if not ctx.get("ignore_low_current_ratio"):
            cr = ratios.get("current_ratio", {})
            cr_val = cr.get("value") if isinstance(cr, dict) else getattr(cr, "value", cr)
            if cr_val and cr_val < 0.8:
                score -= 5
                signals_neg.append(f"سيولة ضيقة CR={cr_val:.2f}")

        z = strategic.get("Altman_Z_Score") if isinstance(strategic, dict) else None
        if z:
            try:
                z_f = float(z)
                if z_f < 1.81:
                    score -= 10
                    signals_neg.append(f"Altman Z={z_f:.2f} منطقة ضيق")
                elif z_f > 3:
                    score += 5
                    signals_pos.append(f"Altman Z={z_f:.2f} آمن")
            except (TypeError, ValueError):
                pass

        if score >= 80:
            verdict, confidence = "PASS", min(95, score)
        elif score >= 60:
            verdict, confidence = "WATCH", min(85, score)
        else:
            verdict, confidence = "FAIL", min(75, score)

        override_t = ctx.get("roic_override_threshold")
        if verdict == "FAIL" and roic_val and override_t and roic_val > override_t:
            verdict = "WATCH"
            confidence = max(confidence, 65)
            if audit is not None:
                audit.correction(year, "verdict", "FAIL", "WATCH", f"ROIC={roic_val:.1%} يُلغي FAIL")

        return {
            "verdict": verdict,
            "confidence": confidence,
            "positive_signals": signals_pos,
            "negative_signals": signals_neg,
            "score": score,
        }
