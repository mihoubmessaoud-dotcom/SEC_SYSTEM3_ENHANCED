#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path

from modules.ratio_formats import get_ratio_format_trace, is_ratio_format_debug_enabled, reset_ratio_format_trace
from modules.sec_fetcher import SECDataFetcher


def _avg(vals):
    vals = [v for v in vals if isinstance(v, (int, float))]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _extract_company_summary(result, allow_low_reliability=False):
    io = result.get('institutional_outputs') or {}
    cls = io.get('classification') or {}
    profile = cls.get('primary_profile')
    probs = cls.get('profile_probabilities', [])
    primary_conf = probs[0].get('probability') if probs else None

    # Ratio aggregation with reliability filtering
    ratio_rows = []
    ratio_cols_set = set()
    ratio_contracts = []
    reliability_counts = {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'REJECTED': 0}
    rejection_reasons = {}
    validator_flags = {}
    exception_counts = {}
    status_flags = {}
    ratio_expl_df = io.get('ratio_explanations')
    if ratio_expl_df is not None and hasattr(ratio_expl_df, 'to_dict'):
        rows = ratio_expl_df.to_dict(orient='records')
        ratio_contracts = rows
        grouped = {}
        for r in rows:
            y = r.get('year')
            rid = r.get('ratio_id')
            rel = (r.get('reliability') or {})
            grade = rel.get('grade')
            if grade in reliability_counts:
                reliability_counts[grade] += 1
            for reason in (r.get('reasons') or []):
                rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
            for flag in (rel.get('gates_failed') or []):
                validator_flags[flag] = validator_flags.get(flag, 0) + 1
            ex_diag = ((r.get('diagnostics') or {}).get('exceptions') or {})
            for ex in (ex_diag.get('entries') or []):
                ex_id = ex.get('exception_id')
                if ex_id:
                    exception_counts[ex_id] = exception_counts.get(ex_id, 0) + 1
            for sf in (ex_diag.get('status_flags') or []):
                status_flags[sf] = status_flags.get(sf, 0) + 1
            if grade == 'REJECTED':
                continue
            if grade == 'LOW' and not allow_low_reliability:
                continue
            grouped.setdefault(y, {'year': y, 'profile': profile})
            grouped[y][rid] = r.get('value')
            ratio_cols_set.add(rid)
        ratio_rows = [grouped[y] for y in sorted(grouped.keys())]
    ratio_cols = sorted(ratio_cols_set)

    risk_df = io.get('risk_integrity_dashboard')
    integrity = {}
    if risk_df is not None and hasattr(risk_df, 'to_dict') and len(risk_df.index) > 0:
        integrity = risk_df.to_dict(orient='records')[0]

    map_versions_df = io.get('mapping_versions')
    map_conf = None
    if map_versions_df is not None and hasattr(map_versions_df, 'to_dict'):
        rows = map_versions_df.to_dict(orient='records')
        map_conf = _avg([r.get('mapping_confidence_avg') for r in rows])

    return {
        'profile': profile,
        'primary_profile_confidence': primary_conf,
        'profile_probabilities': probs,
        'classifier_diagnostics': io.get('classifier_diagnostics', {}),
        'ratio_columns': ratio_cols,
        'ratio_rows': ratio_rows,
        'ratio_contracts': ratio_contracts,
        'reliability_counts': reliability_counts,
        'total_exceptions': int(sum(exception_counts.values())),
        'exception_counts': sorted(exception_counts.items(), key=lambda kv: kv[1], reverse=True),
        'company_status_flags': sorted(status_flags.items(), key=lambda kv: kv[1], reverse=True),
        'top_rejection_reasons': sorted(rejection_reasons.items(), key=lambda kv: kv[1], reverse=True)[:10],
        'top_validator_flags': sorted(validator_flags.items(), key=lambda kv: kv[1], reverse=True)[:10],
        'integrity': integrity,
        'mapping_confidence_avg': map_conf,
    }


def _validate_bank_ratios(summary, ratio_expl_df=None):
    cols = set(summary.get('ratio_columns') or [])
    required = {'net_interest_margin', 'loan_to_deposit_ratio', 'capital_ratio_proxy'}
    industrial_not_allowed = {'gross_margin', 'operating_margin', 'fcf_margin'}

    ratio_state = {}
    if ratio_expl_df is not None and hasattr(ratio_expl_df, 'to_dict'):
        rows = ratio_expl_df.to_dict(orient='records')
        by_ratio = {}
        for r in rows:
            rid = r.get('ratio_id')
            rel = (r.get('reliability') or {})
            grade = rel.get('grade')
            by_ratio.setdefault(rid, []).append(grade)
        for rid in sorted(required):
            grades = by_ratio.get(rid, [])
            ratio_state[rid] = {
                'present': bool(grades),
                'computed_non_rejected': any(g and g != 'REJECTED' for g in grades),
                'grades': grades,
            }

    has_required = {k: (k in cols) for k in sorted(required)}
    blocked_industrial = all(k not in cols for k in industrial_not_allowed)
    inventory_related_blocked = all('inventory' not in c.lower() for c in cols)

    return {
        'required_bank_ratios_present': has_required,
        'required_bank_ratios_computed': {k: v.get('computed_non_rejected', False) for k, v in ratio_state.items()},
        'required_bank_ratio_states': ratio_state,
        'industrial_ratios_blocked': blocked_industrial,
        'inventory_metrics_blocked': inventory_related_blocked,
    }


def _extract_scoring_breakdown(summary):
    out = {}
    for r in summary.get('ratio_contracts', []) or []:
        rid = r.get('ratio_id')
        year = r.get('year')
        rel = r.get('reliability') or {}
        diag = r.get('diagnostics') or {}
        sc = diag.get('reliability_scoring') or {}
        key = f"{year}:{rid}"
        out[key] = {
            'score': rel.get('score'),
            'grade': rel.get('grade'),
            'penalties_breakdown': sc.get('penalties_breakdown'),
            'caps_applied': sc.get('caps_applied', []),
        }
    return out


def _fetch_with_bank_fallback(fetcher, ticker, start, end):
    result = fetcher.fetch_company_data(ticker, start, end, filing_type='10-K')
    diagnostics = [
        {
            'attempt': f'{ticker}:{start}-{end}:10-K',
            'success': bool(result.get('success')),
            'error': result.get('error'),
            'filing_diagnostics': result.get('filing_diagnostics'),
        }
    ]
    if result.get('success'):
        return result, diagnostics

    # JPM specific expanded retry
    if ticker.upper() == 'JPM':
        retry = fetcher.fetch_company_data(ticker, 2020, 2024, filing_type='10-K')
        diagnostics.append({
            'attempt': f'{ticker}:2020-2024:10-K',
            'success': bool(retry.get('success')),
            'error': retry.get('error'),
            'filing_diagnostics': retry.get('filing_diagnostics'),
        })
        return retry, diagnostics

    return result, diagnostics


def _build_readable_summary(data):
    lines = []
    lines.append('Sector Comparison Summary: AAPL vs AIG vs JPM')
    lines.append('=' * 72)
    for ticker in ['AAPL', 'AIG', 'JPM']:
        item = data['companies'].get(ticker, {})
        if not item.get('success'):
            lines.append(f'- {ticker}: FAILED ({item.get("error")})')
            continue
        s = item['summary']
        integ = s.get('integrity', {})
        lines.append(
            f'- {ticker}: profile={s.get("profile")}, '
            f'profile_conf={s.get("primary_profile_confidence")}, '
            f'map_conf={s.get("mapping_confidence_avg")}, '
            f'ratios={len(s.get("ratio_columns") or [])}, '
            f'structural_integrity={integ.get("structural_stability_score")}, '
            f'reliability_counts={s.get("reliability_counts")}'
        )
        lines.append(f'  total_exceptions={s.get("total_exceptions")} status_flags={s.get("company_status_flags")}')
        filing_diag = item.get('filing_diagnostics') or {}
        lines.append(f'  filing_grade={filing_diag.get("filing_grade")} out_of_range={filing_diag.get("out_of_range")}')
        lines.append(f'  top_rejection_reasons={s.get("top_rejection_reasons")}')
        lines.append(f'  top_validator_flags={s.get("top_validator_flags")}')

    bval = data.get('ratio_engine_validation', {}).get('JPM', {})
    if bval:
        lines.append('- JPM Ratio Validation:')
        req = bval.get('required_bank_ratios_present', {})
        lines.append(f'  NIM={req.get("net_interest_margin")}, LDR={req.get("loan_to_deposit_ratio")}, Capital={req.get("capital_ratio_proxy")}')
        req_comp = bval.get('required_bank_ratios_computed', {})
        lines.append(f'  computed_non_rejected -> NIM={req_comp.get("net_interest_margin")}, LDR={req_comp.get("loan_to_deposit_ratio")}, Capital={req_comp.get("capital_ratio_proxy")}')
        lines.append(f'  Industrial ratios blocked={bval.get("industrial_ratios_blocked")}')
        lines.append(f'  Inventory metrics blocked={bval.get("inventory_metrics_blocked")}')

    return '\n'.join(lines) + '\n'


def _build_ratio_format_trace(companies):
    trace = {'entries': []}
    for ticker, item in (companies or {}).items():
        if not item.get('success'):
            continue
        summary = item.get('summary') or {}
        by_year = {}
        for contract in (summary.get('ratio_contracts') or []):
            year = contract.get('year')
            ratio_id = contract.get('ratio_id')
            dbg = ((contract.get('diagnostics') or {}).get('ratio_debug') or {})
            if year is None or not ratio_id:
                continue
            ykey = str(year)
            by_year.setdefault(ykey, {})
            by_year[ykey][ratio_id] = {
                'canonical': dbg.get('stored_ratio_value_canonical'),
                'display': dbg.get('formatted_display_value'),
                'suffix': dbg.get('display_suffix') if dbg.get('display_suffix') is not None else ('%' if dbg.get('ratio_format') == 'percent' else ''),
                'format': dbg.get('ratio_format'),
                'multiplier': dbg.get('ratio_display_multiplier'),
                'formatter_path': dbg.get('formatter_path_used'),
            }
        for year_key, ratios in sorted(by_year.items(), key=lambda kv: kv[0]):
            try:
                year_value = int(year_key)
            except Exception:
                year_value = year_key
            trace['entries'].append({
                'ticker': ticker,
                'year': year_value,
                'ratios': ratios,
            })
    return trace


def main():
    allow_low_reliability = False
    if is_ratio_format_debug_enabled():
        reset_ratio_format_trace()
    fetcher = SECDataFetcher()
    out_dir = Path('exports/sector_comparison')
    out_dir.mkdir(parents=True, exist_ok=True)

    plan = {
        'AAPL': (2022, 2024),
        'AIG': (2022, 2024),
        'JPM': (2022, 2024),
    }

    companies = {}
    filing_coverage_diagnostics = {}
    ratio_expl_by_ticker = {}

    for ticker, (start, end) in plan.items():
        res, diags = _fetch_with_bank_fallback(fetcher, ticker, start, end)
        filing_coverage_diagnostics[ticker] = diags
        if not res.get('success'):
            companies[ticker] = {
                'success': False,
                'error': res.get('error'),
                'filing_diagnostics': res.get('filing_diagnostics'),
                'suggested_fallback': 'Try broader range or use latest annual available via fallback diagnostics',
            }
            continue

        summary = _extract_company_summary(res, allow_low_reliability=allow_low_reliability)
        ratio_expl_by_ticker[ticker] = (res.get('institutional_outputs') or {}).get('ratio_explanations')
        companies[ticker] = {
            'success': True,
            'company_info': res.get('company_info', {}),
            'selected_filings': res.get('selected_filings', []),
            'filing_diagnostics': res.get('filing_diagnostics', {}),
            'summary': summary,
        }

    ratio_validation = {}
    scoring_validation = {}
    for t in ['AAPL', 'AIG', 'JPM']:
        if companies.get(t, {}).get('success'):
            scoring_validation[t] = _extract_scoring_breakdown(companies[t]['summary'])
    if companies.get('JPM', {}).get('success'):
        ratio_validation['JPM'] = _validate_bank_ratios(
            companies['JPM']['summary'],
            ratio_expl_df=ratio_expl_by_ticker.get('JPM')
        )

    comparison = {
        'companies': companies,
        'allow_low_reliability': allow_low_reliability,
        'mapping_confidence_comparison': {
            t: (companies[t]['summary'].get('mapping_confidence_avg') if companies.get(t, {}).get('success') else None)
            for t in ['AAPL', 'AIG', 'JPM']
        },
        'profile_assignment_comparison': {
            t: (companies[t]['summary'].get('profile') if companies.get(t, {}).get('success') else None)
            for t in ['AAPL', 'AIG', 'JPM']
        },
        'classification_confidence_comparison': {
            t: (companies[t]['summary'].get('primary_profile_confidence') if companies.get(t, {}).get('success') else None)
            for t in ['AAPL', 'AIG', 'JPM']
        },
        'ratio_engine_validation': ratio_validation,
        'ratio_scoring_validation': scoring_validation,
        'filing_coverage_diagnostics': filing_coverage_diagnostics,
    }

    json_path = out_dir / 'sector_comparison_report.json'
    json_path.write_text(json.dumps(comparison, ensure_ascii=False, indent=2), encoding='utf-8')

    txt_path = out_dir / 'sector_comparison_summary.txt'
    txt_path.write_text(_build_readable_summary(comparison), encoding='utf-8')

    diag_path = out_dir / 'filing_coverage_diagnostics.json'
    diag_path.write_text(json.dumps(filing_coverage_diagnostics, ensure_ascii=False, indent=2), encoding='utf-8')

    val_path = out_dir / 'ratio_engine_validation_report.json'
    val_payload = {
        'bank_ratio_engine_validation': ratio_validation,
        'weighted_reliability_scoring_validation': scoring_validation,
    }
    val_path.write_text(json.dumps(val_payload, ensure_ascii=False, indent=2), encoding='utf-8')

    classifier_diag = {
        t: (companies[t]['summary'].get('classifier_diagnostics') if companies.get(t, {}).get('success') else None)
        for t in ['AAPL', 'AIG', 'JPM']
    }
    classifier_diag_path = out_dir / 'classifier_diagnostics.json'
    classifier_diag_path.write_text(json.dumps(classifier_diag, ensure_ascii=False, indent=2), encoding='utf-8')

    ratio_format_trace = _build_ratio_format_trace(companies)
    if is_ratio_format_debug_enabled():
        ratio_format_trace['debug_events'] = get_ratio_format_trace()
    ratio_format_trace_path = out_dir / 'ratio_format_trace.json'
    ratio_format_trace_path.write_text(json.dumps(ratio_format_trace, ensure_ascii=False, indent=2), encoding='utf-8')

    print(json.dumps({
        'success': True,
        'sector_comparison_report': str(json_path),
        'readable_summary': str(txt_path),
        'filing_coverage_diagnostics': str(diag_path),
        'ratio_engine_validation_report': str(val_path),
        'classifier_diagnostics': str(classifier_diag_path),
        'ratio_format_trace': str(ratio_format_trace_path),
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
