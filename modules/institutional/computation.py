from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class AnomalyFlag:
    year: int
    node_id: str
    flag_type: str
    details: str
    severity: float


class IntelligentComputationEngine:
    """
    Computes parent nodes dynamically from child behavior and formulas.
    Handles reconciliation, tolerances, restatements, and FX normalization.
    """

    def __init__(self, tolerance: float = 0.05) -> None:
        self.tolerance = tolerance

    def normalize_currency(
        self,
        data_by_year: Dict[int, Dict[str, float]],
        fx_rates: Optional[Dict[int, float]],
        base_currency: str = 'USD',
    ) -> Dict[int, Dict[str, float]]:
        if not fx_rates:
            return data_by_year
        out: Dict[int, Dict[str, float]] = {}
        for y, row in data_by_year.items():
            rate = fx_rates.get(y, 1.0)
            rate = rate if rate and rate > 0 else 1.0
            out[y] = {}
            for k, v in row.items():
                if isinstance(v, (int, float)):
                    out[y][k] = float(v) / rate
                else:
                    out[y][k] = v
            out[y]['_base_currency'] = base_currency
        return out

    def compute(
        self,
        data_by_year: Dict[int, Dict[str, float]],
        ontology,
        mapping_decisions: Iterable,
    ) -> Dict:
        map_by_node: Dict[str, List[str]] = {}
        for d in mapping_decisions:
            map_by_node.setdefault(d.ontology_node_id, []).append(d.raw_concept)

        computed_by_year: Dict[int, Dict[str, float]] = {}
        node_inputs_by_year: Dict[int, Dict[str, List[Dict]]] = {}
        flags: List[AnomalyFlag] = []
        reconciliation_by_year: Dict[int, Dict[str, object]] = {}

        for year, raw_row in data_by_year.items():
            c_row: Dict[str, float] = {}
            node_inputs_by_year[year] = {}

            # Direct picks first
            for node_id, concepts in map_by_node.items():
                values = [raw_row.get(c) for c in concepts if isinstance(raw_row.get(c), (int, float))]
                if not values:
                    continue
                entries = []
                for c in concepts:
                    v = raw_row.get(c)
                    if isinstance(v, (int, float)):
                        entries.append({
                            'value': float(v),
                            'unit': 'USD',
                            'currency': 'USD',
                            'period_end': f'{year}-12-31',
                            'context_id': f'{year}:{node_id}:{c}',
                            'source_tag': c,
                            'source_label': c,
                        })
                if entries:
                    node_inputs_by_year[year][node_id] = entries
                if len(values) == 1:
                    c_row[node_id] = float(values[0])
                else:
                    c_row[node_id] = float(sum(values))

            # Derived and balancing behavior
            # Conservative fallback inference for bank loan stock when direct mapping misses it.
            if not isinstance(c_row.get('BS.LOANS'), (int, float)):
                inferred_loans = self._infer_best_stock_value(
                    raw_row,
                    include_patterns=[
                        'loansandleasesreceivable',
                        'loansreceivable',
                        'financingreceivableexcludingaccruedinterestafterallowanceforcreditloss',
                        'financingreceivableexcludingaccruedinterestbeforeallowanceforcreditloss',
                        'financingreceivablecollectivelyevaluatedforimpairment',
                    ],
                    exclude_patterns=[
                        'provision', 'gain', 'loss', 'sale', 'purchase', 'writeoff',
                        'recovery', 'expense', 'interest', 'fee', 'fairvaluedisclosure',
                        'heldforsale', 'reclassification', 'allowance',
                    ],
                )
                if inferred_loans is not None:
                    c_row['BS.LOANS'] = inferred_loans

            rev = c_row.get('IS.REV')
            cogs = c_row.get('IS.COGS')
            if rev is not None and cogs is not None:
                gp = rev - abs(cogs)
                if 'IS.GP' not in c_row:
                    c_row['IS.GP'] = gp
                else:
                    self._reconcile(year, 'IS.GP', c_row['IS.GP'], gp, flags)

            ocf = c_row.get('CF.OCF')
            capex = c_row.get('CF.CAPEX')
            if ocf is not None and capex is not None and 'CF.FCF' not in c_row:
                c_row['CF.FCF'] = ocf - abs(capex)

            assets = c_row.get('BS.ASSETS')
            liab = c_row.get('BS.LIAB')
            eq = c_row.get('BS.EQ')
            bs_ok = True
            bs_rel_diff = 0.0
            if assets is not None and liab is not None and eq is not None:
                delta = abs(assets - (liab + eq))
                denom = max(abs(assets), 1.0)
                rel = delta / denom
                bs_rel_diff = rel
                if rel > self.tolerance:
                    bs_ok = False
                    flags.append(AnomalyFlag(
                        year=year,
                        node_id='BS',
                        flag_type='balance_sheet_imbalance',
                        details=f'Assets != Liabilities + Equity; rel_diff={rel:.4f}',
                        severity=min(1.0, rel * 5),
                    ))
            reconciliation_by_year[year] = {
                'balance_sheet_ok': bs_ok,
                'balance_sheet_rel_diff': bs_rel_diff,
                'cash_roll_ok': True,  # unavailable from current extracted structure
                'cash_roll_note': 'cash roll inputs unavailable from current extraction',
            }

            # Parent-child structural validation with missing child tolerance
            self._compute_parent_with_missing_child_tolerance(year, c_row, ontology, flags)
            computed_by_year[year] = c_row

        return {
            'computed_by_year': computed_by_year,
            'node_inputs_by_year': node_inputs_by_year,
            'reconciliation_by_year': reconciliation_by_year,
            'anomaly_flags': [f.__dict__ for f in flags],
        }

    def _infer_best_stock_value(
        self,
        raw_row: Dict[str, float],
        include_patterns: List[str],
        exclude_patterns: List[str],
    ) -> Optional[float]:
        best: Optional[float] = None
        for k, v in raw_row.items():
            if not isinstance(v, (int, float)):
                continue
            lv = str(k).lower()
            if not any(p in lv for p in include_patterns):
                continue
            if any(p in lv for p in exclude_patterns):
                continue
            fv = float(v)
            if fv <= 0:
                continue
            if best is None or fv > best:
                best = fv
        return best

    def _compute_parent_with_missing_child_tolerance(
        self,
        year: int,
        c_row: Dict[str, float],
        ontology,
        flags: List[AnomalyFlag]
    ) -> None:
        for node in ontology.nodes.values():
            if not node.children:
                continue
            child_vals = [c_row[ch] for ch in node.children if ch in c_row and isinstance(c_row[ch], (int, float))]
            if not child_vals:
                continue
            if node.calculation_type == 'SUM':
                expected = sum(child_vals)
                if node.id in c_row and isinstance(c_row.get(node.id), (int, float)):
                    self._reconcile(year, node.id, c_row[node.id], expected, flags)
                else:
                    c_row[node.id] = expected
                    flags.append(AnomalyFlag(
                        year=year,
                        node_id=node.id,
                        flag_type='parent_computed_from_children',
                        details='parent missing, computed by SUM(children)',
                        severity=0.10,
                    ))
            elif node.calculation_type == 'SUBTRACT':
                first = child_vals[0]
                rest = sum(abs(v) for v in child_vals[1:])
                expected = first - rest
                if node.id in c_row and isinstance(c_row.get(node.id), (int, float)):
                    self._reconcile(year, node.id, c_row[node.id], expected, flags)
                else:
                    c_row[node.id] = expected
                    flags.append(AnomalyFlag(
                        year=year,
                        node_id=node.id,
                        flag_type='parent_computed_from_children',
                        details='parent missing, computed by SUBTRACT(childA-childB)',
                        severity=0.10,
                    ))

    def _reconcile(self, year: int, node_id: str, actual: float, expected: float, flags: List[AnomalyFlag]) -> None:
        delta = abs(actual - expected)
        denom = max(abs(expected), 1.0)
        rel = delta / denom
        if rel > self.tolerance:
            flags.append(AnomalyFlag(
                year=year,
                node_id=node_id,
                flag_type='reconciliation_mismatch',
                details=f'actual={actual}, expected={expected}, rel_diff={rel:.4f}',
                severity=min(1.0, rel * 4),
            ))

    def track_restatements(self, data_by_period: Dict[str, Dict[str, float]]) -> List[Dict]:
        """
        Detect restatements by repeated concept-year entries with changed values.
        Expects period keys like YYYY-*
        """
        snapshots: Dict[Tuple[int, str], List[float]] = {}
        for period_key, row in data_by_period.items():
            try:
                year = int(str(period_key)[:4])
            except Exception:
                continue
            for concept, value in row.items():
                if isinstance(value, (int, float)):
                    snapshots.setdefault((year, concept), []).append(float(value))

        restatements: List[Dict] = []
        for (year, concept), vals in snapshots.items():
            uniq = sorted(set(round(v, 4) for v in vals))
            if len(uniq) > 1:
                restatements.append({
                    'year': year,
                    'concept': concept,
                    'versions': len(uniq),
                    'min_value': min(uniq),
                    'max_value': max(uniq),
                })
        return restatements


class DynamicTemporalStructureEngine:
    """
    Handles structural drift across years with core stability tracking.
    """

    def build_yearly_mapping_versions(
        self,
        computed_by_year: Dict[int, Dict[str, float]],
        mapping_decisions: Optional[Iterable] = None,
        raw_data_by_year: Optional[Dict[int, Dict[str, float]]] = None,
    ) -> Dict[int, Dict]:
        mapping_decisions = list(mapping_decisions or [])
        versions = {}
        for y, row in computed_by_year.items():
            active_raw = set((raw_data_by_year or {}).get(y, {}).keys())
            year_mappings = []
            for d in mapping_decisions:
                if d.raw_concept in active_raw:
                    year_mappings.append({
                        'raw_concept': d.raw_concept,
                        'ontology_node_id': d.ontology_node_id,
                        'confidence': d.confidence,
                        'source': d.source,
                    })
            avg_conf = 0.0
            if year_mappings:
                avg_conf = sum(m['confidence'] for m in year_mappings) / len(year_mappings)
            versions[y] = {
                'year': y,
                'active_nodes': sorted(row.keys()),
                'node_count': len(row),
                'mapping_confidence_avg': avg_conf,
                'mappings': year_mappings,
            }
        return versions

    def detect_structural_breaks(self, mapping_versions: Dict[int, Dict]) -> Dict:
        years = sorted(mapping_versions.keys())
        breaks = []
        volatility_parts = []

        for i in range(1, len(years)):
            y0, y1 = years[i - 1], years[i]
            s0 = set(mapping_versions[y0]['active_nodes'])
            s1 = set(mapping_versions[y1]['active_nodes'])
            added = s1 - s0
            removed = s0 - s1
            drift = (len(added) + len(removed)) / max(1, len(s0 | s1))
            volatility_parts.append(drift)
            if drift > 0.20:
                breaks.append({
                    'year': y1,
                    'drift_ratio': drift,
                    'added_nodes': sorted(added),
                    'removed_nodes': sorted(removed),
                    'reason': 'possible segment or reporting structure reorganization',
                })

        volatility_score = sum(volatility_parts) / max(1, len(volatility_parts))
        return {'structural_breaks': breaks, 'structural_volatility_score': volatility_score}
