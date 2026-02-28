from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Tuple


XLINK_NS = "{http://www.w3.org/1999/xlink}"


def _local(tag: str) -> str:
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _concept_from_href(href: Optional[str]) -> Optional[str]:
    if not isinstance(href, str) or "#" not in href:
        return None
    frag = href.split("#", 1)[-1]
    return frag.split(":", 1)[-1] if ":" in frag else frag


def normalize_unit(unit: Optional[str]) -> Optional[str]:
    if not isinstance(unit, str):
        return None
    u = unit.strip().upper()
    if not u:
        return None
    if "USD" in u:
        return "USD"
    if u.startswith("ISO4217:"):
        return u.split(":", 1)[-1]
    return u


def parse_presentation_linkbase(xml_text: Optional[str]) -> Dict[str, Dict]:
    if not isinstance(xml_text, str) or not xml_text.strip():
        return {}
    root = ET.fromstring(xml_text)
    by_role: Dict[str, Dict] = {}
    for link in root.iter():
        if _local(link.tag) != "presentationLink":
            continue
        role_uri = link.attrib.get(f"{XLINK_NS}role") or "unknown_role"
        loc_map: Dict[str, str] = {}
        children_map: Dict[str, List[str]] = {}
        parent_map: Dict[str, str] = {}
        child_order_map: Dict[Tuple[str, str], float] = {}
        for node in list(link):
            nname = _local(node.tag)
            if nname == "loc":
                label = node.attrib.get(f"{XLINK_NS}label")
                href = node.attrib.get(f"{XLINK_NS}href")
                concept = _concept_from_href(href)
                if label and concept:
                    loc_map[label] = concept
            elif nname == "presentationArc":
                src = node.attrib.get(f"{XLINK_NS}from")
                dst = node.attrib.get(f"{XLINK_NS}to")
                parent = loc_map.get(src)
                child = loc_map.get(dst)
                if not parent or not child:
                    continue
                try:
                    order = float(node.attrib.get("order", "999999"))
                except Exception:
                    order = 999999.0
                children_map.setdefault(parent, [])
                if child not in children_map[parent]:
                    children_map[parent].append(child)
                parent_map[child] = parent
                child_order_map[(parent, child)] = order
        all_nodes = set(children_map.keys()) | set(parent_map.keys())
        roots = [n for n in all_nodes if n not in parent_map]
        depth_map: Dict[str, int] = {}
        stack = [(r, 0) for r in roots]
        while stack:
            cur, depth = stack.pop()
            if cur in depth_map and depth_map[cur] <= depth:
                continue
            depth_map[cur] = depth
            for ch in children_map.get(cur, []):
                stack.append((ch, depth + 1))
        nodes = []
        for concept in sorted(all_nodes):
            parent_concept = parent_map.get(concept)
            order = None
            if parent_concept:
                order = child_order_map.get((parent_concept, concept))
            nodes.append(
                {
                    "concept_name": concept,
                    "label": humanize_concept(concept),
                    "role_uri": role_uri,
                    "depth": int(depth_map.get(concept, 0)),
                    "parent_concept": parent_concept,
                    "children_concepts": list(children_map.get(concept, [])),
                    "order": order,
                    "isAbstract": concept.lower().endswith("abstract"),
                }
            )
        by_role[role_uri] = {
            "statement_name": role_uri.rsplit("/", 1)[-1],
            "nodes": nodes,
            "children_map": children_map,
            "parent_map": parent_map,
        }
    return by_role


def parse_calculation_linkbase(xml_text: Optional[str]) -> Dict[str, Dict]:
    if not isinstance(xml_text, str) or not xml_text.strip():
        return {}
    root = ET.fromstring(xml_text)
    by_role: Dict[str, Dict] = {}
    for link in root.iter():
        if _local(link.tag) != "calculationLink":
            continue
        role_uri = link.attrib.get(f"{XLINK_NS}role") or "unknown_role"
        loc_map: Dict[str, str] = {}
        arcs = []
        for node in list(link):
            nname = _local(node.tag)
            if nname == "loc":
                label = node.attrib.get(f"{XLINK_NS}label")
                href = node.attrib.get(f"{XLINK_NS}href")
                concept = _concept_from_href(href)
                if label and concept:
                    loc_map[label] = concept
            elif nname == "calculationArc":
                src = node.attrib.get(f"{XLINK_NS}from")
                dst = node.attrib.get(f"{XLINK_NS}to")
                parent = loc_map.get(src)
                child = loc_map.get(dst)
                if not parent or not child:
                    continue
                try:
                    weight = float(node.attrib.get("weight", "1"))
                except Exception:
                    weight = 1.0
                arcs.append({"parent": parent, "child": child, "weight": weight})
        children_by_parent: Dict[str, List[Dict]] = {}
        for a in arcs:
            children_by_parent.setdefault(a["parent"], []).append({"concept": a["child"], "weight": a["weight"]})
        by_role[role_uri] = {"arcs": arcs, "children_by_parent": children_by_parent}
    return by_role


def parse_instance_xbrl(xml_text: Optional[str]) -> Dict:
    if not isinstance(xml_text, str) or not xml_text.strip():
        return {"contexts": {}, "units": {}, "facts_by_concept": {}}
    root = ET.fromstring(xml_text)

    contexts: Dict[str, Dict] = {}
    units: Dict[str, str] = {}

    for node in root.iter():
        if _local(node.tag) == "context":
            ctx_id = node.attrib.get("id")
            if not ctx_id:
                continue
            period_type = None
            start = None
            end = None
            instant = None
            has_dimensions = False
            for c in node.iter():
                name = _local(c.tag)
                if name == "segment":
                    has_dimensions = True
                if name == "startDate":
                    start = (c.text or "").strip() or None
                elif name == "endDate":
                    end = (c.text or "").strip() or None
                elif name == "instant":
                    instant = (c.text or "").strip() or None
            if instant:
                period_type = "INSTANT"
                end_final = instant
            else:
                period_type = "DURATION"
                end_final = end
            contexts[ctx_id] = {
                "context_id": ctx_id,
                "period_type": period_type,
                "period_start": start,
                "period_end": end_final,
                "has_dimensions": has_dimensions,
            }
        elif _local(node.tag) == "unit":
            uid = node.attrib.get("id")
            if not uid:
                continue
            measure = None
            for c in node.iter():
                if _local(c.tag) == "measure":
                    measure = (c.text or "").strip()
                    break
            units[uid] = normalize_unit(measure) or measure

    facts_by_concept: Dict[str, List[Dict]] = {}
    skip = {"context", "unit", "schemaRef", "footnoteLink"}
    for node in root.iter():
        name = _local(node.tag)
        if name in skip:
            continue
        ctx = node.attrib.get("contextRef")
        if not ctx:
            continue
        txt = (node.text or "").strip()
        if not txt:
            continue
        try:
            value = float(txt.replace(",", ""))
        except Exception:
            continue
        unit_ref = node.attrib.get("unitRef")
        unit = units.get(unit_ref)
        cmeta = contexts.get(ctx, {})
        facts_by_concept.setdefault(name, []).append(
            {
                "concept_name": name,
                "label": humanize_concept(name),
                "value": value,
                "context_id": ctx,
                "period_type": cmeta.get("period_type"),
                "period_start": cmeta.get("period_start"),
                "period_end": cmeta.get("period_end"),
                "unit": unit,
                "unit_normalized": normalize_unit(unit),
                "has_dimensions": cmeta.get("has_dimensions", False),
            }
        )
    return {"contexts": contexts, "units": units, "facts_by_concept": facts_by_concept}


def humanize_concept(concept_name: Optional[str]) -> str:
    s = str(concept_name or "")
    if not s:
        return ""
    out = []
    cur = ""
    for ch in s:
        if ch.isupper() and cur and not cur[-1].isupper():
            out.append(cur)
            cur = ch
        else:
            cur += ch
    if cur:
        out.append(cur)
    return " ".join(out)


def role_kind(role_uri: Optional[str]) -> Optional[str]:
    txt = str(role_uri or "").lower()
    if ("cashflow" in txt) or ("cashflows" in txt):
        return "cash_flow"
    if ("balancesheet" in txt) or ("financialposition" in txt):
        return "balance_sheet"
    if ("income" in txt) or ("operations" in txt):
        return "income_statement"
    return None


def pick_primary_role(presentation_by_role: Dict[str, Dict], statement_kind: str) -> Optional[str]:
    for role_uri in presentation_by_role.keys():
        if role_kind(role_uri) == statement_kind:
            return role_uri
    return None


def select_income_statement_role(
    presentation_by_role: Dict[str, Dict],
    facts_by_concept: Optional[Dict[str, List[Dict]]] = None,
) -> Optional[str]:
    """
    Prefer Income Statement role by:
    1) role name keywords
    2) coverage of revenue + net income concepts present in the role and facts
    """
    if not presentation_by_role:
        return None
    facts_lc = {str(k).lower() for k in (facts_by_concept or {}).keys()}
    role_keywords = (
        "consolidatedstatementsofoperations",
        "statementofincome",
        "statementsofincome",
        "incomestatement",
        "operation",
    )
    best = None
    best_score = -10**9
    for role_uri, payload in presentation_by_role.items():
        txt = str(role_uri or "").lower().replace(" ", "")
        role_score = 0
        if any(k in txt for k in role_keywords):
            role_score += 8
        if role_kind(role_uri) == "income_statement":
            role_score += 5
        nodes = (payload or {}).get("nodes") or []
        concepts = [str(n.get("concept_name") or "") for n in nodes]
        concepts_lc = [c.lower() for c in concepts]

        # Coverage: reward roles containing likely revenue + net-income concepts.
        has_revenue = any(("revenue" in c and "cost" not in c) or "salesrevenue" in c for c in concepts_lc)
        has_net_income = any(("netincome" in c) or ("profitloss" in c) for c in concepts_lc)
        if has_revenue:
            role_score += 6
        if has_net_income:
            role_score += 6

        # Prefer if those concepts also exist in instance facts.
        facts_hit = 0
        if facts_lc:
            for c in concepts:
                base = c.split(":", 1)[-1]
                if "_" in base:
                    base = base.split("_", 1)[-1]
                if base.lower() in facts_lc:
                    facts_hit += 1
            role_score += min(10, facts_hit)

        # Prefer deeper / richer role if tie.
        role_score += len(nodes) / 1000.0
        if role_score > best_score:
            best_score = role_score
            best = role_uri
    return best


def find_statement_anchor_context(
    facts_by_concept: Dict[str, List[Dict]],
    concept_candidates: Dict[str, List[str]],
) -> Tuple[Optional[Dict], Dict]:
    # metric keys: revenue,gross_profit,operating_income,net_income
    # choose context that maximizes co-occurrence across these four.
    contexts_score: Dict[str, Dict] = {}
    for metric, concepts in (concept_candidates or {}).items():
        for concept in concepts or []:
            for fact in facts_by_concept.get(concept, []):
                ctx = fact.get("context_id")
                if not ctx:
                    continue
                row = contexts_score.setdefault(
                    ctx,
                    {
                        "context_id": ctx,
                        "hit_metrics": set(),
                        "period_type": fact.get("period_type"),
                        "period_end": fact.get("period_end"),
                        "unit": fact.get("unit_normalized"),
                    },
                )
                row["hit_metrics"].add(metric)
    if not contexts_score:
        return None, {"reason": "no_context_candidates"}
    ranked = []
    for ctx, row in contexts_score.items():
        hit_count = len(row.get("hit_metrics", set()))
        is_duration = 1 if str(row.get("period_type") or "").upper() == "DURATION" else 0
        end = _parse_date(row.get("period_end"))
        ranked.append((hit_count, is_duration, end or datetime.min, ctx, row))
    ranked.sort(key=lambda t: (t[0], t[1], t[2]), reverse=True)
    best = ranked[0][4]
    if len(best.get("hit_metrics", set())) < 3:
        return None, {"reason": "anchor_not_consistent", "best_context_hits": sorted(list(best.get("hit_metrics", set())))}
    best["hit_metrics"] = sorted(list(best.get("hit_metrics", set())))
    return best, {"reason": "ok", "candidates": len(contexts_score)}


def _parse_date(v: Optional[str]) -> Optional[datetime]:
    if not isinstance(v, str) or not v.strip():
        return None
    txt = v.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            continue
    return None
