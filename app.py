"""
Optional Streamlit dashboard layer for portfolio/company drill-down.
This file is additive and does not alter the Tk desktop workflow in main.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

try:
    import streamlit as st
    import plotly.express as px
    import plotly.graph_objects as go
except Exception:  # pragma: no cover
    st = None
    px = None
    go = None


@dataclass
class ForecastPack:
    years: List[int] = field(default_factory=list)
    base: List[float] = field(default_factory=list)
    bull: List[float] = field(default_factory=list)
    bear: List[float] = field(default_factory=list)
    confidence_pct: List[float] = field(default_factory=list)


@dataclass
class CompanyData:
    ticker: str
    sub_sector: str
    original_score: float
    corrected_score: float
    original_verdict: str
    corrected_verdict: str
    recommended_action: str = ""
    roic_latest: Optional[float] = None
    pe_latest: Optional[float] = None
    fcf_yield_latest: Optional[float] = None
    fcf_yield: Optional[float] = None
    altman_z: Optional[float] = None
    revenue_cagr: Optional[float] = None
    market_cap: Optional[float] = None
    years: List[int] = field(default_factory=list)
    gross_margins: List[float] = field(default_factory=list)
    operating_margins: List[float] = field(default_factory=list)
    net_margins: List[float] = field(default_factory=list)
    forecasts: ForecastPack = field(default_factory=ForecastPack)
    quality_issues: List[str] = field(default_factory=list)
    impossible_values: List[str] = field(default_factory=list)
    ratio_audit_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    failure_prob_3y: float = 0.0


def get_sector_averages(_sub_sector: str):
    return {"gross_margin": None}


def show_correction_alert(ticker, original_score, corrected_score, sub_sector, original_verdict, corrected_verdict):
    if st is None:
        return
    diff = corrected_score - original_score
    if abs(diff) > 15:
        direction = "ارتفعت" if diff > 0 else "انخفضت"
        st.info(
            f"تصحيح تلقائي: درجة {ticker} {direction} من {original_score} إلى {corrected_score} "
            f"بعد تطبيق نموذج {sub_sector}. الحكم الأصلي: {original_verdict} ← الحكم المُصحَّح: {corrected_verdict}"
        )


def portfolio_dashboard(companies: List[CompanyData]):
    if st is None or px is None:
        raise RuntimeError("streamlit/plotly غير متوفرين في البيئة الحالية.")

    st.title("لوحة المحفظة — التحليل المالي الاحترافي")
    col1, col2, col3 = st.columns(3)
    with col1:
        sector_filter = st.multiselect("القطاع الفرعي", options=sorted({c.sub_sector for c in companies}), default=None)
    with col2:
        verdict_filter = st.multiselect("الحكم", options=["PASS", "WATCH", "FAIL"], default=["PASS", "WATCH", "FAIL"])
    with col3:
        min_score = st.slider("الدرجة الدنيا", 0, 100, 0)

    filtered = [
        c for c in companies
        if (not sector_filter or c.sub_sector in sector_filter)
        and c.corrected_verdict in verdict_filter
        and c.corrected_score >= min_score
    ]
    rows = [{
        "رمز": c.ticker,
        "القطاع الفرعي": c.sub_sector,
        "الدرجة": c.corrected_score,
        "الحكم": c.corrected_verdict,
        "ROIC": f"{c.roic_latest:.1%}" if c.roic_latest is not None else "N/A",
        "P/E": f"{c.pe_latest:.1f}×" if c.pe_latest is not None else "N/A",
        "FCF Yield": f"{(c.fcf_yield_latest if c.fcf_yield_latest is not None else c.fcf_yield):.1%}" if (c.fcf_yield_latest is not None or c.fcf_yield is not None) else "N/A",
        "الإجراء": c.recommended_action,
    } for c in filtered]

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, height=500)

    st.subheader("خريطة المحفظة: ROIC × نمو الإيرادات")
    df_enriched = pd.DataFrame([{
        "ticker": c.ticker,
        "revenue_cagr": c.revenue_cagr or 0.0,
        "roic": c.roic_latest or 0.0,
        "market_cap": c.market_cap or 1.0,
        "corrected_verdict": c.corrected_verdict,
    } for c in filtered])
    if not df_enriched.empty:
        fig = px.scatter(
            df_enriched,
            x="revenue_cagr",
            y="roic",
            size="market_cap",
            color="corrected_verdict",
            hover_name="ticker",
            color_discrete_map={"PASS": "#1E7C47", "WATCH": "#B7720A", "FAIL": "#C0392B"},
            labels={"revenue_cagr": "نمو الإيرادات السنوي %", "roic": "العائد على رأس المال المستثمر %"},
            title="خريطة القيمة — حجم الفقاعة = القيمة السوقية",
        )
        st.plotly_chart(fig, use_container_width=True)


def company_detail_page(company: CompanyData):
    if st is None or go is None:
        raise RuntimeError("streamlit/plotly غير متوفرين في البيئة الحالية.")

    st.title(f"تحليل {company.ticker} — {company.sub_sector}")
    show_correction_alert(
        company.ticker,
        company.original_score,
        company.corrected_score,
        company.sub_sector,
        company.original_verdict,
        company.corrected_verdict,
    )
    tabs = st.tabs(["الملخص", "الأداء", "السيولة", "التقييم", "التوقعات", "النظراء", "التقرير", "التدقيق"])
    with tabs[0]:
        m1, m2, m3 = st.columns(3)
        m1.metric("الدرجة المُصحَّحة", f"{company.corrected_score}/100", delta=f"{company.corrected_score - company.original_score:+.0f}")
        m2.metric("الحكم", company.corrected_verdict)
        m3.metric("ROIC", f"{company.roic_latest:.1%}" if company.roic_latest is not None else "N/A")
    with tabs[1]:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=company.years, y=company.gross_margins, name="Gross Margin", line=dict(color="#1E7C47", width=2)))
        fig.add_trace(go.Scatter(x=company.years, y=company.operating_margins, name="Operating Margin", line=dict(color="#2E75B6", width=2)))
        fig.add_trace(go.Scatter(x=company.years, y=company.net_margins, name="Net Margin", line=dict(color="#B7720A", width=2, dash="dash")))
        fig.update_layout(title="الهوامش عبر السنوات", yaxis_tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)
    with tabs[4]:
        fc = company.forecasts
        fig2 = go.Figure([
            go.Scatter(x=fc.years, y=fc.base, name="أساسي", line=dict(color="#2E75B6")),
            go.Scatter(x=fc.years, y=fc.bull, name="متفائل", line=dict(color="#1E7C47", dash="dash")),
            go.Scatter(x=fc.years, y=fc.bear, name="متحفظ", line=dict(color="#C0392B", dash="dot")),
        ])
        st.plotly_chart(fig2, use_container_width=True)
    with tabs[7]:
        if company.quality_issues:
            for issue in company.quality_issues:
                st.warning(issue)
        if company.impossible_values:
            for iv in company.impossible_values:
                st.error(iv)
        st.dataframe(company.ratio_audit_df, use_container_width=True)

