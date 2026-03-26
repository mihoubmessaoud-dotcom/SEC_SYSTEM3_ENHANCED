import pandas as pd
import streamlit as st

from financial_analyzer.core.academic_policy import ACADEMIC_DISCLAIMER
from financial_analyzer.core.pipeline_orchestrator import PipelineOrchestrator
from financial_analyzer.models.ratio_result import RatioResult


st.set_page_config(page_title="التحليل المالي الأكاديمي", layout="wide", page_icon="📊")

if "orchestrator" not in st.session_state:
    st.session_state.orchestrator = PipelineOrchestrator()
if "results_cache" not in st.session_state:
    st.session_state.results_cache = {}

orchestrator = st.session_state.orchestrator
results_cache = st.session_state.results_cache


def _load_files(uploaded_files):
    for f in uploaded_files:
        result = orchestrator.run(f, results_cache)
        results_cache[result.ticker] = result


def _to_display(v, fmt="percent"):
    if isinstance(v, RatioResult):
        return v.to_display(fmt)
    if isinstance(v, dict):
        if v.get("value") is None:
            reason = v.get("reason") or "N/A"
            return f"— ({reason}) ⚠️"
        return str(v.get("value"))
    return "—" if v is None else str(v)


def _ratio_status(v):
    if isinstance(v, RatioResult):
        return v.status or ""
    if isinstance(v, dict):
        return v.get("status", "")
    return ""


def _ratio_reason(v):
    if isinstance(v, RatioResult):
        return v.reason or v.note or ""
    if isinstance(v, dict):
        return v.get("reason", "") or v.get("note", "")
    return ""


st.sidebar.markdown("### وضع العرض")
view_mode = st.sidebar.selectbox("النمط", ["أكاديمي"], index=0)
page = st.sidebar.radio("الصفحة", ["لوحة المحفظة", "تحليل شركة", "تشخيص"])

st.caption(ACADEMIC_DISCLAIMER)

if page == "لوحة المحفظة":
    files = st.file_uploader("ارفع ملفات Excel", accept_multiple_files=True, type=["xlsx"])
    if files:
        _load_files(files)

    if results_cache:
        rows = []
        for t, r in results_cache.items():
            rows.append(
                {
                    "الرمز": t,
                    "القطاع الفرعي": r.sub_sector,
                    "درجة جودة البيانات": r.data_quality_grade or "—",
                    "مؤشر الصحة المالية": r.financial_health_indicator or "—",
                    "المؤشر الأكاديمي العام": r.quality_score,
                    "الدرجة المهنية": r.professional_score,
                    "سنوات صالحة": len(r.valid_years),
                    "سنوات محدودة": len(r.blocked_years),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=500)

elif page == "تحليل شركة":
    if not results_cache:
        st.info("ارفع ملفًا واحدًا على الأقل أولاً.")
    else:
        ticker = st.selectbox("اختر الشركة", list(results_cache.keys()))
        r = results_cache[ticker]

        c1, c2, c3 = st.columns(3)
        c1.metric("درجة جودة البيانات", r.data_quality_grade or "—")
        c2.metric("مؤشر الصحة المالية", r.financial_health_indicator or "—")
        c3.metric("المؤشر الأكاديمي العام", f"{r.quality_score}/100")
        st.caption(ACADEMIC_DISCLAIMER)

        if r.blocked_years:
            st.warning(f"سنوات محدودة/محجوبة: {r.blocked_years}")

        latest = max(r.valid_years) if r.valid_years else None
        if latest is not None:
            st.subheader(f"النسب — السنة {latest}")
            ratio_rows = []
            for k, v in (r.ratios.get(latest, {}) or {}).items():
                ratio_rows.append(
                    {
                        "النسبة": k,
                        "القيمة": _to_display(v, "percent"),
                        "الحالة": _ratio_status(v),
                        "السبب/الملاحظة": _ratio_reason(v),
                    }
                )

            df_ratios = pd.DataFrame(ratio_rows)

            def _style_row(row):
                status = str(row.get("الحالة", ""))
                val = str(row.get("القيمة", ""))
                if "⚠️" in val or status not in ("COMPUTED", ""):
                    return ["color:#C0392B; font-weight:600"] * len(row)
                return [""] * len(row)

            st.dataframe(
                df_ratios.style.apply(_style_row, axis=1),
                use_container_width=True,
                height=420,
            )

            st.subheader("جودة السنوات المعروضة")
            q_rows = []
            for y in r.display_years:
                yq = (r.year_quality or {}).get(y, {})
                q_rows.append(
                    {
                        "السنة": y,
                        "التصنيف": yq.get("quality", "—"),
                        "الموثوقية": yq.get("reliability", "—"),
                        "تستخدم في الدرجة": yq.get("use_in_scoring", False),
                        "ملاحظة": yq.get("note", ""),
                    }
                )
            st.dataframe(pd.DataFrame(q_rows), use_container_width=True, height=260)

elif page == "تشخيص":
    if not results_cache:
        st.info("ارفع ملفات أولاً.")
    else:
        ticker = st.selectbox("اختر الشركة للتشخيص", list(results_cache.keys()), key="diag_ticker")
        r = results_cache[ticker]
        st.caption(ACADEMIC_DISCLAIMER)
        st.write("ملخص التدقيق:")
        st.json((r.audit.summary() if r.audit else {}))
        if r.audit and r.audit.flags:
            st.write("الإشارات التحذيرية:")
            st.json(r.audit.flags)
        if r.audit and r.audit.corrections:
            st.write("التصحيحات المطبقة:")
            st.dataframe(pd.DataFrame(r.audit.corrections), use_container_width=True, height=260)
