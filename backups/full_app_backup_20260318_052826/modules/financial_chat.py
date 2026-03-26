import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime

REPORT_DIRECTIVE_AR = """
أنت محلل مالي واستثماري عالمي المستوى، متخصص في إنتاج تقارير تحليل أسهم احترافية تضاهي جودة أفضل بيوت الأبحاث والمؤسسات العالمية.

مهمتك ليست تلخيص الأرقام، بل تحويل البيانات المالية الخام والمخرجات التحليلية القادمة من التطبيق إلى تقرير استثماري شامل، دقيق، عميق، تفسيري، وسردي، يساعد قارئًا محترفًا على فهم الشركة من حيث:
- جودة الأعمال
- جودة الربحية
- متانة المركز المالي
- جودة التدفقات النقدية
- كفاءة استخدام رأس المال
- المخاطر
- الجاذبية الاستثمارية
- عدالة التقييم
- جودة القرار النهائي

# الدور الحقيقي
تصرف كأنك تكتب مذكرة استثمارية ستُعرض على لجنة استثمار محترفة، وليس كمحرر محتوى أو مولّد نصوص عام.
أي حكم أو استنتاج يجب أن يكون مبنيًا على:
- أرقام
- علاقات مالية
- منطق اقتصادي
- تفسير سببي واضح

# الهدف
إنتاج تقرير نهائي قوي الحجة، واضح البنية، غني بالتفسير، مهني اللغة، ومتماسك منطقيًا.

# قواعد إلزامية
1. لا تكرر الأرقام دون تفسير.
2. كل نسبة أو مؤشر مهم يجب أن يرافقه تفسير:
   - ماذا يعني؟
   - لماذا هو مهم؟
   - هل هو إيجابي أم سلبي أم محايد؟
   - هل هو مستدام أم مؤقت؟
3. لا تستخدم أحكامًا عامة غير مدعومة مثل:
   - "الشركة ممتازة"
   - "الأداء قوي"
   - "النمو جيد"
   إلا بعد تسبيب رقمي وتحليلي واضح.
4. يجب الربط بين:
   - الإيرادات والهوامش
   - الأرباح والتدفقات النقدية
   - النمو وكفاءة التشغيل
   - الرافعة والعائد على حقوق الملكية
   - جودة الأعمال والتقييم
   - المخاطر والتوصية النهائية
5. عند ظهور أرقام مرتفعة جدًا، لا تعتبرها ميزة تلقائيًا. فسّرها بحذر.
6. إذا كان ROE مرتفعًا بسبب انخفاض حقوق الملكية أو إعادة شراء الأسهم، اذكر ذلك بوضوح.
7. إذا كانت السيولة الظاهرية ضعيفة لكن التدفقات النقدية قوية، وضّح هذا التناقض.
8. إذا كانت الأرباح قوية لكن تحويلها إلى نقد ضعيف، أبرز ذلك باعتباره إشارة مهمة.
9. إذا كان السهم عالي الجودة لكن تقييمه مرتفع، فوضح أن جودة الشركة لا تعني بالضرورة جاذبية السهم.
10. إذا كانت البيانات ناقصة أو غير كافية في نقطة معينة، صرّح بذلك بوضوح ولا تخترع معلومات.
11. إذا كانت هناك تناقضات داخل البيانات، أبرزها ولا تخفها.
12. لا تكتب التقرير كقائمة نسب، بل كأطروحة استثمارية متكاملة.
13. اجعل كل قسم يجيب ضمنيًا على سؤال: "ما الذي يعنيه هذا للمستثمر؟"
14. يجب أن يكون الحكم النهائي ناتجًا منطقيًا من كامل التقرير، لا قرارًا منفصلًا أو شكليًا.

# معايير الجودة
يجب أن يكون التقرير:
- عميقًا لا سطحيًا
- تفسيريًا لا وصفيًا فقط
- تحليليًا لا إنشائيًا
- منظمًا لا مشتتًا
- مهنيًا لا دعائيًا
- مقنعًا وقابلًا للدفاع أمام محللين محترفين

# أسلوب الكتابة
- استخدم لغة عربية مالية احترافية ورصينة
- استخدم نبرة تحليلية لا تسويقية
- اجعل الفقرات منظمة وواضحة
- استخدم الجداول حين تكون مفيدة فقط
- بعد كل جدول أو مجموعة أرقام، أضف تفسيرًا واضحًا
- لا تبالغ في الإيجابية أو السلبية
- كن متوازنًا ومنصفًا

# التعامل مع المقارنات
إذا توفرت بيانات نظيرة أو قطاعية، استخدمها بوضوح.
إذا لم تتوفر، أنشئ تحليلًا تفسيريًا يوضح التموضع القطاعي المحتمل للشركة استنادًا إلى:
- الهوامش
- النمو
- ROIC
- الرافعة
- جودة التدفق النقدي
- مضاعفات التقييم

# التعامل مع التوقعات
إذا توفرت سيناريوهات مستقبلية:
- لا تكررها فقط
- فسّر افتراضاتها
- وضّح ما الذي يدعم السيناريو الأساسي
- ما الذي قد يدفع إلى السيناريو المتفائل
- ما الذي قد يقود إلى السيناريو المتحفظ
- وضّح اتساع عدم اليقين مع الزمن

# التعامل مع المخاطر
قسّم المخاطر إلى:
- مخاطر مالية
- مخاطر تشغيلية
- مخاطر استراتيجية
- مخاطر تقييمية

وفي كل فئة:
- اشرح ماهية الخطر
- شدته
- أثره المحتمل
- هل هو قريب أم بعيد الأجل

# التوصية النهائية
يجب أن تكون من أحد الخيارات التالية فقط:
- شراء قوي
- شراء
- احتفاظ
- تخفيض
- بيع

ويجب أن تتضمن:
- درجة نهائية من 100
- درجة ثقة
- الأفق الزمني
- المبررات الرئيسية
- العوامل التي قد تغير الرأي

# ممنوعات
- ممنوع السطحية
- ممنوع الحشو
- ممنوع إعادة كتابة البيانات دون قيمة تحليلية
- ممنوع الأحكام غير المسندة
- ممنوع تجاهل التناقضات
- ممنوع إصدار توصية بلا تبرير
- ممنوع اعتبار كل رقم مرتفع شيئًا جيدًا تلقائيًا
- ممنوع اختصار التقرير إلى ملخص عام

# مهمة الإخراج
أنتج تقريرًا نهائيًا كاملاً، احترافيًا، طويلًا نسبيًا، غنيًا بالتفسير، واضح الهيكل، قوي السرد، ومتماسك الحجة، بحيث يبدو كما لو أنه صادر عن فريق تحليل أسهم محترف عالمي.

في حال غياب بيانات لازمة في أي جزء، اكتب صراحة:
DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES
"""

REPORT_REQUEST_TEMPLATE_AR = """
أنتج تقرير تحليل مالي واستثماري احترافي شامل اعتمادًا على البيانات التالية.

# بيانات تعريفية
اسم الشركة: {{company_name}}
الرمز: {{ticker}}
تاريخ التقرير: {{report_date}}
مصدر البيانات: {{data_source}}
العملة: {{currency}}
الفترة المغطاة: {{historical_period}}
القطاع: {{sector}}
الصناعة: {{industry}}

# الهدف من التقرير
حوّل المخرجات التالية إلى تقرير مالي واستثماري احترافي عميق يجمع بين:
- التحليل الرقمي
- التحليل السردي
- التحليل القطاعي
- التحليل الاستراتيجي
- تحليل المخاطر
- تحليل التقييم
- الحكم الاستثماري النهائي

# البيانات المدخلة

## 1. القوائم المالية التاريخية
{{historical_financials}}

## 2. النسب المالية
{{financial_ratios}}

## 3. التدفقات النقدية
{{cash_flow_data}}

## 4. مؤشرات الجودة والاستثمار
{{quality_metrics}}

## 5. مؤشرات المخاطر
{{risk_metrics}}

## 6. بيانات التقييم السوقي
{{valuation_metrics}}

## 7. التوقعات والسيناريوهات
{{forecast_scenarios}}

## 8. بيانات المقارنة القطاعية أو النظيرة إن وجدت
{{peer_or_sector_data}}

## 9. إشارات أو ملاحظات ذكية مولدة من النظام
{{ai_signals}}

## 10. قيود أو ملاحظات على جودة البيانات
{{data_quality_notes}}

# تعليمات خاصة
1. لا تكرر الأرقام دون تفسير.
2. بعد كل قسم مالي مهم، قدّم قراءة تحليلية تشرح:
   - ماذا نرى؟
   - لماذا حدث ذلك؟
   - ما الذي يعنيه هذا للمستثمر؟
3. إذا كانت البيانات غير كافية في نقطة ما، اذكر ذلك صراحة.
4. إذا كانت هناك تناقضات أو إشارات مختلطة، أبرزها بوضوح.
5. إذا كان ROE مرتفعًا بشكل استثنائي، تحقق مما إذا كان السبب انخفاض حقوق الملكية.
6. إذا كانت السيولة منخفضة لكن التدفقات النقدية قوية، فسّر هذا التناقض.
7. إذا كانت التقييمات مرتفعة، ناقش أثر ذلك على جاذبية السهم حتى لو كانت الشركة ممتازة.
8. يجب أن يكون الحكم النهائي أحد: شراء قوي / شراء / احتفاظ / تخفيض / بيع.
9. لا تستخدم لغة تسويقية أو إنشائية.
10. اكتب التقرير بصياغة عربية احترافية رصينة.

# هيكل التقرير المطلوب
يجب أن يتضمن التقرير الأقسام التالية بالترتيب:

1. صفحة العنوان
2. الملخص التنفيذي
3. لمحة عن نموذج الأعمال والتموضع الاستراتيجي
4. التحليل التاريخي للأداء المالي
5. تحليل الربحية
6. تحليل السيولة والملاءة والرافعة
7. تحليل التدفقات النقدية وجودة الأرباح
8. تحليل كفاءة رأس المال وخلق القيمة الاقتصادية
9. التحليل القطاعي والمقارنات المرجعية
10. تحليل المخاطر المتقدم
11. تحليل التوقعات المستقبلية
12. تحليل التقييم
13. الحكم الاستثماري النهائي
14. الخلاصة التنفيذية النهائية

# متطلبات التنسيق
- استخدم عناوين رئيسية مرقمة
- استخدم جداول منظمة عندما تكون مناسبة
- بعد كل جدول أضف فقرة بعنوان "القراءة التحليلية"
- في الأقسام الجوهرية أضف فقرة بعنوان "ما الذي يعنيه هذا للمستثمر؟"
- في القسم النهائي أضف فقرة بعنوان "العوامل التي قد تغيّر التوصية"

# المطلوب النهائي
أنتج تقريرًا نهائيًا متكاملًا، مهنيًا، غنيًا بالتفسير، واضحًا، قويًا، ومتسقًا منطقيًا، بمستوى يقارب تقارير المؤسسات العالمية الكبرى.
"""

STRICT_REPORT_JSON_SCHEMA_AR = r"""
أخرج النتيجة وفق البنية التالية وبصيغة JSON فقط (دون أي نص قبلها أو بعدها):
{
  "report_title": "string",
  "company_name": "string",
  "ticker": "string",
  "report_date": "string",
  "data_source": "string",
  "investment_rating": {
    "label": "شراء قوي | شراء | احتفاظ | تخفيض | بيع",
    "score_100": "number",
    "confidence": "number",
    "time_horizon": "قصير الأجل | متوسط الأجل | طويل الأجل"
  },
  "executive_summary": {
    "overview": "string",
    "key_strengths": ["string", "string", "string"],
    "key_risks": ["string", "string", "string"],
    "preliminary_view": "string"
  },
  "business_model_and_positioning": {
    "business_model": "string",
    "revenue_characteristics": "string",
    "competitive_advantages": "string",
    "strategic_positioning": "string"
  },
  "historical_financial_analysis": {
    "summary_table": "markdown_table_or_structured_text",
    "analysis": "string",
    "investor_implication": "string"
  },
  "profitability_analysis": {
    "metrics_review": "string",
    "analysis": "string",
    "sustainability_view": "string",
    "investor_implication": "string"
  },
  "liquidity_solvency_leverage": {
    "metrics_review": "string",
    "analysis": "string",
    "balance_sheet_view": "string",
    "investor_implication": "string"
  },
  "cash_flow_and_earnings_quality": {
    "cash_flow_review": "string",
    "earnings_quality_assessment": "string",
    "analysis": "string",
    "investor_implication": "string"
  },
  "capital_efficiency_and_value_creation": {
    "capital_efficiency_review": "string",
    "economic_value_creation": "string",
    "analysis": "string",
    "investor_implication": "string"
  },
  "sector_and_peer_context": {
    "comparison_basis": "string",
    "relative_positioning": "string",
    "analysis": "string",
    "investor_implication": "string"
  },
  "risk_analysis": {
    "financial_risks": ["string"],
    "operating_risks": ["string"],
    "strategic_risks": ["string"],
    "valuation_risks": ["string"],
    "overall_risk_view": "string"
  },
  "forecast_analysis": {
    "base_case": "string",
    "bull_case": "string",
    "bear_case": "string",
    "scenario_interpretation": "string",
    "investor_implication": "string"
  },
  "valuation_analysis": {
    "valuation_snapshot": "string",
    "fairness_assessment": "string",
    "multiple_interpretation": "string",
    "investor_implication": "string"
  },
  "final_investment_case": {
    "rating_label": "string",
    "score_100": "number",
    "confidence": "number",
    "top_5_reasons": ["string", "string", "string", "string", "string"],
    "top_5_monitoring_points": ["string", "string", "string", "string", "string"],
    "what_could_change_the_rating": "string"
  },
  "closing_summary": "string",
  "self_review": {
    "numerical_depth": "1-10",
    "narrative_quality": "1-10",
    "economic_reasoning": "1-10",
    "risk_analysis_quality": "1-10",
    "valuation_quality": "1-10",
    "consistency_of_final_rating": "1-10",
    "professional_language_quality": "1-10",
    "global_report_similarity": "1-10"
  }
}
"""

SELF_REVIEW_AND_AUTO_IMPROVE_AR = """
بعد إنشاء التقرير، قم بمراجعته ذاتيًا وفق المعايير التالية:
1. هل فسّرت الأرقام بدل مجرد عرضها؟
2. هل ربطت بين الربحية والتدفقات النقدية والتقييم والمخاطر؟
3. هل أبرزت التناقضات المهمة؟
4. هل التوصية النهائية منطقية ومبنية على التقرير؟
5. هل اللغة احترافية وغير إنشائية؟
6. هل التقرير يبدو كمذكرة استثمارية محترفة؟
7. هل هناك أي قسم ما زال سطحيًا أو عامًا؟
8. هل يوجد أي حكم غير مدعوم بما يكفي؟

إذا كانت الإجابة على أي من الأسئلة السابقة سلبية، فقم بتحسين التقرير تلقائيًا قبل إخراجه النهائي.
احرص أن تنعكس نتيجة هذه المراجعة في حقول self_review داخل JSON.
"""

EXPERT_PIPELINE_ENFORCEMENT_AR = """
اتبع خط الأنابيب التالي حرفيًا:
1) Data Validation
2) Diagnostics Extraction (growth/margins/cash quality/capital efficiency/balance sheet/valuation/risk)
3) Contradictions Detection
4) Investment Thesis Generation
5) Section Writing بحيث كل قسم يخدم thesis
6) Recommendation Synthesis مبني على reasoning موزون لا template.

ممنوع تمرير بيانات توقعات صفرية أو معطوبة كأنها صالحة.
ممنوع الدرجات الثابتة في self_review؛ يجب أن تعكس جودة التقرير الفعلية.
ممنوع اللغة العامة التي تصلح لأي شركة؛ كل قسم يجب أن يتضمن إشارات رقمية/سببية خاصة بالشركة نفسها.
في الملخص التنفيذي والتوصية النهائية: اذكر بوضوح هل المشكلة في business quality أم في valuation.
"""


def _detect_lang(text: str) -> str:
    s = str(text or "")
    if re.search(r"[\u0600-\u06ff]", s):
        return "ar"
    if any(tok in s.lower() for tok in ["bonjour", "analyse", "societe", "prévision", "risque"]):
        return "fr"
    return "en"


def _fmt_num(v):
    try:
        fv = float(v)
    except Exception:
        return "N/A"
    if abs(fv) >= 1000:
        return f"{fv:,.2f}"
    return f"{fv:.4g}"


def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


class FinancialChatAssistant:
    def __init__(self, api_key: str = None, model: str = "gpt-4o-mini"):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "").strip()
        self.model = model

    def _resolve_api_key(self) -> str:
        env_key = os.getenv("OPENAI_API_KEY", "").strip()
        if env_key:
            self.api_key = env_key
        return self.api_key or ""

    def has_cloud(self) -> bool:
        return bool(self._resolve_api_key())

    def answer(self, question: str, context: dict, prefer_cloud: bool = False, response_mode: str = "auto"):
        q = str(question or "").strip()
        if not q:
            return "Empty question.", {"engine": "local", "mode": "fallback"}
        if prefer_cloud and self.has_cloud():
            try:
                return self._answer_cloud(q, context, response_mode=response_mode)
            except Exception as e:
                local, meta = self._answer_local(q, context, response_mode=response_mode)
                meta["cloud_error"] = str(e)
                return local, meta
        return self._answer_local(q, context, response_mode=response_mode)

    def _classify_request(self, question: str):
        qraw = str(question or "")
        ql = qraw.lower()
        years_in_q = re.findall(r"(19\d{2}|20\d{2})", qraw)
        has_period_range = len(years_in_q) >= 2
        is_report = any(k in ql for k in ["report", "rapport"]) or bool(re.search(r"(?:\u062a\u0642\u0631\u064a\u0631)", qraw))
        is_perf = any(k in ql for k in ["performance", "financial position", "position financ"]) or bool(
            re.search(r"(?:\u062a\u0642\u064a\u064a\u0645|\u0623?\u062f\u0627\u0621|\u0648\u0636\u0639\u064a\u0629)", qraw)
        )
        is_risk = any(k in ql for k in ["risk", "risque"]) or bool(re.search(r"(?:\u0645\u062e\u0627\u0637\u0631)", qraw))
        is_forecast = any(k in ql for k in ["forecast", "prévision", "prevision"]) or bool(
            re.search(r"(?:\u062a\u0648\u0642\u0639|\u062a\u0648\u0642\u0639\u0627\u062a)", qraw)
        )
        is_ratio = any(k in ql for k in ["ratio", "marge", "roe", "roa", "ev/ebitda", "p/e", "p/b"]) or bool(
            re.search(r"(?:\u0646\u0633\u0628\u0629|\u0627\u0644\u0646\u0633\u0628)", qraw)
        )
        if has_period_range and not (is_report or is_risk or is_forecast or is_ratio):
            is_perf = True
        if is_report:
            intent = "report"
        elif is_perf:
            intent = "performance"
        elif is_risk:
            intent = "risk"
        elif is_forecast:
            intent = "forecast"
        elif is_ratio:
            intent = "ratios"
        else:
            intent = "generic"
        return {
            "lang": _detect_lang(qraw),
            "intent": intent,
            "has_period_range": has_period_range,
            "is_report": is_report,
        }

    def _resolve_response_mode(self, question: str, requested_mode: str = "auto"):
        sig = self._classify_request(question)
        mode = str(requested_mode or "auto").strip().lower()
        if mode not in {"auto", "quick", "expert", "report"}:
            mode = "auto"
        if mode == "auto":
            if sig["intent"] == "report":
                mode = "report"
            elif sig["intent"] in {"performance", "risk", "forecast", "ratios"}:
                mode = "expert"
            else:
                mode = "quick"
        return mode, sig

    def _build_context_snapshot(self, context: dict):
        ctx = context or {}
        company = (ctx.get("company") or {})
        latest = (ctx.get("latest_ratios") or {})
        forecast = (ctx.get("forecast") or {})
        quality = (ctx.get("ai_quality") or {})
        return {
            "company": {
                "name": company.get("name"),
                "ticker": company.get("ticker"),
                "sector": company.get("sector"),
                "industry": company.get("industry"),
            },
            "latest_year": ctx.get("latest_year"),
            "latest_ratios": {
                "gross_margin": latest.get("gross_margin"),
                "operating_margin": latest.get("operating_margin"),
                "net_margin": latest.get("net_margin"),
                "roa": latest.get("roa"),
                "roe": latest.get("roe"),
                "roic": latest.get("roic"),
                "current_ratio": latest.get("current_ratio"),
                "quick_ratio": latest.get("quick_ratio"),
                "cash_ratio": latest.get("cash_ratio"),
                "debt_to_equity": latest.get("debt_to_equity"),
                "interest_coverage": latest.get("interest_coverage"),
                "pe_ratio": latest.get("pe_ratio"),
                "pb_ratio": latest.get("pb_ratio"),
                "ev_ebitda": latest.get("ev_ebitda"),
                "fcf_yield": latest.get("fcf_yield"),
            },
            "forecast": forecast,
            "ai_quality": quality,
        }

    def _answer_cloud(self, question: str, context: dict, response_mode: str = "auto"):
        mode, sig = self._resolve_response_mode(question, response_mode)
        lang = sig["lang"]
        is_report = sig["intent"] == "report"

        system_prompt = (
            "You are an institutional financial analysis assistant inside a SEC/XBRL platform. "
            "Use ONLY provided context values. Do not invent numbers. "
            "If data is unavailable, state DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES. "
            "Output must be in the user's language and structured professionally."
        )
        if is_report and lang == "ar":
            system_prompt += "\n" + REPORT_DIRECTIVE_AR

        ctx_snapshot = self._build_context_snapshot(context)
        user_content = (
            f"Language: {lang}\n"
            f"Response mode: {mode}\n"
            f"Intent: {sig['intent']}\n"
            f"Question: {question}\n\n"
            f"Authoritative context snapshot:\n{json.dumps(ctx_snapshot, ensure_ascii=False)}\n\n"
            f"Historical ratios by year:\n{json.dumps((context or {}).get('ratios_by_year') or {}, ensure_ascii=False)}"
        )
        if is_report and lang == "ar":
            user_content = self._build_ar_report_request(question=question, context=context)
        if is_report:
            user_content += (
                "\n\nQuality target: produce a long institutional-grade report with rigorous "
                "reasoning, explicit investment judgment, and no superficial narration."
            )
            user_content += (
                "\n\nOutput for chat readability: natural professional prose with clear headings and bullets; "
                "do not output JSON unless the user explicitly asks for JSON."
            )
            if lang == "ar":
                user_content += "\n\n" + SELF_REVIEW_AND_AUTO_IMPROVE_AR
                user_content += "\n\n" + EXPERT_PIPELINE_ENFORCEMENT_AR
        elif mode == "expert":
            user_content += (
                "\n\nAnswer as an expert analyst with this structure: "
                "1) direct answer 2) numerical evidence 3) causal interpretation 4) investor implication."
            )
        else:
            user_content += (
                "\n\nAnswer briefly and directly. Prioritize the exact question, then the most relevant numbers."
            )
        body = {
            "model": self.model,
            "temperature": 0.2,
            "max_tokens": 3800,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
        }
        key = self._resolve_api_key()
        if not key:
            raise RuntimeError("OPENAI_API_KEY is missing.")

        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"HTTP {e.code}: {detail}") from e
        text = (((payload.get("choices") or [{}])[0].get("message") or {}).get("content") or "No answer.")
        return text.strip(), {"engine": "cloud", "model": self.model, "mode": mode, "intent": sig["intent"]}

    @staticmethod
    def _json_block(payload):
        if payload is None:
            return "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"
        if isinstance(payload, (dict, list)) and not payload:
            return "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"
        try:
            return json.dumps(payload, ensure_ascii=False, indent=2)
        except Exception:
            return str(payload)

    @staticmethod
    def _first_num(row, keys):
        row = row or {}
        for k in keys:
            try:
                v = row.get(k)
            except Exception:
                v = None
            try:
                if v is not None:
                    return float(v)
            except Exception:
                continue
        return None

    def _canonical_year_financials(self, data_by_year, ratios_by_year):
        """
        Build canonical multi-year financial map from BOTH statement rows and ratio rows,
        so report fields don't become N/A when one source is missing a specific key alias.
        """
        data_by_year = data_by_year or {}
        ratios_by_year = ratios_by_year or {}
        years = sorted(
            {
                int(y)
                for y in list(data_by_year.keys()) + list(ratios_by_year.keys())
                if str(y).isdigit()
            }
        )

        out = {}
        for y in years:
            raw = data_by_year.get(y, {}) or {}
            rr = ratios_by_year.get(y, {}) or {}
            out[y] = {
                "revenue": self._first_num(rr, ["revenue"]) or self._first_num(raw, ["Revenues", "Revenue", "SalesRevenueNet", "RevenueFromContractWithCustomerExcludingAssessedTax"]),
                "gross_profit": self._first_num(rr, ["gross_profit"]) or self._first_num(raw, ["GrossProfit"]),
                "operating_income": self._first_num(rr, ["operating_income"]) or self._first_num(raw, ["OperatingIncomeLoss", "IncomeLossFromOperations"]),
                "net_income": self._first_num(rr, ["net_income"]) or self._first_num(raw, ["NetIncomeLoss", "ProfitLoss"]),
                "total_assets": self._first_num(rr, ["total_assets"]) or self._first_num(raw, ["Assets", "TotalAssets", "Total Assets"]),
                "total_liabilities": self._first_num(rr, ["total_liabilities"]) or self._first_num(raw, ["Liabilities", "TotalLiabilities", "Total Liabilities"]),
                "shareholders_equity": self._first_num(rr, ["shareholders_equity"]) or self._first_num(raw, ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "TotalEquity", "Total Equity"]),
                "operating_cash_flow": self._first_num(rr, ["operating_cash_flow"]) or self._first_num(raw, ["NetCashProvidedByUsedInOperatingActivities", "OperatingCashFlow"]),
                "capital_expenditures": self._first_num(rr, ["capital_expenditures"]) or self._first_num(raw, ["PaymentsToAcquirePropertyPlantAndEquipment", "CapitalExpenditures"]),
                "free_cash_flow": self._first_num(rr, ["free_cash_flow"]),
            }
        return out

    def _build_ar_report_request(self, question: str, context: dict) -> str:
        ctx = context or {}
        company = (ctx.get("company") or {})
        data_by_year = (ctx.get("data_by_year") or {})
        ratios_by_year = (ctx.get("ratios_by_year") or {})
        canonical_by_year = self._canonical_year_financials(data_by_year, ratios_by_year)
        forecast = (ctx.get("forecast") or {})
        quality = (ctx.get("ai_quality") or {})
        live = (ctx.get("live_trusted_context") or {})
        market = (live.get("market") or {}).get("snapshot") or {}
        latest_ratios = (ctx.get("latest_ratios") or {})

        years = sorted(set(int(y) for y in canonical_by_year.keys() if str(y).isdigit()))
        historical_period = f"{years[0]}-{years[-1]}" if years else "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"

        risk_metrics = {
            "altman_z_score": latest_ratios.get("altman_z_score"),
            "interest_coverage": latest_ratios.get("interest_coverage"),
            "debt_to_equity": latest_ratios.get("debt_to_equity"),
            "debt_to_assets": latest_ratios.get("debt_to_assets"),
            "current_ratio": latest_ratios.get("current_ratio"),
            "quick_ratio": latest_ratios.get("quick_ratio"),
            "cash_ratio": latest_ratios.get("cash_ratio"),
        }
        valuation_metrics = {
            "pe_ratio": latest_ratios.get("pe_ratio"),
            "pb_ratio": latest_ratios.get("pb_ratio"),
            "ev_ebitda": latest_ratios.get("ev_ebitda"),
            "fcf_yield": latest_ratios.get("fcf_yield"),
            "market_snapshot": market,
        }
        cash_flow_data = {
            str(y): {
                "operating_cash_flow": (canonical_by_year.get(y, {}) or {}).get("operating_cash_flow"),
                "capital_expenditures": (canonical_by_year.get(y, {}) or {}).get("capital_expenditures"),
                "free_cash_flow": (canonical_by_year.get(y, {}) or {}).get("free_cash_flow"),
            }
            for y in sorted(canonical_by_year.keys())
        }

        data_source_parts = ["SEC/XBRL (platform)"]
        sec_source = (live.get("sec") or {}).get("source")
        market_source = (live.get("market") or {}).get("source")
        if sec_source:
            data_source_parts.append(str(sec_source))
        if market_source:
            data_source_parts.append(str(market_source))
        data_source = " | ".join(data_source_parts)

        peer_or_sector_data = {
            "sector": company.get("sector"),
            "industry": company.get("industry"),
            "peer_data_available": False,
        }
        ai_signals = {
            "question": question,
            "ai_quality": quality,
            "live_notes": (live.get("notes") or []),
        }
        data_quality_notes = {
            "missing_data_policy": "Any unavailable point must be marked as DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES",
            "ratios_coverage_years": years,
            "canonical_history_years": sorted(canonical_by_year.keys()),
        }

        prompt = REPORT_REQUEST_TEMPLATE_AR
        replacements = {
            "{{company_name}}": str(company.get("name") or company.get("ticker") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
            "{{ticker}}": str(company.get("ticker") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
            "{{report_date}}": datetime.utcnow().strftime("%Y-%m-%d"),
            "{{data_source}}": data_source,
            "{{currency}}": "USD",
            "{{historical_period}}": historical_period,
            "{{sector}}": str(company.get("sector") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
            "{{industry}}": str(company.get("industry") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
            "{{historical_financials}}": self._json_block(canonical_by_year),
            "{{financial_ratios}}": self._json_block(ratios_by_year),
            "{{cash_flow_data}}": self._json_block(cash_flow_data),
            "{{quality_metrics}}": self._json_block(quality),
            "{{risk_metrics}}": self._json_block(risk_metrics),
            "{{valuation_metrics}}": self._json_block(valuation_metrics),
            "{{forecast_scenarios}}": self._json_block(forecast),
            "{{peer_or_sector_data}}": self._json_block(peer_or_sector_data),
            "{{ai_signals}}": self._json_block(ai_signals),
            "{{data_quality_notes}}": self._json_block(data_quality_notes),
        }
        for k, v in replacements.items():
            prompt = prompt.replace(k, v)
        return prompt

    def _build_strict_report_json_local(self, name, ticker, year, ratios, quality, forecast, ratios_by_year, y0, y1, data_by_year=None):
        def _num(v, default=None):
            try:
                return float(v)
            except Exception:
                return default

        quality = quality or {}
        ratios = ratios or {}
        forecast = forecast or {}
        ratios_by_year = ratios_by_year or {}
        canonical_by_year = self._canonical_year_financials(data_by_year or {}, ratios_by_year)
        years = sorted([int(y) for y in ratios_by_year.keys() if str(y).isdigit()])
        if not years:
            years = sorted([int(y) for y in canonical_by_year.keys() if str(y).isdigit()])
        y_from = years[0] if years else (y0 or year)
        y_to = years[-1] if years else (y1 or year)
        latest_year = y_to
        latest_row = (ratios_by_year.get(latest_year) or ratios) if latest_year is not None else ratios
        latest_canonical = (canonical_by_year.get(latest_year) or {}) if latest_year is not None else {}

        q_score = _num(quality.get("quality_score"), 60.0)
        q_score = max(0.0, min(100.0, q_score or 60.0))

        gm = _num(latest_row.get("gross_margin"), 0.0) or 0.0
        om = _num(latest_row.get("operating_margin"), 0.0) or 0.0
        nm = _num(latest_row.get("net_margin"), 0.0) or 0.0
        roe = _num(latest_row.get("roe"), 0.0) or 0.0
        roa = _num(latest_row.get("roa"), 0.0) or 0.0
        roic = _num(latest_row.get("roic"), 0.0) or 0.0
        cr = _num(latest_row.get("current_ratio"), 0.0) or 0.0
        qr = _num(latest_row.get("quick_ratio"), 0.0) or 0.0
        cash_r = _num(latest_row.get("cash_ratio"), 0.0) or 0.0
        dte = _num(latest_row.get("debt_to_equity"), 0.0) or 0.0
        da = _num(latest_row.get("debt_to_assets"), 0.0) or 0.0
        ic = _num(latest_row.get("interest_coverage"), 0.0) or 0.0
        pe = _num(latest_row.get("pe_ratio"), 0.0) or 0.0
        pb = _num(latest_row.get("pb_ratio"), 0.0) or 0.0
        ev = _num(latest_row.get("ev_ebitda"), 0.0) or 0.0
        fcf_y = _num(latest_row.get("fcf_yield"), 0.0) or 0.0
        wacc = _num(latest_row.get("wacc"), None)
        spread = (roic - wacc) if (wacc is not None) else None

        latest_revenue = _num(latest_row.get("revenue"), None)
        if latest_revenue is None:
            latest_revenue = _num(latest_canonical.get("revenue"), None)
        latest_net_income = _num(latest_row.get("net_income"), None)
        if latest_net_income is None:
            latest_net_income = _num(latest_canonical.get("net_income"), None)
        latest_cfo = _num(latest_row.get("operating_cash_flow"), None)
        if latest_cfo is None:
            latest_cfo = _num(latest_canonical.get("operating_cash_flow"), None)
        latest_fcf = _num(latest_row.get("free_cash_flow"), None)
        if latest_fcf is None:
            latest_fcf = _num(latest_canonical.get("free_cash_flow"), None)
        cash_conversion = None
        if latest_net_income not in (None, 0) and latest_cfo is not None:
            cash_conversion = latest_cfo / latest_net_income

        # Stage 1: Data validation layer
        base = (forecast.get("base", {}) or {})
        bull = (forecast.get("bull", {}) or {})
        bear = (forecast.get("bear", {}) or {})
        base_rev = _num(base.get("revenue_next"), None)
        base_ni = _num(base.get("net_income_next"), None)
        bull_rev = _num(bull.get("revenue_next"), None)
        bull_ni = _num(bull.get("net_income_next"), None)
        bear_rev = _num(bear.get("revenue_next"), None)
        bear_ni = _num(bear.get("net_income_next"), None)

        validation_flags = []
        forecast_valid = True
        for v, label in [(base_rev, "base.revenue_next"), (base_ni, "base.net_income_next"), (bull_rev, "bull.revenue_next"), (bull_ni, "bull.net_income_next"), (bear_rev, "bear.revenue_next"), (bear_ni, "bear.net_income_next")]:
            if v is None:
                forecast_valid = False
                validation_flags.append(f"Missing forecast field: {label}")
            elif abs(v) < 1e-12:
                forecast_valid = False
                validation_flags.append(f"Zero forecast field: {label}")

        # Stage 2 + 3: Diagnostics + contradictions
        contradictions = []
        if roe > 0.35 and dte > 1.5:
            contradictions.append("ROE مرتفع مع رافعة مرتفعة نسبيًا؛ جزء من العائد قد يكون ماليًا لا تشغيليًا.")
        if cr < 1.0 and (latest_cfo is not None and latest_cfo > 0):
            contradictions.append("السيولة المحاسبية أقل من 1 لكن التدفق النقدي التشغيلي موجب؛ ضعف ظاهري لا يعني ضائقة فورية.")
        if latest_net_income is not None and latest_cfo is not None and latest_net_income > 0 and latest_cfo < latest_net_income * 0.6:
            contradictions.append("الأرباح المحاسبية أقوى من النقد التشغيلي؛ جودة الأرباح تحتاج مراقبة.")
        if q_score >= 75 and pe > 25:
            contradictions.append("جودة الشركة مرتفعة لكن التقييم مضغوط؛ جودة النشاط لا تعني تلقائيًا جاذبية سعرية.")

        # Stage 4 + 6: Thesis + weighted recommendation
        score = 0.0
        score += min(22.0, max(0.0, gm * 60.0))
        score += min(16.0, max(0.0, roic * 70.0))
        score += min(12.0, max(0.0, nm * 45.0))
        score += min(12.0, max(0.0, (2.0 - min(2.0, dte)) * 6.0))
        score += min(10.0, max(0.0, min(2.0, cr) * 5.0))
        score += min(10.0, max(0.0, q_score * 0.10))
        if spread is not None and spread > 0:
            score += min(8.0, spread * 30.0)
        if cash_conversion is not None and cash_conversion > 0.9:
            score += 6.0
        if not forecast_valid:
            score -= 10.0
        score -= min(12.0, len(contradictions) * 3.0)
        if pe > 30:
            score -= 6.0
        final_score = max(0.0, min(100.0, score))

        if final_score >= 88:
            rating = "شراء قوي"
        elif final_score >= 74:
            rating = "شراء"
        elif final_score >= 58:
            rating = "احتفاظ"
        elif final_score >= 43:
            rating = "تخفيض"
        else:
            rating = "بيع"

        confidence = 82.0
        if not forecast_valid:
            confidence -= 12.0
        confidence -= min(15.0, len(validation_flags) * 3.0)
        confidence -= min(10.0, len(contradictions) * 2.0)
        confidence = max(30.0, min(95.0, confidence))

        years_hist = years[-5:] if len(years) >= 5 else years
        metric_map = [
            ("Revenue", "revenue"),
            ("GrossProfit", "gross_profit"),
            ("OperatingIncome", "operating_income"),
            ("NetIncome", "net_income"),
            ("TotalAssets", "total_assets"),
            ("TotalLiabilities", "total_liabilities"),
            ("ShareholderEquity", "shareholders_equity"),
            ("OperatingCashFlow", "operating_cash_flow"),
            ("Capex", "capital_expenditures"),
            ("FreeCashFlow", "free_cash_flow"),
        ]
        if years_hist:
            header = "| البند | " + " | ".join(str(y) for y in years_hist) + " |\n"
            sep = "|---|" + "|".join(["---"] * len(years_hist)) + "|\n"
            rows_md = []
            for lbl, key in metric_map:
                vals = []
                for yy in years_hist:
                    rv = _num((canonical_by_year.get(yy, {}) or {}).get(key), None)
                    if rv is None:
                        rv = _num((ratios_by_year.get(yy, {}) or {}).get(key), None)
                    vals.append("N/A" if rv is None else f"{rv:,.2f}")
                rows_md.append("| " + lbl + " | " + " | ".join(vals) + " |")
            summary_table = header + sep + "\n".join(rows_md)
        else:
            summary_table = "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"

        forecast_base_text = (
            f"Revenue_next={base_rev:.2f}, NetIncome_next={base_ni:.2f}"
            if forecast_valid and base_rev is not None and base_ni is not None
            else "بيانات التوقعات الحالية غير كافية أو غير صالحة للبناء عليها تحليليًا."
        )
        forecast_bull_text = (
            f"Revenue_next={bull_rev:.2f}, NetIncome_next={bull_ni:.2f}"
            if forecast_valid and bull_rev is not None and bull_ni is not None
            else "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"
        )
        forecast_bear_text = (
            f"Revenue_next={bear_rev:.2f}, NetIncome_next={bear_ni:.2f}"
            if forecast_valid and bear_rev is not None and bear_ni is not None
            else "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"
        )

        def _series(values_key):
            out_vals = []
            for yy in years_hist:
                vv = _num((canonical_by_year.get(yy, {}) or {}).get(values_key), None)
                out_vals.append(vv)
            return out_vals

        def _trend_text(values, label):
            vals = [v for v in values if v is not None]
            if len(vals) < 2:
                return f"{label}: بيانات غير كافية."
            start = vals[0]
            end = vals[-1]
            delta = end - start
            if abs(start) > 1e-12:
                pct = (delta / abs(start)) * 100.0
                pct_txt = f"{pct:+.1f}%"
            else:
                pct_txt = "N/A"
            direction = "صاعد" if delta > 0 else ("هابط" if delta < 0 else "أفقي")
            return f"{label}: اتجاه {direction} (Δ={delta:,.2f}, {pct_txt})"

        rev_trend = _trend_text(_series("revenue"), "الإيرادات")
        ni_trend = _trend_text(_series("net_income"), "صافي الربح")
        cfo_trend = _trend_text(_series("operating_cash_flow"), "التدفق التشغيلي")
        fcf_trend = _trend_text(_series("free_cash_flow"), "التدفق النقدي الحر")

        payload = {
            "report_title": "تقرير تحليل مالي واستثماري احترافي شامل",
            "company_name": str(name or ticker or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
            "ticker": str(ticker or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
            "report_date": datetime.utcnow().strftime("%Y-%m-%d"),
            "data_source": "SEC/XBRL + Internal Analytics",
            "investment_rating": {
                "label": rating,
                "score_100": round(final_score, 2),
                "confidence": round(confidence, 2),
                "time_horizon": "متوسط الأجل"
            },
            "executive_summary": {
                "overview": (
                    f"خلاصة الحكم: {rating}. الشركة تظهر جودة تشغيلية ملحوظة، لكن القرار الاستثماري يتأثر بجودة التحول النقدي "
                    f"وانضباط التقييم. الإشكال الرئيسي الحالي يميل إلى {'السعر/التقييم' if pe > 25 else 'استدامة التشغيل'} أكثر من جودة النشاط الأساسية."
                ),
                "key_strengths": [
                    f"هوامش تشغيلية وربحية داعمة (GM={gm:.2%}, OM={om:.2%}, NM={nm:.2%})",
                    "إشارات كفاءة رأس مال إيجابية نسبيًا عبر ROIC",
                    "قدرة الشركة على الحفاظ على إطار مالي قابل للإدارة في السيناريو الأساسي"
                ],
                "key_risks": [
                    "احتمال ضغط تقييمي يحد من هامش الأمان",
                    "خطر تراجع جودة الأرباح إذا ضعفت نسبة تحويل الربح إلى نقد",
                    "أي اتساع في الرافعة أو هبوط التغطية يزيد حساسية السهم"
                ],
                "preliminary_view": "الأطروحة: نشاط قوي نسبيًا + تقييم يحتاج انضباطًا. الجاذبية تعتمد على توازن الجودة مع السعر."
            },
            "business_model_and_positioning": {
                "business_model": "تشير الأرقام إلى نموذج أعمال يخلق قيمة عبر مزيج من هوامش مرتفعة نسبيًا وكفاءة تشغيل.",
                "revenue_characteristics": "استقرار الهوامش عبر الزمن يوحي بوجود قوة تسعير أو انضباط تكاليف يفوق المتوسط.",
                "competitive_advantages": "الميزة الاقتصادية تُستدل من قدرة الشركة على حماية الربحية مع توليد نقد مستمر.",
                "strategic_positioning": "تموضع الشركة أقرب إلى شريحة الجودة داخل القطاع، لكن حساسية السعر قد تحد من العائد المستقبلي."
            },
            "historical_financial_analysis": {
                "summary_table": summary_table,
                "analysis": f"خلال الفترة {y_from}-{y_to}، القراءة المؤسسية تركز على المسار الهيكلي لا اللقطات السنوية. يجب مراقبة تزامن النمو مع استقرار الهوامش والتحول النقدي.",
                "investor_implication": "قيمة الاستثمار ترتفع عندما يترافق التوسع مع جودة ربح أعلى وليس مجرد نمو اسمي."
            },
            "profitability_analysis": {
                "metrics_review": f"GM={gm:.4f}, OM={om:.4f}, NM={nm:.4f}, ROA={roa:.4f}, ROE={roe:.4f}, ROIC={roic:.4f}",
                "analysis": "الهوامش المرتفعة تُقرأ كمؤشر قوة تسعير وكفاءة. ROE المرتفع يحتاج فحص هيكل رأس المال حتى لا يُفسّر كجودة تشغيل خالصة.",
                "sustainability_view": "الاستدامة تُعد قوية فقط إذا استمر ROIC مرتفعًا دون تدهور في السيولة أو ارتفاع مفرط في الرافعة.",
                "investor_implication": "الربحية الجيدة تدعم السهم، لكن قرار الدخول يتطلب التحقق من استدامتها النقدية والسعرية."
            },
            "liquidity_solvency_leverage": {
                "metrics_review": f"Current={cr:.4f}, Quick={qr:.4f}, Cash={cash_r:.4f}, D/E={dte:.4f}, D/A={da:.4f}, InterestCoverage={ic:.4f}",
                "analysis": "يجب التفريق بين ضعف السيولة المحاسبية وقوة توليد النقد التشغيلي. الحكم لا يعتمد على Current Ratio وحده.",
                "balance_sheet_view": (
                    f"السيولة: {'ضعيفة ظاهريًا' if cr < 1 else ('مقبولة' if cr < 1.5 else 'قوية')} | "
                    f"الملاءة: {'هشة' if dte > 2 else ('متوسطة' if dte > 1 else 'قوية')} | "
                    f"الرافعة: {'مرتفعة' if dte > 2 else ('تحتاج مراقبة' if dte > 1 else 'منضبطة')}"
                ),
                "investor_implication": "ثبات التغطية مع دين منضبط يخفف المخاطر، بينما تراجع التغطية يغير الموقف سريعًا."
            },
            "cash_flow_and_earnings_quality": {
                "cash_flow_review": (
                    f"CFO={('N/A' if latest_cfo is None else f'{latest_cfo:.2f}')} | "
                    f"NI={('N/A' if latest_net_income is None else f'{latest_net_income:.2f}')} | "
                    f"FCF={('N/A' if latest_fcf is None else f'{latest_fcf:.2f}')} | "
                    f"CashConversion={('N/A' if cash_conversion is None else f'{cash_conversion:.2f}')}"
                ),
                "earnings_quality_assessment": "جودة الأرباح قوية عندما يتحول صافي الربح إلى نقد بكفاءة متسقة.",
                "analysis": "الفجوة بين الربح المحاسبي والنقد التشغيلي تُقرأ كإشارة مبكرة على مخاطر جودة الأرباح.",
                "investor_implication": "القيمة الحقيقية للمستثمر تأتي من أرباح قابلة للتحول النقدي لا من نمو محاسبي فقط."
            },
            "capital_efficiency_and_value_creation": {
                "capital_efficiency_review": (
                    f"ROIC={roic:.4f}, WACC={('N/A' if wacc is None else f'{wacc:.4f}')}, "
                    f"EconomicSpread={('N/A' if spread is None else f'{spread:.4f}')}"
                ),
                "economic_value_creation": "إذا بقي ROIC أعلى من WACC فالنمو مخلق للقيمة اقتصاديًا وليس استهلاكًا لرأس المال.",
                "analysis": "هذا القسم هو محور الأطروحة طويلة الأجل لأنه يميز الشركات الجيدة من الشركات المربحة ظاهريًا فقط.",
                "investor_implication": "استدامة spread الإيجابي تمنح أفضلية مركبة للمستثمر طويل الأجل."
            },
            "sector_and_peer_context": {
                "comparison_basis": "مقارنة تفسيرية (عند غياب peers مباشرة) عبر الهوامش + ROIC + الرافعة + جودة النقد + مضاعفات التقييم.",
                "relative_positioning": "تشير المعطيات إلى تموضع أقرب للشريحة الأعلى جودة، مع قيد تقييمي محتمل.",
                "analysis": "القوة النسبية في القطاع لا تكفي وحدها؛ يجب دمجها مع سعر الدخول وهامش الأمان.",
                "investor_implication": "حتى الشركات الرائدة قطاعيًا قد تصبح أقل جاذبية عند تضخم التسعير."
            },
            "risk_analysis": {
                "financial_risks": [
                    "ضغط السيولة إذا انخفض النقد التشغيلي بالتزامن مع التزامات قصيرة",
                    "ارتفاع الرافعة وتأثيره على مرونة الميزانية"
                ],
                "operating_risks": [
                    "تراجع الهوامش عند اشتداد المنافسة أو ارتفاع التكلفة",
                    "تذبذب جودة التحول النقدي"
                ],
                "strategic_risks": [
                    "اعتماد مسار النمو على محركات محددة",
                    "مخاطر إعادة التسعير الاستراتيجي في دورات الطلب الضعيفة"
                ],
                "valuation_risks": [
                    "مضاعفات مرتفعة دون نمو مواكب",
                    "انكماش التقييم عند تباطؤ الأرباح"
                ],
                "overall_risk_view": "خريطة المخاطر تشير إلى أن أكبر قيد حالي يميل للتقييم أكثر من جودة النشاط الأساسي."
            },
            "forecast_analysis": {
                "base_case": forecast_base_text,
                "bull_case": forecast_bull_text,
                "bear_case": forecast_bear_text,
                "scenario_interpretation": (
                    "تم فحص صلاحية البيانات قبل التحليل. " +
                    ("السيناريوهات قابلة للاستخدام التحليلي مع مستوى عدم يقين متدرج." if forecast_valid else "بيانات السيناريوهات الحالية غير صالحة لاتخاذ حكم قوي.")
                ),
                "investor_implication": "لا يجوز بناء توصية مرتفعة الثقة على Forecast غير متحقق الصلاحية."
            },
            "valuation_analysis": {
                "valuation_snapshot": f"P/E={pe:.4f}, P/B={pb:.4f}, EV/EBITDA={ev:.4f}, FCF_Yield={fcf_y:.4f}",
                "fairness_assessment": "عدالة السعر تُقاس بقدرة الأرباح والنقد على تبرير المضاعفات، لا بجودة الاسم فقط.",
                "multiple_interpretation": "المضاعفات المرتفعة قد تكون مبررة جزئيًا بجودة الأعمال لكنها ترفع حساسية الانكماش السعري.",
                "investor_implication": "المشكلة الاستثمارية قد تكون في السعر وليس النشاط، لذلك هامش الأمان عنصر حاسم."
            },
            "final_investment_case": {
                "rating_label": rating,
                "score_100": round(final_score, 2),
                "confidence": round(confidence, 2),
                "top_5_reasons": [
                    "هوامش تشغيلية تدعم جودة النشاط",
                    "قراءة ROIC تدعم خلق قيمة نسبيًا",
                    "إطار مخاطر مالي قابل للمراقبة",
                    "تحليل يوازن بين التشغيل والتقييم",
                    "كشف تناقضات رئيسية قبل الحكم النهائي"
                ],
                "top_5_monitoring_points": [
                    "اتجاه الهوامش الفصلية",
                    "جودة التحول النقدي",
                    "اتجاه الرافعة وتغطية الفائدة",
                    "فجوة ROIC مقابل WACC",
                    "انضباط التقييم مقابل النمو"
                ],
                "what_could_change_the_rating": "تحسن التقييم مع استمرار جودة التشغيل يرفع التوصية، بينما تدهور النقد أو الهوامش أو الرافعة يخفضها."
            },
            "closing_summary": (
                f"الخلاصة: هذه الشركة أقرب إلى حالة جودة تشغيلية جيدة، لكن الجاذبية الاستثمارية النهائية تتحدد عند التوازن بين "
                f"قوة الأعمال وسعر السهم. أكبر ميزة: القدرة على توليد ربحية وكفاءة رأس مال. أكبر قيد: احتمال التقييم المرتفع "
                f"أو ضعف التحول النقدي. لذلك القرار ليس هل الشركة ممتازة أم لا، بل هل السعر الحالي يعكس هامش أمان كافي."
            ),
            "self_review": {
                "numerical_depth": str(max(3, min(9, (5 + (1 if len(years) >= 4 else 0) + (1 if len(years) >= 6 else 0) - (1 if validation_flags else 0))))),
                "narrative_quality": str(max(3, min(9, 7 - min(2, len(contradictions) // 2)))),
                "economic_reasoning": str(max(3, min(9, 7 - min(2, len(validation_flags) // 2)))),
                "risk_analysis_quality": str(max(3, min(9, 7 + (1 if contradictions else -1)))),
                "valuation_quality": str(max(3, min(9, 7 - (1 if pe > 35 else 0)))),
                "consistency_of_final_rating": str(max(3, min(9, 8 - (1 if (not forecast_valid) else 0)))),
                "professional_language_quality": "8",
                "global_report_similarity": str(max(3, min(9, 7 - (1 if validation_flags else 0))))
            }
        }
        payload["historical_financial_analysis"]["analysis"] += f" | {rev_trend} | {ni_trend}"
        payload["cash_flow_and_earnings_quality"]["analysis"] += f" | {cfo_trend} | {fcf_trend}"
        payload["business_model_and_positioning"]["business_model"] += (
            f" الدليل العددي: هامش إجمالي {gm:.2%}، هامش تشغيلي {om:.2%}، ROIC {roic:.2%}."
        )
        payload["final_investment_case"]["top_5_reasons"] = [
            f"اتجاه الإيرادات: {rev_trend}",
            f"اتجاه صافي الربح: {ni_trend}",
            f"اتجاه النقد التشغيلي: {cfo_trend}",
            "توازن نسبي بين كفاءة رأس المال والمخاطر التمويلية",
            "التوصية مبنية على وزن الربحية والنقد والتقييم والمخاطر"
        ]
        if contradictions:
            payload["risk_analysis"]["operating_risks"] = list(payload["risk_analysis"]["operating_risks"]) + contradictions[:2]
        if validation_flags:
            payload["forecast_analysis"]["scenario_interpretation"] += " | ValidationFlags: " + "; ".join(validation_flags[:4])

        def _fmt(v, pct=False):
            if v is None:
                return "غير متاح"
            try:
                fv = float(v)
                return f"{fv:.2%}" if pct else f"{fv:,.2f}"
            except Exception:
                return str(v)

        y_start = years_hist[0] if years_hist else y_from
        y_end = years_hist[-1] if years_hist else y_to
        start_row = canonical_by_year.get(y_start, {}) if y_start is not None else {}
        end_row = canonical_by_year.get(y_end, {}) if y_end is not None else {}
        start_rat = ratios_by_year.get(y_start, {}) if y_start is not None else {}
        end_rat = ratios_by_year.get(y_end, {}) if y_end is not None else {}

        rev_s = _num(start_row.get("revenue"), None)
        rev_e = _num(end_row.get("revenue"), None)
        ni_s = _num(start_row.get("net_income"), None)
        ni_e = _num(end_row.get("net_income"), None)
        cfo_s = _num(start_row.get("operating_cash_flow"), None)
        cfo_e = _num(end_row.get("operating_cash_flow"), None)
        fcf_s = _num(start_row.get("free_cash_flow"), None)
        fcf_e = _num(end_row.get("free_cash_flow"), None)
        gm_s = _num(start_rat.get("gross_margin"), None)
        gm_e = _num(end_rat.get("gross_margin"), None)
        om_s = _num(start_rat.get("operating_margin"), None)
        om_e = _num(end_rat.get("operating_margin"), None)
        nm_s = _num(start_rat.get("net_margin"), None)
        nm_e = _num(end_rat.get("net_margin"), None)

        spread_txt = "غير متاح" if spread is None else _fmt(spread, pct=True)
        wacc_txt = "غير متاح" if wacc is None else _fmt(wacc, pct=True)
        cash_conv_txt = "غير متاح" if cash_conversion is None else _fmt(cash_conversion)

        thesis_line = (
            "القضية الاستثمارية الأساسية: جودة تشغيلية قوية نسبيًا مقابل حساسية تقييمية؛ "
            "وبالتالي فإن القرار يرتبط بسعر الدخول وهامش الأمان أكثر من كونه حكمًا على النشاط وحده."
        )
        if pe <= 25 and pb <= 8:
            thesis_line = (
                "القضية الاستثمارية الأساسية: جودة تشغيلية مدعومة بتقييم أقل ضغطًا نسبيًا، "
                "ما يحسن توازن العائد/المخاطر إذا استمرت جودة النقد والهوامش."
            )

        exec_overview = (
            f"يغطي هذا التقرير الفترة {y_from}-{y_to} مع تركيز تحليلي على المسار الأخير {y_start}-{y_end}. "
            f"تاريخيًا، انتقلت الإيرادات من {_fmt(rev_s)} إلى {_fmt(rev_e)}، وصافي الربح من {_fmt(ni_s)} إلى {_fmt(ni_e)}، "
            f"بينما تحرك التدفق النقدي التشغيلي من {_fmt(cfo_s)} إلى {_fmt(cfo_e)} والتدفق النقدي الحر من {_fmt(fcf_s)} إلى {_fmt(fcf_e)}. "
            f"هذا المسار يعني أن تقييم الأداء لا يجب أن يعتمد على سنة منفردة، بل على اتساق النمو مع جودة التحول النقدي. "
            f"من زاوية الربحية، تغيّر الهامش الإجمالي من {_fmt(gm_s, pct=True)} إلى {_fmt(gm_e, pct=True)}، "
            f"والهامش التشغيلي من {_fmt(om_s, pct=True)} إلى {_fmt(om_e, pct=True)}، "
            f"والهامش الصافي من {_fmt(nm_s, pct=True)} إلى {_fmt(nm_e, pct=True)}. "
            f"من زاوية خلق القيمة، ROIC الحالي عند {_fmt(roic, pct=True)} مقابل WACC عند {wacc_txt} "
            f"(الفارق الاقتصادي: {spread_txt})، وهو محور الحكم على جودة النمو. "
            f"أما جودة الأرباح فتُقرأ عبر Cash Conversion ({cash_conv_txt}) وليس عبر الربح المحاسبي وحده. "
            f"{thesis_line}"
        )
        payload["executive_summary"]["overview"] = exec_overview
        payload["executive_summary"]["preliminary_view"] = (
            f"{rating} بدرجة {final_score:.2f}/100 وثقة {confidence:.2f}%. "
            f"الحكم قابل للتحسن إذا استمر التحول النقدي وهدأ ضغط التقييم، "
            f"وقابل للتراجع إذا ضعفت الهوامش أو ارتفعت الرافعة."
        )

        payload["historical_financial_analysis"]["analysis"] = (
            f"التحليل التاريخي يبين مسارًا مركبًا: الإيرادات {_fmt(rev_s)} -> {_fmt(rev_e)}، "
            f"وصافي الربح {_fmt(ni_s)} -> {_fmt(ni_e)}، والتدفق التشغيلي {_fmt(cfo_s)} -> {_fmt(cfo_e)}، "
            f"والتدفق الحر {_fmt(fcf_s)} -> {_fmt(fcf_e)}. "
            f"المعنى الاقتصادي: جودة النمو تُعد مرتفعة فقط إذا تزامن تحسن الأرباح مع ثبات/تحسن التحول النقدي، "
            f"وليس بمجرد توسع الإيرادات. {rev_trend} | {ni_trend}"
        )
        payload["historical_financial_analysis"]["investor_implication"] = (
            "للمستثمر، أهم نقطة ليست اتجاه الإيرادات وحده، بل ما إذا كان النمو يترجم إلى نقد حر قابل لإعادة التخصيص "
            "(توزيعات/إعادة شراء/استثمار) دون تآكل هيكل الميزانية."
        )

        payload["profitability_analysis"]["analysis"] = (
            f"الربحية الحالية (GM={_fmt(gm, pct=True)}, OM={_fmt(om, pct=True)}, NM={_fmt(nm, pct=True)}) "
            "تشير إلى قدرة تشغيلية جيدة نسبيًا. لكن القراءة المهنية تتطلب تفكيك ROE: "
            "إذا كان مرتفعًا بسبب هيكل رأس المال أو انخفاض حقوق الملكية، فلا يجب اعتباره جودة تشغيلية خالصة. "
            "الأهم هو اتساق ROIC مع استدامة الهوامش عبر الزمن."
        )
        payload["liquidity_solvency_leverage"]["analysis"] = (
            f"السيولة الجارية {_fmt(cr)} والسريعة {_fmt(qr)} والنقدية {_fmt(cash_r)} تُقرأ معًا مع الدين "
            f"(D/E={_fmt(dte)}, D/A={_fmt(da)}) وتغطية الفائدة ({_fmt(ic)}). "
            "ضعف السيولة المحاسبية لا يعني تلقائيًا ضائقة إذا كان النقد التشغيلي قويًا، "
            "لكن استمرار هذا الضعف مع تباطؤ النقد يرفع المخاطر."
        )
        payload["cash_flow_and_earnings_quality"]["analysis"] = (
            f"جودة الأرباح تُقاس هنا عبر المقارنة بين صافي الربح ({_fmt(latest_net_income)}) والتدفق التشغيلي ({_fmt(latest_cfo)}) "
            f"والتدفق الحر ({_fmt(latest_fcf)}). Cash Conversion={cash_conv_txt}. "
            "كلما اقترب النقد التشغيلي من صافي الربح بشكل مستقر، زادت موثوقية الأرباح وقلت مخاطر الانعكاس المحاسبي. "
            f"{cfo_trend} | {fcf_trend}"
        )
        payload["capital_efficiency_and_value_creation"]["analysis"] = (
            f"ROIC عند {_fmt(roic, pct=True)} مقابل WACC عند {wacc_txt} (Spread={spread_txt}). "
            "إذا استمر الفارق موجبًا، فإن التوسع يخلق قيمة اقتصادية؛ "
            "أما إذا انكمش الفارق نحو الصفر، يصبح النمو أقل جاذبية حتى مع بقاء الأرباح المحاسبية موجبة."
        )
        payload["valuation_analysis"]["multiple_interpretation"] = (
            f"قراءة المضاعفات الحالية: P/E={_fmt(pe)}, P/B={_fmt(pb)}, EV/EBITDA={_fmt(ev)}, FCF Yield={_fmt(fcf_y, pct=True)}. "
            "ارتفاع المضاعفات قد يكون مبررًا جزئيًا بجودة الأعمال، لكنه يضغط هامش الأمان ويرفع حساسية السهم لأي تباطؤ تشغيلي."
        )

        payload["final_investment_case"]["top_5_reasons"] = [
            f"مسار الإيرادات: {_fmt(rev_s)} -> {_fmt(rev_e)} مع قراءة اتجاهية: {rev_trend}",
            f"مسار صافي الربح: {_fmt(ni_s)} -> {_fmt(ni_e)} مع قراءة اتجاهية: {ni_trend}",
            f"جودة التحول النقدي الحالية: CashConversion={cash_conv_txt}",
            f"خلق القيمة: ROIC={_fmt(roic, pct=True)} مقابل WACC={wacc_txt} (Spread={spread_txt})",
            f"التوصية ناتجة عن موازنة الربحية والنقد والمخاطر والتقييم، لا عن مؤشر منفرد"
        ]
        payload["final_investment_case"]["top_5_monitoring_points"] = [
            "استمرارية نمو الإيرادات دون تآكل في الهوامش",
            "اتجاه Cash Conversion وFCF عبر النتائج القادمة",
            "تطور الرافعة وتغطية الفائدة تحت سيناريو تباطؤ الطلب",
            "استمرار الفارق الاقتصادي (ROIC-WACC) فوق الصفر",
            "أي اتساع في مضاعفات التقييم دون دعم تشغيلي مماثل"
        ]
        payload["closing_summary"] = (
            f"خلاصة تنفيذية نهائية: السهم يصنف حاليًا ضمن {rating} بدرجة {final_score:.2f}/100 وثقة {confidence:.2f}%. "
            "جوهر الأطروحة أن النشاط التشغيلي يظهر جودة مقبولة إلى قوية، لكن قرار الاستثمار يتحدد بصرامة عبر تقييم السعر "
            "وجودة النقد واستدامة خلق القيمة. أكبر عنصر قوة هو كفاءة تحويل النشاط إلى ربحية تشغيلية، "
            "وأكبر عنصر خطر هو انكماش هامش الأمان عند ارتفاع التقييم أو تراجع التحول النقدي. "
            "بالتالي، المتابعة الدورية للهوامش والنقد والرافعة أهم من الاكتفاء بلقطة سنوية واحدة."
        )
        return self._auto_improve_local_report_payload(payload)

    @staticmethod
    def _auto_improve_local_report_payload(payload: dict) -> dict:
        """
        Self-review and auto-improve local strict JSON report before final output.
        """
        if not isinstance(payload, dict):
            return payload

        def _is_weak(v):
            if v is None:
                return True
            s = str(v).strip()
            return (not s) or (s == "N/A") or (s == "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES")

        # Strengthen mandatory narrative fields if weak.
        must_have_paths = [
            ("executive_summary", "overview"),
            ("historical_financial_analysis", "analysis"),
            ("profitability_analysis", "analysis"),
            ("liquidity_solvency_leverage", "analysis"),
            ("cash_flow_and_earnings_quality", "analysis"),
            ("capital_efficiency_and_value_creation", "analysis"),
            ("sector_and_peer_context", "analysis"),
            ("forecast_analysis", "scenario_interpretation"),
            ("valuation_analysis", "fairness_assessment"),
            ("final_investment_case", "what_could_change_the_rating"),
            ("closing_summary", None),
        ]
        for section, key in must_have_paths:
            if key is None:
                if _is_weak(payload.get(section)):
                    payload[section] = "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"
                continue
            sec = payload.get(section) or {}
            if not isinstance(sec, dict):
                sec = {}
                payload[section] = sec
            if _is_weak(sec.get(key)):
                sec[key] = "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"

        # Ensure recommendation logic consistency.
        rating_block = payload.get("investment_rating") or {}
        final_block = payload.get("final_investment_case") or {}
        if isinstance(rating_block, dict) and isinstance(final_block, dict):
            lbl = str(rating_block.get("label") or "").strip()
            if lbl:
                final_block["rating_label"] = lbl
            score = rating_block.get("score_100")
            conf = rating_block.get("confidence")
            if score is not None:
                final_block["score_100"] = score
            if conf is not None:
                final_block["confidence"] = conf
            payload["final_investment_case"] = final_block

        # Self-review: keep 1..10 and avoid weak grading labels.
        sr = payload.get("self_review") or {}
        if isinstance(sr, dict):
            weak_fields = 0
            for section, key in must_have_paths:
                if key is None:
                    if _is_weak(payload.get(section)):
                        weak_fields += 1
                else:
                    sec = payload.get(section) or {}
                    if _is_weak((sec or {}).get(key)):
                        weak_fields += 1
            for k in [
                "numerical_depth",
                "narrative_quality",
                "economic_reasoning",
                "risk_analysis_quality",
                "valuation_quality",
                "consistency_of_final_rating",
                "professional_language_quality",
                "global_report_similarity",
            ]:
                try:
                    vv = int(float(sr.get(k, 8)))
                except Exception:
                    vv = 8
                if weak_fields > 0:
                    vv = max(1, vv - min(3, weak_fields))
                vv = max(1, min(10, vv))
                sr[k] = str(vv)
            payload["self_review"] = sr

        return payload

    def _extract_year_range(self, question: str, context: dict):
        years = []
        for m in re.findall(r"(19\d{2}|20\d{2})", str(question or "")):
            try:
                years.append(int(m))
            except Exception:
                pass
        if len(years) >= 2:
            return min(years), max(years)
        ctx_years = []
        for y in (context or {}).get("ratios_by_year", {}).keys():
            try:
                ctx_years.append(int(y))
            except Exception:
                pass
        ctx_years = sorted(ctx_years)
        if ctx_years:
            return ctx_years[0], ctx_years[-1]
        ly = (context or {}).get("latest_year")
        return ly, ly

    def _pick_year_val(self, ratios_by_year, year, keys):
        if year is None:
            return None
        row = (ratios_by_year or {}).get(int(year), {}) or {}
        for k in keys:
            v = _safe_float(row.get(k))
            if v is not None:
                return v
        return None

    def _trend_line(self, start_v, end_v, ratio=False):
        if start_v is None or end_v is None:
            return "N/A", "N/A"
        delta = end_v - start_v
        if ratio:
            return _fmt_num(delta), _fmt_num(end_v)
        if abs(start_v) < 1e-12:
            return _fmt_num(delta), "N/A"
        pct = (delta / abs(start_v)) * 100.0
        return _fmt_num(delta), f"{pct:.2f}%"

    def _period_years(self, ratios_by_year, data_by_year, y0, y1):
        years = sorted(
            {
                int(y)
                for y in list((ratios_by_year or {}).keys()) + list((data_by_year or {}).keys())
                if str(y).isdigit()
            }
        )
        if not years:
            return []
        if y0 is None or y1 is None:
            return years
        y0 = int(y0)
        y1 = int(y1)
        if y0 > y1:
            y0, y1 = y1, y0
        scoped = [y for y in years if y0 <= y <= y1]
        return scoped or years

    def _collect_expert_diagnostics(self, ratios_by_year, data_by_year, latest_ratios, forecast, quality, y0, y1):
        canonical = self._canonical_year_financials(data_by_year or {}, ratios_by_year or {})
        years = self._period_years(ratios_by_year, data_by_year, y0, y1)
        if not years:
            return {}
        sy, ey = years[0], years[-1]
        start_raw = canonical.get(sy, {}) or {}
        end_raw = canonical.get(ey, {}) or {}
        start_rat = (ratios_by_year or {}).get(sy, {}) or {}
        end_rat = (ratios_by_year or {}).get(ey, {}) or {}
        latest = dict(latest_ratios or {})
        if not latest:
            latest = end_rat

        def _n(v):
            return _safe_float(v)

        def _pct(v):
            if v is None:
                return "غير متاح"
            return f"{float(v):.2%}"

        rev_s = _n(start_raw.get("revenue"))
        rev_e = _n(end_raw.get("revenue"))
        ni_s = _n(start_raw.get("net_income"))
        ni_e = _n(end_raw.get("net_income"))
        cfo_s = _n(start_raw.get("operating_cash_flow"))
        cfo_e = _n(end_raw.get("operating_cash_flow"))
        fcf_s = _n(start_raw.get("free_cash_flow"))
        fcf_e = _n(end_raw.get("free_cash_flow"))
        assets_e = _n(end_raw.get("total_assets"))
        liab_e = _n(end_raw.get("total_liabilities"))
        eq_e = _n(end_raw.get("shareholders_equity"))

        gm_s = _n(start_rat.get("gross_margin"))
        gm_e = _n(end_rat.get("gross_margin"))
        om_s = _n(start_rat.get("operating_margin"))
        om_e = _n(end_rat.get("operating_margin"))
        nm_s = _n(start_rat.get("net_margin"))
        nm_e = _n(end_rat.get("net_margin"))
        cr_s = _n(start_rat.get("current_ratio"))
        cr_e = _n(end_rat.get("current_ratio"))
        qr_e = _n(end_rat.get("quick_ratio"))
        cash_r_e = _n(end_rat.get("cash_ratio"))
        dte_s = _n(start_rat.get("debt_to_equity"))
        dte_e = _n(end_rat.get("debt_to_equity"))
        ic_e = _n(end_rat.get("interest_coverage"))
        roa_e = _n(end_rat.get("roa"))
        roe_e = _n(end_rat.get("roe"))
        roic_e = _n(end_rat.get("roic"))
        pe_e = _n(end_rat.get("pe_ratio"))
        pb_e = _n(end_rat.get("pb_ratio"))
        ev_e = _n(end_rat.get("ev_ebitda"))
        fcf_y_e = _n(end_rat.get("fcf_yield"))
        wacc_e = _n(end_rat.get("wacc"))
        spread_e = roic_e - wacc_e if roic_e is not None and wacc_e is not None else None

        rev_delta, rev_growth = self._trend_line(rev_s, rev_e, ratio=False)
        ni_delta, ni_growth = self._trend_line(ni_s, ni_e, ratio=False)
        cfo_delta, cfo_growth = self._trend_line(cfo_s, cfo_e, ratio=False)
        fcf_delta, fcf_growth = self._trend_line(fcf_s, fcf_e, ratio=False)
        gm_delta, _ = self._trend_line(gm_s, gm_e, ratio=True)
        om_delta, _ = self._trend_line(om_s, om_e, ratio=True)
        nm_delta, _ = self._trend_line(nm_s, nm_e, ratio=True)
        cr_delta, _ = self._trend_line(cr_s, cr_e, ratio=True)
        dte_delta, _ = self._trend_line(dte_s, dte_e, ratio=True)

        cash_conversion = None
        if ni_e not in (None, 0) and cfo_e is not None:
            cash_conversion = cfo_e / ni_e

        q_score = _n((quality or {}).get("quality_score"))

        contradictions = []
        if roe_e is not None and roe_e > 1.0 and dte_e is not None and dte_e > 1.0:
            contradictions.append("ROE مرتفع جدًا لكن الرافعة مرتفعة أيضًا، ما يرجح أن جزءًا من العائد ناتج عن هيكل رأس المال لا التشغيل فقط.")
        if cr_e is not None and cr_e < 1.0 and cfo_e is not None and cfo_e > 0:
            contradictions.append("السيولة الجارية أقل من 1، لكن الشركة ما تزال تولد نقدًا تشغيليًا موجبًا؛ هذا ضعف ظاهري أكثر منه أزمة فورية.")
        if cash_conversion is not None and cash_conversion < 0.85:
            contradictions.append("تحويل الأرباح إلى نقد ليس قويًا بما يكفي، ما يفرض الحذر في تقييم جودة الأرباح.")
        if q_score is not None and q_score >= 70 and ((pe_e is not None and pe_e > 30) or (pb_e is not None and pb_e > 10)):
            contradictions.append("جودة النشاط تبدو جيدة نسبيًا، لكن التقييم السوقي مضغوط ويحد من هامش الأمان.")

        if roic_e is None:
            business_quality = "غير محسوم بسبب نقص بيانات ROIC."
        elif roic_e >= 0.2 and (gm_e or 0) >= 0.35:
            business_quality = "جودة النشاط مرتفعة نسبيًا، لأن الهوامش والعائد على رأس المال يدلان على قدرة جيدة على خلق القيمة."
        elif roic_e >= 0.1:
            business_quality = "جودة النشاط متوسطة إلى جيدة، لكن ليست بمنأى عن ضغط تنافسي أو دوري."
        else:
            business_quality = "جودة النشاط محدودة نسبيًا لأن العائد على رأس المال لا يبرز ميزة اقتصادية واضحة."

        if pe_e is None and pb_e is None and fcf_y_e is None:
            valuation_view = "التقييم غير قابل للحكم بدقة لغياب مضاعفات كافية."
        elif (pe_e is not None and pe_e > 30) or (pb_e is not None and pb_e > 10):
            valuation_view = "التقييم مرتفع نسبيًا، ما يعني أن السهم يحتاج استمرارًا في جودة التشغيل لتبرير السعر الحالي."
        elif fcf_y_e is not None and fcf_y_e < 0.03:
            valuation_view = "العائد النقدي الحر منخفض، ما يوحي بأن السوق سبق جزءًا معتبرًا من الأساسيات."
        else:
            valuation_view = "التقييم يبدو أقل ضغطًا نسبيًا من جودة النشاط، ما يحسن مرونة القرار الاستثماري."

        forecast_valid = True
        for block in ("base", "bull", "bear"):
            row = (forecast or {}).get(block, {}) or {}
            if _n(row.get("revenue_next")) in (None, 0) or _n(row.get("net_income_next")) in (None, 0):
                forecast_valid = False
                break

        return {
            "years": years,
            "start_year": sy,
            "end_year": ey,
            "start_raw": start_raw,
            "end_raw": end_raw,
            "start_rat": start_rat,
            "end_rat": end_rat,
            "latest": latest,
            "quality_score": q_score,
            "revenue": {"start": rev_s, "end": rev_e, "delta": rev_delta, "growth": rev_growth},
            "net_income": {"start": ni_s, "end": ni_e, "delta": ni_delta, "growth": ni_growth},
            "cfo": {"start": cfo_s, "end": cfo_e, "delta": cfo_delta, "growth": cfo_growth},
            "fcf": {"start": fcf_s, "end": fcf_e, "delta": fcf_delta, "growth": fcf_growth},
            "margins": {
                "gross_start": gm_s, "gross_end": gm_e, "gross_delta": gm_delta,
                "operating_start": om_s, "operating_end": om_e, "operating_delta": om_delta,
                "net_start": nm_s, "net_end": nm_e, "net_delta": nm_delta,
            },
            "liquidity": {"current_start": cr_s, "current_end": cr_e, "current_delta": cr_delta, "quick_end": qr_e, "cash_end": cash_r_e},
            "leverage": {"dte_start": dte_s, "dte_end": dte_e, "dte_delta": dte_delta, "interest_coverage_end": ic_e},
            "returns": {"roa_end": roa_e, "roe_end": roe_e, "roic_end": roic_e, "wacc_end": wacc_e, "spread_end": spread_e},
            "valuation": {"pe_end": pe_e, "pb_end": pb_e, "ev_ebitda_end": ev_e, "fcf_yield_end": fcf_y_e},
            "balance_sheet": {"assets_end": assets_e, "liabilities_end": liab_e, "equity_end": eq_e},
            "cash_conversion": cash_conversion,
            "contradictions": contradictions,
            "business_quality": business_quality,
            "valuation_view": valuation_view,
            "forecast_valid": forecast_valid,
        }

    def _build_expert_intent_answer(self, lang, intent, name, ticker, diag, forecast):
        if not diag:
            return "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"

        def _fmt(v, pct=False):
            if v is None:
                return "غير متاح" if lang == "ar" else "N/A"
            try:
                fv = float(v)
                return f"{fv:.2%}" if pct else f"{fv:,.2f}"
            except Exception:
                return str(v)

        rev = diag["revenue"]
        ni = diag["net_income"]
        cfo = diag["cfo"]
        fcf = diag["fcf"]
        margins = diag["margins"]
        liq = diag["liquidity"]
        lev = diag["leverage"]
        ret = diag["returns"]
        val = diag["valuation"]
        contradictions = diag["contradictions"] or []
        contradictions_text = "\n".join(f"- {x}" for x in contradictions) if contradictions else "- لا توجد تناقضات جوهرية بارزة في الفترة الحالية."

        if lang == "ar":
            if intent == "performance":
                return (
                    f"الجواب المباشر حول الأداء المالي لـ {name} ({ticker})\n"
                    f"خلال الفترة {diag['start_year']} إلى {diag['end_year']}، ارتفعت الإيرادات من {_fmt(rev['start'])} إلى {_fmt(rev['end'])} "
                    f"(التغير: {rev['delta']} | النمو: {rev['growth']})، وارتفع صافي الربح من {_fmt(ni['start'])} إلى {_fmt(ni['end'])} "
                    f"(التغير: {ni['delta']} | النمو: {ni['growth']}).\n\n"
                    f"الأدلة الرقمية:\n"
                    f"- الهامش الإجمالي: {_fmt(margins['gross_start'], pct=True)} -> {_fmt(margins['gross_end'], pct=True)}\n"
                    f"- الهامش التشغيلي: {_fmt(margins['operating_start'], pct=True)} -> {_fmt(margins['operating_end'], pct=True)}\n"
                    f"- الهامش الصافي: {_fmt(margins['net_start'], pct=True)} -> {_fmt(margins['net_end'], pct=True)}\n"
                    f"- التدفق النقدي التشغيلي: {_fmt(cfo['start'])} -> {_fmt(cfo['end'])}\n"
                    f"- التدفق النقدي الحر: {_fmt(fcf['start'])} -> {_fmt(fcf['end'])}\n\n"
                    f"التفسير السببي:\n"
                    f"{diag['business_quality']} كما أن مسار الهوامش يوحي بأن التحسن لم يكن اسميًا فقط، بل انعكس جزئيًا على الربحية التشغيلية. "
                    f"وفي الوقت نفسه، فإن مقارنة صافي الربح بالنقد التشغيلي تكشف أن الحكم النهائي يجب أن يبقى مرتبطًا بجودة التحول النقدي لا بالأرباح وحدها.\n\n"
                    f"التناقضات أو العناصر المختلطة:\n{contradictions_text}\n\n"
                    f"ما الذي يعنيه هذا للمستثمر؟\n"
                    f"الصورة العامة تميل إلى نشاط قوي نسبيًا، لكن القرار الاستثماري السليم يتطلب ربط هذا الأداء بمسار السيولة والرافعة والتقييم، "
                    f"لا الاكتفاء بنمو الإيرادات أو الأرباح وحده."
                )
            if intent == "risk":
                return (
                    f"الجواب المباشر حول المخاطر في {name} ({ticker})\n"
                    f"الخطر المالي الحالي يبدو {'منضبطًا نسبيًا' if (lev['interest_coverage_end'] or 0) > 5 else 'قيد المراقبة'}، "
                    f"لأن تغطية الفائدة عند {_fmt(lev['interest_coverage_end'])} والدين إلى حقوق الملكية عند {_fmt(lev['dte_end'])}.\n\n"
                    f"الأدلة الرقمية:\n"
                    f"- Current Ratio: {_fmt(liq['current_end'])}\n"
                    f"- Quick Ratio: {_fmt(liq['quick_end'])}\n"
                    f"- Cash Ratio: {_fmt(liq['cash_end'])}\n"
                    f"- Debt/Equity: {_fmt(lev['dte_end'])}\n"
                    f"- Interest Coverage: {_fmt(lev['interest_coverage_end'])}\n"
                    f"- Alt business view: {diag['business_quality']}\n\n"
                    f"التفسير السببي:\n"
                    f"إذا كانت السيولة الجارية منخفضة لكن التدفق النقدي التشغيلي موجبًا، فإن الخطر يكون في المرونة قصيرة الأجل أكثر من كونه خطر تعثر مباشر. "
                    f"أما إذا تراجعت التغطية وارتفعت الرافعة بالتزامن مع تباطؤ النقد، فإن المخاطر تنتقل من مجرد مراقبة إلى ضغط فعلي على الأطروحة الاستثمارية.\n\n"
                    f"التناقضات أو العناصر المختلطة:\n{contradictions_text}\n\n"
                    f"ما الذي يعنيه هذا للمستثمر؟\n"
                    f"المتابعة يجب أن تركز على التغطية النقدية والفائدة واتجاه الرافعة، لأن هذه العناصر تسبق تدهور الحكم النهائي على السهم."
                )
            if intent == "forecast":
                base = (forecast or {}).get("base", {}) or {}
                bull = (forecast or {}).get("bull", {}) or {}
                bear = (forecast or {}).get("bear", {}) or {}
                if not diag.get("forecast_valid"):
                    return (
                        f"الجواب المباشر حول التوقعات لـ {name} ({ticker})\n"
                        f"بيانات التوقعات الحالية غير صالحة بما يكفي لبناء قراءة خبير موثوقة.\n\n"
                        f"ما الذي يعنيه هذا للمستثمر؟\n"
                        f"لا ينبغي رفع الثقة أو إصدار حكم استثماري قوي اعتمادًا على توقعات ناقصة أو صفرية."
                    )
                return (
                    f"الجواب المباشر حول التوقعات لـ {name} ({ticker})\n"
                    f"السيناريو الأساسي يشير إلى إيرادات متوقعة عند {_fmt(base.get('revenue_next'))} وصافي ربح عند {_fmt(base.get('net_income_next'))}. "
                    f"أما السيناريو المتفائل فيرفع الإيرادات إلى {_fmt(bull.get('revenue_next'))} وصافي الربح إلى {_fmt(bull.get('net_income_next'))}، "
                    f"بينما السيناريو المتحفظ يخفضهما إلى {_fmt(bear.get('revenue_next'))} و{_fmt(bear.get('net_income_next'))}.\n\n"
                    f"التفسير السببي:\n"
                    f"معقولية السيناريو الأساسي تعتمد على استمرار الهوامش الحالية وعدم تدهور التحول النقدي. "
                    f"أما تحقق السيناريو المتفائل فيتطلب استدامة قوة التسعير وكفاءة تخصيص رأس المال، "
                    f"في حين أن السيناريو المتحفظ يصبح أقرب إذا تباطأ النمو أو انضغطت الهوامش أو ارتفعت تكلفة رأس المال.\n\n"
                    f"ما الذي يعنيه هذا للمستثمر؟\n"
                    f"قيمة التوقعات ليست في الرقم وحده، بل في اتساع الفارق بين السيناريوهات؛ كلما اتسع هذا الفارق ارتفعت أهمية هامش الأمان."
                )
            if intent == "ratios":
                return (
                    f"الجواب المباشر حول النسب الحالية لـ {name} ({ticker})\n"
                    f"الربحية الحالية تبدو قوية نسبيًا مع ROIC عند {_fmt(ret['roic_end'], pct=True)} وهامش صافٍ عند {_fmt(margins['net_end'], pct=True)}، "
                    f"لكن قراءة السهم لا تكتمل دون وضع ذلك بجانب التقييم الحالي.\n\n"
                    f"الأدلة الرقمية:\n"
                    f"- ROA: {_fmt(ret['roa_end'], pct=True)}\n"
                    f"- ROE: {_fmt(ret['roe_end'], pct=True)}\n"
                    f"- ROIC: {_fmt(ret['roic_end'], pct=True)}\n"
                    f"- P/E: {_fmt(val['pe_end'])}\n"
                    f"- P/B: {_fmt(val['pb_end'])}\n"
                    f"- EV/EBITDA: {_fmt(val['ev_ebitda_end'])}\n"
                    f"- FCF Yield: {_fmt(val['fcf_yield_end'], pct=True)}\n\n"
                    f"التفسير السببي:\n"
                    f"{diag['valuation_view']} هذا مهم لأن جودة الشركة لا تعني تلقائيًا أن السهم رخيص، "
                    f"بل قد يكون السوق سبق جزءًا كبيرًا من القوة التشغيلية في التسعير الحالي.\n\n"
                    f"ما الذي يعنيه هذا للمستثمر؟\n"
                    f"أفضل استخدام للنسب هو ربط الربحية بخلق القيمة والتقييم، لا قراءة كل نسبة بمعزل عن الأخرى."
                )
            return (
                f"الجواب المباشر حول {name} ({ticker})\n"
                f"{diag['business_quality']}\n\n"
                f"الأدلة الرقمية:\n"
                f"- الإيرادات: {_fmt(rev['start'])} -> {_fmt(rev['end'])}\n"
                f"- صافي الربح: {_fmt(ni['start'])} -> {_fmt(ni['end'])}\n"
                f"- ROIC: {_fmt(ret['roic_end'], pct=True)}\n"
                f"- Debt/Equity: {_fmt(lev['dte_end'])}\n"
                f"- P/E: {_fmt(val['pe_end'])}\n\n"
                f"التفسير السببي:\n"
                f"{diag['valuation_view']}\n\n"
                f"ما الذي يعنيه هذا للمستثمر؟\n"
                f"السؤال الحاسم ليس فقط هل الشركة جيدة، بل هل الجمع بين جودة النشاط والسعر الحالي يبرر بناء مركز استثماري الآن."
            )

        return (
            f"Expert answer on {name} ({ticker})\n"
            f"Period reviewed: {diag['start_year']} to {diag['end_year']}\n"
            f"Revenue moved from {_fmt(rev['start'])} to {_fmt(rev['end'])}; net income moved from {_fmt(ni['start'])} to {_fmt(ni['end'])}. "
            f"Current ROIC is {_fmt(ret['roic_end'], pct=True)} and current P/E is {_fmt(val['pe_end'])}.\n\n"
            f"Analytical interpretation:\n"
            f"{diag['business_quality']} {diag['valuation_view']}\n\n"
            f"Investor implication:\n"
            f"The correct judgment depends on linking quality, cash conversion, leverage, and valuation rather than any isolated metric."
        )

    def _render_payload_report_ar(self, payload: dict):
        if not isinstance(payload, dict):
            return "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"

        sections = self.get_report_sections_ar(payload)
        parts = [
            f"{payload.get('report_title') or 'تقرير تحليل مالي واستثماري'}",
            f"الشركة: {payload.get('company_name') or 'غير متاح'} ({payload.get('ticker') or 'غير متاح'})",
            f"تاريخ التقرير: {payload.get('report_date') or 'غير متاح'}",
            f"مصدر البيانات: {payload.get('data_source') or 'غير متاح'}",
            "",
        ]
        for sec in sections:
            parts.append(str(sec.get("title") or ""))
            for block in (sec.get("blocks") or []):
                btype = block[0]
                if btype == "text":
                    parts.append(str(block[1] or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"))
                elif btype == "label":
                    parts.append(str(block[1] or ""))
                elif btype == "bullets":
                    items = [str(x).strip() for x in (block[1] or []) if str(x).strip()]
                    parts.append("\n".join(f"- {x}" for x in items) if items else "- DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES")
            parts.append("")
        return "\n".join(parts)

    def get_report_sections_ar(self, payload: dict):
        if not isinstance(payload, dict):
            return []

        rating = (payload.get("investment_rating") or {})
        es = (payload.get("executive_summary") or {})
        bm = (payload.get("business_model_and_positioning") or {})
        hist = (payload.get("historical_financial_analysis") or {})
        prof = (payload.get("profitability_analysis") or {})
        ls = (payload.get("liquidity_solvency_leverage") or {})
        cf = (payload.get("cash_flow_and_earnings_quality") or {})
        ce = (payload.get("capital_efficiency_and_value_creation") or {})
        sp = (payload.get("sector_and_peer_context") or {})
        rk = (payload.get("risk_analysis") or {})
        fc = (payload.get("forecast_analysis") or {})
        va = (payload.get("valuation_analysis") or {})
        final_case = (payload.get("final_investment_case") or {})
        sr = (payload.get("self_review") or {})

        return [
            {
                "title": "1. الملخص التنفيذي",
                "blocks": [
                    ("text", es.get("overview") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("label", "أبرز نقاط القوة:"),
                    ("bullets", es.get("key_strengths") or []),
                    ("label", "أبرز المخاطر:"),
                    ("bullets", es.get("key_risks") or []),
                    ("label", "القراءة الأولية:"),
                    ("text", es.get("preliminary_view") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "2. نموذج الأعمال والتموضع الاستراتيجي",
                "blocks": [
                    ("text", f"نموذج الأعمال: {bm.get('business_model') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"خصائص الإيرادات: {bm.get('revenue_characteristics') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"المزايا التنافسية: {bm.get('competitive_advantages') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"التموضع الاستراتيجي: {bm.get('strategic_positioning') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                ],
            },
            {
                "title": "3. التحليل التاريخي للأداء المالي",
                "blocks": [
                    ("text", hist.get("summary_table") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("label", "القراءة التحليلية:"),
                    ("text", hist.get("analysis") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("label", "ما الذي يعنيه هذا للمستثمر؟"),
                    ("text", hist.get("investor_implication") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "4. تحليل الربحية",
                "blocks": [
                    ("text", f"مراجعة المؤشرات: {prof.get('metrics_review') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "القراءة التحليلية:"),
                    ("text", prof.get("analysis") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("text", f"رؤية الاستدامة: {prof.get('sustainability_view') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "ما الذي يعنيه هذا للمستثمر؟"),
                    ("text", prof.get("investor_implication") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "5. تحليل السيولة والملاءة والرافعة",
                "blocks": [
                    ("text", f"مراجعة المؤشرات: {ls.get('metrics_review') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "القراءة التحليلية:"),
                    ("text", ls.get("analysis") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("text", f"حكم الميزانية: {ls.get('balance_sheet_view') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "ما الذي يعنيه هذا للمستثمر؟"),
                    ("text", ls.get("investor_implication") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "6. تحليل التدفقات النقدية وجودة الأرباح",
                "blocks": [
                    ("text", f"مراجعة التدفقات: {cf.get('cash_flow_review') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"تقييم جودة الأرباح: {cf.get('earnings_quality_assessment') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "القراءة التحليلية:"),
                    ("text", cf.get("analysis") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("label", "ما الذي يعنيه هذا للمستثمر؟"),
                    ("text", cf.get("investor_implication") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "7. تحليل كفاءة رأس المال وخلق القيمة الاقتصادية",
                "blocks": [
                    ("text", f"مراجعة كفاءة رأس المال: {ce.get('capital_efficiency_review') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"خلق القيمة الاقتصادية: {ce.get('economic_value_creation') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "القراءة التحليلية:"),
                    ("text", ce.get("analysis") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("label", "ما الذي يعنيه هذا للمستثمر؟"),
                    ("text", ce.get("investor_implication") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "8. التحليل القطاعي والمقارنات المرجعية",
                "blocks": [
                    ("text", f"أساس المقارنة: {sp.get('comparison_basis') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"التموضع النسبي: {sp.get('relative_positioning') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "القراءة التحليلية:"),
                    ("text", sp.get("analysis") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("label", "ما الذي يعنيه هذا للمستثمر؟"),
                    ("text", sp.get("investor_implication") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "9. تحليل المخاطر المتقدم",
                "blocks": [
                    ("label", "المخاطر المالية:"),
                    ("bullets", rk.get("financial_risks") or []),
                    ("label", "المخاطر التشغيلية:"),
                    ("bullets", rk.get("operating_risks") or []),
                    ("label", "المخاطر الاستراتيجية:"),
                    ("bullets", rk.get("strategic_risks") or []),
                    ("label", "المخاطر التقييمية:"),
                    ("bullets", rk.get("valuation_risks") or []),
                    ("text", f"الرؤية الإجمالية للمخاطر: {rk.get('overall_risk_view') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                ],
            },
            {
                "title": "10. تحليل التوقعات المستقبلية",
                "blocks": [
                    ("text", f"السيناريو الأساسي: {fc.get('base_case') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"السيناريو المتفائل: {fc.get('bull_case') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"السيناريو المتحفظ: {fc.get('bear_case') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "القراءة التحليلية:"),
                    ("text", fc.get("scenario_interpretation") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("label", "ما الذي يعنيه هذا للمستثمر؟"),
                    ("text", fc.get("investor_implication") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "11. تحليل التقييم",
                "blocks": [
                    ("text", f"لقطة التقييم: {va.get('valuation_snapshot') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"تقييم العدالة السعرية: {va.get('fairness_assessment') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("label", "القراءة التحليلية:"),
                    ("text", va.get("multiple_interpretation") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                    ("label", "ما الذي يعنيه هذا للمستثمر؟"),
                    ("text", va.get("investor_implication") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "12. الحكم الاستثماري النهائي",
                "blocks": [
                    ("text", f"التوصية: {final_case.get('rating_label') or rating.get('label') or 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'}"),
                    ("text", f"الدرجة من 100: {final_case.get('score_100', rating.get('score_100', 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'))}"),
                    ("text", f"الثقة: {final_case.get('confidence', rating.get('confidence', 'DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES'))}%"),
                    ("label", "أهم 5 مبررات:"),
                    ("bullets", final_case.get("top_5_reasons") or []),
                    ("label", "أهم 5 نقاط مراقبة:"),
                    ("bullets", final_case.get("top_5_monitoring_points") or []),
                    ("label", "العوامل التي قد تغيّر التوصية:"),
                    ("text", final_case.get("what_could_change_the_rating") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "13. الخلاصة التنفيذية النهائية",
                "blocks": [
                    ("text", payload.get("closing_summary") or "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"),
                ],
            },
            {
                "title": "14. المراجعة الذاتية",
                "blocks": [
                    ("text", f"العمق الرقمي: {sr.get('numerical_depth', 'N/A')}/10"),
                    ("text", f"الجودة السردية: {sr.get('narrative_quality', 'N/A')}/10"),
                    ("text", f"المنطق الاقتصادي: {sr.get('economic_reasoning', 'N/A')}/10"),
                    ("text", f"جودة المخاطر: {sr.get('risk_analysis_quality', 'N/A')}/10"),
                    ("text", f"جودة التقييم: {sr.get('valuation_quality', 'N/A')}/10"),
                    ("text", f"اتساق التوصية: {sr.get('consistency_of_final_rating', 'N/A')}/10"),
                    ("text", f"جودة اللغة الاحترافية: {sr.get('professional_language_quality', 'N/A')}/10"),
                    ("text", f"مدى القرب من التقارير العالمية: {sr.get('global_report_similarity', 'N/A')}/10"),
                ],
            },
        ]

    def _answer_local(self, question: str, context: dict, response_mode: str = "auto"):
        mode, sig = self._resolve_response_mode(question, response_mode)
        lang = sig["lang"]
        company = (context or {}).get("company", {}) or {}
        ticker = company.get("ticker") or "N/A"
        name = company.get("name") or ticker
        latest_year = (context or {}).get("latest_year")
        latest_ratios = (context or {}).get("latest_ratios", {}) or {}
        ratios_by_year = (context or {}).get("ratios_by_year", {}) or {}
        data_by_year = (context or {}).get("data_by_year", {}) or {}
        quality = (context or {}).get("ai_quality", {}) or {}
        forecast = (context or {}).get("forecast", {}) or {}
        y0, y1 = self._extract_year_range(question, context)

        intent = sig["intent"]

        if mode == "report" or intent == "report":
            return (
                self._report_text(lang, name, ticker, latest_year, latest_ratios, quality, forecast, ratios_by_year, y0, y1, data_by_year),
                {"engine": "local", "mode": "report", "intent": "report"},
            )
        diag = self._collect_expert_diagnostics(
            ratios_by_year=ratios_by_year,
            data_by_year=data_by_year,
            latest_ratios=latest_ratios,
            forecast=forecast,
            quality=quality,
            y0=y0,
            y1=y1,
        )
        if intent == "performance":
            text = self._performance_text(lang, name, ticker, latest_year, ratios_by_year, y0, y1, quality)
        elif intent == "risk":
            text = self._risk_text(lang, name, latest_year, latest_ratios, quality)
        elif intent == "forecast":
            text = self._forecast_text(lang, name, forecast)
        elif intent == "ratios":
            text = self._ratio_text(lang, name, latest_year, latest_ratios)
        else:
            text = self._generic_text(lang, name, ticker, latest_year, latest_ratios, quality)

        if mode == "quick":
            text = self._make_quick_answer(lang, text)
        else:
            text = self._build_expert_intent_answer(lang, intent, name, ticker, diag, forecast)
        return text, {"engine": "local", "mode": mode, "intent": intent}

    def _make_quick_answer(self, lang, text: str):
        lines = [ln.strip() for ln in str(text or "").splitlines() if ln.strip()]
        if not lines:
            return "DATA_NOT_AVAILABLE_FROM_AUTHORITATIVE_SOURCES"
        head = lines[:5]
        if lang == "ar":
            return "الجواب المباشر:\n" + "\n".join(head)
        if lang == "fr":
            return "Réponse directe :\n" + "\n".join(head)
        return "Direct answer:\n" + "\n".join(head)

    def _make_expert_answer(self, lang, name, ticker, base_text: str, latest_ratios: dict, quality: dict):
        q = _fmt_num((quality or {}).get("quality_score"))
        gm = _fmt_num((latest_ratios or {}).get("gross_margin"))
        nm = _fmt_num((latest_ratios or {}).get("net_margin"))
        roic = _fmt_num((latest_ratios or {}).get("roic"))
        pe = _fmt_num((latest_ratios or {}).get("pe_ratio"))
        pb = _fmt_num((latest_ratios or {}).get("pb_ratio"))
        if lang == "ar":
            return (
                f"الجواب المباشر حول {name} ({ticker})\n"
                f"{base_text}\n\n"
                f"الأدلة الرقمية المرجعية:\n"
                f"- هامش الربح الإجمالي: {gm}\n"
                f"- هامش صافي الربح: {nm}\n"
                f"- ROIC: {roic}\n"
                f"- P/E: {pe}\n"
                f"- P/B: {pb}\n"
                f"- Investment Quality Score: {q}/100\n\n"
                f"التفسير التحليلي:\n"
                f"القراءة المهنية لا تتوقف عند الأرقام الفردية، بل تربط بين الربحية، خلق القيمة، والتقييم. "
                f"إذا بقيت الهوامش والعائد على رأس المال مرتفعة بينما التقييم يتمدد سريعًا، تكون جودة الشركة قوية لكن جاذبية السهم أقل نسبيًا.\n\n"
                f"ما الذي يعنيه هذا للمستثمر؟\n"
                f"استخدم هذه الإجابة كنقطة حكم أولي، ثم اربطها بمسار النقد، الرافعة، واتجاه التقييم قبل اتخاذ قرار نهائي."
            )
        if lang == "fr":
            return (
                f"Réponse experte sur {name} ({ticker})\n{base_text}\n\n"
                f"Repères chiffrés: marge brute={gm}, marge nette={nm}, ROIC={roic}, P/E={pe}, P/B={pb}, quality score={q}/100.\n"
                f"Lecture analytique: il faut relier rentabilité, création de valeur et niveau de valorisation avant de conclure."
            )
        return (
            f"Expert answer on {name} ({ticker})\n{base_text}\n\n"
            f"Numeric anchors: gross margin={gm}, net margin={nm}, ROIC={roic}, P/E={pe}, P/B={pb}, quality score={q}/100.\n"
            f"Analytical read: the correct judgment links profitability, value creation, and valuation rather than any single metric."
        )

    def _performance_text(self, lang, name, ticker, latest_year, ratios_by_year, y0, y1, quality):
        y0 = int(y0) if y0 is not None else latest_year
        y1 = int(y1) if y1 is not None else latest_year
        if y0 is None or y1 is None:
            return self._generic_text(lang, name, ticker, latest_year, {}, quality)
        if y0 > y1:
            y0, y1 = y1, y0

        rev0 = self._pick_year_val(ratios_by_year, y0, ["revenue"])
        rev1 = self._pick_year_val(ratios_by_year, y1, ["revenue"])
        ni0 = self._pick_year_val(ratios_by_year, y0, ["net_income"])
        ni1 = self._pick_year_val(ratios_by_year, y1, ["net_income"])
        gm0 = self._pick_year_val(ratios_by_year, y0, ["gross_margin"])
        gm1 = self._pick_year_val(ratios_by_year, y1, ["gross_margin"])
        om0 = self._pick_year_val(ratios_by_year, y0, ["operating_margin"])
        om1 = self._pick_year_val(ratios_by_year, y1, ["operating_margin"])
        nm0 = self._pick_year_val(ratios_by_year, y0, ["net_margin"])
        nm1 = self._pick_year_val(ratios_by_year, y1, ["net_margin"])
        dte0 = self._pick_year_val(ratios_by_year, y0, ["debt_to_equity"])
        dte1 = self._pick_year_val(ratios_by_year, y1, ["debt_to_equity"])
        cr0 = self._pick_year_val(ratios_by_year, y0, ["current_ratio"])
        cr1 = self._pick_year_val(ratios_by_year, y1, ["current_ratio"])
        q = _fmt_num((quality or {}).get("quality_score"))

        rev_delta, rev_pct = self._trend_line(rev0, rev1, ratio=False)
        ni_delta, ni_pct = self._trend_line(ni0, ni1, ratio=False)
        gm_delta, _ = self._trend_line(gm0, gm1, ratio=True)
        om_delta, _ = self._trend_line(om0, om1, ratio=True)
        nm_delta, _ = self._trend_line(nm0, nm1, ratio=True)
        dte_delta, _ = self._trend_line(dte0, dte1, ratio=True)
        cr_delta, _ = self._trend_line(cr0, cr1, ratio=True)

        if lang == "ar":
            return (
                f"تقييم الأداء المالي والوضعية - {name} ({ticker})\n"
                f"الفترة: {y0} إلى {y1}\n"
                f"1) النمو:\n"
                f"- الإيرادات: من {_fmt_num(rev0)} إلى {_fmt_num(rev1)} | التغير={rev_delta} | النمو={rev_pct}\n"
                f"- صافي الدخل: من {_fmt_num(ni0)} إلى {_fmt_num(ni1)} | التغير={ni_delta} | النمو={ni_pct}\n"
                f"2) الربحية:\n"
                f"- Gross Margin: {_fmt_num(gm0)} -> {_fmt_num(gm1)} (Δ={gm_delta})\n"
                f"- Operating Margin: {_fmt_num(om0)} -> {_fmt_num(om1)} (Δ={om_delta})\n"
                f"- Net Margin: {_fmt_num(nm0)} -> {_fmt_num(nm1)} (Δ={nm_delta})\n"
                f"3) المتانة المالية:\n"
                f"- Debt/Equity: {_fmt_num(dte0)} -> {_fmt_num(dte1)} (Δ={dte_delta})\n"
                f"- Current Ratio: {_fmt_num(cr0)} -> {_fmt_num(cr1)} (Δ={cr_delta})\n"
                f"4) جودة الاستثمار الحالية (AI): {q}/100\n"
                f"الخلاصة: الحكم النهائي يعتمد على الاتجاه متعدد السنوات لا على لقطة سنة واحدة."
            )
        if lang == "fr":
            return (
                f"Évaluation de la performance financière - {name} ({ticker})\n"
                f"Période: {y0} à {y1}\n"
                f"Croissance revenu: {_fmt_num(rev0)} -> {_fmt_num(rev1)} (Δ={rev_delta}, {rev_pct})\n"
                f"Croissance RN: {_fmt_num(ni0)} -> {_fmt_num(ni1)} (Δ={ni_delta}, {ni_pct})\n"
                f"Marges Δ: GM={gm_delta}, OM={om_delta}, NM={nm_delta}\n"
                f"Solidité Δ: D/E={dte_delta}, Current Ratio={cr_delta}\n"
                f"Score qualité IA: {q}/100"
            )
        return (
            f"Financial performance assessment - {name} ({ticker})\n"
            f"Period: {y0} to {y1}\n"
            f"Revenue: {_fmt_num(rev0)} -> {_fmt_num(rev1)} (delta={rev_delta}, growth={rev_pct})\n"
            f"Net Income: {_fmt_num(ni0)} -> {_fmt_num(ni1)} (delta={ni_delta}, growth={ni_pct})\n"
            f"Margin deltas: GM={gm_delta}, OM={om_delta}, NM={nm_delta}\n"
            f"Resilience deltas: D/E={dte_delta}, Current Ratio={cr_delta}\n"
            f"AI quality score: {q}/100"
        )

    def _report_text(self, lang, name, ticker, year, ratios, quality, forecast, ratios_by_year, y0, y1, data_by_year=None):
        rev = _fmt_num((forecast.get("base", {}) or {}).get("revenue_next"))
        ni = _fmt_num((forecast.get("base", {}) or {}).get("net_income_next"))
        q = _fmt_num(quality.get("quality_score"))
        gm = _fmt_num(ratios.get("gross_margin"))
        om = _fmt_num(ratios.get("operating_margin"))
        nm = _fmt_num(ratios.get("net_margin"))
        if lang == "ar":
            payload = self._build_strict_report_json_local(
                name=name,
                ticker=ticker,
                year=year,
                ratios=ratios,
                quality=quality,
                forecast=forecast,
                ratios_by_year=ratios_by_year,
                y0=y0,
                y1=y1,
                data_by_year=data_by_year,
            )
            return self._render_payload_report_ar(payload)
        if lang == "fr":
            return (
                f"Rapport synthétique - {name} ({ticker})\n"
                f"- Année de base: {year}\n"
                f"- Marges: brute={gm}, opérationnelle={om}, nette={nm}\n"
                f"- Score qualité (IA): {q}/100\n"
                f"- Prévision prochaine année (Base): revenu={rev}, résultat net={ni}\n"
                f"- Conclusion: combiner qualité, trajectoire des marges et levier avant décision."
            )
        return (
            f"Brief report - {name} ({ticker})\n"
            f"- Base year: {year}\n"
            f"- Margins: gross={gm}, operating={om}, net={nm}\n"
            f"- AI quality score: {q}/100\n"
            f"- Next-year base forecast: revenue={rev}, net income={ni}\n"
            f"- Conclusion: combine quality score, margin trend, and leverage before action."
        )

    def _risk_text(self, lang, name, year, ratios, quality):
        dte = _fmt_num(ratios.get("debt_to_equity"))
        ic = _fmt_num(ratios.get("interest_coverage"))
        cr = _fmt_num(ratios.get("current_ratio"))
        score = _fmt_num(quality.get("quality_score"))
        if lang == "ar":
            return (
                f"تحليل المخاطر - {name} ({year})\n"
                f"- Debt/Equity: {dte}\n"
                f"- Interest Coverage: {ic}\n"
                f"- Current Ratio: {cr}\n"
                f"- Investment Quality Score: {score}/100\n"
                f"التفسير: انخفاض التغطية أو السيولة مع مديونية مرتفعة يرفع المخاطر."
            )
        if lang == "fr":
            return (
                f"Analyse du risque - {name} ({year})\n"
                f"- Dette/Capitaux propres: {dte}\n"
                f"- Couverture des intérêts: {ic}\n"
                f"- Ratio courant: {cr}\n"
                f"- Score qualité investissement: {score}/100\n"
                f"Interprétation: faible couverture/liquidité avec levier élevé = risque accru."
            )
        return (
            f"Risk analysis - {name} ({year})\n"
            f"- Debt/Equity: {dte}\n"
            f"- Interest Coverage: {ic}\n"
            f"- Current Ratio: {cr}\n"
            f"- Investment Quality Score: {score}/100\n"
            f"Interpretation: weaker coverage/liquidity with high leverage implies higher risk."
        )

    def _forecast_text(self, lang, name, forecast):
        base = forecast.get("base", {}) or {}
        bull = forecast.get("bull", {}) or {}
        bear = forecast.get("bear", {}) or {}
        if lang == "ar":
            return (
                f"ملخص التوقعات - {name}\n"
                f"- Revenue (Bear/Base/Bull): {_fmt_num(bear.get('revenue_next'))} / {_fmt_num(base.get('revenue_next'))} / {_fmt_num(bull.get('revenue_next'))}\n"
                f"- Net Income (Bear/Base/Bull): {_fmt_num(bear.get('net_income_next'))} / {_fmt_num(base.get('net_income_next'))} / {_fmt_num(bull.get('net_income_next'))}\n"
                f"ملاحظة: هذه توقعات سيناريو وليست ضمانات."
            )
        if lang == "fr":
            return (
                f"Résumé des prévisions - {name}\n"
                f"- Revenu (Bear/Base/Bull): {_fmt_num(bear.get('revenue_next'))} / {_fmt_num(base.get('revenue_next'))} / {_fmt_num(bull.get('revenue_next'))}\n"
                f"- Résultat net (Bear/Base/Bull): {_fmt_num(bear.get('net_income_next'))} / {_fmt_num(base.get('net_income_next'))} / {_fmt_num(bull.get('net_income_next'))}\n"
                f"Note: scénarios de projection, pas des garanties."
            )
        return (
            f"Forecast summary - {name}\n"
            f"- Revenue (Bear/Base/Bull): {_fmt_num(bear.get('revenue_next'))} / {_fmt_num(base.get('revenue_next'))} / {_fmt_num(bull.get('revenue_next'))}\n"
            f"- Net income (Bear/Base/Bull): {_fmt_num(bear.get('net_income_next'))} / {_fmt_num(base.get('net_income_next'))} / {_fmt_num(bull.get('net_income_next'))}\n"
            f"Note: scenario forecasts, not guarantees."
        )

    def _ratio_text(self, lang, name, year, ratios):
        picked = [
            ("gross_margin", ratios.get("gross_margin")),
            ("operating_margin", ratios.get("operating_margin")),
            ("net_margin", ratios.get("net_margin")),
            ("roe", ratios.get("roe")),
            ("roa", ratios.get("roa")),
            ("roic", ratios.get("roic")),
            ("debt_to_equity", ratios.get("debt_to_equity")),
            ("interest_coverage", ratios.get("interest_coverage")),
            ("fcf_yield", ratios.get("fcf_yield")),
        ]
        body = "\n".join([f"- {k}: {_fmt_num(v)}" for k, v in picked])
        if lang == "ar":
            return f"قراءة النسب - {name} ({year})\n{body}"
        if lang == "fr":
            return f"Lecture des ratios - {name} ({year})\n{body}"
        return f"Ratio readout - {name} ({year})\n{body}"

    def _generic_text(self, lang, name, ticker, year, ratios, quality):
        q = _fmt_num(quality.get("quality_score"))
        gm = _fmt_num(ratios.get("gross_margin"))
        nm = _fmt_num(ratios.get("net_margin"))
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        if lang == "ar":
            return (
                f"جاهز للتحليل: {name} ({ticker})\n"
                f"- سنة مرجعية: {year}\n"
                f"- Gross Margin: {gm}\n"
                f"- Net Margin: {nm}\n"
                f"- Investment Quality Score: {q}/100\n"
                f"اسألني مباشرة: تقييم الأداء خلال فترة (مثال 2018-2025)، تحليل مخاطر، أو تقرير احترافي.\n"
                f"وقت الإنشاء: {ts}"
            )
        if lang == "fr":
            return (
                f"Prêt pour l'analyse: {name} ({ticker})\n"
                f"- Année de référence: {year}\n"
                f"- Marge brute: {gm}\n"
                f"- Marge nette: {nm}\n"
                f"- Score qualité investissement: {q}/100\n"
                f"Demandez: évaluation de performance sur période, risque, ou rapport professionnel.\n"
                f"Horodatage: {ts}"
            )
        return (
            f"Analysis ready: {name} ({ticker})\n"
            f"- Reference year: {year}\n"
            f"- Gross margin: {gm}\n"
            f"- Net margin: {nm}\n"
            f"- Investment quality score: {q}/100\n"
            f"Ask for: period performance assessment, risk analysis, or professional report.\n"
            f"Generated at: {ts}"
        )
