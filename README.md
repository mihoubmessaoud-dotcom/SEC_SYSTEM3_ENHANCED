# 🚀 SEC Financial Analysis System - Enhanced Version

## نظام تحليل SEC المالي - النسخة المحسّنة

نظام متكامل لجلب وتحليل البيانات المالية من SEC مع **تحليل استراتيجي متقدم** خالٍ من قيم N/A.

---

## ✨ الميزات الرئيسية

### 🎯 التحليل الاستراتيجي المتقدم
- ✅ **5 نقاط محورية محلولة** - لا مزيد من N/A
- ✅ جلب تلقائي لبيانات السوق (السعر، Beta، الأسهم)
- ✅ حساب WACC باستخدام نموذج CAPM
- ✅ دعم كامل لتوزيعات الأرباح
- ✅ مقاييس FCF متقدمة

### 📊 البيانات المالية
- جلب بيانات من SEC EDGAR API
- دعم 10-K و 10-Q
- بيانات تاريخية متعددة السنوات
- Dynamic XBRL mapping

### 📈 النسب المالية
- 30+ نسبة مالية محسوبة
- Altman Z-Score
- نسب السيولة والربحية والرفع المالي
- Cash Conversion Cycle

### 💎 التحليل الاستراتيجي
- **Economic Spread** (ROIC - WACC)
- **Fair Value Estimate**
- **Investment Score**
- **SGR Internal** (معدل النمو المستدام)
- **FCF Yield** و **FCF per Share**
- **Beta** و **WACC** الدقيق

### 📉 الرسوم البيانية
- رسوم الاتجاهات التاريخية
- مقارنة بين الشركات
- توقعات مستقبلية

### 💾 التصدير
- تصدير إلى Excel (3 أوراق عمل)
- Raw Data by Year
- Financial Ratios
- Strategic Analysis

---

## 🆕 ما الجديد في هذه النسخة؟

### ✅ النقطة 1: ربط المدينين
- البيانات من `AccountsReceivableNetCurrent` تظهر الآن في `AR_Days`
- دورة رأس المال العامل (CCC) مكتملة

### ✅ النقطة 2: توزيعات الأرباح
- جلب `PaymentsOfDividendsCommonStock` من SEC
- حساب `Retention_Ratio` و `SGR_Internal` بدقة

### ✅ النقطة 3: سعر السهم اللحظي
- جلب تلقائي من Yahoo Finance
- دعم لـ `Fair_Value_Estimate` و `P/E Ratio`

### ✅ النقطة 4: Beta وتكلفة الملكية
- جلب Beta من السوق
- حساب WACC باستخدام CAPM
- `Economic_Spread` دقيق

### ✅ النقطة 5: عدد الأسهم القائمة
- دعم شامل من SEC و Yahoo Finance
- `FCF_Yield` و `FCF_per_Share` محسوبان

---

## 🛠️ المتطلبات

### Python Packages:
```bash
pip install requests pandas openpyxl yfinance matplotlib tkinter
```

### متطلبات النظام:
- Python 3.7+
- اتصال بالإنترنت (للوصول إلى SEC و Yahoo Finance)
- 50MB مساحة حرة على الأقل

---

## 🚀 البدء السريع

### 1. التثبيت:
```bash
cd SEC_SYSTEM3_ENHANCED
pip install -r requirements.txt
```

### 2. التشغيل:
```bash
python main.py
```

### 3. الاستخدام:
1. أدخل رمز السهم (مثل: `AAPL, MSFT, GOOGL`)
2. اضغط "إضافة"
3. اختر الفترة الزمنية (سنة البداية - سنة النهاية)
4. اضغط "جلب البيانات"
5. انتظر (سيتم جلب البيانات من SEC و Yahoo تلقائيًا)
6. راجع التحليل في التبويبات المختلفة

---

## 📖 دليل الاستخدام المفصّل

### التبويبات الرئيسية:

#### 1. البيانات المالية (Raw Data)
- عرض جميع بنود XBRL من SEC
- تنظيم حسب السنة
- بيانات Balance Sheet, Income Statement, Cash Flow

#### 2. النسب المالية (Financial Ratios)
- 30+ نسبة محسوبة
- Liquidity, Profitability, Leverage
- Activity & Efficiency ratios
- Cash Flow metrics

#### 3. التحليل الاستراتيجي (Strategic Analysis)
أهم تبويب! يحتوي على:

**Strategic & Value Tier:**
- Fair Value Estimate
- Investment Score (0-100)
- Economic Spread
- ROIC vs WACC
- ✅ Beta
- ✅ SGR Internal

**Quality & Risk Tier:**
- Altman Z-Score
- Warning Signals
- Accruals Analysis
- Credit Rating Score

**Performance Analysis Tier:**
- ROE
- Net Income Growth
- ✅ Retention Ratio
- ✅ Dividends Paid
- EBITDA
- ✅ FCF Yield
- EPS
- ✅ FCF per Share

**Operational Efficiency Tier:**
- Cash Conversion Cycle
- Inventory Days
- ✅ AR Days (DSO)
- AP Days (DPO)

#### 4. التحليل المقارن (Comparison)
- مقارنة بين الشركات المضافة
- Side-by-side metrics

#### 5. التوقعات (Forecasts)
- توقعات الإيرادات (10 سنوات)
- توقعات صافي الربح

---

## 🎯 أمثلة الاستخدام

### مثال 1: تحليل شركة واحدة
```
1. أدخل: AAPL
2. الفترة: 2020 - 2024
3. اضغط "جلب البيانات"
4. راجع التحليل الاستراتيجي
```

### مثال 2: مقارنة شركتين
```
1. أدخل: AAPL, MSFT
2. اضغط "إضافة"
3. اجلب البيانات
4. افتح تبويب "التحليل المقارن"
```

### مثال 3: تصدير لـ Excel
```
1. بعد جلب البيانات
2. اضغط "تصدير إلى Excel"
3. احفظ الملف
4. افتحه في Excel
```

---

## 📊 المقاييس الاستراتيجية المهمة

### Economic Spread:
```
Economic Spread = ROIC - WACC

إذا > 0: الشركة تخلق قيمة
إذا < 0: الشركة تدمر قيمة
```

### Investment Score:
```
درجة من 0-100 بناءً على:
- Economic Spread
- FCF Yield
- Altman Z-Score
- ROE
- Revenue Growth

> 70: استثمار ممتاز
50-70: استثمار جيد
30-50: محايد
< 30: احذر
```

### SGR Internal:
```
SGR = Retention Ratio × ROE

معدل النمو المستدام بدون تمويل خارجي
إذا كان Growth الفعلي > SGR → الشركة تحتاج تمويل
```

### FCF Yield:
```
FCF Yield = Free Cash Flow / Market Cap

> 5%: جذاب
3-5%: جيد
< 3%: قد لا يكون جذابًا
```

---

## 🔧 الإعدادات المتقدمة

### تعديل Risk-Free Rate:
في `main.py` سطر 518:
```python
risk_free_rate = 0.04  # 4% (عدّله حسب الحاجة)
```

### تعديل Market Risk Premium:
في `main.py` سطر 519:
```python
market_risk_premium = 0.08  # 8% (عدّله حسب الحاجة)
```

### تعديل Tax Rate:
في `main.py` سطر 386:
```python
tax_rate_default = 0.21  # 21% (عدّله حسب الحاجة)
```

---

## ⚠️ ملاحظات هامة

### SEC Rate Limiting:
- SEC API محدود بـ 10 طلبات/ثانية
- النظام يضيف تأخير 0.5 ثانية بين الطلبات
- يُنصح بعدم الجلب لأكثر من 5 شركات مرة واحدة

### Yahoo Finance:
- بعض الشركات قد لا تتوفر بياناتها
- Beta قد يكون None للشركات الصغيرة
- سيتم استخدام تقديرات بديلة في حالة الفشل

### البيانات المفقودة:
- بعض الشركات لا تُبلّغ عن كل البنود
- إذا كان البند فعلاً مفقود من SEC → سيظهر N/A
- لكن معظم المقاييس المحسوبة لن تكون N/A

---

## 🐛 حل المشاكل

### المشكلة: "لم يتم العثور على الشركة"
**الحل:** تأكد من رمز السهم صحيح (استخدم رمز NYSE/NASDAQ)

### المشكلة: "فشل الاتصال بـ SEC"
**الحل:** 
- تحقق من اتصال الإنترنت
- انتظر قليلاً وحاول مرة أخرى (قد يكون Rate Limiting)

### المشكلة: Beta = N/A
**الحل:**
- طبيعي لبعض الشركات الصغيرة
- سيتم استخدام تقدير بديل للـ WACC
- يمكنك إدخال Beta يدويًا إذا كنت تعرفه

### المشكلة: السعر لا يُجلب تلقائيًا
**الحل:**
- تحقق من أن yfinance مثبّت
- أدخل السعر يدويًا في حقل "سعر السهم الحالي"

---

## 📚 المراجع والمصادر

- **SEC EDGAR**: https://www.sec.gov/edgar.shtml
- **SEC API Documentation**: https://www.sec.gov/edgar/sec-api-documentation
- **XBRL US-GAAP**: https://xbrl.us/
- **Yahoo Finance**: https://finance.yahoo.com/
- **CAPM Model**: Investopedia
- **DCF Valuation**: Corporate Finance Institute

---

## 📝 هيكل الملفات

```
SEC_SYSTEM3_ENHANCED/
│
├── main.py                          # الملف الرئيسي (الواجهة)
├── modules/
│   ├── __init__.py
│   └── sec_fetcher.py              # جالب البيانات من SEC
├── exports/                         # مجلد التصدير (فارغ افتراضيًا)
├── requirements.txt                 # المكتبات المطلوبة
├── ENHANCEMENTS_GUIDE.md           # دليل التحسينات المفصّل
├── QUICK_COMPARISON.md             # مقارنة سريعة
└── README.md                        # هذا الملف
```

---

## 🤝 المساهمة

هذا مشروع مفتوح المصدر. إذا كنت تريد المساهمة:
1. Fork المشروع
2. أنشئ Branch جديد
3. اعمل التعديلات
4. اعمل Pull Request

---

## 📄 الترخيص

هذا المشروع مفتوح المصدر ومتاح للاستخدام الشخصي والتعليمي.

---

## 📞 الدعم

للأسئلة والدعم:
- راجع `ENHANCEMENTS_GUIDE.md` للتفاصيل الفنية
- راجع `QUICK_COMPARISON.md` للمقارنة السريعة
- اتبع قسم "حل المشاكل" أعلاه

---

## ⭐ الخلاصة

نظام تحليل SEC المالي المحسّن يوفر:
- ✅ تحليل استراتيجي متقدم **بدون N/A**
- ✅ جلب تلقائي لبيانات السوق
- ✅ حسابات دقيقة لـ WACC و Economic Spread
- ✅ دعم كامل للأرباح الموزعة
- ✅ مقاييس FCF متقدمة
- ✅ واجهة سهلة الاستخدام
- ✅ تصدير Excel احترافي

**جاهز للاستخدام المهني! 🚀**

---

**تم التطوير بواسطة:** Claude (Anthropic)  
**التاريخ:** February 2026  
**الإصدار:** 3.0 Enhanced

---

## Ratio Percent Canonical Convention

- All percent-like ratios are stored internally as FRACTION values.
- Example: `55.44%` must be stored as `0.5544`.
- The only module allowed to convert fraction to `%` display is `modules/ratio_formats.py`.
- Ratio computation code must not use `*100` or `/100` for ratio values.
- Use `format_ratio_value(ratio_id, value)` for all ratio rendering in UI/reports.
- Set `SEC_DEBUG_RATIO_FORMAT=1` to emit formatter traces and generate `exports/sector_comparison/ratio_format_trace.json`.
