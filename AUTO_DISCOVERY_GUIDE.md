# 🔍 دليل الكشف التلقائي عن مفاهيم SEC

## 🎯 المشكلة

**الأسماء في SEC تختلف من شركة لأخرى!**

### مثال:
```
شركة A: Revenue → "RevenueFromContractWithCustomer"
شركة B: Revenue → "NetRevenueFromContinuingOperations"
شركة C: Revenue → "SalesRevenueNet"
```

---

## ✅ الحل الجديد

### **نظام الكشف التلقائي (Auto-Discovery)**

عند جلب بيانات أي شركة، سيقوم النظام بـ:

1. **فحص جميع المفاهيم المتاحة**
2. **مطابقتها مع القاعدة**
3. **عرض المفاهيم غير المطابقة**
4. **حفظها في ملف للمراجعة**

---

## 🎮 كيفية الاستخدام

### **الخطوة 1: جلب البيانات كالمعتاد**

```
1. أضف شركة (مثل: AAPL)
2. اضغط "جلب البيانات"
3. راقب الـ Console
```

### **الخطوة 2: مراقبة الـ Console**

ستظهر رسائل مثل:

```
🔍 بدء التعيين الذكي للمفاهيم...

✅ revenue: وُجد 2 تطابق
   - RevenueFromContractWithCustomerExcludingAssessedTax
   - Revenues

✅ cogs: وُجد 1 تطابق
   - CostOfRevenue

⚠️ توجد 15 مفهوم غير مطابق:

   💰 Revenue-related (3):
      - NetRevenueFromContinuingOperations
      - RevenuesFromExternalCustomers
      - SalesAndServiceRevenue
   
   ⚠️ تنبيه: أضف هذه الأسماء إلى قائمة 'revenue' في sec_fetcher.py!

📊 ملخص التعيين: 25 فئة مطابقة من 30 فئة إجمالي

💾 تم حفظ المفاهيم غير المطابقة في: unmatched_concepts.txt
```

### **الخطوة 3: فحص الملف المحفوظ**

افتح ملف `unmatched_concepts.txt` في نفس مجلد البرنامج:

```
======================================================================
المفاهيم غير المطابقة من SEC
Unmatched SEC Concepts
======================================================================

Total unmatched: 15

======================================================================
REVENUE-RELATED (3)
======================================================================
NetRevenueFromContinuingOperations
RevenuesFromExternalCustomers
SalesAndServiceRevenue

======================================================================
INCOME-RELATED (5)
======================================================================
IncomeLossFromContinuingOperationsBeforeIncomeTaxes
NetIncomeLossAttributableToNoncontrollingInterest
...
```

---

## 🔧 إضافة المفاهيم الجديدة

### **الطريقة السريعة:**

1. **افتح الملف:**
   ```
   modules/sec_fetcher.py
   ```

2. **ابحث عن:**
   ```python
   'revenue': [
   ```

3. **أضف الأسماء الجديدة:**
   ```python
   'revenue': [
       # الأسماء الموجودة...
       'revenue', 'revenues',
       
       # ✅ أضف الأسماء الجديدة من الملف
       'netrevenuefromcontinuingoperations',
       'revenuesfromexternalcustomers',
       'salesandservicerevenue',
   ],
   ```

4. **احفظ الملف وشغّل البرنامج مرة أخرى**

---

## 📊 أمثلة عملية

### **مثال 1: شركة ذات أسماء غريبة**

**Console Output:**
```
⚠️ توجد 8 مفهوم غير مطابق:

   💰 Revenue-related (2):
      - NetRevenueFromContinuingOperations
      - RevenueRecognizedFromPerformanceObligations
   
   📊 Income-related (3):
      - ComprehensiveIncomeLoss
      - IncomeLossFromDiscontinuedOperations
      - OtherComprehensiveIncome
```

**الإجراء:**
1. افتح `unmatched_concepts.txt`
2. انسخ الأسماء تحت "REVENUE-RELATED"
3. أضفها إلى قائمة `'revenue'` في `sec_fetcher.py`
4. أعد تشغيل البرنامج

### **مثال 2: بعد الإضافة**

```
🔍 بدء التعيين الذكي للمفاهيم...

✅ revenue: وُجد 3 تطابق
   - NetRevenueFromContinuingOperations  ← ✅ تم التعرف عليه الآن!
   - RevenueFromContractWithCustomer
   - Revenues

✅ جميع المفاهيم مطابقة! (100% coverage)

📊 ملخص التعيين: 30 فئة مطابقة من 30 فئة
```

---

## 🎯 نصائح مهمة

### **1. ركّز على Revenue أولاً:**
```
إذا رأيت مفاهيم متعلقة بـ Revenue غير مطابقة،
أضفها فوراً لأن Revenue هو الأهم!
```

### **2. انتبه للأسماء المشابهة:**
```python
# خطأ شائع:
'netrevenuefromcontinuingoperations'  # صحيح (lowercase)
'NetRevenueFromContinuingOperations'  # خطأ (uppercase في القائمة)

# النظام يحوّل كل شيء لـ lowercase تلقائياً
# لذا اكتب دائماً lowercase في القائمة
```

### **3. لا تضيف مفاهيم نادرة:**
```
إذا ظهر مفهوم في شركة واحدة فقط، اتركه.
أضف فقط المفاهيم التي تتكرر في عدة شركات.
```

---

## 📋 قائمة التحقق

عند إضافة مفاهيم جديدة:

- [ ] افتح `unmatched_concepts.txt`
- [ ] حدد المفاهيم المتكررة (3+ شركات)
- [ ] افتح `modules/sec_fetcher.py`
- [ ] ابحث عن الفئة المناسبة (`revenue`, `cogs`, إلخ)
- [ ] أضف الأسماء بـ **lowercase**
- [ ] احفظ الملف
- [ ] أعد تشغيل البرنامج
- [ ] تحقق من الـ Console - يجب أن ترى "✅" للمفاهيم الجديدة

---

## 🔍 فحص سريع

### **للتأكد من نجاح الإضافة:**

```
قبل الإضافة:
⚠️ توجد 15 مفهوم غير مطابق
📊 ملخص: 25 فئة مطابقة

بعد الإضافة:
⚠️ توجد 8 مفهوم غير مطابق  ← أقل!
📊 ملخص: 28 فئة مطابقة  ← أكثر!

الهدف:
✅ جميع المفاهيم مطابقة!
📊 ملخص: 30 فئة مطابقة  ← 100%!
```

---

## 🚀 التحسين المستمر

النظام مصمم للتحسين التلقائي:

```
جلب شركة 1 → اكتشاف 10 مفاهيم جديدة → إضافتها
جلب شركة 2 → اكتشاف 5 مفاهيم جديدة → إضافتها
جلب شركة 3 → اكتشاف 2 مفاهيم جديدة → إضافتها
جلب شركة 10 → اكتشاف 0 مفاهيم جديدة → ✅ قاعدة كاملة!
```

---

## 📝 أمثلة للإضافة

### **Revenue:**
```python
'revenue': [
    # الموجودة
    'revenue', 'revenues',
    
    # ✅ إضافات جديدة (من الملف)
    'netrevenuefromcontinuingoperations',
    'revenuesfromexternalcustomers',
    'salesandservicerevenue',
    'revenuerecognizedfromperformanceobligations',
],
```

### **Operating Income:**
```python
'operating_income': [
    # الموجودة
    'operatingincome', 'operatingincomeloss',
    
    # ✅ إضافات جديدة
    'incomelossFromcontinuingoperationsbeforeincometaxes',
    'operatingprofitorloss',
],
```

### **Assets:**
```python
'assets': [
    # الموجودة
    'assets', 'totalassets',
    
    # ✅ إضافات جديدة
    'assetsfairvaluedisclosure',
    'assetsnet',
],
```

---

## ✅ الخلاصة

### **النظام الجديد:**
1. ✅ يكتشف المفاهيم غير المطابقة تلقائياً
2. ✅ يصنفها حسب النوع (Revenue, Income, Asset...)
3. ✅ يحفظها في ملف للمراجعة
4. ✅ يعطيك تعليمات واضحة لإضافتها

### **دورك:**
1. راقب الـ Console
2. افتح `unmatched_concepts.txt`
3. أضف المفاهيم المتكررة إلى `sec_fetcher.py`
4. استمتع بتغطية 100%!

---

**🎯 الهدف: 100% تغطية لجميع الشركات!**

**الإصدار:** 7.1 - Auto-Discovery  
**التاريخ:** February 03, 2026  
**الحالة:** ✅ Ready for Continuous Improvement
