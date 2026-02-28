# -*- coding: utf-8 -*-
"""
sec_auto_learner.py
نظام التعلم التلقائي من SEC
Automatic Learning System for SEC Data
"""

import json
import os
import re
from typing import Dict, List, Tuple, Set
from collections import defaultdict


class SECAutoLearner:
    """
    نظام ذكي يتعلم تلقائياً كيف يفهم بيانات SEC
    ويربطها بالمفاهيم المالية المطلوبة
    """
    
    def __init__(self, learning_file='sec_learned_mappings.json'):
        self.learning_file = learning_file
        self.learned_mappings = self._load_learned_mappings()
        self.confidence_scores = {}
        
        # قاعدة الكلمات المفتاحية للمفاهيم المالية
        self.concept_keywords = {
            'revenue': {
                'must_have': ['revenue', 'sales'],
                'exclude': ['cost', 'expense', 'deferred', 'unearned'],
                'aliases': ['income', 'proceeds']
            },
            'cogs': {
                'must_have': ['cost'],
                'include_any': ['revenue', 'sales', 'goods', 'services', 'products'],
                'exclude': ['operating', 'selling', 'administrative']
            },
            'operating_income': {
                'must_have': ['operating', 'income'],
                'alternatives': [['operating', 'profit'], ['income', 'operations']],
                'exclude': ['net', 'comprehensive', 'discontinued']
            },
            'net_income': {
                'must_have': ['net', 'income'],
                'alternatives': [['profit', 'loss'], ['earnings']],
                'exclude': ['operating', 'comprehensive']
            },
            'assets': {
                'must_have': ['asset'],
                'exclude': ['liability', 'equity', 'deferred', 'intangible'],
                'context': ['total', 'current']
            },
            'current_assets': {
                'must_have': ['asset', 'current'],
                'exclude': ['noncurrent', 'total']
            },
            'liabilities': {
                'must_have': ['liabilit'],
                'exclude': ['asset', 'equity', 'deferred'],
                'context': ['total', 'current']
            },
            'current_liabilities': {
                'must_have': ['liabilit', 'current'],
                'exclude': ['noncurrent', 'total']
            },
            'equity': {
                'must_have': ['equity'],
                'alternatives': [['stockholder'], ['shareholder']],
                'exclude': ['liability', 'mezzanine']
            },
            'ar': {
                'must_have': ['account', 'receivable'],
                'alternatives': [['trade', 'receivable'], ['receivable', 'net']],
                'exclude': ['note', 'loan', 'noncurrent']
            },
            'inventory': {
                'must_have': ['inventor'],
                'exclude': ['reserve', 'obsolescence'],
                'context': ['net', 'total']
            },
            'ap': {
                'must_have': ['account', 'payable'],
                'alternatives': [['trade', 'payable']],
                'exclude': ['note', 'long', 'noncurrent']
            },
            'cash': {
                'must_have': ['cash'],
                'include_any': ['equivalent', 'restricted'],
                'exclude': ['flow', 'paid', 'received']
            },
            'short_debt': {
                'must_have': ['debt'],
                'include_any': ['short', 'current'],
                'exclude': ['long', 'noncurrent']
            },
            'long_debt': {
                'must_have': ['debt'],
                'include_any': ['long', 'noncurrent'],
                'exclude': ['short', 'current']
            },
            'ocf': {
                'must_have': ['cash', 'operating'],
                'alternatives': [['cash', 'operation'], ['operating', 'activities']],
                'exclude': ['investing', 'financing']
            },
            'capex': {
                'must_have': ['property', 'plant', 'equipment'],
                'include_any': ['payment', 'acquisition', 'purchase', 'addition'],
                'exclude': ['sale', 'disposal', 'proceeds']
            },
            'dividends': {
                'must_have': ['dividend'],
                'include_any': ['paid', 'payment', 'cash'],
                'exclude': ['receivable', 'declared', 'per']
            },
            'shares': {
                'must_have': ['share'],
                'include_any': ['outstanding', 'issued', 'common'],
                'exclude': ['preferred', 'treasury', 'price', 'based']
            },
            'gross_profit': {
                'must_have': ['gross', 'profit'],
                'alternatives': [['gross', 'margin']],
                'exclude': ['net', 'operating']
            },
            'depreciation': {
                'must_have': ['depreciat'],
                'include_any': ['amortization'],
                'exclude': ['accumulated', 'asset']
            },
            'interest_expense': {
                'must_have': ['interest', 'expense'],
                'alternatives': [['interest', 'paid']],
                'exclude': ['income', 'receivable']
            }
        }
    
    def analyze_concept_name(self, concept_name: str) -> Dict:
        """
        تحليل ذكي لاسم المفهوم من SEC
        """
        name_lower = concept_name.lower()
        
        # تقسيم الاسم إلى كلمات
        words = re.findall(r'[a-z]+', name_lower)
        
        analysis = {
            'original': concept_name,
            'words': words,
            'matches': {},
            'best_match': None,
            'confidence': 0.0
        }
        
        # البحث عن تطابقات
        for financial_concept, rules in self.concept_keywords.items():
            score = self._calculate_match_score(words, name_lower, rules)
            if score > 0:
                analysis['matches'][financial_concept] = score
        
        # تحديد أفضل تطابق
        if analysis['matches']:
            best = max(analysis['matches'].items(), key=lambda x: x[1])
            analysis['best_match'] = best[0]
            analysis['confidence'] = best[1]
        
        return analysis
    
    def _calculate_match_score(self, words: List[str], full_name: str, rules: Dict) -> float:
        """
        حساب نقاط التطابق بناءً على القواعد
        """
        score = 0.0
        
        # 1. فحص must_have (ضروري)
        must_have = rules.get('must_have', [])
        if must_have:
            must_have_count = sum(1 for keyword in must_have if any(keyword in word for word in words))
            if must_have_count < len(must_have):
                return 0.0  # لا تطابق إذا لم تتوفر الكلمات الضرورية
            score += 50.0  # نقاط أساسية للتطابق
        
        # 2. فحص alternatives (بدائل)
        alternatives = rules.get('alternatives', [])
        for alt_set in alternatives:
            if all(any(keyword in word for word in words) for keyword in alt_set):
                score += 40.0
                break
        
        # 3. فحص include_any (أي منها يزيد النقاط)
        include_any = rules.get('include_any', [])
        if include_any:
            matches = sum(1 for keyword in include_any if any(keyword in word for word in words))
            score += matches * 10.0
        
        # 4. فحص context (السياق)
        context = rules.get('context', [])
        if context:
            matches = sum(1 for keyword in context if any(keyword in word for word in words))
            score += matches * 5.0
        
        # 5. فحص exclude (استبعاد)
        exclude = rules.get('exclude', [])
        if exclude:
            exclusions = sum(1 for keyword in exclude if any(keyword in word for word in words))
            if exclusions > 0:
                score -= exclusions * 30.0  # خصم كبير للكلمات المستبعدة
        
        # 6. فحص aliases (مرادفات)
        aliases = rules.get('aliases', [])
        if aliases:
            matches = sum(1 for keyword in aliases if any(keyword in word for word in words))
            score += matches * 8.0
        
        return max(0.0, score)
    
    def auto_discover_mappings(self, sec_concepts: List[str]) -> Dict[str, List[Tuple[str, float]]]:
        """
        اكتشاف تلقائي للتعيينات
        """
        print("\n🤖 بدء التعلم التلقائي من SEC...")
        
        discovered = defaultdict(list)
        all_analyses = []
        
        # تحليل كل مفهوم
        for concept in sec_concepts:
            analysis = self.analyze_concept_name(concept)
            all_analyses.append(analysis)
            
            if analysis['best_match'] and analysis['confidence'] > 30:
                discovered[analysis['best_match']].append(
                    (concept, analysis['confidence'])
                )
        
        # ترتيب حسب الثقة
        for key in discovered:
            discovered[key].sort(key=lambda x: x[1], reverse=True)
        
        # حفظ النتائج
        self._save_discovered_mappings(discovered)
        
        # طباعة الملخص
        self._print_discovery_summary(discovered, all_analyses)
        
        return dict(discovered)
    
    def _print_discovery_summary(self, discovered: Dict, all_analyses: List):
        """
        طباعة ملخص الاكتشاف
        """
        print(f"\n{'='*70}")
        print("📊 ملخص التعلم التلقائي")
        print(f"{'='*70}\n")
        
        total_concepts = len(all_analyses)
        matched_concepts = sum(1 for a in all_analyses if a['best_match'] and a['confidence'] > 30)
        
        print(f"إجمالي المفاهيم: {total_concepts}")
        print(f"المفاهيم المطابقة: {matched_concepts} ({matched_concepts/total_concepts*100:.1f}%)")
        print(f"المفاهيم غير المطابقة: {total_concepts - matched_concepts}\n")
        
        # عرض التعيينات المكتشفة
        for financial_concept, mappings in sorted(discovered.items()):
            print(f"✅ {financial_concept}:")
            for concept, confidence in mappings[:3]:  # أول 3
                print(f"   - {concept} (ثقة: {confidence:.1f}%)")
            if len(mappings) > 3:
                print(f"   ... و {len(mappings) - 3} أخرى")
            print()
        
        # عرض المفاهيم غير المطابقة
        unmatched = [a['original'] for a in all_analyses if not a['best_match'] or a['confidence'] <= 30]
        if unmatched:
            print(f"⚠️ مفاهيم غير مطابقة ({len(unmatched)}):")
            for concept in unmatched[:10]:
                print(f"   - {concept}")
            if len(unmatched) > 10:
                print(f"   ... و {len(unmatched) - 10} أخرى")
        
        print(f"\n{'='*70}\n")
    
    def get_best_concept(self, financial_concept: str, available_concepts: Dict) -> Tuple[str, any]:
        """
        الحصول على أفضل مفهوم SEC للمفهوم المالي المطلوب
        """
        # أولوية 1: من التعيينات المتعلمة
        if financial_concept in self.learned_mappings:
            for learned_concept in self.learned_mappings[financial_concept]:
                if learned_concept in available_concepts:
                    return learned_concept, available_concepts[learned_concept]
        
        # أولوية 2: التحليل الذكي المباشر
        candidates = []
        for concept_name in available_concepts.keys():
            analysis = self.analyze_concept_name(concept_name)
            if analysis['best_match'] == financial_concept and analysis['confidence'] > 30:
                candidates.append((concept_name, analysis['confidence']))
        
        if candidates:
            candidates.sort(key=lambda x: x[1], reverse=True)
            best_concept = candidates[0][0]
            return best_concept, available_concepts[best_concept]
        
        return None, None
    
    def learn_from_usage(self, financial_concept: str, sec_concept: str, success: bool):
        """
        التعلم من الاستخدام
        """
        if financial_concept not in self.learned_mappings:
            self.learned_mappings[financial_concept] = []
        
        if success:
            # إضافة أو ترقية
            if sec_concept not in self.learned_mappings[financial_concept]:
                self.learned_mappings[financial_concept].insert(0, sec_concept)
            else:
                # نقل إلى الأعلى
                self.learned_mappings[financial_concept].remove(sec_concept)
                self.learned_mappings[financial_concept].insert(0, sec_concept)
            
            # حفظ
            self._save_learned_mappings()
            print(f"✅ تم التعلم: {financial_concept} → {sec_concept}")
    
    def _save_discovered_mappings(self, discovered: Dict):
        """
        حفظ التعيينات المكتشفة
        """
        # دمج مع التعيينات الموجودة
        for key, mappings in discovered.items():
            if key not in self.learned_mappings:
                self.learned_mappings[key] = []
            
            for concept, confidence in mappings:
                if concept not in self.learned_mappings[key]:
                    self.learned_mappings[key].append(concept)
                    self.confidence_scores[concept] = confidence
        
        self._save_learned_mappings()
    
    def _load_learned_mappings(self) -> Dict:
        """
        تحميل التعيينات المتعلمة
        """
        if os.path.exists(self.learning_file):
            try:
                with open(self.learning_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def _save_learned_mappings(self):
        """
        حفظ التعيينات المتعلمة
        """
        with open(self.learning_file, 'w', encoding='utf-8') as f:
            json.dump(self.learned_mappings, f, indent=2, ensure_ascii=False)
    
    def get_statistics(self) -> Dict:
        """
        الحصول على إحصائيات التعلم
        """
        total_mappings = sum(len(v) for v in self.learned_mappings.values())
        
        return {
            'total_financial_concepts': len(self.learned_mappings),
            'total_sec_mappings': total_mappings,
            'average_per_concept': total_mappings / len(self.learned_mappings) if self.learned_mappings else 0,
            'concepts_with_mappings': list(self.learned_mappings.keys())
        }
    
    def export_to_code(self, output_file='auto_generated_mappings.py'):
        """
        تصدير التعيينات المتعلمة إلى ملف Python
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# -*- coding: utf-8 -*-\n")
            f.write("# Auto-generated SEC mappings from learning system\n")
            f.write("# Generated automatically - DO NOT EDIT MANUALLY\n\n")
            f.write("AUTO_LEARNED_MAPPINGS = {\n")
            
            for concept, mappings in sorted(self.learned_mappings.items()):
                f.write(f"    '{concept}': [\n")
                for mapping in mappings:
                    f.write(f"        '{mapping.lower()}',\n")
                f.write("    ],\n")
            
            f.write("}\n")
        
        print(f"✅ تم تصدير التعيينات إلى: {output_file}")


def test_auto_learner():
    """
    اختبار النظام
    """
    learner = SECAutoLearner()
    
    # أمثلة من SEC
    test_concepts = [
        'RevenueFromContractWithCustomerExcludingAssessedTax',
        'NetRevenueFromContinuingOperations',
        'CostOfRevenue',
        'CostOfGoodsAndServicesSold',
        'OperatingIncomeLoss',
        'NetIncomeLoss',
        'Assets',
        'AssetsCurrent',
        'Liabilities',
        'LiabilitiesCurrent',
        'StockholdersEquity',
        'AccountsReceivableNetCurrent',
        'InventoryNet',
        'AccountsPayableCurrent',
        'CashAndCashEquivalentsAtCarryingValue',
        'LongTermDebt',
        'ShortTermBorrowings',
        'NetCashProvidedByUsedInOperatingActivities',
        'PaymentsToAcquirePropertyPlantAndEquipment',
        'PaymentsOfDividendsCommonStock'
    ]
    
    # اكتشاف تلقائي
    discovered = learner.auto_discover_mappings(test_concepts)
    
    # عرض الإحصائيات
    stats = learner.get_statistics()
    print("\n📊 إحصائيات التعلم:")
    print(f"   المفاهيم المالية: {stats['total_financial_concepts']}")
    print(f"   التعيينات الكلية: {stats['total_sec_mappings']}")
    print(f"   المتوسط لكل مفهوم: {stats['average_per_concept']:.1f}")
    
    # تصدير
    learner.export_to_code()


if __name__ == '__main__':
    test_auto_learner()
