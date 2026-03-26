# -*- coding: utf-8 -*-
"""
ml_trainer.py
نظام التدريب المستمر للتحليل الذكي
Continuous Machine Learning Training System
"""

import json
import os
import pickle
from datetime import datetime
from typing import Dict, List, Tuple
import numpy as np


class AITrainingSystem:
    """
    نظام التدريب المستمر للتحليل الذكي
    يتعلم من البيانات التاريخية ويحسّن دقة التنبؤات
    """
    
    def __init__(self, data_dir='ml_training_data'):
        self.data_dir = data_dir
        self.training_data_file = os.path.join(data_dir, 'training_data.json')
        self.models_file = os.path.join(data_dir, 'trained_models.pkl')
        self.stats_file = os.path.join(data_dir, 'training_stats.json')
        
        # إنشاء المجلد إذا لم يكن موجوداً
        os.makedirs(data_dir, exist_ok=True)
        
        # تحميل البيانات الموجودة
        self.training_data = self._load_training_data()
        self.trained_models = self._load_models()
        self.stats = self._load_stats()
    
    # ═══════════════════════════════════════════════════════════
    # جمع البيانات التدريبية
    # ═══════════════════════════════════════════════════════════
    
    def collect_company_data(self, ticker: str, data_by_year: Dict, 
                            ratios_by_year: Dict, ai_results: Dict) -> None:
        """
        جمع بيانات الشركة للتدريب
        """
        if ticker not in self.training_data:
            self.training_data[ticker] = {
                'history': [],
                'last_updated': None
            }
        
        # استخراج الميزات (features)
        features = self._extract_features(data_by_year, ratios_by_year)
        
        # حفظ السجل
        record = {
            'timestamp': datetime.now().isoformat(),
            'features': features,
            'ai_results': ai_results,
            'years': sorted([y for y in data_by_year.keys() if isinstance(y, int)])
        }
        
        self.training_data[ticker]['history'].append(record)
        self.training_data[ticker]['last_updated'] = datetime.now().isoformat()
        
        # حفظ
        self._save_training_data()
        
        # إحصائيات
        self.stats['total_companies'] = len(self.training_data)
        self.stats['total_records'] = sum(len(v['history']) for v in self.training_data.values())
        self.stats['last_update'] = datetime.now().isoformat()
        self._save_stats()
        
        print(f"✅ تم حفظ بيانات {ticker} للتدريب")
        print(f"📊 إجمالي الشركات: {self.stats['total_companies']}")
        print(f"📊 إجمالي السجلات: {self.stats['total_records']}")
    
    def _extract_features(self, data_by_year: Dict, ratios_by_year: Dict) -> Dict:
        """
        استخراج الميزات من البيانات
        """
        years = sorted([y for y in data_by_year.keys() if isinstance(y, int)])
        if not years:
            return {}
        
        latest = years[-1]
        latest_ratios = ratios_by_year.get(latest, {})
        
        features = {
            # نسب الربحية
            'roic': latest_ratios.get('roic'),
            'roe': latest_ratios.get('roe'),
            'net_margin': latest_ratios.get('net_margin'),
            'operating_margin': latest_ratios.get('operating_margin'),
            'gross_margin': latest_ratios.get('gross_margin'),
            
            # نسب السيولة
            'current_ratio': latest_ratios.get('current_ratio'),
            'quick_ratio': latest_ratios.get('quick_ratio'),
            'cash_ratio': latest_ratios.get('cash_ratio'),
            
            # نسب المديونية
            'debt_to_equity': latest_ratios.get('debt_to_equity'),
            'debt_to_assets': latest_ratios.get('debt_to_assets'),
            'net_debt_ebitda': latest_ratios.get('net_debt_ebitda'),
            'interest_coverage': latest_ratios.get('interest_coverage'),
            
            # نسب الأمان
            'altman_z_score': latest_ratios.get('altman_z_score'),
            'accruals_ratio': latest_ratios.get('accruals_ratio'),
            
            # نسب الكفاءة
            'asset_turnover': latest_ratios.get('asset_turnover'),
            'inventory_turnover': latest_ratios.get('inventory_turnover'),
            'days_sales_outstanding': latest_ratios.get('days_sales_outstanding'),
            
            # نسب النمو
            'sgr_internal': latest_ratios.get('sgr_internal'),
            'retention_ratio': latest_ratios.get('retention_ratio'),
            
            # نسب السوق
            'pe_ratio': latest_ratios.get('pe_ratio'),
            'pb_ratio': latest_ratios.get('pb_ratio'),
            'fcf_yield': latest_ratios.get('fcf_yield'),
            
            # التدفقات النقدية
            'ocf_margin': latest_ratios.get('ocf_margin'),
            'free_cash_flow': latest_ratios.get('free_cash_flow'),
        }
        
        # حساب اتجاهات (trends) إذا توفرت بيانات متعددة
        if len(years) >= 3:
            features['z_score_trend'] = self._calculate_trend(
                [ratios_by_year.get(y, {}).get('altman_z_score') for y in years[-3:]]
            )
            features['roe_trend'] = self._calculate_trend(
                [ratios_by_year.get(y, {}).get('roe') for y in years[-3:]]
            )
            features['debt_trend'] = self._calculate_trend(
                [ratios_by_year.get(y, {}).get('debt_to_equity') for y in years[-3:]]
            )
        
        return features
    
    def _calculate_trend(self, values: List) -> float:
        """حساب الاتجاه (slope)"""
        valid_values = [v for v in values if v is not None]
        if len(valid_values) < 2:
            return 0.0
        
        x = np.arange(len(valid_values))
        y = np.array(valid_values)
        
        try:
            # Linear regression
            slope = np.polyfit(x, y, 1)[0]
            return float(slope)
        except:
            return 0.0
    
    # ═══════════════════════════════════════════════════════════
    # التدريب والتحسين
    # ═══════════════════════════════════════════════════════════
    
    def train_models(self, min_samples: int = 10) -> Dict:
        """
        تدريب النماذج على البيانات المجمعة
        """
        print("🤖 بدء التدريب...")
        
        if self.stats['total_records'] < min_samples:
            print(f"⚠️ عدد السجلات ({self.stats['total_records']}) أقل من الحد الأدنى ({min_samples})")
            print("   سيتم استخدام القواعد الافتراضية حتى جمع المزيد من البيانات")
            return {}
        
        results = {
            'fraud_model': self._train_fraud_model(),
            'failure_model': self._train_failure_model(),
            'growth_model': self._train_growth_model(),
            'liquidity_model': self._train_liquidity_model(),
            'quality_model': self._train_quality_model(),
        }
        
        # حفظ النماذج
        self.trained_models = results
        self._save_models()
        
        # تحديث الإحصائيات
        self.stats['last_training'] = datetime.now().isoformat()
        self.stats['models_trained'] = len(results)
        self._save_stats()
        
        print(f"✅ تم تدريب {len(results)} نموذج بنجاح!")
        return results
    
    def _train_fraud_model(self) -> Dict:
        """
        تدريب نموذج كشف الاحتيال
        يتعلم من العلاقات بين Accruals والتدفقات النقدية
        """
        print("   🔍 تدريب نموذج كشف الاحتيال...")
        
        # جمع البيانات
        X = []  # features
        y = []  # labels (1 = احتيال محتمل, 0 = آمن)
        
        for ticker, data in self.training_data.items():
            for record in data['history']:
                features = record['features']
                ai_results = record.get('ai_results', {})
                
                # استخراج الميزات
                accruals = features.get('accruals_ratio', 0)
                net_margin = features.get('net_margin', 0)
                ocf_margin = features.get('ocf_margin', 0)
                z_score = features.get('altman_z_score', 3)
                
                if accruals is not None and net_margin is not None:
                    X.append([
                        abs(accruals) if accruals else 0,
                        net_margin if net_margin else 0,
                        ocf_margin if ocf_margin else 0,
                        z_score if z_score else 3,
                        features.get('z_score_trend', 0)
                    ])
                    
                    # التسمية (labeling) - يمكن تحسينها لاحقاً
                    fraud_prob = ai_results.get('fraud_detection', {}).get('fraud_probability', 0)
                    y.append(1 if fraud_prob > 0.5 else 0)
        
        if len(X) < 10:
            return {'type': 'rule_based', 'samples': len(X)}
        
        X = np.array(X)
        y = np.array(y)
        
        # حساب أوزان جديدة بناءً على البيانات
        fraud_cases = np.sum(y == 1)
        safe_cases = np.sum(y == 0)
        
        # حساب متوسط الميزات للحالات الاحتيالية
        if fraud_cases > 0:
            fraud_features_mean = np.mean(X[y == 1], axis=0)
        else:
            fraud_features_mean = [0.08, 10, -5, 1.5, -0.5]  # defaults

        if hasattr(fraud_features_mean, "tolist"):
            fraud_features_mean_out = fraud_features_mean.tolist()
        else:
            fraud_features_mean_out = list(fraud_features_mean)

        model = {
            'type': 'statistical',
            'samples': len(X),
            'fraud_cases': int(fraud_cases),
            'safe_cases': int(safe_cases),
            'fraud_features_mean': fraud_features_mean_out,
            'thresholds': {
                'accruals_high': float(np.percentile(X[:, 0], 75)),
                'margin_net_divergence': float(np.percentile(X[:, 1] - X[:, 2], 75)),
                'z_score_low': float(np.percentile(X[:, 3], 25)),
            },
            'weights': self._calculate_feature_importance(X, y)
        }
        
        print(f"      ✅ تم التدريب على {len(X)} سجل ({fraud_cases} احتيال، {safe_cases} آمن)")
        return model
    
    def _train_failure_model(self) -> Dict:
        """تدريب نموذج التنبؤ بالتعثر"""
        print("   📉 تدريب نموذج التنبؤ بالتعثر...")
        
        X = []
        y = []
        
        for ticker, data in self.training_data.items():
            for record in data['history']:
                features = record['features']
                
                z_score = features.get('altman_z_score')
                debt_ebitda = features.get('net_debt_ebitda')
                interest_cov = features.get('interest_coverage')
                z_trend = features.get('z_score_trend', 0)
                
                if z_score is not None:
                    X.append([
                        z_score,
                        debt_ebitda if debt_ebitda else 3,
                        interest_cov if interest_cov else 3,
                        z_trend,
                        features.get('debt_trend', 0)
                    ])
                    
                    # التسمية: 1 = خطر تعثر، 0 = آمن
                    y.append(1 if z_score < 1.8 else 0)
        
        if len(X) < 10:
            return {'type': 'rule_based', 'samples': len(X)}
        
        X = np.array(X)
        y = np.array(y)
        
        model = {
            'type': 'statistical',
            'samples': len(X),
            'risk_cases': int(np.sum(y == 1)),
            'safe_cases': int(np.sum(y == 0)),
            'thresholds': {
                'z_score_critical': float(np.percentile(X[y == 1, 0], 75)) if np.sum(y == 1) > 0 else 1.8,
                'debt_high': float(np.percentile(X[:, 1], 75)),
                'coverage_low': float(np.percentile(X[:, 2], 25)),
            },
            'weights': self._calculate_feature_importance(X, y)
        }
        
        print(f"      ✅ تم التدريب على {len(X)} سجل")
        return model
    
    def _train_growth_model(self) -> Dict:
        """تدريب نموذج استدامة النمو"""
        print("   📈 تدريب نموذج استدامة النمو...")
        
        X = []
        y_scores = []
        
        for ticker, data in self.training_data.items():
            for record in data['history']:
                features = record['features']
                
                roic = features.get('roic')
                retention = features.get('retention_ratio')
                sgr = features.get('sgr_internal')
                
                if roic is not None and retention is not None:
                    X.append([
                        roic if roic else 10,
                        retention if retention else 0.7,
                        sgr if sgr else 0.05,
                        features.get('roe_trend', 0),
                        features.get('debt_trend', 0)
                    ])
                    
                    # نتيجة بناءً على ROIC و Retention
                    score = 0
                    if roic and roic > 15:
                        score += 40
                    elif roic and roic > 10:
                        score += 25
                    if retention and retention > 0.7:
                        score += 30
                    elif retention and retention > 0.5:
                        score += 15
                    
                    y_scores.append(score)
        
        if len(X) < 10:
            return {'type': 'rule_based', 'samples': len(X)}
        
        X = np.array(X)
        y_scores = np.array(y_scores)
        
        model = {
            'type': 'regression',
            'samples': len(X),
            'score_stats': {
                'mean': float(np.mean(y_scores)),
                'std': float(np.std(y_scores)),
                'median': float(np.median(y_scores)),
            },
            'thresholds': {
                'roic_excellent': float(np.percentile(X[:, 0], 75)),
                'retention_high': float(np.percentile(X[:, 1], 75)),
            }
        }
        
        print(f"      ✅ تم التدريب على {len(X)} سجل")
        return model
    
    def _train_liquidity_model(self) -> Dict:
        """تدريب نموذج تحليل رأس المال العامل"""
        print("   💰 تدريب نموذج رأس المال العامل...")
        
        X = []
        y = []
        
        for ticker, data in self.training_data.items():
            for record in data['history']:
                features = record['features']
                
                dso = features.get('days_sales_outstanding')
                current_ratio = features.get('current_ratio')
                quick_ratio = features.get('quick_ratio')
                
                if dso is not None and current_ratio is not None:
                    # حساب CCC تقريبي
                    inventory_days = 365 / features.get('inventory_turnover', 10) if features.get('inventory_turnover') else 30
                    ap_days = 60  # افتراضي
                    ccc = inventory_days + dso - ap_days
                    
                    X.append([
                        ccc,
                        dso,
                        current_ratio,
                        quick_ratio if quick_ratio else 1,
                        features.get('sgr_internal', 0.05)
                    ])
                    
                    # التسمية: 1 = خطر سيولة، 0 = آمن
                    y.append(1 if (ccc > 90 or current_ratio < 1.2) else 0)
        
        if len(X) < 10:
            return {'type': 'rule_based', 'samples': len(X)}
        
        X = np.array(X)
        y = np.array(y)
        
        model = {
            'type': 'statistical',
            'samples': len(X),
            'risk_cases': int(np.sum(y == 1)),
            'safe_cases': int(np.sum(y == 0)),
            'thresholds': {
                'ccc_high': float(np.percentile(X[:, 0], 75)),
                'dso_high': float(np.percentile(X[:, 1], 75)),
                'current_low': float(np.percentile(X[:, 2], 25)),
            }
        }
        
        print(f"      ✅ تم التدريب على {len(X)} سجل")
        return model
    
    def _train_quality_model(self) -> Dict:
        """تدريب نموذج جودة الاستثمار"""
        print("   ⭐ تدريب نموذج جودة الاستثمار...")
        
        X = []
        y_scores = []
        
        for ticker, data in self.training_data.items():
            for record in data['history']:
                features = record['features']
                ai_results = record.get('ai_results', {})
                
                roic = features.get('roic', 10)
                z_score = features.get('altman_z_score', 3)
                fcf_yield = features.get('fcf_yield', 0.03)
                pe = features.get('pe_ratio', 20)
                
                X.append([
                    roic,
                    z_score,
                    fcf_yield * 100 if fcf_yield and fcf_yield < 1 else fcf_yield if fcf_yield else 3,
                    pe if pe and pe > 0 else 20,
                    features.get('net_margin', 10)
                ])
                
                # استخراج النتيجة الفعلية إذا كانت موجودة
                quality_score = ai_results.get('investment_quality', {}).get('quality_score', 50)
                y_scores.append(quality_score)
        
        if len(X) < 10:
            return {'type': 'rule_based', 'samples': len(X)}
        
        X = np.array(X)
        y_scores = np.array(y_scores)
        
        model = {
            'type': 'regression',
            'samples': len(X),
            'score_stats': {
                'mean': float(np.mean(y_scores)),
                'std': float(np.std(y_scores)),
                'percentile_75': float(np.percentile(y_scores, 75)),
                'percentile_50': float(np.percentile(y_scores, 50)),
                'percentile_25': float(np.percentile(y_scores, 25)),
            },
            'feature_correlations': self._calculate_correlations(X, y_scores)
        }
        
        print(f"      ✅ تم التدريب على {len(X)} سجل")
        return model
    
    def _calculate_feature_importance(self, X: np.ndarray, y: np.ndarray) -> Dict:
        """حساب أهمية الميزات"""
        try:
            correlations = []
            for i in range(X.shape[1]):
                xi = X[:, i]
                if np.std(xi) == 0 or np.std(y) == 0:
                    corr = 0.0
                else:
                    corr = float(np.corrcoef(xi, y)[0, 1])
                    if np.isnan(corr) or np.isinf(corr):
                        corr = 0.0
                correlations.append(abs(corr))
            
            # Normalize to sum to 1
            total = sum(correlations)
            if total > 0:
                weights = [c / total for c in correlations]
            else:
                weights = [1.0 / len(correlations)] * len(correlations)
            
            return {f'feature_{i}': float(w) for i, w in enumerate(weights)}
        except:
            return {}
    
    def _calculate_correlations(self, X: np.ndarray, y: np.ndarray) -> Dict:
        """حساب الارتباطات"""
        try:
            correlations = {}
            for i in range(X.shape[1]):
                xi = X[:, i]
                if np.std(xi) == 0 or np.std(y) == 0:
                    corr = 0.0
                else:
                    corr = float(np.corrcoef(xi, y)[0, 1])
                    if np.isnan(corr) or np.isinf(corr):
                        corr = 0.0
                correlations[f'feature_{i}'] = corr
            return correlations
        except:
            return {}
    
    # ═══════════════════════════════════════════════════════════
    # التنبؤ باستخدام النماذج المدربة
    # ═══════════════════════════════════════════════════════════
    
    def predict_fraud_probability(self, features: Dict) -> float:
        """
        التنبؤ باحتمالية الاحتيال باستخدام النموذج المدرب
        """
        if not self.trained_models or 'fraud_model' not in self.trained_models:
            return None  # استخدام القواعد الافتراضية
        
        model = self.trained_models['fraud_model']
        if model['type'] == 'rule_based':
            return None
        
        # استخدام العتبات المتعلمة
        thresholds = model['thresholds']
        score = 0.0
        
        try:
            accruals_raw = features.get('accruals_ratio', 0)
            accruals = abs(float(accruals_raw)) if accruals_raw is not None else 0.0
        except Exception:
            accruals = 0.0
        accruals_high = thresholds.get('accruals_high', 0.08)
        try:
            accruals_high = float(accruals_high) if accruals_high is not None else 0.08
        except Exception:
            accruals_high = 0.08
        if accruals_high <= 0:
            accruals_high = 0.08
        if accruals > accruals_high:
            score += 0.35 * (accruals / accruals_high)
        
        net_margin = features.get('net_margin', 0)
        ocf_margin = features.get('ocf_margin', 0)
        try:
            net_margin = 0.0 if net_margin is None else float(net_margin)
        except Exception:
            net_margin = 0.0
        try:
            ocf_margin = 0.0 if ocf_margin is None else float(ocf_margin)
        except Exception:
            ocf_margin = 0.0
        margin_divergence = net_margin - ocf_margin
        margin_net_divergence = thresholds.get('margin_net_divergence', 10)
        try:
            margin_net_divergence = float(margin_net_divergence) if margin_net_divergence is not None else 10.0
        except Exception:
            margin_net_divergence = 10.0
        if margin_divergence > margin_net_divergence:
            score += 0.25
        
        z_score = features.get('altman_z_score', 3)
        try:
            z_score = 3.0 if z_score is None else float(z_score)
        except Exception:
            z_score = 3.0
        z_score_low = thresholds.get('z_score_low', 2)
        try:
            z_score_low = float(z_score_low) if z_score_low is not None else 2.0
        except Exception:
            z_score_low = 2.0
        if z_score < z_score_low:
            score += 0.20
        
        return min(score, 0.95)
    
    # ═══════════════════════════════════════════════════════════
    # حفظ وتحميل
    # ═══════════════════════════════════════════════════════════
    
    def _load_training_data(self) -> Dict:
        """تحميل البيانات التدريبية"""
        if os.path.exists(self.training_data_file):
            try:
                with open(self.training_data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_training_data(self):
        """حفظ البيانات التدريبية"""
        with open(self.training_data_file, 'w', encoding='utf-8') as f:
            json.dump(self.training_data, f, indent=2, ensure_ascii=False)
    
    def _load_models(self) -> Dict:
        """تحميل النماذج المدربة"""
        if os.path.exists(self.models_file):
            try:
                with open(self.models_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return {}
        return {}
    
    def _save_models(self):
        """حفظ النماذج المدربة"""
        with open(self.models_file, 'wb') as f:
            pickle.dump(self.trained_models, f)
    
    def _load_stats(self) -> Dict:
        """تحميل الإحصائيات"""
        if os.path.exists(self.stats_file):
            try:
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        
        return {
            'total_companies': 0,
            'total_records': 0,
            'last_update': None,
            'last_training': None,
            'models_trained': 0
        }
    
    def _save_stats(self):
        """حفظ الإحصائيات"""
        with open(self.stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2, ensure_ascii=False)
    
    def get_stats_summary(self) -> str:
        """الحصول على ملخص الإحصائيات"""
        return f"""
📊 إحصائيات التدريب:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
عدد الشركات: {self.stats['total_companies']}
عدد السجلات: {self.stats['total_records']}
آخر تحديث: {self.stats.get('last_update', 'N/A')}
آخر تدريب: {self.stats.get('last_training', 'لم يتم التدريب بعد')}
النماذج المدربة: {self.stats['models_trained']}
"""


# ═══════════════════════════════════════════════════════════
# دوال مساعدة للتكامل
# ═══════════════════════════════════════════════════════════

def initialize_training_system():
    """تهيئة نظام التدريب"""
    return AITrainingSystem()


def auto_train_if_needed(trainer: AITrainingSystem, threshold: int = 50):
    """
    تدريب تلقائي إذا وصل عدد السجلات للحد الأدنى
    """
    if trainer.stats['total_records'] >= threshold:
        last_training = trainer.stats.get('last_training')
        
        # تدريب إذا لم يتم التدريب من قبل أو مر وقت طويل
        if not last_training:
            print(f"🎓 تم الوصول للحد الأدنى ({threshold} سجل) - بدء التدريب التلقائي...")
            trainer.train_models()
            return True
    
    return False
