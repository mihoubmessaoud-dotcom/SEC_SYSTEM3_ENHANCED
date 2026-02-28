# -*- coding: utf-8 -*-
"""
main.py â€” ÙˆØ§Ø¬Ù‡Ø© Ø§Ù„Ù…Ø³ØªØ®Ø¯Ù… Ø§Ù„Ù…Ø¹Ø¯Ù„Ø© Ù„Ø±Ø¨Ø· Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ Ø£ÙˆØªÙˆÙ…Ø§ØªÙŠÙƒÙŠØ§Ù‹ ÙˆØ¥Ù†ØªØ§Ø¬ Ø¬Ø¯ÙˆÙ„ Ø§Ù„ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ø§Ø³ØªØ±Ø§ØªÙŠØ¬ÙŠ
Ø§Ù„ØªØ¹Ø¯ÙŠÙ„Ø§Øª:
- Ø²Ø± Ø­Ø°Ù Ø´Ø±ÙƒØ©/Ù…Ø³Ø­ Ø§Ù„ÙƒÙ„
- ØªØµØ¯ÙŠØ± Excel (Raw_by_Year, Ratios, Strategic per-year)
- Ø±Ø¨Ø· get_market_data Ù…Ù† fetcher Ù„Ù…Ù„Ø¡ Ø§Ù„Ø³Ø¹Ø±/Ø¹Ø¯Ø¯ Ø§Ù„Ø£Ø³Ù‡Ù… ØªÙ„Ù‚Ø§Ø¦ÙŠÙ‹Ø§
- Ø§Ù„Ø¹Ø±Ø¶ Ù…Ù‚ØªØµØ± Ø¹Ù„Ù‰ Ù†Ø·Ø§Ù‚ Ø§Ù„Ø³Ù†ÙˆØ§Øª (start..end)
"""

import os
import sys
import threading
import time
import json
import csv
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk, messagebox, filedialog

import matplotlib
matplotlib.use('TkAgg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from modules.ratio_formats import canonicalize_ratio_value, format_ratio_value
from modules.ratio_source import UnifiedRatioSource, maybe_guard_ratios_by_year

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from modules.sec_fetcher import SECDataFetcher
except Exception as e:
    tk.messagebox.showerror("Import Error", f"ÙØ´Ù„ Ø§Ø³ØªÙŠØ±Ø§Ø¯ modules.sec_fetcher: {e}")
    raise

PALETTE = {
    'bg': '#eef2f7',
    'panel': '#ffffff',
    'header': '#0f2747',
    'accent': '#0b6efd',
    'muted': '#5b6675',
    'button': '#0b6efd',
    'border': '#d9e1ec',
    'success': '#198754',
    'warning': '#f59e0b',
}

FONTS = {
    'header': ('Segoe UI', 22, 'bold'),
    'title': ('Segoe UI', 14, 'bold'),
    'label': ('Segoe UI', 11, 'bold'),
    'normal': ('Segoe UI', 10),
    'button': ('Segoe UI', 11, 'bold'),
    'tree': ('Segoe UI', 10)
}


class SECFinancialSystem:
    def __init__(self, root):
        self.root = root
        self._init_language_pack()
        self._install_tk_text_decoder()
        self.root.title("منصة التحليل المالي الذكي - SEC")
        self.root.geometry("1600x950")
        self.root.minsize(1360, 860)
        self.root.configure(bg=PALETTE['bg'])
        self.fetcher = None
        self.current_data = None
        self.multi_company_data = {}
        self._ratio_row_meta = {}
        self._ratio_years = []
        self._configure_typography()
        self.translate_technical_var = tk.BooleanVar(master=self.root, value=False)
        
        # âœ… NEW: Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¯Ø±ÙŠØ¨ Ø§Ù„Ù…Ø³ØªÙ…Ø±
        self.ml_trainer = None
        
        self._init_ui()
        self._apply_professional_theme()
        self._apply_ui_text_fixes()
        try:
            self.fetcher = SECDataFetcher()
            
            # ØªÙ‡ÙŠØ¦Ø© Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¯Ø±ÙŠØ¨
            try:
                from modules.ml_trainer import initialize_training_system, auto_train_if_needed
                self.ml_trainer = initialize_training_system()
                print("âœ… Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¯Ø±ÙŠØ¨ Ø§Ù„Ù…Ø³ØªÙ…Ø± Ø¬Ø§Ù‡Ø²")
                print(self.ml_trainer.get_stats_summary())
                
                # Ù…Ø­Ø§ÙˆÙ„Ø© ØªØ¯Ø±ÙŠØ¨ ØªÙ„Ù‚Ø§Ø¦ÙŠ Ø¥Ø°Ø§ ÙƒØ§Ù† Ù‡Ù†Ø§Ùƒ Ø¨ÙŠØ§Ù†Ø§Øª ÙƒØ§ÙÙŠØ©
                auto_train_if_needed(self.ml_trainer, threshold=20)
            except Exception as e:
                print(f"âš ï¸ ØªØ¹Ø°Ø± ØªÙ‡ÙŠØ¦Ø© Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¯Ø±ÙŠØ¨: {e}")
                
        except Exception as e:
            messagebox.showerror("Ø®Ø·Ø£", f"ÙØ´Ù„ ØªÙ‡ÙŠØ¦Ø© Ø¬Ø§Ù„Ø¨ SEC: {e}")

    def _init_ui(self):
        header = tk.Frame(self.root, bg=PALETTE['header'], height=70)
        header.pack(fill='x')
        header.pack_propagate(False)
        self.header_title_label = tk.Label(
            header,
            text=self._t('app_header'),
            font=FONTS['header'],
            bg=PALETTE['header'],
            fg='white',
        )
        self.header_title_label.pack(side='left', pady=15, padx=14)
        lang_box = tk.Frame(header, bg=PALETTE['header'])
        lang_box.pack(side='right', padx=14)
        self.lang_label = tk.Label(
            lang_box,
            text=self._t('lang_label'),
            font=FONTS['label'],
            bg=PALETTE['header'],
            fg='white',
        )
        self.lang_label.pack(side='left', padx=(0, 6))
        self.lang_combo = ttk.Combobox(
            lang_box,
            textvariable=self._lang_choice_var,
            state='readonly',
            values=[lbl for lbl, _ in self._lang_options],
            width=12,
        )
        self.lang_combo.pack(side='left')
        self.lang_combo.bind('<<ComboboxSelected>>', self._on_language_changed)

        main = tk.Frame(self.root, bg=PALETTE['bg'])
        main.pack(fill='both', expand=True, padx=12, pady=12)

        left = tk.Frame(main, bg=PALETTE['panel'], width=420)
        left.pack(side='left', fill='y', padx=(0, 10))
        left.pack_propagate(False)
        self._build_left(left)

        right = tk.Frame(main, bg=PALETTE['panel'])
        right.pack(side='left', fill='both', expand=True)
        self._build_right(right)
        self._build_ai_analysis_tab()  # âœ… Ø¨Ù†Ø§Ø¡ ØªØ¨ÙˆÙŠØ¨ Ø§Ù„ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ø°ÙƒÙŠ

    def _select_font_family(self, preferred_families):
        try:
            installed = {f.lower(): f for f in tkfont.families(self.root)}
        except Exception:
            installed = {}
        for fam in preferred_families:
            hit = installed.get(str(fam).lower())
            if hit:
                return hit
        return "Segoe UI"

    def _configure_typography(self):
        arabic_family = self._select_font_family([
            "Cairo",
            "Noto Naskh Arabic",
            "Noto Sans Arabic",
            "IBM Plex Sans Arabic",
            "Tahoma",
            "Segoe UI",
            "Arial",
        ])
        mono_family = self._select_font_family(["JetBrains Mono", "Consolas", "Courier New"])
        FONTS.update({
            'header': (arabic_family, 22, 'bold'),
            'title': (arabic_family, 14, 'bold'),
            'label': (arabic_family, 11, 'bold'),
            'normal': (arabic_family, 10),
            'button': (arabic_family, 11, 'bold'),
            'tree': (arabic_family, 10),
            'mono': (mono_family, 9),
        })
        self.root.option_add("*Font", f"{arabic_family} 10")

    def _init_language_pack(self):
        self.current_lang = 'ar'
        self._lang_options = [
            ('العربية', 'ar'),
            ('English', 'en'),
            ('Français', 'fr'),
        ]
        self._lang_choice_var = tk.StringVar(master=self.root, value='العربية')
        self._i18n = {
            'app_title': {
                'ar': 'منصة التحليل المالي الذكي - SEC',
                'en': 'Smart Financial Intelligence Platform - SEC',
                'fr': 'Plateforme intelligente d\'analyse financière - SEC',
            },
            'app_header': {
                'ar': 'منصة التحليل المالي الذكي - معايير SEC',
                'en': 'Smart Financial Intelligence Platform - SEC Standards',
                'fr': 'Plateforme intelligente d\'analyse financière - Normes SEC',
            },
            'input_data': {
                'ar': 'بيانات الإدخال',
                'en': 'Input Data',
                'fr': 'Données d\'entrée',
            },
            'company_symbols': {
                'ar': 'رمز/رموز الشركة (مفصولة بفواصل):',
                'en': 'Company ticker(s) (comma-separated):',
                'fr': 'Symbole(s) boursier(s) (séparés par des virgules) :',
            },
            'add': {'ar': 'إضافة', 'en': 'Add', 'fr': 'Ajouter'},
            'companies_list': {'ar': 'قائمة الشركات:', 'en': 'Company List:', 'fr': 'Liste des sociétés :'},
            'remove_selected': {'ar': 'حذف المحدد', 'en': 'Remove Selected', 'fr': 'Supprimer la sélection'},
            'clear_all': {'ar': 'مسح الكل', 'en': 'Clear All', 'fr': 'Tout effacer'},
            'period_range': {
                'ar': 'الفترة (سنة البداية - سنة النهاية):',
                'en': 'Period (Start Year - End Year):',
                'fr': 'Période (année début - année fin) :',
            },
            'to_sep': {'ar': ' إلى ', 'en': ' to ', 'fr': ' à '},
            'filing_type': {'ar': 'نوع التقرير:', 'en': 'Filing Type:', 'fr': 'Type de déclaration :'},
            'annual_10k': {'ar': '10-K (سنوي)', 'en': '10-K (Annual)', 'fr': '10-K (Annuel)'},
            'fetch_data': {'ar': 'التحميل الآلي للبيانات', 'en': 'Auto Data Loading', 'fr': 'Chargement automatique des données'},
            'filing_supported': {
                'ar': 'الأنواع المدعومة حالياً: 10-K',
                'en': 'Currently supported: 10-K',
                'fr': 'Actuellement pris en charge : 10-K',
            },
            'filing_warn_unsupported': {
                'ar': 'النوع المحدد غير مدعوم حالياً في الوضع المباشر. سيتم استخدام 10-K تلقائياً.',
                'en': 'Selected filing type is not supported in direct mode. 10-K will be used automatically.',
                'fr': 'Le type de déclaration sélectionné n’est pas pris en charge en mode direct. 10-K sera utilisé automatiquement.',
            },
            'filing_10k': {'ar': '10-K (سنوي)', 'en': '10-K (Annual)', 'fr': '10-K (Annuel)'},
            'filing_10ka': {'ar': '10-K/A (تعديل)', 'en': '10-K/A (Amendment)', 'fr': '10-K/A (Amendement)'},
            'filing_20f': {'ar': '20-F (دولي)', 'en': '20-F (International)', 'fr': '20-F (International)'},
            'market_inputs': {'ar': 'مدخلات السوق (اختياري)', 'en': 'Market Inputs (Optional)', 'fr': 'Entrées de marché (optionnel)'},
            'current_price': {'ar': 'سعر السهم الحالي (USD):', 'en': 'Current Share Price (USD):', 'fr': 'Cours actuel de l\'action (USD) :'},
            'shares_outstanding': {'ar': 'عدد الأسهم المصدرة:', 'en': 'Shares Outstanding:', 'fr': 'Actions en circulation :'},
            'cost_of_debt': {'ar': 'تكلفة الدين الفعلية (%):', 'en': 'Effective Cost of Debt (%):', 'fr': 'Coût effectif de la dette (%) :'},
            'tab_raw': {'ar': 'البيانات المالية', 'en': 'Financial Data', 'fr': 'Données financières'},
            'tab_ratios': {'ar': 'النسب المالية', 'en': 'Financial Ratios', 'fr': 'Ratios financiers'},
            'tab_strategic': {'ar': 'التحليل الاستراتيجي', 'en': 'Strategic Analysis', 'fr': 'Analyse stratégique'},
            'tab_comparison': {'ar': 'التحليل المقارن', 'en': 'Comparative Analysis', 'fr': 'Analyse comparative'},
            'tab_forecast': {'ar': 'التوقعات', 'en': 'Forecasts', 'fr': 'Prévisions'},
            'tab_ai': {'ar': 'التحليل الذكي AI', 'en': 'AI Intelligence', 'fr': 'Intelligence IA'},
            'layer_view': {'ar': 'عرض الطبقة:', 'en': 'Layer View:', 'fr': 'Vue par couche :'},
            'sec_view_mode_label': {'ar': 'وضع العرض:', 'en': 'View Mode:', 'fr': 'Mode d\'affichage :'},
            'sec_view_mode_official': {'ar': 'عرض SEC الرسمي', 'en': 'Official SEC View', 'fr': 'Vue SEC officielle'},
            'sec_view_mode_canonical': {'ar': 'عرض تحليلي موحّد', 'en': 'Canonical Analysis View', 'fr': 'Vue analytique canonique'},
            'tool_export': {'ar': 'تصدير نتائج إكسل (التحليل الحالي)', 'en': 'Export Excel (Current Analysis)', 'fr': 'Exporter Excel (analyse courante)'},
            'tool_load': {'ar': 'تحميل بيانات أو نتائج إكسل', 'en': 'Load Excel Data/Results', 'fr': 'Charger données/résultats Excel'},
            'tool_export_compact': {'ar': 'تصدير إكسل', 'en': 'Export Excel', 'fr': 'Exporter Excel'},
            'tool_load_compact': {'ar': 'تحميل إكسل', 'en': 'Load Excel', 'fr': 'Charger Excel'},
            'tool_plot': {'ar': 'فتح الرسم البياني', 'en': 'Open Chart', 'fr': 'Ouvrir le graphique'},
            'translate_technical_labels': {
                'ar': 'ترجمة الأسماء التقنية',
                'en': 'Translate Technical Labels',
                'fr': 'Traduire les noms techniques',
            },
            'lang_label': {'ar': '🌐 اللغة', 'en': '🌐 Language', 'fr': '🌐 Langue'},
            'msg_info': {'ar': 'معلومة', 'en': 'Information', 'fr': 'Information'},
            'msg_warning': {'ar': 'تحذير', 'en': 'Warning', 'fr': 'Avertissement'},
            'msg_error': {'ar': 'خطأ', 'en': 'Error', 'fr': 'Erreur'},
            'msg_success': {'ar': 'نجاح', 'en': 'Success', 'fr': 'Succès'},
            'msg_confirm': {'ar': 'تأكيد', 'en': 'Confirm', 'fr': 'Confirmer'},
            'progress_done': {'ar': 'اكتمل جلب الشركات', 'en': 'Company fetch completed', 'fr': 'Récupération des sociétés terminée'},
            'summary_prefix': {'ar': '🏢', 'en': '🏢', 'fr': '🏢'},
            'sec_direct_label': {'ar': 'SEC الرسمي 10-K (مباشر)', 'en': 'SEC Official 10-K (Direct)', 'fr': 'SEC officiel 10-K (Direct)'},
            'layer1': {'ar': 'الطبقة 1 - SEC XBRL (EDGAR)', 'en': 'Layer 1 - SEC XBRL (EDGAR)', 'fr': 'Couche 1 - SEC XBRL (EDGAR)'},
            'layer2': {'ar': 'الطبقة 2 - السوق (Polygon)', 'en': 'Layer 2 - Market (Polygon)', 'fr': 'Couche 2 - Marché (Polygon)'},
            'layer3': {'ar': 'الطبقة 3 - الاقتصاد الكلي (FRED)', 'en': 'Layer 3 - Macro (FRED)', 'fr': 'Couche 3 - Macroéconomie (FRED)'},
            'layer4': {'ar': 'الطبقة 4 - Yahoo (yfinance)', 'en': 'Layer 4 - Yahoo (yfinance)', 'fr': 'Couche 4 - Yahoo (yfinance)'},
            'load_excel_title': {'ar': 'تحميل بيانات أو نتائج من Excel', 'en': 'Load Data or Results from Excel', 'fr': 'Charger des données ou résultats depuis Excel'},
            'load_excel_success': {'ar': 'تم تحميل البيانات/النتائج بنجاح من ملف Excel.', 'en': 'Excel data/results loaded successfully.', 'fr': 'Données/résultats Excel chargés avec succès.'},
            'load_excel_failed': {'ar': 'فشل تحميل ملف Excel', 'en': 'Failed to load Excel file', 'fr': 'Échec du chargement du fichier Excel'},
            'ai_header': {'ar': '🤖 التحليل الذكي المتقدم - AI Analysis', 'en': '🤖 Advanced AI Analysis', 'fr': '🤖 Analyse IA avancée'},
            'ai_desc': {
                'ar': 'تحليل ذكي متقدم يستخدم خوارزميات مالية لاكتشاف المخاطر وتقييم جودة الاستثمار',
                'en': 'Advanced AI analysis using financial algorithms to detect risk and evaluate investment quality',
                'fr': 'Analyse IA avancée utilisant des algorithmes financiers pour détecter les risques et évaluer la qualité d\'investissement',
            },
            'ai_fraud_frame': {'ar': '🚨 1. مؤشر احتمالية الاحتيال (AI Fraud Probability)', 'en': '🚨 1. AI Fraud Probability', 'fr': '🚨 1. Probabilité de fraude (IA)'},
            'ai_fraud_prob': {'ar': 'احتمالية الاحتيال: --', 'en': 'Fraud Probability: --', 'fr': 'Probabilité de fraude : --'},
            'ai_fraud_flags': {'ar': 'عدد العلامات الحمراء: --', 'en': 'Red Flags Count: --', 'fr': 'Nombre de signaux d\'alerte : --'},
            'ai_risk_level': {'ar': 'مستوى المخاطر: --', 'en': 'Risk Level: --', 'fr': 'Niveau de risque : --'},
            'ai_recommendation': {'ar': 'التوصية: --', 'en': 'Recommendation: --', 'fr': 'Recommandation : --'},
            'ai_fraud_hint': {
                'ar': '💡 يحلل العلاقة بين الأرباح المحاسبية والتدفقات النقدية لكشف التجميل المالي',
                'en': '💡 Analyzes the relationship between accounting profit and cash flows to detect earnings manipulation',
                'fr': '💡 Analyse la relation entre résultat comptable et flux de trésorerie pour détecter les manipulations',
            },
            'ai_failure_frame': {'ar': '📉 2. التنبؤ بالتعثر المتقدم (Dynamic Failure Prediction)', 'en': '📉 2. Dynamic Failure Prediction', 'fr': '📉 2. Prévision dynamique de défaillance'},
            'ai_failure_3y': {'ar': 'احتمالية التعثر خلال 3 سنوات: --', 'en': 'Default Probability over 3 years: --', 'fr': 'Probabilité de défaut sur 3 ans : --'},
            'ai_failure_5y': {'ar': 'احتمالية التعثر خلال 5 سنوات: --', 'en': 'Default Probability over 5 years: --', 'fr': 'Probabilité de défaut sur 5 ans : --'},
            'ai_main_concerns': {'ar': 'المخاوف الرئيسية: --', 'en': 'Primary Concerns: --', 'fr': 'Préoccupations majeures : --'},
            'ai_failure_hint': {
                'ar': '💡 يحلل اتجاهات Z-Score والديون لرصد التدهور المالي قبل وقوعه',
                'en': '💡 Tracks Z-Score and debt trends to detect financial deterioration early',
                'fr': '💡 Suit les tendances du Z-Score et de la dette pour anticiper la détérioration financière',
            },
            'ai_growth_frame': {'ar': '📈 3. درجة استدامة النمو (Growth Sustainability Grade)', 'en': '📈 3. Growth Sustainability Grade', 'fr': '📈 3. Score de durabilité de croissance'},
            'ai_score': {'ar': 'النتيجة: -- / 100', 'en': 'Score: -- / 100', 'fr': 'Score : -- / 100'},
            'ai_grade': {'ar': 'الدرجة: --', 'en': 'Grade: --', 'fr': 'Niveau : --'},
            'ai_assessment': {'ar': 'التقييم: --', 'en': 'Assessment: --', 'fr': 'Évaluation : --'},
            'ai_growth_hint': {
                'ar': '💡 يقيم قدرة الشركة على تحقيق النمو بدون زيادة خطيرة في الديون',
                'en': '💡 Evaluates the company’s ability to grow without excessive leverage',
                'fr': '💡 Évalue la capacité de croissance sans hausse excessive de l’endettement',
            },
            'ai_wc_frame': {'ar': '💰 4. تحليل رأس المال العامل الذكي (AI Working Capital Analysis)', 'en': '💰 4. AI Working Capital Analysis', 'fr': '💰 4. Analyse IA du besoin en fonds de roulement'},
            'ai_wc_crisis': {'ar': 'احتمالية أزمة سيولة: --', 'en': 'Liquidity Crisis Probability: --', 'fr': 'Probabilité de crise de liquidité : --'},
            'ai_wc_ccc': {'ar': 'دورة التحويل النقدي الحالية: --', 'en': 'Current Cash Conversion Cycle: --', 'fr': 'Cycle de conversion de trésorerie actuel : --'},
            'ai_wc_trend': {'ar': 'الاتجاه: --', 'en': 'Trend: --', 'fr': 'Tendance : --'},
            'ai_wc_hint': {
                'ar': '💡 يتنبأ بالاختناقات في السيولة النقدية رغم وجود أرباح',
                'en': '💡 Forecasts cash bottlenecks even when accounting profits are positive',
                'fr': '💡 Anticipe les tensions de trésorerie malgré des bénéfices comptables positifs',
            },
            'ai_quality_frame': {'ar': '⭐ 5. تقييم جودة الاستثمار النهائي (AI Investment Quality Score)', 'en': '⭐ 5. AI Investment Quality Score', 'fr': '⭐ 5. Score IA de qualité d\'investissement'},
            'ai_quality_score': {'ar': 'النتيجة النهائية: -- / 100', 'en': 'Final Score: -- / 100', 'fr': 'Score final : -- / 100'},
            'ai_verdict': {'ar': 'الحكم: --', 'en': 'Verdict: --', 'fr': 'Verdict : --'},
            'ai_invest_action': {'ar': 'التوصية الاستثمارية: --', 'en': 'Investment Action: --', 'fr': 'Action d\'investissement : --'},
            'ai_percentile': {'ar': 'أفضل من: --% من الشركات', 'en': 'Better than: --% of companies', 'fr': 'Meilleur que : --% des sociétés'},
            'ai_quality_hint': {
                'ar': '💡 تقييم شامل يجمع Economic Spread و FCF Yield و ROIC و Z-Score',
                'en': '💡 Composite score based on Economic Spread, FCF Yield, ROIC, and Z-Score',
                'fr': '💡 Score composite basé sur Economic Spread, FCF Yield, ROIC et Z-Score',
            },
            'ai_btn_refresh': {'ar': '🔄 إعادة حساب التحليل الذكي', 'en': '🔄 Recalculate AI Analysis', 'fr': '🔄 Recalculer l\'analyse IA'},
            'ai_btn_train': {'ar': '🎓 تدريب النماذج الآن', 'en': '🎓 Train Models Now', 'fr': '🎓 Entraîner les modèles'},
            'ai_btn_stats': {'ar': '📊 إحصائيات التدريب', 'en': '📊 Training Statistics', 'fr': '📊 Statistiques d\'entraînement'},
            'raw_col_item': {'ar': 'البند', 'en': 'Item', 'fr': 'Poste'},
            'raw_col_unit': {'ar': 'الوحدة', 'en': 'Unit', 'fr': 'Unité'},
            'raw_col_source': {'ar': 'المصدر', 'en': 'Source', 'fr': 'Source'},
            'raw_col_category': {'ar': 'التصنيف', 'en': 'Category', 'fr': 'Catégorie'},
            'raw_col_normalized': {'ar': 'البند المعياري', 'en': 'Normalized Item', 'fr': 'Poste normalisé'},
            'ratio_col_name': {'ar': 'النسبة', 'en': 'Ratio', 'fr': 'Ratio'},
            'ratio_col_explanation': {'ar': 'تفسير', 'en': 'Explanation', 'fr': 'Explication'},
            'strategic_col_metric': {'ar': 'المقياس', 'en': 'Metric', 'fr': 'Indicateur'},
        }
        self._i18n_reverse = {}
        for key, langs in self._i18n.items():
            for val in langs.values():
                norm = re.sub(r'\s+', ' ', str(val)).strip()
                if norm:
                    self._i18n_reverse[norm] = key
        self._init_financial_term_translations()

    def _normalize_term_for_lookup(self, text: str) -> str:
        s = self._decode_mojibake_text(str(text or ""))
        s = s.replace('\u200f', '').replace('\u200e', '')
        s = s.replace('—', '-').replace('–', '-').replace('_', ' ')
        s = re.sub(r'\s+', ' ', s).strip().lower()
        return s

    def _init_financial_term_translations(self):
        self._term_i18n = {}
        self._term_reverse_to_en = {}
        self._ratio_expl_i18n = {}

        def add_term(source, ar, en=None, fr=None):
            en_val = en if en is not None else source
            fr_val = fr if fr is not None else en_val
            self._term_i18n[self._normalize_term_for_lookup(source)] = {
                'ar': ar,
                'en': en_val,
                'fr': fr_val,
            }

        # Core financial statement terms
        add_term('Line Item', 'البند')
        add_term('Item', 'البند')
        add_term('Unit', 'الوحدة')
        add_term('Source', 'المصدر')
        add_term('Category', 'التصنيف')
        add_term('Normalized Item', 'البند المعياري')
        add_term('Revenue', 'الإيرادات')
        add_term('Revenues', 'الإيرادات')
        add_term('SalesRevenueNet', 'صافي إيرادات المبيعات')
        add_term('CostOfRevenue', 'تكلفة الإيرادات')
        add_term('GrossProfit', 'إجمالي الربح')
        add_term('OperatingIncomeLoss', 'الدخل التشغيلي (الخسارة)')
        add_term('NetIncomeLoss', 'صافي الدخل (الخسارة)')
        add_term('AssetsCurrent', 'الأصول المتداولة')
        add_term('LiabilitiesCurrent', 'الخصوم المتداولة')
        add_term('StockholdersEquity', 'حقوق المساهمين')
        add_term('AccountsReceivableNetCurrent', 'الذمم المدينة - صافي')
        add_term('AccountsPayableCurrent', 'الذمم الدائنة')
        add_term('InventoryNet', 'المخزون الصافي')
        add_term('CashAndCashEquivalentsAtCarryingValue', 'النقد وما يعادله')
        add_term('Gross margin', 'الهامش الإجمالي')
        add_term('Net revenue', 'صافي الإيرادات')
        add_term('Net sales', 'صافي المبيعات')
        add_term('Cost of sales', 'تكلفة المبيعات')
        add_term('Cost of revenue', 'تكلفة الإيرادات')
        add_term('Products', 'المنتجات')
        add_term('Marketable securities', 'الأوراق المالية القابلة للتداول')
        add_term('Vendor non-trade receivables', 'ذمم مدينة غير تجارية من الموردين')
        add_term('Gross profit', 'إجمالي الربح')
        add_term('Operating expenses', 'المصروفات التشغيلية')
        add_term('Total operating expenses', 'إجمالي المصروفات التشغيلية')
        add_term('Operating expenses:', 'المصروفات التشغيلية:')
        add_term('Operating income (loss)', 'الدخل التشغيلي (الخسارة)')
        add_term('Operating income', 'الدخل التشغيلي')
        add_term('Other income/(expense), net', 'إيرادات/(مصروفات) أخرى - صافي')
        add_term('Income before provision for income taxes', 'الدخل قبل مخصص ضرائب الدخل')
        add_term('Provision for income taxes', 'مخصص ضرائب الدخل')
        add_term('Net income (loss)', 'صافي الدخل (الخسارة)')
        add_term('Net income', 'صافي الدخل')
        add_term('Income (loss) before taxes', 'الدخل (الخسارة) قبل الضرائب')
        add_term('Provision for (benefit from) taxes', 'مخصص (منفعة) الضرائب')
        add_term('Research and development', 'البحث والتطوير')
        add_term('Selling, general and administrative', 'البيع والعمومية والإدارية')
        add_term('Marketing, general, and administrative', 'التسويق والعمومية والإدارية')
        add_term('Restructuring and other charges', 'تكاليف إعادة الهيكلة وبنود أخرى')
        add_term('Assets', 'إجمالي الأصول')
        add_term('Total assets', 'إجمالي الأصول')
        add_term('Total current assets', 'إجمالي الأصول المتداولة')
        add_term('Current assets', 'الأصول المتداولة')
        add_term('Current liabilities', 'الخصوم المتداولة')
        add_term('Total liabilities', 'إجمالي الخصوم')
        add_term('Liabilities', 'إجمالي الخصوم')
        add_term('Stockholders equity', 'حقوق المساهمين')
        add_term('Total equity', 'إجمالي حقوق الملكية')
        add_term('Cash and cash equivalents', 'النقد وما يعادله')
        add_term('Short-term investments', 'الاستثمارات قصيرة الأجل')
        add_term('Accounts receivable, net', 'الذمم المدينة - صافي')
        add_term('Accounts payable', 'الذمم الدائنة')
        add_term('Inventories', 'المخزون')
        add_term('Property, plant and equipment, net', 'الممتلكات والمعدات - صافي')
        add_term('Goodwill', 'الشهرة')
        add_term('Identified intangible assets, net', 'الأصول غير الملموسة المحددة - صافي')
        add_term('Other current assets', 'أصول متداولة أخرى')
        add_term('Other long-term assets', 'أصول طويلة الأجل أخرى')
        add_term('Temporary equity', 'حقوق ملكية مؤقتة')
        add_term('Basic (shares)', 'الأساسي (عدد الأسهم)')
        add_term('Diluted (shares)', 'المخفف (عدد الأسهم)')
        add_term('Basic (in shares)', 'أساسي (بالأسهم)')
        add_term('Diluted (in shares)', 'مخفف (بالأسهم)')
        add_term('Basic (in dollars per share)', 'أساسي (بالدولار لكل سهم)')
        add_term('Diluted (in dollars per share)', 'مخفف (بالدولار لكل سهم)')
        add_term('Earnings per share:', 'ربحية السهم:')
        add_term('Shares used in computing earnings per share', 'الأسهم المستخدمة في احتساب ربحية السهم')
        add_term('Shares used in computing earnings per share:', 'الأسهم المستخدمة في احتساب ربحية السهم:')
        add_term('Weighted average shares of common stock outstanding:', 'المتوسط المرجح لأسهم الأسهم العادية القائمة:')
        add_term('Income Statement [Abstract]', 'قائمة الدخل [ملخص]')
        add_term('Current liabilities:', 'الخصوم المتداولة:')
        add_term('Current assets:', 'الأصول المتداولة:')
        add_term('Non-current assets:', 'الأصول غير المتداولة:')
        add_term('Non-current liabilities:', 'الخصوم غير المتداولة:')
        add_term('Other non-current assets', 'أصول غير متداولة أخرى')
        add_term('Other current liabilities', 'خصوم متداولة أخرى')

        # Raw concept names / canonical labels
        add_term('Operating Cash Flow', 'التدفق النقدي التشغيلي')
        add_term('Investing Cash Flow', 'التدفق النقدي الاستثماري')
        add_term('Financing Cash Flow', 'التدفق النقدي التمويلي')
        add_term('Capital Expenditures', 'النفقات الرأسمالية')
        add_term('Depreciation and Amortization', 'الاستهلاك والإطفاء')
        add_term('Interest Expense', 'مصروف الفائدة')
        add_term('Basic (in shares)', 'أساسي (بالأسهم)')
        add_term('Shares outstanding', 'الأسهم القائمة')

        # Ratio group headers
        add_term('Profitability Ratios', 'نسب الربحية')
        add_term('Activity & Efficiency Ratios', 'نسب النشاط والكفاءة')
        add_term('Liquidity Ratios', 'نسب السيولة')
        add_term('Solvency Ratios', 'نسب الملاءة')
        add_term('Market Ratios', 'نسب السوق')
        add_term('Valuation & Capital Ratios', 'نسب التقييم ورأس المال')
        add_term('Safety & Risk Ratios', 'نسب الأمان والمخاطر')
        add_term('Cash Flow Ratios', 'نسب التدفقات النقدية')
        add_term('Banking Core Ratios', 'نسب البنوك الأساسية')
        add_term('Banking Profitability & Solvency', 'ربحية وملاءة البنوك')
        add_term('Banking Market Ratios', 'نسب السوق للبنوك')
        add_term('Insurance Core Ratios', 'نسب التأمين الأساسية')
        add_term('Insurance Market Ratios', 'نسب السوق لقطاع التأمين')

        # Common ratio names
        add_term('Gross Profit Margin', 'هامش إجمالي الربح')
        add_term('Operating Profit Margin', 'هامش الربح التشغيلي')
        add_term('Net Profit Margin', 'هامش صافي الربح')
        add_term('EBITDA Margin', 'هامش EBITDA')
        add_term('ROA (Return on Assets)', 'العائد على الأصول (ROA)')
        add_term('ROE (Return on Equity)', 'العائد على حقوق الملكية (ROE)')
        add_term('ROIC (Return on Invested Capital)', 'العائد على رأس المال المستثمر (ROIC)')
        add_term('Current Ratio', 'نسبة التداول')
        add_term('Quick Ratio', 'النسبة السريعة')
        add_term('Cash Ratio', 'النسبة النقدية')
        add_term('Debt-to-Equity', 'الدين إلى حقوق الملكية')
        add_term('Debt-to-Assets', 'الدين إلى الأصول')
        add_term('Interest Coverage Ratio', 'نسبة تغطية الفائدة')
        add_term('Net Debt / EBITDA', 'صافي الدين إلى EBITDA')
        add_term('P/E Ratio', 'مكرر الربحية (P/E)')
        add_term('P/B Ratio', 'مكرر القيمة الدفترية (P/B)')
        add_term('Dividend Yield', 'عائد التوزيعات')
        add_term('EPS (Earnings Per Share)', 'ربحية السهم (EPS)')
        add_term('Book Value Per Share', 'القيمة الدفترية للسهم')
        add_term('Market Cap (Million USD)', 'القيمة السوقية (مليون دولار)')
        add_term('Enterprise Value (Million USD)', 'قيمة المنشأة (مليون دولار)')
        add_term('EV/EBITDA', 'مضاعف EV/EBITDA')
        add_term('WACC', 'متوسط تكلفة رأس المال المرجّح')
        add_term('FCF Yield', 'عائد التدفق النقدي الحر')
        add_term('Operating Cash Flow Margin', 'هامش التدفق النقدي التشغيلي')
        add_term('Free Cash Flow', 'التدفق النقدي الحر')
        add_term('FCF Per Share', 'التدفق النقدي الحر لكل سهم')

        # Strategic section/group labels
        add_term('--- Strategic & Value Tier ---', '--- شريحة الاستراتيجية والقيمة ---')
        add_term('--- Quality & Risk Tier ---', '--- شريحة الجودة والمخاطر ---')
        add_term('--- Performance Analysis Tier ---', '--- شريحة تحليل الأداء ---')
        add_term('--- Operational Efficiency Tier ---', '--- شريحة الكفاءة التشغيلية ---')
        add_term('--- Market Valuation Tier ---', '--- شريحة تقييم السوق ---')
        add_term('--- Banking Strategic Tier ---', '--- الشريحة الاستراتيجية للبنوك ---')
        add_term('--- Banking Market Tier ---', '--- شريحة السوق للبنوك ---')
        add_term('--- Insurance Strategic Tier ---', '--- الشريحة الاستراتيجية للتأمين ---')
        add_term('--- Insurance Market Tier ---', '--- شريحة السوق للتأمين ---')

        # Strategic metric display names
        add_term('Fair_Value_Estimate (per share)', 'القيمة العادلة المقدّرة (للسهم)')
        add_term('Investment_Score (0-100)', 'درجة الاستثمار (0-100)')
        add_term('Economic_Spread (ROIC - WACC)', 'الفارق الاقتصادي (ROIC - WACC)')
        add_term('Altman_Z_Score', 'درجة Altman Z')
        add_term('Warning_Signal', 'إشارة التحذير')
        add_term('Accruals_Ratio', 'نسبة الاستحقاقات')
        add_term('Accruals_Change', 'تغير الاستحقاقات')
        add_term('Credit_Rating', 'التصنيف الائتماني')
        add_term('Credit_Rating_Score', 'درجة التصنيف الائتماني')
        add_term('NI_Growth (1y)', 'نمو صافي الدخل (سنة واحدة)')
        add_term('Retention_Ratio', 'نسبة الاحتجاز')
        add_term('Dividends_Paid', 'التوزيعات المدفوعة')
        add_term('FCF_per_Share', 'FCF لكل سهم')
        add_term('Inventory Days (DIH)', 'أيام المخزون (DIH)')
        add_term('AR Days (DSO)', 'أيام الذمم المدينة (DSO)')
        add_term('AP Days (DPO)', 'أيام الذمم الدائنة (DPO)')
        add_term('Cost_of_Debt (input)', 'تكلفة الدين (مدخل)')
        add_term('P/E Ratio (Used)', 'مكرر الربحية المستخدم')
        add_term('P/B Ratio (Used)', 'مكرر القيمة الدفترية المستخدم')
        add_term('ROA', 'العائد على الأصول')
        add_term('ROE', 'العائد على حقوق الملكية')
        add_term('ROIC', 'العائد على رأس المال المستثمر')
        add_term('EPS', 'ربحية السهم')
        add_term('Net Margin', 'هامش صافي الربح')
        add_term('Op_Leverage', 'الرافعة التشغيلية')
        add_term('CCC_Days', 'أيام دورة التحويل النقدي')
        add_term('AR Days (DSO)', 'أيام الذمم المدينة (DSO)')
        add_term('AP Days (DPO)', 'أيام الذمم الدائنة (DPO)')
        add_term('Inventory_Days', 'أيام المخزون')
        add_term('AR_Days', 'أيام الذمم المدينة')
        add_term('AP_Days', 'أيام الذمم الدائنة')
        add_term('Beta (Market Risk)', 'بيتا (مخاطر السوق)')
        add_term('SGR_Internal (Sustainable Growth)', 'معدل النمو الداخلي المستدام')
        add_term('Cost_of_Debt', 'تكلفة الدين')
        add_term('Beta', 'بيتا')
        add_term('FCF_Yield', 'عائد التدفق النقدي الحر')
        add_term('Dividend_Yield', 'عائد التوزيعات')
        add_term('Net Interest Margin (NIM)', 'هامش صافي الفائدة (NIM)')
        add_term('Loan-to-Deposit Ratio', 'نسبة القروض إلى الودائع')
        add_term('Loan-to-Deposit Ratio (LDR)', 'نسبة القروض إلى الودائع (LDR)')
        add_term('Capital Ratio Proxy', 'مؤشر كفاية رأس المال')
        add_term('Efficiency Ratio', 'نسبة الكفاءة التشغيلية')
        add_term('Net Income / Assets', 'صافي الدخل إلى الأصول')
        add_term('Equity Ratio', 'نسبة حقوق الملكية')
        add_term('Combined Ratio Proxy', 'مؤشر النسبة المجمعة')
        add_term('Capital Adequacy Proxy', 'مؤشر كفاية رأس المال')
        add_term('Bank Total Revenue', 'إجمالي إيرادات البنك')
        add_term('Net Interest Margin', 'هامش صافي الفائدة')
        add_term('Operating Margin', 'الهامش التشغيلي')
        add_term('Net Margin', 'هامش صافي الربح')
        add_term('PE_Ratio', 'مكرر الربحية')
        add_term('PB_Ratio', 'مكرر القيمة الدفترية')
        add_term('PE_Ratio_Used', 'مكرر الربحية المستخدم')
        add_term('PB_Ratio_Used', 'مكرر القيمة الدفترية المستخدم')
        add_term('Dividend_Yield', 'عائد التوزيعات')
        add_term('Cost_of_Debt (input)', 'تكلفة الدين (مدخل)')
        add_term('EV_EBITDA', 'مضاعف EV/EBITDA')

        # Ratio explanation by ratio id (source of truth independent from UI text)
        self._ratio_expl_i18n = {
            'gross_margin': {
                'ar': 'هامش الربح الإجمالي',
                'en': 'Gross profitability margin',
                'fr': 'Marge brute',
            },
            'operating_margin': {
                'ar': 'كفاءة التشغيل الأساسية',
                'en': 'Operating profitability margin',
                'fr': 'Marge opérationnelle',
            },
            'net_margin': {
                'ar': 'هامش صافي الربح',
                'en': 'Net profitability margin',
                'fr': 'Marge nette',
            },
            'current_ratio': {
                'ar': 'القدرة على السداد قصير الأجل',
                'en': 'Short-term solvency',
                'fr': 'Capacité de paiement à court terme',
            },
            'quick_ratio': {
                'ar': 'السيولة الفورية',
                'en': 'Immediate liquidity',
                'fr': 'Liquidité immédiate',
            },
            'debt_to_equity': {
                'ar': 'الاعتماد على التمويل بالدين',
                'en': 'Leverage versus equity',
                'fr': 'Endettement par rapport aux capitaux propres',
            },
            'debt_to_assets': {
                'ar': 'نسبة الأصول الممولة بالدين',
                'en': 'Debt share in assets',
                'fr': 'Part des actifs financés par la dette',
            },
            'interest_coverage': {
                'ar': 'قدرة تغطية الفوائد',
                'en': 'Interest payment coverage',
                'fr': 'Couverture des intérêts',
            },
            'pe_ratio': {
                'ar': 'مكرر الربحية',
                'en': 'Price-to-earnings multiple',
                'fr': 'Multiple cours/bénéfice',
            },
            'pb_ratio': {
                'ar': 'مكرر القيمة الدفترية',
                'en': 'Price-to-book multiple',
                'fr': 'Multiple cours/valeur comptable',
            },
            'dividend_yield': {
                'ar': 'عائد التوزيعات النقدية',
                'en': 'Dividend cash yield',
                'fr': 'Rendement du dividende',
            },
            'fcf_yield': {
                'ar': 'عائد التدفق النقدي الحر',
                'en': 'Free cash flow yield',
                'fr': 'Rendement du flux de trésorerie libre',
            },
            'ebitda_margin': {
                'ar': 'هامش الأرباح قبل الفوائد والضرائب والاستهلاك',
                'en': 'EBITDA profitability margin',
                'fr': 'Marge EBITDA',
            },
            'roa': {
                'ar': 'العائد على الأصول',
                'en': 'Return on assets',
                'fr': 'Rendement des actifs',
            },
            'roe': {
                'ar': 'العائد على حقوق الملكية',
                'en': 'Return on equity',
                'fr': 'Rendement des capitaux propres',
            },
            'roic': {
                'ar': 'العائد على رأس المال المستثمر',
                'en': 'Return on invested capital',
                'fr': 'Rendement du capital investi',
            },
            'inventory_turnover': {
                'ar': 'معدل دوران المخزون',
                'en': 'Inventory turnover rate',
                'fr': 'Rotation des stocks',
            },
            'inventory_days': {
                'ar': 'عدد أيام الاحتفاظ بالمخزون',
                'en': 'Days inventory held',
                'fr': 'Jours de détention des stocks',
            },
            'days_sales_outstanding': {
                'ar': 'عدد أيام التحصيل من العملاء',
                'en': 'Days sales outstanding',
                'fr': 'Délai moyen de recouvrement',
            },
            'payables_turnover': {
                'ar': 'معدل دوران الذمم الدائنة',
                'en': 'Payables turnover rate',
                'fr': 'Rotation des dettes fournisseurs',
            },
            'ap_days': {
                'ar': 'عدد أيام سداد الموردين',
                'en': 'Days payable outstanding',
                'fr': 'Délai moyen de paiement fournisseurs',
            },
            'asset_turnover': {
                'ar': 'معدل دوران الأصول',
                'en': 'Asset turnover rate',
                'fr': 'Rotation des actifs',
            },
            'cash_ratio': {
                'ar': 'نسبة السيولة النقدية',
                'en': 'Cash liquidity ratio',
                'fr': 'Ratio de liquidité immédiate',
            },
            'net_debt_ebitda': {
                'ar': 'صافي الدين مقارنة بـ EBITDA',
                'en': 'Net debt relative to EBITDA',
                'fr': 'Dette nette rapportée à l’EBITDA',
            },
            'eps_basic': {
                'ar': 'ربحية السهم الأساسية',
                'en': 'Basic earnings per share',
                'fr': 'Bénéfice par action de base',
            },
            'book_value_per_share': {
                'ar': 'القيمة الدفترية لكل سهم',
                'en': 'Book value per share',
                'fr': 'Valeur comptable par action',
            },
            'market_cap': {
                'ar': 'القيمة السوقية',
                'en': 'Market capitalization',
                'fr': 'Capitalisation boursière',
            },
            'enterprise_value': {
                'ar': 'قيمة المنشأة',
                'en': 'Enterprise value',
                'fr': 'Valeur d’entreprise',
            },
            'ev_ebitda': {
                'ar': 'مضاعف قيمة المنشأة إلى EBITDA',
                'en': 'Enterprise value to EBITDA multiple',
                'fr': 'Multiple valeur d’entreprise / EBITDA',
            },
            'cost_of_debt': {
                'ar': 'تكلفة الدين',
                'en': 'Cost of debt',
                'fr': 'Coût de la dette',
            },
            'wacc': {
                'ar': 'متوسط تكلفة رأس المال المرجح',
                'en': 'Weighted average cost of capital',
                'fr': 'Coût moyen pondéré du capital',
            },
            'altman_z_score': {
                'ar': 'مؤشر ألتمان Z للمخاطر',
                'en': 'Altman Z risk score',
                'fr': 'Score de risque Altman Z',
            },
            'accruals_ratio': {
                'ar': 'نسبة الاستحقاقات',
                'en': 'Accruals ratio',
                'fr': 'Ratio des régularisations',
            },
            'ocf_margin': {
                'ar': 'هامش التدفق النقدي التشغيلي',
                'en': 'Operating cash flow margin',
                'fr': 'Marge de flux de trésorerie opérationnel',
            },
            'free_cash_flow': {
                'ar': 'التدفق النقدي الحر',
                'en': 'Free cash flow',
                'fr': 'Flux de trésorerie libre',
            },
            'fcf_per_share': {
                'ar': 'التدفق النقدي الحر لكل سهم',
                'en': 'Free cash flow per share',
                'fr': 'Flux de trésorerie libre par action',
            },
            'net_interest_margin': {
                'ar': 'هامش صافي الفائدة',
                'en': 'Net interest margin',
                'fr': 'Marge nette d’intérêt',
            },
            'loan_to_deposit_ratio': {
                'ar': 'نسبة القروض إلى الودائع',
                'en': 'Loan-to-deposit ratio',
                'fr': 'Ratio prêts/dépôts',
            },
            'capital_ratio_proxy': {
                'ar': 'مؤشر كفاية رأس المال',
                'en': 'Capital adequacy proxy',
                'fr': 'Proxy de solvabilité du capital',
            },
            'bank_efficiency_ratio': {
                'ar': 'نسبة كفاءة البنك',
                'en': 'Bank efficiency ratio',
                'fr': 'Ratio d’efficacité bancaire',
            },
            'net_income_to_assets': {
                'ar': 'صافي الدخل إلى الأصول',
                'en': 'Net income to assets',
                'fr': 'Résultat net rapporté aux actifs',
            },
            'equity_ratio': {
                'ar': 'نسبة حقوق الملكية',
                'en': 'Equity ratio',
                'fr': 'Ratio des capitaux propres',
            },
            'combined_proxy': {
                'ar': 'مؤشر النسبة المجمعة',
                'en': 'Combined ratio proxy',
                'fr': 'Proxy du ratio combiné',
            },
            'capital_adequacy_proxy': {
                'ar': 'مؤشر كفاية رأس المال',
                'en': 'Capital adequacy proxy',
                'fr': 'Proxy de suffisance du capital',
            },
        }

        # Phrase-level fallback translations for unseen labels.
        self._term_phrase_i18n = {
            'ar': {
                'gross margin': 'الهامش الإجمالي',
                'gross profit': 'إجمالي الربح',
                'operating expenses': 'المصروفات التشغيلية',
                'total operating expenses': 'إجمالي المصروفات التشغيلية',
                'operating income': 'الدخل التشغيلي',
                'other income': 'إيرادات أخرى',
                'expense': 'مصروف',
                'net income': 'صافي الدخل',
                'net income loss': 'صافي الدخل (الخسارة)',
                'products': 'المنتجات',
                'marketable securities': 'الأوراق المالية القابلة للتداول',
                'vendor non-trade receivables': 'ذمم مدينة غير تجارية من الموردين',
                'non-current assets': 'الأصول غير المتداولة',
                'non-current liabilities': 'الخصوم غير المتداولة',
                'other non-current assets': 'أصول غير متداولة أخرى',
                'income before': 'الدخل قبل',
                'provision for income taxes': 'مخصص ضرائب الدخل',
                'research and development': 'البحث والتطوير',
                'selling, general and administrative': 'البيع والعمومية والإدارية',
                'earnings per share': 'ربحية السهم',
                'basic': 'أساسي',
                'diluted': 'مخفف',
                'shares used in computing': 'الأسهم المستخدمة في احتساب',
                'shares': 'الأسهم',
                'cash and cash equivalents': 'النقد وما يعادله',
                'accounts receivable': 'الذمم المدينة',
                'accounts payable': 'الذمم الدائنة',
                'inventory': 'المخزون',
                'assets': 'الأصول',
                'liabilities': 'الخصوم',
                'equity': 'حقوق الملكية',
                'revenue': 'الإيرادات',
                'cost of revenue': 'تكلفة الإيرادات',
                'loss': 'الخسارة',
            },
            'fr': {
                'gross margin': 'marge brute',
                'gross profit': 'profit brut',
                'operating expenses': 'charges opérationnelles',
                'operating income': 'résultat opérationnel',
                'net income': 'résultat net',
                'products': 'produits',
                'marketable securities': 'titres négociables',
                'vendor non-trade receivables': 'créances non commerciales sur fournisseurs',
                'non-current assets': 'actifs non courants',
                'non-current liabilities': 'passifs non courants',
                'research and development': 'recherche et développement',
                'selling, general and administrative': 'frais commerciaux, généraux et administratifs',
                'earnings per share': 'bénéfice par action',
                'cash and cash equivalents': 'trésorerie et équivalents',
                'accounts receivable': 'créances clients',
                'accounts payable': 'dettes fournisseurs',
                'inventory': 'stocks',
                'assets': 'actifs',
                'liabilities': 'passifs',
                'equity': 'capitaux propres',
                'revenue': 'revenus',
            },
        }

        # Reverse lookup map: Arabic/English/French variants -> canonical EN label.
        # This allows strong semantic deduplication across languages in UI/export.
        for _, bundle in (self._term_i18n or {}).items():
            en_label = self._normalize_term_for_lookup(bundle.get('en', ''))
            if not en_label:
                continue
            for vv in (bundle.get('ar', ''), bundle.get('en', ''), bundle.get('fr', '')):
                nk = self._normalize_term_for_lookup(vv)
                if nk:
                    self._term_reverse_to_en[nk] = en_label

    def _translate_financial_item(self, label: str) -> str:
        txt = self._decode_mojibake_text(str(label or ""))
        if self._is_technical_label(txt) and not bool(self.translate_technical_var.get()):
            return txt
        key = self._normalize_term_for_lookup(txt)
        if key in self._term_i18n:
            return self._term_i18n[key].get(self.current_lang, txt)

        # Handle " - Group Title - " style wrappers.
        m = re.match(r'^\s*-\s*(.*?)\s*-\s*$', txt)
        if m:
            core = m.group(1).strip()
            translated = self._translate_financial_item(core)
            return f"- {translated} -"

        return self._smart_translate_financial_phrase(txt)

    def _is_technical_label(self, label: str) -> bool:
        txt = self._decode_mojibake_text(str(label or '')).strip()
        if not txt:
            return False
        # XBRL/SEC tags and namespaced concepts.
        if re.fullmatch(r'[a-z][a-z0-9\-]*:[A-Za-z0-9_]+', txt):
            return True
        # Typical technical identifiers / ratio keys.
        if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', txt):
            if '_' in txt:
                return True
            if re.search(r'[A-Z].*[A-Z]', txt) and ' ' not in txt:
                return True
        # Common finance acronyms/codes should stay as-is.
        if re.fullmatch(r'(ROE|ROA|ROIC|EBITDA|EPS|WACC|FCF|DSO|DPO|DIH|CCC|P/E|P/B|EV/EBITDA|NIM)', txt):
            return True
        return False

    def _smart_translate_financial_phrase(self, label: str) -> str:
        txt = self._decode_mojibake_text(str(label or '')).strip()
        if not txt:
            return txt
        if self.current_lang == 'en':
            return txt
        # If already contains Arabic text and Arabic UI is selected, keep as-is.
        if self.current_lang == 'ar' and any('\u0600' <= ch <= '\u06ff' for ch in txt):
            return txt

        # Try normalized variants first.
        variants = [txt]
        cleaned = re.sub(r'^[A-Za-z0-9_.-]+:', '', txt).strip()
        if cleaned and cleaned not in variants:
            variants.append(cleaned)
        camel_spaced = re.sub(r'(?<!^)(?=[A-Z])', ' ', cleaned).replace('_', ' ')
        camel_spaced = re.sub(r'\s+', ' ', camel_spaced).strip()
        if camel_spaced and camel_spaced not in variants:
            variants.append(camel_spaced)

        for candidate in variants:
            ckey = self._normalize_term_for_lookup(candidate)
            if ckey in self._term_i18n:
                return self._term_i18n[ckey].get(self.current_lang, candidate)

        phrase_map = (self._term_phrase_i18n or {}).get(self.current_lang, {}) or {}
        out = camel_spaced if camel_spaced else txt
        original = out
        for src, dst in sorted(phrase_map.items(), key=lambda kv: len(kv[0]), reverse=True):
            out = re.sub(rf'(?i)\b{re.escape(src)}\b', dst, out)
        out = re.sub(r'\s+', ' ', out).strip()
        return out if out != original else txt

    def _translate_ratio_explanation(self, ratio_key: str, fallback_text: str = "") -> str:
        rk = str(ratio_key or '').strip()
        bundle = self._ratio_expl_i18n.get(rk)
        if bundle:
            return bundle.get(self.current_lang, bundle.get('en', ''))
        fallback = self._decode_mojibake_text(str(fallback_text or '')).strip()
        if not fallback:
            return ''
        # Guard against mojibake artifacts leaking to UI.
        if re.search(r'[ØÙÐ]{2,}', fallback):
            return ''
        if self.current_lang == 'ar':
            return fallback
        if any('\u0600' <= ch <= '\u06ff' for ch in fallback):
            return ''
        return fallback

    def _is_parent_line_item(self, label: str, year_values: list | None = None) -> bool:
        txt = self._decode_mojibake_text(str(label or '')).strip()
        key = self._normalize_term_for_lookup(txt)
        if not key:
            return False
        if txt.startswith('---') and txt.endswith('---'):
            return True
        if txt.startswith('-') and txt.endswith('-'):
            return True
        if '[abstract]' in key:
            return True
        if key.endswith(':'):
            return True
        if ' statement [abstract]' in key:
            return True
        if key.startswith('total '):
            return True
        if key.startswith('subtotal ') or key.startswith('grand total'):
            return True
        if txt.startswith('إجمالي') or txt.startswith('المجموع'):
            return True
        if '_parent' in key or ' parent' in key:
            return True
        if 'hierarchy' in key:
            return True
        if year_values is not None:
            non_empty = 0
            for v in (year_values or []):
                if str(v or '').strip() not in ('', 'None', 'nan'):
                    non_empty += 1
            # section headers usually have no numeric values.
            if non_empty == 0:
                return True
        return False

    def _t(self, key: str) -> str:
        bundle = self._i18n.get(key, {})
        if self.current_lang in bundle:
            return bundle[self.current_lang]
        return bundle.get('ar', key)

    def _translate_ui_text(self, text: str) -> str:
        txt = self._decode_mojibake_text(text)
        norm = re.sub(r'\s+', ' ', str(txt)).strip()
        key = self._i18n_reverse.get(norm)
        if key:
            return self._t(key)

        # Dynamic prefix translation for runtime strings.
        prefix_map = {
            'الشركة:': {'en': 'Company:', 'fr': 'Société :'},
            'السنة:': {'en': 'Year:', 'fr': 'Année :'},
            'النسبة:': {'en': 'Ratio:', 'fr': 'Ratio :'},
            'الحالة:': {'en': 'Status:', 'fr': 'Statut :'},
            'السبب التقني:': {'en': 'Technical Reason:', 'fr': 'Raison technique :'},
            'لا توجد': {'en': 'No', 'fr': 'Aucun'},
            'تم حذف:': {'en': 'Deleted:', 'fr': 'Supprimé :'},
            'تم حفظ الملف:': {'en': 'File saved:', 'fr': 'Fichier enregistré :'},
            'فشل': {'en': 'Failed', 'fr': 'Échec'},
            'جلب:': {'en': 'Fetching:', 'fr': 'Récupération :'},
        }
        for ar_prefix, trans in prefix_map.items():
            if norm.startswith(ar_prefix):
                rep = trans.get(self.current_lang)
                if rep:
                    return rep + norm[len(ar_prefix):]
        return txt

    def _translate_layer_title(self, original_title: str) -> str:
        t = str(original_title or '')
        if 'Layer 1' in t or 'الطبقة 1' in t:
            return self._t('layer1')
        if 'Layer 2' in t or 'الطبقة 2' in t:
            return self._t('layer2')
        if 'Layer 3' in t or 'الطبقة 3' in t:
            return self._t('layer3')
        if 'Layer 4' in t or 'الطبقة 4' in t:
            return self._t('layer4')
        return self._translate_ui_text(t)

    def _on_language_changed(self, _event=None):
        choice = self._lang_choice_var.get()
        code = dict(self._lang_options).get(choice)
        if not code:
            ch = str(choice or '').strip().lower()
            ch_simple = re.sub(r'[^a-z\u0600-\u06ff]', '', ch)
            if ch in {'ar', 'arabic', 'العربية'}:
                code = 'ar'
            elif ch in {'en', 'english'}:
                code = 'en'
            elif ch in {'fr', 'français', 'francais', 'french'}:
                code = 'fr'
            elif ch_simple.startswith('fran'):
                code = 'fr'
        if not code:
            for lbl, c in self._lang_options:
                if str(lbl).strip().lower() == str(choice).strip().lower() or c == choice:
                    code = c
                    break
        if code not in ('ar', 'en', 'fr'):
            code = 'ar'
        self.current_lang = code
        self._apply_ui_text_fixes()
        if hasattr(self, 'raw_layer_combo') and not self.current_data:
            vals = [self._t('layer1'), self._t('layer2'), self._t('layer3')]
            self.raw_layer_combo.configure(values=vals)
            if self.raw_layer_var.get() not in vals:
                self.raw_layer_var.set(vals[0])
        self._sync_layer_selector_from_data()
        if self.current_data:
            self.display_all()
            self.display_comparison()

    def _apply_professional_theme(self):
        style = ttk.Style(self.root)
        try:
            style.theme_use('clam')
        except Exception:
            pass
        style.configure(
            "TNotebook",
            background=PALETTE['bg'],
            borderwidth=0,
        )
        style.configure(
            "TNotebook.Tab",
            font=FONTS['label'],
            padding=(14, 8),
            background="#dde5f0",
            foreground="#1d2633",
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", "#ffffff"), ("active", "#e7edf6")],
            foreground=[("selected", PALETTE['accent'])],
        )
        style.configure(
            "Treeview",
            font=FONTS['tree'],
            rowheight=30,
            background="#ffffff",
            fieldbackground="#ffffff",
            foreground="#1f2937",
            bordercolor=PALETTE['border'],
            relief="flat",
        )
        style.configure(
            "Treeview.Heading",
            font=FONTS['label'],
            background="#f1f5fb",
            foreground="#0f2747",
            relief="flat",
            padding=(8, 8),
        )
        style.map(
            "Treeview",
            background=[("selected", "#0b6efd")],
            foreground=[("selected", "#ffffff")],
        )
        style.configure(
            "TCombobox",
            fieldbackground="#ffffff",
            background="#ffffff",
            bordercolor=PALETTE['border'],
            arrowsize=14,
            padding=4,
        )
        style.configure(
            "TProgressbar",
            troughcolor="#e8eef7",
            background=PALETTE['accent'],
            bordercolor="#d1dbe8",
            lightcolor=PALETTE['accent'],
            darkcolor=PALETTE['accent'],
            thickness=12,
        )
        style.configure(
            "TSpinbox",
            arrowsize=14,
            padding=3,
            fieldbackground="#ffffff",
        )

    def _build_left(self, parent):
        # Scrollable left pane so controls remain visible on high DPI / small heights.
        left_canvas = tk.Canvas(parent, bg=PALETTE['panel'], highlightthickness=0, borderwidth=0)
        left_scroll = ttk.Scrollbar(parent, orient='vertical', command=left_canvas.yview)
        left_canvas.configure(yscrollcommand=left_scroll.set)
        left_canvas.pack(side='left', fill='both', expand=True, padx=(0, 2), pady=0)
        left_scroll.pack(side='right', fill='y', padx=(0, 2), pady=0)
        content = tk.Frame(left_canvas, bg=PALETTE['panel'])
        content_win = left_canvas.create_window((0, 0), window=content, anchor='nw')

        def _on_content_configure(_event=None):
            left_canvas.configure(scrollregion=left_canvas.bbox("all"))

        def _on_canvas_configure(event):
            try:
                left_canvas.itemconfigure(content_win, width=event.width)
            except Exception:
                pass

        content.bind('<Configure>', _on_content_configure)
        left_canvas.bind('<Configure>', _on_canvas_configure)

        frame = tk.LabelFrame(content, text=self._t('input_data'), bg=PALETTE['panel'], fg='#0f2747', font=FONTS['label'])
        frame.pack(fill='x', padx=8, pady=8)
        inner = tk.Frame(frame, bg=PALETTE['panel'])
        inner.pack(fill='x', padx=8, pady=8)
        inner.grid_columnconfigure(0, weight=1, minsize=260)
        inner.grid_columnconfigure(1, weight=0, minsize=112)

        tk.Label(inner, text=self._t('company_symbols'), bg=PALETTE['panel'], font=FONTS['label']).grid(row=0, column=0, sticky='w')
        self.company_entry = tk.Entry(inner, font=FONTS['normal'], relief='solid', bd=1)
        self.company_entry.grid(row=1, column=0, sticky='ew', pady=6)
        self.company_entry.insert(0, "AAPL, MSFT")
        self.company_entry.bind('<Return>', lambda _e: self._add_companies())
        self.company_entry.bind('<KP_Enter>', lambda _e: self._add_companies())
        self.add_company_btn = tk.Button(
            inner,
            text=self._t('add'),
            command=self._add_companies,
            bg=PALETTE['button'],
            fg='white',
            relief='flat',
            font=FONTS['button'],
            width=10,
            anchor='center',
            justify='center',
        )
        self.add_company_btn.grid(row=1, column=1, padx=6, sticky='ew')

        tk.Label(inner, text=self._t('companies_list'), bg=PALETTE['panel'], font=FONTS['label']).grid(row=2, column=0, sticky='w', pady=(8, 0))
        self.companies_listbox = tk.Listbox(inner, height=5, selectmode=tk.EXTENDED)
        self.companies_listbox.grid(row=3, column=0, columnspan=2, sticky='ew', pady=6)
        
        # âœ… Ø±Ø¨Ø· Ø­Ø¯Ø« ØªØºÙŠÙŠØ± Ø§Ù„Ø§Ø®ØªÙŠØ§Ø± Ø¨ØªØ­Ø¯ÙŠØ« Ø§Ù„Ø¹Ø±Ø¶
        self.companies_listbox.bind('<<ListboxSelect>>', self._on_company_select)

        btns_frame = tk.Frame(inner, bg=PALETTE['panel'])
        btns_frame.grid(row=4, column=0, columnspan=2, sticky='ew', pady=(4, 8))
        tk.Button(btns_frame, text=self._t('remove_selected'), bg='#dc3545', fg='white', relief='flat', font=FONTS['button'], command=self._remove_selected_companies).pack(side='left', padx=(0, 6))
        tk.Button(btns_frame, text=self._t('clear_all'), bg='#6c757d', fg='white', relief='flat', font=FONTS['button'], command=self._clear_companies).pack(side='left')

        tk.Label(inner, text=self._t('period_range'), bg=PALETTE['panel'], font=FONTS['label']).grid(row=5, column=0, sticky='w')
        years_frame = tk.Frame(inner, bg=PALETTE['panel'])
        years_frame.grid(row=6, column=0, columnspan=2, sticky='w')
        self.start_year_var = tk.StringVar(value=str(datetime.now().year - 3))
        self.end_year_var = tk.StringVar(value=str(datetime.now().year))
        ttk.Spinbox(years_frame, from_=1990, to=2035, textvariable=self.start_year_var, width=8).pack(side='left')
        tk.Label(years_frame, text=self._t('to_sep'), bg=PALETTE['panel'], font=FONTS['normal']).pack(side='left')
        ttk.Spinbox(years_frame, from_=1990, to=2035, textvariable=self.end_year_var, width=8).pack(side='left')

        tk.Label(inner, text=self._t('filing_type'), bg=PALETTE['panel'], font=FONTS['label']).grid(row=7, column=0, sticky='w', pady=(8, 0))
        self.filing_type_var = tk.StringVar(value='10-K')
        self._filing_display_options = [
            (self._t('filing_10k'), '10-K'),
            (self._t('filing_10ka'), '10-K/A'),
            (self._t('filing_20f'), '20-F'),
        ]
        self._filing_display_to_real = {disp: real for disp, real in self._filing_display_options}
        self._filing_real_to_display = {real: disp for disp, real in self._filing_display_options}
        self.filing_type_display_var = tk.StringVar(value=self._filing_real_to_display.get('10-K', '10-K'))
        self.filing_type_combo = ttk.Combobox(
            inner,
            textvariable=self.filing_type_display_var,
            state='readonly',
            values=[x[0] for x in self._filing_display_options],
            width=26,
        )
        self.filing_type_combo.grid(row=8, column=0, columnspan=2, sticky='ew', padx=2, pady=(2, 2))
        self.filing_type_combo.bind('<<ComboboxSelected>>', self._on_filing_type_changed)
        self.filing_support_label = tk.Label(
            inner,
            text=self._t('filing_supported'),
            bg=PALETTE['panel'],
            fg=PALETTE['muted'],
            font=FONTS['normal'],
        )
        self.filing_support_label.grid(row=9, column=0, columnspan=2, sticky='w', padx=2)
        self.translate_tech_check = ttk.Checkbutton(
            inner,
            text=self._t('translate_technical_labels'),
            variable=self.translate_technical_var,
            command=self._on_translate_technical_toggle,
        )
        self.translate_tech_check.grid(row=10, column=0, columnspan=2, sticky='w', padx=2, pady=(2, 0))

        self.fetch_btn = tk.Button(
            inner,
            text=self._t('fetch_data'),
            bg=PALETTE['accent'],
            fg='white',
            relief='flat',
            font=FONTS['button'],
            command=self.fetch_data,
            anchor='center',
            justify='center',
            wraplength=320,
            padx=6,
            pady=10,
        )
        self.fetch_btn.grid(row=11, column=0, columnspan=2, pady=10, sticky='ew')
        io_row = tk.Frame(inner, bg=PALETTE['panel'])
        io_row.grid(row=12, column=0, columnspan=2, sticky='ew', pady=(2, 6))
        self.quick_load_btn = tk.Button(
            io_row,
            text=self._t('tool_load_compact'),
            bg='#6f42c1',
            fg='white',
            relief='flat',
            font=FONTS['normal'],
            command=self.load_results_from_excel,
            anchor='center',
            justify='center',
            wraplength=130,
            padx=4,
            pady=6,
        )
        self.quick_load_btn.pack(side='left', fill='x', expand=True, padx=(0, 4))
        self.quick_export_btn = tk.Button(
            io_row,
            text=self._t('tool_export_compact'),
            bg=PALETTE['button'],
            fg='white',
            relief='flat',
            font=FONTS['normal'],
            command=self.export_to_excel_safe,
            anchor='center',
            justify='center',
            wraplength=130,
            padx=4,
            pady=6,
        )
        self.quick_export_btn.pack(side='left', fill='x', expand=True, padx=(4, 0))

        self.progress_label = tk.Label(
            inner,
            text="",
            bg=PALETTE['panel'],
            fg=PALETTE['muted'],
            font=FONTS['normal'],
            anchor='e',
            justify='right',
            wraplength=360,
        )
        self.progress_label.grid(row=13, column=0, columnspan=2)
        self.progress_bar = ttk.Progressbar(inner, mode='indeterminate', length=300)
        self.progress_bar.grid(row=14, column=0, columnspan=2, pady=6)
        self.loading_indicator_canvas = tk.Canvas(inner, width=300, height=12, bg=PALETTE['panel'], highlightthickness=0)
        self.loading_indicator_canvas.grid(row=15, column=0, columnspan=2, pady=(2, 4))
        self._loading_dot = self.loading_indicator_canvas.create_oval(2, 2, 10, 10, fill='#28a745', outline='#28a745', state='hidden')
        self._loading_anim_job = None
        self._loading_anim_pos = 2
        self._loading_anim_dir = 1
        self._loading_anim_visible = True

        # Market inputs
        market_frame = tk.LabelFrame(content, text=self._t('market_inputs'), bg=PALETTE['panel'], fg='#0f2747', font=FONTS['label'])
        market_frame.pack(fill='x', padx=8, pady=(4, 8))
        mf = tk.Frame(market_frame, bg=PALETTE['panel'])
        mf.pack(fill='x', padx=8, pady=8)
        tk.Label(mf, text=self._t('current_price'), bg=PALETTE['panel'], font=FONTS['normal']).grid(row=0, column=0, sticky='w')
        self.price_var = tk.DoubleVar(value=0.0)
        ttk.Entry(mf, textvariable=self.price_var, width=14).grid(row=0, column=1, sticky='w', padx=6)
        tk.Label(mf, text=self._t('shares_outstanding'), bg=PALETTE['panel'], font=FONTS['normal']).grid(row=1, column=0, sticky='w')
        self.shares_var = tk.DoubleVar(value=0.0)
        ttk.Entry(mf, textvariable=self.shares_var, width=14).grid(row=1, column=1, sticky='w', padx=6)
        tk.Label(mf, text=self._t('cost_of_debt'), bg=PALETTE['panel'], font=FONTS['normal']).grid(row=2, column=0, sticky='w')
        self.cost_of_debt_var = tk.DoubleVar(value=4.0)
        ttk.Entry(mf, textvariable=self.cost_of_debt_var, width=14).grid(row=2, column=1, sticky='w', padx=6)

        # Mouse wheel support on left pane.
        def _on_mousewheel(event):
            try:
                delta = int(-1 * (event.delta / 120))
                left_canvas.yview_scroll(delta, "units")
            except Exception:
                pass
        left_canvas.bind_all("<MouseWheel>", _on_mousewheel)

    def _build_right(self, parent):
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill='both', expand=True)

        self.raw_tab = tk.Frame(self.notebook, bg=PALETTE['panel'])
        self.ratios_tab = tk.Frame(self.notebook, bg=PALETTE['panel'])
        self.strategic_tab = tk.Frame(self.notebook, bg=PALETTE['panel'])
        self.comparison_tab = tk.Frame(self.notebook, bg=PALETTE['panel'])
        self.forecast_tab = tk.Frame(self.notebook, bg=PALETTE['panel'])
        self.ai_analysis_tab = tk.Frame(self.notebook, bg=PALETTE['panel'])  # âœ… NEW

        self.notebook.add(self.raw_tab, text=self._t('tab_raw'))
        self.notebook.add(self.ratios_tab, text=self._t('tab_ratios'))
        self.notebook.add(self.strategic_tab, text=self._t('tab_strategic'))
        self.notebook.add(self.comparison_tab, text=self._t('tab_comparison'))
        self.notebook.add(self.forecast_tab, text=self._t('tab_forecast'))
        self.notebook.add(self.ai_analysis_tab, text=self._t('tab_ai'))  # âœ… NEW

        # raw
        self.company_info_label = tk.Label(self.raw_tab, text="", bg=PALETTE['panel'], font=FONTS['title'])
        self.company_info_label.pack(fill='x', padx=8, pady=6)
        raw_controls = tk.Frame(self.raw_tab, bg=PALETTE['panel'])
        raw_controls.pack(fill='x', padx=8, pady=(0, 4))
        tk.Label(raw_controls, text=self._t('layer_view'), bg=PALETTE['panel'], font=FONTS['label']).pack(side='left')
        self.raw_layer_var = tk.StringVar(value=self._t('layer1'))
        self.raw_layer_combo = ttk.Combobox(
            raw_controls,
            textvariable=self.raw_layer_var,
            state='readonly',
            values=[
                self._t('layer1'),
                self._t('layer2'),
                self._t('layer3')
            ],
            width=34
        )
        self.raw_layer_combo.pack(side='left', padx=8)
        self.raw_layer_combo.bind('<<ComboboxSelected>>', lambda e: self.display_raw_data())
        tk.Label(raw_controls, text=self._t('sec_view_mode_label'), bg=PALETTE['panel'], font=FONTS['label']).pack(side='left', padx=(12, 0))
        self._sec_view_mode_var = tk.StringVar(value='official')
        self.sec_view_mode_display_var = tk.StringVar(value=self._t('sec_view_mode_official'))
        self.sec_view_mode_combo = ttk.Combobox(
            raw_controls,
            textvariable=self.sec_view_mode_display_var,
            state='readonly',
            width=25
        )
        self.sec_view_mode_combo.pack(side='left', padx=8)
        self._refresh_sec_view_mode_display()
        self.sec_view_mode_combo.bind(
            '<<ComboboxSelected>>',
            lambda e: (
                self._sec_view_mode_var.set(self._get_selected_sec_view_mode()),
                self.display_raw_data()
            )
        )
        tree_frame = tk.Frame(self.raw_tab)
        tree_frame.pack(fill='both', expand=True, padx=8, pady=8)
        self.raw_tree = ttk.Treeview(tree_frame, show='headings')
        raw_y = ttk.Scrollbar(tree_frame, orient='vertical', command=self.raw_tree.yview)
        raw_x = ttk.Scrollbar(tree_frame, orient='horizontal', command=self.raw_tree.xview)
        self.raw_tree.configure(yscrollcommand=raw_y.set, xscrollcommand=raw_x.set)
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(0, weight=1)
        self.raw_tree.grid(row=0, column=0, sticky='nsew')
        raw_y.grid(row=0, column=1, sticky='ns')
        raw_x.grid(row=1, column=0, sticky='ew')

        # ratios
        frame_r = tk.Frame(self.ratios_tab)
        frame_r.pack(fill='both', expand=True, padx=8, pady=8)
        self.ratios_tree = ttk.Treeview(frame_r, show='headings')
        ratios_y = ttk.Scrollbar(frame_r, orient='vertical', command=self.ratios_tree.yview)
        ratios_x = ttk.Scrollbar(frame_r, orient='horizontal', command=self.ratios_tree.xview)
        self.ratios_tree.configure(yscrollcommand=ratios_y.set, xscrollcommand=ratios_x.set)
        frame_r.grid_columnconfigure(0, weight=1)
        frame_r.grid_rowconfigure(0, weight=1)
        self.ratios_tree.grid(row=0, column=0, sticky='nsew')
        ratios_y.grid(row=0, column=1, sticky='ns')
        ratios_x.grid(row=1, column=0, sticky='ew')
        self.ratios_tree.bind('<ButtonRelease-1>', self._on_ratio_tree_click)
        self.ratios_comment = tk.Text(self.ratios_tab, height=4, bg='#f8f9fa', wrap='word')
        self.ratios_comment.pack(fill='x', padx=8, pady=(4, 8))

        # strategic
        frame_s = tk.Frame(self.strategic_tab)
        frame_s.pack(fill='both', expand=True, padx=8, pady=8)
        self.strat_tree = ttk.Treeview(frame_s, show='headings')
        strat_y = ttk.Scrollbar(frame_s, orient='vertical', command=self.strat_tree.yview)
        strat_x = ttk.Scrollbar(frame_s, orient='horizontal', command=self.strat_tree.xview)
        self.strat_tree.configure(yscrollcommand=strat_y.set, xscrollcommand=strat_x.set)
        frame_s.grid_columnconfigure(0, weight=1)
        frame_s.grid_rowconfigure(0, weight=1)
        self.strat_tree.grid(row=0, column=0, sticky='nsew')
        strat_y.grid(row=0, column=1, sticky='ns')
        strat_x.grid(row=1, column=0, sticky='ew')

        # comparison (institutional reliability + audit trace)
        comp_top = tk.Frame(self.comparison_tab, bg=PALETTE['panel'])
        comp_top.pack(fill='x', padx=8, pady=(8, 4))
        self.comparison_summary_label = tk.Label(
            comp_top,
            text="",
            bg=PALETTE['panel'],
            fg=PALETTE['muted'],
            font=FONTS['label'],
            anchor='w',
            justify='left'
        )
        self.comparison_summary_label.pack(fill='x')

        comp_mid = tk.Frame(self.comparison_tab, bg=PALETTE['panel'])
        comp_mid.pack(fill='both', expand=True, padx=8, pady=(0, 4))
        self.comparison_tree = ttk.Treeview(comp_mid, show='headings')
        self.comparison_tree.pack(fill='both', expand=True, side='left')
        self.comparison_tree.bind('<<TreeviewSelect>>', self._on_comparison_select)
        ttk.Scrollbar(comp_mid, orient='vertical', command=self.comparison_tree.yview).pack(side='left', fill='y')

        comp_bottom = tk.Frame(self.comparison_tab, bg=PALETTE['panel'])
        comp_bottom.pack(fill='both', expand=True, padx=8, pady=(0, 8))

        detail_frame = tk.Frame(comp_bottom, bg=PALETTE['panel'])
        detail_frame.pack(fill='both', expand=True)
        self.comparison_detail_tree = ttk.Treeview(detail_frame, show='headings')
        self.comparison_detail_tree.pack(fill='both', expand=True, side='left')
        ttk.Scrollbar(detail_frame, orient='vertical', command=self.comparison_detail_tree.yview).pack(side='left', fill='y')

        self.comparison_diag_text = tk.Text(
            comp_bottom,
            height=9,
            bg='#f8f9fa',
            wrap='word',
            font=FONTS['mono']
        )
        self.comparison_diag_text.pack(fill='x', pady=(6, 0))

        # forecasts
        frame_f = tk.Frame(self.forecast_tab)
        frame_f.pack(fill='both', expand=True, padx=8, pady=8)
        self.forecast_tree = ttk.Treeview(frame_f, show='headings')
        self.forecast_tree.pack(fill='both', expand=True, side='left')
        ttk.Scrollbar(frame_f, orient='vertical', command=self.forecast_tree.yview).pack(side='left', fill='y')

        # bottom tools
        tools = tk.Frame(parent, bg=PALETTE['panel'])
        tools.pack(fill='x', padx=12, pady=(0, 12))
        self.export_btn = tk.Button(
            tools,
            text=self._t('tool_export'),
            bg=PALETTE['button'],
            fg='white',
            relief='flat',
            font=FONTS['button'],
            command=self.export_to_excel_safe,
        )
        self.export_btn.pack(side='left', padx=6)
        self.load_excel_btn = tk.Button(
            tools,
            text=self._t('tool_load'),
            bg='#6f42c1',
            fg='white',
            relief='flat',
            font=FONTS['button'],
            command=self.load_results_from_excel,
        )
        self.load_excel_btn.pack(side='left', padx=6)
        self.plot_btn = tk.Button(
            tools,
            text=self._t('tool_plot'),
            bg='#1b7f6a',
            fg='white',
            relief='flat',
            font=FONTS['button'],
            command=self.open_plots_window,
        )
        self.plot_btn.pack(side='left', padx=6)

    # ---------- Companies list actions ----------
    def _add_companies(self):
        txt = self.company_entry.get().strip()
        if not txt:
            return
        parts = [s.strip() for s in re.split(r"[,،;\n\t]+", txt) if s.strip()]
        for p in parts:
            existing = self.companies_listbox.get(0, tk.END)
            if p not in existing:
                self.companies_listbox.insert(tk.END, p)
        self.company_entry.delete(0, tk.END)

    def _decode_mojibake_text(self, text: str) -> str:
        if not isinstance(text, str):
            return text
        if any("\u0600" <= ch <= "\u06ff" for ch in text):
            return text
        suspicious = set("ÃØÙÂâðïÐ¢¤")
        if not any(ch in suspicious for ch in text):
            return text

        # Try common mojibake repair paths (utf-8 bytes decoded as latin1/cp1252).
        candidates = [text]
        for _ in range(3):
            expanded = list(candidates)
            for base in list(candidates):
                for enc in ("latin-1", "cp1252"):
                    try:
                        repaired = base.encode(enc, errors="strict").decode("utf-8", errors="strict")
                        if repaired and repaired not in expanded:
                            expanded.append(repaired)
                    except Exception:
                        continue
            candidates = expanded

        for c in candidates:
            if any("\u0600" <= ch <= "\u06ff" for ch in c):
                return c

        # Fallback cleanup for common mojibake fragments in Arabic UI labels.
        cleaned = str(text)
        cleanup_map = {
            "â”â”â”": "-",
            "ðŸ¤–": "AI",
            "ðŸš¨": "تحذير",
            "ðŸ“‰": "تحليل",
            "ðŸ“ˆ": "نمو",
            "ðŸ’°": "سيولة",
            "âœ…": "",
            "âš ï¸": "تنبيه",
        }
        for bad, good in cleanup_map.items():
            cleaned = cleaned.replace(bad, good)
        return re.sub(r"\s{2,}", " ", cleaned).strip()

    def _install_tk_text_decoder(self) -> None:
        if getattr(tk, "_sec_text_decoder_installed", False):
            return
        original_configure = tk.Misc.configure
        original_heading = ttk.Treeview.heading
        original_insert = ttk.Treeview.insert
        original_messagebox = {}

        def _decode_value(key, value):
            if key not in {"text", "label", "title"}:
                return value
            if not isinstance(value, str):
                return value
            return self._translate_ui_text(value)

        def configure_with_decoder(widget, cnf=None, **kw):
            if isinstance(cnf, dict):
                cnf = {k: _decode_value(k, v) for k, v in cnf.items()}
            if kw:
                kw = {k: _decode_value(k, v) for k, v in kw.items()}
            return original_configure(widget, cnf, **kw)

        def heading_with_decoder(widget, column, option=None, **kw):
            if "text" in kw and isinstance(kw["text"], str):
                kw["text"] = self._translate_ui_text(kw["text"])
            return original_heading(widget, column, option, **kw)

        def insert_with_decoder(widget, parent, index, iid=None, **kw):
            vals = kw.get("values")
            if isinstance(vals, (list, tuple)):
                fixed = []
                for v in vals:
                    if isinstance(v, str):
                        fixed.append(self._translate_ui_text(v))
                    else:
                        fixed.append(v)
                kw["values"] = tuple(fixed)
            if "text" in kw and isinstance(kw["text"], str):
                kw["text"] = self._translate_ui_text(kw["text"])
            row_iid = original_insert(widget, parent, index, iid=iid, **kw)
            try:
                if not getattr(widget, "_sec_zebra_configured", False):
                    widget.tag_configure("zebra_even", background="#ffffff")
                    widget.tag_configure("zebra_odd", background="#f7fbff")
                    widget.tag_configure("header", background="#e8f4f8", foreground="#0f2747")
                    widget._sec_zebra_configured = True
                tags = widget.item(row_iid, "tags")
                if not tags:
                    idx = max(len(widget.get_children(parent)) - 1, 0)
                    widget.item(row_iid, tags=("zebra_even" if idx % 2 == 0 else "zebra_odd",))
            except Exception:
                pass
            return row_iid

        tk.Misc.configure = configure_with_decoder
        tk.Misc.config = configure_with_decoder
        ttk.Treeview.heading = heading_with_decoder
        ttk.Treeview.insert = insert_with_decoder

        # Ensure popup messages are also decoded.
        for fn_name in (
            "showinfo",
            "showwarning",
            "showerror",
            "askyesno",
            "askokcancel",
            "askretrycancel",
            "askyesnocancel",
            "askquestion",
        ):
            fn = getattr(messagebox, fn_name, None)
            if not callable(fn):
                continue
            original_messagebox[fn_name] = fn

            def _wrap_popup(*args, __fn_name=fn_name, **kwargs):
                base_fn = original_messagebox.get(__fn_name)
                if not callable(base_fn):
                    return None
                args = list(args)
                if len(args) >= 1 and isinstance(args[0], str):
                    args[0] = self._translate_ui_text(args[0])
                if len(args) >= 2 and isinstance(args[1], str):
                    args[1] = self._translate_ui_text(args[1])
                if isinstance(kwargs.get("title"), str):
                    kwargs["title"] = self._translate_ui_text(kwargs["title"])
                if isinstance(kwargs.get("message"), str):
                    kwargs["message"] = self._translate_ui_text(kwargs["message"])
                return base_fn(*args, **kwargs)

            setattr(messagebox, fn_name, _wrap_popup)
        tk._sec_text_decoder_installed = True

    def _apply_ui_text_fixes(self) -> None:
        self.root.title(self._t('app_title'))
        self._fix_widget_tree_texts(self.root)
        self._fix_notebook_tab_texts()
        self._polish_arabic_layout(self.root)
        try:
            if hasattr(self, 'header_title_label'):
                self.header_title_label.config(text=self._t('app_header'))
            if hasattr(self, 'lang_label'):
                self.lang_label.config(text=self._t('lang_label'))
            if hasattr(self, 'export_btn'):
                self.export_btn.config(text=self._t('tool_export'))
            if hasattr(self, 'load_excel_btn'):
                self.load_excel_btn.config(text=self._t('tool_load'))
            if hasattr(self, 'plot_btn'):
                self.plot_btn.config(text=self._t('tool_plot'))
            if hasattr(self, 'quick_load_btn'):
                self.quick_load_btn.config(text=self._t('tool_load_compact'))
            if hasattr(self, 'quick_export_btn'):
                self.quick_export_btn.config(text=self._t('tool_export_compact'))
            if hasattr(self, 'fetch_btn'):
                self.fetch_btn.config(text=self._t('fetch_data'))
            if hasattr(self, 'add_company_btn'):
                self.add_company_btn.config(text=self._t('add'))
            if hasattr(self, 'translate_tech_check'):
                self.translate_tech_check.config(text=self._t('translate_technical_labels'))
        except Exception:
            pass
        self._refresh_language_combo_display()
        self._refresh_filing_type_display()
        self._refresh_sec_view_mode_display()

    def _fix_widget_tree_texts(self, widget) -> None:
        self._fix_widget_text(widget)
        for child in widget.winfo_children():
            self._fix_widget_tree_texts(child)

    def _fix_widget_text(self, widget) -> None:
        for key in ("text", "label", "title"):
            try:
                cur = widget.cget(key)
            except Exception:
                continue
            fixed = self._translate_ui_text(cur)
            if fixed != cur:
                try:
                    widget.configure(**{key: fixed})
                except Exception:
                    pass

    def _fix_notebook_tab_texts(self) -> None:
        nb = getattr(self, "notebook", None)
        if nb is None:
            return
        try:
            tabs = nb.tabs()
        except Exception:
            return
        for tab_id in tabs:
            try:
                text = nb.tab(tab_id, "text")
            except Exception:
                continue
            fixed = self._translate_ui_text(text)
            if fixed != text:
                try:
                    nb.tab(tab_id, text=fixed)
                except Exception:
                    pass

    def _refresh_language_combo_display(self):
        if not hasattr(self, 'lang_combo'):
            return
        values = []
        selected_label = None
        for label, code in self._lang_options:
            if code == 'ar':
                values.append('العربية')
            elif code == 'en':
                values.append('English')
            elif code == 'fr':
                values.append('Français')
            else:
                values.append(label)
            if code == self.current_lang:
                selected_label = values[-1]
        self.lang_combo.configure(values=values)
        if selected_label:
            self._lang_choice_var.set(selected_label)

    def _refresh_filing_type_display(self):
        if not hasattr(self, 'filing_type_combo'):
            return
        current_real = str(getattr(self, 'filing_type_var', tk.StringVar(value='10-K')).get() or '10-K')
        self._filing_display_options = [
            (self._t('filing_10k'), '10-K'),
            (self._t('filing_10ka'), '10-K/A'),
            (self._t('filing_20f'), '20-F'),
        ]
        self._filing_display_to_real = {disp: real for disp, real in self._filing_display_options}
        self._filing_real_to_display = {real: disp for disp, real in self._filing_display_options}
        self.filing_type_combo.configure(values=[x[0] for x in self._filing_display_options])
        display_value = self._filing_real_to_display.get(current_real, self._filing_real_to_display.get('10-K'))
        if display_value:
            self.filing_type_display_var.set(display_value)
        if hasattr(self, 'filing_support_label'):
            self.filing_support_label.config(text=self._t('filing_supported'))

    def _on_filing_type_changed(self, _event=None):
        selected_display = self.filing_type_display_var.get()
        selected_real = self._filing_display_to_real.get(selected_display, '10-K')
        self.filing_type_var.set(selected_real)

    def _on_translate_technical_toggle(self):
        try:
            if self.current_data:
                self.display_all()
                self.display_comparison()
        except Exception:
            pass

    def _animate_loading_indicator(self):
        if not hasattr(self, 'loading_indicator_canvas'):
            return
        canvas = self.loading_indicator_canvas
        dot = getattr(self, '_loading_dot', None)
        if dot is None:
            return
        width = max(int(canvas.winfo_width()), 300)
        if self._loading_anim_visible:
            canvas.itemconfigure(dot, state='normal')
        else:
            canvas.itemconfigure(dot, state='hidden')
        self._loading_anim_visible = not self._loading_anim_visible

        self._loading_anim_pos += (5 * self._loading_anim_dir)
        if self._loading_anim_pos >= (width - 12):
            self._loading_anim_pos = width - 12
            self._loading_anim_dir = -1
        elif self._loading_anim_pos <= 2:
            self._loading_anim_pos = 2
            self._loading_anim_dir = 1
        canvas.coords(dot, self._loading_anim_pos, 2, self._loading_anim_pos + 8, 10)
        self._loading_anim_job = self.root.after(80, self._animate_loading_indicator)

    def _start_loading_indicator(self):
        if not hasattr(self, 'loading_indicator_canvas'):
            return
        if self._loading_anim_job is not None:
            return
        self._loading_anim_pos = 2
        self._loading_anim_dir = 1
        self._loading_anim_visible = True
        self.loading_indicator_canvas.itemconfigure(self._loading_dot, state='normal')
        self._animate_loading_indicator()

    def _stop_loading_indicator(self):
        if hasattr(self, 'root') and self._loading_anim_job is not None:
            try:
                self.root.after_cancel(self._loading_anim_job)
            except Exception:
                pass
        self._loading_anim_job = None
        if hasattr(self, 'loading_indicator_canvas'):
            self.loading_indicator_canvas.itemconfigure(self._loading_dot, state='hidden')

    def _polish_arabic_layout(self, root_widget) -> None:
        def _is_arabic(s):
            return isinstance(s, str) and any("\u0600" <= ch <= "\u06ff" for ch in s)

        for widget in [root_widget] + list(root_widget.winfo_children()):
            stack = [widget]
            while stack:
                w = stack.pop()
                stack.extend(w.winfo_children())
                try:
                    txt = w.cget("text")
                except Exception:
                    txt = None
                # Do not force anchor/justify on buttons; it can clip long Arabic labels.
                if isinstance(w, (tk.Button, ttk.Button)):
                    continue
                if _is_arabic(txt):
                    cfg = {}
                    try:
                        if str(w.cget("justify")) != "right":
                            cfg["justify"] = "right"
                    except Exception:
                        pass
                    try:
                        if str(w.cget("anchor")) in ("w", "center"):
                            cfg["anchor"] = "e"
                    except Exception:
                        pass
                    if cfg:
                        try:
                            w.configure(**cfg)
                        except Exception:
                            pass

    def _normalize_line_item_key(self, item_name: str) -> str:
        txt = self._decode_mojibake_text(str(item_name or "")).strip().lower()
        txt = txt.replace("\u200f", "").replace("\u200e", "")
        txt = txt.replace("’", "'")
        txt = re.sub(r"\s+", " ", txt)
        txt = re.sub(r"\s*[-–—]\s*", "-", txt)
        txt = re.sub(r"\s*\(\s*", " (", txt)
        txt = re.sub(r"\s*\)\s*", ")", txt)
        return txt.strip()

    def _semantic_line_item_key(self, item_name: str) -> str:
        """
        Normalize a line item into a language-agnostic semantic key.
        Ensures Arabic/English/French variants of the same concept merge into one row.
        """
        raw = self._decode_mojibake_text(str(item_name or "")).strip()
        if not raw:
            return ''
        # Keep technical tags isolated to avoid over-merging XBRL-specific concepts.
        if self._is_technical_label(raw):
            return f"tech::{self._normalize_line_item_key(raw)}"

        base = re.sub(r'[:：]\s*$', '', raw).strip()
        nk = self._normalize_term_for_lookup(base)
        if nk in (self._term_reverse_to_en or {}):
            return f"sem::{self._term_reverse_to_en[nk]}"

        # Try phrase translation path for partially translated/free-text labels.
        try:
            as_en = self._normalize_term_for_lookup(self._smart_translate_financial_phrase(base))
            if as_en in (self._term_reverse_to_en or {}):
                return f"sem::{self._term_reverse_to_en[as_en]}"
        except Exception:
            pass

        return f"raw::{self._normalize_line_item_key(base)}"

    def _prefer_display_label(self, current_label: str, candidate_label: str) -> str:
        cur = re.sub(r"\s+", " ", str(current_label or "")).strip()
        cand = re.sub(r"\s+", " ", str(candidate_label or "")).strip()
        if not cur:
            return cand
        if not cand:
            return cur

        def _score(lbl: str):
            return (
                1 if lbl[:1].isupper() else 0,
                1 if any(ch.isupper() for ch in lbl) else 0,
                -lbl.count("_"),
                -lbl.count(":"),
                -lbl.count("  "),
                len(lbl),
            )

        return cand if _score(cand) > _score(cur) else cur

    def _debug_ui_contracts_enabled(self) -> bool:
        return str(os.getenv('SEC_DEBUG_UI_CONTRACTS', '')).strip() == '1'

    def _strict_ui_merge_enabled(self) -> bool:
        """
        Safe-mode UI merge:
        - default ON (strict) to avoid semantic over-merge pollution.
        - set SEC_STRICT_UI_MERGE=0 to re-enable aggressive fuzzy merge.
        """
        v = str(os.getenv('SEC_STRICT_UI_MERGE', '1')).strip().lower()
        return v not in ('0', 'false', 'off', 'no')

    def _anchored_semantic_key(self, item_name: str) -> str:
        """
        Keep core accounting anchors isolated from fuzzy semantic merge.
        This prevents accidental collapse (e.g., AssetsCurrent with Assets).
        """
        raw = self._decode_mojibake_text(str(item_name or "")).strip()
        if not raw:
            return ''
        # Technical label bridge: map common XBRL-style tags to stable canonical terms.
        tech_bridge = {
            'grossprofit': 'gross profit',
            'earningspersharebasic': 'basic eps',
            'earningspersharediluted': 'diluted eps',
            'totalassets': 'assets',
            'assets': 'assets',
            'totalliabilities': 'liabilities',
            'liabilities': 'liabilities',
            'totalequity': 'stockholders equity',
            'stockholdersequity': 'stockholders equity',
            'accountsreceivable': 'accounts receivable',
            'accountsreceivablenetcurrent': 'accounts receivable',
            'accountspayable': 'accounts payable',
            'accountspayablecurrent': 'accounts payable',
            'inventorynet': 'inventory',
            'assetscurrent': 'current assets',
            'liabilitiescurrent': 'current liabilities',
            'netincomeloss': 'net income',
            'netincome': 'net income',
            'operatingincomeloss': 'operating income',
            'operatingincome': 'operating income',
            'operatingcashflow': 'operating cash flow',
            'costofrevenue': 'cost of revenue',
            'revenues': 'revenues',
        }

        raw_compact = re.sub(r'[^A-Za-z0-9\u0600-\u06FF]+', '', raw).lower()
        bridged = tech_bridge.get(raw_compact, raw)
        norm = self._normalize_term_for_lookup(bridged)

        # If lookup has an exact canonical dictionary mapping, force semantic key.
        rev = (self._term_reverse_to_en or {})
        if norm in rev:
            return f"sem::{rev[norm]}"

        anchor_terms = {
            'revenues',
            'cost of revenue',
            'net income',
            'assets',
            'liabilities',
            'stockholders equity',
            'current assets',
            'current liabilities',
        }
        if norm in anchor_terms:
            return f"anchor::{norm}"
        return self._semantic_line_item_key(bridged)

    def _safe_merge_key_for_label(self, label: str, *, allow_anchor_for_free_text: bool = False) -> str:
        """
        Conservative semantic merge key:
        - always keep technical/XBRL labels isolated;
        - only anchor-merge free-text labels when explicitly allowed;
        - default to raw-key merge to avoid collapsing parent totals with sub-lines.
        """
        raw = self._decode_mojibake_text(str(label or "")).strip()
        if not raw:
            return ""
        if self._is_technical_label(raw):
            return f"tech::{self._normalize_line_item_key(raw)}"
        if allow_anchor_for_free_text:
            return self._anchored_semantic_key(raw)
        return f"raw::{self._normalize_line_item_key(raw)}"

    def _is_internal_helper_label(self, label: str) -> bool:
        l = str(label or '').lower()
        return any(mark in l for mark in (
            '_hierarchy',
            '_parentpreferred',
            '_parent',
            '_legacy_conflicted',
            '_hierarchyconflict',
            '__canonical_',
            '_source_tag',
            '[abstract]',
            '[line items]',
            '_abstract',
            '_lineitems',
            'legacy current',
        ))

    def _refresh_sec_view_mode_display(self):
        if not hasattr(self, 'sec_view_mode_combo'):
            return
        current_mode = getattr(self, '_sec_view_mode_var', tk.StringVar(value='official')).get() or 'official'
        options = [
            (self._t('sec_view_mode_official'), 'official'),
            (self._t('sec_view_mode_canonical'), 'canonical'),
        ]
        self._sec_view_display_to_real = {disp: real for disp, real in options}
        self._sec_view_real_to_display = {real: disp for disp, real in options}
        self.sec_view_mode_combo.configure(values=[x[0] for x in options])
        self.sec_view_mode_display_var.set(self._sec_view_real_to_display.get(current_mode, options[0][0]))

    def _get_selected_sec_view_mode(self) -> str:
        display = self.sec_view_mode_display_var.get() if hasattr(self, 'sec_view_mode_display_var') else ''
        mode = (getattr(self, '_sec_view_display_to_real', {}) or {}).get(display)
        return mode or 'official'

    def _assert_no_legacy_ratio_keys(self, ratios_by_year: dict) -> None:
        if not self._debug_ui_contracts_enabled():
            return
        banned = {'ROE', 'DSO', 'CCC', 'P_B', 'SGR'}
        for _, row in (ratios_by_year or {}).items():
            if not isinstance(row, dict):
                continue
            hit = banned.intersection(set(row.keys()))
            if hit:
                raise AssertionError(f"Legacy raw ratio keys blocked in UI path: {sorted(hit)}")

    def _remove_selected_companies(self):
        sel = list(self.companies_listbox.curselection())
        if not sel:
            messagebox.showinfo(self._t('msg_info'), self._translate_ui_text("اختر شركة واحدة أو أكثر للحذف"))
            return
        removed = []
        for idx in reversed(sel):
            name = self.companies_listbox.get(idx)
            removed.append(name)
            self.companies_listbox.delete(idx)
            if name in self.multi_company_data:
                del self.multi_company_data[name]
            if self.current_data and self.current_data.get('company_info', {}).get('ticker') == name:
                self.current_data = None
        messagebox.showinfo(self._t('msg_success'), self._translate_ui_text(f"تم حذف: {', '.join(removed)}"))
        self.display_all()

    def _on_company_select(self, event=None):
        """
        âœ… Ù…Ø¹Ø§Ù„Ø¬ ØªØºÙŠÙŠØ± Ø§Ø®ØªÙŠØ§Ø± Ø§Ù„Ø´Ø±ÙƒØ© - ÙŠØ­Ø¯Ø« Ø§Ù„Ø¹Ø±Ø¶ ØªÙ„Ù‚Ø§Ø¦ÙŠØ§Ù‹
        """
        sel = self.companies_listbox.curselection()
        if not sel:
            return
        
        # Ø§Ù„Ø­ØµÙˆÙ„ Ø¹Ù„Ù‰ Ø§Ù„Ø´Ø±ÙƒØ© Ø§Ù„Ù…Ø­Ø¯Ø¯Ø© (Ø£ÙˆÙ„ Ø´Ø±ÙƒØ© Ù…Ø­Ø¯Ø¯Ø©)
        idx = sel[0]
        company_name = self.companies_listbox.get(idx)
        
        # ØªØ­Ø¯ÙŠØ« Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø­Ø§Ù„ÙŠØ©
        if company_name in self.multi_company_data:
            self.current_data = self.multi_company_data[company_name]
            
            # ØªØ­Ø¯ÙŠØ« Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø¹Ø±ÙˆØ¶ Ø¨Ù…Ø§ ÙÙŠÙ‡Ø§ Ø§Ù„ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ø°ÙƒÙŠ
            self.display_all()

    def _build_ai_analysis_tab(self):
        """
        ðŸ¤– Ø¨Ù†Ø§Ø¡ ØªØ¨ÙˆÙŠØ¨ Ø§Ù„ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ø°ÙƒÙŠ AI
        ÙŠØ­ØªÙˆÙŠ Ø¹Ù„Ù‰ 5 Ù…Ø¤Ø´Ø±Ø§Øª Ø°ÙƒÙŠØ©
        """
        # Ø¹Ù†ÙˆØ§Ù†
        header = tk.Label(self.ai_analysis_tab, 
                         text=self._t('ai_header'),
                         bg='#1f77b4', fg='white', 
                         font=FONTS['title'], pady=10)
        header.pack(fill='x')
        
        # ÙˆØµÙ
        desc = tk.Label(self.ai_analysis_tab,
                       text=self._t('ai_desc'),
                       bg=PALETTE['panel'], font=FONTS['normal'], fg='#666')
        desc.pack(fill='x', padx=10, pady=5)
        
        # Ø¥Ø·Ø§Ø± Ù‚Ø§Ø¨Ù„ Ù„Ù„ØªÙ…Ø±ÙŠØ±
        canvas = tk.Canvas(self.ai_analysis_tab, bg=PALETTE['panel'])
        scrollbar = ttk.Scrollbar(self.ai_analysis_tab, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg=PALETTE['panel'])
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        # 1ï¸âƒ£ AI Fraud Probability
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        fraud_frame = tk.LabelFrame(scrollable_frame, text=self._t('ai_fraud_frame'),
                                   bg='#fff3cd', font=FONTS['label'], padx=15, pady=10)
        fraud_frame.pack(fill='x', padx=10, pady=10)
        
        self.fraud_prob_label = tk.Label(fraud_frame, text=self._t('ai_fraud_prob'),
                                        bg='#fff3cd', font=FONTS['title'], fg='#856404')
        self.fraud_prob_label.pack(anchor='w')
        
        self.fraud_flags_label = tk.Label(fraud_frame, text=self._t('ai_fraud_flags'),
                                         bg='#fff3cd', font=FONTS['normal'])
        self.fraud_flags_label.pack(anchor='w')
        
        self.fraud_level_label = tk.Label(fraud_frame, text=self._t('ai_risk_level'),
                                         bg='#fff3cd', font=FONTS['normal'])
        self.fraud_level_label.pack(anchor='w')
        
        self.fraud_recommendation_label = tk.Label(fraud_frame, text=self._t('ai_recommendation'),
                                                  bg='#fff3cd', font=FONTS['normal'], fg='#004085')
        self.fraud_recommendation_label.pack(anchor='w')
        
        tk.Label(fraud_frame,
                text=self._t('ai_fraud_hint'),
                bg='#fff3cd', font=FONTS['normal'], fg='#666').pack(anchor='w', pady=(5,0))
        
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        # 2ï¸âƒ£ Dynamic Failure Prediction
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        failure_frame = tk.LabelFrame(scrollable_frame, text=self._t('ai_failure_frame'),
                                     bg='#f8d7da', font=FONTS['label'], padx=15, pady=10)
        failure_frame.pack(fill='x', padx=10, pady=10)
        
        self.failure_3y_label = tk.Label(failure_frame, text=self._t('ai_failure_3y'),
                                        bg='#f8d7da', font=FONTS['title'], fg='#721c24')
        self.failure_3y_label.pack(anchor='w')
        
        self.failure_5y_label = tk.Label(failure_frame, text=self._t('ai_failure_5y'),
                                        bg='#f8d7da', font=FONTS['normal'])
        self.failure_5y_label.pack(anchor='w')
        
        self.failure_risk_label = tk.Label(failure_frame, text=self._t('ai_risk_level'),
                                          bg='#f8d7da', font=FONTS['normal'])
        self.failure_risk_label.pack(anchor='w')
        
        self.failure_concerns_label = tk.Label(failure_frame, text=self._t('ai_main_concerns'),
                                              bg='#f8d7da', font=FONTS['normal'], wraplength=600, justify='left')
        self.failure_concerns_label.pack(anchor='w')
        
        tk.Label(failure_frame,
                text=self._t('ai_failure_hint'),
                bg='#f8d7da', font=FONTS['normal'], fg='#666').pack(anchor='w', pady=(5,0))
        
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        # 3ï¸âƒ£ Growth Sustainability Grade
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        growth_frame = tk.LabelFrame(scrollable_frame, text=self._t('ai_growth_frame'),
                                    bg='#d1ecf1', font=FONTS['label'], padx=15, pady=10)
        growth_frame.pack(fill='x', padx=10, pady=10)
        
        self.growth_score_label = tk.Label(
            growth_frame,
            text=self._t('ai_score'),
            bg='#d1ecf1',
            font=(FONTS['title'][0], 16, 'bold'),
            fg='#0c5460',
        )
        self.growth_score_label.pack(anchor='w')
        
        self.growth_grade_label = tk.Label(growth_frame, text=self._t('ai_grade'),
                                          bg='#d1ecf1', font=FONTS['title'])
        self.growth_grade_label.pack(anchor='w')
        
        self.growth_assessment_label = tk.Label(growth_frame, text=self._t('ai_assessment'),
                                               bg='#d1ecf1', font=FONTS['normal'])
        self.growth_assessment_label.pack(anchor='w')
        
        self.growth_warning_label = tk.Label(growth_frame, text="",
                                            bg='#d1ecf1', font=FONTS['normal'], fg='#d9534f')
        self.growth_warning_label.pack(anchor='w')
        
        tk.Label(growth_frame,
                text=self._t('ai_growth_hint'),
                bg='#d1ecf1', font=FONTS['normal'], fg='#666').pack(anchor='w', pady=(5,0))
        
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        # 4ï¸âƒ£ AI Working Capital Analysis
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        wc_frame = tk.LabelFrame(scrollable_frame, text=self._t('ai_wc_frame'),
                                bg='#fff3cd', font=FONTS['label'], padx=15, pady=10)
        wc_frame.pack(fill='x', padx=10, pady=10)
        
        self.wc_crisis_prob_label = tk.Label(wc_frame, text=self._t('ai_wc_crisis'),
                                            bg='#fff3cd', font=FONTS['title'], fg='#856404')
        self.wc_crisis_prob_label.pack(anchor='w')
        
        self.wc_ccc_label = tk.Label(wc_frame, text=self._t('ai_wc_ccc'),
                                    bg='#fff3cd', font=FONTS['normal'])
        self.wc_ccc_label.pack(anchor='w')
        
        self.wc_trend_label = tk.Label(wc_frame, text=self._t('ai_wc_trend'),
                                      bg='#fff3cd', font=FONTS['normal'])
        self.wc_trend_label.pack(anchor='w')
        
        self.wc_recommendation_label = tk.Label(wc_frame, text=self._t('ai_recommendation'),
                                               bg='#fff3cd', font=FONTS['normal'], fg='#004085')
        self.wc_recommendation_label.pack(anchor='w')
        
        tk.Label(wc_frame,
                text=self._t('ai_wc_hint'),
                bg='#fff3cd', font=FONTS['normal'], fg='#666').pack(anchor='w', pady=(5,0))
        
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        # 5ï¸âƒ£ AI Investment Quality Score
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        quality_frame = tk.LabelFrame(scrollable_frame, text=self._t('ai_quality_frame'),
                                     bg='#d4edda', font=FONTS['label'], padx=15, pady=10)
        quality_frame.pack(fill='x', padx=10, pady=10)
        
        self.quality_score_label = tk.Label(
            quality_frame,
            text=self._t('ai_quality_score'),
            bg='#d4edda',
            font=(FONTS['title'][0], 18, 'bold'),
            fg='#155724',
        )
        self.quality_score_label.pack(anchor='w')
        
        self.quality_verdict_label = tk.Label(quality_frame, text=self._t('ai_verdict'),
                                             bg='#d4edda', font=FONTS['title'])
        self.quality_verdict_label.pack(anchor='w')
        
        self.quality_action_label = tk.Label(quality_frame, text=self._t('ai_invest_action'),
                                            bg='#d4edda', font=FONTS['normal'], fg='#004085')
        self.quality_action_label.pack(anchor='w')
        
        self.quality_percentile_label = tk.Label(quality_frame, text=self._t('ai_percentile'),
                                                bg='#d4edda', font=FONTS['normal'])
        self.quality_percentile_label.pack(anchor='w')
        
        tk.Label(quality_frame,
                text=self._t('ai_quality_hint'),
                bg='#d4edda', font=FONTS['normal'], fg='#666').pack(anchor='w', pady=(5,0))
        
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        # Ø£Ø²Ø±Ø§Ø± Ø§Ù„ØªØ­ÙƒÙ…
        # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        controls_frame = tk.Frame(scrollable_frame, bg=PALETTE['panel'])
        controls_frame.pack(fill='x', pady=10, padx=10)
        
        refresh_btn = tk.Button(controls_frame, text=self._t('ai_btn_refresh'),
                               font=FONTS['button'], bg='#007bff', fg='white',
                               command=self.display_ai_analysis, pady=8)
        refresh_btn.pack(side='left', padx=5)
        
        # âœ… NEW: Ø²Ø± Ø§Ù„ØªØ¯Ø±ÙŠØ¨ Ø§Ù„ÙŠØ¯ÙˆÙŠ
        train_btn = tk.Button(controls_frame, text=self._t('ai_btn_train'),
                             font=FONTS['button'], bg='#28a745', fg='white',
                             command=self._manual_train_models, pady=8)
        train_btn.pack(side='left', padx=5)
        
        # âœ… NEW: Ø¹Ø±Ø¶ Ø¥Ø­ØµØ§Ø¦ÙŠØ§Øª Ø§Ù„ØªØ¯Ø±ÙŠØ¨
        stats_btn = tk.Button(controls_frame, text=self._t('ai_btn_stats'),
                             font=FONTS['button'], bg='#17a2b8', fg='white',
                             command=self._show_training_stats, pady=8)
        stats_btn.pack(side='left', padx=5)
        
        # ØªØ¹Ø¨Ø¦Ø©
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def _clear_companies(self):
        if messagebox.askyesno(self._t('msg_confirm'), self._translate_ui_text("هل تريد مسح جميع الشركات من القائمة؟")):
            self.companies_listbox.delete(0, tk.END)
            self.multi_company_data.clear()
            self.current_data = None
            self.display_all()

    # ---------- Fetch ----------
    def fetch_data(self):
        companies = list(self.companies_listbox.get(0, tk.END))
        if not companies:
            messagebox.showerror(self._t('msg_error'), self._translate_ui_text("أضف شركة واحدة على الأقل"))
            return
        try:
            start_year = int(self.start_year_var.get())
            end_year = int(self.end_year_var.get())
        except:
            messagebox.showerror(self._t('msg_error'), self._translate_ui_text("السنوات غير صحيحة"))
            return
        if start_year > end_year:
            messagebox.showerror(self._t('msg_error'), self._translate_ui_text("سنة البداية يجب أن تكون ≤ سنة النهاية"))
            return
        filing_type = self.filing_type_var.get()
        if filing_type != '10-K':
            messagebox.showwarning(self._t('msg_warning'), self._t('filing_warn_unsupported'))
            filing_type = '10-K'
            self.filing_type_var.set('10-K')
            if hasattr(self, 'filing_type_display_var') and hasattr(self, '_filing_real_to_display'):
                self.filing_type_display_var.set(self._filing_real_to_display.get('10-K', '10-K'))
        if self.current_lang == 'ar':
            self.progress_label.config(text=f"بدء التحميل الآلي للبيانات | نوع التقرير: {filing_type} | الفترة: {start_year}-{end_year}")
        elif self.current_lang == 'fr':
            self.progress_label.config(text=f"Démarrage du chargement automatique | Type: {filing_type} | Période: {start_year}-{end_year}")
        else:
            self.progress_label.config(text=f"Starting auto data loading | Filing: {filing_type} | Period: {start_year}-{end_year}")
        self.progress_bar.start()
        self._start_loading_indicator()

        def cb(msg):
            self.root.after(0, lambda: self.progress_label.config(text=msg))

        def worker():
            try:
                for idx, comp in enumerate(companies, start=1):
                    cb(self._translate_ui_text(f"({idx}/{len(companies)}) جلب: {comp} ..."))
                    res = self.fetcher.fetch_company_data(comp, start_year, end_year, filing_type, callback=cb, include_all_concepts=True)
                    if res.get('success'):
                        t = res['company_info'].get('ticker') or comp
                        self.multi_company_data[t] = res
                        self.current_data = res

                        self.root.after(0, self.display_all)
                    else:
                        cb(self._translate_ui_text(f"❌ فشل {comp}: {res.get('error')}"))
                    time.sleep(0.5)
            finally:
                self.root.after(0, lambda: self.progress_bar.stop())
                self.root.after(0, lambda: self._stop_loading_indicator())
                self.root.after(0, lambda: self.progress_label.config(text=self._t('progress_done')))

        threading.Thread(target=worker, daemon=True).start()

    def display_ai_analysis(self):
        """
        ðŸ¤– Ø¹Ø±Ø¶ Ø§Ù„ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ø°ÙƒÙŠ - 5 Ù…Ø¤Ø´Ø±Ø§Øª Ù…ØªÙ‚Ø¯Ù…Ø©
        """
        print("ðŸ¤– [DEBUG] display_ai_analysis called")
        
        if not self.current_data:
            print("âš ï¸ [DEBUG] No current_data available")
            # Ù…Ø³Ø­ Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø¹Ø±ÙˆØ¶
            self.fraud_prob_label.config(text=self._t('ai_fraud_prob').replace('--', '-- (N/A)'))
            self.fraud_flags_label.config(text=self._t('ai_fraud_flags'))
            self.fraud_level_label.config(text=self._t('ai_risk_level'))
            self.fraud_recommendation_label.config(text=self._t('ai_recommendation'))

            self.failure_3y_label.config(text=self._t('ai_failure_3y'))
            self.failure_5y_label.config(text=self._t('ai_failure_5y'))
            self.failure_risk_label.config(text=self._t('ai_risk_level'))
            self.failure_concerns_label.config(text=self._t('ai_main_concerns'))

            self.growth_score_label.config(text=self._t('ai_score'))
            self.growth_grade_label.config(text=self._t('ai_grade'))
            self.growth_assessment_label.config(text=self._t('ai_assessment'))
            self.growth_warning_label.config(text="")

            self.wc_crisis_prob_label.config(text=self._t('ai_wc_crisis'))
            self.wc_ccc_label.config(text=self._t('ai_wc_ccc'))
            self.wc_trend_label.config(text=self._t('ai_wc_trend'))
            self.wc_recommendation_label.config(text=self._t('ai_recommendation'))

            self.quality_score_label.config(text=self._t('ai_quality_score'))
            self.quality_verdict_label.config(text=self._t('ai_verdict'))
            self.quality_action_label.config(text=self._t('ai_invest_action'))
            self.quality_percentile_label.config(text=self._t('ai_percentile'))
            return
        
        try:
            print("âœ… [DEBUG] Importing advanced_analysis...")
            from modules.advanced_analysis import generate_ai_insights
            
            data_by_year_raw = ((self.current_data.get('data_layers', {}) or {}).get('layer1_by_year') or self.current_data.get('data_by_year', {}))
            ratios_by_year_raw = maybe_guard_ratios_by_year(self.current_data.get('financial_ratios', {}))
            data_by_year = {}
            for yk, row in (data_by_year_raw or {}).items():
                try:
                    ky = int(yk)
                except Exception:
                    continue
                data_by_year[ky] = row or {}
            ratios_by_year = {}
            for yk, row in (ratios_by_year_raw or {}).items():
                try:
                    ky = int(yk)
                except Exception:
                    continue
                ratios_by_year[ky] = row or {}
            ticker = (self.current_data.get('company_info', {}) or {}).get('ticker', 'CURRENT')
            ratio_source = UnifiedRatioSource()
            ratio_source.load(ticker, data_by_year, ratios_by_year)
            
            print(f"ðŸ“Š [DEBUG] data_by_year keys: {list(data_by_year.keys())}")
            print(f"ðŸ“Š [DEBUG] ratios_by_year keys: {list(ratios_by_year.keys())}")
            
            if not data_by_year or not ratios_by_year:
                print("âš ï¸ [DEBUG] Missing data_by_year or ratios_by_year")
                self.fraud_prob_label.config(text=self._t('ai_fraud_prob').replace('--', '-- (N/A)'))
                return
            
            # Ø¬Ù…Ø¹ Ø§Ù„Ù…Ù‚Ø§ÙŠÙŠØ³ Ø§Ù„Ù…Ø·Ù„ÙˆØ¨Ø©
            years = sorted([y for y in data_by_year.keys() if isinstance(y, int)])
            if not years:
                return
            
            latest_year = years[-1]
            investment_score = ratio_source.get_ratio_contract(ticker, latest_year, 'investment_score').get('value')
            if investment_score is None:
                investment_score = 50
            economic_spread = ratio_source.get_ratio_contract(ticker, latest_year, 'economic_spread').get('value')
            if economic_spread is None:
                roic = ratio_source.get_ratio_contract(ticker, latest_year, 'roic').get('value')
                wacc = ratio_source.get_ratio_contract(ticker, latest_year, 'wacc').get('value')
                if roic and wacc:
                    economic_spread = roic - wacc
                else:
                    economic_spread = 0.0
            fcf_yield = ratio_source.get_ratio_contract(ticker, latest_year, 'fcf_yield').get('value')
            if fcf_yield is None:
                fcf = ratio_source.get_ratio_contract(ticker, latest_year, 'free_cash_flow').get('value')
                market_cap = ratio_source.get_ratio_contract(ticker, latest_year, 'market_cap').get('value')
                if fcf and market_cap and market_cap != 0:
                    fcf_yield = (fcf / market_cap)
                else:
                    fcf_yield = 0.0
            
            # âœ… ØªÙˆÙ„ÙŠØ¯ Ø§Ù„ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ø°ÙƒÙŠ
            print("ðŸ”„ [DEBUG] Calling generate_ai_insights...")
            
            # âœ… NEW: Ø§Ø³ØªØ®Ø¯Ø§Ù… Ø§Ù„Ù†Ù…Ø§Ø°Ø¬ Ø§Ù„Ù…Ø¯Ø±Ø¨Ø© Ø¥Ø°Ø§ ÙƒØ§Ù†Øª Ù…ØªÙˆÙØ±Ø©
            enhanced_insights = None
            if self.ml_trainer and self.ml_trainer.trained_models:
                try:
                    print("ðŸ¤– [DEBUG] Ø§Ø³ØªØ®Ø¯Ø§Ù… Ø§Ù„Ù†Ù…Ø§Ø°Ø¬ Ø§Ù„Ù…Ø¯Ø±Ø¨Ø© Ù„Ù„ØªØ­Ø³ÙŠÙ†...")
                    features = self.ml_trainer._extract_features(data_by_year, ratios_by_year)
                    
                    # ØªØ­Ø³ÙŠÙ† Ø§Ø­ØªÙ…Ø§Ù„ÙŠØ© Ø§Ù„Ø§Ø­ØªÙŠØ§Ù„
                    ml_fraud_prob = self.ml_trainer.predict_fraud_probability(features)
                    if ml_fraud_prob is not None:
                        print(f"   ðŸ“Š Ø§Ø­ØªÙ…Ø§Ù„ÙŠØ© Ø§Ù„Ø§Ø­ØªÙŠØ§Ù„ Ù…Ù† ML: {ml_fraud_prob*100:.1f}%")
                        enhanced_insights = {'ml_fraud_probability': ml_fraud_prob}
                except Exception as e:
                    print(f"âš ï¸ Ø®Ø·Ø£ ÙÙŠ Ø§Ø³ØªØ®Ø¯Ø§Ù… Ø§Ù„Ù†Ù…Ø§Ø°Ø¬ Ø§Ù„Ù…Ø¯Ø±Ø¨Ø©: {e}")
            
            insights = generate_ai_insights(
                data_by_year=data_by_year,
                ratios_by_year=ratios_by_year,
                investment_score=investment_score,
                economic_spread=economic_spread,
                fcf_yield=fcf_yield
            )
            
            # âœ… Ø¯Ù…Ø¬ Ø§Ù„Ù†ØªØ§Ø¦Ø¬ Ø§Ù„Ù…Ø­Ø³Ù‘Ù†Ø© Ù…Ù† ML
            if enhanced_insights and 'ml_fraud_probability' in enhanced_insights:
                ml_prob = enhanced_insights['ml_fraud_probability']
                rule_prob = insights['fraud_detection']['fraud_probability']
                # Ø§Ø³ØªØ®Ø¯Ø§Ù… Ù…ØªÙˆØ³Ø· Ù…Ø±Ø¬Ø­: 70% ML + 30% Rules
                insights['fraud_detection']['fraud_probability'] = (ml_prob * 0.7) + (rule_prob * 0.3)
                insights['fraud_detection']['using_ml'] = True
                print(f"   âœ… ØªÙ… Ø¯Ù…Ø¬ Ù†ØªØ§Ø¦Ø¬ ML: {insights['fraud_detection']['fraud_probability']*100:.1f}%")
            
            print(f"âœ… [DEBUG] AI Insights generated successfully!")
            print(f"   Keys: {list(insights.keys())}")
            
            # âœ… NEW: Ø­ÙØ¸ Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ù„Ù„ØªØ¯Ø±ÙŠØ¨ Ø§Ù„Ù…Ø³ØªÙ…Ø±
            if self.ml_trainer:
                try:
                    ticker = self.current_data.get('company_info', {}).get('ticker', 'UNKNOWN')
                    self.ml_trainer.collect_company_data(
                        ticker=ticker,
                        data_by_year=data_by_year,
                        ratios_by_year=ratios_by_year,
                        ai_results=insights
                    )
                    
                    # Ù…Ø­Ø§ÙˆÙ„Ø© ØªØ¯Ø±ÙŠØ¨ ØªÙ„Ù‚Ø§Ø¦ÙŠ Ø¥Ø°Ø§ ÙˆØµÙ„Ù†Ø§ Ù„Ù„Ø­Ø¯ Ø§Ù„Ù…Ø·Ù„ÙˆØ¨
                    from modules.ml_trainer import auto_train_if_needed
                    if auto_train_if_needed(self.ml_trainer, threshold=20):
                        print("ðŸŽ“ ØªÙ… Ø§Ù„ØªØ¯Ø±ÙŠØ¨ Ø§Ù„ØªÙ„Ù‚Ø§Ø¦ÙŠ Ø¨Ù†Ø¬Ø§Ø­!")
                except Exception as e:
                    print(f"âš ï¸ Ø®Ø·Ø£ ÙÙŠ Ø­ÙØ¸ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„ØªØ¯Ø±ÙŠØ¨: {e}")
            
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            # 1ï¸âƒ£ Ø¹Ø±Ø¶ AI Fraud Probability
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            fraud = insights.get('fraud_detection', {})
            fraud_prob = fraud.get('fraud_probability', 0) * 100
            
            self.fraud_prob_label.config(text=self._t('ai_fraud_prob').replace('--', f"{fraud_prob:.1f}%"))
            self.fraud_flags_label.config(text=self._t('ai_fraud_flags').replace('--', str(fraud.get('red_flags_count', 0))))
            self.fraud_level_label.config(text=self._t('ai_risk_level').replace('--', str(fraud.get('risk_level', '--'))))
            self.fraud_recommendation_label.config(text=self._t('ai_recommendation').replace('--', str(fraud.get('recommendation', '--'))))
            
            # Ù„ÙˆÙ† Ø­Ø³Ø¨ Ø§Ù„Ù…Ø®Ø§Ø·Ø±
            risk_level = fraud.get('risk_level', '')
            if risk_level == 'Ù…Ù†Ø®ÙØ¶':
                self.fraud_prob_label.config(fg='#28a745')
            elif risk_level == 'Ù…ØªÙˆØ³Ø·':
                self.fraud_prob_label.config(fg='#ffc107')
            else:
                self.fraud_prob_label.config(fg='#dc3545')
            
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            # 2ï¸âƒ£ Ø¹Ø±Ø¶ Dynamic Failure Prediction
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            failure = insights.get('failure_prediction', {})
            failure_3y = failure.get('failure_prob_3y', 0) * 100
            failure_5y = failure.get('failure_prob_5y', 0) * 100
            
            self.failure_3y_label.config(text=self._t('ai_failure_3y').replace('--', f"{failure_3y:.1f}%"))
            self.failure_5y_label.config(text=self._t('ai_failure_5y').replace('--', f"{failure_5y:.1f}%"))
            self.failure_risk_label.config(text=self._t('ai_risk_level').replace('--', str(failure.get('risk_level', '--'))))
            
            concerns = failure.get('key_concerns', [])
            concerns_text = "ØŒ ".join(concerns) if concerns else "Ù„Ø§ ØªÙˆØ¬Ø¯ Ù…Ø®Ø§ÙˆÙ ÙƒØ¨ÙŠØ±Ø©"
            self.failure_concerns_label.config(text=self._t('ai_main_concerns').replace('--', concerns_text))
            
            # Ù„ÙˆÙ† Ø­Ø³Ø¨ Ø§Ù„Ù…Ø®Ø§Ø·Ø±
            f_risk = failure.get('risk_level', '')
            if f_risk == 'Ù…Ù†Ø®ÙØ¶':
                self.failure_3y_label.config(fg='#28a745')
            elif f_risk == 'Ù…ØªÙˆØ³Ø·':
                self.failure_3y_label.config(fg='#ffc107')
            else:
                self.failure_3y_label.config(fg='#dc3545')
            
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            # 3ï¸âƒ£ Ø¹Ø±Ø¶ Growth Sustainability Grade
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            growth = insights.get('growth_sustainability', {})
            g_score = growth.get('sustainability_score', 0)
            g_grade = growth.get('grade', '--')
            g_assessment = growth.get('assessment', '--')
            g_warning = growth.get('debt_warning', False)
            
            self.growth_score_label.config(text=self._t('ai_score').replace('--', str(g_score)))
            self.growth_grade_label.config(text=self._t('ai_grade').replace('--', str(g_grade)))
            self.growth_assessment_label.config(text=self._t('ai_assessment').replace('--', str(g_assessment)))
            
            if g_warning:
                self.growth_warning_label.config(text=self._translate_ui_text("⚠️ تحذير: النمو يتطلب زيادة كبيرة في الديون!"))
            else:
                self.growth_warning_label.config(text=self._translate_ui_text("✅ النمو مستدام بدون ديون خطيرة"))
            
            # Ù„ÙˆÙ† Ø­Ø³Ø¨ Ø§Ù„Ø¯Ø±Ø¬Ø©
            if g_grade in ['A', 'B']:
                self.growth_grade_label.config(fg='#28a745')
            elif g_grade == 'C':
                self.growth_grade_label.config(fg='#ffc107')
            else:
                self.growth_grade_label.config(fg='#dc3545')
            
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            # 4ï¸âƒ£ Ø¹Ø±Ø¶ AI Working Capital Analysis
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            wc = insights.get('working_capital_analysis', {})
            wc_crisis = wc.get('liquidity_crisis_prob', 0) * 100
            wc_ccc = wc.get('latest_ccc', 0)
            wc_trend = wc.get('ccc_trend', 0)
            wc_risk = wc.get('risk_level', '--')
            wc_rec = wc.get('recommendation', '--')
            
            self.wc_crisis_prob_label.config(text=self._t('ai_wc_crisis').replace('--', f"{wc_crisis:.1f}%"))
            self.wc_ccc_label.config(text=self._t('ai_wc_ccc').replace('--', f"{wc_ccc:.1f}"))
            
            if wc_trend > 0:
                self.wc_trend_label.config(text=self._t('ai_wc_trend').replace('--', self._translate_ui_text(f"⬆️ متزايد بمعدل {wc_trend:.1f} يوم/سنة")))
            elif wc_trend < 0:
                self.wc_trend_label.config(text=self._t('ai_wc_trend').replace('--', self._translate_ui_text(f"⬇️ تحسن بمعدل {abs(wc_trend):.1f} يوم/سنة")))
            else:
                self.wc_trend_label.config(text=self._t('ai_wc_trend').replace('--', self._translate_ui_text("↔️ مستقر")))
            
            self.wc_recommendation_label.config(text=self._t('ai_recommendation').replace('--', str(wc_rec)))
            
            # Ù„ÙˆÙ† Ø­Ø³Ø¨ Ø§Ù„Ù…Ø®Ø§Ø·Ø±
            if wc_risk == 'Ù…Ù†Ø®ÙØ¶':
                self.wc_crisis_prob_label.config(fg='#28a745')
            elif wc_risk == 'Ù…ØªÙˆØ³Ø·':
                self.wc_crisis_prob_label.config(fg='#ffc107')
            else:
                self.wc_crisis_prob_label.config(fg='#dc3545')
            
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            # 5ï¸âƒ£ Ø¹Ø±Ø¶ AI Investment Quality Score
            # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
            quality = insights.get('investment_quality', {})
            q_score = quality.get('quality_score', 0)
            q_verdict = quality.get('verdict', '--')
            q_action = quality.get('action', '--')
            q_percentile = quality.get('percentile', 0)
            
            self.quality_score_label.config(text=self._t('ai_quality_score').replace('--', str(q_score)))
            self.quality_verdict_label.config(text=self._t('ai_verdict').replace('--', str(q_verdict)))
            self.quality_action_label.config(text=self._t('ai_invest_action').replace('--', str(q_action)))
            self.quality_percentile_label.config(text=self._t('ai_percentile').replace('--', f"{q_percentile:.0f}"))
            
            # Ù„ÙˆÙ† Ø­Ø³Ø¨ Ø§Ù„Ù†ØªÙŠØ¬Ø©
            if q_score >= 85:
                self.quality_verdict_label.config(fg='#28a745')
            elif q_score >= 70:
                self.quality_verdict_label.config(fg='#5cb85c')
            elif q_score >= 55:
                self.quality_verdict_label.config(fg='#5bc0de')
            elif q_score >= 40:
                self.quality_verdict_label.config(fg='#ffc107')
            else:
                self.quality_verdict_label.config(fg='#dc3545')
                
        except Exception as e:
            print(f"âŒ Ø®Ø·Ø£ ÙÙŠ Ø¹Ø±Ø¶ Ø§Ù„ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ø°ÙƒÙŠ: {e}")
            import traceback
            traceback.print_exc()

    def _manual_train_models(self):
        """
        âœ… ØªØ¯Ø±ÙŠØ¨ ÙŠØ¯ÙˆÙŠ Ù„Ù„Ù†Ù…Ø§Ø°Ø¬
        """
        if not self.ml_trainer:
            messagebox.showwarning("ØªÙ†Ø¨ÙŠÙ‡", "Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¯Ø±ÙŠØ¨ ØºÙŠØ± Ù…ØªØ§Ø­")
            return
        
        if self.ml_trainer.stats['total_records'] < 10:
            messagebox.showinfo("Ù…Ø¹Ù„ÙˆÙ…Ø©", 
                              f"Ø¹Ø¯Ø¯ Ø§Ù„Ø³Ø¬Ù„Ø§Øª Ø§Ù„Ù…ØªØ§Ø­Ø©: {self.ml_trainer.stats['total_records']}\n"
                              "Ø§Ù„Ø­Ø¯ Ø§Ù„Ø£Ø¯Ù†Ù‰ Ù„Ù„ØªØ¯Ø±ÙŠØ¨: 10 Ø³Ø¬Ù„Ø§Øª\n\n"
                              "Ù‚Ù… Ø¨ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ù…Ø²ÙŠØ¯ Ù…Ù† Ø§Ù„Ø´Ø±ÙƒØ§Øª Ù„Ø¬Ù…Ø¹ Ø¨ÙŠØ§Ù†Ø§Øª ÙƒØ§ÙÙŠØ©")
            return
        
        # ØªØ£ÙƒÙŠØ¯
        if not messagebox.askyesno("ØªØ£ÙƒÙŠØ¯ Ø§Ù„ØªØ¯Ø±ÙŠØ¨",
                                  f"Ø³ÙŠØªÙ… ØªØ¯Ø±ÙŠØ¨ Ø§Ù„Ù†Ù…Ø§Ø°Ø¬ Ø¹Ù„Ù‰ {self.ml_trainer.stats['total_records']} Ø³Ø¬Ù„\n"
                                  f"Ù…Ù† {self.ml_trainer.stats['total_companies']} Ø´Ø±ÙƒØ©\n\n"
                                  "Ù‡Ù„ ØªØ±ÙŠØ¯ Ø§Ù„Ù…ØªØ§Ø¨Ø¹Ø©ØŸ"):
            return
        
        # Ø§Ù„ØªØ¯Ø±ÙŠØ¨
        try:
            print("ðŸŽ“ Ø¨Ø¯Ø¡ Ø§Ù„ØªØ¯Ø±ÙŠØ¨ Ø§Ù„ÙŠØ¯ÙˆÙŠ...")
            results = self.ml_trainer.train_models(min_samples=10)
            
            if results:
                msg = f"âœ… ØªÙ… ØªØ¯Ø±ÙŠØ¨ {len(results)} Ù†Ù…ÙˆØ°Ø¬ Ø¨Ù†Ø¬Ø§Ø­!\n\n"
                for model_name, model_data in results.items():
                    if model_data.get('samples'):
                        msg += f"â€¢ {model_name}: {model_data['samples']} Ø³Ø¬Ù„\n"
                
                msg += f"\nØ§Ù„Ù†Ù…Ø§Ø°Ø¬ Ø§Ù„Ø¢Ù† Ø³ØªÙØ³ØªØ®Ø¯Ù… Ù„ØªØ­Ø³ÙŠÙ† Ø¯Ù‚Ø© Ø§Ù„ØªÙ†Ø¨Ø¤Ø§Øª"
                messagebox.showinfo("Ù†Ø¬Ø§Ø­ Ø§Ù„ØªØ¯Ø±ÙŠØ¨", msg)
                
                # Ø¥Ø¹Ø§Ø¯Ø© Ø­Ø³Ø§Ø¨ Ø§Ù„ØªØ­Ù„ÙŠÙ„ Ø§Ù„Ø°ÙƒÙŠ Ø¨Ø§Ø³ØªØ®Ø¯Ø§Ù… Ø§Ù„Ù†Ù…Ø§Ø°Ø¬ Ø§Ù„Ø¬Ø¯ÙŠØ¯Ø©
                self.display_ai_analysis()
            else:
                messagebox.showinfo("Ù…Ø¹Ù„ÙˆÙ…Ø©", "Ù„Ø§ ØªÙˆØ¬Ø¯ Ø¨ÙŠØ§Ù†Ø§Øª ÙƒØ§ÙÙŠØ© Ù„Ù„ØªØ¯Ø±ÙŠØ¨ Ø¨Ø¹Ø¯")
                
        except Exception as e:
            messagebox.showerror("Ø®Ø·Ø£ ÙÙŠ Ø§Ù„ØªØ¯Ø±ÙŠØ¨", f"Ø­Ø¯Ø« Ø®Ø·Ø£ Ø£Ø«Ù†Ø§Ø¡ Ø§Ù„ØªØ¯Ø±ÙŠØ¨:\n{e}")
            print(f"âŒ Ø®Ø·Ø£ ÙÙŠ Ø§Ù„ØªØ¯Ø±ÙŠØ¨: {e}")
            import traceback
            traceback.print_exc()
    
    def _show_training_stats(self):
        """
        âœ… Ø¹Ø±Ø¶ Ø¥Ø­ØµØ§Ø¦ÙŠØ§Øª Ø§Ù„ØªØ¯Ø±ÙŠØ¨
        """
        if not self.ml_trainer:
            messagebox.showwarning("ØªÙ†Ø¨ÙŠÙ‡", "Ù†Ø¸Ø§Ù… Ø§Ù„ØªØ¯Ø±ÙŠØ¨ ØºÙŠØ± Ù…ØªØ§Ø­")
            return
        
        stats = self.ml_trainer.get_stats_summary()
        
        # Ø¥Ø¶Ø§ÙØ© Ù…Ø¹Ù„ÙˆÙ…Ø§Øª Ø¹Ù† Ø§Ù„Ù†Ù…Ø§Ø°Ø¬
        if self.ml_trainer.trained_models:
            stats += "\nðŸ¤– Ø§Ù„Ù†Ù…Ø§Ø°Ø¬ Ø§Ù„Ù…Ø¯Ø±Ø¨Ø©:\n"
            stats += "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n"
            for model_name, model_data in self.ml_trainer.trained_models.items():
                samples = model_data.get('samples', 0)
                model_type = model_data.get('type', 'unknown')
                stats += f"â€¢ {model_name}: {samples} Ø³Ø¬Ù„ ({model_type})\n"
        else:
            stats += "\nâš ï¸ Ù„Ù… ÙŠØªÙ… ØªØ¯Ø±ÙŠØ¨ Ø£ÙŠ Ù†Ù…Ø§Ø°Ø¬ Ø¨Ø¹Ø¯\n"
            stats += "Ù‚Ù… Ø¨ØªØ­Ù„ÙŠÙ„ 10+ Ø´Ø±ÙƒØ§Øª Ø«Ù… Ø§Ø¶ØºØ· 'ØªØ¯Ø±ÙŠØ¨ Ø§Ù„Ù†Ù…Ø§Ø°Ø¬'\n"
        
        messagebox.showinfo("Ø¥Ø­ØµØ§Ø¦ÙŠØ§Øª Ø§Ù„ØªØ¯Ø±ÙŠØ¨", stats)

    def _fill_missing_market_ratios(self, market_data):
        """
        âœ… Ø¥ÙƒÙ…Ø§Ù„ Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ù†Ø§Ù‚ØµØ© Ù…Ù† Yahoo Finance ØªÙ„Ù‚Ø§Ø¦ÙŠØ§Ù‹
        ÙŠØ³ØªØ®Ø¯Ù… Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø¥Ø¶Ø§ÙÙŠØ©: P/E, P/B, Dividend Yield Ù…Ù† yfinance
        """
        if not self.current_data or not market_data:
            return
        
        ratios_by_year = self.current_data.get('financial_ratios', {})
        data_by_year = self.current_data.get('data_by_year', {})
        
        price = market_data.get('price')
        shares_yf = market_data.get('shares')
        market_cap = market_data.get('market_cap')
        try:
            if market_cap is not None and abs(float(market_cap)) > 1_000_000_000:
                market_cap = float(market_cap) / 1_000_000.0
        except Exception:
            pass
        beta = market_data.get('beta')
        pe_yf = market_data.get('pe_ratio')
        pb_yf = market_data.get('pb_ratio')
        div_yield_yf = market_data.get('dividend_yield')
        
        if not ratios_by_year:
            return
        
        print(f"ðŸ”„ Ø¥ÙƒÙ…Ø§Ù„ Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ù†Ø§Ù‚ØµØ© Ù…Ù† Yahoo Finance...")
        filled_count = 0
        
        # Ø¥ÙƒÙ…Ø§Ù„ Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ù„ÙƒÙ„ Ø³Ù†Ø© (Ù†Ø³ØªØ®Ø¯Ù… Ø£Ø­Ø¯Ø« Ø³Ù†Ø© Ù„Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ÙŠØ©)
        latest_year = max(ratios_by_year.keys()) if ratios_by_year else None
        
        for year in ratios_by_year.keys():
            if year not in ratios_by_year:
                continue
            
            # âœ… Ø¥ÙƒÙ…Ø§Ù„ Shares Outstanding Ø¥Ø°Ø§ ÙƒØ§Ù†Øª Ù†Ø§Ù‚ØµØ©
            if year == latest_year and not ratios_by_year[year].get('shares_outstanding') and shares_yf:
                ratios_by_year[year]['shares_outstanding'] = shares_yf
                filled_count += 1
            
            # âœ… Ø¥ÙƒÙ…Ø§Ù„ Market Cap
            if year == latest_year and not ratios_by_year[year].get('market_cap') and market_cap:
                ratios_by_year[year]['market_cap'] = market_cap
                filled_count += 1
            
            # âœ… Ø¥ÙƒÙ…Ø§Ù„ Beta (Ù„Ù„Ø³Ù†Ø© Ø§Ù„Ø£Ø­Ø¯Ø« ÙÙ‚Ø· Ù„Ø£Ù† Beta Ù…ØªØºÙŠØ±)
            if year == latest_year and not ratios_by_year[year].get('beta') and beta:
                ratios_by_year[year]['beta'] = beta
                filled_count += 1
            
            # âœ… Ø¥ÙƒÙ…Ø§Ù„ P/E Ratio - Ø£ÙˆÙ„ÙˆÙŠØ© Ù„Ù„Ø­Ø³Ø§Ø¨ØŒ Ø«Ù… yfinance
            if not ratios_by_year[year].get('pe_ratio'):
                if year == latest_year and price:
                    eps = ratios_by_year[year].get('eps_basic')
                    if eps and eps != 0:
                        ratios_by_year[year]['pe_ratio'] = price / eps
                        filled_count += 1
                elif year == latest_year and pe_yf:  # Ø§Ø³ØªØ®Ø¯Ø§Ù… yfinance Ù„Ù„Ø³Ù†Ø© Ø§Ù„Ø£Ø­Ø¯Ø«
                    ratios_by_year[year]['pe_ratio'] = pe_yf
                    filled_count += 1
            
            # âœ… Ø¥ÙƒÙ…Ø§Ù„ P/B Ratio - Ø£ÙˆÙ„ÙˆÙŠØ© Ù„Ù„Ø­Ø³Ø§Ø¨ØŒ Ø«Ù… yfinance
            if not ratios_by_year[year].get('pb_ratio'):
                if year == latest_year and price:
                    bvps = ratios_by_year[year].get('book_value_per_share')
                    if bvps and bvps != 0:
                        ratios_by_year[year]['pb_ratio'] = price / bvps
                        filled_count += 1
                elif year == latest_year and pb_yf:  # Ø§Ø³ØªØ®Ø¯Ø§Ù… yfinance Ù„Ù„Ø³Ù†Ø© Ø§Ù„Ø£Ø­Ø¯Ø«
                    ratios_by_year[year]['pb_ratio'] = pb_yf
                    filled_count += 1
            
            # âœ… Ø¥ÙƒÙ…Ø§Ù„ Dividend Yield - Ø£ÙˆÙ„ÙˆÙŠØ© Ù„Ù„Ø­Ø³Ø§Ø¨ØŒ Ø«Ù… yfinance
            if not ratios_by_year[year].get('dividend_yield'):
                if year == latest_year and price:
                    dividends = ratios_by_year[year].get('dividends_paid')
                    shares = ratios_by_year[year].get('shares_outstanding') or shares_yf
                    if dividends and shares and shares != 0 and price != 0:
                        div_per_share = abs(dividends) / shares
                        ratios_by_year[year]['dividend_yield'] = (div_per_share / price)
                        filled_count += 1
                elif year == latest_year and div_yield_yf:  # Ø§Ø³ØªØ®Ø¯Ø§Ù… yfinance Ù„Ù„Ø³Ù†Ø© Ø§Ù„Ø£Ø­Ø¯Ø«
                    ratios_by_year[year]['dividend_yield'] = div_yield_yf
                    filled_count += 1
            
            # âœ… Ø¥ÙƒÙ…Ø§Ù„ FCF Yield
            if year == latest_year and not ratios_by_year[year].get('fcf_yield') and market_cap:
                fcf = ratios_by_year[year].get('free_cash_flow')
                if fcf and market_cap != 0:
                    ratios_by_year[year]['fcf_yield'] = (fcf / market_cap)
                    filled_count += 1
        
        if filled_count > 0:
            print(f"âœ… ØªÙ… Ø¥ÙƒÙ…Ø§Ù„ {filled_count} Ù‚ÙŠÙ…Ø© Ù†Ø§Ù‚ØµØ© Ù…Ù† Yahoo Finance")
        else:
            print(f"â„¹ï¸ Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø¨ÙŠØ§Ù†Ø§Øª Ù…ØªÙˆÙØ±Ø©ØŒ Ù„Ù… ÙŠØªÙ… Ø¥ÙƒÙ…Ø§Ù„ Ø£ÙŠ Ù‚ÙŠÙ…")

    # ---------- Display helpers ----------
    def display_all(self):
        self._sync_layer_selector_from_data()
        self.display_raw_data()
        self.display_ratios()
        self.display_strategic_analysis()
        # Keep all analytical tabs synchronized after every successful data refresh.
        self.display_forecasts()
        self.display_comparison()
        self.display_ai_analysis()

    def _sync_layer_selector_from_data(self):
        if not hasattr(self, 'raw_layer_combo'):
            return
        data_layers = ((self.current_data or {}).get('data_layers', {}) or {})
        catalog = data_layers.get('layer_catalog') or []
        if not isinstance(catalog, list) or not catalog:
            return
        values = []
        self._raw_layer_display_to_real = {}
        self._raw_layer_real_to_display = {}
        for item in catalog:
            title = str(item.get('title') or '')
            if not title:
                continue
            disp = self._translate_layer_title(title)
            values.append(disp)
            self._raw_layer_display_to_real[disp] = title
            self._raw_layer_real_to_display[title] = disp
        if not values:
            return
        self.raw_layer_combo.configure(values=values)
        current_display = self.raw_layer_var.get()
        current_real = self._raw_layer_display_to_real.get(current_display, current_display)
        target_display = self._raw_layer_real_to_display.get(current_real)
        if target_display in values:
            self.raw_layer_var.set(target_display)
        else:
            self.raw_layer_var.set(values[0])

    def _collect_comparison_rows(self):
        rows = []
        details = {}
        source = self.multi_company_data or {}
        if not source and self.current_data:
            ticker = self.current_data.get('company_info', {}).get('ticker', 'CURRENT')
            source = {ticker: self.current_data}
        for key, data in source.items():
            company = data.get('company_info', {}) or {}
            ticker = company.get('ticker') or key
            inst = data.get('institutional_outputs') or {}
            classification = inst.get('classification', {}) or {}
            classifier_diag = inst.get('classifier_diagnostics', {}) or classification.get('classifier_diagnostics', {}) or {}
            filing_diag = data.get('filing_diagnostics', {}) or {}
            yearly = inst.get('yearly_outputs', {}) or {}
            if not yearly:
                # Fallback when institutional contract payload is not present:
                # build a lightweight yearly structure from computed ratios.
                fr = (data.get('financial_ratios') or {})
                yearly = {}
                for yk, rrow in fr.items():
                    try:
                        yy = int(yk)
                    except Exception:
                        yy = yk
                    ratio_items = []
                    if isinstance(rrow, dict):
                        for rk, rv in rrow.items():
                            if str(rk).startswith('_'):
                                continue
                            if isinstance(rv, (int, float)) or rv is None:
                                ratio_items.append({
                                    'ratio': rk,
                                    'value': rv,
                                    'reliability': {'grade': 'MEDIUM', 'score': 70, 'gates_failed': []},
                                    'reasons': [] if rv is not None else ['MISSING_VALUE'],
                                })
                    yearly[yy] = {'ratio_explanations': ratio_items}

            ratio_contracts = []
            grade_counts = Counter()
            reason_counts = Counter()
            gate_fail_counts = Counter()
            for y, payload in sorted(yearly.items(), key=lambda t: str(t[0])):
                for item in payload.get('ratio_explanations', []) or []:
                    ratio_name = item.get('ratio') or item.get('name') or item.get('id') or 'unknown_ratio'
                    reliability = item.get('reliability', {}) or {}
                    grade = reliability.get('grade', 'REJECTED')
                    score = reliability.get('score', 0)
                    reasons = item.get('reasons', []) or []
                    gates_failed = reliability.get('gates_failed', []) or []
                    grade_counts[grade] += 1
                    reason_counts.update(reasons)
                    gate_fail_counts.update(gates_failed)
                    ratio_contracts.append({
                        'year': y,
                        'ratio': ratio_name,
                        'value': item.get('value'),
                        'grade': grade,
                        'score': score,
                        'reasons': reasons,
                        'gates_failed': gates_failed,
                    })

            top_reasons = ', '.join([r for r, _ in reason_counts.most_common(3)]) or 'None'
            top_flags = ', '.join([g for g, _ in gate_fail_counts.most_common(3)]) or 'None'
            sector = (
                classifier_diag.get('sector')
                or classification.get('primary_profile')
                or ((data.get('sector_gating', {}) or {}).get('profile'))
                or classification.get('sector_profile')
                or 'unknown'
            )
            confidence = (
                classifier_diag.get('confidence_score')
                or classification.get('confidence')
                or ((data.get('sector_gating', {}) or {}).get('confidence'))
            )
            if confidence is None and str(sector).lower() not in ('', 'unknown', 'none'):
                confidence = 80.0
            filing_grade = filing_diag.get('filing_grade') or ('IN_RANGE_ANNUAL' if yearly else 'N/A')
            out_of_range = filing_diag.get('out_of_range', False)

            row = {
                'ticker': ticker,
                'sector': sector,
                'confidence': confidence,
                'filing_grade': filing_grade,
                'out_of_range': out_of_range,
                'high': grade_counts.get('HIGH', 0),
                'medium': grade_counts.get('MEDIUM', 0),
                'low': grade_counts.get('LOW', 0),
                'rejected': grade_counts.get('REJECTED', 0),
                'top_reasons': top_reasons,
                'top_flags': top_flags,
            }
            rows.append(row)
            details[ticker] = {
                'company': company,
                'classification': classification,
                'classifier_diagnostics': classifier_diag,
                'filing_diagnostics': filing_diag,
                'ratio_contracts': ratio_contracts,
                'top_reasons': top_reasons,
                'top_flags': top_flags,
            }
        rows.sort(key=lambda x: x['ticker'])
        return rows, details

    def display_comparison(self):
        if not hasattr(self, 'comparison_tree'):
            return
        for item in self.comparison_tree.get_children():
            self.comparison_tree.delete(item)
        for item in self.comparison_detail_tree.get_children():
            self.comparison_detail_tree.delete(item)
        self.comparison_diag_text.delete('1.0', 'end')

        rows, details = self._collect_comparison_rows()
        self._comparison_detail_cache = details

        if not rows:
            self.comparison_summary_label.config(text="Ù„Ø§ ØªÙˆØ¬Ø¯ Ù†ØªØ§Ø¦Ø¬ Ù…Ù‚Ø§Ø±Ù†Ø© Ø¨Ø¹Ø¯. Ù‚Ù… Ø¨Ø¬Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª Ø´Ø±ÙƒØ© ÙˆØ§Ø­Ø¯Ø© Ø¹Ù„Ù‰ Ø§Ù„Ø£Ù‚Ù„.")
            return

        cols = [
            'Ticker', 'Sector', 'Confidence', 'Filing Grade', 'OutOfRange',
            'HIGH', 'MEDIUM', 'LOW', 'REJECTED', 'Top Rejection Reasons', 'Top Validator Flags'
        ]
        self.comparison_tree.config(columns=cols)
        for c in cols:
            self.comparison_tree.heading(c, text=c)
            if c in ('Top Rejection Reasons', 'Top Validator Flags'):
                self.comparison_tree.column(c, width=240, anchor='w')
            elif c in ('Ticker', 'Sector', 'Filing Grade'):
                self.comparison_tree.column(c, width=130, anchor='center')
            elif c == 'Confidence':
                self.comparison_tree.column(c, width=110, anchor='center')
            else:
                self.comparison_tree.column(c, width=90, anchor='center')

        for r in rows:
            conf = r['confidence']
            conf_txt = f"{conf:.1f}" if isinstance(conf, (int, float)) else "N/A"
            values = (
                r['ticker'],
                r['sector'],
                conf_txt,
                r['filing_grade'],
                'YES' if r['out_of_range'] else 'NO',
                r['high'],
                r['medium'],
                r['low'],
                r['rejected'],
                r['top_reasons'],
                r['top_flags'],
            )
            self.comparison_tree.insert('', 'end', iid=r['ticker'], values=values)

        out_of_range_count = sum(1 for r in rows if r['out_of_range'])
        self.comparison_summary_label.config(
            text=f"Ù†ØªØ§Ø¦Ø¬ Ù…Ø¤Ø³Ø³ÙŠØ©/ØªØ¯Ù‚ÙŠÙ‚ÙŠØ©: {len(rows)} Ø´Ø±ÙƒØ© | Out-of-range: {out_of_range_count} | Ø§Ø¹Ø±Ø¶ Ø§Ù„ØªÙØ§ØµÙŠÙ„ Ø¨Ø§Ø®ØªÙŠØ§Ø± Ø§Ù„Ø´Ø±ÙƒØ© Ù…Ù† Ø§Ù„Ø¬Ø¯ÙˆÙ„"
        )

        first = rows[0]['ticker']
        self.comparison_tree.selection_set(first)
        self._render_comparison_details(first)

    def _on_comparison_select(self, _event=None):
        if not hasattr(self, 'comparison_tree'):
            return
        sel = self.comparison_tree.selection()
        if not sel:
            return
        self._render_comparison_details(sel[0])

    def _render_comparison_details(self, ticker):
        details = getattr(self, '_comparison_detail_cache', {}).get(ticker)
        if not details:
            return
        for item in self.comparison_detail_tree.get_children():
            self.comparison_detail_tree.delete(item)

        cols = ['Year', 'Ratio', 'Value', 'Grade', 'Score', 'Reasons', 'Failed Gates']
        self.comparison_detail_tree.config(columns=cols)
        for c in cols:
            self.comparison_detail_tree.heading(c, text=c)
            if c in ('Reasons', 'Failed Gates'):
                self.comparison_detail_tree.column(c, width=260, anchor='w')
            elif c == 'Ratio':
                self.comparison_detail_tree.column(c, width=180, anchor='w')
            elif c == 'Value':
                self.comparison_detail_tree.column(c, width=120, anchor='center')
            else:
                self.comparison_detail_tree.column(c, width=90, anchor='center')

        for item in details.get('ratio_contracts', []):
            value = item.get('value')
            if isinstance(value, (int, float)):
                value_txt = f"{value:.6g}"
            elif value is None:
                value_txt = "null"
            else:
                value_txt = str(value)
            self.comparison_detail_tree.insert(
                '',
                'end',
                values=(
                    str(item.get('year')),
                    item.get('ratio'),
                    value_txt,
                    item.get('grade'),
                    item.get('score'),
                    ', '.join(item.get('reasons', [])) or 'None',
                    ', '.join(item.get('gates_failed', [])) or 'None',
                )
            )

        diag_payload = {
            'ticker': ticker,
            'company_info': details.get('company', {}),
            'classifier_diagnostics': details.get('classifier_diagnostics', {}),
            'classification': {
                'primary_profile': details.get('classification', {}).get('primary_profile'),
                'profile_probabilities': details.get('classification', {}).get('profile_probabilities', []),
            },
            'filing_diagnostics': details.get('filing_diagnostics', {}),
            'top_rejection_reasons': details.get('top_reasons'),
            'top_validator_flags': details.get('top_flags'),
        }
        self.comparison_diag_text.delete('1.0', 'end')
        self.comparison_diag_text.insert('1.0', json.dumps(diag_payload, ensure_ascii=False, indent=2))

    def _get_selected_years_range(self):
        """Return the full user-selected period, inclusive."""
        try:
            sel_start = int(self.start_year_var.get())
            sel_end = int(self.end_year_var.get())
        except Exception:
            data_by_year = self.current_data.get('data_by_year', {}) if self.current_data else {}
            years = sorted([y for y in (data_by_year or {}).keys() if isinstance(y, int)])
            return years

        if sel_start > sel_end:
            return []
        if (sel_end - sel_start) > 50:
            # Safety guard against accidental huge ranges.
            sel_end = sel_start + 50
        return list(range(sel_start, sel_end + 1))

    def _get_analysis_years_range(self):
        if not self.current_data:
            return []
        try:
            sel_start = int(self.start_year_var.get())
            sel_end = int(self.end_year_var.get())
        except Exception:
            return self._get_selected_years_range()
        if sel_start > sel_end:
            return []
        return list(range(sel_start, sel_end + 1))

    def display_raw_data(self):
        if not self.current_data:
            for i in self.raw_tree.get_children():
                self.raw_tree.delete(i)
            self.company_info_label.config(text="")
            return
        for i in self.raw_tree.get_children():
            self.raw_tree.delete(i)
        def _raw_tags(base_tag: str):
            idx = len(self.raw_tree.get_children(''))
            zebra_tag = 'zebra_even' if (idx % 2 == 0) else 'zebra_odd'
            return (base_tag, zebra_tag)

        data_layers = self.current_data.get('data_layers', {}) or {}
        selected_layer_display = self.raw_layer_var.get() if hasattr(self, 'raw_layer_var') else self._t('layer1')
        selected_layer = getattr(self, '_raw_layer_display_to_real', {}).get(selected_layer_display, selected_layer_display)
        catalog = data_layers.get('layer_catalog') or []
        selected_key = None
        selected_source = None
        for item in catalog:
            if str(item.get('title')) == selected_layer:
                selected_key = str(item.get('key'))
                selected_source = str(item.get('source') or '')
                break
        if not selected_key:
            if selected_layer.startswith('Layer 2') or selected_layer.startswith('الطبقة 2'):
                selected_key = 'layer2_by_year'
                selected_source = 'MARKET'
            elif selected_layer.startswith('Layer 3') or selected_layer.startswith('الطبقة 3'):
                selected_key = 'layer3_by_year'
                selected_source = 'MACRO'
            elif selected_layer.startswith('Layer 4') or selected_layer.startswith('الطبقة 4'):
                selected_key = 'layer4_by_year'
                selected_source = 'YAHOO'
            else:
                selected_key = 'layer1_by_year'
                selected_source = 'SEC'

        sec_view_mode = getattr(self, '_sec_view_mode_var', tk.StringVar(value='official')).get() or 'official'
        if selected_key != 'layer1_by_year':
            sec_view_mode = 'official'

        # Official SEC mode: display SEC official statement CSV directly.
        sec_csv = ((self.current_data.get('institutional_saved_files', {}) or {}).get('sec_official_statement'))
        if (
            selected_key == 'layer1_by_year'
            and sec_view_mode == 'official'
            and isinstance(sec_csv, str)
            and sec_csv
            and os.path.exists(sec_csv)
        ):
            requested_years = self._get_selected_years_range() or []
            layer1_active = (data_layers.get('layer1_by_year', {}) or self.current_data.get('data_by_year', {}) or {})
            with open(sec_csv, 'r', encoding='utf-8-sig', newline='') as fh:
                reader = csv.DictReader(fh)
                rows = list(reader)
                fields = reader.fieldnames or []

            # Structured one-layer format: rows are line items, columns are SEC dates.
            if 'Line Item' in fields:
                date_cols = [c for c in fields if re.search(r'20\d{2}', str(c))]
                requested_year_cols = [str(y) for y in requested_years]
                if requested_year_cols:
                    merged_cols = []
                    seen = set()
                    for c in requested_year_cols + date_cols:
                        cc = str(c)
                        if cc not in seen:
                            seen.add(cc)
                            merged_cols.append(cc)
                    date_cols = merged_cols
                # Deduplicate repeated line items from legacy CSV exports by coalescing
                # non-empty year values (first non-empty wins).
                merged = {}
                order = []
                for r in rows:
                    raw_item = self._decode_mojibake_text(str(r.get('Line Item') or '').strip())
                    raw_item = re.sub(r"\s+", " ", raw_item).strip()
                    if not raw_item:
                        continue
                    raw_item_l = raw_item.lower()
                    if 'legacy current' in raw_item_l or 'legacy' in raw_item_l and 'current' in raw_item_l:
                        continue
                    item_key = f"official::{self._normalize_line_item_key(raw_item)}"
                    if item_key not in merged:
                        merged[item_key] = {'Line Item': raw_item, '_label_source': 'csv'}
                        for d in date_cols:
                            merged[item_key][d] = ''
                        order.append(item_key)
                    else:
                        # Keep SEC CSV display label stable when already sourced from CSV.
                        if merged[item_key].get('_label_source') != 'csv':
                            merged[item_key]['Line Item'] = self._prefer_display_label(
                                merged[item_key].get('Line Item'),
                                raw_item,
                            )
                    for d in date_cols:
                        cur = str(merged[item_key].get(d, '') or '').strip()
                        newv = str(r.get(d, '') or '').strip()
                        if cur == '' and newv != '':
                            merged[item_key][d] = newv

                # Canonical enrichment is intentionally disabled in official SEC mode.
                if sec_view_mode == 'canonical' and isinstance(layer1_active, dict) and date_cols:
                    def _norm_tokens(txt):
                        s = self._decode_mojibake_text(str(txt or ''))
                        # Split CamelCase / snake_case first (e.g., NetInterestIncome -> net interest income).
                        s = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', s)
                        s = s.replace('_', ' ')
                        s = re.sub(r'[^A-Za-z0-9\u0600-\u06FF ]+', ' ', s).lower()
                        tokens = [t for t in s.split() if t]
                        stop = {
                            'total', 'net', 'operating', 'current', 'noncurrent',
                            'other', 'and', 'of', 'the', 'by', 'for', 'from',
                            'in', 'on', 'at', 'to',
                            'إجمالي', 'صافي', 'ال', 'من', 'في', 'الى', 'على', 'و',
                        }
                        return [t for t in tokens if t not in stop]

                    def _best_existing_key_for_label(label_text):
                        # 1) Exact normalized label match first (safe path).
                        norm_target = self._normalize_line_item_key(label_text)
                        for kx, vx in merged.items():
                            base_label = vx.get('Line Item') or ''
                            if self._normalize_line_item_key(base_label) == norm_target:
                                return kx

                        # 1.b) exact semantic-key match only (safe).
                        sk = self._anchored_semantic_key(label_text)
                        if sk in merged:
                            return sk

                        # In strict mode, stop here to avoid accidental cross-concept merges.
                        if self._strict_ui_merge_enabled():
                            return None

                        # 2) Conservative fuzzy bridge for close aliases only.
                        # Avoid aggressive semantic collapsing that can pollute values
                        # (e.g., AOCI accidentally filled with NetIncome).
                        # Fuzzy semantic bridge: map close labels (e.g., Interest income vs InterestIncomeOperating).
                        tgt = set(_norm_tokens(label_text))
                        if not tgt:
                            return None
                        core = {
                            'revenue', 'revenues', 'income', 'expense', 'expenses', 'asset', 'assets',
                            'liability', 'liabilities', 'equity', 'cash', 'debt', 'interest',
                            'inventory', 'receivable', 'payable', 'lease', 'loan', 'deposit',
                            'profit', 'tax', 'operating', 'comprehensive',
                        }
                        best_key = None
                        best_score = 0.0
                        for kx, vx in merged.items():
                            base_label = vx.get('Line Item') or ''
                            src = set(_norm_tokens(base_label))
                            if not src:
                                continue
                            tgt_core = tgt & core
                            src_core = src & core
                            # Hard family guards.
                            if tgt_core and src_core and len(tgt_core & src_core) == 0:
                                continue
                            if ('comprehensive' in tgt_core) != ('comprehensive' in src_core):
                                continue
                            inter = len(tgt & src)
                            union = len(tgt | src)
                            if union == 0:
                                continue
                            j = inter / union
                            subset_bonus = 0.15 if (tgt.issubset(src) or src.issubset(tgt)) else 0.0
                            score = j + subset_bonus
                            # Require meaningful shared signal (not one generic token).
                            if inter < 2:
                                continue
                            if score > best_score:
                                best_score = score
                                best_key = kx
                        if best_score >= 0.82:
                            return best_key
                        return None

                    year_ints = []
                    for dc in date_cols:
                        try:
                            year_ints.append(int(str(dc)))
                        except Exception:
                            pass
                    concept_pool = set()
                    for yy in year_ints:
                        concept_pool.update((layer1_active.get(yy, {}) or {}).keys())
                    for concept in sorted(concept_pool):
                        c_label = self._decode_mojibake_text(str(concept or '').strip())
                        if not c_label:
                            continue
                        # Hide internal diagnostic/helper concepts from SEC UI statement.
                        if self._is_internal_helper_label(c_label):
                            continue
                        c_key = _best_existing_key_for_label(c_label) or self._anchored_semantic_key(c_label)
                        if c_key not in merged:
                            merged[c_key] = {'Line Item': c_label, '_label_source': 'layer1'}
                            for d in date_cols:
                                merged[c_key][d] = ''
                            order.append(c_key)
                        else:
                            # Do not override a human SEC CSV label with technical Layer1 alias.
                            if merged[c_key].get('_label_source') != 'csv':
                                merged[c_key]['Line Item'] = self._prefer_display_label(
                                    merged[c_key].get('Line Item'),
                                    c_label,
                                )
                        for d in date_cols:
                            try:
                                yy = int(str(d))
                            except Exception:
                                continue
                            raw_v = (layer1_active.get(yy, {}) or {}).get(concept)
                            cur = str(merged[c_key].get(d, '') or '').strip()
                            if cur == '' and raw_v is not None:
                                merged[c_key][d] = raw_v
                rows = [merged[i] for i in order]
                for _r in rows:
                    _r.pop('_label_source', None)
                cols = ['Line Item'] + date_cols
                self.raw_tree.config(columns=cols)
                self.raw_tree.tag_configure('parent_row', font=FONTS['label'])
                self.raw_tree.tag_configure('child_row', font=FONTS['tree'])
                self.raw_tree.tag_configure('zebra_even', background='#ffffff')
                self.raw_tree.tag_configure('zebra_odd', background='#f7fbff')
                for c in cols:
                    self.raw_tree.heading(c, text=self._translate_financial_item(c))
                    if c == 'Line Item':
                        self.raw_tree.column(c, width=320, anchor='w')
                    else:
                        self.raw_tree.column(c, width=160, anchor='center')
                for r in rows:
                    raw_label = r.get('Line Item', '')
                    vals = [r.get(c, '') for c in cols]
                    vals[0] = self._translate_financial_item(raw_label)
                    tag = 'parent_row' if self._is_parent_line_item(raw_label, vals[1:]) else 'child_row'
                    self.raw_tree.insert('', 'end', values=tuple(vals), tags=_raw_tags(tag))
            else:
                cols = fields
                self.raw_tree.config(columns=cols)
                self.raw_tree.tag_configure('parent_row', font=FONTS['label'])
                self.raw_tree.tag_configure('child_row', font=FONTS['tree'])
                self.raw_tree.tag_configure('zebra_even', background='#ffffff')
                self.raw_tree.tag_configure('zebra_odd', background='#f7fbff')
                for c in cols:
                    self.raw_tree.heading(c, text=self._translate_financial_item(c))
                    self.raw_tree.column(c, width=180, anchor='center')
                for r in rows:
                    values = [r.get(c, '') for c in cols]
                    raw_label = values[0] if values else ''
                    if values:
                        values[0] = self._translate_financial_item(raw_label)
                    tag = 'parent_row' if self._is_parent_line_item(raw_label) else 'child_row'
                    self.raw_tree.insert('', 'end', values=tuple(values), tags=_raw_tags(tag))
            ci = self.current_data.get('company_info', {})
            self.company_info_label.config(
                text=f"{self._t('summary_prefix')} {ci.get('name','')} ({ci.get('ticker','')}) | {self._t('sec_direct_label')}"
            )
            return

        data_by_year = self.current_data.get('data_by_year', {}) or {}

        if selected_key.startswith('extra::'):
            extra_name = selected_key.split('::', 1)[1]
            active_by_year = ((data_layers.get('extra_layers_by_year', {}) or {}).get(extra_name, {}) or {})
        else:
            active_by_year = data_layers.get(selected_key, {}) or data_by_year

        years = self._get_selected_years_range()
        if not years:
            return

        # For non-SEC source layers, prefer full payload representation (value + unit + source) by year.
        if selected_source in ('MARKET', 'MACRO', 'YAHOO'):
            payloads = (self.current_data.get('source_layer_payloads') or {})
            layer_payload = payloads.get(selected_source, {}) or {}
            periods = (layer_payload.get('periods') or {})
            fields_set = set()
            value_map = {}
            unit_map = {}
            source_map = {}
            for y in years:
                pobj = (periods.get(str(y)) or {})
                fobj = (pobj.get('fields') or {})
                for field_name, field_val in fobj.items():
                    fields_set.add(field_name)
                    if isinstance(field_val, dict):
                        value_map.setdefault(field_name, {})[y] = field_val.get('value')
                        unit_map.setdefault(field_name, {})[y] = field_val.get('unit')
                        source_map.setdefault(field_name, {})[y] = field_val.get('source') or selected_source
                    else:
                        value_map.setdefault(field_name, {})[y] = field_val
                        source_map.setdefault(field_name, {})[y] = selected_source

            def fmt_value(v):
                if v is None:
                    return ''
                if isinstance(v, (int, float)):
                    if abs(v) >= 1_000_000_000:
                        return f"{v/1_000_000_000:,.2f}B"
                    if abs(v) >= 1_000_000:
                        return f"{v/1_000_000:,.2f}M"
                    return f"{v:,.6g}"
                return str(v)

            cols = [self._t('raw_col_item')] + [str(y) for y in years] + [self._t('raw_col_unit'), self._t('raw_col_source')]
            self.raw_tree.config(columns=cols)
            self.raw_tree.tag_configure('parent_row', font=FONTS['label'])
            self.raw_tree.tag_configure('child_row', font=FONTS['tree'])
            self.raw_tree.tag_configure('zebra_even', background='#ffffff')
            self.raw_tree.tag_configure('zebra_odd', background='#f7fbff')
            for c in cols:
                self.raw_tree.heading(c, text=c)
                if c == self._t('raw_col_item'):
                    self.raw_tree.column(c, width=380, anchor='w')
                elif c in (self._t('raw_col_unit'), self._t('raw_col_source')):
                    self.raw_tree.column(c, width=180, anchor='center')
                else:
                    self.raw_tree.column(c, width=130, anchor='center')

            for field_name in sorted(fields_set):
                row = [self._translate_financial_item(field_name)]
                for y in years:
                    row.append(fmt_value((value_map.get(field_name) or {}).get(y)))
                first_unit = ''
                for y in years:
                    u = (unit_map.get(field_name) or {}).get(y)
                    if u:
                        first_unit = str(u)
                        break
                first_source = ''
                for y in years:
                    s = (source_map.get(field_name) or {}).get(y)
                    if s:
                        first_source = str(s)
                        break
                row.append(first_unit)
                row.append(first_source)
                tag = 'parent_row' if self._is_parent_line_item(field_name, row[1:1+len(years)]) else 'child_row'
                self.raw_tree.insert('', 'end', values=row, tags=_raw_tags(tag))

            ci = self.current_data.get('company_info', {})
            self.company_info_label.config(
                text=f"{self._t('summary_prefix')} {ci.get('name','')} ({ci.get('ticker','')}) | {selected_layer_display}"
            )
            return
        
        # available years in the selected layer
        all_available_years = sorted([y for y in active_by_year.keys() if isinstance(y, int)])
        
        if not years:
            return
        
        requested_start = int(self.start_year_var.get())
        requested_end = int(self.end_year_var.get())
        ci = self.current_data.get('company_info', {}) or {}
        if self.current_lang == 'ar':
            info_msg = f"🏢 {ci.get('name', '')} ({ci.get('ticker', '')})\nالفترة المطلوبة: {requested_start} - {requested_end}"
        elif self.current_lang == 'fr':
            info_msg = f"🏢 {ci.get('name', '')} ({ci.get('ticker', '')})\nPériode demandée : {requested_start} - {requested_end}"
        else:
            info_msg = f"🏢 {ci.get('name', '')} ({ci.get('ticker', '')})\nRequested period: {requested_start} - {requested_end}"

        if all_available_years:
            if self.current_lang == 'ar':
                info_msg += f"\n📅 السنوات المتاحة بالمصدر: {min(all_available_years)} - {max(all_available_years)}"
            elif self.current_lang == 'fr':
                info_msg += f"\n📅 Années disponibles à la source : {min(all_available_years)} - {max(all_available_years)}"
            else:
                info_msg += f"\n📅 Available source years: {min(all_available_years)} - {max(all_available_years)}"
            if requested_end > max(all_available_years):
                if self.current_lang == 'ar':
                    info_msg += f"\n⚠️ بعض السنوات خارج البيانات المتاحة، وسيتم عرضها كخانات فارغة (N/A)."
                elif self.current_lang == 'fr':
                    info_msg += f"\n⚠️ Certaines années sont hors du jeu de données source et seront affichées en N/A."
                else:
                    info_msg += f"\n⚠️ Some years are outside available source data and will be shown as N/A."
        
        self.company_info_label.config(text=info_msg)
        
        concepts = set()
        for y in years:
            concepts.update(active_by_year.get(y, {}).keys())

        # Sector-aware concept filtering for raw statement display.
        sector_profile = ((self.current_data or {}).get('sector_gating', {}) or {}).get('profile', 'industrial')
        if selected_key == 'layer1_by_year':
            bank_only_markers = (
                'LoansReceivable', 'Deposits', 'NetInterestIncome',
                'ProvisionForCreditLosses', 'AllowanceForCreditLosses',
                'FederalFundsSold', 'NoninterestBearingDeposits'
            )
            industrial_only_markers = (
                'Inventory', 'CostOfRevenue', 'GrossProfit',
                'ResearchAndDevelopmentExpense', 'SellingGeneralAndAdministrativeExpense',
                'RevenueFromContractWithCustomerExcludingAssessedTax'
            )

            filtered = set()
            for concept in concepts:
                ctxt = str(concept)
                if sector_profile == 'bank':
                    if any(marker in ctxt for marker in industrial_only_markers):
                        continue
                elif sector_profile in ('industrial', 'technology', 'unknown'):
                    if any(marker in ctxt for marker in bank_only_markers):
                        continue
                filtered.add(concept)
            concepts = filtered

        def fmt_value(v):
            if v is None:
                return ''
            if isinstance(v, (int, float)):
                if abs(v) >= 1_000_000_000:
                    return f"{v/1_000_000_000:,.2f}B"
                if abs(v) >= 1_000_000:
                    return f"{v/1_000_000:,.2f}M"
                return f"{v:,.0f}"
            return str(v)

        if selected_key == 'layer3_by_year':
            cols = [self._t('raw_col_category'), self._t('raw_col_normalized')] + [str(y) for y in years]
            self.raw_tree.config(columns=cols)
            self.raw_tree.tag_configure('parent_row', font=FONTS['label'])
            self.raw_tree.tag_configure('child_row', font=FONTS['tree'])
            self.raw_tree.tag_configure('zebra_even', background='#ffffff')
            self.raw_tree.tag_configure('zebra_odd', background='#f7fbff')
            for c in cols:
                self.raw_tree.heading(c, text=c)
                if c == self._t('raw_col_category'):
                    self.raw_tree.column(c, width=180)
                elif c == self._t('raw_col_normalized'):
                    self.raw_tree.column(c, width=320)
                else:
                    self.raw_tree.column(c, width=140)

            parsed_items = []
            for concept in concepts:
                if '::' in concept:
                    category, normalized = concept.split('::', 1)
                else:
                    category, normalized = 'Unclassified', concept
                parsed_items.append((category, normalized, concept))

            for category, normalized, concept_key in sorted(parsed_items, key=lambda t: (t[0], t[1])):
                row = [self._translate_financial_item(category), self._translate_financial_item(normalized)]
                for y in years:
                    row.append(fmt_value(active_by_year.get(y, {}).get(concept_key)))
                tag = 'parent_row' if self._is_parent_line_item(normalized, row[2:]) else 'child_row'
                self.raw_tree.insert('', 'end', values=row, tags=_raw_tags(tag))
        else:
            first_col = self._t('raw_col_item')
            if selected_key in ('layer2_by_year', 'layer4_by_year'):
                first_col = self._t('raw_col_normalized')
            cols = [first_col] + [str(y) for y in years] + [self._t('raw_col_unit')]
            self.raw_tree.config(columns=cols)
            self.raw_tree.tag_configure('parent_row', font=FONTS['label'])
            self.raw_tree.tag_configure('child_row', font=FONTS['tree'])
            self.raw_tree.tag_configure('zebra_even', background='#ffffff')
            self.raw_tree.tag_configure('zebra_odd', background='#f7fbff')
            for c in cols:
                self.raw_tree.heading(c, text=c)
                self.raw_tree.column(c, width=140 if c != first_col else 360)

            display_concepts = sorted(concepts)
            if selected_key == 'layer1_by_year':
                display_concepts = [
                    c for c in display_concepts
                    if not self._is_internal_helper_label(c)
                ]
                # Strong semantic dedup across Arabic/English/French labels.
                merged = {}
                order = []
                for concept in display_concepts:
                    sem_key = self._anchored_semantic_key(concept)
                    if sem_key not in merged:
                        merged[sem_key] = {
                            'label': str(concept),
                            'aliases': {str(concept)},
                            'vals': {y: None for y in years},
                        }
                        order.append(sem_key)
                    else:
                        merged[sem_key]['aliases'].add(str(concept))
                        merged[sem_key]['label'] = self._prefer_display_label(merged[sem_key]['label'], str(concept))

                    for y in years:
                        cur = merged[sem_key]['vals'].get(y)
                        new_v = active_by_year.get(y, {}).get(concept)
                        if cur is None and new_v is not None:
                            merged[sem_key]['vals'][y] = new_v
                        # Safe mode: keep first non-null, avoid magnitude-based replacement.

                for sem_key in order:
                    meta = merged[sem_key]
                    raw_label = meta.get('label') or ''
                    row = [self._translate_financial_item(raw_label)]
                    for y in years:
                        row.append(fmt_value(meta['vals'].get(y)))
                    row.append('')
                    tag = 'parent_row' if self._is_parent_line_item(raw_label, row[1:1+len(years)]) else 'child_row'
                    self.raw_tree.insert('', 'end', values=row, tags=_raw_tags(tag))
            else:
                for concept in display_concepts:
                    row = [self._translate_financial_item(concept)]
                    for y in years:
                        row.append(fmt_value(active_by_year.get(y, {}).get(concept)))
                    row.append('')
                    tag = 'parent_row' if self._is_parent_line_item(concept, row[1:1+len(years)]) else 'child_row'
                    self.raw_tree.insert('', 'end', values=row, tags=_raw_tags(tag))
        ci = self.current_data.get('company_info', {})
        self.company_info_label.config(text=f"{self._t('summary_prefix')} {ci.get('name','')} ({ci.get('ticker','')}) | {selected_layer_display}")

    def _get_sector_profile(self):
        sg = (self.current_data.get('sector_gating', {}) if self.current_data else {}) or {}
        return (sg.get('profile') or 'industrial')

    @staticmethod
    def _normalize_sector_for_packs(sector_profile: str) -> str:
        s = str(sector_profile or 'industrial').strip().lower()
        if s in ('technology', 'tech'):
            return 'industrial'
        return s

    @staticmethod
    def _is_present_metric_value(v):
        if v is None:
            return False
        if isinstance(v, str):
            return v.strip() != '' and 'N/A' not in v.upper()
        if isinstance(v, (int, float)):
            try:
                import math
                return not (math.isnan(float(v)) or math.isinf(float(v)))
            except Exception:
                return True
        return True

    def _get_sector_ratio_export_keys(self, sector_profile: str):
        sector = self._normalize_sector_for_packs(sector_profile)
        metric_packs = {
            'industrial': [
                'gross_margin', 'operating_margin', 'net_margin', 'ebitda_margin', 'roa', 'roe', 'roic',
                'inventory_turnover', 'inventory_days', 'days_sales_outstanding', 'payables_turnover', 'ap_days', 'asset_turnover',
                'current_ratio', 'quick_ratio', 'cash_ratio',
                'debt_to_equity', 'debt_to_assets', 'interest_coverage', 'net_debt_ebitda',
                'pe_ratio', 'pb_ratio', 'dividend_yield', 'eps_basic', 'book_value_per_share',
                'altman_z_score', 'accruals_ratio', 'ocf_margin', 'free_cash_flow', 'fcf_per_share',
                'market_cap', 'total_debt',
            ],
            'bank': [
                'net_interest_margin', 'loan_to_deposit_ratio', 'capital_ratio_proxy', 'net_income_to_assets', 'equity_ratio',
                'bank_efficiency_ratio',
                'roa', 'roe', 'net_margin',
                'debt_to_equity', 'debt_to_assets',
                'pe_ratio', 'pb_ratio', 'dividend_yield', 'eps_basic', 'book_value_per_share',
                'market_cap', 'total_debt',
            ],
            'insurance': [
                'combined_proxy', 'capital_adequacy_proxy', 'net_income_to_assets', 'equity_ratio',
                'roa', 'roe', 'net_margin',
                'pe_ratio', 'pb_ratio', 'dividend_yield', 'eps_basic', 'book_value_per_share',
                'market_cap', 'total_debt',
            ],
        }
        return metric_packs.get(sector, metric_packs['industrial'])

    def _get_sector_strategic_export_keys(self, sector_profile: str):
        sector = self._normalize_sector_for_packs(sector_profile)
        metric_packs = {
            'industrial': [
                'Fair_Value', 'Investment_Score', 'Economic_Spread', 'ROIC', 'WACC', 'Beta', 'SGR_Internal',
                'Altman_Z_Score', 'Warning_Signal', 'Accruals_Ratio', 'Accruals_Change', 'Credit_Rating', 'Credit_Rating_Score',
                'Net_Debt_EBITDA', 'Op_Leverage',
                'ROE', 'NI_Growth', 'Retention_Ratio', 'Dividends_Paid', 'EBITDA', 'FCF_Yield', 'EPS', 'FCF_per_Share',
                'CCC_Days', 'Inventory_Days', 'AR_Days', 'AP_Days', 'Cost_of_Debt',
                'PE_Ratio', 'PE_Ratio_Used', 'PB_Ratio', 'PB_Ratio_Used', 'EV_EBITDA', 'Dividend_Yield',
            ],
            'bank': [
                'Net_Interest_Margin', 'Loan_to_Deposit_Ratio', 'Capital_Ratio_Proxy', 'Net_Income_to_Assets', 'Equity_Ratio',
                'Bank_Efficiency_Ratio', 'Bank_Total_Revenue',
                'ROA', 'ROE', 'Net_Margin', 'Cost_of_Debt', 'Credit_Rating', 'Credit_Rating_Score', 'Warning_Signal',
                'PE_Ratio', 'PE_Ratio_Used', 'PB_Ratio', 'PB_Ratio_Used', 'Dividend_Yield', 'Beta', 'WACC',
            ],
            'insurance': [
                'Combined_Ratio_Proxy', 'Capital_Adequacy_Proxy', 'Net_Income_to_Assets', 'Equity_Ratio',
                'ROA', 'ROE', 'Net_Margin', 'Cost_of_Debt', 'Credit_Rating', 'Credit_Rating_Score', 'Warning_Signal',
                'PE_Ratio', 'PE_Ratio_Used', 'PB_Ratio', 'PB_Ratio_Used', 'Dividend_Yield', 'Beta', 'WACC',
            ],
        }
        return metric_packs.get(sector, metric_packs['industrial'])

    def _ratio_bounds_for_sector(self, sector_profile: str):
        """
        Hard sanity bounds used for acceptance audit only.
        Values outside these bands are flagged for review.
        """
        sector = self._normalize_sector_for_packs(sector_profile)
        common = {
            # Negative P/E can be valid when earnings are negative.
            'pe_ratio': (-500.0, 500.0),
            'pb_ratio': (-5.0, 200.0),
            'debt_to_equity': (-10.0, 20.0),
            'debt_to_assets': (-2.0, 5.0),
            'net_debt_ebitda': (-20.0, 20.0),
            'interest_coverage': (-200.0, 500.0),
            'book_value_per_share': (-2_000.0, 2_000.0),
            'eps_basic': (-2_000.0, 2_000.0),
            'fcf_per_share': (-5_000.0, 5_000.0),
            'inventory_days': (0.0, 3_650.0),
            'days_sales_outstanding': (0.0, 3_650.0),
            'ap_days': (0.0, 3_650.0),
            'ccc_days': (-3_650.0, 3_650.0),
        }
        percent_metrics = {
            'gross_margin',
            'operating_margin',
            'net_margin',
            'ebitda_margin',
            'roa',
            'roe',
            'roic',
            'ocf_margin',
            'fcf_yield',
            'dividend_yield',
            'wacc',
            'economic_spread',
            'retention_ratio',
            'sgr_internal',
            'cost_of_debt',
            'net_interest_margin',
            'net_income_to_assets',
            'equity_ratio',
            'bank_efficiency_ratio',
        }
        for rid in percent_metrics:
            common[rid] = (-2.5, 2.5)
        if sector == 'bank':
            common['loan_to_deposit_ratio'] = (0.0, 5.0)
            common['capital_ratio_proxy'] = (-1.0, 2.0)
        if sector == 'insurance':
            common['combined_proxy'] = (0.0, 5.0)
            common['capital_adequacy_proxy'] = (-1.0, 3.0)
        return common

    def _build_export_acceptance_frames(
        self,
        *,
        years,
        ticker: str,
        sector_profile: str,
        data_by_year: dict,
        per_year: dict,
        ratio_source: UnifiedRatioSource,
        ratio_export_keys: list,
        strategic_export_keys: list,
        blocked_ratios: set,
        blocked_strategic_metrics: set,
        gate_issues: list,
    ):
        import math
        import pandas as pd

        def _nk(x):
            return re.sub(r'[^a-z0-9]+', '', str(x or '').lower())

        def _pick_num_ci(row_dict, aliases):
            if not isinstance(row_dict, dict):
                return None
            # Fast exact-path first.
            for a in aliases:
                if a in row_dict:
                    fv = self._safe_excel_number(row_dict.get(a))
                    if fv is not None:
                        return float(fv)
            # Case/format-insensitive fallback.
            nmap = {}
            for k, v in row_dict.items():
                kk = _nk(k)
                if kk and kk not in nmap:
                    nmap[kk] = v
            for a in aliases:
                fv = self._safe_excel_number(nmap.get(_nk(a)))
                if fv is not None:
                    return float(fv)
            return None

        ratio_bounds = self._ratio_bounds_for_sector(sector_profile)
        ratio_audit_rows = []
        critical_rows = []
        not_computable_count = 0
        ratio_cell_count = 0
        ratio_computed_count = 0
        reason_counter = Counter()
        incomplete_years = []
        effective_years = []

        for year in years:
            row = (data_by_year or {}).get(year, {}) or {}
            assets_anchor = _pick_num_ci(row, ['Assets', 'TotalAssets', 'Total Assets', 'assets'])
            liab_anchor = _pick_num_ci(row, ['Liabilities', 'TotalLiabilities', 'Total Liabilities', 'liabilities'])
            eq_anchor = _pick_num_ci(row, [
                'StockholdersEquity',
                'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                'TotalEquity',
                'Total Equity',
                'stockholdersequity',
                'stockholdersequityincludingportionattributabletononcontrollinginterest',
            ])
            ctl_anchor = _pick_num_ci(row, [
                'LiabilitiesAndStockholdersEquity',
                'Total liabilities and equity',
                "Total liabilities and stockholders' equity",
                'liabilitiesandstockholdersequity',
            ])
            # A year is "effective" for acceptance only when balance-sheet anchors exist.
            # This prevents score distortion from revenue-only years with missing balance anchors.
            bs_present = len([v for v in (assets_anchor, liab_anchor, eq_anchor) if isinstance(v, (int, float))])
            is_effective = (
                bs_present >= 2
                and isinstance(assets_anchor, (int, float))
            ) or (
                isinstance(assets_anchor, (int, float))
                and isinstance(ctl_anchor, (int, float))
            )
            if is_effective:
                effective_years.append(year)
            else:
                incomplete_years.append(year)

        for ratio_id in ratio_export_keys:
            if ratio_id in blocked_ratios:
                continue
            for year in years:
                contract = ratio_source.get_ratio_contract(ticker, year, ratio_id) or {}
                status = str(contract.get('status') or 'NOT_COMPUTABLE')
                reason = str(contract.get('reason') or '')
                reliability = int(contract.get('reliability') or 0)
                value = contract.get('value')
                missing_inputs = contract.get('missing_inputs') or []
                missing_inputs_txt = ', '.join([str(x) for x in missing_inputs]) if missing_inputs else ''
                bounds_status = ((contract.get('bounds_result') or {}).get('status') or 'unknown')

                is_computed = isinstance(value, (int, float)) and not (math.isnan(float(value)) or math.isinf(float(value)))
                if year in effective_years:
                    ratio_cell_count += 1
                    if is_computed:
                        ratio_computed_count += 1
                    else:
                        not_computable_count += 1
                        reason_counter[reason or 'UNKNOWN'] += 1

                outlier_flag = False
                outlier_note = ''
                if is_computed and ratio_id in ratio_bounds:
                    lo, hi = ratio_bounds[ratio_id]
                    fv = float(value)
                    if fv < lo or fv > hi:
                        outlier_flag = True
                        outlier_note = f"value={fv:.6g} outside [{lo}, {hi}]"
                        critical_rows.append({
                            'Severity': 'CRITICAL',
                            'Type': 'RATIO_OUTLIER',
                            'Metric': ratio_id,
                            'Year': year,
                            'Details': outlier_note,
                        })

                ratio_audit_rows.append({
                    'Metric': ratio_id,
                    'Year': year,
                    'Status': status,
                    'Reason': reason if reason else '',
                    'Reliability': reliability,
                    'Value': value if is_computed else None,
                    'Display': format_ratio_value(ratio_id, value).get('display_text') if is_computed else f"N/A ({reason or 'NOT_COMPUTABLE'})",
                    'Missing_Inputs': missing_inputs_txt,
                    'Bounds_Status': bounds_status,
                    'Outlier_Flag': outlier_flag,
                    'Outlier_Note': outlier_note,
                    'Formula': contract.get('formula_used'),
                    'Period': contract.get('period'),
                    'Source': contract.get('source'),
                })

        # Balance-sheet identity checks
        balance_checks = []
        balance_passes = 0
        balance_evaluable_count = 0
        for year in years:
            row = (data_by_year or {}).get(year, {}) or {}
            assets = _pick_num_ci(row, ['Assets', 'TotalAssets', 'Total Assets', 'assets'])
            liabilities = _pick_num_ci(row, ['Liabilities', 'TotalLiabilities', 'Total Liabilities', 'liabilities'])
            # Guard against control-total pollution where TotalLiabilities mirrors Assets.
            if isinstance(assets, (int, float)) and isinstance(liabilities, (int, float)):
                if abs(float(liabilities) - float(assets)) <= max(1.0, abs(float(assets)) * 0.005):
                    alt_liab = _pick_num_ci(row, ['Liabilities', 'liabilities'])
                    if isinstance(alt_liab, (int, float)) and abs(float(alt_liab) - float(assets)) > max(1.0, abs(float(assets)) * 0.005):
                        liabilities = alt_liab
            equity_candidates = [
                _pick_num_ci(row, ['StockholdersEquity', 'stockholdersequity']),
                _pick_num_ci(row, [
                    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                    'stockholdersequityincludingportionattributabletononcontrollinginterest',
                ]),
                _pick_num_ci(row, ['TotalEquity', 'Total Equity', 'totalequity']),
            ]
            equity_candidates = [e for e in equity_candidates if isinstance(e, (int, float))]
            equity = None
            if equity_candidates:
                if isinstance(assets, (int, float)) and isinstance(liabilities, (int, float)):
                    target_equity = float(assets) - float(liabilities)
                    equity = min(equity_candidates, key=lambda ev: abs(float(ev) - target_equity))
                else:
                    equity = equity_candidates[0]
            mezzanine_candidates = [
                _pick_num_ci(row, ['TemporaryEquity', 'temporaryequity']),
                _pick_num_ci(row, ['RedeemableNoncontrollingInterest', 'redeemablenoncontrollinginterest']),
                _pick_num_ci(row, ['RedeemableNoncontrollingInterestsInSubsidiaries', 'Redeemable noncontrolling interests in subsidiaries']),
            ]
            mezzanine_candidates = [
                float(v) for v in mezzanine_candidates
                if isinstance(v, (int, float))
            ]
            mezzanine = 0.0
            if all(isinstance(v, (int, float)) for v in (assets, liabilities, equity)):
                lhs = float(assets)
                base_rhs = float(liabilities) + float(equity)
                control_total = _pick_num_ci(row, [
                    'LiabilitiesAndStockholdersEquity',
                    'Total liabilities and equity',
                    "Total liabilities and stockholders' equity",
                    'liabilitiesandstockholdersequity',
                ])
                rhs = base_rhs
                # Prefer SEC control-total when present to avoid false negatives under
                # temporary/redeemable equity presentation differences.
                if isinstance(control_total, (int, float)):
                    # Reject stale/misaligned control totals if L+E already matches assets.
                    ctl_delta = abs(float(control_total) - float(lhs)) / max(abs(float(lhs)), 1.0)
                    le_delta = abs(base_rhs - float(lhs)) / max(abs(float(lhs)), 1.0)
                    if ctl_delta <= 0.01 or le_delta > 0.001:
                        rhs = float(control_total)
                elif mezzanine_candidates:
                    # Add mezzanine only if it materially improves the identity.
                    base_delta = abs(base_rhs - float(lhs))
                    best = None
                    for mz in mezzanine_candidates:
                        d = abs((base_rhs + float(mz)) - float(lhs))
                        if best is None or d < best[0]:
                            best = (d, float(mz))
                    if best and best[0] < (base_delta * 0.5):
                        mezzanine = best[1]
                        rhs = base_rhs + mezzanine
                delta = lhs - rhs
                denom = max(abs(lhs), 1.0)
                delta_pct = abs(delta) / denom
                # Tolerate minor filing-scale/rounding noise up to 0.05%.
                passed = delta_pct <= 0.0005
                if year in effective_years:
                    balance_evaluable_count += 1
                if passed and year in effective_years:
                    balance_passes += 1
                else:
                    if year in effective_years:
                        critical_rows.append({
                            'Severity': 'CRITICAL',
                            'Type': 'BALANCE_IDENTITY',
                            'Metric': 'Assets = Liabilities + Equity',
                            'Year': year,
                            'Details': f"delta={delta:.6g}, delta_pct={delta_pct:.6%}",
                        })
                balance_checks.append({
                    'Year': year,
                    'Assets': lhs,
                    'Liabilities': float(liabilities),
                    'Equity': float(equity),
                    'Delta': delta,
                    'Delta_Pct': delta_pct,
                    'Status': 'PASS' if passed else 'FAIL',
                })
            else:
                sev = 'HIGH' if year in effective_years else 'INFO'
                critical_rows.append({
                    'Severity': sev,
                    'Type': 'BALANCE_ANCHOR_MISSING',
                    'Metric': 'Assets/Liabilities/Equity',
                    'Year': year,
                    'Details': 'One or more anchors are missing',
                })
                balance_checks.append({
                    'Year': year,
                    'Assets': assets,
                    'Liabilities': liabilities,
                    'Equity': equity,
                    'Delta': None,
                    'Delta_Pct': None,
                    'Status': 'MISSING' if year in effective_years else 'SKIPPED_INCOMPLETE',
                })

        # Strategic coverage
        strategic_cell_count = 0
        strategic_present_count = 0
        for metric_key in strategic_export_keys:
            if metric_key in blocked_strategic_metrics:
                continue
            for year in years:
                v = (per_year.get(year, {}) or {}).get(metric_key)
                if year in effective_years:
                    strategic_cell_count += 1
                    if self._is_present_metric_value(v):
                        strategic_present_count += 1

        ratio_coverage = (ratio_computed_count / ratio_cell_count * 100.0) if ratio_cell_count else 0.0
        strategic_coverage = (strategic_present_count / strategic_cell_count * 100.0) if strategic_cell_count else 0.0
        balance_pass_rate = (balance_passes / balance_evaluable_count * 100.0) if balance_evaluable_count else 0.0
        critical_count = len([r for r in critical_rows if r.get('Severity') == 'CRITICAL'])
        high_count = len([r for r in critical_rows if r.get('Severity') == 'HIGH'])
        gate_issue_count = len(gate_issues or [])

        # Weighted final score for audit acceptance.
        final_score = (
            0.50 * ratio_coverage
            + 0.30 * strategic_coverage
            + 0.20 * balance_pass_rate
        )
        final_score -= min(critical_count * 4.0, 25.0)
        final_score -= min(high_count * 1.5, 10.0)
        final_score = max(0.0, min(100.0, final_score))

        if final_score >= 90.0 and critical_count == 0:
            verdict = 'APPROVED_FOR_EXPERT_REVIEW'
        elif final_score >= 80.0:
            verdict = 'CONDITIONAL_APPROVAL'
        else:
            verdict = 'REQUIRES_REMEDIATION'

        top_reasons = ', '.join([f"{k}:{v}" for k, v in reason_counter.most_common(5)]) if reason_counter else 'None'

        acceptance_rows = [
            {'Metric': 'Ticker', 'Value': ticker},
            {'Metric': 'Sector_Profile', 'Value': sector_profile},
            {'Metric': 'Years', 'Value': f"{years[0]}-{years[-1]}" if years else ''},
            {'Metric': 'Effective_Years_For_Scoring', 'Value': ', '.join([str(y) for y in effective_years]) if effective_years else ''},
            {'Metric': 'Incomplete_Years_Skipped', 'Value': ', '.join([str(y) for y in incomplete_years]) if incomplete_years else 'None'},
            {'Metric': 'Ratio_Coverage_Pct', 'Value': round(ratio_coverage, 2)},
            {'Metric': 'Strategic_Coverage_Pct', 'Value': round(strategic_coverage, 2)},
            {'Metric': 'Balance_Identity_Pass_Pct', 'Value': round(balance_pass_rate, 2)},
            {'Metric': 'Ratio_Not_Computable_Count', 'Value': not_computable_count},
            {'Metric': 'Critical_Flag_Count', 'Value': critical_count},
            {'Metric': 'High_Flag_Count', 'Value': high_count},
            {'Metric': 'Quality_Gate_Corrections', 'Value': gate_issue_count},
            {'Metric': 'Top_NA_Reasons', 'Value': top_reasons},
            {'Metric': 'Final_Professional_Score', 'Value': round(final_score, 2)},
            {'Metric': 'Verdict', 'Value': verdict},
            {'Metric': 'Generated_At', 'Value': datetime.now().isoformat(timespec='seconds')},
        ]

        ratio_audit_df = pd.DataFrame(ratio_audit_rows)
        balance_df = pd.DataFrame(balance_checks)
        critical_df = pd.DataFrame(critical_rows) if critical_rows else pd.DataFrame([
            {
                'Severity': 'INFO',
                'Type': 'NONE',
                'Metric': 'No critical issues',
                'Year': '',
                'Details': '',
            }
        ])
        acceptance_df = pd.DataFrame(acceptance_rows)
        return ratio_audit_df, balance_df, critical_df, acceptance_df

    def display_ratios(self):
        if not self.current_data:
            for i in self.ratios_tree.get_children():
                self.ratios_tree.delete(i)
            return
        self._ratio_row_meta = {}
        for i in self.ratios_tree.get_children():
            self.ratios_tree.delete(i)
        ratios_by_year = maybe_guard_ratios_by_year(self.current_data.get('financial_ratios', {}) or {})
        data_by_year = ((self.current_data.get('data_layers', {}) or {}).get('layer1_by_year') or self.current_data.get('data_by_year', {}) or {})
        ticker = (self.current_data.get('company_info', {}) or {}).get('ticker', 'CURRENT')
        ratio_source = UnifiedRatioSource()
        ratio_source.load(ticker, data_by_year, ratios_by_year)
        sector_gating = (self.current_data.get('sector_gating', {}) if self.current_data else {}) or {}
        sector_profile = (sector_gating.get('profile') or 'industrial')
        blocked_ratios = set(sector_gating.get('blocked_ratios', []) or [])
        years = self._get_analysis_years_range()
        self._ratio_years = list(years)
        if not years:
            return
        
        ratio_col = self._t('ratio_col_name')
        explanation_col = self._t('ratio_col_explanation')
        cols = [ratio_col] + [str(y) for y in years] + [explanation_col]
        self.ratios_tree.config(columns=cols)
        for c in cols:
            self.ratios_tree.heading(c, text=self._translate_financial_item(c))
            if c == ratio_col:
                self.ratios_tree.column(c, width=280, anchor='w')
            elif c == explanation_col:
                self.ratios_tree.column(c, width=200)
            else:
                self.ratios_tree.column(c, width=120)
        
        def fmt_ratio_contract(m, contract):
            """Format ratio cell using structured contract output."""
            c = contract if isinstance(contract, dict) else {}
            v = c.get('value')
            if not isinstance(v, (int, float)):
                reason = str(c.get('reason') or c.get('status') or 'NOT_COMPUTABLE')
                return f"N/A ({reason})"
            dbg = format_ratio_value(m, v)
            return dbg.get('display_text', 'N/A')
        
        def insert_category_header(title):
            translated_title = self._translate_financial_item(title)
            self.ratios_tree.insert(
                '',
                'end',
                values=(f"- {translated_title} -",) + tuple([''] * (len(years) + 1)),
                tags=('header',),
            )
        
        def insert_ratio(display_name, ratio_key, explanation=''):
            if ratio_key in blocked_ratios:
                return
            translated_name = self._translate_financial_item(display_name)
            row = [translated_name]
            contracts_by_year = {}
            for y in years:
                c = ratio_source.get_ratio_contract(ticker, y, ratio_key)
                contracts_by_year[y] = c
                row.append(fmt_ratio_contract(ratio_key, c))
            row.append(self._translate_ratio_explanation(ratio_key, explanation))
            row_idx = len(self.ratios_tree.get_children(''))
            zebra_tag = 'zebra_even' if (row_idx % 2 == 0) else 'zebra_odd'
            iid = self.ratios_tree.insert('', 'end', values=row, tags=('child_row', zebra_tag))
            self._ratio_row_meta[iid] = {
                'ratio_key': ratio_key,
                'display_name': translated_name,
                'contracts_by_year': contracts_by_year,
            }
        
        self.ratios_tree.tag_configure('header', background='#e8f4f8', font=FONTS['label'])
        self.ratios_tree.tag_configure('child_row', font=FONTS['tree'])
        self.ratios_tree.tag_configure('zebra_even', background='#ffffff')
        self.ratios_tree.tag_configure('zebra_odd', background='#f7fbff')
        
        # â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
        # 1. Ù†Ø³Ø¨ Ø§Ù„Ø±Ø¨Ø­ÙŠØ© (Profitability Ratios)
        # â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
        def insert_ratio_group(title, entries):
            visible_entries = [entry for entry in entries if entry[1] not in blocked_ratios]
            if not visible_entries:
                return
            insert_category_header(title)
            for display_name, ratio_key, explanation in visible_entries:
                insert_ratio(display_name, ratio_key, explanation)

        sector_profile = (sector_gating.get('profile') or 'industrial')
        if sector_profile == 'bank':
            insert_ratio_group("Banking Core Ratios", [
                ("Net Interest Margin (NIM)", 'net_interest_margin', 'Ù‡Ø§Ù…Ø´ ØµØ§ÙÙŠ Ø¯Ø®Ù„ Ø§Ù„ÙÙˆØ§Ø¦Ø¯'),
                ("Loan-to-Deposit Ratio (LDR)", 'loan_to_deposit_ratio', 'Ù†Ø³Ø¨Ø© Ø§Ù„Ù‚Ø±ÙˆØ¶ Ø¥Ù„Ù‰ Ø§Ù„ÙˆØ¯Ø§Ø¦Ø¹'),
                ("Capital Ratio Proxy", 'capital_ratio_proxy', 'Ù…Ù„Ø§Ø¡Ø© Ø±Ø£Ø³ Ø§Ù„Ù…Ø§Ù„'),
                ("Efficiency Ratio", 'bank_efficiency_ratio', 'ÙƒÙØ§Ø¡Ø© Ø§Ù„ØªØ´ØºÙŠÙ„ Ø§Ù„Ø¨Ù†ÙƒÙŠ'),
                ("Net Income / Assets", 'net_income_to_assets', 'Ø±Ø¨Ø­ÙŠØ© Ø§Ù„Ø£ØµÙˆÙ„ Ø§Ù„Ø¨Ù†ÙƒÙŠØ©'),
                ("Equity Ratio", 'equity_ratio', 'Ù†Ø³Ø¨Ø© Ø­Ù‚ÙˆÙ‚ Ø§Ù„Ù…Ù„ÙƒÙŠØ© Ø¥Ù„Ù‰ Ø§Ù„Ø£ØµÙˆÙ„'),
            ])
            insert_ratio_group("Banking Profitability & Solvency", [
                ("ROA (Return on Assets)", 'roa', 'Ø§Ù„Ø¹Ø§Ø¦Ø¯ Ø¹Ù„Ù‰ Ø§Ù„Ø£ØµÙˆÙ„'),
                ("ROE (Return on Equity)", 'roe', 'Ø§Ù„Ø¹Ø§Ø¦Ø¯ Ø¹Ù„Ù‰ Ø§Ù„Ù…Ù„ÙƒÙŠØ©'),
                ("Net Profit Margin", 'net_margin', 'Ø§Ù„Ø±Ø¨Ø­ÙŠØ© Ø§Ù„Ù†Ù‡Ø§Ø¦ÙŠØ©'),
                ("Debt-to-Equity", 'debt_to_equity', 'Ø§Ù„Ø§Ø¹ØªÙ…Ø§Ø¯ Ø¹Ù„Ù‰ Ø§Ù„Ø§Ù‚ØªØ±Ø§Ø¶'),
                ("Debt-to-Assets", 'debt_to_assets', 'Ù†Ø³Ø¨Ø© Ø§Ù„Ø£ØµÙˆÙ„ Ø§Ù„Ù…Ù…ÙˆÙ‘Ù„Ø© Ø¨Ø§Ù„Ø¯ÙŠÙˆÙ†'),
            ])
            insert_ratio_group("Banking Market Ratios", [
                ("P/E Ratio", 'pe_ratio', 'Ù…ÙƒØ±Ø± Ø§Ù„Ø±Ø¨Ø­ÙŠØ©'),
                ("P/B Ratio", 'pb_ratio', 'Ø§Ù„Ø³ÙˆÙ‚ÙŠØ© Ù„Ù„Ø¯ÙØªØ±ÙŠØ©'),
                ("Dividend Yield", 'dividend_yield', 'Ø¹Ø§Ø¦Ø¯ ØªÙˆØ²ÙŠØ¹Ø§Øª Ø§Ù„Ø£Ø±Ø¨Ø§Ø­'),
                ("EPS (Earnings Per Share)", 'eps_basic', 'Ø±Ø¨Ø­ÙŠØ© Ø§Ù„Ø³Ù‡Ù…'),
                ("Book Value Per Share", 'book_value_per_share', 'Ø§Ù„Ù‚ÙŠÙ…Ø© Ø§Ù„Ø¯ÙØªØ±ÙŠØ© Ù„Ù„Ø³Ù‡Ù…'),
            ])
        elif sector_profile == 'insurance':
            insert_ratio_group("Insurance Core Ratios", [
                ("Combined Ratio Proxy", 'combined_proxy', 'Ù…Ø¤Ø´Ø± Ø§Ù„ÙƒÙØ§Ø¡Ø© Ø§Ù„ØªØ£Ù…ÙŠÙ†ÙŠØ©'),
                ("Capital Adequacy Proxy", 'capital_adequacy_proxy', 'Ø§Ù„Ù…Ù„Ø§Ø¡Ø© Ø§Ù„ØªØ£Ù…ÙŠÙ†ÙŠØ©'),
                ("Net Income / Assets", 'net_income_to_assets', 'Ø±Ø¨Ø­ÙŠØ© Ø§Ù„Ø£ØµÙˆÙ„'),
                ("Equity Ratio", 'equity_ratio', 'Ù†Ø³Ø¨Ø© Ø­Ù‚ÙˆÙ‚ Ø§Ù„Ù…Ù„ÙƒÙŠØ© Ø¥Ù„Ù‰ Ø§Ù„Ø£ØµÙˆÙ„'),
            ])
            insert_ratio_group("Insurance Market Ratios", [
                ("P/E Ratio", 'pe_ratio', 'Ù…ÙƒØ±Ø± Ø§Ù„Ø±Ø¨Ø­ÙŠØ©'),
                ("P/B Ratio", 'pb_ratio', 'Ø§Ù„Ø³ÙˆÙ‚ÙŠØ© Ù„Ù„Ø¯ÙØªØ±ÙŠØ©'),
                ("Dividend Yield", 'dividend_yield', 'Ø¹Ø§Ø¦Ø¯ ØªÙˆØ²ÙŠØ¹Ø§Øª Ø§Ù„Ø£Ø±Ø¨Ø§Ø­'),
                ("EPS (Earnings Per Share)", 'eps_basic', 'Ø±Ø¨Ø­ÙŠØ© Ø§Ù„Ø³Ù‡Ù…'),
                ("Book Value Per Share", 'book_value_per_share', 'Ø§Ù„Ù‚ÙŠÙ…Ø© Ø§Ù„Ø¯ÙØªØ±ÙŠØ© Ù„Ù„Ø³Ù‡Ù…'),
            ])
        else:
            insert_ratio_group("Profitability Ratios", [
                ("Gross Profit Margin", 'gross_margin', 'ÙƒÙØ§Ø¡Ø© Ø§Ù„Ø¥Ù†ØªØ§Ø¬'),
                ("Operating Profit Margin", 'operating_margin', 'ÙƒÙØ§Ø¡Ø© Ø§Ù„Ø¥Ø¯Ø§Ø±Ø©'),
                ("Net Profit Margin", 'net_margin', 'Ø§Ù„Ø±Ø¨Ø­ÙŠØ© Ø§Ù„Ù†Ù‡Ø§Ø¦ÙŠØ©'),
                ("EBITDA Margin", 'ebitda_margin', 'Ø§Ù„Ø±Ø¨Ø­ÙŠØ© Ø§Ù„Ù†Ù‚Ø¯ÙŠØ©'),
                ("ROA (Return on Assets)", 'roa', 'Ø§Ù„Ø¹Ø§Ø¦Ø¯ Ø¹Ù„Ù‰ Ø§Ù„Ø£ØµÙˆÙ„'),
                ("ROE (Return on Equity)", 'roe', 'Ø§Ù„Ø¹Ø§Ø¦Ø¯ Ø¹Ù„Ù‰ Ø§Ù„Ù…Ù„ÙƒÙŠØ©'),
                ("ROIC (Return on Invested Capital)", 'roic', 'Ø§Ù„Ø¹Ø§Ø¦Ø¯ Ø¹Ù„Ù‰ Ø±Ø£Ø³ Ø§Ù„Ù…Ø§Ù„'),
            ])
            insert_ratio_group("Activity & Efficiency Ratios", [
                ("Inventory Turnover", 'inventory_turnover', 'Ù…Ø¹Ø¯Ù„ Ø¯ÙˆØ±Ø§Ù† Ø§Ù„Ù…Ø®Ø²ÙˆÙ†'),
                ("Days Inventory Held (DIH)", 'inventory_days', 'ÙØªØ±Ø© Ø¨Ù‚Ø§Ø¡ Ø§Ù„Ù…Ø®Ø²ÙˆÙ†'),
                ("Days Sales Outstanding (DSO)", 'days_sales_outstanding', 'ÙØªØ±Ø© Ø§Ù„ØªØ­ØµÙŠÙ„'),
                ("Payables Turnover", 'payables_turnover', 'Ù…Ø¹Ø¯Ù„ Ø¯ÙˆØ±Ø§Ù† Ø§Ù„Ù…ÙˆØ±Ø¯ÙŠÙ†'),
                ("Days Payable Outstanding (DPO)", 'ap_days', 'ÙØªØ±Ø© Ø§Ù„Ø³Ø¯Ø§Ø¯'),
                ("Asset Turnover", 'asset_turnover', 'ÙƒÙØ§Ø¡Ø© Ø§Ø³ØªØ®Ø¯Ø§Ù… Ø§Ù„Ø£ØµÙˆÙ„'),
            ])
            insert_ratio_group("Liquidity Ratios", [
                ("Current Ratio", 'current_ratio', 'Ø§Ù„Ù‚Ø¯Ø±Ø© Ø¹Ù„Ù‰ Ø§Ù„Ø³Ø¯Ø§Ø¯ Ù‚ØµÙŠØ± Ø§Ù„Ø£Ø¬Ù„'),
                ("Quick Ratio", 'quick_ratio', 'Ø§Ù„Ø³ÙŠÙˆÙ„Ø© Ø§Ù„ÙÙˆØ±ÙŠØ©'),
                ("Cash Ratio", 'cash_ratio', 'Ø§Ù„Ø³ÙŠÙˆÙ„Ø© Ø§Ù„Ù†Ù‚Ø¯ÙŠØ© Ø§Ù„Ø¨Ø­ØªØ©'),
            ])
            insert_ratio_group("Solvency Ratios", [
                ("Debt-to-Equity", 'debt_to_equity', 'Ø§Ù„Ø§Ø¹ØªÙ…Ø§Ø¯ Ø¹Ù„Ù‰ Ø§Ù„Ø§Ù‚ØªØ±Ø§Ø¶'),
                ("Debt-to-Assets", 'debt_to_assets', 'Ù†Ø³Ø¨Ø© Ø§Ù„Ø£ØµÙˆÙ„ Ø§Ù„Ù…Ù…ÙˆÙ‘Ù„Ø© Ø¨Ø§Ù„Ø¯ÙŠÙˆÙ†'),
                ("Interest Coverage Ratio", 'interest_coverage', 'Ù‚Ø¯Ø±Ø© Ø¯ÙØ¹ Ø§Ù„ÙÙˆØ§Ø¦Ø¯'),
                ("Net Debt / EBITDA", 'net_debt_ebitda', 'Ù‚Ø¯Ø±Ø© ØªØºØ·ÙŠØ© Ø§Ù„Ø¯ÙŠÙˆÙ†'),
            ])
            insert_ratio_group("Market Ratios", [
                ("P/E Ratio", 'pe_ratio', 'Ù…ÙƒØ±Ø± Ø§Ù„Ø±Ø¨Ø­ÙŠØ©'),
                ("P/B Ratio", 'pb_ratio', 'Ø§Ù„Ø³ÙˆÙ‚ÙŠØ© Ù„Ù„Ø¯ÙØªØ±ÙŠØ©'),
                ("Dividend Yield", 'dividend_yield', 'Ø¹Ø§Ø¦Ø¯ ØªÙˆØ²ÙŠØ¹Ø§Øª Ø§Ù„Ø£Ø±Ø¨Ø§Ø­'),
                ("EPS (Earnings Per Share)", 'eps_basic', 'Ø±Ø¨Ø­ÙŠØ© Ø§Ù„Ø³Ù‡Ù…'),
                ("Book Value Per Share", 'book_value_per_share', 'Ø§Ù„Ù‚ÙŠÙ…Ø© Ø§Ù„Ø¯ÙØªØ±ÙŠØ© Ù„Ù„Ø³Ù‡Ù…'),
            ])

        insert_ratio_group("Valuation & Capital Ratios", [
            ("Market Cap (Million USD)", 'market_cap', 'Ø§Ù„Ù‚ÙŠÙ…Ø© Ø§Ù„Ø³ÙˆÙ‚ÙŠØ© Ø¨Ø§Ù„Ù…Ù„ÙŠÙˆÙ† Ø¯ÙˆÙ„Ø§Ø±'),
            ("Enterprise Value (Million USD)", 'enterprise_value', 'EV = Market Cap + Total Debt - Cash'),
            ("EV/EBITDA", 'ev_ebitda', 'Ù…Ø¶Ø§Ø¹Ù Ù‚ÙŠÙ…Ø© Ø§Ù„Ù…Ù†Ø´Ø£Ø© Ø¥Ù„Ù‰ EBITDA'),
            ("Cost of Debt", 'cost_of_debt', 'Ù…ØªÙˆØ³Ø· ØªÙƒÙ„ÙØ© Ø§Ù„Ø¯ÙŠÙ† Ø§Ù„ÙØ¹Ù„ÙŠØ©'),
            ("WACC", 'wacc', 'Ù…ØªÙˆØ³Ø· ØªÙƒÙ„ÙØ© Ø±Ø£Ø³ Ø§Ù„Ù…Ø§Ù„ Ø§Ù„Ù…Ø±Ø¬Ø­'),
            ("FCF Yield", 'fcf_yield', 'Ø¹Ø§Ø¦Ø¯ Ø§Ù„ØªØ¯ÙÙ‚ Ø§Ù„Ù†Ù‚Ø¯ÙŠ Ø§Ù„Ø­Ø±'),
        ])
        
        # Safety/risk and cashflow ratios are primarily meaningful for non-financial corporates.
        if sector_profile in ('industrial', 'technology'):
            altman_first = ratio_source.get_ratio_contract(ticker, years[0], 'altman_z_score').get('value') if years else None
            accrual_first = ratio_source.get_ratio_contract(ticker, years[0], 'accruals_ratio').get('value') if years else None
            insert_ratio_group("Safety & Risk Ratios", [
                ("Altman Z-Score", 'altman_z_score', self.fetcher.explain_ratio('altman_z_score', altman_first)),
                ("Accruals Ratio", 'accruals_ratio', self.fetcher.explain_ratio('accruals_ratio', accrual_first)),
            ])
            insert_ratio_group("Cash Flow Ratios", [
                ("Operating Cash Flow Margin", 'ocf_margin', 'Ù‡Ø§Ù…Ø´ Ø§Ù„ØªØ¯ÙÙ‚ Ø§Ù„ØªØ´ØºÙŠÙ„ÙŠ'),
                ("Free Cash Flow", 'free_cash_flow', 'Ø§Ù„ØªØ¯ÙÙ‚ Ø§Ù„Ù†Ù‚Ø¯ÙŠ Ø§Ù„Ø­Ø±'),
                ("FCF Per Share", 'fcf_per_share', 'Ø§Ù„ØªØ¯ÙÙ‚ Ø§Ù„Ø­Ø± Ù„Ù„Ø³Ù‡Ù…'),
            ])
        
        name = self.current_data.get('company_info', {}).get('name', '')
        if years:
            summary_by_lang = {
                'ar': (
                    f"📊 نظرة شاملة للنسب المالية لـ {name} — الفترة: {years[0]} - {years[-1]}\n"
                    "النسب مقسمة إلى فئات رئيسية لتسهيل التحليل والمقارنة."
                ),
                'en': (
                    f"📊 Comprehensive financial ratio view for {name} — Period: {years[0]} - {years[-1]}\n"
                    "Ratios are grouped into main categories for easier analysis and comparison."
                ),
                'fr': (
                    f"📊 Vue complète des ratios financiers pour {name} — Période : {years[0]} - {years[-1]}\n"
                    "Les ratios sont regroupés par catégories pour faciliter l'analyse et la comparaison."
                ),
            }
            self.ratios_comment.delete('1.0', 'end')
            self.ratios_comment.insert('1.0', summary_by_lang.get(self.current_lang, summary_by_lang['en']))

    def _on_ratio_tree_click(self, event):
        row_id = self.ratios_tree.identify_row(event.y)
        col_id = self.ratios_tree.identify_column(event.x)
        if not row_id or not col_id:
            return
        meta = (self._ratio_row_meta or {}).get(row_id)
        if not meta:
            return
        try:
            col_idx = int(str(col_id).replace('#', '')) - 1
        except Exception:
            return
        # 0: ratio name, 1..N: years, last: explanation
        if col_idx <= 0 or col_idx > len(self._ratio_years):
            return
        year = self._ratio_years[col_idx - 1]
        values = self.ratios_tree.item(row_id, 'values') or []
        if col_idx >= len(values):
            return
        cell_text = str(values[col_idx]).strip()
        if cell_text and not cell_text.upper().startswith('N/A') and cell_text not in ('', 'None'):
            return

        contract = (meta.get('contracts_by_year') or {}).get(year, {}) or {}
        # Keep UI explanation consistent with the exact source used to render the cell.
        # Only fallback to core_ratio_results if no contract details are present.
        if not isinstance(contract, dict) or not contract:
            try:
                core_year = ((self.current_data or {}).get('core_ratio_results', {}) or {}).get(year, {})
                core_contract = ((core_year.get('ratio_results', {}) or {}).get(meta.get('ratio_key')) or {})
                if isinstance(core_contract, dict) and core_contract:
                    contract = core_contract
            except Exception:
                pass
        reason_code = str(contract.get('reason') or contract.get('status') or 'UNKNOWN')
        reason_maps = {
            'ar': {
                'MISSING_SEC_CONCEPT': 'نقص بند محاسبي مطلوب من SEC لهذه السنة.',
                'MISSING_MARKET_DATA': 'نقص بيانات سوق مطلوبة (سعر/قيمة سوقية/بيتا...).',
                'MISSING_MARKET_LAYER': 'طبقة بيانات السوق غير مفعلة.',
                'MISSING_REQUIRED_LAYER': 'إحدى الطبقات المطلوبة للحساب غير مفعلة أو غير متاحة.',
                'PERIOD_MISMATCH': 'عدم تطابق الفترات الزمنية بين مدخلات النسبة.',
                'UNIT_MISMATCH': 'اختلاف وحدات القياس بين المدخلات.',
                'ZERO_DENOMINATOR': 'المقام يساوي صفر، لا يمكن إتمام القسمة.',
                'INSUFFICIENT_HISTORY': 'البيانات التاريخية غير كافية للحساب.',
                'DATA_NOT_APPLICABLE': 'النسبة غير قابلة للتطبيق على البيانات المتوفرة.',
                'NOT_APPLICABLE_FOR_SECTOR': 'النسبة غير ملائمة لقطاع الشركة وتم حجبها تلقائياً.',
                'NOT_COMPUTABLE': 'النسبة غير قابلة للحساب من البيانات الحالية.',
                'UNKNOWN': 'سبب غير محدد في محرك الحساب.',
            },
            'en': {
                'MISSING_SEC_CONCEPT': 'Required SEC concept is missing for this year.',
                'MISSING_MARKET_DATA': 'Required market input is missing (price/market cap/beta).',
                'MISSING_MARKET_LAYER': 'Market data layer is disabled.',
                'MISSING_REQUIRED_LAYER': 'One required layer is disabled or unavailable.',
                'PERIOD_MISMATCH': 'Input facts are not aligned to the same period.',
                'UNIT_MISMATCH': 'Input facts use incompatible units.',
                'ZERO_DENOMINATOR': 'Denominator is zero; division is not possible.',
                'INSUFFICIENT_HISTORY': 'Historical data is not sufficient for this ratio.',
                'DATA_NOT_APPLICABLE': 'Ratio is not applicable to available data.',
                'NOT_APPLICABLE_FOR_SECTOR': 'Ratio is hidden for this sector profile.',
                'NOT_COMPUTABLE': 'Ratio cannot be computed from current inputs.',
                'UNKNOWN': 'Unspecified reason from ratio engine.',
            },
            'fr': {
                'MISSING_SEC_CONCEPT': 'Concept SEC requis manquant pour cette année.',
                'MISSING_MARKET_DATA': 'Donnée de marché requise manquante (prix/capitalisation/bêta).',
                'MISSING_MARKET_LAYER': 'La couche de marché est désactivée.',
                'MISSING_REQUIRED_LAYER': 'Une couche requise est désactivée ou indisponible.',
                'PERIOD_MISMATCH': 'Les périodes des données ne sont pas alignées.',
                'UNIT_MISMATCH': 'Les unités des données sont incompatibles.',
                'ZERO_DENOMINATOR': 'Le dénominateur est nul; division impossible.',
                'INSUFFICIENT_HISTORY': 'Historique insuffisant pour ce ratio.',
                'DATA_NOT_APPLICABLE': 'Ratio non applicable aux données disponibles.',
                'NOT_APPLICABLE_FOR_SECTOR': 'Ratio masqué pour ce profil sectoriel.',
                'NOT_COMPUTABLE': 'Ratio non calculable avec les entrées actuelles.',
                'UNKNOWN': 'Raison non spécifiée par le moteur des ratios.',
            },
        }
        reason_map = reason_maps.get(self.current_lang, reason_maps['en'])
        missing_inputs = contract.get('missing_inputs') or []
        missing_text = ''
        if missing_inputs:
            missing_label = {
                'ar': 'البنود الناقصة',
                'en': 'Missing Inputs',
                'fr': 'Entrées manquantes',
            }.get(self.current_lang, 'Missing Inputs')
            missing_text = f"\n{missing_label}: {', '.join([str(x) for x in missing_inputs])}"

        labels = {
            'ar': ('النسبة', 'السنة', 'الحالة', 'السبب التقني', 'التفسير', 'سبب N/A'),
            'en': ('Ratio', 'Year', 'Status', 'Technical Reason', 'Explanation', 'N/A Reason'),
            'fr': ('Ratio', 'Année', 'Statut', 'Raison technique', 'Explication', 'Raison N/A'),
        }.get(self.current_lang, ('Ratio', 'Year', 'Status', 'Technical Reason', 'Explanation', 'N/A Reason'))
        details = (
            f"{labels[0]}: {meta.get('display_name')}\n"
            f"{labels[1]}: {year}\n"
            f"{labels[2]}: N/A\n"
            f"{labels[3]}: {reason_code}\n"
            f"{labels[4]}: {reason_map.get(reason_code, reason_map['UNKNOWN'])}"
            f"{missing_text}"
        )
        self.ratios_comment.delete('1.0', 'end')
        self.ratios_comment.insert('1.0', details)
        messagebox.showinfo(labels[5], details)

    # ---------- compute per-year metrics ----------
    def _compute_per_year_metrics(self, data_by_year, ratios_by_year):
        years = self._get_analysis_years_range()
        if not years:
            return {}
        ratios_by_year = maybe_guard_ratios_by_year(ratios_by_year or {})
        self._assert_no_legacy_ratio_keys(ratios_by_year)
        ticker = (self.current_data or {}).get('company_info', {}).get('ticker', 'CURRENT')
        sector_gating = (self.current_data.get('sector_gating', {}) if self.current_data else {}) or {}
        blocked_ratios = set(sector_gating.get('blocked_ratios', []) or [])
        ratio_source = UnifiedRatioSource()
        ratio_source.load(ticker, data_by_year or {}, ratios_by_year)
        data_layers_ctx = (self.current_data.get('data_layers', {}) if self.current_data else {}) or {}
        layer2_by_year = data_layers_ctx.get('layer2_by_year', {}) or {}
        layer4_by_year = data_layers_ctx.get('layer4_by_year', {}) or {}
        manual_split_rules = {
            'NVDA': {'cutoff_year': 2023, 'ratio': 10.0},
        }
        # Keep quality gate conservative by default: do not rewrite core accounting ratios.
        allow_aggressive = str(os.environ.get('QUALITY_GATE_AGGRESSIVE', '0')).strip().lower() in ('1', 'true', 'yes')

        def get_layer_num(year_key, *keys):
            try:
                row2 = layer2_by_year.get(year_key, {}) or {}
                row4 = layer4_by_year.get(year_key, {}) or {}
                for k in keys:
                    v = row2.get(k)
                    if isinstance(v, (int, float)):
                        fv = float(v)
                        lk = str(k).lower()
                        if any(tok in lk for tok in ('market_cap', 'enterprise_value', 'total_debt')):
                            if abs(fv) > 1_000_000_000:
                                fv = fv / 1_000_000.0
                        return fv
                    v = row4.get(k)
                    if isinstance(v, (int, float)):
                        fv = float(v)
                        lk = str(k).lower()
                        if any(tok in lk for tok in ('market_cap', 'enterprise_value', 'total_debt')):
                            if abs(fv) > 1_000_000_000:
                                fv = fv / 1_000_000.0
                        return fv
                return None
            except Exception:
                return None

        def get_split_factor(year_key):
            sf = get_layer_num(year_key, 'market:split_latest_ratio', 'yahoo:split_latest_ratio')
            if isinstance(sf, (int, float)) and sf > 1.0:
                return float(sf)
            rule = manual_split_rules.get(str(ticker).upper())
            if rule and int(year_key) < int(rule.get('cutoff_year', 0)):
                return float(rule.get('ratio') or 1.0)
            return 1.0

        def _to_million_market(value):
            try:
                if value is None:
                    return None
                fv = float(value)
                if abs(fv) > 1_000_000_000:
                    return fv / 1_000_000.0
                return fv
            except Exception:
                return None
        # market inputs: check UI then current_data.market_data
        price = float(self.price_var.get() or 0.0)
        shares = float(self.shares_var.get() or 0.0)
        if shares <= 0:
            shares = None
        market_cap_ui = (price * shares / 1_000_000.0) if price and shares else None
        market_data = self.current_data.get('market_data', {}) if self.current_data else {}
        market_cap_md = _to_million_market(market_data.get('market_cap')) if market_data else None
        market_cap = market_cap_ui or market_cap_md
        sector_profile_current = (
            ((self.current_data.get('sector_gating', {}) if self.current_data else {}) or {}).get('profile')
            or 'industrial'
        ).lower()
        def _first_not_none_outer(*vals):
            for vv in vals:
                if vv is not None:
                    return vv
            return None
        # Detect structural no-dividend profile to avoid repeated NOT_COMPUTABLE gaps
        # for issuers that historically do not pay dividends.
        no_dividend_profile = False
        try:
            dy_vals = []
            div_vals = []
            for yy in years:
                row_yy = (data_by_year.get(yy, {}) or {})
                l2_yy = (layer2_by_year.get(yy, {}) or {})
                l4_yy = (layer4_by_year.get(yy, {}) or {})
                dy_vals.append(_first_not_none_outer(
                    l2_yy.get('market:dividend_yield'),
                    l4_yy.get('yahoo:dividend_yield'),
                ))
                div_vals.append(_first_not_none_outer(
                    row_yy.get('DividendsPaid'),
                    row_yy.get('PaymentsOfDividends'),
                    row_yy.get('PaymentsOfDividendsCommonStock'),
                    row_yy.get('CashDividendsPaid'),
                ))
            has_positive_dividend_marker = any(
                isinstance(v, (int, float)) and abs(float(v)) > 0
                for v in (dy_vals + div_vals)
            )
            if not has_positive_dividend_marker:
                no_dividend_profile = True
        except Exception:
            no_dividend_profile = False

        cost_of_debt_input = float(self.cost_of_debt_var.get() or 0.0) / 100.0
        tax_rate_default = 0.21

        per_year = {}
        for idx, y in enumerate(years):
            vals = {}
            is_bank = (sector_profile_current == 'bank')
            alias = {
                'NetIncomeLoss': ['Net Income'],
                'OperatingIncomeLoss': ['Operating Income'],
                'Revenues': [],
                'SalesRevenueNet': ['Net revenue', 'Net sales'],
                'Assets': [],
                'Liabilities': [],
                'StockholdersEquity': ['Total Equity'],
                'NetCashProvidedByUsedInOperatingActivities': ['Operating Cash Flow'],
                'PaymentsToAcquirePropertyPlantAndEquipment': ['Capital Expenditures'],
                'CashAndCashEquivalentsAtCarryingValue': ['Cash and Cash Equivalents'],
                'WeightedAverageNumberOfSharesOutstandingBasic': ['Basic (shares)', 'SharesBasic'],
                'DividendsPaid': [
                    'DividendsPaid',
                    'PaymentsOfDividends',
                    'PaymentsOfDividendsCommonStock',
                    'DividendsCommonStockCash',
                    'Dividends',
                    'Payments for dividends and dividend equivalents',
                ],
            }

            def _get_from_row(row, key):
                if not isinstance(row, dict):
                    return None
                for k in [key] + alias.get(key, []):
                    if k in row and row.get(k) is not None:
                        return row.get(k)
                return None

            def get_data(key):
                return _get_from_row(data_by_year.get(y, {}), key)

            def get_data_year(year_key, key):
                return _get_from_row(data_by_year.get(year_key, {}), key)
            def first_not_none(*vals):
                for vv in vals:
                    if vv is not None:
                        return vv
                return None
            def get_contract(ratio_id):
                c = ratio_source.get_ratio_contract(ticker, y, ratio_id)
                return {
                    'value': c.get('value'),
                    'reliability': c.get('reliability', 0),
                    'reason': c.get('reason'),
                    'source': c.get('source', 'ratio_engine'),
                    'format': c.get('ratio_format'),
                    'suffix': '%' if c.get('ratio_format') == 'percent' else '',
                    'display': format_ratio_value(ratio_id, c.get('value')).get('display_text'),
                }
            def get_ratio(key):
                if key in blocked_ratios:
                    return None
                # Prefer ratio-engine contract for scale-sensitive metrics.
                sensitive = {
                    'eps_basic',
                    'pe_ratio',
                    'pb_ratio',
                    'book_value_per_share',
                    'fcf_yield',
                    'fcf_per_share',
                    'shares_outstanding',
                }
                if key in sensitive:
                    cval = get_contract(key).get('value')
                    if isinstance(cval, (int, float)):
                        return float(cval)
                row_val = (ratios_by_year.get(y, {}) or {}).get(key)
                if isinstance(row_val, dict) and 'value' in row_val:
                    row_val = row_val.get('value')
                if isinstance(row_val, (int, float)):
                    return float(row_val)
                return get_contract(key).get('value')

            def get_ratio_year(year_key, key):
                if year_key is None:
                    return None
                c = ratio_source.get_ratio_contract(ticker, year_key, key)
                cval = c.get('value')
                if isinstance(cval, (int, float)):
                    return float(cval)
                row_val = (ratios_by_year.get(year_key, {}) or {}).get(key)
                if isinstance(row_val, dict) and 'value' in row_val:
                    row_val = row_val.get('value')
                if isinstance(row_val, (int, float)):
                    return float(row_val)
                return None

            vals['ROIC'] = get_ratio('roic')
            vals['ROA'] = get_ratio('roa')
            vals['ROE'] = get_ratio('roe')
            vals['Net_Interest_Margin'] = get_ratio('net_interest_margin')
            vals['Loan_to_Deposit_Ratio'] = get_ratio('loan_to_deposit_ratio')
            vals['Capital_Ratio_Proxy'] = get_ratio('capital_ratio_proxy')
            vals['Bank_Efficiency_Ratio'] = get_ratio('bank_efficiency_ratio')
            vals['Bank_Total_Revenue'] = get_ratio('bank_total_revenue')
            vals['Net_Income_to_Assets'] = get_ratio('net_income_to_assets')
            vals['Equity_Ratio'] = get_ratio('equity_ratio')
            vals['Combined_Ratio_Proxy'] = get_ratio('combined_proxy')
            vals['Capital_Adequacy_Proxy'] = get_ratio('capital_adequacy_proxy')
            
            # Retention/SGR with fallback using SEC/Yahoo layer fields.
            dividends_paid = get_ratio('dividends_paid')
            if dividends_paid is None:
                dividends_paid = get_data('DividendsPaid')
            if dividends_paid is None:
                # Additional SEC aliases frequently used across issuers/years.
                dividends_paid = first_not_none(
                    get_data('PaymentsOfDividends'),
                    get_data('PaymentsOfDividendsCommonStock'),
                    get_data('DividendsCommonStockCash'),
                    get_data('CashDividendsPaid'),
                    get_data('Dividends'),
                )
            if dividends_paid is None:
                dy = get_layer_num(y, 'market:dividend_yield', 'yahoo:dividend_yield')
                px = get_layer_num(y, 'market:price', 'yahoo:price')
                sh = get_layer_num(y, 'market:shares_outstanding', 'yahoo:shares_outstanding')
                if dy is not None and px is not None and sh is not None:
                    try:
                        dividends_paid = abs(dy * px * sh)
                    except Exception:
                        dividends_paid = None
            if dividends_paid is None:
                # Yahoo payout ratio fallback: Dividends = payout_ratio * net income
                payout_ratio = get_layer_num(y, 'yahoo:payout_ratio')
                ni_tmp = get_data('Net Income')
                if ni_tmp is None:
                    ni_tmp = get_data('NetIncomeLoss')
                if payout_ratio is not None and ni_tmp is not None:
                    try:
                        dividends_paid = abs(payout_ratio * ni_tmp)
                    except Exception:
                        dividends_paid = None
            if dividends_paid is None:
                # Yahoo dividend rate fallback: annual dividend per share * shares outstanding
                div_rate = get_layer_num(y, 'yahoo:dividend_rate', 'market:annual_dividends_per_share')
                sh = get_layer_num(y, 'market:shares_outstanding', 'yahoo:shares_outstanding')
                if div_rate is not None and sh is not None:
                    try:
                        dividends_paid = abs(div_rate * sh)
                    except Exception:
                        dividends_paid = None
            vals['Dividends_Paid'] = dividends_paid if dividends_paid is not None else None

            ni_for_retention = get_data('Net Income')
            if ni_for_retention is None:
                ni_for_retention = get_data('NetIncomeLoss')
            retention_ratio = get_ratio('retention_ratio')
            if retention_ratio is None and ni_for_retention is not None and dividends_paid is not None and ni_for_retention != 0:
                try:
                    ni_base = float(ni_for_retention)
                    div_base = float(dividends_paid)
                    # Align likely scale mismatch (SEC rows often in millions vs market absolute).
                    if abs(div_base) > 1_000_000_000 and abs(ni_base) < 1_000_000:
                        ni_base = ni_base * 1_000_000.0
                    retention_ratio = 1.0 - (abs(div_base) / abs(ni_base))
                    retention_ratio = max(0.0, min(1.0, retention_ratio))
                except Exception:
                    retention_ratio = None
            vals['Retention_Ratio'] = retention_ratio

            sgr_contract = get_contract('sgr_internal')
            sgr_value = sgr_contract.get('value') if isinstance(sgr_contract, dict) else None
            if sgr_value is None and retention_ratio is not None:
                try:
                    roe_val = get_contract('roe').get('value')
                    if roe_val is not None:
                        sgr_value = retention_ratio * roe_val
                except Exception:
                    sgr_value = None
            vals['SGR_Internal'] = sgr_value if sgr_value is not None else None
            vals['Gross_Margin'] = get_ratio('gross_margin')
            vals['Operating_Margin'] = get_ratio('operating_margin')
            vals['Net_Margin'] = get_ratio('net_margin')
            vals['Current_Ratio'] = get_ratio('current_ratio')
            vals['Quick_Ratio'] = get_ratio('quick_ratio')
            vals['Cash_Ratio'] = get_ratio('cash_ratio')
            vals['OCF_Margin'] = get_ratio('ocf_margin')

            ni = get_data('Net Income')
            if ni is None:
                ni = get_data('NetIncomeLoss')
            # âœ… FIX: prev_y should be the PREVIOUS year (idx-1), not next year!
            prev_y = years[idx-1] if idx > 0 else None
            ni_growth = None
            if prev_y:
                ni_prev = get_data_year(prev_y, 'Net Income')
                if ni_prev is None:
                    ni_prev = get_data_year(prev_y, 'NetIncomeLoss')
                if ni is not None and ni_prev is not None and ni_prev != 0:
                    try:
                        ni_growth = (ni - ni_prev) / abs(ni_prev)
                    except:
                        ni_growth = None
            vals['NI_Growth'] = ni_growth

            ebitda = get_data('EBITDA')
            if ebitda is None:
                op = get_data('OperatingIncomeLoss') or 0.0
                dep = get_data('DepreciationDepletionAndAmortization') or 0.0
                ebitda = op + dep if op is not None else None
            vals['EBITDA'] = ebitda

            eps = get_ratio('eps_basic')
            if eps is None:
                try:
                    shares_basic = get_data('WeightedAverageNumberOfSharesOutstandingBasic') or get_data('EntityCommonStockSharesOutstanding')
                    if shares_basic and shares_basic != 0 and ni is not None:
                        split_factor = get_split_factor(y)
                        sh_adj = shares_basic * split_factor if split_factor > 1.0 else shares_basic
                        eps = ni / sh_adj if sh_adj else None
                except:
                    eps = None
            vals['EPS'] = eps

            ocf = get_data('NetCashProvidedByUsedInOperatingActivities')
            capex = first_not_none(
                get_data('PaymentsToAcquirePropertyPlantAndEquipment'),
                get_data('CapitalExpenditures'),
                0.0,
            )
            fcf = get_ratio('free_cash_flow')
            if fcf is None and ocf is not None:
                fcf = ocf - abs(capex or 0.0)
            vals['FCF'] = fcf
            shares_basic_year = get_ratio('shares_outstanding')
            if shares_basic_year is None:
                shares_basic_year = first_not_none(
                    get_data('WeightedAverageNumberOfSharesOutstandingBasic'),
                    get_data('EntityCommonStockSharesOutstanding'),
                )
            split_factor = get_split_factor(y)
            if isinstance(shares_basic_year, (int, float)) and shares_basic_year > 0 and split_factor > 1.0:
                shares_basic_year = shares_basic_year * split_factor
            if isinstance(shares_basic_year, (int, float)) and shares_basic_year <= 0:
                shares_basic_year = None
            market_cap_layer = get_layer_num(y, 'market:market_cap', 'yahoo:market_cap')
            price_year = price if price and price > 0 else get_layer_num(y, 'market:price', 'yahoo:price')
            # Reconstruct market cap from price * shares (million USD) with split-aware shares
            # and prefer reconstructed value when layer market cap is clearly inconsistent.
            market_cap_reconstructed = None
            if isinstance(price_year, (int, float)) and price_year > 0 and isinstance(shares_basic_year, (int, float)) and shares_basic_year > 0:
                market_cap_reconstructed = (price_year * shares_basic_year) / 1_000_000.0

            market_cap_year = market_cap_ui or market_cap_layer or market_cap_reconstructed or market_cap
            if (
                isinstance(market_cap_layer, (int, float))
                and market_cap_layer > 0
                and isinstance(market_cap_reconstructed, (int, float))
                and market_cap_reconstructed > 0
            ):
                ratio_mc = abs(float(market_cap_layer)) / max(abs(float(market_cap_reconstructed)), 1e-9)
                # Hard guard against split/unit mismatch (e.g., 10x error on pre-split years).
                if ratio_mc < 0.2 or ratio_mc > 5.0:
                    market_cap_year = market_cap_reconstructed
                    vals['Market_Cap_Source'] = 'PRICE_X_SPLIT_ADJUSTED_SHARES_RECONSTRUCTED'
                else:
                    vals['Market_Cap_Source'] = 'LAYER_MARKET_CAP'
            elif market_cap_year is not None:
                vals['Market_Cap_Source'] = 'LAYER_OR_UI_FALLBACK'
            fcf_yield = get_ratio('fcf_yield')
            if fcf_yield is None and (fcf is not None and market_cap_year):
                fcf_yield = (fcf / market_cap_year)
            vals['FCF_Yield'] = fcf_yield
            
            # âœ… Point 5: FCF per Share
            fcf_per_share = get_ratio('fcf_per_share')
            if fcf_per_share is None and fcf is not None:
                try:
                    shares_basic = first_not_none(
                        shares_basic_year,
                        get_data('WeightedAverageNumberOfSharesOutstandingBasic'),
                        get_data('EntityCommonStockSharesOutstanding'),
                    )
                    if isinstance(shares_basic, (int, float)) and shares_basic > 0 and split_factor > 1.0:
                        shares_basic = shares_basic * split_factor
                    if shares_basic and shares_basic != 0:
                        cand = [
                            fcf / shares_basic,
                            (fcf * 1_000.0) / shares_basic,
                            (fcf * 1_000_000.0) / shares_basic,
                        ]
                        if eps is not None and isinstance(eps, (int, float)) and abs(eps) >= 0.1:
                            plausible = [c for c in cand if isinstance(c, (int, float)) and abs(c) < 50000]
                            fcf_per_share = min(plausible, key=lambda c: abs(abs(c) - abs(eps))) if plausible else cand[0]
                        else:
                            plausible = [c for c in cand if isinstance(c, (int, float)) and 0.1 <= abs(c) < 50000]
                            fcf_per_share = plausible[0] if plausible else cand[0]
                except:
                    fcf_per_share = None
            vals['FCF_per_Share'] = fcf_per_share

            # âœ… Market Ratios: consume base ratios first, fallback only if missing
            price = float(self.price_var.get() or 0.0)
            price_effective = price if price and price > 0 else get_layer_num(y, 'market:price', 'yahoo:price')
            vals['PE_Ratio'] = get_ratio('pe_ratio')
            vals['PB_Ratio'] = get_ratio('pb_ratio')
            vals['PE_Ratio_Used'] = first_not_none(get_ratio('pe_ratio_used'), vals['PE_Ratio'])
            vals['PB_Ratio_Used'] = first_not_none(get_ratio('pb_ratio_used'), vals['PB_Ratio'])
            vals['Dividend_Yield'] = first_not_none(
                get_ratio('dividend_yield'),
                get_layer_num(y, 'market:dividend_yield', 'yahoo:dividend_yield'),
            )
            if vals['Dividend_Yield'] is None and no_dividend_profile:
                vals['Dividend_Yield'] = 0.0
            vals['EV_EBITDA'] = None
            if price_effective and price_effective > 0:
                if vals['PE_Ratio'] is None and eps and eps != 0:
                    vals['PE_Ratio'] = price_effective / eps
                if vals['PB_Ratio'] is None:
                    book_value_per_share = get_ratio('book_value_per_share')
                    if book_value_per_share and book_value_per_share != 0:
                        vals['PB_Ratio'] = price_effective / book_value_per_share
                if vals['Dividend_Yield'] is None:
                    shares_basic = first_not_none(
                        shares_basic_year,
                        get_data('WeightedAverageNumberOfSharesOutstandingBasic'),
                        get_data('EntityCommonStockSharesOutstanding'),
                    )
                    if dividends_paid and shares_basic and shares_basic != 0:
                        div_per_share = abs(dividends_paid) / shares_basic
                        vals['Dividend_Yield'] = (div_per_share / price_effective)

            # EV / EBITDA with explicit unit alignment (EV in USD, EBITDA often in millions).
            try:
                ev_year = get_layer_num(y, 'market:enterprise_value', 'yahoo:enterprise_value')
                if ev_year is None:
                    debt_ev = first_not_none(
                        get_ratio('total_debt'),
                        get_layer_num(y, 'market:total_debt', 'yahoo:total_debt'),
                    )
                    cash_ev = first_not_none(
                        get_data('CashAndCashEquivalentsAtCarryingValue'),
                        get_data('CashAndCashEquivalents'),
                        get_data('Cash and Cash Equivalents'),
                    )
                    mcap_ev = market_cap_year
                    if mcap_ev is not None and debt_ev is not None and cash_ev is not None:
                        try:
                            cash_candidates = [float(cash_ev), float(cash_ev) * 1_000.0, float(cash_ev) * 1_000_000.0]
                            cash_adj = min(cash_candidates, key=lambda c: abs((c / max(abs(mcap_ev), 1e-9)) - 0.02))
                            ev_year = float(mcap_ev) + float(debt_ev) - float(cash_adj)
                        except Exception:
                            ev_year = None
                ebitda_for_ev = get_data('Ebitda') or get_data('EBITDA') or ebitda
                if ev_year is not None and ebitda_for_ev not in (None, 0):
                    # Normalize EV around market-cap anchor to avoid million/dollar slips.
                    ev_candidates = [float(ev_year), float(ev_year) / 1_000.0, float(ev_year) / 1_000_000.0, float(ev_year) * 1_000.0]
                    ev_candidates = [v for v in ev_candidates if v > 0]
                    if isinstance(market_cap_year, (int, float)) and market_cap_year > 0:
                        ev_norm = min(ev_candidates, key=lambda v: abs((v - float(market_cap_year)) / max(abs(float(market_cap_year)), 1.0)))
                    else:
                        ev_norm = max(ev_candidates)

                    cands = [
                        float(ebitda_for_ev),
                        float(ebitda_for_ev) * 1_000.0,
                        float(ebitda_for_ev) * 1_000_000.0,
                        float(ebitda_for_ev) * 1_000_000_000.0,
                    ]
                    cands = [c for c in cands if c > 0]
                    strict = [c for c in cands if 2.0 <= (ev_norm / c) <= 120.0]
                    plausible = strict if strict else [c for c in cands if 0.5 <= (ev_norm / c) <= 200.0]
                    denom = min(plausible, key=lambda c: abs((ev_norm / c) - 20.0)) if plausible else None
                    vals['EV_EBITDA'] = (ev_norm / denom) if denom else None
            except Exception:
                vals['EV_EBITDA'] = None

            nd_eb = get_ratio('net_debt_ebitda')
            if nd_eb is None:
                try:
                    short = get_data('ShortTermBorrowings') or 0.0
                    longd = get_data('LongTermDebt') or 0.0
                    cash = get_data('CashAndCashEquivalentsAtCarryingValue') or 0.0
                    net_debt = short + longd - cash
                    if ebitda and ebitda != 0:
                        nd_eb = net_debt / ebitda
                except:
                    nd_eb = None
            vals['Net_Debt_EBITDA'] = nd_eb

            vals['Altman_Z_Score'] = get_ratio('altman_z_score')

            accr = get_ratio('accruals_ratio')
            if accr is None:
                try:
                    assets = get_data('Assets')
                    if ni is not None and ocf is not None and assets:
                        accr = (ni - ocf) / assets
                except:
                    accr = None
            vals['Accruals_Ratio'] = accr

            accr_change = None
            if prev_y:
                try:
                    accr_prev = get_ratio_year(prev_y, 'accruals_ratio')
                    if accr_prev is None:
                        assets_prev = get_data_year(prev_y, 'Assets')
                        ni_prev = get_data_year(prev_y, 'NetIncomeLoss')
                        ocf_prev = get_data_year(prev_y, 'NetCashProvidedByUsedInOperatingActivities')
                        accr_prev = (ni_prev - ocf_prev) / assets_prev if assets_prev and ni_prev is not None and ocf_prev is not None else None
                    if accr is not None and accr_prev is not None:
                        accr_change = accr - accr_prev
                except:
                    accr_change = None
            vals['Accruals_Change'] = accr_change

            op_lev = None
            try:
                op_income = get_data('OperatingIncomeLoss') or 0.0
                if prev_y:
                    op_prev = get_data_year(prev_y, 'OperatingIncomeLoss')
                    rev = get_data('Revenues') or get_data('SalesRevenueNet') or 0.0
                    rev_prev = get_data_year(prev_y, 'Revenues') or get_data_year(prev_y, 'SalesRevenueNet')
                    if op_prev is not None and rev_prev is not None and rev_prev != 0 and op_prev != 0:
                        pct_op = (op_income - op_prev) / abs(op_prev)
                        pct_rev = (rev - rev_prev) / abs(rev_prev) if rev_prev != 0 else None
                        if pct_rev and pct_rev != 0:
                            op_lev = pct_op / pct_rev
            except:
                op_lev = None
            vals['Op_Leverage'] = op_lev

            dih = get_ratio('inventory_days')
            dso = get_ratio('days_sales_outstanding')
            dpo = get_ratio('ap_days')
            ccc = get_ratio('ccc_days')
            vals['Inventory_Days'] = dih
            vals['AR_Days'] = dso
            vals['AP_Days'] = dpo
            vals['CCC_Days'] = ccc
            wacc_bank_low = False
            fair_value_warning = None
            rating_adjusted_for_losses = False

            # Prefer ratio-engine CoD first, then sector-aware derivation.
            ratio_cost_of_debt = get_ratio('cost_of_debt')
            derived_cost_of_debt = None
            cost_of_debt_source = None
            try:
                if ratio_cost_of_debt is None:
                    if is_bank:
                        ltd_interest = first_not_none(
                            get_data('InterestExpenseLongTermDebt'),
                            get_data('InterestExpenseDebt'),
                            get_data('InterestAndDebtExpense'),
                            get_data('InterestExpenseBorrowings'),
                        )
                        ltd_debt = first_not_none(
                            get_data('LongTermDebtNoncurrent'),
                            get_data('LongTermDebt'),
                            get_data('DebtNoncurrent'),
                            get_data('LongTermBorrowings'),
                        )
                        if ltd_interest is not None and ltd_debt not in (None, 0):
                            cod_candidates = []
                            scale_penalty = {1.0: 0.0, 1_000.0: 1.0, 1_000_000.0: 2.0, 0.001: 1.0, 0.000001: 2.0}
                            i_cands = [
                                (abs(float(ltd_interest)), 1.0),
                                (abs(float(ltd_interest)) / 1_000.0, 0.001),
                                (abs(float(ltd_interest)) / 1_000_000.0, 0.000001),
                                (abs(float(ltd_interest)) * 1_000.0, 1_000.0),
                            ]
                            d_cands = [
                                (abs(float(ltd_debt)), 1.0),
                                (abs(float(ltd_debt)) / 1_000.0, 0.001),
                                (abs(float(ltd_debt)) / 1_000_000.0, 0.000001),
                                (abs(float(ltd_debt)) * 1_000.0, 1_000.0),
                            ]
                            for ic, isc in i_cands:
                                for dc, dsc in d_cands:
                                    if dc in (None, 0):
                                        continue
                                    cv = ic / dc
                                    if 0.001 <= cv <= 0.10:
                                        score = abs(cv - 0.04) + (0.05 * (scale_penalty.get(isc, 1.0) + scale_penalty.get(dsc, 1.0)))
                                        cod_candidates.append((score, cv))
                            if cod_candidates:
                                derived_cost_of_debt = min(cod_candidates, key=lambda x: x[0])[1]
                                cost_of_debt_source = 'BANK_LT_DEBT_INTEREST'
                    if derived_cost_of_debt is None:
                        interest_used = get_ratio('interest_expense_used')
                        debt_now = get_ratio('total_debt')
                        debt_prev = get_ratio_year(prev_y, 'total_debt') if prev_y else None
                        debt_avg = None
                        if debt_now and debt_prev:
                            debt_avg = (abs(debt_now) + abs(debt_prev)) / 2.0
                        elif debt_now:
                            debt_avg = abs(debt_now)
                        if interest_used and debt_avg and debt_avg > 0:
                            derived_cost_of_debt = abs(float(interest_used)) / float(debt_avg)
                            cost_of_debt_source = 'SEC_INTEREST_OVER_DEBT_BASE'
            except Exception:
                derived_cost_of_debt = None
                cost_of_debt_source = None

            vals['Cost_of_Debt'] = (
                ratio_cost_of_debt
                if ratio_cost_of_debt is not None
                else (
                    derived_cost_of_debt
                    if derived_cost_of_debt is not None
                    else (cost_of_debt_input if cost_of_debt_input and cost_of_debt_input > 0 else None)
                )
            )
            if vals['Cost_of_Debt'] is not None:
                try:
                    cod = float(vals['Cost_of_Debt'])
                    if is_bank:
                        if cod <= 0 or cod > 0.10:
                            vals['Cost_of_Debt'] = None
                    else:
                        if cod <= 0 or cod > 0.5:
                            vals['Cost_of_Debt'] = None
                except Exception:
                    vals['Cost_of_Debt'] = None
            if vals['Cost_of_Debt'] is not None:
                vals['Cost_of_Debt_Source'] = (
                    'RATIO_ENGINE'
                    if ratio_cost_of_debt is not None
                    else (cost_of_debt_source or 'UI_INPUT')
                )
            else:
                vals['Cost_of_Debt_Source'] = None

            # âœ… Point 4: Enhanced WACC calculation using Beta for Cost of Equity
            wacc = get_ratio('wacc')
            beta = None
            try:
                liabilities = get_data('Liabilities') or 0.0
                equity_bs = get_data('StockholdersEquity')
                E = market_cap_year if market_cap_year else (equity_bs if equity_bs is not None else None)
                debt_ratio_value = get_ratio('total_debt')
                D = debt_ratio_value if debt_ratio_value is not None else liabilities
                
                # Get Beta from per-year market layers first, then legacy market_data.
                beta = get_layer_num(y, 'market:beta', 'yahoo:beta')
                if beta is None and hasattr(self, 'current_data') and self.current_data:
                    market_data = self.current_data.get('market_data', {})
                    beta = market_data.get('beta')
                
                effective_cost_of_debt = vals.get('Cost_of_Debt')
                if wacc is None and E is not None and effective_cost_of_debt is not None and (D + (E or 0)) != 0:
                    # Use CAPM if Beta is available: Cost of Equity = Risk-Free Rate + Beta * Market Risk Premium
                    if beta is not None:
                        risk_free_rate = 0.04  # 4% default risk-free rate
                        market_risk_premium = 0.08  # 8% default market risk premium
                        cost_of_equity = risk_free_rate + (beta * market_risk_premium)
                    else:
                        # Fallback: simple approximation
                        cost_of_equity = effective_cost_of_debt + 0.05
                    
                    if (D + E) != 0:
                        wacc = (E / (D + E)) * cost_of_equity + (D / (D + E)) * effective_cost_of_debt * (1 - tax_rate_default)
            except:
                wacc = None
            vals['WACC'] = wacc
            vals['Beta'] = beta  # Store beta for display
            if is_bank and isinstance(wacc, (int, float)) and wacc < 0.06:
                wacc_bank_low = True

            econ_spread = None
            try:
                roic = get_ratio('roic')
                if roic is not None and wacc is not None:
                    roic_dec = canonicalize_ratio_value('roic', roic)
                    econ_spread = roic_dec - wacc
            except:
                econ_spread = None
            vals['Economic_Spread'] = econ_spread

            fair = None
            shares_layer = get_layer_num(y, 'market:shares_outstanding', 'yahoo:shares_outstanding')
            share_inputs = []
            for sv in (shares, shares_basic_year, shares_layer):
                if isinstance(sv, (int, float)) and sv > 0:
                    share_inputs.append(float(sv))
                    if split_factor > 1.0:
                        share_inputs.append(float(sv) * float(split_factor))
            price_anchor = price_year if (isinstance(price_year, (int, float)) and price_year > 0) else None
            price_anchor_band = 3.0
            if price_anchor is None:
                pe_anchor = get_ratio('pe_ratio')
                eps_anchor = eps if isinstance(eps, (int, float)) else get_ratio('eps_basic')
                if isinstance(pe_anchor, (int, float)) and isinstance(eps_anchor, (int, float)) and pe_anchor > 0 and eps_anchor > 0:
                    price_anchor = abs(float(pe_anchor) * float(eps_anchor))
                    price_anchor_band = 3.0
                elif (
                    isinstance(market_cap_year, (int, float))
                    and market_cap_year > 0
                    and isinstance(shares_basic_year, (int, float))
                    and shares_basic_year > 0
                ):
                    price_anchor = abs((float(market_cap_year) * 1_000_000.0) / float(shares_basic_year))
                    price_anchor_band = 3.0
            if vals.get('FCF') is not None and vals.get('WACC') is not None and share_inputs:
                try:
                    # Build an internal, robust growth estimate from historical revenues.
                    rev_hist = []
                    for yy in years:
                        rv = get_data_year(yy, 'Revenues')
                        if rv is None:
                            rv = get_data_year(yy, 'SalesRevenueNet')
                        if isinstance(rv, (int, float)) and rv > 0:
                            rev_hist.append(float(rv))
                    growth_rates = []
                    for i in range(1, len(rev_hist)):
                        prev_r = rev_hist[i - 1]
                        curr_r = rev_hist[i]
                        if prev_r and prev_r > 0:
                            growth_rates.append((curr_r - prev_r) / prev_r)
                    if growth_rates:
                        g = sum(growth_rates) / len(growth_rates)
                        # Clamp to a reasonable strategic valuation band.
                        g = max(-0.10, min(0.15, g))
                    else:
                        g = 0.03
                    pv = 0.0
                    last_fcf = float(vals['FCF'])
                    for t in range(1, 6):
                        ft = last_fcf * ((1 + g) ** t)
                        pv += ft / ((1 + vals['WACC']) ** t)
                    if vals['WACC'] > g:
                        tv = (last_fcf * ((1 + g) ** 6)) / (vals['WACC'] - g)
                        pv += tv / ((1 + vals['WACC']) ** 6)
                    # pv is in million USD; convert to USD then infer the correct share scale.
                    pv_usd = pv * 1_000_000.0
                    fair_candidates = []
                    for sv in share_inputs:
                        for mul, penalty in ((1.0, 0.0), (1_000.0, 0.20), (1_000_000.0, 0.45)):
                            shares_abs = sv * mul
                            if shares_abs <= 0:
                                continue
                            fair_ps = pv_usd / shares_abs
                            if fair_ps <= 0 or fair_ps > 100_000:
                                continue
                            score = penalty
                            if price_anchor and price_anchor > 0:
                                ratio = fair_ps / price_anchor
                                score += abs(ratio - 1.0)
                                if ratio < 0.10 or ratio > price_anchor_band:
                                    score += 5.0
                            else:
                                if fair_ps < 0.1 or fair_ps > 5_000:
                                    score += 5.0
                            fair_candidates.append((score, fair_ps))
                    if fair_candidates:
                        fair = min(fair_candidates, key=lambda x: x[0])[1]
                        if price_anchor and price_anchor > 0 and (
                            fair < 0.10 * price_anchor or fair > price_anchor_band * price_anchor
                        ):
                            fair = None
                            fair_value_warning = 'FAIR_VALUE_OUT_OF_BOUNDS_VS_MARKET'
                        elif (not price_anchor or price_anchor <= 0) and fair > 500:
                            fair = None
                            fair_value_warning = 'FAIR_VALUE_NO_MARKET_ANCHOR'
                except:
                    fair = None
            # Fallback valuation when DCF inputs are incomplete:
            # blend EPS*PE and BVPS*PB anchors if available.
            if fair is None:
                pe_used = first_not_none(get_ratio('pe_ratio_used'), get_ratio('pe_ratio'))
                pb_used = first_not_none(get_ratio('pb_ratio_used'), get_ratio('pb_ratio'))
                eps_used = first_not_none(eps, get_ratio('eps_basic'))
                bvps_used = first_not_none(get_ratio('book_value_per_share'), vals.get('Book_Value_Per_Share'))
                fair_candidates = []
                if isinstance(pe_used, (int, float)) and pe_used > 0 and isinstance(eps_used, (int, float)) and eps_used > 0:
                    fair_candidates.append(float(pe_used) * float(eps_used))
                if isinstance(pb_used, (int, float)) and pb_used > 0 and isinstance(bvps_used, (int, float)) and bvps_used > 0:
                    fair_candidates.append(float(pb_used) * float(bvps_used))
                if fair_candidates:
                    fair = sum(fair_candidates) / len(fair_candidates)
                    fair_value_warning = 'FAIR_VALUE_RATIO_FALLBACK'
            vals['Fair_Value'] = fair

            comps = []
            if vals['Economic_Spread'] is not None:
                comps.append(min(max((vals['Economic_Spread'] + 0.2) / 0.4, 0.0), 1.0))
            if vals['FCF_Yield'] is not None:
                comps.append(min(max((vals['FCF_Yield'] + 0.1) / 0.3, 0.0), 1.0))
            if vals.get('Altman_Z_Score') is not None:
                comps.append(min(max((vals['Altman_Z_Score'] - 1.8) / (3.0 - 1.8), 0.0), 1.0))
            vals['Investment_Score'] = (sum(comps) / len(comps) * 100.0) if comps else None

            cr = None
            # External rating (if available from market/yahoo layer) has top priority.
            ext_rating_raw = first_not_none(
                get_layer_num(y, 'market:credit_rating', 'yahoo:credit_rating'),
                (layer2_by_year.get(y, {}) or {}).get('market:credit_rating'),
                (layer4_by_year.get(y, {}) or {}).get('yahoo:credit_rating'),
            )
            if isinstance(ext_rating_raw, str) and ext_rating_raw.strip():
                rt = ext_rating_raw.strip().upper()
                if rt.startswith('AAA') or rt.startswith('AA'):
                    cr = 'AA'
                elif rt.startswith('A'):
                    cr = 'A'
                elif rt.startswith('BBB') or rt.startswith('BAA'):
                    cr = 'BBB'
                elif rt.startswith('BB') or rt.startswith('BA'):
                    cr = 'BB'
                else:
                    cr = 'B or lower'
                vals['Credit_Rating_Source'] = 'EXTERNAL_LAYER_RATING'
            if vals['Net_Debt_EBITDA'] is not None:
                nd = vals['Net_Debt_EBITDA']
                if cr is None:
                    # Conservative mapping: avoid over-rating non-financial corporates.
                    # AA is reserved to financial profiles with explicit capital proxy support.
                    ic = first_not_none(get_ratio('interest_coverage'), vals.get('Interest_Coverage'))
                    da = first_not_none(get_ratio('debt_to_assets'), vals.get('Debt_to_Assets'))
                    if is_bank:
                        if nd < 1:
                            cr = "AA"
                        elif nd < 2:
                            cr = "A"
                        elif nd < 3:
                            cr = "BBB"
                        elif nd < 4:
                            cr = "BB"
                        else:
                            cr = "B or lower"
                    else:
                        if nd < 0.5 and isinstance(ic, (int, float)) and ic >= 20 and isinstance(da, (int, float)) and da <= 0.25:
                            cr = "A"
                        elif nd < 1.5 and isinstance(ic, (int, float)) and ic >= 6:
                            cr = "BBB"
                        elif nd < 3:
                            cr = "BB"
                        else:
                            cr = "B or lower"
            if cr is None:
                da = first_not_none(get_ratio('debt_to_assets'), vals.get('Debt_to_Assets'))
                dte = first_not_none(get_ratio('debt_to_equity'), vals.get('Debt_to_Equity'))
                cap_proxy = first_not_none(vals.get('Capital_Adequacy_Proxy'), vals.get('Capital_Ratio_Proxy'))
                try:
                    if isinstance(cap_proxy, (int, float)):
                        if cap_proxy >= 0.12:
                            cr = "AA" if is_bank else "A"
                        elif cap_proxy >= 0.08:
                            cr = "A"
                        elif cap_proxy >= 0.05:
                            cr = "BBB"
                        else:
                            cr = "BB"
                    elif isinstance(da, (int, float)):
                        if da <= 0.30:
                            cr = "AA" if is_bank else "A"
                        elif da <= 0.50:
                            cr = "BBB" if not is_bank else "A"
                        elif da <= 0.70:
                            cr = "BB" if not is_bank else "BBB"
                        else:
                            cr = "B or lower"
                    elif isinstance(dte, (int, float)):
                        if dte <= 1.0:
                            cr = "AA" if is_bank else "A"
                        elif dte <= 2.0:
                            cr = "BBB" if not is_bank else "A"
                        elif dte <= 3.5:
                            cr = "BB" if not is_bank else "BBB"
                        else:
                            cr = "B or lower"
                except Exception:
                    cr = cr
            if ni is not None and ni < 0 and cr in {"AA", "A"}:
                cr = "BBB"
                rating_adjusted_for_losses = True
            if cr is None and prev_y:
                assets_now = first_not_none(get_data('Assets'), get_data('TotalAssets'), get_data('Total Assets'))
                liab_now = first_not_none(get_data('Liabilities'), get_data('TotalLiabilities'), get_data('Total Liabilities'))
                eq_now = first_not_none(
                    get_data('StockholdersEquity'),
                    get_data('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'),
                    get_data('TotalEquity'),
                    get_data('Total Equity'),
                )
                if assets_now is None or liab_now is None or eq_now is None:
                    prev_vals = per_year.get(prev_y, {}) or {}
                    prev_cr = prev_vals.get('Credit_Rating')
                    if isinstance(prev_cr, str) and prev_cr.strip():
                        cr = prev_cr
                        vals['Credit_Rating_Source'] = 'PREV_YEAR_PROXY_MISSING_ANCHORS'
            vals['Credit_Rating'] = cr
            rating_map = {'AA': 95, 'A': 85, 'BBB': 75, 'BB': 60, 'B or lower': 40}
            vals['Credit_Rating_Score'] = rating_map.get(cr) if cr else None

            warns = []
            if vals['Altman_Z_Score'] is not None and vals['Altman_Z_Score'] < 1.8:
                warns.append("Altman low")
            if vals['Net_Debt_EBITDA'] is not None and vals['Net_Debt_EBITDA'] > 3:
                warns.append("High NetDebt/EBITDA")
            if vals.get('Op_Leverage') is not None and abs(vals['Op_Leverage']) > 3:
                warns.append("High Op Leverage")
            if vals['Accruals_Ratio'] is not None and vals['Accruals_Ratio'] > 0.05:
                warns.append("High accruals")
            if wacc_bank_low:
                warns.append("Low WACC for bank")
            if fair_value_warning:
                warns.append("Fair value flagged")
            if rating_adjusted_for_losses:
                warns.append("Rating capped due to losses")
            vals['Warning_Signal'] = ", ".join(warns) if warns else "None"

            per_year[y] = vals

        return per_year

    # ---------- Strategic display ----------
    def display_strategic_analysis(self):
        if not self.current_data:
            for i in self.strat_tree.get_children():
                self.strat_tree.delete(i)
            return
        for iid in self.strat_tree.get_children():
            self.strat_tree.delete(iid)
        data_by_year = ((self.current_data.get('data_layers', {}) or {}).get('layer1_by_year') or self.current_data.get('data_by_year', {}) or {})
        ratios_by_year = self.current_data.get('financial_ratios', {}) or {}
        years = self._get_analysis_years_range()
        if not years:
            return
        per_year = self._compute_per_year_metrics(data_by_year, ratios_by_year)
        sector_gating = (self.current_data.get('sector_gating', {}) if self.current_data else {}) or {}
        sector_profile = (sector_gating.get('profile') or 'industrial')
        blocked_strategic_metrics = set(sector_gating.get('blocked_strategic_metrics', []) or [])

        metric_col = self._t('strategic_col_metric')
        cols = [metric_col] + [str(y) for y in years]
        self.strat_tree.config(columns=cols)
        for c in cols:
            self.strat_tree.heading(c, text=self._translate_financial_item(c))
            if c == metric_col:
                self.strat_tree.column(c, width=320, anchor='w')
            else:
                self.strat_tree.column(c, width=160, anchor='center')
        self.strat_tree.tag_configure('header', background='#e8f4f8', font=FONTS['label'])
        self.strat_tree.tag_configure('child_row', font=FONTS['tree'])
        self.strat_tree.tag_configure('zebra_even', background='#ffffff')
        self.strat_tree.tag_configure('zebra_odd', background='#f7fbff')

        def fmt_num_local(v):
            if v is None:
                return "N/A"
            try:
                if isinstance(v, (int, float)):
                    if abs(v) >= 1_000_000_000:
                        return f"{v/1_000_000_000:,.2f}B"
                    if abs(v) >= 1_000_000:
                        return f"{v/1_000_000:,.2f}M"
                    if abs(v - round(v)) < 1e-6:
                        return f"{int(round(v)):,}"
                    return f"{v:,.2f}"
                return str(v)
            except:
                return str(v)

        def format_contract(v):
            if not isinstance(v, dict):
                return None
            rel = int(v.get('reliability') or 0)
            reason = v.get('reason') or 'no_reason'
            if v.get('value') is None or rel == 0 or rel < 55:
                base = f"N/A ({reason})"
            else:
                base = v.get('display')
                if not base:
                    fmt = v.get('format')
                    raw_value = v.get('value')
                    if fmt == 'percent':
                        base = f"{float(raw_value) * 100:.2f}%"
                    else:
                        base = f"{float(raw_value):.2f}"
            if self._debug_ui_contracts_enabled():
                base = f"{base} [src={v.get('source')} rel={rel} reason={reason}]"
            return base

        def insert_metric(display, key, fmt='num'):
            if key in blocked_strategic_metrics:
                return
            row = [self._translate_financial_item(display)]
            for y in years:
                v = per_year.get(y, {}).get(key)
                c = format_contract(v)
                if c is not None:
                    row.append(c)
                elif fmt == 'pct':
                    if v is None:
                        row.append("N/A")
                    else:
                        try:
                            row.append(f"{float(v) * 100:.2f}%")
                        except Exception:
                            row.append(str(v))
                else:
                    row.append(fmt_num_local(v))
            row_idx = len(self.strat_tree.get_children(''))
            zebra_tag = 'zebra_even' if (row_idx % 2 == 0) else 'zebra_odd'
            self.strat_tree.insert('', 'end', values=row, tags=('child_row', zebra_tag))

        def insert_metric_group(title, metrics):
            visible_metrics = [m for m in metrics if m[1] not in blocked_strategic_metrics]
            if not visible_metrics:
                return
            group_title = self._translate_financial_item(title)
            self.strat_tree.insert('', 'end', values=(group_title,) + tuple([""] * len(years)), tags=('header',))
            for display, key, fmt in visible_metrics:
                insert_metric(display, key, fmt=fmt)

        # Groups
        if sector_profile == 'bank':
            insert_metric_group("--- Banking Strategic Tier ---", [
                ("Bank Total Revenue", 'Bank_Total_Revenue', 'num'),
                ("Net Interest Margin (NIM)", 'Net_Interest_Margin', 'pct'),
                ("Efficiency Ratio", 'Bank_Efficiency_Ratio', 'pct'),
                ("Loan-to-Deposit Ratio", 'Loan_to_Deposit_Ratio', 'num'),
                ("Capital Ratio Proxy", 'Capital_Ratio_Proxy', 'pct'),
                ("Net Income / Assets", 'Net_Income_to_Assets', 'pct'),
                ("Equity Ratio", 'Equity_Ratio', 'pct'),
                ("ROA", 'ROA', 'pct'),
                ("ROE", 'ROE', 'pct'),
                ("Net Margin", 'Net_Margin', 'pct'),
                ("Cost_of_Debt", 'Cost_of_Debt', 'pct'),
                ("Credit_Rating", 'Credit_Rating', 'num'),
                ("Credit_Rating_Score", 'Credit_Rating_Score', 'num'),
                ("Warning_Signal", 'Warning_Signal', 'num'),
                ("Beta", 'Beta', 'num'),
                ("WACC", 'WACC', 'pct'),
            ])
            insert_metric_group("--- Banking Market Tier ---", [
                ("P/E Ratio", 'PE_Ratio', 'num'),
                ("P/E Ratio (Used)", 'PE_Ratio_Used', 'num'),
                ("P/B Ratio", 'PB_Ratio', 'num'),
                ("P/B Ratio (Used)", 'PB_Ratio_Used', 'num'),
                ("Dividend Yield", 'Dividend_Yield', 'pct'),
            ])
        elif sector_profile == 'insurance':
            insert_metric_group("--- Insurance Strategic Tier ---", [
                ("Combined Ratio Proxy", 'Combined_Ratio_Proxy', 'num'),
                ("Capital Adequacy Proxy", 'Capital_Adequacy_Proxy', 'num'),
                ("Net Income / Assets", 'Net_Income_to_Assets', 'pct'),
                ("Equity Ratio", 'Equity_Ratio', 'pct'),
                ("Cost_of_Debt", 'Cost_of_Debt', 'pct'),
                ("WACC", 'WACC', 'pct'),
            ])
            insert_metric_group("--- Insurance Market Tier ---", [
                ("P/E Ratio", 'PE_Ratio', 'num'),
                ("P/E Ratio (Used)", 'PE_Ratio_Used', 'num'),
                ("P/B Ratio", 'PB_Ratio', 'num'),
                ("P/B Ratio (Used)", 'PB_Ratio_Used', 'num'),
                ("Dividend Yield", 'Dividend_Yield', 'pct'),
            ])
        else:
            insert_metric_group("--- Strategic & Value Tier ---", [
                ("Fair_Value_Estimate (per share)", 'Fair_Value', 'num'),
                ("Investment_Score (0-100)", 'Investment_Score', 'num'),
                ("Economic_Spread (ROIC - WACC)", 'Economic_Spread', 'pct'),
                ("ROIC", 'ROIC', 'pct'),
                ("WACC", 'WACC', 'pct'),
                ("Beta (Market Risk)", 'Beta', 'num'),
                ("SGR_Internal (Sustainable Growth)", 'SGR_Internal', 'pct'),
            ])

            insert_metric_group("--- Quality & Risk Tier ---", [
                ("Altman_Z_Score", 'Altman_Z_Score', 'num'),
                ("Warning_Signal", 'Warning_Signal', 'num'),
                ("Accruals_Ratio", 'Accruals_Ratio', 'num'),
                ("Accruals_Change", 'Accruals_Change', 'num'),
                ("Credit_Rating", 'Credit_Rating', 'num'),
                ("Credit_Rating_Score", 'Credit_Rating_Score', 'num'),
                ("Net_Debt_EBITDA", 'Net_Debt_EBITDA', 'num'),
                ("Op_Leverage", 'Op_Leverage', 'num'),
            ])

            insert_metric_group("--- Performance Analysis Tier ---", [
                ("ROE", 'ROE', 'pct'),
                ("NI_Growth (1y)", 'NI_Growth', 'pct'),
                ("Retention_Ratio", 'Retention_Ratio', 'pct'),
                ("Dividends_Paid", 'Dividends_Paid', 'num'),
                ("EBITDA", 'EBITDA', 'num'),
                ("FCF_Yield", 'FCF_Yield', 'pct'),
                ("EPS", 'EPS', 'num'),
                ("FCF_per_Share", 'FCF_per_Share', 'num'),
            ])

            insert_metric_group("--- Operational Efficiency Tier ---", [
                ("CCC_Days", 'CCC_Days', 'num'),
                ("Inventory Days (DIH)", 'Inventory_Days', 'num'),
                ("AR Days (DSO)", 'AR_Days', 'num'),
                ("AP Days (DPO)", 'AP_Days', 'num'),
                ("Cost_of_Debt (input)", 'Cost_of_Debt', 'pct'),
            ])

            insert_metric_group("--- Market Valuation Tier ---", [
                ("P/E Ratio", 'PE_Ratio', 'num'),
                ("P/E Ratio (Used)", 'PE_Ratio_Used', 'num'),
                ("P/B Ratio", 'PB_Ratio', 'num'),
                ("P/B Ratio (Used)", 'PB_Ratio_Used', 'num'),
                ("EV/EBITDA", 'EV_EBITDA', 'num'),
                ("Dividend Yield", 'Dividend_Yield', 'pct'),
            ])

    def _apply_pre_export_quality_gate(self, years, data_by_year, ratios_by_year, data_layers):
        """
        Pre-export unit/consistency guardrail.
        Heals known scale issues for EPS/PE/PB/FCF_Yield before writing Excel.
        """
        issues = []
        layer2_by_year = (data_layers or {}).get('layer2_by_year', {}) or {}
        layer4_by_year = (data_layers or {}).get('layer4_by_year', {}) or {}
        # Conservative default: keep aggressive rewrites disabled unless explicitly requested.
        allow_aggressive = str(os.environ.get('QUALITY_GATE_AGGRESSIVE', '0')).strip().lower() in ('1', 'true', 'yes')
        sector_gating = (self.current_data.get('sector_gating', {}) if self.current_data else {}) or {}
        blocked_ratios = set(sector_gating.get('blocked_ratios', []) or [])
        ticker = str((self.current_data or {}).get('company_info', {}).get('ticker', '')).upper()
        # Committee-driven fallback where split metadata is unavailable in layers.
        manual_split_rules = {
            'NVDA': {'cutoff_year': 2023, 'ratio': 10.0},
        }

        def _num(v):
            try:
                if v is None:
                    return None
                return float(v)
            except Exception:
                return None

        def _nk(x):
            return re.sub(r'[^a-z0-9]+', '', str(x or '').lower())

        def _pick_num_ci(row_dict, aliases):
            if not isinstance(row_dict, dict):
                return None
            # Exact aliases first.
            for a in aliases:
                if a in row_dict:
                    fv = _num(row_dict.get(a))
                    if fv is not None:
                        return fv
            # Case/format-insensitive fallback.
            nmap = {}
            for k, v in row_dict.items():
                kk = _nk(k)
                if kk and kk not in nmap:
                    nmap[kk] = v
            for a in aliases:
                fv = _num(nmap.get(_nk(a)))
                if fv is not None:
                    return fv
            return None

        def _align_to_reference(value, reference, *, lo=0.01, hi=2.5, target=0.35):
            val = _num(value)
            ref = _num(reference)
            if val is None or ref in (None, 0):
                return val
            cands = [
                val,
                val / 1_000.0,
                val / 1_000_000.0,
                val / 1_000_000_000.0,
                val * 1_000.0,
                val * 1_000_000.0,
            ]
            scored = []
            for c in cands:
                if c is None:
                    continue
                ratio = abs(c / ref) if ref else None
                if ratio is None:
                    continue
                if lo <= ratio <= hi:
                    scored.append((abs(ratio - target), c))
            if scored:
                scored.sort(key=lambda x: x[0])
                return scored[0][1]
            # fallback: nearest order-of-magnitude to reference
            try:
                import math
                return min(cands, key=lambda c: abs(math.log10(max(abs(c), 1e-9)) - math.log10(max(abs(ref), 1e-9))))
            except Exception:
                return val

        def _as_million(v):
            fv = _num(v)
            if fv is None:
                return None
            if abs(fv) > 1_000_000_000:
                return fv / 1_000_000.0
            return fv

        def _split_ratio_for_year(year):
            # Strict cutoff-first policy for known split issuers:
            # apply split factor only on years BEFORE cutoff; never after.
            rule = manual_split_rules.get(ticker)
            if rule:
                try:
                    cutoff = int(rule.get('cutoff_year', 0))
                    ratio = float(rule.get('ratio') or 1.0)
                    return ratio if year < cutoff and ratio > 1.0 else 1.0
                except Exception:
                    return 1.0
            row2 = layer2_by_year.get(year, {}) or {}
            row4 = layer4_by_year.get(year, {}) or {}
            for key in ('market:split_latest_ratio', 'yahoo:split_latest_ratio'):
                sv = _num(row2.get(key))
                if sv is None:
                    sv = _num(row4.get(key))
                if sv is not None and sv > 1.0:
                    return sv
            return 1.0

        def _normalize_shares_to_millions(sh):
            sv = _num(sh)
            if sv is None:
                return None
            av = abs(sv)
            # SEC often mixes absolute shares and "in millions" shares across years.
            if av >= 1_000_000:
                return sv / 1_000_000.0
            return sv

        def _year_shares_million(year):
            """
            Return shares (in millions) with per-year unit harmonization.
            This handles mixed SEC yearly units (absolute shares vs millions).
            """
            row = (data_by_year or {}).get(year, {}) or {}
            raw_candidates = [
                _pick_num_ci(row, ['WeightedAverageNumberOfSharesOutstandingBasic', 'weightedaveragenumberofsharesoutstandingbasic']),
                _pick_num_ci(row, ['SharesBasic', 'sharesbasic']),
                _pick_num_ci(row, ['CommonStockSharesOutstanding', 'commonstocksharesoutstanding']),
                (ratios_by_year or {}).get(year, {}).get('shares_outstanding'),
            ]
            values = []
            for v in raw_candidates:
                fv = _num(v)
                if fv is not None and fv > 0:
                    values.append(float(fv))
            if not values:
                return None

            # Convert each candidate into "million shares" candidates.
            million_cands = []
            for v in values:
                million_cands.extend([v, v / 1_000.0, v / 1_000_000.0, v * 1_000.0])
            million_cands = [c for c in million_cands if c is not None and c > 0]
            if not million_cands:
                return None

            # Prefer plausible public-company ranges in millions.
            plausible = [c for c in million_cands if 10.0 <= c <= 100_000.0]
            if plausible:
                # Use layer anchors when available:
                # 1) market_cap/price implied shares, 2) layer shares_outstanding.
                row2 = layer2_by_year.get(year, {}) or {}
                px = _num(row2.get('market:price') or row2.get('yahoo:price'))
                mcap_m = _as_million(row2.get('market:market_cap') or row2.get('yahoo:market_cap'))
                layer_sh = _num(row2.get('market:shares_outstanding') or row2.get('yahoo:shares_outstanding'))
                layer_sh_m = _normalize_shares_to_millions(layer_sh) if layer_sh is not None else None
                target_mcap = None
                if px not in (None, 0) and mcap_m not in (None, 0):
                    target_mcap = abs(mcap_m / px)
                    if target_mcap <= 0:
                        target_mcap = None
                # If the two anchors disagree by orders of magnitude, trust shares_outstanding anchor.
                if target_mcap not in (None, 0) and layer_sh_m not in (None, 0):
                    mx = max(abs(float(target_mcap)), abs(float(layer_sh_m)))
                    mn = max(min(abs(float(target_mcap)), abs(float(layer_sh_m))), 1e-9)
                    if (mx / mn) > 20.0:
                        target_mcap = None

                def _score(c):
                    score = 0.0
                    used = 0
                    if target_mcap not in (None, 0):
                        score += abs(abs(c) - target_mcap) / max(target_mcap, 1.0)
                        used += 1
                    if layer_sh_m not in (None, 0):
                        score += abs(abs(c) - abs(layer_sh_m)) / max(abs(layer_sh_m), 1.0)
                        used += 1
                    if used == 0:
                        # Central tendency fallback when no anchors exist.
                        med = sorted(plausible)[len(plausible) // 2]
                        return abs(float(c) - float(med)) / max(abs(float(med)), 1.0)
                    return score / float(used)

                return min(plausible, key=_score)

            # Fallback to previous heuristic.
            return _normalize_shares_to_millions(values[0])

        # Split-aware market-cap sanity (always-on for known split issuers).
        # Prevent historical 10x compression when adjusted price is paired with pre-split shares.
        split_rule = manual_split_rules.get(ticker)
        if split_rule:
            repaired = 0
            for y in years:
                row2 = layer2_by_year.setdefault(y, {})
                px = _num(row2.get('market:price') or row2.get('yahoo:price'))
                sh_m = _year_shares_million(y)
                if px in (None, 0) or sh_m in (None, 0):
                    continue
                sf = _split_ratio_for_year(y)
                sh_adj_m = sh_m * sf if sf and sf > 1.0 else sh_m
                recon_mcap_m = (px * sh_adj_m) / 1_000_000.0
                mcap_old_m = _as_million(row2.get('market:market_cap') or row2.get('yahoo:market_cap'))
                if mcap_old_m in (None, 0):
                    row2['market:market_cap'] = recon_mcap_m
                    repaired += 1
                    continue
                ratio_mc = abs(mcap_old_m) / max(abs(recon_mcap_m), 1e-9)
                if ratio_mc < 0.2 or ratio_mc > 5.0:
                    row2['market:market_cap'] = recon_mcap_m
                    repaired += 1
            if repaired:
                issues.append(f"Layer2: split-aware market_cap repaired for {repaired} years ({ticker}).")

        # ----- Layer2 time-series normalization -----
        def _series_values(field):
            vals = []
            for y in years:
                vv = _num((layer2_by_year.get(y, {}) or {}).get(field))
                if any(tok in str(field).lower() for tok in ('market_cap', 'enterprise_value', 'total_debt')):
                    vv = _as_million(vv)
                vals.append(vv)
            return vals

        def _is_constant(vals):
            clean = [v for v in vals if v is not None]
            return len(clean) >= 2 and len(set(clean)) == 1

        # 1) Shares outstanding: if constant across years, derive from market_cap / price.
        sh_vals = _series_values('market:shares_outstanding')
        if allow_aggressive and _is_constant(sh_vals):
            changed = 0
            for y in years:
                row2 = layer2_by_year.setdefault(y, {})
                mcap = _as_million(row2.get('market:market_cap'))
                price = _num(row2.get('market:price'))
                if mcap is not None and price not in (None, 0):
                    derived = (mcap * 1_000_000.0) / price
                    if derived > 0 and _num(row2.get('market:shares_outstanding')) != derived:
                        row2['market:shares_outstanding'] = derived
                        changed += 1
            if changed:
                issues.append(f"Layer2: market:shares_outstanding derived as market_cap/price for {changed} years.")

        # 2) Total debt: if constant across years, rebuild from SEC debt facts first.
        debt_vals = _series_values('market:total_debt')
        if allow_aggressive and _is_constant(debt_vals):
            debt_component_keys = (
                'DebtCurrent',
                'ShortTermBorrowings',
                'CommercialPaper',
                'LongTermDebtCurrent',
                'CurrentPortionOfLongTermDebt',
                'LongTermDebtNoncurrent',
                'DebtNoncurrent',
                'LongTermDebt',
                'LongTermDebtAndCapitalLeaseObligation',
                'LongTermDebtAndCapitalLeaseObligations',
            )

            def _sec_total_debt_million(year):
                rr = (ratios_by_year or {}).get(year, {}) or {}
                sec_td = _as_million(rr.get('total_debt'))
                if sec_td not in (None, 0):
                    return sec_td
                raw_row = (data_by_year or {}).get(year, {}) or {}
                comps = [_pick_num_ci(raw_row, [k]) for k in debt_component_keys]
                comps = [c for c in comps if c is not None and c != 0]
                if not comps:
                    return None
                direct_td = None
                for dk in ('LongTermDebt', 'LongTermDebtAndCapitalLeaseObligation', 'LongTermDebtAndCapitalLeaseObligations'):
                    dv = _pick_num_ci(raw_row, [dk])
                    if dv not in (None, 0):
                        direct_td = dv
                        break
                sum_td = float(sum(comps))
                chosen = sum_td
                if direct_td not in (None, 0):
                    if abs(sum_td - direct_td) <= max(1.0, abs(direct_td) * 0.20):
                        chosen = max(sum_td, direct_td)
                    else:
                        chosen = direct_td
                return _as_million(chosen)

            sec_series = {y: _sec_total_debt_million(y) for y in years}
            sec_valid = {y: v for y, v in sec_series.items() if v not in (None, 0)}
            if len(sec_valid) >= 2 and len({round(v, 6) for v in sec_valid.values()}) > 1:
                for y, v in sec_valid.items():
                    layer2_by_year.setdefault(y, {})['market:total_debt'] = v
                issues.append(f"Layer2: market:total_debt synchronized from SEC debt facts for {len(sec_valid)} years.")
            elif sec_valid:
                # Snapshot fallback: keep only the latest known debt instead of a fake flat series.
                latest_y = max(sec_valid.keys())
                for y in years:
                    row2 = layer2_by_year.setdefault(y, {})
                    row2['market:total_debt'] = sec_valid[latest_y] if y == latest_y else None
                issues.append("Layer2: market:total_debt kept as snapshot (insufficient annual SEC debt coverage).")

        # 3) Enterprise value: if constant across years, derive as market_cap + total_debt - cash.
        ev_vals = _series_values('market:enterprise_value')
        if allow_aggressive and _is_constant(ev_vals):
            changed = 0
            for y in years:
                row2 = layer2_by_year.setdefault(y, {})
                raw_row = (data_by_year or {}).get(y, {}) or {}
                mcap = _as_million(row2.get('market:market_cap'))
                debt = _as_million(row2.get('market:total_debt'))
                cash = _num(
                    raw_row.get('CashAndCashEquivalentsAtCarryingValue')
                    or raw_row.get('CashAndCashEquivalents')
                    or raw_row.get('Cash and Cash Equivalents')
                )
                if None in (mcap, debt, cash):
                    continue
                cash_m = _as_million(cash)
                derived_ev = mcap + debt - cash_m
                if derived_ev > 0 and _num(row2.get('market:enterprise_value')) != derived_ev:
                    row2['market:enterprise_value'] = derived_ev
                    changed += 1
            if changed:
                issues.append(f"Layer2: market:enterprise_value derived as market_cap + total_debt - cash for {changed} years.")

        # 4) Debt prefix anomaly guard:
        # If earliest years are a flat repeated debt value and then drop sharply,
        # rebuild early debt from liabilities using the first non-flat anchor year.
        debt_series = []
        for y in years:
            rr = (ratios_by_year or {}).get(y, {}) or {}
            debt_series.append((y, _as_million(rr.get('total_debt'))))
        prefix_years = []
        prefix_val = None
        for y, dv in debt_series:
            if dv is None:
                if prefix_years:
                    break
                continue
            if prefix_val is None:
                prefix_val = dv
                prefix_years.append(y)
                continue
            if abs(dv - prefix_val) <= 1e-9:
                prefix_years.append(y)
                continue
            break
        if allow_aggressive and len(prefix_years) >= 2 and prefix_val not in (None, 0):
            anchor_year = None
            anchor_debt = None
            for y, dv in debt_series:
                if y in prefix_years:
                    continue
                if dv is not None and dv > 0:
                    anchor_year = y
                    anchor_debt = dv
                    break
            if anchor_year is not None and anchor_debt is not None and prefix_val > (1.8 * anchor_debt):
                anchor_row = (data_by_year or {}).get(anchor_year, {}) or {}
                liab_anchor = _num(
                    anchor_row.get('Liabilities')
                    or anchor_row.get('TotalLiabilities')
                    or anchor_row.get('Total Liabilities')
                )
                liab_anchor_m = _as_million(liab_anchor)
                ratio_anchor = (anchor_debt / liab_anchor_m) if liab_anchor_m not in (None, 0) else None
                if ratio_anchor is not None and 0.02 <= ratio_anchor <= 1.5:
                    changed = 0
                    for py in prefix_years:
                        prow = (data_by_year or {}).get(py, {}) or {}
                        liab_py = _num(
                            prow.get('Liabilities')
                            or prow.get('TotalLiabilities')
                            or prow.get('Total Liabilities')
                        )
                        liab_py_m = _as_million(liab_py)
                        if liab_py_m in (None, 0):
                            continue
                        new_debt = liab_py_m * ratio_anchor
                        rr = (ratios_by_year or {}).setdefault(py, {})
                        cur_debt = _as_million(rr.get('total_debt'))
                        if cur_debt is None or abs(cur_debt - new_debt) > 1e-9:
                            rr['total_debt'] = new_debt
                            rr['total_debt_source'] = f'PREFIX_FLAT_DEBT_REPAIRED_FROM_{anchor_year}'
                            changed += 1
                    if changed:
                        issues.append(
                            f"Debt guard: repaired flat early-year total_debt series for {changed} years using {anchor_year} anchor."
                        )

        # 5) Bank debt unit harmonization (prevents mixed-unit jumps across years).
        sector_profile_qg = ((self.current_data or {}).get('sector_gating', {}) or {}).get('profile', 'industrial')
        if allow_aggressive and str(sector_profile_qg).lower() == 'bank':
            debt_liab_ratios = []
            for y in years:
                rr = (ratios_by_year or {}).get(y, {}) or {}
                row = (data_by_year or {}).get(y, {}) or {}
                td = _as_million(rr.get('total_debt'))
                li = _as_million(_pick_num_ci(row, ['Liabilities', 'TotalLiabilities', 'Total Liabilities', 'liabilities']))
                if td in (None, 0) or li in (None, 0):
                    continue
                ratio = abs(float(td)) / max(abs(float(li)), 1e-9)
                if 0.02 <= ratio <= 0.80:
                    debt_liab_ratios.append(ratio)
            if debt_liab_ratios:
                debt_liab_ratios = sorted(debt_liab_ratios)
                anchor_ratio = debt_liab_ratios[len(debt_liab_ratios) // 2]
                changed = 0
                for y in years:
                    rr = (ratios_by_year or {}).setdefault(y, {})
                    row = (data_by_year or {}).get(y, {}) or {}
                    td = _as_million(rr.get('total_debt'))
                    li = _as_million(_pick_num_ci(row, ['Liabilities', 'TotalLiabilities', 'Total Liabilities', 'liabilities']))
                    if td in (None, 0) or li in (None, 0):
                        continue
                    ratio = abs(float(td)) / max(abs(float(li)), 1e-9)
                    if ratio < 0.01 or ratio > 1.20:
                        rebuilt = abs(float(li)) * anchor_ratio
                        if rebuilt > 0:
                            rr['total_debt'] = rebuilt
                            rr['total_debt_source'] = 'QUALITY_GATE_BANK_UNIT_HARMONIZED'
                            changed += 1
                if changed:
                    issues.append(f"Bank debt unit harmonization applied for {changed} years (anchor={anchor_ratio:.4f}).")

        for idx, y in enumerate(years):
            row = (data_by_year or {}).get(y, {}) or {}
            r = (ratios_by_year or {}).setdefault(y, {})
            m = layer2_by_year.get(y, {}) or {}
            prev_y = years[idx - 1] if idx > 0 else None

            # Market price / market-cap hard sanity:
            # fix obvious scale slips (e.g., 0.003 instead of 177).
            price_raw = _num(m.get('market:price'))
            pe_hint = _num(r.get('pe_ratio')) or _num(m.get('market:pe_ratio')) or _num(m.get('yahoo:pe_ratio'))
            eps_hint = _num(r.get('eps_basic')) or _pick_num_ci(row, ['EarningsPerShareBasic', 'Basic (in dollars per share)', 'EPS Basic'])
            implied_price = None
            if pe_hint not in (None, 0) and eps_hint not in (None, 0):
                implied_price = abs(float(pe_hint) * float(eps_hint))
                if implied_price <= 0 or implied_price > 20_000:
                    implied_price = None
            if price_raw is not None and implied_price is not None:
                ratio = max(abs(price_raw), abs(implied_price)) / max(min(abs(price_raw), abs(implied_price)), 1e-9)
                if price_raw < 0.5 or ratio > 20.0:
                    m['market:price'] = implied_price
                    issues.append(f"{y}: market:price hard-fixed ({price_raw} -> {implied_price}) via PE*EPS")
                    price_raw = implied_price

            # Rebuild market cap from annual price*shares when layer value is implausible.
            sh_m = _year_shares_million(y)
            mcap_raw = _as_million(m.get('market:market_cap'))
            if price_raw not in (None, 0) and sh_m not in (None, 0):
                sh_candidates = [float(sh_m)]
                # Add direct layer shares anchors to avoid 1/1000 drift in certain split histories.
                sh_layer_raw = _num(m.get('market:shares_outstanding') or m.get('yahoo:shares_outstanding'))
                if sh_layer_raw not in (None, 0):
                    sh_layer_m_candidates = [
                        float(sh_layer_raw),
                        float(sh_layer_raw) / 1_000.0,
                        float(sh_layer_raw) / 1_000_000.0,
                    ]
                    sh_layer_m_candidates = [sv for sv in sh_layer_m_candidates if 10.0 <= abs(sv) <= 100_000.0]
                    sh_candidates.extend(sh_layer_m_candidates)
                # de-duplicate numeric candidates
                uniq = []
                seen = set()
                for sv in sh_candidates:
                    try:
                        k = round(float(sv), 8)
                    except Exception:
                        continue
                    if k in seen:
                        continue
                    seen.add(k)
                    uniq.append(float(sv))
                sh_candidates = uniq
                mcap_candidates = []
                for shv in sh_candidates:
                    if shv <= 0:
                        continue
                    mc = abs(float(price_raw) * shv)
                    if 1.0 <= mc <= 20_000_000.0:
                        mcap_candidates.append(mc)
                if mcap_candidates:
                    preferred_target = None
                    if sh_layer_raw not in (None, 0):
                        sh_pref = None
                        for sv in (
                            float(sh_layer_raw),
                            float(sh_layer_raw) / 1_000.0,
                            float(sh_layer_raw) / 1_000_000.0,
                        ):
                            if 10.0 <= abs(sv) <= 100_000.0:
                                sh_pref = float(sv)
                                break
                        if sh_pref is not None:
                            preferred_target = abs(float(price_raw) * sh_pref)
                    mcap_derived = None
                    if mcap_raw not in (None, 0):
                        mcap_near = min(mcap_candidates, key=lambda mc: abs(float(mc) - float(mcap_raw)))
                        mcap_max = max(mcap_candidates)
                        diff_near = abs(float(mcap_raw) - float(mcap_near)) / max(abs(float(mcap_near)), 1e-9)
                        diff_max = abs(float(mcap_raw) - float(mcap_max)) / max(abs(float(mcap_max)), 1e-9)
                        # Final lock:
                        # prefer layer-shares target when available; fallback to nearest.
                        if preferred_target not in (None, 0):
                            mcap_derived = min(mcap_candidates, key=lambda mc: abs(float(mc) - float(preferred_target)))
                        elif sh_layer_raw not in (None, 0) and diff_max > 0.80:
                            mcap_derived = mcap_max
                        elif diff_near > 0.80 and diff_max > 0.80:
                            mcap_derived = mcap_max
                        else:
                            mcap_derived = mcap_near
                    else:
                        mcap_derived = max(mcap_candidates)
                    if mcap_raw is None:
                        m['market:market_cap'] = mcap_derived
                        issues.append(f"{y}: market:market_cap derived from price*shares ({mcap_derived})")
                    else:
                        diff_ratio = abs(mcap_raw - mcap_derived) / max(abs(mcap_derived), 1e-9)
                        if diff_ratio > 0.80:
                            m['market:market_cap'] = mcap_derived
                            issues.append(f"{y}: market:market_cap corrected ({mcap_raw} -> {mcap_derived})")

            assets_chk = _pick_num_ci(row, ['Assets', 'TotalAssets', 'Total Assets', 'assets'])
            liab_chk = _pick_num_ci(row, ['Liabilities', 'TotalLiabilities', 'Total Liabilities', 'liabilities'])
            eq_candidates = [
                _pick_num_ci(row, ['StockholdersEquity', 'stockholdersequity']),
                _pick_num_ci(row, [
                    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                    'stockholdersequityincludingportionattributabletononcontrollinginterest',
                ]),
                _pick_num_ci(row, ['TotalEquity', 'Total Equity', 'totalequity']),
            ]
            eq_candidates = [v for v in eq_candidates if v is not None]
            eq_chk = None
            if eq_candidates:
                if assets_chk is not None and liab_chk is not None:
                    eq_chk = min(eq_candidates, key=lambda ev: abs((assets_chk - liab_chk) - ev))
                else:
                    eq_chk = eq_candidates[0]

            # Prefer control-total only when internally consistent with the current year.
            control_total_chk = _pick_num_ci(row, [
                'LiabilitiesAndStockholdersEquity',
                'Total liabilities and equity',
                "Total liabilities and stockholders' equity",
                'liabilitiesandstockholdersequity',
            ])
            if assets_chk is not None and control_total_chk is not None and liab_chk is not None and eq_chk is not None:
                ctl_delta = abs(assets_chk - control_total_chk) / max(abs(assets_chk), 1.0)
                le_delta = abs(assets_chk - (liab_chk + eq_chk)) / max(abs(assets_chk), 1.0)
                # If control total is clearly misaligned while L+E is coherent, keep L+E anchors.
                if ctl_delta > 0.01 and le_delta <= 0.001:
                    control_total_chk = None
            if eq_chk is None and assets_chk is not None and liab_chk is not None and control_total_chk is not None:
                # Derive equity from consistent same-year control total.
                ctl_delta = abs(assets_chk - control_total_chk) / max(abs(assets_chk), 1.0)
                if ctl_delta <= 0.01:
                    eq_chk = assets_chk - liab_chk

            # Conservative proxy fill from previous year only for missing anchors.
            proxy_fill = False
            if allow_aggressive and prev_y is not None and (assets_chk is None or liab_chk is None or eq_chk is None):
                prev_row = (data_by_year or {}).get(prev_y, {}) or {}
                if assets_chk is None:
                    assets_chk = _pick_num_ci(prev_row, ['Assets', 'TotalAssets', 'Total Assets', 'assets'])
                    proxy_fill = proxy_fill or (assets_chk is not None)
                if liab_chk is None:
                    liab_chk = _pick_num_ci(prev_row, ['Liabilities', 'TotalLiabilities', 'Total Liabilities', 'liabilities'])
                    proxy_fill = proxy_fill or (liab_chk is not None)
                if eq_chk is None:
                    prev_eq_candidates = [
                        _pick_num_ci(prev_row, ['StockholdersEquity', 'stockholdersequity']),
                        _pick_num_ci(prev_row, [
                            'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                            'stockholdersequityincludingportionattributabletononcontrollinginterest',
                        ]),
                        _pick_num_ci(prev_row, ['TotalEquity', 'Total Equity', 'totalequity']),
                    ]
                    prev_eq_candidates = [v for v in prev_eq_candidates if v is not None]
                    if prev_eq_candidates:
                        eq_chk = prev_eq_candidates[0]
                        proxy_fill = True
                if proxy_fill:
                    issues.append(f"{y}: balance anchors proxy-filled from {prev_y} for validation only.")

            if assets_chk is None or liab_chk is None or eq_chk is None:
                missing = []
                if assets_chk is None:
                    missing.append('Assets')
                if liab_chk is None:
                    missing.append('Liabilities')
                if eq_chk is None:
                    missing.append('Equity')
                issues.append(f"{y}: Balance anchors missing ({', '.join(missing)}) - balance-based ratios may stay N/A")
            elif allow_aggressive and prev_y is not None:
                # Prevent silent duplicate carry-forward when anchors are incomplete.
                prev_r = (ratios_by_year or {}).get(prev_y, {}) or {}
                for mk in (
                    'combined_proxy',
                    'capital_adequacy_proxy',
                    'net_income_to_assets',
                    'roa',
                    'roe',
                    'net_margin',
                    'loan_to_deposit_ratio',
                    'capital_ratio_proxy',
                    'equity_ratio',
                ):
                    cv = _num(r.get(mk))
                    pv = _num(prev_r.get(mk))
                    if cv is not None and pv is not None and abs(cv - pv) <= 1e-12:
                        src = str(r.get(f'{mk}_source') or '')
                        if 'CARRY_FORWARD' in src or 'PROXY' in src or proxy_fill:
                            r[mk] = None
                            r[f'{mk}_source'] = 'MISSING_SEC_ANCHOR'
                            issues.append(f"{y}: {mk} cleared (duplicate carry-forward under missing anchors)")

            net_income = _pick_num_ci(row, ['NetIncomeLoss', 'NetIncome', 'Net Income', 'netincomeloss'])
            revenue = _pick_num_ci(
                row,
                [
                    'Revenues',
                    'Revenue',
                    'SalesRevenueNet',
                    'RevenueFromContractWithCustomerExcludingAssessedTax',
                    'revenues',
                    'revenue',
                ],
            )
            shares = _year_shares_million(y)

            # EPS scale guard with split-awareness.
            eps = _num(r.get('eps_basic'))
            price_for_eps = _num(m.get('market:price') or m.get('yahoo:price'))
            market_pe_for_eps = _num(m.get('market:pe_ratio') or m.get('yahoo:pe_ratio'))
            split_factor = _split_ratio_for_year(y)
            if allow_aggressive and net_income is not None and shares not in (None, 0):
                share_candidates = [shares]
                if split_factor > 1.0:
                    share_candidates.extend([shares * split_factor, shares * (split_factor / 2.0)])
                eps_candidates = []
                for sh_c in share_candidates:
                    if sh_c in (None, 0):
                        continue
                    eps_candidates.extend([
                        net_income / sh_c,
                        (net_income * 1_000.0) / sh_c,
                        (net_income * 1_000_000.0) / sh_c,
                    ])
                eps_candidates = [c for c in eps_candidates if isinstance(c, (int, float)) and 0.01 <= abs(c) <= 5000]
                if eps_candidates:
                    eps_now_for_pe = eps
                    pe_now = None
                    if price_for_eps not in (None, 0) and eps_now_for_pe not in (None, 0):
                        pe_now = abs(price_for_eps / eps_now_for_pe)
                    requires_fix = (
                        eps is None
                        or abs(eps) < 0.05
                        or (pe_now is not None and (pe_now < 3.0 or pe_now > 1_500.0))
                    )
                    if requires_fix:
                        def _eps_score(c):
                            if price_for_eps in (None, 0):
                                return abs(abs(c) - 8.0)
                            pe_c = abs(price_for_eps / c) if c not in (None, 0) else None
                            if pe_c is None or pe_c <= 0:
                                return 1e12
                            target_pe = abs(market_pe_for_eps) if market_pe_for_eps not in (None, 0) else 25.0
                            score = abs(pe_c - target_pe)
                            if pe_c < 1.0 or pe_c > 2_000.0:
                                score += 100.0
                            return score
                        new_eps = min(eps_candidates, key=_eps_score)
                        if eps != new_eps:
                            r['eps_basic'] = new_eps
                            if split_factor > 1.0:
                                r['eps_source'] = f'QUALITY_GATE_SPLIT_ADJUST_X{split_factor:g}'
                            issues.append(f"{y}: eps_basic rescaled ({eps} -> {new_eps})")

            # Market canonical inputs (TTM) + annual ratio normalization.
            m_pe = _num(m.get('market:pe_ratio'))
            m_pb = _num(m.get('market:pb_ratio'))
            mcap = _as_million(r.get('market_cap'))
            if mcap is None:
                mcap = _as_million(m.get('market:market_cap'))
            price_y = _num(m.get('market:price')) or _num(m.get('yahoo:price'))
            equity = _pick_num_ci(
                row,
                [
                    'StockholdersEquity',
                    'TotalEquity',
                    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                    'Total Equity',
                    'stockholdersequity',
                    'stockholdersequityincludingportionattributabletononcontrollinginterest',
                ],
            )
            eps_now = _num(r.get('eps_basic'))
            bvps_now = _num(r.get('book_value_per_share'))
            if price_y not in (None, 0) and eps_now not in (None, 0):
                pe_annual = price_y / eps_now
                if 0.1 <= abs(pe_annual) <= 1_500:
                    cur_pe = _num(r.get('pe_ratio'))
                    if cur_pe is None or abs(cur_pe - pe_annual) > 1e-9:
                        r['pe_ratio'] = pe_annual
                        r['pe_ratio_source'] = 'ANNUAL_PRICE_OVER_EPS'
                        r['pe_ratio_used'] = pe_annual
                        r['pe_ratio_used_source'] = 'ANNUAL_PRICE_OVER_EPS'
                        issues.append(f"{y}: pe_ratio normalized to annual price/eps ({cur_pe} -> {pe_annual})")
                    if m_pe is not None and m_pe > 0:
                        r['pe_ratio_market_ttm'] = m_pe
            elif m_pe is not None and m_pe > 0:
                # Keep TTM only when annual inputs are unavailable.
                if _num(r.get('pe_ratio')) != m_pe:
                    issues.append(f"{y}: pe_ratio fallback to Layer2 market:pe_ratio (TTM)")
                r['pe_ratio'] = m_pe
                r['pe_ratio_source'] = 'MARKET_TTM_FALLBACK'
                r['pe_ratio_used'] = m_pe
                r['pe_ratio_used_source'] = 'MARKET_TTM_FALLBACK'

            if price_y not in (None, 0) and bvps_now not in (None, 0):
                pb_annual = price_y / bvps_now
                if 0.05 <= abs(pb_annual) <= 500:
                    cur_pb = _num(r.get('pb_ratio'))
                    if cur_pb is None or abs(cur_pb - pb_annual) > 1e-9:
                        r['pb_ratio'] = pb_annual
                        r['pb_ratio_source'] = 'ANNUAL_PRICE_OVER_BVPS'
                        r['pb_ratio_used'] = pb_annual
                        r['pb_ratio_used_source'] = 'ANNUAL_PRICE_OVER_BVPS'
                        issues.append(f"{y}: pb_ratio normalized to annual price/bvps ({cur_pb} -> {pb_annual})")
                    if m_pb is not None and m_pb > 0:
                        r['pb_ratio_market_ttm'] = m_pb
            elif m_pb is not None and m_pb > 0:
                if _num(r.get('pb_ratio')) != m_pb:
                    issues.append(f"{y}: pb_ratio fallback to Layer2 market:pb_ratio (TTM)")
                r['pb_ratio'] = m_pb
                r['pb_ratio_source'] = 'MARKET_TTM_FALLBACK'
                r['pb_ratio_used'] = m_pb
                r['pb_ratio_used_source'] = 'MARKET_TTM_FALLBACK'
            elif mcap not in (None, 0) and equity not in (None, 0):
                pb_candidates = [
                    mcap / equity,
                    mcap / (equity * 1_000.0),
                    mcap / (equity * 1_000_000.0),
                ]
                pb_plausible = [c for c in pb_candidates if 0.1 <= c <= 200.0]
                if pb_plausible:
                    pb_val = min(pb_plausible, key=lambda c: abs(c - 8.0))
                    cur_pb = _num(r.get('pb_ratio'))
                    if cur_pb is None or abs(cur_pb - pb_val) > 1e-9:
                        r['pb_ratio'] = pb_val
                        r['pb_ratio_source'] = 'DERIVED_MARKETCAP_EQUITY'
                        r['pb_ratio_used'] = pb_val
                        r['pb_ratio_used_source'] = 'DERIVED_MARKETCAP_EQUITY'
                        issues.append(f"{y}: pb_ratio derived from market_cap/equity ({cur_pb} -> {pb_val})")
            # Hard anomaly guard for P/B outliers (e.g., 0.04 for mega-cap tech).
            pb_now = _num(r.get('pb_ratio'))
            if pb_now is not None and (abs(pb_now) < 0.10 or abs(pb_now) > 500.0):
                pb_fix = None
                if price_y not in (None, 0) and bvps_now not in (None, 0):
                    pbt = price_y / bvps_now
                    if 0.10 <= abs(pbt) <= 500.0:
                        pb_fix = pbt
                if pb_fix is None and mcap not in (None, 0) and equity not in (None, 0):
                    cands = [
                        mcap / equity,
                        mcap / (equity * 1_000.0),
                        mcap / (equity * 1_000_000.0),
                    ]
                    cands = [c for c in cands if c is not None and 0.10 <= abs(c) <= 500.0]
                    if cands:
                        pb_fix = min(cands, key=lambda c: abs(abs(c) - 8.0))
                if pb_fix is not None:
                    r['pb_ratio'] = pb_fix
                    r['pb_ratio_source'] = 'QUALITY_GATE_HARD_PB_GUARD'
                    r['pb_ratio_used'] = pb_fix
                    r['pb_ratio_used_source'] = 'QUALITY_GATE_HARD_PB_GUARD'
                    issues.append(f"{y}: pb_ratio hard-guard fixed ({pb_now} -> {pb_fix})")

            # FCF yield unit guard (statement rows can be in millions).
            fcf = _num(r.get('free_cash_flow'))
            if fcf is not None and mcap not in (None, 0):
                candidates = [
                    fcf / mcap,
                    (fcf * 1_000.0) / mcap,
                    (fcf * 1_000_000.0) / mcap,
                ]
                plausible = [c for c in candidates if -1.0 <= c <= 1.0]
                if plausible:
                    new_yield = min(plausible, key=lambda c: abs(c - 0.04))
                    cur_yield = _num(r.get('fcf_yield'))
                    if cur_yield is None or abs(cur_yield - new_yield) > 1e-9:
                        r['fcf_yield'] = new_yield
                        issues.append(f"{y}: fcf_yield normalized ({cur_yield} -> {new_yield})")
            # Hard anomaly guard for absurd FCF yield magnitudes.
            cur_yield = _num(r.get('fcf_yield'))
            if cur_yield is not None and abs(cur_yield) > 1.0 and fcf is not None and mcap not in (None, 0):
                candidates = [
                    fcf / mcap,
                    (fcf * 1_000.0) / mcap,
                    (fcf * 1_000_000.0) / mcap,
                ]
                plausible = [c for c in candidates if -1.0 <= c <= 1.0]
                if plausible:
                    fixed = min(plausible, key=lambda c: abs(c - 0.04))
                    r['fcf_yield'] = fixed
                    issues.append(f"{y}: fcf_yield hard-guard fixed ({cur_yield} -> {fixed})")

            # OCF margin guard: recover missing values when OCF and revenue exist.
            ocf = _num(r.get('operating_cash_flow')) or _pick_num_ci(
                row,
                [
                    'NetCashProvidedByUsedInOperatingActivities',
                    'NetCashProvidedByOperatingActivities',
                    'OperatingCashFlow',
                    'operatingcashflow',
                ],
            )
            if allow_aggressive and ocf is not None and revenue not in (None, 0):
                ocf_margin_candidates = [
                    ocf / revenue,
                    (ocf * 1_000.0) / revenue,
                    (ocf * 1_000_000.0) / revenue,
                ]
                plausible_margin = [c for c in ocf_margin_candidates if -2.0 <= c <= 2.0]
                if plausible_margin:
                    ocf_margin = min(plausible_margin, key=lambda c: abs(c - 0.28))
                    cur_om = _num(r.get('ocf_margin'))
                    if cur_om is None or abs(cur_om - ocf_margin) > 1e-9:
                        r['ocf_margin'] = ocf_margin
                        issues.append(f"{y}: ocf_margin normalized ({cur_om} -> {ocf_margin})")

            # Book value per share guard.
            bvps = _num(r.get('book_value_per_share'))
            if allow_aggressive and (bvps is None or abs(bvps) < 0.05) and equity is not None and shares not in (None, 0):
                bvps_candidates = [
                    equity / shares,
                    (equity * 1_000.0) / shares,
                    (equity * 1_000_000.0) / shares,
                ]
                plausible_bvps = [c for c in bvps_candidates if -500.0 <= c <= 500.0]
                if plausible_bvps:
                    pb_ref = _num(r.get('pb_ratio')) or _num(m.get('market:pb_ratio')) or _num(m.get('yahoo:pb_ratio'))
                    px_ref = _num(m.get('market:price')) or _num(m.get('yahoo:price'))
                    if pb_ref not in (None, 0) and px_ref not in (None, 0):
                        # Choose BVPS that best matches observed P/B and price.
                        new_bvps = min(plausible_bvps, key=lambda c: abs((px_ref / c) - pb_ref) if c not in (None, 0) else 1e12)
                    else:
                        new_bvps = min(plausible_bvps, key=lambda c: abs(abs(c) - 6.0))
                    if bvps != new_bvps:
                        r['book_value_per_share'] = new_bvps
                        issues.append(f"{y}: book_value_per_share normalized ({bvps} -> {new_bvps})")

            # Additional hard guard for mixed-unit BVPS outliers seen in banks/insurers.
            bvps_now = _num(r.get('book_value_per_share'))
            if allow_aggressive and equity is not None and shares not in (None, 0):
                bvps_guard_candidates = [
                    equity / shares,
                    (equity * 1_000.0) / shares,
                    (equity * 1_000_000.0) / shares,
                ]
                bvps_guard_candidates = [c for c in bvps_guard_candidates if isinstance(c, (int, float)) and -2_000.0 <= c <= 2_000.0]
                if bvps_guard_candidates:
                    # Penalize tiny near-zero values when equity is large.
                    def _bvps_score(c):
                        score = 0.0
                        if abs(c) < 0.5 and abs(equity) > 1_000.0:
                            score += 100.0
                        pb_ref = _num(r.get('pb_ratio')) or _num(m.get('market:pb_ratio')) or _num(m.get('yahoo:pb_ratio'))
                        px_ref = _num(m.get('market:price')) or _num(m.get('yahoo:price'))
                        if pb_ref not in (None, 0) and px_ref not in (None, 0) and c not in (None, 0):
                            score += abs((px_ref / c) - pb_ref)
                        else:
                            score += abs(abs(c) - 8.0)
                        return score
                    best_bvps = min(bvps_guard_candidates, key=_bvps_score)
                    if bvps_now is None or abs(bvps_now - best_bvps) > 1e-9:
                        r['book_value_per_share'] = best_bvps
                        issues.append(f"{y}: book_value_per_share hard-guard applied ({bvps_now} -> {best_bvps})")

            # Net Debt / EBITDA guardrail (critical)
            debt = _num(r.get('total_debt'))
            allow_market_debt_override = str(os.environ.get('ALLOW_MARKET_DEBT_OVERRIDE', '0')).strip().lower() in ('1', 'true', 'yes')
            debt_layer = None
            if allow_market_debt_override:
                debt_layer = _as_million(m.get('market:total_debt')) or _as_million(m.get('yahoo:total_debt'))
            else:
                # Conservative fallback: only use market debt when SEC debt is missing.
                if debt is None:
                    debt_layer = _as_million(m.get('market:total_debt')) or _as_million(m.get('yahoo:total_debt'))
            liab = _pick_num_ci(row, ['Liabilities', 'TotalLiabilities', 'Total Liabilities', 'liabilities'])
            assets = _pick_num_ci(row, ['Assets', 'TotalAssets', 'Total Assets', 'assets'])
            cash = _pick_num_ci(
                row,
                [
                    'CashAndCashEquivalentsAtCarryingValue',
                    'CashAndCashEquivalents',
                    'Cash and Cash Equivalents',
                    'cashandcashequivalentsatcarryingvalue',
                    'cashandcashequivalents',
                ],
            )
            ebitda = _pick_num_ci(row, ['EBITDA', 'ebitda'])
            if ebitda is None:
                op = _pick_num_ci(row, ['OperatingIncomeLoss', 'OperatingIncome', 'Operating Income', 'operatingincomeloss'])
                dep = _pick_num_ci(
                    row,
                    [
                        'DepreciationDepletionAndAmortization',
                        'DepreciationAmortization',
                        'Depreciation and Amortization',
                        'depreciationdepletionandamortization',
                    ],
                )
                if op is not None:
                    ebitda = op + (dep or 0.0)

            ref_scale = assets or equity or liab
            if allow_market_debt_override and debt_layer is not None:
                debt = debt_layer
            elif debt is None and debt_layer is not None:
                debt = debt_layer
            if allow_aggressive and debt is not None and ref_scale not in (None, 0):
                debt_aligned = _align_to_reference(debt, ref_scale, lo=0.02, hi=2.5, target=0.35)
            else:
                debt_aligned = debt
            if allow_aggressive and cash is not None and (debt_aligned not in (None, 0)):
                cash_aligned = _align_to_reference(cash, debt_aligned, lo=0.001, hi=2.0, target=0.2)
            else:
                cash_aligned = cash
            if allow_aggressive and ebitda is not None and ref_scale not in (None, 0):
                ebitda_aligned = _align_to_reference(ebitda, ref_scale, lo=0.01, hi=1.5, target=0.25)
            else:
                ebitda_aligned = ebitda

            if allow_aggressive and debt_aligned is not None:
                cur_debt = _num(r.get('total_debt'))
                if cur_debt is None or abs(cur_debt - debt_aligned) > 1e-9:
                    r['total_debt'] = debt_aligned
                    issues.append(f"{y}: total_debt normalized ({cur_debt} -> {debt_aligned})")
                if equity not in (None, 0):
                    dte = debt_aligned / equity
                    if 0 <= abs(dte) <= 10:
                        cur_dte = _num(r.get('debt_to_equity'))
                        if cur_dte is None or abs(cur_dte - dte) > 1e-9:
                            r['debt_to_equity'] = dte
                            issues.append(f"{y}: debt_to_equity recomputed from total_debt/equity")
                if assets not in (None, 0):
                    dta = debt_aligned / assets
                    if 0 <= abs(dta) <= 5:
                        cur_dta = _num(r.get('debt_to_assets'))
                        if cur_dta is None or abs(cur_dta - dta) > 1e-9:
                            r['debt_to_assets'] = dta
                            issues.append(f"{y}: debt_to_assets recomputed from total_debt/assets")
            elif allow_aggressive and _num(r.get('total_debt')) is None:
                for debt_ratio_key in ('debt_to_equity', 'debt_to_assets'):
                    if _num(r.get(debt_ratio_key)) is not None:
                        r[debt_ratio_key] = None
                        issues.append(f"{y}: {debt_ratio_key} cleared because total_debt is unavailable")
            elif (not allow_aggressive) and debt_aligned is not None:
                # Keep ratios computable when only SEC debt is missing.
                if _num(r.get('total_debt')) is None:
                    r['total_debt'] = debt_aligned
                    r['total_debt_source'] = r.get('total_debt_source') or 'QUALITY_GATE_MARKET_FALLBACK'
                    issues.append(f"{y}: total_debt backfilled from market layer")
                if equity not in (None, 0) and _num(r.get('debt_to_equity')) is None:
                    dte = debt_aligned / equity
                    if 0 <= abs(dte) <= 10:
                        r['debt_to_equity'] = dte
                        issues.append(f"{y}: debt_to_equity backfilled from total_debt/equity")
                if assets not in (None, 0) and _num(r.get('debt_to_assets')) is None:
                    dta = debt_aligned / assets
                    if 0 <= abs(dta) <= 5:
                        r['debt_to_assets'] = dta
                        issues.append(f"{y}: debt_to_assets backfilled from total_debt/assets")

            if allow_aggressive and None not in (debt_aligned, cash_aligned) and ebitda not in (None, 0):
                # Re-evaluate EBITDA scaling against net debt directly to avoid 10x/100x outliers.
                nd_base = debt_aligned - (cash_aligned or 0.0)
                ebitda_cands = [
                    float(ebitda),
                    float(ebitda) / 1_000.0,
                    float(ebitda) / 1_000_000.0,
                    float(ebitda) * 1_000.0,
                    float(ebitda) * 1_000_000.0,
                ]
                plausible_e = [c for c in ebitda_cands if c not in (None, 0) and 0.01 <= (abs(c) / max(abs(nd_base), 1e-9)) <= 2.5]
                e_for_nd = min(plausible_e, key=lambda c: abs((abs(c) / max(abs(nd_base), 1e-9)) - 0.35)) if plausible_e else ebitda_aligned
                if e_for_nd not in (None, 0):
                    nd_eb_calc = nd_base / e_for_nd
                    # Reject impossible magnitudes after scaling attempts.
                    if -20 <= nd_eb_calc <= 20:
                        cur_nd = _num(r.get('net_debt_ebitda'))
                        if cur_nd is None or abs(cur_nd - nd_eb_calc) > 1e-9:
                            r['net_debt_ebitda'] = nd_eb_calc
                            issues.append(f"{y}: net_debt_ebitda normalized ({cur_nd} -> {nd_eb_calc})")
                    else:
                        if _num(r.get('net_debt_ebitda')) is not None:
                            r['net_debt_ebitda'] = None
                            issues.append(f"{y}: net_debt_ebitda cleared as implausible after scale checks")

        # Final sector enforcement: blocked ratios must remain hidden in exported sheets.
        if blocked_ratios:
            for y in years:
                r = (ratios_by_year or {}).setdefault(y, {})
                for rid in blocked_ratios:
                    if rid in r and r.get(rid) is not None:
                        r[rid] = None
                        issues.append(f"{y}: {rid} force-blocked for sector policy")
                    reasons = r.get('_ratio_reasons')
                    if not isinstance(reasons, dict):
                        reasons = {}
                        r['_ratio_reasons'] = reasons
                    reasons[rid] = 'NOT_APPLICABLE_FOR_SECTOR'

        return issues

    def _safe_excel_number(self, value):
        try:
            import math
            if value is None:
                return None
            if isinstance(value, (int, float)):
                if isinstance(value, float) and math.isnan(value):
                    return None
                return float(value)
            txt = str(value).strip()
            if not txt:
                return None
            up = txt.upper()
            if up.startswith('N/A') or up in {'NONE', 'NULL', 'NAN'}:
                return None
            neg = txt.startswith('(') and txt.endswith(')')
            txt = txt.replace(',', '').replace('$', '').replace(' ', '')
            mult = 1.0
            if txt.endswith('%'):
                mult = 0.01
                txt = txt[:-1]
            elif txt.endswith('B'):
                mult = 1_000_000_000.0
                txt = txt[:-1]
            elif txt.endswith('M'):
                mult = 1_000_000.0
                txt = txt[:-1]
            if neg:
                txt = txt[1:-1]
            num = float(txt)
            if neg:
                num = -num
            return num * mult
        except Exception:
            return None

    def _sheet_to_year_dict(self, df, label_candidates, allow_text=False, key_prefix=None):
        out = {}
        if df is None or df.empty:
            return out
        cols = [str(c) for c in df.columns]
        year_cols_map = {}
        for c in cols:
            c_txt = str(c).strip()
            y_match = re.search(r'(19|20)\d{2}', c_txt)
            if y_match:
                year_cols_map[c_txt] = int(y_match.group(0))
        year_cols = list(year_cols_map.keys())
        if not year_cols:
            return out
        label_col = None
        for cand in label_candidates:
            if cand in cols:
                label_col = cand
                break
        if not label_col:
            label_col = cols[0]
        for _, row in df.iterrows():
            raw_key = row.get(label_col)
            if raw_key is None:
                continue
            key = str(raw_key).strip()
            if not key:
                continue
            if key_prefix:
                key = f"{key_prefix}{key}"
            for yc in year_cols:
                y = year_cols_map.get(str(yc))
                if y is None:
                    continue
                val = row.get(yc)
                num = self._safe_excel_number(val)
                if num is not None:
                    out.setdefault(y, {})[key] = num
                elif allow_text:
                    sval = str(val).strip() if val is not None else ''
                    if sval and not sval.upper().startswith('N/A'):
                        out.setdefault(y, {})[key] = sval
        return out

    def _merge_year_dict(self, base, extra):
        out = {int(y): dict(v or {}) for y, v in (base or {}).items()}
        for y, row in (extra or {}).items():
            y_int = int(y)
            slot = out.setdefault(y_int, {})
            for k, v in (row or {}).items():
                if slot.get(k) is None and v is not None:
                    slot[k] = v
        return out

    def load_results_from_excel(self):
        fn = filedialog.askopenfilename(
            title=self._t('load_excel_title'),
            filetypes=[("Excel files", "*.xlsx;*.xlsm"), ("All files", "*.*")],
        )
        if not fn:
            return
        try:
            import pandas as pd

            xls = pd.ExcelFile(fn)
            sheets = set(xls.sheet_names or [])

            def _norm_sheet_name(s: str) -> str:
                return re.sub(r'[^a-z0-9]+', '', str(s or '').lower())

            sheet_index = {_norm_sheet_name(n): n for n in sheets}

            def _pick_sheet(*aliases):
                for alias in aliases:
                    hit = sheet_index.get(_norm_sheet_name(alias))
                    if hit:
                        return hit
                return None

            def _read_sheet(*aliases):
                sheet_name = _pick_sheet(*aliases)
                if not sheet_name:
                    return None
                return pd.read_excel(fn, sheet_name=sheet_name)

            raw_df = _read_sheet('Raw_by_Year', 'Raw by Year', 'Financial Data', 'البيانات المالية')
            layer1_df = _read_sheet('Layer1_Raw_SEC', 'Layer1 Raw SEC', 'SEC_Official_Statement')
            ratios_df = _read_sheet('Ratios', 'Financial Ratios', 'النسب المالية')
            strategic_df = _read_sheet('Strategic', 'Strategic Analysis', 'التحليل الاستراتيجي')
            layer2_df = _read_sheet('Layer2_Market', 'Layer2 Market', 'Market')
            layer3_df = _read_sheet('Layer3_Macro', 'Layer3 Macro', 'Macro')
            income_df = _read_sheet('Income Statement', 'StatementOfIncome')
            balance_df = _read_sheet('Balance Sheet', 'StatementOfBalanceSheet')
            cashflow_df = _read_sheet('Cash Flow', 'StatementOfCashFlows')

            data_by_year = self._sheet_to_year_dict(raw_df, ['Concept', 'Line Item', 'Item', 'البند'], allow_text=False)
            layer1_by_year = self._sheet_to_year_dict(layer1_df, ['Raw Label', 'Line Item', 'Concept', 'Item', 'البند'], allow_text=False)
            for stmt_df in (income_df, balance_df, cashflow_df):
                stmt_rows = self._sheet_to_year_dict(stmt_df, ['Line Item', 'Item', 'Concept', 'البند'], allow_text=False)
                data_by_year = self._merge_year_dict(data_by_year, stmt_rows)
            if not data_by_year:
                data_by_year = dict(layer1_by_year)
            else:
                data_by_year = self._merge_year_dict(data_by_year, layer1_by_year)

            ratios_by_year = self._sheet_to_year_dict(ratios_df, ['Metric', 'Ratio', 'Name', 'النسبة'], allow_text=False)
            strategic_by_year = self._sheet_to_year_dict(strategic_df, ['Metric', 'Indicator', 'المقياس'], allow_text=True)
            layer2_by_year = self._sheet_to_year_dict(layer2_df, ['Normalized Label', 'Item', 'البند المعياري'], allow_text=True)

            layer3_by_year = {}
            if layer3_df is not None and not layer3_df.empty:
                cols = [str(c) for c in layer3_df.columns]
                year_cols = [c for c in cols if re.search(r'(19|20)\d{2}', str(c))]
                cat_col = 'Category' if 'Category' in cols else None
                lbl_col = 'Normalized Label' if 'Normalized Label' in cols else (cols[0] if cols else None)
                if lbl_col:
                    for _, row in layer3_df.iterrows():
                        label = str(row.get(lbl_col) or '').strip()
                        if not label:
                            continue
                        category = str(row.get(cat_col) or '').strip() if cat_col else ''
                        key = f"{category}::{label}" if category else label
                        for yc in year_cols:
                            m = re.search(r'(19|20)\d{2}', str(yc))
                            if not m:
                                continue
                            y = int(m.group(0))
                            v = row.get(yc)
                            num = self._safe_excel_number(v)
                            if num is not None:
                                layer3_by_year.setdefault(y, {})[key] = num
                            else:
                                sval = str(v).strip() if v is not None else ''
                                if sval and not sval.upper().startswith('N/A'):
                                    layer3_by_year.setdefault(y, {})[key] = sval

            if not data_by_year:
                messagebox.showerror(self._t('msg_error'), self._translate_ui_text("لم يتم العثور على بيانات مالية صالحة داخل الملف."))
                return

            ticker_guess = Path(fn).stem.split('_')[0].upper()
            company_info = {'ticker': ticker_guess, 'name': ticker_guess, 'cik': None}
            data_layers = {
                'layer1_by_year': layer1_by_year or data_by_year,
                'layer2_by_year': layer2_by_year,
                'layer3_by_year': layer3_by_year,
                'layer4_by_year': {},
                'label_rows': [],
                'layer_catalog': [
                    {'key': 'layer1_by_year', 'title': 'Layer 1 - SEC XBRL (EDGAR)', 'source': 'SEC'},
                    {'key': 'layer2_by_year', 'title': 'Layer 2 - Market (Polygon)', 'source': 'MARKET'},
                    {'key': 'layer3_by_year', 'title': 'Layer 3 - Macro (FRED)', 'source': 'MACRO'},
                ],
            }

            loaded = {
                'success': True,
                'company_info': company_info,
                'data_by_year': data_by_year,
                'financial_ratios': ratios_by_year,
                'strategic_analysis': strategic_by_year,
                'data_layers': data_layers,
            }
            self.current_data = loaded
            self.multi_company_data[ticker_guess] = loaded
            if ticker_guess not in self.companies_listbox.get(0, tk.END):
                self.companies_listbox.insert(tk.END, ticker_guess)
            all_years = sorted({
                int(y)
                for src in (data_by_year, ratios_by_year, strategic_by_year)
                for y in (src or {}).keys()
                if str(y).isdigit()
            })
            if all_years:
                self.start_year_var.set(str(min(all_years)))
                self.end_year_var.set(str(max(all_years)))
            self._sync_layer_selector_from_data()
            self.display_all()
            self.display_comparison()
            messagebox.showinfo(self._t('msg_success'), f"{self._t('load_excel_success')}\n{fn}")
        except Exception as e:
            messagebox.showerror(self._t('msg_error'), f"{self._t('load_excel_failed')}: {e}")

    def _build_forecast_export_df(self):
        try:
            import pandas as pd
            if not self.current_data:
                return pd.DataFrame([{'Year': None, 'Revenue_Forecast': None, 'NetIncome_Forecast': None, 'Method': 'NO_CURRENT_DATA'}])
            db = ((self.current_data.get('data_layers', {}) or {}).get('layer1_by_year') or self.current_data.get('data_by_year', {}) or {})
            norm_db = {}
            for yk, row in (db or {}).items():
                try:
                    ky = int(yk)
                except Exception:
                    continue
                norm_db[ky] = row or {}
            if not norm_db:
                return pd.DataFrame([{'Year': None, 'Revenue_Forecast': None, 'NetIncome_Forecast': None, 'Method': 'NO_HISTORY'}])
            if hasattr(self.fetcher, 'generate_forecast'):
                rev_fore = self.fetcher.generate_forecast(norm_db, metric='Revenues', years_forward=10)
                net_fore = self.fetcher.generate_forecast(norm_db, metric='NetIncomeLoss', years_forward=10)
            else:
                rev_fore = self._generate_forecast_fallback(norm_db, metric='Revenues', years_forward=10)
                net_fore = self._generate_forecast_fallback(norm_db, metric='NetIncomeLoss', years_forward=10)
            years = sorted(set(list((rev_fore.get('forecast') or {}).keys()) + list((net_fore.get('forecast') or {}).keys())))
            rows = []
            for y in years:
                rows.append({
                    'Year': y,
                    'Revenue_Forecast': (rev_fore.get('forecast') or {}).get(y),
                    'NetIncome_Forecast': (net_fore.get('forecast') or {}).get(y),
                    'Revenue_Method': rev_fore.get('method'),
                    'NetIncome_Method': net_fore.get('method'),
                })
            return pd.DataFrame(rows if rows else [{'Year': None, 'Revenue_Forecast': None, 'NetIncome_Forecast': None, 'Method': 'EMPTY_FORECAST'}])
        except Exception as e:
            return pd.DataFrame([{'Year': None, 'Revenue_Forecast': None, 'NetIncome_Forecast': None, 'Method': f'ERROR: {e}'}])

    def _build_comparison_export_df(self):
        try:
            import pandas as pd
            rows, _details = self._collect_comparison_rows()
            if not rows:
                return pd.DataFrame([{
                    'Ticker': None,
                    'Sector': None,
                    'Confidence': None,
                    'Filing_Grade': None,
                    'OutOfRange': None,
                    'HIGH': None,
                    'MEDIUM': None,
                    'LOW': None,
                    'REJECTED': None,
                    'Top_Rejection_Reasons': 'NO_COMPARISON_DATA',
                    'Top_Validator_Flags': None,
                }])
            out = []
            for r in rows:
                out.append({
                    'Ticker': r.get('ticker'),
                    'Sector': r.get('sector'),
                    'Confidence': r.get('confidence'),
                    'Filing_Grade': r.get('filing_grade'),
                    'OutOfRange': r.get('out_of_range'),
                    'HIGH': r.get('high'),
                    'MEDIUM': r.get('medium'),
                    'LOW': r.get('low'),
                    'REJECTED': r.get('rejected'),
                    'Top_Rejection_Reasons': r.get('top_reasons'),
                    'Top_Validator_Flags': r.get('top_flags'),
                })
            return pd.DataFrame(out)
        except Exception as e:
            return pd.DataFrame([{'Ticker': None, 'Top_Rejection_Reasons': f'ERROR: {e}'}])

    def _build_ai_export_df(self):
        try:
            import pandas as pd
            if not self.current_data:
                return pd.DataFrame([{'Metric': 'AI_Status', 'Value': 'NO_CURRENT_DATA'}])
            from modules.advanced_analysis import generate_ai_insights
            data_by_year_raw = ((self.current_data.get('data_layers', {}) or {}).get('layer1_by_year') or self.current_data.get('data_by_year', {}))
            ratios_by_year_raw = maybe_guard_ratios_by_year(self.current_data.get('financial_ratios', {}))
            data_by_year = {}
            for yk, row in (data_by_year_raw or {}).items():
                try:
                    ky = int(yk)
                except Exception:
                    continue
                data_by_year[ky] = row or {}
            ratios_by_year = {}
            for yk, row in (ratios_by_year_raw or {}).items():
                try:
                    ky = int(yk)
                except Exception:
                    continue
                ratios_by_year[ky] = row or {}
            if not data_by_year or not ratios_by_year:
                return pd.DataFrame([{'Metric': 'AI_Status', 'Value': 'MISSING_INPUTS'}])
            ticker = (self.current_data.get('company_info', {}) or {}).get('ticker', 'CURRENT')
            years = sorted([y for y in data_by_year.keys() if isinstance(y, int)])
            latest_year = years[-1] if years else None
            ratio_source = UnifiedRatioSource()
            ratio_source.load(ticker, data_by_year, ratios_by_year)
            investment_score = ratio_source.get_ratio_contract(ticker, latest_year, 'investment_score').get('value')
            if investment_score is None:
                investment_score = 50
            economic_spread = ratio_source.get_ratio_contract(ticker, latest_year, 'economic_spread').get('value')
            if economic_spread is None:
                roic = ratio_source.get_ratio_contract(ticker, latest_year, 'roic').get('value')
                wacc = ratio_source.get_ratio_contract(ticker, latest_year, 'wacc').get('value')
                economic_spread = (roic - wacc) if (roic is not None and wacc is not None) else 0.0
            fcf_yield = ratio_source.get_ratio_contract(ticker, latest_year, 'fcf_yield').get('value')
            if fcf_yield is None:
                fcf = ratio_source.get_ratio_contract(ticker, latest_year, 'free_cash_flow').get('value')
                market_cap = ratio_source.get_ratio_contract(ticker, latest_year, 'market_cap').get('value')
                fcf_yield = (fcf / market_cap) if (fcf and market_cap) else 0.0
            insights = generate_ai_insights(
                data_by_year=data_by_year,
                ratios_by_year=ratios_by_year,
                investment_score=investment_score,
                economic_spread=economic_spread,
                fcf_yield=fcf_yield,
            ) or {}
            rows = []
            def _flatten(prefix, obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        np = f"{prefix}.{k}" if prefix else str(k)
                        _flatten(np, v)
                elif isinstance(obj, list):
                    rows.append({'Metric': prefix, 'Value': ', '.join([str(x) for x in obj])})
                else:
                    rows.append({'Metric': prefix, 'Value': obj})
            _flatten('', insights)
            rows.append({'Metric': 'meta.ticker', 'Value': ticker})
            rows.append({'Metric': 'meta.latest_year', 'Value': latest_year})
            return pd.DataFrame(rows if rows else [{'Metric': 'AI_Status', 'Value': 'EMPTY'}])
        except Exception as e:
            return pd.DataFrame([{'Metric': 'AI_Status', 'Value': f'ERROR: {e}'}])

    # ---------- Export to Excel ----------
    def export_to_excel(self):
        # Diagnostic trace: prove button click reached export path.
        try:
            log_dir = Path("outputs")
            log_dir.mkdir(parents=True, exist_ok=True)
            with open(log_dir / "export_click.log", "a", encoding="utf-8") as fh:
                fh.write(f"[{datetime.now().isoformat()}] export_to_excel invoked\n")
        except Exception:
            pass

        if not self.current_data:
            # Fallback: recover data from selected company in list, or first loaded company.
            try:
                selected = self.companies_listbox.curselection()
                if selected:
                    cname = self.companies_listbox.get(selected[0])
                    if cname in self.multi_company_data:
                        self.current_data = self.multi_company_data[cname]
                if not self.current_data and self.multi_company_data:
                    first_key = next(iter(self.multi_company_data.keys()))
                    self.current_data = self.multi_company_data.get(first_key)
            except Exception:
                pass

            if not self.current_data:
                messagebox.showwarning(self._t('msg_warning'), self._translate_ui_text("لا توجد بيانات حالية للتصدير. جلب البيانات أولاً."))
                return
        try:
            import pandas as pd
        except Exception as e:
            messagebox.showerror(self._t('msg_error'), f"{self._translate_ui_text('مطلوب تثبيت pandas و openpyxl')}:\n{e}\n\npip install pandas openpyxl")
            return

        data_by_year = self.current_data.get('data_by_year', {}) or {}
        data_layers = self.current_data.get('data_layers', {}) or {}
        ratios_by_year = self.current_data.get('financial_ratios', {}) or {}
        sector_gating = (self.current_data.get('sector_gating', {}) if self.current_data else {}) or {}
        blocked_ratios = set(sector_gating.get('blocked_ratios', []) or [])
        blocked_strategic_metrics = set(sector_gating.get('blocked_strategic_metrics', []) or [])
        gate_issues = self._apply_pre_export_quality_gate(
            years=self._get_selected_years_range(),
            data_by_year=data_by_year,
            ratios_by_year=ratios_by_year,
            data_layers=data_layers,
        )
        years = self._get_selected_years_range()
        per_year = self._compute_per_year_metrics(data_by_year, ratios_by_year)
        # Keep Ratios and Strategic fully synchronized on shared metrics.
        strategic_to_ratio = {
            'WACC': 'wacc',
            'PE_Ratio': 'pe_ratio',
            'PB_Ratio': 'pb_ratio',
            'FCF_Yield': 'fcf_yield',
            'Net_Debt_EBITDA': 'net_debt_ebitda',
            'ROIC': 'roic',
            'ROE': 'roe',
            'EPS': 'eps_basic',
            'EV_EBITDA': 'ev_ebitda',
            'Retention_Ratio': 'retention_ratio',
            'SGR_Internal': 'sgr_internal',
            'Dividends_Paid': 'dividends_paid',
            'Inventory_Days': 'inventory_days',
            'AR_Days': 'days_sales_outstanding',
            'AP_Days': 'ap_days',
            'CCC_Days': 'ccc_days',
            'Cost_of_Debt': 'cost_of_debt',
            'Accruals_Ratio': 'accruals_ratio',
        }
        for y in years:
            py = per_year.get(y, {}) or {}
            ry = ratios_by_year.setdefault(y, {})
            for s_key, r_key in strategic_to_ratio.items():
                v = py.get(s_key)
                if isinstance(v, (int, float)):
                    ry[r_key] = float(v)
        if gate_issues:
            print(f"🛡️ Quality gate applied {len(gate_issues)} corrections before export.")

        raw_rows = []
        concepts = sorted({k for y in years for k in data_by_year.get(y, {}).keys()})
        for c in concepts:
            row = {'Concept': c}
            for y in years:
                row[str(y)] = data_by_year.get(y, {}).get(c)
            raw_rows.append(row)
        raw_df = pd.DataFrame(raw_rows)
        canonical_df = pd.DataFrame()

        # Prevent misleading legacy labels in exported raw sheet.
        try:
            def _get_row(concept_name):
                m = raw_df[raw_df['Concept'] == concept_name]
                return m.iloc[0] if not m.empty else None

            rev_row = _get_row('Revenue')
            revs_row = _get_row('Revenues')
            cogs_row = _get_row('CostOfRevenue')
            if rev_row is not None and revs_row is not None and cogs_row is not None:
                same_as_cogs = all(
                    float(rev_row[str(y)]) == float(cogs_row[str(y)])
                    for y in years
                    if rev_row[str(y)] is not None and cogs_row[str(y)] is not None
                )
                if same_as_cogs:
                    raw_df.loc[raw_df['Concept'] == 'Revenue', 'Concept'] = 'Revenue_Legacy_Conflicted'

            assets_row = _get_row('Assets')
            ta_row = _get_row('Total Assets')
            if assets_row is not None and ta_row is not None:
                ratios = []
                for y in years:
                    a = assets_row[str(y)]
                    t = ta_row[str(y)]
                    if isinstance(a, (int, float)) and isinstance(t, (int, float)) and a != 0:
                        ratios.append(float(t) / float(a))
                if ratios and sum(ratios) / len(ratios) < 0.8:
                    raw_df.loc[raw_df['Concept'] == 'Total Assets', 'Concept'] = 'Total Assets (Legacy Current Assets)'

            liab_row = _get_row('Liabilities')
            tl_row = _get_row('Total Liabilities')
            if liab_row is not None and tl_row is not None:
                ratios = []
                for y in years:
                    l = liab_row[str(y)]
                    t = tl_row[str(y)]
                    if isinstance(l, (int, float)) and isinstance(t, (int, float)) and l != 0:
                        ratios.append(float(t) / float(l))
                if ratios and sum(ratios) / len(ratios) < 0.8:
                    raw_df.loc[raw_df['Concept'] == 'Total Liabilities', 'Concept'] = 'Total Liabilities (Legacy Current Liabilities)'
        except Exception:
            pass

        # Canonicalized raw view to reduce duplicate semantic concepts while keeping Raw_by_Year unchanged.
        try:
            alias_groups = {
                'Revenues': ['Revenue', 'NetRevenue', 'NetRevenue_Hierarchy', 'SalesRevenueNet', 'Revenue_Hierarchy'],
                'CostOfRevenue': ['COGS', 'Cost of sales', 'CostOfGoodsAndServicesSold'],
                'NetIncomeLoss': ['NetIncome', 'Net Income'],
                'OperatingIncomeLoss': ['OperatingIncome', 'Operating Income', 'OperatingIncome_Hierarchy'],
                'Assets': ['TotalAssets', 'Total Assets'],
                'Liabilities': ['TotalLiabilities', 'Total Liabilities'],
                'StockholdersEquity': ['TotalEquity', 'Total Equity'],
                'AssetsCurrent': ['CurrentAssets', 'TotalCurrentAssets_Parent', 'Current Assets', 'TotalCurrentAssets_Hierarchy'],
                'LiabilitiesCurrent': ['CurrentLiabilities', 'TotalCurrentLiabilities_Parent', 'Current Liabilities', 'TotalCurrentLiabilities_Hierarchy'],
                'AccountsReceivableNetCurrent': ['AccountsReceivable', 'AccountsReceivableNetCurrent_Hierarchy'],
                'AccountsPayableCurrent': ['AccountsPayable', 'AccountsPayableCurrent_Hierarchy'],
                'InventoryNet': ['Inventory', 'InventoryNet_Hierarchy'],
                'NetCashProvidedByUsedInOperatingActivities': ['OperatingCashFlow', 'Operating Cash Flow', 'NetCashProvidedByOperatingActivities'],
                'DepreciationDepletionAndAmortization': ['DepreciationAmortization', 'Depreciation and Amortization'],
                'WeightedAverageNumberOfSharesOutstandingBasic': ['Basic (shares)', 'SharesBasic', 'CommonStockSharesOutstanding'],
            }
            concept_to_canonical = {}
            for canonical_name, aliases in alias_groups.items():
                concept_to_canonical[canonical_name] = canonical_name
                for alias_name in aliases:
                    concept_to_canonical[alias_name] = canonical_name

            grouped = {}
            for _, row_obj in raw_df.iterrows():
                concept = row_obj.get('Concept')
                if self._is_internal_helper_label(concept):
                    continue
                canonical = concept_to_canonical.get(concept, concept)
                # Safe merge: only concepts explicitly listed in alias_groups
                # are allowed to use anchored semantic merge.
                allow_anchor = concept in concept_to_canonical
                canonical = self._safe_merge_key_for_label(
                    canonical,
                    allow_anchor_for_free_text=allow_anchor,
                ) or canonical
                score = 0
                values = {}
                for y in years:
                    v = row_obj.get(str(y))
                    values[str(y)] = v
                    if isinstance(v, (int, float)):
                        score += 1
                entry = {
                    'Concept': canonical,
                    'Canonical_Source': concept,
                    **values,
                }
                grouped.setdefault(canonical, []).append((score, entry))

            canonical_rows = []
            canonical_collision_rows = []
            for canonical_name, candidates in grouped.items():
                # Prefer the richest row; then prefer direct canonical label.
                candidates.sort(key=lambda t: (t[0], 1 if t[1].get('Canonical_Source') == canonical_name else 0), reverse=True)
                best = dict(candidates[0][1])
                # Coalesce missing/conflicting yearly values across aliases.
                for y in years:
                    yk = str(y)
                    cur = best.get(yk)
                    if cur is None:
                        for _, cand in candidates:
                            if cand.get(yk) is not None:
                                best[yk] = cand.get(yk)
                                break
                    # Safe mode: never override an existing non-null value by magnitude.
                    # This prevents accidental anchor pollution during alias merge.
                sem_key = str(canonical_name or '')
                if sem_key.startswith('sem::'):
                    best['Concept'] = sem_key.split('::', 1)[1]
                elif sem_key.startswith('raw::') or sem_key.startswith('tech::'):
                    best['Concept'] = sem_key.split('::', 1)[1]
                best['Aliases_Merged_Count'] = len(candidates)
                if len(candidates) > 1:
                    best['Aliases_Merged'] = ', '.join(sorted({str(c[1].get('Canonical_Source')) for c in candidates}))
                else:
                    best['Aliases_Merged'] = str(best.get('Canonical_Source'))
                canonical_rows.append(best)
                if len(candidates) > 1:
                    canonical_collision_rows.append({
                        'Canonical_Key': best.get('Concept'),
                        'Aliases_Count': len(candidates),
                        'Aliases': best.get('Aliases_Merged'),
                    })
            canonical_df = pd.DataFrame(canonical_rows)
            canonical_collision_df = pd.DataFrame(canonical_collision_rows)
            # Export deduplicated/coalesced view as main Raw_by_Year to prevent bilingual
            # duplicates splitting values across separate rows.
            if not canonical_df.empty:
                export_cols = ['Concept'] + [str(y) for y in years]
                raw_df = canonical_df[[c for c in export_cols if c in canonical_df.columns]].copy()
        except Exception:
            canonical_df = pd.DataFrame()
            canonical_collision_df = pd.DataFrame()

        ticker = (self.current_data.get('company_info', {}) or {}).get('ticker', 'CURRENT')
        ratio_source = UnifiedRatioSource()
        ratio_source.load(ticker, data_by_year, ratios_by_year)

        ratio_rows = []
        sector_profile_export = self._get_sector_profile()
        ratio_export_keys = self._get_sector_ratio_export_keys(sector_profile_export)
        ratio_metrics = []
        for metric_key in ratio_export_keys:
            if metric_key in blocked_ratios:
                continue
            has_any_value = False
            for y in years:
                contract = ratio_source.get_ratio_contract(ticker, y, metric_key) or {}
                raw_v = contract.get('value')
                if self._is_present_metric_value(raw_v) or str(contract.get('status') or '').upper() == 'NOT_COMPUTABLE':
                    has_any_value = True
                    break
            if has_any_value:
                ratio_metrics.append(metric_key)
        for m in ratio_metrics:
            row = {'Metric': m}
            for y in years:
                contract = ratio_source.get_ratio_contract(ticker, y, m) or {}
                raw_v = contract.get('value')
                if isinstance(raw_v, (int, float)):
                    row[str(y)] = raw_v
                else:
                    reason = str(contract.get('reason') or contract.get('status') or 'NOT_COMPUTABLE')
                    row[str(y)] = f"N/A ({reason})"
            ratio_rows.append(row)
        ratios_df = pd.DataFrame(ratio_rows)

        strat_rows = []
        strategic_export_keys = self._get_sector_strategic_export_keys(sector_profile_export)
        metric_keys = []
        for metric_key in strategic_export_keys:
            if metric_key in blocked_strategic_metrics:
                continue
            has_any_value = False
            for y in years:
                raw_v = (per_year.get(y, {}) or {}).get(metric_key)
                if self._is_present_metric_value(raw_v):
                    has_any_value = True
                    break
            if has_any_value:
                metric_keys.append(metric_key)
        for key in metric_keys:
            row = {'Metric': key}
            for y in years:
                row[str(y)] = per_year.get(y, {}).get(key)
            strat_rows.append(row)
        strat_df = pd.DataFrame(strat_rows)

        ratio_audit_df, balance_audit_df, critical_df, acceptance_df = self._build_export_acceptance_frames(
            years=years,
            ticker=ticker,
            sector_profile=sector_profile_export,
            data_by_year=data_by_year,
            per_year=per_year,
            ratio_source=ratio_source,
            ratio_export_keys=ratio_export_keys,
            strategic_export_keys=strategic_export_keys,
            blocked_ratios=blocked_ratios,
            blocked_strategic_metrics=blocked_strategic_metrics,
            gate_issues=gate_issues,
        )

        # 3 data layers (requested structure)
        layer1_by_year = data_layers.get('layer1_by_year', {}) or data_by_year
        layer2_by_year = data_layers.get('layer2_by_year', {}) or {}
        layer3_by_year = data_layers.get('layer3_by_year', {}) or {}
        sector_profile = (sector_gating or {}).get('profile', 'industrial')

        layer1_rows = []
        l1_keys = sorted({k for y in years for k in layer1_by_year.get(y, {}).keys()})
        if sector_profile in ('bank', 'industrial', 'unknown'):
            bank_only_markers = (
                'LoansReceivable', 'Deposits', 'NetInterestIncome',
                'ProvisionForCreditLosses', 'AllowanceForCreditLosses',
                'FederalFundsSold', 'NoninterestBearingDeposits'
            )
            industrial_only_markers = (
                'Inventory', 'CostOfRevenue', 'GrossProfit',
                'ResearchAndDevelopmentExpense', 'SellingGeneralAndAdministrativeExpense',
                'RevenueFromContractWithCustomerExcludingAssessedTax'
            )
            filtered_l1 = []
            for k in l1_keys:
                ks = str(k)
                if sector_profile == 'bank' and any(m in ks for m in industrial_only_markers):
                    continue
                if sector_profile in ('industrial', 'technology', 'unknown') and any(m in ks for m in bank_only_markers):
                    continue
                filtered_l1.append(k)
            l1_keys = filtered_l1
        # Merge display-duplicate labels (same semantic key with spacing/case variants).
        layer1_merged = {}
        layer1_order = []
        for k in l1_keys:
            display_label = self._decode_mojibake_text(str(k))
            display_label = re.sub(r"\s+", " ", display_label).strip()
            if self._is_internal_helper_label(display_label):
                continue
            # Safe merge: prevent over-merging long free-text SEC labels into anchors
            # such as "current liabilities" when they are actually sub-lines.
            allow_anchor = bool(str(k) in {
                'Assets', 'TotalAssets', 'AssetsCurrent', 'CurrentAssets',
                'Liabilities', 'TotalLiabilities', 'LiabilitiesCurrent', 'CurrentLiabilities',
                'StockholdersEquity', 'TotalEquity',
                'Revenues', 'Revenue', 'CostOfRevenue', 'NetIncomeLoss',
            })
            label_key = self._safe_merge_key_for_label(
                display_label,
                allow_anchor_for_free_text=allow_anchor,
            )
            if label_key not in layer1_merged:
                layer1_merged[label_key] = {'Raw Label': display_label, '__aliases__': {str(k)}}
                for y in years:
                    layer1_merged[label_key][str(y)] = None
                layer1_order.append(label_key)
            else:
                layer1_merged[label_key]['__aliases__'].add(str(k))
                layer1_merged[label_key]['Raw Label'] = self._prefer_display_label(
                    layer1_merged[label_key].get('Raw Label'),
                    display_label,
                )

            for y in years:
                col = str(y)
                cur_val = layer1_merged[label_key].get(col)
                new_val = layer1_by_year.get(y, {}).get(k)
                if cur_val is None and new_val is not None:
                    layer1_merged[label_key][col] = new_val
                # Safe mode: keep first non-null; do not override by absolute magnitude.

        for lk in layer1_order:
            row = layer1_merged[lk]
            aliases = sorted(
                {
                    re.sub(r"\s+", " ", str(a)).strip()
                    for a in row.pop('__aliases__', set())
                    if a is not None
                }
            )
            aliases = [a for a in aliases if a and a != row.get('Raw Label')]
            row['Aliases_Merged'] = ', '.join(aliases) if aliases else row.get('Raw Label')
            layer1_rows.append(row)
        layer1_df = pd.DataFrame(layer1_rows)

        layer2_rows = []
        l2_keys = sorted({k for y in years for k in layer2_by_year.get(y, {}).keys()})
        for k in l2_keys:
            row = {'Normalized Label': k}
            for y in years:
                row[str(y)] = layer2_by_year.get(y, {}).get(k)
            layer2_rows.append(row)
        layer2_df = pd.DataFrame(layer2_rows)

        layer3_rows = []
        l3_keys = sorted({k for y in years for k in layer3_by_year.get(y, {}).keys()})
        for k in l3_keys:
            if '::' in k:
                category, normalized = k.split('::', 1)
            else:
                category, normalized = 'Unclassified', k
            row = {'Category': category, 'Normalized Label': normalized}
            for y in years:
                row[str(y)] = layer3_by_year.get(y, {}).get(k)
            layer3_rows.append(row)
        layer3_df = pd.DataFrame(layer3_rows)

        labels_df = pd.DataFrame(data_layers.get('label_rows', []) or [])
        gate_df = pd.DataFrame([{'Issue': i} for i in gate_issues]) if gate_issues else pd.DataFrame([{'Issue': 'No corrections required'}])
        methodology_rows = [
            {'Topic': 'Version', 'Rule': 'Final actual export (non-breaking)', 'Details': 'Documentation-only; numeric results are unchanged.'},
            {'Topic': 'Sector Profile', 'Rule': 'Detected from SEC/SIC + structural facts', 'Details': f"Applied profile: {sector_profile}"},
            {'Topic': 'Units', 'Rule': 'Statement rows in million USD', 'Details': 'Market cap exported in million USD; percentage ratios as decimals (e.g., 0.031 = 3.1%).'},
            {'Topic': 'P/B (Raw)', 'Rule': 'Primary ratio line', 'Details': 'May come from annual price/BVPS or market layer depending on availability.'},
            {'Topic': 'P/B (Used)', 'Rule': 'Quality-gated reference', 'Details': 'Stabilized value used for strategic mapping when raw value has unit anomaly.'},
            {'Topic': 'FCF Yield', 'Rule': 'FCF / Market Cap', 'Details': 'Scale-guarded to prevent 1000x unit slips.'},
            {'Topic': 'Interest Coverage', 'Rule': 'Uses SEC interest expense resolver', 'Details': 'Falls back to explicit tagged proxies with reliability tracing.'},
            {'Topic': 'Fair Value', 'Rule': 'DCF first, ratio fallback second', 'Details': 'If DCF anchors are weak, uses PE*EPS and PB*BVPS fallback with warning flag.'},
            {'Topic': 'Traceability', 'Rule': 'Audit sheets included', 'Details': 'See Quality_Gate, Ratio_Audit, Balance_Check, Final_Acceptance.'},
        ]
        methodology_df = pd.DataFrame(methodology_rows)
        forecasts_df = self._build_forecast_export_df()
        comparison_df = self._build_comparison_export_df()
        ai_df = self._build_ai_export_df()

        default_name = f"{self.current_data.get('company_info', {}).get('ticker','company')}_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        fn = filedialog.asksaveasfilename(defaultextension=".xlsx", initialfile=default_name, filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])
        if not fn:
            # Fallback: auto-save to a deterministic local path if save dialog is cancelled/fails.
            try:
                auto_dir = Path("exports") / "manual_exports"
                auto_dir.mkdir(parents=True, exist_ok=True)
                fn = str(auto_dir / default_name)
            except Exception:
                return
        try:
            with pd.ExcelWriter(fn, engine='openpyxl') as writer:
                raw_df.to_excel(writer, sheet_name='Raw_by_Year', index=False)
                if not canonical_df.empty:
                    canonical_df.to_excel(writer, sheet_name='Raw_by_Year_Canonical', index=False)
                ratios_df.to_excel(writer, sheet_name='Ratios', index=False)
                strat_df.to_excel(writer, sheet_name='Strategic', index=False)
                layer1_df.to_excel(writer, sheet_name='Layer1_Raw_SEC', index=False)
                layer2_df.to_excel(writer, sheet_name='Layer2_Market', index=False)
                layer3_df.to_excel(writer, sheet_name='Layer3_Macro', index=False)
                labels_df.to_excel(writer, sheet_name='Label_Map_Audit', index=False)
                gate_df.to_excel(writer, sheet_name='Quality_Gate', index=False)
                ratio_audit_df.to_excel(writer, sheet_name='Ratio_Audit', index=False)
                if not canonical_collision_df.empty:
                    canonical_collision_df.to_excel(writer, sheet_name='Canonical_Collision_Audit', index=False)
                balance_audit_df.to_excel(writer, sheet_name='Balance_Check', index=False)
                critical_df.to_excel(writer, sheet_name='Critical_Flags', index=False)
                acceptance_df.to_excel(writer, sheet_name='Final_Acceptance', index=False)
                forecasts_df.to_excel(writer, sheet_name='Forecasts', index=False)
                comparison_df.to_excel(writer, sheet_name='Comparative_Analysis', index=False)
                ai_df.to_excel(writer, sheet_name='AI_Analysis', index=False)
                methodology_df.to_excel(writer, sheet_name='Methodology_Final', index=False)
            try:
                score_row = acceptance_df[acceptance_df['Metric'] == 'Final_Professional_Score']
                verdict_row = acceptance_df[acceptance_df['Metric'] == 'Verdict']
                score_val = float(score_row.iloc[0]['Value']) if not score_row.empty else 0.0
                verdict_val = str(verdict_row.iloc[0]['Value']) if not verdict_row.empty else 'UNKNOWN'
            except Exception:
                score_val = 0.0
                verdict_val = 'UNKNOWN'
            if verdict_val == 'APPROVED_FOR_EXPERT_REVIEW':
                messagebox.showinfo(
                    self._t('msg_success'),
                    f"تم حفظ الملف: {fn}\n"
                    f"Final Professional Score: {score_val:.2f}\n"
                    f"Verdict: {verdict_val}",
                )
            else:
                messagebox.showwarning(
                    self._t('msg_warning'),
                    f"تم حفظ الملف: {fn}\n"
                    f"Final Professional Score: {score_val:.2f}\n"
                    f"Verdict: {verdict_val}\n"
                    f"راجع الأوراق: Final_Acceptance و Critical_Flags.",
                )
        except Exception as e:
            try:
                log_dir = Path("outputs")
                log_dir.mkdir(parents=True, exist_ok=True)
                log_path = log_dir / "export_error.log"
                with open(log_path, "a", encoding="utf-8") as fh:
                    fh.write(f"\n[{datetime.now().isoformat()}] Export failed\n")
                    fh.write(f"Target: {fn}\n")
                    fh.write(f"Error: {repr(e)}\n")
                    fh.write(traceback.format_exc())
                    fh.write("\n" + ("-" * 80) + "\n")
            except Exception:
                pass
            messagebox.showerror(
                self._t('msg_error'),
                f"{self._translate_ui_text('فشل التصدير')}: {e}\n{self._translate_ui_text('راجع السجل')}: outputs/export_error.log"
            )

    def _export_to_excel_minimal(self):
        """
        Guaranteed fallback exporter:
        writes core sheets only (Raw_by_Year, Ratios, Strategic) without advanced audit transformations.
        """
        try:
            import pandas as pd
        except Exception as e:
            messagebox.showerror(self._t('msg_error'), f"{self._translate_ui_text('فشل التصدير')}: {e}")
            return

        if not self.current_data and self.multi_company_data:
            try:
                self.current_data = next(iter(self.multi_company_data.values()))
            except Exception:
                self.current_data = None
        if not self.current_data:
            messagebox.showwarning(self._t('msg_warning'), self._translate_ui_text("لا توجد بيانات حالية للتصدير."))
            return

        years = self._get_selected_years_range()
        data_by_year = self.current_data.get('data_by_year', {}) or {}
        ratios_by_year = self.current_data.get('financial_ratios', {}) or {}
        strategic_by_year = self.current_data.get('strategic_analysis', {}) or {}

        raw_rows = []
        concepts = sorted({k for y in years for k in (data_by_year.get(y, {}) or {}).keys()})
        for c in concepts:
            row = {'Concept': c}
            for y in years:
                row[str(y)] = (data_by_year.get(y, {}) or {}).get(c)
            raw_rows.append(row)

        ratio_rows = []
        ratio_keys = sorted({k for y in years for k in (ratios_by_year.get(y, {}) or {}).keys() if not str(k).startswith('_')})
        for k in ratio_keys:
            row = {'Metric': k}
            for y in years:
                row[str(y)] = (ratios_by_year.get(y, {}) or {}).get(k)
            ratio_rows.append(row)

        strat_rows = []
        strat_keys = sorted({k for y in years for k in (strategic_by_year.get(y, {}) or {}).keys() if not str(k).startswith('_')})
        for k in strat_keys:
            row = {'Metric': k}
            for y in years:
                row[str(y)] = (strategic_by_year.get(y, {}) or {}).get(k)
            strat_rows.append(row)

        raw_df = pd.DataFrame(raw_rows)
        ratios_df = pd.DataFrame(ratio_rows)
        strat_df = pd.DataFrame(strat_rows)
        forecasts_df = self._build_forecast_export_df()
        comparison_df = self._build_comparison_export_df()
        ai_df = self._build_ai_export_df()
        methodology_df = pd.DataFrame([
            {'Topic': 'Version', 'Rule': 'Minimal export', 'Details': 'Core sheets only; values are exported as-is from current analysis.'},
            {'Topic': 'Units', 'Rule': 'Statement rows in million USD', 'Details': 'Market-dependent metrics may use million-USD market cap base.'},
            {'Topic': 'Ratios', 'Rule': 'From ratio engine', 'Details': 'N/A values remain explicit when inputs are missing.'},
            {'Topic': 'Strategic', 'Rule': 'From strategic engine', 'Details': 'Uses available ratio outputs and layer inputs without rewriting values.'},
        ])

        ticker = (self.current_data.get('company_info', {}) or {}).get('ticker', 'company')
        default_name = f"{ticker}_analysis_minimal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        fn = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            title=self._translate_ui_text("اختر مكان حفظ ملف إكسل"),
        )
        if not fn:
            out_dir = Path("exports") / "manual_exports"
            out_dir.mkdir(parents=True, exist_ok=True)
            fn = str(out_dir / default_name)

        fn_path = Path(fn).resolve()

        with pd.ExcelWriter(str(fn_path), engine='openpyxl') as writer:
            raw_df.to_excel(writer, sheet_name='Raw_by_Year', index=False)
            ratios_df.to_excel(writer, sheet_name='Ratios', index=False)
            strat_df.to_excel(writer, sheet_name='Strategic', index=False)
            forecasts_df.to_excel(writer, sheet_name='Forecasts', index=False)
            comparison_df.to_excel(writer, sheet_name='Comparative_Analysis', index=False)
            ai_df.to_excel(writer, sheet_name='AI_Analysis', index=False)
            methodology_df.to_excel(writer, sheet_name='Methodology_Final', index=False)

        messagebox.showinfo(self._t('msg_success'), f"{self._translate_ui_text('تم حفظ الملف')}:\n{fn_path}")

    def export_to_excel_safe(self):
        """
        Safe wrapper:
        1) try full export path
        2) if anything fails, log and fallback to guaranteed minimal export
        """
        try:
            self.export_to_excel()
        except Exception as e:
            try:
                log_dir = Path("outputs")
                log_dir.mkdir(parents=True, exist_ok=True)
                log_path = log_dir / "export_error.log"
                with open(log_path, "a", encoding="utf-8") as fh:
                    fh.write(f"\n[{datetime.now().isoformat()}] Full export crashed in wrapper\n")
                    fh.write(f"Error: {repr(e)}\n")
                    fh.write(traceback.format_exc())
                    fh.write("\n" + ("-" * 80) + "\n")
            except Exception:
                pass
            self._export_to_excel_minimal()

    # ---------- Forecasts & Plots ----------
    def display_forecasts(self):
        if not self.current_data:
            return
        for i in self.forecast_tree.get_children():
            self.forecast_tree.delete(i)
        db = ((self.current_data.get('data_layers', {}) or {}).get('layer1_by_year') or self.current_data.get('data_by_year', {}) or {})
        # normalize year keys to int where possible
        norm_db = {}
        for yk, row in (db or {}).items():
            try:
                ky = int(yk)
            except Exception:
                continue
            norm_db[ky] = row or {}
        db = norm_db
        if not db:
            return
        # Robust call path: use fetcher forecast if available, otherwise local fallback.
        if hasattr(self.fetcher, 'generate_forecast'):
            rev_fore = self.fetcher.generate_forecast(db, metric='Revenues', years_forward=10)
            net_fore = self.fetcher.generate_forecast(db, metric='NetIncomeLoss', years_forward=10)
        else:
            rev_fore = self._generate_forecast_fallback(db, metric='Revenues', years_forward=10)
            net_fore = self._generate_forecast_fallback(db, metric='NetIncomeLoss', years_forward=10)
        cols = ['Ø§Ù„Ø³Ù†Ø©', 'Ø§Ù„Ø¥ÙŠØ±Ø§Ø¯Ø§Øª (Ù…ØªÙˆÙ‚Ø¹)', 'ØµØ§ÙÙŠ Ø§Ù„Ø±Ø¨Ø­ (Ù…ØªÙˆÙ‚Ø¹)']
        self.forecast_tree.config(columns=cols)
        for c in cols:
            self.forecast_tree.heading(c, text=c)
            self.forecast_tree.column(c, width=220)
        years = sorted(set(list(rev_fore.get('forecast', {}).keys()) + list(net_fore.get('forecast', {}).keys())))
        for y in years:
            rv = rev_fore.get('forecast', {}).get(y)
            nv = net_fore.get('forecast', {}).get(y)
            def fmt(v):
                if v is None:
                    return 'N/A'
                if isinstance(v, (int, float)):
                    if abs(v) > 1e9:
                        return f"{v/1e9:,.2f}B"
                    if abs(v) > 1e6:
                        return f"{v/1e6:,.2f}M"
                    return f"{v:,.0f}"
                return str(v)
            self.forecast_tree.insert('', 'end', values=(str(y), fmt(rv), fmt(nv)))

    def _generate_forecast_fallback(self, data_by_year, metric='Revenues', years_forward=10):
        metric_aliases = {
            'Revenues': ['Revenues', 'Revenue', 'SalesRevenueNet', 'TotalRevenue'],
            'NetIncomeLoss': ['NetIncomeLoss', 'NetIncome', 'ProfitLoss'],
        }
        aliases = metric_aliases.get(metric, [metric])
        hist = []
        for y in sorted([k for k in data_by_year.keys() if isinstance(k, int)]):
            row = data_by_year.get(y, {}) or {}
            val = None
            for k in aliases:
                v = row.get(k)
                if isinstance(v, (int, float)):
                    val = float(v)
                    break
            if val is not None:
                hist.append((y, val))
        out = {'metric': metric, 'forecast': {}, 'method': 'fallback_cagr_or_linear'}
        if len(hist) < 2:
            return out
        years = [x[0] for x in hist]
        vals = [x[1] for x in hist]
        y0, y1 = years[0], years[-1]
        v0, v1 = vals[0], vals[-1]
        n = max(1, y1 - y0)
        # Prefer CAGR for positive anchors, fallback to linear trend otherwise.
        use_cagr = (v0 > 0 and v1 > 0)
        last_y = y1
        last_v = v1
        if use_cagr:
            growth = (v1 / v0) ** (1.0 / n) - 1.0
            growth = max(min(growth, 0.35), -0.35)
            for i in range(1, int(years_forward) + 1):
                fy = last_y + i
                fv = last_v * ((1.0 + growth) ** i)
                out['forecast'][fy] = fv
        else:
            slope = (v1 - v0) / float(n)
            for i in range(1, int(years_forward) + 1):
                fy = last_y + i
                fv = last_v + (slope * i)
                out['forecast'][fy] = fv
        return out

    def open_plots_window(self):
        if not self.multi_company_data:
            messagebox.showinfo("Ù…Ø¹Ù„ÙˆÙ…Ø©", "Ù‚Ù… Ø¨Ø¬Ù„Ø¨ Ø¨ÙŠØ§ï¿½ï¿½Ø§Øª Ø´Ø±ÙƒØ© ÙˆØ§Ø­Ø¯Ø© Ø¹Ù„Ù‰ Ø§Ù„Ø£Ù‚Ù„")
            return
        win = tk.Toplevel(self.root)
        win.title("Ø§Ø®ØªÙŠØ§Ø± Ø§Ù„Ø´Ø±ÙƒØ© Ù„Ù„Ø±Ø³Ù…")
        tk.Label(win, text="Ø§Ø®ØªØ± Ø´Ø±ÙƒØ©:").pack(padx=8, pady=8)
        lb = tk.Listbox(win)
        lb.pack(padx=8, pady=8)
        for comp in sorted(self.multi_company_data.keys()):
            lb.insert(tk.END, comp)
        def on_plot():
            sel = lb.curselection()
            if not sel:
                messagebox.showinfo("Ù…Ø¹Ù„ÙˆÙ…Ø©", "Ø§Ø®ØªØ± Ø´Ø±ÙƒØ©")
                return
            comp = lb.get(sel[0])
            self.plot_trends(company_key=comp)
        tk.Button(win, text="Ø¹Ø±Ø¶ Ø§Ù„Ø±Ø³Ù…", command=on_plot).pack(pady=8)

    def plot_trends(self, company_key=None):
        if company_key is None:
            if not self.current_data:
                return
            data = self.current_data
        else:
            data = self.multi_company_data.get(company_key)
            if not data:
                messagebox.showinfo("Ù…Ø¹Ù„ÙˆÙ…Ø©", "Ø§Ù„Ø´Ø±ÙƒØ© ØºÙŠØ± Ù…ÙˆØ¬ÙˆØ¯Ø©")
                return
        db = data.get('data_by_year', {}) or {}
        years = sorted([y for y in db.keys() if isinstance(y, int)])
        if not years:
            messagebox.showinfo("Ù…Ø¹Ù„ÙˆÙ…Ø©", "Ù„Ø§ ØªÙˆØ¬Ø¯ Ø¨ÙŠØ§Ù†Ø§Øª ØªØ§Ø±ÙŠØ®ÙŠØ© ÙƒØ§ÙÙŠØ© Ù„Ù„Ø±Ø³Ù…")
            return
        revs = [db[y].get('Revenues') or db[y].get('SalesRevenueNet') or 0 for y in years]
        nets = [db[y].get('NetIncomeLoss') or 0 for y in years]
        fig = Figure(figsize=(9, 4), dpi=100)
        ax1 = fig.add_subplot(111)
        ax1.plot(years, revs, marker='o', label='Revenues')
        ax1.plot(years, nets, marker='o', label='Net Income')
        ax1.set_xlabel('Year'); ax1.set_ylabel('USD'); ax1.legend(loc='upper left')
        win = tk.Toplevel(self.root)
        win.title(f"Trends - {company_key or self.current_data.get('company_info', {}).get('ticker','')}")
        canvas = FigureCanvasTkAgg(fig, master=win); canvas.draw(); canvas.get_tk_widget().pack(fill='both', expand=True)

def main():
    root = tk.Tk()
    app = SECFinancialSystem(root)
    root.mainloop()

if __name__ == '__main__':
    main()


