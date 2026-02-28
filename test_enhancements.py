#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_enhancements.py
Ø§Ø®ØªØ¨Ø§Ø± Ø³Ø±ÙŠØ¹ Ù„Ù„ØªØ­Ù‚Ù‚ Ù…Ù† Ø£Ù† Ø¬Ù…ÙŠØ¹ Ø§Ù„ØªØ­Ø³ÙŠÙ†Ø§Øª ØªØ¹Ù…Ù„ Ø¨Ø´ÙƒÙ„ ØµØ­ÙŠØ­
"""

import sys
import os

# Ø¥Ø¶Ø§ÙØ© Ø§Ù„Ù…Ø¬Ù„Ø¯ Ø§Ù„Ø­Ø§Ù„ÙŠ Ù„Ù„Ù€ path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    from modules.sec_fetcher import SECDataFetcher
    print("âœ… ØªÙ… Ø§Ø³ØªÙŠØ±Ø§Ø¯ SECDataFetcher Ø¨Ù†Ø¬Ø§Ø­")
except Exception as e:
    print(f"âŒ ÙØ´Ù„ Ø§Ø³ØªÙŠØ±Ø§Ø¯ SECDataFetcher: {e}")
    sys.exit(1)

def test_market_data():
    """Ø§Ø®ØªØ¨Ø§Ø± Ø¬Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ (Point 3, 4, 5)"""
    print("\n" + "="*60)
    print("ðŸ§ª Ø§Ø®ØªØ¨Ø§Ø± 1: Ø¬Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ (Beta, Price, Shares)")
    print("="*60)
    
    fetcher = SECDataFetcher()
    
    # Ø§Ø®ØªØ¨Ø§Ø± Ø¹Ù„Ù‰ Apple
    ticker = "AAPL"
    print(f"\nØ¬Ù„Ø¨ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚ Ù„Ù€ {ticker}...")
    
    try:
        market_data = fetcher.get_market_data(ticker)
        
        print(f"\nØ§Ù„Ù†ØªØ§Ø¦Ø¬:")
        print(f"  Ø§Ù„Ø³Ø¹Ø±: ${market_data.get('price', 'N/A')}")
        print(f"  Ø¹Ø¯Ø¯ Ø§Ù„Ø£Ø³Ù‡Ù…: {market_data.get('shares', 'N/A'):,}" if market_data.get('shares') else "  Ø¹Ø¯Ø¯ Ø§Ù„Ø£Ø³Ù‡Ù…: N/A")
        print(f"  Ø§Ù„Ù‚ÙŠÙ…Ø© Ø§Ù„Ø³ÙˆÙ‚ÙŠØ©: ${market_data.get('market_cap', 'N/A'):,}" if market_data.get('market_cap') else "  Ø§Ù„Ù‚ÙŠÙ…Ø© Ø§Ù„Ø³ÙˆÙ‚ÙŠØ©: N/A")
        print(f"  âœ… Beta: {market_data.get('beta', 'N/A')}")
        
        # Ø§Ù„ØªØ­Ù‚Ù‚
        if market_data.get('price') is not None:
            print("\nâœ… Point 3: Live Price - ÙŠØ¹Ù…Ù„!")
        else:
            print("\nâš ï¸ Point 3: Live Price - Ù„Ù… ÙŠÙØ¬Ù„Ø¨ (ØªØ­Ù‚Ù‚ Ù…Ù† Ø§Ù„Ø§ØªØµØ§Ù„)")
            
        if market_data.get('beta') is not None:
            print("âœ… Point 4: Beta - ÙŠØ¹Ù…Ù„!")
        else:
            print("âš ï¸ Point 4: Beta - ØºÙŠØ± Ù…ØªÙˆÙØ± (Ø·Ø¨ÙŠØ¹ÙŠ Ù„Ø¨Ø¹Ø¶ Ø§Ù„Ø´Ø±ÙƒØ§Øª)")
            
        if market_data.get('shares') is not None:
            print("âœ… Point 5: Shares Outstanding - ÙŠØ¹Ù…Ù„!")
        else:
            print("âš ï¸ Point 5: Shares - Ù„Ù… ÙŠÙØ¬Ù„Ø¨")
            
        return True
    except Exception as e:
        print(f"âŒ Ø®Ø·Ø£: {e}")
        return False

def test_dynamic_mapping():
    """Ø§Ø®ØªØ¨Ø§Ø± Dynamic Mapping Ù„Ù„Ù€ AR Ùˆ Dividends (Point 1, 2)"""
    print("\n" + "="*60)
    print("ðŸ§ª Ø§Ø®ØªØ¨Ø§Ø± 2: Dynamic Mapping (AR, Dividends, Shares)")
    print("="*60)
    
    fetcher = SECDataFetcher()
    
    # Ø¥Ù†Ø´Ø§Ø¡ items_by_concept ÙˆÙ‡Ù…ÙŠ Ù„Ù„Ø§Ø®ØªØ¨Ø§Ø±
    test_items = {
        'AccountsReceivableNetCurrent': {'2024-FY': {'value': 100000}},
        'PaymentsOfDividendsCommonStock': {'2024-FY': {'value': 50000}},
        'EntityCommonStockSharesOutstanding': {'2024-FY': {'value': 1000000}}
    }
    
    print("\nØ§Ø®ØªØ¨Ø§Ø± Dynamic Mapping...")
    dynamic_map = fetcher._discover_and_extend_alt_map(test_items)
    
    print(f"\nØ§Ù„Ù†ØªØ§Ø¦Ø¬:")
    print(f"  AR mapping: {dynamic_map.get('ar', [])}")
    print(f"  Dividends mapping: {dynamic_map.get('dividends', [])}")
    print(f"  Shares mapping: {dynamic_map.get('shares', [])}")
    
    # Ø§Ù„ØªØ­Ù‚Ù‚
    ar_ok = 'AccountsReceivableNetCurrent' in dynamic_map.get('ar', [])
    div_ok = 'PaymentsOfDividendsCommonStock' in dynamic_map.get('dividends', [])
    shares_ok = 'EntityCommonStockSharesOutstanding' in dynamic_map.get('shares', [])
    
    if ar_ok:
        print("\nâœ… Point 1: AR Mapping - ÙŠØ¹Ù…Ù„!")
    else:
        print("\nâŒ Point 1: AR Mapping - Ù„Ø§ ÙŠØ¹Ù…Ù„")
        
    if div_ok:
        print("âœ… Point 2: Dividends Mapping - ÙŠØ¹Ù…Ù„!")
    else:
        print("âŒ Point 2: Dividends Mapping - Ù„Ø§ ÙŠØ¹Ù…Ù„")
        
    if shares_ok:
        print("âœ… Point 5: Shares Mapping - ÙŠØ¹Ù…Ù„!")
    else:
        print("âŒ Point 5: Shares Mapping - Ù„Ø§ ÙŠØ¹Ù…Ù„")
        
    return ar_ok and div_ok and shares_ok

def test_ratios_calculation():
    """Ø§Ø®ØªØ¨Ø§Ø± Ø­Ø³Ø§Ø¨ Ø§Ù„Ù†Ø³Ø¨ Ø§Ù„Ù…Ø§Ù„ÙŠØ© Ø§Ù„Ù…Ø­Ø³Ù‘Ù†Ø©"""
    print("\n" + "="*60)
    print("ðŸ§ª Ø§Ø®ØªØ¨Ø§Ø± 3: Ø­Ø³Ø§Ø¨ Ø§Ù„Ù†Ø³Ø¨ Ø§Ù„Ù…Ø§Ù„ÙŠØ© Ø§Ù„Ù…Ø­Ø³Ù‘Ù†Ø©")
    print("="*60)
    
    # Ø¨ÙŠØ§Ù†Ø§Øª ÙˆÙ‡Ù…ÙŠØ© Ù„Ù„Ø§Ø®ØªØ¨Ø§Ø±
    data_by_year = {
        2024: {
            'NetIncomeLoss': 100000000,
            'Revenues': 500000000,
            'Assets': 1000000000,
            'StockholdersEquity': 600000000,
            'PaymentsOfDividendsCommonStock': 25000000,
            'WeightedAverageNumberOfSharesOutstandingBasic': 10000000,
            'NetCashProvidedByUsedInOperatingActivities': 120000000,
            'PaymentsToAcquirePropertyPlantAndEquipment': 20000000,
            'AccountsReceivableNetCurrent': 50000000
        }
    }
    
    fetcher = SECDataFetcher()
    
    # ØªØ¹ÙŠÙŠÙ† dynamic map
    fetcher.latest_dynamic_map = {
        'dividends': ['PaymentsOfDividendsCommonStock'],
        'shares': ['WeightedAverageNumberOfSharesOutstandingBasic'],
        'ar': ['AccountsReceivableNetCurrent']
    }
    
    print("\nØ­Ø³Ø§Ø¨ Ø§Ù„Ù†Ø³Ø¨ Ø§Ù„Ù…Ø§Ù„ÙŠØ©...")
    try:
        ratios = fetcher._calculate_financial_ratios(data_by_year)
        
        if 2024 in ratios:
            r = ratios[2024]
            print(f"\nØ§Ù„Ù†ØªØ§Ø¦Ø¬ Ù„Ø³Ù†Ø© 2024:")
            print(f"  ROE: {r.get('roe', 'N/A'):.2f}%" if r.get('roe') else "  ROE: N/A")
            print(f"  âœ… Retention Ratio: {r.get('retention_ratio', 'N/A'):.2%}" if r.get('retention_ratio') is not None else "  Retention Ratio: N/A")
            print(f"  âœ… SGR Internal: {r.get('sgr_internal', 'N/A'):.2%}" if r.get('sgr_internal') is not None else "  SGR Internal: N/A")
            print(f"  âœ… FCF per Share: ${r.get('fcf_per_share', 'N/A'):.2f}" if r.get('fcf_per_share') is not None else "  FCF per Share: N/A")
            print(f"  âœ… Dividends Paid: ${r.get('dividends_paid', 'N/A'):,.0f}" if r.get('dividends_paid') is not None else "  Dividends Paid: N/A")
            print(f"  DSO (AR Days): {r.get('days_sales_outstanding', 'N/A'):.1f} days" if r.get('days_sales_outstanding') else "  DSO: N/A")
            
            # Ø§Ù„ØªØ­Ù‚Ù‚
            checks = []
            if r.get('retention_ratio') is not None:
                print("\nâœ… Point 2: Retention Ratio - ÙŠÙØ­Ø³Ø¨ Ø¨Ø´ÙƒÙ„ ØµØ­ÙŠØ­!")
                checks.append(True)
            else:
                print("\nâŒ Point 2: Retention Ratio - Ù„Ø§ ÙŠÙØ­Ø³Ø¨")
                checks.append(False)
                
            if r.get('sgr_internal') is not None:
                print("âœ… Point 2: SGR Internal - ÙŠÙØ­Ø³Ø¨ Ø¨Ø´ÙƒÙ„ ØµØ­ÙŠØ­!")
                checks.append(True)
            else:
                print("âŒ Point 2: SGR Internal - Ù„Ø§ ÙŠÙØ­Ø³Ø¨")
                checks.append(False)
                
            if r.get('fcf_per_share') is not None:
                print("âœ… Point 5: FCF per Share - ÙŠÙØ­Ø³Ø¨ Ø¨Ø´ÙƒÙ„ ØµØ­ÙŠØ­!")
                checks.append(True)
            else:
                print("âŒ Point 5: FCF per Share - Ù„Ø§ ÙŠÙØ­Ø³Ø¨")
                checks.append(False)
                
            if r.get('days_sales_outstanding') is not None:
                print("âœ… Point 1: AR Days - ÙŠÙØ­Ø³Ø¨ Ø¨Ø´ÙƒÙ„ ØµØ­ÙŠØ­!")
                checks.append(True)
            else:
                print("âŒ Point 1: AR Days - Ù„Ø§ ÙŠÙØ­Ø³Ø¨")
                checks.append(False)
                
            return all(checks)
        else:
            print("âŒ Ù„Ù… ÙŠØªÙ… Ø­Ø³Ø§Ø¨ Ø§Ù„Ù†Ø³Ø¨")
            return False
            
    except Exception as e:
        print(f"âŒ Ø®Ø·Ø£ ÙÙŠ Ø­Ø³Ø§Ø¨ Ø§Ù„Ù†Ø³Ø¨: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """ØªØ´ØºÙŠÙ„ Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø§Ø®ØªØ¨Ø§Ø±Ø§Øª"""
    print("\n" + "="*60)
    print("ðŸš€ Ø¨Ø¯Ø¡ Ø§Ù„Ø§Ø®ØªØ¨Ø§Ø±Ø§Øª Ø§Ù„Ø´Ø§Ù…Ù„Ø©")
    print("="*60)
    
    results = []
    
    # Ø§Ø®ØªØ¨Ø§Ø± 1: Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø³ÙˆÙ‚
    results.append(("Market Data", test_market_data()))
    
    # Ø§Ø®ØªØ¨Ø§Ø± 2: Dynamic Mapping
    results.append(("Dynamic Mapping", test_dynamic_mapping()))
    
    # Ø§Ø®ØªØ¨Ø§Ø± 3: Ø­Ø³Ø§Ø¨ Ø§Ù„Ù†Ø³Ø¨
    results.append(("Ratios Calculation", test_ratios_calculation()))
    
    # Ù…Ù„Ø®Øµ Ø§Ù„Ù†ØªØ§Ø¦Ø¬
    print("\n" + "="*60)
    print("ðŸ“Š Ù…Ù„Ø®Øµ Ø§Ù„Ù†ØªØ§Ø¦Ø¬")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "âœ… Ù†Ø¬Ø­" if result else "âŒ ÙØ´Ù„"
        print(f"  {name}: {status}")
    
    print(f"\nØ§Ù„Ø¥Ø¬Ù…Ø§Ù„ÙŠ: {passed}/{total} Ø§Ø®ØªØ¨Ø§Ø±Ø§Øª Ù†Ø¬Ø­Øª")
    
    if passed == total:
        print("\nðŸŽ‰ Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø§Ø®ØªØ¨Ø§Ø±Ø§Øª Ù†Ø¬Ø­Øª! Ø§Ù„Ù†Ø¸Ø§Ù… Ø¬Ø§Ù‡Ø² Ù„Ù„Ø§Ø³ØªØ®Ø¯Ø§Ù….")
        return 0
    else:
        print(f"\nâš ï¸ Ø¨Ø¹Ø¶ Ø§Ù„Ø§Ø®ØªØ¨Ø§Ø±Ø§Øª ÙØ´Ù„Øª ({total-passed}/{total})")
        print("Ø±Ø§Ø¬Ø¹ Ø§Ù„Ø£Ø®Ø·Ø§Ø¡ Ø£Ø¹Ù„Ø§Ù‡ Ù„Ù„ØªÙØ§ØµÙŠÙ„.")
        return 1

if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nâš ï¸ ØªÙ… Ø¥Ù„ØºØ§Ø¡ Ø§Ù„Ø§Ø®ØªØ¨Ø§Ø±Ø§Øª")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nâŒ Ø®Ø·Ø£ ØºÙŠØ± Ù…ØªÙˆÙ‚Ø¹: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

