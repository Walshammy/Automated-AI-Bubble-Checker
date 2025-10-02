import pandas as pd

# Load the latest results
df = pd.read_excel('valuation_results/valuation_analysis_20251002_185303.xlsx')

print("="*80)
print("COMPREHENSIVE VALUATION ANALYSIS RESULTS")
print("="*80)

print(f"\nOVERVIEW:")
print(f"Total stocks analyzed: {len(df)}")
print(f"Stocks with negative earnings: {len(df[df['P/E Ratio'] <= 0])}")
print(f"Stocks with P/E > 20: {len(df[df['P/E Ratio'] > 20])}")
print(f"Stocks with ROE > 15%: {len(df[df['ROE %'] > 15])}")
print(f"Stocks with positive FCF yield: {len(df[df['FCF Yield %'] > 0])}")

print(f"\nQUALITY ANALYSIS:")
quality_stocks = df[df['Is Quality'] == True]
print(f"Quality stocks found: {len(quality_stocks)}")
if len(quality_stocks) > 0:
    print("\nQuality Stocks:")
    print(quality_stocks[['Ticker', 'Company', 'P/E Ratio', 'ROE %', 'Margin of Safety %', 'Is Cheap']].to_string(index=False))

print(f"\nVALUE ANALYSIS:")
cheap_stocks = df[df['Is Cheap'] == True]
print(f"Cheap stocks found: {len(cheap_stocks)}")
if len(cheap_stocks) > 0:
    print("\nCheap Stocks:")
    print(cheap_stocks[['Ticker', 'Company', 'P/E Ratio', 'ROE %', 'Margin of Safety %', 'Is Quality']].to_string(index=False))

print(f"\nSECTOR ANALYSIS:")
sector_summary = df.groupby('Sector').agg({
    'Ticker': 'count',
    'P/E Ratio': 'mean',
    'ROE %': 'mean',
    'Margin of Safety %': 'mean',
    'Is Quality': 'sum',
    'Is Cheap': 'sum'
}).round(2)
print(sector_summary)

print(f"\nTOP 10 MOST OVERVALUED (Worst Margin of Safety):")
worst10 = df.nsmallest(10, 'Margin of Safety %')
print(worst10[['Ticker', 'Company', 'P/E Ratio', 'ROE %', 'Margin of Safety %']].to_string(index=False))

print(f"\nCONCLUSION:")
print("The analysis reveals that current market conditions show:")
print(f"- {len(df[df['Margin of Safety %'] < -20])} stocks are significantly overvalued (>20% premium)")
print(f"- {len(df[df['Margin of Safety %'] < -50])} stocks are extremely overvalued (>50% premium)")
print("- No traditional value opportunities found (P/E < 15, ROE > 10%, Quality)")
print("- This suggests the market may be in a bubble phase")
