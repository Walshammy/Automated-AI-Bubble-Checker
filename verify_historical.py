import pandas as pd

# Read the combined dataset
df = pd.read_excel(r'C:\Users\james\OneDrive - Silverdale Medical Limited\AIbubble\bubble_indicators_combined.xlsx', sheet_name='Combined Data')

print('Historical Data Verification:')
print(f'Total records: {len(df)}')
print(f'Date range: {df["date"].min()} to {df["date"].max()}')
print(f'Historical records: {len(df[df["is_historical"] == True])}')
print(f'Current records: {len(df[df["is_historical"] == False])}')

print('\nFirst few historical records:')
hist_df = df[df['is_historical'] == True].head()
print(hist_df[['date', 'sp500_price', 'vix_level', 'ten_year_treasury']].to_string())

print('\nLatest current record:')
current_df = df[df['is_historical'] == False]
if len(current_df) > 0:
    print(current_df[['date', 'sp500_price', 'vix_level', 'ten_year_treasury']].to_string())

print('\nSample of historical S&P 500 prices:')
sp500_hist = df[df['is_historical'] == True]['sp500_price'].dropna()
print(f'Min S&P 500: ${sp500_hist.min():,.2f}')
print(f'Max S&P 500: ${sp500_hist.max():,.2f}')
print(f'Average S&P 500: ${sp500_hist.mean():,.2f}')

print('\nSample of historical VIX levels:')
vix_hist = df[df['is_historical'] == True]['vix_level'].dropna()
print(f'Min VIX: {vix_hist.min():.2f}')
print(f'Max VIX: {vix_hist.max():.2f}')
print(f'Average VIX: {vix_hist.mean():.2f}')
