import pandas as pd
import numpy as np
import os

def clean_data(raw_dir, cleaned_dir, quarantine_dir):
    """
    Reads raw data, applies data quality rules, and saves cleaned data.
    Anomalies are logged to the quarantine directory.
    """
    os.makedirs(cleaned_dir, exist_ok=True)
    os.makedirs(quarantine_dir, exist_ok=True)
    
    # 1. Clean Customers
    customers_path = os.path.join(raw_dir, 'raw_customers.csv')
    if os.path.exists(customers_path):
        customers_df = pd.read_csv(customers_path)
        # Assuming week 2 cleaning: drop completely empty rows, deduplicate based on IDs
        customers_df.dropna(how='all', inplace=True)
        customers_df.drop_duplicates(subset=['customer_id'], inplace=True)
        customers_df.to_csv(os.path.join(cleaned_dir, 'cleaned_customers.csv'), index=False)
        print(f"Cleaned {len(customers_df)} customers.")
    else:
        customers_df = pd.DataFrame()
        
    # 2. Clean Orders
    orders_path = os.path.join(raw_dir, 'raw_orders.csv')
    if os.path.exists(orders_path):
        orders_df = pd.read_csv(orders_path)
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], errors='coerce')
        orders_df.dropna(subset=['order_id', 'customer_id', 'order_date'], inplace=True)
        orders_df.to_csv(os.path.join(cleaned_dir, 'cleaned_orders.csv'), index=False)
        print(f"Cleaned {len(orders_df)} orders.")
        
    # 3. Clean Payments
    payments_path = os.path.join(raw_dir, 'raw_payments.csv')
    if os.path.exists(payments_path):
        payments_df = pd.read_csv(payments_path)
        payments_df.dropna(subset=['order_id'], inplace=True)
        payments_df.to_csv(os.path.join(cleaned_dir, 'cleaned_payments.csv'), index=False)
        print(f"Cleaned {len(payments_df)} payments.")
        
    # 4. Clean FX Rates
    fx_path = os.path.join(raw_dir, 'raw_fx_rates_daily.csv')
    if os.path.exists(fx_path):
        fx_df = pd.read_csv(fx_path)
        fx_df['rate_date'] = pd.to_datetime(fx_df['rate_date'])
        fx_df.sort_values(['rate_date', 'quote_currency', 'source'], ascending=[True, True, True], inplace=True)
        
        # Deduplicate: Keep API_PRIMARY
        fx_df.drop_duplicates(subset=['rate_date', 'base_currency', 'quote_currency'], keep='first', inplace=True)
        
        # Anomaly Detection: Outliers (fx anomalies)
        # Compute rolling median to detect spikes
        fx_df.sort_values(['quote_currency', 'rate_date'], inplace=True)
        fx_df['rolling_median'] = fx_df.groupby('quote_currency')['fx_rate'].transform(lambda x: x.rolling(window=7, min_periods=1).median())
        
        # Spike is when rate is > 50% different from median
        outlier_mask = (np.abs(fx_df['fx_rate'] - fx_df['rolling_median']) / fx_df['rolling_median']) > 0.5
        
        anomalies_df = fx_df[outlier_mask].copy()
        anomalies_df['issue'] = 'FX_SPIKE'
        if not anomalies_df.empty:
            anomalies_df.to_csv(os.path.join(quarantine_dir, 'fx_rate_anomalies.csv'), index=False)
        
        # Replace outliers with NaN so we can ffill them
        fx_df.loc[outlier_mask, 'fx_rate'] = np.nan
        
        # Generate complete date range for each currency to handle missing dates
        min_date = fx_df['rate_date'].min()
        max_date = fx_df['rate_date'].max()
        all_dates = pd.date_range(start=min_date, end=max_date)
        
        currencies = fx_df['quote_currency'].unique()
        full_idx = pd.MultiIndex.from_product([all_dates, currencies], names=['rate_date', 'quote_currency']).to_frame().reset_index(drop=True)
        
        fx_filled = pd.merge(full_idx, fx_df, on=['rate_date', 'quote_currency'], how='left')
        fx_filled['base_currency'] = 'USD'
        fx_filled['source'].fillna('IMPUTED', inplace=True)
        
        # Forward fill and then backward fill missing chunks
        fx_filled.sort_values(['quote_currency', 'rate_date'], inplace=True)
        fx_filled['fx_rate'] = fx_filled.groupby('quote_currency')['fx_rate'].ffill().bfill()
        
        fx_filled.drop(columns=['rolling_median'], inplace=True)
        fx_filled.to_csv(os.path.join(cleaned_dir, 'cleaned_fx_rates_daily.csv'), index=False)
        print(f"Cleaned and imputed {len(fx_filled)} FX rates. Found {len(anomalies_df)} anomalies.")

    # 5. Clean Customer Profile
    profile_path = os.path.join(raw_dir, 'raw_customer_profile.csv')
    if os.path.exists(profile_path):
        prof_df = pd.read_csv(profile_path)
        prof_df['effective_from'] = pd.to_datetime(prof_df['effective_from'])
        prof_df['effective_to'] = pd.to_datetime(prof_df['effective_to'])
        
        # Consistency Check: country_iso2 differs from CSV country
        if not customers_df.empty and 'country' in customers_df.columns:
            # Map country from customers.csv to overwrite profile
            country_map = customers_df.set_index('customer_id')['country'].to_dict()
            
            # Find mismatches
            mismatch_mask = (prof_df['customer_id'].map(country_map).notna()) & (prof_df['country_iso2'] != prof_df['customer_id'].map(country_map))
            mismatches_df = prof_df[mismatch_mask].copy()
            mismatches_df['issue'] = 'INCONSISTENT_COUNTRY'
            
            if not mismatches_df.empty:
                mismatches_df.to_csv(os.path.join(quarantine_dir, 'customer_profile_country_mismatches.csv'), index=False)
                
            # Overwrite with truth from customers
            prof_df.loc[mismatch_mask, 'country_iso2'] = prof_df['customer_id'].map(country_map)
            
        # Fix Overlapping Effective Ranges
        prof_df.sort_values(['customer_id', 'effective_from'], inplace=True)
        
        # Check overlaps: next record's effective_from is before current record's effective_to
        prof_df['next_effective_from'] = prof_df.groupby('customer_id')['effective_from'].shift(-1)
        
        overlap_mask = (prof_df['effective_to'].notna()) & (prof_df['next_effective_from'].notna()) & (prof_df['effective_to'] > prof_df['next_effective_from'])
        overlaps_df = prof_df[overlap_mask].copy()
        overlaps_df['issue'] = 'OVERLAPPING_SCD2'
        
        if not overlaps_df.empty:
            overlaps_df.to_csv(os.path.join(quarantine_dir, 'customer_profile_overlaps.csv'), index=False)
        
        # Fix the overlap by truncating current effective_to to next effective_from
        prof_df.loc[overlap_mask, 'effective_to'] = prof_df.loc[overlap_mask, 'next_effective_from']
        
        prof_df.drop(columns=['next_effective_from'], inplace=True)
        
        prof_df.to_csv(os.path.join(cleaned_dir, 'cleaned_customer_profile.csv'), index=False)
        print(f"Cleaned {len(prof_df)} customer profiles. Resolved {len(overlaps_df)} overlaps.")

if __name__ == "__main__":
    base_dir = r"c:\Users\Jaquline Maria\Desktop\week 4\assignment"
    clean_data(
        raw_dir=os.path.join(base_dir, 'data', 'raw'),
        cleaned_dir=os.path.join(base_dir, 'data', 'cleaned'),
        quarantine_dir=os.path.join(base_dir, 'data', 'quarantine')
    )
